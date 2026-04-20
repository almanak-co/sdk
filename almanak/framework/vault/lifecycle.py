# MULTI-WALLET: Vault lifecycle currently uses a single wallet. When multi-wallet
# support is enabled, settlement must handle per-chain vault wallets.
"""VaultLifecycleManager - manages vault settlement lifecycle and state."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.core.models.config import VaultVersion
from almanak.core.models.params import SettleDepositParams, SettleRedeemParams, UpdateTotalAssetsParams
from almanak.framework.connectors.lagoon.adapter import LagoonVaultAdapter
from almanak.framework.connectors.lagoon.sdk import LagoonVaultSDK
from almanak.framework.data.tokens import get_token_resolver
from almanak.framework.vault.config import SettlementPhase, SettlementResult, VaultAction, VaultConfig, VaultState

logger = logging.getLogger(__name__)

# Key used to store vault state within the strategy state dict
VAULT_STATE_KEY = "vault_state"


def validate_nav_change_bps(
    old_total_assets: int,
    new_total_assets: int,
    max_up_bps: int = 1000,
    max_down_bps: int = 500,
) -> tuple[bool, str]:
    """Validate NAV change is within bounds.

    Args:
        old_total_assets: Previous total assets value.
        new_total_assets: Proposed new total assets value.
        max_up_bps: Maximum allowed increase in basis points (default 10%).
        max_down_bps: Maximum allowed decrease in basis points (default 5%).

    Returns:
        Tuple of (ok, reason). ok is True if within bounds.
    """
    if old_total_assets <= 0:
        return True, ""
    change_bps = abs(new_total_assets - old_total_assets) * 10000 // old_total_assets
    if new_total_assets > old_total_assets and change_bps > max_up_bps:
        return False, f"NAV increase {change_bps} bps exceeds max {max_up_bps} bps"
    if new_total_assets < old_total_assets and change_bps > max_down_bps:
        return False, f"NAV decrease {change_bps} bps exceeds max {max_down_bps} bps"
    return True, ""


class VaultLifecycleManager:
    """Manages the vault settlement lifecycle.

    Checks whether settlement is needed before each decide() cycle,
    and manages vault state persistence across strategy restarts.

    State is managed in-memory during a session. The caller (runner/CLI)
    is responsible for loading initial state and persisting updates via
    ``get_vault_state_dict()``. This decouples the lifecycle manager from
    specific state manager implementations (StateManager, GatewayStateManager).
    """

    def __init__(
        self,
        vault_config: VaultConfig,
        vault_sdk: LagoonVaultSDK,
        vault_adapter: LagoonVaultAdapter,
        execution_orchestrator: Any,
        strategy_id: str = "",
        initial_vault_state: dict | None = None,
        persistence_callback: Callable[[dict], None] | None = None,
    ) -> None:
        self._config = vault_config
        self._vault_sdk = vault_sdk
        self._vault_adapter = vault_adapter
        self._execution_orchestrator = execution_orchestrator
        self._strategy_id = strategy_id
        self._initial_vault_state = initial_vault_state
        self._vault_state: VaultState | None = None
        self._persistence_callback = persistence_callback
        self._preflight_done = False
        self._preflight_interval = 10  # Re-run every N settlements
        self._settlements_since_preflight = 0

    def pre_decide_hook(self, strategy: Any) -> VaultAction:
        """Check if settlement is needed before the strategy's decide() call.

        Args:
            strategy: The IntentStrategy instance (unused in this method,
                      but passed for consistency with the hook interface).

        Returns:
            VaultAction indicating what the lifecycle manager should do.
        """
        vault_state = self.get_vault_state()

        # Check for interrupted settlements first
        if vault_state.settlement_phase != SettlementPhase.IDLE:
            logger.info(
                "Vault settlement interrupted in phase %s, resuming",
                vault_state.settlement_phase.value,
            )
            return VaultAction.RESUME_SETTLE

        # Check if settlement interval has elapsed
        if vault_state.last_valuation_time is None:
            # Never settled before - settle now
            logger.info("Vault has never been settled, initiating first settlement")
            return VaultAction.SETTLE

        elapsed_minutes = (datetime.now(UTC) - vault_state.last_valuation_time).total_seconds() / 60
        if elapsed_minutes >= self._config.settlement_interval_minutes:
            logger.info(
                "Settlement interval elapsed (%.1f min >= %d min), initiating settlement",
                elapsed_minutes,
                self._config.settlement_interval_minutes,
            )
            return VaultAction.SETTLE

        return VaultAction.HOLD

    async def run_settlement_cycle(self, strategy: Any) -> SettlementResult:
        """Execute the full propose-settle settlement cycle with crash recovery.

        Supports resumption from any phase if the process crashed mid-settlement.
        On entry, checks the current settlement_phase and resumes from the
        interrupted point:
        - IDLE: Start fresh (compute valuation, propose, settle)
        - PROPOSING: Check on-chain state; retry propose if needed
        - PROPOSED: Skip propose, proceed to settle
        - SETTLING: Check on-chain state; retry settle if needed
        - SETTLED: Complete finalization to IDLE

        Args:
            strategy: The IntentStrategy instance with valuate() and
                create_market_snapshot() methods.

        Returns:
            SettlementResult describing the outcome of the settlement.
        """
        # Run preflight checks (once on first settlement, then periodically)
        if not self._preflight_done or self._settlements_since_preflight >= self._preflight_interval:
            try:
                self._preflight_checks(strategy)
                self._preflight_done = True
                self._settlements_since_preflight = 0
            except RuntimeError:
                logger.error("Preflight checks failed", exc_info=True)
                return SettlementResult(success=False)

        # MVP single-signer guard: gateway only has one private key, so
        # valuator_address must match the strategy's wallet_address.
        wallet_addr = getattr(strategy, "wallet_address", None)
        if wallet_addr and self._config.valuator_address.lower() != wallet_addr.lower():
            logger.error(
                "Signer mismatch: valuator_address (%s) != wallet_address (%s). "
                "MVP requires both to use the same signing key (ALMANAK_PRIVATE_KEY).",
                self._config.valuator_address,
                wallet_addr,
            )
            return SettlementResult(success=False)

        vault_state = self.get_vault_state()
        phase = vault_state.settlement_phase

        # --- Resume from SETTLED: just finalize ---
        if phase == SettlementPhase.SETTLED:
            logger.info("Resuming from SETTLED phase, finalizing")
            return self._finalize_settlement(vault_state)

        # --- Resume from SETTLING: check if settle already succeeded on-chain ---
        if phase == SettlementPhase.SETTLING:
            logger.info(
                "Resuming from SETTLING phase, checking on-chain state (nonce=%d)", vault_state.settlement_nonce
            )
            total_assets_raw = vault_state.last_proposed_total_assets
            on_chain_total_assets = self._vault_sdk.get_total_assets(self._config.vault_address)
            # Only skip if value matches AND this is from the current nonce (not a prior epoch)
            if (
                on_chain_total_assets == total_assets_raw
                and (total_assets_raw > 0 or not vault_state.initialized)
                and vault_state.settlement_nonce > 0
            ):
                logger.info(
                    "Settle already confirmed on-chain (total_assets=%d, nonce=%d), advancing",
                    on_chain_total_assets,
                    vault_state.settlement_nonce,
                )
                vault_state.settlement_phase = SettlementPhase.SETTLED
                self.save_vault_state()
                return self._finalize_settlement(vault_state)
            # Otherwise retry settle
            return await self._execute_settle(strategy, vault_state, total_assets_raw)

        # --- Resume from PROPOSED: skip propose, go to settle ---
        if phase == SettlementPhase.PROPOSED:
            logger.info("Resuming from PROPOSED phase, proceeding to settle")
            total_assets_raw = vault_state.last_proposed_total_assets
            return await self._execute_settle(strategy, vault_state, total_assets_raw)

        # --- Resume from PROPOSING: check if propose already succeeded on-chain ---
        if phase == SettlementPhase.PROPOSING:
            logger.info(
                "Resuming from PROPOSING phase, checking on-chain state (nonce=%d)", vault_state.settlement_nonce
            )
            total_assets_raw = vault_state.last_proposed_total_assets
            on_chain_proposed = self._vault_sdk.get_proposed_total_assets(self._config.vault_address)
            if (
                on_chain_proposed == total_assets_raw
                and (total_assets_raw > 0 or not vault_state.initialized)
                and vault_state.settlement_nonce > 0
            ):
                logger.info(
                    "Propose already confirmed on-chain (proposed=%d, nonce=%d), advancing",
                    on_chain_proposed,
                    vault_state.settlement_nonce,
                )
                vault_state.settlement_phase = SettlementPhase.PROPOSED
                self.save_vault_state()
                return await self._execute_settle(strategy, vault_state, total_assets_raw)
            # Otherwise retry propose
            return await self._execute_propose_and_settle(strategy, vault_state, total_assets_raw)

        # --- Start fresh from IDLE ---
        computed_assets = self._compute_total_assets(strategy, vault_state)
        if computed_assets is None:
            return SettlementResult(success=False)
        total_assets_raw = computed_assets

        # Validate valuation bounds
        if not self._validate_bounds(vault_state, total_assets_raw):
            return SettlementResult(success=False)

        # Execute full propose -> settle flow
        vault_state.last_proposed_total_assets = total_assets_raw
        return await self._execute_propose_and_settle(strategy, vault_state, total_assets_raw)

    def _preflight_checks(self, strategy: Any) -> None:
        """Run on-chain preflight checks before settlement.

        Verifies vault version, valuation manager, and curator alignment.
        Raises RuntimeError on mismatch to prevent settlement with misconfigured vault.
        """
        vault_address = self._config.vault_address

        # Check vault version
        try:
            self._vault_sdk.verify_version(vault_address, self._config.version)
        except ValueError as e:
            raise RuntimeError(f"Preflight failed: {e}") from e

        # Check valuation manager
        on_chain_valuator = self._vault_sdk.get_valuation_manager(vault_address)
        if on_chain_valuator.lower() != self._config.valuator_address.lower():
            raise RuntimeError(
                f"Preflight failed: on-chain valuation manager {on_chain_valuator} "
                f"!= configured {self._config.valuator_address}"
            )

        # Check curator (Safe)
        wallet_addr = getattr(strategy, "wallet_address", None)
        if wallet_addr:
            on_chain_curator = self._vault_sdk.get_curator(vault_address)
            if on_chain_curator.lower() != wallet_addr.lower():
                raise RuntimeError(
                    f"Preflight failed: on-chain curator {on_chain_curator} != strategy wallet {wallet_addr}"
                )

    def _compute_total_assets(self, strategy: Any, vault_state: VaultState) -> int | None:
        """Compute total assets from valuation. Returns None on failure."""
        # Guard: vault lifecycle is not compatible with multi-chain strategies
        chains = getattr(strategy, "chains", None) or []
        if len(chains) > 1:
            logger.error("Vault lifecycle does not support multi-chain strategies")
            return None

        market = strategy.create_market_snapshot()

        # Prefetch balances so default valuate() sees real holdings
        if hasattr(strategy, "_get_tracked_tokens"):
            try:
                tokens = strategy._get_tracked_tokens() or []
                for t in tokens:
                    try:
                        market.balance(t)
                    except (ValueError, AttributeError, RuntimeError):
                        logger.debug("Could not prefetch balance for %s", t)
            except (AttributeError, TypeError):
                logger.debug("Could not get tracked tokens from strategy")

        total_usd = strategy.valuate(market)
        logger.info("Vault valuation: $%s USD", total_usd)

        underlying_price = market.price(self._config.underlying_token)
        if underlying_price <= 0:
            logger.error("Invalid underlying token price: %s", underlying_price)
            return None

        decimals = get_token_resolver().get_decimals(strategy.chain, self._config.underlying_token)
        total_assets_raw = int(total_usd / underlying_price * Decimal(10) ** decimals)

        # First settlement guard: V0.5.0 requires total_assets=0 on first settle.
        # initialized is flipped to True in _finalize_settlement after success.
        if not vault_state.initialized:
            if self._config.version == VaultVersion.V0_5_0:
                logger.info("First settlement (V0.5.0): forcing total_assets=0")
                total_assets_raw = 0

        # Version-aware accounting: pre-V0.5.0 adds pending deposits
        if self._config.version < VaultVersion.V0_5_0:
            pending_deposits = self._vault_sdk.get_pending_deposits(self._config.vault_address)
            total_assets_raw += pending_deposits
            vault_state.last_pending_deposits = pending_deposits

        return total_assets_raw

    def _validate_bounds(self, vault_state: VaultState, total_assets_raw: int) -> bool:
        """Validate valuation change is within configured bounds. Returns False on violation."""
        ok, reason = validate_nav_change_bps(
            vault_state.last_total_assets,
            total_assets_raw,
            max_up_bps=self._config.max_valuation_change_up_bps,
            max_down_bps=self._config.min_valuation_change_down_bps,
        )
        if not ok:
            logger.warning("Valuation bounds check failed: %s, rejecting", reason)
            vault_state.settlement_phase = SettlementPhase.IDLE
            self.save_vault_state()
            return False
        return True

    async def _execute_propose_and_settle(
        self, strategy: Any, vault_state: VaultState, total_assets_raw: int
    ) -> SettlementResult:
        """Execute the propose transaction, then proceed to settle."""
        vault_state.settlement_phase = SettlementPhase.PROPOSING
        vault_state.settlement_nonce += 1
        self.save_vault_state()

        propose_bundle = self._vault_adapter.build_propose_valuation_bundle(
            UpdateTotalAssetsParams(
                vault_address=self._config.vault_address,
                valuator_address=self._config.valuator_address,
                new_total_assets=total_assets_raw,
                pending_deposits=vault_state.last_pending_deposits,
            )
        )
        propose_result = await self._execution_orchestrator.execute(
            propose_bundle,
            wallet_address=self._config.valuator_address,
        )
        if not propose_result.success:
            logger.error("Propose transaction failed: %s", propose_result.error)
            vault_state.settlement_phase = SettlementPhase.IDLE
            self.save_vault_state()
            return SettlementResult(success=False)

        vault_state.settlement_phase = SettlementPhase.PROPOSED
        self.save_vault_state()

        return await self._execute_settle(strategy, vault_state, total_assets_raw)

    async def _execute_settle(self, strategy: Any, vault_state: VaultState, total_assets_raw: int) -> SettlementResult:
        """Execute the settle deposit (and optionally redeem) transactions."""
        vault_state.settlement_phase = SettlementPhase.SETTLING
        self.save_vault_state()

        settle_deposit_bundle = self._vault_adapter.build_settle_deposit_bundle(
            SettleDepositParams(
                vault_address=self._config.vault_address,
                safe_address=strategy.wallet_address,
                total_assets=total_assets_raw,
            )
        )
        settle_deposit_result = await self._execution_orchestrator.execute(
            settle_deposit_bundle,
            wallet_address=strategy.wallet_address,
        )
        if not settle_deposit_result.success:
            logger.error("Settle deposit transaction failed: %s", settle_deposit_result.error)
            vault_state.settlement_phase = SettlementPhase.PROPOSED
            self.save_vault_state()
            return SettlementResult(success=False)

        # Parse deposit receipt for accounting (raw on-chain units, no decimal normalization --
        # receipt parser operates on event log integers; callers use raw values for state tracking)
        deposits_received = 0
        shares_minted = 0
        try:
            from almanak.framework.connectors.lagoon.receipt_parser import LagoonReceiptParser

            parser = LagoonReceiptParser()
            if hasattr(settle_deposit_result, "receipt") and settle_deposit_result.receipt:
                parsed = parser.parse_receipt(settle_deposit_result.receipt)
                if parsed.settle_deposits:
                    deposits_received = parsed.settle_deposits[0].assets_deposited
                    shares_minted = parsed.settle_deposits[0].shares_minted
        except Exception:
            logger.debug("Could not parse settle_deposit receipt for accounting (non-fatal)")

        # Settle redeems (if configured)
        if self._config.auto_settle_redeems:
            settle_redeem_bundle = self._vault_adapter.build_settle_redeem_bundle(
                SettleRedeemParams(
                    vault_address=self._config.vault_address,
                    safe_address=strategy.wallet_address,
                    total_assets=total_assets_raw,
                )
            )
            settle_redeem_result = await self._execution_orchestrator.execute(
                settle_redeem_bundle,
                wallet_address=strategy.wallet_address,
            )
            if not settle_redeem_result.success:
                if self._config.redeem_failure_fatal:
                    logger.error("Settle redeem transaction failed (fatal): %s", settle_redeem_result.error)
                    vault_state.settlement_phase = SettlementPhase.PROPOSED
                    self.save_vault_state()
                    return SettlementResult(success=False)
                logger.warning("Settle redeem transaction failed (non-fatal): %s", settle_redeem_result.error)

        vault_state.settlement_phase = SettlementPhase.SETTLED
        self.save_vault_state()

        return self._finalize_settlement(
            vault_state,
            deposits_received=deposits_received,
            shares_minted=shares_minted,
        )

    def _finalize_settlement(
        self,
        vault_state: VaultState,
        deposits_received: int = 0,
        shares_minted: int = 0,
        redemptions_processed: int = 0,
        shares_burned: int = 0,
    ) -> SettlementResult:
        """Finalize the settlement: update state and return result."""
        total_assets_raw = vault_state.last_proposed_total_assets
        vault_state.last_total_assets = total_assets_raw
        vault_state.last_valuation_time = datetime.now(UTC)
        vault_state.last_settlement_epoch += 1
        self._settlements_since_preflight += 1
        vault_state.settlement_phase = SettlementPhase.IDLE
        vault_state.settlement_nonce = 0

        # Flip initialized=True only after settlement succeeds (P1 fix)
        if not vault_state.initialized:
            vault_state.initialized = True

        self.save_vault_state()

        result = SettlementResult(
            success=True,
            new_total_assets=total_assets_raw,
            epoch_id=vault_state.last_settlement_epoch,
            deposits_received=deposits_received,
            shares_minted=shares_minted,
            redemptions_processed=redemptions_processed,
            shares_burned=shares_burned,
        )

        logger.info(
            "Settlement cycle complete: epoch=%d, total_assets=%d",
            result.epoch_id,
            result.new_total_assets,
        )
        return result

    def get_vault_state(self) -> VaultState:
        """Get the current vault state, loading from initial state if needed.

        Returns:
            Current VaultState. Returns default VaultState if no initial state was provided.
        """
        if self._vault_state is not None:
            return self._vault_state

        self._vault_state = self._load_vault_state()
        return self._vault_state

    def save_vault_state(self) -> None:
        """Persist vault state via the callback if provided.

        This is called at semantic save-points during the settlement state
        machine (phase transitions, bounds rejection, etc.). If a persistence
        callback was provided at construction, the serialized state is passed
        to it. Persistence failures are logged but do not abort settlement.
        """
        if self._vault_state is None or self._persistence_callback is None:
            return

        try:
            state_dict = self._serialize_vault_state(self._vault_state)
            self._persistence_callback(state_dict)
        except Exception:
            logger.warning("Failed to persist vault state via callback", exc_info=True)

    def get_vault_state_dict(self) -> dict | None:
        """Return the current vault state as a serializable dict.

        The runner/CLI calls this to persist vault state into the strategy
        state dict under ``VAULT_STATE_KEY``.

        Returns:
            Serialized vault state dict, or None if no state exists.
        """
        if self._vault_state is None:
            return None
        return self._serialize_vault_state(self._vault_state)

    def _load_vault_state(self) -> VaultState:
        """Load vault state from the initial state dict provided at construction.

        Returns:
            VaultState loaded from initial state, or default VaultState if not available.
        """
        try:
            if self._initial_vault_state is None:
                return VaultState()
            return self._deserialize_vault_state(self._initial_vault_state)
        except (TypeError, ValueError, KeyError) as e:
            logger.warning("Failed to load vault state, using defaults: %s", e)
            return VaultState()

    @staticmethod
    def _serialize_vault_state(vault_state: VaultState) -> dict:
        """Serialize VaultState to a dict for storage."""
        return {
            "last_valuation_time": vault_state.last_valuation_time.isoformat()
            if vault_state.last_valuation_time
            else None,
            "last_total_assets": vault_state.last_total_assets,
            "last_proposed_total_assets": vault_state.last_proposed_total_assets,
            "last_pending_deposits": vault_state.last_pending_deposits,
            "last_settlement_epoch": vault_state.last_settlement_epoch,
            "settlement_phase": vault_state.settlement_phase.value,
            "initialized": vault_state.initialized,
            "settlement_nonce": vault_state.settlement_nonce,
        }

    @staticmethod
    def _deserialize_vault_state(data: dict) -> VaultState:
        """Deserialize VaultState from a stored dict."""
        last_valuation_time = data.get("last_valuation_time")
        if last_valuation_time is not None:
            last_valuation_time = datetime.fromisoformat(last_valuation_time)

        return VaultState(
            last_valuation_time=last_valuation_time,
            last_total_assets=data.get("last_total_assets", 0),
            last_proposed_total_assets=data.get("last_proposed_total_assets", 0),
            last_pending_deposits=data.get("last_pending_deposits", 0),
            last_settlement_epoch=data.get("last_settlement_epoch", 0),
            settlement_phase=SettlementPhase(data.get("settlement_phase", "idle")),
            initialized=data.get("initialized", False),
            settlement_nonce=data.get("settlement_nonce", 0),
        )
