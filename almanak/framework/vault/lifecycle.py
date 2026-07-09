# MULTI-WALLET: Vault lifecycle currently uses a single wallet. When multi-wallet
# support is enabled, settlement must handle per-chain vault wallets.
"""VaultLifecycleManager - manages vault settlement lifecycle and state."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

from almanak.core.models.config import VaultVersion
from almanak.core.models.params import SettleDepositParams, SettleRedeemParams, UpdateTotalAssetsParams
from almanak.framework.data.tokens import get_token_resolver
from almanak.framework.vault.capability import default_vault_protocol
from almanak.framework.vault.config import SettlementPhase, SettlementResult, VaultAction, VaultConfig, VaultState

logger = logging.getLogger(__name__)

# Key used to store vault state within the strategy state dict
VAULT_STATE_KEY = "vault_state"


class VaultSDKHandle(Protocol):
    """Runtime SDK surface the vault lifecycle manager consumes."""

    def verify_version(self, vault_address: str, expected_version: VaultVersion) -> None: ...

    def get_valuation_manager(self, vault_address: str) -> str: ...

    def get_curator(self, vault_address: str) -> str: ...

    def get_total_assets(self, vault_address: str) -> int: ...

    def get_proposed_total_assets(self, vault_address: str) -> int: ...

    def get_pending_deposits(self, vault_address: str) -> int: ...

    def has_live_proposal(self, vault_address: str, expected: int | None = None) -> bool: ...

    def get_silo_address(self, vault_address: str) -> str: ...

    def get_underlying_balance(self, vault_address: str, wallet_address: str) -> int: ...


class VaultAdapterHandle(Protocol):
    """Runtime adapter surface the vault lifecycle manager consumes."""

    def build_propose_valuation_bundle(self, params: UpdateTotalAssetsParams) -> Any: ...

    def build_settle_deposit_bundle(self, params: SettleDepositParams) -> Any: ...

    def build_settle_redeem_bundle(self, params: SettleRedeemParams) -> Any: ...


class VaultReceiptParserHandle(Protocol):
    """Receipt parser surface used for settlement accounting hints."""

    def parse_receipt(self, receipt: dict[str, Any]) -> Any: ...


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
        vault_sdk: VaultSDKHandle,
        vault_adapter: VaultAdapterHandle,
        execution_orchestrator: Any,
        deployment_id: str = "",
        initial_vault_state: dict | None = None,
        persistence_callback: Callable[[dict], None] | None = None,
        receipt_parser_protocol: str | None = None,
        receipt_parser: VaultReceiptParserHandle | None = None,
        execution_mode: str = "live",
    ) -> None:
        self._config = vault_config
        self._vault_sdk = vault_sdk
        self._vault_adapter = vault_adapter
        self._execution_orchestrator = execution_orchestrator
        self._deployment_id = deployment_id
        # Execution-mode label ("live" / "paper" / "dry_run"). Governs the
        # share-backed AUM guard's failure semantics (VIB-5672): live REFUSES a
        # mis-priced propose; paper / dry_run log ERROR and continue (mirroring the
        # accounting layer's mode-aware writes). Defaults to the strictest mode so an
        # unset / unknown value fails safe.
        self._execution_mode = (execution_mode or "live").strip().lower()
        self._initial_vault_state = initial_vault_state
        self._vault_state: VaultState | None = None
        self._persistence_callback = persistence_callback
        self._preflight_done = False
        self._preflight_interval = 10  # Re-run every N settlements
        self._settlements_since_preflight = 0
        if receipt_parser_protocol is not None:
            self._receipt_parser_protocol = receipt_parser_protocol
        elif receipt_parser is not None:
            self._receipt_parser_protocol = "<injected>"
        else:
            self._receipt_parser_protocol = default_vault_protocol()
        self._receipt_parser = receipt_parser

    def pre_decide_hook(self, strategy: Any) -> VaultAction:
        """Check if settlement is needed before the strategy's decide() call.

        Args:
            strategy: The IntentStrategy instance. Consulted for the optional
                ``vault_settlement_allowed()`` interleave gate (VIB-5664).

        Returns:
            VaultAction indicating what the lifecycle manager should do.
        """
        vault_state = self.get_vault_state()

        # Check for interrupted settlements first. An in-flight settlement must
        # ALWAYS resume to completion — never gated — or a partially-proposed
        # NAV would be stranded on-chain.
        if vault_state.settlement_phase != SettlementPhase.IDLE:
            logger.info(
                "Vault settlement interrupted in phase %s, resuming",
                vault_state.settlement_phase.value,
            )
            return VaultAction.RESUME_SETTLE

        # Settlement/rebalance interleave guard (VIB-5664). Before STARTING a
        # fresh settlement, honour the strategy's optional
        # ``vault_settlement_allowed()`` gate: a strategy that is mid-rebalance
        # (position closed but not yet reopened) must not have a NAV snapshotted
        # from its transient, cash-only state — that would understate NAV and
        # crater the vault share price. The gate only defers a fresh start; it
        # never blocks resuming an in-flight settlement (handled above).
        allowed = getattr(strategy, "vault_settlement_allowed", None)
        if callable(allowed):
            try:
                if not allowed():
                    logger.info(
                        "Strategy deferred settlement (vault_settlement_allowed()=False); holding until a stable phase"
                    )
                    return VaultAction.HOLD
            except Exception:
                # A raising gate is a strategy bug, not a licence to snapshot a
                # possibly-transient NAV. Fail safe: defer this cycle.
                logger.warning(
                    "vault_settlement_allowed() raised; deferring settlement this cycle",
                    exc_info=True,
                )
                return VaultAction.HOLD

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
            # Only treat the deposit leg as landed if value matches AND this is from the
            # current nonce (not a prior epoch).
            if (
                on_chain_total_assets == total_assets_raw
                and (total_assets_raw > 0 or not vault_state.initialized)
                and vault_state.settlement_nonce > 0
            ):
                logger.info(
                    "settleDeposit already confirmed on-chain (total_assets=%d, nonce=%d), evaluating redeem gate",
                    on_chain_total_assets,
                    vault_state.settlement_nonce,
                )
                # The deposit landed; the proposal that fed it is now spent. Do NOT retry
                # settleDeposit -- evaluate whether redeem shares remain instead.
                return await self._resume_after_deposit_settled(strategy, vault_state, total_assets_raw)
            # Deposit not confirmed. Route through the spent-proposal-aware PROPOSED
            # resume so we never retry settleDeposit against a consumed proposal.
            return await self._resume_from_proposed(strategy, vault_state, total_assets_raw)

        # --- Resume from PROPOSED: settle, but guard against a spent proposal ---
        if phase == SettlementPhase.PROPOSED:
            logger.info("Resuming from PROPOSED phase")
            total_assets_raw = vault_state.last_proposed_total_assets
            return await self._resume_from_proposed(strategy, vault_state, total_assets_raw)

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

        # --- Resume redeem leg (Lagoon v0.5.0 second-proposal path) ---
        if phase in (
            SettlementPhase.PROPOSING_REDEEM,
            SettlementPhase.PROPOSED_REDEEM,
            SettlementPhase.SETTLING_REDEEM,
        ):
            return await self._resume_redeem_leg(strategy, vault_state, phase)

        # --- Start fresh from IDLE ---
        computed_assets = self._compute_total_assets(strategy, vault_state)
        if computed_assets is None:
            return SettlementResult(success=False)
        total_assets_raw = computed_assets

        # Validate valuation bounds
        if not self._validate_bounds(vault_state, total_assets_raw):
            return SettlementResult(success=False)

        # Share-backed AUM invariant (VIB-5672, vault ship-gate #1). Refuse to
        # propose a NAV that materially exceeds the share-backed base = on-chain
        # totalAssets + pending deposit assets. This runs on every FRESH settlement
        # (IDLE start), including the very first one (boot-time coverage), and
        # BEFORE updateNewTotalAssets is submitted -- the whole point is to never
        # propose a mis-priced NAV. Resume paths re-propose an already-validated
        # value and are intentionally not re-checked, mirroring _validate_bounds.
        if not self._validate_share_backed_aum(vault_state, total_assets_raw):
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

    def _is_live_guard_mode(self) -> bool:
        """Return True when the share-backed AUM guard must fail closed (refuse).

        Live mode refuses a mis-priced propose (real funds at stake). Paper /
        dry_run modes log the violation and continue, mirroring the accounting
        layer's mode-aware writes (blueprint 27). Any unrecognised label fails
        safe as live.
        """
        return self._execution_mode not in ("paper", "dry_run")

    def _share_backed_base(self) -> int | None:
        """Read the share-backed base = on-chain totalAssets + pending deposits.

        Both legs are the ONLY capital the vault has minted (or is about to mint)
        shares against: ``totalAssets`` is settled AUM, pending deposits are
        depositor capital in-flight through requestDeposit that will settle into
        shares. Read-only: does NOT mutate vault_state (version-aware pending-deposit
        accounting stays owned by ``_compute_total_assets``). Returns None if either
        on-chain read fails -- Empty != Zero: an unreadable value is an error, never
        a silent 0.
        """
        try:
            total_assets = self._vault_sdk.get_total_assets(self._config.vault_address)
            pending_deposits = self._vault_sdk.get_pending_deposits(self._config.vault_address)
        except Exception:
            logger.error(
                "Share-backed AUM guard: could not read on-chain totalAssets / pending "
                "deposits for vault %s; cannot validate the invariant",
                self._config.vault_address,
                exc_info=True,
            )
            return None
        # Defensive Empty != Zero: a None from a stubbed / degraded SDK is unmeasured,
        # not a measured 0. Treat it as an unreadable value rather than base += 0.
        if total_assets is None or pending_deposits is None:
            logger.error(
                "Share-backed AUM guard: on-chain totalAssets (%r) or pending deposits "
                "(%r) unreadable for vault %s; cannot validate the invariant",
                total_assets,
                pending_deposits,
                self._config.vault_address,
            )
            return None
        return total_assets + pending_deposits

    def _validate_share_backed_aum(self, vault_state: VaultState, proposed_total_assets: int) -> bool:
        """Refuse to propose a NAV that materially exceeds share-backed AUM (VIB-5672).

        The vault share price = ``totalAssets / totalSupply``; shares are only minted
        for capital that flowed through ``requestDeposit`` -> settle. If the Safe also
        holds non-depositor capital, the default ``valuate()``-of-the-whole-Safe inflates
        the proposed NAV, letting depositors' shares be priced against -- and redeemed
        for -- capital they do not own, and minting fee-shares against phantom AUM
        (irreversible). Operating rule (Option A, ratified): the vault Safe holds ONLY
        share-backed AUM; the manager gets skin-in-the-game by depositing like anyone
        else. See ``docs/internal/blueprints/24-vault-integration.md`` §Share-Backed AUM.

        The invariant, in raw asset units::

            proposed <= (totalAssets + pending_deposits) * (1 + tol_bps/1e4) + abs_floor

        A material excess implies commingled non-depositor capital that never flowed
        through the deposit lane. Failure semantics are mode-aware: live REFUSES (returns
        False, settlement does not proceed, state machine stays resumable at IDLE);
        paper / dry_run log ERROR and continue (return True). An unreadable on-chain base
        is likewise a refusal in live mode.

        Returns:
            True to proceed with the propose, False to refuse it (live mode).
        """
        base = self._share_backed_base()
        if base is None:
            # Unreadable base -> cannot validate. Fail safe (refuse in live).
            return self._resolve_guard_failure(
                vault_state,
                "share-backed base unreadable (on-chain totalAssets / pending deposits)",
            )

        tol_bps = self._config.nav_share_backed_tolerance_bps
        abs_floor = self._config.nav_share_backed_abs_floor
        allowed_max = base + (base * tol_bps) // 10000 + abs_floor

        if proposed_total_assets <= allowed_max:
            logger.debug(
                "Share-backed AUM guard passed: proposed=%d <= allowed=%d (base=%d, tol=%d bps, floor=%d)",
                proposed_total_assets,
                allowed_max,
                base,
                tol_bps,
                abs_floor,
            )
            return True

        excess = proposed_total_assets - base
        reason = (
            f"proposed NAV {proposed_total_assets} exceeds share-backed base {base} "
            f"(totalAssets + pending deposits) by {excess} > tolerance "
            f"({tol_bps} bps + floor {abs_floor}); allowed_max={allowed_max}. "
            f"Non-depositor capital appears commingled in the vault Safe -- the Safe "
            f"must hold ONLY share-backed AUM (VIB-5672)"
        )
        return self._resolve_guard_failure(vault_state, reason)

    def _resolve_guard_failure(self, vault_state: VaultState, reason: str) -> bool:
        """Apply mode-aware failure semantics for a share-backed AUM guard violation.

        Live mode: log ERROR, reset to IDLE (state machine stays safe / resumable),
        refuse the propose (return False). Paper / dry_run: log ERROR and continue
        (return True) so a mis-priced propose is surfaced loudly but does not halt a
        simulation where no real funds move.
        """
        if self._is_live_guard_mode():
            logger.error("Share-backed AUM invariant violated (refusing settlement): %s", reason)
            vault_state.settlement_phase = SettlementPhase.IDLE
            self.save_vault_state()
            return False
        logger.error(
            "Share-backed AUM invariant violated (%s mode: continuing): %s",
            self._execution_mode,
            reason,
        )
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

        deposits_received, shares_minted = self._parse_settle_deposit_receipt(settle_deposit_result)

        # Deposits are now committed on-chain and the proposal that fed settleDeposit is
        # spent (Lagoon v0.5.0: updateNewTotalAssets is single-use). settleDeposit already
        # honoured any redeem shares the safe could cover in the same call. If redeem shares
        # still remain in the pending silo AND auto-settle is enabled, the redeem leg needs
        # its OWN fresh proposal before settleRedeem -- reusing the spent one reverts
        # NewTotalAssetsMissing(). Otherwise (first settlement, deposits-only, or disabled)
        # we finalize directly -- the old unconditional settleRedeem was the deadlock.
        if not self._config.auto_settle_redeems or not self._has_pending_redeem_shares():
            vault_state.settlement_phase = SettlementPhase.SETTLED
            self.save_vault_state()
            return self._finalize_settlement(
                vault_state,
                deposits_received=deposits_received,
                shares_minted=shares_minted,
            )

        return await self._execute_redeem_settle(
            strategy,
            vault_state,
            total_assets_raw,
            deposits_received=deposits_received,
            shares_minted=shares_minted,
        )

    def _has_pending_redeem_shares(self) -> bool:
        """Return True if redeem shares are waiting in the pending silo.

        Lagoon v0.5.0 parks requested-redeem shares in the pending silo until a
        ``settleRedeem`` covers them. The correct "are there redeem shares to settle"
        signal is the vault-share balance of the silo (``vault.balanceOf(silo)``), NOT
        ``pendingRedeemRequest(0, vault)`` -- the latter passes the vault as controller
        and always returns 0.
        """
        silo = self._vault_sdk.get_silo_address(self._config.vault_address)
        # NOTE: get_underlying_balance returns the raw vault-SHARE balance of the silo
        # (vault.balanceOf(silo)), not an underlying-asset amount. It is only used here as
        # a boolean "shares parked?" gate -- never as a money quantity. (Rename tracked as
        # a follow-up.)
        remaining_shares = self._vault_sdk.get_underlying_balance(self._config.vault_address, silo)
        return remaining_shares > 0

    async def _resume_from_proposed(
        self, strategy: Any, vault_state: VaultState, total_assets_raw: int
    ) -> SettlementResult:
        """Resume the deposit leg from PROPOSED, guarding against a spent proposal.

        A crash between settleDeposit landing and the phase advancing leaves the state
        at PROPOSED while the on-chain proposal is already consumed. Blindly retrying
        settleDeposit against a spent proposal reverts NewTotalAssetsMissing(), which
        used to roll back to PROPOSED and deadlock. This detects the three cases:

        - proposal still live -> settleDeposit as normal;
        - proposal spent AND deposit already landed -> skip to the redeem gate / finalize;
        - proposal spent AND deposit never landed -> re-propose from scratch.
        """
        proposal_live = self._vault_sdk.has_live_proposal(self._config.vault_address, total_assets_raw)
        if proposal_live and (total_assets_raw > 0 or not vault_state.initialized):
            return await self._execute_settle(strategy, vault_state, total_assets_raw)

        on_chain_total = self._vault_sdk.get_total_assets(self._config.vault_address)
        deposit_landed = on_chain_total == total_assets_raw and (total_assets_raw > 0 or not vault_state.initialized)
        if deposit_landed:
            logger.info(
                "PROPOSED resume: proposal spent and settleDeposit already landed "
                "(total_assets=%d); skipping settleDeposit retry",
                on_chain_total,
            )
            return await self._resume_after_deposit_settled(strategy, vault_state, total_assets_raw)

        logger.info("PROPOSED resume: proposal spent but settleDeposit did not land; re-proposing")
        return await self._execute_propose_and_settle(strategy, vault_state, total_assets_raw)

    async def _resume_after_deposit_settled(
        self, strategy: Any, vault_state: VaultState, total_assets_raw: int
    ) -> SettlementResult:
        """Evaluate the redeem gate after the deposit leg is known to have settled.

        Used by resume paths where the receipt is unavailable, so deposit/share hints
        are unknown (0). Mirrors the tail of :meth:`_execute_settle`.
        """
        if not self._config.auto_settle_redeems or not self._has_pending_redeem_shares():
            vault_state.settlement_phase = SettlementPhase.SETTLED
            self.save_vault_state()
            return self._finalize_settlement(vault_state)
        return await self._execute_redeem_settle(strategy, vault_state, total_assets_raw)

    async def _resume_redeem_leg(
        self, strategy: Any, vault_state: VaultState, phase: SettlementPhase
    ) -> SettlementResult:
        """Resume the redeem leg (proposal #2 -> settleRedeem) after a crash.

        Handles PROPOSING_REDEEM / PROPOSED_REDEEM / SETTLING_REDEEM. Deposits are
        already committed on-chain at this point, so every path here either settles the
        remaining redeem shares or finalizes and carries them to the next cycle -- it
        never touches the deposit leg.
        """
        total_assets_raw = vault_state.last_proposed_total_assets

        if phase == SettlementPhase.PROPOSING_REDEEM:
            logger.info(
                "Resuming from PROPOSING_REDEEM phase, checking proposal #2 (nonce=%d)",
                vault_state.settlement_nonce,
            )
            proposal_live = self._vault_sdk.has_live_proposal(self._config.vault_address, total_assets_raw)
            if (
                proposal_live
                and (total_assets_raw > 0 or not vault_state.initialized)
                and vault_state.settlement_nonce > 0
            ):
                logger.info("Redeem proposal #2 confirmed on-chain, proceeding to settleRedeem")
                vault_state.settlement_phase = SettlementPhase.PROPOSED_REDEEM
                self.save_vault_state()
                return await self._execute_settle_redeem_tx(strategy, vault_state, total_assets_raw)
            # Proposal #2 not (yet) live -- re-issue it.
            return await self._execute_redeem_settle(strategy, vault_state, total_assets_raw)

        if phase == SettlementPhase.PROPOSED_REDEEM:
            logger.info("Resuming from PROPOSED_REDEEM phase, proceeding to settleRedeem")
            return await self._execute_settle_redeem_tx(strategy, vault_state, total_assets_raw)

        # SETTLING_REDEEM
        logger.info("Resuming from SETTLING_REDEEM phase, checking on-chain redeem state")
        if not self._has_pending_redeem_shares():
            # Silo drained -> settleRedeem landed. Finalize.
            logger.info("settleRedeem confirmed on-chain (silo drained), finalizing")
            vault_state.settlement_phase = SettlementPhase.SETTLED
            self.save_vault_state()
            return self._finalize_settlement(vault_state)
        # Shares still in the silo. If proposal #2 is still live, the settleRedeem tx
        # reverted -> retry it. If it was consumed, settleRedeem ran but the safe could
        # not honour every share (illiquidity); one attempt per cycle -> finalize and
        # carry the remaining shares to the next cycle.
        proposal_live = self._vault_sdk.has_live_proposal(self._config.vault_address, total_assets_raw)
        if proposal_live and (total_assets_raw > 0 or not vault_state.initialized):
            return await self._execute_settle_redeem_tx(strategy, vault_state, total_assets_raw)
        logger.info("settleRedeem consumed proposal but shares remain (safe illiquidity); carrying over")
        vault_state.settlement_phase = SettlementPhase.SETTLED
        self.save_vault_state()
        return self._finalize_settlement(vault_state)

    async def _execute_redeem_settle(
        self,
        strategy: Any,
        vault_state: VaultState,
        total_assets_raw: int,
        deposits_received: int = 0,
        shares_minted: int = 0,
    ) -> SettlementResult:
        """Settle remaining redeem shares with a FRESH proposal (Lagoon v0.5.0).

        settleDeposit consumed the first proposal, so settleRedeem needs a brand-new
        ``updateNewTotalAssets`` (proposal #2). Exactly one redeem attempt per cycle:
        any shares the safe still cannot cover carry over to the next settlement.
        """
        vault_state.settlement_phase = SettlementPhase.PROPOSING_REDEEM
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
            return self._handle_redeem_leg_failure(
                vault_state,
                SettlementPhase.PROPOSING_REDEEM,
                "propose #2",
                propose_result.error,
                deposits_received,
                shares_minted,
            )

        vault_state.settlement_phase = SettlementPhase.PROPOSED_REDEEM
        self.save_vault_state()

        return await self._execute_settle_redeem_tx(
            strategy,
            vault_state,
            total_assets_raw,
            deposits_received=deposits_received,
            shares_minted=shares_minted,
        )

    async def _execute_settle_redeem_tx(
        self,
        strategy: Any,
        vault_state: VaultState,
        total_assets_raw: int,
        deposits_received: int = 0,
        shares_minted: int = 0,
    ) -> SettlementResult:
        """Execute settleRedeem against the fresh proposal, then finalize the cycle."""
        vault_state.settlement_phase = SettlementPhase.SETTLING_REDEEM
        self.save_vault_state()

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
            # settleRedeem reverted -> proposal #2 is NOT consumed and is still live, so
            # retry the settleRedeem tx (not the propose) on the next resume.
            return self._handle_redeem_leg_failure(
                vault_state,
                SettlementPhase.PROPOSED_REDEEM,
                "settleRedeem",
                settle_redeem_result.error,
                deposits_received,
                shares_minted,
            )

        vault_state.settlement_phase = SettlementPhase.SETTLED
        self.save_vault_state()
        return self._finalize_settlement(
            vault_state,
            deposits_received=deposits_received,
            shares_minted=shares_minted,
        )

    def _handle_redeem_leg_failure(
        self,
        vault_state: VaultState,
        retry_phase: SettlementPhase,
        leg_name: str,
        error: Any,
        deposits_received: int,
        shares_minted: int,
    ) -> SettlementResult:
        """Handle a failure in the redeem leg.

        Deposits are already committed on-chain and the first proposal is spent, so we
        must NEVER roll back to PROPOSED (retrying settleDeposit would revert forever).
        ``redeem_failure_fatal`` therefore no longer aborts/undoes the whole cycle -- it
        only governs whether an unrecoverable redeem-leg error surfaces as
        ``SettlementResult(success=False)``:

        - fatal (default): park at ``retry_phase`` (a redeem-leg-retry phase) and return
          failure; the next resume retries only the redeem leg;
        - non-fatal: finalize the cycle (deposits stand) and carry remaining redeem
          shares to the next settlement.
        """
        if self._config.redeem_failure_fatal:
            logger.error("Vault redeem leg %s failed (fatal): %s", leg_name, error)
            vault_state.settlement_phase = retry_phase
            self.save_vault_state()
            return SettlementResult(success=False)

        logger.warning(
            "Vault redeem leg %s failed (non-fatal): %s; finalizing deposits, carrying redeem over",
            leg_name,
            error,
        )
        vault_state.settlement_phase = SettlementPhase.SETTLED
        self.save_vault_state()
        return self._finalize_settlement(
            vault_state,
            deposits_received=deposits_received,
            shares_minted=shares_minted,
        )

    def _parse_settle_deposit_receipt(self, settle_deposit_result: Any) -> tuple[int, int]:
        """Parse settle-deposit accounting hints from the connector receipt parser."""
        receipt = getattr(settle_deposit_result, "receipt", None)
        if not receipt:
            return 0, 0

        parser = self._receipt_parser
        if parser is None:
            from almanak.framework.execution.receipt_registry import ReceiptParserRegistry

            try:
                parser = ReceiptParserRegistry().get(self._receipt_parser_protocol)
            except Exception as exc:
                logger.warning(
                    "Could not resolve receipt parser %r for settle_deposit accounting: %s",
                    self._receipt_parser_protocol,
                    exc,
                    exc_info=True,
                )
                return 0, 0

        if parser is None:
            logger.warning("No receipt parser found for protocol %r", self._receipt_parser_protocol)
            return 0, 0

        try:
            parsed = parser.parse_receipt(receipt)
            settle_deposits = parsed.settle_deposits
        except Exception as exc:
            logger.warning(
                "Could not parse settle_deposit receipt for accounting with parser %r: %s",
                self._receipt_parser_protocol,
                exc,
                exc_info=True,
            )
            return 0, 0

        if not settle_deposits:
            return 0, 0

        try:
            settle_deposit = settle_deposits[0]
            return settle_deposit.assets_deposited, settle_deposit.shares_minted
        except (AttributeError, IndexError, TypeError) as exc:
            logger.warning(
                "Could not extract settle_deposit accounting fields with parser %r: %s",
                self._receipt_parser_protocol,
                exc,
                exc_info=True,
            )
            return 0, 0

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
