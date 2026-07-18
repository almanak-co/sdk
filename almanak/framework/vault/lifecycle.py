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
from almanak.core.models.params import (
    CloseVaultParams,
    InitiateClosingParams,
    RedeemVaultParams,
    SettleDepositParams,
    SettleRedeemParams,
    UpdateTotalAssetsParams,
)
from almanak.framework.data.tokens import get_token_resolver
from almanak.framework.vault.capability import default_vault_protocol
from almanak.framework.vault.config import (
    ReleaseResult,
    SettlementPhase,
    SettlementResult,
    VaultAction,
    VaultConfig,
    VaultReleasePhase,
    VaultState,
)

logger = logging.getLogger(__name__)

# Key used to store vault state within the strategy state dict
VAULT_STATE_KEY = "vault_state"

# Lagoon v0.5.0 ``State`` enum labels as returned by the vault SDK handle's
# ``get_vault_state`` (see :class:`VaultSDKHandle`). Kept as local constants (not
# imported from the connector) so the framework lifecycle stays connector-agnostic;
# the connector owns the ordinal->label map.
_VAULT_STATE_OPEN = "Open"
_VAULT_STATE_CLOSING = "Closing"
_VAULT_STATE_CLOSED = "Closed"

# VIB-5667 vault-release NAV safety (§f4). Post-unwind the Safe holds all cash;
# the proposed final NAV must be BACKED by the Safe's underlying balance so
# ``close()``'s ``transferFrom(safe, vault, totalAssets)`` cannot revert. A small
# realized-vs-settled shortfall (rounding / fee-share drag / benign LP slippage)
# is clamped down and the close proceeds. A shortfall LARGER than this tolerance
# is a genuine loss vs depositor obligations: we do NOT force a haircut close —
# release degrades loudly and a human decides (top up the Safe, or accept the
# haircut deliberately). Expressed in basis points of the settled obligations.
_RELEASE_HAIRCUT_TOLERANCE_BPS = 500  # 5%

# ``approve(spender, amount)`` amount used when authorising the vault to pull the
# Safe's underlying at close. MAX is idempotent + cheap to re-issue on resume.
_MAX_UINT256 = (1 << 256) - 1


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

    # --- VIB-5667 vault-release reads ---
    def get_vault_state(self, vault_address: str) -> str: ...

    def get_new_total_assets(self, vault_address: str) -> int: ...

    def get_owner(self, vault_address: str) -> str: ...

    def get_roles_storage(self, vault_address: str) -> dict: ...

    def get_underlying_token_address(self, vault_address: str) -> str: ...

    def convert_to_assets(self, vault_address: str, shares: int) -> int: ...

    def build_approve_deposit_tx(
        self, underlying_token: str, vault_address: str, depositor: str, amount: int
    ) -> dict: ...


class VaultAdapterHandle(Protocol):
    """Runtime adapter surface the vault lifecycle manager consumes."""

    def build_propose_valuation_bundle(self, params: UpdateTotalAssetsParams) -> Any: ...

    def build_settle_deposit_bundle(self, params: SettleDepositParams) -> Any: ...

    def build_settle_redeem_bundle(self, params: SettleRedeemParams) -> Any: ...

    # --- VIB-5667 vault-release bundles ---
    def build_initiate_closing_bundle(self, params: InitiateClosingParams) -> Any: ...

    def build_close_bundle(self, params: CloseVaultParams) -> Any: ...

    def build_redeem_bundle(self, params: RedeemVaultParams) -> Any: ...


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
        # VIB-5666 — runner-owned settlement-commit callable. Injected per cycle
        # via ``run_settlement_cycle(strategy, settlement_commit=...)`` so every
        # settleDeposit / settleRedeem / propose tx routes through the
        # commit/accounting pipeline (ledger → outbox+fire → sidecar). ``None``
        # (the default) makes every commit call a no-op — legacy callers, unit
        # tests, and any non-runner driver keep working with zero accounting
        # side-effects, and a non-vault deployment never constructs this manager.
        self._settlement_commit: Any = None
        # Underlying USD price captured at ``_compute_total_assets`` time (fresh
        # settlements only) so the commit can price ``assets_usd``. ``None`` on
        # resume paths → assets_usd stays unmeasured (Empty ≠ Zero).
        self._last_underlying_price: Decimal | None = None
        # Cached (underlying_decimals, share_decimals) for raw→human scaling.
        self._settlement_decimals: tuple[int | None, int | None] | None = None
        # Set True when any per-tx commit reports accounting_degraded; stamped
        # onto the SettlementResult at finalize and reset at cycle start.
        self._settlement_accounting_degraded = False
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

    async def run_settlement_cycle(self, strategy: Any, settlement_commit: Any = None) -> SettlementResult:
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

        Args (cont.):
            settlement_commit: Optional runner-owned async callable that routes
                each settlement tx through the commit/accounting pipeline
                (VIB-5666). ``None`` (default) → settlement executes exactly as
                before with no accounting side-effects (legacy / test / non-runner
                callers). When supplied it is stored for the duration of the cycle
                and invoked after every successful ``orchestrator.execute``.

        Returns:
            SettlementResult describing the outcome of the settlement.
        """
        # VIB-5666 — bind the commit callable for this cycle. Persist across the
        # call (resume paths re-enter without re-supplying it) but let a fresh
        # explicit value win.
        if settlement_commit is not None:
            self._settlement_commit = settlement_commit
        self._settlement_accounting_degraded = False

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

        # VIB-5666 — stash the settlement-time underlying USD price so the
        # settlement commit can value ``assets_usd`` for the deposit/redeem legs.
        self._last_underlying_price = underlying_price

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

        # VIB-5666 — the propose (updateNewTotalAssets) tx moves no capital; book a
        # ledger row (gas / tx visibility so the books tie) with no typed event.
        await self._emit_settlement_commit(strategy, vault_state, leg="propose", result=propose_result)

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

        deposits_measured, shares_measured = self._parse_settle_deposit_receipt(settle_deposit_result)

        # VIB-5666 — settleDeposit confirmed: assets flowed IN, shares minted.
        # Route through the commit/accounting pipeline (SETTLE_DEPOSIT event).
        # None = unmeasured (Empty ≠ Zero) — the commit lands an unmeasured delta.
        await self._emit_settlement_commit(
            strategy,
            vault_state,
            leg="deposit",
            result=settle_deposit_result,
            assets_raw=deposits_measured,
            shares_raw=shares_measured,
            new_total_assets_raw=total_assets_raw,
        )
        # SettlementResult display fields are plain ints: unmeasured → 0 here only.
        deposits_received = deposits_measured or 0
        shares_minted = shares_measured or 0

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

    def _mark_resume_leg_unverifiable(self, leg: str) -> None:
        """Surface accounting-degraded for a leg that landed in the pre-crash run.

        On crash-resume the tx receipt is gone, so whether the pre-crash run
        emitted its ``SETTLE_*`` commit before dying is unverifiable — and
        re-emitting here could double-book the event. Conservatively mark the
        cycle ``accounting_degraded`` (loud-but-never-blocking) so an operator
        reconciles the ledger row for this epoch; a false alarm when the
        pre-crash run did commit is acceptable, a silently missing row is not.
        """
        logger.error(
            "Settlement resume: %s leg landed pre-crash; its accounting commit is "
            "unverifiable — marking cycle accounting_degraded for reconciliation",
            leg,
        )
        self._settlement_accounting_degraded = True

    async def _resume_after_deposit_settled(
        self, strategy: Any, vault_state: VaultState, total_assets_raw: int
    ) -> SettlementResult:
        """Evaluate the redeem gate after the deposit leg is known to have settled.

        Used by resume paths where the receipt is unavailable, so deposit/share hints
        are unknown (0). Mirrors the tail of :meth:`_execute_settle`.
        """
        self._mark_resume_leg_unverifiable("settleDeposit")
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
            self._mark_resume_leg_unverifiable("settleRedeem")
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
        self._mark_resume_leg_unverifiable("settleRedeem")
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

        # VIB-5666 — proposal #2 (redeem valuation) moves no capital; book a
        # ledger row for gas / tx visibility with no typed event.
        await self._emit_settlement_commit(strategy, vault_state, leg="propose", result=propose_result)

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

        # VIB-5666 — settleRedeem confirmed: assets flowed OUT, shares burned.
        # Route through the commit/accounting pipeline (SETTLE_REDEEM event).
        # None = unmeasured (Empty ≠ Zero) — the commit lands an unmeasured delta.
        redemptions_measured, burned_measured = self._parse_settle_redeem_receipt(settle_redeem_result)
        await self._emit_settlement_commit(
            strategy,
            vault_state,
            leg="redeem",
            result=settle_redeem_result,
            assets_raw=redemptions_measured,
            shares_raw=burned_measured,
            new_total_assets_raw=total_assets_raw,
        )
        # SettlementResult display fields are plain ints: unmeasured → 0 here only.
        redemptions_processed = redemptions_measured or 0
        shares_burned = burned_measured or 0

        vault_state.settlement_phase = SettlementPhase.SETTLED
        self.save_vault_state()
        return self._finalize_settlement(
            vault_state,
            deposits_received=deposits_received,
            shares_minted=shares_minted,
            redemptions_processed=redemptions_processed,
            shares_burned=shares_burned,
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

    def _settlement_leg_receipts(self, result: Any) -> list[dict[str, Any]]:
        """Parseable receipt dicts for a settlement leg, in priority order.

        ``ExecutionResult`` has NO top-level ``.receipt`` — per-tx receipts live
        in ``transaction_results[i].receipt`` (VIB-5666 real-fork finding: the
        old top-level read meant the connector parser never ran and every
        settlement leg booked zero deltas). A top-level ``.receipt`` is still
        honoured first for injected doubles. ``TransactionReceipt`` dataclasses
        are normalized via ``to_dict()`` — the connector parser consumes
        log-bearing dicts.
        """
        candidates: list[Any] = []
        top = getattr(result, "receipt", None)
        if top:
            candidates.append(top)
        for tx in getattr(result, "transaction_results", None) or []:
            tx_receipt = getattr(tx, "receipt", None)
            if tx_receipt:
                candidates.append(tx_receipt)
        receipts: list[dict[str, Any]] = []
        for candidate in candidates:
            if isinstance(candidate, dict):
                receipts.append(candidate)
            elif hasattr(candidate, "to_dict"):
                try:
                    receipts.append(candidate.to_dict())
                except Exception:  # noqa: BLE001 — accounting hint, never block settlement
                    logger.warning("Settlement receipt could not be serialized for parsing", exc_info=True)
        return receipts

    def _resolve_settlement_receipt_parser(self, leg_name: str) -> Any | None:
        """Resolve the connector receipt parser for settlement accounting hints."""
        parser = self._receipt_parser
        if parser is None:
            from almanak.framework.execution.receipt_registry import ReceiptParserRegistry

            try:
                parser = ReceiptParserRegistry().get(self._receipt_parser_protocol)
            except Exception as exc:
                logger.warning(
                    "Could not resolve receipt parser %r for %s accounting: %s",
                    self._receipt_parser_protocol,
                    leg_name,
                    exc,
                    exc_info=True,
                )
                return None
        if parser is None:
            logger.warning("No receipt parser found for protocol %r", self._receipt_parser_protocol)
        return parser

    def _parse_settle_deposit_receipt(self, settle_deposit_result: Any) -> tuple[int | None, int | None]:
        """Parse settle-deposit accounting hints from the connector receipt parser.

        Returns ``(assets_deposited, shares_minted)`` raw ints, or ``(None, None)``
        when the leg is unmeasurable (no receipt / parser / event) — Empty ≠ Zero:
        an unparseable leg lands as an unmeasured accounting delta, never a
        fabricated measured zero.
        """
        receipts = self._settlement_leg_receipts(settle_deposit_result)
        if not receipts:
            return None, None

        parser = self._resolve_settlement_receipt_parser("settle_deposit")
        if parser is None:
            return None, None

        settle_deposits: list[Any] = []
        for receipt in receipts:
            try:
                parsed = parser.parse_receipt(receipt)
            except Exception as exc:
                logger.warning(
                    "Could not parse settle_deposit receipt for accounting with parser %r: %s",
                    self._receipt_parser_protocol,
                    exc,
                    exc_info=True,
                )
                continue
            if parsed.settle_deposits:
                settle_deposits = parsed.settle_deposits
                break

        if not settle_deposits:
            logger.warning(
                "No SettleDeposit event found in %d receipt(s); deposit leg accounting is unmeasured",
                len(receipts),
            )
            return None, None

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
            return None, None

    def _parse_settle_redeem_receipt(self, settle_redeem_result: Any) -> tuple[int | None, int | None]:
        """Parse settle-redeem accounting hints (assets_withdrawn, shares_burned).

        Twin of :meth:`_parse_settle_deposit_receipt` for the redeem leg. Returns
        ``(None, None)`` when the leg is unmeasurable — Empty ≠ Zero: the commit
        must land an unmeasured delta, never a fabricated measured zero.
        """
        receipts = self._settlement_leg_receipts(settle_redeem_result)
        if not receipts:
            return None, None

        parser = self._resolve_settlement_receipt_parser("settle_redeem")
        if parser is None:
            return None, None

        settle_redeems: list[Any] = []
        for receipt in receipts:
            try:
                parsed = parser.parse_receipt(receipt)
            except Exception as exc:
                logger.warning(
                    "Could not parse settle_redeem receipt for accounting with parser %r: %s",
                    self._receipt_parser_protocol,
                    exc,
                    exc_info=True,
                )
                continue
            if parsed.settle_redeems:
                settle_redeems = parsed.settle_redeems
                break

        if not settle_redeems:
            logger.warning(
                "No SettleRedeem event found in %d receipt(s); redeem leg accounting is unmeasured",
                len(receipts),
            )
            return None, None

        try:
            settle_redeem = settle_redeems[0]
            return settle_redeem.assets_withdrawn, settle_redeem.shares_burned
        except (AttributeError, IndexError, TypeError) as exc:
            logger.warning(
                "Could not extract settle_redeem accounting fields with parser %r: %s",
                self._receipt_parser_protocol,
                exc,
                exc_info=True,
            )
            return None, None

    def _resolve_settlement_decimals(self, strategy: Any) -> tuple[int | None, int | None]:
        """Resolve (underlying_decimals, share_decimals), cached per manager.

        The vault's own address IS its ERC-20 share token, so its ``decimals()``
        gives the share scale. Both reads go through the gateway-backed token
        resolver. On failure a leg's decimals stays ``None`` → the commit scales
        that amount to ``None`` (unmeasured), never a fabricated human value.
        """
        if self._settlement_decimals is not None:
            return self._settlement_decimals
        chain = getattr(strategy, "chain", "") or ""
        resolver = get_token_resolver()
        underlying_decimals: int | None = None
        share_decimals: int | None = None
        try:
            underlying_decimals = resolver.get_decimals(chain, self._config.underlying_token)
        except Exception:
            logger.warning(
                "Settlement accounting: could not resolve underlying decimals for %s on %s",
                self._config.underlying_token,
                chain,
                exc_info=True,
            )
        try:
            share_decimals = resolver.get_decimals(chain, self._config.vault_address)
        except Exception:
            logger.warning(
                "Settlement accounting: could not resolve vault share decimals for %s on %s",
                self._config.vault_address,
                chain,
                exc_info=True,
            )
        # Cache only a fully-resolved pair. The manager is long-lived: caching a
        # partial/failed resolution (transient RPC error on the first cycle)
        # would pin every later settlement event to unmeasured deltas. Unresolved
        # legs stay None for THIS cycle (Empty ≠ Zero) and retry next cycle.
        if underlying_decimals is not None and share_decimals is not None:
            self._settlement_decimals = (underlying_decimals, share_decimals)
        return (underlying_decimals, share_decimals)

    async def _emit_settlement_commit(
        self,
        strategy: Any,
        vault_state: VaultState,
        *,
        leg: str,
        result: Any,
        assets_raw: int | None = None,
        shares_raw: int | None = None,
        new_total_assets_raw: int | None = None,
        fee_shares_raw: int | None = None,
    ) -> None:
        """Route one confirmed settlement tx through the runner's commit pipeline.

        No-op when no ``settlement_commit`` callable was injected (legacy / test /
        non-runner callers). LOUD-BUT-NEVER-BLOCK (blueprint 27 §Teardown inverted
        semantics): the share-moving tx has already confirmed on-chain, so an
        accounting-write failure must be surfaced (ERROR + degraded flag) but must
        NEVER raise into the settlement state machine — halting a half-settled
        epoch would strand depositor capital. ``commit_settlement_intent`` already
        swallows its own failures into ``accounting_degraded``; this wrapper is the
        belt-and-suspenders backstop for anything upstream (decimals resolution, a
        misbehaving injected callable).
        """
        if self._settlement_commit is None:
            return
        epoch = vault_state.last_settlement_epoch + 1
        try:
            underlying_decimals, share_decimals = self._resolve_settlement_decimals(strategy)
            outcome = await self._settlement_commit(
                strategy,
                leg=leg,
                execution_result=result,
                settlement_cycle_id=f"settlement-{epoch}",
                vault_address=self._config.vault_address,
                underlying_token=self._config.underlying_token,
                assets_raw=assets_raw,
                shares_raw=shares_raw,
                new_total_assets_raw=new_total_assets_raw,
                fee_shares_raw=fee_shares_raw,
                epoch_id=epoch,
                underlying_decimals=underlying_decimals,
                share_decimals=share_decimals,
                underlying_price=self._last_underlying_price,
            )
            if getattr(outcome, "accounting_degraded", False):
                self._settlement_accounting_degraded = True
                logger.error(
                    "Settlement accounting degraded for %s leg (epoch %d): %s",
                    leg,
                    epoch,
                    getattr(outcome, "degraded_reason", None),
                )
        except Exception:
            # The on-chain settle stands; never block the state machine.
            self._settlement_accounting_degraded = True
            logger.error(
                "Settlement commit raised for %s leg (epoch %d); on-chain tx stands, "
                "continuing settlement state machine",
                leg,
                epoch,
                exc_info=True,
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
            # VIB-5666 — surface whether any per-tx commit degraded this cycle so
            # the runner logs it and an operator can replay the deferred writes.
            accounting_degraded=self._settlement_accounting_degraded,
        )

        logger.info(
            "Settlement cycle complete: epoch=%d, total_assets=%d",
            result.epoch_id,
            result.new_total_assets,
        )
        return result

    # ------------------------------------------------------------------
    # VIB-5667 — vault-safe teardown (release depositor capital)
    # ------------------------------------------------------------------
    async def release_on_teardown(
        self,
        strategy: Any,
        market: Any,
        *,
        commit: Any | None = None,
    ) -> ReleaseResult:
        """Transition the vault Open->Closing->Closed on teardown (VIB-5667).

        Runs AFTER the strategy's position-closure teardown intents have unwound
        the LP to underlying in the Safe. Releasing the vault makes EVERY
        depositor's capital claimable — including a deposit-only user who never
        requested a redemption and otherwise could never withdraw once the manager
        stops settling (the stranding bug this closes).

        The sequence is idempotent + crash-resumable, anchored on the live
        on-chain ``state()``:

        - ``Open``:    propose final NAV -> ``initiateClosing`` -> read NAV back ->
                       ``close`` -> redeem manager's own shares.
        - ``Closing``: read NAV back (re-propose if consumed) -> ``close`` -> redeem.
        - ``Closed``:  redeem manager's own residual shares only.

        ``commit`` is an optional async callback ``commit(*, action_type, bundle,
        execution_result, signer)`` bound by the runner to
        ``commit_teardown_intent`` so every release leg drives the teardown
        accounting pipeline (ledger / outbox / sidecar). Every orchestrator
        execution is funnelled through :meth:`_execute_release_leg`, which pairs it
        with ``commit`` — the anti-bypass invariant (CLAUDE.md §Teardown).

        Never raises: a failure degrades loudly (``ReleaseResult.degraded``) but
        never blocks the caller's next risk-reducing action (teardown's inverted
        failure semantics).
        """
        if not self._config.release_on_teardown:
            logger.info("Vault release_on_teardown disabled by config — skipping (vault stays Open)")
            return ReleaseResult(skipped=True, reason="release_on_teardown disabled")

        vault_address = self._config.vault_address

        # VIB-5667 (audit #7): the release path reads and mutates version-specific
        # storage — ``get_vault_state`` decodes the Lagoon v0.5.0 ``State`` enum
        # from a hard-coded ERC-7201 slot, and ``close``/``initiateClosing`` target
        # v0.5.0 selectors. Verify the on-chain implementation IS the expected
        # version BEFORE interpreting that slot; on a version mismatch the slot
        # read is meaningless and closing legs could misbehave, so degrade loudly
        # rather than act on a misread state.
        try:
            self._vault_sdk.verify_version(vault_address, self._config.version)
        except Exception as e:  # noqa: BLE001 — version fault must not raise into teardown
            logger.error("Vault release: version verification failed for %s: %s", vault_address, e, exc_info=True)
            return ReleaseResult(degraded=True, reason=f"vault version verification failed: {e}")

        try:
            state = self._vault_sdk.get_vault_state(vault_address)
        except Exception as e:  # noqa: BLE001 — read fault must not raise into teardown
            logger.error("Vault release: could not read vault state for %s: %s", vault_address, e, exc_info=True)
            return ReleaseResult(degraded=True, reason=f"could not read vault state: {e}")

        vault_state = self.get_vault_state()
        logger.info(
            "Vault release starting: on-chain state=%s, persisted phase=%s", state, vault_state.release_phase.value
        )

        try:
            # Already Closed (or a resumed run past close): only the manager's own
            # residual shares remain to sweep. External depositors redeem themselves.
            if state == _VAULT_STATE_CLOSED:
                vault_state.release_phase = VaultReleasePhase.CLOSED
                self.save_vault_state()
                return await self._release_redeem_manager_shares(strategy, vault_state, commit, final_state=state)

            # Any leg from here signs a closing transaction — enforce single-signer
            # alignment (owner == safe == valuator == gateway key) as a HARD preflight.
            guard_reason = self._release_preflight(strategy)
            if guard_reason is not None:
                logger.error("Vault release preflight FAILED: %s", guard_reason)
                return ReleaseResult(degraded=True, final_state=state, reason=guard_reason)

            if state == _VAULT_STATE_CLOSING:
                return await self._release_from_closing(strategy, market, vault_state, commit)

            # state == Open — full sequence.
            final_nav, degrade_reason = self._resolve_release_nav(strategy, market)
            if degrade_reason is not None:
                return ReleaseResult(degraded=True, final_state=state, reason=degrade_reason)
            assert final_nav is not None  # noqa: S101 — narrowed by degrade_reason is None
            return await self._release_open_to_closed(strategy, market, vault_state, final_nav, commit)
        except Exception as e:  # noqa: BLE001 — release must never raise into teardown
            logger.error("Vault release errored for %s: %s", vault_address, e, exc_info=True)
            return ReleaseResult(degraded=True, reason=f"release errored: {e}")

    async def _execute_release_leg(
        self,
        action_type: str,
        bundle: Any,
        signer: str,
        commit: Any | None,
    ) -> Any:
        """Execute one vault-release bundle and pair it with the commit pipeline.

        The SINGLE orchestrator-execution site for the release sequence. Pairing
        the execute with ``commit`` here (never a bare ``orchestrator.execute``)
        is the anti-bypass invariant — every successful teardown intent must drive
        the runner's ledger/accounting commit. A commit failure is loud but never
        blocks the next risk-reducing leg (teardown's inverted semantics).
        """
        result = await self._execution_orchestrator.execute(bundle, wallet_address=signer)
        if commit is not None and getattr(result, "success", False):
            try:
                await commit(action_type=action_type, bundle=bundle, execution_result=result, signer=signer)
            except Exception:  # noqa: BLE001 — accounting is loud-but-never-block
                logger.error(
                    "Vault release: commit pipeline failed for %s leg (chain-side OK) — continuing",
                    action_type,
                    exc_info=True,
                )
        return result

    def _release_preflight(self, strategy: Any) -> str | None:
        """Single-signer alignment hard preflight (§f2). Returns a reason on failure.

        The MVP release path signs ``updateNewTotalAssets`` (valuator),
        ``initiateClosing`` (owner) and ``close`` (safe) with ONE gateway key. If
        those roles are distinct the closing legs are unsignable and the vault would
        be left Open with depositors stranded — so a mismatch fails LOUDLY and
        actionably rather than silently skipping the release. Dual-key release is a
        deferred Phase-5 concern.
        """
        wallet = (getattr(strategy, "wallet_address", "") or "").lower()
        if not wallet:
            return "strategy has no wallet_address — cannot verify single-signer alignment for vault release"
        vault_address = self._config.vault_address
        try:
            roles = self._vault_sdk.get_roles_storage(vault_address) or {}
            on_chain_safe = (roles.get("safe") or "").lower()
            on_chain_valuator = (roles.get("valuationManager") or "").lower()
            on_chain_owner = (self._vault_sdk.get_owner(vault_address) or "").lower()
        except Exception as e:  # noqa: BLE001
            return f"could not read vault roles/owner for single-signer preflight: {e}"

        mismatches: list[str] = []
        if on_chain_safe != wallet:
            mismatches.append(f"safe({on_chain_safe})")
        if on_chain_valuator != wallet:
            mismatches.append(f"valuationManager({on_chain_valuator})")
        if on_chain_owner != wallet:
            mismatches.append(f"owner({on_chain_owner})")
        if self._config.valuator_address.lower() != wallet:
            mismatches.append(f"config.valuator({self._config.valuator_address.lower()})")
        if mismatches:
            return (
                "vault-release requires single-signer alignment owner==safe==valuator==gateway key "
                f"({wallet}); mismatched roles: {', '.join(mismatches)}. The closing legs "
                "(initiateClosing/close/updateNewTotalAssets) are unsignable — teardown left the "
                "LP closed but the vault OPEN. A human must align the roles (or run a dual-key "
                "release) so depositors are not stranded."
            )
        return None

    def _resolve_release_nav(self, strategy: Any, market: Any) -> tuple[int | None, str | None]:
        """Compute the final NAV to propose for ``close()`` (§c / §f4).

        Two constraints:

        1. The NAV must be BACKED by the Safe's realized post-unwind underlying
           balance so ``close()``'s ``transferFrom(safe, vault, totalAssets)``
           cannot revert.
        2. The NAV must back the DEPOSITOR OBLIGATIONS (the settled share value,
           ``totalAssets()``), NOT the full Safe balance — the Safe may also hold
           the manager's own uncounted capital (e.g. seed funds not represented by
           shares), and proposing the full balance would hand that capital to
           depositors at close.

        So, with ``obligations = totalAssets()`` and ``realized = Safe balance``:

        - ``realized >= obligations`` -> propose ``obligations`` (back the shares
          exactly; leave any non-depositor excess in the Safe).
        - ``realized < obligations`` within tolerance -> propose ``realized``
          (clamp down; a small rounding / fee-drag / benign-slippage haircut).
        - shortfall LARGER than tolerance -> genuine loss vs obligations; DO NOT
          force a haircut close. Degrade + surface for a human (§f4).
        - ``obligations <= 0`` (no settled depositor backing) -> propose realized.

        Returns ``(final_nav, None)`` on success or ``(None, reason)`` to degrade.
        """
        if market is None:
            return None, "no market snapshot available to measure post-unwind Safe balance for vault release"
        realized = self._read_safe_underlying_raw(strategy, market)
        if realized is None:
            return (
                None,
                "could not measure Safe underlying balance (Empty != Zero) — refusing to propose an unbacked NAV",
            )

        try:
            obligations = self._vault_sdk.get_total_assets(self._config.vault_address)
        except Exception as e:  # noqa: BLE001
            return None, f"could not read vault obligations (totalAssets) for NAV safety check: {e}"

        if obligations <= 0:
            # No settled depositor backing (zero share-value). Propose ZERO, NOT the
            # realized Safe balance (audit #6): with no shares to redeem, proposing
            # the full balance would pull the Safe's (entirely non-depositor) capital
            # into a Closed vault where nobody can redeem it — a strand/loss path.
            # Closing at zero leaves that capital in the Safe for the manager to
            # recover. (A teardown racing UNSETTLED pending deposits is out of scope
            # here — that needs protocol-aware pending handling; tracked with the #4
            # composite-close accounting follow-up.)
            logger.info(
                "Vault release: no settled obligations (totalAssets<=0) — proposing NAV=0 "
                "(leave non-depositor Safe capital in place, do not transfer to a closed vault)"
            )
            return 0, None

        if realized >= obligations:
            # Back the shares exactly; leave any non-depositor excess (e.g. the
            # manager's own seed capital) in the Safe rather than paying it out.
            return obligations, None

        shortfall = obligations - realized
        shortfall_bps = shortfall * 10000 // obligations
        if shortfall_bps > _RELEASE_HAIRCUT_TOLERANCE_BPS:
            return None, (
                f"vault-release NAV shortfall too large to auto-close: Safe holds {realized} underlying "
                f"but obligations are {obligations} ({shortfall_bps} bps short > "
                f"{_RELEASE_HAIRCUT_TOLERANCE_BPS} bps tolerance). Refusing to force a haircut close "
                "(would dilute all depositors). Human decision required: top up the Safe or accept the haircut."
            )
        logger.warning(
            "Vault release: realized %d < obligations %d (%d bps) within tolerance — clamping NAV to realized",
            realized,
            obligations,
            shortfall_bps,
        )
        return realized, None

    def _read_safe_underlying_raw(self, strategy: Any, market: Any) -> int | None:
        """Read the Safe's underlying-token balance in raw units, or None if unmeasured."""
        underlying = self._config.underlying_token
        # VIB-5667 (audit #2): the teardown MarketSnapshot was built BEFORE the
        # closing intents unwound the LP to underlying, so its balance cache holds
        # a PRE-unwind figure. Release NAV must reflect the realized POST-unwind
        # Safe balance — evict the memo so ``balance`` does a fresh live read
        # (mirrors the teardown exec lanes' VIB-5465/5074 invalidate-before-read).
        invalidate = getattr(market, "invalidate_balance", None)
        if callable(invalidate):
            try:
                invalidate(underlying)
            except Exception:  # noqa: BLE001 — best-effort; a stale read still degrades safely
                logger.debug(
                    "Vault release: invalidate_balance(%s) failed; using cached balance", underlying, exc_info=True
                )
        try:
            bal = market.balance(underlying)
        except Exception:  # noqa: BLE001 — Empty != Zero: an unread balance is not zero
            logger.warning("Vault release: could not read Safe %s balance for NAV", underlying, exc_info=True)
            return None
        raw = bal.balance if hasattr(bal, "balance") else bal
        if raw is None:
            return None
        try:
            decimals = get_token_resolver().get_decimals(strategy.chain, underlying)
            return int(Decimal(str(raw)) * Decimal(10) ** decimals)
        except Exception:  # noqa: BLE001
            logger.warning("Vault release: could not convert Safe %s balance to raw units", underlying, exc_info=True)
            return None

    async def _release_open_to_closed(
        self,
        strategy: Any,
        market: Any,
        vault_state: VaultState,
        final_nav: int,
        commit: Any | None,
    ) -> ReleaseResult:
        """Open->Closing->Closed: propose NAV, initiateClosing, then close + redeem."""
        vault_address = self._config.vault_address
        wallet = strategy.wallet_address

        # 2b: propose final NAV [valuator].
        propose_bundle = self._vault_adapter.build_propose_valuation_bundle(
            UpdateTotalAssetsParams(
                vault_address=vault_address,
                valuator_address=self._config.valuator_address,
                new_total_assets=final_nav,
                pending_deposits=0,
            )
        )
        propose_result = await self._execute_release_leg(
            "PROPOSE_VAULT_VALUATION", propose_bundle, self._config.valuator_address, commit
        )
        if not getattr(propose_result, "success", False):
            return ReleaseResult(
                degraded=True, final_state=_VAULT_STATE_OPEN, reason="vault-release propose NAV failed"
            )
        vault_state.release_phase = VaultReleasePhase.PROPOSED
        vault_state.release_final_nav = final_nav
        self.save_vault_state()

        # 2d: initiateClosing [owner] -> Open becomes Closing (re-proposes NAV).
        initiate_bundle = self._vault_adapter.build_initiate_closing_bundle(
            InitiateClosingParams(vault_address=vault_address, owner_address=wallet)
        )
        initiate_result = await self._execute_release_leg("INITIATE_VAULT_CLOSING", initiate_bundle, wallet, commit)
        if not getattr(initiate_result, "success", False):
            return ReleaseResult(
                degraded=True, final_state=_VAULT_STATE_OPEN, reason="vault-release initiateClosing failed"
            )
        vault_state.release_phase = VaultReleasePhase.CLOSING_INITIATED
        self.save_vault_state()

        return await self._release_from_closing(strategy, market, vault_state, commit, approved_nav=final_nav)

    async def _release_from_closing(
        self,
        strategy: Any,
        market: Any,
        vault_state: VaultState,
        commit: Any | None,
        *,
        approved_nav: int | None = None,
    ) -> ReleaseResult:
        """Closing->Closed: close with the SAFETY-APPROVED NAV, then redeem.

        ``approved_nav`` is the obligations-capped / backing-checked value from
        :meth:`_resolve_release_nav` (passed by :meth:`_release_open_to_closed`).
        When ``None`` — a run that resumed directly into on-chain ``Closing`` — it is
        recovered from the persisted ``release_final_nav`` if a prior run reached the
        PROPOSED/CLOSING phase, else recomputed under the same safety rules. It is
        NEVER taken from whatever the on-chain slot currently holds.
        """
        vault_address = self._config.vault_address
        wallet = strategy.wallet_address

        # The safety-approved NAV is the SOURCE OF TRUTH for close() (audit #4).
        if approved_nav is None:
            if vault_state.release_phase in (VaultReleasePhase.PROPOSED, VaultReleasePhase.CLOSING_INITIATED):
                approved_nav = vault_state.release_final_nav
            else:
                approved_nav, degrade_reason = self._resolve_release_nav(strategy, market)
                if degrade_reason is not None:
                    return ReleaseResult(degraded=True, final_state=_VAULT_STATE_CLOSING, reason=degrade_reason)
                assert approved_nav is not None  # noqa: S101

        # ``close(nav)`` reverts ``WrongNewTotalAssets`` unless ``nav`` matches the
        # on-chain ``newTotalAssets()`` slot EXACTLY, so we must READ the slot. But we
        # must NEVER close with whatever the slot happens to hold (audit #4): the slot
        # may be consumed (max sentinel), stale, or a divergent proposal, and closing
        # with it would BYPASS the obligations cap ``_resolve_release_nav`` enforced —
        # pulling non-depositor Safe capital into a Closed vault nobody can redeem. So
        # require the slot to hold EXACTLY ``approved_nav``; if it does not, re-propose
        # the approved value; if it still cannot be established, degrade (never close).
        nta = self._vault_sdk.get_new_total_assets(vault_address)
        if nta != approved_nav:
            logger.warning(
                "Vault release: newTotalAssets slot=%s != approved NAV=%s — re-proposing the approved value before close",
                nta,
                approved_nav,
            )
            propose_bundle = self._vault_adapter.build_propose_valuation_bundle(
                UpdateTotalAssetsParams(
                    vault_address=vault_address,
                    valuator_address=self._config.valuator_address,
                    new_total_assets=approved_nav,
                    pending_deposits=0,
                )
            )
            propose_result = await self._execute_release_leg(
                "PROPOSE_VAULT_VALUATION", propose_bundle, self._config.valuator_address, commit
            )
            if not getattr(propose_result, "success", False):
                return ReleaseResult(
                    degraded=True, final_state=_VAULT_STATE_CLOSING, reason="vault-release re-propose (Closing) failed"
                )
            nta = self._vault_sdk.get_new_total_assets(vault_address)
            if nta != approved_nav:
                return ReleaseResult(
                    degraded=True,
                    final_state=_VAULT_STATE_CLOSING,
                    reason=(
                        f"vault-release could not establish the approved newTotalAssets ({approved_nav}); "
                        f"slot still reads {nta} — refusing to close() with an unvetted NAV"
                    ),
                )

        vault_state.release_final_nav = approved_nav
        self.save_vault_state()

        # Ensure the vault is authorised to pull the Safe's underlying at close.
        approve_bundle = self._build_release_approve_bundle(wallet)
        if approve_bundle is not None:
            await self._execute_release_leg("APPROVE", approve_bundle, wallet, commit)

        # 2e: close(approved_nav) [safe] — atomic takeFees->settleDeposit->
        # settleRedeem->state=Closed->transferFrom(safe, vault, totalAssets). Reverts
        # if Safe short. Closes with the VETTED value, never the raw slot readback.
        close_bundle = self._vault_adapter.build_close_bundle(
            CloseVaultParams(vault_address=vault_address, safe_address=wallet, new_total_assets=approved_nav)
        )
        close_result = await self._execute_release_leg("CLOSE_VAULT", close_bundle, wallet, commit)
        if not getattr(close_result, "success", False):
            return ReleaseResult(
                degraded=True,
                final_state=_VAULT_STATE_CLOSING,
                reason="vault-release close() failed (Safe may not back totalAssets — do NOT force; human check)",
            )
        vault_state.release_phase = VaultReleasePhase.CLOSED
        self.save_vault_state()
        logger.info("Vault release: close() landed — vault CLOSED, all depositor capital now claimable")

        return await self._release_redeem_manager_shares(strategy, vault_state, commit, final_state=_VAULT_STATE_CLOSED)

    async def _release_redeem_manager_shares(
        self,
        strategy: Any,
        vault_state: VaultState,
        commit: Any | None,
        *,
        final_state: str,
    ) -> ReleaseResult:
        """Redeem the manager's own residual shares post-close (permissionless, sync).

        External depositors (already-redeemed or deposit-only) redeem THEMSELVES via
        ``vault.redeem`` — teardown neither can nor should pull their shares. This
        only sweeps the manager's own shares back to underlying. No-op when the
        manager holds none.
        """
        vault_address = self._config.vault_address
        wallet = strategy.wallet_address
        redeemed = 0
        try:
            shares = self._vault_sdk.get_underlying_balance(vault_address, wallet)
        except Exception as e:  # noqa: BLE001
            logger.warning("Vault release: could not read manager share balance for redeem: %s", e)
            shares = 0

        if shares > 0:
            redeem_bundle = self._vault_adapter.build_redeem_bundle(
                RedeemVaultParams(vault_address=vault_address, controller_address=wallet, shares=shares)
            )
            redeem_result = await self._execute_release_leg("REDEEM_VAULT", redeem_bundle, wallet, commit)
            if getattr(redeem_result, "success", False):
                redeemed = shares
                logger.info("Vault release: redeemed manager's own %d shares back to underlying", shares)
            else:
                # A failed self-redeem does NOT strand depositors (the vault is
                # Closed; the manager can retry). Degrade but report release done.
                logger.warning("Vault release: manager self-redeem failed (vault still Closed; retryable)")
        else:
            logger.info("Vault release: manager holds no residual shares — nothing to redeem")

        vault_state.release_phase = VaultReleasePhase.DEPOSITORS_RELEASED
        self.save_vault_state()
        return ReleaseResult(
            released=True,
            final_state=final_state,
            final_nav=vault_state.release_final_nav,
            manager_shares_redeemed=redeemed,
        )

    def _build_release_approve_bundle(self, safe_address: str) -> Any | None:
        """Build a Safe->vault MAX approve for the underlying (idempotent), or None."""
        from almanak.core.enums import ActionType
        from almanak.framework.models.reproduction_bundle import ActionBundle

        vault_address = self._config.vault_address
        try:
            underlying_token = self._vault_sdk.get_underlying_token_address(vault_address)
        except Exception as e:  # noqa: BLE001
            logger.warning("Vault release: could not read underlying token address for approve: %s", e)
            return None
        tx = self._vault_sdk.build_approve_deposit_tx(underlying_token, vault_address, safe_address, _MAX_UINT256)
        return ActionBundle(
            intent_type=ActionType.APPROVE.value,
            transactions=[tx],
            metadata={"vault_address": vault_address, "spender": vault_address, "token": underlying_token},
        )

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
            "release_phase": vault_state.release_phase.value,
            "release_final_nav": vault_state.release_final_nav,
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
            release_phase=VaultReleasePhase(data.get("release_phase", "not_started")),
            release_final_nav=data.get("release_final_nav", 0),
        )
