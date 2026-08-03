"""Callable bag plumbed from StrategyRunner into TeardownManager — VIB-3773.

The teardown lane needs to call back into the runner for two purposes:

1. Per-intent **commit pipeline** (enrich → ledger → outbox+fire → sidecar)
   after a successful ``orchestrator.execute_bundle`` call.
2. Pre- and post-teardown **snapshot bracket** (snapshot + metrics writes
   stamped with the teardown's cycle id).

Rather than widening :class:`TeardownManager`'s protocol surface to a full
``StrategyRunner`` instance — which would couple a deliberately narrow
component to the runner's whole API — we pass two pre-bound async callables.

* :attr:`commit` is :func:`runner.teardown_commit.commit_teardown_intent`
  with the runner already bound, exposing the keyword-only contract:
  ``commit(strategy, intent, *, execution_result, execution_context,
  bundle_metadata=None, teardown_cycle_id) -> TeardownCommitOutcome``.
* :attr:`capture_snapshot` is
  :func:`_run_loop_helpers.capture_teardown_snapshot_with_accounting`
  bound similarly: ``capture_snapshot(strategy, *, teardown_cycle_id,
  pre_teardown) -> TeardownSnapshotOutcome``.

Either may be ``None`` for backward compatibility — :class:`TeardownManager`
falls back to the legacy bypass behaviour (no accounting writes) so existing
unit tests that construct the manager without a runner keep working. Phase
3 wiring at ``_teardown_helpers.build_teardown_manager`` always populates
both in production.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:  # pragma: no cover
    from ..runner._run_loop_helpers import TeardownSnapshotOutcome
    from ..runner.teardown_commit import TeardownCommitOutcome

logger = logging.getLogger(__name__)

_TERMINAL_SETTLEMENT_MARKER = "_teardown_async_settlement_terminal"
_TERMINAL_SETTLEMENT_ORDER_KEYS = "_teardown_async_settlement_order_keys"


def _set_terminal_settlement_marker(execution_result: Any, orders: tuple[Any, ...]) -> None:
    """Mark terminal settlement and retain the exact accepted order identities."""

    def _order_key(order: Any) -> str:
        if isinstance(order, dict):
            raw = order.get("order_id") or order.get("order_key") or ""
        else:
            raw = getattr(order, "order_id", "") or getattr(order, "order_key", "") or ""
        return str(raw).lower()

    order_keys = tuple(order_key for order in orders if (order_key := _order_key(order)))
    if isinstance(execution_result, dict):
        execution_result[_TERMINAL_SETTLEMENT_MARKER] = True
        execution_result[_TERMINAL_SETTLEMENT_ORDER_KEYS] = order_keys
    else:
        setattr(execution_result, _TERMINAL_SETTLEMENT_MARKER, True)
        setattr(execution_result, _TERMINAL_SETTLEMENT_ORDER_KEYS, order_keys)


def _has_terminal_settlement_marker(execution_result: Any) -> bool:
    if isinstance(execution_result, dict):
        return execution_result.get(_TERMINAL_SETTLEMENT_MARKER) is True
    return getattr(execution_result, _TERMINAL_SETTLEMENT_MARKER, False) is True


def _terminal_settlement_order_keys(execution_result: Any) -> tuple[str, ...]:
    if isinstance(execution_result, dict):
        raw = execution_result.get(_TERMINAL_SETTLEMENT_ORDER_KEYS, ())
    else:
        raw = getattr(execution_result, _TERMINAL_SETTLEMENT_ORDER_KEYS, ())
    return tuple(str(key).lower() for key in raw or () if str(key))


async def _reconcile_terminal_perp_settlement(
    runner: Any,
    strategy: Any,
    execution_result: Any,
    execution_context: Any,
    teardown_cycle_id: str,
) -> tuple[Any, ...]:
    """Book correlated Phase-2 settlement after Phase-1 teardown commit."""
    if not _has_terminal_settlement_marker(execution_result):
        return ()
    # Phase 1 (submission ledger/event) must exist before Phase 2 settlement
    # accounting. Reuse the restart-safe, order-key-correlated reconciler
    # immediately after the terminal barrier so teardown does not exit before
    # booking keeper economics / closing the durable registry row. Never parse
    # an uncorrelated batched keeper receipt through ResultEnricher (VIB-6152).
    try:
        from ..runner.perp_settlement_reconciler import reconcile_perp_settlements

        reconciliation = await reconcile_perp_settlements(
            runner,
            strategy,
            deployment_id=strategy.deployment_id,
            cycle_id=teardown_cycle_id or strategy.deployment_id,
            gateway_client=runner._get_gateway_client(),
            chain=str(getattr(execution_context, "chain", "") or getattr(strategy, "chain", "") or ""),
            wallet_address=str(
                getattr(execution_context, "wallet_address", "") or getattr(strategy, "wallet_address", "") or ""
            ),
        )
    except Exception as exc:  # noqa: BLE001 — settlement accounting is loud but never strands risk reduction
        logger.exception(
            "Teardown terminal perp settlement reconciliation raised for %s; "
            "the durable watch set will retry on the next runner tick",
            strategy.deployment_id,
        )
        degraded_reason = f"terminal settlement reconciliation raised: {exc}"
    else:
        expected_order_keys = set(_terminal_settlement_order_keys(execution_result))
        booked_order_keys = {str(key).lower() for key in reconciliation.booked_order_keys}
        missing_order_keys = expected_order_keys - booked_order_keys
        if not reconciliation.accounting_degraded and not missing_order_keys:
            return ()
        reasons = list(reconciliation.degraded_reasons)
        if missing_order_keys:
            reasons.append(
                "terminal settlement was observed but Phase-2 did not book accepted order(s): "
                + ", ".join(sorted(missing_order_keys))
            )
        elif not expected_order_keys and reconciliation.booked == 0:
            # Fail loud for legacy/test result containers that carry only the
            # terminal marker. Production markers always retain exact keys.
            reasons.append("terminal settlement was observed but Phase-2 booked no settlement")
        degraded_reason = "; ".join(reasons) or "terminal settlement reconciliation degraded"

    from ..accounting.deferred_log import DeferredWrite
    from ..accounting.deferred_log import append as deferred_append

    record = DeferredWrite.now(
        kind="perp_settlement",
        deployment_id=strategy.deployment_id,
        cycle_id=teardown_cycle_id or strategy.deployment_id,
        intent_type="PERP_SETTLEMENT",
        error=degraded_reason,
    )
    deferred_append(record)
    return (record,)


CommitTeardownIntent = Callable[..., Awaitable["TeardownCommitOutcome"]]
"""Type alias for the runner-bound commit callable."""

CaptureTeardownSnapshot = Callable[..., Awaitable["TeardownSnapshotOutcome"]]
"""Type alias for the runner-bound snapshot-bracket callable."""

SnapshotIntentBalances = Callable[..., Awaitable[Any | None]]
"""Async ``(strategy, intent) -> BalanceSnapshot | None``. Captures wallet
balances for the tokens this intent will move, BEFORE it executes — the
teardown counterpart of the iteration lane's
``_snapshot_balances_for_intent``. Used to seed
``transaction_ledger.pre_state_json`` with per-intent (not pre-bracket-only)
wallet snapshots so the second teardown intent's pre-state correctly
follows the first's post-state."""

ReconcilePostBalances = Callable[..., Awaitable[dict[str, Any] | None]]
"""Async ``(strategy, intent, execution_result, pre_snapshot) -> recon dict``.
Mirrors the iteration lane's ``_reconcile_post_execution_balances`` so
``transaction_ledger.post_state_json`` lands populated on every teardown
row."""

SnapshotIntentLendingState = Callable[..., Awaitable[Any | None]]
"""Async ``(strategy, intent) -> lending state object | None``. Captures the
on-chain lending position state (collateral / debt / HF) BEFORE the intent
executes — the teardown counterpart of the iteration lane's pre-state
capture at ``_init_single_chain_state``. Threaded into the commit pipeline
so ``transaction_ledger.pre_state_json`` carries lending fields lane-
symmetric with iteration (VIB-3934)."""

SnapshotIntentV4LpCloseFees = Callable[..., Awaitable[tuple[int, int] | None]]
"""Async ``(strategy, intent) -> (tokens_owed0, tokens_owed1) | None``. Reads
Uniswap V4 uncollected fees ON-CHAIN BEFORE the LP_CLOSE / LP_COLLECT_FEES burn
executes — the teardown counterpart of the iteration lane's
``state.v4_lp_close_fees`` capture at ``_init_single_chain_state`` (VIB-4482).
A post-burn read returns zero liquidity, so the read MUST happen pre-execute.
Threaded into the commit pipeline so the LP accounting handler emits measured
fees (``fees0/1``) lane-symmetric with iteration. Returns ``None`` for
non-V4-LP-close intents, missing gateway, undeployed chains, or read failures —
never raises, never fabricates a zero (Empty ≠ Zero)."""

SnapshotIntentV4LpCloseNativePrincipal = Callable[..., Awaitable[tuple[int | None, int | None] | None]]
"""Async ``(strategy, intent) -> (amount0, amount1) | None``. Reads the closing
V4 position's native-leg PRINCIPAL ON-CHAIN BEFORE the LP_CLOSE burn executes —
the teardown counterpart of the iteration lane's
``state.v4_lp_close_native_principal`` capture at ``_init_single_chain_state``
(VIB-5117). A native-ETH leg is withdrawn as raw ETH (no Transfer), so the burn
receipt cannot measure it; the principal is derived from the pre-burn position
state (post-burn read = zero liquidity). Threaded into the commit pipeline so the
LP accounting handler records the real native proceeds (instead of a measured-
zero lie) lane-symmetric with iteration. Returns ``None`` for non-native-leg
closes, missing gateway, undeployed chains, or read failures — never raises,
never fabricates a zero (Empty ≠ Zero)."""

WarnSweepNonStrategyBalance = Callable[..., None]
"""Sync ``(strategy, intent, balance_token, balance_value) -> None``. Logs a
WARNING when teardown's ``amount='all'`` SWAP would sweep a wallet balance
the strategy never emitted any accounting events for. Bound via
:func:`build_runner_helpers` against the runner's **accounting** StateManager
(``runner.state_manager``) — the teardown lifecycle state manager does not
expose ``get_accounting_events_sync`` (VIB-4587 / F5)."""

GetTokenUniverse = Callable[..., set[str]]
"""Sync ``(strategy, closing_intents, positions) -> set[str]``. Derives the
strategy-scoped token universe for the token-consolidation phase (VIB-5011).
Bound via :func:`build_runner_helpers` to
:func:`almanak.framework.teardown.consolidation.derive_strategy_token_universe`
with the runner's **accounting** StateManager — so the universe includes the
deployment's accounting-event token footprint, never the full shared wallet."""

GetAccountingEvents = Callable[..., list]
"""Sync ``(strategy) -> list[dict]``. Returns the deployment's accounting
events (timestamp ASC) via the runner's accounting StateManager. Used by the
token-consolidation phase to resolve the ``entry_token`` policy's
earliest-SWAP fallback (VIB-5011). Best-effort: returns ``[]`` on any
failure."""

GetTrackedSwapInventory = Callable[..., dict[str, Any] | None]
"""Sync ``(strategy) -> {canonical_symbol: Decimal} | None``. Deployment-scoped
tracked wallet inventory (Σ open wallet-basis lot ``remaining`` per token, all
sources) used by the ALM-2766 teardown swap-back clamp: a default teardown may
swap back only ``min(tracked, live_balance)``, never the full commingled wallet.
``None`` is the UNMEASURED sentinel (empty deployment id / unreadable events /
FIFO replay failure) and the clamp then fails closed. Bound via
:func:`build_runner_helpers` against the runner's **accounting** StateManager
(``runner.state_manager``) — the teardown lifecycle SM does not expose
``get_accounting_events_sync``. Read-only; never raises."""

DiscoverLpPositions = Callable[..., Awaitable[Any]]
"""Async ``(strategy, candidate_token_ids) -> LpDiscoveryResult``. Runs BOUNDED
on-chain LP discovery (VIB-5138) for the strategy's wallet/chain via the gateway
RpcService — the same NPM scan the ``--discover`` CLI flag uses
(``teardown.discovery``). The teardown manager's auto-fallback path calls
this when the strategy reports no LP (state desync — NFT live on-chain but
``_position_id`` lost, often after an ``AccountingPersistenceError`` on LP
open) so the orphaned NFT is still closed instead of being silently
stranded. ``candidate_token_ids`` is the deployment's provable LP ownership set
(``ownership.token_ids``); it drives the Uniswap V4 verification pass (VIB-6109),
which cannot enumerate the wallet because the V4 PositionManager is not
``ERC721Enumerable``. Returns an ``LpDiscoveryResult`` carrying the discovered
``TeardownPositionSummary`` and an ``incomplete`` flag (True when discovery
could not enumerate every NPM-reported / verify every candidate position — strict
mode raised ``DiscoveryIncomplete``). Never raises: discovery failure degrades the
teardown loudly but must never block the next risk-reducing intent."""

GetDeploymentLpOwnership = Callable[..., Awaitable[Any]]
"""Async ``(strategy, chain) -> DeploymentLpOwnership``. Returns the LP NFT
token ids attributable to THIS deployment on ``chain`` (VIB-5138 / VIB-4976
fund-safety scoping). Built from the deployment's own durable accounting state
— ``position_registry`` OPEN rows (``payload.token_id``, the robust
post-cutover signal that survives the LP-open ``AccountingPersistenceError``
because it is committed atomically with the ledger BEFORE the typed accounting
event) unioned with ``position_events`` LP OPEN rows (``position_id`` = token
id, the pre-cutover fallback). NEVER enumerates the shared wallet. Used to
scope on-chain LP discovery so teardown can only ever close positions this
deployment opened — a sibling strategy's live LP on the same wallet is not in
the set. Never raises; on total read failure returns ``available=False`` so
recovery refuses to close anything (ownership unprovable)."""

AwaitIntentSettlement = Callable[..., Awaitable[str | None]]
"""Async settlement barrier for one successfully submitted teardown intent.

The callable enriches the submission receipt to discover connector-owned async
orders, waits for terminal settlement, and attaches any keeper receipts to the
execution result before teardown reconciliation/accounting runs. ``None`` means
the intent is synchronous or terminally settled; a string is a fail-closed,
operator-facing reason. The caller must never resubmit after a non-``None``
result because the original order was already accepted on-chain.
"""


@dataclass(frozen=True)
class SettlementPreparation:
    """Pre-commit classification of an accepted async submission."""

    applicable: bool
    error: str | None = None
    orders: tuple[Any, ...] = ()


PrepareIntentSettlement = Callable[..., SettlementPreparation]
"""Pre-commit extraction of the accepted connector-owned async order key."""

ReconcileIntentSettlement = Callable[..., Awaitable[tuple[Any, ...]]]
"""Immediate Phase-2 reconciliation after terminal settlement."""

RecoverAcceptedOrderKeys = Callable[[str], Awaitable[tuple[str, ...]]]
"""Recover receipt-enriched order keys from one durable Phase-1 ledger row."""

AcceptedIntentSettlement = Literal["executed", "terminal_failed", "unproven"]
CheckIntentSettlement = Callable[..., Awaitable[AcceptedIntentSettlement | None]]
"""Classify persisted accepted orders; ``None`` means the read was unmeasured."""


@dataclass(frozen=True)
class TeardownRunnerHelpers:
    """Callable bag supplied to :class:`TeardownManager` by Phase 3 wiring.

    All callables are async and pre-bound to a :class:`StrategyRunner`
    instance via :func:`functools.partial`; the teardown manager does not
    need to know about the runner directly.

    Set fields to ``None`` (the dataclass default) to retain pre-VIB-3773
    / pre-VIB-3918 behaviour (no accounting writes from the teardown lane,
    or no per-intent pre/post state). Tests that don't care about the
    accounting lane construct ``TeardownRunnerHelpers()`` and pass it
    straight through.
    """

    commit: CommitTeardownIntent | None = None
    capture_snapshot: CaptureTeardownSnapshot | None = None
    snapshot_intent_balances: SnapshotIntentBalances | None = None
    reconcile_post_balances: ReconcilePostBalances | None = None
    snapshot_intent_lending_state: SnapshotIntentLendingState | None = None
    snapshot_intent_v4_lp_close_fees: SnapshotIntentV4LpCloseFees | None = None
    snapshot_intent_v4_lp_close_native_principal: SnapshotIntentV4LpCloseNativePrincipal | None = None
    warn_sweep_non_strategy_balance: WarnSweepNonStrategyBalance | None = None
    get_token_universe: GetTokenUniverse | None = None
    get_accounting_events: GetAccountingEvents | None = None
    get_tracked_swap_inventory: GetTrackedSwapInventory | None = None
    discover_lp_positions: DiscoverLpPositions | None = None
    get_deployment_lp_ownership: GetDeploymentLpOwnership | None = None
    prepare_intent_settlement: PrepareIntentSettlement | None = None
    await_intent_settlement: AwaitIntentSettlement | None = None
    reconcile_intent_settlement: ReconcileIntentSettlement | None = None
    recover_accepted_order_keys: RecoverAcceptedOrderKeys | None = None
    check_intent_settlement: CheckIntentSettlement | None = None

    @property
    def has_commit(self) -> bool:
        return self.commit is not None

    @property
    def has_snapshot(self) -> bool:
        return self.capture_snapshot is not None

    @property
    def has_per_intent_balances(self) -> bool:
        """True iff both pre- and post-execution balance helpers are wired.
        Either-only is useless: pre without post can't produce post_state,
        post without pre can't produce pre_state. Treat as all-or-nothing.
        """
        return self.snapshot_intent_balances is not None and self.reconcile_post_balances is not None

    @property
    def has_lending_pre_state(self) -> bool:
        """True iff the lending pre-state capture helper is wired (VIB-3934)."""
        return self.snapshot_intent_lending_state is not None

    @property
    def has_v4_lp_close_fees(self) -> bool:
        """True iff the V4 LP-close pre-fee capture helper is wired (VIB-4482)."""
        return self.snapshot_intent_v4_lp_close_fees is not None

    @property
    def has_v4_lp_close_native_principal(self) -> bool:
        """True iff the V4 LP-close native-principal capture helper is wired (VIB-5117)."""
        return self.snapshot_intent_v4_lp_close_native_principal is not None

    @property
    def has_sweep_warning(self) -> bool:
        """True iff the teardown-sweep DX warning helper is wired (VIB-4587 / F5)."""
        return self.warn_sweep_non_strategy_balance is not None

    @property
    def has_token_universe(self) -> bool:
        """True iff the consolidation token-universe helper is wired (VIB-5011)."""
        return self.get_token_universe is not None

    @property
    def has_accounting_events(self) -> bool:
        """True iff the accounting-events accessor is wired (VIB-5011)."""
        return self.get_accounting_events is not None

    @property
    def has_tracked_inventory(self) -> bool:
        """True iff the ALM-2766 tracked-inventory accessor is wired."""
        return self.get_tracked_swap_inventory is not None

    @property
    def has_lp_discovery(self) -> bool:
        """True iff the on-chain LP discovery fallback is wired (VIB-5138)."""
        return self.discover_lp_positions is not None and self.get_deployment_lp_ownership is not None

    @property
    def has_async_settlement(self) -> bool:
        """True iff terminal async settlement is wired for teardown intents."""
        return (
            self.prepare_intent_settlement is not None
            and self.await_intent_settlement is not None
            and self.reconcile_intent_settlement is not None
        )


def _intent_protocol(intent: Any) -> str:
    raw = intent.get("protocol") if isinstance(intent, dict) else getattr(intent, "protocol", None)
    return str(raw or "").lower()


def _intent_type(intent: Any) -> str:
    raw = (
        intent.get("type") or intent.get("intent_type")
        if isinstance(intent, dict)
        else getattr(intent, "intent_type", None)
    )
    return str(getattr(raw, "value", raw) or "").upper()


def _async_settlement_capability(intent: Any) -> tuple[bool, str | None]:
    """Return whether the connector requires a terminal barrier, plus lookup failure."""
    protocol = _intent_protocol(intent)
    if not protocol:
        return False, None
    try:
        from almanak.connectors._base.types import ProtocolName
        from almanak.connectors._strategy_runner_hook_registry import STRATEGY_RUNNER_HOOK_REGISTRY

        policy = STRATEGY_RUNNER_HOOK_REGISTRY.async_settlement_policy(ProtocolName(protocol))
    except (ValueError, KeyError):
        return False, None
    except Exception as policy_exc:  # noqa: BLE001 — submission already landed; never escape into retry
        return (
            True,
            f"Async settlement capability lookup failed after submission; refusing to retry the order: {policy_exc}",
        )
    if policy is None:
        return False, None
    submission_intent_types = getattr(policy, "submission_intent_types", None)
    if submission_intent_types is not None and _intent_type(intent) not in submission_intent_types:
        return False, None
    return True, None


def _async_settlement_enrichment_failure(intent: Any, exc: Exception) -> str | None:
    """Classify a receipt-enrichment failure without permitting resubmission."""
    required, lookup_error = _async_settlement_capability(intent)
    if lookup_error is not None:
        return lookup_error
    if not required:
        return None
    protocol = _intent_protocol(intent)
    return (
        "Async settlement receipt enrichment failed; refusing to treat the submitted "
        f"{protocol} order as terminal: {exc}"
    )


def _enrich_teardown_async_orders(
    runner: Any,
    strategy: Any,
    intent: Any,
    execution_result: Any,
    execution_context: Any,
    *,
    bundle_metadata: dict[str, Any] | None = None,
) -> tuple[tuple[Any, ...], str | None]:
    """Extract connector-owned async order identities from the submission."""
    from ..execution.result_enricher import ResultEnricher

    # Async order identifiers are receipt-enriched data. This preliminary
    # enrichment happens before the Phase-1 submission commit so the durable
    # ledger carries the accepted order key before any keeper wait. The commit
    # repeats submission enrichment for its ordinary accounting pipeline;
    # terminal keeper economics are booked separately by the order-key-
    # correlated settlement reconciler.
    try:
        enricher = ResultEnricher(
            live_mode=runner._is_live_mode(),
            pool_key_lookup=runner._build_pool_key_lookup(),
            pool_meta_lookup=runner._build_curve_pool_meta_lookup(),
        )
        enriched = enricher.enrich(
            execution_result,
            intent,
            execution_context,
            bundle_metadata=bundle_metadata,
        )
    except Exception as exc:  # noqa: BLE001 — fail closed only for connectors that declare async settlement
        logger.exception(
            "Teardown pre-settlement enrichment failed for %s; async barrier could not inspect the receipt",
            strategy.deployment_id,
        )
        return (), _async_settlement_enrichment_failure(intent, exc)
    return tuple(getattr(enriched, "async_orders", ()) or ()), None


def _prepare_teardown_intent_settlement(
    runner: Any,
    strategy: Any,
    intent: Any,
    execution_result: Any,
    execution_context: Any,
    *,
    bundle_metadata: dict[str, Any] | None = None,
) -> SettlementPreparation:
    """Attach async order identities before the accepted submission is committed."""
    orders, enrichment_error = _enrich_teardown_async_orders(
        runner,
        strategy,
        intent,
        execution_result,
        execution_context,
        bundle_metadata=bundle_metadata,
    )
    if not orders and enrichment_error is None:
        required, lookup_error = _async_settlement_capability(intent)
        if lookup_error is not None:
            enrichment_error = lookup_error
        elif required:
            enrichment_error = (
                "Async settlement receipt enrichment produced no accepted order identity; "
                "refusing to treat the submitted order as terminal"
            )
    return SettlementPreparation(
        applicable=bool(orders) or enrichment_error is not None,
        error=enrichment_error,
        orders=orders,
    )


async def _await_teardown_intent_settlement(
    runner: Any,
    strategy: Any,
    intent: Any,
    execution_result: Any,
    execution_context: Any,
    *,
    bundle_metadata: dict[str, Any] | None = None,
    preparation: SettlementPreparation | None = None,
) -> str | None:
    """Wait for pre-enriched connector-owned async orders after Phase-1 commit."""
    from ..runner.async_settlement import await_async_settlement

    del bundle_metadata
    orders = (
        preparation.orders if preparation is not None else tuple(getattr(execution_result, "async_orders", ()) or ())
    )
    if not orders:
        return "Async settlement was applicable but its accepted order identity was unavailable"
    gateway_client = runner._get_gateway_client()
    if gateway_client is None:
        return "Async settlement could not be observed because no gateway client is available"

    try:
        barrier = await await_async_settlement(
            gateway_client=gateway_client,
            chain=getattr(execution_context, "chain", "") or getattr(strategy, "chain", ""),
            wallet_address=getattr(execution_context, "wallet_address", "")
            or getattr(strategy, "wallet_address", "")
            or "",
            network=str(getattr(strategy, "_gateway_network", "") or ""),
            orders=orders,
            intent=intent,
            timeout_seconds=runner.config.async_settlement_timeout_seconds,
            poll_interval_seconds=runner.config.async_settlement_poll_interval_seconds,
        )
    except Exception as exc:  # noqa: BLE001 — submission already landed; caller persists then fails without retry
        logger.exception(
            "Teardown async settlement observation raised after submission for %s",
            strategy.deployment_id,
        )
        return f"Async settlement observation failed after submission; refusing to retry the order: {exc}"
    if barrier.terminal and barrier.status.value == "SETTLED":
        if barrier.receipts:
            runner._append_settlement_receipts(execution_result, barrier.receipts)
        _set_terminal_settlement_marker(execution_result, orders)
        logger.info(
            "Teardown async settlement terminal: deployment=%s orders=%s keeper_receipts=%d attempts=%d",
            strategy.deployment_id,
            [str(getattr(order, "order_id", "") or "") for order in orders],
            len(barrier.receipts),
            barrier.attempts,
        )
        return None

    return (
        f"Async settlement {barrier.status.value}: "
        f"{barrier.reason or 'submitted order did not reach terminal settlement'}"
    )


async def _recover_accepted_order_keys(runner: Any, ledger_entry_id: str) -> tuple[str, ...]:
    """Recover receipt-enriched order keys from a committed Phase-1 ledger row."""
    hydrate = getattr(getattr(runner, "state_manager", None), "get_ledger_entry_by_id", None)
    if not callable(hydrate):
        return ()
    try:
        ledger_row = await hydrate(ledger_entry_id)
    except Exception:  # noqa: BLE001 — unmeasured recovery remains fail-closed
        logger.exception(
            "Could not hydrate Phase-1 ledger %s for accepted order-key recovery",
            ledger_entry_id,
        )
        return ()
    if not isinstance(ledger_row, dict):
        return ()
    from ..runner.perp_settlement_reconciler import _parse_async_orders

    return tuple(key.lower() for key, _is_long in _parse_async_orders(ledger_row.get("extracted_data_json") or ""))


async def _check_teardown_intent_settlement(
    runner: Any,
    strategy: Any,
    *,
    ledger_entry_id: str | None,
    order_keys: tuple[str, ...],
    cycle_id: str,
    chain: str,
    wallet_address: str,
) -> AcceptedIntentSettlement | None:
    """Reconcile once, then classify the exact persisted accepted orders."""
    if not order_keys and ledger_entry_id:
        # A preliminary receipt-enrichment fault may have persisted the
        # no-resubmit marker before Phase 1 repeated enrichment successfully.
        # Recover the exact accepted keys from that durable ledger row instead
        # of turning the conservative marker into a permanent liveness trap.
        order_keys = await _recover_accepted_order_keys(runner, ledger_entry_id)
    if not order_keys:
        return None
    from ..runner.perp_settlement_reconciler import reconcile_perp_settlements

    await reconcile_perp_settlements(
        runner,
        strategy,
        deployment_id=strategy.deployment_id,
        cycle_id=cycle_id,
        gateway_client=runner._get_gateway_client(),
        chain=chain,
        wallet_address=wallet_address,
    )
    state_manager = getattr(runner, "state_manager", None)
    reader = getattr(state_manager, "read_accounting_events_measured", None)
    if reader is None:
        return None
    events, measured = await asyncio.to_thread(reader, strategy.deployment_id)
    if not measured:
        return None
    states_by_key: dict[str, str] = {}
    for event in events or ():
        if not isinstance(event, dict):
            continue
        if str(event.get("event_type") or "").upper() != "PERP_SETTLEMENT":
            continue
        if ledger_entry_id and str(event.get("ledger_entry_id") or "") != ledger_entry_id:
            continue
        try:
            payload = json.loads(event.get("payload_json") or "{}")
        except (TypeError, ValueError):
            continue
        order_key = str(payload.get("order_key") or "").lower()
        if order_key in order_keys:
            states_by_key[order_key] = str(payload.get("settlement_state") or "").upper()
    expected = set(order_keys)
    if not expected.issubset(states_by_key):
        return "unproven"
    states = {states_by_key[key] for key in expected}
    # One accepted close may own multiple venue order keys (for example after
    # cancellation/replacement recovery). Any measured execution fulfills the
    # economic close; never authorize another close merely because a sibling
    # key was cancelled or frozen.
    if "EXECUTED" in states:
        return "executed"
    # EXECUTED/CANCELLED/FROZEN are measured venue-terminal states. Once every
    # old order is in that set, no prior order can later execute; a replacement
    # close is safe and necessary when any order did not execute.
    if states.issubset({"EXECUTED", "CANCELLED", "FROZEN"}):
        return "terminal_failed"
    return "unproven"


def build_runner_helpers(runner: Any) -> TeardownRunnerHelpers:
    """Bind the runner instance into a :class:`TeardownRunnerHelpers` bag.

    The runner is bound via :func:`functools.partial` so the consumer
    (``TeardownManager``) calls a plain function with the strategy/intent
    arguments, never the runner.
    """
    from functools import partial

    from ..runner._run_loop_helpers import capture_teardown_snapshot_with_accounting
    from ..runner.runner_state import (
        reconcile_post_execution_balances,
        snapshot_balances_for_intent,
    )
    from ..runner.teardown_commit import commit_teardown_intent
    from .sweep_warning import warn_if_sweep_non_strategy_balance

    async def _snapshot_intent_balances(strategy: Any, intent: Any) -> Any | None:
        # ``snapshot_balances_for_intent`` only needs the runner + intent
        # — strategy is unused but kept on the helper signature for
        # symmetry with reconcile and future protocol-aware variants.
        del strategy
        return await snapshot_balances_for_intent(runner, intent)

    def _warn_sweep_non_strategy_balance(strategy: Any, intent: Any, balance_token: str, balance_value: Any) -> None:
        # VIB-4587 / F5 — wallet-scope teardown sweep DX warning. We compute
        # ``deployment_id`` here (using the same fallback the runner uses
        # for accounting writes) so the call site doesn't have to recompute
        # it, and pass the runner's **accounting** StateManager — the
        # teardown lifecycle SM does not expose ``get_accounting_events_sync``.
        deployment_id = strategy.deployment_id
        warn_if_sweep_non_strategy_balance(
            state_manager=getattr(runner, "state_manager", None),
            deployment_id=deployment_id,
            intent=intent,
            balance_token=balance_token,
            balance_value=balance_value,
        )

    async def _commit_with_heartbeat(strategy: Any, intent: Any, **kwargs: Any) -> Any:
        # VIB-3951 — refresh the teardown crash-watchdog heartbeat once per
        # committed teardown intent so the staleness window reflects REAL
        # liveness (not just time-since-mark_started). A long multi-intent
        # unwind (REPAY → WITHDRAW → SWAP, each ~100s on a slow fork) keeps the
        # owning runner out of the watchdog's stale-by-time bucket. Local-only:
        # the hosted gateway teardown manager has no ``heartbeat`` method (the
        # platform owns hosted liveness), so this is guarded and a no-op there.
        # Best-effort — a heartbeat failure must NEVER interrupt the
        # risk-reducing commit (teardown loud-but-non-blocking contract).
        outcome = await commit_teardown_intent(runner, strategy, intent, **kwargs)
        try:
            from . import get_teardown_state_manager_for_runtime

            manager = get_teardown_state_manager_for_runtime(gateway_client=runner._get_gateway_client())
            beat = getattr(manager, "heartbeat", None)
            if beat is not None:
                beat(strategy.deployment_id)
        except Exception as exc:  # noqa: BLE001 — heartbeat is best-effort
            logger.debug("Teardown heartbeat refresh failed (non-fatal): %s", exc)
        return outcome

    def _get_token_universe(strategy: Any, closing_intents: Any, positions: Any) -> set[str]:
        # VIB-5011 — strategy-scoped token universe for the consolidation
        # planner. The accounting StateManager (runner.state_manager) supplies
        # the deployment's event footprint; the wallet is never enumerated
        # (shared across deployments — a wallet-wide sweep would steal
        # sibling-strategy inventory).
        from .consolidation import derive_strategy_token_universe

        return derive_strategy_token_universe(
            getattr(runner, "state_manager", None),
            strategy.deployment_id,
            strategy,
            closing_intents,
            positions,
        )

    def _get_accounting_events(strategy: Any) -> list:
        # VIB-5011 — best-effort accounting-event read for the entry_token
        # policy's earliest-SWAP fallback. Never raises.
        sm = getattr(runner, "state_manager", None)
        if sm is None or not hasattr(sm, "get_accounting_events_sync"):
            return []
        try:
            return sm.get_accounting_events_sync(strategy.deployment_id)
        except Exception:  # noqa: BLE001 — consolidation is best-effort
            logger.debug("accounting-event read for consolidation failed (non-fatal)", exc_info=True)
            return []

    def _get_tracked_swap_inventory(strategy: Any) -> dict[str, Any] | None:
        # ALM-2766 — deployment-scoped tracked wallet inventory for the
        # teardown swap-back clamp. Reads the runner's accounting StateManager
        # and replays FIFO lots; returns the UNMEASURED sentinel (None) on any
        # failure (the clamp then fails closed). Never raises.
        #
        # VIB-5416 — pass the deployment's chain + wallet so the clamp can key
        # NO_ACCOUNTING ledger acquisitions (STAKE/WRAP/MINT) into the SAME
        # ``swap:{chain}:{wallet}`` pool as real swap lots (1 gateway : 1
        # strategy guarantees these equal every real swap lot's key).
        from .swap_clamp import read_tracked_swap_inventory

        return read_tracked_swap_inventory(
            state_manager=getattr(runner, "state_manager", None),
            deployment_id=(getattr(strategy, "deployment_id", "") or ""),
            chain=(getattr(strategy, "chain", "") or ""),
            wallet_address=(getattr(strategy, "wallet_address", "") or ""),
        )

    async def _snapshot_intent_lending_state(strategy: Any, intent: Any) -> Any | None:
        # VIB-3934 — capture lending pre-state via the runner's safe wrapper
        # so REPAY/WITHDRAW/DELEVERAGE teardown rows carry collateral/debt/HF
        # in ``pre_state_json``, lane-symmetric with the iteration lane's
        # ``state.lending_pre_state``. Returns ``None`` for non-lending
        # intents, missing gateway, unsupported protocols, or transient
        # gateway failures — never raises.
        return runner._capture_lending_state_safe(
            intent=intent,
            chain=getattr(strategy, "chain", "") or "",
            wallet_address=getattr(strategy, "wallet_address", "") or "",
            gateway_client=runner._get_gateway_client(),
            price_oracle=getattr(runner, "_teardown_price_oracle", None),
            phase="pre",
        )

    async def _snapshot_intent_v4_lp_close_fees(strategy: Any, intent: Any) -> tuple[int, int] | None:
        # VIB-4482 — capture Uniswap V4 uncollected fees on-chain BEFORE the
        # LP_CLOSE / LP_COLLECT_FEES burn executes, via the runner's safe
        # wrapper. A post-burn read returns zero liquidity, so this MUST run
        # pre-execute. Returns ``None`` for non-V4-LP-close intents, missing
        # gateway, undeployed chains, or transient gateway failures — never
        # raises, never fabricates a zero (Empty ≠ Zero). Lane-symmetric with
        # the iteration lane's ``state.v4_lp_close_fees``.
        return runner._capture_v4_lp_close_fees_safe(
            intent=intent,
            chain=getattr(strategy, "chain", "") or "",
            gateway_client=runner._get_gateway_client(),
        )

    async def _snapshot_intent_v4_lp_close_native_principal(
        strategy: Any, intent: Any
    ) -> tuple[int | None, int | None] | None:
        # VIB-5117 — capture the closing V4 position's native-leg PRINCIPAL
        # on-chain BEFORE the LP_CLOSE burn executes, via the runner's safe
        # wrapper. A native-ETH leg is withdrawn as raw ETH (no Transfer), so a
        # post-burn read returns zero liquidity — this MUST run pre-execute.
        # Returns ``None`` for non-native-leg closes, missing gateway, undeployed
        # chains, or read failures — never raises, never fabricates a zero
        # (Empty ≠ Zero). Lane-symmetric with the iteration lane's
        # ``state.v4_lp_close_native_principal``.
        return runner._capture_v4_lp_close_native_principal_safe(
            intent=intent,
            chain=getattr(strategy, "chain", "") or "",
            gateway_client=runner._get_gateway_client(),
        )

    return TeardownRunnerHelpers(
        commit=_commit_with_heartbeat,
        capture_snapshot=partial(capture_teardown_snapshot_with_accounting, runner),
        snapshot_intent_balances=_snapshot_intent_balances,
        reconcile_post_balances=partial(reconcile_post_execution_balances, runner),
        snapshot_intent_lending_state=_snapshot_intent_lending_state,
        snapshot_intent_v4_lp_close_fees=_snapshot_intent_v4_lp_close_fees,
        snapshot_intent_v4_lp_close_native_principal=_snapshot_intent_v4_lp_close_native_principal,
        warn_sweep_non_strategy_balance=_warn_sweep_non_strategy_balance,
        get_token_universe=_get_token_universe,
        discover_lp_positions=partial(_discover_lp_for_teardown, runner),
        get_deployment_lp_ownership=partial(_deployment_lp_ownership, runner),
        get_accounting_events=_get_accounting_events,
        get_tracked_swap_inventory=_get_tracked_swap_inventory,
        prepare_intent_settlement=partial(_prepare_teardown_intent_settlement, runner),
        await_intent_settlement=partial(_await_teardown_intent_settlement, runner),
        reconcile_intent_settlement=partial(_reconcile_terminal_perp_settlement, runner),
        recover_accepted_order_keys=partial(_recover_accepted_order_keys, runner),
        check_intent_settlement=partial(_check_teardown_intent_settlement, runner),
    )


async def _read_registry_lp_token_ids(state_manager: Any, deployment_id: str, chain: str) -> tuple[set[str], bool]:
    """LP NFT token ids from this deployment's ``position_registry`` OPEN rows.

    Source 1 of :func:`_deployment_lp_ownership` (robust, survives the LP-open
    ``AccountingPersistenceError`` desync — the registry row is committed
    atomically with the ledger BEFORE the typed accounting event). Owns its own
    try/except: a backend without registry storage raises
    ``CutoverStorageNotSupported`` (caught → treated as "no registry signal").

    Returns ``(token_ids, ok)``. ``ok`` is True only when the read completed
    (so the coordinator can compute ``available = registry_ok or events_ok``).
    Never raises.
    """
    if state_manager is None or not hasattr(state_manager, "get_position_registry_open_rows"):
        return set(), False
    token_ids: set[str] = set()
    try:
        rows = await state_manager.get_position_registry_open_rows(deployment_id, chain=chain, primitive="lp")
    except Exception as exc:  # noqa: BLE001 — CutoverStorageNotSupported et al.
        logger.debug(
            "Teardown LP ownership: position_registry read unavailable for %s (%s); falling back to position_events",
            deployment_id,
            exc,
        )
        return set(), False
    for row in rows or []:
        payload = row.get("payload") if isinstance(row, dict) else None
        tid = payload.get("token_id") if isinstance(payload, dict) else None
        if tid is not None:
            token_ids.add(str(tid))
    return token_ids, True


def _extract_lp_open_token_ids(events: Any, chain: str) -> set[str]:
    """Collect NFT token ids from ``position_events`` LP OPEN rows on ``chain``.

    Pure (no IO). Shared by the sync (sqlite/runner) and async (gateway) read
    paths so both apply identical chain-scoping and ``str(position_id)``
    normalisation — ``position_id`` IS the NFT token id. Rows for other chains,
    rows with a missing/empty ``chain``, and rows without a ``position_id`` are
    skipped.

    Chain attribution fails CLOSED (VIB-5476): a row whose ``chain`` is missing
    or empty is AMBIGUOUS, not owned. In the Plan-B break-glass ``--discover``
    lane the entire safety of wallet-wide teardown rests on this gate only
    admitting provably-owned positions, so a row that does not carry the exact
    requested chain must never count toward ``token_ids``.
    """
    token_ids: set[str] = set()
    target_chain = str(chain or "").strip()
    for ev in events or []:
        if not isinstance(ev, dict):
            continue
        ev_chain = str(ev.get("chain") or "").strip()
        pid = ev.get("position_id")
        if pid is None:
            continue
        # Only count this chain's OPENs toward token_ids. Fail closed: a
        # missing/empty chain is ambiguous, not owned, so it is skipped too.
        if not ev_chain or ev_chain != target_chain:
            continue
        token_ids.add(str(pid))
    return token_ids


def _read_position_event_lp_token_ids(state_manager: Any, deployment_id: str, chain: str) -> tuple[set[str], bool]:
    """LP NFT token ids from this deployment's ``position_events`` LP OPEN rows.

    Source 2 of :func:`read_deployment_lp_ownership` (pre- and post-cutover
    fallback — ``position_id`` IS the NFT token id). Sync read used by the
    sqlite-backed runner lane. Only this chain's OPENs are counted. Owns its own
    try/except.

    Returns ``(token_ids, ok)`` like :func:`_read_registry_lp_token_ids`.
    Never raises.
    """
    if state_manager is None or not hasattr(state_manager, "get_position_events_sync"):
        return set(), False
    try:
        events = state_manager.get_position_events_sync(deployment_id, position_type="LP", event_type="OPEN")
    except Exception as exc:  # noqa: BLE001 — best-effort attribution read
        logger.debug("Teardown LP ownership: position_events read failed for %s (%s)", deployment_id, exc)
        return set(), False
    return _extract_lp_open_token_ids(events, chain), True


async def _read_position_event_lp_token_ids_any(
    state_manager: Any, deployment_id: str, chain: str
) -> tuple[set[str], bool]:
    """Read ``position_events`` LP OPEN token ids via whichever accessor exists.

    The sqlite-backed runner state manager exposes the sync
    ``get_position_events_sync``; the gateway-backed manager
    (:class:`GatewayStateManager`, used by the CLI Plan-B ``--discover`` lane and
    by hosted runners) exposes only the async ``get_position_events_filtered``.
    Without this dispatch the ``position_events`` fallback source silently
    vanishes on the gateway path, so a deployment whose LP attribution lives only
    in ``position_events`` (pre-cutover) would have its OWN position refused —
    pushing the operator toward the dangerous ``--wallet-wide`` flag (VIB-5476).

    Prefers the proven sync path; falls back to the gateway async filtered read
    (filtering the OPEN rows client-side, since the filtered API streams every
    event type). Never raises.
    """
    if state_manager is None:
        return set(), False
    if hasattr(state_manager, "get_position_events_sync"):
        return _read_position_event_lp_token_ids(state_manager, deployment_id, chain)
    if hasattr(state_manager, "get_position_events_filtered"):
        try:
            events = await state_manager.get_position_events_filtered(
                deployment_id=deployment_id, position_types=frozenset({"LP"})
            )
        except Exception as exc:  # noqa: BLE001 — best-effort attribution read
            logger.debug("Teardown LP ownership: gateway position_events read failed for %s (%s)", deployment_id, exc)
            return set(), False
        opens = [ev for ev in (events or []) if isinstance(ev, dict) and ev.get("event_type") == "OPEN"]
        return _extract_lp_open_token_ids(opens, chain), True
    return set(), False


async def read_deployment_lp_ownership(state_manager: Any, deployment_id: str, chain: str) -> Any:
    """LP token ids attributable to a deployment from its durable state (VIB-5138).

    Runner-free coordinator so BOTH the runner recovery lane
    (:func:`_deployment_lp_ownership`) and the CLI Plan-B ``--discover`` lane
    (VIB-5476) share one attribution read. Unions two complementary,
    independently-surviving sources — :func:`_read_registry_lp_token_ids`
    (robust, post-cutover) and :func:`_read_position_event_lp_token_ids_any`
    (pre-cutover fallback; sync sqlite OR gateway async accessor). ``had_lp_open``
    is True iff any source contributed
    an id; ``available`` is True iff at least one read completed. When BOTH
    reads fail ``available=False`` so the caller refuses to close anything
    (ownership unprovable). Never raises.

    Reusable for VIB-5485 (registry-authoritative flip backfill): the returned
    :class:`DeploymentLpOwnership` is the deployment's provable on-chain LP set.
    """
    from .lp_recovery import DeploymentLpOwnership

    deployment_id = (deployment_id or "").strip()
    registry_ids, registry_ok = await _read_registry_lp_token_ids(state_manager, deployment_id, chain)
    event_ids, events_ok = await _read_position_event_lp_token_ids_any(state_manager, deployment_id, chain)

    token_ids = registry_ids | event_ids
    available = registry_ok or events_ok
    if not available:
        logger.warning(
            "Teardown LP ownership: NO attribution source readable for %s on %s — "
            "recovery will refuse to close discovered NFTs (ownership unprovable).",
            deployment_id,
            chain,
        )
    return DeploymentLpOwnership(
        token_ids=frozenset(token_ids),
        had_lp_open=bool(token_ids),
        available=available,
    )


async def _deployment_lp_ownership(runner: Any, strategy: Any, chain: str) -> Any:
    """LP token ids attributable to THIS deployment on ``chain`` (VIB-5138).

    Fund-safety scoping (VIB-4976): the on-chain discovery scan is wallet-scoped
    and a wallet may be shared across deployments. This reads the deployment's
    OWN durable accounting state to learn which NFT token ids it opened, so
    teardown recovery can never close a sibling strategy's live LP on the same
    wallet. Delegates to :func:`read_deployment_lp_ownership`.

    Bound to the runner via :func:`functools.partial` so the consumer calls
    ``(strategy, chain) -> DeploymentLpOwnership``.
    """
    deployment_id = (getattr(strategy, "deployment_id", "") or "").strip()
    sm = getattr(runner, "state_manager", None)
    return await read_deployment_lp_ownership(sm, deployment_id, chain)


async def _discover_lp_for_teardown(runner: Any, strategy: Any, candidate_token_ids: Any = None) -> Any:
    """Bounded on-chain LP discovery fallback for teardown recovery (VIB-5138).

    Reuses the SAME gateway-backed NPM scan the ``--discover`` CLI flag uses
    (``teardown.discovery``), so the recovery path stays on the gateway
    boundary (no direct network). Strict mode is REQUIRED: a partial scan that
    silently drops a position would re-create the very orphan this fix closes,
    so ``DiscoveryIncomplete`` is caught and surfaced as ``incomplete=True``
    rather than swallowed. Never raises — discovery failure degrades the
    teardown loudly but must never block the next risk-reducing intent.

    ``candidate_token_ids`` is the deployment's provable LP ownership set
    (``ownership.token_ids``), threaded through so the Uniswap V4 verification
    pass (VIB-6109) can check each owned id against the V4 PositionManager — V4
    cannot be wallet-enumerated. Empty/None simply skips the V4 pass.

    Bound to the runner via :func:`functools.partial` in
    :func:`build_runner_helpers` so the consumer calls
    ``(strategy, candidate_token_ids) -> LpDiscoveryResult``.
    """
    from .discovery import DiscoveryIncomplete, discover_lp_positions, to_teardown_summary
    from .lp_recovery import LpDiscoveryResult
    from .models import TeardownPositionSummary

    deployment_id = (getattr(strategy, "deployment_id", "") or "").strip()
    chain = (getattr(strategy, "chain", "") or "").strip()
    wallet = (getattr(strategy, "wallet_address", "") or "").strip()
    empty = TeardownPositionSummary.empty(deployment_id or "unknown")

    if not (deployment_id and chain and wallet):
        logger.warning(
            "Teardown LP discovery skipped: missing deployment_id/chain/wallet (deployment_id=%r chain=%r wallet=%r)",
            deployment_id,
            chain,
            wallet,
        )
        return LpDiscoveryResult(summary=empty, incomplete=False)

    try:
        discovered = await discover_lp_positions(
            client=runner._get_gateway_client(),
            chain=chain,
            wallet=wallet,
            strict=True,
            candidate_token_ids=candidate_token_ids or None,
        )
    except DiscoveryIncomplete as exc:
        return LpDiscoveryResult(summary=empty, incomplete=True, error=str(exc))
    except Exception as exc:  # noqa: BLE001 — never let discovery block risk reduction
        logger.error("Teardown LP discovery failed (non-fatal): %s", exc, exc_info=True)
        return LpDiscoveryResult(summary=empty, incomplete=True, error=str(exc))

    summary = to_teardown_summary(deployment_id=deployment_id, chain=chain, positions=discovered)
    return LpDiscoveryResult(summary=summary, incomplete=False)


__all__ = [
    "CaptureTeardownSnapshot",
    "CommitTeardownIntent",
    "DiscoverLpPositions",
    "GetAccountingEvents",
    "GetDeploymentLpOwnership",
    "GetTokenUniverse",
    "GetTrackedSwapInventory",
    "ReconcilePostBalances",
    "SnapshotIntentBalances",
    "SnapshotIntentLendingState",
    "SnapshotIntentV4LpCloseFees",
    "SnapshotIntentV4LpCloseNativePrincipal",
    "TeardownRunnerHelpers",
    "AwaitIntentSettlement",
    "PrepareIntentSettlement",
    "ReconcileIntentSettlement",
    "SettlementPreparation",
    "WarnSweepNonStrategyBalance",
    "build_runner_helpers",
]
