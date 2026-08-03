"""GMX V2 asynchronous-order lifecycle hooks for the strategy runner."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

from web3.types import RPCEndpoint

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.runner_hook_registry import (
    AsyncSettlementPolicy,
    AsyncSettlementStatus,
    AsyncSettlementVerdict,
    PerpSettlementVerdict,
    PerpSettlementWatchEntry,
    RunnerAsyncSettlementCapability,
    RunnerHookConnector,
    RunnerPerpSettlementCapability,
)
from almanak.connectors.gmx_v2.anvil_order_executor import execute_pending_orders_on_anvil
from almanak.connectors.gmx_v2.perp_settlement import (
    PERP_SETTLEMENT_POLL_INTERVAL_SECONDS,
    PERP_SETTLEMENT_TIMEOUT_SECONDS,
    resolve_perp_settlements,
)
from almanak.connectors.gmx_v2.teardown_reads import read_open_positions, read_pending_orders
from almanak.framework.web3.gateway_provider import GatewayWeb3Provider

logger = logging.getLogger(__name__)

_PROTOCOL = ProtocolName("gmx_v2")
_USD_SCALE = Decimal(10**30)
_PositionKey = tuple[str, str, bool]


@dataclass(frozen=True)
class _GmxSettlementBaseline:
    """Measured position sizes while every submitted order is still pending."""

    position_sizes: tuple[tuple[_PositionKey, int], ...]

    def as_dict(self) -> dict[_PositionKey, int]:
        return dict(self.position_sizes)


def _classify_execution_failure(result: Any) -> AsyncSettlementStatus:
    """Map a failed managed-Anvil execution onto a settlement status (VIB-6438).

    ``transient`` is checked first and ``order_rejected`` second because the two
    are mutually exclusive by construction (the executor's result rejects the
    both-true combination), so the order only matters for a hand-built stub.
    Preferring ``transient`` keeps the retryable reading in that case, which is
    the safe direction: the barrier can still conclude by observation, whereas a
    wrongly-immediate verdict cannot be revisited.

    ``getattr`` defaults preserve the pre-existing tolerance for results that
    predate either flag.
    """
    if getattr(result, "transient", False):
        return AsyncSettlementStatus.OBSERVATION_FAILED
    if getattr(result, "order_rejected", False):
        return AsyncSettlementStatus.ORDER_REJECTED
    return AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED


def _intent_type_str(intent: Any) -> str:
    """Normalize ``intent.intent_type`` to its string value.

    ``intent_type`` may be a ``StrEnum`` member (has ``.value``) or already a
    plain string; the old ``getattr(..., "value", "")`` silently yielded ``""``
    for the string form, dropping keeper-receipt threading. Return ``.value``
    when present, else ``str(...)``.
    """
    raw = getattr(intent, "intent_type", None)
    if raw is None:
        return ""
    return str(getattr(raw, "value", raw))


def _normalize_address(value: Any) -> str | None:
    address = str(value or "").lower()
    if len(address) != 42 or not address.startswith("0x"):
        return None
    try:
        return address if int(address, 16) != 0 else None
    except ValueError:
        return None


def _requested_position_deltas(orders: tuple[Any, ...]) -> dict[_PositionKey, int] | None:
    """Return exact GMX target -> requested raw USD delta, or None if unmeasured."""
    targets: dict[_PositionKey, int] = {}
    for order in orders:
        market = _normalize_address(getattr(order, "market", None))
        collateral = _normalize_address(getattr(order, "collateral_token", None))
        is_long = getattr(order, "is_long", None)
        try:
            size_delta = Decimal(str(getattr(order, "size_delta_usd", None)))
        except (InvalidOperation, ValueError):
            return None
        if market is None or collateral is None or not isinstance(is_long, bool) or size_delta <= 0:
            return None
        key = (market, collateral, is_long)
        targets[key] = targets.get(key, 0) + int(size_delta * _USD_SCALE)
    return targets or None


def _active_position_sizes(positions: Any) -> dict[_PositionKey, int]:
    sizes: dict[_PositionKey, int] = {}
    for position in positions.positions:
        if not getattr(position, "is_active", False):
            continue
        market = _normalize_address(getattr(position, "market", None))
        collateral = _normalize_address(getattr(position, "collateral_token", None))
        is_long = getattr(position, "is_long", None)
        if market is None or collateral is None or not isinstance(is_long, bool):
            continue
        sizes[(market, collateral, is_long)] = int(getattr(position, "size_in_usd", 0) or 0)
    return sizes


def _position_delta_reached(
    intent_type: str,
    requested: dict[_PositionKey, int],
    baseline: _GmxSettlementBaseline,
    current: dict[_PositionKey, int],
) -> bool:
    before = baseline.as_dict()
    if intent_type == "PERP_OPEN":
        return all(current.get(target, 0) >= before.get(target, 0) + delta for target, delta in requested.items())
    if intent_type == "PERP_CLOSE":
        return all(
            before.get(target, 0) > 0 and current.get(target, 0) <= max(0, before[target] - delta)
            for target, delta in requested.items()
        )
    return False


def _describe_position_sizes(sizes: dict[_PositionKey, int]) -> str:
    """Compact measured evidence for a settlement verdict: raw 1e30 USD sizes per key."""
    if not sizes:
        return "{} (measured empty)"
    return (
        "{"
        + ", ".join(
            f"{market}/{collateral}/{'long' if is_long else 'short'}={size}"
            for (market, collateral, is_long), size in sorted(sizes.items())
        )
        + "}"
    )


def _order_verdict_rows(
    requested_keys: set[str],
    status: AsyncSettlementStatus,
) -> tuple[dict[str, str], ...]:
    return tuple(
        {
            "protocol": str(_PROTOCOL),
            "order_id": key,
            "status": status.value,
        }
        for key in sorted(requested_keys)
        if key
    )


def _observation_failed(reason: str, observation_state: Any = None) -> AsyncSettlementVerdict:
    return AsyncSettlementVerdict(
        status=AsyncSettlementStatus.OBSERVATION_FAILED,
        terminal=False,
        reason=reason,
        observation_state=observation_state,
    )


def _pending_order_keys(pending: Any) -> set[str]:
    keys = {str(key).lower() for key in pending.order_keys}
    keys.update(str(order.order_key).lower() for order in pending.orders if order.order_key)
    return keys


def _pending_verdict(
    requested_keys: set[str],
    *,
    reason: str,
    observation_state: _GmxSettlementBaseline,
) -> AsyncSettlementVerdict:
    return AsyncSettlementVerdict(
        status=AsyncSettlementStatus.PENDING,
        terminal=False,
        orders=_order_verdict_rows(requested_keys, AsyncSettlementStatus.PENDING),
        reason=reason,
        observation_state=observation_state,
    )


def _capture_settlement_baseline(
    *,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    requested_keys: set[str],
) -> AsyncSettlementVerdict:
    positions = read_open_positions(gateway_client, chain, wallet_address)
    if not getattr(positions, "ok", False):
        return _observation_failed("GMX position baseline was unmeasured while the order was pending")

    pending_after_baseline = read_pending_orders(gateway_client, chain, wallet_address)
    if not pending_after_baseline.ok or not requested_keys.issubset(_pending_order_keys(pending_after_baseline)):
        return _observation_failed("GMX order changed state while its position baseline was being measured")

    baseline = _GmxSettlementBaseline(tuple(sorted(_active_position_sizes(positions).items())))
    return _pending_verdict(
        requested_keys,
        reason="GMX order remains pending; exact target position baseline captured",
        observation_state=baseline,
    )


def _goal_is_baseline_free_decidable(intent_type: str) -> bool:
    """Can this intent's target be judged from the current position read alone?

    Writing ``b`` for the unmeasured baseline size at a target key, ``d`` for the
    requested delta and ``c`` for the measured current size:

    - ``PERP_CLOSE`` wants ``c <= max(0, b - d)``. At ``c == 0`` that holds for every
      ``b`` and ``d``, so a flat account settles a close with no baseline. Sufficient,
      not necessary — a partial close at ``c > 0`` stays undecidable.
    - ``PERP_OPEN`` wants ``c >= b + d``. Undecidable: a pre-existing position of
      sufficient size satisfies any absolute reading while the order may in fact have
      been cancelled.

    Deliberately a named predicate rather than an ``if`` inside the caller, so the
    classification of every verb is one greppable place with a census test over it
    (``tests/unit/connectors/gmx_v2/test_baseline_free_verb_census_vib6297.py``).
    Falling through to the close branch by omission is the failure this shape prevents.
    """
    return intent_type == "PERP_CLOSE"


def _baseline_free_verdict(
    *,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    requested_keys: set[str],
    requested_deltas: dict[_PositionKey, int],
    intent: Any,
    still_pending: set[str],
) -> AsyncSettlementVerdict:
    """Judge an order that left the pending set before any baseline could be captured.

    Reached only when at least one requested order is already gone and
    ``observation_state`` is ``None``. A gone order can never re-enter the pending set,
    so ``_capture_settlement_baseline`` is unreachable from here forever and **every
    further poll is information-free**. The verdict must therefore be reached now, and
    must be terminal wherever it is sound to be terminal — that is what stops the
    barrier from burning its whole budget on a close that already succeeded (VIB-6297).

    EXACTLY ONE STATE IS DECIDABLE HERE: every requested order gone, and every requested
    target measured flat. Nothing else. An earlier version of this function judged more
    than that, and both audit engines on #3533 refuted it independently:

    * **Terminal SETTLED while a sibling is still pending** (Codex P1). The earlier
      docstring argued the optimistic branch needed no ``still_pending`` gate "because a
      flat position means the goal holds regardless of the sibling". That is wrong: an
      unfilled GMX order **holds its collateral in the OrderVault**. Declaring the group
      settled lets teardown proceed to consolidation and sweep past that collateral, and
      blueprint 14 requires the exact accepted orders stay pending until terminal
      settlement. The flat position is not the whole goal state — the live order is part
      of it.

    * **Terminal FAILED on a correctly-filled partial close** (Grok, high). A close of
      delta ``d`` against size ``b`` leaves ``c = b - d > 0`` when it works perfectly.
      The earlier code called that TERMINAL_FAILED, and the claim that a retry is "a
      cheap no-op" only ever held for a FULL close — retrying a partial closes ``d``
      again and **over-closes**. Without a baseline, "filled correctly" and "cancelled"
      are genuinely indistinguishable at ``c > 0``, so neither may be asserted.

    So the pessimistic branch is gone entirely. Anything not provably settled returns
    non-terminal OBSERVATION_FAILED — the pre-existing behaviour, which is an honest
    "unknown" and costs the barrier's budget. That budget is the price of not guessing;
    the mainnet stall this ticket fixes is the full-close-to-flat case, and that case is
    still decided on poll #1.

    Fail-safe polarity, corrected: a false ``SETTLED`` on a close completes teardown over
    a live position or live collateral — the silent strand, forbidden. A false
    ``TERMINAL_FAILED`` is NOT symmetrically cheap, which is what the earlier version got
    wrong. Only an honest unknown is safe in every direction.

    KNOWN LIMITATION — THIS VERDICT PROVES A GOAL STATE, NOT AN ATTRIBUTION (VIB-6334)
    ---------------------------------------------------------------------------------
    ``SETTLED`` here means "every requested order has left the pending set AND every
    requested target is measured flat". It does **not** prove that *our* order is what
    flattened the target. Without a baseline there is no before-image to difference
    against, so a position that went flat by liquidation, by an operator's manual close,
    or by any other actor is indistinguishable from one our close filled.

    Scoped deliberately, because the two consumers do not care equally:

    * **Teardown — sound.** Teardown's goal is the absence of exposure, not the
      authorship of it. A target that is flat is flat regardless of who flattened it, and
      the ``still_pending`` gate above independently covers the one way a gone-but-
      unfilled order can still hold money (collateral parked in the OrderVault). There is
      no strand in this direction.
    * **Accounting — NOT exposed today, and the reason is structural.** A consumer that
      read ``SETTLED`` as "our order filled" and booked a fill from it would attribute a
      trade that never executed. Measured, no consumer does: this verdict is constructed
      with **no receipts**, and both call sites
      (``strategy_runner._await_async_settlement`` and
      ``teardown/runner_helpers.py``) gate enrichment on ``if barrier.receipts:``, so the
      baseline-free path appends nothing and enriches nothing. It only unblocks lifecycle
      success. Stated as a measurement rather than a reassurance, because it is the
      emptiness of ``receipts`` — not any check on attribution — that closes the hole.

    Deliberately NOT fixed here. The sound fix is to gate ``SETTLED`` on a measured
    ``OrderExecuted`` log via the reader already in ``perp_settlement.py``, which needs a
    submission block on ``AsyncOrderData`` — a shared execution shape, so it is a design
    change rather than a clause added to this function. Successive review rounds narrowed
    this verdict until it was nearly unreachable, which is the signal that the seam is
    wrong and not that one more condition is missing. VIB-6334 carries it.

    Until then, the load-bearing rule is: **never give this verdict receipts, and never
    key a fill-booking path on it.** The safety above is not a property of the verdict —
    it is a property of the empty ``receipts`` tuple. Attaching receipts here, or adding a
    consumer that books from the status alone, converts a rare mis-attribution into a
    money-correctness bug. ``test_baseline_free_settled_carries_no_receipts`` fails if the
    first half is ever violated.
    """
    intent_type = _intent_type_str(intent)
    if not _goal_is_baseline_free_decidable(intent_type):
        # Checked before the read, not after: a position read this verdict cannot use is
        # a wasted round-trip on every remaining poll of the barrier's budget.
        return _observation_failed(
            f"GMX order left the pending set before a position baseline could be measured; "
            f"intent={intent_type or 'unmeasured'} has no target that is decidable without one"
        )

    if still_pending:
        # A requested order is positively observed still live. It can fill, and until it
        # resolves it holds collateral in the OrderVault, so nothing here is terminal.
        return _observation_failed(
            "GMX order left the pending set with no baseline while a sibling order is "
            "still pending; the group cannot be judged until every requested order resolves"
        )

    positions = read_open_positions(gateway_client, chain, wallet_address)
    if not getattr(positions, "ok", False):
        # Empty is not zero: an unreadable account is UNMEASURED, never "measured flat".
        # Unlike the baseline, this read genuinely can succeed on a later poll.
        return _observation_failed("GMX position state was unmeasured with no settlement baseline")

    # ABSENCE MUST BE MEASURABLE BEFORE IT CAN MEAN "FLAT".
    #
    # The read requests a fixed window `getAccountPositions(dataStore, account, 0, 100)`,
    # so a full page may have been cut short. A requested position beyond the page would
    # then be missing from `current_sizes` and read as size 0 — manufacturing SETTLED
    # over live exposure.
    #
    # The baseline path is not exposed to this: its `before.get(target, 0) > 0` guard
    # turns a truncated read into TERMINAL_FAILED — loud. Without a baseline there is no
    # such proof, so the silent direction is reachable, and that asymmetry is introduced
    # here rather than inherited.
    #
    # `truncated` is computed at the REDUCER from the raw decoded array, because
    # `positions` is filtered to ACTIVE rows before `PerpsReadResult` is built and no
    # caller-side length test can recover completeness afterwards. An earlier revision of
    # this function tried exactly that (`len(positions) >= _MAX_POSITION_RANGE`) and it
    # could not work: a full page containing any inactive row yields fewer active rows
    # than the range and slips straight past. Both audit engines on #3533 called it, and
    # deferring it to a follow-up ticket was the wrong call under AGENTS.md §The Bar —
    # a SETTLED verdict that depends on a guard which cannot discriminate is not a
    # shippable safe design.
    if getattr(positions, "truncated", True):
        return _observation_failed(
            "GMX position read may be truncated (its page came back full), so the "
            "absence of a target position is not measurable and cannot mean 'closed'"
        )

    current_sizes = _active_position_sizes(positions)

    # KEY ABSENCE IS NOT CLOSURE.
    #
    # `current_sizes.get(target, 0) == 0` cannot tell "the target existed and went
    # to zero" from "the target never existed, we were reading the wrong key". The
    # baseline path had that evidence and used it — `_position_delta_reached`
    # requires `before.get(target, 0) > 0` before a close may settle — and deleting
    # the baseline deleted the only existence check with it (#3533 panel).
    #
    # The two keys come from DIFFERENT SOURCES and can legitimately disagree:
    #   requested: (market, initial_collateral_token, is_long) from the OrderCreated
    #              event (`receipt_parser`), where the collateral is whatever the
    #              close intent resolved — and `full_close.py` falls back to
    #              `details["asset"]` when `collateral_token` is absent.
    #   measured:  (market, collateralToken, is_long) from `getAccountPositions`.
    # USDC vs USDC.e on Arbitrum is the obvious trigger; the `asset` fallback is the
    # general one. Either way the decrease order names a GMX position key that does
    # not exist, the keeper cancels it, and the keyed lookup reads 0.
    #
    # On `main` this same mistake fails LOUD (no baseline ⇒ the 360s burn; with one
    # ⇒ TERMINAL_FAILED). Settling on key absence would convert that into a SILENT
    # STRAND — the forbidden direction, introduced here.
    #
    # So settlement additionally requires that NO active position exists at the
    # requested (market, is_long) under ANY collateral token. That closes the
    # collateral-aliasing hole without demanding a wallet-wide zero, which would
    # break every multi-market strategy.
    #
    # THIS DELIBERATELY OVER-REFUSES IN ONE CASE, and it is not free. GMX's position
    # key is keccak(account, market, collateralToken, isLong), so `collateralToken`
    # is part of the identity: a wallet CAN legitimately hold two distinct positions
    # at the same (market, isLong) under different collateral. Closing one of them
    # leaves the other live at that (market, is_long), and this guard then refuses to
    # settle the close that actually succeeded.
    #
    # That is the correct trade. The ambiguity is real and unresolvable here — an
    # active position at the requested market/side under a different collateral is
    # EITHER our own position seen through a mis-resolved collateral (the aliasing
    # bug: settling would strand it) OR a genuinely separate position (refusing costs
    # a wait). Nothing in this branch can tell them apart, and the two errors are not
    # symmetric: settling wrongly is a silent strand, refusing wrongly is loud.
    #
    # And the cost is bounded to "no improvement", not "regression": refusing returns
    # the same non-terminal OBSERVATION_FAILED that `main` returns for this ENTIRE
    # branch today, so that case degrades exactly to current behaviour — the barrier
    # spends its budget and reports the close resumable. Nothing that works today
    # stops working; the multi-collateral case simply does not get the speed-up.
    #
    # If it ever needs to get it, the fix is a real baseline (VIB-6299), not a looser
    # predicate here.
    live_market_sides = {
        (market, is_long) for (market, _collateral, is_long), size in current_sizes.items() if size > 0
    }
    still_live = [
        (market, is_long)
        for (market, _collateral, is_long) in requested_deltas
        if (market, is_long) in live_market_sides
    ]
    if still_live:
        return _observation_failed(
            "GMX order left the pending set with no baseline and an active position "
            f"remains at the requested market/side {sorted(still_live)} (under some "
            f"collateral token); measured={_describe_position_sizes(current_sizes)}. "
            "Key absence is not closure — refusing to settle."
        )

    if all(current_sizes.get(target, 0) == 0 for target in requested_deltas):
        return AsyncSettlementVerdict(
            status=AsyncSettlementStatus.SETTLED,
            terminal=True,
            orders=_order_verdict_rows(requested_keys, AsyncSettlementStatus.SETTLED),
            reason=None,
        )

    # c > 0 for some requested target. With a baseline this would be decidable via
    # `c <= max(0, b - d)`; without one, a correctly-filled partial close and a cancelled
    # order look identical. Refuse to assert either.
    return _observation_failed(
        "GMX order left the pending set with no baseline and its target position is not "
        f"flat (intent={intent_type}, requested={_describe_position_sizes(requested_deltas)}, "
        f"measured={_describe_position_sizes(current_sizes)}, baseline=unmeasured); a partial "
        "fill and a cancellation are indistinguishable without a baseline"
    )


def _final_position_verdict(
    *,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    requested_keys: set[str],
    requested_deltas: dict[_PositionKey, int],
    intent: Any,
    baseline: _GmxSettlementBaseline,
) -> AsyncSettlementVerdict:
    positions = read_open_positions(gateway_client, chain, wallet_address)
    if not getattr(positions, "ok", False):
        return _observation_failed(
            "GMX position read was unmeasured after the order left the pending set",
            baseline,
        )

    intent_type = _intent_type_str(intent)
    current_sizes = _active_position_sizes(positions)
    target_reached = _position_delta_reached(
        intent_type,
        requested_deltas,
        baseline,
        current_sizes,
    )
    status = AsyncSettlementStatus.SETTLED if target_reached else AsyncSettlementStatus.TERMINAL_FAILED
    return AsyncSettlementVerdict(
        status=status,
        terminal=True,
        orders=_order_verdict_rows(requested_keys, status),
        reason=None
        if target_reached
        else (
            "GMX order left the pending set without its exact target position delta "
            f"(intent={intent_type or 'unmeasured'}, requested={_describe_position_sizes(requested_deltas)}, "
            f"baseline={_describe_position_sizes(baseline.as_dict())}, "
            f"measured={_describe_position_sizes(current_sizes)})"
        ),
        observation_state=baseline,
    )


class GmxV2RunnerHookConnector(
    RunnerHookConnector,
    RunnerAsyncSettlementCapability,
    RunnerPerpSettlementCapability,
):
    """Observe GMX keeper settlement and advance its cancel gate on Anvil.

    Implements two distinct capabilities:

    - :class:`RunnerAsyncSettlementCapability` — the strat-test lifecycle barrier
      (ALM-2972): a blocking observe/execute loop used ONLY in the managed-Anvil
      test lane. Unchanged.
    - :class:`RunnerPerpSettlementCapability` (VIB-3872 WI-2) — the non-blocking,
      multi-handle, accounting-facing settlement reconciler seam: given a set of
      watched order keys it correlates each to its keeper receipt and returns a
      typed :class:`PerpSettlementVerdict` (carrying WI-1 ``PerpFillData``). WI-3
      drives this per tick.
    """

    protocol: ClassVar[ProtocolName] = _PROTOCOL
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def perp_settlement_policy(self) -> AsyncSettlementPolicy:
        """Connector-owned watch horizon for the non-blocking settlement reconciler.

        Reuses the ``AsyncSettlementPolicy`` shape (design D1). The horizon is the
        generous upper bound past which an un-settled order books ``UNMEASURED``
        (never a fabricated ``CANCELLED`` — ratified Q2); GMX keeper settlement is
        normally seconds.
        """
        return AsyncSettlementPolicy(
            timeout_seconds=PERP_SETTLEMENT_TIMEOUT_SECONDS,
            poll_interval_seconds=PERP_SETTLEMENT_POLL_INTERVAL_SECONDS,
            supports_local_order_execution=True,
            supports_cancellation=True,
            submission_intent_types=frozenset({"PERP_OPEN", "PERP_CLOSE"}),
        )

    def resolve_perp_settlements(
        self,
        *,
        gateway_client: Any,
        chain: str,
        wallet_address: str,
        watch_entries: tuple[PerpSettlementWatchEntry, ...],
    ) -> tuple[PerpSettlementVerdict, ...]:
        """Correlate each watched order key to its keeper settlement (VIB-3872 WI-2).

        Delegates to :mod:`almanak.connectors.gmx_v2.perp_settlement`; all reads go
        through the strategy's own gateway. Fail-closed: unmeasured reads yield
        ``UNMEASURED`` verdicts, never fabricated outcomes.
        """
        return resolve_perp_settlements(
            gateway_client=gateway_client,
            chain=chain,
            wallet_address=wallet_address,
            watch_entries=watch_entries,
            timeout_seconds=self.perp_settlement_policy().timeout_seconds,
        )

    def async_settlement_policy(self) -> AsyncSettlementPolicy:
        """Return the lifecycle test policy declared by ALM-2972."""
        return AsyncSettlementPolicy(
            timeout_seconds=360,
            poll_interval_seconds=5,
            supports_local_order_execution=True,
            supports_cancellation=True,
            submission_intent_types=frozenset({"PERP_OPEN", "PERP_CLOSE"}),
        )

    def observe_async_orders(
        self,
        *,
        gateway_client: Any,
        chain: str,
        wallet_address: str,
        orders: tuple[Any, ...],
        intent: Any,
        observation_state: Any = None,
    ) -> AsyncSettlementVerdict:
        """Measure whether the submitted order reached the intent's target state."""
        requested_keys = {str(getattr(order, "order_id", "") or "").lower() for order in orders}
        requested_deltas = _requested_position_deltas(orders)
        if not requested_keys or "" in requested_keys or requested_deltas is None:
            return _observation_failed(
                "GMX order target identity or size delta was unmeasured",
                observation_state,
            )

        pending = read_pending_orders(gateway_client, chain, wallet_address)
        if not pending.ok:
            return _observation_failed(pending.error or "GMX pending-order read was unmeasured", observation_state)

        still_pending = requested_keys.intersection(_pending_order_keys(pending))
        if pending.truncated and len(still_pending) != len(requested_keys):
            return _observation_failed(
                "GMX pending-order set was truncated; order absence was not measurable",
                observation_state,
            )

        if observation_state is None:
            if len(still_pending) == len(requested_keys):
                return _capture_settlement_baseline(
                    gateway_client=gateway_client,
                    chain=chain,
                    wallet_address=wallet_address,
                    requested_keys=requested_keys,
                )
            # At least one order is already gone and no baseline exists. It can never
            # re-enter the pending set, so no later poll can capture one — decide now
            # from the absolute goal state instead of polling to the deadline.
            return _baseline_free_verdict(
                gateway_client=gateway_client,
                chain=chain,
                wallet_address=wallet_address,
                requested_keys=requested_keys,
                requested_deltas=requested_deltas,
                intent=intent,
                still_pending=still_pending,
            )

        if not isinstance(observation_state, _GmxSettlementBaseline):
            return _observation_failed("GMX settlement baseline had an invalid connector-private shape")
        if still_pending:
            return _pending_verdict(
                requested_keys,
                reason="GMX order remains in the account pending-order set",
                observation_state=observation_state,
            )

        return _final_position_verdict(
            gateway_client=gateway_client,
            chain=chain,
            wallet_address=wallet_address,
            requested_keys=requested_keys,
            requested_deltas=requested_deltas,
            intent=intent,
            baseline=observation_state,
        )

    def execute_pending_orders_for_test(
        self,
        *,
        gateway_client: Any,
        chain: str,
        wallet_address: str,
        orders: tuple[Any, ...],
        intent: Any,
        network: str,
    ) -> AsyncSettlementVerdict:
        """Execute exact pending GMX orders in the current managed Anvil fork."""
        baseline = self.observe_async_orders(
            gateway_client=gateway_client,
            chain=chain,
            wallet_address=wallet_address,
            orders=orders,
            intent=intent,
        )
        if baseline.status is not AsyncSettlementStatus.PENDING or baseline.observation_state is None:
            return baseline

        result = execute_pending_orders_on_anvil(
            gateway_client=gateway_client,
            chain=chain,
            wallet_address=wallet_address,
            orders=orders,
            network=network,
        )
        if not result.ok:
            # Three failure classes, not two (VIB-6438):
            #
            # * transient cold-fork/upstream blip — retryable within the
            #   barrier's budget (OBSERVATION_FAILED, non-terminal);
            # * the keeper transaction was MINED and REVERTED — the venue
            #   rejected this one order at this one block (ORDER_REJECTED);
            # * everything else is genuinely structural — no keeper role, wrong
            #   network, unreadable dependencies (INFRASTRUCTURE_UNSUPPORTED).
            #
            # The last two are retry-identical: the barrier stops immediately on
            # both and neither resubmits. The split is about the OPERATOR, who
            # was previously told their infrastructure was unsupported when the
            # true message was "this order reverted, here is why" — the reason
            # now rides on ``result.reason`` from the executor's replay/trace.
            status = _classify_execution_failure(result)
            return AsyncSettlementVerdict(
                status=status,
                terminal=False,
                reason=result.reason or "GMX managed-Anvil order execution was unavailable",
                observation_state=baseline.observation_state,
            )
        verdict = self.observe_async_orders(
            gateway_client=gateway_client,
            chain=chain,
            wallet_address=wallet_address,
            orders=orders,
            intent=intent,
            observation_state=baseline.observation_state,
        )
        # Thread the keeper receipts back for BOTH open and close (VIB-3872 WI-2):
        # PERP_OPEN's keeper receipt carries the PositionIncrease + fees the
        # settlement reconciler decodes into fill economics, so the strat-test lane
        # must not drop it (it previously threaded CLOSE only).
        intent_type = _intent_type_str(intent)
        receipts = result.execution_receipts if intent_type in ("PERP_OPEN", "PERP_CLOSE") else ()
        return replace(verdict, receipts=receipts)

    def prepare_pending_orders_for_teardown(
        self,
        *,
        gateway_client: Any,
        chain: str,
        wallet_address: str,
        residuals: tuple[Any, ...],
        network: str,
    ) -> bool:
        """Advance the current managed Anvil session to measured cancel eligibility."""
        del wallet_address
        if str(network or "").lower() != "anvil" or gateway_client is None:
            return False

        waits = [(getattr(residual, "details", None) or {}).get("seconds_until_cancellable") for residual in residuals]
        seconds = max((wait for wait in waits if isinstance(wait, int) and wait > 0), default=None)
        if seconds is None:
            return False

        provider = GatewayWeb3Provider(gateway_client, chain=chain)
        advance = provider.make_request(RPCEndpoint("evm_increaseTime"), [seconds])
        if advance.get("error"):
            logger.warning("GMX teardown could not advance Anvil time: %s", advance["error"])
            return False
        mined = provider.make_request(RPCEndpoint("evm_mine"), [])
        if mined.get("error"):
            logger.warning("GMX teardown could not mine after advancing Anvil time: %s", mined["error"])
            return False
        logger.info("GMX teardown advanced the managed Anvil session by %ds to cancel eligibility", seconds)
        return True


__all__ = ["GmxV2RunnerHookConnector"]
