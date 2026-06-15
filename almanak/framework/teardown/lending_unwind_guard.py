"""Universal fresh-state guard for lending teardown intents (VIB-5139).

Strategies hand-roll their lending teardown intents (REPAY + WITHDRAW). When
those intents are built from STALE cached exposure, the unwind emits invalid
on-chain actions:

* a ``REPAY 0`` (zero measured debt) — reverts gas estimation,
* a ``WITHDRAW all`` / ``withdraw_all`` while debt is still open or collateral is
  already flat — reverts the protocol's LLTV check
  (e.g. Aave ``0xd27b44a9`` HealthFactorLowerThanLiquidationThreshold),
* a collateral withdraw ORDERED before the debt repay.

Each of those fails simulation / reverts on-chain and strands the position in
``STRATEGY_ERROR`` / "Paused awaiting approval".

``generate_leverage_loop_teardown`` already does the correct fresh-state unwind,
but it is opt-in. This guard makes the fresh-exposure discipline the DEFAULT for
*every* lending teardown by sanitising the strategy-emitted intent list before it
is dispatched — in the runner lane, the CLI lane, and the inline fallback lane
(all three derive their intents from ``generate_teardown_intents``).

The guard is a **pure transformation on the intent list**: it never executes,
signs, or commits. The sanitised list flows through the same ``_execute_intents``
funnel, so the per-intent ``runner_helpers.commit`` pairing and the VIB-3773
anti-bypass guards are untouched.

Correctness contract (CLAUDE.md §Accounting — Empty ≠ Zero):

* A FRESH on-chain exposure read (``market.position_health``) yields a MEASURED
  collateral / debt value, or it fails — in which case the value is ``None``
  (unmeasured), NEVER ``Decimal("0")``.
* We only DROP an intent on a MEASURED zero. A ``None`` read is the stale-state
  bug itself; under ``None`` we degrade conservatively — keep risk-reducing
  REPAY, never emit a withdraw-all that we cannot confirm is safe.

Ordering contract — DO NOT mangle an already-correct staircase (VIB-5139 P0):

  ``generate_leverage_loop_teardown`` emits an ORDER-SENSITIVE *interleaved*
  staircase (blueprint 14 §"Leveraged-loop teardown"):
  ``WITHDRAW(slice) → SWAP(collat→borrow) → REPAY → … → WITHDRAW(all) → SWAP``.
  Globally rebuilding the list as ``[*repays, *withdraws, …]`` would push a REPAY
  of the borrow token to the FRONT — but the wallet holds no borrow token until a
  withdraw+swap runs first — re-introducing the exact "Insufficient token" revert
  this guard exists to prevent. So the guard NEVER globally reorders. It detects
  interleaving and chooses:

  * **Order-locked** (interleaved staircase): a passthrough intent sits BETWEEN
    two lending-unwind intents, OR there is more than one repay/withdraw round.
    → measured-zero drops applied IN PLACE; original relative order preserved.
  * **Simple single-round** (the hand-rolled ``REPAY 0`` / ``WITHDRAW all``
    strategies this ticket targets): all passthrough intents are entirely before
    OR entirely after one contiguous lending block, single round.
    → measured-zero drops AND the repay-before-withdraw reorder.

  Measured-zero drops are position-local and order-independent, so they apply in
  BOTH cases.

Exposure read is CHAIN-SCOPED (VIB-5139 P1): ``MarketSnapshot.position_health``
is pinned to the snapshot's primary chain. For a lending intent on a different
chain the read is forced unmeasured (``None``) — never trust an exposure read
from an unrelated chain to drop a live intent.

Limitation (honest): ``position_health`` is an ACCOUNT-LEVEL aggregate (summed
collateral / summed debt across reserves). A per-reserve stale ``withdraw_all``
(one reserve already emptied while another reserve is still live) is NOT caught —
only the all-reserves-flat case registers as measured zero. Per-reserve
validation is out of scope for this ticket.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents.vocabulary import IntentType

if TYPE_CHECKING:  # pragma: no cover
    pass

# Exposure (collateral or debt) below this USD value is treated as measured-flat.
# Deliberately matches ``leverage_loop._DUST_USD`` ($0.01) — both answer the same
# question ("is this leg effectively cleared on-chain?"), so the guard's drop
# threshold must agree with the staircase's "debt cleared" threshold. This is a
# DIFFERENT question from the $5 token-consolidation dust floor
# (``TokenConsolidationConfig.min_swap_value_usd``), which is "is a residual swap
# worth the gas?" — a much higher, economic threshold. Do not unify the two.
_DUST_USD = Decimal("0.01")

# Intent types this guard reasons about. SUPPLY/BORROW never appear in a teardown
# (they ADD exposure); the guard leaves any non-lending or non-unwind intent
# untouched.
_REPAY_TYPES = (IntentType.REPAY, IntentType.DELEVERAGE)
_WITHDRAW_TYPE = IntentType.WITHDRAW


@dataclass
class LendingGuardResult:
    """Outcome of sanitising one teardown intent list.

    Attributes:
        intents: The sanitised, repay-first-ordered intent list to dispatch.
        dropped: Human-readable reasons for every intent the guard removed.
        degraded: True when at least one position's fresh exposure read failed
            (unmeasured) and the guard had to make a conservative call.
        no_op_positions: Position keys found fully flat (no debt, no collateral)
            on the fresh read — their intents were all dropped.
    """

    intents: list[Any]
    dropped: list[str] = field(default_factory=list)
    degraded: bool = False
    no_op_positions: list[str] = field(default_factory=list)


@dataclass
class _Exposure:
    """Fresh exposure for one lending position. ``None`` == unmeasured."""

    collateral_usd: Decimal | None
    debt_usd: Decimal | None

    @property
    def measured(self) -> bool:
        return self.collateral_usd is not None and self.debt_usd is not None

    @property
    def debt_is_zero(self) -> bool:
        """True only on a MEASURED zero debt (Empty ≠ Zero)."""
        return self.debt_usd is not None and self.debt_usd <= _DUST_USD

    @property
    def collateral_is_zero(self) -> bool:
        """True only on a MEASURED zero collateral (Empty ≠ Zero)."""
        return self.collateral_usd is not None and self.collateral_usd <= _DUST_USD


def _is_lending_protocol(protocol: str | None) -> bool:
    """Whether ``position_health`` can read this protocol's exposure.

    Mirrors the exact dispatch in ``PositionHealthProvider.get_health`` (a market-
    health reader OR an account-state spec), NOT ``LendingReadRegistry.has`` — which
    only covers ``spec_loaders`` (aave_v3 / spark) and would silently exclude
    Morpho Blue and Compound V3, the very protocols this ticket targets
    (aave-looping + a bsc deployment). A protocol the health reader cannot serve
    is left untouched (no false drops).
    """
    if not protocol:
        return False
    try:
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        canonical = LendingReadRegistry.normalize_protocol(protocol)
        if not canonical:
            return False
        return LendingReadRegistry.market_health_reader(
            canonical
        ) is not None or LendingReadRegistry.supports_account_state(canonical)
    except Exception:
        return False


def _normalize_protocol(protocol: Any) -> str:
    """Canonicalise a loosely-spelled lending protocol for grouping + reads.

    Routes through ``LendingReadRegistry.normalize_protocol`` (folds case /
    whitespace / hyphens and applies manifest aliases — ``morpho`` → ``morpho_blue``,
    ``comet`` → ``compound_v3``) so ``Aave_V3`` / ``aave`` / ``aave_v3`` group as one
    position and a position read with the same alias hits the same key. Falls back
    to ``.strip().lower()`` only if the registry is unavailable.
    """
    if not isinstance(protocol, str):
        return ""
    try:
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        return LendingReadRegistry.normalize_protocol(protocol)
    except Exception:
        return protocol.strip().lower()


def _intent_type(intent: Any) -> IntentType | None:
    return getattr(intent, "intent_type", None)


def _is_repay(intent: Any) -> bool:
    return _intent_type(intent) in _REPAY_TYPES


def _is_withdraw(intent: Any) -> bool:
    return _intent_type(intent) == _WITHDRAW_TYPE


def _is_lending_unwind(intent: Any) -> bool:
    """A lending REPAY / DELEVERAGE / WITHDRAW on a known lending protocol."""
    return (_is_repay(intent) or _is_withdraw(intent)) and _is_lending_protocol(getattr(intent, "protocol", None))


def _position_key(intent: Any) -> tuple[str, str, str]:
    """Group lending intents by ``(canonical_protocol, chain, market_id)``.

    ``market_id`` stays the raw string ("" for Aave where the protocol treats it
    as informational — one pool per chain; non-empty and required for Morpho /
    Compound isolated markets), so the key type stays ``tuple[str, str, str]``.
    """
    protocol = _normalize_protocol(getattr(intent, "protocol", ""))
    chain = (getattr(intent, "chain", "") or "").lower()
    market_id = getattr(intent, "market_id", "") or ""
    return (protocol, chain, market_id)


def _safe_decimal(value: Any) -> Decimal | None:
    """Coerce a measured value to Decimal; ``None`` stays unmeasured."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _intent_chain_matches_snapshot(market: Any, intent: Any) -> bool:
    """Whether the intent's chain provably matches the snapshot's pinned chain.

    ``position_health`` is pinned to ``MarketSnapshot.chain`` (the primary chain);
    it takes no ``chain=`` argument. For a multi-chain teardown a lending intent on
    a non-primary chain would otherwise read the PRIMARY chain's exposure and could
    drop a live intent based on unrelated data (VIB-5139 P1). We only trust the read
    when the chains provably agree. An intent with no explicit chain inherits the
    snapshot's chain by convention, so it matches.
    """
    intent_chain = (getattr(intent, "chain", None) or "").lower()
    if not intent_chain:
        return True  # no explicit chain → executes on the snapshot's pinned chain
    snapshot_chain = (getattr(market, "chain", None) or "").lower()
    if not snapshot_chain:
        return False  # cannot confirm the snapshot's chain → do not trust the read
    return intent_chain == snapshot_chain


def _read_exposure(market: Any, intent: Any) -> _Exposure:
    """Fresh on-chain exposure for the position ``intent`` targets.

    Empty ≠ Zero: any failure leaves the value ``None`` (unmeasured), never a
    fabricated ``Decimal("0")``. The read is routed through the gateway-backed
    ``MarketSnapshot.position_health`` — no direct network access.

    Chain-scoped (P1): when the intent's chain differs from the snapshot's pinned
    chain (or cannot be confirmed equal), force unmeasured — the read would be for
    the wrong chain and must never drive a drop.
    """
    if market is None or not hasattr(market, "position_health"):
        return _Exposure(collateral_usd=None, debt_usd=None)
    if not _intent_chain_matches_snapshot(market, intent):
        return _Exposure(collateral_usd=None, debt_usd=None)

    protocol = _normalize_protocol(getattr(intent, "protocol", ""))
    market_id = getattr(intent, "market_id", "") or ""
    try:
        health = market.position_health(protocol=protocol, market_id=market_id)
    except Exception:
        return _Exposure(collateral_usd=None, debt_usd=None)

    collateral = _safe_decimal(getattr(health, "collateral_value_usd", None))
    debt = _safe_decimal(getattr(health, "debt_value_usd", None))
    return _Exposure(collateral_usd=collateral, debt_usd=debt)


def _exposures_by_position(market: Any, intents: list[Any]) -> dict[tuple[str, str, str], _Exposure]:
    """One fresh exposure read per distinct lending position in ``intents``."""
    exposures: dict[tuple[str, str, str], _Exposure] = {}
    for intent in intents:
        if not _is_lending_unwind(intent):
            continue
        key = _position_key(intent)
        if key not in exposures:
            exposures[key] = _read_exposure(market, intent)
    return exposures


def _keep_repay(exposure: _Exposure, key: tuple[str, str, str], result: LendingGuardResult) -> bool:
    """Decide whether to KEEP a REPAY intent. Never keeps a measured-zero repay."""
    if exposure.debt_is_zero:
        result.dropped.append(f"REPAY {key}: measured zero debt (no stale REPAY 0)")
        return False
    # Measured nonzero debt OR unmeasured debt → keep the repay (risk-reducing).
    return True


def _keep_withdraw(
    exposure: _Exposure,
    key: tuple[str, str, str],
    has_repay: bool,
    result: LendingGuardResult,
) -> bool:
    """Decide whether to KEEP a WITHDRAW intent (repay-first / safe ordering).

    ``has_repay`` is per-position over the WHOLE plan (a repay anywhere for this
    position ⇒ True), so an interleaved staircase — whose first WITHDRAW precedes
    its repays in execution order but whose plan DOES contain repays — is never
    dropped by the active-debt / unmeasured guards below.
    """
    if exposure.collateral_is_zero:
        result.dropped.append(f"WITHDRAW {key}: measured zero collateral (nothing to withdraw)")
        return False
    if exposure.measured and not exposure.debt_is_zero and not has_repay:
        # MEASURED active debt and no repay-first to clear it — withdrawing
        # collateral now trips the protocol's LLTV check (Aave
        # HealthFactorLowerThanLiquidationThreshold, the ALM-2811 failure).
        result.dropped.append(f"WITHDRAW {key}: active debt and no repay-first — refusing unsafe withdraw")
        return False
    if not exposure.measured and not has_repay:
        # Unmeasured exposure and no repay-first to clear debt before the
        # withdraw runs — refusing a withdraw_all from stale assumptions.
        result.dropped.append(f"WITHDRAW {key}: unmeasured exposure and no repay-first — refusing unsafe withdraw_all")
        return False
    return True


def _decide_keeps(
    intents: list[Any],
    exposures: dict[tuple[str, str, str], _Exposure],
    positions_with_repay: set[tuple[str, str, str]],
    result: LendingGuardResult,
) -> list[Any]:
    """Apply the position-local keep/drop gates IN ORDER.

    Returns the kept intents in their ORIGINAL relative order (no reorder here).
    Drops are position-local and order-independent, so this is correct for both
    interleaved staircases and simple single-round plans. ``result.dropped`` /
    ``result.degraded`` are populated as a side effect.
    """
    kept: list[Any] = []
    for intent in intents:
        if not _is_lending_unwind(intent):
            kept.append(intent)
            continue
        key = _position_key(intent)
        exposure = exposures[key]
        if not exposure.measured:
            result.degraded = True
        if _is_repay(intent):
            if _keep_repay(exposure, key, result):
                kept.append(intent)
        elif _keep_withdraw(exposure, key, key in positions_with_repay, result):
            kept.append(intent)
    return kept


def _is_order_locked(intents: list[Any]) -> bool:
    """Whether the lending plan is an ORDER-SENSITIVE interleaved staircase.

    Order-locked when EITHER:
      (a) a passthrough (non-lending-unwind) intent sits BETWEEN two lending-unwind
          intents — the SWAPs interleaved in ``generate_leverage_loop_teardown``'s
          ``WITHDRAW → SWAP → REPAY`` staircase; OR
      (b) there is more than one repay/withdraw round — i.e. the lending block holds
          more than one REPAY or more than one WITHDRAW (a multi-round staircase).

    A staircase must NOT be globally reordered (P0): reordering would front-load a
    REPAY before the WITHDRAW+SWAP that funds it. Operates on the ORIGINAL list so
    the structure is read before any drop is applied.
    """
    lending_indices = [idx for idx, i in enumerate(intents) if _is_lending_unwind(i)]
    if len(lending_indices) <= 1:
        return False
    # (a) a passthrough intent interleaved inside the lending span.
    span = range(lending_indices[0], lending_indices[-1] + 1)
    if any(not _is_lending_unwind(intents[idx]) for idx in span):
        return True
    # (b) more than one repay or more than one withdraw round.
    repay_count = sum(1 for idx in lending_indices if _is_repay(intents[idx]))
    withdraw_count = sum(1 for idx in lending_indices if _is_withdraw(intents[idx]))
    return repay_count > 1 or withdraw_count > 1


def _reorder_simple(kept: list[Any]) -> list[Any]:
    """Repay-before-withdraw reorder for a SIMPLE single-round plan.

    Only called when the plan is NOT order-locked: all passthrough intents are
    entirely before OR entirely after one contiguous lending block, single round.
    Passthrough intents keep their position relative to the lending block; the
    lending intents are emitted repay(s) then withdraw(s).
    """
    first_lending_index = next((idx for idx, i in enumerate(kept) if _is_lending_unwind(i)), None)
    if first_lending_index is None:
        return list(kept)
    before: list[Any] = []
    repays: list[Any] = []
    withdraws: list[Any] = []
    after: list[Any] = []
    for idx, intent in enumerate(kept):
        if _is_lending_unwind(intent):
            (repays if _is_repay(intent) else withdraws).append(intent)
        elif idx < first_lending_index:
            before.append(intent)
        else:
            after.append(intent)
    return [*before, *repays, *withdraws, *after]


def sanitize_lending_teardown_intents(intents: list[Any], market: Any) -> LendingGuardResult:
    """Sanitise strategy-emitted lending teardown intents against fresh state.

    Args:
        intents: The ordered intent list from ``generate_teardown_intents``.
        market: A ``MarketSnapshot`` (gateway-backed) exposing ``position_health``.
            May be ``None`` (no market available) — every lending position then
            reads as unmeasured and the guard degrades conservatively.

    Returns:
        A :class:`LendingGuardResult` whose ``intents`` is the fresh-state-validated
        list to dispatch. An interleaved leveraged-loop staircase keeps its EXACT
        original order (drops applied in place); a simple single-round hand-rolled
        plan gets the repay-before-withdraw reorder. Non-lending intents pass
        through unchanged and keep their relative order.
    """
    result = LendingGuardResult(intents=list(intents))
    if not intents:
        return result

    exposures = _exposures_by_position(market, intents)
    if not exposures:
        # No lending unwind intents — nothing to guard.
        return result

    positions_with_repay = {_position_key(i) for i in intents if _is_repay(i) and _is_lending_unwind(i)}

    # Read the plan's structure from the ORIGINAL list before any drop, then apply
    # position-local drops in place. An interleaved staircase keeps its exact order;
    # only a simple single-round plan is reordered repay-first.
    order_locked = _is_order_locked(intents)
    kept = _decide_keeps(intents, exposures, positions_with_repay, result)

    # No-op positions: fully flat on the fresh read (measured zero debt AND
    # collateral). Their intents were already dropped above — record for the caller.
    result.no_op_positions = [
        f"{p}/{c}/{m}" for (p, c, m), e in exposures.items() if e.debt_is_zero and e.collateral_is_zero
    ]

    result.intents = list(kept) if order_locked else _reorder_simple(kept)
    return result
