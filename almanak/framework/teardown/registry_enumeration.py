"""Registry-backed open-position enumeration for teardown — VIB-5459 / TD-01.

Routes teardown's "what LP positions are open?" question through the
``position_registry`` WARM tier (SQLite local / Postgres hosted) — the single
durable, restart-safe source of truth — for the two cut-over LP primitives
(the UniV3 LP family, ``primitive='lp'``, plus UniV4 LP, ``primitive='lp_v4'``).
The registry becomes the WARM read path for those two primitives' teardown
ENUMERATION: a restarted runner re-derives the open set from the durable
registry instead of relying solely on the strategy's in-memory ``_position_id``,
the ``position_events`` history, or the ``LPPositionTracker`` shadow (the
"single WARM read path" of the Teardown roadmap §0.1 / blueprint 28 §5 cutover).

Scope (deliberately narrow — this is the foundation read-path cutover):

* **READ PATH ONLY.** This module reads ``position_registry status='open'`` and
  reconciles the strategy's reported ``TeardownPositionSummary`` against it. It
  does NOT synthesize closing intents — the registry payload carries no
  ``protocol`` slug (only ``token_id`` / ``pool_address`` / ``pool_id`` /
  ticks / liquidity), so close-intent derivation stays with the strategy's
  ``generate_teardown_intents`` plus the existing registry-first ``position_id``
  injection in :meth:`LPPositionTracker.maybe_inject`. It also does NOT scan the
  wallet — that is Plan B (``teardown ... --discover``), a separate lane.
* **UniV3 + UniV4 LP only.** GMX perp, Pendle LP, and Aave lending are NOT cut
  over (separate tickets TD-02/03/04); their enumeration is untouched. A
  strategy-reported LP on a non-cut-over venue (e.g. TraderJoe V2 Liquidity Book
  bins) is also left to the strategy, so this change can never strand it.

Durability (blueprint 06 §multi-tier, blueprint 28 §4): ``position_registry``
rows are written atomically with the ``transaction_ledger`` row at LP_OPEN
receipt-confirmation time (``save_ledger_and_registry``) under
``PRAGMA synchronous=FULL`` inside ``BEGIN IMMEDIATE … COMMIT``. The row
therefore survives crash AND reboot, so a restarted runner re-derives the
identical open set from WARM even when every byte of in-memory state was wiped.

Fund-safety (blueprint 20 §1 Gateway : 1 Strategy): registry rows are keyed by
``deployment_id`` and one gateway serves exactly one strategy, so reading this
deployment's OPEN rows can never surface a sibling deployment's position. No
ownership scan is required (contrast the wallet-wide on-chain discovery in
:mod:`almanak.framework.teardown.lp_recovery`, which exists precisely because
the on-chain scan is wallet-scoped, not deployment-scoped).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RegistryReadResult:
    """Outcome of a ``position_registry`` WARM read for the cut-over LP primitives.

    Richer than the legacy ``(positions, available)`` tuple so the caller can
    tell a *partial* read (some primitive's SQL read raised transiently) apart
    from a clean read — the distinction TD-05 (VIB-5463) needs to stop the
    registry-read failure path being warn-only.

    Attributes:
        positions: The OPEN LP positions the registry could read.
        available: ``True`` iff at least one primitive's read returned (an
            answerable registry). ``False`` ⇒ no backend / hosted pre-T19 ⇒ the
            caller keeps the legacy enumeration unchanged.
        failed_primitives: Primitives whose read RAISED a non-cutover error
            (transient gateway / decode fault). Non-empty ⇒ the registry answer
            is **incomplete** for those primitives, so a chain-verify of the
            known set must run before the enumeration is trusted.
    """

    positions: list[PositionInfo] = field(default_factory=list)
    available: bool = False
    failed_primitives: tuple[str, ...] = ()


# (primitive, accounting_category) for the two cut-over LP primitives. Mirrors
# ``ACTIVE_CUTOVERS`` in ``almanak/framework/runner/cutover.py`` (UniV3 LP +
# UniV4 LP). We deliberately do NOT hardcode a protocol slug here: the registry
# payload does not carry the specific slug (uniswap_v3 / sushiswap_v3 /
# slipstream all share ``primitive='lp'``), and the enumerated ``PositionInfo``
# label is informational only — the actual closing intent is the strategy's own
# (with its true protocol) and the position_id is registry-resolved. The
# ``primitive`` value is used as the label, which keeps framework code free of
# protocol-name coupling (blueprint 22 / coupling ratchet).
_LP_REGISTRY_SPECS: tuple[tuple[str, str], ...] = (
    ("lp", "lp"),
    ("lp_v4", "lp_v4"),
)


# (primitive, accounting_category) for the lending cutover (TD-04 / VIB-5462).
# Mirrors the lending ``CutoverSpec`` in ``almanak/framework/runner/cutover.py``
# (``Primitive.LENDING`` / 'lending'). Aave is canonical; the registry row shape
# (market_id + leg) is protocol-agnostic, so the SAME enumeration surfaces every
# lending protocol the cutover enables — no per-protocol code here.
_LENDING_REGISTRY_SPECS: tuple[tuple[str, str], ...] = (("lending", "lending"),)


# (primitive, accounting_category) for the perp cutover (TD-02 / VIB-5460).
# Mirrors the perp ``CutoverSpec`` in ``almanak/framework/runner/cutover.py``
# (``Primitive.PERP`` / 'perp'). GMX V2 is canonical; the registry row shape
# (venue position_key anchor + market/collateral/direction/size payload) is
# protocol-agnostic, so the SAME enumeration surfaces every perp protocol the
# cutover enables — no per-protocol code here.
_PERP_REGISTRY_SPECS: tuple[tuple[str, str], ...] = (("perp", "perp"),)


# A lending registry leg maps onto the teardown-lane risk-ordered position type:
# a supply (collateral) leg is withdrawn (SUPPLY), a borrow (debt) leg is repaid
# (BORROW). The teardown ``PositionType`` priorities already close BORROW before
# SUPPLY (repay frees collateral), so surfacing the legs separately is exactly
# what the HF-safe unwind (TD-09) needs.
_LENDING_LEG_TO_POSITION_TYPE: dict[str, PositionType] = {
    "collateral": PositionType.SUPPLY,
    "debt": PositionType.BORROW,
}


def _position_info_from_registry_row(row: Any, *, primitive: str) -> PositionInfo | None:
    """Build an LP :class:`PositionInfo` from one OPEN ``position_registry`` row.

    Returns ``None`` when the row carries no usable ``token_id`` (the identity
    anchor) — a registry row without it cannot be closed and must not be
    surfaced as an open position.

    The ``protocol`` field is labelled with the registry ``primitive`` (``lp`` /
    ``lp_v4``), the most specific thing the registry actually knows — the row
    carries no protocol slug, and the framework must not invent one. The label
    is cosmetic: registry-derived positions are added to the enumeration for
    visibility / counting, never used to build closing intents.

    USD value is left at ``Decimal("0")``: the registry is the identity surface,
    not a valuation surface (blueprint 28 §2 ownership matrix). Teardown does
    not need a USD figure to close a known position; PortfolioValuer owns
    valuation and is out of scope for this read-path cutover.
    """
    if not isinstance(row, dict):
        return None
    payload = row.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    token_id = payload.get("token_id")
    if token_id is None or token_id == "":
        return None
    chain = str(row.get("chain") or "").lower()
    # V3 stores ``pool_address``; V4 stores ``pool_id`` (the PoolKey hash — V4
    # pools have no per-pool contract address). Surface whichever is present.
    pool = payload.get("pool_address") or payload.get("pool_id")
    details: dict[str, Any] = {"source": "position_registry"}
    if pool:
        details["pool"] = str(pool)
    for key in ("tick_lower", "tick_upper", "liquidity", "fee_tier"):
        value = payload.get(key)
        if value is not None:
            details[key] = value
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=str(token_id),
        chain=chain,
        protocol=primitive,
        value_usd=Decimal("0"),
        details=details,
    )


async def read_open_lp_positions_detailed(
    *,
    state_manager: Any,
    deployment_id: str,
    chain: str | None = None,
) -> RegistryReadResult:
    """Read this deployment's OPEN UniV3 + UniV4 LP positions from WARM (detailed).

    The richer counterpart of :func:`read_open_lp_positions_from_registry`: it
    additionally reports which primitives' reads RAISED so the caller can chain-
    verify the known set instead of silently warning (TD-05 / VIB-5463).

    Args:
        state_manager: The registry-capable :class:`StateManager` (the runner's
            accounting state manager, or the strategy's gateway-backed one). May
            be ``None`` or lack the registry accessor on a backend that has not
            shipped cutover storage.
        deployment_id: The deployment whose rows to read.
        chain: Optional chain filter. ``None`` reads every chain for the
            deployment — which, under 1 gateway : 1 strategy, is exactly this
            strategy's positions.

    Returns:
        A :class:`RegistryReadResult`. ``available`` is ``False`` when the
        backend cannot answer a registry read (no state manager, missing
        accessor, or hosted pre-T19 → :class:`CutoverStorageNotSupported`); the
        caller then keeps the strategy's own enumeration unchanged. ``available``
        ``True`` with an empty list means "registry is authoritative and this
        deployment has zero open LP". ``failed_primitives`` names any primitive
        whose read RAISED a transient fault — the registry answer is incomplete
        for those.

    Never raises — enumeration must never fault the teardown lane.
    """
    from almanak.framework.migration import CutoverStorageNotSupported

    dep = str(deployment_id or "").strip()
    if state_manager is None or not dep or not hasattr(state_manager, "get_position_registry_open_rows"):
        return RegistryReadResult(positions=[], available=False, failed_primitives=())

    positions: list[PositionInfo] = []
    available = False
    failed: list[str] = []
    for primitive, accounting_category in _LP_REGISTRY_SPECS:
        try:
            rows = await state_manager.get_position_registry_open_rows(
                dep,
                chain=chain,
                primitive=primitive,
                accounting_category=accounting_category,
            )
        except (CutoverStorageNotSupported, NotImplementedError) as exc:
            # Backend without cutover storage (hosted pre-T19). Degrade to the
            # legacy enumeration — never treat "can't read" as "nothing open".
            logger.debug(
                "Teardown registry enumeration: %s read unavailable for %s (%s)",
                primitive,
                dep,
                exc,
            )
            continue
        except Exception:  # noqa: BLE001 — enumeration must never raise into teardown
            # A genuinely-failed registry read (transient gateway error, decode
            # fault) during teardown must be OBSERVABLE — this primitive then
            # falls back to the strategy's own enumeration, but on a wiped-state
            # restart it would be invisible. Surface it as a failed primitive so
            # the caller (TD-05) chain-verifies the known set rather than trusting
            # the strategy enumeration blindly (no longer warn-only).
            logger.warning(
                "Teardown registry enumeration: %s read FAILED for %s — registry "
                "answer is incomplete; the known LP set will be chain-verified",
                primitive,
                dep,
                exc_info=True,
            )
            failed.append(primitive)
            continue
        available = True
        for row in rows or []:
            info = _position_info_from_registry_row(row, primitive=primitive)
            if info is not None:
                positions.append(info)
    return RegistryReadResult(positions=positions, available=available, failed_primitives=tuple(failed))


async def read_open_lp_positions_from_registry(
    *,
    state_manager: Any,
    deployment_id: str,
    chain: str | None = None,
) -> tuple[list[PositionInfo], bool]:
    """Read this deployment's OPEN UniV3 + UniV4 LP positions from WARM.

    Back-compat 2-tuple facade over :func:`read_open_lp_positions_detailed`.

    Returns:
        ``(positions, available)``. ``available`` is ``False`` when the backend
        cannot answer a registry read; ``True`` with an empty list means the
        registry is authoritative and this deployment has zero open LP.

    Never raises — enumeration must never fault the teardown lane.
    """
    result = await read_open_lp_positions_detailed(
        state_manager=state_manager,
        deployment_id=deployment_id,
        chain=chain,
    )
    return result.positions, result.available


def _position_info_from_lending_registry_row(row: Any) -> PositionInfo | None:
    """Build a lending :class:`PositionInfo` from one OPEN ``position_registry`` row.

    Returns ``None`` when the row carries no usable ``market_id`` (the identity
    anchor) or an unknown ``leg`` — a registry row without a resolvable
    *(market, leg)* cannot be unwound and must not be surfaced.

    USD value is left at ``Decimal("0")``: the registry is the identity surface,
    not a valuation surface (blueprint 28 §2). The reserve symbol is carried in
    ``details["asset_symbol"]`` (NOT ``details["asset"]``) so it never trips the
    PortfolioValuer wallet-overlap special-casing reserved for TOKEN
    pseudo-positions — these are real protocol legs whose valuation TD-09 / the
    valuer owns, out of scope for this read-path cutover.
    """
    if not isinstance(row, dict):
        return None
    payload = row.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    market_id = payload.get("market_id")
    if market_id is None or market_id == "":
        return None
    leg = str(payload.get("leg") or "").strip().lower()
    position_type = _LENDING_LEG_TO_POSITION_TYPE.get(leg)
    if position_type is None:
        return None
    chain = str(row.get("chain") or "").lower()
    # The protocol slug IS carried in the lending payload (unlike LP, whose
    # payload carries no slug). Prefer it so teardown / TD-09 can route the
    # closing intent to the right connector; fall back to the registry primitive.
    protocol = str(payload.get("protocol") or row.get("primitive") or "lending").lower()
    details: dict[str, Any] = {"source": "position_registry", "leg": leg, "market_id": str(market_id)}
    asset = payload.get("asset")
    if asset:
        details["asset_symbol"] = str(asset)
    return PositionInfo(
        position_type=position_type,
        position_id=str(market_id),
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("0"),
        details=details,
    )


async def read_open_lending_positions_from_registry(
    *,
    state_manager: Any,
    deployment_id: str,
    chain: str | None = None,
) -> tuple[list[PositionInfo], bool]:
    """Read this deployment's OPEN lending legs from WARM (TD-04 / VIB-5462).

    The lending sibling of :func:`read_open_lp_positions_from_registry`: reads
    ``position_registry`` rows for ``primitive='lending'`` and builds one
    :class:`PositionInfo` per open leg (collateral → SUPPLY, debt → BORROW).
    Same ``(positions, available)`` contract and same never-raise discipline —
    ``available=False`` on a backend without cutover storage degrades to the
    strategy's own enumeration; it never means "nothing open".
    """
    from almanak.framework.migration import CutoverStorageNotSupported

    dep = str(deployment_id or "").strip()
    if state_manager is None or not dep or not hasattr(state_manager, "get_position_registry_open_rows"):
        return [], False

    positions: list[PositionInfo] = []
    available = False
    for primitive, accounting_category in _LENDING_REGISTRY_SPECS:
        try:
            rows = await state_manager.get_position_registry_open_rows(
                dep,
                chain=chain,
                primitive=primitive,
                accounting_category=accounting_category,
            )
        except (CutoverStorageNotSupported, NotImplementedError) as exc:
            logger.debug(
                "Teardown registry enumeration: lending read unavailable for %s (%s)",
                dep,
                exc,
            )
            continue
        except Exception:  # noqa: BLE001 — enumeration must never raise into teardown
            logger.warning(
                "Teardown registry enumeration: lending read FAILED for %s — falling back "
                "to strategy enumeration this teardown",
                dep,
                exc_info=True,
            )
            continue
        available = True
        for row in rows or []:
            info = _position_info_from_lending_registry_row(row)
            if info is not None:
                positions.append(info)
    return positions, available


def _position_info_from_perp_registry_row(row: Any) -> PositionInfo | None:
    """Build a perp :class:`PositionInfo` from one OPEN ``position_registry`` row.

    Returns ``None`` when the row carries no usable ``position_id`` (the venue
    position key — the identity anchor) — a registry row without it cannot be
    closed and must not be surfaced.

    USD value is left at ``Decimal("0")``: the registry is the identity surface,
    not a valuation surface (blueprint 28 §2). ``liquidation_risk`` is left at
    the model default (``False``) — the registry knows the position's identity,
    not its on-chain health factor; the teardown ``PositionType.PERP`` priority
    already closes perps FIRST regardless of the flag, so the registry must not
    fabricate a risk signal it cannot measure. Market / collateral / direction /
    size ride in ``details`` (best-effort: the runtime write carries all four;
    a backfill-synthesized row carries only what ``position_events`` persisted).
    """
    if not isinstance(row, dict):
        return None
    payload = row.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    position_id = payload.get("position_id")
    if position_id is None or position_id == "":
        return None
    chain = str(row.get("chain") or "").lower()
    # The protocol slug IS carried in the perp payload (unlike LP). Prefer it so
    # teardown can route the closing intent to the right connector; fall back to
    # the registry primitive.
    protocol = str(payload.get("protocol") or row.get("primitive") or "perp").lower()
    details: dict[str, Any] = {"source": "position_registry"}
    for key in ("market", "collateral_token", "direction", "size_usd"):
        value = payload.get(key)
        if value is not None and value != "":
            details[key] = value
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=str(position_id),
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("0"),
        details=details,
    )


async def read_open_perp_positions_from_registry(
    *,
    state_manager: Any,
    deployment_id: str,
    chain: str | None = None,
) -> tuple[list[PositionInfo], bool]:
    """Read this deployment's OPEN perp positions from WARM (TD-02 / VIB-5460).

    The perp sibling of :func:`read_open_lending_positions_from_registry`: reads
    ``position_registry`` rows for ``primitive='perp'`` and builds one
    :class:`PositionInfo` per open position (venue position key → identity).
    Same ``(positions, available)`` contract and same never-raise discipline —
    ``available=False`` on a backend without cutover storage degrades to the
    strategy's own enumeration; it never means "nothing open".
    """
    from almanak.framework.migration import CutoverStorageNotSupported

    dep = str(deployment_id or "").strip()
    if state_manager is None or not dep or not hasattr(state_manager, "get_position_registry_open_rows"):
        return [], False

    positions: list[PositionInfo] = []
    available = False
    for primitive, accounting_category in _PERP_REGISTRY_SPECS:
        try:
            rows = await state_manager.get_position_registry_open_rows(
                dep,
                chain=chain,
                primitive=primitive,
                accounting_category=accounting_category,
            )
        except (CutoverStorageNotSupported, NotImplementedError) as exc:
            logger.debug(
                "Teardown registry enumeration: perp read unavailable for %s (%s)",
                dep,
                exc,
            )
            continue
        except Exception:  # noqa: BLE001 — enumeration must never raise into teardown
            logger.warning(
                "Teardown registry enumeration: perp read FAILED for %s — falling back "
                "to strategy enumeration this teardown",
                dep,
                exc_info=True,
            )
            continue
        available = True
        for row in rows or []:
            info = _position_info_from_perp_registry_row(row)
            if info is not None:
                positions.append(info)
    return positions, available


def reconcile_lp_with_registry(
    *,
    strategy_summary: TeardownPositionSummary | None,
    registry_positions: list[PositionInfo],
    registry_available: bool,
) -> TeardownPositionSummary:
    """Fold the ``position_registry`` WARM read into the strategy's enumeration.

    Semantics are **additive (union), never subtractive**, and deliberately so:

    * Every strategy-reported position is kept (non-LP, cut-over LP, non-cut-over
      LP alike) — the read-path cutover must never *drop* a position the strategy
      believes is open, because the OPEN-rows read cannot distinguish "genuinely
      closed" from "registry write was skipped at open" (parser-no-payload
      fallback), and a false drop would under-count / hide a live position.
    * Each registry OPEN LP position the strategy did **not** already report
      (keyed on ``(chain, position_type, position_id)``, NOT bare token id —
      token ids are unique only within a chain) is appended — this is the
      restart-safe re-derivation: a runner whose in-memory state was wiped
      reports an empty (or partial) summary, and the registry — the durable WARM
      tier — supplies the open set.

    On a clean restart the strategy reports nothing, so the union IS exactly the
    registry's open set — the determinism the ticket requires. When the registry
    is NOT available (no backend / hosted pre-T19) or holds no rows, the strategy
    summary is returned unchanged (the legacy enumeration is the degrade path).
    """
    if strategy_summary is None:
        strategy_summary = TeardownPositionSummary.empty("unknown")
    if not registry_available or not registry_positions:
        return strategy_summary

    # Dedupe on (chain, position_type, position_id) — NOT bare position_id. A
    # bare NFT token_id is unique only within a chain, and a single deployment
    # can span chains (the inline multi-chain teardown lane, runner_teardown
    # §"For multi-chain strategies"). Keying on token_id alone would let a
    # strategy-reported LP token_id=N on chain A suppress a registry-open LP
    # token_id=N on chain B → under-report → strand chain B's position.
    def _dedupe_key(position: PositionInfo) -> tuple[str, str, str]:
        return (str(position.chain or "").lower(), str(position.position_type), str(position.position_id))

    seen = {_dedupe_key(p) for p in strategy_summary.positions}
    net_new: list[PositionInfo] = []
    for rp in registry_positions:
        key = _dedupe_key(rp)
        if key not in seen:
            net_new.append(rp)
            seen.add(key)
    if not net_new:
        return strategy_summary

    return TeardownPositionSummary(
        deployment_id=strategy_summary.deployment_id,
        timestamp=strategy_summary.timestamp,
        positions=list(strategy_summary.positions) + net_new,
        # Preserve the strategy's explicit totals: the model recomputes
        # ``total_value_usd`` / ``has_liquidation_risk`` from positions when
        # omitted (== 0 / == False), which would silently clobber a strategy
        # that set them explicitly. Registry-derived rows carry value_usd=0 and
        # liquidation_risk=False, so they add nothing to either total.
        total_value_usd=strategy_summary.total_value_usd,
        has_liquidation_risk=(strategy_summary.has_liquidation_risk or any(p.liquidation_risk for p in net_new)),
    )


async def resolve_open_positions_with_registry(strategy: Any) -> TeardownPositionSummary:
    """Strategy enumeration reconciled against the ``position_registry`` WARM read path.

    Calls the strategy's own ``get_open_positions()`` (its authoritative,
    primitive-complete enumeration), then reconciles the cut-over LP slice
    against the registry so a restarted runner re-derives the same open LP set
    from WARM. The registry read degrades to a no-op (legacy enumeration) on a
    backend without cutover storage.

    Errors from ``strategy.get_open_positions()`` are NOT swallowed here — the
    caller (runner / CLI) owns that policy. Registry-read errors are swallowed
    inside :func:`read_open_lp_positions_from_registry`.
    """
    deployment_id = str(getattr(strategy, "deployment_id", "") or "")
    summary = strategy.get_open_positions()
    if summary is None:
        # A custom / degraded ``get_open_positions`` may return None; preserve
        # the deployment id for downstream tracking instead of falling back to
        # the bare "unknown" sentinel inside ``reconcile_lp_with_registry``.
        summary = TeardownPositionSummary.empty(deployment_id or "unknown")
    state_manager = getattr(strategy, "_state_manager", None)
    read = await read_open_lp_positions_detailed(
        state_manager=state_manager,
        deployment_id=deployment_id,
        chain=None,
    )
    # TD-04 (VIB-5462): the lending cutover surfaces open collateral/debt legs
    # through the SAME additive-union reconcile. Read both primitive streams and
    # union them so the restart-safe re-derivation is identical across LP and
    # lending; ``available`` is True if EITHER stream answered. The completeness
    # chain-verify below is TD-05's LP-only concern and stays scoped to LP
    # (lending chain-verify is TD-09's HF-safe-unwind job, not this read path).
    lending_positions, lending_available = await read_open_lending_positions_from_registry(
        state_manager=state_manager,
        deployment_id=deployment_id,
        chain=None,
    )
    # TD-02 (VIB-5460): the perp cutover surfaces open perp positions through the
    # SAME additive-union reconcile. Read the perp stream and union it so the
    # restart-safe re-derivation is identical across LP / lending / perp;
    # ``available`` is True if ANY stream answered.
    perp_positions, perp_available = await read_open_perp_positions_from_registry(
        state_manager=state_manager,
        deployment_id=deployment_id,
        chain=None,
    )
    reconciled = reconcile_lp_with_registry(
        strategy_summary=summary,
        registry_positions=read.positions + lending_positions + perp_positions,
        registry_available=read.available or lending_available or perp_available,
    )
    # TD-05 (VIB-5463): chain-verify the enumeration completeness. This NEVER
    # mutates the additive union (the union→authoritative flip is TD-06's job) —
    # it (a) upgrades the registry-read-failure path from warn-only to an active
    # per-position chain-verify of the known LP set, and (b) emits the structured
    # "registry incomplete" signal TD-06 consumes to decide when the registry can
    # be trusted (a strategy LP that is open on-chain yet absent from the registry
    # is a write-skipped / pre-cutover row, not a closed position).
    await _verify_lp_enumeration_completeness(
        strategy=strategy,
        strategy_summary=summary,
        read=read,
    )
    return reconciled


def _registry_open_keys(read: RegistryReadResult) -> set[tuple[str, str]]:
    """``(chain, token_id)`` keys for the registry-reported OPEN LP positions."""
    return {(str(p.chain or "").lower(), str(p.position_id)) for p in read.positions}


async def _verify_lp_enumeration_completeness(
    *,
    strategy: Any,
    strategy_summary: TeardownPositionSummary,
    read: RegistryReadResult,
) -> None:
    """Chain-verify the LP enumeration's completeness (TD-05 / VIB-5463).

    Observation-only — it NEVER mutates the returned enumeration (the additive
    union is preserved; the authoritative flip is TD-06's). It does two things,
    both bounded to the *discrepancy* set so the common matched case issues zero
    chain reads:

    1. **Registry-read-failure verification (no longer warn-only).** When a
       primitive's registry read RAISED (``read.failed_primitives``), the
       registry answer is incomplete, so the strategy-reported LP set is the only
       known identity. Each such LP is chain-verified; a structured ERROR is
       logged when a position cannot be confirmed open, so an operator sees an
       unverified teardown instead of a silent warning.

    2. **Completeness signal for TD-06 (AC3).** When the registry WAS available
       but a strategy-reported LP is ABSENT from its OPEN rows, chain-verify it:
       if the chain confirms it is open, that row is a write-skipped / pre-cutover
       gap (the registry is not yet complete) — logged so TD-06 knows the
       union→authoritative flip is not yet safe.

    Gateway boundary: verification is gateway-routed via
    :func:`live_position_reads.chain_verify_lp_open`. A strategy without a wired
    gateway client simply skips verification (the additive union still stands).
    """
    gateway_client = getattr(strategy, "_gateway_client", None)
    if gateway_client is None:
        if read.failed_primitives:
            logger.error(
                "Teardown LP enumeration: registry read failed for %s and no gateway "
                "client is available to chain-verify the known LP set — completeness "
                "is UNVERIFIED for this teardown",
                ", ".join(read.failed_primitives),
            )
        return

    from almanak.framework.teardown.live_position_reads import chain_verify_lp_open

    strategy_lp = [p for p in strategy_summary.positions if p.position_type == PositionType.LP]
    if not strategy_lp:
        if read.failed_primitives:
            logger.error(
                "Teardown LP enumeration: registry read failed for %s and the strategy "
                "reported no LP — a forgotten LP cannot be re-derived per-position "
                "(wallet-scan recovery is the separate --discover lane); completeness "
                "is UNVERIFIED",
                ", ".join(read.failed_primitives),
            )
        return

    registry_keys = _registry_open_keys(read)
    network = str(getattr(strategy, "_gateway_network", "") or "")

    for position in strategy_lp:
        key = (str(position.chain or "").lower(), str(position.position_id))
        absent_from_registry = key not in registry_keys
        # Only verify the discrepancy set: a strategy LP the registry already
        # confirms (matched) needs no chain read unless its primitive's read
        # failed (registry answer incomplete for it).
        if not absent_from_registry and not read.failed_primitives:
            continue

        verdict = await chain_verify_lp_open(gateway_client=gateway_client, position=position, network=network)

        if read.failed_primitives:
            if verdict is True:
                logger.warning(
                    "Teardown LP enumeration: registry read failed (%s); LP token_id=%s "
                    "on %s CONFIRMED open on-chain — retained in the teardown set",
                    ", ".join(read.failed_primitives),
                    position.position_id,
                    position.chain,
                )
            elif verdict is None:
                logger.error(
                    "Teardown LP enumeration: registry read failed (%s) AND LP token_id=%s "
                    "on %s could not be confirmed open on-chain — completeness UNVERIFIED; "
                    "manual on-chain check advised before treating teardown as complete",
                    ", ".join(read.failed_primitives),
                    position.position_id,
                    position.chain,
                )
            # verdict is False ⇒ the position is closed on-chain; it harmlessly
            # plans a no-op close — left in the union (no subtraction here).
        elif absent_from_registry and verdict is True:
            logger.warning(
                "Teardown LP enumeration: LP token_id=%s on %s is open on-chain but ABSENT "
                "from position_registry — a write-skipped / pre-cutover row. Union retained "
                "(no position dropped); registry is not yet complete, so the "
                "union→authoritative flip (TD-06) stays blocked",
                position.position_id,
                position.chain,
            )


__all__ = [
    "RegistryReadResult",
    "read_open_lending_positions_from_registry",
    "read_open_lp_positions_detailed",
    "read_open_lp_positions_from_registry",
    "read_open_perp_positions_from_registry",
    "reconcile_lp_with_registry",
    "resolve_open_positions_with_registry",
]
