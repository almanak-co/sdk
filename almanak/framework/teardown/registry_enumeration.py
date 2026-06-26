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
from decimal import Decimal
from typing import Any

from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)

logger = logging.getLogger(__name__)


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


async def read_open_lp_positions_from_registry(
    *,
    state_manager: Any,
    deployment_id: str,
    chain: str | None = None,
) -> tuple[list[PositionInfo], bool]:
    """Read this deployment's OPEN UniV3 + UniV4 LP positions from WARM.

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
        ``(positions, available)``. ``available`` is ``False`` when the backend
        cannot answer a registry read (no state manager, missing accessor, or
        hosted pre-T19 → :class:`CutoverStorageNotSupported`); the caller then
        keeps the strategy's own enumeration unchanged. ``available`` ``True``
        with an empty list means "registry is authoritative and this deployment
        has zero open LP" (a closed position correctly does not appear).

    Never raises — enumeration must never fault the teardown lane.
    """
    from almanak.framework.migration import CutoverStorageNotSupported

    dep = str(deployment_id or "").strip()
    if state_manager is None or not dep or not hasattr(state_manager, "get_position_registry_open_rows"):
        return [], False

    positions: list[PositionInfo] = []
    available = False
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
            # restart it would be invisible. Log at WARNING (not the benign
            # cutover-unavailable case above). Live re-derivation when the
            # registry read fails is owned by TD-05 (VIB-5463).
            logger.warning(
                "Teardown registry enumeration: %s read FAILED for %s — this primitive "
                "falls back to strategy enumeration this teardown",
                primitive,
                dep,
                exc_info=True,
            )
            continue
        available = True
        for row in rows or []:
            info = _position_info_from_registry_row(row, primitive=primitive)
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
    registry_positions, available = await read_open_lp_positions_from_registry(
        state_manager=getattr(strategy, "_state_manager", None),
        deployment_id=deployment_id,
        chain=None,
    )
    return reconcile_lp_with_registry(
        strategy_summary=summary,
        registry_positions=registry_positions,
        registry_available=available,
    )


__all__ = [
    "read_open_lp_positions_from_registry",
    "reconcile_lp_with_registry",
    "resolve_open_positions_with_registry",
]
