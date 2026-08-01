"""Regressions for the four defects the PR #3532 auditor panel found (VIB-6283).

Every test here is written to go RED if its fix is reverted — that is the only
property that makes a regression test worth its runtime. The four defects:

1. ``_drawdowns`` — the SECOND drawdown entrypoint — folded ``strategy_reported``
   marks. Found independently by both external reviewers. ``compute_pnl_summary``
   degrades to it whenever the lifetime scan is unavailable, so gating only the
   lifetime reader left the phantom reachable on a documented fallback path.
2. ``compute_cost_stack`` / ``compute_reconciliation`` folded an
   ``LP_COLLECT_FEES`` row's ``realized_pnl_usd``. The producer computes that as
   ``fees_value - full_open_basis``, so a mid-life harvest booked the
   still-deployed principal as a realized loss. Introduced by this PR when the
   branch was widened to capture COLLECT fee income.
3. ``_measured_lp_composition`` matched the token pair as an unordered set and
   then returned the amounts unswapped, printing each under the other token's
   name whenever canonical order differs from the configured label order.
4. ``_merge_live_and_caller_state`` applied the live-ownership rule to
   ``caller_state`` only, so a strategy that mirrors mint amounts into
   ``get_state()`` froze the composition card at entry.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.dashboard.quant_aggregations import (
    _drawdowns,
    compute_cost_stack,
    compute_reconciliation,
)
from almanak.framework.dashboard.templates.lp_dashboard import (
    LP_MEASURED_COMPOSITION_KEYS,
    LPDashboardConfig,
    _measured_lp_composition,
    _merge_live_and_caller_state,
    prepare_lp_session_state,
)
from almanak.framework.portfolio.models import (
    STRATEGY_REPORTED_VALUATION_SOURCE,
    PortfolioSnapshot,
    PositionValue,
)


def _snapshot(nav: str, *, source: str, confidence: str = "ESTIMATED") -> dict[str, Any]:
    """A recent-window snapshot row carrying one LP position with ``source``."""
    return {
        "total_value_usd": Decimal(nav),
        "available_cash_usd": Decimal("0"),
        "value_confidence": confidence,
        "positions_json": json.dumps(
            {
                "schema_version": 1,
                "positions": [
                    {
                        "position_type": "LP",
                        "protocol": "traderjoe_v2",
                        "chain": "avalanche",
                        "value_usd": nav,
                        "details": {"valuation_source": source},
                    }
                ],
            }
        ),
    }


# --------------------------------------------------------------------------
# 1. The recent-window drawdown fallback must honour provenance
# --------------------------------------------------------------------------


def test_recent_window_drawdown_skips_strategy_reported_marks() -> None:
    """The phantom shape: a self-reported hold, then a collapse to real cash.

    Pre-fix this returned the full ~74% latch. The marks are ESTIMATED, which the
    confidence gate deliberately keeps — provenance is the only axis that can
    refuse them, which is exactly why the fix could not be expressed as a
    confidence demotion.
    """
    snapshots = [
        _snapshot("1000", source=STRATEGY_REPORTED_VALUATION_SOURCE),
        _snapshot("1000", source=STRATEGY_REPORTED_VALUATION_SOURCE),
        _snapshot("260", source="on_chain"),
    ]

    max_dd, current_dd = _drawdowns(snapshots)

    assert max_dd == Decimal("0"), f"a strategy-reported peak latched a drawdown: {max_dd}"
    assert current_dd == Decimal("0")


def test_recent_window_drawdown_still_folds_measured_estimated_marks() -> None:
    """Negative control: ESTIMATED-but-MEASURED must still set the peak.

    Without this, "skip strategy_reported" could be implemented as "skip
    ESTIMATED" and every test above would still pass while the drawdown tile went
    silently blind to real losses on approximate marks.
    """
    snapshots = [
        _snapshot("1000", source="v4_open_amounts"),
        _snapshot("500", source="v4_open_amounts"),
    ]

    max_dd, _current = _drawdowns(snapshots)

    assert max_dd == Decimal("50"), f"a measured 50% drawdown was dropped: {max_dd}"


def _typed_snapshot(nav: str, *, source: str, confidence: str = "ESTIMATED") -> PortfolioSnapshot:
    """The PRODUCTION shape: what ``StateManager.get_recent_snapshots`` returns.

    Constructed from the real types on purpose. The first version of this test
    used a hand-rolled stand-in carrying a ``positions_json`` attribute — which
    a real ``PortfolioSnapshot`` does not have — so it asserted the accessor
    reached a field production never emits. It passed while the gate was
    completely inert: `_drawdowns` returned a 74% max-drawdown on this exact
    phantom. That is the same defect class as D2 (a detector tested only against
    a shape its producer never sends), reproduced inside the fix for it. Build
    fixtures from the real producer type; a stand-in only proves the stand-in.
    """
    return PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        deployment_id="deployment:test",
        total_value_usd=Decimal(nav),
        available_cash_usd=Decimal("0"),
        value_confidence=confidence,
        positions=[
            PositionValue(
                position_type="LP",
                protocol="traderjoe_v2",
                chain="avalanche",
                value_usd=Decimal(nav),
                label="traderjoe_v2 LP",
                details={"valuation_source": source},
            )
        ],
    )


def test_recent_window_drawdown_gates_the_production_snapshot_type() -> None:
    """The gate must fire on a real ``PortfolioSnapshot``, not just on dicts.

    Two independent layers have to hold: the accessor must prefer the typed
    ``.positions`` list (VIB-5170 — reading ``positions_json`` off a real
    snapshot yields ``None``), and the per-position predicate must read
    ``details`` off a typed ``PositionValue`` and not only off a dict. Breaking
    either one alone makes this test red.
    """
    snapshots = [
        _typed_snapshot("1000", source=STRATEGY_REPORTED_VALUATION_SOURCE),
        _typed_snapshot("260", source="on_chain"),
    ]

    max_dd, _current = _drawdowns(snapshots)

    assert max_dd == Decimal("0"), (
        f"the provenance gate is inert against the production snapshot type "
        f"(max_dd={max_dd}) — this is the VIB-5170 shape"
    )


def test_typed_measured_marks_are_still_folded() -> None:
    """Negative control on the production type: real losses must still count."""
    snapshots = [
        _typed_snapshot("1000", source="v4_on_chain"),
        _typed_snapshot("500", source="v4_on_chain"),
    ]

    max_dd, _current = _drawdowns(snapshots)

    assert max_dd == Decimal("50"), f"a measured 50% drawdown was dropped: {max_dd}"


# --------------------------------------------------------------------------
# 2. LP_COLLECT_FEES contributes fee income, never realized PnL
# --------------------------------------------------------------------------


def _collect_event(*, fees: str, realized: str) -> dict[str, Any]:
    """A mid-life harvest as the producer actually emits it.

    ``realized_pnl_usd`` is ``fees_value - full_open_basis`` — hugely negative on
    a position whose principal is still deployed. The number is not invented for
    this test; it is what ``lp_handler._lp_close_realized_pnl`` computes for the
    whole ``_LP_CLOSE_LIKE`` set.
    """
    return {
        "event_type": "LP_COLLECT_FEES",
        "payload_json": json.dumps({"fees_total_usd": fees, "realized_pnl_usd": realized}),
    }


def _recon(events: list[dict[str, Any]]):
    stack = compute_cost_stack([], events)
    return compute_reconciliation(Decimal("0"), Decimal("0"), stack, events)


def test_collect_fees_contributes_income_not_a_principal_loss() -> None:
    stack = compute_cost_stack([], [_collect_event(fees="10", realized="-990")])

    assert stack.fees_earned_usd == Decimal("10"), "the fee-income fix regressed"
    assert stack.realized_pnl_usd == Decimal("0"), (
        f"a $10 harvest booked {stack.realized_pnl_usd} of realized PnL — the "
        "still-deployed principal was treated as disposed"
    )


def test_reconciliation_collect_fees_matches_the_cost_stack_fold() -> None:
    """The two folds are documented as being in lockstep; prove it, don't assert it."""
    recon = _recon([_collect_event(fees="10", realized="-990")])

    assert recon.sum_fees == Decimal("10")
    assert recon.sum_lp == Decimal("0"), (
        f"reconciliation folded a COLLECT's realized_pnl_usd ({recon.sum_lp}); the "
        "G6 gap this branch was widened to close would be re-opened, inverted"
    )


def test_lp_close_still_folds_realized_pnl_and_il() -> None:
    """Negative control: the CLOSE half must be untouched by the COLLECT fix."""
    close = {
        "event_type": "LP_CLOSE",
        "payload_json": json.dumps(
            {"fees_total_usd": "5", "realized_pnl_usd": "-120", "il_usd": "-30"}
        ),
    }

    stack = compute_cost_stack([], [close])
    recon = _recon([close])

    assert stack.realized_pnl_usd == Decimal("-120"), "CLOSE realized PnL was lost"
    assert stack.il_usd == Decimal("-30"), "CLOSE IL was lost"
    assert recon.sum_lp == Decimal("-120")
    assert recon.sum_fees == Decimal("5")


# --------------------------------------------------------------------------
# 3. Canonical vs configured token order
# --------------------------------------------------------------------------


def _lp_row(sym0: str, sym1: str, amount0: str, amount1: str) -> dict[str, Any]:
    return {
        "position_type": "LP",
        "details": {
            "valuation_source": "v4_on_chain",
            "token0_symbol": sym0,
            "token1_symbol": sym1,
            "amount0": amount0,
            "amount1": amount1,
        },
    }


def test_reversed_canonical_order_swaps_the_amounts() -> None:
    """Canonical ``USDC/WETH`` rendered on a ``WETH/USDC`` dashboard.

    Pre-fix the unordered set matched and the amounts came back unswapped, so the
    card showed "WETH 4,385.70 / USDC 1.60" — each amount under the other token's
    name, on a page whose whole purpose is to state the composition.
    """
    config = LPDashboardConfig(token0="WETH", token1="USDC")

    result = _measured_lp_composition([_lp_row("USDC", "WETH", "4385.70", "1.60")], config)

    assert result == (1.60, 4385.70), f"amounts were not re-ordered to config order: {result}"


def test_matching_order_is_left_alone() -> None:
    """Negative control: the common case must not be swapped."""
    config = LPDashboardConfig(token0="WETH", token1="USDC")

    result = _measured_lp_composition([_lp_row("WETH", "USDC", "1.60", "4385.70")], config)

    assert result == (1.60, 4385.70)


def test_a_different_pair_is_still_refused() -> None:
    """The re-order must not weaken the multi-LP pair guard into an any-pair match."""
    config = LPDashboardConfig(token0="WETH", token1="USDC")

    assert _measured_lp_composition([_lp_row("WBTC", "DAI", "1", "2")], config) is None


# --------------------------------------------------------------------------
# 4. Strategy state must not pin the composition card
# --------------------------------------------------------------------------


class _StateClient:
    """An api_client whose strategy state mirrors its own mint amounts."""

    def __init__(self, state: dict[str, Any], position: dict[str, Any] | None = None) -> None:
        self._state = state
        self._position = position or {}

    def get_state(self) -> dict[str, Any]:
        return dict(self._state)

    def get_position(self) -> dict[str, Any]:
        return dict(self._position)


def test_state_sourced_composition_is_dropped_before_the_measured_read() -> None:
    client = _StateClient({"token0_amount": 9.99, "token1_amount": 8.88, "position_id": "42"})

    merged = _merge_live_and_caller_state(client, {}, None)

    assert "token0_amount" not in merged, "strategy state pinned the composition"
    assert "token1_amount" not in merged
    assert merged["position_id"] == "42", "a non-composition live key was collateral damage"


def test_preserve_keys_still_pins_composition() -> None:
    """Negative control: an explicit caller pin is the documented override."""
    client = _StateClient({"token0_amount": 9.99, "token1_amount": 8.88})

    merged = _merge_live_and_caller_state(
        client,
        {"token0_amount": 1.11, "token1_amount": 2.22},
        preserve_keys=list(LP_MEASURED_COMPOSITION_KEYS),
    )

    assert merged["token0_amount"] == 1.11
    assert merged["token1_amount"] == 2.22


def test_measured_composition_wins_over_strategy_state_end_to_end() -> None:
    """The whole seam: state says 9.99, the valuer measured 1.60 — 1.60 must win."""
    client = _StateClient(
        {"token0_amount": 9.99, "token1_amount": 8.88},
        {"strategy_positions": [_lp_row("WETH", "USDC", "1.60", "4385.70")]},
    )

    state = prepare_lp_session_state(
        api_client=client,
        config=LPDashboardConfig(token0="WETH", token1="USDC"),
    )

    assert state["token0_amount"] == 1.60, (
        f"the card showed {state['token0_amount']} — the strategy's own state won "
        "over the valuer's measurement"
    )
    assert state["token1_amount"] == 4385.70


# --------------------------------------------------------------------------
# 5. VIB-6283's own demotion must not narrow the VIB-4970 writer guard
# --------------------------------------------------------------------------


def _snapshot_with(*positions: PositionValue, confidence: str) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        deployment_id="deployment:test",
        total_value_usd=Decimal("0"),
        available_cash_usd=Decimal("0"),
        value_confidence=confidence,
        positions=list(positions),
    )


def _zero_open_lp(source: str | None = None) -> PositionValue:
    """An OPEN LP valued at $0 — unmeasured, never a measured zero."""
    details: dict[str, Any] = {"position_id": "42"}
    if source is not None:
        details["valuation_source"] = source
    return PositionValue(
        position_type="LP",
        protocol="uniswap_v4",
        chain="base",
        value_usd=Decimal("0"),
        label="uniswap_v4 LP",
        details=details,
    )


def test_strategy_reported_demotion_does_not_smuggle_a_zero_lp_past_the_guard() -> None:
    """The reachable case: a strategy whose price lookup failed reports $0.

    Pre-VIB-6283 that snapshot was HIGH, so the VIB-4970 guard caught it and
    refused the write. VIB-6283 demotes any snapshot carrying a self-report to
    ESTIMATED — which would have walked it straight past a guard that only ran
    on HIGH, while the row is still persisted and still folded. The shipped
    ``uniswap_v4_hooks`` demo reports exactly this shape from the ``except``
    branch of its price lookup, so this is not hypothetical.
    """
    from almanak.framework.portfolio import enforce_open_position_value_invariant
    from almanak.framework.portfolio.models import ValueConfidence

    snap = _snapshot_with(
        _zero_open_lp(source=STRATEGY_REPORTED_VALUATION_SOURCE),
        confidence=ValueConfidence.ESTIMATED,
    )

    out = enforce_open_position_value_invariant(snap)

    assert out.value_confidence == ValueConfidence.UNAVAILABLE, (
        "a $0-valued OPEN LP rode a strategy_reported demotion past the VIB-4970 guard"
    )
    assert "open_position_zero_value_guard" in out.snapshot_metadata


def test_the_guard_is_not_widened_beyond_the_reach_that_was_lost() -> None:
    """Negative control, and the reason this is a predicate not a set membership.

    An ESTIMATED snapshot that is ESTIMATED for unrelated reasons (stale price,
    partial data) was untouched before this PR and must stay untouched:
    demoting it to UNAVAILABLE suppresses the whole ``portfolio_metrics`` row,
    which is the exact collateral damage this PR refused elsewhere.
    """
    from almanak.framework.portfolio import enforce_open_position_value_invariant
    from almanak.framework.portfolio.models import ValueConfidence

    snap = _snapshot_with(_zero_open_lp(), confidence=ValueConfidence.ESTIMATED)

    out = enforce_open_position_value_invariant(snap)

    assert out.value_confidence == ValueConfidence.ESTIMATED
    assert "open_position_zero_value_guard" not in out.snapshot_metadata


# --------------------------------------------------------------------------
# 6. A measured zero is not "unmeasured" — the Earn tile
# --------------------------------------------------------------------------


def _CostStackInfoStub(**overrides: Any) -> Any:
    """A real ``CostStackInfo`` — the type ``render_cost_stack`` is handed."""
    from almanak.framework.dashboard.gateway_client import CostStackInfo

    defaults: dict[str, Any] = {
        "cost_gas_usd": Decimal("0"),
        "cost_protocol_fees_usd": Decimal("0"),
        "cost_slippage_usd": Decimal("0"),
        "fees_earned_usd": None,
        "interest_paid_usd": Decimal("0"),
        "interest_earned_usd": Decimal("0"),
        "funding_paid_usd": Decimal("0"),
        "funding_earned_usd": Decimal("0"),
        "realized_pnl_usd": Decimal("0"),
        "il_usd": None,
        "inventory_unrealized_usd": None,
    }
    return CostStackInfo(**{**defaults, **overrides})


def test_measured_zero_interest_is_not_rendered_as_unmeasured() -> None:
    """A lending-only strategy with a genuine $0 of interest earned.

    ``interest_earned_usd`` is a non-optional Decimal, so its zero is MEASURED.
    Branching on ``== 0`` inverted Empty ≠ Zero in the opposite direction from
    the bug this PR fixes — value is not applicability.
    """
    from unittest.mock import patch

    from almanak.framework.dashboard.pages import _detail_header
    from almanak.framework.dashboard.pages._detail_header import render_cost_stack

    # ``fees_earned_partial`` is what says "an LP fee leg EXISTS and is not
    # fully covered". Round 4 split it out of ``fees_earned_usd is None``,
    # which could not tell an unmeasured LP leg from a strategy that has no LP
    # leg at all and therefore captioned every perp / lending / swap deployment.
    cost = _CostStackInfoStub(
        fees_earned_usd=None, interest_earned_usd=Decimal("0"), fees_earned_partial=True
    )
    captured: list[str] = []
    with patch.object(_detail_header.st, "markdown", lambda body, **_kw: captured.append(str(body))):
        render_cost_stack(cost)
    html = "\n".join(captured)

    assert "Earn — unmeasured" not in html, "a measured $0 of interest rendered as unmeasured"
    assert "Earn +$0.00" in html
    assert "LP fees unmeasured" in html, "the genuinely unmeasured LP leg must still be named"


def test_unmeasured_lp_fees_are_still_named_when_interest_is_nonzero() -> None:
    """Negative control: the partial-measurement suffix is not value-dependent."""
    from unittest.mock import patch

    from almanak.framework.dashboard.pages import _detail_header
    from almanak.framework.dashboard.pages._detail_header import render_cost_stack

    cost = _CostStackInfoStub(
        fees_earned_usd=None, interest_earned_usd=Decimal("7.50"), fees_earned_partial=True
    )
    captured: list[str] = []
    with patch.object(_detail_header.st, "markdown", lambda body, **_kw: captured.append(str(body))):
        render_cost_stack(cost)
    html = "\n".join(captured)

    assert "Earn +$7.50" in html
    assert "LP fees unmeasured" in html


def test_a_strategy_with_no_lp_leg_is_not_captioned_about_lp_fees() -> None:
    """The other side of the same coin (VIB-6283 round 4).

    A perp / lending / TA / swap-only strategy has no LP fee bucket at all.
    Captioning it "LP fees unmeasured" forever states something false about a
    quantity that is INAPPLICABLE, not merely unmeasured — and it is the same
    class of error as rendering an unmeasured value as ``$0.00``, just pointed
    the other way.
    """
    from unittest.mock import patch

    from almanak.framework.dashboard.pages import _detail_header
    from almanak.framework.dashboard.pages._detail_header import render_cost_stack

    cost = _CostStackInfoStub(
        fees_earned_usd=None, interest_earned_usd=Decimal("7.50"), fees_earned_partial=False
    )
    captured: list[str] = []
    with patch.object(_detail_header.st, "markdown", lambda body, **_kw: captured.append(str(body))):
        render_cost_stack(cost)
    html = "\n".join(captured)

    assert "Earn +$7.50" in html
    # Narrow to the CAPTION: the tile's own tooltip legitimately reads
    # "Earn: LP fees + lending interest accrued", so a bare substring check
    # would pass for the wrong reason.
    assert "(LP fees unmeasured)" not in html
    assert "(LP fees partially measured)" not in html


def test_full_lp_lifecycle_open_collect_close_folds_correctly() -> None:
    """The realistic sequence the round-1 blocker actually broke.

    An LP opens at $1,000, harvests $10 mid-life, then closes for a $20 gain.
    Correct: realized PnL $20 (the close only), fee income $10 + $5.

    Pre-fix this returned ``realized_pnl_usd = -970`` — the COLLECT's
    ``fees_value - full_open_basis`` swamping the real result. Asserting the
    *sum over the whole lifecycle*, rather than a lone COLLECT row, is what the
    auditor asked for and is the shape that reproduces the failure end to end.

    This also pins G6 lockstep with the Accountant Test harness, which folds
    ``sum_lp`` only on ``et == "LP_CLOSE"`` (``accountant_test.py:~1345``). If
    the dashboard folds a COLLECT's realized PnL and the harness does not, the
    two disagree about the same books and G6 reports an "unexplained" gap whose
    real cause is this fold.
    """
    events = [
        {"event_type": "LP_OPEN", "payload_json": json.dumps({"cost_basis_usd": "1000"})},
        _collect_event(fees="10", realized="-990"),
        {
            "event_type": "LP_CLOSE",
            "payload_json": json.dumps(
                {"fees_total_usd": "5", "realized_pnl_usd": "20", "il_usd": "-3"}
            ),
        },
    ]

    stack = compute_cost_stack([], events)
    recon = _recon(events)

    assert stack.realized_pnl_usd == Decimal("20"), (
        f"lifetime realized PnL is {stack.realized_pnl_usd}, expected 20 — the "
        "mid-life harvest booked deployed principal as a realized loss"
    )
    assert stack.fees_earned_usd == Decimal("15"), "both fee rows must contribute"
    assert stack.il_usd == Decimal("-3"), "IL is CLOSE-only and must survive"
    assert recon.sum_lp == Decimal("20"), "reconciliation must agree with the cost stack"
    assert recon.sum_fees == Decimal("15")
