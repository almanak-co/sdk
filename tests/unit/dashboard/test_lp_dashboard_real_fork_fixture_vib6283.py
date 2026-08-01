"""VIB-6283 — golden tests over the REAL Anvil fee-induction run.

`tests/fixtures/lp_dashboard_vib6283/anvil_fee_induction_run.json` holds the
actual database rows from the fork run that proved the dead panel: a $1.26M
Uniswap V3 WETH/USDC position holding ~8.4% of the pool's active liquidity,
against which ~$2.8M of round-trip volume was induced.

Its accrued fees were measured **three independent ways**, all agreeing:

* ``collect()`` staticcall at block 489688330 — 0.023776898630860097 WETH
  + 44.212848 USDC
* a hand-computed ``feeGrowthInside`` delta from raw pool storage — identical
  to the wei
* the accounting layer's own ``LP_CLOSE`` payload — ``fees_total_usd =
  88.50577177198135678775255617``, agreeing with the on-chain figure to ~3
  cents (the block-vs-close price drift between the two lanes)

The dashboard rendered ``$0.00``.

These tests replay those exact rows so the proof survives without Anvil. They
are the regression contract for the money path: if a future change makes the
panel understate again, this file goes red in under a second.

**KNOWN-INCONSISTENT — do NOT assert against ``portfolio_metrics`` (VIB-6336).**
The fixture's single ``portfolio_metrics`` row carries
``total_value_usd = 593170.69``, which is the final SWAP's output leg alone. The
terminal ``post_state_json`` still holds ~79.875 WETH + ~1,293,156 USDC, worth
roughly $1.44M at the fixture price — so that row is a mid-flight capture, not a
terminal snapshot, and it omits the retained assets. No test here reads it, which
is why the inconsistency is inert today; a future test that reached for a
portfolio total would silently validate behaviour against a number that never
described the wallet. Regenerating means re-running the fork, so it is ticketed
rather than done here. Use ``accounting_events`` / ``position_events`` — the rows
these tests actually replay — and treat the metrics row as untrustworthy.
Flagged by CodeRabbit on PR #3532.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.dashboard.quant_aggregations import (
    compute_cost_stack,
    compute_reconciliation,
)
from almanak.framework.dashboard.utils import format_usd

FIXTURE = Path(__file__).parents[2] / "fixtures" / "lp_dashboard_vib6283" / "anvil_fee_induction_run.json"

# The on-chain truth, restated so a reader never has to leave this file.
ONCHAIN_FEE_WETH = Decimal("0.023776898630860097")
ONCHAIN_FEE_USDC = Decimal("44.212848")
ACCOUNTED_FEES_USD = Decimal("88.50577177198135678775255617")
ACCOUNTED_IL_USD = Decimal("0.041188405836821282508")


@pytest.fixture(scope="module")
def run_rows() -> dict:
    assert FIXTURE.exists(), f"real-fork fixture missing at {FIXTURE}"
    return json.loads(FIXTURE.read_text())


@pytest.fixture(scope="module")
def accounting_events(run_rows: dict) -> list[dict]:
    events = run_rows["accounting_events"]
    # Guard the fixture itself: a silently-emptied fixture would make every
    # assertion below vacuously true. Print-the-denominator, as a test.
    assert len(events) == 3, f"expected 3 accounting events in the fork run, got {len(events)}"
    assert {e["event_type"] for e in events} == {"LP_OPEN", "LP_CLOSE", "SWAP"}
    return events


def test_fixture_carries_the_measured_close(accounting_events: list[dict]) -> None:
    """The fixture must contain the real LP_CLOSE payload, or nothing below means
    anything. This is the fixture's own integrity check."""
    close = next(e for e in accounting_events if e["event_type"] == "LP_CLOSE")
    payload = json.loads(close["payload_json"])

    assert Decimal(payload["fees_total_usd"]) == ACCOUNTED_FEES_USD
    assert Decimal(payload["il_usd"]) == ACCOUNTED_IL_USD


def test_cost_stack_over_real_rows_reports_the_real_fee(accounting_events: list[dict]) -> None:
    """THE regression contract. Pre-fix this produced Decimal('0') because the
    LP branch folded only on LP_CLOSE via ``_payload_decimal`` and the panel
    never read the cost stack at all."""
    stack = compute_cost_stack([], accounting_events)

    assert stack.fees_earned_usd == ACCOUNTED_FEES_USD
    assert stack.fees_earned_measured is True
    assert stack.il_usd == ACCOUNTED_IL_USD
    assert stack.il_measured is True


def test_formatter_renders_real_money_not_zero(accounting_events: list[dict]) -> None:
    """INVARIANT GUARD (passes pre-fix — deliberately).

    Covers only the RC5 formatter class, NOT the panel. It passes on the old
    code because ``compute_cost_stack`` always folded an ``LP_CLOSE`` fee
    correctly — the dead panel's defect was that the panel never READ the cost
    stack. Kept because the corpus median LP fee is ~$0.007 and a 2-dp
    regression here would re-hide every real number; the panel-level proof is
    ``test_panel_reads_accounting_not_session_state`` below.
    """
    stack = compute_cost_stack([], accounting_events)

    assert format_usd(stack.fees_earned_usd, precise_small=True) == "$88.51"
    assert format_usd(stack.il_usd, precise_small=True) == "$0.04"
    # The corpus median fee — the value a 2-dp formatter silently erases.
    assert format_usd(Decimal("0.0066680447828332196032"), precise_small=True) != "$0.00"


def test_panel_reads_accounting_not_session_state(accounting_events: list[dict], monkeypatch) -> None:
    """DISCRIMINATING (fails pre-fix) — the defect's actual layer.

    Drives ``_render_performance_summary`` itself with a ``session_state``
    carrying HOSTILE money values, and an accounting lane carrying the real
    fork numbers. Pre-fix the panel read the dict and would render the hostile
    values (or the ``"0"`` defaults). Post-fix it must render the accounting
    numbers and ignore the dict entirely.

    This is the test the original panel could never have had: it asserts the
    SOURCE of the money, which is the whole contract.
    """
    import almanak.framework.dashboard.templates.lp_dashboard as lp_mod
    from almanak.framework.dashboard.money import StrategyMoney

    stack = compute_cost_stack([], accounting_events)
    monkeypatch.setattr(
        lp_mod,
        "_position_value_usd_for_summary",
        lambda *_a, **_k: Decimal("0"),
    )

    captured: dict[str, str] = {}
    monkeypatch.setattr(lp_mod.st, "columns", lambda n: [_NullCtx() for _ in range(n)])
    monkeypatch.setattr(lp_mod.st, "metric", lambda label, value, **kw: captured.__setitem__(label, value))
    monkeypatch.setattr(
        "almanak.framework.dashboard.money.load_strategy_money",
        lambda _d: StrategyMoney(
            lp_fees_earned_usd=stack.fees_earned_usd,
            lp_il_usd=stack.il_usd,
            strategy_pnl_usd=Decimal("429.00"),
            open_position_nav_usd=Decimal("593170.69"),
        ),
    )

    lp_mod._render_performance_summary(
        {
            "total_fees_usd": "999999",
            "impermanent_loss_pct": "-99",
            "net_pnl_usd": "123456",
        },
        "deployment:b3816ff5ddb8",
    )

    assert captured["Total Fees"] == "$88.51", f"panel must read accounting, got {captured['Total Fees']}"
    assert "999999" not in captured["Total Fees"]
    assert captured["Impermanent Loss"] == "$0.04"
    assert "123456" not in captured["Strategy Net PnL"]
    assert captured["Strategy Net PnL"] == "+$429.00"


class _NullCtx:
    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False


def test_rendered_tile_agrees_with_the_onchain_measurement(accounting_events: list[dict]) -> None:
    """Cross-lane check: the number the dashboard shows must be the number the
    chain owed. Two independent measurements agreeing to **~3 cents on $88.51**.

    The docstring used to claim "under a cent" and that WETH was priced from
    the close payload. Both were false, and CodeRabbit was right to flag the
    gap (PR #3532 review). The captured payload carries no ``prices`` key at
    all, so the pinned rate below is the ONLY path this test has ever taken,
    and the real delta is $0.0301 — an assertion at $0.01 would fail. The
    residual is the block-vs-close price drift between the two lanes; it is
    small enough to prove the lanes agree and too large to call a cent.

    The tolerance is set just above the observed delta rather than at the old
    ``$1.00`` (1.1% of the figure — loose enough to pass while the two lanes
    genuinely disagreed).
    """
    stack = compute_cost_stack([], accounting_events)
    close = next(e for e in accounting_events if e["event_type"] == "LP_CLOSE")
    payload = json.loads(close["payload_json"])

    # ``.get`` chain, not ``payload["prices"]["WETH"]``: a payload carrying
    # ``prices`` without a ``WETH`` entry would raise KeyError instead of
    # taking the documented fallback.
    quoted = (payload.get("prices") or {}).get("WETH")
    weth_usd = Decimal(str(quoted)) if quoted is not None else Decimal("1861.59")
    onchain_usd = ONCHAIN_FEE_WETH * weth_usd + ONCHAIN_FEE_USDC

    assert abs(stack.fees_earned_usd - onchain_usd) < Decimal("0.05"), (
        f"dashboard fee {stack.fees_earned_usd} disagrees with on-chain {onchain_usd}"
    )


def test_g6_reconciliation_includes_mid_life_harvest(accounting_events: list[dict]) -> None:
    """DISCRIMINATING (fails pre-fix).

    ``fees_earned_usd`` feeds ``_net_realized_pnl_usd``, which mirrors the G6
    component decomposition. Pre-fix, ``compute_reconciliation`` folded LP fees
    only from ``LP_CLOSE``, so a mid-life ``LP_COLLECT_FEES`` harvest credited
    the wallet but never the component sum — an unexplained G6 gap.

    The fork run itself never harvested mid-life (no LP_COLLECT_FEES row), which
    is exactly why replaying it alone cannot catch this. We replay the real rows
    PLUS a harvest to exercise the lane the corpus never did.
    """
    harvest = {
        "event_type": "LP_COLLECT_FEES",
        "payload_json": json.dumps({"fees_total_usd": "7.25"}),
    }
    events = [*accounting_events, harvest]
    stack = compute_cost_stack([], events)
    status = compute_reconciliation(
        Decimal("1261091.44"),  # initial value at OPEN, from the run
        Decimal("1261520.44"),  # NAV after the close
        stack,
        events,
    )

    assert stack.fees_earned_usd == ACCOUNTED_FEES_USD + Decimal("7.25")
    assert status.sum_fees == ACCOUNTED_FEES_USD + Decimal("7.25"), (
        "a mid-life harvest must reach the G6 component sum, or its income shows as an unexplained gap"
    )


def test_real_fork_position_registry_is_closed(run_rows: dict) -> None:
    """RC2 context: after teardown the registry row is ``closed``. The rows the
    Positions panel needs exist — ``GetPositions`` simply could not ask for
    them, since its only reader hardcodes ``status='open'``."""
    registry = run_rows["position_registry"]
    assert len(registry) == 1
    assert registry[0]["status"] == "closed"


def test_open_position_state_would_report_unmeasured(accounting_events: list[dict]) -> None:
    """Replay only the OPEN half of the same run. Before the close landed, the
    accounting lane had measured no fees — even though ~$88 was accruing
    on-chain at that very moment. The honest render is "—", not "$0.00"."""
    open_only = [e for e in accounting_events if e["event_type"] == "LP_OPEN"]
    assert len(open_only) == 1

    stack = compute_cost_stack([], open_only)

    assert stack.fees_earned_measured is False
    assert stack.il_measured is False
