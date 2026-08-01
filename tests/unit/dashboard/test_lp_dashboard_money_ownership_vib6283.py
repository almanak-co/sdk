"""VIB-6283 — the LP dashboard must not source money from ``session_state``.

Every test here FAILS on the pre-VIB-6283 code. That is the point: the defect
survived fourteen months and four "fixed" tickets (VIB-5738 closed this exact
symptom and it recurred on 07-10, 07-16, 07-30, 07-31) because
``impermanent_loss_pct`` appeared **0 times** in the entire ``tests/`` tree.
Declared, read, never written, never tested — so there was nothing to go red.

The motivating case is the FIRST test, per "a gate must not pass on its own
motivating defect".
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.dashboard.money import (
    FORBIDDEN_MONEY_KEYS,
    StrategyMoney,
    reject_caller_money_keys,
)
from almanak.framework.dashboard.quant_aggregations import (
    CostStack,
    compute_cost_stack,
    strategy_age_days_exact,
)


class _Metrics:
    def __init__(self, initial_timestamp: str) -> None:
        self.initial_timestamp = initial_timestamp


# ── The motivating defect ────────────────────────────────────────────────


def test_caller_supplied_lp_money_keys_are_ignored() -> None:
    """THE motivating case (VIB-6283).

    A strategy author could put ``net_pnl_usd`` in a dict and the dashboard
    would report it as the strategy's PnL. On the old code these three keys
    were read straight out of ``session_state`` by
    ``_render_performance_summary``; ``prepare_lp_session_state`` never wrote
    them, so the tiles could only ever render $0.00 — while the headline
    Strategy PnL tile, computed off the gateway/accounting path, showed a real
    number on the same page.
    """
    cleaned = reject_caller_money_keys(
        {
            "net_pnl_usd": "999999",
            "total_fees_usd": "888",
            "impermanent_loss_pct": "-42",
            "position_history": [{"t": 1}],
            "range_lower": "1500",
        },
        surface="test",
    )

    assert "net_pnl_usd" not in cleaned
    assert "total_fees_usd" not in cleaned
    assert "impermanent_loss_pct" not in cleaned
    # Non-money keys (chart data, live position state) pass through untouched —
    # L3 presentation is still the strategy's to own.
    assert cleaned["position_history"] == [{"t": 1}]
    assert cleaned["range_lower"] == "1500"


def test_rejection_is_logged_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    """Ignore-and-log, never raise: an existing strategy that passes these keys
    keeps working. But the log must NAME the keys — the failure this replaces
    was silent, which is why it survived so long."""
    with caplog.at_level("WARNING"):
        reject_caller_money_keys({"net_pnl_usd": "1"}, surface="render_lp_dashboard")

    assert "net_pnl_usd" in caplog.text
    assert "render_lp_dashboard" in caplog.text, "the log must identify WHICH surface was fed the key"


def test_every_template_money_key_is_covered() -> None:
    """Census guard. The five primitive templates read 23 money-shaped keys out
    of ``session_state``; the deny-list must name each one it claims to cover.
    A key silently missing from the set is exactly how this class of defect
    reappears in the next primitive."""
    for key in ("total_fees_usd", "impermanent_loss_pct", "net_pnl_usd"):
        assert key in FORBIDDEN_MONEY_KEYS, f"{key} is money and must be in FORBIDDEN_MONEY_KEYS"


# ── Empty != Zero on the money tiles ─────────────────────────────────────


def test_strategy_money_fields_default_to_unmeasured() -> None:
    """``None``, never ``Decimal("0")``. A tile with no measurement renders
    "—"; rendering it as "$0.00" is what made a dead panel indistinguishable
    from an honestly-flat strategy."""
    money = StrategyMoney()
    assert money.lp_fees_earned_usd is None
    assert money.lp_il_usd is None
    assert money.strategy_pnl_usd is None


def test_open_lp_position_reports_fees_unmeasured_not_zero() -> None:
    """An OPEN LP position has no LP_CLOSE / LP_COLLECT_FEES row, so fees and
    IL are UNMEASURED.

    This is the real-fork case: on an Arbitrum fork with induced volume, a
    $1.26M position had ~$88 of fees provably collectable on-chain while the
    accounting lane had measured nothing. "$0.00" there is a lie; "—" is true.
    """
    stack = compute_cost_stack([], [{"event_type": "LP_OPEN", "payload_json": "{}"}])

    assert stack.fees_earned_measured is False
    assert stack.il_measured is False


def test_lp_close_without_fee_field_stays_unmeasured() -> None:
    """18 of 66 LP_CLOSE rows in the run corpus carry no ``fees_total_usd``.
    ``_payload_decimal``'s "0" default turned each into a confident measured
    zero, destroying the ``None`` that ``lp_accounting`` deliberately emits."""
    stack = compute_cost_stack([], [{"event_type": "LP_CLOSE", "payload_json": "{}"}])

    assert stack.fees_earned_measured is False, "a CLOSE with no fee field must not fabricate a measured zero"


def test_lp_close_with_measured_zero_is_measured() -> None:
    """A genuine zero is different from an absent measurement and must survive
    as one — otherwise the fix trades one conflation for another."""
    stack = compute_cost_stack(
        [], [{"event_type": "LP_CLOSE", "payload_json": '{"fees_total_usd": "0", "il_usd": "0"}'}]
    )

    assert stack.fees_earned_measured is True
    assert stack.fees_earned_usd == Decimal("0")
    assert stack.il_measured is True


# ── RC4: mid-life fee collection ─────────────────────────────────────────


def test_mid_life_collect_fees_folds_into_earn() -> None:
    """``LP_COLLECT_FEES`` is emitted by every LP connector and CREDITS the
    wallet, but appeared 0 times in ``quant_aggregations.py``. Its income
    landed in wallet PnL and never in the component sum — a G6 gap whose real
    cause was a missing fold."""
    stack = compute_cost_stack(
        [], [{"event_type": "LP_COLLECT_FEES", "payload_json": '{"fees_total_usd": "12.50"}'}]
    )

    assert stack.fees_earned_usd == Decimal("12.50")
    assert stack.fees_earned_measured is True


def test_collect_then_close_sums_without_double_count() -> None:
    """A CLOSE's ``fees_total_usd`` covers only the fees collected in the close
    tx; mid-life harvests were already paid out on their own rows. Summing both
    is correct, not a double-count."""
    stack = compute_cost_stack(
        [],
        [
            {"event_type": "LP_COLLECT_FEES", "payload_json": '{"fees_total_usd": "10"}'},
            {"event_type": "LP_CLOSE", "payload_json": '{"fees_total_usd": "5", "il_usd": "-3"}'},
        ],
    )

    assert stack.fees_earned_usd == Decimal("15")
    assert stack.il_usd == Decimal("-3"), "a fee harvest does not realise IL; only the CLOSE contributes"


def test_collect_fees_does_not_mark_il_measured() -> None:
    """IL is a CLOSE-only diagnostic. A collect must not mark the IL bucket
    measured-or-missing either way, or a harvesting strategy would report a
    fabricated IL."""
    stack = compute_cost_stack(
        [], [{"event_type": "LP_COLLECT_FEES", "payload_json": '{"fees_total_usd": "1"}'}]
    )

    assert stack.il_measured is False


# ── RC3: the APR denominator ─────────────────────────────────────────────


@pytest.mark.xfail(
    strict=True,
    reason=(
        "VIB-6283 KNOWN-INCOMPLETE: the fractional-age denominator below is correct but "
        "UNREACHABLE in production. `_strategy_age_days` / `strategy_age_days_exact` read "
        "`portfolio_metrics.initial_timestamp` via getattr, and PortfolioMetrics "
        "(almanak/framework/portfolio/models.py:594) has NO such field — 11 fields, none of "
        "them it. So getattr returns the None default and age is pinned at 0 regardless of "
        "run duration. The DB COLUMN exists and is even parsed into a local at "
        "backends/sqlite.py:2884, but the reader then constructs PortfolioMetrics without it, "
        "and the save path re-stamps the column to now on every write. Four independent "
        "defects; the integer floor this change fixed is the LAST of them. Fixing age alone "
        "still leaves `deployed_capital_usd <= dust` firing post-teardown. Remediation is "
        "upstream and mutates the persisted accounting baseline: add an immutable "
        "initial_timestamp to PortfolioMetrics (preserved on update exactly as "
        "initial_value_usd already is), stop the write paths overwriting it, populate it in "
        "both readers. Strict xfail so this flips red the moment that lands."
    ),
)
def test_strategy_age_is_reachable_on_the_real_production_type() -> None:
    """The reachability guard this fix initially lacked.

    A unit proof over a duck-typed fixture is not a proof the path production
    takes. ``test_age_days_exact_is_fractional`` below passes against a stub
    carrying ``initial_timestamp``; production passes a ``PortfolioMetrics``
    that has no such attribute, so the annualisation denominator is 0 for a
    30-day-old run just as it is for a 6-hour one.
    """
    import dataclasses
    from datetime import UTC, datetime, timedelta

    from almanak.framework.portfolio.models import PortfolioMetrics

    kwargs: dict = {}
    for f in dataclasses.fields(PortfolioMetrics):
        if f.default is not dataclasses.MISSING or f.default_factory is not dataclasses.MISSING:
            continue
        kwargs[f.name] = (
            datetime.now(tz=UTC) - timedelta(days=30) if "timestamp" in f.name else Decimal("1")
        )
    metrics = PortfolioMetrics(**kwargs)

    assert strategy_age_days_exact(metrics) > Decimal("0"), (
        "a 30-day-old run must have a non-zero annualisation denominator on the REAL type"
    )


def test_age_days_exact_is_fractional() -> None:
    """The ARITHMETIC of the fractional denominator, over a stub that supplies
    ``initial_timestamp``.

    NOTE this test does NOT prove the fix reaches production — the stub has an
    attribute the real ``PortfolioMetrics`` lacks. See the strict-xfail
    reachability guard above. Kept because the arithmetic still has to be right
    once the upstream field lands."""
    from datetime import UTC, datetime, timedelta

    six_hours_ago = (datetime.now(tz=UTC) - timedelta(hours=6)).isoformat()
    exact = strategy_age_days_exact(_Metrics(six_hours_ago))

    assert exact > Decimal("0"), "a 6h run must have a non-zero annualisation denominator"
    assert Decimal("0.2") < exact < Decimal("0.3"), f"6h should be ~0.25 days, got {exact}"


def test_strategy_apr_computes_on_a_sub_24h_run() -> None:
    """The end-to-end symptom: APR must populate on a 6h run."""
    from almanak.framework.dashboard.pages._detail_header import _strategy_apr_pct

    apr = _strategy_apr_pct(
        strategy_pnl=Decimal("100"),
        deployed_capital_usd=Decimal("1000"),
        age_days=0,  # whole days — the old denominator, still 0
        age_days_exact=Decimal("0.25"),  # 6 hours
    )

    assert apr is not None, "APR must compute on a sub-24h run (this is VIB-6283's headline symptom)"
    # +10% over a quarter-day annualises to 10% * 365/0.25 = 14,600%.
    assert apr == Decimal("100") / Decimal("1000") * Decimal("365") / Decimal("0.25") * Decimal("100")


def test_apr_falls_back_to_whole_days_against_an_old_gateway() -> None:
    """``age_days_exact=None`` means the gateway predates the field. Fall back
    to whole days — reproducing the old behaviour exactly rather than silently
    changing what an old gateway reports."""
    from almanak.framework.dashboard.pages._detail_header import _strategy_apr_pct

    apr = _strategy_apr_pct(Decimal("100"), Decimal("1000"), age_days=10, age_days_exact=None)

    assert apr == Decimal("100") / Decimal("1000") * Decimal("365") / Decimal("10") * Decimal("100")


def test_apr_below_the_annualisation_floor_is_unmeasured_not_zero() -> None:
    """Annualising a 2-minute window multiplies noise by ~260,000. Below the
    floor the answer is "not yet" (None → "—"), never a fabricated 0."""
    from almanak.framework.dashboard.pages._detail_header import _strategy_apr_pct

    apr = _strategy_apr_pct(Decimal("1"), Decimal("1000"), age_days=0, age_days_exact=Decimal("0.001"))

    assert apr is None


def test_both_annualisation_sites_agree_on_the_floor() -> None:
    """``_strategy_apr_pct`` returned None and ``_annualised_return`` returned
    Decimal("0") on the IDENTICAL condition — two computations of annualised
    return with opposite Empty!=Zero behaviour. They must now agree."""
    from almanak.framework.dashboard.pages._detail_header import (
        _MIN_ANNUALISATION_DAYS as detail_floor,
    )
    from almanak.framework.dashboard.quant_aggregations import (
        _MIN_ANNUALISATION_DAYS as agg_floor,
        _annualised_return,
    )

    assert detail_floor == agg_floor
    assert _annualised_return(Decimal("1000"), Decimal("1100"), Decimal("0.001")) is None


def test_cost_stack_defaults_are_unmeasured() -> None:
    """A freshly constructed CostStack claims no measurements."""
    stack = CostStack()

    assert stack.fees_earned_measured is False
    assert stack.il_measured is False
