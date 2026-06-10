"""AppTest smoke tests for the detail-page header helpers (Phase 5b refactor).

Exercises ``render_strategy_header``, ``render_chain_info_row`` and
``render_key_metrics`` via ``streamlit.testing.v1.AppTest.from_function`` so that
each helper is validated under a real Streamlit runtime - not a hand-rolled
mock. All scenarios are pure Streamlit (no gateway, no I/O) which keeps these
tests hermetic.

Scenarios covered (six tests, matching the plan's ``~6 AppTest smoke tests``
directive):

1. Running LP strategy - renders status badge ``RUNNING`` and an LP metric.
2. Paused strategy - renders status badge ``PAUSED``.
3. STUCK strategy - renders status badge ``STUCK``.
4. Multi-chain strategy - renders chain badges and ``Protocols:`` label
   instead of the single ``Protocol:`` label.
5. Single-chain strategy - renders a single ``Chain:`` label and a
   ``Protocol:`` label.
6. Strategy with missing optional fields (no position, no pnl_history, no
   last_action_at, no bridge fees) - no exceptions; defaults render.
"""

from __future__ import annotations

from streamlit.testing.v1 import AppTest

# ---------------------------------------------------------------------------
# Driver functions executed inside AppTest.from_function
#
# AppTest.from_function pickles the function, so these MUST be top-level and
# self-contained (all imports and constructions happen inside).
# ---------------------------------------------------------------------------


def _drive_running_lp() -> None:
    from datetime import UTC, datetime
    from decimal import Decimal

    from almanak.framework.dashboard.models import (
        LPPosition,
        PnLDataPoint,
        PositionSummary,
        Strategy,
        StrategyStatus,
    )
    from almanak.framework.dashboard.pages._detail_header import (
        render_chain_info_row,
        render_key_metrics,
        render_strategy_header,
    )

    strategy = Strategy(
        id="s",
        name="LP Strat",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("50"),
        total_value_usd=Decimal("12345.67"),
        chain="arbitrum",
        protocol="Uniswap V3",
        position=PositionSummary(
            lp_positions=[
                LPPosition(
                    pool="WETH/USDC",
                    token0="WETH",
                    token1="USDC",
                    liquidity_usd=Decimal("10000"),
                    range_lower=Decimal("1800"),
                    range_upper=Decimal("2200"),
                    current_price=Decimal("2000"),
                    in_range=True,
                )
            ],
            total_lp_value_usd=Decimal("10000"),
        ),
        pnl_history=[
            PnLDataPoint(
                timestamp=datetime(2025, 1, 1, tzinfo=UTC),
                value_usd=Decimal("10000"),
                pnl_usd=Decimal("250"),
            )
        ],
    )
    render_strategy_header(strategy)
    render_chain_info_row(strategy)
    render_key_metrics(strategy)


def _drive_paused() -> None:
    from decimal import Decimal

    from almanak.framework.dashboard.models import Strategy, StrategyStatus
    from almanak.framework.dashboard.pages._detail_header import render_strategy_header

    strategy = Strategy(
        id="s",
        name="Paused Strat",
        status=StrategyStatus.PAUSED,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("500"),
        chain="base",
        protocol="Aave V3",
    )
    render_strategy_header(strategy)


def _drive_stuck() -> None:
    from decimal import Decimal

    from almanak.framework.dashboard.models import Strategy, StrategyStatus
    from almanak.framework.dashboard.pages._detail_header import render_strategy_header

    strategy = Strategy(
        id="s",
        name="Stuck Strat",
        status=StrategyStatus.STUCK,
        pnl_24h_usd=Decimal("-10"),
        total_value_usd=Decimal("1000"),
        chain="ethereum",
        protocol="Uniswap V3",
    )
    render_strategy_header(strategy)


def _drive_multi_chain() -> None:
    from datetime import UTC, datetime
    from decimal import Decimal

    from almanak.framework.dashboard.models import (
        ChainHealth,
        ChainHealthStatus,
        Strategy,
        StrategyStatus,
    )
    from almanak.framework.dashboard.pages._detail_header import render_chain_info_row

    strategy = Strategy(
        id="s",
        name="Cross-Chain Strat",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("50000"),
        chain="arbitrum",
        protocol="Uniswap V3, Aerodrome",
        is_multi_chain=True,
        chains=["arbitrum", "base"],
        chain_health={
            "arbitrum": ChainHealth(chain="arbitrum", status=ChainHealthStatus.HEALTHY),
            "base": ChainHealth(chain="base", status=ChainHealthStatus.HEALTHY),
        },
        last_action_at=datetime(2025, 4, 20, 12, 30, tzinfo=UTC),
    )
    render_chain_info_row(strategy)


def _drive_single_chain() -> None:
    from datetime import UTC, datetime
    from decimal import Decimal

    from almanak.framework.dashboard.models import Strategy, StrategyStatus
    from almanak.framework.dashboard.pages._detail_header import render_chain_info_row

    strategy = Strategy(
        id="s",
        name="Single Chain Strat",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("1000"),
        chain="arbitrum",
        protocol="Uniswap V3",
        is_multi_chain=False,
        last_action_at=datetime(2025, 4, 20, 9, 15, tzinfo=UTC),
    )
    render_chain_info_row(strategy)


def _drive_zero_health_factor() -> None:
    """Issue #1724: health_factor == 0 must render as a Health Factor metric,
    not fall through to ``Positions N/A``.

    This mirrors the truthiness bug: ``Decimal("0")`` is falsy, so the old
    ``elif strategy.position.health_factor:`` guard skipped the Health Factor
    metric whenever a lending position had fully repaid its debt. The fix
    checks ``is not None`` explicitly.
    """
    from decimal import Decimal

    from almanak.framework.dashboard.models import (
        PositionSummary,
        Strategy,
        StrategyStatus,
    )
    from almanak.framework.dashboard.pages._detail_header import render_key_metrics

    strategy = Strategy(
        id="s",
        name="Lending Strat (no debt)",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("1000"),
        chain="arbitrum",
        protocol="Aave V3",
        position=PositionSummary(
            health_factor=Decimal("0"),  # fully repaid -> HF == 0, NOT missing data
        ),
    )
    render_key_metrics(strategy)


def _drive_nonzero_health_factor() -> None:
    """Positive health factor still renders the usual two-decimal value."""
    from decimal import Decimal

    from almanak.framework.dashboard.models import (
        PositionSummary,
        Strategy,
        StrategyStatus,
    )
    from almanak.framework.dashboard.pages._detail_header import render_key_metrics

    strategy = Strategy(
        id="s",
        name="Lending Strat (healthy)",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("1000"),
        chain="arbitrum",
        protocol="Aave V3",
        position=PositionSummary(
            health_factor=Decimal("1.5"),
        ),
    )
    render_key_metrics(strategy)


def _drive_missing_optionals() -> None:
    from decimal import Decimal

    from almanak.framework.dashboard.models import Strategy, StrategyStatus
    from almanak.framework.dashboard.pages._detail_header import (
        render_chain_info_row,
        render_key_metrics,
        render_strategy_header,
    )

    # position=None, pnl_history=[], last_action_at=None, bridge_fees_usd=0,
    # value_confidence=None - every optional left at default.
    strategy = Strategy(
        id="s",
        name="Bare Strat",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("0"),
        chain="polygon",
        protocol="Unknown",
    )
    render_strategy_header(strategy)
    render_chain_info_row(strategy)
    render_key_metrics(strategy)


# ---------------------------------------------------------------------------
# AppTest smoke tests
# ---------------------------------------------------------------------------


def _all_markdown_text(at: AppTest) -> str:
    """Concatenate every markdown block into one string for substring assertions."""
    return " ".join(md.value for md in at.markdown)


def test_apptest_running_lp_strategy_renders_status_and_lp_metric() -> None:
    """Running LP strategy renders RUNNING badge and an LP-Value metric."""
    at = AppTest.from_function(_drive_running_lp).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    text = _all_markdown_text(at)
    assert "Test Strategy" not in text  # No header-placeholder leakage
    assert "LP Strat" in text  # h2 rendered via markdown
    assert "RUNNING" in text
    # Key-metrics row emits four st.metric widgets; LP Value is the third slot.
    metric_labels = [m.label for m in at.metric]
    assert "Total Value" in metric_labels
    assert "24h PnL (Net)" in metric_labels
    assert "LP Value" in metric_labels
    assert "7d PnL" in metric_labels


def test_apptest_paused_strategy_renders_paused_status() -> None:
    """Paused strategy renders PAUSED in the header badge."""
    at = AppTest.from_function(_drive_paused).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    text = _all_markdown_text(at)
    assert "Paused Strat" in text
    assert "PAUSED" in text


def test_apptest_stuck_strategy_renders_stuck_status() -> None:
    """STUCK strategy renders STUCK in the header badge."""
    at = AppTest.from_function(_drive_stuck).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    text = _all_markdown_text(at)
    assert "Stuck Strat" in text
    assert "STUCK" in text


def test_apptest_multi_chain_strategy_renders_chain_badges() -> None:
    """Multi-chain strategy renders ``Chains:`` label and ``Protocols:`` (plural)."""
    at = AppTest.from_function(_drive_multi_chain).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    text = _all_markdown_text(at)
    # Multi-chain branch uses the plural label.
    assert "Chains:" in text
    assert "Protocols:" in text
    # Both chain names appear (rendered via format_chain_badge HTML).
    assert "arbitrum" in text
    assert "base" in text


def test_apptest_single_chain_strategy_renders_single_chain_label() -> None:
    """Single-chain strategy renders the singular ``Chain:`` and ``Protocol:`` labels."""
    at = AppTest.from_function(_drive_single_chain).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    text = _all_markdown_text(at)
    assert "Chain:" in text
    assert "Protocol:" in text
    # Singular branch should NOT use plural labels.
    assert "Chains:" not in text
    assert "Protocols:" not in text
    assert "Last Action:" in text


def test_apptest_zero_health_factor_renders_no_debt_metric() -> None:
    """Regression for #1724: ``health_factor == 0`` renders a Health Factor metric.

    The pre-fix code used ``elif strategy.position.health_factor:`` which is
    falsy for ``Decimal("0")``, silently rerouting fully-repaid lending
    positions into the ``Positions N/A`` fallback. The fix uses ``is not None``
    and annotates the zero case as ``0 (no debt)``.
    """
    at = AppTest.from_function(_drive_zero_health_factor).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    metric_labels = [m.label for m in at.metric]
    assert "Health Factor" in metric_labels
    assert "Positions" not in metric_labels  # no N/A fallback
    # The zero case is annotated so operators can tell it apart from missing data.
    hf_metric = next(m for m in at.metric if m.label == "Health Factor")
    assert "0" in hf_metric.value
    assert "no debt" in hf_metric.value


def test_apptest_nonzero_health_factor_renders_two_decimal_value() -> None:
    """Positive ``health_factor`` still renders the formatted two-decimal value."""
    at = AppTest.from_function(_drive_nonzero_health_factor).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    hf_metric = next(m for m in at.metric if m.label == "Health Factor")
    assert hf_metric.value == "1.50"


def _drive_negative_strategy_pnl() -> None:
    """Render the Money Trail with a small NEGATIVE strategy PnL.

    Mirrors the live deployment that exposed the sign-stripping bug: open
    position NAV ~= cost basis (zero unrealized) and a net realized loss of
    a couple of cents from gas, so Strategy PnL = -$0.02.
    """
    from decimal import Decimal

    from almanak.framework.dashboard.gateway_client import CostStackInfo, PnLSummary
    from almanak.framework.dashboard.pages._detail_header import render_money_trail

    pnl = PnLSummary(
        deployed_usd=Decimal("5.00"),
        nav_usd=Decimal("5.02"),
        lifetime_pnl_usd=Decimal("0.02"),
        lifetime_pnl_pct=Decimal("0.4"),
        net_apr_pct=Decimal("0"),
        max_drawdown_pct=Decimal("0.8"),
        current_drawdown_pct=Decimal("0"),
        value_confidence="HIGH",
        age_days=1,
        deployed_capital_usd=Decimal("2.43"),
        available_cash_usd=Decimal("2.59"),
        open_position_count=1,
        primary_risk_kind="lp",
        primary_risk_label="Range",
        primary_risk_value="in-range",
        primary_risk_color="green",
    )
    # Net realized = -$0.02 (gas only); zero unrealized (NAV-cash == cost basis).
    cost = CostStackInfo(
        cost_gas_usd=Decimal("0.02"),
        cost_protocol_fees_usd=Decimal("0"),
        cost_slippage_usd=Decimal("0"),
        fees_earned_usd=Decimal("0"),
        interest_paid_usd=Decimal("0"),
        interest_earned_usd=Decimal("0"),
        funding_paid_usd=Decimal("0"),
        funding_earned_usd=Decimal("0"),
        realized_pnl_usd=Decimal("0"),
        il_usd=Decimal("0"),
    )
    render_money_trail(pnl, cost)


def _drive_negative_24h_pnl_fallback() -> None:
    """Render the fallback key-metrics row with a negative 24h PnL."""
    from decimal import Decimal

    from almanak.framework.dashboard.models import Strategy, StrategyStatus
    from almanak.framework.dashboard.pages._detail_header import render_key_metrics

    strategy = Strategy(
        id="s",
        name="Down Strat",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("-1.50"),
        total_value_usd=Decimal("100.00"),
        chain="arbitrum",
        protocol="Uniswap V3",
    )
    render_key_metrics(strategy)


def test_apptest_negative_strategy_pnl_headline_carries_sign() -> None:
    """Regression: a negative Strategy PnL must render the sign in the BIG number.

    The headline previously used ``format_usd(abs(strategy_pnl))``, so a -$0.02
    loss rendered as ``$0.02`` (looks like a gain) while only the delta chip
    showed red — a direct contradiction the operator reported. The headline now
    carries the leading ``-``.
    """
    at = AppTest.from_function(_drive_negative_strategy_pnl).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    pnl_metric = next(m for m in at.metric if m.label == "Strategy PnL")
    assert pnl_metric.value == "-$0.02", pnl_metric.value
    # The bug was the headline reading as a positive gain.
    assert pnl_metric.value != "$0.02"


def test_apptest_negative_24h_pnl_fallback_headline_carries_sign() -> None:
    """Regression: the fallback 24h PnL (Net) headline carries the sign too."""
    at = AppTest.from_function(_drive_negative_24h_pnl_fallback).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    pnl_metric = next(m for m in at.metric if m.label == "24h PnL (Net)")
    assert pnl_metric.value.startswith("-$"), pnl_metric.value


def test_apptest_strategy_with_missing_optional_fields_does_not_raise() -> None:
    """Strategy with all optional fields at defaults renders without error.

    Exercises the fallback branches: ``Positions N/A``, no ``Last Action`` row,
    no confidence tooltip, no 7d PnL metric, zero bridge fees.
    """
    at = AppTest.from_function(_drive_missing_optionals).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    # The ``Last Action`` row is skipped when ``last_action_at is None``.
    assert "Last Action:" not in _all_markdown_text(at)
    metric_labels = [m.label for m in at.metric]
    assert "Total Value" in metric_labels
    assert "24h PnL (Net)" in metric_labels
    # No position -> the third column falls through to the ``Positions N/A``
    # metric. No pnl_history -> no ``7d PnL`` metric emitted.
    assert "Positions" in metric_labels
    assert "7d PnL" not in metric_labels
