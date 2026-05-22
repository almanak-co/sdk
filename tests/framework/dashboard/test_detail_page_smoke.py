"""AppTest-level smoke tests for ``detail.page`` (Phase 5e).

Final PR of the Phase-5 dashboard track. Stacks on the 5a-5d extractions
which moved the action row, header, content columns, and timeline grouping
out of ``detail.page`` into testable helper modules.

This file is the top-level harness: it drives ``detail.page`` under
``streamlit.testing.v1.AppTest`` with carefully stubbed external
dependencies (gateway RPCs, health API, timeline enrichment) so every
branch the plan calls out in Phase 5e can be validated without a running
gateway, REST API, or SQLite state DB.

The five scenarios from the plan:

1. No ``deployment_id`` query param: the empty-state branch renders the
   select box so the operator can pick a strategy.
2. Running LP strategy: ``render_action_row`` emits a ``Pause`` button.
3. Paused strategy: ``render_action_row`` emits a ``Resume`` button.
4. STUCK strategy (``status == StrategyStatus.STUCK``): ``render_action_row``
   emits a ``Bump Gas`` button.
5. Paper-mode strategy (``execution_mode == "paper"``): the paper
   session-summary section renders and the rest of the page short-circuits.

Rationale for the test approach
-------------------------------
``AppTest.from_function`` pickles the driver and executes it as a
Streamlit script, so every driver below is top-level and self-contained
(all imports happen inside, and all gateway/REST dependencies are
patched inside the driver via ``unittest.mock.patch``). This keeps the
tests hermetic and CI-safe: no network, no SQLite I/O, no reliance on
``detail.py`` internals beyond the public contract.

The tests assert only on user-visible Streamlit primitives
(``at.button``, ``at.selectbox``, ``at.markdown``, ``at.metric``) - the
same assertion vocabulary used in ``test_detail_header.py`` (Phase 5b)
and ``test_detail_content.py`` (Phase 5c).
"""

from __future__ import annotations

from streamlit.testing.v1 import AppTest

# ---------------------------------------------------------------------------
# Driver functions executed inside AppTest.from_function
#
# AppTest.from_function pickles the function, so these MUST be top-level and
# self-contained (all imports and constructions happen inside).
# ---------------------------------------------------------------------------


def _drive_no_deployment_id() -> None:
    """Scenario 1: no ``deployment_id`` query param -> select-box appears."""
    from decimal import Decimal

    import streamlit as st

    from almanak.framework.dashboard.models import Strategy, StrategyStatus
    from almanak.framework.dashboard.pages.detail import page

    # Empty query params: the empty-state branch should kick in.
    if "deployment_id" in st.query_params:
        del st.query_params["deployment_id"]

    # Two RUNNING strategies: auto-select only fires when exactly one is running,
    # so the empty-state selectbox still renders here.
    strategies = [
        Strategy(
            id="strat-abc-1234567890",
            name="Alpha",
            status=StrategyStatus.RUNNING,
            pnl_24h_usd=Decimal("0"),
            total_value_usd=Decimal("1000"),
            chain="arbitrum",
            protocol="Uniswap V3",
        ),
        Strategy(
            id="strat-def-0987654321",
            name="Beta",
            status=StrategyStatus.RUNNING,
            pnl_24h_usd=Decimal("0"),
            total_value_usd=Decimal("2000"),
            chain="base",
            protocol="Aerodrome",
        ),
    ]
    page(strategies)


def _drive_running_lp() -> None:
    """Scenario 2: running LP strategy -> Pause button."""
    from datetime import UTC, datetime
    from decimal import Decimal
    from unittest.mock import patch

    import streamlit as st

    from almanak.framework.dashboard.config import SystemHealth
    from almanak.framework.dashboard.models import (
        LPPosition,
        PnLDataPoint,
        PositionSummary,
        Strategy,
        StrategyStatus,
    )
    from almanak.framework.dashboard.pages.detail import page

    # Select this strategy via the query param.
    st.query_params["deployment_id"] = "strat-lp"

    strategy = Strategy(
        id="strat-lp",
        name="LP Strat",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("25"),
        total_value_usd=Decimal("10500"),
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

    # Feature-enabled health so the Pause button is not disabled.
    health = SystemHealth(
        api_available=True,
        api_status="healthy",
        runners_active=1,
        running_strategies=["strat-lp"],
        features={"pause_resume": True, "bump_gas": True, "execute_teardown": True},
    )

    # ``page`` imports ``get_strategy_details`` lazily; patch the source module
    # so the enrichment step is a no-op (returns None => page() keeps the
    # cached Strategy we passed in).
    with (
        patch(
            "almanak.framework.dashboard.data_source.get_strategy_details",
            return_value=None,
        ),
        patch(
            "almanak.framework.dashboard.pages.detail.check_system_health",
            return_value=health,
        ),
    ):
        page([strategy])


def _drive_paused() -> None:
    """Scenario 3: paused strategy -> Resume button."""
    from decimal import Decimal
    from unittest.mock import patch

    import streamlit as st

    from almanak.framework.dashboard.config import SystemHealth
    from almanak.framework.dashboard.models import Strategy, StrategyStatus
    from almanak.framework.dashboard.pages.detail import page

    st.query_params["deployment_id"] = "strat-paused"

    strategy = Strategy(
        id="strat-paused",
        name="Paused Strat",
        status=StrategyStatus.PAUSED,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("500"),
        chain="base",
        protocol="Aave V3",
    )

    health = SystemHealth(
        api_available=True,
        api_status="healthy",
        runners_active=1,
        running_strategies=["strat-paused"],
        features={"pause_resume": True},
    )

    with (
        patch(
            "almanak.framework.dashboard.data_source.get_strategy_details",
            return_value=None,
        ),
        patch(
            "almanak.framework.dashboard.pages.detail.check_system_health",
            return_value=health,
        ),
    ):
        page([strategy])


def _drive_stuck() -> None:
    """Scenario 4: STUCK strategy -> Bump Gas button."""
    from decimal import Decimal
    from unittest.mock import patch

    import streamlit as st

    from almanak.framework.dashboard.config import SystemHealth
    from almanak.framework.dashboard.models import Strategy, StrategyStatus
    from almanak.framework.dashboard.pages.detail import page

    st.query_params["deployment_id"] = "strat-stuck"

    # StrategyStatus.STUCK is the UI-level projection of the underlying
    # ``consecutive_errors > threshold`` condition. The gateway decides when
    # to promote a strategy to STUCK; the dashboard trusts that enum.
    strategy = Strategy(
        id="strat-stuck",
        name="Stuck Strat",
        status=StrategyStatus.STUCK,
        pnl_24h_usd=Decimal("-10"),
        total_value_usd=Decimal("1000"),
        chain="ethereum",
        protocol="Uniswap V3",
    )

    health = SystemHealth(
        api_available=True,
        api_status="healthy",
        runners_active=1,
        running_strategies=["strat-stuck"],
        features={"pause_resume": True, "bump_gas": True},
    )

    with (
        patch(
            "almanak.framework.dashboard.data_source.get_strategy_details",
            return_value=None,
        ),
        patch(
            "almanak.framework.dashboard.pages.detail.check_system_health",
            return_value=health,
        ),
    ):
        page([strategy])


def _drive_paper_mode() -> None:
    """Scenario 5: paper-mode strategy -> session-summary section renders."""
    from datetime import UTC, datetime
    from decimal import Decimal
    from unittest.mock import patch

    import streamlit as st

    from almanak.framework.dashboard.models import (
        EquityCurvePoint,
        PaperMetrics,
        Strategy,
        StrategyStatus,
    )
    from almanak.framework.dashboard.pages.detail import page

    st.query_params["deployment_id"] = "strat-paper"

    strategy = Strategy(
        id="strat-paper",
        name="Paper Strat",
        status=StrategyStatus.PAPER_TRADING,
        pnl_24h_usd=Decimal("5"),
        total_value_usd=Decimal("10000"),
        chain="arbitrum",
        protocol="Uniswap V3",
        execution_mode="paper",
        paper_metrics=PaperMetrics(
            tick_count=20,
            success_count=5,
            hold_count=10,
            error_count=1,
            simulated_pnl_usd=Decimal("15.50"),
            total_gas_cost_usd=Decimal("2.10"),
            trades_per_hour=Decimal("0.8"),
            session_start=datetime(2025, 4, 20, 8, 0, tzinfo=UTC),
            equity_curve=[
                EquityCurvePoint(
                    timestamp=datetime(2025, 4, 20, 8, 0, tzinfo=UTC),
                    value_usd=Decimal("10000"),
                ),
                EquityCurvePoint(
                    timestamp=datetime(2025, 4, 20, 9, 0, tzinfo=UTC),
                    value_usd=Decimal("10015"),
                ),
            ],
            ticks_with_fork=18,
            ticks_with_indicators=20,
            ticks_with_action=6,
        ),
    )

    # Paper-mode strategies short-circuit before the action row, so we don't
    # strictly need to mock ``check_system_health`` - but do it anyway for
    # determinism (the health module itself calls out to REST).
    with (
        patch(
            "almanak.framework.dashboard.data_source.get_strategy_details",
            return_value=None,
        ),
        patch(
            "almanak.framework.dashboard.pages.detail.check_system_health"
        ) as mock_health,
    ):
        mock_health.side_effect = AssertionError(
            "check_system_health must not be called for paper-mode strategies"
        )
        page([strategy])


# ---------------------------------------------------------------------------
# AppTest smoke tests
# ---------------------------------------------------------------------------


def _all_markdown_text(at: AppTest) -> str:
    """Concatenate every markdown block into one string for substring assertions."""
    return " ".join(md.value for md in at.markdown)


def _button_labels(at: AppTest) -> list[str]:
    """Return the label of every button rendered on the page."""
    return [b.label for b in at.button]


def test_apptest_no_deployment_id_query_param_renders_selectbox() -> None:
    """Empty ``deployment_id`` query param triggers the select-box empty-state."""
    at = AppTest.from_function(_drive_no_deployment_id).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    # The helper prompt and the selectbox both render.
    text = _all_markdown_text(at)
    assert "select a strategy" in text.lower()
    # Exactly one selectbox is rendered with the documented prompt.
    selectbox_labels = [sb.label for sb in at.selectbox]
    assert "Choose a strategy" in selectbox_labels, (
        f"Expected select box labelled 'Choose a strategy', got {selectbox_labels!r}"
    )
    # And the ``View Details`` button appears alongside it.
    assert "View Details" in _button_labels(at)


def test_apptest_running_lp_strategy_renders_pause_button() -> None:
    """Running strategy -> the Pause button is rendered by the action row."""
    at = AppTest.from_function(_drive_running_lp).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    # The action row uses a ``⏸️ Pause`` label; assert via substring so the
    # test is resilient to the leading emoji codepoint.
    labels = _button_labels(at)
    assert any("Pause" in label for label in labels), (
        f"Expected Pause button in running-state action row, got {labels!r}"
    )
    # A running strategy must NOT render the Resume path.
    assert not any("Resume" in label for label in labels), (
        f"Resume button should NOT appear for RUNNING strategy, got {labels!r}"
    )


def test_apptest_paused_strategy_renders_resume_button() -> None:
    """Paused strategy -> the Resume button is rendered by the action row."""
    at = AppTest.from_function(_drive_paused).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    labels = _button_labels(at)
    assert any("Resume" in label for label in labels), (
        f"Expected Resume button for PAUSED strategy, got {labels!r}"
    )
    # The Pause button must NOT appear when paused.
    assert not any(
        label.strip().startswith("\u23f8") or label == "\u23f8\ufe0f Pause"
        for label in labels
    ), f"Pause button should NOT appear for PAUSED strategy, got {labels!r}"


def test_apptest_stuck_strategy_renders_bump_gas_button() -> None:
    """STUCK strategy -> the Bump Gas button is rendered by the action row."""
    at = AppTest.from_function(_drive_stuck).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    labels = _button_labels(at)
    assert any("Bump Gas" in label for label in labels), (
        f"Expected Bump Gas button for STUCK strategy, got {labels!r}"
    )


def test_apptest_paper_mode_strategy_renders_session_summary() -> None:
    """Paper-mode strategy -> the Session Summary section renders."""
    at = AppTest.from_function(_drive_paper_mode).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    text = _all_markdown_text(at)
    # Section heading emitted by ``render_paper_session_detail``.
    assert "Session Summary" in text, (
        f"Expected 'Session Summary' heading for paper-mode strategy; "
        f"markdown blocks were: {[md.value for md in at.markdown]!r}"
    )
    # Paper-mode short-circuits before the action row, so Pause/Resume/Bump
    # Gas must NOT appear.
    labels = _button_labels(at)
    assert not any("Pause" in label for label in labels), (
        f"Paper-mode strategy must not show Pause button, got {labels!r}"
    )
    assert not any("Resume" in label for label in labels), (
        f"Paper-mode strategy must not show Resume button, got {labels!r}"
    )
    assert not any("Bump Gas" in label for label in labels), (
        f"Paper-mode strategy must not show Bump Gas button, got {labels!r}"
    )
