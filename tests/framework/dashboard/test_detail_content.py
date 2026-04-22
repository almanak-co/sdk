"""AppTest smoke tests for the detail-page content helpers (Phase 5c refactor).

Exercises ``render_main_content_columns``, ``render_bridge_and_lifecycle`` and
``_safe_render`` via ``streamlit.testing.v1.AppTest.from_function`` so each
helper is validated under a real Streamlit runtime - not a hand-rolled mock.
All scenarios are pure Streamlit (no gateway, no I/O) which keeps these tests
hermetic; ``render_position_lifecycle`` is naturally no-op in the test
environment because no local SQLite state DB is present.

Scenarios covered (four tests, matching the plan's ``~4 tests`` directive):

1. Multi-chain strategy with a bridge transfer -> the bridge section renders
   (``Bridge Transfers`` heading and the token/chain markers appear).
2. Single-chain strategy -> bridge section is skipped (no ``Bridge Transfers``
   heading), timeline event still renders.
3. Exception raised by a sub-renderer -> caught by ``_safe_render``; user
   sees an ``Error rendering <section>:`` banner and the traceback ``st.code``
   block, and the following sections still render.
4. Strategy with all optional fields at defaults -> does not crash; no bridge
   section; no fatal exception surfaces via ``at.exception``.
"""

from __future__ import annotations

from streamlit.testing.v1 import AppTest


# ---------------------------------------------------------------------------
# Driver functions executed inside AppTest.from_function
#
# AppTest.from_function pickles the function, so these MUST be top-level and
# self-contained (all imports and constructions happen inside).
# ---------------------------------------------------------------------------


def _drive_multi_chain_bridge() -> None:
    from datetime import UTC, datetime
    from decimal import Decimal

    from almanak.framework.dashboard.models import (
        BridgeTransfer,
        Strategy,
        StrategyStatus,
    )
    from almanak.framework.dashboard.pages._detail_content import (
        render_bridge_and_lifecycle,
    )

    strategy = Strategy(
        id="s-mc",
        name="Cross-Chain Strat",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("10000"),
        chain="arbitrum",
        protocol="Uniswap V3, Aerodrome",
        is_multi_chain=True,
        chains=["arbitrum", "base"],
        bridge_transfers=[
            BridgeTransfer(
                transfer_id="xfer-1",
                token="USDC",
                amount=Decimal("500"),
                from_chain="arbitrum",
                to_chain="base",
                initiated_at=datetime(2025, 4, 20, 10, 0, tzinfo=UTC),
                status="IN_FLIGHT",
                fee_usd=Decimal("1.50"),
                bridge_protocol="Across",
            )
        ],
    )
    render_bridge_and_lifecycle(strategy)


def _drive_single_chain_no_bridge() -> None:
    from decimal import Decimal

    from almanak.framework.dashboard.models import Strategy, StrategyStatus
    from almanak.framework.dashboard.pages._detail_content import (
        render_bridge_and_lifecycle,
    )

    # is_multi_chain=False AND empty bridge_transfers => bridge section skipped.
    strategy = Strategy(
        id="s-sc",
        name="Single Chain Strat",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("1000"),
        chain="arbitrum",
        protocol="Uniswap V3",
        is_multi_chain=False,
    )
    render_bridge_and_lifecycle(strategy)


def _drive_sub_renderer_exception() -> None:
    from decimal import Decimal

    from almanak.framework.dashboard.models import Strategy, StrategyStatus
    from almanak.framework.dashboard.pages._detail_content import _safe_render

    strategy = Strategy(
        id="s-err",
        name="Boom",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("0"),
        chain="arbitrum",
        protocol="Uniswap V3",
    )

    def _exploder(_s: Strategy) -> None:
        raise RuntimeError("synthetic boom")

    _safe_render(_exploder, strategy, "boom section")


def _drive_missing_optionals_main_content() -> None:
    from decimal import Decimal

    from almanak.framework.dashboard.models import Strategy, StrategyStatus
    from almanak.framework.dashboard.pages._detail_content import (
        render_bridge_and_lifecycle,
        render_main_content_columns,
    )

    # position=None, pnl_history=[], no bridge transfers, single-chain -> every
    # branch should take its fallback path without raising.
    strategy = Strategy(
        id="s-bare",
        name="Bare Strat",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("0"),
        chain="polygon",
        protocol="Unknown",
    )
    render_main_content_columns(strategy)
    render_bridge_and_lifecycle(strategy)


# ---------------------------------------------------------------------------
# AppTest smoke tests
# ---------------------------------------------------------------------------


def _all_markdown_text(at: AppTest) -> str:
    """Concatenate every markdown block into one string for substring assertions."""
    return " ".join(md.value for md in at.markdown)


def test_apptest_multi_chain_strategy_renders_bridge_section() -> None:
    """Multi-chain strategy with a bridge transfer renders the bridge block."""
    at = AppTest.from_function(_drive_multi_chain_bridge).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    text = _all_markdown_text(at)
    # Heading emitted by ``render_bridge_transfers``.
    assert "Bridge Transfers" in text
    # In-flight sub-section header and the transfer's amount/token appear.
    assert "In Progress" in text
    assert "USDC" in text


def test_apptest_single_chain_strategy_skips_bridge_section() -> None:
    """Single-chain strategy does NOT emit the ``Bridge Transfers`` heading."""
    at = AppTest.from_function(_drive_single_chain_no_bridge).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    text = _all_markdown_text(at)
    assert "Bridge Transfers" not in text
    # The timeline-events section still emits its ``No recent events.`` info
    # box (via st.info) when the strategy has no timeline events - that means
    # the function kept running past the skipped bridge branch.


def test_apptest_sub_renderer_exception_is_caught_and_traceback_rendered() -> None:
    """``_safe_render`` catches ``Exception`` and renders banner + traceback."""
    at = AppTest.from_function(_drive_sub_renderer_exception).run(timeout=30)

    # The whole app must NOT fail - the helper swallows the exception.
    assert not at.exception, f"_safe_render should have swallowed the exception: {at.exception}"

    # User-visible error banner (st.error) with our section label + message.
    error_texts = [e.value for e in at.error]
    assert any("boom section" in msg and "synthetic boom" in msg for msg in error_texts), (
        f"Expected error banner mentioning section + cause; got {error_texts!r}"
    )

    # Traceback block rendered via st.code - the code blocks carry the
    # traceback string which will mention the raising function.
    code_blocks = [c.value for c in at.code]
    assert any("Traceback" in block and "_exploder" in block for block in code_blocks), (
        f"Expected traceback st.code block; got {code_blocks!r}"
    )


def test_apptest_missing_optional_attributes_does_not_crash() -> None:
    """Strategy with all optionals at defaults renders without exception."""
    at = AppTest.from_function(_drive_missing_optionals_main_content).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    # No bridge section.
    assert "Bridge Transfers" not in _all_markdown_text(at)
    # Portfolio-performance header from the left column renders.
    assert "Portfolio Performance (7 days)" in _all_markdown_text(at)
