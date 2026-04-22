"""Regression tests for per-event chain resolution in explorer links (#1733).

The transaction-detail renderer in ``pages/detail.render_timeline_events``
builds block-explorer links for each event's ``tx_hash``. Multi-chain
strategies emit events on different chains, so the chain used to build the
explorer URL must be resolved *per event*, not pulled from
``strategy.chain`` for every row.

Pre-fix priority (buggy): ``event.chain or strategy.chain or "arbitrum"``.
Post-fix priority (#1733):
    1. ``event.chain`` (typed attribute).
    2. ``event.details["chain"]`` (legacy / free-form bag).
    3. ``strategy.chain`` (single-chain fallback).
    4. ``"arbitrum"`` (last-resort default).

These tests exercise the full renderer via AppTest so the actual rendered
``<a href=...>`` URLs can be asserted against the BLOCK_EXPLORER table.
"""

from __future__ import annotations

from streamlit.testing.v1 import AppTest


# ---------------------------------------------------------------------------
# Driver helpers. Must be top-level and self-contained - AppTest pickles them.
# ---------------------------------------------------------------------------


def _drive_event_chain_wins_over_strategy_chain() -> None:
    """``event.chain`` populated -> explorer URL uses that chain."""
    from datetime import UTC, datetime
    from decimal import Decimal

    from almanak.framework.dashboard.models import (
        Strategy,
        StrategyStatus,
        TimelineEvent,
        TimelineEventType,
    )
    from almanak.framework.dashboard.pages.detail import render_timeline_events

    strategy = Strategy(
        id="s",
        name="Multi-chain strat",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("0"),
        chain="arbitrum",  # strategy's primary chain
        protocol="Uniswap V3",
        timeline_events=[
            TimelineEvent(
                timestamp=datetime(2025, 4, 1, 12, 0, tzinfo=UTC),
                event_type=TimelineEventType.SWAP,
                description="swap on Base",
                chain="base",  # per-event chain (wins)
                details={
                    "correlation_id": "intent-1",
                    "intent_description": "Swap USDC->WETH",
                    "execution_event": "TX_CONFIRMED",
                    "tx_hash": "0xabc0000000000000000000000000000000000000000000000000000000000001",
                    "block_number": 100,
                    "gas_used": 21000,
                },
            )
        ],
    )
    render_timeline_events(strategy)


def _drive_details_chain_used_when_event_chain_missing() -> None:
    """``event.chain`` None, ``event.details["chain"]`` present -> details wins."""
    from datetime import UTC, datetime
    from decimal import Decimal

    from almanak.framework.dashboard.models import (
        Strategy,
        StrategyStatus,
        TimelineEvent,
        TimelineEventType,
    )
    from almanak.framework.dashboard.pages.detail import render_timeline_events

    strategy = Strategy(
        id="s",
        name="Legacy event strat",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("0"),
        chain="arbitrum",  # strategy primary chain (should NOT be used)
        protocol="Uniswap V3",
        timeline_events=[
            TimelineEvent(
                timestamp=datetime(2025, 4, 1, 12, 0, tzinfo=UTC),
                event_type=TimelineEventType.SWAP,
                description="swap on Optimism",
                chain=None,  # typed field missing (legacy event)
                details={
                    "correlation_id": "intent-2",
                    "intent_description": "Legacy swap",
                    "execution_event": "TX_CONFIRMED",
                    "tx_hash": "0xabc0000000000000000000000000000000000000000000000000000000000002",
                    "chain": "optimism",  # free-form bag carries the chain
                    "block_number": 200,
                    "gas_used": 21000,
                },
            )
        ],
    )
    render_timeline_events(strategy)


def _drive_strategy_chain_used_when_event_has_no_chain_info() -> None:
    """No per-event chain anywhere -> fall back to ``strategy.chain``."""
    from datetime import UTC, datetime
    from decimal import Decimal

    from almanak.framework.dashboard.models import (
        Strategy,
        StrategyStatus,
        TimelineEvent,
        TimelineEventType,
    )
    from almanak.framework.dashboard.pages.detail import render_timeline_events

    strategy = Strategy(
        id="s",
        name="Single-chain strat",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("0"),
        chain="polygon",
        protocol="Uniswap V3",
        timeline_events=[
            TimelineEvent(
                timestamp=datetime(2025, 4, 1, 12, 0, tzinfo=UTC),
                event_type=TimelineEventType.SWAP,
                description="swap",
                chain=None,
                details={
                    "correlation_id": "intent-3",
                    "intent_description": "Plain swap",
                    "execution_event": "TX_CONFIRMED",
                    "tx_hash": "0xabc0000000000000000000000000000000000000000000000000000000000003",
                    "block_number": 300,
                    "gas_used": 21000,
                },
            )
        ],
    )
    render_timeline_events(strategy)


# ---------------------------------------------------------------------------
# AppTest assertions
# ---------------------------------------------------------------------------


def _all_markdown_text(at: AppTest) -> str:
    return " ".join(md.value for md in at.markdown)


def test_explorer_url_prefers_event_chain_over_strategy_chain() -> None:
    """Regression for #1733: typed ``event.chain`` wins over ``strategy.chain``."""
    at = AppTest.from_function(_drive_event_chain_wins_over_strategy_chain).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    text = _all_markdown_text(at)
    # Base block explorer - not Arbiscan (strategy.chain).
    assert "basescan.org/tx/" in text
    assert "arbiscan.io/tx/" not in text


def test_explorer_url_falls_back_to_details_chain_when_event_chain_missing() -> None:
    """Regression for #1733: ``event.details['chain']`` is consulted before strategy.chain."""
    at = AppTest.from_function(_drive_details_chain_used_when_event_chain_missing).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    text = _all_markdown_text(at)
    # Optimism explorer - pulled from details["chain"].
    assert "optimistic.etherscan.io/tx/" in text
    # NOT the strategy's primary chain (arbitrum).
    assert "arbiscan.io/tx/" not in text


def test_explorer_url_falls_back_to_strategy_chain_when_event_has_no_chain() -> None:
    """Single-chain strategies with no per-event chain info use strategy.chain."""
    at = AppTest.from_function(_drive_strategy_chain_used_when_event_has_no_chain_info).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    text = _all_markdown_text(at)
    assert "polygonscan.com/tx/" in text
