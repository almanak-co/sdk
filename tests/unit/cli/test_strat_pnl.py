"""Tests for ``almanak strat pnl`` CLI (VIB-3206).

These tests seed a real SQLite state DB via the same backend the runner uses,
then invoke the CLI command in-process through Click's ``CliRunner``. That
way we exercise the real read path end-to-end — no mocks.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from click.testing import CliRunner

from almanak.framework.cli.strat_pnl import (
    PnLBreakdown,
    compute_pnl_breakdown,
    render_text,
    strat_pnl,
)
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.observability.position_events import (
    PositionEvent,
    PositionEventType,
    PositionType,
)
from almanak.framework.portfolio.models import (
    PortfolioMetrics,
    PortfolioSnapshot,
    PositionValue,
    ValueConfidence,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

# teardown.models.PositionType is the snapshot-position enum (SUPPLY/BORROW);
# aliased to avoid clashing with the observability PositionEvents PositionType
# imported above.
from almanak.framework.teardown.models import PositionType as SnapshotPositionType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


DEPLOYMENT_ID = "uniswap_rsi:ab12cd34ef56"


def _ts() -> datetime:
    return datetime.now(UTC)


async def _seed_store(
    db_path: Path,
    *,
    metrics: PortfolioMetrics | None,
    ledger: list[LedgerEntry],
    events: list[PositionEvent],
    snapshot: PortfolioSnapshot | None,
) -> None:
    """Create a SQLite DB and populate it with the given fixtures."""
    store = SQLiteStore(SQLiteConfig(db_path=str(db_path)))
    await store.initialize()
    try:
        if metrics is not None:
            await store.save_portfolio_metrics(metrics)
        for entry in ledger:
            await store.save_ledger_entry(entry)
        for event in events:
            await store.save_position_event(event)
        if snapshot is not None:
            await store.save_portfolio_snapshot(snapshot)
    finally:
        await store.close()


def _make_metrics(
    *,
    initial: str,
    total: str,
    gas: str,
    deposits: str = "0",
    withdrawals: str = "0",
) -> PortfolioMetrics:
    return PortfolioMetrics(
        deployment_id=DEPLOYMENT_ID,
        timestamp=_ts(),
        total_value_usd=Decimal(total),
        initial_value_usd=Decimal(initial),
        deposits_usd=Decimal(deposits),
        withdrawals_usd=Decimal(withdrawals),
        gas_spent_usd=Decimal(gas),
    )


def _make_swap_ledger(
    *,
    token_in: str,
    amount_in: str,
    token_out: str,
    amount_out: str,
    gas_usd: str,
    slippage_bps: float | None,
    chain: str = "arbitrum",
) -> LedgerEntry:
    return LedgerEntry(
        deployment_id=DEPLOYMENT_ID,
        timestamp=_ts(),
        intent_type="SWAP",
        token_in=token_in,
        amount_in=amount_in,
        token_out=token_out,
        amount_out=amount_out,
        slippage_bps=slippage_bps,
        gas_used=100000,
        gas_usd=gas_usd,
        chain=chain,
        protocol="uniswap_v3",
        success=True,
    )


def _make_position_event(
    *,
    position_id: str,
    event_type: PositionEventType,
    attribution_net_pnl_usd: str | None = None,
    timestamp: datetime | None = None,
    protocol_fees_usd: str = "",
) -> PositionEvent:
    attribution_json = "{}"
    if attribution_net_pnl_usd is not None:
        attribution_json = json.dumps({"version": 1, "position_type": "LP", "net_pnl_usd": attribution_net_pnl_usd})
    event = PositionEvent(
        deployment_id=DEPLOYMENT_ID,
        position_id=position_id,
        position_type=PositionType.LP.value,
        event_type=event_type.value,
        protocol="uniswap_v3",
        chain="arbitrum",
        attribution_json=attribution_json,
        attribution_version=1 if attribution_net_pnl_usd is not None else 0,
        protocol_fees_usd=protocol_fees_usd,
    )
    # Allow callers to pin an explicit timestamp so event-ordering tests
    # don't depend on datetime.now() ticking between rapid calls.
    if timestamp is not None:
        event.timestamp = timestamp
    return event


# ---------------------------------------------------------------------------
# compute_pnl_breakdown — pure-function tests with no DB round-trip
# ---------------------------------------------------------------------------


def test_compute_pnl_uses_metrics_for_gross_and_net() -> None:
    metrics = _make_metrics(initial="1000", total="1123.45", gas="4.12")

    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=[],
        position_events=[],
        snapshot=None,
    )

    assert result.gross_pnl_usd == Decimal("123.45")
    assert result.net_pnl_usd == Decimal("119.33")  # 123.45 - 4.12


def test_compute_pnl_aggregates_gas_from_ledger() -> None:
    metrics = _make_metrics(initial="1000", total="1000", gas="0")
    ledger = [
        _make_swap_ledger(
            token_in="WETH",
            amount_in="0.5",
            token_out="USDC",
            amount_out="1000",
            gas_usd="1.50",
            slippage_bps=None,
        ),
        _make_swap_ledger(
            token_in="USDC",
            amount_in="500",
            token_out="WETH",
            amount_out="0.24",
            gas_usd="2.62",
            slippage_bps=None,
        ),
    ]

    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=ledger,
        position_events=[],
        snapshot=None,
    )

    assert result.gas_usd == Decimal("4.12")
    assert result.trade_count == 2


def test_compute_pnl_aggregates_slippage_when_stablecoin_leg_present() -> None:
    metrics = _make_metrics(initial="1000", total="1000", gas="0")
    ledger = [
        # USDC -> WETH: notional = 500 USDC, slippage = 20bps -> 1.00 USD
        _make_swap_ledger(
            token_in="USDC",
            amount_in="500",
            token_out="WETH",
            amount_out="0.24",
            gas_usd="0",
            slippage_bps=20,
        ),
        # WETH -> USDC: notional = 300 USDC, slippage = 10bps -> 0.30 USD
        _make_swap_ledger(
            token_in="WETH",
            amount_in="0.15",
            token_out="USDC",
            amount_out="300",
            gas_usd="0",
            slippage_bps=10,
        ),
        # slippage_bps=None -> skipped
        _make_swap_ledger(
            token_in="USDC",
            amount_in="100",
            token_out="WETH",
            amount_out="0.05",
            gas_usd="0",
            slippage_bps=None,
        ),
    ]

    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=ledger,
        position_events=[],
        snapshot=None,
    )

    assert result.slippage_usd == Decimal("1.30")


def test_compute_pnl_slippage_none_when_no_stable_leg() -> None:
    """Swap between two non-stables with no USD anchor can't contribute."""
    metrics = _make_metrics(initial="1000", total="1000", gas="0")
    ledger = [
        _make_swap_ledger(
            token_in="WETH",
            amount_in="0.5",
            token_out="WBTC",
            amount_out="0.025",
            gas_usd="1.50",
            slippage_bps=15,
        ),
    ]

    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=ledger,
        position_events=[],
        snapshot=None,
    )

    assert result.slippage_usd is None


def test_compute_pnl_position_stats_win_rate_and_counts() -> None:
    metrics = _make_metrics(initial="1000", total="1200", gas="10")
    # SQLiteStore.get_position_events returns rows newest-first; mirror that
    # here so the test matches production ordering.
    events = [
        # pos4 is still open — no CLOSE recorded yet
        _make_position_event(position_id="pos4", event_type=PositionEventType.OPEN),
        # pos3: CLOSE (newest) then OPEN
        _make_position_event(
            position_id="pos3",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="15.0",  # win
        ),
        _make_position_event(position_id="pos3", event_type=PositionEventType.OPEN),
        # pos2: CLOSE (newest) then OPEN
        _make_position_event(
            position_id="pos2",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="-20.0",  # loss
        ),
        _make_position_event(position_id="pos2", event_type=PositionEventType.OPEN),
        # pos1: CLOSE (newest) then OPEN
        _make_position_event(
            position_id="pos1",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="50.0",  # win
        ),
        _make_position_event(position_id="pos1", event_type=PositionEventType.OPEN),
    ]

    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=[],
        position_events=[e.to_dict() for e in events],
        snapshot=None,
    )

    assert result.closed_positions == 3
    assert result.open_positions == 1
    assert result.wins == 2
    assert result.win_rate is not None
    # 2/3 wins = 66.67%
    assert Decimal("66") < result.win_rate < Decimal("67")


def test_compute_pnl_position_stats_reopen_reports_position_as_open() -> None:
    """Reopen under same position_id must not be miscounted as closed.

    Sequence (newest-first, matching SQLiteStore order):
      OPEN(pos1)  <- newest event, the position is currently OPEN
      CLOSE(pos1) with -30 PnL (old lifecycle, should NOT leak into win rate)
      OPEN(pos1)
    """
    metrics = _make_metrics(initial="1000", total="1000", gas="0")
    events = [
        # Latest event: OPEN => position is currently open, not counted as a loss
        _make_position_event(position_id="pos1", event_type=PositionEventType.OPEN),
        _make_position_event(
            position_id="pos1",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="-30.0",  # stale attribution — must not leak
        ),
        _make_position_event(position_id="pos1", event_type=PositionEventType.OPEN),
    ]

    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=[],
        position_events=[e.to_dict() for e in events],
        snapshot=None,
    )

    assert result.closed_positions == 0
    assert result.open_positions == 1
    # No closed events means no win-rate to report.
    assert result.wins == 0
    assert result.win_rate is None


def test_compute_pnl_non_lifecycle_events_preserve_prior_state() -> None:
    """CodeRabbit audit fix: SNAPSHOT / COLLECT_FEES events after a CLOSE
    must NOT flip the position back to "open" in the CLI report.

    Sequence (newest-first):
      SNAPSHOT(pos1)  <- newest, non-lifecycle — must NOT determine state
      CLOSE(pos1) with 50 PnL — this is the TRUE latest lifecycle state
      OPEN(pos1)

    Before the fix, the classifier saw SNAPSHOT first, mapped its
    (non-CLOSE) event_type to ``closed=False``, and reported the
    position as currently OPEN — masking the real CLOSE and throwing
    off win-rate stats.
    """
    metrics = _make_metrics(initial="1000", total="1050", gas="0")
    events = [
        _make_position_event(position_id="pos1", event_type=PositionEventType.SNAPSHOT),
        _make_position_event(
            position_id="pos1",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="50.0",
        ),
        _make_position_event(position_id="pos1", event_type=PositionEventType.OPEN),
    ]

    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=[],
        position_events=[e.to_dict() for e in events],
        snapshot=None,
    )

    # The CLOSE is the true lifecycle state — position MUST be counted closed.
    assert result.closed_positions == 1
    assert result.open_positions == 0
    assert result.wins == 1
    assert result.win_rate == Decimal("100")


# ---------------------------------------------------------------------------
# VIB-3493 — strategy-level LP rebalance attribution wired into strat_pnl
# ---------------------------------------------------------------------------


def _make_lp_event_with_gas(
    *,
    position_id: str,
    event_type: PositionEventType,
    gas_usd: str,
    timestamp: datetime | None = None,
) -> PositionEvent:
    """Factory for LP events that carry a gas value (PnL-strategy tests)."""
    event = PositionEvent(
        deployment_id=DEPLOYMENT_ID,
        position_id=position_id,
        position_type=PositionType.LP.value,
        event_type=event_type.value,
        protocol="uniswap_v3",
        chain="arbitrum",
        gas_usd=gas_usd,
        attribution_json="{}",
        attribution_version=0,
    )
    if timestamp is not None:
        event.timestamp = timestamp
    return event


def test_compute_pnl_lp_strategy_attribution_populated_for_multi_rebalance() -> None:
    """Multi-rebalance LP strategy: lp_*_gas_usd + lp_close_open_pairs surfaced."""
    metrics = _make_metrics(initial="1000", total="1000", gas="0")
    base = datetime(2026, 5, 1, tzinfo=UTC)
    events = [
        _make_lp_event_with_gas(
            position_id="A",
            event_type=PositionEventType.OPEN,
            gas_usd="5",
            timestamp=base,
        ),
        _make_lp_event_with_gas(
            position_id="A",
            event_type=PositionEventType.CLOSE,
            gas_usd="3",
            timestamp=base + timedelta(minutes=1),
        ),
        _make_lp_event_with_gas(
            position_id="B",
            event_type=PositionEventType.OPEN,
            gas_usd="5",
            timestamp=base + timedelta(minutes=1, seconds=1),
        ),
        _make_lp_event_with_gas(
            position_id="B",
            event_type=PositionEventType.CLOSE,
            gas_usd="3",
            timestamp=base + timedelta(minutes=2),
        ),
    ]

    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=[],
        position_events=[e.to_dict() for e in events],
        snapshot=None,
    )

    # 2 OPENs + 2 CLOSEs, with one CLOSE→OPEN rebalance pair (A close → B open)
    assert result.lp_open_count == 2
    assert result.lp_close_count == 2
    assert result.lp_close_open_pairs == 1
    assert result.lp_total_gas_usd == Decimal("16")  # 5 + 3 + 5 + 3
    assert result.lp_open_gas_usd == Decimal("10")
    assert result.lp_close_gas_usd == Decimal("6")


def test_compute_pnl_lp_strategy_attribution_absent_for_non_lp_strategy() -> None:
    """Non-LP strategies (no LP events) keep lp_* fields at None / 0."""
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=_make_metrics(initial="1000", total="1000", gas="0"),
        ledger_entries=[],
        position_events=[],
        snapshot=None,
    )

    assert result.lp_total_gas_usd is None
    assert result.lp_open_gas_usd is None
    assert result.lp_close_gas_usd is None
    assert result.lp_open_count == 0
    assert result.lp_close_count == 0
    assert result.lp_close_open_pairs == 0


def test_amount_in_usd_resolves_address_tokens_via_token_resolver(monkeypatch) -> None:
    """Address-form tokens must route through TokenResolver for stablecoin detection."""

    # USDC on Arbitrum (real address) — _amount_in_usd should resolve this to
    # the USDC symbol and match the stablecoin heuristic.
    usdc_arb = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

    ledger_entry = _make_swap_ledger(
        token_in=usdc_arb,
        amount_in="250",
        token_out="WETH",
        amount_out="0.12",
        gas_usd="0",
        slippage_bps=40,
    )
    # Ensure the entry carries a chain so the resolver has context.
    object.__setattr__(ledger_entry, "chain", "arbitrum")

    metrics = _make_metrics(initial="1000", total="1000", gas="0")
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=[ledger_entry],
        position_events=[],
        snapshot=None,
    )

    # 250 * 40bps / 10000 = 1.00 USD
    assert result.slippage_usd == Decimal("1.00")
    # Avg trade size anchored on the resolved USDC notional
    assert result.avg_trade_size_usd == Decimal("250")


def test_amount_in_usd_falls_back_when_resolver_fails(monkeypatch) -> None:
    """Unknown address without resolver coverage must not crash; slippage skipped."""
    from almanak.framework.cli import strat_pnl as sp_module

    class _StubResolver:
        def resolve(self, *args, **kwargs):
            raise RuntimeError("resolver down in test")

    monkeypatch.setattr(
        "almanak.framework.data.tokens.get_token_resolver",
        lambda: _StubResolver(),
    )

    ledger_entry = _make_swap_ledger(
        token_in="0x0000000000000000000000000000000000000001",  # bogus address
        amount_in="500",
        token_out="0x0000000000000000000000000000000000000002",
        amount_out="0.25",
        gas_usd="0",
        slippage_bps=25,
    )
    object.__setattr__(ledger_entry, "chain", "arbitrum")

    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=_make_metrics(initial="1000", total="1000", gas="0"),
        ledger_entries=[ledger_entry],
        position_events=[],
        snapshot=None,
    )

    # Neither leg looks like a stablecoin post-resolver-failure → slippage None.
    assert result.slippage_usd is None
    assert sp_module is not None  # keep import live


def test_compute_pnl_placeholders_for_missing_protocol_fees_and_il() -> None:
    """No events / no fee data -> protocol fees + IL stay None (Empty≠Zero)."""
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=_make_metrics(initial="1000", total="1100", gas="5"),
        ledger_entries=[],
        position_events=[],
        snapshot=None,
    )

    assert result.protocol_fees_usd is None
    assert result.impermanent_loss_usd is None


# ---------------------------------------------------------------------------
# VIB-4846 (T6) — protocol-fee roll-up honoring Empty≠Zero
# ---------------------------------------------------------------------------


def test_compute_pnl_protocol_fees_sums_only_measured_values() -> None:
    """Mixed protocol_fees_usd ('', '0', '1.25') sums only measured values.

    '' = unmeasured (skipped), '0' = measured zero (counts as 0), '1.25' =
    measured. Total = 0 + 1.25 = 1.25; presence of at least one measured value
    means the top line is non-None.
    """
    events = [
        _make_position_event(
            position_id="p1",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="10.0",
            protocol_fees_usd="",  # unmeasured — must NOT count
        ),
        _make_position_event(
            position_id="p2",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="5.0",
            protocol_fees_usd="0",  # measured zero — counts as 0
        ),
        _make_position_event(
            position_id="p3",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="20.0",
            protocol_fees_usd="1.25",  # measured
        ),
    ]
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=_make_metrics(initial="1000", total="1100", gas="0"),
        ledger_entries=[],
        position_events=[e.to_dict() for e in events],
        snapshot=None,
    )
    assert result.protocol_fees_usd == Decimal("1.25")


def test_compute_pnl_protocol_fees_none_when_all_unmeasured() -> None:
    """All-empty protocol_fees_usd -> top line stays None (Empty≠Zero)."""
    events = [
        _make_position_event(
            position_id="p1",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="10.0",
            protocol_fees_usd="",
        ),
        _make_position_event(
            position_id="p2",
            event_type=PositionEventType.OPEN,
            protocol_fees_usd="",
        ),
    ]
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=_make_metrics(initial="1000", total="1100", gas="0"),
        ledger_entries=[],
        position_events=[e.to_dict() for e in events],
        snapshot=None,
    )
    assert result.protocol_fees_usd is None


def test_compute_pnl_protocol_fees_measured_zero_only_is_zero_not_none() -> None:
    """A lone measured-zero fee -> top line is Decimal('0'), not None."""
    events = [
        _make_position_event(
            position_id="p1",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="10.0",
            protocol_fees_usd="0",
        ),
    ]
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=_make_metrics(initial="1000", total="1100", gas="0"),
        ledger_entries=[],
        position_events=[e.to_dict() for e in events],
        snapshot=None,
    )
    assert result.protocol_fees_usd == Decimal("0")


def test_compute_pnl_protocol_fees_parity_with_lp_report() -> None:
    """Top-line protocol fees equal the LP report's per-position total.

    Cross-checks the new strat_pnl roll-up against the already-correct
    lp_report aggregation (lp_report.py:137) over the same loaded dicts.
    """
    from almanak.framework.accounting.reporting import build_lp_report
    from almanak.framework.accounting.reporting.loader import AccountingData

    events = [
        _make_position_event(
            position_id="lp1",
            event_type=PositionEventType.COLLECT_FEES,
            protocol_fees_usd="0.30",
        ),
        _make_position_event(
            position_id="lp1",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="12.0",
            protocol_fees_usd="0.45",
        ),
        _make_position_event(
            position_id="lp2",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="8.0",
            protocol_fees_usd="0.50",
        ),
    ]
    event_dicts = [e.to_dict() for e in events]

    breakdown = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=_make_metrics(initial="1000", total="1100", gas="0"),
        ledger_entries=[],
        position_events=event_dicts,
        snapshot=None,
    )

    lp_section = build_lp_report(
        AccountingData(
            deployment_id=DEPLOYMENT_ID,
            metrics=None,
            ledger_entries=[],
            position_events=event_dicts,
            snapshot=None,
        )
    )
    lp_total = sum((p.protocol_fees_usd for p in lp_section.positions), Decimal("0"))

    # Both paths read the same column over the same dicts -> identical totals.
    assert breakdown.protocol_fees_usd == lp_total == Decimal("1.25")


# ---------------------------------------------------------------------------
# VIB-4846 (T5) — gas-efficiency ratios + total friction
# ---------------------------------------------------------------------------


def test_compute_pnl_gas_efficiency_ratios_known_values() -> None:
    """avg_gas/trade, gas-as-pct-bps and total friction on known inputs."""
    metrics = _make_metrics(initial="1000", total="1000", gas="0")
    # Two USDC-anchored swaps: notionals 500 + 300, avg trade size = 400.
    # Gas 1.00 + 3.00 = 4.00 over 2 trades -> avg gas/trade = 2.00.
    # gas-as-pct-bps = (2.00 / 400) * 10000 = 50 bps.
    ledger = [
        _make_swap_ledger(
            token_in="USDC",
            amount_in="500",
            token_out="WETH",
            amount_out="0.24",
            gas_usd="1.00",
            slippage_bps=20,
        ),
        _make_swap_ledger(
            token_in="USDC",
            amount_in="300",
            token_out="WETH",
            amount_out="0.14",
            gas_usd="3.00",
            slippage_bps=None,
        ),
    ]
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=ledger,
        position_events=[],
        snapshot=None,
    )
    assert result.gas_usd == Decimal("4.00")
    assert result.avg_trade_size_usd == Decimal("400")
    assert result.avg_gas_per_trade_usd == Decimal("2.00")
    assert result.gas_as_pct_of_avg_trade_bps == Decimal("50")
    # slippage = (20/10000)*500 = 1.00; protocol fees unmeasured (None).
    # friction = gas 4.00 + slippage 1.00 = 5.00 (protocol fees skipped).
    assert result.slippage_usd == Decimal("1.00")
    assert result.protocol_fees_usd is None
    assert result.total_friction_usd == Decimal("5.00")


def test_compute_pnl_gas_efficiency_none_safe_no_div_by_zero() -> None:
    """No gas + no trades -> derived ratios stay None; no div-by-zero."""
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=None,  # no metrics -> gas_usd stays None
        ledger_entries=[],
        position_events=[],
        snapshot=None,
    )
    assert result.gas_usd is None
    assert result.avg_gas_per_trade_usd is None
    assert result.gas_as_pct_of_avg_trade_bps is None
    assert result.total_friction_usd is None


def test_compute_pnl_total_friction_rolls_in_protocol_fees() -> None:
    """total_friction = gas + slippage + protocol_fees when all measured."""
    metrics = _make_metrics(initial="1000", total="1000", gas="0")
    ledger = [
        _make_swap_ledger(
            token_in="USDC",
            amount_in="500",
            token_out="WETH",
            amount_out="0.24",
            gas_usd="2.00",
            slippage_bps=20,
        ),
    ]
    events = [
        _make_position_event(
            position_id="p1",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="10.0",
            protocol_fees_usd="0.75",
        ),
    ]
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=ledger,
        position_events=[e.to_dict() for e in events],
        snapshot=None,
    )
    # gas 2.00 + slippage (20/10000*500)=1.00 + protocol 0.75 = 3.75
    assert result.total_friction_usd == Decimal("3.75")


def test_render_text_shows_gas_efficiency_section() -> None:
    """Gas-efficiency section renders the three derived lines."""
    from almanak.framework.cli.strat_pnl import render_text

    bd = PnLBreakdown(
        deployment_id=DEPLOYMENT_ID,
        gas_usd=Decimal("4.00"),
        avg_gas_per_trade_usd=Decimal("2.00"),
        gas_as_pct_of_avg_trade_bps=Decimal("50"),
        total_friction_usd=Decimal("5.00"),
        trade_count=2,
    )
    out = render_text(bd)
    assert "Gas efficiency" in out
    assert "Avg gas / trade:" in out
    assert "Gas % of trade:" in out
    assert "50.0 bps" in out
    assert "0.50%" in out
    assert "Total friction:" in out


def test_render_text_gas_efficiency_placeholders_when_none() -> None:
    """None inputs render the em-dash placeholder, not a misleading zero."""
    from almanak.framework.cli.strat_pnl import render_text

    bd = PnLBreakdown(deployment_id=DEPLOYMENT_ID)  # all derived fields None
    out = render_text(bd)
    assert "Avg gas / trade:  —" in out
    assert "Gas % of trade:   —" in out
    assert "Total friction:   —" in out


# ---------------------------------------------------------------------------
# VIB-4846 (T7) — win-rate transparency (conservative range)
# ---------------------------------------------------------------------------


def test_render_text_win_rate_conservative_range_on_unattributed() -> None:
    """Unattributed closes render a conservative low–high range vs total closes.

    2 scored closes (2 wins, 100% headline) + 3 unattributed = 5 total.
    low  = 2/5 = 40%; high = (2+3)/5 = 100%.
    """
    from almanak.framework.cli.strat_pnl import render_text

    bd = PnLBreakdown(
        deployment_id=DEPLOYMENT_ID,
        win_rate=Decimal("100"),
        wins=2,
        scored_closes=2,
        closed_positions=5,
    )
    out = render_text(bd)
    assert "100% (2/2 scored closes)" in out
    assert "3 unattributed — conservative range 40%–100%" in out


def test_render_text_win_rate_no_range_when_all_attributed() -> None:
    """Fully attributed closes show no range / no warning."""
    from almanak.framework.cli.strat_pnl import render_text

    bd = PnLBreakdown(
        deployment_id=DEPLOYMENT_ID,
        win_rate=Decimal("50"),
        wins=1,
        scored_closes=2,
        closed_positions=2,
    )
    out = render_text(bd)
    assert "50% (1/2 scored closes)" in out
    assert "unattributed" not in out
    assert "WARNING" not in out


def test_render_text_win_rate_warning_when_unattributed_exceeds_25pct() -> None:
    """When >25% of closes are unattributed, a loud warning is appended."""
    from almanak.framework.cli.strat_pnl import render_text

    # 1 scored close out of 4 total -> 3 unattributed = 75% > 25%.
    # render_text now reads the precomputed verdict (compute_pnl_breakdown sets
    # it), so the flag must be set on a directly-constructed breakdown too.
    bd = PnLBreakdown(
        deployment_id=DEPLOYMENT_ID,
        win_rate=Decimal("100"),
        wins=1,
        scored_closes=1,
        closed_positions=4,
        high_unattributed_win_rate=True,
    )
    out = render_text(bd)
    assert "conservative range" in out
    assert "WARNING" in out
    assert "unreliable" in out


def test_pnl_breakdown_to_json_dict_includes_t5_fields() -> None:
    """The T5 derived fields serialize (str / null)."""
    bd = PnLBreakdown(
        deployment_id=DEPLOYMENT_ID,
        avg_gas_per_trade_usd=Decimal("2.00"),
        gas_as_pct_of_avg_trade_bps=Decimal("50"),
        total_friction_usd=Decimal("5.00"),
    )
    d = bd.to_json_dict()
    assert d["avg_gas_per_trade_usd"] == "2.00"
    assert d["gas_as_pct_of_avg_trade_bps"] == "50"
    assert d["total_friction_usd"] == "5.00"

    empty = PnLBreakdown(deployment_id=DEPLOYMENT_ID)
    de = empty.to_json_dict()
    assert de["avg_gas_per_trade_usd"] is None
    assert de["gas_as_pct_of_avg_trade_bps"] is None
    assert de["total_friction_usd"] is None


# ---------------------------------------------------------------------------
# VIB-4788 — net_strategy_nav_usd (positive positions minus lending debt)
# ---------------------------------------------------------------------------


def _nav_snapshot(total: str, *positions: PositionValue) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=_ts(),
        deployment_id=DEPLOYMENT_ID,
        total_value_usd=Decimal(total),
        available_cash_usd=Decimal("0"),
        value_confidence=ValueConfidence.HIGH,
        chain="arbitrum",
        positions=list(positions),
    )


def _supply_pos(value_usd: str, asset: str = "USDC") -> PositionValue:
    return PositionValue(
        position_type=SnapshotPositionType.SUPPLY,
        protocol="aave_v3",
        chain="arbitrum",
        value_usd=Decimal(value_usd),
        label="aave_v3 SUPPLY",
        tokens=[asset],
        details={"asset": asset},
        unrealized_pnl_usd=Decimal("0"),
    )


def _borrow_pos(value_usd: str, asset: str = "USDT") -> PositionValue:
    # BORROW value_usd is signed negative (liability) by convention.
    return PositionValue(
        position_type=SnapshotPositionType.BORROW,
        protocol="aave_v3",
        chain="arbitrum",
        value_usd=Decimal(value_usd),
        label="aave_v3 BORROW",
        tokens=[asset],
        details={"asset": asset},
        unrealized_pnl_usd=Decimal("0"),
    )


def test_net_strategy_nav_equals_total_when_no_debt() -> None:
    """No BORROW positions → NAV equals total_value_usd exactly (bullet 3)."""
    snap = _nav_snapshot("1234.50", _supply_pos("1234.50"))
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=_make_metrics(initial="1000", total="1000", gas="0"),
        ledger_entries=[],
        position_events=[],
        snapshot=snap,
    )
    assert result.net_strategy_nav_usd == Decimal("1234.50")
    # total_value_usd is never mutated (additive contract).
    assert snap.total_value_usd == Decimal("1234.50")


def test_net_strategy_nav_subtracts_lending_debt() -> None:
    """Leveraged lending: NAV = total_value_usd − gross_debt (10.37 − 3.11)."""
    # total_value_usd is positive-position-scoped (VIB-3614): it counts the
    # 10.37 supply but excludes the −3.11 borrow.
    snap = _nav_snapshot("10.37", _supply_pos("10.37"), _borrow_pos("-3.11"))
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=_make_metrics(initial="1000", total="1000", gas="0"),
        ledger_entries=[],
        position_events=[],
        snapshot=snap,
    )
    assert result.net_strategy_nav_usd == Decimal("7.26")
    assert snap.total_value_usd == Decimal("10.37")


def test_net_strategy_nav_none_without_snapshot() -> None:
    """No snapshot → unmeasured (None), never a phantom zero (Empty≠Zero)."""
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=_make_metrics(initial="1000", total="1000", gas="0"),
        ledger_entries=[],
        position_events=[],
        snapshot=None,
    )
    assert result.net_strategy_nav_usd is None


def test_net_strategy_nav_serializes_in_json_dict() -> None:
    """VIB-4793: the field is present in the JSON payload (str / null)."""
    bd = PnLBreakdown(deployment_id=DEPLOYMENT_ID, net_strategy_nav_usd=Decimal("7.26"))
    assert bd.to_json_dict()["net_strategy_nav_usd"] == "7.26"
    assert PnLBreakdown(deployment_id=DEPLOYMENT_ID).to_json_dict()["net_strategy_nav_usd"] is None


def test_net_strategy_nav_renders_in_text_when_measured() -> None:
    """VIB-4788: the text surface emits a `Net strategy NAV:` line with value.

    Locks the render path directly — the compute/JSON tests would still pass
    if `render_text` stopped emitting the line.
    """
    bd = PnLBreakdown(deployment_id=DEPLOYMENT_ID, net_strategy_nav_usd=Decimal("7.26"))
    out = render_text(bd)
    assert "Net strategy NAV:" in out
    assert "7.26" in out


def test_net_strategy_nav_absent_from_text_when_unmeasured() -> None:
    """No snapshot → field None → the line is suppressed (Empty≠Zero)."""
    out = render_text(PnLBreakdown(deployment_id=DEPLOYMENT_ID))
    assert "Net strategy NAV:" not in out


# ---------------------------------------------------------------------------
# VIB-4846 (Codex review) — measured-only gas average, JSON unattributed flag,
# unmeasured-fee label
# ---------------------------------------------------------------------------


def test_avg_gas_per_trade_excludes_unmeasured_gas_rows() -> None:
    """avg_gas_per_trade averages over MEASURED-gas rows only (Empty≠Zero).

    Two swaps carry gas (1.00 + 3.00 = 4.00); two more carry no gas (""),
    so trade_count = 4 but only 2 rows have measured gas. The per-trade
    average must be 4.00 / 2 = 2.00 — NOT 4.00 / 4 = 1.00 (which would
    dilute the average with unmeasured rows).
    """
    metrics = _make_metrics(initial="1000", total="1000", gas="0")
    ledger = [
        _make_swap_ledger(
            token_in="USDC",
            amount_in="500",
            token_out="WETH",
            amount_out="0.24",
            gas_usd="1.00",
            slippage_bps=None,
        ),
        _make_swap_ledger(
            token_in="USDC",
            amount_in="500",
            token_out="WETH",
            amount_out="0.24",
            gas_usd="3.00",
            slippage_bps=None,
        ),
        # Two rows with unmeasured gas ("") — must NOT enter the denominator.
        _make_swap_ledger(
            token_in="USDC",
            amount_in="500",
            token_out="WETH",
            amount_out="0.24",
            gas_usd="",
            slippage_bps=None,
        ),
        _make_swap_ledger(
            token_in="USDC",
            amount_in="500",
            token_out="WETH",
            amount_out="0.24",
            gas_usd="",
            slippage_bps=None,
        ),
    ]
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=ledger,
        position_events=[],
        snapshot=None,
    )
    assert result.trade_count == 4
    assert result.gas_usd == Decimal("4.00")
    # 4.00 / 2 measured rows = 2.00 (not 4.00 / 4 = 1.00).
    assert result.avg_gas_per_trade_usd == Decimal("2.00")


def test_avg_gas_per_trade_none_when_gas_from_metrics_fallback() -> None:
    """Gas sourced from the metrics fallback (no per-row gas) -> avg stays None.

    No ledger row carries a measured gas value, so ``gas_usd`` falls back to
    the rolling ``metrics.gas_spent_usd`` (a strategy total with no per-row
    breakdown). There is no measured-row count to average over, so
    ``avg_gas_per_trade_usd`` MUST stay unmeasured (None) rather than dividing
    the metrics total by ``trade_count`` (which includes unmeasured rows).
    """
    metrics = _make_metrics(initial="1000", total="1000", gas="9.00")
    ledger = [
        _make_swap_ledger(
            token_in="USDC",
            amount_in="500",
            token_out="WETH",
            amount_out="0.24",
            gas_usd="",  # unmeasured
            slippage_bps=None,
        ),
        _make_swap_ledger(
            token_in="USDC",
            amount_in="500",
            token_out="WETH",
            amount_out="0.24",
            gas_usd="",  # unmeasured
            slippage_bps=None,
        ),
    ]
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=ledger,
        position_events=[],
        snapshot=None,
    )
    # gas_usd reflects the metrics fallback (strategy total) ...
    assert result.gas_usd == Decimal("9.00")
    assert result.trade_count == 2
    # ... but the per-trade average is unmeasured: no per-row gas to average.
    assert result.avg_gas_per_trade_usd is None


def test_high_unattributed_flag_set_when_over_25pct() -> None:
    """high_unattributed_win_rate flips True when >25% of closes lack attribution."""
    metrics = _make_metrics(initial="1000", total="1000", gas="0")
    # 4 closes, only 1 scored (3 unattributed = 75% > 25%).
    events = [
        _make_position_event(
            position_id="p1",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="10.0",  # scored win
        ),
        _make_position_event(position_id="p2", event_type=PositionEventType.CLOSE),  # unattributed
        _make_position_event(position_id="p3", event_type=PositionEventType.CLOSE),  # unattributed
        _make_position_event(position_id="p4", event_type=PositionEventType.CLOSE),  # unattributed
    ]
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=[],
        position_events=[e.to_dict() for e in events],
        snapshot=None,
    )
    assert result.closed_positions == 4
    assert result.scored_closes == 1
    assert result.high_unattributed_win_rate is True


def test_high_unattributed_flag_false_when_under_threshold() -> None:
    """high_unattributed_win_rate stays False when <=25% of closes are unattributed."""
    metrics = _make_metrics(initial="1000", total="1000", gas="0")
    # 4 closes, 3 scored (1 unattributed = 25%, NOT strictly > 25%).
    events = [
        _make_position_event(position_id="p1", event_type=PositionEventType.CLOSE, attribution_net_pnl_usd="10.0"),
        _make_position_event(position_id="p2", event_type=PositionEventType.CLOSE, attribution_net_pnl_usd="5.0"),
        _make_position_event(position_id="p3", event_type=PositionEventType.CLOSE, attribution_net_pnl_usd="-2.0"),
        _make_position_event(position_id="p4", event_type=PositionEventType.CLOSE),  # unattributed
    ]
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=metrics,
        ledger_entries=[],
        position_events=[e.to_dict() for e in events],
        snapshot=None,
    )
    assert result.closed_positions == 4
    assert result.scored_closes == 3
    assert result.high_unattributed_win_rate is False


def test_high_unattributed_flag_in_json_output() -> None:
    """The high-unattributed verdict is exposed in JSON for machine consumers."""
    flagged = PnLBreakdown(
        deployment_id=DEPLOYMENT_ID,
        win_rate=Decimal("100"),
        wins=1,
        scored_closes=1,
        closed_positions=4,
        high_unattributed_win_rate=True,
    )
    assert flagged.to_json_dict()["high_unattributed_win_rate"] is True

    clean = PnLBreakdown(deployment_id=DEPLOYMENT_ID)
    assert clean.to_json_dict()["high_unattributed_win_rate"] is False


def test_render_text_unmeasured_protocol_fees_label() -> None:
    """Unmeasured protocol fees render as "(unmeasured)", not "(not yet implemented)"."""
    from almanak.framework.cli.strat_pnl import render_text

    bd = PnLBreakdown(deployment_id=DEPLOYMENT_ID)  # protocol_fees_usd None
    out = render_text(bd)
    proto_line = next(ln for ln in out.splitlines() if ln.startswith("Protocol fees:"))
    assert "(unmeasured)" in proto_line
    assert "not yet implemented" not in proto_line


def test_render_text_measured_protocol_fees_no_label() -> None:
    """A measured protocol-fee total renders the money value, no label."""
    from almanak.framework.cli.strat_pnl import render_text

    bd = PnLBreakdown(deployment_id=DEPLOYMENT_ID, protocol_fees_usd=Decimal("1.25"))
    out = render_text(bd)
    proto_line = next(ln for ln in out.splitlines() if ln.startswith("Protocol fees:"))
    assert "1.25" in proto_line
    assert "unmeasured" not in proto_line
    assert "not yet implemented" not in proto_line


def test_compute_pnl_warns_when_metrics_missing() -> None:
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=None,
        ledger_entries=[],
        position_events=[],
        snapshot=None,
    )

    assert result.gross_pnl_usd is None
    assert result.net_pnl_usd is None
    assert any("PortfolioMetrics" in w for w in result.warnings)


def test_compute_pnl_picks_up_chain_from_snapshot() -> None:
    snapshot = PortfolioSnapshot(
        timestamp=_ts(),
        deployment_id=DEPLOYMENT_ID,
        total_value_usd=Decimal("1000"),
        available_cash_usd=Decimal("1000"),
        value_confidence=ValueConfidence.HIGH,
        chain="arbitrum",
    )
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=_make_metrics(initial="1000", total="1000", gas="0"),
        ledger_entries=[],
        position_events=[],
        snapshot=snapshot,
    )
    assert result.chain == "arbitrum"


# ---------------------------------------------------------------------------
# End-to-end CLI invocation via SQLite state DB
# ---------------------------------------------------------------------------


@pytest.fixture
def seeded_db(tmp_path: Path) -> Path:
    """A SQLite DB populated with realistic fixture data for DEPLOYMENT_ID."""
    db_path = tmp_path / "state.db"

    metrics = _make_metrics(initial="1000", total="1123.45", gas="4.12")
    ledger = [
        _make_swap_ledger(
            token_in="USDC",
            amount_in="412",
            token_out="WETH",
            amount_out="0.20",
            gas_usd="2.00",
            slippage_bps=20,
        ),
        _make_swap_ledger(
            token_in="WETH",
            amount_in="0.20",
            token_out="USDC",
            amount_out="412",
            gas_usd="2.12",
            slippage_bps=None,
        ),
    ]
    # Pin explicit timestamps so OPEN strictly precedes CLOSE regardless of
    # how fast the test executes. The newest-first query on ``position_events``
    # should yield CLOSE before OPEN.
    from datetime import timedelta

    open_ts = _ts()
    close_ts = open_ts + timedelta(seconds=1)
    events = [
        _make_position_event(position_id="p1", event_type=PositionEventType.OPEN, timestamp=open_ts),
        _make_position_event(
            position_id="p1",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="50.0",
            timestamp=close_ts,
        ),
    ]
    snapshot = PortfolioSnapshot(
        timestamp=_ts(),
        deployment_id=DEPLOYMENT_ID,
        total_value_usd=Decimal("1123.45"),
        available_cash_usd=Decimal("500"),
        value_confidence=ValueConfidence.HIGH,
        chain="arbitrum",
    )

    asyncio.run(_seed_store(db_path, metrics=metrics, ledger=ledger, events=events, snapshot=snapshot))
    return db_path


def test_strat_pnl_computes_breakdown_from_persisted_data(seeded_db: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        strat_pnl,
        ["-s", DEPLOYMENT_ID, "--db", str(seeded_db)],
    )

    assert result.exit_code == 0, result.output
    # Core rendered fields
    assert "Strategy:" in result.output
    assert DEPLOYMENT_ID in result.output
    assert "Chain: arbitrum" in result.output
    assert "PnL Breakdown" in result.output
    assert "Gross PnL:" in result.output
    assert "Gas costs:" in result.output
    assert "Protocol fees:" in result.output
    # Protocol-fee roll-up IS implemented; an unmeasured total renders as
    # "(unmeasured)", NOT the stale "(not yet implemented)" label (VIB-4846).
    proto_line = next(ln for ln in result.output.splitlines() if ln.startswith("Protocol fees:"))
    assert "(unmeasured)" in proto_line
    assert "not yet implemented" not in proto_line
    assert "Slippage:" in result.output
    assert "Impermanent loss:" in result.output
    assert "Net PnL:" in result.output
    assert "Win rate:" in result.output
    assert "Avg trade size:" in result.output
    assert "Trade count:" in result.output
    # Gross PnL = 123.45
    assert "$   123.45" in result.output or "123.45" in result.output
    # Net PnL = 119.33
    assert "119.33" in result.output


def test_strat_pnl_json_output(seeded_db: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        strat_pnl,
        ["-s", DEPLOYMENT_ID, "--db", str(seeded_db), "--json"],
    )

    assert result.exit_code == 0, result.output
    # Output must be valid JSON with the expected keys
    payload = json.loads(result.output)
    assert payload["deployment_id"] == DEPLOYMENT_ID
    assert payload["chain"] == "arbitrum"
    assert payload["gross_pnl_usd"] == "123.45"
    assert payload["net_pnl_usd"] == "119.33"
    # Gas: 2.00 + 2.12 = 4.12
    assert Decimal(payload["gas_usd"]) == Decimal("4.12")
    # Slippage: (20/10000) * 412 = 0.824
    assert Decimal(payload["slippage_usd"]) == Decimal("0.824")
    # Placeholders remain null
    assert payload["protocol_fees_usd"] is None
    assert payload["impermanent_loss_usd"] is None
    # Win rate: 1/1 closed -> 100%
    assert payload["closed_positions"] == 1
    assert payload["wins"] == 1
    assert Decimal(payload["win_rate"]) == Decimal("100")
    assert payload["trade_count"] == 2
    # VIB-4793: the document is version-stamped for machine consumers.
    assert payload["schema_version"] == 1
    # VIB-4788: net strategy NAV present; seeded snapshot has no debt, so it
    # equals total_value_usd (1123.45).
    assert Decimal(payload["net_strategy_nav_usd"]) == Decimal("1123.45")


# VIB-4846 — end-to-end CLI surface: protocol-fee roll-up (T6) + gas-efficiency
# section (T5) must round-trip through the real ``-s`` invocation against a
# persisted SQLite DB, not just the in-memory ``compute_pnl_breakdown`` helper.
def test_strat_pnl_protocol_fee_rollup_through_cli(tmp_path: Path) -> None:
    """Measured ``position_events.protocol_fees_usd`` rolls up in the ``-s`` CLI path.

    Seeds a real SQLite DB (via ``SQLiteStore``) with two CLOSE events carrying
    measured protocol fees (``"0.30"`` + ``"0.95"``) and one OPEN with an empty
    (unmeasured) fee, then drives the actual Click command. Asserts the rolled-up
    total surfaces in both text and JSON, and that the new T5 ``Gas efficiency``
    section renders. This closes the UAT-GATE Phase 0b gap: the persisted-column
    aggregation is validated against a DB-backed CLI invocation, not only the
    in-memory helper.
    """
    from datetime import timedelta

    db_path = tmp_path / "fees.db"
    metrics = _make_metrics(initial="1000", total="1100", gas="4.00")
    ledger = [
        _make_swap_ledger(
            token_in="USDC",
            amount_in="400",
            token_out="WETH",
            amount_out="0.18",
            gas_usd="2.00",
            slippage_bps=15,
        ),
        _make_swap_ledger(
            token_in="WETH",
            amount_in="0.18",
            token_out="USDC",
            amount_out="404",
            gas_usd="2.00",
            slippage_bps=10,
        ),
    ]
    t0 = _ts()
    events = [
        _make_position_event(position_id="p1", event_type=PositionEventType.OPEN, timestamp=t0),
        _make_position_event(
            position_id="p1",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="12.0",
            protocol_fees_usd="0.30",  # measured
            timestamp=t0 + timedelta(seconds=1),
        ),
        _make_position_event(position_id="p2", event_type=PositionEventType.OPEN, timestamp=t0),
        _make_position_event(
            position_id="p2",
            event_type=PositionEventType.CLOSE,
            attribution_net_pnl_usd="8.0",
            protocol_fees_usd="0.95",  # measured
            timestamp=t0 + timedelta(seconds=1),
        ),
    ]
    asyncio.run(_seed_store(db_path, metrics=metrics, ledger=ledger, events=events, snapshot=None))

    runner = CliRunner()
    # Text surface: rolled-up protocol fees must NOT render the
    # "not yet implemented" placeholder; the T5 section must appear.
    text_result = runner.invoke(strat_pnl, ["-s", DEPLOYMENT_ID, "--db", str(db_path)])
    assert text_result.exit_code == 0, text_result.output
    # The Protocol fees line itself must show the rolled-up measured total
    # (0.30 + 0.95 = 1.25), NOT the "not yet implemented" placeholder. (The
    # placeholder may still appear on the out-of-scope Impermanent loss line.)
    proto_line = next(ln for ln in text_result.output.splitlines() if ln.startswith("Protocol fees:"))
    assert "not yet implemented" not in proto_line
    assert "1.25" in proto_line
    assert "Gas efficiency" in text_result.output
    assert "Avg gas / trade:" in text_result.output
    assert "Total friction:" in text_result.output

    # JSON surface: rolled-up total is 0.30 + 0.95 = 1.25, friction includes it.
    json_result = runner.invoke(strat_pnl, ["-s", DEPLOYMENT_ID, "--db", str(db_path), "--json"])
    assert json_result.exit_code == 0, json_result.output
    payload = json.loads(json_result.output)
    assert Decimal(payload["protocol_fees_usd"]) == Decimal("1.25")
    # avg gas/trade = 4.00 / 2 trades = 2.00
    assert Decimal(payload["avg_gas_per_trade_usd"]) == Decimal("2.00")
    # total_friction = gas 4.00 + slippage + protocol_fees 1.25 (all measured)
    assert payload["total_friction_usd"] is not None
    assert Decimal(payload["total_friction_usd"]) > Decimal("5.25")


def test_strat_pnl_handles_missing_strategy_gracefully(tmp_path: Path) -> None:
    """A deployment_id with no rows in any table -> exit 1 with a clear error."""
    db_path = tmp_path / "empty.db"
    # Initialize an empty DB so --db existence check passes.
    asyncio.run(_seed_store(db_path, metrics=None, ledger=[], events=[], snapshot=None))

    runner = CliRunner()
    result = runner.invoke(
        strat_pnl,
        ["-s", "nonexistent_strategy:deadbeef", "--db", str(db_path)],
    )

    assert result.exit_code == 1
    assert "No persisted data" in result.output or "nonexistent_strategy" in result.output


def test_strat_pnl_shows_placeholders_for_missing_extraction_fields(
    tmp_path: Path,
) -> None:
    """When we only have metrics + a ledger with no slippage data, the
    breakdown must render em-dash placeholders rather than crashing."""
    db_path = tmp_path / "metrics_only.db"
    metrics = _make_metrics(initial="500", total="510", gas="0.50")
    ledger = [
        # Only a non-stable swap with no slippage_bps -> slippage stays None
        _make_swap_ledger(
            token_in="WETH",
            amount_in="0.5",
            token_out="WBTC",
            amount_out="0.025",
            gas_usd="0.50",
            slippage_bps=None,
        ),
    ]
    asyncio.run(_seed_store(db_path, metrics=metrics, ledger=ledger, events=[], snapshot=None))

    runner = CliRunner()
    result = runner.invoke(
        strat_pnl,
        ["-s", DEPLOYMENT_ID, "--db", str(db_path)],
    )

    assert result.exit_code == 0, result.output
    # Em-dash appears for protocol fees + IL + slippage + win rate (no closes)
    assert "\u2014" in result.output  # em-dash character
    assert "Protocol fees:" in result.output
    assert "Impermanent loss:" in result.output
    # Did not crash; gross/net PnL still present
    assert "Gross PnL:" in result.output
    assert "Net PnL:" in result.output


def test_strat_pnl_help_shows_command() -> None:
    """Help output is a fast sanity check the command registered correctly."""
    runner = CliRunner()
    result = runner.invoke(strat_pnl, ["--help"])
    assert result.exit_code == 0
    assert "--deployment-id" in result.output
    assert "--json" in result.output


def test_strat_pnl_missing_db_exits_with_error(tmp_path: Path) -> None:
    """--db pointing at a non-existent file exits 1 with a clear message."""
    runner = CliRunner()
    result = runner.invoke(
        strat_pnl,
        ["-s", DEPLOYMENT_ID, "--db", str(tmp_path / "does_not_exist.db")],
    )
    assert result.exit_code == 1
    assert "not found" in result.output


def test_strat_pnl_warns_only_on_true_position_limit_truncation(
    seeded_db: Path,
) -> None:
    """The truncation warning must fire iff older events were actually dropped.

    The CLI uses a probe-row pattern (``position_limit + 1`` in the loader
    call) so that the equality boundary (``len(events) == limit``) is the
    "exact fit, no truncation" case, not a false-positive trigger.

    seeded_db has 2 LP position events. We exercise three boundaries:

    - ``--position-limit 1`` (N > limit): warning fires, partial data flag.
    - ``--position-limit 2`` (N == limit, exact fit): no warning.
    - ``--position-limit 1000`` (N < limit): no warning.
    """
    runner = CliRunner()

    # 1. True truncation — warning must fire and surface in both text + JSON.
    truncated = runner.invoke(
        strat_pnl,
        ["-s", DEPLOYMENT_ID, "--db", str(seeded_db), "--position-limit", "1"],
    )
    assert truncated.exit_code == 0, truncated.output
    assert "--position-limit" in truncated.output
    assert "Position-derived" in truncated.output

    truncated_json = runner.invoke(
        strat_pnl,
        ["-s", DEPLOYMENT_ID, "--db", str(seeded_db), "--position-limit", "1", "--json"],
    )
    assert truncated_json.exit_code == 0, truncated_json.output
    payload = json.loads(truncated_json.output)
    assert any("--position-limit" in w and "Position-derived" in w for w in payload["warnings"])

    # 2. Exact fit — N == limit, no rows dropped, no warning.
    exact_fit = runner.invoke(
        strat_pnl,
        ["-s", DEPLOYMENT_ID, "--db", str(seeded_db), "--position-limit", "2"],
    )
    assert exact_fit.exit_code == 0, exact_fit.output
    assert "--position-limit" not in exact_fit.output

    # 3. Limit well above N — no warning.
    no_trunc = runner.invoke(
        strat_pnl,
        ["-s", DEPLOYMENT_ID, "--db", str(seeded_db), "--position-limit", "1000"],
    )
    assert no_trunc.exit_code == 0, no_trunc.output
    assert "--position-limit" not in no_trunc.output


def test_strat_pnl_warns_only_on_true_ledger_limit_truncation(
    seeded_db: Path,
) -> None:
    """Symmetric coverage for ``--ledger-limit``: same probe-row pattern, same
    boundary semantics.

    seeded_db has 2 swap ledger entries (USDC→WETH + WETH→USDC). Boundaries:

    - ``--ledger-limit 1`` (N > limit): warning fires, partial data flag.
    - ``--ledger-limit 2`` (N == limit, exact fit): no warning.
    - ``--ledger-limit 1000`` (N < limit): no warning.
    """
    runner = CliRunner()

    truncated = runner.invoke(
        strat_pnl,
        ["-s", DEPLOYMENT_ID, "--db", str(seeded_db), "--ledger-limit", "1"],
    )
    assert truncated.exit_code == 0, truncated.output
    assert "--ledger-limit" in truncated.output
    assert "Ledger-derived" in truncated.output

    truncated_json = runner.invoke(
        strat_pnl,
        ["-s", DEPLOYMENT_ID, "--db", str(seeded_db), "--ledger-limit", "1", "--json"],
    )
    assert truncated_json.exit_code == 0, truncated_json.output
    payload = json.loads(truncated_json.output)
    assert any("--ledger-limit" in w and "Ledger-derived" in w for w in payload["warnings"])

    exact_fit = runner.invoke(
        strat_pnl,
        ["-s", DEPLOYMENT_ID, "--db", str(seeded_db), "--ledger-limit", "2"],
    )
    assert exact_fit.exit_code == 0, exact_fit.output
    assert "--ledger-limit" not in exact_fit.output

    no_trunc = runner.invoke(
        strat_pnl,
        ["-s", DEPLOYMENT_ID, "--db", str(seeded_db), "--ledger-limit", "1000"],
    )
    assert no_trunc.exit_code == 0, no_trunc.output
    assert "--ledger-limit" not in no_trunc.output


def test_pnl_breakdown_to_json_dict_preserves_null_placeholders() -> None:
    """Decimal fields must serialize as strings; None must remain null."""
    bd = PnLBreakdown(
        deployment_id=DEPLOYMENT_ID,
        gross_pnl_usd=Decimal("10.00"),
        gas_usd=None,
        protocol_fees_usd=None,
        slippage_usd=Decimal("1.5"),
        impermanent_loss_usd=None,
        net_pnl_usd=Decimal("8.50"),
    )
    d = bd.to_json_dict()
    assert d["gross_pnl_usd"] == "10.00"
    assert d["gas_usd"] is None
    assert d["protocol_fees_usd"] is None
    assert d["slippage_usd"] == "1.5"
    assert d["impermanent_loss_usd"] is None
    assert d["net_pnl_usd"] == "8.50"
