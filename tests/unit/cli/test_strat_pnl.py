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
    ValueConfidence,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore


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
        strategy_id=DEPLOYMENT_ID,
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
        strategy_id=DEPLOYMENT_ID,
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
) -> PositionEvent:
    attribution_json = "{}"
    if attribution_net_pnl_usd is not None:
        attribution_json = json.dumps(
            {"version": 1, "position_type": "LP", "net_pnl_usd": attribution_net_pnl_usd}
        )
    event = PositionEvent(
        deployment_id=DEPLOYMENT_ID,
        position_id=position_id,
        position_type=PositionType.LP.value,
        event_type=event_type.value,
        protocol="uniswap_v3",
        chain="arbitrum",
        attribution_json=attribution_json,
        attribution_version=1 if attribution_net_pnl_usd is not None else 0,
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
    from almanak.framework.cli import strat_pnl

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
    """Protocol fees + IL are always placeholders (VIB-3204 / VIB-3205 pending)."""
    result = compute_pnl_breakdown(
        deployment_id=DEPLOYMENT_ID,
        metrics=_make_metrics(initial="1000", total="1100", gas="5"),
        ledger_entries=[],
        position_events=[],
        snapshot=None,
    )

    assert result.protocol_fees_usd is None
    assert result.impermanent_loss_usd is None


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
        strategy_id=DEPLOYMENT_ID,
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
        strategy_id=DEPLOYMENT_ID,
        total_value_usd=Decimal("1123.45"),
        available_cash_usd=Decimal("500"),
        value_confidence=ValueConfidence.HIGH,
        chain="arbitrum",
    )

    asyncio.run(
        _seed_store(
            db_path, metrics=metrics, ledger=ledger, events=events, snapshot=snapshot
        )
    )
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
    assert "(not yet implemented)" in result.output
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


def test_strat_pnl_handles_missing_strategy_gracefully(tmp_path: Path) -> None:
    """A deployment_id with no rows in any table -> exit 1 with a clear error."""
    db_path = tmp_path / "empty.db"
    # Initialize an empty DB so --db existence check passes.
    asyncio.run(
        _seed_store(db_path, metrics=None, ledger=[], events=[], snapshot=None)
    )

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
    asyncio.run(
        _seed_store(
            db_path, metrics=metrics, ledger=ledger, events=[], snapshot=None
        )
    )

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
    assert "--strategy-id" in result.output
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
    assert any(
        "--position-limit" in w and "Position-derived" in w
        for w in payload["warnings"]
    )

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
    assert any(
        "--ledger-limit" in w and "Ledger-derived" in w
        for w in payload["warnings"]
    )

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
