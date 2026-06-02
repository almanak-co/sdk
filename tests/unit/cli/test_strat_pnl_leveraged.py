"""CLI + unit tests for VIB-4975 leveraged-lending headline scoping.

Three shapes, all synthesized from the real ``looping`` mainnet fixture
numbers (``strategies/accounting/looping/almanak_state.db``):

* **open** (``insp4``) — SUPPLY $5.187289, BORROW −$1.556190, initial $4.00,
  gas $2.69651.  The verbatim ``PortfolioMetrics`` headline is a phantom
  +$1.19 (``total_value_usd`` $5.187289 − $4.00); the corrected headline
  derives from the debt-netted lending NAV ($3.631099 − $4.00 ≈ −$0.37 before
  gas).
* **closed / torn down** (``looping-mainnet-may21``) — no live lending
  positions, ``total_value_usd`` $0, initial $8.00, historical BORROW in the
  ledger.  The verbatim headline reads −$8.00 (a false −100%); the corrected
  behaviour SUPPRESSES it.
* **non-leveraged** — a spot SWAP strategy that never borrowed; its headline
  must be left exactly as-is (regression guard).
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from click.testing import CliRunner

from almanak.framework.accounting.reporting.leveraged_lending import (
    LeveragedLendingVerdict,
    detect_leveraged_lending,
)
from almanak.framework.cli.strat_pnl import (
    PnLBreakdown,
    _apply_open_leveraged_headline,
    strat_pnl,
)
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.portfolio.models import (
    PortfolioMetrics,
    PortfolioSnapshot,
    PositionValue,
    ValueConfidence,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.teardown.models import PositionType

_BASE_TS = datetime(2026, 5, 30, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def temp_db_path():
    fd, path = tempfile.mkstemp(suffix=".db", prefix="strat_pnl_lev_")
    os.close(fd)
    yield Path(path)
    for ext in ("", "-wal", "-shm", "-journal"):
        try:
            os.unlink(str(path) + ext)
        except FileNotFoundError:
            pass


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _lending_position(ptype: PositionType, value: Decimal) -> PositionValue:
    return PositionValue(
        position_type=ptype,
        protocol="aave_v3",
        chain="arbitrum",
        value_usd=value,
        label=str(ptype),
    )


def _snapshot(deployment_id: str, positions: list[PositionValue], total: Decimal, cycle_id: str) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=_BASE_TS,
        deployment_id=deployment_id,
        total_value_usd=total,
        available_cash_usd=Decimal("0"),
        value_confidence=ValueConfidence.HIGH,
        deployed_capital_usd=Decimal("0"),
        wallet_total_value_usd=Decimal("0"),
        positions=positions,
        wallet_balances=[],
        token_prices={},
        chain="arbitrum",
        iteration_number=0,
        cycle_id=cycle_id,
    )


def _metrics(deployment_id: str, initial: Decimal, total: Decimal, gas: Decimal) -> PortfolioMetrics:
    return PortfolioMetrics(
        deployment_id=deployment_id,
        timestamp=_BASE_TS,
        initial_value_usd=initial,
        total_value_usd=total,
        deposits_usd=Decimal("0"),
        withdrawals_usd=Decimal("0"),
        gas_spent_usd=gas,
    )


def _ledger(deployment_id: str, intent_type: str, ts: datetime, *, success: bool = True) -> LedgerEntry:
    return LedgerEntry(
        deployment_id=deployment_id,
        timestamp=ts,
        intent_type=intent_type,
        token_in="USDC",
        amount_in="1",
        token_out="USDC",
        amount_out="1",
        gas_used=50000,
        gas_usd="0.01",
        chain="arbitrum",
        protocol="aave_v3",
        success=success,
    )


# ---------------------------------------------------------------------------
# OPEN leveraged seed (insp4 shape)
# ---------------------------------------------------------------------------

_OPEN_ID = "AccountingQuantLoopingStrategy:insp4"


async def _seed_open(db_path: Path) -> None:
    store = SQLiteStore(SQLiteConfig(db_path=str(db_path)))
    await store.initialize()
    try:
        await store.save_portfolio_metrics(
            _metrics(_OPEN_ID, initial=Decimal("4.00"), total=Decimal("5.187289"), gas=Decimal("2.69651"))
        )
        snap = _snapshot(
            _OPEN_ID,
            [
                _lending_position(PositionType.SUPPLY, Decimal("5.187289")),
                _lending_position(PositionType.BORROW, Decimal("-1.556190")),
            ],
            total=Decimal("5.187289"),
            cycle_id="iter-3",
        )
        await store.save_portfolio_snapshot(snap)
        await store.save_ledger_entry(_ledger(_OPEN_ID, "SUPPLY", _BASE_TS + timedelta(seconds=10)))
        await store.save_ledger_entry(_ledger(_OPEN_ID, "BORROW", _BASE_TS + timedelta(seconds=20)))
        await store.save_ledger_entry(_ledger(_OPEN_ID, "SUPPLY", _BASE_TS + timedelta(seconds=30)))
    finally:
        await store.close()


# ---------------------------------------------------------------------------
# CLOSED leveraged seed (looping-mainnet-may21 shape)
# ---------------------------------------------------------------------------

_CLOSED_ID = "AccountingQuantLoopingStrategy:looping-mainnet-may21"


async def _seed_closed(db_path: Path) -> None:
    store = SQLiteStore(SQLiteConfig(db_path=str(db_path)))
    await store.initialize()
    try:
        await store.save_portfolio_metrics(
            _metrics(_CLOSED_ID, initial=Decimal("8.00"), total=Decimal("0"), gas=Decimal("0.10229"))
        )
        # Torn-down: no live lending positions remain.
        snap = _snapshot(_CLOSED_ID, [], total=Decimal("0"), cycle_id="teardown-final")
        await store.save_portfolio_snapshot(snap)
        # Historical borrow proves the loop was leveraged.
        for i, it in enumerate(("SUPPLY", "BORROW", "SUPPLY", "REPAY", "WITHDRAW")):
            await store.save_ledger_entry(_ledger(_CLOSED_ID, it, _BASE_TS + timedelta(seconds=10 * i)))
    finally:
        await store.close()


# ---------------------------------------------------------------------------
# NON-LEVERAGED seed (spot swap; never borrowed)
# ---------------------------------------------------------------------------

_SPOT_ID = "uniswap_rsi:spot123"


async def _seed_non_leveraged(db_path: Path) -> None:
    store = SQLiteStore(SQLiteConfig(db_path=str(db_path)))
    await store.initialize()
    try:
        await store.save_portfolio_metrics(
            _metrics(_SPOT_ID, initial=Decimal("100.00"), total=Decimal("110.00"), gas=Decimal("0.50"))
        )
        snap = _snapshot(_SPOT_ID, [], total=Decimal("110.00"), cycle_id="iter-5")
        await store.save_portfolio_snapshot(snap)
        await store.save_ledger_entry(_ledger(_SPOT_ID, "SWAP", _BASE_TS + timedelta(seconds=10)))
    finally:
        await store.close()


# ---------------------------------------------------------------------------
# CARRY seed: borrowed historically, lending legs unwound, live LP value remains
# ---------------------------------------------------------------------------

_CARRY_ID = "AccountingCarryStrategy:carry1"


async def _seed_carry(db_path: Path) -> None:
    store = SQLiteStore(SQLiteConfig(db_path=str(db_path)))
    await store.initialize()
    try:
        # Deployed an $8 loop, unwound the lending, ended in a $12.50 LP.
        await store.save_portfolio_metrics(
            _metrics(_CARRY_ID, initial=Decimal("8.00"), total=Decimal("12.50"), gas=Decimal("0.30"))
        )
        snap = _snapshot(
            _CARRY_ID,
            [
                PositionValue(
                    position_type=PositionType.LP,
                    protocol="uniswap_v3",
                    chain="arbitrum",
                    value_usd=Decimal("12.50"),
                    label="LP",
                )
            ],
            total=Decimal("12.50"),
            cycle_id="iter-9",
        )
        await store.save_portfolio_snapshot(snap)
        for i, it in enumerate(("SUPPLY", "BORROW", "SWAP", "REPAY", "WITHDRAW", "LP_OPEN")):
            await store.save_ledger_entry(_ledger(_CARRY_ID, it, _BASE_TS + timedelta(seconds=10 * i)))
    finally:
        await store.close()


# ===========================================================================
# Unit-level detection tests
# ===========================================================================


def test_detect_open_leveraged_returns_debt_netted_nav() -> None:
    snap = _snapshot(
        _OPEN_ID,
        [
            _lending_position(PositionType.SUPPLY, Decimal("5.187289")),
            _lending_position(PositionType.BORROW, Decimal("-1.556190")),
        ],
        total=Decimal("5.187289"),
        cycle_id="iter-3",
    )
    verdict = detect_leveraged_lending(snap, [])
    assert verdict.is_leveraged_lending is True
    assert verdict.state == "open"
    # 5.187289 − 1.556190 = 3.631099  (debt-netted, NOT the $5.19 positive scope)
    assert verdict.net_lending_nav_usd == Decimal("3.631099")


def test_detect_closed_leveraged_via_historical_ledger_borrow() -> None:
    snap = _snapshot(_CLOSED_ID, [], total=Decimal("0"), cycle_id="teardown-final")
    ledger = [
        _ledger(_CLOSED_ID, "BORROW", _BASE_TS),
        _ledger(_CLOSED_ID, "WITHDRAW", _BASE_TS + timedelta(seconds=10)),
    ]
    verdict = detect_leveraged_lending(snap, ledger)
    assert verdict.is_leveraged_lending is True
    assert verdict.state == "closed"
    assert verdict.net_lending_nav_usd is None


def test_detect_non_leveraged_is_left_untouched() -> None:
    snap = _snapshot(_SPOT_ID, [], total=Decimal("110.00"), cycle_id="iter-5")
    verdict = detect_leveraged_lending(snap, [_ledger(_SPOT_ID, "SWAP", _BASE_TS)])
    assert verdict.is_leveraged_lending is False
    assert verdict.state == "none"


def test_detect_failed_borrow_does_not_count_as_leverage() -> None:
    snap = _snapshot(_SPOT_ID, [], total=Decimal("0"), cycle_id="iter-5")
    verdict = detect_leveraged_lending(snap, [_ledger(_SPOT_ID, "BORROW", _BASE_TS, success=False)])
    assert verdict.is_leveraged_lending is False


def test_detect_tolerates_none_ledger(temp_db_path: Path) -> None:
    """A None ledger must not raise — no rows means no historical borrow.

    With a live BORROW position in the snapshot, the verdict is still OPEN
    (driven by the snapshot, not the ledger); the None ledger is simply
    inspected without raising TypeError (Gemini review).
    """
    snap = _snapshot(
        _OPEN_ID,
        [
            _lending_position(PositionType.SUPPLY, Decimal("5.0")),
            _lending_position(PositionType.BORROW, Decimal("-1.5")),
        ],
        total=Decimal("5.0"),
        cycle_id="iter-3",
    )
    verdict = detect_leveraged_lending(snap, None)
    assert verdict.state == "open"
    assert verdict.net_lending_nav_usd == Decimal("3.5")


def test_detect_none_ledger_no_borrow_is_none_state() -> None:
    """None ledger + no live BORROW → not leveraged, no TypeError."""
    snap = _snapshot(_SPOT_ID, [], total=Decimal("0"), cycle_id="iter-5")
    verdict = detect_leveraged_lending(snap, None)
    assert verdict.is_leveraged_lending is False
    assert verdict.state == "none"


def test_open_headline_not_derived_when_initial_value_unmeasured() -> None:
    """Empty≠Zero: an unmeasured initial baseline must NOT be defaulted to 0.

    Deriving ``NAV − 0 − flows`` off a None baseline would book the entire NAV
    as PnL (a confident wrong number). The B-open derivation must no-op and
    leave the verbatim headline untouched instead (Gemini review).
    """
    breakdown = PnLBreakdown(deployment_id=_OPEN_ID)
    verdict = LeveragedLendingVerdict(
        is_leveraged_lending=True,
        state="open",
        net_lending_nav_usd=Decimal("3.631099"),
        reason="",
    )
    metrics = PortfolioMetrics(
        deployment_id=_OPEN_ID,
        timestamp=_BASE_TS,
        initial_value_usd=None,  # unmeasured baseline
        total_value_usd=Decimal("5.187289"),
        deposits_usd=Decimal("0"),
        withdrawals_usd=Decimal("0"),
        gas_spent_usd=Decimal("2.69651"),
    )
    _apply_open_leveraged_headline(breakdown, metrics, verdict)
    # No derivation ran: flag stays False and the NAV was NOT booked as PnL.
    assert breakdown.headline_leverage_adjusted is False
    assert breakdown.gross_pnl_usd is None
    assert breakdown.net_pnl_usd is None
    assert breakdown.headline_leverage_note is None


def test_open_headline_derived_when_initial_value_is_measured_zero() -> None:
    """Empty≠Zero contrast: a MEASURED zero baseline DOES derive (0 ≠ None)."""
    breakdown = PnLBreakdown(deployment_id=_OPEN_ID)
    verdict = LeveragedLendingVerdict(
        is_leveraged_lending=True,
        state="open",
        net_lending_nav_usd=Decimal("3.5"),
        reason="",
    )
    metrics = PortfolioMetrics(
        deployment_id=_OPEN_ID,
        timestamp=_BASE_TS,
        initial_value_usd=Decimal("0"),  # measured zero — a real baseline
        total_value_usd=Decimal("3.5"),
        deposits_usd=Decimal("0"),
        withdrawals_usd=Decimal("0"),
        gas_spent_usd=Decimal("0.10"),
    )
    _apply_open_leveraged_headline(breakdown, metrics, verdict)
    assert breakdown.headline_leverage_adjusted is True
    assert breakdown.gross_pnl_usd == Decimal("3.5")  # 3.5 − 0
    assert breakdown.net_pnl_usd == Decimal("3.4")  # 3.5 − 0.10


def test_detect_carry_with_live_non_lending_value_not_suppressed() -> None:
    """borrow→swap→LP carry: borrowed historically, lending legs unwound, live LP
    value remains → headline NOT suppressed (total_value_usd is genuinely non-zero).
    """
    snap = _snapshot(
        _CARRY_ID,
        [
            PositionValue(
                position_type=PositionType.LP,
                protocol="uniswap_v3",
                chain="arbitrum",
                value_usd=Decimal("12.50"),
                label="LP",
            )
        ],
        total=Decimal("12.50"),
        cycle_id="iter-7",
    )
    verdict = detect_leveraged_lending(
        snap, [_ledger(_CARRY_ID, "BORROW", _BASE_TS), _ledger(_CARRY_ID, "SWAP", _BASE_TS)]
    )
    # Real live deployed value remains → leave the verbatim headline untouched.
    assert verdict.is_leveraged_lending is False
    assert verdict.state == "none"


def test_detect_closed_collapse_token_pseudo_position_is_suppressed() -> None:
    """Only TOKEN wallet pseudo-positions remain → genuine collapse → suppress.

    A TOKEN leg IS the capital that returned to the wallet, so it must NOT
    count as 'live non-lending value remains'.
    """
    snap = _snapshot(
        _CLOSED_ID,
        [
            PositionValue(
                position_type=PositionType.TOKEN,
                protocol="wallet",
                chain="arbitrum",
                value_usd=Decimal("7.90"),
                label="USDC",
            )
        ],
        total=Decimal("0"),
        cycle_id="teardown-final",
    )
    verdict = detect_leveraged_lending(snap, [_ledger(_CLOSED_ID, "BORROW", _BASE_TS)])
    assert verdict.state == "closed"


def test_detect_open_unmeasured_borrow_leg_routes_to_suppression() -> None:
    """Live BORROW leg with value_usd=None → debt under-counted → phantom gain;
    route to honest suppression instead of an over-reported headline (Empty≠Zero).
    """
    snap = _snapshot(
        _OPEN_ID,
        [
            _lending_position(PositionType.SUPPLY, Decimal("5.0")),
            PositionValue(
                position_type=PositionType.BORROW, protocol="aave_v3", chain="arbitrum", value_usd=None, label="BORROW"
            ),
        ],
        total=Decimal("5.0"),
        cycle_id="iter-3",
    )
    verdict = detect_leveraged_lending(snap, [])
    assert verdict.state == "closed"
    assert verdict.net_lending_nav_usd is None
    assert "unmeasured" in verdict.reason


def test_detect_open_with_extra_lp_leg_still_uses_lending_nav() -> None:
    """A loop that also holds a live LP leg still derives the headline from the
    debt-netted LENDING NAV (Σ SUPPLY − Σ BORROW), not total_value_usd.
    """
    snap = _snapshot(
        _OPEN_ID,
        [
            _lending_position(PositionType.SUPPLY, Decimal("5.0")),
            _lending_position(PositionType.BORROW, Decimal("-1.5")),
            PositionValue(
                position_type=PositionType.LP,
                protocol="uniswap_v3",
                chain="arbitrum",
                value_usd=Decimal("3.0"),
                label="LP",
            ),
        ],
        total=Decimal("8.0"),
        cycle_id="iter-3",
    )
    verdict = detect_leveraged_lending(snap, [])
    assert verdict.state == "open"
    assert verdict.net_lending_nav_usd == Decimal("3.5")  # 5.0 − 1.5, NOT 8.0 − 1.5


def test_detect_exact_position_type_equality_not_endswith() -> None:
    """A future FLASH_BORROW-style type must NOT match the BORROW check."""

    class _FakePos:
        position_type = "FLASH_BORROW"
        value_usd = Decimal("3.0")

    snap = _snapshot(_SPOT_ID, [], total=Decimal("0"), cycle_id="iter-5")
    snap.positions = [_FakePos()]
    verdict = detect_leveraged_lending(snap, [])
    # FLASH_BORROW is not a live BORROW; no historical borrow either → none.
    assert verdict.is_leveraged_lending is False
    assert verdict.state == "none"


# ===========================================================================
# CLI integration tests
# ===========================================================================


def test_open_headline_derives_from_debt_netted_nav(temp_db_path: Path) -> None:
    asyncio.run(_seed_open(temp_db_path))
    result = CliRunner().invoke(strat_pnl, ["-s", _OPEN_ID, "--db", str(temp_db_path), "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)

    # Phantom +leverage gain is gone: gross = 3.631099 − 4.00 = −0.368901
    assert payload["headline_leverage_adjusted"] is True
    assert Decimal(payload["gross_pnl_usd"]) == Decimal("-0.368901")
    # net = gross − gas (2.69651) = −3.065411
    assert Decimal(payload["net_pnl_usd"]) == Decimal("-3.065411")
    # NOT the verbatim +$1.19 PortfolioMetrics headline.
    assert Decimal(payload["gross_pnl_usd"]) < 0
    assert payload["headline_suppressed"] is False
    assert payload["headline_leverage_note"] is not None


def test_open_headline_text_shows_corrected_number_and_note(temp_db_path: Path) -> None:
    asyncio.run(_seed_open(temp_db_path))
    result = CliRunner().invoke(strat_pnl, ["-s", _OPEN_ID, "--db", str(temp_db_path)])
    assert result.exit_code == 0, result.output
    assert "Gross PnL:" in result.output
    assert "debt-netted lending NAV" in result.output
    assert "VIB-4975" in result.output
    # The phantom +1.19 must NOT be rendered.
    assert "$    1.19" not in result.output


def test_closed_headline_is_suppressed_not_minus_initial(temp_db_path: Path) -> None:
    asyncio.run(_seed_closed(temp_db_path))
    result = CliRunner().invoke(strat_pnl, ["-s", _CLOSED_ID, "--db", str(temp_db_path)])
    assert result.exit_code == 0, result.output
    assert "Headline PnL:     unavailable" in result.output
    assert "VIB-4976" in result.output
    # The confident, wrong −$8.00 must NOT appear.
    assert "Gross PnL:" not in result.output
    assert "$    8.00" not in result.output


def test_closed_headline_json_carries_suppression(temp_db_path: Path) -> None:
    asyncio.run(_seed_closed(temp_db_path))
    result = CliRunner().invoke(strat_pnl, ["-s", _CLOSED_ID, "--db", str(temp_db_path), "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["headline_suppressed"] is True
    assert "VIB-4976" in payload["headline_suppression_reason"]


def test_non_leveraged_headline_unchanged(temp_db_path: Path) -> None:
    asyncio.run(_seed_non_leveraged(temp_db_path))
    result = CliRunner().invoke(strat_pnl, ["-s", _SPOT_ID, "--db", str(temp_db_path), "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    # Regression guard: verbatim PortfolioMetrics headline stands.
    assert payload["headline_leverage_adjusted"] is False
    assert payload["headline_suppressed"] is False
    # 110 − 100 = 10 (before gas); 10 − 0.50 = 9.50 (after gas).
    assert Decimal(payload["gross_pnl_usd"]) == Decimal("10.00")
    assert Decimal(payload["net_pnl_usd"]) == Decimal("9.50")


def test_non_leveraged_text_shows_unmodified_gross(temp_db_path: Path) -> None:
    asyncio.run(_seed_non_leveraged(temp_db_path))
    result = CliRunner().invoke(strat_pnl, ["-s", _SPOT_ID, "--db", str(temp_db_path)])
    assert result.exit_code == 0, result.output
    assert "Gross PnL:" in result.output
    assert "debt-netted lending NAV" not in result.output
    assert "Headline PnL:     unavailable" not in result.output


def test_carry_with_live_lp_value_keeps_verbatim_headline(temp_db_path: Path) -> None:
    """borrow→swap→LP carry with a live LP leg: NOT suppressed, NOT NAV-adjusted —
    the verbatim PortfolioMetrics headline is meaningful (total_value_usd ≠ 0).
    """
    asyncio.run(_seed_carry(temp_db_path))
    result = CliRunner().invoke(strat_pnl, ["-s", _CARRY_ID, "--db", str(temp_db_path), "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["headline_suppressed"] is False
    assert payload["headline_leverage_adjusted"] is False
    # Verbatim: 12.50 − 8.00 = 4.50 before gas; 4.50 − 0.30 = 4.20 after gas.
    assert Decimal(payload["gross_pnl_usd"]) == Decimal("4.50")
    assert Decimal(payload["net_pnl_usd"]) == Decimal("4.20")


def test_carry_text_shows_gross_not_unavailable(temp_db_path: Path) -> None:
    asyncio.run(_seed_carry(temp_db_path))
    result = CliRunner().invoke(strat_pnl, ["-s", _CARRY_ID, "--db", str(temp_db_path)])
    assert result.exit_code == 0, result.output
    assert "Gross PnL:" in result.output
    assert "Headline PnL:     unavailable" not in result.output
    assert "debt-netted lending NAV" not in result.output
