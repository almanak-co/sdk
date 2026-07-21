"""VIB-5941 — the RUNNER write/persistence lane actually persists perp position_events.

The builder tests (``test_vib5941_perp_payload_identity.py``) prove
``build_position_event_from_intent`` returns a perp event with a side-aware
position_id. They do NOT prove the runner persists it — a runner that swallowed a
save failure would pass every builder probe while silently writing zero rows.

These tests drive the REAL persistence seam
(``StrategyRunner._emit_position_event_for_intent`` →
``state_manager.save_position_event`` → ``_handle_position_event_save_failure``)
against a real SQLite-backed ``SQLiteStore`` and assert on **persisted rows via a
direct SELECT**, plus the mode-aware failure contract (live raises
``AccountingPersistenceError``; paper logs ERROR and continues).
"""

from __future__ import annotations

import logging
import sqlite3
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.intents.perp_intents import PerpCloseIntent, PerpOpenIntent
from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.exceptions import AccountingPersistenceError

_DEPLOYMENT = "deployment:vib5941write"
_WALLET = "0xAbCdAbCdAbCdAbCdAbCdAbCdAbCdAbCdAbCdAbCd"
_CHAIN = "arbitrum"


async def _make_store(tmp_path) -> SQLiteStore:
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "wlane.sqlite")))
    await store.initialize()
    return store


def _runner(store: SQLiteStore) -> StrategyRunner:
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=store,
        alert_manager=MagicMock(),
        config=RunnerConfig(default_interval_seconds=0, enable_state_persistence=True, enable_alerting=False),
    )
    # Attribution side-effects are a separate lane; stub so the test isolates
    # the persistence seam (the save has already happened before attribution).
    runner._run_position_event_attribution = AsyncMock()  # type: ignore[method-assign]
    runner._is_live_mode = MagicMock(return_value=False)  # type: ignore[method-assign]
    return runner


def _open_intent() -> PerpOpenIntent:
    return PerpOpenIntent(
        market="ETH/USD", collateral_token="USDC", collateral_amount=Decimal("10"),
        size_usd=Decimal("20"), is_long=True, leverage=Decimal("2"), protocol="gmx_v2", chain=_CHAIN,
    )


def _close_intent() -> PerpCloseIntent:
    return PerpCloseIntent(market="ETH/USD", collateral_token="USDC", is_long=True, protocol="gmx_v2", chain=_CHAIN)


async def _emit(runner: StrategyRunner, intent, *, mode: str, entry_id: str) -> None:
    await runner._emit_position_event_for_intent(
        strategy=SimpleNamespace(deployment_id=_DEPLOYMENT, wallet_address=_WALLET),
        intent=intent,
        result=SimpleNamespace(success=True, extracted_data={}, tx_hash="0xtx", position_id=""),
        entry=SimpleNamespace(id=entry_id),
        chain=_CHAIN,
        deployment_id=_DEPLOYMENT,
        execution_mode=mode,
        cycle_id="cyc-1",
        price_oracle=None,
        post_state=None,
        wallet_address=_WALLET,
    )


def _rows(store: SQLiteStore) -> list[tuple]:
    conn = sqlite3.connect(store._config.db_path)
    try:
        return list(
            conn.execute(
                "SELECT event_type, position_type, position_id FROM position_events "
                "WHERE deployment_id=? ORDER BY timestamp",
                (_DEPLOYMENT,),
            )
        )
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_runner_persists_perp_open_and_close_rows(tmp_path) -> None:
    """Two intents through the REAL runner seam → two PERSISTED position_events rows."""
    store = await _make_store(tmp_path)
    try:
        runner = _runner(store)
        await _emit(runner, _open_intent(), mode="paper", entry_id="led-open")
        await _emit(runner, _close_intent(), mode="paper", entry_id="led-close")

        rows = _rows(store)  # direct SELECT — asserts on PERSISTED rows, not a builder return
        assert len(rows) == 2, f"expected 2 persisted perp position_events, got {rows}"
        by_type = {r[0]: r for r in rows}
        assert set(by_type) == {"OPEN", "CLOSE"}
        expected_id = f"perp:{_CHAIN}:gmx_v2:{_WALLET.lower()}:eth/usd:long:usdc"
        for et in ("OPEN", "CLOSE"):
            assert by_type[et][1] == "PERP"
            # side-aware id, shared by OPEN and CLOSE (the lifecycle pairs on disk).
            assert by_type[et][2] == expected_id
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_live_mode_save_failure_raises(tmp_path) -> None:
    """Fault injection: save returns False in LIVE → AccountingPersistenceError surfaces."""
    store = await _make_store(tmp_path)
    try:
        runner = _runner(store)
        runner._is_live_mode = MagicMock(return_value=True)  # type: ignore[method-assign]
        # Inject a persistence failure at the save seam.
        store.save_position_event = AsyncMock(return_value=False)  # type: ignore[method-assign]
        with pytest.raises(AccountingPersistenceError):
            await _emit(runner, _open_intent(), mode="live", entry_id="led-open")
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_paper_mode_save_failure_logs_error_and_continues(tmp_path, caplog) -> None:
    """Fault injection: save returns False in PAPER → ERROR logged, NO raise."""
    store = await _make_store(tmp_path)
    try:
        runner = _runner(store)  # non-live by default
        store.save_position_event = AsyncMock(return_value=False)  # type: ignore[method-assign]
        with caplog.at_level(logging.ERROR):
            # Must NOT raise.
            await _emit(runner, _open_intent(), mode="paper", entry_id="led-open")
        assert any(
            "Position event save returned False" in r.message and r.levelno == logging.ERROR
            for r in caplog.records
        ), "paper-mode save failure must log an ERROR record"
    finally:
        await store.close()
