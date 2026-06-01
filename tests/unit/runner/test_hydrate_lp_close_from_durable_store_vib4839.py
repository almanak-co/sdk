"""VIB-4839 — unit coverage for ``StrategyRunner._hydrate_lp_close_from_durable_store``.

Per repo coding guidelines: ``tests/unit/`` coverage is REQUIRED for any new
class/function/module. The behavioural / integration coverage for VIB-4839
lives at ``tests/framework/observability/test_position_events_durable_close_hydration_vib4839.py``;
this file isolates the helper itself so the OPEN/CLOSE history selection
contract is cheap to regress-test without standing up the full emit chokepoint.

(CodeRabbit Major on PR #2490 requested the dedicated ``tests/unit/`` location.)
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner


class _Runner(StrategyRunner):
    """Bypass StrategyRunner.__init__ — wire just what the helper reads."""

    def __init__(self, *, state_manager: Any) -> None:
        self.state_manager = state_manager
        self.config = RunnerConfig()


def _open_row(pid: str = "5500290", **overrides) -> dict:
    row = {
        "id": "row-open",
        "position_id": pid,
        "position_type": "LP",
        "event_type": "OPEN",
        "timestamp": "2026-05-22T20:07:14",
        "token0": "WETH",
        "token1": "USDC",
        "amount0": "899864508866453",
        "amount1": "2102959",
        "value_usd": "3.965156",
        "tick_lower": -200490,
        "tick_upper": -199490,
        "liquidity": "5500290000",
        "ledger_entry_id": "led-open",
    }
    row.update(overrides)
    return row


def _close_row(pid: str = "5500290", **overrides) -> dict:
    return {
        "id": "row-close",
        "position_id": pid,
        "position_type": "LP",
        "event_type": "CLOSE",
        "timestamp": "2026-05-23T10:00:00",
        **overrides,
    }


# ──────────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_returns_open_payload_when_history_has_only_open() -> None:
    """Single OPEN in history → return its payload shaped as a cache entry."""
    sm = MagicMock()
    sm.get_position_history = AsyncMock(return_value=[_open_row()])
    runner = _Runner(state_manager=sm)

    payload = await runner._hydrate_lp_close_from_durable_store(deployment_id="d", position_id="5500290")

    assert payload is not None
    assert payload["token0"] == "WETH"
    assert payload["token1"] == "USDC"
    assert payload["tick_lower"] == -200490
    assert payload["tick_upper"] == -199490
    assert payload["liquidity"] == "5500290000"
    assert payload["value_usd"] == "3.965156"
    sm.get_position_history.assert_awaited_once_with("d", "5500290")


# ──────────────────────────────────────────────────────────────────────────
# History selection — newest OPEN wins, intervening CLOSE invalidates
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_returns_none_when_history_empty() -> None:
    sm = MagicMock()
    sm.get_position_history = AsyncMock(return_value=[])
    runner = _Runner(state_manager=sm)
    assert await runner._hydrate_lp_close_from_durable_store(deployment_id="d", position_id="p1") is None


@pytest.mark.asyncio
async def test_returns_none_when_last_event_was_close() -> None:
    """OPEN → CLOSE history → the bracket is spent; do not re-hydrate."""
    sm = MagicMock()
    sm.get_position_history = AsyncMock(return_value=[_open_row(), _close_row()])
    runner = _Runner(state_manager=sm)
    assert await runner._hydrate_lp_close_from_durable_store(deployment_id="d", position_id="p1") is None


@pytest.mark.asyncio
async def test_returns_newest_open_after_close_open() -> None:
    """OPEN1 → CLOSE1 → OPEN2 → return OPEN2's payload (re-mint case)."""
    sm = MagicMock()
    sm.get_position_history = AsyncMock(
        return_value=[
            _open_row(token0="WETH", token1="USDC", tick_lower=-100, tick_upper=100),
            _close_row(),
            _open_row(token0="WBTC", token1="USDT", tick_lower=-200, tick_upper=200),
        ]
    )
    runner = _Runner(state_manager=sm)
    payload = await runner._hydrate_lp_close_from_durable_store(deployment_id="d", position_id="p1")
    assert payload is not None
    assert payload["token0"] == "WBTC"
    assert payload["token1"] == "USDT"
    assert payload["tick_lower"] == -200
    assert payload["tick_upper"] == 200


@pytest.mark.asyncio
async def test_skips_non_open_events_in_history() -> None:
    """Rows with non-OPEN / non-CLOSE event types must not affect selection."""
    sm = MagicMock()
    sm.get_position_history = AsyncMock(
        return_value=[
            _open_row(token0="WETH"),
            {"event_type": "SNAPSHOT", "position_id": "p1"},
            {"event_type": "COLLECT_FEES", "position_id": "p1"},
        ]
    )
    runner = _Runner(state_manager=sm)
    payload = await runner._hydrate_lp_close_from_durable_store(deployment_id="d", position_id="p1")
    assert payload is not None
    assert payload["token0"] == "WETH"


# ──────────────────────────────────────────────────────────────────────────
# Fail-closed paths — never raise; emit WARN on durable surface gaps
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_returns_none_when_state_manager_is_none(caplog) -> None:
    """No state_manager → emit ``lp_close_durable_hydration.unavailable`` WARN."""
    import logging

    runner = _Runner(state_manager=None)
    with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
        result = await runner._hydrate_lp_close_from_durable_store(deployment_id="d", position_id="p1")
    assert result is None
    assert any(
        "lp_close_durable_hydration.unavailable" in rec.message
        for rec in caplog.records
        if rec.levelno == logging.WARNING
    )


@pytest.mark.asyncio
async def test_returns_none_when_state_manager_lacks_method(caplog) -> None:
    """SM without get_position_history → unavailable WARN, return None."""
    import logging

    class _PartialSM:
        async def save_position_event(self, ev):  # noqa: ANN001
            return True

    runner = _Runner(state_manager=_PartialSM())
    with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
        result = await runner._hydrate_lp_close_from_durable_store(deployment_id="d", position_id="p1")
    assert result is None
    assert any(
        "lp_close_durable_hydration.unavailable" in rec.message
        for rec in caplog.records
        if rec.levelno == logging.WARNING
    )


@pytest.mark.asyncio
async def test_returns_none_and_warns_when_backend_raises(caplog) -> None:
    """Backend raise → ``lp_close_durable_hydration.failed`` WARN, return None."""
    import logging

    sm = MagicMock()
    sm.get_position_history = AsyncMock(side_effect=RuntimeError("simulated outage"))
    runner = _Runner(state_manager=sm)
    with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
        result = await runner._hydrate_lp_close_from_durable_store(deployment_id="d", position_id="p1")
    assert result is None
    assert any(
        "lp_close_durable_hydration.failed" in rec.message for rec in caplog.records if rec.levelno == logging.WARNING
    )


# ──────────────────────────────────────────────────────────────────────────
# Payload shape contract — must match _update_recent_open_events_cache
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_payload_shape_matches_cache_entry_contract() -> None:
    """The returned dict has exactly the 8 keys the in-memory cache writes,
    so consumers (``_apply_lp_close_columns``) can't tell the source apart.
    """
    sm = MagicMock()
    sm.get_position_history = AsyncMock(return_value=[_open_row()])
    runner = _Runner(state_manager=sm)
    payload = await runner._hydrate_lp_close_from_durable_store(deployment_id="d", position_id="p1")
    assert payload is not None
    assert set(payload.keys()) == {
        "value_usd",
        "ledger_entry_id",
        "timestamp",
        "tick_lower",
        "tick_upper",
        "liquidity",
        "token0",
        "token1",
    }


@pytest.mark.asyncio
async def test_payload_coerces_string_fields_to_str() -> None:
    """Backend rows may have non-string ledger_entry_id / liquidity (e.g.
    integer). Cache entries must always store strings so consumers don't
    need defensive coercion."""
    sm = MagicMock()
    sm.get_position_history = AsyncMock(return_value=[_open_row(liquidity=99999, ledger_entry_id=42, value_usd=3.14)])
    runner = _Runner(state_manager=sm)
    payload = await runner._hydrate_lp_close_from_durable_store(deployment_id="d", position_id="p1")
    assert payload is not None
    assert payload["liquidity"] == "99999"
    assert payload["ledger_entry_id"] == "42"
    assert payload["value_usd"] == "3.14"


@pytest.mark.asyncio
async def test_payload_preserves_int_ticks() -> None:
    """``tick_lower`` / ``tick_upper`` are typed ``int | None`` in the cache —
    never coerced to str. A consumer that reads ``tick_lower < 0`` would
    break if we stringified.
    """
    sm = MagicMock()
    sm.get_position_history = AsyncMock(return_value=[_open_row(tick_lower=-200490, tick_upper=-199490)])
    runner = _Runner(state_manager=sm)
    payload = await runner._hydrate_lp_close_from_durable_store(deployment_id="d", position_id="p1")
    assert payload is not None
    assert payload["tick_lower"] == -200490
    assert payload["tick_upper"] == -199490
    assert isinstance(payload["tick_lower"], int)
    assert isinstance(payload["tick_upper"], int)
