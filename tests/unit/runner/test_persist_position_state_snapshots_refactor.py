"""Branch and public-path coverage for Track C snapshot persistence."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.accounting import position_state
from almanak.framework.portfolio import PortfolioSnapshot, ValueConfidence
from almanak.framework.runner import runner_state
from almanak.framework.state.exceptions import AccountingPersistenceError, AccountingWriteKind


def _config(mode: str = "live") -> SimpleNamespace:
    return SimpleNamespace(
        dry_run=mode == "dry_run",
        paper_mode=mode == "paper",
    )


def _runner(
    *,
    mode: str = "live",
    state_manager: Any | None = None,
    deployment_id: str = "runner-deployment",
    cycle_id: str = "runner-cycle",
    strategy: Any | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        config=_config(mode),
        state_manager=state_manager,
        deployment_id=deployment_id,
        _last_cycle_id=cycle_id,
        _current_strategy=strategy,
        strategy=None,
    )


def _snapshot(*positions: Any, deployment_id: str = "snapshot-deployment") -> SimpleNamespace:
    return SimpleNamespace(
        timestamp=datetime(2026, 9, 3, 12, tzinfo=UTC),
        deployment_id=deployment_id,
        cycle_id="snapshot-cycle",
        positions=list(positions),
    )


def _install_materializer(monkeypatch: pytest.MonkeyPatch, materializer: Any) -> None:
    monkeypatch.setattr(position_state, "materialise_position_state", materializer)


@pytest.mark.asyncio
async def test_capability_and_empty_position_branches_return_measured_zero() -> None:
    snapshot = _snapshot()

    assert await runner_state._persist_position_state_snapshots(None, snapshot, snapshot_id=1) == 0

    save = AsyncMock(return_value=0)
    runner = _runner(state_manager=SimpleNamespace(save_position_state_snapshots=save))
    assert await runner_state._persist_position_state_snapshots(runner, snapshot, snapshot_id=1) == 0
    save.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("runner_deployment", "runner_cycle", "expected_deployment", "expected_cycle"),
    [
        pytest.param("runner-deployment", "runner-cycle", "runner-deployment", "runner-cycle", id="runner-owned"),
        pytest.param("", "", "snapshot-deployment", "snapshot-cycle", id="snapshot-fallback"),
    ],
)
async def test_context_preserves_identity_prices_and_position_order(
    monkeypatch: pytest.MonkeyPatch,
    runner_deployment: str,
    runner_cycle: str,
    expected_deployment: str,
    expected_cycle: str,
) -> None:
    prices = {"WETH": {"price_usd": "2500", "confidence": "HIGH"}}
    market = SimpleNamespace(prices=prices)
    strategy = SimpleNamespace(create_market_snapshot=lambda: market)
    saved: list[Any] = []

    async def save(snapshot_id: int, rows: list[Any]) -> int:
        assert snapshot_id == 41
        saved.extend(rows)
        return len(rows)

    positions = [SimpleNamespace(label="first"), SimpleNamespace(label="second")]
    calls: list[dict[str, Any]] = []

    def materialize(**kwargs: Any) -> str:
        calls.append(kwargs)
        return kwargs["position"].label

    _install_materializer(monkeypatch, materialize)
    runner = _runner(
        state_manager=SimpleNamespace(save_position_state_snapshots=save),
        deployment_id=runner_deployment,
        cycle_id=runner_cycle,
        strategy=strategy,
    )

    written = await runner_state._persist_position_state_snapshots(runner, _snapshot(*positions), snapshot_id=41)

    assert written == 2
    assert saved == ["first", "second"]
    assert [call["position"] for call in calls] == positions
    assert all(call["market"] is market and call["prices"] is prices for call in calls)
    assert all(call["deployment_id"] == expected_deployment for call in calls)
    assert all(call["cycle_id"] == expected_cycle for call in calls)


@pytest.mark.asyncio
async def test_context_uses_fallback_strategy_and_allows_missing_market_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fallback_market = SimpleNamespace(prices={"USDC": Decimal("1")})
    fallback = SimpleNamespace(create_market_snapshot=lambda: fallback_market)
    materialize = MagicMock(side_effect=["fallback-row", "no-market-row"])
    _install_materializer(monkeypatch, materialize)
    save = AsyncMock(return_value=1)
    runner = _runner(mode="paper", state_manager=SimpleNamespace(save_position_state_snapshots=save))
    runner.strategy = fallback

    assert await runner_state._persist_position_state_snapshots(runner, _snapshot(object()), snapshot_id=42) == 1
    assert materialize.call_args_list[0].kwargs["market"] is fallback_market

    runner.strategy = SimpleNamespace()
    assert await runner_state._persist_position_state_snapshots(runner, _snapshot(object()), snapshot_id=43) == 1
    assert materialize.call_args_list[1].kwargs["market"] is None
    assert materialize.call_args_list[1].kwargs["prices"] is None


@pytest.mark.asyncio
async def test_all_unrecognized_rows_and_backend_zero_remain_distinct_measured_zero_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materialize = MagicMock(return_value=None)
    _install_materializer(monkeypatch, materialize)
    save = AsyncMock(return_value=0)
    runner = _runner(mode="paper", state_manager=SimpleNamespace(save_position_state_snapshots=save))

    assert await runner_state._persist_position_state_snapshots(runner, _snapshot(object()), snapshot_id=44) == 0
    save.assert_not_awaited()

    materialize.return_value = "row"
    assert await runner_state._persist_position_state_snapshots(runner, _snapshot(object()), snapshot_id=45) == 0
    save.assert_awaited_once_with(45, ["row"])


@pytest.mark.asyncio
async def test_market_snapshot_failure_logs_debug_and_uses_position_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = RuntimeError("market unavailable")

    def create_market_snapshot() -> None:
        raise failure

    logger = MagicMock()
    monkeypatch.setattr(runner_state, "logger", logger)
    materialize = MagicMock(return_value="row")
    _install_materializer(monkeypatch, materialize)
    save = AsyncMock(return_value=1)
    runner = _runner(
        mode="paper",
        state_manager=SimpleNamespace(save_position_state_snapshots=save),
        strategy=SimpleNamespace(create_market_snapshot=create_market_snapshot),
    )

    assert await runner_state._persist_position_state_snapshots(runner, _snapshot(object()), snapshot_id=2) == 1
    logger.debug.assert_called_once_with("Track C: market snapshot fetch failed: %s", failure)
    assert materialize.call_args.kwargs["market"] is None
    assert materialize.call_args.kwargs["prices"] is None


@pytest.mark.asyncio
async def test_failing_prices_descriptor_degrades_to_unpriced_materialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MarketWithBrokenPrices:
        @property
        def prices(self) -> Any:
            raise RuntimeError("prices unavailable")

    materialize = MagicMock(return_value="row")
    _install_materializer(monkeypatch, materialize)
    save = AsyncMock(return_value=1)
    runner = _runner(
        state_manager=SimpleNamespace(save_position_state_snapshots=save),
        strategy=SimpleNamespace(create_market_snapshot=MarketWithBrokenPrices),
    )

    assert await runner_state._persist_position_state_snapshots(runner, _snapshot(object()), snapshot_id=3) == 1
    assert materialize.call_args.kwargs["prices"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["paper", "dry_run"])
async def test_non_live_materialization_error_logs_and_fold_continues_in_order(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    positions = [
        SimpleNamespace(label="broken"),
        SimpleNamespace(label="kept"),
        SimpleNamespace(label="unrecognized"),
    ]
    failure = TypeError("bad position")
    outputs: list[Any] = [failure, "kept-row", None]

    def materialize(**kwargs: Any) -> Any:
        output = outputs.pop(0)
        if isinstance(output, Exception):
            raise output
        return output

    _install_materializer(monkeypatch, materialize)
    logger = MagicMock()
    monkeypatch.setattr(runner_state, "logger", logger)
    save = AsyncMock(return_value=1)
    runner = _runner(mode=mode, state_manager=SimpleNamespace(save_position_state_snapshots=save))

    assert await runner_state._persist_position_state_snapshots(runner, _snapshot(*positions), snapshot_id=4) == 1
    save.assert_awaited_once_with(4, ["kept-row"])
    logger.error.assert_called_once_with(
        "Track C: materialise_position_state failed for position %r: %s",
        "broken",
        failure,
        exc_info=True,
    )


@pytest.mark.asyncio
async def test_live_materialization_error_is_typed_and_stops_before_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = ValueError("cannot normalize")

    def materialize(**kwargs: Any) -> None:
        raise failure

    _install_materializer(monkeypatch, materialize)
    save = AsyncMock()
    runner = _runner(state_manager=SimpleNamespace(save_position_state_snapshots=save))

    with pytest.raises(AccountingPersistenceError) as raised:
        await runner_state._persist_position_state_snapshots(
            runner,
            _snapshot(SimpleNamespace(label="broken")),
            snapshot_id=5,
        )

    assert raised.value.write_kind == AccountingWriteKind.SNAPSHOT
    assert raised.value.deployment_id == "runner-deployment"
    assert raised.value.cause is failure
    assert raised.value.__cause__ is failure
    save.assert_not_awaited()


@pytest.mark.asyncio
async def test_live_typed_write_error_propagates_same_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_materializer(monkeypatch, lambda **kwargs: "row")
    failure = AccountingPersistenceError(
        AccountingWriteKind.SNAPSHOT,
        deployment_id="backend-deployment",
        message="typed backend failure",
    )
    save = AsyncMock(side_effect=failure)
    runner = _runner(state_manager=SimpleNamespace(save_position_state_snapshots=save))

    with pytest.raises(AccountingPersistenceError) as raised:
        await runner_state._persist_position_state_snapshots(runner, _snapshot(object()), snapshot_id=6)

    assert raised.value is failure


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["paper", "dry_run"])
async def test_non_live_typed_write_error_logs_exactly_and_returns_zero(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    _install_materializer(monkeypatch, lambda **kwargs: "row")
    failure = AccountingPersistenceError(AccountingWriteKind.SNAPSHOT, deployment_id="backend-deployment")
    save = AsyncMock(side_effect=failure)
    logger = MagicMock()
    monkeypatch.setattr(runner_state, "logger", logger)
    runner = _runner(mode=mode, state_manager=SimpleNamespace(save_position_state_snapshots=save))

    assert await runner_state._persist_position_state_snapshots(runner, _snapshot(object()), snapshot_id=7) == 0
    logger.error.assert_called_once_with(
        "Track C: AccountingPersistenceError saving %d rows for %s (non-live, continuing)",
        1,
        "snapshot-deployment",
        exc_info=True,
    )


@pytest.mark.asyncio
async def test_live_untyped_write_error_is_wrapped_with_runner_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_materializer(monkeypatch, lambda **kwargs: "row")
    failure = OSError("disk full")
    save = AsyncMock(side_effect=failure)
    runner = _runner(state_manager=SimpleNamespace(save_position_state_snapshots=save))

    with pytest.raises(AccountingPersistenceError) as raised:
        await runner_state._persist_position_state_snapshots(runner, _snapshot(object()), snapshot_id=8)

    assert raised.value.write_kind == AccountingWriteKind.SNAPSHOT
    assert raised.value.deployment_id == "runner-deployment"
    assert raised.value.cause is failure
    assert raised.value.__cause__ is failure


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["paper", "dry_run"])
async def test_non_live_untyped_write_error_logs_exactly_and_returns_zero(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    _install_materializer(monkeypatch, lambda **kwargs: "row")
    failure = OSError("disk full")
    save = AsyncMock(side_effect=failure)
    logger = MagicMock()
    monkeypatch.setattr(runner_state, "logger", logger)
    runner = _runner(mode=mode, state_manager=SimpleNamespace(save_position_state_snapshots=save))

    assert await runner_state._persist_position_state_snapshots(runner, _snapshot(object()), snapshot_id=9) == 0
    logger.error.assert_called_once_with(
        "Track C: failed to persist %d position_state_snapshot rows for %s: %s",
        1,
        "snapshot-deployment",
        failure,
        exc_info=True,
    )


@pytest.mark.asyncio
async def test_invalid_mode_derivation_preserves_non_live_degraded_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenConfig:
        @property
        def dry_run(self) -> bool:
            raise RuntimeError("invalid config")

    failure = ValueError("bad position")

    def materialize(**kwargs: Any) -> None:
        raise failure

    _install_materializer(monkeypatch, materialize)
    logger = MagicMock()
    monkeypatch.setattr(runner_state, "logger", logger)
    save = AsyncMock()
    runner = _runner(state_manager=SimpleNamespace(save_position_state_snapshots=save))
    runner.config = BrokenConfig()

    assert await runner_state._persist_position_state_snapshots(runner, _snapshot(object()), snapshot_id=10) == 0
    logger.error.assert_called_once()
    save.assert_not_awaited()


@pytest.mark.asyncio
async def test_capture_portfolio_snapshot_preserves_parent_track_c_mirror_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    first = SimpleNamespace(label="first")
    second = SimpleNamespace(label="second")
    snapshot = PortfolioSnapshot(
        timestamp=datetime(2026, 9, 3, 12, tzinfo=UTC),
        deployment_id="snapshot-deployment",
        total_value_usd=Decimal("2"),
        available_cash_usd=Decimal("0"),
        value_confidence=ValueConfidence.HIGH,
        positions=[first, second],
        snapshot_metadata={"gas_native_status": "ok"},
    )

    async def save_track_c(snapshot_id: int, rows: list[Any]) -> int:
        events.append("track-c")
        assert snapshot_id == 77
        assert rows == ["first-row", "second-row"]
        return 2

    runner = _runner(
        state_manager=SimpleNamespace(save_position_state_snapshots=save_track_c),
        strategy=SimpleNamespace(create_market_snapshot=lambda: SimpleNamespace(prices={})),
    )
    runner._last_snapshot_time = None
    runner._snapshot_interval_seconds = 300
    strategy = SimpleNamespace(deployment_id="strategy-deployment")

    monkeypatch.setattr(runner_state, "_value_via_portfolio_valuer", lambda *args: snapshot)
    monkeypatch.setattr(runner_state, "enforce_open_position_value_invariant", lambda value: value)
    monkeypatch.setattr(runner_state, "_build_metrics_for_snapshot", AsyncMock(return_value=None))

    async def persist_parent(*args: Any) -> int:
        events.append("parent")
        return 77

    async def mirror_state(*args: Any) -> None:
        events.append("mirror")

    monkeypatch.setattr(runner_state, "_persist_snapshot_and_metrics", persist_parent)
    monkeypatch.setattr(runner_state, "_write_valuation_into_strategy_state", mirror_state)

    def materialize(**kwargs: Any) -> str:
        events.append(f"materialize-{kwargs['position'].label}")
        assert kwargs["deployment_id"] == "runner-deployment"
        assert kwargs["cycle_id"] == "runner-cycle"
        return f"{kwargs['position'].label}-row"

    _install_materializer(monkeypatch, materialize)

    result = await runner_state.capture_portfolio_snapshot(runner, strategy, iteration_number=12)

    assert result is snapshot
    assert events == ["parent", "materialize-first", "materialize-second", "track-c", "mirror"]
    assert snapshot.deployment_id == "runner-deployment"
    assert snapshot.cycle_id == "runner-cycle"


@pytest.mark.asyncio
async def test_public_path_preserves_parent_row_when_live_track_c_write_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    snapshot = PortfolioSnapshot(
        timestamp=datetime(2026, 9, 3, 12, tzinfo=UTC),
        deployment_id="snapshot-deployment",
        total_value_usd=Decimal("1"),
        available_cash_usd=Decimal("0"),
        value_confidence=ValueConfidence.HIGH,
        positions=[SimpleNamespace(label="open-position")],
        snapshot_metadata={"gas_native_status": "ok"},
    )
    track_c_failure = OSError("track C unavailable")

    async def save_track_c(snapshot_id: int, rows: list[Any]) -> int:
        events.append("track-c")
        raise track_c_failure

    runner = _runner(
        state_manager=SimpleNamespace(save_position_state_snapshots=save_track_c),
        strategy=SimpleNamespace(create_market_snapshot=lambda: SimpleNamespace(prices={})),
    )
    runner._last_snapshot_time = None
    runner._snapshot_interval_seconds = 300
    strategy = SimpleNamespace(deployment_id="strategy-deployment")

    monkeypatch.setattr(runner_state, "_value_via_portfolio_valuer", lambda *args: snapshot)
    monkeypatch.setattr(runner_state, "enforce_open_position_value_invariant", lambda value: value)
    monkeypatch.setattr(runner_state, "_build_metrics_for_snapshot", AsyncMock(return_value=None))

    async def persist_parent(*args: Any) -> int:
        events.append("parent")
        return 78

    mirror = AsyncMock()
    monkeypatch.setattr(runner_state, "_persist_snapshot_and_metrics", persist_parent)
    monkeypatch.setattr(runner_state, "_write_valuation_into_strategy_state", mirror)
    _install_materializer(monkeypatch, lambda **kwargs: "row")

    with pytest.raises(AccountingPersistenceError) as raised:
        await runner_state.capture_portfolio_snapshot(runner, strategy, iteration_number=13)

    assert events == ["parent", "track-c"]
    assert raised.value.write_kind == AccountingWriteKind.SNAPSHOT
    assert raised.value.cause is track_c_failure
    assert runner._last_snapshot_time is not None
    mirror.assert_not_awaited()
