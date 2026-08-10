from __future__ import annotations

import asyncio
import logging
import warnings
from datetime import datetime, timedelta
from decimal import Decimal

from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig
from scripts.ci.run_benchmarks import (
    BenchmarkResult,
    FastMockDataProvider,
    _run_benchmark_samples,
    _stable_symbol_phase,
)


def test_fast_mock_data_provider_keeps_stablecoin_prices_pegged() -> None:
    start = datetime(2024, 1, 1)
    later = start + timedelta(hours=123)
    provider = FastMockDataProvider(
        {"USDC": Decimal("1"), "USDT": Decimal("1"), "WETH": Decimal("2000")},
        start,
        volatility=Decimal("0.5"),
    )

    assert asyncio.run(provider.get_price("USDC", later)) == Decimal("1")
    assert asyncio.run(provider.get_price("USDT", later)) == Decimal("1")


def test_synthetic_price_phase_is_stable_and_case_insensitive() -> None:
    assert _stable_symbol_phase("WETH") == 65
    assert _stable_symbol_phase("weth") == 65


def test_fast_mock_data_provider_iterate_keeps_stablecoin_prices_pegged() -> None:
    start = datetime(2024, 1, 1)
    provider = FastMockDataProvider(
        {"USDC": Decimal("1"), "USDT": Decimal("1"), "WETH": Decimal("2000")},
        start,
        volatility=Decimal("0.5"),
    )

    async def first_market_prices() -> dict[object, Decimal]:
        config = HistoricalDataConfig(
            start_time=start,
            end_time=start + timedelta(hours=1),
            tokens=["USDC", "USDT"],
            chains=["arbitrum"],
        )
        async for _timestamp, market_state in provider.iterate(config):
            return market_state.prices
        raise AssertionError("provider did not emit market data")

    prices = asyncio.run(first_market_prices())

    assert prices == {"USDC": Decimal("1"), "USDT": Decimal("1")}


def _benchmark_result(*, elapsed: float, ticks: int = 8761, trades: int = 0) -> BenchmarkResult:
    return BenchmarkResult(
        name="sample",
        elapsed_seconds=elapsed,
        limit_seconds=30.0,
        passed=elapsed < 30.0,
        ticks=ticks,
        trades=trades,
        ticks_per_second=ticks / elapsed,
        trades_per_second=trades / elapsed,
    )


def test_sample_runner_discards_warmup_and_uses_measured_median() -> None:
    results = iter(
        [
            _benchmark_result(elapsed=99.0),  # discarded warmup
            _benchmark_result(elapsed=10.0),
            _benchmark_result(elapsed=2.0),
            _benchmark_result(elapsed=3.0),
        ]
    )

    async def benchmark(_verbose: bool) -> BenchmarkResult:
        return next(results)

    summary = asyncio.run(
        _run_benchmark_samples(
            "sample",
            benchmark,
            samples=3,
            warmups=1,
            verbose=False,
        )
    )

    assert summary["elapsed_seconds"] == 3.0
    assert summary["samples_seconds"] == [10.0, 2.0, 3.0]
    assert summary["passed"] is True


def test_sample_runner_fails_on_nondeterministic_work() -> None:
    results = iter(
        [
            _benchmark_result(elapsed=2.0, ticks=8761),
            _benchmark_result(elapsed=2.1, ticks=8760),
            _benchmark_result(elapsed=2.2, ticks=8761),
        ]
    )

    async def benchmark(_verbose: bool) -> BenchmarkResult:
        return next(results)

    summary = asyncio.run(
        _run_benchmark_samples(
            "sample",
            benchmark,
            samples=3,
            warmups=0,
            verbose=False,
        )
    )

    assert summary["passed"] is False
    assert "nondeterministic" in summary["error"]


def test_sample_runner_suppresses_diagnostics_and_restores_logging() -> None:
    records: list[logging.LogRecord] = []

    class CaptureHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger("benchmark-sample-test")
    handler = CaptureHandler()
    logger.addHandler(handler)
    previous_disable_level = logging.root.manager.disable

    async def benchmark(_verbose: bool) -> BenchmarkResult:
        logger.warning("sample warning")
        warnings.warn("sample warning", UserWarning, stacklevel=1)
        return _benchmark_result(elapsed=1.0)

    try:
        with warnings.catch_warnings(record=True) as caught:
            summary = asyncio.run(
                _run_benchmark_samples(
                    "sample",
                    benchmark,
                    samples=1,
                    warmups=0,
                    verbose=False,
                )
            )
    finally:
        logger.removeHandler(handler)

    assert summary["passed"] is True
    assert records == []
    assert caught == []
    assert logging.root.manager.disable == previous_disable_level
