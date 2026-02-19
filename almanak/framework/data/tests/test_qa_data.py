"""Pytest Integration Tests for Data QA Validation.

This module provides pytest tests that run the Data QA Framework against real data sources.
These tests can be used in CI/CD to validate data quality before strategy deployment.

Test Functions:
    - test_qa_cex_spot_prices: Validates CEX spot prices from CoinGecko
    - test_qa_dex_spot_prices: Validates DEX spot prices from multi-DEX aggregator
    - test_qa_cex_historical: Validates historical CEX OHLCV data quality
    - test_qa_dex_historical: Validates historical DEX WETH-denominated prices
    - test_qa_rsi_indicators: Validates RSI indicator calculations

Usage:
    Run all QA tests:
        uv run pytest src/data/tests/test_qa_data.py -v

    Run specific test:
        uv run pytest src/data/tests/test_qa_data.py::test_qa_cex_spot_prices -v

    Generate report (with --qa-report flag):
        uv run pytest src/data/tests/test_qa_data.py -v --qa-report

    Skip plot generation for faster runs:
        uv run pytest src/data/tests/test_qa_data.py -v -m "not generate_plots"
"""

from pathlib import Path

import pytest

from almanak.framework.data.qa import QARunner, load_config
from almanak.framework.data.qa.config import QAConfig, QAThresholds
from almanak.framework.data.qa.tests.cex_historical import CEXHistoricalResult, CEXHistoricalTest
from almanak.framework.data.qa.tests.cex_spot import CEXSpotPriceTest, CEXSpotResult
from almanak.framework.data.qa.tests.dex_historical import DEXHistoricalResult, DEXHistoricalTest
from almanak.framework.data.qa.tests.dex_spot import DEXSpotPriceTest, DEXSpotResult
from almanak.framework.data.qa.tests.rsi import RSIResult, RSITest

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def qa_config() -> QAConfig:
    """Load the default QA configuration from config.yaml.

    Returns:
        QAConfig with token lists and thresholds from config.yaml.
    """
    return load_config()


@pytest.fixture
def qa_config_minimal() -> QAConfig:
    """Create a minimal QA configuration for faster testing.

    Uses a small subset of tokens for quicker test runs while still
    providing meaningful validation.

    Returns:
        QAConfig with minimal token lists for quick testing.
    """
    return QAConfig(
        chain="arbitrum",
        historical_days=7,
        timeframe="4h",
        rsi_period=14,
        thresholds=QAThresholds(
            min_confidence=0.8,
            max_price_impact_bps=100,
            max_gap_hours=8.0,
            max_stale_seconds=120,
        ),
        popular_tokens=["ETH", "WBTC"],
        additional_tokens=["LINK"],
        dex_tokens=["USDC", "LINK"],
    )


@pytest.fixture
def output_dir(request: pytest.FixtureRequest, tmp_path: Path) -> Path:
    """Get the output directory for QA reports.

    If --qa-report flag is passed, uses reports/qa-data directory.
    Otherwise uses pytest's tmp_path fixture.

    Args:
        request: pytest fixture request object.
        tmp_path: pytest built-in temporary path fixture.

    Returns:
        Path to output directory for reports.
    """
    if request.config.getoption("--qa-report", default=False):
        output_path = Path("reports/qa-data")
        output_path.mkdir(parents=True, exist_ok=True)
        return output_path
    else:
        return tmp_path


@pytest.fixture
def skip_plots(request: pytest.FixtureRequest) -> bool:
    """Determine whether to skip plot generation.

    By default, skip plots for faster test runs.
    Use the generate_plots marker to enable plot generation.

    Args:
        request: pytest fixture request object.

    Returns:
        True if plots should be skipped, False otherwise.
    """
    # Check if test has generate_plots marker
    if request.node.get_closest_marker("generate_plots"):
        return False
    return True


@pytest.fixture
def qa_runner(
    qa_config: QAConfig,
    output_dir: Path,
    skip_plots: bool,
) -> QARunner:
    """Create a QA runner with default configuration.

    Args:
        qa_config: QA configuration loaded from config.yaml.
        output_dir: Directory for reports and plots.
        skip_plots: Whether to skip plot generation.

    Returns:
        QARunner configured for testing.
    """
    return QARunner(
        config=qa_config,
        output_dir=output_dir,
        skip_plots=skip_plots,
    )


# =============================================================================
# CEX Spot Price Tests
# =============================================================================


@pytest.mark.asyncio
async def test_qa_cex_spot_prices(qa_config: QAConfig) -> None:
    """Validate CEX spot prices from CoinGecko for all configured tokens.

    This test fetches current USD prices for each token in the config
    and validates:
    - Price is greater than zero
    - Confidence meets minimum threshold
    - Data is fresh (not stale)

    Asserts:
        All CEX spot price tests pass.
    """
    async with CEXSpotPriceTest(qa_config) as test:
        results: list[CEXSpotResult] = await test.run()

    # Log results for visibility
    _log_cex_spot_results(results)

    # Assert all pass
    failed = [r for r in results if not r.passed]
    if failed:
        failure_details = "\n".join(f"  - {r.token}: {r.error}" for r in failed)
        pytest.fail(f"CEX spot price tests failed for {len(failed)}/{len(results)} tokens:\n{failure_details}")


# =============================================================================
# DEX Spot Price Tests
# =============================================================================


@pytest.mark.asyncio
async def test_qa_dex_spot_prices(qa_config: QAConfig) -> None:
    """Validate DEX spot prices (WETH-quoted) for configured DEX tokens.

    This test fetches WETH-quoted prices from the multi-DEX aggregator
    and validates:
    - Price is greater than zero
    - Price impact is within acceptable threshold

    Asserts:
        All DEX spot price tests pass.
    """
    async with DEXSpotPriceTest(qa_config) as test:
        results: list[DEXSpotResult] = await test.run()

    # Log results for visibility
    _log_dex_spot_results(results)

    # Assert all pass
    failed = [r for r in results if not r.passed]
    if failed:
        failure_details = "\n".join(f"  - {r.token}: {r.error}" for r in failed)
        pytest.fail(f"DEX spot price tests failed for {len(failed)}/{len(results)} tokens:\n{failure_details}")


# =============================================================================
# CEX Historical Price Tests
# =============================================================================


@pytest.mark.asyncio
async def test_qa_cex_historical(qa_config: QAConfig) -> None:
    """Validate historical CEX OHLCV data quality for all configured tokens.

    This test fetches historical OHLCV data from CoinGecko and validates:
    - Data completeness (no excessive gaps)
    - Gap detection (max gap within threshold)

    Asserts:
        All CEX historical price tests pass.
    """
    async with CEXHistoricalTest(qa_config) as test:
        results: list[CEXHistoricalResult] = await test.run()

    # Log results for visibility
    _log_cex_historical_results(results)

    # Assert all pass
    failed = [r for r in results if not r.passed]
    if failed:
        failure_details = "\n".join(f"  - {r.token}: {r.error or f'max_gap={r.max_gap_hours:.1f}h'}" for r in failed)
        pytest.fail(f"CEX historical tests failed for {len(failed)}/{len(results)} tokens:\n{failure_details}")


# =============================================================================
# DEX Historical Price Tests
# =============================================================================


@pytest.mark.asyncio
async def test_qa_dex_historical(qa_config: QAConfig) -> None:
    """Validate historical DEX WETH-denominated prices for configured tokens.

    This test derives WETH prices from CEX data and validates:
    - Sufficient data points converted
    - At least 50% of candles successfully converted

    Note:
        DEX historical prices are derived from CEX data (token/USD ÷ ETH/USD).

    Asserts:
        All DEX historical price tests pass.
    """
    async with DEXHistoricalTest(qa_config) as test:
        results: list[DEXHistoricalResult] = await test.run()

    # Log results for visibility
    _log_dex_historical_results(results)

    # Assert all pass
    failed = [r for r in results if not r.passed]
    if failed:
        failure_details = "\n".join(f"  - {r.token}: {r.error or f'points={r.total_points}'}" for r in failed)
        pytest.fail(f"DEX historical tests failed for {len(failed)}/{len(results)} tokens:\n{failure_details}")


# =============================================================================
# RSI Indicator Tests
# =============================================================================


@pytest.mark.asyncio
async def test_qa_rsi_indicators(qa_config: QAConfig) -> None:
    """Validate RSI indicator calculations for all configured tokens.

    This test calculates RSI for each token and validates:
    - All RSI values are within valid range (0-100)
    - Current RSI and statistics are computed correctly

    Asserts:
        All RSI indicator tests pass.
    """
    async with RSITest(qa_config) as test:
        results: list[RSIResult] = await test.run()

    # Log results for visibility
    _log_rsi_results(results)

    # Assert all pass
    failed = [r for r in results if not r.passed]
    if failed:
        failure_details = "\n".join(f"  - {r.token}: {r.error}" for r in failed)
        pytest.fail(f"RSI indicator tests failed for {len(failed)}/{len(results)} tokens:\n{failure_details}")


# =============================================================================
# Full Suite Test (with Report Generation)
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.generate_plots
async def test_qa_full_suite(qa_runner: QARunner) -> None:
    """Run the complete QA test suite with report generation.

    This test runs all QA tests and generates a comprehensive report.
    Use --qa-report flag to save reports to reports/qa-data/.

    Asserts:
        All QA tests pass.
    """
    report = await qa_runner.run_all()

    # Log summary
    print(f"\n{'=' * 60}")
    print("QA Test Suite Summary")
    print(f"{'=' * 60}")
    print(f"Total Tests: {report.total_tests}")
    print(f"Passed: {report.passed_tests}")
    print(f"Failed: {report.failed_tests}")
    print(f"Duration: {report.total_duration_seconds:.2f}s")
    if report.report_path:
        print(f"Report: {report.report_path}")
    print(f"{'=' * 60}\n")

    # Assert all pass
    if not report.passed:
        failed_details = []
        for cex_spot_r in report.cex_spot_results:
            if not cex_spot_r.passed:
                failed_details.append(f"  - CEX Spot {cex_spot_r.token}: {cex_spot_r.error}")
        for dex_spot_r in report.dex_spot_results:
            if not dex_spot_r.passed:
                failed_details.append(f"  - DEX Spot {dex_spot_r.token}: {dex_spot_r.error}")
        for cex_hist_r in report.cex_historical_results:
            if not cex_hist_r.passed:
                failed_details.append(f"  - CEX Historical {cex_hist_r.token}: {cex_hist_r.error}")
        for dex_hist_r in report.dex_historical_results:
            if not dex_hist_r.passed:
                failed_details.append(f"  - DEX Historical {dex_hist_r.token}: {dex_hist_r.error}")
        for rsi_r in report.rsi_results:
            if not rsi_r.passed:
                failed_details.append(f"  - RSI {rsi_r.token}: {rsi_r.error}")

        pytest.fail(
            f"QA test suite failed: {report.failed_tests}/{report.total_tests} tests failed\n"
            + "\n".join(failed_details)
        )


# =============================================================================
# Helper Functions for Logging
# =============================================================================


def _log_cex_spot_results(results: list[CEXSpotResult]) -> None:
    """Log CEX spot price results to stdout."""
    print(f"\n{'=' * 60}")
    print("CEX Spot Prices (USD)")
    print(f"{'=' * 60}")
    print(f"{'Token':<8} {'Price USD':<15} {'Confidence':<12} {'Fresh':<6} {'Status'}")
    print("-" * 60)
    for r in results:
        price_str = f"${r.price_usd:,.2f}" if r.price_usd else "N/A"
        conf_str = f"{r.confidence:.2f}" if r.confidence else "N/A"
        fresh_str = "Yes" if r.is_fresh else "No"
        status = "PASS" if r.passed else "FAIL"
        print(f"{r.token:<8} {price_str:<15} {conf_str:<12} {fresh_str:<6} {status}")
    print(f"{'=' * 60}\n")


def _log_dex_spot_results(results: list[DEXSpotResult]) -> None:
    """Log DEX spot price results to stdout."""
    print(f"\n{'=' * 60}")
    print("DEX Spot Prices (WETH)")
    print(f"{'=' * 60}")
    print(f"{'Token':<8} {'DEX':<15} {'Price WETH':<15} {'Impact BPS':<12} {'Status'}")
    print("-" * 60)
    for r in results:
        dex_str = r.best_dex or "N/A"
        price_str = f"{r.price_weth:.6f}" if r.price_weth else "N/A"
        impact_str = f"{r.price_impact_bps}" if r.price_impact_bps is not None else "N/A"
        status = "PASS" if r.passed else "FAIL"
        print(f"{r.token:<8} {dex_str:<15} {price_str:<15} {impact_str:<12} {status}")
    print(f"{'=' * 60}\n")


def _log_cex_historical_results(results: list[CEXHistoricalResult]) -> None:
    """Log CEX historical results to stdout."""
    print(f"\n{'=' * 60}")
    print("CEX Historical Data Quality")
    print(f"{'=' * 60}")
    print(f"{'Token':<8} {'Total':<8} {'Missing':<10} {'Max Gap':<10} {'Status'}")
    print("-" * 60)
    for r in results:
        total_str = str(r.total_candles) if r.total_candles else "0"
        missing_str = str(r.missing_count) if r.missing_count is not None else "N/A"
        gap_str = f"{r.max_gap_hours:.1f}h" if r.max_gap_hours is not None else "N/A"
        status = "PASS" if r.passed else "FAIL"
        print(f"{r.token:<8} {total_str:<8} {missing_str:<10} {gap_str:<10} {status}")
    print(f"{'=' * 60}\n")


def _log_dex_historical_results(results: list[DEXHistoricalResult]) -> None:
    """Log DEX historical results to stdout."""
    print(f"\n{'=' * 60}")
    print("DEX Historical Data (WETH-denominated)")
    print(f"{'=' * 60}")
    print(f"{'Token':<8} {'Points':<10} {'Status'}")
    print("-" * 60)
    for r in results:
        points_str = str(r.total_points)
        status = "PASS" if r.passed else "FAIL"
        print(f"{r.token:<8} {points_str:<10} {status}")
    print(f"{'=' * 60}\n")


def _log_rsi_results(results: list[RSIResult]) -> None:
    """Log RSI indicator results to stdout."""
    print(f"\n{'=' * 60}")
    print("RSI Indicators")
    print(f"{'=' * 60}")
    print(f"{'Token':<8} {'Current':<10} {'Signal':<12} {'Min':<8} {'Max':<8} {'Avg':<8} {'Status'}")
    print("-" * 60)
    for r in results:
        current_str = f"{r.current_rsi:.1f}" if r.current_rsi is not None else "N/A"
        signal_str = r.signal or "N/A"
        min_str = f"{r.min_rsi:.1f}" if r.min_rsi is not None else "N/A"
        max_str = f"{r.max_rsi:.1f}" if r.max_rsi is not None else "N/A"
        avg_str = f"{r.avg_rsi:.1f}" if r.avg_rsi is not None else "N/A"
        status = "PASS" if r.passed else "FAIL"
        print(f"{r.token:<8} {current_str:<10} {signal_str:<12} {min_str:<8} {max_str:<8} {avg_str:<8} {status}")
    print(f"{'=' * 60}\n")
