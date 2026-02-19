"""QA Runner for Data QA Framework.

This module provides the main test runner that orchestrates all QA tests,
generates plots, and produces the final Markdown report.

Example:
    from almanak.framework.data.qa.runner import QARunner
    from almanak.framework.data.qa.config import load_config

    config = load_config()
    runner = QARunner(config, output_dir=Path("reports/qa-data"))
    result = await runner.run_all()

    print(f"Overall: {'PASSED' if result.passed else 'FAILED'}")
    print(f"Report: {result.report_path}")
"""

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

from almanak.framework.data.qa.config import QAConfig
from almanak.framework.data.qa.reporting import PlotGenerator, ReportGenerator
from almanak.framework.data.qa.tests.cex_historical import CEXHistoricalResult, CEXHistoricalTest
from almanak.framework.data.qa.tests.cex_spot import CEXSpotPriceTest, CEXSpotResult
from almanak.framework.data.qa.tests.dex_historical import DEXHistoricalResult, DEXHistoricalTest
from almanak.framework.data.qa.tests.dex_spot import DEXSpotPriceTest, DEXSpotResult
from almanak.framework.data.qa.tests.rsi import RSIResult, RSITest

logger = logging.getLogger(__name__)


@dataclass
class TestDuration:
    """Duration tracking for a single test category.

    Attributes:
        category: Test category name (e.g., "cex_spot", "dex_historical")
        start_time: Test start timestamp
        end_time: Test end timestamp
        duration_seconds: Total duration in seconds
    """

    category: str
    start_time: float = 0.0
    end_time: float = 0.0
    duration_seconds: float = 0.0

    def start(self) -> None:
        """Mark the start of the test."""
        self.start_time = time.time()

    def stop(self) -> None:
        """Mark the end of the test and calculate duration."""
        self.end_time = time.time()
        self.duration_seconds = self.end_time - self.start_time


@dataclass
class QAReport:
    """Complete QA report containing all test results.

    Attributes:
        config: QA configuration used for the tests
        cex_spot_results: Results from CEX spot price tests
        dex_spot_results: Results from DEX spot price tests
        cex_historical_results: Results from CEX historical tests
        dex_historical_results: Results from DEX historical tests
        rsi_results: Results from RSI indicator tests
        durations: Duration tracking for each test category
        total_duration_seconds: Total test suite duration
        report_path: Path to the generated Markdown report
        passed: Whether all tests passed
    """

    config: QAConfig
    cex_spot_results: list[CEXSpotResult] = field(default_factory=list)
    dex_spot_results: list[DEXSpotResult] = field(default_factory=list)
    cex_historical_results: list[CEXHistoricalResult] = field(default_factory=list)
    dex_historical_results: list[DEXHistoricalResult] = field(default_factory=list)
    rsi_results: list[RSIResult] = field(default_factory=list)
    durations: dict[str, TestDuration] = field(default_factory=dict)
    total_duration_seconds: float = 0.0
    report_path: Path | None = None
    passed: bool = False

    @property
    def total_tests(self) -> int:
        """Return total number of tests run."""
        return (
            len(self.cex_spot_results)
            + len(self.dex_spot_results)
            + len(self.cex_historical_results)
            + len(self.dex_historical_results)
            + len(self.rsi_results)
        )

    @property
    def passed_tests(self) -> int:
        """Return number of tests that passed."""
        return (
            sum(1 for r in self.cex_spot_results if r.passed)
            + sum(1 for r in self.dex_spot_results if r.passed)
            + sum(1 for r in self.cex_historical_results if r.passed)
            + sum(1 for r in self.dex_historical_results if r.passed)
            + sum(1 for r in self.rsi_results if r.passed)
        )

    @property
    def failed_tests(self) -> int:
        """Return number of tests that failed."""
        return self.total_tests - self.passed_tests


class QARunner:
    """Orchestrates all QA tests and generates reports.

    This class is the main entry point for running the complete QA test suite.
    It orchestrates all test categories, generates plots, and produces a
    comprehensive Markdown report.

    Attributes:
        config: QA configuration with token lists and thresholds
        output_dir: Directory to save reports and plots
        skip_plots: If True, skip plot generation for faster runs

    Example:
        config = load_config()
        runner = QARunner(config, output_dir=Path("reports/qa-data"))

        # Run all tests
        result = await runner.run_all()

        if result.passed:
            print("All tests passed!")
        else:
            print(f"Failed: {result.failed_tests}/{result.total_tests}")

        print(f"Report: {result.report_path}")
    """

    def __init__(
        self,
        config: QAConfig,
        output_dir: Path,
        skip_plots: bool = False,
    ) -> None:
        """Initialize the QA runner.

        Args:
            config: QA configuration with token lists and thresholds
            output_dir: Directory to save reports and plots
            skip_plots: If True, skip plot generation for faster runs
        """
        self.config = config
        self.output_dir = output_dir
        self.skip_plots = skip_plots
        self.plots_dir = output_dir / "plots"
        self._ensure_output_dirs()

    def _ensure_output_dirs(self) -> None:
        """Ensure output directories exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if not self.skip_plots:
            self.plots_dir.mkdir(parents=True, exist_ok=True)

    async def run_all(self) -> QAReport:
        """Run all QA tests and generate the report.

        Returns:
            QAReport with all test results, durations, and report path
        """
        logger.info("Starting QA test suite for chain=%s", self.config.chain)
        suite_start = time.time()

        # Initialize report
        report = QAReport(config=self.config)

        # Run all test categories
        report.cex_spot_results, report.durations["cex_spot"] = await self._run_cex_spot()
        report.dex_spot_results, report.durations["dex_spot"] = await self._run_dex_spot()
        report.cex_historical_results, report.durations["cex_historical"] = await self._run_cex_historical()
        report.dex_historical_results, report.durations["dex_historical"] = await self._run_dex_historical()
        report.rsi_results, report.durations["rsi"] = await self._run_rsi()

        # Calculate total duration
        report.total_duration_seconds = time.time() - suite_start

        # Generate plots if enabled
        if not self.skip_plots:
            await self._generate_plots(report)

        # Generate Markdown report
        report.report_path = self._generate_report(report)

        # Determine overall pass/fail
        report.passed = report.failed_tests == 0

        # Log summary
        logger.info(
            "QA test suite complete: %d/%d passed (%.1f%%) in %.2fs",
            report.passed_tests,
            report.total_tests,
            (report.passed_tests / report.total_tests * 100) if report.total_tests > 0 else 0,
            report.total_duration_seconds,
        )
        logger.info("Report saved to: %s", report.report_path)

        return report

    async def _run_cex_spot(self) -> tuple[list[CEXSpotResult], TestDuration]:
        """Run CEX spot price tests.

        Returns:
            Tuple of (results list, duration tracking)
        """
        duration = TestDuration(category="cex_spot")
        duration.start()

        logger.info("Running CEX spot price tests...")
        async with CEXSpotPriceTest(self.config) as test:
            results = await test.run()

        duration.stop()
        logger.info(
            "CEX spot tests complete: %d/%d passed in %.2fs",
            sum(1 for r in results if r.passed),
            len(results),
            duration.duration_seconds,
        )

        return results, duration

    async def _run_dex_spot(self) -> tuple[list[DEXSpotResult], TestDuration]:
        """Run DEX spot price tests.

        Returns:
            Tuple of (results list, duration tracking)
        """
        duration = TestDuration(category="dex_spot")
        duration.start()

        logger.info("Running DEX spot price tests...")
        async with DEXSpotPriceTest(self.config) as test:
            results = await test.run()

        duration.stop()
        logger.info(
            "DEX spot tests complete: %d/%d passed in %.2fs",
            sum(1 for r in results if r.passed),
            len(results),
            duration.duration_seconds,
        )

        return results, duration

    async def _run_cex_historical(self) -> tuple[list[CEXHistoricalResult], TestDuration]:
        """Run CEX historical price tests.

        Returns:
            Tuple of (results list, duration tracking)
        """
        duration = TestDuration(category="cex_historical")
        duration.start()

        logger.info("Running CEX historical tests...")
        async with CEXHistoricalTest(self.config) as test:
            results = await test.run()

        duration.stop()
        logger.info(
            "CEX historical tests complete: %d/%d passed in %.2fs",
            sum(1 for r in results if r.passed),
            len(results),
            duration.duration_seconds,
        )

        return results, duration

    async def _run_dex_historical(self) -> tuple[list[DEXHistoricalResult], TestDuration]:
        """Run DEX historical price tests.

        Returns:
            Tuple of (results list, duration tracking)
        """
        duration = TestDuration(category="dex_historical")
        duration.start()

        logger.info("Running DEX historical tests...")
        async with DEXHistoricalTest(self.config) as test:
            results = await test.run()

        duration.stop()
        logger.info(
            "DEX historical tests complete: %d/%d passed in %.2fs",
            sum(1 for r in results if r.passed),
            len(results),
            duration.duration_seconds,
        )

        return results, duration

    async def _run_rsi(self) -> tuple[list[RSIResult], TestDuration]:
        """Run RSI indicator tests.

        Returns:
            Tuple of (results list, duration tracking)
        """
        duration = TestDuration(category="rsi")
        duration.start()

        logger.info("Running RSI indicator tests...")
        async with RSITest(self.config) as test:
            results = await test.run()

        duration.stop()
        logger.info(
            "RSI tests complete: %d/%d passed in %.2fs",
            sum(1 for r in results if r.passed),
            len(results),
            duration.duration_seconds,
        )

        return results, duration

    async def _generate_plots(self, report: QAReport) -> None:
        """Generate all plots for the report.

        Args:
            report: QAReport with test results to plot
        """
        logger.info("Generating plots...")

        plot_generator = PlotGenerator(output_dir=self.plots_dir)

        # Generate CEX price plots (USD)
        for cex_hist_result in report.cex_historical_results:
            if cex_hist_result.candles:
                plot_generator.create_price_plot(cex_hist_result.token, cex_hist_result.candles, "USD")

        # Generate DEX price plots (WETH)
        for dex_hist_result in report.dex_historical_results:
            if dex_hist_result.weth_prices:
                plot_generator.create_weth_price_plot(dex_hist_result.token, dex_hist_result.weth_prices)

        # Generate RSI plots
        for rsi_result in report.rsi_results:
            if rsi_result.rsi_history:
                plot_generator.create_rsi_plot(rsi_result.token, rsi_result.rsi_history)

        logger.info("Plot generation complete")

    def _generate_report(self, report: QAReport) -> Path:
        """Generate the Markdown report.

        Args:
            report: QAReport with all test results

        Returns:
            Path to the generated report file
        """
        logger.info("Generating Markdown report...")

        report_generator = ReportGenerator(output_dir=self.output_dir)

        report_path = report_generator.generate_report(
            cex_spot_results=report.cex_spot_results,
            dex_spot_results=report.dex_spot_results,
            cex_historical_results=report.cex_historical_results,
            dex_historical_results=report.dex_historical_results,
            rsi_results=report.rsi_results,
            config=self.config,
            duration_seconds=report.total_duration_seconds,
        )

        return report_path


__all__ = [
    "QAReport",
    "QARunner",
    "TestDuration",
]
