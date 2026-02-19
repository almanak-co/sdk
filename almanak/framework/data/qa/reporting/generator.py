"""Report Generator for QA Framework.

This module provides Markdown report generation capabilities for QA test results,
including executive summaries, detailed test results, and plot references.

Example:
    from almanak.framework.data.qa.reporting.generator import ReportGenerator

    generator = ReportGenerator(output_dir=Path("reports/qa-data"))
    report_path = generator.generate_report(
        cex_spot_results=cex_spot_results,
        dex_spot_results=dex_spot_results,
        cex_historical_results=cex_historical_results,
        dex_historical_results=dex_historical_results,
        rsi_results=rsi_results,
        config=config,
        duration_seconds=elapsed,
    )
"""

import logging
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from almanak.framework.data.qa.config import QAConfig
from almanak.framework.data.qa.tests.cex_historical import CEXHistoricalResult
from almanak.framework.data.qa.tests.cex_spot import CEXSpotResult
from almanak.framework.data.qa.tests.dex_historical import DEXHistoricalResult
from almanak.framework.data.qa.tests.dex_spot import DEXSpotResult
from almanak.framework.data.qa.tests.rsi import RSIResult

logger = logging.getLogger(__name__)


@dataclass
class ReportSummary:
    """Summary statistics for the QA report.

    Attributes:
        total_tests: Total number of tests run
        passed_tests: Number of tests that passed
        failed_tests: Number of tests that failed
        cex_spot_passed: Number of CEX spot tests passed
        cex_spot_total: Total CEX spot tests
        dex_spot_passed: Number of DEX spot tests passed
        dex_spot_total: Total DEX spot tests
        cex_historical_passed: Number of CEX historical tests passed
        cex_historical_total: Total CEX historical tests
        dex_historical_passed: Number of DEX historical tests passed
        dex_historical_total: Total DEX historical tests
        rsi_passed: Number of RSI tests passed
        rsi_total: Total RSI tests
    """

    total_tests: int
    passed_tests: int
    failed_tests: int
    cex_spot_passed: int
    cex_spot_total: int
    dex_spot_passed: int
    dex_spot_total: int
    cex_historical_passed: int
    cex_historical_total: int
    dex_historical_passed: int
    dex_historical_total: int
    rsi_passed: int
    rsi_total: int

    @property
    def overall_passed(self) -> bool:
        """Return True if all tests passed."""
        return self.failed_tests == 0

    @property
    def pass_rate(self) -> float:
        """Return the pass rate as a percentage."""
        if self.total_tests == 0:
            return 0.0
        return (self.passed_tests / self.total_tests) * 100


class ReportGenerator:
    """Generates Markdown reports for QA test results.

    This class creates comprehensive Markdown reports including:
    - Header with timestamp, chain, duration, and overall status
    - Executive summary with pass/fail counts per category
    - CEX Spot Prices table
    - DEX Spot Prices table
    - Historical CEX data quality with plot references
    - Historical DEX data with plot references
    - RSI values table with plot references
    - Failures section with error details

    Attributes:
        output_dir: Directory to save generated reports
        plots_dir: Subdirectory for plot images

    Example:
        generator = ReportGenerator(output_dir=Path("reports/qa-data"))
        report_path = generator.generate_report(
            cex_spot_results=cex_spot,
            dex_spot_results=dex_spot,
            cex_historical_results=cex_hist,
            dex_historical_results=dex_hist,
            rsi_results=rsi,
            config=config,
            duration_seconds=123.45,
        )
        print(f"Report saved to: {report_path}")
    """

    def __init__(
        self,
        output_dir: Path,
        plots_subdir: str = "plots",
    ) -> None:
        """Initialize the report generator.

        Args:
            output_dir: Directory to save generated reports
            plots_subdir: Subdirectory name for plot images
        """
        self.output_dir = output_dir
        self.plots_dir = output_dir / plots_subdir
        self._ensure_output_dir()

    def _ensure_output_dir(self) -> None:
        """Ensure the output directory exists."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_report(
        self,
        cex_spot_results: list[CEXSpotResult],
        dex_spot_results: list[DEXSpotResult],
        cex_historical_results: list[CEXHistoricalResult],
        dex_historical_results: list[DEXHistoricalResult],
        rsi_results: list[RSIResult],
        config: QAConfig,
        duration_seconds: float,
        timestamp: datetime | None = None,
    ) -> Path:
        """Generate a comprehensive Markdown report.

        Args:
            cex_spot_results: Results from CEX spot price tests
            dex_spot_results: Results from DEX spot price tests
            cex_historical_results: Results from CEX historical tests
            dex_historical_results: Results from DEX historical tests
            rsi_results: Results from RSI indicator tests
            config: QA configuration used for the tests
            duration_seconds: Total test duration in seconds
            timestamp: Report timestamp (default: current time)

        Returns:
            Path to the generated report file
        """
        if timestamp is None:
            timestamp = datetime.now()

        # Calculate summary statistics
        summary = self._calculate_summary(
            cex_spot_results,
            dex_spot_results,
            cex_historical_results,
            dex_historical_results,
            rsi_results,
        )

        # Build report sections
        sections: list[str] = []

        # Header
        sections.append(self._generate_header(config, timestamp, duration_seconds, summary))

        # Executive Summary
        sections.append(self._generate_executive_summary(summary))

        # Section 1: CEX Spot Prices
        sections.append(self._generate_cex_spot_section(cex_spot_results))

        # Section 2: DEX Spot Prices
        sections.append(self._generate_dex_spot_section(dex_spot_results))

        # Section 3: Historical CEX Data
        sections.append(self._generate_cex_historical_section(cex_historical_results))

        # Section 4: Historical DEX Data
        sections.append(self._generate_dex_historical_section(dex_historical_results))

        # Section 5: RSI Values
        sections.append(self._generate_rsi_section(rsi_results))

        # Failures Section
        sections.append(
            self._generate_failures_section(
                cex_spot_results,
                dex_spot_results,
                cex_historical_results,
                dex_historical_results,
                rsi_results,
            )
        )

        # Combine all sections
        report_content = "\n\n".join(sections)

        # Generate timestamp-based filename
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
        report_filename = f"report_{timestamp_str}.md"
        report_path = self.output_dir / report_filename

        # Write report
        report_path.write_text(report_content)
        logger.info("Generated report: %s", report_path)

        # Copy to report-latest.md
        latest_path = self.output_dir / "report-latest.md"
        shutil.copy(report_path, latest_path)
        logger.info("Copied to: %s", latest_path)

        return report_path

    def _calculate_summary(
        self,
        cex_spot_results: list[CEXSpotResult],
        dex_spot_results: list[DEXSpotResult],
        cex_historical_results: list[CEXHistoricalResult],
        dex_historical_results: list[DEXHistoricalResult],
        rsi_results: list[RSIResult],
    ) -> ReportSummary:
        """Calculate summary statistics from all test results.

        Args:
            cex_spot_results: CEX spot test results
            dex_spot_results: DEX spot test results
            cex_historical_results: CEX historical test results
            dex_historical_results: DEX historical test results
            rsi_results: RSI test results

        Returns:
            ReportSummary with calculated statistics
        """
        cex_spot_passed = sum(1 for r in cex_spot_results if r.passed)
        dex_spot_passed = sum(1 for r in dex_spot_results if r.passed)
        cex_historical_passed = sum(1 for r in cex_historical_results if r.passed)
        dex_historical_passed = sum(1 for r in dex_historical_results if r.passed)
        rsi_passed = sum(1 for r in rsi_results if r.passed)

        total_tests = (
            len(cex_spot_results)
            + len(dex_spot_results)
            + len(cex_historical_results)
            + len(dex_historical_results)
            + len(rsi_results)
        )
        passed_tests = cex_spot_passed + dex_spot_passed + cex_historical_passed + dex_historical_passed + rsi_passed

        return ReportSummary(
            total_tests=total_tests,
            passed_tests=passed_tests,
            failed_tests=total_tests - passed_tests,
            cex_spot_passed=cex_spot_passed,
            cex_spot_total=len(cex_spot_results),
            dex_spot_passed=dex_spot_passed,
            dex_spot_total=len(dex_spot_results),
            cex_historical_passed=cex_historical_passed,
            cex_historical_total=len(cex_historical_results),
            dex_historical_passed=dex_historical_passed,
            dex_historical_total=len(dex_historical_results),
            rsi_passed=rsi_passed,
            rsi_total=len(rsi_results),
        )

    def _generate_header(
        self,
        config: QAConfig,
        timestamp: datetime,
        duration_seconds: float,
        summary: ReportSummary,
    ) -> str:
        """Generate the report header section.

        Args:
            config: QA configuration
            timestamp: Report timestamp
            duration_seconds: Test duration
            summary: Summary statistics

        Returns:
            Markdown header string
        """
        status = "PASSED" if summary.overall_passed else "FAILED"
        status_emoji = "+" if summary.overall_passed else "-"

        return f"""# Data QA Report

**Generated:** {timestamp.strftime("%Y-%m-%d %H:%M:%S")}
**Chain:** {config.chain}
**Duration:** {duration_seconds:.2f} seconds
**Overall Status:** [{status_emoji}] {status}

---"""

    def _generate_executive_summary(self, summary: ReportSummary) -> str:
        """Generate the executive summary section.

        Args:
            summary: Summary statistics

        Returns:
            Markdown executive summary string
        """
        return f"""## Executive Summary

| Category | Passed | Total | Status |
|----------|--------|-------|--------|
| CEX Spot Prices | {summary.cex_spot_passed} | {summary.cex_spot_total} | {self._status_indicator(summary.cex_spot_passed, summary.cex_spot_total)} |
| DEX Spot Prices | {summary.dex_spot_passed} | {summary.dex_spot_total} | {self._status_indicator(summary.dex_spot_passed, summary.dex_spot_total)} |
| CEX Historical | {summary.cex_historical_passed} | {summary.cex_historical_total} | {self._status_indicator(summary.cex_historical_passed, summary.cex_historical_total)} |
| DEX Historical | {summary.dex_historical_passed} | {summary.dex_historical_total} | {self._status_indicator(summary.dex_historical_passed, summary.dex_historical_total)} |
| RSI Indicators | {summary.rsi_passed} | {summary.rsi_total} | {self._status_indicator(summary.rsi_passed, summary.rsi_total)} |
| **Total** | **{summary.passed_tests}** | **{summary.total_tests}** | **{summary.pass_rate:.1f}%** |"""

    def _status_indicator(self, passed: int, total: int) -> str:
        """Generate a status indicator for passed/total counts.

        Args:
            passed: Number of passed tests
            total: Total number of tests

        Returns:
            Status indicator string
        """
        if total == 0:
            return "N/A"
        if passed == total:
            return "PASS"
        return "FAIL"

    def _generate_cex_spot_section(
        self,
        results: list[CEXSpotResult],
    ) -> str:
        """Generate the CEX Spot Prices section.

        Args:
            results: CEX spot test results

        Returns:
            Markdown section string
        """
        if not results:
            return """## 1. CEX Spot Prices

No CEX spot price tests were run."""

        lines = [
            "## 1. CEX Spot Prices",
            "",
            "| Token | Price (USD) | Confidence | Fresh | Status |",
            "|-------|------------|------------|-------|--------|",
        ]

        for result in results:
            price_str = f"${result.price_usd:,.2f}" if result.price_usd else "N/A"
            confidence_str = f"{result.confidence:.2f}" if result.confidence else "N/A"
            fresh_str = "Yes" if result.is_fresh else "No"
            status_str = "PASS" if result.passed else "FAIL"

            lines.append(f"| {result.token} | {price_str} | {confidence_str} | {fresh_str} | {status_str} |")

        return "\n".join(lines)

    def _generate_dex_spot_section(
        self,
        results: list[DEXSpotResult],
    ) -> str:
        """Generate the DEX Spot Prices section.

        Args:
            results: DEX spot test results

        Returns:
            Markdown section string
        """
        if not results:
            return """## 2. DEX Spot Prices

No DEX spot price tests were run."""

        lines = [
            "## 2. DEX Spot Prices",
            "",
            "| Token | DEX | Price (WETH) | Impact (bps) | Status |",
            "|-------|-----|--------------|--------------|--------|",
        ]

        for result in results:
            dex_str = result.best_dex or "N/A"
            price_str = f"{result.price_weth:.8f}" if result.price_weth else "N/A"
            impact_str = str(result.price_impact_bps) if result.price_impact_bps is not None else "N/A"
            status_str = "PASS" if result.passed else "FAIL"

            lines.append(f"| {result.token} | {dex_str} | {price_str} | {impact_str} | {status_str} |")

        return "\n".join(lines)

    def _generate_cex_historical_section(
        self,
        results: list[CEXHistoricalResult],
    ) -> str:
        """Generate the Historical CEX Data section.

        Args:
            results: CEX historical test results

        Returns:
            Markdown section string
        """
        if not results:
            return """## 3. Historical CEX Data

No CEX historical tests were run."""

        lines = [
            "## 3. Historical CEX Data",
            "",
            "| Token | Candles | Expected | Missing | Max Gap (h) | Price Range | Status |",
            "|-------|---------|----------|---------|-------------|-------------|--------|",
        ]

        for result in results:
            if result.price_range:
                range_str = f"${result.price_range[0]:,.2f} - ${result.price_range[1]:,.2f}"
            else:
                range_str = "N/A"
            status_str = "PASS" if result.passed else "FAIL"

            lines.append(
                f"| {result.token} | {result.total_candles} | {result.expected_candles} | "
                f"{result.missing_count} | {result.max_gap_hours:.1f} | {range_str} | {status_str} |"
            )

        # Add plot references
        lines.append("")
        lines.append("### Price Charts")
        lines.append("")
        for result in results:
            plot_path = f"plots/{result.token.lower()}_price_usd.png"
            lines.append(f"![{result.token} Price]({plot_path})")

        return "\n".join(lines)

    def _generate_dex_historical_section(
        self,
        results: list[DEXHistoricalResult],
    ) -> str:
        """Generate the Historical DEX Data section.

        Args:
            results: DEX historical test results

        Returns:
            Markdown section string
        """
        if not results:
            return """## 4. Historical DEX Data

No DEX historical tests were run."""

        lines = [
            "## 4. Historical DEX Data",
            "",
            "| Token | Data Points | Status | Note |",
            "|-------|-------------|--------|------|",
        ]

        for result in results:
            status_str = "PASS" if result.passed else "FAIL"
            note_str = result.note if result.passed else (result.error or result.note)

            lines.append(f"| {result.token} | {result.total_points} | {status_str} | {note_str} |")

        # Add plot references
        lines.append("")
        lines.append("### WETH Price Charts")
        lines.append("")
        for result in results:
            if result.passed:
                plot_path = f"plots/{result.token.lower()}_price_weth.png"
                lines.append(f"![{result.token} WETH Price]({plot_path})")

        return "\n".join(lines)

    def _generate_rsi_section(
        self,
        results: list[RSIResult],
    ) -> str:
        """Generate the RSI Values section.

        Args:
            results: RSI test results

        Returns:
            Markdown section string
        """
        if not results:
            return """## 5. RSI Indicators

No RSI tests were run."""

        lines = [
            "## 5. RSI Indicators",
            "",
            "| Token | Current RSI | Signal | Min | Max | Avg | Status |",
            "|-------|-------------|--------|-----|-----|-----|--------|",
        ]

        for result in results:
            current_str = f"{result.current_rsi:.2f}" if result.current_rsi is not None else "N/A"
            min_str = f"{result.min_rsi:.2f}" if result.min_rsi is not None else "N/A"
            max_str = f"{result.max_rsi:.2f}" if result.max_rsi is not None else "N/A"
            avg_str = f"{result.avg_rsi:.2f}" if result.avg_rsi is not None else "N/A"
            status_str = "PASS" if result.passed else "FAIL"

            lines.append(
                f"| {result.token} | {current_str} | {result.signal} | "
                f"{min_str} | {max_str} | {avg_str} | {status_str} |"
            )

        # Add plot references
        lines.append("")
        lines.append("### RSI Charts")
        lines.append("")
        for result in results:
            if result.passed and result.rsi_history:
                plot_path = f"plots/{result.token.lower()}_rsi.png"
                lines.append(f"![{result.token} RSI]({plot_path})")

        return "\n".join(lines)

    def _generate_failures_section(
        self,
        cex_spot_results: list[CEXSpotResult],
        dex_spot_results: list[DEXSpotResult],
        cex_historical_results: list[CEXHistoricalResult],
        dex_historical_results: list[DEXHistoricalResult],
        rsi_results: list[RSIResult],
    ) -> str:
        """Generate the Failures section with error details.

        Args:
            cex_spot_results: CEX spot test results
            dex_spot_results: DEX spot test results
            cex_historical_results: CEX historical test results
            dex_historical_results: DEX historical test results
            rsi_results: RSI test results

        Returns:
            Markdown failures section string
        """
        failures: list[tuple[str, str, str]] = []

        # Collect failures from each category
        for cex_spot in cex_spot_results:
            if not cex_spot.passed and cex_spot.error:
                failures.append(("CEX Spot", cex_spot.token, cex_spot.error))

        for dex_spot in dex_spot_results:
            if not dex_spot.passed and dex_spot.error:
                failures.append(("DEX Spot", dex_spot.token, dex_spot.error))

        for cex_hist in cex_historical_results:
            if not cex_hist.passed and cex_hist.error:
                failures.append(("CEX Historical", cex_hist.token, cex_hist.error))

        for dex_hist in dex_historical_results:
            if not dex_hist.passed and dex_hist.error:
                failures.append(("DEX Historical", dex_hist.token, dex_hist.error))

        for rsi in rsi_results:
            if not rsi.passed and rsi.error:
                failures.append(("RSI", rsi.token, rsi.error))

        if not failures:
            return """## Failures

No failures detected. All tests passed."""

        lines = [
            "## Failures",
            "",
            "| Category | Token | Error |",
            "|----------|-------|-------|",
        ]

        for category, token, error in failures:
            # Escape pipe characters in error messages
            error_escaped = error.replace("|", "\\|")
            lines.append(f"| {category} | {token} | {error_escaped} |")

        return "\n".join(lines)


__all__ = [
    "ReportGenerator",
    "ReportSummary",
]
