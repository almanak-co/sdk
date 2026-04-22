"""CLI command for running Data QA tests.

Usage:
    python -m src.data.qa.cli --chain arbitrum --days 30
    python -m src.data.qa.cli --test cex_spot --skip-plots
    python -m src.data.qa.cli --config custom_config.yaml --output reports/qa

Example:
    # Run full QA suite with defaults
    python -m src.data.qa.cli

    # Run specific test only
    python -m src.data.qa.cli --test rsi

    # Quick run without plots
    python -m src.data.qa.cli --skip-plots

    # Custom output directory
    python -m src.data.qa.cli --output reports/qa-data
"""

import logging
import sys
from pathlib import Path
from typing import Any

import click
from dotenv import load_dotenv

from .cli_helpers import (
    apply_cli_overrides,
    configure_logging,
    dispatch_test_run,
    echo_category_failures,
    load_qa_config_or_exit,
    print_startup_banner,
    summarize_category,
)
from .config import QAConfig
from .runner import QAReport, QARunner

# Anchor the exception-path logger name on this module so operator log
# filters that match ``almanak.framework.data.qa.cli`` keep working after
# the Phase 6.4 dispatch extraction. Passed explicitly to
# ``dispatch_test_run`` below.
logger = logging.getLogger(__name__)

# Valid test names for --test option
VALID_TESTS = ["cex_spot", "dex_spot", "cex_history", "dex_history", "rsi"]


def _create_test_only_runner(
    config: QAConfig,
    output_dir: Path,
    test: str,
    skip_plots: bool,
) -> tuple[QARunner, str]:
    """Create a runner configured for a single test.

    Args:
        config: QA configuration
        output_dir: Report output directory
        test: Test name to run
        skip_plots: Whether to skip plot generation

    Returns:
        Tuple of (configured runner, display name for the test)
    """
    runner = QARunner(config=config, output_dir=output_dir, skip_plots=skip_plots)

    # Map test names to display names
    test_names = {
        "cex_spot": "CEX Spot Prices",
        "dex_spot": "DEX Spot Prices",
        "cex_history": "CEX Historical",
        "dex_history": "DEX Historical",
        "rsi": "RSI Indicators",
    }

    return runner, test_names.get(test, test)


async def _run_single_test(
    runner: QARunner,
    test: str,
    config: QAConfig,
) -> QAReport:
    """Run a single test category.

    Args:
        runner: QA runner instance
        test: Test name to run
        config: QA configuration

    Returns:
        QAReport with results from the single test
    """
    import time

    report = QAReport(config=config)
    suite_start = time.time()

    if test == "cex_spot":
        cex_spot_results, cex_spot_duration = await runner._run_cex_spot()
        report.cex_spot_results = cex_spot_results
        report.durations["cex_spot"] = cex_spot_duration
    elif test == "dex_spot":
        dex_spot_results, dex_spot_duration = await runner._run_dex_spot()
        report.dex_spot_results = dex_spot_results
        report.durations["dex_spot"] = dex_spot_duration
    elif test == "cex_history":
        cex_hist_results, cex_hist_duration = await runner._run_cex_historical()
        report.cex_historical_results = cex_hist_results
        report.durations["cex_historical"] = cex_hist_duration
    elif test == "dex_history":
        dex_hist_results, dex_hist_duration = await runner._run_dex_historical()
        report.dex_historical_results = dex_hist_results
        report.durations["dex_historical"] = dex_hist_duration
    elif test == "rsi":
        rsi_results, rsi_duration = await runner._run_rsi()
        report.rsi_results = rsi_results
        report.durations["rsi"] = rsi_duration

    report.total_duration_seconds = time.time() - suite_start

    # Generate plots if not skipped
    if not runner.skip_plots:
        await runner._generate_plots(report)

    # Generate report
    report.report_path = runner._generate_report(report)

    # Determine pass/fail
    report.passed = report.failed_tests == 0

    return report


@click.command("qa-data")
@click.option(
    "--chain",
    type=click.Choice(["arbitrum", "base", "ethereum"]),
    default=None,
    help="Select chain (arbitrum, base, ethereum). Default: from config (arbitrum)",
)
@click.option(
    "--days",
    type=int,
    default=None,
    help="Historical data range in days. Default: from config (30)",
)
@click.option(
    "--config",
    "config_file",
    type=click.Path(exists=True),
    default=None,
    help="Path to custom config YAML file",
)
@click.option(
    "--output",
    "output_dir",
    type=click.Path(),
    default="reports/qa-data",
    help="Report output directory. Default: reports/qa-data",
)
@click.option(
    "--test",
    "test_name",
    type=click.Choice(VALID_TESTS),
    default=None,
    help="Run specific test only (cex_spot, dex_spot, cex_history, dex_history, rsi)",
)
@click.option(
    "--skip-plots",
    is_flag=True,
    default=False,
    help="Skip plot generation for faster runs",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Enable verbose output",
)
def qa_data(
    chain: str | None,
    days: int | None,
    config_file: str | None,
    output_dir: str,
    test_name: str | None,
    skip_plots: bool,
    verbose: bool,
) -> None:
    """Run Data QA tests to validate CEX/DEX prices, historical data, and RSI.

    This command runs a comprehensive test suite to validate the Data Module
    before deploying strategies with real capital. It tests CEX spot prices
    from CoinGecko, DEX spot prices from on-chain sources, historical OHLCV
    data quality, and RSI indicator calculations.

    The output includes a detailed Markdown report with tables, charts, and
    pass/fail status for each token tested.

    Examples:

        # Run full QA suite with defaults
        python -m src.data.qa.cli

        # Run only CEX spot price tests
        python -m src.data.qa.cli --test cex_spot

        # Run on Base chain with 14 days of history
        python -m src.data.qa.cli --chain base --days 14

        # Quick run without plots
        python -m src.data.qa.cli --skip-plots

        # Use custom config file
        python -m src.data.qa.cli --config my_config.yaml

        # Save to custom output directory
        python -m src.data.qa.cli --output reports/my-qa-run
    """
    # Load environment variables from .env file
    load_dotenv()

    # Configure logging
    configure_logging(verbose)

    # Load configuration (exits on FileNotFoundError / ValueError)
    config = load_qa_config_or_exit(config_file)

    # Override config with CLI options
    config = apply_cli_overrides(config, chain, days)

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Display startup information
    print_startup_banner(config, output_path, skip_plots, test_name)

    # Run tests. Factories below are only invoked on their respective
    # branch inside ``dispatch_test_run``; ``selected_test`` is the
    # type-narrowed alias the single-test closure captures.
    selected_test = test_name or ""

    def _single() -> tuple[Any, str]:
        runner, display_name = _create_test_only_runner(
            config=config,
            output_dir=output_path,
            test=selected_test,
            skip_plots=skip_plots,
        )
        return _run_single_test(runner, selected_test, config), display_name

    def _all() -> Any:
        runner = QARunner(
            config=config,
            output_dir=output_path,
            skip_plots=skip_plots,
        )
        return runner.run_all()

    report: QAReport = dispatch_test_run(
        test_name,
        run_single=_single,
        run_all=_all,
        error_logger=logger,
    )

    # Display summary
    click.echo()
    click.echo("=" * 60)
    click.echo("QA TEST SUMMARY")
    click.echo("=" * 60)

    # Show test category summaries.
    # (summary_label, results) — order is operator-facing and must be preserved.
    summary_categories: list[tuple[str, list[Any]]] = [
        ("CEX Spot Prices:    ", report.cex_spot_results),
        ("DEX Spot Prices:    ", report.dex_spot_results),
        ("CEX Historical:     ", report.cex_historical_results),
        ("DEX Historical:     ", report.dex_historical_results),
        ("RSI Indicators:     ", report.rsi_results),
    ]
    for summary_label, results in summary_categories:
        summarize_category(results, summary_label)

    click.echo("-" * 60)
    click.echo(f"Total:              {report.passed_tests}/{report.total_tests}")
    click.echo(f"Duration:           {report.total_duration_seconds:.2f}s")
    click.echo(f"Report:             {report.report_path}")
    click.echo()

    # Overall status
    if report.passed:
        click.echo("OVERALL: PASSED")
        sys.exit(0)
    else:
        click.echo("OVERALL: FAILED")
        # Show failure details.
        # (failure_label, results) — order is operator-facing and must be
        # preserved. Labels differ from the summary labels above: no column
        # padding, "Historical" instead of "Prices", "RSI" instead of
        # "RSI Indicators".
        click.echo()
        click.echo("Failed tests:")
        failure_categories: list[tuple[str, list[Any]]] = [
            ("CEX Spot", report.cex_spot_results),
            ("DEX Spot", report.dex_spot_results),
            ("CEX Historical", report.cex_historical_results),
            ("DEX Historical", report.dex_historical_results),
            ("RSI", report.rsi_results),
        ]
        for failure_label, results in failure_categories:
            echo_category_failures(results, failure_label)
        sys.exit(1)


if __name__ == "__main__":
    qa_data()
