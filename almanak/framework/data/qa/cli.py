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

import asyncio
import logging
import sys
from pathlib import Path

import click
from dotenv import load_dotenv

from .config import QAConfig, load_config
from .runner import QAReport, QARunner

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
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Load configuration
    try:
        if config_file:
            config = load_config(config_file)
            click.echo(f"Loaded config from: {config_file}")
        else:
            config = load_config()
            click.echo("Loaded default config")
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except ValueError as e:
        click.echo(f"Invalid config: {e}", err=True)
        sys.exit(1)

    # Override config with CLI options
    if chain:
        config = QAConfig(
            chain=chain,
            historical_days=days if days else config.historical_days,
            timeframe=config.timeframe,
            rsi_period=config.rsi_period,
            thresholds=config.thresholds,
            popular_tokens=config.popular_tokens,
            additional_tokens=config.additional_tokens,
            dex_tokens=config.dex_tokens,
        )
    elif days:
        config = QAConfig(
            chain=config.chain,
            historical_days=days,
            timeframe=config.timeframe,
            rsi_period=config.rsi_period,
            thresholds=config.thresholds,
            popular_tokens=config.popular_tokens,
            additional_tokens=config.additional_tokens,
            dex_tokens=config.dex_tokens,
        )

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Display startup information
    click.echo()
    click.echo("=" * 60)
    click.echo("ALMANAK DATA QA FRAMEWORK")
    click.echo("=" * 60)
    click.echo(f"Chain: {config.chain}")
    click.echo(f"Historical days: {config.historical_days}")
    click.echo(f"Timeframe: {config.timeframe}")
    click.echo(f"RSI period: {config.rsi_period}")
    click.echo(f"Popular tokens: {', '.join(config.popular_tokens)}")
    click.echo(f"Additional tokens: {', '.join(config.additional_tokens)}")
    click.echo(f"DEX tokens: {', '.join(config.dex_tokens)}")
    click.echo(f"Output: {output_path}")
    click.echo(f"Skip plots: {skip_plots}")
    if test_name:
        click.echo(f"Running test: {test_name}")
    else:
        click.echo("Running: All tests")
    click.echo("=" * 60)
    click.echo()

    # Run tests
    try:
        if test_name:
            # Run single test
            runner, test_display_name = _create_test_only_runner(
                config=config,
                output_dir=output_path,
                test=test_name,
                skip_plots=skip_plots,
            )
            click.echo(f"Running {test_display_name} tests...")
            report = asyncio.run(_run_single_test(runner, test_name, config))
        else:
            # Run all tests
            runner = QARunner(
                config=config,
                output_dir=output_path,
                skip_plots=skip_plots,
            )
            click.echo("Running all QA tests...")
            report = asyncio.run(runner.run_all())

    except Exception as e:
        click.echo(f"Error running tests: {e}", err=True)
        logger.exception("Test execution failed")
        sys.exit(1)

    # Display summary
    click.echo()
    click.echo("=" * 60)
    click.echo("QA TEST SUMMARY")
    click.echo("=" * 60)

    # Show test category summaries
    if report.cex_spot_results:
        passed = sum(1 for r in report.cex_spot_results if r.passed)
        total = len(report.cex_spot_results)
        status = "PASS" if passed == total else "FAIL"
        click.echo(f"CEX Spot Prices:    {passed}/{total} [{status}]")

    if report.dex_spot_results:
        passed = sum(1 for r in report.dex_spot_results if r.passed)
        total = len(report.dex_spot_results)
        status = "PASS" if passed == total else "FAIL"
        click.echo(f"DEX Spot Prices:    {passed}/{total} [{status}]")

    if report.cex_historical_results:
        passed = sum(1 for r in report.cex_historical_results if r.passed)
        total = len(report.cex_historical_results)
        status = "PASS" if passed == total else "FAIL"
        click.echo(f"CEX Historical:     {passed}/{total} [{status}]")

    if report.dex_historical_results:
        passed = sum(1 for r in report.dex_historical_results if r.passed)
        total = len(report.dex_historical_results)
        status = "PASS" if passed == total else "FAIL"
        click.echo(f"DEX Historical:     {passed}/{total} [{status}]")

    if report.rsi_results:
        passed = sum(1 for r in report.rsi_results if r.passed)
        total = len(report.rsi_results)
        status = "PASS" if passed == total else "FAIL"
        click.echo(f"RSI Indicators:     {passed}/{total} [{status}]")

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
        # Show failure details
        click.echo()
        click.echo("Failed tests:")
        for cex_spot_r in report.cex_spot_results:
            if not cex_spot_r.passed:
                click.echo(f"  - CEX Spot {cex_spot_r.token}: {cex_spot_r.error or 'validation failed'}")
        for dex_spot_r in report.dex_spot_results:
            if not dex_spot_r.passed:
                click.echo(f"  - DEX Spot {dex_spot_r.token}: {dex_spot_r.error or 'validation failed'}")
        for cex_hist_r in report.cex_historical_results:
            if not cex_hist_r.passed:
                click.echo(f"  - CEX Historical {cex_hist_r.token}: {cex_hist_r.error or 'validation failed'}")
        for dex_hist_r in report.dex_historical_results:
            if not dex_hist_r.passed:
                click.echo(f"  - DEX Historical {dex_hist_r.token}: {dex_hist_r.error or 'validation failed'}")
        for rsi_r in report.rsi_results:
            if not rsi_r.passed:
                click.echo(f"  - RSI {rsi_r.token}: {rsi_r.error or 'validation failed'}")
        sys.exit(1)


if __name__ == "__main__":
    qa_data()
