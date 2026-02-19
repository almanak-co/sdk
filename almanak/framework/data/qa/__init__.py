"""Data QA Framework.

Production-ready validation suite for auditing CEX prices, DEX prices,
historical OHLCV data, and RSI indicators. Enables manual verification
of the Data Module before deploying strategies with real capital.

Key Components:
    - QAConfig: Configuration for QA tests (tokens, thresholds, timeframes)
    - load_config: Load configuration from YAML file
    - QARunner: Main test orchestrator for running all QA tests
    - QAReport: Complete QA report with all test results

Example:
    from almanak.framework.data.qa import load_config, QARunner
    from pathlib import Path

    # Load configuration and run tests
    config = load_config()
    runner = QARunner(config, output_dir=Path("reports/qa-data"))
    report = await runner.run_all()

    print(f"Overall: {'PASSED' if report.passed else 'FAILED'}")
    print(f"Report: {report.report_path}")
"""

from .config import QAConfig, QAThresholds, load_config
from .runner import QAReport, QARunner, TestDuration

__all__ = [
    "QAConfig",
    "QAReport",
    "QARunner",
    "QAThresholds",
    "TestDuration",
    "load_config",
]
