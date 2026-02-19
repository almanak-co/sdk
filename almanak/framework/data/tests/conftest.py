"""Pytest configuration for Data QA Tests.

This conftest adds custom CLI options and markers for the QA test suite.

CLI Options:
    --qa-report: Generate QA report to reports/qa-data/ directory

Markers:
    generate_plots: Enable plot generation for tests (disabled by default)
"""

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add custom CLI options for QA tests.

    Args:
        parser: pytest argument parser.
    """
    parser.addoption(
        "--qa-report",
        action="store_true",
        default=False,
        help="Generate QA report to reports/qa-data/ directory",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers.

    Args:
        config: pytest configuration object.
    """
    config.addinivalue_line(
        "markers",
        "generate_plots: Enable plot generation for this test (disabled by default)",
    )
