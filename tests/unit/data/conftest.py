"""Pytest configuration for Data QA Tests.

This conftest adds custom CLI options and markers for the QA test suite.

CLI Options:
    --qa-report: Generate QA report to reports/qa-data/ directory

Markers:
    generate_plots: Enable plot generation for tests (disabled by default)
"""

import pytest

from almanak.framework.data.ratelimit import reset_buckets


@pytest.fixture(autouse=True)
def _reset_shared_rate_limit_buckets():
    """Isolate the process-wide shared rate-limit registry between tests.

    Providers share named buckets process-wide (almanak.framework.data.ratelimit),
    so a test that drains the "defillama" bucket would otherwise rate-limit
    unrelated tests in the same pytest process.
    """
    reset_buckets()
    yield
    reset_buckets()


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
