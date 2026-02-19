"""Golden test fixtures for backtesting accuracy validation.

This package contains known-good test fixtures for regression testing of
the backtesting system. Each fixture includes:
- Input parameters (position configuration, market conditions)
- Expected outputs (IL, fees, funding, interest, PnL)
- Ground truth source documentation
- Tolerance thresholds for validation

Fixture Categories:
    - LP Positions (3 fixtures): IL and fee calculations
    - Perp Trades (3 fixtures): Funding and PnL calculations
    - Lending Positions (2 fixtures): Interest accrual calculations

Usage:
    from tests.golden_tests import load_lp_fixtures, load_perp_fixtures, load_lending_fixtures

See README.md for detailed documentation on fixture sources and validation methodology.
"""

import json
from pathlib import Path
from typing import Any

GOLDEN_TESTS_DIR = Path(__file__).parent


def load_fixtures(fixture_file: str) -> dict[str, Any]:
    """Load fixtures from a JSON file.

    Args:
        fixture_file: Name of the fixture file (e.g., "lp_fixtures.json")

    Returns:
        Dictionary containing fixture data
    """
    fixture_path = GOLDEN_TESTS_DIR / fixture_file
    with open(fixture_path) as f:
        return json.load(f)


def load_lp_fixtures() -> dict[str, Any]:
    """Load LP position fixtures."""
    return load_fixtures("lp_fixtures.json")


def load_perp_fixtures() -> dict[str, Any]:
    """Load perp trade fixtures."""
    return load_fixtures("perp_fixtures.json")


def load_lending_fixtures() -> dict[str, Any]:
    """Load lending position fixtures."""
    return load_fixtures("lending_fixtures.json")


__all__ = [
    "GOLDEN_TESTS_DIR",
    "load_fixtures",
    "load_lp_fixtures",
    "load_perp_fixtures",
    "load_lending_fixtures",
]
