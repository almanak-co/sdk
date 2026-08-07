"""Shared fixtures for strategy tests."""

from __future__ import annotations

import pytest

from almanak.connectors.gmx_v2 import market_catalog


@pytest.fixture(autouse=True)
def _clean_market_catalog():
    """Isolate the process-wide verified-market catalog between tests.

    Mirrors tests/unit/connectors/gmx_v2/conftest.py — strategy/probe tests
    prime the catalog with fixture records and must not leak them.
    """
    market_catalog.clear()
    yield
    market_catalog.clear()
