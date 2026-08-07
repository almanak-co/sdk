"""Shared fixtures for paper-engine tests."""

from __future__ import annotations

import pytest

from almanak.connectors.gmx_v2 import market_catalog


@pytest.fixture(autouse=True)
def _clean_market_catalog():
    """Isolate the process-wide verified-market catalog between tests.

    The catalog is deliberately module-global in production (one process, one
    verification history); under pytest that global would leak verified rows
    across tests. The GMX paper position queries read index-token decimals
    catalog-first (``_gmx_v2_index_token_decimals``), and the tests here prime
    the catalog to pin that path, so they need the same isolation the connector
    tests get from ``tests/unit/connectors/gmx_v2/conftest.py``.
    """
    market_catalog.clear()
    yield
    market_catalog.clear()
