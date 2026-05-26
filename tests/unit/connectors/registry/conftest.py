"""Shared fixtures for the ConnectorRegistry unit tests.

The registry is a module-level singleton, so each test must run against a
clean slate. The autouse fixture below clears the registry around every
test, including any state populated by previous tests or by an earlier
``_import_all_connectors`` call from a sibling test file.
"""

from __future__ import annotations

import pytest

from almanak.connectors._strategy_base.registry import ConnectorRegistry


@pytest.fixture(autouse=True)
def _isolate_registry() -> None:
    """Wipe the registry before and after each test."""
    ConnectorRegistry._clear()
    yield
    ConnectorRegistry._clear()
