"""Shared fixtures and configuration for integration tests.

This module provides:
- Pytest marker registration for chain-specific tests
- Common fixtures that can be shared across integration test modules
"""

import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers for integration tests."""
    config.addinivalue_line("markers", "ethereum: Tests that run on Ethereum mainnet fork")
    config.addinivalue_line("markers", "bsc: Tests that run on BSC mainnet fork")
    config.addinivalue_line("markers", "blast: Tests that run on Blast mainnet fork")
    config.addinivalue_line("markers", "mantle: Tests that run on Mantle mainnet fork")
    config.addinivalue_line("markers", "berachain: Tests that run on Berachain mainnet fork")
    config.addinivalue_line("markers", "lido: Tests for Lido protocol")
    config.addinivalue_line("markers", "ethena: Tests for Ethena protocol")
    config.addinivalue_line("markers", "spark: Tests for Spark protocol")
    config.addinivalue_line("markers", "pancakeswap: Tests for PancakeSwap protocol")
    config.addinivalue_line("markers", "polymarket: Tests for Polymarket protocol")
    config.addinivalue_line("markers", "lifi: Tests for LiFi protocol")
    config.addinivalue_line("markers", "polygon: Tests that run on Polygon mainnet fork")
    config.addinivalue_line("markers", "integration: Tests that require live API access")
    config.addinivalue_line("markers", "anvil: Tests that require Anvil fork")
