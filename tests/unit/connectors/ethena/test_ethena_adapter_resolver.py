"""Tests for EthenaAdapter TokenResolver integration.

These tests verify that the EthenaAdapter correctly initializes with
the TokenResolver. Ethena uses hardcoded 18 decimals for both tokens
(USDe, sUSDe) so there are no _get_decimals() or _resolve_token()
methods that need TokenResolver integration. The resolver is available
for future use.
"""

from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.ethena.adapter import (
    EthenaAdapter,
    EthenaConfig,
)


TEST_WALLET = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def config():
    """Create an EthenaConfig for testing."""
    return EthenaConfig(
        chain="ethereum",
        wallet_address=TEST_WALLET,
    )


@pytest.fixture
def mock_resolver():
    """Create a mock TokenResolver."""
    return MagicMock()


class TestEthenaAdapterResolverInit:
    """Test EthenaAdapter initializes with TokenResolver."""

    def test_custom_resolver_injected(self, config, mock_resolver):
        """Test custom resolver is used when provided."""
        adapter = EthenaAdapter(config, token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    def test_default_resolver_initialized(self, config):
        """Test default resolver is initialized when not provided."""
        adapter = EthenaAdapter(config)
        assert adapter._token_resolver is not None

    def test_resolver_init_failure_graceful(self, config):
        """Test adapter works even if resolver init is set to None."""
        adapter = EthenaAdapter(config, token_resolver=MagicMock())
        adapter._token_resolver = None
        # Adapter should still be functional - all operations use hardcoded addresses
        assert adapter.chain == "ethereum"
        assert adapter.usde_address is not None
        assert adapter.susde_address is not None
