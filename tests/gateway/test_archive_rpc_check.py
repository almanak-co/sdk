"""Tests for ManagedGateway._check_archive_rpc_availability pre-flight check."""

import logging
import os
from unittest.mock import patch

import pytest

from almanak.gateway.managed import ManagedGateway


@pytest.fixture
def gateway():
    """Create a ManagedGateway configured for polygon (archive-required chain)."""
    from almanak.gateway.core.settings import GatewaySettings

    settings = GatewaySettings(grpc_host="127.0.0.1", grpc_port=50099)
    return ManagedGateway(settings, anvil_chains=["polygon"])


class TestArchiveRpcCheck:
    """Tests for the archive RPC pre-flight warning."""

    def test_warns_when_no_rpc_configured(self, gateway, caplog):
        """Should warn when polygon has no archive-capable RPC."""
        with patch.dict(os.environ, {}, clear=True):
            with caplog.at_level(logging.WARNING):
                gateway._check_archive_rpc_availability()
            assert "archive-capable RPC" in caplog.text
            assert "polygon" in caplog.text

    def test_no_warning_with_alchemy_key(self, gateway, caplog):
        """No warning when ALCHEMY_API_KEY is set."""
        with patch.dict(os.environ, {"ALCHEMY_API_KEY": "test-key"}, clear=True):
            with caplog.at_level(logging.WARNING):
                gateway._check_archive_rpc_availability()
            assert "archive-capable RPC" not in caplog.text

    def test_no_warning_with_chain_specific_rpc(self, gateway, caplog):
        """No warning when chain-specific RPC URL is set."""
        with patch.dict(os.environ, {"POLYGON_RPC_URL": "https://rpc.example.com"}, clear=True):
            with caplog.at_level(logging.WARNING):
                gateway._check_archive_rpc_availability()
            assert "archive-capable RPC" not in caplog.text

    def test_no_warning_with_almanak_chain_rpc(self, gateway, caplog):
        """No warning when ALMANAK_POLYGON_RPC_URL is set."""
        with patch.dict(os.environ, {"ALMANAK_POLYGON_RPC_URL": "https://rpc.example.com"}, clear=True):
            with caplog.at_level(logging.WARNING):
                gateway._check_archive_rpc_availability()
            assert "archive-capable RPC" not in caplog.text

    def test_no_warning_with_generic_rpc_url(self, gateway, caplog):
        """No warning when generic RPC_URL is set."""
        with patch.dict(os.environ, {"RPC_URL": "https://rpc.example.com"}, clear=True):
            with caplog.at_level(logging.WARNING):
                gateway._check_archive_rpc_availability()
            assert "archive-capable RPC" not in caplog.text

    def test_no_warning_with_almanak_rpc_url(self, gateway, caplog):
        """No warning when generic ALMANAK_RPC_URL is set."""
        with patch.dict(os.environ, {"ALMANAK_RPC_URL": "https://rpc.example.com"}, clear=True):
            with caplog.at_level(logging.WARNING):
                gateway._check_archive_rpc_availability()
            assert "archive-capable RPC" not in caplog.text

    def test_no_warning_for_non_archive_chain(self, caplog):
        """No warning for chains that work with public RPCs (e.g., arbitrum)."""
        from almanak.gateway.core.settings import GatewaySettings

        settings = GatewaySettings(grpc_host="127.0.0.1", grpc_port=50099)
        gw = ManagedGateway(settings, anvil_chains=["arbitrum"])
        with patch.dict(os.environ, {}, clear=True):
            with caplog.at_level(logging.WARNING):
                gw._check_archive_rpc_availability()
            assert "archive-capable RPC" not in caplog.text

    def test_no_warning_for_external_anvil(self, caplog):
        """No warning when external Anvil is provided (user manages RPC)."""
        from almanak.gateway.core.settings import GatewaySettings

        settings = GatewaySettings(grpc_host="127.0.0.1", grpc_port=50099)
        gw = ManagedGateway(settings, anvil_chains=["polygon"], external_anvil_ports={"polygon": 8545})
        with patch.dict(os.environ, {}, clear=True):
            with caplog.at_level(logging.WARNING):
                gw._check_archive_rpc_availability()
            assert "archive-capable RPC" not in caplog.text

    def test_warns_for_multiple_archive_chains(self, caplog):
        """Should warn for each archive-required chain without RPC."""
        from almanak.gateway.core.settings import GatewaySettings

        settings = GatewaySettings(grpc_host="127.0.0.1", grpc_port=50099)
        gw = ManagedGateway(settings, anvil_chains=["polygon", "ethereum", "avalanche", "arbitrum"])
        with patch.dict(os.environ, {}, clear=True):
            with caplog.at_level(logging.WARNING):
                gw._check_archive_rpc_availability()
            assert "polygon" in caplog.text
            assert "ethereum" in caplog.text
            assert "avalanche" in caplog.text
            # arbitrum should NOT appear in warnings
            assert "arbitrum" not in caplog.text
            assert caplog.text.count("archive-capable RPC") == 3

    def test_archive_chains_constant(self):
        """Verify the ARCHIVE_RPC_REQUIRED_CHAINS set contains expected chains."""
        assert ManagedGateway.ARCHIVE_RPC_REQUIRED_CHAINS == {"polygon", "ethereum", "avalanche"}
