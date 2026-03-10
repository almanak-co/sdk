"""Tests for MetaMorpho Vault Adapter - Token Resolver Integration.

Tests that the adapter correctly uses TokenResolver for token resolution and
decimals, following the same pattern as morpho_blue tests.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.morpho_vault.adapter import (
    MetaMorphoAdapter,
    MetaMorphoConfig,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError

VAULT_ADDR = "0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB"
WALLET_ADDR = "0x1234567890123456789012345678901234567890"
ASSET_ADDR = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"


# =============================================================================
# Helpers
# =============================================================================


def _make_adapter(resolver=None):
    config = MetaMorphoConfig(chain="ethereum", wallet_address=WALLET_ADDR)
    gw = MagicMock()
    return MetaMorphoAdapter(config, gateway_client=gw, token_resolver=resolver)


def _make_resolver(address=ASSET_ADDR, decimals=6, symbol="USDC"):
    resolver = MagicMock()
    resolved = MagicMock()
    resolved.address = address
    resolved.decimals = decimals
    resolved.symbol = symbol
    resolver.resolve.return_value = resolved
    return resolver


# =============================================================================
# Token Resolution
# =============================================================================


class TestTokenResolution:
    def test_resolve_symbol(self):
        resolver = _make_resolver()
        adapter = _make_adapter(resolver)
        address = adapter._resolve_token("USDC")
        assert address == ASSET_ADDR
        resolver.resolve.assert_called_once_with("USDC", "ethereum")

    def test_resolve_address_passthrough(self):
        adapter = _make_adapter()
        address = adapter._resolve_token(ASSET_ADDR)
        assert address == ASSET_ADDR

    def test_resolve_failure_raises(self):
        resolver = MagicMock()
        resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN",
            chain="ethereum",
            reason="Not found",
            suggestions=[],
        )
        adapter = _make_adapter(resolver)
        with pytest.raises(TokenResolutionError, match="MetaMorphoAdapter"):
            adapter._resolve_token("UNKNOWN")


class TestDecimalsResolution:
    def test_get_decimals_symbol(self):
        resolver = _make_resolver(decimals=6)
        adapter = _make_adapter(resolver)
        decimals = adapter._get_decimals("USDC")
        assert decimals == 6

    def test_get_decimals_address(self):
        resolver = _make_resolver(decimals=18)
        adapter = _make_adapter(resolver)
        decimals = adapter._get_decimals_for_address(ASSET_ADDR)
        assert decimals == 18

    def test_get_decimals_failure_raises(self):
        resolver = MagicMock()
        resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN",
            chain="ethereum",
            reason="Not found",
            suggestions=[],
        )
        adapter = _make_adapter(resolver)
        with pytest.raises(TokenResolutionError, match="Cannot determine decimals"):
            adapter._get_decimals("UNKNOWN")


class TestDefaultResolver:
    @patch("almanak.framework.data.tokens.resolver.get_token_resolver")
    def test_uses_singleton_when_no_resolver_provided(self, mock_get_resolver):
        mock_resolver = MagicMock()
        mock_get_resolver.return_value = mock_resolver

        config = MetaMorphoConfig(chain="ethereum", wallet_address=WALLET_ADDR)
        adapter = MetaMorphoAdapter(config, gateway_client=MagicMock())

        assert adapter._token_resolver is mock_resolver
        mock_get_resolver.assert_called_once()

    def test_uses_provided_resolver(self):
        resolver = _make_resolver()
        adapter = _make_adapter(resolver)
        assert adapter._token_resolver is resolver


class TestDepositTokenResolution:
    """Verify that deposit() correctly uses token resolver for decimals."""

    def test_deposit_resolves_asset_decimals(self):
        resolver = _make_resolver(decimals=6)
        adapter = _make_adapter(resolver)

        mock_sdk = MagicMock()
        mock_sdk.get_vault_asset.return_value = ASSET_ADDR
        mock_sdk.get_max_deposit.return_value = 10**30
        mock_sdk.build_approve_tx.return_value = {
            "to": ASSET_ADDR, "data": "0x", "value": "0", "gas_estimate": 60000,
        }
        mock_sdk.build_deposit_tx.return_value = {
            "to": VAULT_ADDR, "data": "0x", "value": "0", "gas_estimate": 450000,
        }
        adapter._sdk = mock_sdk

        result = adapter.deposit(VAULT_ADDR, Decimal("1000"))
        assert result.success is True

        # Verify the deposit amount was calculated with 6 decimals
        call_args = mock_sdk.build_deposit_tx.call_args
        assert call_args.kwargs["assets"] == 1000 * 10**6

    def test_deposit_fails_on_unresolvable_asset(self):
        resolver = MagicMock()
        resolver.resolve.side_effect = TokenResolutionError(
            token=ASSET_ADDR,
            chain="ethereum",
            reason="Unknown token",
            suggestions=[],
        )
        adapter = _make_adapter(resolver)

        mock_sdk = MagicMock()
        mock_sdk.get_vault_asset.return_value = ASSET_ADDR
        adapter._sdk = mock_sdk

        result = adapter.deposit(VAULT_ADDR, Decimal("1000"))
        assert result.success is False
        assert "Cannot determine decimals" in result.error or "Unknown token" in result.error
