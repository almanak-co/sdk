"""Tests that all adapter _get_token_symbol methods use skip_gateway=True.

VIB-267: Token resolver gateway timeout on cosmetic LP token symbol lookup.
All adapters must use skip_gateway=True for _get_token_symbol() to avoid
30-second gateway timeouts on addresses not in the static registry (e.g.,
LP pool contracts, gauge contracts). The symbol is only used in cosmetic
TransactionData.description fields.
"""

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken


# ── Adapter factories ──────────────────────────────────────────────────


def _make_aerodrome_adapter(mock_resolver):
    from almanak.framework.connectors.aerodrome.adapter import AerodromeAdapter

    with patch.object(AerodromeAdapter, "__init__", lambda self, *a, **kw: None):
        adapter = AerodromeAdapter.__new__(AerodromeAdapter)
        adapter.chain = "base"
        adapter._token_resolver = mock_resolver
        return adapter


def _make_uniswap_v3_adapter(mock_resolver):
    from almanak.framework.connectors.uniswap_v3.adapter import UniswapV3Adapter

    with patch.object(UniswapV3Adapter, "__init__", lambda self, *a, **kw: None):
        adapter = UniswapV3Adapter.__new__(UniswapV3Adapter)
        adapter.chain = "arbitrum"
        adapter._token_resolver = mock_resolver
        return adapter


def _make_sushiswap_v3_adapter(mock_resolver):
    from almanak.framework.connectors.sushiswap_v3.adapter import SushiSwapV3Adapter

    with patch.object(SushiSwapV3Adapter, "__init__", lambda self, *a, **kw: None):
        adapter = SushiSwapV3Adapter.__new__(SushiSwapV3Adapter)
        adapter.chain = "arbitrum"
        adapter._token_resolver = mock_resolver
        return adapter


def _make_enso_adapter(mock_resolver):
    from almanak.framework.connectors.enso.adapter import EnsoAdapter

    with patch.object(EnsoAdapter, "__init__", lambda self, *a, **kw: None):
        adapter = EnsoAdapter.__new__(EnsoAdapter)
        adapter.chain = "arbitrum"
        adapter._token_resolver = mock_resolver
        return adapter


def _make_curve_adapter(mock_resolver):
    from almanak.framework.connectors.curve.adapter import CurveAdapter

    with patch.object(CurveAdapter, "__init__", lambda self, *a, **kw: None):
        adapter = CurveAdapter.__new__(CurveAdapter)
        adapter.chain = "ethereum"
        adapter._token_resolver = mock_resolver
        return adapter


ADAPTER_FACTORIES = [
    ("aerodrome", _make_aerodrome_adapter),
    ("uniswap_v3", _make_uniswap_v3_adapter),
    ("sushiswap_v3", _make_sushiswap_v3_adapter),
    ("enso", _make_enso_adapter),
    ("curve", _make_curve_adapter),
]


@pytest.fixture
def mock_resolver():
    return MagicMock()


class TestGetTokenSymbolSkipGateway:
    """All adapters must use skip_gateway=True for cosmetic symbol lookups."""

    @pytest.mark.parametrize("name,factory", ADAPTER_FACTORIES, ids=[n for n, _ in ADAPTER_FACTORIES])
    def test_known_address_resolves(self, name, factory, mock_resolver):
        """Known token addresses resolve normally."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        adapter = factory(mock_resolver)
        result = adapter._get_token_symbol("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")

        assert result == "USDC"
        mock_resolver.resolve.assert_called_once()
        call_kwargs = mock_resolver.resolve.call_args[1]
        assert call_kwargs.get("skip_gateway") is True, f"{name} must pass skip_gateway=True"
        assert call_kwargs.get("log_errors") is False, f"{name} must pass log_errors=False"

    @pytest.mark.parametrize("name,factory", ADAPTER_FACTORIES, ids=[n for n, _ in ADAPTER_FACTORIES])
    def test_unknown_address_returns_truncated(self, name, factory, mock_resolver):
        """Unknown addresses return truncated address, never raise."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="0x0000000000000000000000000000000000000001",
            chain="arbitrum",
            reason="Not found",
        )
        adapter = factory(mock_resolver)
        result = adapter._get_token_symbol("0x0000000000000000000000000000000000000001")

        # Must not raise - returns truncated address for cosmetic use
        assert result.startswith("0x0000")
        assert "..." in result
        assert len(result) < 42  # Shorter than full address

    @pytest.mark.parametrize("name,factory", ADAPTER_FACTORIES, ids=[n for n, _ in ADAPTER_FACTORIES])
    def test_non_address_passthrough(self, name, factory, mock_resolver):
        """Non-address strings (symbols) pass through unchanged."""
        adapter = factory(mock_resolver)
        result = adapter._get_token_symbol("USDC")

        assert result == "USDC"
        mock_resolver.resolve.assert_not_called()
