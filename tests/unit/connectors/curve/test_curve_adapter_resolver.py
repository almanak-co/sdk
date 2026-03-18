"""Tests for CurveAdapter TokenResolver integration.

These tests verify that the CurveAdapter correctly uses the TokenResolver
for token resolution.
"""

from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.curve.adapter import (
    CurveAdapter,
    CurveConfig,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken


TEST_WALLET = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def config():
    """Create a CurveConfig for testing."""
    return CurveConfig(
        chain="ethereum",
        wallet_address=TEST_WALLET,
    )


@pytest.fixture
def arb_config():
    """Create a CurveConfig for Arbitrum testing."""
    return CurveConfig(
        chain="arbitrum",
        wallet_address=TEST_WALLET,
    )


@pytest.fixture
def mock_resolver():
    """Create a mock TokenResolver."""
    return MagicMock()


@pytest.fixture
def adapter(config, mock_resolver):
    """Create a CurveAdapter with mock resolver."""
    return CurveAdapter(config, token_resolver=mock_resolver)


class TestCurveAdapterResolverInit:
    """Test CurveAdapter initializes with TokenResolver."""

    def test_custom_resolver_injected(self, config, mock_resolver):
        """Test custom resolver is used when provided."""
        adapter = CurveAdapter(config, token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    def test_default_resolver_initialized(self, config):
        """Test default resolver is initialized when not provided."""
        adapter = CurveAdapter(config)
        assert adapter._token_resolver is not None

    def test_resolver_none_raises_error(self, config):
        """Test adapter raises error when resolver is None."""
        adapter = CurveAdapter(config, token_resolver=MagicMock())
        adapter._token_resolver = None
        with pytest.raises(AttributeError):
            adapter._resolve_token("USDC")


class TestCurveAdapterResolveToken:
    """Test _resolve_token uses TokenResolver."""

    def test_resolve_symbol_via_resolver(self, adapter, mock_resolver):
        """Test symbol resolution via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
        )
        result = adapter._resolve_token("USDC")
        assert result == "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        mock_resolver.resolve.assert_called_once_with("USDC", "ethereum")

    def test_resolve_address_passthrough(self, adapter, mock_resolver):
        """Test address passthrough (no resolver call)."""
        addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        result = adapter._resolve_token(addr)
        assert result == addr
        mock_resolver.resolve.assert_not_called()

    def test_resolve_unknown_raises_error(self, adapter, mock_resolver):
        """Test unknown token raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="NONEXISTENT_TOKEN_XYZ", chain="ethereum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._resolve_token("NONEXISTENT_TOKEN_XYZ")


class TestCurveAdapterGetDecimals:
    """Test _get_token_decimals uses TokenResolver."""

    def test_decimals_via_resolver(self, adapter, mock_resolver):
        """Test decimals resolution via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
        )
        result = adapter._get_token_decimals("USDC")
        assert result == 6

    def test_wbtc_decimals(self, adapter, mock_resolver):
        """Test WBTC has 8 decimals."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WBTC",
            address="0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
            decimals=8,
            chain="ethereum",
            chain_id=1,
        )
        result = adapter._get_token_decimals("WBTC")
        assert result == 8

    def test_steth_decimals(self, adapter, mock_resolver):
        """Test stETH has 18 decimals."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="stETH",
            address="0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
            decimals=18,
            chain="ethereum",
            chain_id=1,
        )
        result = adapter._get_token_decimals("stETH")
        assert result == 18

    def test_unknown_raises_error(self, adapter, mock_resolver):
        """Test unknown token raises TokenResolutionError instead of defaulting to 18."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN_TOKEN", chain="ethereum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._get_token_decimals("UNKNOWN_TOKEN")


class TestCurveMultiChain:
    """Test multi-chain resolution."""

    def test_arbitrum_chain(self, arb_config, mock_resolver):
        """Test Arbitrum chain resolution."""
        adapter = CurveAdapter(arb_config, token_resolver=mock_resolver)
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        result = adapter._resolve_token("USDC")
        assert result == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_resolver.resolve.assert_called_once_with("USDC", "arbitrum")


class TestApprovalLPTokenFallback:
    """Test _build_approve_tx graceful fallback for LP tokens (VIB-1501)."""

    def test_approval_lp_token_no_crash(self, adapter, mock_resolver):
        """LP token addresses not in resolver should fallback to truncated address."""
        # First call succeeds (for checking allowance is irrelevant here)
        # _build_approve_tx calls _get_token_symbol which calls resolve
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
            chain="ethereum",
            reason="Not found",
        )
        lp_address = "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"
        spender = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"
        result = adapter._build_approve_tx(lp_address, spender, 1000)
        assert result is not None
        assert "0x6c3F90f0..." in result.description

    def test_approval_known_token_uses_symbol(self, adapter, mock_resolver):
        """Known tokens should still use their symbol in the description."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
        )
        result = adapter._build_approve_tx(
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
            1000,
        )
        assert result is not None
        assert "USDC" in result.description


class TestCryptoSwapEstimation:
    """Test _estimate_swap_output for CryptoSwap pools (VIB-1417)."""

    def test_stableswap_same_decimals(self, adapter, mock_resolver):
        """StableSwap with same decimals returns 1:1."""
        from almanak.framework.connectors.curve.adapter import PoolInfo, PoolType

        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="DAI", address="0x" + "0" * 40, decimals=18, chain="ethereum", chain_id=1
        )
        pool = PoolInfo(
            address="0x" + "0" * 40,
            lp_token="0x" + "0" * 40,
            coins=["DAI", "USDC"],
            coin_addresses=["0x" + "0" * 40, "0x" + "1" * 40],
            pool_type=PoolType.STABLESWAP,
            n_coins=2,
        )
        result = adapter._estimate_swap_output(pool, 0, 1, 1000000000000000000)
        # Same decimals => same amount
        assert result == 1000000000000000000

    def test_stableswap_cross_decimals(self, adapter, mock_resolver):
        """StableSwap adjusts for decimal differences (DAI 18 -> USDC 6)."""
        from almanak.framework.connectors.curve.adapter import PoolInfo, PoolType

        # First call for coins[0] (DAI, 18 decimals)
        # Second call for coins[1] (USDC, 6 decimals)
        mock_resolver.resolve.side_effect = [
            ResolvedToken(symbol="DAI", address="0x" + "0" * 40, decimals=18, chain="ethereum", chain_id=1),
            ResolvedToken(symbol="USDC", address="0x" + "1" * 40, decimals=6, chain="ethereum", chain_id=1),
        ]
        pool = PoolInfo(
            address="0x" + "0" * 40,
            lp_token="0x" + "0" * 40,
            coins=["DAI", "USDC"],
            coin_addresses=["0x" + "0" * 40, "0x" + "1" * 40],
            pool_type=PoolType.STABLESWAP,
            n_coins=2,
        )
        # 100 DAI (18 decimals) -> USDC (6 decimals)
        result = adapter._estimate_swap_output(pool, 0, 1, 100 * 10**18)
        assert result == 100 * 10**6

    def test_cryptoswap_returns_minimal(self, adapter, mock_resolver):
        """CryptoSwap returns 1 (no meaningful estimate without on-chain oracle)."""
        from almanak.framework.connectors.curve.adapter import PoolInfo, PoolType

        mock_resolver.resolve.side_effect = [
            ResolvedToken(symbol="USDT", address="0x" + "0" * 40, decimals=6, chain="ethereum", chain_id=1),
            ResolvedToken(symbol="WETH", address="0x" + "1" * 40, decimals=18, chain="ethereum", chain_id=1),
        ]
        pool = PoolInfo(
            address="0xD51a44d3FaE010294C616388b506AcdA1bfAAE46",
            lp_token="0x" + "0" * 40,
            coins=["USDT", "WBTC", "WETH"],
            coin_addresses=["0x" + "0" * 40, "0x" + "1" * 40, "0x" + "2" * 40],
            pool_type=PoolType.TRICRYPTO,
            n_coins=3,
        )
        # 100 USDT -> WETH: should return 1 (no estimate)
        result = adapter._estimate_swap_output(pool, 0, 2, 100 * 10**6)
        assert result == 1

    def test_cryptoswap_same_decimals_returns_minimal(self, adapter, mock_resolver):
        """CryptoSwap with same decimals still returns 1 (can't assume 1:1 price)."""
        from almanak.framework.connectors.curve.adapter import PoolInfo, PoolType

        mock_resolver.resolve.side_effect = [
            ResolvedToken(symbol="WBTC", address="0x" + "0" * 40, decimals=8, chain="ethereum", chain_id=1),
            ResolvedToken(symbol="WETH", address="0x" + "1" * 40, decimals=18, chain="ethereum", chain_id=1),
        ]
        pool = PoolInfo(
            address="0xD51a44d3FaE010294C616388b506AcdA1bfAAE46",
            lp_token="0x" + "0" * 40,
            coins=["USDT", "WBTC", "WETH"],
            coin_addresses=["0x" + "0" * 40, "0x" + "1" * 40, "0x" + "2" * 40],
            pool_type=PoolType.TRICRYPTO,
            n_coins=3,
        )
        result = adapter._estimate_swap_output(pool, 1, 2, 1 * 10**8)
        assert result == 1


class TestDeprecatedDictsRemoved:
    """Verify deprecated token dicts have been removed (US-028)."""

    def test_deprecated_dicts_removed(self):
        """Verify deprecated token dicts have been removed (US-028)."""
        import almanak.framework.connectors.curve.adapter as adapter_module

        assert not hasattr(adapter_module, "TOKEN_DECIMALS")
        assert not hasattr(adapter_module, "CURVE_TOKENS")
