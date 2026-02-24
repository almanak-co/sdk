"""Tests for VIB-102: Eliminate silent $1 price fallback across all adapters.

Verifies that adapters raise or return errors when a token price is missing
from the price oracle during amount_usd conversion, instead of silently
falling back to $1 (which would cause catastrophic mis-sizing of trades).
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.aave_v3.adapter import AaveV3Adapter, AaveV3Config
from almanak.framework.connectors.aerodrome.adapter import AerodromeAdapter, AerodromeConfig
from almanak.framework.connectors.enso.adapter import EnsoAdapter
from almanak.framework.connectors.enso.client import EnsoConfig
from almanak.framework.connectors.lifi.adapter import LiFiAdapter
from almanak.framework.connectors.lifi.client import LiFiConfig
from almanak.framework.connectors.uniswap_v3.adapter import UniswapV3Adapter, UniswapV3Config
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.intents.vocabulary import SwapIntent


# =============================================================================
# Shared Constants
# =============================================================================

WALLET = "0x1234567890abcdef1234567890abcdef12345678"
USDC_ARB = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WETH_ARB = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"


# =============================================================================
# Helpers
# =============================================================================


def _mock_token_resolver():
    """Token resolver that knows USDC (6 decimals) and WETH (18 decimals)."""
    resolver = MagicMock()

    def resolve(token, chain):
        tokens = {
            ("USDC", "arbitrum"): MagicMock(address=USDC_ARB, decimals=6, symbol="USDC"),
            ("WETH", "arbitrum"): MagicMock(address=WETH_ARB, decimals=18, symbol="WETH"),
            ("ETH", "arbitrum"): MagicMock(address=WETH_ARB, decimals=18, symbol="ETH"),
            ("USDC", "base"): MagicMock(address=USDC_BASE, decimals=6, symbol="USDC"),
            ("WETH", "base"): MagicMock(address=WETH_ARB, decimals=18, symbol="WETH"),
            (USDC_ARB, "arbitrum"): MagicMock(address=USDC_ARB, decimals=6, symbol="USDC"),
            (WETH_ARB, "arbitrum"): MagicMock(address=WETH_ARB, decimals=18, symbol="WETH"),
        }
        key = (token, chain)
        if key in tokens:
            return tokens[key]
        raise TokenResolutionError(token=token, chain=chain, reason=f"Unknown: {token}")

    resolver.resolve.side_effect = resolve
    resolver.resolve_for_swap = resolver.resolve
    return resolver


def _make_swap_intent_usd(from_token="ETH", to_token="USDC", amount_usd=1000):
    """Create a SwapIntent with amount_usd (the dangerous path)."""
    return SwapIntent(
        from_token=from_token,
        to_token=to_token,
        amount_usd=Decimal(str(amount_usd)),
        max_slippage=Decimal("0.005"),
    )


def _make_swap_intent_amount(from_token="USDC", to_token="WETH", amount=1000):
    """Create a SwapIntent with direct token amount (the safe path)."""
    return SwapIntent(
        from_token=from_token,
        to_token=to_token,
        amount=Decimal(str(amount)),
        max_slippage=Decimal("0.005"),
    )


# =============================================================================
# LiFi Adapter Tests
# =============================================================================


class TestLiFiPriceFallback:
    """LiFi adapter: _resolve_amount must return None when price is missing."""

    @pytest.fixture
    def adapter(self):
        config = LiFiConfig(chain_id=42161, wallet_address=WALLET, api_key="test")
        return LiFiAdapter(
            config=config,
            price_provider={"USDC": Decimal("1")},  # Only USDC, no ETH
            token_resolver=_mock_token_resolver(),
        )

    @pytest.fixture
    def adapter_with_prices(self):
        config = LiFiConfig(chain_id=42161, wallet_address=WALLET, api_key="test")
        return LiFiAdapter(
            config=config,
            price_provider={"USDC": Decimal("1"), "ETH": Decimal("3400"), "WETH": Decimal("3400")},
            token_resolver=_mock_token_resolver(),
        )

    def test_missing_price_returns_none(self, adapter):
        """amount_usd with missing ETH price must return None, not use $1."""
        intent = _make_swap_intent_usd(from_token="ETH", amount_usd=1000)
        price_oracle = {"USDC": Decimal("1")}  # No ETH price
        result = adapter._resolve_amount(intent, price_oracle)
        assert result is None

    def test_zero_price_returns_none(self, adapter):
        """amount_usd with zero price must return None."""
        intent = _make_swap_intent_usd(from_token="ETH", amount_usd=1000)
        price_oracle = {"ETH": Decimal("0"), "USDC": Decimal("1")}
        result = adapter._resolve_amount(intent, price_oracle)
        assert result is None

    def test_valid_price_works(self, adapter_with_prices):
        """amount_usd with valid price returns correct amount."""
        intent = _make_swap_intent_usd(from_token="USDC", amount_usd=1000)
        price_oracle = {"USDC": Decimal("1"), "ETH": Decimal("3400")}
        result = adapter_with_prices._resolve_amount(intent, price_oracle)
        # $1000 / $1 per USDC = 1000 USDC = 1000 * 10^6 = 1_000_000_000
        assert result == 1_000_000_000

    def test_direct_amount_still_works(self, adapter_with_prices):
        """Direct token amount (not USD) still works without price oracle."""
        intent = _make_swap_intent_amount(from_token="USDC", amount=1000)
        result = adapter_with_prices._resolve_amount(intent, {})
        assert result == 1_000_000_000


# =============================================================================
# Enso Adapter Tests
# =============================================================================


class TestEnsoPriceFallback:
    """Enso adapter: compile_swap_intent must return error bundle when price missing."""

    @pytest.fixture
    def adapter(self):
        config = EnsoConfig(
            chain="arbitrum",
            wallet_address=WALLET,
            api_key="test-api-key",
        )
        with patch.object(EnsoAdapter, "__init__", lambda self, *a, **kw: None):
            adapter = EnsoAdapter.__new__(EnsoAdapter)
            adapter.config = config
            adapter.chain = config.chain
            adapter.wallet_address = config.wallet_address
            adapter.tokens = {}
            adapter.use_safe_route_single = False
            adapter._token_resolver = _mock_token_resolver()
            adapter._using_placeholders = False
            adapter._price_provider = {"USDC": Decimal("1")}  # No ETH
            return adapter

    def test_missing_price_returns_error_bundle(self, adapter):
        """amount_usd with missing ETH price must return error bundle."""
        intent = _make_swap_intent_usd(from_token="ETH", amount_usd=1000)
        price_oracle = {"USDC": Decimal("1")}  # No ETH
        bundle = adapter.compile_swap_intent(intent, price_oracle=price_oracle)
        assert len(bundle.transactions) == 0
        assert "error" in bundle.metadata
        assert "Price unavailable" in bundle.metadata["error"]
        assert "ETH" in bundle.metadata["error"]

    def test_zero_price_returns_error_bundle(self, adapter):
        """amount_usd with zero price must return error bundle."""
        intent = _make_swap_intent_usd(from_token="ETH", amount_usd=1000)
        price_oracle = {"ETH": Decimal("0"), "USDC": Decimal("1")}
        bundle = adapter.compile_swap_intent(intent, price_oracle=price_oracle)
        assert len(bundle.transactions) == 0
        assert "error" in bundle.metadata
        assert "Price unavailable" in bundle.metadata["error"]


# =============================================================================
# Uniswap V3 Adapter Tests
# =============================================================================


class TestUniswapV3PriceFallback:
    """Uniswap V3 adapter: compile_swap_intent must raise ValueError when price missing."""

    @pytest.fixture
    def adapter(self):
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address=WALLET,
            price_provider={"USDC": Decimal("1")},  # No ETH
        )
        return UniswapV3Adapter(config, token_resolver=_mock_token_resolver())

    def test_missing_price_raises_valueerror(self, adapter):
        """amount_usd with missing ETH price must raise ValueError."""
        intent = _make_swap_intent_usd(from_token="ETH", amount_usd=1000)
        price_oracle = {"USDC": Decimal("1")}
        with pytest.raises(ValueError, match="Price unavailable.*ETH"):
            adapter.compile_swap_intent(intent, price_oracle=price_oracle)

    def test_zero_price_raises_valueerror(self, adapter):
        """amount_usd with zero price must raise ValueError."""
        intent = _make_swap_intent_usd(from_token="ETH", amount_usd=1000)
        price_oracle = {"ETH": Decimal("0"), "USDC": Decimal("1")}
        with pytest.raises(ValueError, match="Price unavailable.*ETH"):
            adapter.compile_swap_intent(intent, price_oracle=price_oracle)

    def test_valid_price_does_not_raise(self, adapter):
        """amount_usd with valid price should not raise."""
        intent = _make_swap_intent_usd(from_token="USDC", to_token="WETH", amount_usd=1000)
        price_oracle = {"USDC": Decimal("1"), "WETH": Decimal("3400")}
        # Should not raise -- may fail downstream (swap_exact_input mocked) but
        # the price resolution path should succeed
        try:
            adapter.compile_swap_intent(intent, price_oracle=price_oracle)
        except ValueError as e:
            # Only fail if the error is about price -- other ValueError is fine
            assert "Price unavailable" not in str(e)


# =============================================================================
# Aerodrome Adapter Tests
# =============================================================================


class TestAerodromePriceFallback:
    """Aerodrome adapter: compile_swap_intent must raise ValueError when price missing."""

    @pytest.fixture
    def adapter(self):
        config = AerodromeConfig(
            chain="base",
            wallet_address=WALLET,
            price_provider={"USDC": Decimal("1")},  # No ETH
        )
        return AerodromeAdapter(config, token_resolver=_mock_token_resolver())

    def test_missing_price_raises_valueerror(self, adapter):
        """amount_usd with missing ETH price must raise ValueError."""
        intent = _make_swap_intent_usd(from_token="ETH", amount_usd=1000)
        price_oracle = {"USDC": Decimal("1")}
        with pytest.raises(ValueError, match="Price unavailable.*ETH"):
            adapter.compile_swap_intent(intent, price_oracle=price_oracle)

    def test_zero_price_raises_valueerror(self, adapter):
        """amount_usd with zero price must raise ValueError."""
        intent = _make_swap_intent_usd(from_token="ETH", amount_usd=1000)
        price_oracle = {"ETH": Decimal("0"), "USDC": Decimal("1")}
        with pytest.raises(ValueError, match="Price unavailable.*ETH"):
            adapter.compile_swap_intent(intent, price_oracle=price_oracle)


# =============================================================================
# Aave V3 Adapter Tests
# =============================================================================


class TestAaveV3PriceFallback:
    """Aave V3 adapter: _default_price_oracle must raise ValueError for unknown assets."""

    @pytest.fixture
    def adapter(self):
        config = AaveV3Config(
            chain="arbitrum",
            wallet_address=WALLET,
            allow_placeholder_prices=True,
        )
        return AaveV3Adapter(config, token_resolver=_mock_token_resolver())

    def test_unknown_asset_raises_valueerror(self, adapter):
        """Unknown asset not in placeholder dict must raise ValueError."""
        with pytest.raises(ValueError, match="No placeholder price available.*UNKNOWN_TOKEN"):
            adapter._default_price_oracle("UNKNOWN_TOKEN")

    def test_known_asset_returns_price(self, adapter):
        """Known asset returns placeholder price."""
        price = adapter._default_price_oracle("USDC")
        assert price == Decimal("1")

    def test_known_asset_case_insensitive(self, adapter):
        """Asset lookup should work with case variation."""
        price = adapter._default_price_oracle("weth")
        # Should find WETH via upper-case fallback
        assert price == Decimal("2000")
