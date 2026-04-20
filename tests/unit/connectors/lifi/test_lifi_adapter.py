"""Unit tests for LiFi Adapter."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.lifi.adapter import (
    ERC20_APPROVE_SELECTOR,
    LIFI_GAS_ESTIMATES,
    LiFiAdapter,
    TransactionData,
)
from almanak.framework.connectors.lifi.client import LiFiClient, LiFiConfig
from almanak.framework.connectors.lifi.exceptions import LiFiRouteNotFoundError
from almanak.framework.connectors.lifi.models import (
    LiFiAction,
    LiFiEstimate,
    LiFiGasCost,
    LiFiStep,
    LiFiToken,
    LiFiTransactionRequest,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError


# ============================================================================
# Fixtures
# ============================================================================


WALLET_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WETH_ARBITRUM = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"


def _mock_token_resolver():
    """Create a mock token resolver."""
    resolver = MagicMock()

    def resolve_side_effect(token, chain):
        tokens = {
            ("USDC", "arbitrum"): MagicMock(address=USDC_ARBITRUM, decimals=6, symbol="USDC"),
            ("WETH", "arbitrum"): MagicMock(address=WETH_ARBITRUM, decimals=18, symbol="WETH"),
            ("USDC", "base"): MagicMock(address=USDC_BASE, decimals=6, symbol="USDC"),
            (USDC_ARBITRUM, "arbitrum"): MagicMock(address=USDC_ARBITRUM, decimals=6, symbol="USDC"),
            (WETH_ARBITRUM, "arbitrum"): MagicMock(address=WETH_ARBITRUM, decimals=18, symbol="WETH"),
        }
        key = (token, chain)
        if key in tokens:
            return tokens[key]
        raise TokenResolutionError(
            token=token, chain=chain, reason=f"Unknown token: {token}"
        )

    resolver.resolve.side_effect = resolve_side_effect
    return resolver


def _make_lifi_step(
    tool="across",
    step_type="cross",
    from_chain_id=42161,
    to_chain_id=8453,
    to_amount="995000000",
    to_amount_min="990000000",
    approval_address="0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE",
    gas_estimate="250000",
):
    """Create a mock LiFiStep."""
    return LiFiStep(
        id="test-step-id",
        type=step_type,
        tool=tool,
        action=LiFiAction(
            from_chain_id=from_chain_id,
            to_chain_id=to_chain_id,
            from_token=LiFiToken(address=USDC_ARBITRUM, chain_id=from_chain_id, symbol="USDC", decimals=6),
            to_token=LiFiToken(address=USDC_BASE, chain_id=to_chain_id, symbol="USDC", decimals=6),
            from_amount="1000000000",
            from_address=WALLET_ADDRESS,
        ),
        estimate=LiFiEstimate(
            from_amount="1000000000",
            to_amount=to_amount,
            to_amount_min=to_amount_min,
            approval_address=approval_address,
            execution_duration=120,
            gas_costs=[LiFiGasCost(type="SUM", estimate=gas_estimate)],
        ),
        transaction_request=LiFiTransactionRequest(
            from_address=WALLET_ADDRESS,
            to="0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE",
            chain_id=from_chain_id,
            data="0xabcdef1234567890",
            value="0",
            gas_limit="350000",
        ),
    )


@pytest.fixture
def adapter():
    """Create a LiFi adapter with mock dependencies."""
    config = LiFiConfig(
        chain_id=42161,
        wallet_address=WALLET_ADDRESS,
        api_key="test-key",
    )
    return LiFiAdapter(
        config=config,
        allow_placeholder_prices=True,
        token_resolver=_mock_token_resolver(),
    )


# ============================================================================
# Adapter Initialization Tests
# ============================================================================


class TestLiFiAdapterInit:
    """Tests for adapter initialization."""

    def test_init_with_price_provider(self):
        """Adapter initializes with explicit price provider."""
        config = LiFiConfig(chain_id=42161, wallet_address=WALLET_ADDRESS, api_key="k")
        adapter = LiFiAdapter(
            config=config,
            price_provider={"USDC": Decimal("1"), "WETH": Decimal("3400")},
            token_resolver=_mock_token_resolver(),
        )
        assert not adapter._using_placeholders

    def test_init_without_price_provider_raises(self):
        """Adapter without price provider raises ValueError."""
        config = LiFiConfig(chain_id=42161, wallet_address=WALLET_ADDRESS, api_key="k")
        with pytest.raises(ValueError, match="price_provider"):
            LiFiAdapter(config=config, token_resolver=_mock_token_resolver())

    def test_init_with_placeholder_prices(self):
        """Adapter initializes with placeholder prices for testing."""
        config = LiFiConfig(chain_id=42161, wallet_address=WALLET_ADDRESS, api_key="k")
        adapter = LiFiAdapter(
            config=config,
            allow_placeholder_prices=True,
            token_resolver=_mock_token_resolver(),
        )
        assert adapter._using_placeholders


# ============================================================================
# Token Resolution Tests
# ============================================================================


class TestTokenResolution:
    """Tests for token resolution in adapter."""

    def test_resolve_symbol(self, adapter):
        """Resolve token symbol to address."""
        address = adapter.resolve_token_address("USDC")
        assert address == USDC_ARBITRUM

    def test_resolve_address_passthrough(self, adapter):
        """Address strings pass through unchanged."""
        address = adapter.resolve_token_address(USDC_ARBITRUM)
        assert address == USDC_ARBITRUM

    def test_resolve_unknown_token_raises(self, adapter):
        """Unknown token raises TokenResolutionError."""
        with pytest.raises(TokenResolutionError):
            adapter.resolve_token_address("UNKNOWN_TOKEN")

    def test_get_token_decimals(self, adapter):
        """Get token decimals."""
        assert adapter.get_token_decimals("USDC") == 6
        assert adapter.get_token_decimals("WETH") == 18


# ============================================================================
# Compile Swap Intent Tests
# ============================================================================


class TestCompileSwapIntent:
    """Tests for compiling swap intents."""

    @patch.object(LiFiClient, "get_quote")
    def test_compile_same_chain_swap(self, mock_get_quote, adapter):
        """Compile same-chain swap intent."""
        from almanak.framework.intents.vocabulary import SwapIntent

        mock_get_quote.return_value = _make_lifi_step(
            tool="1inch",
            step_type="swap",
            from_chain_id=42161,
            to_chain_id=42161,
        )

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
            max_slippage=Decimal("0.005"),
        )

        bundle = adapter.compile_swap_intent(intent)

        assert bundle.intent_type == "SWAP"
        assert len(bundle.transactions) == 2  # approve + swap
        assert bundle.metadata["protocol"] == "lifi"
        assert bundle.metadata["tool"] == "1inch"
        assert bundle.metadata["is_cross_chain"] is False
        assert bundle.metadata["deferred_swap"] is True

        # First tx should be approve
        assert bundle.transactions[0]["tx_type"] == "approve"
        # Second tx should be deferred swap
        assert bundle.transactions[1]["tx_type"] == "swap_deferred"

    @patch.object(LiFiClient, "get_quote")
    def test_compile_cross_chain_swap(self, mock_get_quote, adapter):
        """Compile cross-chain swap intent."""
        from almanak.framework.intents.vocabulary import SwapIntent

        mock_get_quote.return_value = _make_lifi_step(
            tool="across",
            step_type="cross",
            from_chain_id=42161,
            to_chain_id=8453,
        )

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDC",
            amount=Decimal("1000"),
            max_slippage=Decimal("0.005"),
        )

        bundle = adapter.compile_swap_intent(intent, destination_chain_id=8453)

        assert bundle.intent_type == "SWAP"
        assert bundle.metadata["is_cross_chain"] is True
        assert bundle.metadata["from_chain_id"] == 42161
        assert bundle.metadata["to_chain_id"] == 8453
        assert bundle.metadata["tool"] == "across"

        # Should have bridge_deferred type
        swap_tx = bundle.transactions[-1]
        assert swap_tx["tx_type"] == "bridge_deferred"

    @patch.object(LiFiClient, "get_quote")
    def test_compile_with_amount_usd(self, mock_get_quote, adapter):
        """Compile swap intent with USD amount."""
        from almanak.framework.intents.vocabulary import SwapIntent

        mock_get_quote.return_value = _make_lifi_step(
            tool="1inch", step_type="swap",
            from_chain_id=42161, to_chain_id=42161,
        )

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.005"),
        )

        bundle = adapter.compile_swap_intent(intent)

        assert bundle.intent_type == "SWAP"
        assert len(bundle.transactions) >= 1
        assert "error" not in bundle.metadata

    @patch.object(LiFiClient, "get_quote")
    def test_compile_native_token_no_approve(self, mock_get_quote, adapter):
        """Compile swap with native token skips approval."""
        from almanak.framework.intents.vocabulary import SwapIntent

        # Mock resolver to return native token
        adapter._token_resolver.resolve.side_effect = None
        adapter._token_resolver.resolve.return_value = MagicMock(
            address="0x0000000000000000000000000000000000000000",
            decimals=18,
            symbol="ETH",
        )

        mock_get_quote.return_value = _make_lifi_step(
            tool="1inch", step_type="swap",
            from_chain_id=42161, to_chain_id=42161,
            approval_address="0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE",
        )

        intent = SwapIntent(
            from_token="ETH",
            to_token="ETH",
            amount=Decimal("1"),
            max_slippage=Decimal("0.005"),
        )

        bundle = adapter.compile_swap_intent(intent)

        # Should only have swap, no approve
        assert len(bundle.transactions) == 1
        assert bundle.transactions[0]["tx_type"] in ("swap_deferred", "bridge_deferred")

    @patch.object(LiFiClient, "get_quote")
    def test_compile_error_returns_error_bundle(self, mock_get_quote, adapter):
        """Failed compilation returns error bundle."""
        from almanak.framework.intents.vocabulary import SwapIntent

        mock_get_quote.side_effect = LiFiRouteNotFoundError("No route found")

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
            max_slippage=Decimal("0.005"),
        )

        bundle = adapter.compile_swap_intent(intent)

        assert bundle.intent_type == "SWAP"
        assert len(bundle.transactions) == 0
        assert "error" in bundle.metadata

    def test_compile_amount_all_returns_error(self, adapter):
        """Compile swap with amount='all' returns error (must be resolved first)."""
        from almanak.framework.intents.vocabulary import SwapIntent

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount="all",
            max_slippage=Decimal("0.005"),
        )

        bundle = adapter.compile_swap_intent(intent)

        assert len(bundle.transactions) == 0
        # Should have an error because amount='all' needs resolution
        assert "error" in bundle.metadata


# ============================================================================
# Fresh Transaction Tests
# ============================================================================


class TestGetFreshTransaction:
    """Tests for fetching fresh transaction data."""

    @patch.object(LiFiClient, "get_quote")
    def test_get_fresh_transaction(self, mock_get_quote, adapter):
        """Get fresh transaction returns updated data."""
        mock_get_quote.return_value = _make_lifi_step(
            tool="across",
            step_type="cross",
            to_amount="997000000",
        )

        metadata = {
            "from_token": "USDC",
            "to_token": "USDC",
            "is_cross_chain": True,
            "route_params": {
                "from_chain_id": 42161,
                "to_chain_id": 8453,
                "from_token": USDC_ARBITRUM,
                "to_token": USDC_BASE,
                "from_amount": "1000000000",
                "from_address": WALLET_ADDRESS,
                "slippage": 0.005,
            },
        }

        result = adapter.get_fresh_transaction(metadata)

        assert result["to"] == "0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE"
        assert result["data"] == "0xabcdef1234567890"
        assert result["amount_out"] == 997000000
        assert result["tool"] == "across"

    def test_get_fresh_transaction_no_params_raises(self, adapter):
        """Get fresh transaction without route_params raises."""
        with pytest.raises(ValueError, match="route_params"):
            adapter.get_fresh_transaction({"some": "metadata"})


# ============================================================================
# Approval Transaction Tests
# ============================================================================


class TestApprovalTransaction:
    """Tests for approval transaction building."""

    def test_build_approve_transaction(self, adapter):
        """Build approve transaction has correct calldata."""
        tx = adapter._build_approve_transaction(
            token_address=USDC_ARBITRUM,
            spender="0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE",
            amount=1000000000,
        )

        assert tx is not None
        assert tx.to == USDC_ARBITRUM
        assert tx.value == 0
        assert tx.data.startswith(ERC20_APPROVE_SELECTOR)
        assert tx.tx_type == "approve"
        assert tx.gas_estimate == LIFI_GAS_ESTIMATES["approve"]

    def test_native_token_no_approval(self, adapter):
        """Native token doesn't need approval."""
        tx = adapter._build_approve_transaction(
            token_address="0x0000000000000000000000000000000000000000",
            spender="0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE",
            amount=1000000000000000000,
        )

        assert tx is None

    def test_native_token_eee_no_approval(self, adapter):
        """Native token (0xeee...) doesn't need approval."""
        tx = adapter._build_approve_transaction(
            token_address="0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            spender="0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE",
            amount=1000000000000000000,
        )

        assert tx is None


# ============================================================================
# Utility Tests
# ============================================================================


class TestAdapterUtilities:
    """Tests for adapter utility methods."""

    def test_is_native_token(self):
        """Check native token detection."""
        assert LiFiAdapter._is_native_token("0x0000000000000000000000000000000000000000")
        assert LiFiAdapter._is_native_token("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
        assert not LiFiAdapter._is_native_token(USDC_ARBITRUM)

    def test_pad_address(self):
        """Address padding to 32 bytes."""
        padded = LiFiAdapter._pad_address("0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE")
        assert len(padded) == 64
        assert padded.startswith("0000")

    def test_pad_uint256(self):
        """uint256 padding to 32 bytes."""
        padded = LiFiAdapter._pad_uint256(1000)
        assert len(padded) == 64

    def test_transaction_data_to_dict(self):
        """TransactionData serializes correctly."""
        tx = TransactionData(
            to="0x1234",
            value=100,
            data="0xabcdef",
            gas_estimate=50000,
            description="Test tx",
            tx_type="swap",
        )
        d = tx.to_dict()
        assert d["to"] == "0x1234"
        assert d["value"] == "100"
        assert d["tx_type"] == "swap"
