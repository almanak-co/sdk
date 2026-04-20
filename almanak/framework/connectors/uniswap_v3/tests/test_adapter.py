"""Tests for Uniswap V3 Adapter.

This test suite covers:
- Configuration and initialization
- Swap operations (exact input, exact output)
- Token resolution
- Transaction building
- Intent compilation to ActionBundle
"""

from decimal import Decimal

import pytest

from ....intents.vocabulary import SwapIntent
from ..adapter import (
    DEFAULT_FEE_TIER,
    FEE_TIERS,
    TOKEN_DECIMALS,
    UNISWAP_V3_ADDRESSES,
    UNISWAP_V3_GAS_ESTIMATES,
    UNISWAP_V3_TOKENS,
    SwapQuote,
    SwapResult,
    TransactionData,
    UniswapV3Adapter,
    UniswapV3Config,
)

# =============================================================================
# Configuration Tests
# =============================================================================


class TestUniswapV3Config:
    """Tests for UniswapV3Config."""

    def test_config_creation_arbitrum(self) -> None:
        """Test config creation for Arbitrum."""
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )

        assert config.chain == "arbitrum"
        assert config.wallet_address == "0x1234567890123456789012345678901234567890"
        assert config.default_slippage_bps == 50
        assert config.default_fee_tier == DEFAULT_FEE_TIER

    def test_config_creation_ethereum(self) -> None:
        """Test config creation for Ethereum."""
        config = UniswapV3Config(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )

        assert config.chain == "ethereum"
        assert config.default_fee_tier == 3000

    def test_config_creation_all_chains(self) -> None:
        """Test config creation for all supported chains."""
        supported_chains = ["ethereum", "arbitrum", "optimism", "polygon", "base"]

        for chain in supported_chains:
            config = UniswapV3Config(
                chain=chain,
                wallet_address="0x1234567890123456789012345678901234567890",
                allow_placeholder_prices=True,  # Allow for unit tests
            )
            assert config.chain == chain

    def test_config_requires_price_provider_by_default(self) -> None:
        """Test config raises ValueError when no price_provider and placeholders not allowed."""
        with pytest.raises(ValueError, match="requires price_provider"):
            UniswapV3Config(
                chain="arbitrum",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_config_allows_placeholder_prices_for_testing(self) -> None:
        """Test config accepts missing price_provider when explicitly allowed for testing."""
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,
        )
        assert config.price_provider is None
        assert config.allow_placeholder_prices is True

    def test_config_accepts_real_price_provider(self) -> None:
        """Test config accepts real price provider."""
        prices = {"ETH": Decimal("3400"), "USDC": Decimal("1")}
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            price_provider=prices,
        )
        assert config.price_provider == prices
        assert config.allow_placeholder_prices is False

    def test_config_invalid_chain(self) -> None:
        """Test config with invalid chain."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            UniswapV3Config(
                chain="invalid_chain",
                wallet_address="0x1234567890123456789012345678901234567890",
                allow_placeholder_prices=True,
            )

    def test_config_custom_slippage(self) -> None:
        """Test config with custom slippage."""
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            default_slippage_bps=100,  # 1%
            allow_placeholder_prices=True,  # Allow for unit tests
        )

        assert config.default_slippage_bps == 100

    def test_config_invalid_slippage_negative(self) -> None:
        """Test config with negative slippage."""
        with pytest.raises(ValueError, match="Slippage must be between"):
            UniswapV3Config(
                chain="arbitrum",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=-1,
                allow_placeholder_prices=True,
            )

    def test_config_invalid_slippage_too_high(self) -> None:
        """Test config with too high slippage."""
        with pytest.raises(ValueError, match="Slippage must be between"):
            UniswapV3Config(
                chain="arbitrum",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=10001,
                allow_placeholder_prices=True,
            )

    def test_config_custom_fee_tier(self) -> None:
        """Test config with custom fee tier."""
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            default_fee_tier=500,  # 0.05%
            allow_placeholder_prices=True,  # Allow for unit tests
        )

        assert config.default_fee_tier == 500

    def test_config_invalid_fee_tier(self) -> None:
        """Test config with invalid fee tier."""
        with pytest.raises(ValueError, match="Invalid fee tier"):
            UniswapV3Config(
                chain="arbitrum",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_fee_tier=999,
                allow_placeholder_prices=True,
            )

    def test_config_to_dict(self) -> None:
        """Test config serialization."""
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )

        config_dict = config.to_dict()

        assert config_dict["chain"] == "arbitrum"
        assert config_dict["wallet_address"] == "0x1234567890123456789012345678901234567890"
        assert config_dict["default_slippage_bps"] == 50
        assert config_dict["default_fee_tier"] == 3000


# =============================================================================
# Adapter Initialization Tests
# =============================================================================


class TestUniswapV3AdapterInit:
    """Tests for UniswapV3Adapter initialization."""

    def test_adapter_creation(self) -> None:
        """Test adapter creation."""
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        adapter = UniswapV3Adapter(config)

        assert adapter.chain == "arbitrum"
        assert adapter.wallet_address == "0x1234567890123456789012345678901234567890"
        assert adapter.addresses == UNISWAP_V3_ADDRESSES["arbitrum"]
        assert adapter.tokens == UNISWAP_V3_TOKENS["arbitrum"]

    def test_adapter_has_swap_router(self) -> None:
        """Test adapter has swap router address."""
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        adapter = UniswapV3Adapter(config)

        assert "swap_router" in adapter.addresses
        assert adapter.addresses["swap_router"].startswith("0x")

    def test_adapter_has_factory(self) -> None:
        """Test adapter has factory address."""
        config = UniswapV3Config(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        adapter = UniswapV3Adapter(config)

        assert "factory" in adapter.addresses
        assert adapter.addresses["factory"].startswith("0x")


# =============================================================================
# Token Resolution Tests
# =============================================================================


class TestUniswapV3AdapterTokenResolution:
    """Tests for token resolution methods."""

    @pytest.fixture
    def adapter(self) -> UniswapV3Adapter:
        """Create adapter for testing."""
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        return UniswapV3Adapter(config)

    def test_resolve_token_by_symbol(self, adapter: UniswapV3Adapter) -> None:
        """Test token resolution by symbol."""
        token_address = adapter._resolve_token("USDC")

        assert token_address == UNISWAP_V3_TOKENS["arbitrum"]["USDC"]

    def test_resolve_token_by_address(self, adapter: UniswapV3Adapter) -> None:
        """Test token resolution by address."""
        address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        token_address = adapter._resolve_token(address)

        assert token_address == address

    def test_resolve_token_unknown(self, adapter: UniswapV3Adapter) -> None:
        """Test token resolution for unknown token."""
        token_address = adapter._resolve_token("UNKNOWN")

        assert token_address is None

    def test_resolve_token_case_insensitive(self, adapter: UniswapV3Adapter) -> None:
        """Test token resolution is case insensitive."""
        token_address_lower = adapter._resolve_token("usdc")
        token_address_upper = adapter._resolve_token("USDC")

        assert token_address_lower == token_address_upper

    def test_get_token_decimals(self, adapter: UniswapV3Adapter) -> None:
        """Test getting token decimals."""
        assert adapter._get_token_decimals("USDC") == 6
        assert adapter._get_token_decimals("WETH") == 18
        assert adapter._get_token_decimals("WBTC") == 8
        assert adapter._get_token_decimals("UNKNOWN") == 18  # Default

    def test_is_native_token_eth(self, adapter: UniswapV3Adapter) -> None:
        """Test native token detection for ETH."""
        assert adapter._is_native_token("ETH") is True
        assert adapter._is_native_token("WETH") is False
        assert adapter._is_native_token("USDC") is False

    def test_is_native_token_placeholder(self, adapter: UniswapV3Adapter) -> None:
        """Test native token detection for placeholder address."""
        placeholder = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
        assert adapter._is_native_token(placeholder) is True


# =============================================================================
# Swap Tests
# =============================================================================


class TestUniswapV3AdapterSwaps:
    """Tests for swap operations."""

    @pytest.fixture
    def adapter(self) -> UniswapV3Adapter:
        """Create adapter for testing."""
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        return UniswapV3Adapter(config)

    def test_swap_exact_input_success(self, adapter: UniswapV3Adapter) -> None:
        """Test successful exact input swap."""
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
        )

        assert result.success is True
        assert len(result.transactions) >= 1  # At least swap tx
        assert result.amount_in > 0
        assert result.amount_out_minimum > 0
        assert result.gas_estimate > 0

    def test_swap_exact_input_with_custom_slippage(self, adapter: UniswapV3Adapter) -> None:
        """Test exact input swap with custom slippage."""
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
            slippage_bps=100,  # 1%
        )

        assert result.success is True
        # Higher slippage means lower minimum output
        result_default = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
            slippage_bps=50,  # 0.5%
        )
        assert result.amount_out_minimum < result_default.amount_out_minimum

    def test_swap_exact_input_unknown_token_in(self, adapter: UniswapV3Adapter) -> None:
        """Test swap with unknown input token."""
        result = adapter.swap_exact_input(
            token_in="UNKNOWN_TOKEN",
            token_out="WETH",
            amount_in=Decimal("1000"),
        )

        assert result.success is False
        assert "Unknown input token" in (result.error or "")

    def test_swap_exact_input_unknown_token_out(self, adapter: UniswapV3Adapter) -> None:
        """Test swap with unknown output token."""
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="UNKNOWN_TOKEN",
            amount_in=Decimal("1000"),
        )

        assert result.success is False
        assert "Unknown output token" in (result.error or "")

    def test_swap_exact_output_success(self, adapter: UniswapV3Adapter) -> None:
        """Test successful exact output swap."""
        result = adapter.swap_exact_output(
            token_in="USDC",
            token_out="WETH",
            amount_out=Decimal("0.5"),  # 0.5 ETH
        )

        assert result.success is True
        assert len(result.transactions) >= 1
        assert result.amount_in > 0

    def test_swap_exact_output_with_custom_slippage(self, adapter: UniswapV3Adapter) -> None:
        """Test exact output swap with custom slippage."""
        result = adapter.swap_exact_output(
            token_in="USDC",
            token_out="WETH",
            amount_out=Decimal("0.5"),
            slippage_bps=100,  # 1%
        )

        assert result.success is True
        # Higher slippage means higher maximum input

    def test_swap_with_fee_tier(self, adapter: UniswapV3Adapter) -> None:
        """Test swap with specific fee tier."""
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
            fee_tier=500,  # 0.05%
        )

        assert result.success is True
        assert result.quote is not None
        assert result.quote.fee_tier == 500


# =============================================================================
# Transaction Building Tests
# =============================================================================


class TestUniswapV3AdapterTransactionBuilding:
    """Tests for transaction building."""

    @pytest.fixture
    def adapter(self) -> UniswapV3Adapter:
        """Create adapter for testing."""
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        return UniswapV3Adapter(config)

    def test_build_exact_input_single_tx(self, adapter: UniswapV3Adapter) -> None:
        """Test building exact input single transaction."""
        token_in = UNISWAP_V3_TOKENS["arbitrum"]["USDC"]
        token_out = UNISWAP_V3_TOKENS["arbitrum"]["WETH"]

        tx = adapter._build_exact_input_single_tx(
            token_in=token_in,
            token_out=token_out,
            fee=3000,
            recipient=adapter.wallet_address,
            amount_in=1000000000,  # 1000 USDC
            amount_out_minimum=450000000000000000,  # 0.45 ETH
        )

        assert tx.to == adapter.addresses["swap_router"]
        assert tx.value == 0  # Not native token
        assert tx.data.startswith("0x04e45aaf")  # exactInputSingle selector (SwapRouter02)
        assert tx.gas_estimate == UNISWAP_V3_GAS_ESTIMATES["swap_exact_input"]
        assert tx.tx_type == "swap"

    def test_build_exact_output_single_tx(self, adapter: UniswapV3Adapter) -> None:
        """Test building exact output single transaction."""
        token_in = UNISWAP_V3_TOKENS["arbitrum"]["USDC"]
        token_out = UNISWAP_V3_TOKENS["arbitrum"]["WETH"]

        tx = adapter._build_exact_output_single_tx(
            token_in=token_in,
            token_out=token_out,
            fee=3000,
            recipient=adapter.wallet_address,
            amount_out=500000000000000000,  # 0.5 ETH
            amount_in_maximum=1100000000,  # 1100 USDC
        )

        assert tx.to == adapter.addresses["swap_router"]
        assert tx.value == 0  # Not native token
        assert tx.data.startswith("0x5023b4df")  # exactOutputSingle selector (SwapRouter02)
        assert tx.gas_estimate == UNISWAP_V3_GAS_ESTIMATES["swap_exact_output"]

    def test_build_approve_tx(self, adapter: UniswapV3Adapter) -> None:
        """Test building approve transaction."""
        token_address = UNISWAP_V3_TOKENS["arbitrum"]["USDC"]
        spender = adapter.addresses["swap_router"]

        tx = adapter._build_approve_tx(
            token_address=token_address,
            spender=spender,
            amount=1000000000,
        )

        assert tx is not None
        assert tx.to == token_address
        assert tx.value == 0
        assert tx.data.startswith("0x095ea7b3")  # approve selector
        assert tx.gas_estimate == UNISWAP_V3_GAS_ESTIMATES["approve"]
        assert tx.tx_type == "approve"

    def test_build_approve_tx_cached(self, adapter: UniswapV3Adapter) -> None:
        """Test that approve transaction is skipped when cached."""
        token_address = UNISWAP_V3_TOKENS["arbitrum"]["USDC"]
        spender = adapter.addresses["swap_router"]

        # Set high allowance in cache
        adapter.set_allowance(token_address, spender, 2**256 - 1)

        tx = adapter._build_approve_tx(
            token_address=token_address,
            spender=spender,
            amount=1000000000,
        )

        assert tx is None

    def test_transaction_data_to_dict(self, adapter: UniswapV3Adapter) -> None:
        """Test TransactionData serialization."""
        tx = TransactionData(
            to="0x1234",
            value=0,
            data="0xabcd",
            gas_estimate=150000,
            description="Test swap",
            tx_type="swap",
        )

        tx_dict = tx.to_dict()

        assert tx_dict["to"] == "0x1234"
        assert tx_dict["value"] == "0"
        assert tx_dict["data"] == "0xabcd"
        assert tx_dict["gas_estimate"] == 150000


# =============================================================================
# Intent Compilation Tests
# =============================================================================


class TestUniswapV3AdapterIntentCompilation:
    """Tests for intent compilation to ActionBundle."""

    @pytest.fixture
    def adapter(self) -> UniswapV3Adapter:
        """Create adapter for testing."""
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        return UniswapV3Adapter(config)

    def test_compile_swap_intent_with_amount(self, adapter: UniswapV3Adapter) -> None:
        """Test compiling SwapIntent with direct amount."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
            max_slippage=Decimal("0.005"),  # 0.5%
        )

        bundle = adapter.compile_swap_intent(intent)

        assert bundle.intent_type == "SWAP"
        assert len(bundle.transactions) >= 1
        assert bundle.metadata["from_token"] == "USDC"
        assert bundle.metadata["to_token"] == "WETH"
        assert bundle.metadata["chain"] == "arbitrum"
        assert "error" not in bundle.metadata

    def test_compile_swap_intent_with_usd_amount(self, adapter: UniswapV3Adapter) -> None:
        """Test compiling SwapIntent with USD amount."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.005"),
        )

        bundle = adapter.compile_swap_intent(intent)

        assert bundle.intent_type == "SWAP"
        assert len(bundle.transactions) >= 1
        assert bundle.metadata["from_token"] == "USDC"

    def test_compile_swap_intent_error(self, adapter: UniswapV3Adapter) -> None:
        """Test compiling SwapIntent with invalid tokens."""
        intent = SwapIntent(
            from_token="UNKNOWN",
            to_token="WETH",
            amount=Decimal("1000"),
            max_slippage=Decimal("0.005"),
        )

        bundle = adapter.compile_swap_intent(intent)

        assert bundle.intent_type == "SWAP"
        assert len(bundle.transactions) == 0
        assert "error" in bundle.metadata


# =============================================================================
# Quote Tests
# =============================================================================


class TestUniswapV3AdapterQuotes:
    """Tests for quote functionality."""

    @pytest.fixture
    def adapter(self) -> UniswapV3Adapter:
        """Create adapter for testing."""
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        return UniswapV3Adapter(config)

    def test_get_quote_exact_input(self, adapter: UniswapV3Adapter) -> None:
        """Test getting quote for exact input."""
        token_in = UNISWAP_V3_TOKENS["arbitrum"]["USDC"]
        token_out = UNISWAP_V3_TOKENS["arbitrum"]["WETH"]

        quote = adapter._get_quote_exact_input(
            token_in=token_in,
            token_out=token_out,
            amount_in=1000000000,  # 1000 USDC
            fee_tier=3000,
        )

        assert quote.token_in == token_in
        assert quote.token_out == token_out
        assert quote.amount_in == 1000000000
        assert quote.amount_out > 0
        assert quote.fee_tier == 3000

    def test_get_quote_exact_output(self, adapter: UniswapV3Adapter) -> None:
        """Test getting quote for exact output."""
        token_in = UNISWAP_V3_TOKENS["arbitrum"]["USDC"]
        token_out = UNISWAP_V3_TOKENS["arbitrum"]["WETH"]

        quote = adapter._get_quote_exact_output(
            token_in=token_in,
            token_out=token_out,
            amount_out=500000000000000000,  # 0.5 ETH
            fee_tier=3000,
        )

        assert quote.token_in == token_in
        assert quote.token_out == token_out
        assert quote.amount_out == 500000000000000000
        assert quote.amount_in > 0

    def test_swap_quote_to_dict(self, adapter: UniswapV3Adapter) -> None:
        """Test SwapQuote serialization."""
        quote = SwapQuote(
            token_in="0x1234",
            token_out="0x5678",
            amount_in=1000,
            amount_out=500,
            fee_tier=3000,
            effective_price=Decimal("0.5"),
        )

        quote_dict = quote.to_dict()

        assert quote_dict["token_in"] == "0x1234"
        assert quote_dict["amount_in"] == "1000"
        assert quote_dict["effective_price"] == "0.5"


# =============================================================================
# SwapResult Tests
# =============================================================================


class TestSwapResult:
    """Tests for SwapResult dataclass."""

    def test_swap_result_success(self) -> None:
        """Test successful swap result."""
        result = SwapResult(
            success=True,
            amount_in=1000000000,
            amount_out_minimum=450000000000000000,
            gas_estimate=150000,
        )

        assert result.success is True
        assert result.error is None

    def test_swap_result_failure(self) -> None:
        """Test failed swap result."""
        result = SwapResult(
            success=False,
            error="Unknown token",
        )

        assert result.success is False
        assert result.error == "Unknown token"

    def test_swap_result_to_dict(self) -> None:
        """Test SwapResult serialization."""
        result = SwapResult(
            success=True,
            amount_in=1000000000,
            amount_out_minimum=450000000000000000,
            gas_estimate=150000,
        )

        result_dict = result.to_dict()

        assert result_dict["success"] is True
        assert result_dict["amount_in"] == "1000000000"


# =============================================================================
# State Management Tests
# =============================================================================


class TestUniswapV3AdapterStateManagement:
    """Tests for adapter state management."""

    @pytest.fixture
    def adapter(self) -> UniswapV3Adapter:
        """Create adapter for testing."""
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        return UniswapV3Adapter(config)

    def test_set_allowance(self, adapter: UniswapV3Adapter) -> None:
        """Test setting allowance in cache."""
        token = "0x1234"
        spender = "0x5678"
        amount = 1000000000

        adapter.set_allowance(token, spender, amount)

        # Verify by trying to build approve tx
        tx = adapter._build_approve_tx(token, spender, amount)
        assert tx is None  # Should be cached

    def test_clear_allowance_cache(self, adapter: UniswapV3Adapter) -> None:
        """Test clearing allowance cache."""
        token = "0x1234"
        spender = "0x5678"

        adapter.set_allowance(token, spender, 1000000000)
        adapter.clear_allowance_cache()

        # Now approve tx should be generated
        tx = adapter._build_approve_tx(token, spender, 100)
        assert tx is not None


# =============================================================================
# Helper Method Tests
# =============================================================================


class TestUniswapV3AdapterHelpers:
    """Tests for adapter helper methods."""

    @pytest.fixture
    def adapter(self) -> UniswapV3Adapter:
        """Create adapter for testing."""
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        return UniswapV3Adapter(config)

    def test_pad_address(self, adapter: UniswapV3Adapter) -> None:
        """Test address padding."""
        address = "0x1234567890123456789012345678901234567890"
        padded = adapter._pad_address(address)

        assert len(padded) == 64
        assert padded.endswith("1234567890123456789012345678901234567890")

    def test_pad_uint256(self, adapter: UniswapV3Adapter) -> None:
        """Test uint256 padding."""
        value = 1000
        padded = adapter._pad_uint256(value)

        assert len(padded) == 64
        assert int(padded, 16) == value

    def test_pad_uint24(self, adapter: UniswapV3Adapter) -> None:
        """Test uint24 padding."""
        value = 3000
        padded = adapter._pad_uint24(value)

        assert len(padded) == 64
        assert int(padded, 16) == value

    def test_get_token_symbol(self, adapter: UniswapV3Adapter) -> None:
        """Test getting token symbol from address."""
        usdc_address = UNISWAP_V3_TOKENS["arbitrum"]["USDC"]
        symbol = adapter._get_token_symbol(usdc_address)

        assert symbol == "USDC"

    def test_get_token_symbol_unknown(self, adapter: UniswapV3Adapter) -> None:
        """Test getting symbol for unknown address."""
        symbol = adapter._get_token_symbol("0x0000000000000000000000000000000000000000")

        assert symbol == "UNKNOWN"

    def test_get_default_price_oracle(self, adapter: UniswapV3Adapter) -> None:
        """Test default price oracle."""
        prices = adapter._get_default_price_oracle()

        assert "ETH" in prices
        assert "USDC" in prices
        assert prices["USDC"] == Decimal("1")
        assert prices["ETH"] > Decimal("0")


# =============================================================================
# Constants Tests
# =============================================================================


class TestUniswapV3Constants:
    """Tests for Uniswap V3 constants."""

    def test_addresses_defined_for_all_chains(self) -> None:
        """Test that addresses are defined for all chains."""
        required_addresses = ["swap_router", "factory", "position_manager", "quoter_v2"]

        for chain, addresses in UNISWAP_V3_ADDRESSES.items():
            for addr_key in required_addresses:
                assert addr_key in addresses, f"Missing {addr_key} for {chain}"
                assert addresses[addr_key].startswith("0x")

    def test_fee_tiers(self) -> None:
        """Test fee tier constants."""
        assert 100 in FEE_TIERS  # 0.01%
        assert 500 in FEE_TIERS  # 0.05%
        assert 3000 in FEE_TIERS  # 0.3%
        assert 10000 in FEE_TIERS  # 1%

    def test_token_decimals(self) -> None:
        """Test token decimal constants."""
        assert TOKEN_DECIMALS["USDC"] == 6
        assert TOKEN_DECIMALS["WETH"] == 18
        assert TOKEN_DECIMALS["WBTC"] == 8

    def test_gas_estimates(self) -> None:
        """Test gas estimate constants."""
        assert "approve" in UNISWAP_V3_GAS_ESTIMATES
        assert "swap_exact_input" in UNISWAP_V3_GAS_ESTIMATES
        assert "swap_exact_output" in UNISWAP_V3_GAS_ESTIMATES
        assert all(v > 0 for v in UNISWAP_V3_GAS_ESTIMATES.values())
