"""Tests for Aerodrome Adapter.

This test suite covers:
- Configuration and initialization
- Swap operations (exact input for volatile and stable pools)
- Liquidity operations (add/remove)
- Token resolution
- Transaction building
- Intent compilation to ActionBundle
"""

from decimal import Decimal

import pytest

from ....intents.vocabulary import SwapIntent
from ..adapter import (
    AERODROME_ADDRESSES,
    AERODROME_GAS_ESTIMATES,
    AERODROME_TOKENS,
    TOKEN_DECIMALS,
    AerodromeAdapter,
    AerodromeConfig,
)

# =============================================================================
# Configuration Tests
# =============================================================================


class TestAerodromeConfig:
    """Tests for AerodromeConfig."""

    def test_config_creation_base(self) -> None:
        """Test config creation for Base chain."""
        config = AerodromeConfig(
            chain="base",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )

        assert config.chain == "base"
        assert config.wallet_address == "0x1234567890123456789012345678901234567890"
        assert config.default_slippage_bps == 50
        assert config.deadline_seconds == 300

    def test_config_invalid_chain(self) -> None:
        """Test config with invalid chain."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            AerodromeConfig(
                chain="ethereum",  # Aerodrome is only on Base
                wallet_address="0x1234567890123456789012345678901234567890",
                allow_placeholder_prices=True,
            )

    def test_config_custom_slippage(self) -> None:
        """Test config with custom slippage."""
        config = AerodromeConfig(
            chain="base",
            wallet_address="0x1234567890123456789012345678901234567890",
            default_slippage_bps=100,  # 1%
            allow_placeholder_prices=True,  # Allow for unit tests
        )

        assert config.default_slippage_bps == 100

    def test_config_invalid_slippage_negative(self) -> None:
        """Test config with negative slippage."""
        with pytest.raises(ValueError, match="Slippage must be between"):
            AerodromeConfig(
                chain="base",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=-1,
                allow_placeholder_prices=True,
            )

    def test_config_invalid_slippage_too_high(self) -> None:
        """Test config with too high slippage."""
        with pytest.raises(ValueError, match="Slippage must be between"):
            AerodromeConfig(
                chain="base",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=10001,
                allow_placeholder_prices=True,
            )

    def test_config_custom_deadline(self) -> None:
        """Test config with custom deadline."""
        config = AerodromeConfig(
            chain="base",
            wallet_address="0x1234567890123456789012345678901234567890",
            deadline_seconds=600,  # 10 minutes
            allow_placeholder_prices=True,  # Allow for unit tests
        )

        assert config.deadline_seconds == 600

    def test_config_to_dict(self) -> None:
        """Test config serialization."""
        config = AerodromeConfig(
            chain="base",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )

        config_dict = config.to_dict()

        assert config_dict["chain"] == "base"
        assert config_dict["wallet_address"] == "0x1234567890123456789012345678901234567890"
        assert config_dict["default_slippage_bps"] == 50
        assert config_dict["deadline_seconds"] == 300


# =============================================================================
# Adapter Initialization Tests
# =============================================================================


class TestAerodromeAdapterInit:
    """Tests for AerodromeAdapter initialization."""

    def test_adapter_creation(self) -> None:
        """Test adapter creation."""
        config = AerodromeConfig(
            chain="base",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        adapter = AerodromeAdapter(config)

        assert adapter.chain == "base"
        assert adapter.wallet_address == "0x1234567890123456789012345678901234567890"
        assert adapter.config == config

    def test_adapter_has_addresses(self) -> None:
        """Test adapter has correct contract addresses."""
        config = AerodromeConfig(
            chain="base",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        adapter = AerodromeAdapter(config)

        assert "router" in adapter.addresses
        assert "factory" in adapter.addresses
        assert adapter.addresses["router"] == AERODROME_ADDRESSES["base"]["router"]

    def test_adapter_has_tokens(self) -> None:
        """Test adapter has token addresses."""
        config = AerodromeConfig(
            chain="base",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        adapter = AerodromeAdapter(config)

        assert "USDC" in adapter.tokens
        assert "WETH" in adapter.tokens
        assert "USDbC" in adapter.tokens
        assert "AERO" in adapter.tokens


# =============================================================================
# Token Resolution Tests
# =============================================================================


class TestTokenResolution:
    """Tests for token resolution."""

    @pytest.fixture
    def adapter(self) -> AerodromeAdapter:
        """Create adapter fixture."""
        config = AerodromeConfig(
            chain="base",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        return AerodromeAdapter(config)

    def test_resolve_token_by_symbol(self, adapter: AerodromeAdapter) -> None:
        """Test resolving token by symbol."""
        result = adapter._resolve_token("USDC")
        assert result is not None
        assert result.lower() == AERODROME_TOKENS["base"]["USDC"].lower()

    def test_resolve_token_by_symbol_lowercase(self, adapter: AerodromeAdapter) -> None:
        """Test resolving token by lowercase symbol."""
        result = adapter._resolve_token("usdc")
        assert result is not None
        assert result.lower() == AERODROME_TOKENS["base"]["USDC"].lower()

    def test_resolve_token_by_address(self, adapter: AerodromeAdapter) -> None:
        """Test resolving token by address."""
        address = AERODROME_TOKENS["base"]["USDC"]
        result = adapter._resolve_token(address)
        assert result == address

    def test_resolve_unknown_token(self, adapter: AerodromeAdapter) -> None:
        """Test resolving unknown token."""
        result = adapter._resolve_token("UNKNOWN_TOKEN")
        assert result is None

    def test_get_token_symbol(self, adapter: AerodromeAdapter) -> None:
        """Test getting token symbol from address."""
        address = AERODROME_TOKENS["base"]["WETH"]
        symbol = adapter._get_token_symbol(address)
        assert symbol == "WETH"

    def test_get_token_symbol_unknown(self, adapter: AerodromeAdapter) -> None:
        """Test getting symbol for unknown address."""
        symbol = adapter._get_token_symbol("0x0000000000000000000000000000000000000001")
        assert symbol == "UNKNOWN"

    def test_get_token_decimals(self, adapter: AerodromeAdapter) -> None:
        """Test getting token decimals."""
        assert adapter._get_token_decimals("USDC") == 6
        assert adapter._get_token_decimals("USDBC") == 6  # Normalized to uppercase
        assert adapter._get_token_decimals("WETH") == 18
        assert adapter._get_token_decimals("DAI") == 18

    def test_get_token_decimals_default(self, adapter: AerodromeAdapter) -> None:
        """Test default token decimals for unknown token."""
        assert adapter._get_token_decimals("UNKNOWN") == 18

    def test_is_native_token(self, adapter: AerodromeAdapter) -> None:
        """Test native token detection."""
        assert adapter._is_native_token("ETH") is True
        assert adapter._is_native_token("eth") is True
        assert adapter._is_native_token("WETH") is False
        assert adapter._is_native_token("USDC") is False
        assert adapter._is_native_token("0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE") is True


# =============================================================================
# Swap Tests
# =============================================================================


class TestSwapOperations:
    """Tests for swap operations."""

    @pytest.fixture
    def adapter(self) -> AerodromeAdapter:
        """Create adapter fixture."""
        config = AerodromeConfig(
            chain="base",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        return AerodromeAdapter(config)

    def test_swap_exact_input_volatile(self, adapter: AerodromeAdapter) -> None:
        """Test building a volatile pool swap."""
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
            stable=False,
            slippage_bps=50,
        )

        assert result.success is True
        assert len(result.transactions) > 0
        assert result.amount_in > 0
        assert result.amount_out_minimum > 0
        assert result.gas_estimate > 0

    def test_swap_exact_input_stable(self, adapter: AerodromeAdapter) -> None:
        """Test building a stable pool swap."""
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="USDbC",
            amount_in=Decimal("1000"),
            stable=True,
            slippage_bps=10,
        )

        assert result.success is True
        assert len(result.transactions) > 0
        # Stable pools should have higher output due to lower fees
        assert result.amount_out_minimum > 0

    def test_swap_with_quote(self, adapter: AerodromeAdapter) -> None:
        """Test swap includes quote information."""
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
            stable=False,
        )

        assert result.success is True
        assert result.quote is not None
        assert result.quote.amount_in > 0
        assert result.quote.amount_out > 0
        assert result.quote.stable is False

    def test_swap_unknown_input_token(self, adapter: AerodromeAdapter) -> None:
        """Test swap with unknown input token."""
        result = adapter.swap_exact_input(
            token_in="UNKNOWN_TOKEN",
            token_out="WETH",
            amount_in=Decimal("1000"),
            stable=False,
        )

        assert result.success is False
        assert "Unknown input token" in result.error

    def test_swap_unknown_output_token(self, adapter: AerodromeAdapter) -> None:
        """Test swap with unknown output token."""
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="UNKNOWN_TOKEN",
            amount_in=Decimal("1000"),
            stable=False,
        )

        assert result.success is False
        assert "Unknown output token" in result.error

    def test_swap_includes_approval(self, adapter: AerodromeAdapter) -> None:
        """Test swap includes approval transaction."""
        # Clear allowance cache
        adapter.clear_allowance_cache()

        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
            stable=False,
        )

        assert result.success is True
        # Should have approve + swap
        assert len(result.transactions) >= 2
        assert result.transactions[0].tx_type == "approve"
        assert result.transactions[1].tx_type == "swap"

    def test_swap_skips_approval_if_cached(self, adapter: AerodromeAdapter) -> None:
        """Test swap skips approval if allowance is cached."""
        # Set allowance cache
        usdc_address = AERODROME_TOKENS["base"]["USDC"]
        router_address = AERODROME_ADDRESSES["base"]["router"]
        adapter.set_allowance(usdc_address, router_address, 10**30)

        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
            stable=False,
        )

        assert result.success is True
        # Should only have swap (no approve)
        assert len(result.transactions) == 1
        assert result.transactions[0].tx_type == "swap"


# =============================================================================
# Liquidity Tests
# =============================================================================


class TestLiquidityOperations:
    """Tests for liquidity operations."""

    @pytest.fixture
    def adapter(self) -> AerodromeAdapter:
        """Create adapter fixture."""
        config = AerodromeConfig(
            chain="base",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        return AerodromeAdapter(config)

    def test_add_liquidity_volatile(self, adapter: AerodromeAdapter) -> None:
        """Test adding liquidity to volatile pool."""
        adapter.clear_allowance_cache()

        result = adapter.add_liquidity(
            token_a="USDC",
            token_b="WETH",
            amount_a=Decimal("1000"),
            amount_b=Decimal("0.5"),
            stable=False,
            slippage_bps=50,
        )

        assert result.success is True
        assert len(result.transactions) >= 1
        assert result.amount_a > 0
        assert result.amount_b > 0
        assert result.stable is False

    def test_add_liquidity_stable(self, adapter: AerodromeAdapter) -> None:
        """Test adding liquidity to stable pool."""
        adapter.clear_allowance_cache()

        result = adapter.add_liquidity(
            token_a="USDC",
            token_b="USDbC",
            amount_a=Decimal("1000"),
            amount_b=Decimal("1000"),
            stable=True,
            slippage_bps=10,
        )

        assert result.success is True
        assert result.stable is True

    def test_add_liquidity_unknown_token(self, adapter: AerodromeAdapter) -> None:
        """Test adding liquidity with unknown token."""
        result = adapter.add_liquidity(
            token_a="UNKNOWN",
            token_b="WETH",
            amount_a=Decimal("1000"),
            amount_b=Decimal("0.5"),
            stable=False,
        )

        assert result.success is False
        assert "Unknown token" in result.error

    def test_remove_liquidity_volatile(self, adapter: AerodromeAdapter) -> None:
        """Test removing liquidity from volatile pool."""
        result = adapter.remove_liquidity(
            token_a="USDC",
            token_b="WETH",
            liquidity=Decimal("100"),  # LP tokens
            stable=False,
            slippage_bps=50,
        )

        assert result.success is True
        assert len(result.transactions) >= 1
        assert result.liquidity > 0
        assert result.stable is False

    def test_remove_liquidity_stable(self, adapter: AerodromeAdapter) -> None:
        """Test removing liquidity from stable pool."""
        result = adapter.remove_liquidity(
            token_a="USDC",
            token_b="USDbC",
            liquidity=Decimal("100"),
            stable=True,
            slippage_bps=10,
        )

        assert result.success is True
        assert result.stable is True


# =============================================================================
# Transaction Building Tests
# =============================================================================


class TestTransactionBuilding:
    """Tests for transaction building."""

    @pytest.fixture
    def adapter(self) -> AerodromeAdapter:
        """Create adapter fixture."""
        config = AerodromeConfig(
            chain="base",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        return AerodromeAdapter(config)

    def test_swap_transaction_has_correct_target(self, adapter: AerodromeAdapter) -> None:
        """Test swap transaction targets router."""
        adapter.clear_allowance_cache()

        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
            stable=False,
        )

        # Find swap transaction
        swap_tx = next((tx for tx in result.transactions if tx.tx_type == "swap"), None)

        assert swap_tx is not None
        assert swap_tx.to.lower() == AERODROME_ADDRESSES["base"]["router"].lower()

    def test_swap_transaction_has_calldata(self, adapter: AerodromeAdapter) -> None:
        """Test swap transaction has calldata."""
        adapter.clear_allowance_cache()

        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
            stable=False,
        )

        swap_tx = next((tx for tx in result.transactions if tx.tx_type == "swap"), None)

        assert swap_tx is not None
        assert swap_tx.data.startswith("0x")
        assert len(swap_tx.data) > 10  # Has actual calldata

    def test_swap_transaction_has_description(self, adapter: AerodromeAdapter) -> None:
        """Test swap transaction has human-readable description."""
        adapter.clear_allowance_cache()

        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
            stable=False,
        )

        swap_tx = next((tx for tx in result.transactions if tx.tx_type == "swap"), None)

        assert swap_tx is not None
        assert "Aerodrome" in swap_tx.description
        assert "volatile" in swap_tx.description
        assert "USDC" in swap_tx.description
        assert "WETH" in swap_tx.description

    def test_stable_swap_description(self, adapter: AerodromeAdapter) -> None:
        """Test stable swap has correct description."""
        adapter.clear_allowance_cache()

        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="USDbC",
            amount_in=Decimal("1000"),
            stable=True,
        )

        swap_tx = next((tx for tx in result.transactions if tx.tx_type == "swap"), None)

        assert swap_tx is not None
        assert "stable" in swap_tx.description


# =============================================================================
# Intent Compilation Tests
# =============================================================================


class TestIntentCompilation:
    """Tests for intent compilation."""

    @pytest.fixture
    def adapter(self) -> AerodromeAdapter:
        """Create adapter fixture."""
        config = AerodromeConfig(
            chain="base",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,  # Allow for unit tests
        )
        return AerodromeAdapter(config)

    def test_compile_swap_intent_by_amount(self, adapter: AerodromeAdapter) -> None:
        """Test compiling SwapIntent with direct amount."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
            max_slippage=Decimal("0.005"),  # 0.5%
        )

        bundle = adapter.compile_swap_intent(intent, stable=False)

        assert bundle.intent_type == "SWAP"
        assert len(bundle.transactions) > 0
        assert bundle.metadata["protocol"] == "aerodrome"
        assert bundle.metadata["stable"] is False

    def test_compile_swap_intent_by_usd(self, adapter: AerodromeAdapter) -> None:
        """Test compiling SwapIntent with USD amount."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.005"),
        )

        bundle = adapter.compile_swap_intent(intent, stable=False)

        assert bundle.intent_type == "SWAP"
        assert len(bundle.transactions) > 0

    def test_compile_swap_intent_stable(self, adapter: AerodromeAdapter) -> None:
        """Test compiling SwapIntent for stable pool."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="USDbC",
            amount=Decimal("1000"),
            max_slippage=Decimal("0.001"),  # 0.1%
        )

        bundle = adapter.compile_swap_intent(intent, stable=True)

        assert bundle.intent_type == "SWAP"
        assert bundle.metadata["stable"] is True

    def test_compile_swap_intent_chained_amount_fails(self, adapter: AerodromeAdapter) -> None:
        """Test compiling SwapIntent with chained amount fails."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount="all",  # Chained amount
            max_slippage=Decimal("0.005"),
        )

        with pytest.raises(ValueError, match="amount='all' must be resolved"):
            adapter.compile_swap_intent(intent, stable=False)

    def test_compile_swap_intent_no_amount_fails(self, adapter: AerodromeAdapter) -> None:
        """Test creating SwapIntent without amount fails.

        Note: SwapIntent validates amount requirement at init time.
        """
        with pytest.raises(ValueError, match="Either amount_usd or amount"):
            SwapIntent(
                from_token="USDC",
                to_token="WETH",
                max_slippage=Decimal("0.005"),
            )


# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for constants."""

    def test_aerodrome_addresses_has_base(self) -> None:
        """Test Aerodrome addresses include Base."""
        assert "base" in AERODROME_ADDRESSES
        assert "router" in AERODROME_ADDRESSES["base"]
        assert "factory" in AERODROME_ADDRESSES["base"]

    def test_aerodrome_tokens_has_common_tokens(self) -> None:
        """Test Aerodrome tokens include common tokens."""
        base_tokens = AERODROME_TOKENS["base"]
        assert "WETH" in base_tokens
        assert "USDC" in base_tokens
        assert "USDbC" in base_tokens
        assert "AERO" in base_tokens

    def test_gas_estimates_reasonable(self) -> None:
        """Test gas estimates are reasonable."""
        assert AERODROME_GAS_ESTIMATES["approve"] > 0
        assert AERODROME_GAS_ESTIMATES["swap"] > 0
        assert AERODROME_GAS_ESTIMATES["add_liquidity"] > 0
        assert AERODROME_GAS_ESTIMATES["remove_liquidity"] > 0
        # Swap should be more than approve
        assert AERODROME_GAS_ESTIMATES["swap"] > AERODROME_GAS_ESTIMATES["approve"]

    def test_token_decimals_correct(self) -> None:
        """Test token decimals are correct (keys normalized to uppercase)."""
        assert TOKEN_DECIMALS["USDC"] == 6
        assert TOKEN_DECIMALS["USDBC"] == 6  # Bridged USDC, normalized to uppercase
        assert TOKEN_DECIMALS["WETH"] == 18
        assert TOKEN_DECIMALS["DAI"] == 18
        assert TOKEN_DECIMALS["AERO"] == 18
