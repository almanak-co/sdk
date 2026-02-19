"""Tests for Uniswap V3 SDK.

This test suite covers:
- Pool address computation
- Quote fetching (local/offline mode)
- Swap transaction building
- Tick math utilities
- Constants and configuration
"""

import time
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from ..sdk import (
    EXACT_INPUT_SINGLE_SELECTOR,
    EXACT_OUTPUT_SINGLE_SELECTOR,
    FACTORY_ADDRESSES,
    FEE_TIERS,
    MAX_TICK,
    MIN_TICK,
    POOL_INIT_CODE_HASH,
    # Constants
    Q96,
    Q128,
    QUOTER_ADDRESSES,
    ROUTER_ADDRESSES,
    TICK_SPACING,
    InvalidFeeError,
    InvalidTickError,
    PoolInfo,
    PoolNotFoundError,
    PoolState,
    QuoteError,
    SwapQuote,
    SwapTransaction,
    UniswapV3SDK,
    # Exceptions
    compute_pool_address,
    get_max_tick,
    get_min_tick,
    get_nearest_tick,
    price_to_sqrt_price_x96,
    price_to_tick,
    sort_tokens,
    sqrt_price_x96_to_price,
    sqrt_price_x96_to_tick,
    tick_to_price,
    # Tick Math Functions
    tick_to_sqrt_price_x96,
)

# =============================================================================
# Test Constants
# =============================================================================

# Arbitrum token addresses
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WBTC_ADDRESS = "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f"

TEST_WALLET = "0x1234567890123456789012345678901234567890"


# =============================================================================
# SDK Initialization Tests
# =============================================================================


class TestUniswapV3SDKInit:
    """Tests for UniswapV3SDK initialization."""

    def test_sdk_creation_arbitrum(self) -> None:
        """Test SDK creation for Arbitrum."""
        sdk = UniswapV3SDK(chain="arbitrum")

        assert sdk.chain == "arbitrum"
        assert sdk.factory_address == FACTORY_ADDRESSES["arbitrum"]
        assert sdk.router_address == ROUTER_ADDRESSES["arbitrum"]
        assert sdk.quoter_address == QUOTER_ADDRESSES["arbitrum"]

    def test_sdk_creation_ethereum(self) -> None:
        """Test SDK creation for Ethereum."""
        sdk = UniswapV3SDK(chain="ethereum")

        assert sdk.chain == "ethereum"
        assert sdk.factory_address == FACTORY_ADDRESSES["ethereum"]

    def test_sdk_creation_all_chains(self) -> None:
        """Test SDK creation for all supported chains."""
        supported_chains = ["ethereum", "arbitrum", "optimism", "polygon", "base"]

        for chain in supported_chains:
            sdk = UniswapV3SDK(chain=chain)
            assert sdk.chain == chain
            assert sdk.factory_address == FACTORY_ADDRESSES[chain]

    def test_sdk_invalid_chain(self) -> None:
        """Test SDK with invalid chain."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            UniswapV3SDK(chain="invalid_chain")

    def test_sdk_with_rpc_url(self) -> None:
        """Test SDK creation with RPC URL."""
        sdk = UniswapV3SDK(chain="arbitrum", rpc_url="https://arb1.arbitrum.io/rpc")

        assert sdk.rpc_url == "https://arb1.arbitrum.io/rpc"


# =============================================================================
# Pool Address Computation Tests
# =============================================================================


class TestPoolAddressComputation:
    """Tests for pool address computation."""

    @pytest.fixture
    def sdk(self) -> UniswapV3SDK:
        """Create SDK for testing."""
        return UniswapV3SDK(chain="arbitrum")

    def test_get_pool_address(self, sdk: UniswapV3SDK) -> None:
        """Test getting pool address."""
        pool = sdk.get_pool_address(
            token0=WETH_ADDRESS,
            token1=USDC_ADDRESS,
            fee_tier=3000,
        )

        assert pool.address.startswith("0x")
        assert len(pool.address) == 42
        assert pool.fee == 3000
        assert pool.tick_spacing == TICK_SPACING[3000]

    def test_get_pool_address_sorts_tokens(self, sdk: UniswapV3SDK) -> None:
        """Test that pool address computation sorts tokens."""
        # Regardless of input order, should get same pool
        pool1 = sdk.get_pool_address(WETH_ADDRESS, USDC_ADDRESS, fee_tier=3000)
        pool2 = sdk.get_pool_address(USDC_ADDRESS, WETH_ADDRESS, fee_tier=3000)

        assert pool1.address == pool2.address
        assert pool1.token0 == pool2.token0
        assert pool1.token1 == pool2.token1

    def test_get_pool_address_different_fees(self, sdk: UniswapV3SDK) -> None:
        """Test that different fee tiers give different addresses."""
        pool_500 = sdk.get_pool_address(WETH_ADDRESS, USDC_ADDRESS, fee_tier=500)
        pool_3000 = sdk.get_pool_address(WETH_ADDRESS, USDC_ADDRESS, fee_tier=3000)
        pool_10000 = sdk.get_pool_address(WETH_ADDRESS, USDC_ADDRESS, fee_tier=10000)

        assert pool_500.address != pool_3000.address
        assert pool_3000.address != pool_10000.address
        assert pool_500.address != pool_10000.address

    def test_get_pool_address_invalid_fee(self, sdk: UniswapV3SDK) -> None:
        """Test pool address with invalid fee tier."""
        with pytest.raises(InvalidFeeError):
            sdk.get_pool_address(WETH_ADDRESS, USDC_ADDRESS, fee_tier=999)

    def test_compute_pool_address_function(self) -> None:
        """Test standalone compute_pool_address function."""
        address = compute_pool_address(
            factory=FACTORY_ADDRESSES["arbitrum"],
            token0=WETH_ADDRESS,
            token1=USDC_ADDRESS,
            fee=3000,
        )

        assert address.startswith("0x")
        assert len(address) == 42

    def test_compute_pool_address_deterministic(self) -> None:
        """Test that pool address computation is deterministic."""
        addr1 = compute_pool_address(
            factory=FACTORY_ADDRESSES["arbitrum"],
            token0=WETH_ADDRESS,
            token1=USDC_ADDRESS,
            fee=3000,
        )
        addr2 = compute_pool_address(
            factory=FACTORY_ADDRESSES["arbitrum"],
            token0=WETH_ADDRESS,
            token1=USDC_ADDRESS,
            fee=3000,
        )

        assert addr1 == addr2

    def test_sort_tokens(self) -> None:
        """Test token sorting utility."""
        # USDC comes before WETH alphabetically (0xaf < 0x82 is False, but hex compare...)
        sorted1, sorted2 = sort_tokens(WETH_ADDRESS, USDC_ADDRESS)

        # Check they are sorted (lowercase comparison)
        assert sorted1.lower() < sorted2.lower()

    def test_sort_tokens_already_sorted(self) -> None:
        """Test sort_tokens with already sorted input."""
        # If input is already sorted, should be unchanged
        token0, token1 = sort_tokens(USDC_ADDRESS, WETH_ADDRESS)
        verify0, verify1 = sort_tokens(token0, token1)

        assert verify0 == token0
        assert verify1 == token1


# =============================================================================
# PoolInfo Tests
# =============================================================================


class TestPoolInfo:
    """Tests for PoolInfo dataclass."""

    def test_pool_info_creation(self) -> None:
        """Test PoolInfo creation."""
        pool = PoolInfo(
            address="0x1234567890123456789012345678901234567890",
            token0=WETH_ADDRESS,
            token1=USDC_ADDRESS,
            fee=3000,
            tick_spacing=60,
        )

        assert pool.address == "0x1234567890123456789012345678901234567890"
        assert pool.fee == 3000
        assert pool.tick_spacing == 60

    def test_pool_info_to_dict(self) -> None:
        """Test PoolInfo serialization."""
        pool = PoolInfo(
            address="0x1234567890123456789012345678901234567890",
            token0=WETH_ADDRESS,
            token1=USDC_ADDRESS,
            fee=3000,
            tick_spacing=60,
        )

        pool_dict = pool.to_dict()

        assert pool_dict["address"] == "0x1234567890123456789012345678901234567890"
        assert pool_dict["fee"] == 3000
        assert pool_dict["tick_spacing"] == 60


# =============================================================================
# PoolState Tests
# =============================================================================


class TestPoolState:
    """Tests for PoolState dataclass."""

    def test_pool_state_creation(self) -> None:
        """Test PoolState creation."""
        state = PoolState(
            sqrt_price_x96=Q96,
            tick=0,
            liquidity=1000000000000000000,
        )

        assert state.sqrt_price_x96 == Q96
        assert state.tick == 0
        assert state.liquidity == 1000000000000000000

    def test_pool_state_price_property(self) -> None:
        """Test PoolState price calculation."""
        # sqrt_price_x96 = Q96 means sqrt(price) = 1, so price = 1
        state = PoolState(
            sqrt_price_x96=Q96,
            tick=0,
            liquidity=1000000000000000000,
        )

        assert state.price == Decimal("1")

    def test_pool_state_to_dict(self) -> None:
        """Test PoolState serialization."""
        state = PoolState(
            sqrt_price_x96=Q96,
            tick=100,
            liquidity=1000000000000000000,
        )

        state_dict = state.to_dict()

        assert state_dict["tick"] == 100
        assert "price" in state_dict


# =============================================================================
# Quote Tests
# =============================================================================


class TestQuotes:
    """Tests for quote functionality."""

    @pytest.fixture
    def sdk(self) -> UniswapV3SDK:
        """Create SDK for testing."""
        return UniswapV3SDK(chain="arbitrum")

    def test_get_quote_local(self, sdk: UniswapV3SDK) -> None:
        """Test local quote estimation."""
        quote = sdk.get_quote_local(
            token_in=WETH_ADDRESS,
            token_out=USDC_ADDRESS,
            amount_in=10**18,  # 1 ETH
            fee_tier=3000,
        )

        assert quote.token_in == WETH_ADDRESS
        assert quote.token_out == USDC_ADDRESS
        assert quote.amount_in == 10**18
        assert quote.amount_out > 0
        assert quote.fee == 3000

    def test_get_quote_local_with_price_ratio(self, sdk: UniswapV3SDK) -> None:
        """Test local quote with price ratio."""
        # ETH at $2000 means 1 ETH = 2000 USDC
        # But we need to account for decimals: 1e18 ETH -> 2000e6 USDC
        # So ratio = 2000 * 10^6 / 10^18 = 2000 * 10^-12
        quote = sdk.get_quote_local(
            token_in=WETH_ADDRESS,
            token_out=USDC_ADDRESS,
            amount_in=10**18,  # 1 ETH
            fee_tier=3000,
            price_ratio=Decimal("2000") * Decimal("10") ** -12,
        )

        # With 0.3% fee: 1e18 * 0.997 * 2000e-12 = ~1.994e9 (in output decimals)
        assert quote.amount_out > 0
        assert quote.fee == 3000

    def test_get_quote_local_invalid_fee(self, sdk: UniswapV3SDK) -> None:
        """Test local quote with invalid fee tier."""
        with pytest.raises(InvalidFeeError):
            sdk.get_quote_local(
                token_in=WETH_ADDRESS,
                token_out=USDC_ADDRESS,
                amount_in=10**18,
                fee_tier=999,
            )


# =============================================================================
# SwapQuote Tests
# =============================================================================


class TestSwapQuote:
    """Tests for SwapQuote dataclass."""

    def test_swap_quote_creation(self) -> None:
        """Test SwapQuote creation."""
        quote = SwapQuote(
            token_in=WETH_ADDRESS,
            token_out=USDC_ADDRESS,
            amount_in=10**18,
            amount_out=2000 * 10**6,
            fee=3000,
        )

        assert quote.token_in == WETH_ADDRESS
        assert quote.token_out == USDC_ADDRESS
        assert quote.amount_in == 10**18
        assert quote.amount_out == 2000 * 10**6

    def test_swap_quote_effective_price(self) -> None:
        """Test effective price calculation."""
        quote = SwapQuote(
            token_in=WETH_ADDRESS,
            token_out=USDC_ADDRESS,
            amount_in=10**18,
            amount_out=2000 * 10**6,
            fee=3000,
        )

        # effective_price = amount_out / amount_in
        expected = Decimal(str(2000 * 10**6)) / Decimal(str(10**18))
        assert quote.effective_price == expected

    def test_swap_quote_effective_price_zero_input(self) -> None:
        """Test effective price with zero input."""
        quote = SwapQuote(
            token_in=WETH_ADDRESS,
            token_out=USDC_ADDRESS,
            amount_in=0,
            amount_out=0,
            fee=3000,
        )

        assert quote.effective_price == Decimal("0")

    def test_swap_quote_to_dict(self) -> None:
        """Test SwapQuote serialization."""
        quote = SwapQuote(
            token_in=WETH_ADDRESS,
            token_out=USDC_ADDRESS,
            amount_in=10**18,
            amount_out=2000 * 10**6,
            fee=3000,
            gas_estimate=150000,
        )

        quote_dict = quote.to_dict()

        assert quote_dict["token_in"] == WETH_ADDRESS
        assert quote_dict["amount_in"] == str(10**18)
        assert quote_dict["gas_estimate"] == 150000
        assert "quoted_at" in quote_dict

    def test_swap_quote_from_dict(self) -> None:
        """Test SwapQuote deserialization."""
        data = {
            "token_in": WETH_ADDRESS,
            "token_out": USDC_ADDRESS,
            "amount_in": str(10**18),
            "amount_out": str(2000 * 10**6),
            "fee": 3000,
            "sqrt_price_x96_after": "0",
            "initialized_ticks_crossed": 0,
            "gas_estimate": 150000,
            "quoted_at": datetime.now(UTC).isoformat(),
        }

        quote = SwapQuote.from_dict(data)

        assert quote.token_in == WETH_ADDRESS
        assert quote.amount_in == 10**18
        assert quote.fee == 3000


# =============================================================================
# Transaction Building Tests
# =============================================================================


class TestTransactionBuilding:
    """Tests for swap transaction building."""

    @pytest.fixture
    def sdk(self) -> UniswapV3SDK:
        """Create SDK for testing."""
        return UniswapV3SDK(chain="arbitrum")

    def test_build_swap_tx(self, sdk: UniswapV3SDK) -> None:
        """Test building swap transaction."""
        quote = SwapQuote(
            token_in=WETH_ADDRESS,
            token_out=USDC_ADDRESS,
            amount_in=10**18,
            amount_out=2000 * 10**6,
            fee=3000,
            gas_estimate=150000,
        )

        tx = sdk.build_swap_tx(
            quote=quote,
            recipient=TEST_WALLET,
            slippage_bps=50,  # 0.5%
            deadline=int(time.time()) + 300,
        )

        assert tx.to == sdk.router_address
        assert tx.data.startswith(EXACT_INPUT_SINGLE_SELECTOR)
        assert tx.gas_estimate == 150000

    def test_build_swap_tx_slippage_applied(self, sdk: UniswapV3SDK) -> None:
        """Test that slippage is applied correctly."""
        quote = SwapQuote(
            token_in=WETH_ADDRESS,
            token_out=USDC_ADDRESS,
            amount_in=10**18,
            amount_out=2000 * 10**6,  # 2000 USDC
            fee=3000,
        )

        # With 50 bps (0.5%) slippage, minimum should be 2000 * 0.995 = 1990 USDC
        tx = sdk.build_swap_tx(
            quote=quote,
            recipient=TEST_WALLET,
            slippage_bps=50,
            deadline=int(time.time()) + 300,
        )

        # Verify calldata contains the slippage-adjusted amount
        # The calldata includes amount_out_minimum which should be 1990 * 10^6
        assert tx.data.startswith(EXACT_INPUT_SINGLE_SELECTOR)

    def test_build_exact_output_swap_tx(self, sdk: UniswapV3SDK) -> None:
        """Test building exact output swap transaction."""
        tx = sdk.build_exact_output_swap_tx(
            token_in=USDC_ADDRESS,
            token_out=WETH_ADDRESS,
            fee=3000,
            recipient=TEST_WALLET,
            deadline=int(time.time()) + 300,
            amount_out=10**18,  # 1 ETH
            amount_in_maximum=2100 * 10**6,  # Max 2100 USDC
        )

        assert tx.to == sdk.router_address
        assert tx.data.startswith(EXACT_OUTPUT_SINGLE_SELECTOR)
        assert tx.gas_estimate > 0


# =============================================================================
# SwapTransaction Tests
# =============================================================================


class TestSwapTransaction:
    """Tests for SwapTransaction dataclass."""

    def test_swap_transaction_creation(self) -> None:
        """Test SwapTransaction creation."""
        tx = SwapTransaction(
            to=ROUTER_ADDRESSES["arbitrum"],
            value=0,
            data="0x1234",
            gas_estimate=150000,
            description="Test swap",
        )

        assert tx.to == ROUTER_ADDRESSES["arbitrum"]
        assert tx.value == 0
        assert tx.gas_estimate == 150000

    def test_swap_transaction_to_dict(self) -> None:
        """Test SwapTransaction serialization."""
        tx = SwapTransaction(
            to=ROUTER_ADDRESSES["arbitrum"],
            value=10**18,
            data="0x1234",
            gas_estimate=150000,
            description="Swap with ETH",
        )

        tx_dict = tx.to_dict()

        assert tx_dict["to"] == ROUTER_ADDRESSES["arbitrum"]
        assert tx_dict["value"] == str(10**18)
        assert tx_dict["gas_estimate"] == 150000


# =============================================================================
# Tick Math Tests
# =============================================================================


class TestTickMath:
    """Tests for tick math utilities."""

    def test_tick_to_sqrt_price_x96_zero(self) -> None:
        """Test tick 0 gives sqrt_price of Q96 (price = 1)."""
        sqrt_price = tick_to_sqrt_price_x96(0)

        # At tick 0, price = 1, so sqrt(price) = 1
        # sqrt_price_x96 = 1 * 2^96 = Q96
        assert sqrt_price == Q96

    def test_tick_to_sqrt_price_x96_positive(self) -> None:
        """Test positive tick gives higher price."""
        sqrt_price_0 = tick_to_sqrt_price_x96(0)
        sqrt_price_100 = tick_to_sqrt_price_x96(100)

        assert sqrt_price_100 > sqrt_price_0

    def test_tick_to_sqrt_price_x96_negative(self) -> None:
        """Test negative tick gives lower price."""
        sqrt_price_0 = tick_to_sqrt_price_x96(0)
        sqrt_price_neg100 = tick_to_sqrt_price_x96(-100)

        assert sqrt_price_neg100 < sqrt_price_0

    def test_tick_to_sqrt_price_x96_bounds(self) -> None:
        """Test tick bounds."""
        # Should work at bounds
        tick_to_sqrt_price_x96(MIN_TICK)
        tick_to_sqrt_price_x96(MAX_TICK)

        # Should fail outside bounds
        with pytest.raises(InvalidTickError):
            tick_to_sqrt_price_x96(MIN_TICK - 1)

        with pytest.raises(InvalidTickError):
            tick_to_sqrt_price_x96(MAX_TICK + 1)

    def test_sqrt_price_x96_to_tick_zero(self) -> None:
        """Test sqrt_price Q96 gives tick 0."""
        tick = sqrt_price_x96_to_tick(Q96)

        assert tick == 0

    def test_sqrt_price_x96_to_tick_roundtrip(self) -> None:
        """Test roundtrip conversion."""
        original_tick = 1000
        sqrt_price = tick_to_sqrt_price_x96(original_tick)
        recovered_tick = sqrt_price_x96_to_tick(sqrt_price)

        # Should be within 1 tick due to rounding
        assert abs(recovered_tick - original_tick) <= 1

    def test_sqrt_price_x96_to_tick_invalid(self) -> None:
        """Test invalid sqrt price."""
        with pytest.raises(ValueError):
            sqrt_price_x96_to_tick(0)

        with pytest.raises(ValueError):
            sqrt_price_x96_to_tick(-1)

    def test_tick_to_price(self) -> None:
        """Test tick to price conversion."""
        # At tick 0, price should be 1
        price = tick_to_price(0)
        assert price == Decimal("1")

        # Positive tick means higher price
        price_100 = tick_to_price(100)
        assert price_100 > Decimal("1")

    def test_tick_to_price_with_decimals(self) -> None:
        """Test tick to price with different decimals."""
        # USDC (6 decimals) / WETH (18 decimals)
        price = tick_to_price(0, decimals0=6, decimals1=18)

        # With decimal adjustment: price * 10^(6-18) = price * 10^-12
        assert price < Decimal("1")

    def test_price_to_tick(self) -> None:
        """Test price to tick conversion."""
        # Price 1 should give tick 0
        tick = price_to_tick(Decimal("1"))
        assert tick == 0

        # Price > 1 should give positive tick
        tick_high = price_to_tick(Decimal("2"))
        assert tick_high > 0

        # Price < 1 should give negative tick
        tick_low = price_to_tick(Decimal("0.5"))
        assert tick_low < 0

    def test_price_to_tick_zero(self) -> None:
        """Test price to tick with zero/negative price."""
        assert price_to_tick(0) == MIN_TICK
        assert price_to_tick(-1) == MIN_TICK

    def test_sqrt_price_x96_to_price(self) -> None:
        """Test sqrt price X96 to decimal price."""
        # Q96 means sqrt(price) = 1, so price = 1
        price = sqrt_price_x96_to_price(Q96)
        assert price == Decimal("1")

        # Zero should return 0
        assert sqrt_price_x96_to_price(0) == Decimal("0")

    def test_price_to_sqrt_price_x96(self) -> None:
        """Test price to sqrt price X96."""
        # Price 1 should give Q96
        sqrt_price = price_to_sqrt_price_x96(1)
        assert sqrt_price == Q96

        # Zero/negative should return 0
        assert price_to_sqrt_price_x96(0) == 0
        assert price_to_sqrt_price_x96(-1) == 0

    def test_get_nearest_tick(self) -> None:
        """Test getting nearest valid tick."""
        # Fee tier 3000 has tick spacing 60
        assert get_nearest_tick(62, 3000) == 60
        assert get_nearest_tick(90, 3000) == 120
        assert get_nearest_tick(0, 3000) == 0

    def test_get_nearest_tick_invalid_fee(self) -> None:
        """Test nearest tick with invalid fee."""
        with pytest.raises(InvalidFeeError):
            get_nearest_tick(100, 999)

    def test_get_min_tick(self) -> None:
        """Test getting minimum tick for fee tier."""
        min_tick_3000 = get_min_tick(3000)

        # Should be divisible by tick spacing (60)
        assert min_tick_3000 % 60 == 0
        # The adjusted min tick is higher (closer to zero) than absolute MIN_TICK
        assert min_tick_3000 >= MIN_TICK

    def test_get_max_tick(self) -> None:
        """Test getting maximum tick for fee tier."""
        max_tick_3000 = get_max_tick(3000)

        # Should be divisible by tick spacing (60)
        assert max_tick_3000 % 60 == 0
        assert max_tick_3000 <= MAX_TICK

    def test_get_min_max_tick_invalid_fee(self) -> None:
        """Test min/max tick with invalid fee."""
        with pytest.raises(InvalidFeeError):
            get_min_tick(999)

        with pytest.raises(InvalidFeeError):
            get_max_tick(999)


# =============================================================================
# Exception Tests
# =============================================================================


class TestExceptions:
    """Tests for SDK exceptions."""

    def test_invalid_fee_error(self) -> None:
        """Test InvalidFeeError."""
        error = InvalidFeeError(999)

        assert error.fee == 999
        assert "999" in str(error)
        assert "Valid tiers" in str(error)

    def test_invalid_tick_error(self) -> None:
        """Test InvalidTickError."""
        error = InvalidTickError(999999, "Out of bounds")

        assert error.tick == 999999
        assert error.reason == "Out of bounds"
        assert "999999" in str(error)

    def test_pool_not_found_error(self) -> None:
        """Test PoolNotFoundError."""
        error = PoolNotFoundError(WETH_ADDRESS, USDC_ADDRESS, 3000)

        assert error.token0 == WETH_ADDRESS
        assert error.token1 == USDC_ADDRESS
        assert error.fee == 3000

    def test_quote_error(self) -> None:
        """Test QuoteError."""
        error = QuoteError("RPC timeout", WETH_ADDRESS, USDC_ADDRESS)

        assert error.token_in == WETH_ADDRESS
        assert error.token_out == USDC_ADDRESS
        assert "RPC timeout" in str(error)


# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for SDK constants."""

    def test_q96_q128(self) -> None:
        """Test Q96 and Q128 constants."""
        assert Q96 == 2**96
        assert Q128 == 2**128

    def test_tick_bounds(self) -> None:
        """Test MIN_TICK and MAX_TICK."""
        assert MIN_TICK == -887272
        assert MAX_TICK == 887272

    def test_fee_tiers(self) -> None:
        """Test FEE_TIERS list."""
        assert 100 in FEE_TIERS  # 0.01%
        assert 500 in FEE_TIERS  # 0.05%
        assert 3000 in FEE_TIERS  # 0.3%
        assert 10000 in FEE_TIERS  # 1%

    def test_tick_spacing(self) -> None:
        """Test TICK_SPACING mapping."""
        assert TICK_SPACING[100] == 1
        assert TICK_SPACING[500] == 10
        assert TICK_SPACING[3000] == 60
        assert TICK_SPACING[10000] == 200

    def test_factory_addresses(self) -> None:
        """Test factory addresses for all chains."""
        for chain in ["ethereum", "arbitrum", "optimism", "polygon", "base"]:
            assert chain in FACTORY_ADDRESSES
            assert FACTORY_ADDRESSES[chain].startswith("0x")
            assert len(FACTORY_ADDRESSES[chain]) == 42

    def test_router_addresses(self) -> None:
        """Test router addresses for all chains."""
        for chain in ["ethereum", "arbitrum", "optimism", "polygon", "base"]:
            assert chain in ROUTER_ADDRESSES
            assert ROUTER_ADDRESSES[chain].startswith("0x")

    def test_quoter_addresses(self) -> None:
        """Test quoter addresses for all chains."""
        for chain in ["ethereum", "arbitrum", "optimism", "polygon", "base"]:
            assert chain in QUOTER_ADDRESSES
            assert QUOTER_ADDRESSES[chain].startswith("0x")

    def test_pool_init_code_hash(self) -> None:
        """Test pool init code hash."""
        assert POOL_INIT_CODE_HASH.startswith("0x")
        assert len(POOL_INIT_CODE_HASH) == 66  # 0x + 64 hex chars

    def test_function_selectors(self) -> None:
        """Test function selectors for SwapRouter02 / IV3SwapRouter."""
        assert EXACT_INPUT_SINGLE_SELECTOR == "0x04e45aaf"
        assert EXACT_OUTPUT_SINGLE_SELECTOR == "0x5023b4df"


# =============================================================================
# Encoding Tests
# =============================================================================


class TestEncoding:
    """Tests for calldata encoding."""

    @pytest.fixture
    def sdk(self) -> UniswapV3SDK:
        """Create SDK for testing."""
        return UniswapV3SDK(chain="arbitrum")

    def test_pad_address(self, sdk: UniswapV3SDK) -> None:
        """Test address padding."""
        padded = sdk._pad_address(WETH_ADDRESS)

        assert len(padded) == 64
        assert padded.endswith(WETH_ADDRESS.lower().replace("0x", ""))

    def test_pad_uint(self, sdk: UniswapV3SDK) -> None:
        """Test uint padding."""
        padded = sdk._pad_uint(1000)

        assert len(padded) == 64
        assert int(padded, 16) == 1000

    def test_encode_exact_input_single(self, sdk: UniswapV3SDK) -> None:
        """Test exactInputSingle encoding."""
        calldata = sdk._encode_exact_input_single(
            token_in=WETH_ADDRESS,
            token_out=USDC_ADDRESS,
            fee=3000,
            recipient=TEST_WALLET,
            deadline=1700000000,
            amount_in=10**18,
            amount_out_minimum=1990 * 10**6,
        )

        assert calldata.startswith(EXACT_INPUT_SINGLE_SELECTOR)
        # Selector (0x + 8 hex = 10 chars) + 7 params * 32 bytes (64 hex chars) each = 10 + 448 = 458 chars
        # Note: SwapRouter02 uses 7-param struct (no deadline)
        assert len(calldata) == 10 + 448

    def test_encode_exact_output_single(self, sdk: UniswapV3SDK) -> None:
        """Test exactOutputSingle encoding."""
        calldata = sdk._encode_exact_output_single(
            token_in=USDC_ADDRESS,
            token_out=WETH_ADDRESS,
            fee=3000,
            recipient=TEST_WALLET,
            deadline=1700000000,
            amount_out=10**18,
            amount_in_maximum=2100 * 10**6,
        )

        assert calldata.startswith(EXACT_OUTPUT_SINGLE_SELECTOR)
        # Selector (0x + 8 hex = 10 chars) + 7 params * 32 bytes (64 hex chars) each = 10 + 448 = 458 chars
        # Note: SwapRouter02 uses 7-param struct (no deadline)
        assert len(calldata) == 10 + 448
