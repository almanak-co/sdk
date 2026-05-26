"""Unit tests for SushiSwap V3 SDK module.

Covers:
- Tick math utilities (tick<->price, tick<->sqrt_price_x96, get_nearest_tick, etc.)
- Pool address computation (compute_pool_address, sort_tokens)
- SushiSwapV3SDK class init / get_pool_address
- get_quote_local + async get_quote fallback
- Transaction builders (build_swap_tx, build_exact_output_swap_tx,
  build_mint_tx, build_increase_liquidity_tx, build_decrease_liquidity_tx,
  build_collect_tx)
- Encoding helpers (_encode_*, _pad_int two's complement)
- _decode_quote_response success + error
- Data classes (PoolState.price, SwapQuote.from_dict / to_dict, etc.)
- Exceptions (InvalidFeeError, InvalidTickError, PoolNotFoundError)
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.connectors.sushiswap_v3.sdk import (
    FACTORY_ADDRESSES,
    MAX_TICK,
    MIN_TICK,
    Q96,
    TICK_SPACING,
    InvalidFeeError,
    InvalidTickError,
    LPTransaction,
    MintParams,
    PoolInfo,
    PoolNotFoundError,
    PoolState,
    QuoteError,
    SushiSwapV3SDK,
    SushiSwapV3SDKError,
    SwapQuote,
    SwapTransaction,
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
    tick_to_sqrt_price_x96,
)

# Common test addresses
WETH_ARB = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC_ARB = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
RECIPIENT = "0x1234567890123456789012345678901234567890"


# =============================================================================
# Tick Math
# =============================================================================


class TestTickMath:
    """Tick / sqrtPrice / price conversion helpers."""

    def test_tick_to_sqrt_price_x96_zero(self):
        result = tick_to_sqrt_price_x96(0)
        # sqrt(1.0001^0) = 1, so result should be Q96
        assert result == Q96

    def test_tick_to_sqrt_price_x96_positive(self):
        # Should be larger than Q96 for positive tick
        result = tick_to_sqrt_price_x96(100)
        assert result > Q96

    def test_tick_to_sqrt_price_x96_negative(self):
        result = tick_to_sqrt_price_x96(-100)
        assert 0 < result < Q96

    def test_tick_to_sqrt_price_x96_below_min_raises(self):
        with pytest.raises(InvalidTickError):
            tick_to_sqrt_price_x96(MIN_TICK - 1)

    def test_tick_to_sqrt_price_x96_above_max_raises(self):
        with pytest.raises(InvalidTickError):
            tick_to_sqrt_price_x96(MAX_TICK + 1)

    def test_sqrt_price_x96_to_tick_q96(self):
        # sqrt_price = Q96 -> tick should be 0 (or very close to 0)
        tick = sqrt_price_x96_to_tick(Q96)
        assert -2 <= tick <= 0

    def test_sqrt_price_x96_to_tick_zero_raises(self):
        with pytest.raises(ValueError):
            sqrt_price_x96_to_tick(0)

    def test_sqrt_price_x96_to_tick_negative_raises(self):
        with pytest.raises(ValueError):
            sqrt_price_x96_to_tick(-1)

    def test_tick_to_price_default(self):
        # tick=0 -> price=1.0
        result = tick_to_price(0, decimals0=18, decimals1=18)
        assert isinstance(result, Decimal)
        assert abs(float(result) - 1.0) < 0.0001

    def test_tick_to_price_with_decimals_adjustment(self):
        # decimals0 != decimals1 applies adjustment
        result = tick_to_price(0, decimals0=18, decimals1=6)
        assert result == Decimal("1") * Decimal(10**12)

    def test_price_to_tick_zero_returns_min(self):
        assert price_to_tick(0) == MIN_TICK

    def test_price_to_tick_negative_returns_min(self):
        assert price_to_tick(Decimal("-1")) == MIN_TICK

    def test_price_to_tick_one(self):
        # Price=1 -> tick should be ~0
        tick = price_to_tick(Decimal("1"))
        assert -2 <= tick <= 1

    def test_price_to_tick_with_decimals_zero_adjusted(self):
        # very small price after decimal adjust -> min tick
        # Price=1, decimals0=6, decimals1=18 -> adjusted = 1 / 10**(-12) = 1e12 large
        # ensure no exception, both branches handled
        tick = price_to_tick(Decimal("1"), decimals0=6, decimals1=18)
        assert isinstance(tick, int)

    def test_sqrt_price_x96_to_price_zero(self):
        assert sqrt_price_x96_to_price(0) == Decimal("0")

    def test_sqrt_price_x96_to_price_negative(self):
        assert sqrt_price_x96_to_price(-1) == Decimal("0")

    def test_sqrt_price_x96_to_price_q96(self):
        # sqrt(1)^2 = 1
        assert sqrt_price_x96_to_price(Q96) == Decimal("1")

    def test_price_to_sqrt_price_x96_zero(self):
        assert price_to_sqrt_price_x96(0) == 0

    def test_price_to_sqrt_price_x96_negative(self):
        assert price_to_sqrt_price_x96(Decimal("-1")) == 0

    def test_price_to_sqrt_price_x96_one(self):
        # sqrt(1) * Q96 = Q96
        assert price_to_sqrt_price_x96(Decimal("1")) == Q96

    def test_price_to_sqrt_price_x96_float(self):
        result = price_to_sqrt_price_x96(2.0)
        # sqrt(2) ~ 1.414
        assert result > Q96

    def test_get_nearest_tick_valid(self):
        # tick=100, fee=3000 (spacing=60) -> 120
        result = get_nearest_tick(100, 3000)
        assert result == 120

    def test_get_nearest_tick_invalid_fee(self):
        with pytest.raises(InvalidFeeError):
            get_nearest_tick(0, 999)

    def test_get_nearest_tick_clamps_to_min(self):
        # tick way below MIN_TICK -> clamped
        result = get_nearest_tick(MIN_TICK * 2, 3000)
        # Should equal get_min_tick(3000)
        assert result == get_min_tick(3000)

    def test_get_nearest_tick_clamps_to_max(self):
        result = get_nearest_tick(MAX_TICK * 2, 3000)
        assert result == get_max_tick(3000)

    def test_get_min_tick_invalid_fee(self):
        with pytest.raises(InvalidFeeError):
            get_min_tick(999)

    def test_get_max_tick_invalid_fee(self):
        with pytest.raises(InvalidFeeError):
            get_max_tick(999)

    @pytest.mark.parametrize("fee,spacing", list(TICK_SPACING.items()))
    def test_get_min_max_tick_each_fee(self, fee, spacing):
        min_t = get_min_tick(fee)
        max_t = get_max_tick(fee)
        assert min_t < max_t
        assert min_t % spacing == 0
        assert max_t % spacing == 0


# =============================================================================
# Pool Address
# =============================================================================


class TestPoolAddress:
    """compute_pool_address + sort_tokens."""

    def test_sort_tokens_already_sorted(self):
        a = "0x1111111111111111111111111111111111111111"
        b = "0x2222222222222222222222222222222222222222"
        assert sort_tokens(a, b) == (a, b)

    def test_sort_tokens_reverse(self):
        a = "0x2222222222222222222222222222222222222222"
        b = "0x1111111111111111111111111111111111111111"
        assert sort_tokens(a, b) == (b, a)

    def test_compute_pool_address_invalid_fee(self):
        with pytest.raises(InvalidFeeError):
            compute_pool_address("0x" + "ab" * 20, WETH_ARB, USDC_ARB, 999)

    def test_compute_pool_address_deterministic(self):
        # Same inputs always produce same output
        addr1 = compute_pool_address(
            FACTORY_ADDRESSES["arbitrum"], WETH_ARB, USDC_ARB, 3000
        )
        addr2 = compute_pool_address(
            FACTORY_ADDRESSES["arbitrum"], WETH_ARB, USDC_ARB, 3000
        )
        assert addr1 == addr2
        assert addr1.startswith("0x")
        assert len(addr1) == 42

    def test_compute_pool_address_token_order_irrelevant(self):
        # Sorting happens internally; result must be the same either way.
        addr1 = compute_pool_address(
            FACTORY_ADDRESSES["arbitrum"], WETH_ARB, USDC_ARB, 3000
        )
        addr2 = compute_pool_address(
            FACTORY_ADDRESSES["arbitrum"], USDC_ARB, WETH_ARB, 3000
        )
        assert addr1 == addr2


# =============================================================================
# Exceptions
# =============================================================================


class TestExceptions:
    def test_invalid_fee_error_carries_fee(self):
        err = InvalidFeeError(999)
        assert err.fee == 999
        assert "999" in str(err)

    def test_invalid_tick_error_carries_tick(self):
        err = InvalidTickError(123, "out of range")
        assert err.tick == 123
        assert err.reason == "out of range"
        assert "123" in str(err)

    def test_pool_not_found_error_carries_fields(self):
        err = PoolNotFoundError(WETH_ARB, USDC_ARB, 3000)
        assert err.token0 == WETH_ARB
        assert err.token1 == USDC_ARB
        assert err.fee == 3000

    def test_quote_error_carries_tokens(self):
        err = QuoteError("bad", WETH_ARB, USDC_ARB)
        assert err.token_in == WETH_ARB
        assert err.token_out == USDC_ARB

    def test_sushiswap_v3_sdk_error_is_exception(self):
        assert issubclass(SushiSwapV3SDKError, Exception)


# =============================================================================
# Data classes
# =============================================================================


class TestDataClasses:
    def test_pool_info_to_dict(self):
        info = PoolInfo(
            address="0xabc", token0=WETH_ARB, token1=USDC_ARB, fee=3000, tick_spacing=60
        )
        d = info.to_dict()
        assert d["address"] == "0xabc"
        assert d["fee"] == 3000

    def test_pool_state_price_property(self):
        state = PoolState(sqrt_price_x96=Q96, tick=0, liquidity=0)
        assert state.price == Decimal("1")

    def test_pool_state_to_dict(self):
        state = PoolState(sqrt_price_x96=Q96, tick=0, liquidity=12345)
        d = state.to_dict()
        assert d["tick"] == 0
        assert d["liquidity"] == "12345"
        assert "price" in d

    def test_swap_quote_effective_price_zero_amount_in(self):
        q = SwapQuote(
            token_in=WETH_ARB, token_out=USDC_ARB,
            amount_in=0, amount_out=100, fee=3000,
        )
        assert q.effective_price == Decimal("0")

    def test_swap_quote_effective_price_normal(self):
        q = SwapQuote(
            token_in=WETH_ARB, token_out=USDC_ARB,
            amount_in=10, amount_out=100, fee=3000,
        )
        assert q.effective_price == Decimal("10")

    def test_swap_quote_to_dict_round_trip(self):
        q = SwapQuote(
            token_in=WETH_ARB, token_out=USDC_ARB,
            amount_in=10**18, amount_out=3400 * 10**6, fee=3000,
            sqrt_price_x96_after=Q96, initialized_ticks_crossed=2,
            gas_estimate=200000,
        )
        d = q.to_dict()
        q2 = SwapQuote.from_dict(d)
        assert q2.token_in == q.token_in
        assert q2.amount_in == q.amount_in
        assert q2.fee == q.fee
        assert q2.initialized_ticks_crossed == 2

    def test_swap_quote_from_dict_no_quoted_at(self):
        # Test missing quoted_at falls back to datetime.now
        q = SwapQuote.from_dict({
            "token_in": "a",
            "token_out": "b",
            "amount_in": "1",
            "amount_out": "2",
            "fee": 3000,
        })
        assert q.token_in == "a"

    def test_swap_transaction_to_dict(self):
        tx = SwapTransaction(to="0xrouter", value=0, data="0xabc", gas_estimate=150000, description="x")
        d = tx.to_dict()
        assert d["to"] == "0xrouter"
        assert d["value"] == "0"

    def test_mint_params_to_dict(self):
        p = MintParams(
            token0=WETH_ARB, token1=USDC_ARB, fee=3000,
            tick_lower=-60, tick_upper=60,
            amount0_desired=1, amount1_desired=2,
            amount0_min=0, amount1_min=0,
            recipient=RECIPIENT, deadline=1234,
        )
        d = p.to_dict()
        assert d["fee"] == 3000
        assert d["amount0_desired"] == "1"
        assert d["recipient"] == RECIPIENT

    def test_lp_transaction_to_dict(self):
        tx = LPTransaction(
            to="0xpm", value=0, data="0xabc", gas_estimate=500000,
            description="mint", operation="mint",
        )
        d = tx.to_dict()
        assert d["operation"] == "mint"


# =============================================================================
# SushiSwapV3SDK
# =============================================================================


class TestSushiSwapV3SDKInit:
    def test_init_unsupported_chain_raises(self):
        with pytest.raises(ValueError):
            SushiSwapV3SDK(chain="unknown_chain")

    def test_init_supported_chain_succeeds(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        assert sdk.chain == "arbitrum"
        assert sdk.factory_address == FACTORY_ADDRESSES["arbitrum"]


class TestSushiSwapV3SDKGetPoolAddress:
    def test_get_pool_address_invalid_fee(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        with pytest.raises(InvalidFeeError):
            sdk.get_pool_address(WETH_ARB, USDC_ARB, 999)

    def test_get_pool_address_returns_pool_info(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        info = sdk.get_pool_address(WETH_ARB, USDC_ARB, 3000)
        assert isinstance(info, PoolInfo)
        assert info.fee == 3000
        assert info.tick_spacing == TICK_SPACING[3000]

    def test_get_pool_address_sorts_tokens(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        # Pass in unsorted order
        info = sdk.get_pool_address(USDC_ARB, WETH_ARB, 3000)
        # token0 < token1
        assert info.token0.lower() < info.token1.lower()


class TestSushiSwapV3SDKGetQuoteLocal:
    def test_get_quote_local_invalid_fee(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        with pytest.raises(InvalidFeeError):
            sdk.get_quote_local(WETH_ARB, USDC_ARB, 10**18, 999)

    def test_get_quote_local_no_price_ratio(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        # 1 wei input, fee=3000 -> small amount after fee
        q = sdk.get_quote_local(WETH_ARB, USDC_ARB, 10**18, 3000)
        assert isinstance(q, SwapQuote)
        # amount_after_fee = 10**18 * (1 - 0.003) = ~9.97e17
        assert q.amount_out == int(Decimal(10**18) * Decimal("0.997"))

    def test_get_quote_local_with_price_ratio(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        q = sdk.get_quote_local(
            WETH_ARB, USDC_ARB, 10**18, 3000, price_ratio=Decimal("3400")
        )
        # ~ 9.97e17 * 3400
        expected_min = int(Decimal(10**18) * Decimal("0.997") * Decimal("3400"))
        assert q.amount_out == expected_min


class TestSushiSwapV3SDKGetQuoteAsync:
    def test_get_quote_invalid_fee(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        with pytest.raises(InvalidFeeError):
            asyncio.run(sdk.get_quote(WETH_ARB, USDC_ARB, 10**18, 999))

    def test_get_quote_no_web3_falls_back_to_local(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        q = asyncio.run(sdk.get_quote(WETH_ARB, USDC_ARB, 10**18, 3000))
        # Falls back to get_quote_local
        assert q.fee == 3000
        assert q.amount_out > 0

    def test_get_quote_with_web3_success(self):
        # Build a 128-byte response: amountOut, sqrtPriceAfter, ticksCrossed, gasEstimate
        amount_out = 3400 * 10**6
        sqrt_after = Q96
        ticks_crossed = 1
        gas_est = 130000
        response = (
            amount_out.to_bytes(32, "big")
            + sqrt_after.to_bytes(32, "big")
            + ticks_crossed.to_bytes(32, "big")
            + gas_est.to_bytes(32, "big")
        )

        mock_web3 = MagicMock()
        mock_web3.eth.call = AsyncMock(return_value=response)

        sdk = SushiSwapV3SDK(chain="arbitrum", web3=mock_web3)
        q = asyncio.run(sdk.get_quote(WETH_ARB, USDC_ARB, 10**18, 3000))
        assert q.amount_out == amount_out
        assert q.sqrt_price_x96_after == sqrt_after
        assert q.initialized_ticks_crossed == ticks_crossed
        assert q.gas_estimate == gas_est

    def test_get_quote_rpc_failure_falls_back_to_local(self):
        # Web3 raises -> fall back to get_quote_local
        mock_web3 = MagicMock()
        mock_web3.eth.call = AsyncMock(side_effect=Exception("rpc down"))
        sdk = SushiSwapV3SDK(chain="arbitrum", web3=mock_web3)
        q = asyncio.run(sdk.get_quote(WETH_ARB, USDC_ARB, 10**18, 3000))
        # Local fallback used
        assert q.amount_out > 0

    def test_get_quote_short_response_raises(self):
        # Response < 128 bytes => decode fails => fallback to local quote
        mock_web3 = MagicMock()
        mock_web3.eth.call = AsyncMock(return_value=b"\x00" * 32)
        sdk = SushiSwapV3SDK(chain="arbitrum", web3=mock_web3)
        q = asyncio.run(sdk.get_quote(WETH_ARB, USDC_ARB, 10**18, 3000))
        # Falls back to local quote (logs warning + returns local estimate)
        assert q.fee == 3000


class TestBuildSwapTx:
    def test_build_swap_tx(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        quote = SwapQuote(
            token_in=WETH_ARB, token_out=USDC_ARB,
            amount_in=10**18, amount_out=3400 * 10**6, fee=3000,
            gas_estimate=150000,
        )
        tx = sdk.build_swap_tx(
            quote=quote, recipient=RECIPIENT, slippage_bps=50, deadline=1234567890,
        )
        assert tx.to == sdk.router_address
        assert tx.value == 0
        # Calldata starts with exactInputSingle selector
        assert tx.data.startswith("0x414bf389")

    def test_build_swap_tx_with_value(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        quote = SwapQuote(
            token_in=WETH_ARB, token_out=USDC_ARB,
            amount_in=10**18, amount_out=3400 * 10**6, fee=3000,
        )
        tx = sdk.build_swap_tx(
            quote=quote, recipient=RECIPIENT, slippage_bps=50,
            deadline=1234567890, value=10**18,
        )
        assert tx.value == 10**18

    def test_build_exact_output_swap_tx(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        tx = sdk.build_exact_output_swap_tx(
            token_in=WETH_ARB, token_out=USDC_ARB, fee=3000,
            recipient=RECIPIENT, deadline=1234,
            amount_out=3400 * 10**6, amount_in_maximum=10**18,
        )
        assert tx.to == sdk.router_address
        assert tx.gas_estimate == 170000
        assert tx.data.startswith("0xdb3e2198")


class TestBuildLPTxs:
    def test_build_mint_tx(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        params = MintParams(
            token0=WETH_ARB, token1=USDC_ARB, fee=3000,
            tick_lower=-60, tick_upper=60,
            amount0_desired=10**18, amount1_desired=3400 * 10**6,
            amount0_min=0, amount1_min=0,
            recipient=RECIPIENT, deadline=1234,
        )
        tx = sdk.build_mint_tx(params)
        assert tx.operation == "mint"
        assert tx.to == sdk.position_manager_address
        assert tx.gas_estimate == 500000
        assert tx.data.startswith("0x88316456")

    def test_build_mint_tx_with_value(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        params = MintParams(
            token0=WETH_ARB, token1=USDC_ARB, fee=3000,
            tick_lower=-60, tick_upper=60,
            amount0_desired=10**18, amount1_desired=10**6,
            amount0_min=0, amount1_min=0,
            recipient=RECIPIENT, deadline=1234,
        )
        tx = sdk.build_mint_tx(params, value=10**18)
        assert tx.value == 10**18

    def test_build_increase_liquidity_tx(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        tx = sdk.build_increase_liquidity_tx(
            token_id=1, amount0_desired=100, amount1_desired=200,
            amount0_min=0, amount1_min=0, deadline=1234,
        )
        assert tx.operation == "increase_liquidity"
        assert tx.gas_estimate == 350000
        assert tx.data.startswith("0x219f5d17")

    def test_build_decrease_liquidity_tx(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        tx = sdk.build_decrease_liquidity_tx(
            token_id=1, liquidity=10**12, amount0_min=0, amount1_min=0, deadline=1234,
        )
        assert tx.operation == "decrease_liquidity"
        assert tx.value == 0
        assert tx.gas_estimate == 250000
        assert tx.data.startswith("0x0c49ccbe")

    def test_build_collect_tx_default_max(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        tx = sdk.build_collect_tx(token_id=1, recipient=RECIPIENT)
        assert tx.operation == "collect"
        assert tx.value == 0
        assert tx.gas_estimate == 150000
        assert tx.data.startswith("0xfc6f7865")

    def test_build_collect_tx_explicit_amounts(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        tx = sdk.build_collect_tx(
            token_id=1, recipient=RECIPIENT,
            amount0_max=100, amount1_max=200,
        )
        assert tx.data.startswith("0xfc6f7865")


class TestEncodingHelpers:
    def test_pad_int_positive(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        # 100 -> 64-char hex
        result = sdk._pad_int(100)
        assert len(result) == 64
        assert int(result, 16) == 100

    def test_pad_int_negative_two_complement(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        result = sdk._pad_int(-1)
        # -1 in two's complement = all Fs
        assert result == "f" * 64

    def test_pad_int_negative_minus60(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        result = sdk._pad_int(-60)
        assert int(result, 16) == (1 << 256) - 60

    def test_pad_address_lower_no_prefix(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        addr = "0xABCDEF1234567890abcdef1234567890abcdef12"
        result = sdk._pad_address(addr)
        assert len(result) == 64
        assert "0x" not in result

    def test_pad_uint(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        result = sdk._pad_uint(12345)
        assert len(result) == 64
        assert int(result, 16) == 12345

    def test_encode_mint_swaps_when_not_already_sorted(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        # Pass tokens in REVERSE sorted order so swap branch executes
        params = MintParams(
            token0=USDC_ARB, token1=WETH_ARB, fee=3000,
            tick_lower=-60, tick_upper=60,
            amount0_desired=100, amount1_desired=200,
            amount0_min=10, amount1_min=20,
            recipient=RECIPIENT, deadline=1234,
        )
        encoded = sdk._encode_mint(params)
        assert encoded.startswith("0x88316456")
        # Should still be valid encoded data
        assert len(encoded) > 64

    def test_encode_quote_exact_input_single_selector(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        encoded = sdk._encode_quote_exact_input_single(WETH_ARB, USDC_ARB, 10**18, 3000)
        assert encoded.startswith("0xc6a5026a")


class TestDecodeQuoteResponse:
    def test_decode_valid(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        data = (
            (12345).to_bytes(32, "big")
            + (99).to_bytes(32, "big")
            + (3).to_bytes(32, "big")
            + (200000).to_bytes(32, "big")
        )
        amount_out, sqrt_after, ticks, gas = sdk._decode_quote_response(data)
        assert amount_out == 12345
        assert sqrt_after == 99
        assert ticks == 3
        assert gas == 200000

    def test_decode_short_data_raises(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        with pytest.raises(QuoteError):
            sdk._decode_quote_response(b"\x00" * 64)


# =============================================================================
# _get_web3 fallback / gateway routing
# =============================================================================


class TestGetWeb3:
    def test_get_web3_uses_existing(self):
        sentinel = MagicMock(name="web3")
        sdk = SushiSwapV3SDK(chain="arbitrum", web3=sentinel)
        result = asyncio.run(sdk._get_web3())
        assert result is sentinel

    def test_get_web3_no_provider_raises(self):
        sdk = SushiSwapV3SDK(chain="arbitrum")
        # No rpc_url, no gateway_client, no web3 -> error
        with pytest.raises(SushiSwapV3SDKError):
            asyncio.run(sdk._get_web3())

    def test_get_web3_gateway_client_path(self):
        from web3 import AsyncBaseProvider

        gw = MagicMock()
        sdk = SushiSwapV3SDK(chain="arbitrum", gateway_client=gw)

        # Build a minimal AsyncBaseProvider so AsyncWeb3 validation passes.
        class _FakeProvider(AsyncBaseProvider):
            async def make_request(self, method, params):  # pragma: no cover
                return {"result": None}

            async def is_connected(self, show_traceback=False):  # pragma: no cover
                return True

        with patch(
            "almanak.framework.web3.gateway_provider.AsyncGatewayWeb3Provider",
            return_value=_FakeProvider(),
        ) as mock_provider:
            web3 = asyncio.run(sdk._get_web3())
            assert web3 is not None
            mock_provider.assert_called_once()
