"""Extra coverage tests for ``almanak.connectors.uniswap_v3.sdk``.

Targets uncovered branches in the SDK: dataclass round-trips, async quote
fallback, gateway-vs-rpc-vs-web3 ``_get_web3`` resolution, decode-quote
error path, and the missing-source error path. The in-package
``test_sdk.py`` covers the deterministic helpers; these tests exercise the
ones that require either async dispatch or a mocked Web3.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.connectors.uniswap_v3.sdk import (
    EXACT_OUTPUT_SINGLE_SELECTOR,
    QUOTE_EXACT_INPUT_SINGLE_SELECTOR,
    Q96,
    InvalidFeeError,
    InvalidTickError,
    PoolInfo,
    PoolNotFoundError,
    PoolState,
    QuoteError,
    SwapQuote,
    SwapTransaction,
    UniswapV3SDK,
    UniswapV3SDKError,
    compute_pool_address,
    sqrt_price_x96_to_price,
    sqrt_price_x96_to_tick,
)

WETH_ADDR = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
USDC_ADDR = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
WALLET = "0x" + "11" * 20
ARBITRUM_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"


# ---------------------------------------------------------------------------
# SwapTransaction.to_dict round trip
# ---------------------------------------------------------------------------


class TestSwapTransactionDict:
    def test_to_dict_includes_all_fields(self) -> None:
        tx = SwapTransaction(
            to="0xabc",
            value=10**17,
            data="0xdead",
            gas_estimate=200_000,
            description="Swap test",
        )
        d = tx.to_dict()
        assert d["to"] == "0xabc"
        assert d["value"] == str(10**17)
        assert d["data"] == "0xdead"
        assert d["gas_estimate"] == 200_000
        assert d["description"] == "Swap test"


# ---------------------------------------------------------------------------
# PoolInfo / PoolState extra coverage
# ---------------------------------------------------------------------------


class TestPoolStateZero:
    def test_zero_sqrt_price_yields_zero(self) -> None:
        state = PoolState(sqrt_price_x96=0, tick=0, liquidity=0)
        # sqrt_price_x96_to_price returns Decimal('0') when input is 0
        assert state.price == Decimal("0")

    def test_to_dict_serialises_all_fields(self) -> None:
        state = PoolState(
            sqrt_price_x96=Q96,
            tick=42,
            liquidity=1234,
            fee_growth_global_0=5,
            fee_growth_global_1=7,
        )
        out = state.to_dict()
        assert out["tick"] == 42
        assert out["liquidity"] == "1234"
        assert out["fee_growth_global_0"] == "5"
        assert out["fee_growth_global_1"] == "7"
        assert "price" in out


class TestPoolInfoToDict:
    def test_pool_info_dict(self) -> None:
        pool = PoolInfo(address="0xabc", token0=WETH_ADDR, token1=USDC_ADDR, fee=500, tick_spacing=10)
        d = pool.to_dict()
        assert d == {
            "address": "0xabc",
            "token0": WETH_ADDR,
            "token1": USDC_ADDR,
            "fee": 500,
            "tick_spacing": 10,
        }


# ---------------------------------------------------------------------------
# Tick / sqrt-price math edge cases
# ---------------------------------------------------------------------------


class TestSqrtPriceTickEdges:
    def test_sqrt_price_x96_to_price_negative_returns_zero(self) -> None:
        assert sqrt_price_x96_to_price(-1) == Decimal("0")

    def test_sqrt_price_x96_to_tick_clamps_extreme_value(self) -> None:
        """Extremely large sqrt price would overflow but the implementation
        clamps to MAX_TICK."""
        # A huge value should clamp to MAX_TICK
        from almanak.connectors.uniswap_v3.sdk import MAX_TICK

        result = sqrt_price_x96_to_tick(2**200)
        assert result == MAX_TICK


# ---------------------------------------------------------------------------
# Pool address: compute_pool_address invalid fee
# ---------------------------------------------------------------------------


class TestComputePoolAddressErrors:
    def test_invalid_fee_raises(self) -> None:
        with pytest.raises(InvalidFeeError):
            compute_pool_address(factory=ARBITRUM_FACTORY, token0=WETH_ADDR, token1=USDC_ADDR, fee=42)


# ---------------------------------------------------------------------------
# Quote dataclass: from_dict / to_dict round-trip
# ---------------------------------------------------------------------------


class TestSwapQuoteRoundtrip:
    def test_from_dict_minimal(self) -> None:
        q = SwapQuote.from_dict(
            {
                "token_in": WETH_ADDR,
                "token_out": USDC_ADDR,
                "amount_in": "1000",
                "amount_out": "990",
                "fee": 500,
            }
        )
        assert q.amount_in == 1000
        assert q.amount_out == 990
        assert q.fee == 500
        assert q.gas_estimate == 150_000  # default

    def test_from_dict_with_quoted_at(self) -> None:
        q = SwapQuote.from_dict(
            {
                "token_in": WETH_ADDR,
                "token_out": USDC_ADDR,
                "amount_in": "100",
                "amount_out": "99",
                "fee": 100,
                "quoted_at": datetime.now(UTC).isoformat(),
            }
        )
        assert isinstance(q.quoted_at, datetime)


# ---------------------------------------------------------------------------
# Async get_quote behavior
# ---------------------------------------------------------------------------


class TestSDKAsyncGetQuote:
    def test_invalid_fee_raises(self) -> None:
        sdk = UniswapV3SDK(chain="arbitrum")
        with pytest.raises(InvalidFeeError):
            asyncio.run(sdk.get_quote(WETH_ADDR, USDC_ADDR, 10**18, 999))

    def test_no_rpc_no_gateway_falls_back_to_local(self) -> None:
        sdk = UniswapV3SDK(chain="arbitrum")
        # No web3, no rpc_url, no gateway → falls back to get_quote_local
        result = asyncio.run(sdk.get_quote(WETH_ADDR, USDC_ADDR, 10**18, 3000))
        assert result.amount_out > 0
        assert result.fee == 3000

    def test_web3_call_success_returns_swap_quote(self) -> None:
        # Pre-supply a mock web3 — eth.call returns 4 packed uint256
        mock_web3 = MagicMock()
        amount_out = 990
        sqrt_price = 12345
        ticks_crossed = 2
        gas = 145_000
        packed = (
            amount_out.to_bytes(32, "big")
            + sqrt_price.to_bytes(32, "big")
            + ticks_crossed.to_bytes(32, "big")
            + gas.to_bytes(32, "big")
        )
        mock_web3.eth.call = AsyncMock(return_value=packed)
        sdk = UniswapV3SDK(chain="arbitrum", web3=mock_web3)
        result = asyncio.run(sdk.get_quote(WETH_ADDR, USDC_ADDR, 10**18, 3000))
        assert result.amount_out == amount_out
        assert result.sqrt_price_x96_after == sqrt_price
        assert result.initialized_ticks_crossed == ticks_crossed
        assert result.gas_estimate == gas

    def test_web3_call_failure_falls_back_to_local(self) -> None:
        mock_web3 = MagicMock()
        mock_web3.eth.call = AsyncMock(side_effect=RuntimeError("rpc down"))
        sdk = UniswapV3SDK(chain="arbitrum", web3=mock_web3)
        result = asyncio.run(sdk.get_quote(WETH_ADDR, USDC_ADDR, 10**18, 3000))
        # Local fallback returns deterministic estimate (amount * (1 - fee))
        assert result.amount_out > 0


# ---------------------------------------------------------------------------
# _get_web3 resolution: gateway > rpc > error
# ---------------------------------------------------------------------------


class TestGetWeb3Resolution:
    def test_returns_existing_web3(self) -> None:
        existing = MagicMock(name="web3")
        sdk = UniswapV3SDK(chain="arbitrum", web3=existing)
        out = asyncio.run(sdk._get_web3())
        assert out is existing

    def test_no_source_raises(self) -> None:
        sdk = UniswapV3SDK(chain="arbitrum")
        with pytest.raises(UniswapV3SDKError, match="No gateway_client"):
            asyncio.run(sdk._get_web3())

    def test_gateway_path_invokes_provider_factory(self) -> None:
        """Cover the gateway_client branch by patching AsyncWeb3 to bypass
        provider type validation — we only need to confirm the import path
        executes and the provider factory is called."""
        sdk = UniswapV3SDK(chain="arbitrum", gateway_client=MagicMock())
        with patch(
            "almanak.framework.web3.gateway_provider.AsyncGatewayWeb3Provider"
        ) as mock_provider, patch("web3.AsyncWeb3") as mock_async_web3:
            mock_provider.return_value = MagicMock(name="provider")
            mock_async_web3.return_value = MagicMock(name="web3-instance")
            out = asyncio.run(sdk._get_web3())
            mock_provider.assert_called_once()
            assert out is not None

    def test_rpc_path_invokes_http_provider(self) -> None:
        """Cover the rpc_url fallback branch — AsyncWeb3 instantiation is
        patched out because the real validator rejects MagicMock providers."""
        sdk = UniswapV3SDK(chain="arbitrum", rpc_url="https://example.com/rpc")
        with patch("web3.AsyncHTTPProvider") as mock_http, patch(
            "almanak.gateway.utils.ssl_context.build_ssl_context"
        ) as mock_ssl, patch("web3.AsyncWeb3") as mock_async_web3:
            mock_http.return_value = MagicMock(name="provider")
            mock_ssl.return_value = MagicMock()
            mock_async_web3.return_value = MagicMock(name="web3-instance")
            out = asyncio.run(sdk._get_web3())
            mock_http.assert_called_once()
            assert out is not None


# ---------------------------------------------------------------------------
# _decode_quote_response edge cases
# ---------------------------------------------------------------------------


class TestDecodeQuoteResponse:
    def test_short_data_raises(self) -> None:
        sdk = UniswapV3SDK(chain="arbitrum")
        with pytest.raises(QuoteError):
            sdk._decode_quote_response(b"\x00" * 64)

    def test_decodes_packed_uints(self) -> None:
        sdk = UniswapV3SDK(chain="arbitrum")
        amount_out = 1234
        sqrt_after = 5678
        ticks = 9
        gas = 100_000
        data = (
            amount_out.to_bytes(32, "big")
            + sqrt_after.to_bytes(32, "big")
            + ticks.to_bytes(32, "big")
            + gas.to_bytes(32, "big")
        )
        out = sdk._decode_quote_response(data)
        assert out == (amount_out, sqrt_after, ticks, gas)


# ---------------------------------------------------------------------------
# build_exact_output_swap_tx + encoding
# ---------------------------------------------------------------------------


class TestBuildExactOutputCalldata:
    def test_calldata_length_and_selector(self) -> None:
        sdk = UniswapV3SDK(chain="arbitrum")
        tx = sdk.build_exact_output_swap_tx(
            token_in=USDC_ADDR,
            token_out=WETH_ADDR,
            fee=500,
            recipient=WALLET,
            deadline=1700000000,
            amount_out=10**18,
            amount_in_maximum=2100 * 10**6,
        )
        assert tx.data.startswith(EXACT_OUTPUT_SINGLE_SELECTOR)
        assert tx.gas_estimate == 170_000

    def test_quote_calldata_starts_with_quote_selector(self) -> None:
        sdk = UniswapV3SDK(chain="arbitrum")
        cd = sdk._encode_quote_exact_input_single(
            token_in=WETH_ADDR,
            token_out=USDC_ADDR,
            amount_in=10**18,
            fee=500,
        )
        assert cd.startswith(QUOTE_EXACT_INPUT_SINGLE_SELECTOR)


# ---------------------------------------------------------------------------
# Exception types
# ---------------------------------------------------------------------------


class TestSDKExceptions:
    def test_invalid_tick_error_carries_reason(self) -> None:
        err = InvalidTickError(999_999, "out of bounds")
        assert err.reason == "out of bounds"
        assert err.tick == 999_999

    def test_pool_not_found_error_includes_tokens(self) -> None:
        err = PoolNotFoundError(WETH_ADDR, USDC_ADDR, 3000)
        assert err.fee == 3000
        assert WETH_ADDR in str(err) or err.token0 == WETH_ADDR
