"""Branch-complete tests for the V4 LP-open compilation phases."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, InvalidOperation
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.uniswap_v4 import adapter as adapter_module
from almanak.connectors.uniswap_v4.adapter import (
    UniswapV4Adapter,
    UniswapV4Config,
    UniswapV4UnsupportedPoolError,
)
from almanak.connectors.uniswap_v4.sdk import SwapTransaction, _tick_to_sqrt_ratio_x96

_LOW_ADDRESS = "0x0000000000000000000000000000000000000010"
_HIGH_ADDRESS = "0x0000000000000000000000000000000000000020"


def _make_adapter(*, swapped: bool = False, rpc_url: str | None = None) -> UniswapV4Adapter:
    resolver = MagicMock()
    tokens = {
        "AAA": MagicMock(address=_HIGH_ADDRESS if swapped else _LOW_ADDRESS, decimals=18, is_native=False),
        "BBB": MagicMock(address=_LOW_ADDRESS if swapped else _HIGH_ADDRESS, decimals=6, is_native=False),
    }
    resolver.resolve_for_swap.side_effect = lambda symbol, chain: tokens[symbol]
    resolver.resolve.side_effect = lambda symbol, chain: tokens[symbol]
    return UniswapV4Adapter(
        config=UniswapV4Config(
            chain="arbitrum",
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            rpc_url=rpc_url,
        ),
        token_resolver=resolver,
    )


def _intent(**overrides):
    values = {
        "pool": "AAA/BBB/3000",
        "amount0": Decimal("2"),
        "amount1": Decimal("3"),
        "range_lower": Decimal("2"),
        "range_upper": Decimal("4"),
        "protocol": "uniswap_v4",
        "protocol_params": {"allow_estimated_price": True},
        "max_slippage": Decimal("0.10"),
        "intent_id": "lp-open-helper-test",
        "registry_handle": None,
        "chain": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.parametrize(
    ("swapped", "expected_symbols", "expected_range", "expected_amounts"),
    [
        (False, ("AAA", "BBB"), (Decimal("2"), Decimal("4")), (2 * 10**18, 3 * 10**6)),
        (True, ("BBB", "AAA"), (Decimal("0.25"), Decimal("0.5")), (3 * 10**6, 2 * 10**18)),
    ],
)
def test_prepare_lp_open_pool_canonicalizes_pair_and_expands_collapsed_ticks(
    swapped,
    expected_symbols,
    expected_range,
    expected_amounts,
):
    adapter = _make_adapter(swapped=swapped)
    intent = _intent(protocol_params={"tick_spacing": 10, "hook_data": "0xdeadbeef"})

    with patch.object(adapter._sdk, "price_to_tick", side_effect=[101, 109]):
        pool = adapter._prepare_lp_open_pool(intent)

    assert (pool.token0_symbol, pool.token1_symbol) == expected_symbols
    assert (pool.range_lower, pool.range_upper) == expected_range
    assert (pool.amount0_wei, pool.amount1_wei) == expected_amounts
    assert (pool.tick_lower, pool.tick_upper) == (100, 110)
    assert pool.pool_key.tick_spacing == 10
    assert pool.hook_data == bytes.fromhex("deadbeef")


def test_prepare_lp_open_pool_rejects_non_liquidity_hook():
    adapter = _make_adapter()
    intent = _intent(protocol_params={"hooks": "0x0000000000000000000000000000000000000080"})

    with pytest.raises(UniswapV4UnsupportedPoolError):
        adapter._prepare_lp_open_pool(intent)


def test_prepare_lp_open_pool_validates_amount_before_inverting_range():
    adapter = _make_adapter(swapped=True)
    intent = _intent(amount1="invalid", range_upper=Decimal("0"))

    with pytest.raises(InvalidOperation):
        adapter._prepare_lp_open_pool(intent)


@pytest.mark.parametrize(
    (
        "rpc_url",
        "rpc_price",
        "oracle_prices",
        "price_range",
        "expected_source",
        "expected_sqrt_price",
        "expected_onchain",
    ),
    [
        ("http://localhost:8545", 2**96, (None, None), (Decimal("2"), Decimal("4")), "on_chain", 2**96, True),
        (
            "http://localhost:8545",
            None,
            (Decimal("2"), Decimal("1")),
            (Decimal("2"), Decimal("4")),
            "oracle_estimate",
            123,
            False,
        ),
        (None, None, (None, None), (Decimal("2"), Decimal("4")), "range_midpoint_estimate", 123, False),
        (
            None,
            None,
            (None, None),
            (None, None),
            "range_midpoint_estimate",
            (_tick_to_sqrt_ratio_x96(0) + _tick_to_sqrt_ratio_x96(60)) // 2,
            False,
        ),
    ],
)
def test_resolve_lp_open_price_sources(
    rpc_url,
    rpc_price,
    oracle_prices,
    price_range,
    expected_source,
    expected_sqrt_price,
    expected_onchain,
):
    adapter = _make_adapter(rpc_url=rpc_url)
    pool = SimpleNamespace(
        pool_key=object(),
        token0_symbol="AAA",
        token1_symbol="BBB",
        token0_dec=18,
        token1_dec=6,
        range_lower=price_range[0],
        range_upper=price_range[1],
        tick_lower=0,
        tick_upper=60,
    )

    with (
        patch.object(adapter._sdk, "get_pool_sqrt_price", return_value=rpc_price) as get_pool_price,
        patch.object(adapter._sdk, "estimate_sqrt_price_x96", return_value=123) as estimate_price,
        patch(
            "almanak.framework.intents.compiler_queries.lenient_oracle_price",
            side_effect=oracle_prices,
        ) as oracle_price,
    ):
        result = adapter._resolve_lp_open_price(_intent(), pool, {})

    assert result == adapter_module._LPOpenPrice(expected_sqrt_price, expected_onchain, expected_source)
    assert get_pool_price.call_count == bool(rpc_url)
    assert oracle_price.call_count == (0 if expected_onchain else 2)
    assert estimate_price.call_count == (1 if expected_sqrt_price == 123 else 0)


def test_build_lp_open_bundle_includes_warnings():
    adapter = _make_adapter()
    pool = replace(adapter._prepare_lp_open_pool(_intent()), warnings=["hook warning"])
    price = adapter_module._LPOpenPrice(sqrt_price_x96=2**96, used_onchain_price=True, source="on_chain")
    liquidity = adapter_module._LPOpenLiquidity(
        amount0_budget=100,
        amount1_budget=200,
        amount0_max=110,
        amount1_max=220,
        liquidity=300,
        slippage_bps=500,
    )
    transaction = SwapTransaction(
        to=adapter.addresses["position_manager"],
        value=0,
        data="0x1234",
        gas_estimate=450_000,
        description="Mint V4 LP position",
    )

    bundle = adapter._build_lp_open_bundle(_intent(), pool, price, liquidity, [transaction])

    assert bundle.metadata["warnings"] == ["hook warning"]
    assert bundle.metadata["estimated_sqrt_price_x96"] is None
    assert bundle.metadata["gas_estimate"] == 450_000
    assert bundle.transactions[0]["value"] == "0"


def test_compile_lp_open_zero_liquidity_returns_exact_soft_error():
    adapter = _make_adapter()
    with patch.object(adapter._sdk, "compute_liquidity_from_amounts", return_value=0):
        bundle = adapter.compile_lp_open_intent(
            _intent(),
            {"AAA": Decimal("2"), "BBB": Decimal("1")},
        )

    assert bundle.intent_type == "LP_OPEN"
    assert bundle.transactions == []
    assert bundle.metadata == {"error": "Computed liquidity is zero — check amounts and price range"}
