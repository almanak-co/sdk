"""T07 (VIB-4476): native-ETH currency-leg pools are rejected at close-time.

V0 (VIB-4426) supports hookless ERC20-ERC20 pools only. A V4 pool whose
``currency0 == 0x0000…0000`` (native ETH) is out of V0 scope. The
extract_lp_close_data path resolves the canonical PoolKey via the injected
``pool_key_lookup`` and raises :class:`UniswapV4UnsupportedPoolError`
citing VIB-4483 (P-V1-B). This mirrors the T06 adapter compile-time guard.
"""

from __future__ import annotations

import pytest

from almanak.framework.connectors.uniswap_v4.adapter import UniswapV4UnsupportedPoolError
from almanak.framework.connectors.uniswap_v4.receipt_parser import (
    EVENT_TOPICS,
    UniswapV4ReceiptParser,
)
from almanak.framework.connectors.uniswap_v4.sdk import NATIVE_CURRENCY, PoolKey, _pad_int24, _pad_uint

CHAIN = "base"
POOL_MANAGER = "0x000000000004444c5dc75cB358380D2e3dE08A90"
POSITION_MANAGER = "0x7C5f5A4bBd8fD63184577525326123B519429bDc"
WETH = "0x4200000000000000000000000000000000000006"
POOL_ID_HEX = "0x" + "cd" * 32


def _modify_liquidity_burn_log(*, liquidity_delta: int) -> dict:
    data_hex = (
        "0x" + _pad_int24(-60000) + _pad_int24(60000) + _pad_uint((1 << 256) + liquidity_delta) + "0" * 64  # salt
    )
    return {
        "address": POOL_MANAGER,
        "topics": [
            EVENT_TOPICS["ModifyLiquidity"],
            POOL_ID_HEX,
            "0x" + "00" * 12 + POSITION_MANAGER.lower().replace("0x", ""),
        ],
        "data": data_hex,
    }


def test_native_eth_currency0_raises_unsupported_pool_error():
    """PoolKey with currency0 == 0x0 → raise UniswapV4UnsupportedPoolError."""
    native_eth_pool_key = PoolKey(
        currency0=NATIVE_CURRENCY,
        currency1=WETH,
        fee=3000,
        tick_spacing=60,
    )
    parser = UniswapV4ReceiptParser(
        chain=CHAIN,
        pool_manager_address=POOL_MANAGER,
        position_manager_address=POSITION_MANAGER,
        pool_key_lookup=lambda pid, chain: native_eth_pool_key,
    )

    receipt = {
        "transactionHash": "0xnativeclose",
        "logs": [_modify_liquidity_burn_log(liquidity_delta=-1_000_000)],
    }

    with pytest.raises(UniswapV4UnsupportedPoolError) as exc_info:
        parser.extract_lp_close_data(receipt)

    msg = str(exc_info.value)
    assert "VIB-4483" in msg
    assert "native ETH" in msg
    assert "V0" in msg


def test_native_eth_error_mentions_pool_id_for_diagnostics():
    native_eth_pool_key = PoolKey(
        currency0=NATIVE_CURRENCY,
        currency1=WETH,
        fee=3000,
        tick_spacing=60,
    )
    parser = UniswapV4ReceiptParser(
        chain=CHAIN,
        pool_manager_address=POOL_MANAGER,
        position_manager_address=POSITION_MANAGER,
        pool_key_lookup=lambda pid, chain: native_eth_pool_key,
    )
    receipt = {
        "transactionHash": "0xnativeclose",
        "logs": [_modify_liquidity_burn_log(liquidity_delta=-1_000_000)],
    }
    with pytest.raises(UniswapV4UnsupportedPoolError) as exc_info:
        parser.extract_lp_close_data(receipt)
    assert POOL_ID_HEX in str(exc_info.value)
    assert CHAIN in str(exc_info.value)


def test_erc20_only_pool_does_not_raise():
    """Sanity check: an ERC20-ERC20 PoolKey does NOT trigger the native-ETH guard."""
    usdc = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    pool_key = PoolKey(currency0=usdc, currency1=WETH, fee=500, tick_spacing=10)
    wallet = "0x1234567890abcdef1234567890abcdef12345678"
    parser = UniswapV4ReceiptParser(
        chain=CHAIN,
        pool_manager_address=POOL_MANAGER,
        position_manager_address=POSITION_MANAGER,
        pool_key_lookup=lambda pid, chain: pool_key,
    )
    # currency0 < currency1 sorted: USDC (0x83...) > WETH (0x42...) so PoolKey
    # swaps them: currency0 = WETH, currency1 = USDC.
    receipt = {
        "transactionHash": "0xerc20close",
        "logs": [
            _modify_liquidity_burn_log(liquidity_delta=-1_000_000),
            {
                "address": pool_key.currency0,
                "topics": [
                    EVENT_TOPICS["Transfer"],
                    "0x" + "00" * 12 + POOL_MANAGER.lower().replace("0x", ""),
                    "0x" + "00" * 12 + wallet.replace("0x", ""),
                ],
                "data": "0x" + _pad_uint(5 * 10**17),
            },
            {
                "address": pool_key.currency1,
                "topics": [
                    EVENT_TOPICS["Transfer"],
                    "0x" + "00" * 12 + POOL_MANAGER.lower().replace("0x", ""),
                    "0x" + "00" * 12 + wallet.replace("0x", ""),
                ],
                "data": "0x" + _pad_uint(1_000_000_000),
            },
        ],
    }
    # Should not raise; should produce a valid LPCloseData.
    data = parser.extract_lp_close_data(receipt)
    assert data is not None
    assert data.source == "modify_liquidity"
