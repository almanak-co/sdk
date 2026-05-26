"""T07 (VIB-4476): transfer-set integrity check.

The set of token addresses observed in Transfer logs leaving the
PoolManager MUST equal ``{currency0, currency1}`` from the PoolKey. On
mismatch, ``extract_lp_close_data`` emits a structured WARNING and returns
``None`` (fail-loud over silent misattribution).

Covers:

- Missing transfer (only currency0 or only currency1 observed)
- Wrong token observed (currency mismatch with PoolKey)
- Extra unrelated token in the transfer set
- Empty transfer set (burn with no withdrawal transfers at all)
"""

from __future__ import annotations

import logging

import pytest

from almanak.connectors.uniswap_v4.receipt_parser import (
    EVENT_TOPICS,
    UniswapV4ReceiptParser,
)
from almanak.connectors.uniswap_v4.sdk import PoolKey, _pad_int24, _pad_uint

CHAIN = "arbitrum"
POOL_MANAGER = "0x000000000004444c5dc75cB358380D2e3dE08A90"
POSITION_MANAGER = "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24"
WALLET = "0x1234567890abcdef1234567890abcdef12345678"
USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"  # arbitrum USDC
WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"  # arbitrum WETH
UNRELATED = "0x0000000000000000000000000000000000000c0c"  # not in PoolKey
POOL_ID_HEX = "0x" + "de" * 32


def _modify_liquidity_burn_log(*, liquidity_delta: int = -500_000) -> dict:
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


def _transfer_log(*, token: str, from_addr: str, to_addr: str, amount: int) -> dict:
    return {
        "address": token,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + "00" * 12 + from_addr.lower().replace("0x", ""),
            "0x" + "00" * 12 + to_addr.lower().replace("0x", ""),
        ],
        "data": "0x" + _pad_uint(amount),
    }


def _make_parser() -> UniswapV4ReceiptParser:
    pool_key = PoolKey(currency0=USDC, currency1=WETH, fee=500, tick_spacing=10)
    return UniswapV4ReceiptParser(
        chain=CHAIN,
        pool_manager_address=POOL_MANAGER,
        position_manager_address=POSITION_MANAGER,
        pool_key_lookup=lambda pid, chain: pool_key,
    )


def test_single_sided_close_returns_measured_zero_on_missing_leg(caplog: pytest.LogCaptureFixture):
    """VIB-4426 P1 #3 — only currency0 (WETH) observed → keep, measured-zero
    on the missing currency1 leg.

    Pre-fix the strict transfer-set equality dropped this as
    ``transfer_set_mismatch``. A concentrated-liquidity position out of range
    at burn time legitimately returns one token only; the missing leg is a
    measured zero (the PoolKey lookup succeeded AND we observed all
    transfers from PoolManager, so a non-observed currency truly received
    zero in this burn).
    """
    parser = _make_parser()
    # PoolKey auto-sorts: WETH (0x82af…) < USDC (0xaf88…), so canonical
    # currency0 = WETH, currency1 = USDC.
    receipt = {
        "transactionHash": "0xsinglesided1",
        "logs": [
            _modify_liquidity_burn_log(),
            _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=10**17),
        ],
    }
    result = parser.extract_lp_close_data(receipt)
    assert result is not None, (
        "single-sided V4 LP close on an in-PoolKey currency must NOT drop"
    )
    # WETH = currency0 (lower address), so amount0 = WETH leg, amount1 = 0.
    assert result.amount0_collected == 10**17, "currency0 (WETH) leg observed"
    assert result.amount1_collected == 0, "currency1 (USDC) leg is measured zero"
    assert result.currency0 == WETH.lower()
    assert result.currency1 == USDC.lower()


def test_single_sided_close_other_leg_returns_measured_zero(caplog: pytest.LogCaptureFixture):
    """VIB-4426 P1 #3 — mirror of the above with USDC (currency1) observed."""
    parser = _make_parser()
    receipt = {
        "transactionHash": "0xsinglesided2",
        "logs": [
            _modify_liquidity_burn_log(),
            _transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=1_000_000),
        ],
    }
    result = parser.extract_lp_close_data(receipt)
    assert result is not None
    # USDC = currency1 (higher address), so amount0 = 0 (WETH leg), amount1 = USDC leg.
    assert result.amount0_collected == 0
    assert result.amount1_collected == 1_000_000


def test_wrong_token_observed_returns_none(caplog: pytest.LogCaptureFixture):
    """Currency0 absent, an unrelated token in its place → drop."""
    parser = _make_parser()
    receipt = {
        "transactionHash": "0xmismatch3",
        "logs": [
            _modify_liquidity_burn_log(),
            _transfer_log(token=UNRELATED, from_addr=POOL_MANAGER, to_addr=WALLET, amount=1_000),
            _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=10**17),
        ],
    }
    with caplog.at_level(logging.WARNING, logger="almanak.connectors.uniswap_v4.receipt_parser"):
        result = parser.extract_lp_close_data(receipt)
    assert result is None
    joined = " ".join(rec.message for rec in caplog.records)
    assert "transfer_set_mismatch" in joined


def test_extra_token_in_set_returns_none(caplog: pytest.LogCaptureFixture):
    """Both currencies present + an extra unrelated token leaving PoolManager → drop."""
    parser = _make_parser()
    receipt = {
        "transactionHash": "0xmismatch4",
        "logs": [
            _modify_liquidity_burn_log(),
            _transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=1_000_000),
            _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=10**17),
            _transfer_log(token=UNRELATED, from_addr=POOL_MANAGER, to_addr=WALLET, amount=42),
        ],
    }
    with caplog.at_level(logging.WARNING, logger="almanak.connectors.uniswap_v4.receipt_parser"):
        result = parser.extract_lp_close_data(receipt)
    assert result is None
    joined = " ".join(rec.message for rec in caplog.records)
    assert "transfer_set_mismatch" in joined


def test_empty_transfer_set_returns_none(caplog: pytest.LogCaptureFixture):
    """Burn ModifyLiquidity but zero withdrawal Transfers → drop (mismatch with PoolKey)."""
    parser = _make_parser()
    receipt = {
        "transactionHash": "0xmismatch5",
        "logs": [_modify_liquidity_burn_log()],
    }
    with caplog.at_level(logging.WARNING, logger="almanak.connectors.uniswap_v4.receipt_parser"):
        result = parser.extract_lp_close_data(receipt)
    assert result is None
    joined = " ".join(rec.message for rec in caplog.records)
    assert "transfer_set_mismatch" in joined


def test_unrelated_transfer_not_from_pool_manager_is_ignored():
    """Transfers NOT leaving the PoolManager don't count toward the observed set.

    This guards against pollution from intra-tx routing legs (e.g. WETH unwrap
    from PositionManager to wallet) that don't affect close attribution.
    """
    parser = _make_parser()
    receipt = {
        "transactionHash": "0xignore",
        "logs": [
            _modify_liquidity_burn_log(),
            _transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=1_000_000),
            _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=10**17),
            # Pollution: WETH from PositionManager (unwrap leg) — must NOT
            # tip the observed set into mismatch territory.
            _transfer_log(token=WETH, from_addr=POSITION_MANAGER, to_addr=WALLET, amount=10**16),
            # Pollution: random user-to-user — must NOT count.
            _transfer_log(token=UNRELATED, from_addr=WALLET, to_addr=POOL_MANAGER, amount=99),
        ],
    }
    result = parser.extract_lp_close_data(receipt)
    assert result is not None
    assert result.source == "modify_liquidity"


def test_lookup_returning_none_drops_with_warning(caplog: pytest.LogCaptureFixture):
    """pool_key_lookup returning None (gateway NOT_FOUND) → drop + WARN."""
    parser = UniswapV4ReceiptParser(
        chain=CHAIN,
        pool_manager_address=POOL_MANAGER,
        position_manager_address=POSITION_MANAGER,
        pool_key_lookup=lambda pid, chain: None,
    )
    receipt = {
        "transactionHash": "0xnotfound",
        "logs": [
            _modify_liquidity_burn_log(),
            _transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=1_000_000),
            _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=10**17),
        ],
    }
    with caplog.at_level(logging.WARNING, logger="almanak.connectors.uniswap_v4.receipt_parser"):
        result = parser.extract_lp_close_data(receipt)
    assert result is None
    joined = " ".join(rec.message for rec in caplog.records)
    assert "pool_key_not_found" in joined


def test_lookup_raising_drops_with_warning(caplog: pytest.LogCaptureFixture):
    """pool_key_lookup raising any exception → drop + WARN (no crash)."""

    def _raises(pid: str, chain: str):
        raise RuntimeError("gateway transport blew up")

    parser = UniswapV4ReceiptParser(
        chain=CHAIN,
        pool_manager_address=POOL_MANAGER,
        position_manager_address=POSITION_MANAGER,
        pool_key_lookup=_raises,
    )
    receipt = {
        "transactionHash": "0xraises",
        "logs": [_modify_liquidity_burn_log()],
    }
    with caplog.at_level(logging.WARNING, logger="almanak.connectors.uniswap_v4.receipt_parser"):
        result = parser.extract_lp_close_data(receipt)
    assert result is None
    joined = " ".join(rec.message for rec in caplog.records)
    assert "pool_key_lookup_error" in joined


def test_missing_pool_key_lookup_drops_with_warning(caplog: pytest.LogCaptureFixture):
    """No pool_key_lookup configured → drop + WARN, do NOT silently fall back."""
    parser = UniswapV4ReceiptParser(
        chain=CHAIN,
        pool_manager_address=POOL_MANAGER,
        position_manager_address=POSITION_MANAGER,
        # pool_key_lookup intentionally omitted
    )
    receipt = {
        "transactionHash": "0xnolookup",
        "logs": [_modify_liquidity_burn_log()],
    }
    with caplog.at_level(logging.WARNING, logger="almanak.connectors.uniswap_v4.receipt_parser"):
        result = parser.extract_lp_close_data(receipt)
    assert result is None
    joined = " ".join(rec.message for rec in caplog.records)
    assert "missing_pool_key_lookup" in joined
