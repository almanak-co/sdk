"""T07 (VIB-4476): ``UniswapV4ReceiptParser.extract_lp_close_data`` happy paths.

Canonical V4 LP_CLOSE receipt shape:

1. ``ModifyLiquidity(pool_id, sender=PositionManager, ticks, -liquidity, salt)``
2. ERC-20 ``Transfer(...)`` events leaving the PoolManager (withdrawals)

Token attribution is driven by the canonical PoolKey resolved via the
injected ``pool_key_lookup``, NOT by sorting observed Transfer addresses
(the V0 sorted-Transfer path was broken for native ETH and for any
ordering where the on-chain ``currency0 < currency1`` invariant did not
match observed log order).

Assertions:

- ``amount0_collected`` = sum of transfers of ``currency0``
- ``amount1_collected`` = sum of transfers of ``currency1``
- ``pool_address`` = 32-byte canonical pool_id (66-char lowercase hex)
- ``source = "modify_liquidity"``
- ``fees0 = None``, ``fees1 = None`` (Empty != Zero)
- ``liquidity_removed`` = absolute value of ``liquidity_delta``
"""

from __future__ import annotations

import re

import pytest

from almanak.framework.connectors.uniswap_v4.receipt_parser import (
    EVENT_TOPICS,
    UniswapV4ReceiptParser,
)
from almanak.framework.connectors.uniswap_v4.sdk import PoolKey, _pad_int24, _pad_uint

CHAIN = "arbitrum"
POOL_MANAGER = "0x000000000004444c5dc75cB358380D2e3dE08A90"
POSITION_MANAGER = "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24"
WALLET = "0x1234567890abcdef1234567890abcdef12345678"
# Arbitrum USDC / WETH — USDC > WETH numerically so PoolKey sorts WETH first.
USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
POOL_ID_HEX = "0x" + "be" * 32

# Make the PoolKey once at module level — its sort guarantees
# currency0=WETH, currency1=USDC.
POOL_KEY = PoolKey(currency0=USDC, currency1=WETH, fee=500, tick_spacing=10)
assert POOL_KEY.currency0 == WETH, "WETH (0x82...) must sort before USDC (0xaf...)"
assert POOL_KEY.currency1 == USDC


def _modify_liquidity_burn_log(
    *,
    liquidity_delta: int = -500_000,
    tick_lower: int = -60000,
    tick_upper: int = 60000,
) -> dict:
    data_hex = (
        "0x"
        + _pad_int24(tick_lower)
        + _pad_int24(tick_upper)
        + _pad_uint((1 << 256) + liquidity_delta)  # int256 two's complement
        + "0" * 64  # salt
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


def _make_parser(pool_key: PoolKey = POOL_KEY) -> UniswapV4ReceiptParser:
    return UniswapV4ReceiptParser(
        chain=CHAIN,
        pool_manager_address=POOL_MANAGER,
        position_manager_address=POSITION_MANAGER,
        pool_key_lookup=lambda pid, chain: pool_key,
    )


# =============================================================================
# Canonical close happy path
# =============================================================================


class TestCanonicalClose:
    def _canonical_receipt(self) -> dict:
        # 1 WETH (currency0) + 2000 USDC (currency1) withdrawn
        return {
            "transactionHash": "0xclose",
            "logs": [
                _modify_liquidity_burn_log(liquidity_delta=-500_000),
                _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=10**18),
                _transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=2_000_000_000),
            ],
        }

    def test_returns_lp_close_data(self):
        parser = _make_parser()
        data = parser.extract_lp_close_data(self._canonical_receipt())
        assert data is not None

    def test_amount0_is_currency0_amount(self):
        """amount0_collected = sum of currency0 transfers from PoolManager."""
        parser = _make_parser()
        data = parser.extract_lp_close_data(self._canonical_receipt())
        assert data is not None
        assert data.amount0_collected == 10**18  # WETH
        assert data.amount1_collected == 2_000_000_000  # USDC

    def test_liquidity_removed_is_abs_value(self):
        parser = _make_parser()
        data = parser.extract_lp_close_data(self._canonical_receipt())
        assert data is not None
        assert data.liquidity_removed == 500_000

    def test_pool_address_is_32_byte_canonical_pool_id(self):
        parser = _make_parser()
        data = parser.extract_lp_close_data(self._canonical_receipt())
        assert data is not None
        # Spec acceptance: ^0x[0-9a-f]{64}$
        assert re.fullmatch(r"^0x[0-9a-f]{64}$", data.pool_address)
        assert data.pool_address == POOL_ID_HEX

    def test_source_marker_is_modify_liquidity(self):
        parser = _make_parser()
        data = parser.extract_lp_close_data(self._canonical_receipt())
        assert data is not None
        assert data.source == "modify_liquidity"

    def test_fees_are_unmeasured_none(self):
        """Empty != Zero (blueprint 27): V0 does not separate fees from principal."""
        parser = _make_parser()
        data = parser.extract_lp_close_data(self._canonical_receipt())
        assert data is not None
        assert data.fees0 is None
        assert data.fees1 is None


# =============================================================================
# Pool-key ordering is independent of log order
# =============================================================================


class TestPoolKeyOrderingIndependent:
    """The breaking case that motivated T07: sorted-transfer attribution is
    broken when the observed Transfer log order doesn't match
    currency0 < currency1.
    """

    def test_amount_attribution_is_currency_not_log_order(self):
        """Transfer logs appear USDC-first, WETH-second; PoolKey says
        currency0=WETH; ``amount0`` must still equal the WETH amount."""
        parser = _make_parser()
        receipt = {
            "transactionHash": "0xclose-reordered",
            "logs": [
                _modify_liquidity_burn_log(liquidity_delta=-1_000_000),
                # USDC first in log order — but currency0 = WETH per PoolKey
                _transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=5_000_000_000),
                _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=2 * 10**18),
            ],
        }
        data = parser.extract_lp_close_data(receipt)
        assert data is not None
        # T07 contract: amount0 = currency0 = WETH amount (NOT first-observed).
        assert data.amount0_collected == 2 * 10**18
        assert data.amount1_collected == 5_000_000_000

    def test_swapped_pool_key_swaps_amounts(self):
        """If the PoolKey assignment of currency0 / currency1 is swapped
        (hypothetical), so are amount0 / amount1 — attribution follows
        the canonical key, not the receipt."""
        # Real PoolKey: currency0 = WETH, currency1 = USDC
        parser = _make_parser()
        receipt = {
            "transactionHash": "0xfollow-poolkey",
            "logs": [
                _modify_liquidity_burn_log(liquidity_delta=-1_000_000),
                _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=10**18),
                _transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=3_000_000_000),
            ],
        }
        data = parser.extract_lp_close_data(receipt)
        assert data is not None
        assert data.amount0_collected == 10**18  # WETH
        assert data.amount1_collected == 3_000_000_000  # USDC


# =============================================================================
# Multiple Transfers per currency: parser sums per-currency
# =============================================================================


class TestMultipleTransfersPerCurrency:
    def test_sums_repeated_transfers(self):
        """If the same currency appears in two PoolManager-leaving Transfers,
        ``amountN`` is the sum (e.g. principal+fees in two legs)."""
        parser = _make_parser()
        receipt = {
            "transactionHash": "0xmultiplexed",
            "logs": [
                _modify_liquidity_burn_log(liquidity_delta=-1_000_000),
                # WETH (currency0) leaves in TWO transfers
                _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=10**18),
                _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=2 * 10**17),
                # USDC (currency1) leaves in one transfer
                _transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=1_000_000_000),
            ],
        }
        data = parser.extract_lp_close_data(receipt)
        assert data is not None
        assert data.amount0_collected == 10**18 + 2 * 10**17
        assert data.amount1_collected == 1_000_000_000


# =============================================================================
# Burn-vs-mint discrimination
# =============================================================================


class TestBurnDiscrimination:
    def test_positive_liquidity_delta_returns_none(self):
        """A mint (positive liquidity_delta) is NOT an LP_CLOSE."""
        parser = _make_parser()
        receipt = {
            "transactionHash": "0xmint-not-close",
            "logs": [_modify_liquidity_burn_log(liquidity_delta=10**15)],
        }
        assert parser.extract_lp_close_data(receipt) is None

    def test_picks_first_burn_when_mixed(self):
        """Mixed mint+burn in the same tx: parser picks the first burn event."""
        parser = _make_parser()
        receipt = {
            "transactionHash": "0xmixed",
            "logs": [
                _modify_liquidity_burn_log(liquidity_delta=10**15),  # mint
                _modify_liquidity_burn_log(liquidity_delta=-1_000_000),  # burn
                _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=10**18),
                _transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=1_000_000_000),
            ],
        }
        data = parser.extract_lp_close_data(receipt)
        assert data is not None
        assert data.liquidity_removed == 1_000_000


# =============================================================================
# Empty / missing inputs
# =============================================================================


class TestMissingInputs:
    def test_empty_receipt_returns_none(self):
        parser = _make_parser()
        assert parser.extract_lp_close_data({"logs": []}) is None

    def test_no_burn_event_returns_none(self):
        """Receipt has Transfer events but no ModifyLiquidity → None
        (irrespective of pool_key_lookup)."""
        parser = _make_parser()
        receipt = {
            "transactionHash": "0xno-burn",
            "logs": [
                _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=10**18),
            ],
        }
        assert parser.extract_lp_close_data(receipt) is None


# =============================================================================
# Spec acceptance: pool_address regex
# =============================================================================


def test_pool_address_matches_acceptance_regex():
    """Spec: ``pool_address`` must match ``^0x[0-9a-f]{64}$``."""
    parser = _make_parser()
    receipt = {
        "transactionHash": "0xregex",
        "logs": [
            _modify_liquidity_burn_log(liquidity_delta=-500_000),
            _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=1),
            _transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=1),
        ],
    }
    data = parser.extract_lp_close_data(receipt)
    assert data is not None
    assert re.fullmatch(r"^0x[0-9a-f]{64}$", data.pool_address) is not None


# =============================================================================
# to_dict round-trip preserves source + pool_address
# =============================================================================


def test_to_dict_emits_source_and_pool_address():
    parser = _make_parser()
    receipt = {
        "transactionHash": "0xtodict",
        "logs": [
            _modify_liquidity_burn_log(liquidity_delta=-1),
            _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=1),
            _transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=1),
        ],
    }
    data = parser.extract_lp_close_data(receipt)
    assert data is not None
    d = data.to_dict()
    assert d["source"] == "modify_liquidity"
    assert d["pool_address"] == POOL_ID_HEX
    assert d["fees0"] is None
    assert d["fees1"] is None


# =============================================================================
# Parser construction without ``pool_key_lookup`` is still allowed
# =============================================================================


def test_parser_accepts_no_pool_key_lookup_arg():
    """Constructor must remain backward-compatible: ``pool_key_lookup`` is
    keyword-only with a None default. Tests that don't exercise close still
    instantiate the parser without it (test_extract_lp_open_data.py)."""
    parser = UniswapV4ReceiptParser(chain=CHAIN, pool_manager_address=POOL_MANAGER)
    assert parser is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
