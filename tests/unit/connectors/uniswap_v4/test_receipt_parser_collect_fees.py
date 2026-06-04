"""VIB-4637: ``UniswapV4ReceiptParser.extract_lp_close_data`` fees-only collect path.

A V4 fees-only ``LP_COLLECT_FEES`` compiles to
``DECREASE_LIQUIDITY(liquidity=0) + TAKE_PAIR`` (see
``sdk.build_collect_fees_tx``), so the PoolManager emits a ``ModifyLiquidity``
with ``liquidity_delta == 0`` and NO principal-removing burn. The negative-
delta close branch never fires, so before VIB-4637 ``extract_lp_close_data``
returned ``None`` → no typed ``pool_address`` → the LP accounting handler
dropped the entire LP_COLLECT_FEES event (the ``weth/usdc/3000`` V4 position-
key tail is rejected by ``_clean_pool_address_candidate`` as a V3 fee-tier
descriptor).

This suite pins the fees-only branch:

- A zero-delta ``ModifyLiquidity`` yields an ``LPCloseData`` carrying the
  canonical 32-byte V4 PoolId (``topics[1]``) as ``pool_address``.
- The directional null-contract (Empty != Zero != None, blueprint 27 §10.10):
  principal / liquidity legs are measured-zero (no principal moved), fees are
  unmeasured ``None`` (V4 bundles them in V0), currencies are unmeasured
  ``None`` (this path is lookup-free and does not resolve the PoolKey).
- The fees-only branch does NOT require ``pool_key_lookup`` — the PoolId is
  chain truth in the event itself.
"""

from __future__ import annotations

import re

import pytest

from almanak.connectors.uniswap_v4.receipt_parser import (
    EVENT_TOPICS,
    UniswapV4ReceiptParser,
)
from almanak.connectors.uniswap_v4.sdk import _pad_int24, _pad_uint

CHAIN = "ethereum"
POOL_MANAGER = "0x000000000004444c5dc75cB358380D2e3dE08A90"
POSITION_MANAGER = "0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e"
WALLET = "0x1234567890abcdef1234567890abcdef12345678"
WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
POOL_ID_HEX = "0x" + "be" * 32


def _modify_liquidity_log(
    *,
    liquidity_delta: int,
    pool_id: str = POOL_ID_HEX,
    tick_lower: int = -60000,
    tick_upper: int = 60000,
) -> dict:
    data_hex = (
        "0x"
        + _pad_int24(tick_lower)
        + _pad_int24(tick_upper)
        + _pad_uint((1 << 256) + liquidity_delta if liquidity_delta < 0 else liquidity_delta)
        + "0" * 64  # salt
    )
    return {
        "address": POOL_MANAGER,
        "topics": [
            EVENT_TOPICS["ModifyLiquidity"],
            pool_id,
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


def _parser_without_lookup() -> UniswapV4ReceiptParser:
    """The fees-only path must work WITHOUT a pool_key_lookup — the PoolId is
    carried in the event itself, no gateway resolution required."""
    return UniswapV4ReceiptParser(
        chain=CHAIN,
        pool_manager_address=POOL_MANAGER,
        position_manager_address=POSITION_MANAGER,
    )


def _collect_fees_receipt(*, with_fee_transfers: bool = False) -> dict:
    """A fees-only collect: zero-delta ModifyLiquidity, optionally with the
    accrued-fee Transfer legs (a fresh position has none)."""
    logs = [_modify_liquidity_log(liquidity_delta=0)]
    if with_fee_transfers:
        logs.append(_transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=10**15))
        logs.append(_transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=1_000_000))
    return {"transactionHash": "0xcollect", "logs": logs}


# =============================================================================
# Fees-only collect happy path (the VIB-4637 fix)
# =============================================================================


class TestFeesOnlyCollect:
    def test_returns_lp_close_data_without_pool_key_lookup(self):
        parser = _parser_without_lookup()
        data = parser.extract_lp_close_data(_collect_fees_receipt())
        assert data is not None, "fees-only collect (delta=0) must yield LPCloseData"

    def test_pool_address_is_canonical_v4_pool_id(self):
        parser = _parser_without_lookup()
        data = parser.extract_lp_close_data(_collect_fees_receipt())
        assert data is not None
        # The handler's accept-branch is ^0x[0-9a-f]{64}$ (the 32-byte PoolId).
        assert re.fullmatch(r"^0x[0-9a-f]{64}$", data.pool_address)
        assert data.pool_address == POOL_ID_HEX

    def test_principal_legs_are_measured_zero(self):
        """No principal withdrawn on a fees-only collect → measured zero
        (the fields are typed int; 0 is the honest value, not None)."""
        parser = _parser_without_lookup()
        data = parser.extract_lp_close_data(_collect_fees_receipt(with_fee_transfers=True))
        assert data is not None
        assert data.amount0_collected == 0
        assert data.amount1_collected == 0

    def test_liquidity_removed_is_measured_zero(self):
        parser = _parser_without_lookup()
        data = parser.extract_lp_close_data(_collect_fees_receipt())
        assert data is not None
        assert data.liquidity_removed == 0

    def test_fees_are_unmeasured_none(self):
        """Empty != Zero (blueprint 27 §10.10): V4 bundles fees into the
        withdrawal Transfer in V0; explicit None is the honest signal."""
        parser = _parser_without_lookup()
        data = parser.extract_lp_close_data(_collect_fees_receipt(with_fee_transfers=True))
        assert data is not None
        assert data.fees0 is None
        assert data.fees1 is None

    def test_currencies_unmeasured_not_fabricated(self):
        """The lookup-free path does not resolve the PoolKey, so currencies
        stay None rather than being guessed (Empty != Zero != None)."""
        parser = _parser_without_lookup()
        data = parser.extract_lp_close_data(_collect_fees_receipt())
        assert data is not None
        assert data.currency0 is None
        assert data.currency1 is None

    def test_source_marker_is_modify_liquidity(self):
        parser = _parser_without_lookup()
        data = parser.extract_lp_close_data(_collect_fees_receipt())
        assert data is not None
        assert data.source == "modify_liquidity"

    def test_to_dict_round_trip(self):
        parser = _parser_without_lookup()
        data = parser.extract_lp_close_data(_collect_fees_receipt())
        assert data is not None
        d = data.to_dict()
        assert d["pool_address"] == POOL_ID_HEX
        assert d["source"] == "modify_liquidity"
        assert d["amount0_collected"] == "0"
        assert d["amount1_collected"] == "0"
        assert d["fees0"] is None
        assert d["fees1"] is None


# =============================================================================
# Burn still takes precedence over the fees-only branch
# =============================================================================


class TestBurnPrecedence:
    def test_negative_delta_does_not_hit_fees_only_branch(self):
        """A real burn (delta<0) without a pool_key_lookup still drops via the
        burn path's missing_pool_key_lookup guard — it must NOT fall through
        to the fees-only branch and silently book a principal-zero close."""
        parser = _parser_without_lookup()
        receipt = {
            "transactionHash": "0xburn",
            "logs": [
                _modify_liquidity_log(liquidity_delta=-500_000),
                _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=10**18),
            ],
        }
        # Burn branch fires first; without a lookup it drops (returns None) —
        # it does NOT misroute to the zero-delta fees-only path.
        assert parser.extract_lp_close_data(receipt) is None

    def test_mixed_burn_and_zero_delta_prefers_burn(self):
        """If a tx has both a burn and a zero-delta event, the burn wins."""
        from almanak.connectors.uniswap_v4.sdk import PoolKey

        pool_key = PoolKey(currency0=USDC, currency1=WETH, fee=500, tick_spacing=10)
        parser = UniswapV4ReceiptParser(
            chain=CHAIN,
            pool_manager_address=POOL_MANAGER,
            position_manager_address=POSITION_MANAGER,
            pool_key_lookup=lambda pid, chain: pool_key,
        )
        receipt = {
            "transactionHash": "0xmixed",
            "logs": [
                _modify_liquidity_log(liquidity_delta=0),  # zero-delta first
                _modify_liquidity_log(liquidity_delta=-1_000_000),  # burn second
                _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=10**18),
                _transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=1_000_000_000),
            ],
        }
        data = parser.extract_lp_close_data(receipt)
        assert data is not None
        # Burn path: real principal attributed, liquidity_removed > 0.
        assert data.liquidity_removed == 1_000_000
        assert data.amount0_collected == 10**18


# =============================================================================
# No ModifyLiquidity at all → None (no event)
# =============================================================================


def test_no_modify_liquidity_returns_none():
    parser = _parser_without_lookup()
    receipt = {
        "transactionHash": "0xnone",
        "logs": [
            _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=1),
        ],
    }
    assert parser.extract_lp_close_data(receipt) is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
