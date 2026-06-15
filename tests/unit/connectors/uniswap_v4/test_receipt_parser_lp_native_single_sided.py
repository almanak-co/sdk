"""VIB-5119: fully-native single-sided V4 LP_OPEN / LP_CLOSE receipts.

The VIB-4483 native round-trip is TWO-sided (native ETH + an ERC-20 such as
USDC), so neither edge below is exercised by the two-sided ``lp_v4_native``
fixture. A fully-native single-sided position (out-of-range mint/burn that
touches ONLY the native ETH leg) emits NO ERC-20 Transfer at all — the native
ETH moves via ``msg.value`` (open) / ``TAKE_PAIR`` (close):

* **Open (Case 1):** ``_sum_deposit_transfers_by_currency_order`` returns
  all-``None``. Pre-VIB-5119 the single-sided resolve branch
  (``amount0 is not None and amount1 is None``) was skipped (amount0 is None),
  so ``currency0`` / ``currency1`` stayed unset, the runner's native-amount
  capture (``_native_v4_open_eligible``) capability-gated on ``currency0`` and
  skipped, and the native deposit was never stamped — an unmeasured LP_OPEN for
  a valid native-only mint. VIB-5119 resolves the native-leg PoolKey via the
  gateway lookup so both currencies ARE set; the native amount stays ``None``
  for the runner stamp, the ERC-20 leg is a measured ``0``.

* **Close (Case 2):** the transfer-integrity gate required a non-empty
  ``observed_tokens`` set, but a burn returning only raw ETH emits no ERC-20
  Transfer → ``observed_tokens`` empty → ``transfer_set_mismatch`` drop → the
  LP_CLOSE accounting event was silently lost even though the close succeeded
  on-chain. VIB-5119 allows the empty-observed case ONLY when the PoolKey
  carries the native-ETH leg, so the LP_CLOSE event IS booked (native leg
  ``None`` per VIB-5117's nullability; the runner fills the native principal
  pre-burn). The genuine all-ERC-20 empty-observed mismatch STILL drops.
"""

from __future__ import annotations

import logging

import pytest

from almanak.connectors.uniswap_v4.receipt_parser import (
    EVENT_TOPICS,
    UniswapV4ReceiptParser,
)
from almanak.connectors.uniswap_v4.sdk import (
    NATIVE_CURRENCY,
    PoolKey,
    _pad_int24,
    _pad_uint,
)

CHAIN = "base"
POOL_MANAGER = "0x000000000004444c5dc75cB358380D2e3dE08A90"
POSITION_MANAGER = "0x7C5f5A4bBd8fD63184577525326123B519429bDc"
# Native ETH (0x0) sorts first; pair it with a real ERC-20 (Base USDC).
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
WETH = "0x4200000000000000000000000000000000000006"
WALLET = "0x1234567890abcdef1234567890abcdef12345678"
POOL_ID_HEX = "0x" + "cd" * 32
TOKEN_ID = 7777


# =============================================================================
# Log builders
# =============================================================================


def _modify_liquidity_log(*, liquidity_delta: int) -> dict:
    if liquidity_delta < 0:
        liquidity_bytes = (1 << 256) + liquidity_delta
    else:
        liquidity_bytes = liquidity_delta
    data_hex = "0x" + _pad_int24(-60000) + _pad_int24(60000) + _pad_uint(liquidity_bytes) + _pad_uint(TOKEN_ID)  # salt
    return {
        "address": POOL_MANAGER,
        "topics": [
            EVENT_TOPICS["ModifyLiquidity"],
            POOL_ID_HEX,
            "0x" + "00" * 12 + POSITION_MANAGER.lower().replace("0x", ""),
        ],
        "data": data_hex,
    }


def _erc721_mint_log(*, token_id: int) -> dict:
    return {
        "address": POSITION_MANAGER,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + "0" * 64,
            "0x" + "00" * 12 + WALLET.replace("0x", ""),
            "0x" + format(token_id, "064x"),
        ],
        "data": "0x",
    }


def _erc20_transfer_log(*, token: str, from_addr: str, to_addr: str, amount: int) -> dict:
    return {
        "address": token,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + "00" * 12 + from_addr.lower().replace("0x", ""),
            "0x" + "00" * 12 + to_addr.lower().replace("0x", ""),
        ],
        "data": "0x" + _pad_uint(amount),
    }


def _native_pool_key() -> PoolKey:
    """currency0 = native ETH (0x0 sorts first), currency1 = USDC."""
    pool_key = PoolKey(currency0=NATIVE_CURRENCY, currency1=USDC, fee=3000, tick_spacing=60)
    assert pool_key.currency0.lower() == NATIVE_CURRENCY
    assert pool_key.currency1.lower() == USDC.lower()
    return pool_key


def _native_pool_parser() -> UniswapV4ReceiptParser:
    return UniswapV4ReceiptParser(
        chain=CHAIN,
        pool_manager_address=POOL_MANAGER,
        position_manager_address=POSITION_MANAGER,
        pool_key_lookup=lambda pid, chain: _native_pool_key(),
    )


def _erc20_pool_parser() -> UniswapV4ReceiptParser:
    """An all-ERC-20 PoolKey (WETH/USDC) — no native leg."""
    pool_key = PoolKey(currency0=WETH, currency1=USDC, fee=500, tick_spacing=10)
    return UniswapV4ReceiptParser(
        chain=CHAIN,
        pool_manager_address=POOL_MANAGER,
        position_manager_address=POSITION_MANAGER,
        pool_key_lookup=lambda pid, chain: pool_key,
    )


# =============================================================================
# Case 1 — native-only LP_OPEN
# =============================================================================


class TestNativeOnlyOpen:
    def test_native_only_open_resolves_currencies_native_amount_none(self) -> None:
        """A native-only out-of-range mint (no ERC-20 deposit Transfer) resolves
        both currencies from the PoolKey; the native leg is ``None`` (for the
        runner stamp), the ERC-20 leg a measured ``0``."""
        parser = _native_pool_parser()
        receipt = {
            "transactionHash": "0xnativeonlyopen",
            "logs": [
                _modify_liquidity_log(liquidity_delta=1_000_000),
                _erc721_mint_log(token_id=TOKEN_ID),
                # NO ERC-20 deposit Transfer — native ETH moved via msg.value.
            ],
        }

        data = parser.extract_lp_open_data(receipt)

        assert data is not None, "native-only open must NOT drop (VIB-5119)"
        # Currencies resolved → the runner's _native_v4_open_eligible gate fires.
        assert data.currency0 == NATIVE_CURRENCY
        assert data.currency1 == USDC.lower()
        # Native leg unmeasured (None — runner stamps it post-mint); ERC-20 leg
        # is a measured 0 (genuinely zero this out-of-range native-only mint).
        assert data.amount0 is None
        assert data.amount1 == 0
        assert data.position_id == TOKEN_ID

    def test_native_only_open_drops_when_no_lookup_preserves_legacy_shape(self) -> None:
        """No pool_key_lookup configured (degraded/unit path) → preserve the
        legacy both-None / null-currency shape; do NOT regress to a drop."""
        parser = UniswapV4ReceiptParser(
            chain=CHAIN,
            pool_manager_address=POOL_MANAGER,
            position_manager_address=POSITION_MANAGER,
            # pool_key_lookup intentionally omitted
        )
        receipt = {
            "transactionHash": "0xnativeopen_nolookup",
            "logs": [
                _modify_liquidity_log(liquidity_delta=1_000_000),
                _erc721_mint_log(token_id=TOKEN_ID),
            ],
        }
        data = parser.extract_lp_open_data(receipt)
        assert data is not None
        assert data.amount0 is None
        assert data.amount1 is None
        assert data.currency0 is None
        assert data.currency1 is None

    def test_zero_deposit_erc20_only_pool_still_drops(self, caplog: pytest.LogCaptureFixture) -> None:
        """An all-ERC-20 PoolKey that landed ZERO deposit Transfers is not
        attributable (no native leg to excuse the absence) → drop, mirroring the
        close-side empty-observed protection."""
        parser = _erc20_pool_parser()
        receipt = {
            "transactionHash": "0xerc20open_nodeposit",
            "logs": [
                _modify_liquidity_log(liquidity_delta=1_000_000),
                _erc721_mint_log(token_id=TOKEN_ID),
            ],
        }
        with caplog.at_level(logging.WARNING, logger="almanak.connectors.uniswap_v4.receipt_parser"):
            data = parser.extract_lp_open_data(receipt)
        assert data is None
        joined = " ".join(rec.message for rec in caplog.records)
        assert "transfer_set_mismatch" in joined

    def test_two_sided_open_unaffected(self) -> None:
        """Regression: a two-sided native+USDC mint (USDC deposit observed) still
        resolves via the single-sided branch (amount0 observed)."""
        parser = _native_pool_parser()
        receipt = {
            "transactionHash": "0xtwosidednative",
            "logs": [
                _modify_liquidity_log(liquidity_delta=1_000_000),
                _erc721_mint_log(token_id=TOKEN_ID),
                _erc20_transfer_log(token=USDC, from_addr=WALLET, to_addr=POOL_MANAGER, amount=1_000_000_000),
            ],
        }
        data = parser.extract_lp_open_data(receipt)
        assert data is not None
        assert data.currency0 == NATIVE_CURRENCY
        assert data.currency1 == USDC.lower()
        # USDC (currency1) observed; native (currency0) leg None for the stamp.
        assert data.amount0 is None
        assert data.amount1 == 1_000_000_000


# =============================================================================
# Case 2 — native-only LP_CLOSE
# =============================================================================


class TestNativeOnlyClose:
    def test_native_only_close_books_event_native_leg_none(self) -> None:
        """A native-only burn (only raw ETH returned via TAKE_PAIR — NO ERC-20
        Transfer) books an LP_CLOSE event instead of a transfer_set_mismatch
        drop; the native leg is ``None`` (runner stamps the principal pre-burn),
        the ERC-20 leg a measured ``0``."""
        parser = _native_pool_parser()
        receipt = {
            "transactionHash": "0xnativeonlyclose",
            "logs": [
                _modify_liquidity_log(liquidity_delta=-1_000_000),
                # NO ERC-20 withdrawal Transfer — only raw ETH via TAKE_PAIR.
            ],
        }

        data = parser.extract_lp_close_data(receipt)

        assert data is not None, "native-only close must be booked, not dropped (VIB-5119)"
        assert data.source == "modify_liquidity"
        assert data.pool_address == POOL_ID_HEX
        assert data.currency0 == NATIVE_CURRENCY
        assert data.currency1 == USDC.lower()
        # Native principal leg None (runner fills it pre-burn); ERC-20 leg 0.
        assert data.amount0_collected is None
        assert data.amount1_collected == 0
        assert data.liquidity_removed == 1_000_000

    def test_genuine_erc20_empty_observed_still_drops(self, caplog: pytest.LogCaptureFixture) -> None:
        """No-regression guard: an all-ERC-20 PoolKey burn with NO withdrawal
        Transfer is still a real attribution failure → ``transfer_set_mismatch``
        drop. The native bypass must NOT weaken this gate globally."""
        parser = _erc20_pool_parser()
        receipt = {
            "transactionHash": "0xerc20emptyclose",
            "logs": [
                _modify_liquidity_log(liquidity_delta=-1_000_000),
                # NO withdrawal Transfer at all.
            ],
        }
        with caplog.at_level(logging.WARNING, logger="almanak.connectors.uniswap_v4.receipt_parser"):
            data = parser.extract_lp_close_data(receipt)
        assert data is None, "all-ERC-20 empty-observed burn must STILL drop (no regression)"
        joined = " ".join(rec.message for rec in caplog.records)
        assert "transfer_set_mismatch" in joined

    def test_native_close_with_erc20_leg_measures_it(self) -> None:
        """Two-sided native close (native + USDC observed) measures the ERC-20
        leg and leaves the native leg None — VIB-5117 path, unaffected."""
        parser = _native_pool_parser()
        receipt = {
            "transactionHash": "0xtwosidednativeclose",
            "logs": [
                _modify_liquidity_log(liquidity_delta=-1_000_000),
                _erc20_transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=2_500_000),
            ],
        }
        data = parser.extract_lp_close_data(receipt)
        assert data is not None
        assert data.amount0_collected is None  # native leg
        assert data.amount1_collected == 2_500_000  # USDC leg measured


# =============================================================================
# Sanity: NATIVE_CURRENCY ordering assumption
# =============================================================================


def test_native_currency_sorts_first() -> None:
    assert int(NATIVE_CURRENCY, 16) < int(USDC, 16)
    assert int(NATIVE_CURRENCY, 16) < int(WETH, 16)
