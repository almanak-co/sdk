"""VIB-4483 (P-V1-B): native-ETH currency-leg V4 pools are SUPPORTED at close.

V0 (VIB-4426) scope-limited V4 LP to ERC20-ERC20 pools and the close parser
RAISED ``UniswapV4UnsupportedPoolError`` for a PoolKey whose
``currency0 == 0x0000…0000`` (native ETH). VIB-4483 lifts that rejection.

The native leg is returned to the wallet as raw ETH (TAKE_PAIR) and emits NO
ERC-20 Transfer, so:

* The parser does NOT raise — it produces a valid ``LPCloseData``.
* The observed ERC-20 (currency1) leg is measured from its Transfer.
* The native (currency0) PRINCIPAL leg is ``None`` (unmeasured, Empty ≠ Zero)
  on ``amount0_collected`` — VIB-5117. The native ETH is returned via TAKE_PAIR
  with NO Transfer, so the receipt cannot measure it; stamping ``0`` would be a
  misattribution that understates realized PnL by the full native principal. The
  runner fills the real value pre-burn from a ``QueryV4PositionState`` read
  (``_stamp_v4_lp_close_native_principal``), mirroring the open-side native fill
  (VIB-4483) and the pre-burn fee stamp (``_stamp_v4_lp_close_fees``, VIB-4482).
"""

from __future__ import annotations

from almanak.connectors.uniswap_v4.receipt_parser import (
    EVENT_TOPICS,
    UniswapV4ReceiptParser,
)
from almanak.connectors.uniswap_v4.sdk import NATIVE_CURRENCY, PoolKey, _pad_int24, _pad_uint

CHAIN = "base"
POOL_MANAGER = "0x000000000004444c5dc75cB358380D2e3dE08A90"
POSITION_MANAGER = "0x7C5f5A4bBd8fD63184577525326123B519429bDc"
WETH = "0x4200000000000000000000000000000000000006"
WALLET = "0x1234567890abcdef1234567890abcdef12345678"
POOL_ID_HEX = "0x" + "cd" * 32
WETH_WITHDRAWN = 5 * 10**17  # 0.5 WETH returned to the wallet on close


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


def _erc20_withdraw_log(*, token: str, amount: int) -> dict:
    """A Transfer of ``token`` FROM the PoolManager to the wallet (close withdrawal)."""
    return {
        "address": token,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + "00" * 12 + POOL_MANAGER.lower().replace("0x", ""),
            "0x" + "00" * 12 + WALLET.replace("0x", ""),
        ],
        "data": "0x" + _pad_uint(amount),
    }


def _native_pool_parser() -> UniswapV4ReceiptParser:
    # currency0 = native ETH (0x0 sorts first), currency1 = WETH.
    native_eth_pool_key = PoolKey(
        currency0=NATIVE_CURRENCY,
        currency1=WETH,
        fee=3000,
        tick_spacing=60,
    )
    return UniswapV4ReceiptParser(
        chain=CHAIN,
        pool_manager_address=POOL_MANAGER,
        position_manager_address=POSITION_MANAGER,
        pool_key_lookup=lambda pid, chain: native_eth_pool_key,
    )


def test_native_eth_close_does_not_raise_and_measures_erc20_leg():
    """Native-ETH currency0 close produces a valid LPCloseData (VIB-4483).

    The ERC-20 (currency1 = WETH) leg is measured from its withdrawal Transfer;
    the native (currency0) principal leg is ``None`` here (no Transfer →
    unmeasured per Empty ≠ Zero, VIB-5117; the runner fills it pre-burn). The
    runner fills it pre-burn from a ``QueryV4PositionState`` read. Crucially:
    no raise.
    """
    parser = _native_pool_parser()
    receipt = {
        "transactionHash": "0xnativeclose",
        "logs": [
            _modify_liquidity_burn_log(liquidity_delta=-1_000_000),
            _erc20_withdraw_log(token=WETH, amount=WETH_WITHDRAWN),
        ],
    }

    data = parser.extract_lp_close_data(receipt)

    assert data is not None, "native-ETH close must NOT raise/drop (VIB-4483)"
    assert data.source == "modify_liquidity"
    assert data.pool_address == POOL_ID_HEX
    # currency0 = native, currency1 = WETH.
    assert data.currency0 == NATIVE_CURRENCY
    assert data.currency1 == WETH.lower()
    # Native principal leg = None (no Transfer → unmeasured, VIB-5117); WETH leg
    # measured from its transfer. The native leg is NOT a measured zero — that
    # would understate realized PnL by the full native principal; the runner
    # fills it pre-burn.
    assert data.amount0_collected is None
    assert data.amount1_collected == WETH_WITHDRAWN
    # Fees stay None (Empty != Zero) — V4 bundles fees; separation is VIB-4482.
    assert data.fees0 is None
    assert data.fees1 is None
    assert data.liquidity_removed == 1_000_000


def test_erc20_only_pool_still_measures_both_legs():
    """Sanity: an ERC20-ERC20 PoolKey close measures both observed legs."""
    usdc = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    # currency0 < currency1 sorted: WETH (0x42...) < USDC (0x83...).
    pool_key = PoolKey(currency0=WETH, currency1=usdc, fee=500, tick_spacing=10)
    parser = UniswapV4ReceiptParser(
        chain=CHAIN,
        pool_manager_address=POOL_MANAGER,
        position_manager_address=POSITION_MANAGER,
        pool_key_lookup=lambda pid, chain: pool_key,
    )
    receipt = {
        "transactionHash": "0xerc20close",
        "logs": [
            _modify_liquidity_burn_log(liquidity_delta=-1_000_000),
            _erc20_withdraw_log(token=WETH, amount=5 * 10**17),
            _erc20_withdraw_log(token=usdc, amount=1_000_000_000),
        ],
    }
    data = parser.extract_lp_close_data(receipt)
    assert data is not None
    assert data.source == "modify_liquidity"
    assert data.amount0_collected == 5 * 10**17
    assert data.amount1_collected == 1_000_000_000
