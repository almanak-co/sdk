"""VIB-3887 — Uniswap V3 receipt parser populates current_tick on LP_OPEN.

The framework consumer (``_apply_lp_open`` in observability/position_events.py)
derives ``in_range`` from ``LPOpenData.current_tick`` once it lands. This test
fences the producer side: the ``UniswapV3ReceiptParser.extract_lp_open_data``
method must populate ``current_tick`` from any Swap event emitted by the same
pool address in the same receipt — which is the canonical Almanak LP_OPEN
shape (atomic swap-then-mint).

Pure NPM.mint receipts without a Swap leg leave ``current_tick=None``;
``in_range`` stays None until the gateway adds a slot0() lookup.
"""

from __future__ import annotations

from almanak.connectors.uniswap_v3.receipt_parser import (
    EVENT_TOPICS,
    UniswapV3ReceiptParser,
)


# ──────────────────────────────────────────────────────────────────────────
# Constants — synthetic but layout-correct
# ──────────────────────────────────────────────────────────────────────────


def _hex32(value: int, width: int = 64) -> str:
    """Pack a non-negative int into a left-padded hex string of ``width`` chars."""
    return f"{value & ((1 << (width * 4)) - 1):0{width}x}"


def _signed_hex32(value: int) -> str:
    """Two's-complement encode a signed int24 (or smaller) into a 32-byte hex slot."""
    return _hex32(value if value >= 0 else (1 << 256) + value)


_POOL = "0xC6962004f452bE9203591991D15f6b388e09E8D0".lower()  # WETH/USDC 500 on Arbitrum
_NPM = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88".lower()  # canonical NPM
_TOKEN_ID = 5463956
_TICK_LOWER = -199960
_TICK_UPPER = -197960
_CURRENT_TICK = -198500
_SQRT_PRICE_X96 = 1234567890123
_LIQUIDITY_AT_SWAP = 928906698000


def _topic(value: int) -> str:
    """Indexed-int24 topic encoding (right-aligned in 32 bytes)."""
    return "0x" + _signed_hex32(value)


def _addr_topic(addr: str) -> str:
    return "0x" + ("0" * 24) + addr.lower().removeprefix("0x")


def _pool_mint_log() -> dict:
    """Pool Mint log emitted by the WETH/USDC pool when NPM mints."""
    return {
        "address": _POOL,
        "topics": [
            EVENT_TOPICS["Mint"],
            _addr_topic(_NPM),  # owner = NPM
            _topic(_TICK_LOWER),
            _topic(_TICK_UPPER),
        ],
        "data": "0x" + _hex32(int(0.000891 * 1e18)) * 4,  # placeholder payload
    }


def _swap_log(tick: int = _CURRENT_TICK, pool: str = _POOL) -> dict:
    """Pool Swap log carrying the current ``tick`` post-swap."""
    # Data layout: amount0 | amount1 | sqrtPriceX96 | liquidity | tick.
    # All 32-byte slots. amount0 < 0 in this case (token1 → token0).
    data = (
        "0x"
        + _signed_hex32(-2294332)  # amount0
        + _signed_hex32(891556839636852)  # amount1
        + _hex32(_SQRT_PRICE_X96)
        + _hex32(_LIQUIDITY_AT_SWAP)
        + _signed_hex32(tick)
    )
    return {
        "address": pool,
        "topics": [
            EVENT_TOPICS["Swap"],
            _addr_topic("0x000000000000000000000000000000000000Beef"),  # sender
            _addr_topic("0x0000000000000000000000000000000000C0FFEE"),  # recipient
        ],
        "data": data,
    }


def _increase_liquidity_log(
    token_id: int = _TOKEN_ID,
    liquidity: int = 928906698473,
    amount0: int = 891556839636852,
    amount1: int = 2294332,
) -> dict:
    """NPM IncreaseLiquidity log — actually emits the per-position values."""
    token_id_topic = "0x" + _hex32(token_id)
    data = "0x" + _hex32(liquidity, 64) + _hex32(amount0, 64) + _hex32(amount1, 64)
    return {
        "address": _NPM,
        "topics": [EVENT_TOPICS["IncreaseLiquidity"], token_id_topic],
        "data": data,
    }


# ──────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────


def test_current_tick_populated_when_swap_event_in_receipt():
    """Canonical Almanak LP_OPEN: swap-then-mint atomically. The Swap
    event's ``tick`` is the live tick at mint time."""
    receipt = {
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 458_443_424,
        "status": 1,
        "logs": [
            _swap_log(),  # balance swap before mint
            _pool_mint_log(),  # pool fires Mint
            _increase_liquidity_log(),  # NPM fires IncreaseLiquidity
        ],
    }
    parser = UniswapV3ReceiptParser(chain="arbitrum")
    lp_data = parser.extract_lp_open_data(receipt)

    assert lp_data is not None
    assert lp_data.position_id == _TOKEN_ID
    assert lp_data.tick_lower == _TICK_LOWER
    assert lp_data.tick_upper == _TICK_UPPER
    assert lp_data.current_tick == _CURRENT_TICK, (
        f"VIB-3887: parser must propagate current_tick={_CURRENT_TICK} from "
        f"the Swap event; got {lp_data.current_tick}"
    )
    # Sanity: the in_range derivation downstream sees a populated bracket.
    assert lp_data.tick_lower < lp_data.current_tick < lp_data.tick_upper


def test_current_tick_none_when_no_swap_event():
    """Pure NPM.mint() without a balancing swap → current_tick stays None.
    The framework consumer leaves in_range undecided until the gateway
    adds a slot0() lookup (separate ticket)."""
    receipt = {
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 458_443_424,
        "status": 1,
        "logs": [
            _pool_mint_log(),
            _increase_liquidity_log(),
        ],
    }
    parser = UniswapV3ReceiptParser(chain="arbitrum")
    lp_data = parser.extract_lp_open_data(receipt)

    assert lp_data is not None
    assert lp_data.position_id == _TOKEN_ID
    assert lp_data.current_tick is None


def test_current_tick_ignores_swap_from_unrelated_pool():
    """Swap on a different pool address must NOT pollute current_tick.
    Multi-position bundles in the same tx routinely include unrelated
    swaps."""
    other_pool = "0xdeadBeefCafe000000000000000000000000Cafe"
    receipt = {
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 458_443_424,
        "status": 1,
        "logs": [
            _swap_log(tick=42, pool=other_pool.lower()),  # unrelated pool
            _pool_mint_log(),
            _increase_liquidity_log(),
        ],
    }
    parser = UniswapV3ReceiptParser(chain="arbitrum")
    lp_data = parser.extract_lp_open_data(receipt)

    assert lp_data is not None
    assert lp_data.current_tick is None, (
        "Swap on an unrelated pool must NOT be picked up — would mis-attribute "
        "current_tick across positions."
    )


def test_current_tick_uses_latest_swap_when_multiple():
    """Multi-step routing can emit several Swap events on the same pool.
    The post-mint tick is the LAST one chronologically (logs come back in
    log-index order)."""
    receipt = {
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 458_443_424,
        "status": 1,
        "logs": [
            _swap_log(tick=-199500),  # earlier swap
            _swap_log(tick=_CURRENT_TICK),  # later swap — wins
            _pool_mint_log(),
            _increase_liquidity_log(),
        ],
    }
    parser = UniswapV3ReceiptParser(chain="arbitrum")
    lp_data = parser.extract_lp_open_data(receipt)

    assert lp_data is not None
    assert lp_data.current_tick == _CURRENT_TICK
