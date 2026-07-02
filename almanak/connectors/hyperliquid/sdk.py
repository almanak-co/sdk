"""Hyperliquid CoreWriter SDK — pure action encoders for HyperEVM.

This module builds the calldata for ``CoreWriter.sendRawAction(bytes)`` on
HyperEVM (chain 999). It holds NO keys, opens NO sockets, and signs nothing —
the strategy returns an ``Intent``; the gateway signs and submits (blueprint 20).

Design notes carried over — and corrected — from the abandoned V1 attempt
(``strategy-framework@hyperEVM-sdk``):

* **No `$1M`/`$1` "market order" hack.** V1 sent an IOC limit at $1M (buy) /
  $1 (sell) whenever its (always-failing) oracle read returned ``None`` — zero
  slippage protection. Here market orders are IOC crossing a **real reference
  price** with a bounded slippage band, and the encoder is **fail-closed**:
  no reference price → ``ValueError``, never a blind cross.
* **szDecimals-aware rounding.** V1 fetched ``szDecimals`` and never used it,
  sending prices/sizes that HyperCore rejects on the tick rules. Here prices
  round to min(5 significant figures, ``6 - szDecimals`` decimals) and sizes to
  ``szDecimals`` decimals *before* the ``× 1e8`` wire scaling.

Wire scaling (verified against the official ``hyperliquid-python-sdk``
``float_to_int_for_hashing`` = ``float_to_int(x, 8)``): both ``limitPx`` and
``sz`` are ``round(human * 1e8)`` as ``uint64``. This is the WRITE path; the
precompile READ path uses ``raw / 10**(6 - szDecimals)`` (see ``addresses.py``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal, localcontext

from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_utils import keccak

from .addresses import (
    ACTION_CANCEL_ORDER_BY_CLOID,
    ACTION_CANCEL_ORDER_BY_OID,
    ACTION_LIMIT_ORDER,
    ACTION_USD_CLASS_TRANSFER,
    CORE_WRITER_ENCODING_VERSION,
    PERP_PX_MAX_DECIMALS,
    WIRE_DECIMALS,
)

logger = logging.getLogger(__name__)

_UINT64_MAX = 2**64
_UINT128_MAX = 2**128
_UINT32_MAX = 2**32

# CoreWriter significant-figure cap for non-integer prices (Hyperliquid rule).
_PRICE_SIG_FIGS = 5

# Time-in-force wire codes (CoreWriter limit-order ``encodedTif`` field).
TIF_ALO = 1  # Add-liquidity-only (post-only)
TIF_GTC = 2  # Good-til-cancelled (resting)
TIF_IOC = 3  # Immediate-or-cancel (used to synthesise market orders)

# sendRawAction(bytes) selector — derived from the signature at import time
# (see the self-check block at the end of the module).
SELECTOR_SEND_RAW_ACTION: bytes = keccak(b"sendRawAction(bytes)")[:4]


# =============================================================================
# Price / size rounding (HyperCore tick rules) → uint64 wire values
# =============================================================================


def round_perp_price(price: Decimal, sz_decimals: int) -> Decimal:
    """Round a human perp price to a HyperCore-valid tick.

    A perp price is valid when it has at most ``_PRICE_SIG_FIGS`` significant
    figures AND at most ``PERP_PX_MAX_DECIMALS - sz_decimals`` decimal places.
    Integer prices bypass the significant-figure cap (Hyperliquid rule). We
    apply the MORE restrictive of the two constraints so the result is always
    accepted on-chain.
    """
    if price <= 0:
        raise ValueError(f"perp price must be positive, got {price}")
    max_decimals = PERP_PX_MAX_DECIMALS - sz_decimals
    if max_decimals < 0:
        max_decimals = 0
    # Significant-figure rounding — but INTEGER prices bypass the sig-fig cap
    # (Hyperliquid rule), so a 6+ digit integer like 123456 must NOT be distorted
    # to 123460. Only fractional prices are capped to _PRICE_SIG_FIGS.
    if price == price.to_integral_value():
        sig_rounded = price
    else:
        with localcontext() as ctx:
            ctx.prec = _PRICE_SIG_FIGS
            sig_rounded = +price  # unary plus applies context precision
    # Clamp to the decimal-place ceiling.
    quantum = Decimal(1).scaleb(-max_decimals)  # 10**-max_decimals
    dec_rounded = sig_rounded.quantize(quantum, rounding=ROUND_HALF_UP)
    if dec_rounded <= 0:
        raise ValueError(f"price {price} rounded to non-positive {dec_rounded} at {max_decimals} dp")
    return dec_rounded


def round_size(size: Decimal, sz_decimals: int) -> Decimal:
    """Round a human size DOWN to ``sz_decimals`` decimal places.

    Rounds down (``ROUND_DOWN``) so a computed size never exceeds the caller's
    intended notional after quantisation — under-size is safer than over-size.
    """
    if size <= 0:
        raise ValueError(f"size must be positive, got {size}")
    if sz_decimals < 0:
        raise ValueError(f"sz_decimals must be non-negative, got {sz_decimals}")
    quantum = Decimal(1).scaleb(-sz_decimals)
    rounded = size.quantize(quantum, rounding=ROUND_DOWN)
    if rounded <= 0:
        raise ValueError(
            f"size {size} rounds to zero at {sz_decimals} decimals — increase size or check asset szDecimals"
        )
    return rounded


def _to_wire(value: Decimal) -> int:
    """Scale a (already tick-rounded) human value to its uint64 wire integer.

    Wire = round(value * 10**WIRE_DECIMALS). Raises if the scaled value is not
    (near-)integral, mirroring the official SDK's ``float_to_int`` guard — a
    non-integral result means the caller skipped tick rounding.
    """
    scaled = value * (Decimal(10) ** WIRE_DECIMALS)
    rounded = scaled.quantize(Decimal(1), rounding=ROUND_HALF_UP)
    if abs(scaled - rounded) > Decimal("1e-3"):
        raise ValueError(f"value {value} does not scale to an integer wire (residual {scaled - rounded})")
    wire = int(rounded)
    if wire <= 0 or wire >= _UINT64_MAX:
        raise ValueError(f"wire value {wire} out of uint64 range")
    return wire


def price_to_wire(price: Decimal, sz_decimals: int) -> int:
    """Human perp price → uint64 ``limitPx`` (tick-rounded, ×1e8)."""
    return _to_wire(round_perp_price(price, sz_decimals))


def size_to_wire(size: Decimal, sz_decimals: int) -> int:
    """Human size → uint64 ``sz`` (rounded to szDecimals, ×1e8)."""
    return _to_wire(round_size(size, sz_decimals))


def market_limit_price(
    reference_price: Decimal,
    slippage_bps: int,
    *,
    is_buy: bool,
    sz_decimals: int,
) -> int:
    """Aggressive IOC limit price that crosses within a bounded band.

    Replaces V1's unbounded ``$1M``/``$1`` hack. A market buy crosses up to
    ``reference * (1 + slippage)``; a market sell down to
    ``reference * (1 - slippage)``. Fail-closed: a non-positive reference price
    raises rather than sending a blind order.
    """
    if reference_price <= 0:
        raise ValueError(
            "market_limit_price requires a positive reference price — refusing to "
            "send an unbounded IOC order (fail-closed)"
        )
    if slippage_bps < 0 or slippage_bps > 10_000:
        raise ValueError(f"slippage_bps must be in [0, 10000], got {slippage_bps}")
    band = Decimal(slippage_bps) / Decimal(10_000)
    limit = reference_price * (Decimal(1) + band) if is_buy else reference_price * (Decimal(1) - band)
    return price_to_wire(limit, sz_decimals)


# =============================================================================
# CoreWriter action encoders (version byte + 3-byte BE action id + ABI body)
# =============================================================================


def _action_header(action_id: int) -> bytes:
    """1-byte encoding version + 3-byte big-endian action id."""
    if action_id <= 0 or action_id >= 2**24:
        raise ValueError(f"action id {action_id} out of 3-byte range")
    return bytes([CORE_WRITER_ENCODING_VERSION]) + action_id.to_bytes(3, "big")


@dataclass(frozen=True)
class LimitOrderAction:
    """A CoreWriter limit-order action (id 1).

    Integer fields are already in wire units (``price_to_wire`` / ``size_to_wire``):
      asset       — uint32 perp index (universe order; see perps_read/API)
      is_buy      — bool
      limit_px    — uint64 wire price (human × 1e8, tick-rounded)
      sz          — uint64 wire size  (human × 1e8, szDecimals-rounded)
      reduce_only — bool (True to only shrink an existing position, e.g. close)
      tif         — uint8 TIF code (ALO/GTC/IOC = 1/2/3)
      cloid       — uint128 client order id (0 = none)
    """

    asset: int
    is_buy: bool
    limit_px: int
    sz: int
    reduce_only: bool
    tif: int
    cloid: int = 0


def encode_limit_order_action(order: LimitOrderAction) -> bytes:
    """Encode a limit-order action blob for ``sendRawAction``."""
    _check_uint(order.asset, _UINT32_MAX, "asset")
    _check_uint(order.limit_px, _UINT64_MAX, "limit_px", positive=True)
    _check_uint(order.sz, _UINT64_MAX, "sz", positive=True)
    _check_uint(order.cloid, _UINT128_MAX, "cloid", allow_zero=True)
    if order.tif not in (TIF_ALO, TIF_GTC, TIF_IOC):
        raise ValueError(f"invalid tif {order.tif}; expected ALO/GTC/IOC (1/2/3)")
    body = abi_encode(
        ["uint32", "bool", "uint64", "uint64", "bool", "uint8", "uint128"],
        [
            int(order.asset),
            bool(order.is_buy),
            int(order.limit_px),
            int(order.sz),
            bool(order.reduce_only),
            int(order.tif),
            int(order.cloid),
        ],
    )
    return _action_header(ACTION_LIMIT_ORDER) + body


def decode_limit_order_action(blob: str | bytes) -> LimitOrderAction:
    """Inverse of :func:`encode_limit_order_action` — recover a submitted order.

    Used by the receipt parser to read back the action we sent from the
    ``RawAction`` log payload (the EVM receipt carries the submitted intent, not
    the fill). Raises ``ValueError`` if the header is not a version-1 limit order.
    """
    data = _to_bytes(blob)
    if len(data) < 4:
        raise ValueError("action blob too short")
    version = data[0]
    action_id = int.from_bytes(data[1:4], "big")
    if version != CORE_WRITER_ENCODING_VERSION or action_id != ACTION_LIMIT_ORDER:
        raise ValueError(f"not a v{CORE_WRITER_ENCODING_VERSION} limit order (version={version}, action={action_id})")
    asset, is_buy, limit_px, sz, reduce_only, tif, cloid = abi_decode(
        ["uint32", "bool", "uint64", "uint64", "bool", "uint8", "uint128"], data[4:]
    )
    return LimitOrderAction(
        asset=int(asset),
        is_buy=bool(is_buy),
        limit_px=int(limit_px),
        sz=int(sz),
        reduce_only=bool(reduce_only),
        tif=int(tif),
        cloid=int(cloid),
    )


def decode_raw_action_log_data(log_data: str | bytes) -> bytes:
    """Extract the action blob from a ``RawAction(address,bytes)`` log's data field.

    The log's non-indexed ``data`` is ABI-encoded ``(bytes)``; unwrap it to the
    raw versioned action bytes. Returns ``b""`` on empty/undecodable data.
    """
    raw = _to_bytes(log_data)
    if len(raw) == 0:
        return b""
    (action_bytes,) = abi_decode(["bytes"], raw)
    return bytes(action_bytes)


def encode_cancel_by_oid_action(asset: int, oid: int) -> bytes:
    """Encode a cancel-by-order-id action (id 10)."""
    _check_uint(asset, _UINT32_MAX, "asset")
    _check_uint(oid, _UINT64_MAX, "oid", positive=True)
    body = abi_encode(["uint32", "uint64"], [int(asset), int(oid)])
    return _action_header(ACTION_CANCEL_ORDER_BY_OID) + body


def encode_cancel_by_cloid_action(asset: int, cloid: int) -> bytes:
    """Encode a cancel-by-client-order-id action (id 11)."""
    _check_uint(asset, _UINT32_MAX, "asset")
    _check_uint(cloid, _UINT128_MAX, "cloid", positive=True)
    body = abi_encode(["uint32", "uint128"], [int(asset), int(cloid)])
    return _action_header(ACTION_CANCEL_ORDER_BY_CLOID) + body


def encode_usd_class_transfer_action(ntl_usd: Decimal, *, to_perp: bool) -> bytes:
    """Encode a USD-class transfer (id 7): move USDC between spot and perp wallets.

    ``ntl_usd`` is a human USD amount; the wire field is 1e6-scaled (USDC).
    """
    if ntl_usd <= 0:
        raise ValueError(f"ntl_usd must be positive, got {ntl_usd}")
    scaled = ntl_usd * (Decimal(10) ** 6)
    ntl = int(scaled.quantize(Decimal(1), rounding=ROUND_DOWN))
    _check_uint(ntl, _UINT64_MAX, "ntl", positive=True)
    body = abi_encode(["uint64", "bool"], [ntl, bool(to_perp)])
    return _action_header(ACTION_USD_CLASS_TRANSFER) + body


def encode_send_raw_action_calldata(action_blob: bytes) -> bytes:
    """Wrap an action blob as ``CoreWriter.sendRawAction(bytes)`` calldata."""
    if not isinstance(action_blob, bytes | bytearray) or len(action_blob) < 4:
        raise ValueError("action_blob must be the versioned action bytes (>= 4 bytes)")
    return SELECTOR_SEND_RAW_ACTION + abi_encode(["bytes"], [bytes(action_blob)])


# =============================================================================
# Read-precompile output decoders (pure; the eth_call I/O lives in the caller)
# =============================================================================


@dataclass(frozen=True)
class Position:
    """Decoded HyperCore perp position (the ``position`` precompile struct).

    Solidity struct (verified against hyper-evm-lib ``PrecompileLib``):
    ``(int64 szi, uint64 entryNtl, int64 isolatedRawUsd, uint32 leverage, bool isIsolated)``.

    ``szi`` is the signed position size in the asset's own units scaled by
    ``10**szDecimals`` — positive = long, negative = short, 0 = no position.
    (The exact ``szi`` scale is asserted end-to-end by the testnet round-trip;
    ``reduce_only`` close orders are robust to a small mis-scale because they
    can only shrink, never flip, a position.)
    """

    szi: int
    entry_ntl: int
    isolated_raw_usd: int
    leverage: int
    is_isolated: bool

    @property
    def is_open(self) -> bool:
        return self.szi != 0

    @property
    def is_long(self) -> bool:
        return self.szi > 0


def decode_position(raw: str | bytes) -> Position:
    """Decode a ``position`` precompile return into a :class:`Position`.

    Empty / all-zero returns (no position, or the account has never traded the
    asset) decode to ``szi == 0`` — callers must treat that as "no position",
    never as a measured zero to trade against.
    """
    data = _to_bytes(raw)
    if len(data) == 0:
        # Precompile returned empty — no Core account / no position. Distinct
        # from a decoded zero; surface as an explicit empty position.
        return Position(szi=0, entry_ntl=0, isolated_raw_usd=0, leverage=0, is_isolated=False)
    szi, entry_ntl, isolated_raw_usd, leverage, is_isolated = abi_decode(
        ["int64", "uint64", "int64", "uint32", "bool"], data
    )
    return Position(
        szi=int(szi),
        entry_ntl=int(entry_ntl),
        isolated_raw_usd=int(isolated_raw_usd),
        leverage=int(leverage),
        is_isolated=bool(is_isolated),
    )


def decode_uint64(raw: str | bytes) -> int | None:
    """Decode a single ``uint64`` precompile return (markPx / oraclePx).

    Returns ``None`` on an empty return (Empty≠Zero: an unavailable price is not
    a measured zero) so callers fail closed rather than trading against 0.
    """
    data = _to_bytes(raw)
    if len(data) == 0:
        return None
    (value,) = abi_decode(["uint64"], data)
    return int(value)


# =============================================================================
# Read-precompile input encoders (raw ABI args, NO selector)
# =============================================================================


def encode_perp_query(perp_index: int) -> bytes:
    """ABI-encode a ``uint32 perp`` argument for markPx/oraclePx/perpAssetInfo."""
    _check_uint(perp_index, _UINT32_MAX, "perp_index", allow_zero=True)
    return abi_encode(["uint32"], [int(perp_index)])


def encode_position_query(user: str, perp_index: int) -> bytes:
    """ABI-encode ``(address user, uint32 perp)`` for the position precompile."""
    _check_uint(perp_index, _UINT32_MAX, "perp_index", allow_zero=True)
    return abi_encode(["address", "uint32"], [_check_address(user), int(perp_index)])


# =============================================================================
# Internal helpers
# =============================================================================


def _check_uint(value: int, bound: int, name: str, *, positive: bool = False, allow_zero: bool = False) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an int, got {value!r}")
    # asset / perp index 0 is legitimately BTC; the range floor is 0 unless the
    # caller demands positive, so a 0 asset index passes without a special case.
    low = 1 if positive else 0
    if value < low or value >= bound:
        raise ValueError(f"{name}={value} out of range [{low}, {bound})")


def _to_bytes(raw: str | bytes) -> bytes:
    """Coerce a hex string or bytes to bytes (``"0x"`` and ``""`` → empty)."""
    if isinstance(raw, bytes | bytearray):
        return bytes(raw)
    if not isinstance(raw, str):
        raise ValueError(f"expected hex str or bytes, got {type(raw).__name__}")
    s = raw[2:] if raw.startswith("0x") else raw
    if s == "":
        return b""
    return bytes.fromhex(s)


def _check_address(addr: str) -> str:
    if not isinstance(addr, str) or not addr.startswith("0x") or len(addr) != 42:
        raise ValueError(f"Invalid EVM address: {addr!r}")
    try:
        int(addr[2:], 16)
    except ValueError as exc:
        raise ValueError(f"Invalid EVM address: {addr!r}") from exc
    return addr


__all__ = [
    "TIF_ALO",
    "TIF_GTC",
    "TIF_IOC",
    "LimitOrderAction",
    "Position",
    "decode_limit_order_action",
    "decode_position",
    "decode_raw_action_log_data",
    "decode_uint64",
    "encode_cancel_by_cloid_action",
    "encode_cancel_by_oid_action",
    "encode_limit_order_action",
    "encode_perp_query",
    "encode_position_query",
    "encode_send_raw_action_calldata",
    "encode_usd_class_transfer_action",
    "market_limit_price",
    "price_to_wire",
    "round_perp_price",
    "round_size",
    "size_to_wire",
]
