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
    ACTION_SPOT_SEND,
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


def spot_wei(amount: Decimal, wei_decimals: int) -> int:
    """Scale a human spot-token amount to its integer ``wei`` (10**weiDecimals).

    HyperCore spot tokens carry their amount in the token's OWN ``weiDecimals``
    (e.g. USDC on HyperCore is 8-decimal — NOT the 6-decimal EVM USDC ERC-20, and
    NOT the 1e6 ``ntl`` scale ``usdClassTransfer`` uses). Confusing these scales
    silently sends 100× / 0.01× the intended amount, so ``spot_send`` requires the
    caller to pass the token's ``weiDecimals`` explicitly rather than inheriting a
    hard-coded default (mirrors how :func:`size_to_wire` takes ``sz_decimals``).

    Rounds DOWN so a computed amount never exceeds the caller's intent after
    quantisation (under-send is safer than over-send). Fail-closed: a non-positive
    amount or a resulting zero wei raises rather than sending a no-op / negative.
    """
    if amount <= 0:
        raise ValueError(f"spot send amount must be positive, got {amount}")
    if wei_decimals < 0:
        raise ValueError(f"wei_decimals must be non-negative, got {wei_decimals}")
    scaled = amount * (Decimal(10) ** wei_decimals)
    wei = int(scaled.quantize(Decimal(1), rounding=ROUND_DOWN))
    if wei <= 0:
        raise ValueError(
            f"amount {amount} rounds to zero wei at {wei_decimals} decimals — increase amount or check weiDecimals"
        )
    if wei >= _UINT64_MAX:
        raise ValueError(f"spot send wei {wei} out of uint64 range")
    return wei


def encode_spot_send_action(destination: str, token: int, wei: int) -> bytes:
    """Encode a spot-send action (id 6): transfer a HyperCore spot token.

    CoreWriter ``spotSend`` body is ABI ``(address destination, uint64 token,
    uint64 wei)`` — ``token`` is the HyperCore spot-token INDEX (e.g. USDC = 0),
    ``wei`` is the amount in that token's ``weiDecimals`` (see :func:`spot_wei`).
    Sending to a token's system address (``0x2000…00 | index``) is detected by
    HyperCore as a HyperCore→HyperEVM bridge and credits the sender's EVM wallet;
    sending to any other address is a plain spot transfer. A Safe (a contract
    that cannot ECDSA-sign an L1 withdraw) uses this to move HyperCore funds
    programmatically (VIB-5615).
    """
    dest = _check_address(destination)
    _check_uint(token, _UINT64_MAX, "token", allow_zero=True)
    _check_uint(wei, _UINT64_MAX, "wei", positive=True)
    body = abi_encode(["address", "uint64", "uint64"], [dest, int(token), int(wei)])
    return _action_header(ACTION_SPOT_SEND) + body


def encode_send_raw_action_calldata(action_blob: bytes) -> bytes:
    """Wrap an action blob as ``CoreWriter.sendRawAction(bytes)`` calldata."""
    if not isinstance(action_blob, bytes | bytearray) or len(action_blob) < 4:
        raise ValueError("action_blob must be the versioned action bytes (>= 4 bytes)")
    return SELECTOR_SEND_RAW_ACTION + abi_encode(["bytes"], [bytes(action_blob)])


def build_spot_send_calldata(destination: str, token: int, amount: Decimal, wei_decimals: int) -> bytes:
    """Build full ``CoreWriter.sendRawAction`` calldata for a spot-token transfer.

    Convenience over :func:`spot_wei` → :func:`encode_spot_send_action` →
    :func:`encode_send_raw_action_calldata` for a human ``amount`` of a HyperCore
    spot ``token`` (index) with the token's ``wei_decimals``. Returns the calldata
    for a ``CoreWriter`` (``0x3333…3333``) call; the gateway signs and submits.
    """
    wei = spot_wei(amount, wei_decimals)
    blob = encode_spot_send_action(destination, token, wei)
    return encode_send_raw_action_calldata(blob)


def build_usdc_withdraw_calldata(amount: Decimal) -> bytes:
    """Build ``sendRawAction`` calldata to bridge USDC HyperCore→HyperEVM (VIB-5615).

    A spot-send of USDC (token index 0, weiDecimals 8) to the USDC system address
    (``0x2000…0000``) is detected by HyperCore as a bridge request and credits the
    SENDER's HyperEVM wallet with the linked ERC-20 (funds appear in ~seconds).
    This is the programmatic HyperCore→L1 withdraw path a Safe (which cannot
    ECDSA-sign an L1 ``withdraw3``) uses to move parked HyperCore funds back
    on-chain. Token index / weiDecimals / system address come from the connector's
    own constants so calldata can't drift from a hand-typed literal.
    """
    from .addresses import USDC_SPOT_SYSTEM_ADDRESS, USDC_SPOT_TOKEN_INDEX, USDC_SPOT_WEI_DECIMALS

    return build_spot_send_calldata(
        USDC_SPOT_SYSTEM_ADDRESS,
        USDC_SPOT_TOKEN_INDEX,
        amount,
        USDC_SPOT_WEI_DECIMALS,
    )


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


@dataclass(frozen=True)
class AccountMarginSummary:
    """Decoded HyperCore cross-margin account summary (``accountMarginSummary``).

    Solidity struct (verified against hyper-evm-lib ``PrecompileLib``):
    ``(int64 accountValue, uint64 marginUsed, uint64 ntlPos, int64 rawUsd)``.

    All four fields are HyperCore perp USD scaled by ``1e6`` (the same convention
    as the position struct's ``entryNtl``).

    MONEY-PATH SCALE + LAYOUT — CONFIRMED against LIVE MAINNET (2026-07-02,
    chain 999): the ``0x080F`` precompile was read back-to-back against
    ``clearinghouseState.marginSummary`` from ``api.hyperliquid.xyz`` (human
    units) for TWO independent live cross-margin accounts and every field matched
    to sub-cent (residual is market-maker drift between the two snapshots, not a
    scale error):
      * ``0x7fdafde5…517d1`` ($23.9M, 27 cross positions): precompile
        accountValue ``23,942,354.3341`` vs API ``23,942,354.3918``; marginUsed,
        ntlPos, rawUsd all matched to &lt;0.001%.
      * ``0x31ca8395…974b`` ($3.0M): precompile accountValue ``3,001,008.76`` vs
        API ``3,001,056.26``; the other three fields matched to &lt;0.01%.
    The account equity identity ``accountValue = rawUsd + Σ signed_mark_notional``
    was also verified exactly on the $23.9M account. ✅ 1e6 USD, field order as
    above.

    Note the INPUT arg order is INVERTED relative to the position precompile:
    ``position`` takes ``(address user, uint32 perp)`` but
    ``accountMarginSummary`` takes ``(uint32 perpDexIndex, address user)`` — see
    :func:`encode_account_margin_query`.
    """

    account_value: int  # 1e6 USD — total cross-account equity (marked-to-market)
    margin_used: int  # 1e6 USD — margin currently committed across cross positions
    ntl_pos: int  # 1e6 USD — total |notional| of open cross positions
    raw_usd: int  # 1e6 USD — signed net USD basis (accountValue = rawUsd + Σ signed mark ntl)


def decode_account_margin_summary(raw: str | bytes) -> AccountMarginSummary | None:
    """Decode an ``accountMarginSummary`` (0x080F) precompile return.

    Returns ``None`` on an empty return (Empty≠Zero: an unmeasured account is not
    a measured all-zero account). A wallet with no HyperCore cross account /
    reverting read yields ``None`` so callers fall back to the PnL-only value
    rather than fabricating collateral.
    """
    data = _to_bytes(raw)
    if len(data) == 0:
        return None
    account_value, margin_used, ntl_pos, raw_usd = abi_decode(["int64", "uint64", "uint64", "int64"], data)
    return AccountMarginSummary(
        account_value=int(account_value),
        margin_used=int(margin_used),
        ntl_pos=int(ntl_pos),
        raw_usd=int(raw_usd),
    )


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


def encode_account_margin_query(user: str, perp_dex_index: int = 0) -> bytes:
    """ABI-encode ``(uint32 perpDexIndex, address user)`` for accountMarginSummary.

    Note the arg order is INVERTED versus :func:`encode_position_query`: the
    ``accountMarginSummary`` precompile takes ``perpDexIndex`` FIRST then
    ``user`` (verified against hyper-evm-lib ``PrecompileLib`` and live — the
    ``(address, uint32)`` order reverts with ``PrecompileError``). ``perpDexIndex``
    is 0 for the standard first perp DEX.
    """
    _check_uint(perp_dex_index, _UINT32_MAX, "perp_dex_index", allow_zero=True)
    return abi_encode(["uint32", "address"], [int(perp_dex_index), _check_address(user)])


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
    "AccountMarginSummary",
    "LimitOrderAction",
    "Position",
    "build_spot_send_calldata",
    "build_usdc_withdraw_calldata",
    "decode_account_margin_summary",
    "decode_limit_order_action",
    "decode_position",
    "decode_raw_action_log_data",
    "decode_uint64",
    "encode_account_margin_query",
    "encode_cancel_by_cloid_action",
    "encode_cancel_by_oid_action",
    "encode_limit_order_action",
    "encode_perp_query",
    "encode_position_query",
    "encode_send_raw_action_calldata",
    "encode_spot_send_action",
    "encode_usd_class_transfer_action",
    "market_limit_price",
    "price_to_wire",
    "round_perp_price",
    "round_size",
    "size_to_wire",
    "spot_wei",
]
