"""GMX V2 pending-order read — pure calldata + decode helpers (VIB-5116).

The GMX V2 teardown gap this closes: a MARKET_INCREASE order sends its collateral
to the **OrderVault** and waits for a keeper to execute it into a position. On an
Anvil fork (and any time a keeper is slow / absent) the order stays **pending**;
it is not a position, so ``Reader.getAccountPositions`` never sees it and teardown
strands the committed collateral. This module reads the wallet's pending orders so
teardown can surface them.

Two on-chain read paths, both **pure** (calldata build + ABI decode only — the
gateway-routed ``eth_call`` is executed by the caller, mirroring
``perps_read.py``; no provider is opened here, gateway-boundary rule):

* **Robust detection (stable ABI).** ``DataStore.getBytes32Count`` /
  ``getBytes32ValuesAt`` over the account's ``ACCOUNT_ORDER_LIST`` set answer
  "does this wallet have pending orders, and what are their keys?" with a
  drift-proof signature. Detection NEVER depends on the (versioned) ``Order.Props``
  struct — a future struct change can therefore never silently make teardown miss
  a strand (contrast the VIB-5289 ``Position.Props`` drift that silently broke
  ``perps_read`` decoding).
* **Detail (versioned struct).** ``Reader.getAccountOrders`` returns the full
  ``Order.Props[]`` so we can surface each order's market / collateral token /
  committed amount / type. The struct is version-sensitive, so the decode is
  best-effort: if it drifts and fails, detection still holds via the key list and
  the collateral detail is simply reported unmeasured (Empty ≠ Zero).

Verified against real Arbitrum chain bytes on a managed-Anvil fork
(tests/reports/tdverify-vib5116-gmx-pending-*.md).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_utils import function_signature_to_4byte_selector, keccak, to_checksum_address

logger = logging.getLogger(__name__)

# GMX V2 stores USD values with 30 decimals (mirrors perps_read / perps_valuer).
GMX_USD_DECIMALS = 30

# GMX OrderType enum (Order.sol). MARKET_INCREASE is the strand case (collateral
# committed to the OrderVault awaiting keeper execution), but ANY pending order
# holds capital/fees in the vault, so teardown surfaces every pending order.
ORDER_TYPE_MARKET_SWAP = 0
ORDER_TYPE_LIMIT_SWAP = 1
ORDER_TYPE_MARKET_INCREASE = 2
ORDER_TYPE_LIMIT_INCREASE = 3
ORDER_TYPE_MARKET_DECREASE = 4
ORDER_TYPE_LIMIT_DECREASE = 5
ORDER_TYPE_STOP_LOSS_DECREASE = 6
ORDER_TYPE_LIQUIDATION = 7

# --- Selectors (pure derivation; no provider) --------------------------------
_GET_ACCOUNT_ORDERS_SELECTOR = function_signature_to_4byte_selector("getAccountOrders(address,address,uint256,uint256)")
_GET_BYTES32_COUNT_SELECTOR = function_signature_to_4byte_selector("getBytes32Count(bytes32)")
_GET_BYTES32_VALUES_AT_SELECTOR = function_signature_to_4byte_selector("getBytes32ValuesAt(bytes32,uint256,uint256)")

# GMX DataStore set key for an account's order list:
#   keccak256(abi.encode(ACCOUNT_ORDER_LIST, account))
# where the base key ACCOUNT_ORDER_LIST is itself GMX Keys.sol's
#   keccak256(abi.encode("ACCOUNT_ORDER_LIST"))
# — the string is ABI-ENCODED before hashing, NOT hashed as raw UTF-8 bytes.
# This distinction is load-bearing and invisible to a mocked unit test: the wrong
# ``keccak(text=...)`` slot reads a real, always-empty DataStore set, so detection
# silently returns "no orders" on-chain (VIB-5116 real-fork proof caught it — the
# correct slot returned count=6 where keccak(text=...) returned 0). Verified on a
# real Arbitrum DataStore. (Note: sdk.py's legacy ACCOUNT_POSITION_LIST fallback
# uses keccak(text=...) but is masked by a Reader.getAccountPositionCount primary;
# the order path has no such primary, so the slot must be exact.)
_ACCOUNT_ORDER_LIST_HASH = keccak(abi_encode(["string"], ["ACCOUNT_ORDER_LIST"]))

# Max pending orders read in one range call. GMX Reader tolerates a range wider
# than the set, returning only the existing entries — so a single [0, N) read
# replaces a count pre-query for the detail path.
MAX_ORDER_RANGE = 100

# Order.Props ABI as deployed on Arbitrum/Avalanche GMX SyntheticsReader
# (Order.sol). VERIFIED byte-for-byte against real Arbitrum chain bytes on a
# managed-Anvil fork (VIB-5116 real-fork proof; re-encoding the decode reproduces
# the exact 1056-byte return). The output type is built from the known struct so a
# field typo can't silently corrupt the decode (same discipline as
# ``perps_read._POSITION_PROPS``). THREE non-obvious facts the real bytes proved,
# each of which silently breaks a naive decode:
#
#   1. ``getAccountOrders`` returns ``(bytes32 orderKey, Order.Props)[]`` — each
#      element PAIRS the order key with the struct, NOT a bare ``Order.Props[]``.
#      The order key therefore rides inline (no separate key read needed).
#   2. ``Order.Props`` carries a trailing ``bytes32[] _dataList`` (4th member) —
#      omitting it shifts the ``addresses`` head-offset and the decode fails.
#   3. ``Numbers`` has **12** ``uint256`` fields, not 11 — an 11-field decode reads
#      ``isLong`` at the wrong offset (VIB-5289 ``Position.Props`` drift, again).
#
#   Addresses = (account, receiver, cancellationReceiver, callbackContract,
#                uiFeeReceiver, market, initialCollateralToken, address[] swapPath)
#   Numbers   = (orderType, decreasePositionSwapType, sizeDeltaUsd,
#                initialCollateralDeltaAmount, triggerPrice, acceptablePrice,
#                executionFee, callbackGasLimit, minOutputAmount, updatedAtTime,
#                + two trailing time/block/reserved slots)  [12 total]
#   Flags     = (isLong, shouldUnwrapNativeToken, isFrozen, autoCancel)
_ORDER_ADDRESSES = "(address,address,address,address,address,address,address,address[])"
_ORDER_NUMBERS = "(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256)"
_ORDER_FLAGS = "(bool,bool,bool,bool)"
_ORDER_PROPS = f"({_ORDER_ADDRESSES},{_ORDER_NUMBERS},{_ORDER_FLAGS},bytes32[])"
# Each element is (orderKey, Props).
_GET_ACCOUNT_ORDERS_OUTPUT = f"(bytes32,{_ORDER_PROPS})[]"

# Field indices: element = (key, props); props = (addresses, numbers, flags, dataList).
_ELEM_KEY = 0
_ELEM_PROPS = 1
_PROPS_ADDRESSES = 0
_PROPS_NUMBERS = 1
_PROPS_FLAGS = 2
_ADDR_MARKET = 5
_ADDR_INITIAL_COLLATERAL_TOKEN = 6
_NUM_ORDER_TYPE = 0
_NUM_SIZE_DELTA_USD = 2
_NUM_INITIAL_COLLATERAL_DELTA_AMOUNT = 3
_NUM_EXECUTION_FEE = 6
_NUM_UPDATED_AT_TIME = 9  # unix seconds the order was last created/updated (GMX cancel time-gate anchor)
_FLAG_IS_LONG = 0


@dataclass(frozen=True)
class PendingOrder:
    """A single decoded pending GMX V2 order (the detail path).

    ``order_key`` is set only when the order was surfaced via the key list
    (``getBytes32ValuesAt``); the ``getAccountOrders`` struct does not carry the
    key. All raw integer fields keep their on-chain scaling (USD = 30 decimals,
    collateral = collateral-token decimals).
    """

    market: str
    initial_collateral_token: str
    initial_collateral_delta_amount: int
    size_delta_usd: int
    order_type: int
    execution_fee: int
    is_long: bool
    order_key: str = ""
    # Unix seconds the order was created/updated on-chain. ``0`` ⇒ unmeasured (a
    # key-only stub, or an older struct without the field) — callers treat 0 as
    # "age unknown" and fail-closed (defer cancellation), never as "age 0" (which
    # would look infinitely old and cancel too early). GMX gates account-initiated
    # cancellation on ``now - updated_at_time >= REQUEST_EXPIRATION_TIME`` (VIB-5568).
    updated_at_time: int = 0


@dataclass(frozen=True)
class PendingOrdersResult:
    """Outcome of a pending-order read (Empty ≠ Zero).

    Attributes:
        orders: The pending orders measured (detail from ``getAccountOrders`` when
            it decoded; otherwise key-only stubs from the DataStore key list).
        order_keys: The raw ``bytes32`` order keys (``0x``-prefixed) — the
            drift-proof identity surface.
        ok: ``True`` iff the read was MEASURED. ``False`` ⇒ UNMEASURED
            (gateway/RPC error, undecodable count) — the caller must fail-closed,
            never treat it as "no pending orders".
        measured_count: The on-chain pending-order COUNT when the (stable-ABI)
            count read succeeded (informational). ``None`` ⇒ the count read itself
            was unmeasured. ``0`` ⇒ measured-empty (no orders). Note: the sentinel
            decision is NOT made on this field — it is made on ``ok`` plus the
            deployment's declared connector usage (framework ``residual_discovery``).
        truncated: ``True`` when ``measured_count`` exceeded the single-read window
            (``MAX_ORDER_RANGE``) so ``orders``/``order_keys`` are only the first
            window — the aggregate strand still fires on ``count > 0``, but a key
            ABSENT from this partial set must NOT be treated as "closed" (it may lie
            beyond the window); callers fail-closed on a not-found key when truncated.
        error: Populated on ``ok=False`` (or a non-fatal note, e.g. the detail
            decode drifted but the key-based detection held, or a truncation note).
    """

    orders: list[PendingOrder] = field(default_factory=list)
    order_keys: list[str] = field(default_factory=list)
    ok: bool = True
    measured_count: int | None = None
    truncated: bool = False
    error: str | None = None


def account_order_list_key(account: str) -> bytes:
    """The DataStore ``ACCOUNT_ORDER_LIST`` set key for ``account`` (bytes32)."""
    return keccak(abi_encode(["bytes32", "address"], [_ACCOUNT_ORDER_LIST_HASH, to_checksum_address(account)]))


def build_order_count_calldata(account: str) -> str:
    """Calldata for ``DataStore.getBytes32Count(accountOrderListKey)`` (0x hex)."""
    args = abi_encode(["bytes32"], [account_order_list_key(account)])
    return "0x" + (_GET_BYTES32_COUNT_SELECTOR + args).hex()


def build_order_keys_calldata(account: str, start: int = 0, end: int = MAX_ORDER_RANGE) -> str:
    """Calldata for ``DataStore.getBytes32ValuesAt(key, start, end)`` (0x hex)."""
    args = abi_encode(["bytes32", "uint256", "uint256"], [account_order_list_key(account), start, end])
    return "0x" + (_GET_BYTES32_VALUES_AT_SELECTOR + args).hex()


def build_account_orders_calldata(data_store: str, account: str, start: int = 0, end: int = MAX_ORDER_RANGE) -> str:
    """Calldata for ``Reader.getAccountOrders(dataStore, account, start, end)`` (0x hex)."""
    args = abi_encode(
        ["address", "address", "uint256", "uint256"],
        [to_checksum_address(data_store), to_checksum_address(account), start, end],
    )
    return "0x" + (_GET_ACCOUNT_ORDERS_SELECTOR + args).hex()


def _to_bytes(blob: Any) -> bytes | None:
    """Coerce a hex/bytes eth_call return into bytes; ``None`` when unusable."""
    if blob is None:
        return None
    if isinstance(blob, bytes | bytearray):
        return bytes(blob)
    if isinstance(blob, str):
        text = blob[2:] if blob[:2].lower() == "0x" else blob
        if text == "":
            return b""
        try:
            return bytes.fromhex(text)
        except ValueError:
            return None
    return None


def decode_uint(blob: Any) -> int | None:
    """Decode a single ``uint256`` eth_call return; ``None`` on any failure."""
    raw = _to_bytes(blob)
    if raw is None or len(raw) < 32:
        return None
    try:
        return int(abi_decode(["uint256"], raw)[0])
    except Exception:  # noqa: BLE001 — undecodable ⇒ unmeasured
        return None


def decode_bytes32_array(blob: Any) -> list[str] | None:
    """Decode a ``bytes32[]`` eth_call return into ``0x`` hex keys; ``None`` on failure."""
    raw = _to_bytes(blob)
    if raw is None:
        return None
    if raw == b"":
        return []
    try:
        decoded = abi_decode(["bytes32[]"], raw)[0]
    except Exception:  # noqa: BLE001
        return None
    return ["0x" + bytes(k).hex() for k in decoded]


def decode_account_orders(blob: Any, order_keys: list[str] | None = None) -> list[PendingOrder] | None:
    """Decode a ``Reader.getAccountOrders`` return into :class:`PendingOrder`.

    The return is ``(bytes32 orderKey, Order.Props)[]`` (verified on-chain), so the
    order key rides inline. Returns ``None`` when the blob is missing or the
    (version-sensitive) struct does not decode — the caller treats that as *detail
    unavailable* and falls back to the drift-proof key list, never as "no orders".
    ``order_keys`` is accepted for API parity but the inline key is authoritative.
    """
    raw = _to_bytes(blob)
    if raw is None:
        return None
    if raw == b"":
        return []
    try:
        decoded = abi_decode([_GET_ACCOUNT_ORDERS_OUTPUT], raw)[0]
    except Exception:  # noqa: BLE001 — struct drift ⇒ detail unavailable (detection still holds via keys)
        logger.debug("GMX getAccountOrders decode failed (struct drift?)", exc_info=True)
        return None

    orders: list[PendingOrder] = []
    for idx, element in enumerate(decoded):
        try:
            key_bytes = element[_ELEM_KEY]
            props = element[_ELEM_PROPS]
            addresses, numbers, flags = props[_PROPS_ADDRESSES], props[_PROPS_NUMBERS], props[_PROPS_FLAGS]
            inline_key = "0x" + bytes(key_bytes).hex()
            fallback_key = order_keys[idx] if order_keys and idx < len(order_keys) else ""
            orders.append(
                PendingOrder(
                    market=to_checksum_address(addresses[_ADDR_MARKET]),
                    initial_collateral_token=to_checksum_address(addresses[_ADDR_INITIAL_COLLATERAL_TOKEN]),
                    initial_collateral_delta_amount=int(numbers[_NUM_INITIAL_COLLATERAL_DELTA_AMOUNT]),
                    size_delta_usd=int(numbers[_NUM_SIZE_DELTA_USD]),
                    order_type=int(numbers[_NUM_ORDER_TYPE]),
                    execution_fee=int(numbers[_NUM_EXECUTION_FEE]),
                    updated_at_time=int(numbers[_NUM_UPDATED_AT_TIME]),
                    is_long=bool(flags[_FLAG_IS_LONG]),
                    order_key=inline_key if int(inline_key, 16) != 0 else fallback_key,
                )
            )
        except Exception:  # noqa: BLE001 — a malformed row ⇒ the WHOLE detail decode is unreliable
            # Do NOT skip-and-continue: a partial list would silently DROP the failed
            # row's pending order (the exact silent-strand class VIB-5116 fixes). Fail
            # the whole detail decode to ``None`` so the caller falls back to the
            # drift-proof key list / fail-closed evidence (Empty != Zero).
            logger.debug("GMX getAccountOrders row %d decode failed — failing the whole decode", idx, exc_info=True)
            return None
    return orders


__all__ = [
    "GMX_USD_DECIMALS",
    "MAX_ORDER_RANGE",
    "ORDER_TYPE_MARKET_INCREASE",
    "PendingOrder",
    "PendingOrdersResult",
    "account_order_list_key",
    "build_account_orders_calldata",
    "build_order_count_calldata",
    "build_order_keys_calldata",
    "decode_account_orders",
    "decode_bytes32_array",
    "decode_uint",
]
