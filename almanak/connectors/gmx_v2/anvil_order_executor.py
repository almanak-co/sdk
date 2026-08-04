"""Managed-Anvil execution for GMX V2 keeper-settled orders.

Production GMX orders are executed by an authorized keeper with signed oracle
payloads. A historical Anvil fork has neither the keeper process nor a matching
off-chain oracle archive. This module reproduces the same on-chain execution
entrypoint without weakening production behavior:

* every mutation is routed through :class:`GatewayWeb3Provider`;
* the caller must explicitly name the ``anvil`` network;
* the exact submitted order key must still belong to the strategy wallet;
* controller and keeper authority are verified against the forked RoleStore;
* prices come from the gateway price service. Collateral tokens are keyed by
  address and scaled by measured on-chain decimals; a market's INDEX token is
  keyed by the MARKET instead — base symbol plus declared decimals — because a
  GMX synthetic index token has no contract to answer either question
  (ALM-3108);
* Oracle state, impersonation, and temporary balances are cleaned up.

The gateway rejects all Anvil mutation methods on mainnet, providing a second
boundary beyond the connector-level network guard.
"""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal

import grpc
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_typing import HexStr
from eth_utils import function_signature_to_4byte_selector, keccak, to_checksum_address
from web3 import Web3
from web3.types import RPCEndpoint

from almanak.config.connectors import connectors_config_from_env
from almanak.connectors._strategy_base.rpc import (
    decode_revert_data,
    extract_revert_reason,
    looks_like_revert,
)
from almanak.connectors.gmx_v2.adapter import GMX_V2_ADDRESSES
from almanak.connectors.gmx_v2.addresses import index_price_symbol, index_token_decimals
from almanak.connectors.gmx_v2.receipt_parser import (
    _EVENT_LOG_DATA_ABI_TYPE,
    GMXv2EventType,
    GMXv2ReceiptParser,
)
from almanak.connectors.gmx_v2.teardown_reads import read_pending_orders
from almanak.framework.utils.grpc_utils import is_transient_grpc_error
from almanak.framework.web3.gateway_provider import GatewayWeb3Provider
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)

_GMX_USD_DECIMALS = 30
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
_TX_RECEIPT_TIMEOUT_SECONDS = 30

# Cold-fork keeper execution can exceed GatewayWeb3Provider's default 30s
# deadline: estimating OrderHandler.executeOrder pulls hundreds of cold
# storage slots through the fork's upstream RPC one call at a time, and the
# gateway additionally retries transient upstream failures internally (up to
# ~3 x 30s). The client deadline must sit above that whole envelope, or a
# slow first estimate surfaces as an opaque gRPC DEADLINE_EXCEEDED.
_COLD_FORK_RPC_TIMEOUT_SECONDS = 120.0

# Anvil caches upstream state fetched during a call even when the call itself
# times out, so a timed-out estimate leaves the fork warmer than it found it;
# bounded retries converge instead of thrashing. Budget math: worst case is
# attempts x _COLD_FORK_RPC_TIMEOUT_SECONDS = 240s, which must leave headroom
# inside the connector's 360s AsyncSettlementPolicy timeout for oracle
# seeding, executeOrder, and outcome verification — do not raise attempts
# without rechecking that envelope.
_ESTIMATE_GAS_ATTEMPTS = 2

_CONTROLLER_ROLE = keccak(abi_encode(["string"], ["CONTROLLER"]))
_ORDER_KEEPER_ROLE = keccak(abi_encode(["string"], ["ORDER_KEEPER"]))

_GET_ADDRESS_SIGNATURES = {
    "oracle": "oracle()",
    "role_store": "roleStore()",
    "data_store": "dataStore()",
}
_GET_MARKET_SIGNATURE = "getMarket(address,address)"
_GET_ROLE_MEMBER_COUNT_SIGNATURE = "getRoleMemberCount(bytes32)"
_GET_ROLE_MEMBERS_SIGNATURE = "getRoleMembers(bytes32,uint256,uint256)"
_HAS_ROLE_SIGNATURE = "hasRole(address,bytes32)"
_DECIMALS_SIGNATURE = "decimals()"
_GET_TOKENS_WITH_PRICES_COUNT_SIGNATURE = "getTokensWithPricesCount()"
_SET_PRIMARY_PRICE_SIGNATURE = "setPrimaryPrice(address,(uint256,uint256))"
_SET_TIMESTAMPS_SIGNATURE = "setTimestamps(uint256,uint256)"
_CLEAR_ALL_PRICES_SIGNATURE = "clearAllPrices()"
_EXECUTE_ORDER_SIGNATURE = "executeOrder(bytes32,(address[],address[],bytes[]))"


class GmxAnvilOrderExecutionError(RuntimeError):
    """Raised when the managed fork cannot safely execute a pending order."""


class GmxAnvilOrderRejectedError(GmxAnvilOrderExecutionError):
    """The venue refused THIS order. The fork itself is fine (VIB-6438).

    Distinct from its parent because the two need opposite operator responses.
    The parent means "this fork cannot execute orders at all" — no keeper role,
    wrong network, unreadable dependencies. This one means the plumbing worked
    and GMX declined this specific order.

    GMX declines an order in **two** shapes, and both belong here:

    * the keeper transaction is mined and REVERTS
      (:class:`GmxAnvilTransactionRevertedError`); and
    * the keeper transaction **SUCCEEDS** and GMX cancels or freezes the order
      inside it — the *dominant* shape for a market order, covering
      acceptable-price bounds, open-interest caps and collateral floors
      (``_verify_execution_outcome``).

    Classifying only the revert would leave the common case reported as
    ``INFRASTRUCTURE_UNSUPPORTED``, which is the exact defect this ticket exists
    to fix. Neither shape is retryable; the split exists to make the failure
    legible, not to resubmit it.
    """


class GmxAnvilTransactionRevertedError(GmxAnvilOrderRejectedError):
    """A keeper transaction for the ORDER was mined and reverted on-chain.

    Raised only for ``OrderHandler.executeOrder``. A revert in the harness
    oracle setup/cleanup (``setPrimaryPrice`` / ``setTimestamps`` /
    ``clearAllPrices``) is NOT an order rejection — the venue never saw the
    order — so those raise
    :class:`GmxAnvilHarnessTransactionRevertedError` instead. Telling an operator
    "the venue rejected this order" because our own oracle plumbing failed is the
    same mislabel as the one this ticket fixes, pointed the other way.
    """


class GmxAnvilHarnessTransactionRevertedError(GmxAnvilOrderExecutionError):
    """A HARNESS oracle transaction was mined and reverted on-chain (VIB-6438).

    Deliberately NOT a :class:`GmxAnvilOrderRejectedError`: the venue never saw
    the order, so calling this an order rejection would be the mislabel above,
    reversed. But it is still a definitive on-chain answer, and it needs its own
    type rather than the bare parent for one reason — ``_is_transient_execution_error``
    falls through to a substring scan over ``_TRANSIENT_ERROR_MARKERS`` for any
    exception it does not recognise, and the raised message now embeds the replay
    diagnosis, which quotes node error text verbatim ("error sending request",
    "Request timeout", …).

    Without this class a mined, deterministic harness revert whose diagnosis
    merely *mentions* a transport phrase is retried until the barrier's budget
    runs out, and the operator is then handed a timeout instead of the revert —
    the precise defect this ticket fixes, displaced onto the harness lane.
    """


@dataclass(frozen=True)
class GmxAnvilOrderExecutionResult:
    """Measured result of one managed-fork execution pass."""

    ok: bool
    executed_order_keys: tuple[str, ...] = ()
    transaction_hashes: tuple[str, ...] = ()
    execution_receipts: tuple[dict[str, Any], ...] = ()
    reason: str | None = None
    # True when the failure is a transient cold-fork/upstream class the caller
    # may retry within its own budget; False = the caller must not retry.
    transient: bool = False
    # True when a keeper transaction was mined and reverted — the venue rejected
    # THIS order at THIS block (VIB-6438). Mutually exclusive with ``transient``:
    # a mined revert is a definitive answer, never a transport blip. Non-retryable
    # exactly like a structural failure; it is reported separately so the operator
    # is not told their infrastructure is unsupported.
    order_rejected: bool = False

    def __post_init__(self) -> None:
        if self.transient and self.order_rejected:
            raise ValueError("GmxAnvilOrderExecutionResult cannot be both transient and order_rejected")


@dataclass(frozen=True)
class _GmxDependencies:
    order_handler: str
    oracle: str
    role_store: str
    data_store: str
    reader: str


def _selector(signature: str) -> bytes:
    return function_signature_to_4byte_selector(signature)


def _calldata(signature: str, types: list[str] | None = None, values: list[Any] | None = None) -> str:
    encoded = abi_encode(types or [], values or [])
    return "0x" + (_selector(signature) + encoded).hex()


def _rpc(provider: GatewayWeb3Provider, method: str, params: list[Any]) -> Any:
    response = provider.make_request(RPCEndpoint(method), params)
    error = response.get("error")
    if error:
        message = error.get("message", str(error)) if isinstance(error, dict) else str(error)
        raise GmxAnvilOrderExecutionError(f"{method} failed: {message}")
    if "result" not in response:
        raise GmxAnvilOrderExecutionError(f"{method} returned no result")
    return response["result"]


def _eth_call(provider: GatewayWeb3Provider, to: str, data: str) -> Any:
    return _rpc(provider, "eth_call", [{"to": to_checksum_address(to), "data": data}, "latest"])


def _decode_address(raw: Any, label: str) -> str:
    try:
        decoded = abi_decode(["address"], bytes.fromhex(str(raw).removeprefix("0x")))[0]
        address = to_checksum_address(decoded)
    except Exception as exc:
        raise GmxAnvilOrderExecutionError(f"Could not decode GMX {label} address") from exc
    if address.lower() == _ZERO_ADDRESS:
        raise GmxAnvilOrderExecutionError(f"GMX {label} address is zero")
    return address


def _decode_uint(raw: Any, label: str) -> int:
    try:
        return int(abi_decode(["uint256"], bytes.fromhex(str(raw).removeprefix("0x")))[0])
    except Exception as exc:
        raise GmxAnvilOrderExecutionError(f"Could not decode GMX {label}") from exc


def _read_address_getter(provider: GatewayWeb3Provider, contract: str, name: str) -> str:
    signature = _GET_ADDRESS_SIGNATURES[name]
    return _decode_address(_eth_call(provider, contract, _calldata(signature)), name)


def _load_dependencies(provider: GatewayWeb3Provider, chain: str) -> _GmxDependencies:
    addresses = GMX_V2_ADDRESSES.get(chain.lower())
    if addresses is None:
        raise GmxAnvilOrderExecutionError(f"GMX V2 managed-order execution is not configured for {chain}")

    order_handler = to_checksum_address(addresses["order_handler"])
    data_store = _read_address_getter(provider, order_handler, "data_store")
    configured_data_store = to_checksum_address(addresses["data_store"])
    if data_store != configured_data_store:
        raise GmxAnvilOrderExecutionError(
            f"GMX OrderHandler DataStore mismatch: handler={data_store} configured={configured_data_store}"
        )

    return _GmxDependencies(
        order_handler=order_handler,
        oracle=_read_address_getter(provider, order_handler, "oracle"),
        role_store=_read_address_getter(provider, order_handler, "role_store"),
        data_store=data_store,
        reader=to_checksum_address(addresses["synthetics_reader"]),
    )


def _has_role(provider: GatewayWeb3Provider, role_store: str, account: str, role: bytes) -> bool:
    raw = _eth_call(
        provider,
        role_store,
        _calldata(_HAS_ROLE_SIGNATURE, ["address", "bytes32"], [to_checksum_address(account), role]),
    )
    try:
        return bool(abi_decode(["bool"], bytes.fromhex(str(raw).removeprefix("0x")))[0])
    except Exception as exc:
        raise GmxAnvilOrderExecutionError("Could not decode GMX RoleStore.hasRole result") from exc


def _find_order_keeper(provider: GatewayWeb3Provider, role_store: str) -> str:
    count_raw = _eth_call(
        provider,
        role_store,
        _calldata(_GET_ROLE_MEMBER_COUNT_SIGNATURE, ["bytes32"], [_ORDER_KEEPER_ROLE]),
    )
    count = _decode_uint(count_raw, "ORDER_KEEPER member count")
    if count <= 0:
        raise GmxAnvilOrderExecutionError("GMX RoleStore has no ORDER_KEEPER members")

    members_raw = _eth_call(
        provider,
        role_store,
        _calldata(
            _GET_ROLE_MEMBERS_SIGNATURE,
            ["bytes32", "uint256", "uint256"],
            [_ORDER_KEEPER_ROLE, 0, count],
        ),
    )
    try:
        members = tuple(
            to_checksum_address(member)
            for member in abi_decode(["address[]"], bytes.fromhex(str(members_raw).removeprefix("0x")))[0]
        )
    except Exception as exc:
        raise GmxAnvilOrderExecutionError("Could not decode GMX ORDER_KEEPER members") from exc
    if not members:
        raise GmxAnvilOrderExecutionError("GMX RoleStore returned an empty ORDER_KEEPER member list")

    keeper = members[0]
    if not _has_role(provider, role_store, keeper, _ORDER_KEEPER_ROLE):
        raise GmxAnvilOrderExecutionError(f"Enumerated GMX keeper {keeper} does not hold ORDER_KEEPER")
    return keeper


@dataclass(frozen=True)
class _MarketOracleTokens:
    """One market's oracle tokens, split by how each can be PRICED (ALM-3108).

    ``Reader.getMarket`` already returns ``indexToken`` and the two collateral
    tokens as separate fields; an earlier revision collapsed all three into one
    set and priced every member by its raw on-chain address. That works only
    while every index token happens to be a real ERC-20. GMX *synthetic* index
    tokens — BTC, DOGE, LTC, XRP, ATOM and NEAR on Arbitrum, LTC on Avalanche —
    are identifier addresses with **no deployed contract**: ``decimals()``
    returns nothing and no token registry maps the address to a price. Six of
    fifteen Arbitrum markets could not settle on a managed fork as a result.

    The distinction is therefore load-bearing, not cosmetic:

    * ``index_token`` is an ORACLE KEY only. Its price and decimals come from
      the market (symbol + ``GMX_V2_INDEX_TOKEN_DECIMALS``), never from a call
      to the address itself.
    * ``collateral_tokens`` are real ERC-20s and keep the address route —
      address price lookup plus a measured on-chain ``decimals()``.

    A collateral equal to the index token is dropped from ``collateral_tokens``:
    it is one oracle key with one price, and the index route is the one that
    holds for every market.
    """

    index_token: str
    collateral_tokens: tuple[str, ...]


def _read_market_tokens(
    provider: GatewayWeb3Provider, dependencies: _GmxDependencies, market: str
) -> _MarketOracleTokens:
    raw = _eth_call(
        provider,
        dependencies.reader,
        _calldata(
            _GET_MARKET_SIGNATURE,
            ["address", "address"],
            [dependencies.data_store, to_checksum_address(market)],
        ),
    )
    try:
        _market_token, index_token, long_token, short_token = abi_decode(
            ["(address,address,address,address)"],
            bytes.fromhex(str(raw).removeprefix("0x")),
        )[0]
    except Exception as exc:
        raise GmxAnvilOrderExecutionError(f"Could not decode GMX market {market}") from exc

    if str(index_token).lower() == _ZERO_ADDRESS:
        # A zero index token means this address is a SWAP pool, not a perp
        # market (VIB-6155 found exactly such an address mislabelled as
        # arbitrum:OP/USD). Executing a position order against it cannot fill,
        # so say which market is wrong rather than seeding a partial oracle.
        raise GmxAnvilOrderExecutionError(
            f"GMX market {market} reports no index token (indexToken == 0); it is not a position market"
        )
    index = to_checksum_address(index_token)
    collaterals = {
        to_checksum_address(token) for token in (long_token, short_token) if str(token).lower() != _ZERO_ADDRESS
    }
    collaterals.discard(index)
    return _MarketOracleTokens(index_token=index, collateral_tokens=tuple(sorted(collaterals)))


def _read_token_decimals(provider: GatewayWeb3Provider, token: str) -> int:
    decimals = _decode_uint(
        _eth_call(provider, token, _calldata(_DECIMALS_SIGNATURE)),
        f"token decimals for {token}",
    )
    if decimals > _GMX_USD_DECIMALS:
        raise GmxAnvilOrderExecutionError(
            f"Token {token} has {decimals} decimals, above GMX's {_GMX_USD_DECIMALS}-decimal USD scale"
        )
    return decimals


def _gateway_usd_price(gateway_client: Any, chain: str, token: str, label: str) -> Decimal:
    """One measured USD price from the gateway, or raise. Never substitutes.

    ``stale`` is advisory here, never fatal (VIB-6491). This module only runs
    against a managed Anvil fork (network guard in
    :func:`execute_pending_orders_on_anvil`, plus the gateway rejecting Anvil
    mutation methods on mainnet), and on a fork the flag measures the harness,
    not the feed: the Chainlink ``updatedAt`` is frozen at the forked block
    while the staleness clock is live wall time, so ANY fork pinned longer than
    the 1-hour threshold flags every aggregate stale — treating that as fatal
    gave pinned forks a hard shelf life and destroyed reproducibility of
    chain-state-dependent defects (it cost VIB-6437 its only reproduction
    block). The price layer itself already treats fork staleness as expected —
    the Chainlink source returns the price at reduced confidence rather than
    failing. A genuinely unavailable feed still fails loudly: it surfaces as a
    gateway error or the invalid/non-positive guards below, never as a quiet
    substitution.

    Known limitation (VIB-6518): the aggregate ``stale`` boolean is a lossy OR —
    the REST sources also set it when serving a cached price after an upstream
    outage, and nothing on the wire attributes the flag to a source. Until the
    gateway exposes per-source staleness, that cache-fallback case passes here
    with the same warning instead of failing. Accepted on this Anvil-only path;
    do not widen further, and narrow to Chainlink-attributed staleness once
    VIB-6518 lands.
    """
    response = gateway_client.market.GetPrice(gateway_pb2.PriceRequest(token=token, quote="USD", chain=chain.lower()))
    if bool(getattr(response, "stale", False)):
        logger.warning(
            "Gateway price for GMX oracle token %s is stale; expected on a pinned Anvil fork "
            "(fork-frozen feed timestamp vs live wall clock) — proceeding with the measured price (VIB-6491)",
            label,
        )
    try:
        price = Decimal(str(response.price))
    except (InvalidOperation, ValueError) as exc:
        raise GmxAnvilOrderExecutionError(f"Gateway returned an invalid price for GMX oracle token {label}") from exc
    if not price.is_finite() or price <= 0:
        raise GmxAnvilOrderExecutionError(f"Gateway returned a non-positive price for GMX oracle token {label}")
    return price


def _scale_price_bounds(price: Decimal, decimals: int, label: str) -> tuple[int, int]:
    """Scale a USD price to GMX's 30-decimal oracle bounds."""
    if decimals < 0 or decimals > _GMX_USD_DECIMALS:
        raise GmxAnvilOrderExecutionError(
            f"GMX oracle token {label} has {decimals} decimals, outside GMX's {_GMX_USD_DECIMALS}-decimal USD scale"
        )
    scaled = price * (Decimal(10) ** (_GMX_USD_DECIMALS - decimals))
    minimum = int(scaled.to_integral_value(rounding=ROUND_FLOOR))
    maximum = int(scaled.to_integral_value(rounding=ROUND_CEILING))
    if minimum <= 0 or maximum <= 0:
        raise GmxAnvilOrderExecutionError(f"Scaled GMX oracle price for {label} is non-positive")
    return minimum, maximum


def _read_price_bounds(gateway_client: Any, provider: GatewayWeb3Provider, chain: str, token: str) -> tuple[int, int]:
    """Oracle bounds for a real ERC-20 oracle token, by address (collateral path).

    Unchanged behaviour: the long/short tokens of every GMX market are deployed
    ERC-20s, so both their price and their decimals are readable from the
    address itself. Only the INDEX token needs the market-keyed route below.
    """
    price = _gateway_usd_price(gateway_client, chain, token, token)
    return _scale_price_bounds(price, _read_token_decimals(provider, token), token)


def _index_price_bounds(gateway_client: Any, chain: str, market: str) -> tuple[int, int]:
    """Oracle bounds for a market's INDEX token, without touching its address (ALM-3108).

    Both inputs are keyed by the MARKET, which is metadata this SDK already
    ships and trusts elsewhere: the base symbol from ``GMX_V2_MARKETS`` (the
    same route ``acceptable_price`` uses) and the decimals from
    ``GMX_V2_INDEX_TOKEN_DECIMALS``. A synthetic index token therefore never has
    to answer for itself.

    Known limitation (ALM-3119): for a market whose index token *is* a real
    ERC-20, this trades a chain-sourced datum (the ``indexToken`` address
    ``Reader.getMarket`` returned) for a table-sourced one (the market's label).
    A mislabelled ``GMX_V2_MARKETS`` row — the VIB-6155 class — would therefore
    seed the label's asset rather than the market's. Bounded to fork tests: this
    module refuses any network but ``anvil``, and catalogue identity is asserted
    by ``tests/audit/test_gmx_v2_market_identity.py``.

    Every failure raises. An index price that cannot be resolved is *unmeasured*
    (``Empty ≠ Zero``): seeding a guessed or zero price onto a fork's oracle
    would let an order fill at a fabricated price and certify a lifecycle that
    never really happened, which is strictly worse than a failed run.
    """
    symbol = index_price_symbol(chain, market)
    if symbol is None:
        raise GmxAnvilOrderExecutionError(
            f"GMX market {market} is not listed for {chain}; its index token has no price symbol"
        )
    decimals = index_token_decimals(chain, market)
    if decimals is None:
        raise GmxAnvilOrderExecutionError(f"GMX market {market} on {chain} has no declared index-token decimals")
    label = f"{symbol} (index of market {market})"
    return _scale_price_bounds(_gateway_usd_price(gateway_client, chain, symbol, label), decimals, label)


def _latest_block_timestamp(provider: GatewayWeb3Provider) -> int:
    block = _rpc(provider, "eth_getBlockByNumber", ["latest", False])
    if not isinstance(block, dict) or "timestamp" not in block:
        raise GmxAnvilOrderExecutionError("Latest Anvil block did not include a timestamp")
    timestamp = block["timestamp"]
    return int(timestamp, 16) if isinstance(timestamp, str) else int(timestamp)


def _normalize_order_key(order_key: str) -> str:
    try:
        key = bytes.fromhex(str(order_key).strip().removeprefix("0x"))
    except ValueError as exc:
        raise GmxAnvilOrderExecutionError(f"Invalid GMX order key: {order_key!r}") from exc
    if len(key) != 32 or not any(key):
        raise GmxAnvilOrderExecutionError(f"Invalid GMX order key: {order_key!r}")
    return "0x" + key.hex()


# Anvil surfaces its own upstream (fork-backend) fetch failures as JSON-RPC
# errors with these shapes; they are the same transient cold-fork class as a
# timeout — the missing state is refetched on the next attempt.
_TRANSIENT_ERROR_MARKERS = (
    "Request timeout",
    "failed to get storage",
    "failed to get account",
    "error sending request",
)


def _is_transient_execution_error(exc: Exception) -> bool:
    """True for the shapes a transient cold-fork RPC failure can take."""
    # A mined revert is a definitive on-chain answer, so it is never transient —
    # checked first because the decoded revert reason is now part of the message
    # and a contract's own error text must never be pattern-matched as a
    # transport marker below (VIB-6438). BOTH mined-revert types are listed:
    # the harness one is not an order rejection, but it is just as definitive,
    # and its message embeds the same node text.
    if isinstance(exc, GmxAnvilOrderRejectedError | GmxAnvilHarnessTransactionRevertedError):
        return False
    if isinstance(exc, grpc.RpcError):
        # Defer to the shared classifier rather than a local code list. The
        # local list admitted only DEADLINE_EXCEEDED, so a gateway rate limit
        # (RESOURCE_EXHAUSTED — self-healing, and the server states exactly
        # when) was reported as structural: the caller mapped transient=False
        # to INFRASTRUCTURE_UNSUPPORTED and the settlement barrier gave up
        # immediately instead of waiting inside its 360s budget (ALM-3025).
        # Retrying is safe because the caller re-reads the pending-order set
        # first and only executes keys still on-chain — an order settled by an
        # earlier attempt is filtered out, never re-executed.
        return is_transient_grpc_error(exc)
    # The gateway maps upstream timeouts, and Anvil maps fork-backend fetch
    # failures, to JSON-RPC error responses that _rpc re-raises as
    # GmxAnvilOrderExecutionError.
    if not isinstance(exc, GmxAnvilOrderExecutionError):
        return False
    message = str(exc)
    return any(marker in message for marker in _TRANSIENT_ERROR_MARKERS)


# ---------------------------------------------------------------------------
# Mined-revert diagnosis (VIB-6438)
# ---------------------------------------------------------------------------
#
# A managed-Anvil keeper failure used to surface as gas numbers alone, so the
# one datum that identifies the cause — the revert reason — was discarded at the
# raise site and the mechanism had to be reconstructed from source. Anvil is the
# one environment with guaranteed full trace access, so the replay probe below
# is free here.
#
# EVERY function in this section is best-effort and MUST NOT raise: they run
# inside an error path, and an exception here would mask the real failure with a
# diagnostic-implementation bug. That is strictly worse than no diagnosis.


@dataclass(frozen=True)
class _ReplayProbe:
    """Three-way outcome of one ``eth_call`` re-simulation.

    Three states, not two, because ``GatewayWeb3Provider.make_request`` does NOT
    raise for most transport failures — it collapses them into a JSON-RPC error
    object with code ``-32603`` and RETURNS it (``gateway_provider.py:132-139``).
    Reading any error object as a revert therefore reports a dropped channel or
    an upstream timeout as a measured venue rejection.

    ``Empty ≠ Zero``: an unmeasured replay is not a measured non-revert, and it
    is not a measured revert either. ``transport`` says only "no answer".
    """

    outcome: Literal["success", "revert", "transport"]
    reason: str | None = None
    raw: str | None = None
    error: str | None = None

    def describe(self) -> str:
        detail = self.reason or "no decodable reason"
        return f"{detail} (raw={_format_raw_payload(self.raw)})" if self.raw else detail


# The cap is in WHOLE 32-byte words, and a cut is always announced (VIB-6483).
# The previous bound was a flat ``raw[:74]`` — ``0x`` + 4-byte selector + exactly
# ONE word — so every custom error with more than one argument was silently cut
# to its first operand, with no ellipsis and no length: a truncated payload read
# as a complete one. That cost the answer to VIB-6437 on this diagnostic's first
# real-fork use: ``InsufficientGasForCancellation(uint256,uint256)`` surfaced with
# the measured gas and without the threshold it failed against.
#
# ``raw`` is the ONLY place an operand ever reaches the operator —
# ``decode_revert_data`` reports a custom error as its selector alone — so a cut
# here is a lost measurement, not a shorter message.
#
# 16 words covers every custom error GMX defines many times over; the cap exists
# only so a pathological ``Error(string)`` cannot flood a log line. Cutting on a
# word boundary keeps whatever is shown ABI-decodable.
_RAW_MAX_WORDS = 16
_RAW_HEX_CAP = 8 + _RAW_MAX_WORDS * 64  # 4-byte selector + N whole 32-byte words


def _format_raw_payload(raw: str) -> str:
    """``raw``, cut only on a 32-byte-word boundary and never silently.

    Best-effort like every function in this section: pure string arithmetic, so
    it cannot raise on a malformed or odd-length payload.
    """
    blob = raw[2:] if raw[:2].lower() == "0x" else raw
    if len(blob) <= _RAW_HEX_CAP:
        return raw
    # Announce the cut AND the true size, so a reader can tell a truncated
    # payload from a complete one and knows how much was dropped.
    #
    # Known limitation (VIB-6485): on an ODD-length blob this floors, so the
    # reported total can equal the shown size and stop conveying how much was
    # dropped — the `…` still does. Not reachable from production: _replay_once
    # sets `raw` only from `0x`-prefixed RPC data, and ABI revert data is
    # even-length.
    return f"0x{blob[:_RAW_HEX_CAP]}… truncated, {len(blob) // 2} bytes total"


# Gas exhaustion is a MEASURED execution outcome, but it does not look like one:
# on Anvil 1.5.1 the response is ``{"code": -32603, "message": "EVM error
# OutOfGas"}`` — no revert data, no ``revert`` substring, and the *same*
# ``-32603`` the gateway uses for genuine transport failures. Every arm of
# ``looks_like_revert`` misses it, so it must be recognised by message.
#
# An exact-phrase ALLOWLIST, deliberately, not a substring scan. The gateway
# embeds raw upstream text into transport error messages
# (``rpc_service.py`` ``f"HTTP {status}: {error_text}"``), so ``"outofgas" in
# message`` would classify an upstream 5xx body that merely mentions gas as an
# executed revert — reporting a lost answer as a measured one, which is the
# inversion this module exists to prevent. An unknown vendor spelling instead
# reads as transport, i.e. unmeasured: the safe direction (VIB-6481).
_OUT_OF_GAS_MESSAGES = frozenset({"evm error outofgas", "out of gas", "outofgas"})


def _is_out_of_gas(message: str) -> bool:
    """Whether a failed ``eth_call`` EXECUTED and exhausted its gas (VIB-6438)."""
    return " ".join(message.split()).lower() in _OUT_OF_GAS_MESSAGES


def _replay_once(provider: GatewayWeb3Provider, call: dict[str, str], block: str) -> _ReplayProbe:
    """One ``eth_call`` at ``block``, classified three ways."""
    response = provider.make_request(RPCEndpoint("eth_call"), [call, block])
    # Success is the ABSENCE of an error, never the presence of one we could not
    # parse (VIB-6438). Reading an unparseable error as success is not a lenient
    # default — it is a fabricated measurement: a lost answer would be reported
    # as "the replay SUCCEEDED", telling the operator the mined failure is not
    # reproducible when in truth nothing was measured at all.
    #
    # Not hypothetical. The gateway forwards upstream JSON-RPC errors verbatim
    # (``rpc_service.py`` ``rpc_error = data["error"]``) and its own code guards
    # ``isinstance(rpc_error, dict)`` there, so a non-dict error is a shape the
    # transport genuinely produces; ``gateway_provider.py`` then ``json.loads``
    # it straight back into this response.
    if not isinstance(response, dict):
        return _ReplayProbe(outcome="transport", error=f"non-dict RPC response: {type(response).__name__}")
    if "error" not in response:
        # The gateway's success response carries no ``error`` key at all, so key
        # presence is an exact discriminator.
        return _ReplayProbe(outcome="success")
    error = response["error"]
    if not isinstance(error, dict):
        return _ReplayProbe(outcome="transport", error=f"malformed RPC error: {error!r}")

    message = str(error.get("message", "")).strip()
    data = error.get("data")
    # Discriminate an EXECUTED failure from a lost answer with the SAME predicate
    # the connector static-call probe uses — a second local predicate would drift
    # from it — widened by the out-of-gas shape that predicate cannot see.
    # Anything that still does not positively look executed is inconclusive.
    has_revert_data = isinstance(data, str) and data.startswith("0x") and len(data) > 2
    out_of_gas = _is_out_of_gas(message)
    if not has_revert_data and not out_of_gas and not looks_like_revert(str(error)):
        return _ReplayProbe(outcome="transport", error=message or str(error))

    # A bare "0x" is EMPTY revert data, not undecodable data — the two are
    # different diagnoses. Only a non-empty payload goes to the decoder.
    if has_revert_data:
        return _ReplayProbe(outcome="revert", reason=decode_revert_data(str(data)), raw=str(data))
    if out_of_gas:
        # Executed and ran out of gas. Carry the node's own words: the ABI
        # decoders have nothing to decode here, so falling through to them below
        # would discard the one phrase that names the outcome.
        return _ReplayProbe(outcome="revert", reason=message)
    # No structured ``data`` field: fall back to the node's inline message text,
    # which is what the gateway forwards for most revert shapes.
    return _ReplayProbe(outcome="revert", reason=extract_revert_reason(message) if message else None)


def _replay_revert_reason(
    provider: GatewayWeb3Provider,
    transaction: dict[str, str],
    block_number: int,
) -> str:
    """Re-simulate the failed transaction where it ran and describe the outcome.

    The replay targets the PARENT block: ``eth_call`` at block ``N`` executes
    against the state *after* ``N`` is applied, while the transaction itself ran
    against the state after ``N-1``. Anvil mines the keeper transaction into its
    own block, so the parent block is exactly the pre-transaction state. This is
    NOT an off-by-one — replaying at ``N`` would run against post-transaction
    state.

    ONE probe, carrying the transaction's own ``gas`` and ``gasPrice``, and it
    reports only what that probe measured. An earlier revision also ran a second
    ``eth_call`` with unbounded gas and reported ``GAS-BOUND`` when the outcome
    flipped. That is removed: the executor tops the sender's balance to exactly
    ``submitted_gas_limit * gas_price`` (the estimate plus the bounded VIB-6450
    headroom), so a probe omitting ``gas`` is billed against
    the block gas limit and always fails ``Insufficient funds`` — the verdict
    could never fire, and dead code on an error path is a place for defects to
    live rather than a diagnostic. Rebuilding it correctly (``eth_call`` state
    overrides) is VIB-6477.
    """
    # Carry every field that can DECIDE the outcome, not just the ones that
    # identify the call. GMX validates ``executionFee >= gasLimit * tx.gasprice``
    # (``sdk.py``), so dropping ``gasPrice`` lets a fee-caused revert replay as a
    # success and exonerate the true cause — the same false negative as dropping
    # ``gas`` (VIB-6438).
    base = {
        key: value
        for key, value in transaction.items()
        if key in ("from", "to", "data", "value", "gasPrice", "maxFeePerGas", "maxPriorityFeePerGas")
    }
    submitted_gas = transaction.get("gas")
    if block_number <= 0:
        # Without the receipt block the parent is unknown, and "latest" is
        # POST-transaction state — a replay there measures the wrong state and
        # could report a confident "SUCCEEDED". Unmeasured, so say so.
        return "eth_call replay unavailable — the receipt carried no usable block number, and replaying at head would measure post-transaction state"
    block = hex(block_number - 1)

    faithful = _replay_once(provider, {**base, "gas": submitted_gas} if submitted_gas else base, block)
    if faithful.outcome == "transport":
        # Inconclusive, and it must READ as inconclusive. Returning a revert or a
        # gas verdict here would be a fabricated measurement.
        return f"eth_call replay unavailable — no answer from the node ({faithful.error or 'unknown'})"
    if faithful.outcome == "success":
        return (
            f"eth_call replay at block {block} SUCCEEDED at the submitted gas limit — the mined failure "
            "is not reproducible from that state with that gas, so it is neither a deterministic revert "
            "nor gas exhaustion; look for state ordering (a prior transaction in the same block) or a "
            "node-level condition"
        )

    return f"eth_call replay reverted: {faithful.describe()}"


# Call-trace probe (VIB-6437). The replay above reports the OUTERMOST revert —
# and on GMX that is structurally the wrong frame to explain anything: the venue
# wraps order execution in a try/catch, so when the error HANDLER itself fails
# (InsufficientGasForCancellation fired while cancelling), the primary revert
# that put GMX on the cancellation path is swallowed before it can surface.
# Every prior instrument on this ticket measured the symptom for exactly this
# reason. Only a call trace can recover the inner frames' own revert payloads.
#
# ``debug_traceTransaction`` is Anvil-gated gateway-side (ANVIL_ONLY_RPC_METHODS,
# ``almanak/gateway/validation.py``) and this module refuses non-anvil networks,
# so this probe cannot run anywhere but a managed fork.

_TRACE_DIR_ENV = "ALMANAK_GMX_ANVIL_TRACE_DIR"  # read via ConnectorsConfig, never directly
_TRACE_MAX_FRAMES = 4096  # defensive cap on the tree walk; a GMX executeOrder is a few hundred frames
_TX_HASH_PATTERN = re.compile(r"^0x[0-9a-fA-F]{64}$")


def _trace_artifact_dir() -> str:
    """The callTracer artifact directory, or empty when capture is off.

    Routed through the typed config service (config-boundary gate,
    ``scripts/ci/check_config_boundary.py``) and read at CALL time, not import
    time, so ``ALMANAK_GMX_ANVIL_TRACE_DIR`` behaves as a per-run switch.
    """
    return (connectors_config_from_env().gmx_anvil_trace_dir or "").strip()


def _call_tracer_trace(provider: GatewayWeb3Provider, tx_hash: str) -> dict[str, Any] | None:
    """One ``debug_traceTransaction`` callTracer tree, or ``None``. Never raises.

    ``None`` means UNMEASURED (transport failure, method unavailable, malformed
    answer) — the caller must say so rather than treating it as an empty trace.
    """
    try:
        result = _rpc(provider, "debug_traceTransaction", [tx_hash, {"tracer": "callTracer"}])
    except Exception as exc:  # noqa: BLE001 — a diagnostic must never mask the real failure
        logger.debug("GMX call-trace fetch failed for %s: %s", tx_hash, exc, exc_info=True)
        return None
    return result if isinstance(result, dict) else None


def _erroring_frames(root: dict[str, Any]) -> tuple[list[dict[str, Any]], bool]:
    """Erroring call frames in document order (outermost first), plus a truncation flag.

    Document order matters for reading: the first frame is the outer revert the
    receipt already names; the frames after it are the inner failures it
    absorbed — the innermost one is normally the primary cause. Each returned
    frame carries its ``depth`` so that nesting survives the flattening.
    """
    frames: list[dict[str, Any]] = []
    stack: list[tuple[Any, int]] = [(root, 0)]
    visited = 0
    while stack:
        if visited >= _TRACE_MAX_FRAMES:
            return frames, True
        node, depth = stack.pop()
        visited += 1
        if not isinstance(node, dict):
            continue
        if node.get("error"):
            frames.append({**node, "depth": depth})
        calls = node.get("calls")
        if isinstance(calls, list):
            for child in reversed(calls):
                stack.append((child, depth + 1))
    return frames, False


def _describe_trace_frame(frame: dict[str, Any]) -> str:
    """One erroring frame as a compact, ABI-faithful log fragment."""
    parts: list[str] = [f"depth={frame.get('depth', '?')}"]
    call_type = frame.get("type")
    if call_type:
        parts.append(str(call_type))
    to = frame.get("to")
    if to:
        parts.append(f"to={to}")
    raw_input = frame.get("input")
    if isinstance(raw_input, str) and raw_input.startswith("0x") and len(raw_input) >= 10:
        parts.append(f"selector={raw_input[:10]}")
    gas_used = frame.get("gasUsed")
    if isinstance(gas_used, str) and gas_used.startswith("0x"):
        try:
            parts.append(f"gas_used={int(gas_used, 16)}")
        except ValueError:
            pass
    elif isinstance(gas_used, int):
        parts.append(f"gas_used={gas_used}")
    parts.append(f"error={frame.get('error')!r}")
    revert_reason = frame.get("revertReason")
    if revert_reason:
        parts.append(f"reason={revert_reason!r}")
    output = frame.get("output")
    if isinstance(output, str) and output.startswith("0x") and len(output) > 2:
        # The frame's own revert payload — the datum this probe exists for.
        # Same decoder and same never-silently-cut formatting as the replay
        # probe (VIB-6483), so the two probes cannot disagree about rendering.
        parts.append(f"decoded={decode_revert_data(output)}")
        parts.append(f"raw={_format_raw_payload(output)}")
    return " ".join(parts)


def _write_trace_artifact(tx_hash: str, trace: dict[str, Any]) -> None:
    """Persist the raw callTracer JSON to ``$ALMANAK_GMX_ANVIL_TRACE_DIR``. Never raises.

    A flattened log line cannot be diffed against a control run's call tree;
    the raw JSON can. No-op unless the env var is set, so ordinary runs write
    nothing.
    """
    directory = _trace_artifact_dir()
    if not directory or not _TX_HASH_PATTERN.match(tx_hash):
        return
    try:
        target = Path(directory)
        target.mkdir(parents=True, exist_ok=True)
        (target / f"{tx_hash.lower()}.calltrace.json").write_text(json.dumps(trace, indent=1))
    except Exception as exc:  # noqa: BLE001 — an artifact writer must never mask the real failure
        logger.warning("GMX call-trace artifact write failed for %s: %s", tx_hash, exc)


def _capture_trace_artifact_if_enabled(provider: GatewayWeb3Provider, tx_hash: str) -> None:
    """Trace a NON-reverting keeper transaction when artifact capture is on.

    The revert path traces unconditionally (the trace IS the diagnosis); filling
    transactions are traced only under ``ALMANAK_GMX_ANVIL_TRACE_DIR`` because
    their only use is as the control arm of a differential — and a revert probe
    without a control invites over-reading a single sample.
    """
    if not _trace_artifact_dir():
        return
    trace = _call_tracer_trace(provider, tx_hash)
    if trace is not None:
        _write_trace_artifact(tx_hash, trace)


def _trace_revert_frames(provider: GatewayWeb3Provider, tx_hash: str) -> str:
    """Describe the mined revert's erroring call frames. Never raises."""
    trace = _call_tracer_trace(provider, tx_hash)
    if trace is None:
        return "call trace unavailable — debug_traceTransaction returned no usable answer"
    _write_trace_artifact(tx_hash, trace)
    frames, truncated = _erroring_frames(trace)
    if not frames:
        # Empty ≠ Zero, applied to traces: a mined revert whose trace shows no
        # erroring frame means the TRACE is not corroborating, not that the
        # revert did not happen. Say which one was measured.
        return "call trace carried no erroring frames — trace does not corroborate the mined revert; treat the trace as unmeasured"
    rendered = " || ".join(_describe_trace_frame(frame) for frame in frames)
    suffix = f" (walk truncated at {_TRACE_MAX_FRAMES} frames)" if truncated else ""
    return f"call trace erroring frames x{len(frames)}, outermost first: {rendered}{suffix}"


def _diagnose_mined_revert(
    provider: GatewayWeb3Provider,
    transaction: dict[str, str],
    tx_hash: str,
    block_number: int,
) -> str:
    """Assemble the best-effort cause of a mined revert. Never raises."""
    parts: list[str] = []
    for label, probe in (
        ("replay", lambda: _replay_revert_reason(provider, transaction, block_number)),
        ("trace", lambda: _trace_revert_frames(provider, tx_hash)),
    ):
        try:
            detail = probe()
        except Exception as exc:  # noqa: BLE001 — a diagnostic must never mask the real failure
            logger.debug("GMX mined-revert %s probe failed: %s", label, exc, exc_info=True)
            parts.append(f"{label} unavailable ({type(exc).__name__})")
            continue
        if detail:
            parts.append(detail)
    if not parts:
        return "no revert reason could be obtained"
    return "; ".join(parts)


def _estimate_gas_with_warm_retry(provider: GatewayWeb3Provider, transaction: dict[str, str]) -> Any:
    for attempt in range(1, _ESTIMATE_GAS_ATTEMPTS + 1):
        try:
            return _rpc(provider, "eth_estimateGas", [transaction])
        except (GmxAnvilOrderExecutionError, grpc.RpcError) as exc:
            if not _is_transient_execution_error(exc) or attempt == _ESTIMATE_GAS_ATTEMPTS:
                raise
            logger.warning(
                "GMX keeper eth_estimateGas hit a transient cold-fork failure (attempt %d/%d): %s; "
                "state fetched so far stays cached — retrying",
                attempt,
                _ESTIMATE_GAS_ATTEMPTS,
                exc,
            )
    raise GmxAnvilOrderExecutionError("eth_estimateGas retry loop exited without a result")  # pragma: no cover


def _log_mined_block_timing(
    provider: GatewayWeb3Provider,
    tx_hash: str,
    kind: str,
    block_number: int,
) -> None:
    """Record where a keeper transaction landed in FORK TIME vs wall time. Never raises.

    R15 (VIB-6437) enumerated the inputs that could distinguish a head-mode fork
    from a pinned fork of the same base block, and the timestamps Anvil assigns
    to newly mined blocks were the one input no artifact had ever recorded. The
    ``wall_delta_s`` here is that measurement: ~0 means mined blocks track the
    system clock; a large negative value means they continue the forked block's
    frozen timeline.
    """
    if block_number <= 0:
        return
    try:
        block = _rpc(provider, "eth_getBlockByNumber", [hex(block_number), False])
        if not isinstance(block, dict) or "timestamp" not in block:
            return
        raw_timestamp = block["timestamp"]
        # Same both-forms parse as _latest_block_timestamp: hex string on the
        # wire, but int if a provider layer already decoded it — feeding a
        # decimal int through int(str(x), 16) would log a silently wrong delta.
        timestamp = int(raw_timestamp, 16) if isinstance(raw_timestamp, str) else int(raw_timestamp)
    except Exception as exc:  # noqa: BLE001 — timing telemetry must never mask the transaction outcome
        logger.debug("GMX keeper block-timing read failed for %s: %s", tx_hash, exc)
        return
    logger.info(
        "GMX keeper %s transaction mined: %s block=%d timestamp=%d wall_delta_s=%+d",
        kind,
        tx_hash,
        block_number,
        timestamp,
        timestamp - int(time.time()),
    )


# Headroom applied to eth_estimateGas before SUBMITTING a keeper transaction
# (VIB-6450). A limit equal to the bare estimate encodes the assumption that the
# estimator's simulation and the mined execution cost exactly the same — and R16
# measured that assumption failing by ~50k gas (<1.6%) on a partial decrease,
# which the EIP-150 63/64 cascade turned into an out-of-gas at the FINAL
# EventEmitter emission and GMX's error handler turned into a whole-transaction
# InsufficientGasForCancellation revert (the VIB-6437 user strand). Proportional
# because the drift scales with the work, ×1.10 because the measured drift is
# <2% and R14's attempt 4 already demonstrated a far larger margin (+1.5M)
# filling safely. Raising the limit raises the OUTER allowance one-for-one
# (R17 measured: +477,307 submitted appeared as +477,307 root-frame allowance);
# what reaches the inner execution call passes through GMX's execution-gas
# computation and EIP-150 forwarding, and R17 shows it suffices with wide
# spare. No venue validation is weakened and a real revert still reverts —
# this clears only the knife-edge OOG class.
#
# The headroom is BOUNDED, per VIB-6450's own requirement ("a floor and a
# ceiling, not an unbounded multiplier" — VIB-6427: cost guards key off the
# limit and nodes can reject on admission). The floor keeps small harness
# transactions from carrying a headroom too thin to absorb any drift at all;
# the ceiling keeps a pathological estimate from growing without bound. The
# R17-validated case is preserved bit-for-bit: estimate 4,108,084 → margin
# 410,808 → submitted 4,518,892, exactly the run that filled the 7/7 reproducer.
_SUBMIT_HEADROOM_FRACTION = 10  # margin = estimate // 10 within the bounds below
_SUBMIT_HEADROOM_FLOOR = 100_000
_SUBMIT_HEADROOM_CEILING = 1_000_000


def _submitted_gas_limit(gas_estimate: int) -> int:
    """The limit actually submitted: the estimate plus bounded drift headroom."""
    margin = min(max(gas_estimate // _SUBMIT_HEADROOM_FRACTION, _SUBMIT_HEADROOM_FLOOR), _SUBMIT_HEADROOM_CEILING)
    return gas_estimate + margin


def _send_transaction(
    provider: GatewayWeb3Provider,
    sender: str,
    target: str,
    data: str,
    *,
    kind: Literal["order", "harness"],
) -> str:
    """Send one keeper transaction on the managed fork.

    ``kind`` is keyword-only and required on purpose: it decides whether a mined
    revert is reported as an order-level rejection, and a default would silently
    mislabel the three harness call sites (VIB-6438).
    """
    sender = to_checksum_address(sender)
    transaction = {
        "from": sender,
        "to": to_checksum_address(target),
        "data": data,
        "value": "0x0",
    }
    gas_estimate_raw = _estimate_gas_with_warm_retry(provider, transaction)
    try:
        gas_estimate = int(gas_estimate_raw, 16) if isinstance(gas_estimate_raw, str) else int(gas_estimate_raw)
    except (TypeError, ValueError) as exc:
        raise GmxAnvilOrderExecutionError(
            f"Anvil eth_estimateGas returned an invalid result: {gas_estimate_raw!r}"
        ) from exc
    if gas_estimate <= 0:
        raise GmxAnvilOrderExecutionError(f"Anvil eth_estimateGas returned a non-positive result: {gas_estimate}")
    gas_limit = _submitted_gas_limit(gas_estimate)

    gas_price_raw = _rpc(provider, "eth_gasPrice", [])
    balance_raw = _rpc(provider, "eth_getBalance", [sender, "latest"])
    try:
        gas_price = int(gas_price_raw, 16) if isinstance(gas_price_raw, str) else int(gas_price_raw)
        balance = int(balance_raw, 16) if isinstance(balance_raw, str) else int(balance_raw)
    except (TypeError, ValueError) as exc:
        raise GmxAnvilOrderExecutionError("Anvil returned an invalid gas price or sender balance") from exc
    # The top-up moves WITH the submitted limit: funding only the bare estimate
    # while submitting more would fail the send on sender balance.
    required_balance = gas_limit * gas_price
    if balance < required_balance:
        _rpc(provider, "anvil_setBalance", [sender, hex(required_balance)])

    transaction_with_gas = {
        **transaction,
        "gas": hex(gas_limit),
        "gasPrice": hex(gas_price),
    }

    tx_hash = _rpc(
        provider,
        "eth_sendTransaction",
        [transaction_with_gas],
    )
    if not isinstance(tx_hash, str) or not tx_hash.startswith("0x"):
        raise GmxAnvilOrderExecutionError("Anvil eth_sendTransaction returned an invalid transaction hash")

    receipt = Web3(provider).eth.wait_for_transaction_receipt(
        HexStr(tx_hash),
        timeout=_TX_RECEIPT_TIMEOUT_SECONDS,
    )
    try:
        block_number = int(receipt["blockNumber"])
    except (KeyError, TypeError, ValueError):
        block_number = 0
    _log_mined_block_timing(provider, tx_hash, kind, block_number)
    if int(receipt["status"]) != 1:
        gas_used = int(receipt["gasUsed"])
        # Diagnose BEFORE raising: the reason is the one datum that identifies
        # the cause, and it is only obtainable here (VIB-6438).
        diagnosis = _diagnose_mined_revert(provider, transaction_with_gas, tx_hash, block_number)
        # Only the ORDER call can be an order-level rejection. A harness
        # oracle setup/cleanup revert means our own plumbing failed and the
        # venue never saw the order; reporting that as "the venue rejected this
        # order" is the same mislabel this ticket fixes, reversed (VIB-6438).
        error_type = GmxAnvilTransactionRevertedError if kind == "order" else GmxAnvilHarnessTransactionRevertedError
        raise error_type(
            f"GMX managed-Anvil {kind} transaction reverted: {tx_hash} "
            f"(gas_used={gas_used}, submitted_gas_limit={gas_limit}, measured_gas_limit={gas_estimate}) — {diagnosis}"
        )
    return tx_hash


@contextmanager
def _impersonated(provider: GatewayWeb3Provider, account: str) -> Iterator[None]:
    account = to_checksum_address(account)
    original_balance = _rpc(provider, "eth_getBalance", [account, "latest"])
    # Outside the try: a failed impersonation leaves nothing to undo, and the
    # error is the caller's to see.
    _rpc(provider, "anvil_impersonateAccount", [account])
    body_failed = False
    try:
        yield
    except BaseException:
        body_failed = True
        raise
    finally:
        _undo_impersonation(provider, account, original_balance, body_failed=body_failed)


def _undo_impersonation(
    provider: GatewayWeb3Provider,
    account: str,
    original_balance: Any,
    *,
    body_failed: bool,
) -> None:
    """Undo one impersonation bracket without masking the body's failure.

    Cleanup runs on the same gateway channel that may have just failed the
    body, so it can fail too. Python would let an exception raised in a
    ``finally`` REPLACE the in-flight one, which is how a rate-limited GMX run
    reported ``anvil_stopImpersonatingAccount`` as its root cause and buried
    the real failure (ALM-3025). When the body already failed we log the
    residual fork state and let the original propagate; when it did not, a
    cleanup failure IS the failure and must surface — the fork is left
    impersonating and a later run would inherit it.

    Both steps are attempted independently rather than bracketed in a nested
    ``try/finally``: a sustained rate limit fails consecutive cleanup RPCs on
    the same channel, and ``finally``-replacement would drop the *first*
    failure — the balance restore, the more actionable of the two — from the
    operator's log. Every failure is reported; the first is the one re-raised.
    """
    failures: list[str] = []
    first_error: Exception | None = None
    for method, params in (
        ("anvil_setBalance", [account, original_balance]),
        ("anvil_stopImpersonatingAccount", [account]),
    ):
        try:
            _rpc(provider, method, params)
        except Exception as exc:
            # Caught per step, not around the loop: a failed balance restore
            # must not stop us from dropping impersonation.
            failures.append(str(exc))
            if first_error is None:
                first_error = exc

    if first_error is None:
        return

    logger.error(
        "GMX managed-Anvil could not undo impersonation of %s: %s. The fork is left impersonating this account%s",
        account,
        "; ".join(failures),
        "; reporting the original failure instead" if body_failed else "",
    )
    if not body_failed:
        raise first_error


def _measure_oracle_bounds(
    *,
    gateway_client: Any,
    provider: GatewayWeb3Provider,
    dependencies: _GmxDependencies,
    chain: str,
    markets: tuple[str, ...],
) -> dict[str, tuple[int, int]]:
    """Measure every oracle token's price bounds, index tokens by market (ALM-3108).

    Index tokens are resolved first and win the dedup: a token that is the index
    of one market and the collateral of another is one oracle key with one
    price, and the market-keyed route is the one that holds even when the token
    is a synthetic identifier.

    Every price is measured BEFORE any ``setPrimaryPrice`` is sent, so a market
    the fork cannot price leaves the Oracle untouched instead of half-seeded.
    """
    bounds_by_token: dict[str, tuple[int, int]] = {}
    collateral_tokens: set[str] = set()
    for market in markets:
        tokens = _read_market_tokens(provider, dependencies, market)
        bounds_by_token[tokens.index_token] = _index_price_bounds(gateway_client, chain, market)
        collateral_tokens.update(tokens.collateral_tokens)

    for token in sorted(collateral_tokens - set(bounds_by_token)):
        bounds_by_token[token] = _read_price_bounds(gateway_client, provider, chain, token)
    return bounds_by_token


def _seed_oracle_prices(
    *,
    gateway_client: Any,
    provider: GatewayWeb3Provider,
    dependencies: _GmxDependencies,
    chain: str,
    markets: tuple[str, ...],
) -> tuple[str, ...]:
    count = _decode_uint(
        _eth_call(
            provider,
            dependencies.oracle,
            _calldata(_GET_TOKENS_WITH_PRICES_COUNT_SIGNATURE),
        ),
        "Oracle tokens-with-prices count",
    )
    if count != 0:
        raise GmxAnvilOrderExecutionError(
            f"GMX Oracle already has {count} transient price token(s); refusing to overwrite execution state"
        )

    bounds_by_token = _measure_oracle_bounds(
        gateway_client=gateway_client,
        provider=provider,
        dependencies=dependencies,
        chain=chain,
        markets=markets,
    )

    hashes: list[str] = []
    for token in sorted(bounds_by_token):
        minimum, maximum = bounds_by_token[token]
        hashes.append(
            _send_transaction(
                provider,
                dependencies.order_handler,
                dependencies.oracle,
                _calldata(
                    _SET_PRIMARY_PRICE_SIGNATURE,
                    ["address", "(uint256,uint256)"],
                    [token, (minimum, maximum)],
                ),
                kind="harness",
            )
        )

    timestamp = _latest_block_timestamp(provider)
    hashes.append(
        _send_transaction(
            provider,
            dependencies.order_handler,
            dependencies.oracle,
            _calldata(
                _SET_TIMESTAMPS_SIGNATURE,
                ["uint256", "uint256"],
                [timestamp, timestamp],
            ),
            kind="harness",
        )
    )
    return tuple(hashes)


def _oracle_price_count(provider: GatewayWeb3Provider, dependencies: _GmxDependencies) -> int:
    return _decode_uint(
        _eth_call(
            provider,
            dependencies.oracle,
            _calldata(_GET_TOKENS_WITH_PRICES_COUNT_SIGNATURE),
        ),
        "Oracle tokens-with-prices count",
    )


def _clear_seeded_oracle_prices(
    provider: GatewayWeb3Provider,
    dependencies: _GmxDependencies,
    transaction_hashes: list[str],
    *,
    body_failed: bool,
) -> None:
    """Clear this pass's transient oracle prices without masking the body's failure.

    Leaving them seeded is what makes a failed run *unrecoverable* rather than
    merely failed: the next attempt reads a non-zero
    ``getTokensWithPricesCount()`` and refuses to execute at all, so the fork
    has to be thrown away (ALM-3025). Same ``finally``-masking rule as
    :func:`_undo_impersonation` — if the body already failed, that error is the
    actionable one; a cleanup failure is logged with the residual state named
    so the operator knows the fork is dirty.
    """
    try:
        if _oracle_price_count(provider, dependencies) != 0:
            transaction_hashes.append(_clear_oracle_prices(provider, dependencies))
    except Exception as exc:
        logger.error(
            "GMX managed-Anvil could not clear seeded oracle prices: %s. The fork is left with "
            "transient prices set and the next execution attempt will refuse to run against it%s",
            exc,
            "; reporting the original failure instead" if body_failed else "",
        )
        if not body_failed:
            raise


def _clear_oracle_prices(provider: GatewayWeb3Provider, dependencies: _GmxDependencies) -> str:
    return _send_transaction(
        provider,
        dependencies.order_handler,
        dependencies.oracle,
        _calldata(_CLEAR_ALL_PRICES_SIGNATURE),
        kind="harness",
    )


def _execute_order(
    provider: GatewayWeb3Provider,
    dependencies: _GmxDependencies,
    keeper: str,
    order_key: str,
) -> tuple[str, dict[str, Any]]:
    key = bytes.fromhex(_normalize_order_key(order_key).removeprefix("0x"))
    tx_hash = _send_transaction(
        provider,
        keeper,
        dependencies.order_handler,
        _calldata(
            _EXECUTE_ORDER_SIGNATURE,
            ["bytes32", "(address[],address[],bytes[])"],
            [key, ([], [], [])],
        ),
        kind="order",
    )
    receipt = _rpc(provider, "eth_getTransactionReceipt", [tx_hash])
    if not isinstance(receipt, dict):
        raise GmxAnvilOrderExecutionError(f"GMX executeOrder receipt was not a JSON-RPC object: {tx_hash}")
    # BEFORE outcome verification: a cancelled/frozen order raises there, and a
    # trace of that transaction is control-arm evidence, not an error artifact.
    _capture_trace_artifact_if_enabled(provider, tx_hash)
    _verify_execution_outcome(dict(receipt), order_key, tx_hash)
    return tx_hash, dict(receipt)


_ORDER_OUTCOME_LABELS = {
    GMXv2EventType.ORDER_EXECUTED: "executed",
    GMXv2EventType.ORDER_CANCELLED: "cancelled",
    GMXv2EventType.ORDER_FROZEN: "frozen",
}


def _verify_execution_outcome(receipt: dict[str, Any], order_key: str, tx_hash: str) -> None:
    """Fail loud when executeOrder settled the order WITHOUT filling it.

    A successful ``OrderHandler.executeOrder`` transaction is not proof of a
    fill: GMX cancels or freezes the order inside that same transaction when
    venue validation fails (acceptable-price bounds, open-interest caps,
    collateral floors, oracle constraints, ...). Without this check the only
    downstream signal is the settlement observer's generic "order left the
    pending set without its exact target position delta" — this surfaces the
    venue's actual outcome and reason instead.
    """
    logs = receipt.get("logs")
    if not isinstance(logs, list):
        raise GmxAnvilOrderExecutionError(
            f"GMX executeOrder receipt carried no log list to verify the outcome: {tx_hash}"
        )
    events = GMXv2ReceiptParser().parse_logs([log for log in logs if isinstance(log, dict)])
    key = _normalize_order_key(order_key)
    outcomes: dict[str, Any] = {}
    for event in events:
        label = _ORDER_OUTCOME_LABELS.get(event.event_type)
        if label is None:
            continue
        # Require an exact key match: an outcome event whose key is missing or
        # undecodable must never be attributed to this order — tolerating an
        # empty key as "no opinion" reopens the misattribution class this
        # verification exists to close.
        event_key = str(event.data.get("key") or "").lower()
        if event_key != key:
            continue
        outcomes[label] = event
    if "executed" in outcomes:
        return
    if "cancelled" in outcomes or "frozen" in outcomes:
        label = "cancelled" if "cancelled" in outcomes else "frozen"
        raise GmxAnvilOrderRejectedError(
            f"GMX keeper executeOrder {label} order {key} instead of filling it "
            f"(venue reason: {_venue_outcome_reason(outcomes[label])}; tx: {tx_hash})"
        )
    raise GmxAnvilOrderExecutionError(
        f"GMX executeOrder receipt for order {key} carried no Order outcome event "
        f"(executed/cancelled/frozen) — outcome unmeasured (tx: {tx_hash})"
    )


def _venue_outcome_reason(event: Any) -> str:
    """Decode the venue's ``reason`` / ``reasonBytes`` from an Order outcome event.

    The receipt parser's ``cancelled_reason`` field is a static placeholder, so
    the real reason is decoded here from the EventEmitter payload's string and
    bytes item groups. Extraction failure must never mask the outcome itself.
    """
    raw = str(getattr(event, "raw_data", "") or "").removeprefix("0x")
    try:
        _sender, _name, event_data = abi_decode(
            ["address", "string", _EVENT_LOG_DATA_ABI_TYPE],
            bytes.fromhex(raw),
        )
        strings = {str(k): str(v) for k, v in event_data[6][0]}
        if strings.get("reason"):
            return strings["reason"]
        bytes_items = {str(k): v for k, v in event_data[5][0]}
        reason_bytes = bytes_items.get("reasonBytes")
        if isinstance(reason_bytes, bytes) and reason_bytes:
            return "0x" + reason_bytes.hex()
    except Exception:  # noqa: BLE001 - reason extraction must never mask the outcome
        logger.debug("GMX order outcome reason decode failed", exc_info=True)
    return "unmeasured"


def _prepare_execution_request(
    *,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    orders: tuple[Any, ...],
    network: str,
) -> GmxAnvilOrderExecutionResult | tuple[tuple[str, ...], dict[str, str]]:
    """Validate the managed-fork request and measure executable order markets."""
    if str(network or "").strip().lower() != "anvil":
        return GmxAnvilOrderExecutionResult(
            ok=False,
            reason="GMX local order execution is restricted to the managed Anvil network",
        )
    if gateway_client is None:
        return GmxAnvilOrderExecutionResult(ok=False, reason="GMX local order execution requires a gateway client")

    try:
        requested_keys = tuple(
            dict.fromkeys(_normalize_order_key(str(getattr(order, "order_id", "") or "")) for order in orders)
        )
    except GmxAnvilOrderExecutionError as exc:
        return GmxAnvilOrderExecutionResult(ok=False, reason=str(exc))
    if not requested_keys:
        return GmxAnvilOrderExecutionResult(
            ok=False, reason="GMX local order execution requires exact bytes32 order keys"
        )

    pending = read_pending_orders(gateway_client, chain, wallet_address)
    if not pending.ok:
        return GmxAnvilOrderExecutionResult(
            ok=False,
            reason=pending.error or "GMX pending-order ownership could not be measured",
        )

    pending_by_key = {str(order.order_key).lower(): order for order in pending.orders if order.order_key}
    pending_keys = {str(key).lower() for key in pending.order_keys}
    missing = [key for key in requested_keys if key not in pending_keys]
    if missing and pending.truncated:
        return GmxAnvilOrderExecutionResult(
            ok=False,
            reason="GMX pending-order set was truncated; exact order ownership could not be proven",
        )

    executable_keys = tuple(key for key in requested_keys if key in pending_keys)
    if not executable_keys:
        return GmxAnvilOrderExecutionResult(ok=True)

    market_by_key: dict[str, str] = {}
    for key in executable_keys:
        detail = pending_by_key.get(key)
        market = str(getattr(detail, "market", "") or "")
        if not market or market.lower() == _ZERO_ADDRESS:
            return GmxAnvilOrderExecutionResult(
                ok=False,
                reason=f"GMX order {key} has no measured market detail",
            )
        market_by_key[key] = to_checksum_address(market)
    return executable_keys, market_by_key


def execute_pending_orders_on_anvil(
    *,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    orders: tuple[Any, ...],
    network: str,
) -> GmxAnvilOrderExecutionResult:
    """Execute the exact submitted GMX orders in the current managed fork."""
    prepared = _prepare_execution_request(
        gateway_client=gateway_client,
        chain=chain,
        wallet_address=wallet_address,
        orders=orders,
        network=network,
    )
    if isinstance(prepared, GmxAnvilOrderExecutionResult):
        return prepared
    executable_keys, market_by_key = prepared

    provider = GatewayWeb3Provider(
        gateway_client,
        chain=chain,
        request_timeout=_COLD_FORK_RPC_TIMEOUT_SECONDS,
    )
    try:
        dependencies = _load_dependencies(provider, chain)
        if not _has_role(provider, dependencies.role_store, dependencies.order_handler, _CONTROLLER_ROLE):
            raise GmxAnvilOrderExecutionError(f"GMX OrderHandler {dependencies.order_handler} does not hold CONTROLLER")
        keeper = _find_order_keeper(provider, dependencies.role_store)

        transaction_hashes: list[str] = []
        execution_receipts: list[dict[str, Any]] = []
        with _impersonated(provider, dependencies.order_handler), _impersonated(provider, keeper):
            for key in executable_keys:
                oracle_state_owned = False
                body_failed = False
                try:
                    initial_count = _oracle_price_count(provider, dependencies)
                    if initial_count != 0:
                        raise GmxAnvilOrderExecutionError(
                            f"GMX Oracle already has {initial_count} transient price token(s); "
                            "refusing to overwrite execution state"
                        )
                    oracle_state_owned = True
                    transaction_hashes.extend(
                        _seed_oracle_prices(
                            gateway_client=gateway_client,
                            provider=provider,
                            dependencies=dependencies,
                            chain=chain,
                            markets=(market_by_key[key],),
                        )
                    )
                    execute_hash, execute_receipt = _execute_order(provider, dependencies, keeper, key)
                    transaction_hashes.append(execute_hash)
                    execution_receipts.append(execute_receipt)
                except BaseException:
                    body_failed = True
                    raise
                finally:
                    if oracle_state_owned:
                        _clear_seeded_oracle_prices(
                            provider,
                            dependencies,
                            transaction_hashes,
                            body_failed=body_failed,
                        )

        logger.info(
            "GMX managed-Anvil keeper executed %d exact order(s): keys=%s transactions=%s",
            len(executable_keys),
            ", ".join(executable_keys),
            ", ".join(transaction_hashes),
        )
        return GmxAnvilOrderExecutionResult(
            ok=True,
            executed_order_keys=executable_keys,
            transaction_hashes=tuple(transaction_hashes),
            execution_receipts=tuple(execution_receipts),
        )
    except Exception as exc:
        logger.warning("GMX managed-Anvil order execution failed: %s", exc, exc_info=True)
        return GmxAnvilOrderExecutionResult(
            ok=False,
            reason=f"{type(exc).__name__}: {exc}",
            transient=_is_transient_execution_error(exc),
            order_rejected=isinstance(exc, GmxAnvilOrderRejectedError),
        )


__all__ = [
    "GmxAnvilHarnessTransactionRevertedError",
    "GmxAnvilOrderExecutionError",
    "GmxAnvilOrderExecutionResult",
    "GmxAnvilOrderRejectedError",
    "GmxAnvilTransactionRevertedError",
    "execute_pending_orders_on_anvil",
]
