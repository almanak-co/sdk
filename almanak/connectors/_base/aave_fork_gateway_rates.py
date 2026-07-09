"""Shared Aave-V3-fork gateway lending-rate pipeline (gateway-side foundation).

Live supply / borrow APY + utilisation via the Aave V3
``AaveProtocolDataProvider.getReserveData(address asset)`` ABI. The on-chain
ABI is identical across Aave V3 and its forks (Spark), so this pipeline lives
in the gateway-side connector foundation rather than in the Aave V3 connector
-- forks call :func:`fetch_aave_fork_lending_current` from here instead of
importing each other's gateway providers (same pattern as
:mod:`~almanak.connectors._base.v3_gateway_twap` for the Uniswap V3 family).

Parameterisation: each fork passes its ``protocol`` slug (the
``RateHistoryUnavailable`` source), a human ``display_name`` for error
messages, and its own address tables (``contracts_by_chain`` mapping
``chain -> {"pool_data_provider": ...}`` and ``tokens_by_chain`` mapping
``chain -> {symbol: address}``; forks without a curated token table pass an
empty mapping and rely on the global ``TokenResolver`` fallback).

Gateway-side: the gateway-service imports (``LendingRatePoint`` /
``RateHistoryUnavailable``) are deferred inside the functions so importing
this module stays cheap and free of import cycles.

Extracted verbatim (modulo parameterisation) from
``almanak/connectors/aave_v3/gateway/provider.py``, which had migrated the
body from ``framework/data/rates/monitor.py`` (W7 / VIB-4859). The strategy
container never makes this call -- every byte of HTTP egress happens through
the ``RateHistoryService`` servicer's shared aiohttp session.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)

# Function selector for AaveProtocolDataProvider.getReserveData(address)
# Computed: keccak256("getReserveData(address)")[:4].hex() == "35ea6a75"
_GET_RESERVE_DATA_SELECTOR = "35ea6a75"

# Return value indices for AaveProtocolDataProvider.getReserveData()
# Returns: (unbacked, accruedToTreasuryScaled, totalAToken, totalStableDebt,
#           totalVariableDebt, liquidityRate, variableBorrowRate, stableBorrowRate,
#           averageStableBorrowRate, liquidityIndex, variableBorrowIndex, lastUpdateTimestamp)
_IDX_TOTAL_ATOKEN = 2  # totalAToken (for utilization numerator)
_IDX_TOTAL_VARIABLE_DEBT = 4  # totalVariableDebt (for utilization denominator)
_IDX_LIQUIDITY_RATE = 5  # currentLiquidityRate = supply APY in ray
_IDX_VARIABLE_BORROW_RATE = 6  # currentVariableBorrowRate = borrow APY in ray
_MIN_RESPONSE_WORDS = 7  # Minimum words needed to read both rates

# Ray unit for the Aave family (1e27)
_RAY = Decimal("1000000000000000000000000000")


def _resolve_data_provider(
    protocol: str,
    contracts_by_chain: Mapping[str, Mapping[str, str]],
    chain: str,
) -> str:
    """Resolve the PoolDataProvider address for ``chain``.

    Raises ``RateHistoryUnavailable`` when the chain has no provider mapping.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    chain_contracts = contracts_by_chain.get(chain, {})
    data_provider = chain_contracts.get("pool_data_provider")
    if not data_provider:
        raise RateHistoryUnavailable(
            protocol,
            f"No PoolDataProvider configured on chain {chain!r}",
        )
    return data_provider


def _resolve_token_address(
    protocol: str,
    display_name: str,
    tokens_by_chain: Mapping[str, Mapping[str, str]],
    chain: str,
    asset_symbol: str,
) -> str:
    """Resolve the on-chain token address for ``asset_symbol`` on ``chain``.

    Tries the fork's curated token registry first, then falls back to the
    global ``TokenResolver`` (in-process registry — no network call).
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    chain_tokens = tokens_by_chain.get(chain, {})
    token_address = chain_tokens.get(asset_symbol)
    if token_address:
        return token_address

    from almanak.framework.data.tokens import get_token_resolver
    from almanak.framework.data.tokens.exceptions import TokenNotFoundError

    try:
        return get_token_resolver().resolve(asset_symbol, chain).address
    except TokenNotFoundError:
        raise RateHistoryUnavailable(
            protocol,
            f"Token {asset_symbol!r} not in {display_name} catalogue on {chain!r}",
        ) from None


def _resolve_rpc_url(protocol: str, servicer: Any, chain: str) -> str:
    """Resolve the RPC URL for ``chain``, raising ``RateHistoryUnavailable`` on failure."""
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable
    from almanak.gateway.utils import get_rpc_url

    try:
        return get_rpc_url(chain, network=servicer.settings.network)
    except ValueError as exc:
        raise RateHistoryUnavailable(
            protocol,
            f"No RPC URL configured for chain {chain!r}: {exc}",
        ) from exc


async def _post_get_reserve_data(
    protocol: str,
    session: Any,
    *,
    rpc_url: str,
    data_provider: str,
    calldata: str,
    chain: str,
) -> str:
    """POST ``getReserveData(asset)`` and return the raw hex result.

    Normalises transport / decode / RPC failures to ``RateHistoryUnavailable``.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": data_provider, "data": calldata}, "latest"],
        "id": 1,
    }
    try:
        async with session.post(rpc_url, json=payload) as response:
            response.raise_for_status()
            rpc_result = await response.json()
    except Exception as exc:
        raise RateHistoryUnavailable(
            protocol,
            f"RPC request / decode failed for chain {chain!r}: {exc}",
        ) from exc

    # A well-formed JSON-RPC reply is an object; a misbehaving node / proxy can
    # yield a list, string, or null (parsed cleanly by ``.json()`` yet unusable
    # here). Fail closed instead of raising AttributeError on the ``.get`` below.
    if not isinstance(rpc_result, dict):
        raise RateHistoryUnavailable(
            protocol,
            f"unexpected RPC response for chain {chain!r}: expected object, got {type(rpc_result).__name__}",
        )

    if "error" in rpc_result:
        error_val = rpc_result["error"]
        # The ``error`` member is an object per spec, but tolerate a bare string.
        msg = error_val.get("message", "RPC error") if isinstance(error_val, dict) else str(error_val)
        raise RateHistoryUnavailable(protocol, f"eth_call error: {msg}")

    return rpc_result.get("result", "") or ""


def _split_hex_words(hex_data: str) -> list[bytes]:
    """Split a 0x-prefixed hex blob into 32-byte words."""
    raw = bytes.fromhex(hex_data[2:])
    word_size = 32
    return [raw[i : i + word_size] for i in range(0, len(raw), word_size)]


def _words_all_zero(words: list[bytes]) -> bool:
    """Return True when every word in ``words`` is the 32-byte zero word."""
    zero = b"\x00" * 32
    return all(word == zero for word in words)


def _decode_reserve_words(
    protocol: str,
    display_name: str,
    hex_data: str,
    *,
    chain: str,
    asset_symbol: str,
) -> list[bytes]:
    """Decode the ``getReserveData`` hex blob to a list of 32-byte words.

    Raises ``RateHistoryUnavailable`` on empty results, short responses,
    or all-zero structs (which signals "not a listed reserve").
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    if not hex_data or not hex_data.startswith("0x") or hex_data == "0x":
        raise RateHistoryUnavailable(
            protocol,
            f"Token {asset_symbol!r} not a registered {display_name} reserve on {chain!r}",
        )

    # getReserveData returns fixed 32-byte ABI words, so the payload (minus the
    # ``0x``) is always a multiple of 64 hex chars. A non-multiple is a truncated
    # / malformed response; reject it before ``bytes.fromhex`` misreads a partial
    # final word (or raises on odd length).
    if (len(hex_data) - 2) % 64 != 0:
        raise RateHistoryUnavailable(
            protocol,
            f"malformed getReserveData response for {asset_symbol!r} on {chain!r}: "
            f"{len(hex_data) - 2} hex chars not word-aligned",
        )

    try:
        words = _split_hex_words(hex_data)
    except ValueError as exc:
        raise RateHistoryUnavailable(
            protocol,
            f"malformed getReserveData hex for {asset_symbol!r} on {chain!r}: {exc}",
        ) from exc

    if len(words) < _MIN_RESPONSE_WORDS:
        raise RateHistoryUnavailable(
            protocol,
            f"unexpected getReserveData response: {len(words)} words (need {_MIN_RESPONSE_WORDS})",
        )

    if _words_all_zero(words):
        raise RateHistoryUnavailable(
            protocol,
            f"Token {asset_symbol!r} resolved but is not a listed {display_name} reserve on {chain!r}",
        )
    return words


def _compute_apy_and_utilization(
    words: list[bytes],
    *,
    side: str,
) -> tuple[Decimal, Decimal | None]:
    """Compute APY percentage (ray -> %) and utilisation (variable-debt/aToken)."""
    if side == "supply":
        apy_ray = Decimal(int.from_bytes(words[_IDX_LIQUIDITY_RATE], "big"))
    else:
        apy_ray = Decimal(int.from_bytes(words[_IDX_VARIABLE_BORROW_RATE], "big"))
    apy_percent = apy_ray / _RAY * Decimal("100")

    total_atoken = Decimal(int.from_bytes(words[_IDX_TOTAL_ATOKEN], "big"))
    total_variable_debt = Decimal(int.from_bytes(words[_IDX_TOTAL_VARIABLE_DEBT], "big"))
    utilization: Decimal | None = None
    if total_atoken > 0:
        utilization = total_variable_debt / total_atoken * Decimal("100")
    return apy_percent, utilization


async def fetch_aave_fork_lending_current(
    servicer: Any,
    *,
    protocol: str,
    display_name: str,
    contracts_by_chain: Mapping[str, Mapping[str, str]],
    tokens_by_chain: Mapping[str, Mapping[str, str]],
    chain: str,
    asset_symbol: str,
    side: str,
) -> Any:
    """Fetch live Aave-fork supply / borrow / utilisation via on-chain
    ``eth_call`` to ``PoolDataProvider.getReserveData(asset)``.

    ``servicer`` is the gateway-side ``RateHistoryServiceServicer`` — we read
    its shared aiohttp session + settings. Returns a ``LendingRatePoint``
    with only the requested ``side`` populated (Empty != Zero: the other
    side is unmeasured by this call, not zero).
    """
    from almanak.gateway.services.rate_history_service import LendingRatePoint

    data_provider = _resolve_data_provider(protocol, contracts_by_chain, chain)
    token_address = _resolve_token_address(protocol, display_name, tokens_by_chain, chain, asset_symbol)
    rpc_url = _resolve_rpc_url(protocol, servicer, chain)

    # Encode eth_call: getReserveData(address)
    padded_addr = token_address[2:].lower().zfill(64)
    calldata = f"0x{_GET_RESERVE_DATA_SELECTOR}{padded_addr}"

    session = await servicer._get_http_session()
    hex_data = await _post_get_reserve_data(
        protocol,
        session,
        rpc_url=rpc_url,
        data_provider=data_provider,
        calldata=calldata,
        chain=chain,
    )

    words = _decode_reserve_words(
        protocol,
        display_name,
        hex_data,
        chain=chain,
        asset_symbol=asset_symbol,
    )

    apy_percent, utilization = _compute_apy_and_utilization(words, side=side)

    logger.debug(
        "%s %s/%s/%s on %s: %.4f%% (utilization=%s)",
        display_name,
        asset_symbol,
        side,
        chain,
        data_provider,
        float(apy_percent),
        f"{float(utilization):.2f}%" if utilization is not None else "n/a",
    )

    # Side selection means the OTHER side is unmeasured by this call —
    # the framework caller passed ``side`` and only that side is what
    # we expose. Empty fields on the wire encode that ("Empty != Zero").
    supply = apy_percent if side == "supply" else None
    borrow = apy_percent if side == "borrow" else None

    # Timestamp 0 == "now" sentinel from the caller; the framework
    # client substitutes ``datetime.now(UTC)`` when packaging the
    # response into ``LendingRate`` to preserve pre-W7 semantics.
    return LendingRatePoint(
        timestamp=0,
        supply_apy_pct=supply,
        borrow_apy_pct=borrow,
        utilization_pct=utilization,
    )


__all__ = ["fetch_aave_fork_lending_current"]
