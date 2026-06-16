"""Gateway-side Etherscan-compatible gas price lookup.

The backtesting gas provider used to open Etherscan-family HTTP sessions
and archive-RPC connections inside the framework process. This module is the
gateway-owned egress path for the first small RateHistory gas slice.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.gateway.utils.rpc_provider import get_gateway_api_key

if TYPE_CHECKING:
    from almanak.gateway.services.rate_history_service import (
        GasPricePoint,
        RateHistoryServiceServicer,
    )

_DEFAULT_BLOCK_TIME_SECONDS = 12.0
_MIN_REQUEST_INTERVAL_SECONDS = 0.25
_CURRENT_GAS_CACHE_TTL_SECONDS = 60.0
_MAX_GAS_CACHE_ENTRIES = 10_000
_WEI_PER_GWEI = Decimal("1000000000")
_GAS_CACHE: dict[tuple[str, int], tuple[Any, str, float | None]] = {}
_GAS_CACHE_LOCK = asyncio.Lock()
_GAS_IN_FLIGHT: dict[tuple[str, int], asyncio.Task[tuple[Any, str]]] = {}
_GAS_IN_FLIGHT_LOCK = asyncio.Lock()
_EGRESS_RATE_LOCK = asyncio.Lock()
_last_egress_at = 0.0


async def fetch_gas_price_at(
    servicer: RateHistoryServiceServicer,
    *,
    chain: str,
    timestamp: int,
    descriptor: Any | None = None,
) -> tuple[GasPricePoint, str]:
    """Fetch current or historical gas price through gateway-owned egress.

    ``timestamp=0`` uses the chain explorer's gas oracle. ``timestamp>0``
    estimates a block by descriptor block time and reads ``baseFeePerGas``
    from the configured RPC provider.
    """
    descriptor = _evm_descriptor(chain) if descriptor is None else descriptor
    cache_key = (descriptor.name, timestamp)
    cached = await _gas_cache_get(cache_key)
    if cached is not None:
        return cached

    async with _GAS_IN_FLIGHT_LOCK:
        cached = await _gas_cache_get(cache_key)
        if cached is not None:
            return cached
        task = _GAS_IN_FLIGHT.get(cache_key)
        if task is None:
            task = asyncio.create_task(
                _fetch_and_cache_gas_price(servicer, descriptor=descriptor, timestamp=timestamp, cache_key=cache_key)
            )
            task.add_done_callback(lambda completed: _clear_completed_gas_request(cache_key, completed))
            _GAS_IN_FLIGHT[cache_key] = task

    return await asyncio.shield(task)


async def _fetch_and_cache_gas_price(
    servicer: RateHistoryServiceServicer,
    *,
    descriptor: Any,
    timestamp: int,
    cache_key: tuple[str, int],
) -> tuple[GasPricePoint, str]:
    if timestamp == 0:
        result = await _fetch_current_gas_oracle(servicer, chain=descriptor.name, descriptor=descriptor)
    else:
        result = await _fetch_historical_archive_gas(
            servicer,
            chain=descriptor.name,
            timestamp=timestamp,
            descriptor=descriptor,
        )
    await _gas_cache_set(cache_key, result, current=timestamp == 0)
    return result


def _clear_completed_gas_request(key: tuple[str, int], task: asyncio.Task[tuple[Any, str]]) -> None:
    if _GAS_IN_FLIGHT.get(key) is task:
        _GAS_IN_FLIGHT.pop(key, None)


async def _fetch_current_gas_oracle(
    servicer: RateHistoryServiceServicer,
    *,
    chain: str,
    descriptor: Any | None = None,
) -> tuple[GasPricePoint, str]:
    from almanak.gateway.services.rate_history_service import GasPricePoint, RateHistoryUnavailable

    descriptor = _evm_descriptor(chain) if descriptor is None else descriptor
    if descriptor.explorer.api_url is None:
        raise RateHistoryUnavailable("etherscan", f"no explorer API URL configured for chain {chain!r}")

    params = {"module": "gastracker", "action": "gasoracle"}
    api_key = _api_key_for_descriptor(descriptor.explorer.api_key_env)
    if api_key:
        params["apikey"] = api_key

    session = await servicer._get_http_session()
    try:
        await _wait_for_egress_slot()
        async with session.get(descriptor.explorer.api_url, params=params) as response:
            if response.status != 200:
                raise RateHistoryUnavailable("etherscan", f"gas oracle HTTP {response.status}")
            payload = await response.json()
    except RateHistoryUnavailable:
        raise
    except Exception as exc:
        raise RateHistoryUnavailable("etherscan", "gas oracle request failed") from exc

    if not isinstance(payload, dict):
        raise RateHistoryUnavailable("etherscan", f"gas oracle returned {type(payload).__name__}, expected object")
    if str(payload.get("status", "")) not in ("", "1"):
        raise RateHistoryUnavailable("etherscan", "gas oracle returned unsuccessful status")

    result = payload.get("result")
    if not isinstance(result, dict):
        raise RateHistoryUnavailable("etherscan", "gas oracle response missing result object")

    base_fee = _decimal_or_none(result.get("suggestBaseFee"))
    gas_price = _decimal_or_none(result.get("ProposeGasPrice"))
    priority_fee = None
    if base_fee is not None and gas_price is not None and gas_price >= base_fee:
        priority_fee = gas_price - base_fee

    if base_fee is None and gas_price is None:
        raise RateHistoryUnavailable("etherscan", "gas oracle returned no usable gas fields")

    return (
        GasPricePoint(
            timestamp=int(datetime.now(UTC).timestamp()),
            base_fee_gwei=base_fee,
            priority_fee_gwei=priority_fee,
            gas_price_gwei=gas_price,
        ),
        "etherscan",
    )


async def _fetch_historical_archive_gas(
    servicer: RateHistoryServiceServicer,
    *,
    chain: str,
    timestamp: int,
    descriptor: Any | None = None,
) -> tuple[GasPricePoint, str]:
    from almanak.gateway.services.rate_history_service import GasPricePoint, RateHistoryUnavailable

    descriptor = _evm_descriptor(chain) if descriptor is None else descriptor
    block_time = descriptor.rpc.block_time_seconds or _DEFAULT_BLOCK_TIME_SECONDS
    web3 = await servicer._get_web3(chain)

    try:
        await _wait_for_egress_slot()
        latest = await web3.eth.get_block("latest")
        latest_number = int(latest["number"])
        latest_ts = int(latest["timestamp"])
    except Exception as exc:
        raise RateHistoryUnavailable("archive_rpc", "latest block lookup failed") from exc

    seconds_ago = latest_ts - timestamp
    if seconds_ago < 0:
        raise RateHistoryUnavailable("archive_rpc", "requested timestamp is after the latest block")

    target_block = max(1, latest_number - int(seconds_ago / block_time))
    try:
        await _wait_for_egress_slot()
        block = await web3.eth.get_block(target_block)
    except Exception as exc:
        raise RateHistoryUnavailable("archive_rpc", "historical block lookup failed") from exc

    base_fee_raw = _block_value(block, "baseFeePerGas")
    if base_fee_raw is None:
        raise RateHistoryUnavailable("archive_rpc", f"block {target_block} has no baseFeePerGas")

    timestamp_raw = _block_value(block, "timestamp")
    try:
        block_timestamp = _int_from_rpc_quantity(timestamp_raw)
    except (TypeError, ValueError) as exc:
        raise RateHistoryUnavailable("archive_rpc", f"invalid timestamp on block {target_block}") from exc

    try:
        base_fee_gwei = Decimal(_int_from_rpc_quantity(base_fee_raw)) / _WEI_PER_GWEI
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise RateHistoryUnavailable("archive_rpc", f"invalid baseFeePerGas on block {target_block}") from exc

    return (
        GasPricePoint(
            timestamp=block_timestamp,
            base_fee_gwei=base_fee_gwei,
            priority_fee_gwei=None,
            gas_price_gwei=None,
        ),
        "archive_rpc",
    )


def _evm_descriptor(chain: str):
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None:
        raise RateHistoryUnavailable("chain_registry", f"unknown chain {chain!r}")
    if descriptor.family is not ChainFamily.EVM:
        raise RateHistoryUnavailable("chain_registry", f"chain {chain!r} is not EVM")
    return descriptor


def _api_key_for_descriptor(env_name: str | None) -> str | None:
    if not env_name:
        return None
    return get_gateway_api_key(env_name)


async def _gas_cache_get(key: tuple[str, int]) -> tuple[GasPricePoint, str] | None:
    now = time.monotonic()
    async with _GAS_CACHE_LOCK:
        cached = _GAS_CACHE.get(key)
        if cached is None:
            return None
        point, source, expires_at = cached
        if expires_at is not None and now >= expires_at:
            _GAS_CACHE.pop(key, None)
            return None
        return point, source


async def _gas_cache_set(key: tuple[str, int], value: tuple[GasPricePoint, str], *, current: bool) -> None:
    expires_at = time.monotonic() + _CURRENT_GAS_CACHE_TTL_SECONDS if current else None
    async with _GAS_CACHE_LOCK:
        _GAS_CACHE.pop(key, None)
        _GAS_CACHE[key] = (value[0], value[1], expires_at)
        _evict_gas_cache_locked(time.monotonic())


def _evict_gas_cache_locked(now: float) -> None:
    expired_keys = [
        key for key, (_, _, expires_at) in _GAS_CACHE.items() if expires_at is not None and now >= expires_at
    ]
    for key in expired_keys:
        _GAS_CACHE.pop(key, None)
    while len(_GAS_CACHE) > _MAX_GAS_CACHE_ENTRIES:
        _GAS_CACHE.pop(next(iter(_GAS_CACHE)), None)


async def _wait_for_egress_slot() -> None:
    global _last_egress_at

    async with _EGRESS_RATE_LOCK:
        now = time.monotonic()
        remaining = _MIN_REQUEST_INTERVAL_SECONDS - (now - _last_egress_at)
        if remaining > 0:
            await asyncio.sleep(remaining)
        _last_egress_at = time.monotonic()


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _block_value(block: Any, key: str) -> Any:
    if isinstance(block, dict):
        return block.get(key)
    return getattr(block, key, None)


def _int_from_rpc_quantity(value: Any) -> int:
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    if isinstance(value, bytes | bytearray):
        return int.from_bytes(value, byteorder="big")
    hex_method = getattr(value, "hex", None)
    if callable(hex_method):
        hex_value = hex_method()
        if isinstance(hex_value, str):
            return int(hex_value[2:] if hex_value.startswith("0x") else hex_value, 16)
    return int(value)
