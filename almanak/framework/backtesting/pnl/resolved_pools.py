"""Preflight resolution and pinning for strategy-referenced liquidity pools.

The resolver deliberately separates immutable identity from time-varying
historical state. It discovers an address (for pair references), authenticates
the pool against its connector-owned factory at the backtest boundary, and
pins the result. Simulation consumers then use only the pinned descriptor and
historical data already owned by the run; they never query current liquidity.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from almanak.core.constants import canonical_chain_name
from almanak.framework.backtesting.pnl.data_provider import is_address_like
from almanak.framework.backtesting.pnl.providers.perp._gateway_history import (
    get_connected_gateway_client,
    run_sync_gateway_call,
)
from almanak.framework.backtesting.pnl.providers.snapshot_pool_state import (
    HistoricalPoolStatePoint,
    fetch_historical_pool_state_points,
    require_historical_pool_state,
)
from almanak.framework.data.pools.descriptor import ResolvedPoolDescriptor

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

_POOL_REFERENCE_KEYS = ("pool_address", "pool", "swap_pool")
_PAIR_SEPARATOR_RE = re.compile(r"\s*[/|]\s*")
_TOKEN_CONFIG_FIELDS = (
    ("base_token", "base_token_address"),
    ("quote_token", "quote_token_address"),
    ("token0_token", "token0_token_address"),
    ("token1_token", "token1_token_address"),
)


class PoolResolutionError(ValueError):
    """An explicit strategy pool could not be authenticated for this run."""


@dataclass(frozen=True, slots=True)
class ConfiguredPoolReference:
    """Normalized exact-address or pair-form reference extracted from config."""

    chain: str
    protocol: str
    source_key: str
    address: str | None = None
    token_a: str | None = None
    token_b: str | None = None
    discriminator: int | bool | None = None

    def __post_init__(self) -> None:
        chain = canonical_chain_name(self.chain.strip()).strip().lower()
        protocol = self.protocol.strip().lower().replace("-", "_")
        address = self.address.strip().lower() if self.address is not None else None
        if not chain or not protocol:
            raise PoolResolutionError("pool reference requires chain and protocol")
        if address is not None and not is_address_like(address):
            raise PoolResolutionError(f"{self.source_key} is not an EVM pool address: {address!r}")
        pair = self.token_a is not None or self.token_b is not None
        if (address is None) == (not pair):
            raise PoolResolutionError("pool reference must contain exactly one of address or token pair")
        if pair and (not self.token_a or not self.token_b or self.token_a == self.token_b):
            raise PoolResolutionError(f"{self.source_key} must name two distinct pool tokens")
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "protocol", protocol)
        object.__setattr__(self, "address", address)

    @property
    def display(self) -> str:
        target = self.address or f"{self.token_a}/{self.token_b}"
        return f"{self.chain}:{self.protocol}:{target} ({self.source_key})"


def _string(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _strategy_metadata_protocols(strategy: Any) -> tuple[str, ...]:
    metadata = getattr(strategy, "STRATEGY_METADATA", None)
    if metadata is None:
        getter = getattr(strategy, "get_metadata", None)
        metadata = getter() if callable(getter) else None
    return tuple(
        str(value).strip().lower().replace("-", "_")
        for value in (getattr(metadata, "supported_protocols", None) or ())
        if str(value).strip()
    )


def _reference_protocol(strategy: Any, config: Mapping[str, Any], value: Any, source_key: str) -> str:
    from almanak.connectors._strategy_pool_reader_registry import POOL_READER_REGISTRY

    nested = _string(value.get("protocol")) if isinstance(value, Mapping) else None
    explicit = nested or _string(config.get("protocol")) or _string(getattr(strategy, "protocol", None))
    if explicit is not None:
        normalized = explicit.lower().replace("-", "_")
        spec = POOL_READER_REGISTRY.lookup(normalized)
        if spec is None:
            raise PoolResolutionError(f"{source_key} names unsupported pool protocol {explicit!r}")
        return spec.protocol

    supported: list[str] = []
    for protocol in _strategy_metadata_protocols(strategy):
        spec = POOL_READER_REGISTRY.lookup(protocol)
        if spec is not None and spec.protocol not in supported:
            supported.append(spec.protocol)
    if len(supported) != 1:
        raise PoolResolutionError(f"{source_key} needs one unambiguous pool protocol; candidates={sorted(supported)!r}")
    return supported[0]


def _normalized_address(value: Any) -> str | None:
    text = _string(value)
    return text.lower() if text is not None and is_address_like(text) else None


def _token_reference_text(token: Any) -> str | None:
    if not isinstance(token, Mapping):
        return _string(token)
    return _normalized_address(token.get("address")) or _string(token.get("symbol"))


def _address_from_config_fields(symbol: str, config: Mapping[str, Any]) -> str | None:
    for token_key, address_key in _TOKEN_CONFIG_FIELDS:
        configured = config.get(token_key)
        configured_symbol = configured.get("symbol") if isinstance(configured, Mapping) else configured
        if _string(configured_symbol) is None or str(configured_symbol).upper() != symbol:
            continue
        nested_address = configured.get("address") if isinstance(configured, Mapping) else None
        address = _normalized_address(nested_address) or _normalized_address(config.get(address_key))
        if address is not None:
            return address
    return None


def _address_from_funding(symbol: str, config: Mapping[str, Any]) -> str | None:
    for entry in config.get("token_funding") or ():
        if not isinstance(entry, Mapping) or str(entry.get("symbol", "")).upper() != symbol:
            continue
        address = _normalized_address(entry.get("address"))
        if address is not None:
            return address
    return None


def _address_from_token_resolver(text: str, chain: str) -> str | None:
    from almanak.framework.data.tokens import TokenResolutionError, get_token_resolver

    try:
        resolved = get_token_resolver().resolve(text, chain, log_errors=False, skip_gateway=True)
    except TokenResolutionError:
        return None
    return _normalized_address(getattr(resolved, "address", None))


def _configured_token_address(token: Any, config: Mapping[str, Any], chain: str) -> str | None:
    text = _token_reference_text(token)
    if text is None:
        return None
    direct = _normalized_address(text)
    if direct is not None:
        return direct

    symbol = text.upper()
    return (
        _address_from_config_fields(symbol, config)
        or _address_from_funding(symbol, config)
        or _address_from_token_resolver(text, chain)
    )


def _raw_discriminator(config: Mapping[str, Any], value: Any, kind: str) -> int | bool | None:
    sources = (value, config) if isinstance(value, Mapping) else (config,)
    keys = {
        "fee_tier": ("discriminator", "fee_tier_units", "fee_tier"),
        "tick_spacing": ("discriminator", "tick_spacing"),
        "stable_flag": ("discriminator", "stable", "is_stable"),
    }.get(kind, ("discriminator",))
    raw: Any = None
    for source in sources:
        for key in keys:
            if key in source and source[key] is not None:
                raw = source[key]
                break
        if raw is not None:
            break
    if raw is None:
        return None
    if kind == "stable_flag":
        if type(raw) is bool:
            return raw
        if str(raw).strip().lower() in {"true", "1", "stable"}:
            return True
        if str(raw).strip().lower() in {"false", "0", "volatile"}:
            return False
        raise PoolResolutionError(f"stable_flag discriminator must be boolean, got {raw!r}")
    try:
        parsed = int(raw)
    except (TypeError, ValueError) as exc:
        raise PoolResolutionError(f"{kind} discriminator must be an integer, got {raw!r}") from exc
    if parsed <= 0:
        raise PoolResolutionError(f"{kind} discriminator must be positive, got {parsed}")
    return parsed


def _parse_reference(
    strategy: Any,
    config: Mapping[str, Any],
    source_key: str,
    value: Any,
    *,
    default_chain: str,
) -> ConfiguredPoolReference | None:
    from almanak.connectors._strategy_pool_reader_registry import POOL_READER_REGISTRY

    if value is None:
        return None
    protocol = _reference_protocol(strategy, config, value, source_key)
    spec = POOL_READER_REGISTRY.require(protocol)
    nested_chain = _string(value.get("chain")) if isinstance(value, Mapping) else None
    chain = nested_chain or _string(config.get("chain")) or default_chain
    address: str | None = None
    token_a: Any = None
    token_b: Any = None
    pair_discriminator: Any = None

    if isinstance(value, Mapping):
        address = _string(value.get("address") or value.get("pool_address"))
        token_a = value.get("token0", value.get("token_a"))
        token_b = value.get("token1", value.get("token_b"))
    elif isinstance(value, str):
        text = value.strip()
        if is_address_like(text):
            address = text
        else:
            parts = _PAIR_SEPARATOR_RE.split(text)
            if len(parts) not in (2, 3) or not all(parts):
                raise PoolResolutionError(
                    f"{source_key} must be an EVM address or TOKEN_A/TOKEN_B[/DISCRIMINATOR], got {value!r}"
                )
            token_a, token_b = parts[:2]
            if len(parts) == 3:
                pair_discriminator = parts[2]
    else:
        raise PoolResolutionError(f"{source_key} must be a string or mapping, got {type(value).__name__}")

    if address is not None:
        return ConfiguredPoolReference(
            chain=chain,
            protocol=protocol,
            source_key=source_key,
            address=address,
            discriminator=_raw_discriminator(config, value, spec.discriminator_kind.value),
        )

    token0 = _configured_token_address(token_a, config, chain)
    token1 = _configured_token_address(token_b, config, chain)
    if token0 is None or token1 is None:
        missing = token_a if token0 is None else token_b
        raise PoolResolutionError(f"{source_key} token {missing!r} is not offline-resolvable on {chain}")
    discriminator = (
        _raw_discriminator(config, {"discriminator": pair_discriminator}, spec.discriminator_kind.value)
        if pair_discriminator is not None
        else _raw_discriminator(config, value, spec.discriminator_kind.value)
    )
    return ConfiguredPoolReference(
        chain=chain,
        protocol=protocol,
        source_key=source_key,
        token_a=token0,
        token_b=token1,
        discriminator=discriminator,
    )


def extract_configured_pool_references(
    strategy: Any,
    config: Mapping[str, Any],
    *,
    default_chain: str,
) -> tuple[ConfiguredPoolReference, ...]:
    """Extract all explicit address/pair pool references without guessing arbitrary keys."""
    references: list[ConfiguredPoolReference] = []
    seen_inputs: set[tuple[str, str]] = set()
    for source_key in _POOL_REFERENCE_KEYS:
        for owner, value in (("config", config.get(source_key)), ("strategy", getattr(strategy, source_key, None))):
            if value is None:
                continue
            marker = (source_key, repr(value))
            if marker in seen_inputs:
                continue
            seen_inputs.add(marker)
            reference = _parse_reference(
                strategy,
                config,
                f"{owner}.{source_key}",
                value,
                default_chain=default_chain,
            )
            if reference is not None:
                references.append(reference)

    unique: dict[tuple[Any, ...], ConfiguredPoolReference] = {}
    for reference in references:
        key = (
            reference.chain,
            reference.protocol,
            reference.address,
            reference.token_a,
            reference.token_b,
            reference.discriminator,
        )
        unique.setdefault(key, reference)
    return tuple(unique[key] for key in sorted(unique, key=str))


def _decode_address(raw: str | None) -> str | None:
    if not isinstance(raw, str) or not raw.startswith("0x"):
        return None
    payload = raw[2:]
    if len(payload) < 64:
        return None
    address = "0x" + payload[-40:].lower()
    return None if address == "0x" + "0" * 40 else address


def _decode_uint(raw: str | None, *, signed: bool = False) -> int | None:
    if not isinstance(raw, str) or not raw.startswith("0x") or len(raw) < 66:
        return None
    value = int(raw[2:66], 16)
    return value - 2**256 if signed and value >= 2**255 else value


def _eth_call(client: Any, chain: str, to: str, data: str, block: int) -> str:
    raw = client.eth_call(chain=chain, to=to, data=data, block=block, raise_on_error=True)
    if not isinstance(raw, str) or not raw.startswith("0x"):
        raise PoolResolutionError(f"historical eth_call returned no data for {to} at block {block}")
    return raw


def _resolve_pair_address(client: Any, reference: ConfiguredPoolReference, at_block: int) -> str:
    from almanak.connectors._strategy_base.v3_pool_abi import encode_get_pool
    from almanak.connectors._strategy_pool_reader_registry import POOL_READER_REGISTRY

    assert reference.token_a is not None and reference.token_b is not None
    spec = POOL_READER_REGISTRY.require(reference.protocol)
    factories = spec.factories_for(reference.chain)
    if not factories:
        raise PoolResolutionError(f"{reference.display} has no connector-declared factory")
    keys: Sequence[int | bool]
    if reference.discriminator is not None:
        keys = (reference.discriminator,)
    else:
        keys = spec.candidate_pool_keys
    matches: list[tuple[str, str, int | bool]] = []
    token0, token1 = sorted((reference.token_a, reference.token_b))
    for factory in factories:
        for discriminator in keys:
            encoded = int(discriminator) if type(discriminator) is bool else discriminator
            raw = _eth_call(
                client,
                reference.chain,
                factory,
                encode_get_pool(spec.get_pool_selector, token0, token1, encoded),
                at_block,
            )
            address = _decode_address(raw)
            if address is not None:
                matches.append((address, factory.lower(), discriminator))
    identities = {(address, factory) for address, factory, _ in matches}
    addresses = {address for address, _, _ in matches}
    if not matches:
        raise PoolResolutionError(f"no factory pool exists for {reference.display} at the backtest start")
    if len(addresses) != 1 or len(identities) != 1:
        raise PoolResolutionError(f"ambiguous factory resolution for {reference.display}: {sorted(identities)!r}")
    return matches[0][0]


def _boundary_point(protocol: str, chain: str, address: str, sample: int, interval: int) -> HistoricalPoolStatePoint:
    points = fetch_historical_pool_state_points(
        protocol=protocol,
        chain=chain,
        pool_address=address,
        start_ts=sample,
        end_ts=sample,
        interval_secs=interval,
    )
    if len(points) != 1:
        raise PoolResolutionError(f"historical pool-state boundary returned {len(points)} points, expected 1")
    point = points[0]
    if point.timestamp > sample or sample - point.timestamp > interval:
        raise PoolResolutionError(
            f"historical pool-state boundary is stale/future at {datetime.fromtimestamp(sample, UTC).isoformat()}"
        )
    return point


def _gateway_code(client: Any, gateway_pb2: Any, chain: str, address: str, block: int) -> bytes:
    response = client.rpc.Call(
        gateway_pb2.RpcRequest(
            chain=chain,
            method="eth_getCode",
            params=json.dumps([address, hex(block)]),
        ),
        timeout=client.config.timeout,
    )
    if not response.success or not response.result:
        raise PoolResolutionError(
            f"eth_getCode failed for {chain}:{address} at block {block}: {response.error or 'empty result'}"
        )
    raw = json.loads(response.result)
    if not isinstance(raw, str) or not raw.startswith("0x"):
        raise PoolResolutionError(f"eth_getCode returned malformed data for {chain}:{address} at block {block}")
    return bytes.fromhex(raw[2:])


def _deployment_block(client: Any, gateway_pb2: Any, chain: str, address: str, upper_bound: int) -> int:
    """Binary-search the first block containing code for an immutable pool contract."""
    if not _gateway_code(client, gateway_pb2, chain, address, upper_bound):
        raise PoolResolutionError(f"pool contract {chain}:{address} has no code at verification block {upper_bound}")
    low, high = 0, upper_bound
    while low < high:
        midpoint = (low + high) // 2
        if _gateway_code(client, gateway_pb2, chain, address, midpoint):
            high = midpoint
        else:
            low = midpoint + 1
    return low


def _read_pool_discriminator(
    client: Any,
    reference: ConfiguredPoolReference,
    address: str,
    point: HistoricalPoolStatePoint,
    kind: str,
) -> int | bool | None:
    """Read the connector-declared immutable discriminator at the boundary block."""
    from almanak.connectors._strategy_base.pool_identity_base import STABLE_SELECTOR, TICK_SPACING_SELECTOR

    if kind == "fee_tier":
        return point.fee_tier
    if kind == "tick_spacing":
        return _decode_uint(
            _eth_call(client, reference.chain, address, TICK_SPACING_SELECTOR, point.block_number),
            signed=True,
        )
    if kind == "stable_flag":
        raw = _decode_uint(_eth_call(client, reference.chain, address, STABLE_SELECTOR, point.block_number))
        return bool(raw) if raw in (0, 1) else None
    return None


def _validate_observed_discriminator(
    reference: ConfiguredPoolReference,
    kind: str,
    discriminator: int | bool | None,
) -> None:
    """Require a usable observation and agreement with any configured pool key."""
    if kind != "none" and discriminator is None:
        raise PoolResolutionError(f"could not read {kind} discriminator for {reference.display}")
    if reference.discriminator is not None and reference.discriminator != discriminator:
        raise PoolResolutionError(
            f"{reference.display} discriminator mismatch: configured={reference.discriminator!r}, "
            f"observed={discriminator!r}"
        )


def _factories_for_authentication(spec: Any, reference: ConfiguredPoolReference, kind: str) -> tuple[str, ...]:
    """Return connector-declared factories or reject unsupported keyed pools."""
    factories = tuple(spec.factories_for(reference.chain))
    if factories:
        return factories
    if kind == "none":
        return ()
    raise PoolResolutionError(f"{reference.display} has no connector-declared factory")


def _factory_pool_key(
    reference: ConfiguredPoolReference,
    kind: str,
    discriminator: int | bool | None,
) -> int:
    """Return the ABI-ready factory key, rejecting factory-backed keyless specs."""
    if discriminator is None:
        raise PoolResolutionError(
            f"{reference.display} declares factories but no {kind!r} discriminator; "
            "factory authentication needs a pool key"
        )
    return int(discriminator) if type(discriminator) is bool else discriminator


def _factory_owners(
    client: Any,
    reference: ConfiguredPoolReference,
    address: str,
    point: HistoricalPoolStatePoint,
    spec: Any,
    factories: tuple[str, ...],
    pool_key: int,
) -> list[str]:
    """Return factories whose historical lookup owns the observed pool address."""
    from almanak.connectors._strategy_base.v3_pool_abi import encode_get_pool

    owners = []
    for factory in factories:
        factory_raw = _eth_call(
            client,
            reference.chain,
            factory,
            encode_get_pool(spec.get_pool_selector, point.token0, point.token1, pool_key),
            point.block_number,
        )
        if _decode_address(factory_raw) == address:
            owners.append(factory.lower())
    return owners


def _unique_factory_owner(reference: ConfiguredPoolReference, owners: list[str]) -> str:
    """Require exactly one historical factory owner for an immutable pool."""
    if len(owners) != 1:
        raise PoolResolutionError(
            f"factory verification for {reference.display} expected one owner, observed {owners!r}"
        )
    return owners[0]


def _authenticate_factory(
    client: Any,
    reference: ConfiguredPoolReference,
    address: str,
    point: HistoricalPoolStatePoint,
) -> tuple[str, int | bool | None, str | None]:
    from almanak.connectors._strategy_pool_reader_registry import POOL_READER_REGISTRY

    spec = POOL_READER_REGISTRY.require(reference.protocol)
    kind = spec.discriminator_kind.value
    discriminator = _read_pool_discriminator(client, reference, address, point, kind)
    _validate_observed_discriminator(reference, kind, discriminator)
    factories = _factories_for_authentication(spec, reference, kind)
    if not factories:
        return kind, discriminator, None
    pool_key = _factory_pool_key(reference, kind, discriminator)
    owners = _factory_owners(client, reference, address, point, spec, factories, pool_key)
    return kind, discriminator, _unique_factory_owner(reference, owners)


def _resolve_reference_blocking(
    reference: ConfiguredPoolReference,
    *,
    start_ts: int,
    end_ts: int,
    interval_seconds: int,
) -> ResolvedPoolDescriptor:
    client, gateway_pb2 = get_connected_gateway_client()
    require_historical_pool_state(reference.protocol)

    address = reference.address
    # Pair lookup needs a historical block. Use a boundary observation from a
    # resolved address when available; otherwise the chain head is only used
    # to discover the address, and the address is immediately re-authenticated
    # at the historical start below. No current liquidity participates.
    if address is None:
        head = client.block_number(reference.chain)
        if type(head) is not int or head <= 0:
            raise PoolResolutionError(f"gateway could not determine the {reference.chain} head block")
        address = _resolve_pair_address(client, reference, head)

    start = _boundary_point(reference.protocol, reference.chain, address, start_ts, interval_seconds)
    end = (
        start
        if end_ts == start_ts
        else _boundary_point(reference.protocol, reference.chain, address, end_ts, interval_seconds)
    )
    start_identity = (start.token0, start.token1, start.token0_decimals, start.token1_decimals, start.fee_tier)
    end_identity = (end.token0, end.token1, end.token0_decimals, end.token1_decimals, end.fee_tier)
    if start_identity != end_identity:
        raise PoolResolutionError(
            f"immutable pool identity drifted across requested window for {reference.chain}:{reference.protocol}:{address}"
        )
    if reference.token_a is not None and {start.token0, start.token1} != {reference.token_a, reference.token_b}:
        raise PoolResolutionError(
            f"factory-resolved pool token mismatch for {reference.display}: observed={(start.token0, start.token1)!r}"
        )
    kind, discriminator, factory = _authenticate_factory(client, reference, address, start)
    created_at = _deployment_block(client, gateway_pb2, reference.chain, address, start.block_number)
    return ResolvedPoolDescriptor(
        chain=reference.chain,
        protocol=reference.protocol,
        address=address,
        token0=start.token0,
        token1=start.token1,
        token0_decimals=start.token0_decimals,
        token1_decimals=start.token1_decimals,
        fee_tier_units=start.fee_tier,
        provenance=f"historical:{start.source}",
        factory=factory,
        discriminator_kind=kind,
        discriminator=discriminator,
        deployment_block=created_at,
    )


def _matches_reference(descriptor: ResolvedPoolDescriptor, reference: ConfiguredPoolReference) -> bool:
    if descriptor.chain != reference.chain or descriptor.protocol != reference.protocol:
        return False
    if reference.address is not None and descriptor.address != reference.address:
        return False
    if reference.token_a is not None and {descriptor.token0, descriptor.token1} != {
        reference.token_a,
        reference.token_b,
    }:
        return False
    return reference.discriminator is None or descriptor.discriminator == reference.discriminator


def _validate_pinned_descriptor(descriptor: ResolvedPoolDescriptor) -> None:
    """Require a complete connector-consistent replay identity without RPC discovery."""
    from almanak.connectors._strategy_pool_reader_registry import POOL_READER_REGISTRY

    if descriptor.deployment_block is None:
        raise PoolResolutionError(
            f"pinned descriptor for {descriptor.manifest_key} has no deployment_block; resolve it again"
        )
    spec = POOL_READER_REGISTRY.lookup(descriptor.protocol)
    if spec is None:
        raise PoolResolutionError(f"pinned descriptor names unsupported pool protocol {descriptor.protocol!r}")
    if descriptor.protocol != spec.protocol:
        raise PoolResolutionError(
            f"pinned descriptor protocol {descriptor.protocol!r} is not canonical; use {spec.protocol!r}"
        )
    expected_kind = spec.discriminator_kind.value
    if descriptor.discriminator_kind != expected_kind:
        raise PoolResolutionError(
            f"pinned descriptor for {descriptor.manifest_key} declares discriminator_kind="
            f"{descriptor.discriminator_kind!r}; connector requires {expected_kind!r}"
        )
    expected_factories = {factory.lower() for factory in spec.factories_for(descriptor.chain)}
    if expected_kind != "none":
        if descriptor.factory is None:
            raise PoolResolutionError(f"pinned descriptor for {descriptor.manifest_key} has no authenticated factory")
        if descriptor.factory not in expected_factories:
            raise PoolResolutionError(
                f"pinned descriptor for {descriptor.manifest_key} names factory {descriptor.factory!r}; "
                f"connector declares {sorted(expected_factories)!r}"
            )


async def resolve_configured_pool_descriptors(
    strategy: Any,
    strategy_config: Mapping[str, Any],
    config: PnLBacktestConfig,
) -> tuple[ResolvedPoolDescriptor, ...]:
    """Resolve every explicit pool once, reusing complete pinned identities."""
    references = extract_configured_pool_references(strategy, strategy_config, default_chain=config.chain)
    pinned = {descriptor.key: descriptor for descriptor in config.resolved_pool_descriptors}
    for descriptor in pinned.values():
        _validate_pinned_descriptor(descriptor)
    resolved = dict(pinned)
    start_time = config.start_time if config.start_time.tzinfo is not None else config.start_time.replace(tzinfo=UTC)
    end_time = config.end_time if config.end_time.tzinfo is not None else config.end_time.replace(tzinfo=UTC)
    for reference in references:
        candidates = [descriptor for descriptor in resolved.values() if _matches_reference(descriptor, reference)]
        if len(candidates) > 1:
            raise PoolResolutionError(f"multiple pinned descriptors match {reference.display}")
        if candidates:
            continue
        if reference.address is not None:
            identity = (reference.chain, reference.protocol, reference.address)
            if identity in resolved:
                raise PoolResolutionError(
                    f"pinned descriptor for {reference.display} conflicts with the configured discriminator"
                )
        descriptor = await run_sync_gateway_call(
            _resolve_reference_blocking,
            reference,
            start_ts=int(start_time.astimezone(UTC).timestamp()),
            end_ts=int(end_time.astimezone(UTC).timestamp()),
            interval_seconds=config.interval_seconds,
        )
        previous = resolved.get(descriptor.key)
        if previous is not None and previous != descriptor:
            raise PoolResolutionError(f"resolved pool identity conflicts with pinned {descriptor.manifest_key}")
        resolved[descriptor.key] = descriptor
    return tuple(resolved[key] for key in sorted(resolved))


__all__ = [
    "ConfiguredPoolReference",
    "PoolResolutionError",
    "extract_configured_pool_references",
    "resolve_configured_pool_descriptors",
]
