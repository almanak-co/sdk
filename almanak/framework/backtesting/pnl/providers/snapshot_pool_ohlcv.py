"""Run-scoped exact-pool OHLCV for backtest snapshots."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from almanak.core.asset_identity import AssetIdentity, AssetNamespace
from almanak.framework.backtesting.pnl.data_provider import is_address_like
from almanak.framework.data.interfaces import OHLCVCandle
from almanak.framework.data.pools.descriptor import PoolDescriptor
from almanak.framework.data.timeframes import OHLCVTimeframe, parse_ohlcv_timeframe
from almanak.framework.primitives.types import Primitive
from almanak.framework.venues import (
    ExactVenueFeatureRequest,
    ExactVenueObservation,
    GatewayClientVenueVerificationGateway,
    OhlcvParameters,
    VenueBindingComponent,
    VenueBindingFailure,
    VenueDataFailure,
    VenueReferenceNamespace,
    VenueTargetRef,
    VenueTargetRole,
    VenueVerificationRequest,
    VerifiedVenueBinding,
    observe_exact_venue_data,
)

_EVM_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
_MAX_CANDLES_PER_REQUEST = 1000
_MAX_INTERVAL_SECONDS = 180 * 24 * 60 * 60
MAX_EXACT_POOL_OHLCV_REQUESTS = 30
_DEFAULT_LOOKBACK_CANDLES = 100


def _safe_detail(exc: BaseException) -> str:
    return " ".join(str(exc).split())[:512] or type(exc).__name__


def _unix_seconds(timestamp: datetime) -> int:
    normalized = timestamp.replace(tzinfo=UTC) if timestamp.tzinfo is None else timestamp.astimezone(UTC)
    return int(normalized.timestamp())


def page_candle_capacity(timeframe: OHLCVTimeframe) -> int:
    """Maximum candles one exact-provider request can carry."""
    return min(_MAX_CANDLES_PER_REQUEST, _MAX_INTERVAL_SECONDS // timeframe.seconds)


def materialization_page_count(candle_count: int, timeframe: OHLCVTimeframe) -> int:
    """Bounded provider requests needed for a contiguous candle range."""
    if candle_count <= 0:
        return 0
    capacity = page_candle_capacity(timeframe)
    return (candle_count + capacity - 1) // capacity


def materialization_range(
    start_ts: int,
    end_ts: int,
    timeframe: OHLCVTimeframe,
    lookback_candles: int,
) -> tuple[int, int]:
    """Return the exact half-open candle range loaded for one run."""
    if end_ts < start_ts:
        raise ValueError("end_ts must be greater than or equal to start_ts")
    if type(lookback_candles) is not int or lookback_candles <= 0:
        raise ValueError("lookback_candles must be a positive integer")
    step = timeframe.seconds
    return start_ts - start_ts % step - lookback_candles * step, end_ts - end_ts % step


def materialization_candle_count(
    start_ts: int,
    end_ts: int,
    timeframe: OHLCVTimeframe,
    lookback_candles: int,
) -> int:
    """Candles in the exact range used by materialization and feasibility."""
    range_start, range_end = materialization_range(start_ts, end_ts, timeframe, lookback_candles)
    return (range_end - range_start) // timeframe.seconds


@dataclass(frozen=True, slots=True)
class HistoricalPoolOHLCVTarget:
    """One exact-pool candle lane required by a backtest strategy."""

    chain: str
    protocol: str
    pool_address: str
    base_token_address: str
    quote_token_address: str
    timeframe: OHLCVTimeframe
    lookback_candles: int = _DEFAULT_LOOKBACK_CANDLES

    def __post_init__(self) -> None:
        chain = self.chain.strip().lower()
        protocol = self.protocol.strip().lower().replace("-", "_")
        pool = self.pool_address.strip().lower()
        base = self.base_token_address.strip().lower()
        quote = self.quote_token_address.strip().lower()
        if not chain or not protocol:
            raise ValueError("HistoricalPoolOHLCVTarget chain and protocol are required")
        for field_name, value in (("pool_address", pool), ("base_token_address", base), ("quote_token_address", quote)):
            if not is_address_like(value):
                raise ValueError(f"HistoricalPoolOHLCVTarget.{field_name} is not an EVM address: {value!r}")
        if base == quote:
            raise ValueError("HistoricalPoolOHLCVTarget base and quote tokens must differ")
        timeframe = parse_ohlcv_timeframe(self.timeframe, field_name="HistoricalPoolOHLCVTarget.timeframe")
        if type(self.lookback_candles) is not int or self.lookback_candles <= 0:
            raise ValueError("HistoricalPoolOHLCVTarget.lookback_candles must be a positive integer")
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "protocol", protocol)
        object.__setattr__(self, "pool_address", pool)
        object.__setattr__(self, "base_token_address", base)
        object.__setattr__(self, "quote_token_address", quote)
        object.__setattr__(self, "timeframe", timeframe)

    @property
    def lane_key(self) -> tuple[str, str, str, str, str, OHLCVTimeframe]:
        return (
            self.chain,
            self.protocol,
            self.pool_address,
            self.base_token_address,
            self.quote_token_address,
            self.timeframe,
        )

    @property
    def manifest_key(self) -> str:
        return (
            f"pool:{self.chain}:{self.pool_address}:"
            f"{self.base_token_address}/{self.quote_token_address}@{self.timeframe.value}"
        )


def _address_from_token_config(value: object) -> str | None:
    if not isinstance(value, Mapping):
        return None
    address = value.get("address")
    if not isinstance(address, str):
        return None
    normalized = address.strip().lower()
    return normalized if is_address_like(normalized) else None


def _typed_declarations(strategy: Any) -> object | None:
    getter = getattr(strategy, "get_backtest_pool_ohlcv_targets", None)
    if callable(getter):
        return getter()
    return getattr(strategy, "backtest_pool_ohlcv_targets", None)


def _generated_target(
    strategy: Any,
    strategy_config: Mapping[str, Any],
    *,
    default_chain: str,
) -> HistoricalPoolOHLCVTarget | None:
    """Decode only the generated exact-pool candle call shape."""
    raw_pool = strategy_config.get("swap_pool")
    raw_protocol = strategy_config.get("protocol")
    raw_timeframe = strategy_config.get("data_granularity")
    base = _address_from_token_config(strategy_config.get("base_token"))
    quote = _address_from_token_config(strategy_config.get("quote_token"))
    if (
        not isinstance(raw_pool, str)
        or not isinstance(raw_protocol, str)
        or raw_timeframe is None
        or base is None
        or quote is None
    ):
        return None
    pool = raw_pool.strip().lower()
    protocol = raw_protocol.strip().lower().replace("-", "_")
    if (
        str(getattr(strategy, "swap_pool", "")).strip().lower() != pool
        or str(getattr(strategy, "protocol", "")).strip().lower().replace("-", "_") != protocol
    ):
        return None
    metadata = getattr(strategy, "STRATEGY_METADATA", None)
    if metadata is None:
        get_metadata = getattr(strategy, "get_metadata", None)
        metadata = get_metadata() if callable(get_metadata) else None
    supported = {
        str(value).strip().lower().replace("-", "_") for value in (getattr(metadata, "supported_protocols", None) or ())
    }
    if protocol not in supported:
        return None
    return HistoricalPoolOHLCVTarget(
        chain=default_chain,
        protocol=protocol,
        pool_address=pool,
        base_token_address=base,
        quote_token_address=quote,
        timeframe=parse_ohlcv_timeframe(raw_timeframe, field_name="strategy config data_granularity"),
    )


def declared_historical_pool_ohlcv_targets(
    strategy: Any,
    strategy_config: Mapping[str, Any],
    *,
    default_chain: str,
) -> tuple[HistoricalPoolOHLCVTarget, ...]:
    """Return exact candle lanes safe to materialize before tick 1."""
    raw = _typed_declarations(strategy)
    if raw is not None:
        if isinstance(raw, HistoricalPoolOHLCVTarget):
            values: Iterable[object] = (raw,)
        elif isinstance(raw, Iterable) and not isinstance(raw, str | bytes | Mapping):
            values = raw
        else:
            raise ValueError("pool OHLCV declarations must be HistoricalPoolOHLCVTarget values")
        targets = tuple(values)
        if not all(isinstance(target, HistoricalPoolOHLCVTarget) for target in targets):
            raise ValueError("every pool OHLCV declaration must be a HistoricalPoolOHLCVTarget")
        return tuple(dict.fromkeys(cast(tuple[HistoricalPoolOHLCVTarget, ...], targets)))
    generated = _generated_target(strategy, strategy_config, default_chain=default_chain)
    return (generated,) if generated is not None else ()


def _verify_descriptor(descriptor: PoolDescriptor, block_number: int) -> tuple[VerifiedVenueBinding, Any]:
    """Verify one archive-authenticated descriptor through its connector."""
    if descriptor.fee_tier_units is None:
        raise ValueError("exact V3 OHLCV requires an archive-authenticated fee tier")

    from almanak.connectors._strategy_base.venue_verifier_registry import VenueVerifierRegistry
    from almanak.framework.backtesting.pnl.providers.perp._gateway_history import get_connected_gateway_client

    registry = VenueVerifierRegistry()
    declaration = registry.declaration(descriptor.protocol)
    if declaration.component_names != ("fee",):
        raise ValueError(
            f"exact-pool OHLCV cannot construct verifier components {declaration.component_names!r} "
            f"for protocol {descriptor.protocol!r}"
        )
    request = VenueVerificationRequest(
        chain=descriptor.chain,
        protocol=descriptor.protocol,
        primitive=Primitive.SWAP,
        requested_refs=(
            VenueTargetRef(
                role=VenueTargetRole.POOL,
                reference_namespace=VenueReferenceNamespace.EVM_ADDRESS,
                reference=descriptor.address,
            ),
        ),
        ordered_assets=(
            AssetIdentity(descriptor.chain, AssetNamespace.ERC20, descriptor.token0),
            AssetIdentity(descriptor.chain, AssetNamespace.ERC20, descriptor.token1),
        ),
        binding_components=(VenueBindingComponent(name="fee", value=str(descriptor.fee_tier_units)),),
        binding_policy_version=declaration.binding_policy_version,
    )
    client, _ = get_connected_gateway_client()
    verifier = registry.load_class(descriptor.protocol)()
    result = registry.validate_result(
        request,
        verifier.verify_venue(
            request,
            GatewayClientVenueVerificationGateway(client),
            block_number=block_number,
        ),
    )
    if type(result) is VenueBindingFailure:
        raise ValueError(f"{result.reason_code.value}: {result.detail}")
    return cast(VerifiedVenueBinding, result), client


class SnapshotExactPoolOHLCVSource:
    """Materialize exact-pool ranges once and serve no-lookahead slices."""

    def __init__(
        self,
        pool_state_source: Any,
        *,
        start_time: datetime,
        end_time: datetime,
        token_addresses: Mapping[str, tuple[str, str]] | None = None,
    ) -> None:
        self._pool_state_source = pool_state_source
        self._start_ts = _unix_seconds(start_time)
        self._end_ts = _unix_seconds(end_time)
        if self._end_ts < self._start_ts:
            raise ValueError("end_time must be greater than or equal to start_time")
        self._token_addresses = {str(symbol).upper(): value for symbol, value in (token_addresses or {}).items()}
        self._descriptors: dict[tuple[str, str], PoolDescriptor] = {}
        for descriptor in pool_state_source.descriptors():
            pool_key = (descriptor.chain, descriptor.address)
            previous = self._descriptors.get(pool_key)
            if previous is not None and previous != descriptor:
                raise ValueError(f"ambiguous exact-pool identity for {descriptor.chain}:{descriptor.address}")
            self._descriptors[pool_key] = descriptor
        self._verified: dict[tuple[str, str, str], tuple[VerifiedVenueBinding, Any]] = {}
        self._candles: dict[tuple[str, str, str, int, int, OHLCVTimeframe], dict[int, OHLCVCandle]] = {}
        self._coverage: dict[tuple[str, str, str, int, int, OHLCVTimeframe], tuple[int, int]] = {}
        self._sources: dict[tuple[str, str, str, int, int, OHLCVTimeframe], str] = {}

    def _descriptor(self, chain: str, pool_address: str) -> PoolDescriptor:
        normalized_chain = chain.strip().lower()
        normalized_pool = pool_address.strip().lower()
        descriptor = self._descriptors.get((normalized_chain, normalized_pool))
        if descriptor is None:
            raise ValueError(
                f"ohlcv unavailable: exact pool {normalized_pool!r} on {normalized_chain!r} "
                "was not declared and prewarmed with historical pool state"
            )
        return descriptor

    def _resolve_asset(self, value: str, chain: str) -> str:
        candidate = value.strip()
        lowered = candidate.lower()
        if _EVM_ADDRESS_RE.fullmatch(lowered):
            return lowered
        if ":" in lowered:
            scoped_chain, scoped_address = lowered.split(":", 1)
            if scoped_chain != chain or _EVM_ADDRESS_RE.fullmatch(scoped_address) is None:
                raise ValueError(f"ohlcv unavailable: invalid cross-chain pool asset {value!r}")
            return scoped_address
        alias = self._token_addresses.get(candidate.upper())
        if alias is not None:
            alias_chain, address = alias
            if alias_chain.strip().lower() != chain:
                raise ValueError(f"ohlcv unavailable: pool asset {value!r} resolves on a different chain")
            normalized = address.strip().lower()
            if _EVM_ADDRESS_RE.fullmatch(normalized) is not None:
                return normalized

        from almanak.framework.data.tokens import TokenResolutionError, get_token_resolver

        try:
            resolved = get_token_resolver().resolve(candidate, chain, log_errors=False, skip_gateway=True)
        except TokenResolutionError:
            resolved = None
        address = str(getattr(resolved, "address", "")).strip().lower()
        if _EVM_ADDRESS_RE.fullmatch(address) is None:
            raise ValueError(f"ohlcv unavailable: pool asset {value!r} is not address-resolvable on {chain!r}")
        return address

    def _orientation(
        self,
        descriptor: PoolDescriptor,
        requested_symbol: str | None,
        requested_quote: str | None,
    ) -> tuple[int, int]:
        if not requested_symbol:
            if requested_quote is not None:
                quote = self._resolve_asset(requested_quote, descriptor.chain)
                if quote != descriptor.token1:
                    raise ValueError(
                        f"ohlcv unavailable: explicit quote {requested_quote!r} does not match exact pool "
                        f"quote token {descriptor.token1!r}"
                    )
            return 0, 1
        parts = tuple(part.strip() for part in requested_symbol.split("/"))
        if len(parts) not in (1, 2) or any(not part for part in parts):
            raise ValueError(f"ohlcv unavailable: invalid pool pair {requested_symbol!r}")
        requested = tuple(self._resolve_asset(part, descriptor.chain) for part in parts)
        assets = (descriptor.token0, descriptor.token1)
        if len(requested) == 1:
            if requested[0] not in assets:
                raise ValueError(
                    f"ohlcv unavailable: token {requested_symbol!r} is not in exact pool {descriptor.address!r}"
                )
            base_index = assets.index(requested[0])
            quote_index = 1 - base_index
            if requested_quote is not None:
                quote = self._resolve_asset(requested_quote, descriptor.chain)
                if quote != assets[quote_index]:
                    raise ValueError(
                        f"ohlcv unavailable: explicit quote {requested_quote!r} does not match exact pool "
                        f"counter-asset {assets[quote_index]!r}"
                    )
            return base_index, quote_index
        if requested[0] == requested[1] or set(requested) != set(assets):
            raise ValueError(
                f"ohlcv unavailable: exact pool {descriptor.address!r} assets {assets!r} do not match "
                f"requested pair {requested!r}"
            )
        if requested_quote is not None and self._resolve_asset(requested_quote, descriptor.chain) != requested[1]:
            raise ValueError(
                f"ohlcv unavailable: explicit quote {requested_quote!r} conflicts with requested pair "
                f"{requested_symbol!r}"
            )
        return assets.index(requested[0]), assets.index(requested[1])

    def _binding(self, descriptor: PoolDescriptor) -> tuple[VerifiedVenueBinding, Any]:
        cached = self._verified.get(descriptor.key)
        if cached is not None:
            return cached
        try:
            block_number = self._pool_state_source.verification_block(*descriptor.key)
            verified = _verify_descriptor(descriptor, block_number)
        except Exception as exc:
            raise ValueError(
                f"ohlcv unavailable: exact-pool verification failed for "
                f"{descriptor.chain}:{descriptor.protocol}:{descriptor.address}: {_safe_detail(exc)}"
            ) from exc
        self._verified[descriptor.key] = verified
        return verified

    def _fetch_range(
        self,
        *,
        cache_key: tuple[str, str, str, int, int, OHLCVTimeframe],
        binding: VerifiedVenueBinding,
        client: Any,
        start_ts: int,
        end_ts: int,
    ) -> tuple[dict[int, OHLCVCandle], str | None]:
        from almanak.connectors._strategy_base.v3_exact_data_provider import OHLCV_FEATURE_CONTRACT_VERSION

        timeframe = cache_key[-1]
        page_candles = page_candle_capacity(timeframe)
        requests = materialization_page_count((end_ts - start_ts) // timeframe.seconds, timeframe)
        if requests > MAX_EXACT_POOL_OHLCV_REQUESTS:
            raise ValueError(
                f"exact-pool OHLCV range needs {requests} provider requests, exceeding the "
                f"{MAX_EXACT_POOL_OHLCV_REQUESTS}-request materialization limit"
            )
        pending: dict[int, OHLCVCandle] = {}
        source: str | None = None
        cursor = start_ts
        while cursor < end_ts:
            page_end = min(end_ts, cursor + page_candles * timeframe.seconds)
            request = ExactVenueFeatureRequest(
                verified_binding=binding,
                parameters=OhlcvParameters(
                    base_asset_index=cache_key[3],
                    quote_asset_index=cache_key[4],
                    timeframe=timeframe,
                    start_at=datetime.fromtimestamp(cursor, tz=UTC),
                    end_at=datetime.fromtimestamp(page_end, tz=UTC),
                ),
                feature_contract_version=OHLCV_FEATURE_CONTRACT_VERSION,
            )
            result = observe_exact_venue_data(request, client)
            if isinstance(result, VenueDataFailure):
                raise ValueError(f"{result.reason_code.value}: {result.detail}")
            if type(result) is not ExactVenueObservation:
                raise ValueError("exact-pool OHLCV provider returned an invalid observation")
            if type(result.value) is not tuple or any(type(candle) is not OHLCVCandle for candle in result.value):
                raise ValueError("exact-pool OHLCV provider returned an invalid candle collection")
            page_source = result.provenance.source
            if source is not None and page_source != source:
                raise ValueError("exact-pool OHLCV provider source changed during materialization")
            source = page_source
            for candle in result.value:
                candle_ts = _unix_seconds(candle.timestamp)
                if candle_ts in pending:
                    raise ValueError("exact-pool OHLCV provider returned a duplicate candle timestamp")
                pending[candle_ts] = candle
            cursor = page_end

        expected = tuple(range(start_ts, end_ts, timeframe.seconds))
        if tuple(sorted(pending)) != expected:
            raise ValueError("exact-pool OHLCV provider did not return complete interval coverage")
        return pending, source

    def _ensure_range(
        self,
        *,
        cache_key: tuple[str, str, str, int, int, OHLCVTimeframe],
        binding: VerifiedVenueBinding,
        client: Any,
        start_ts: int,
        end_ts: int,
    ) -> None:
        covered = self._coverage.get(cache_key)
        covered_start, covered_end = covered if covered is not None else (start_ts, start_ts)
        missing_ranges: list[tuple[int, int]] = []
        if start_ts < covered_start:
            missing_ranges.append((start_ts, covered_start))
        if end_ts > covered_end:
            missing_ranges.append((covered_end, end_ts))

        pending: dict[int, OHLCVCandle] = {}
        source = self._sources.get(cache_key)
        for missing_start, missing_end in missing_ranges:
            fetched, fetched_source = self._fetch_range(
                cache_key=cache_key,
                binding=binding,
                client=client,
                start_ts=missing_start,
                end_ts=missing_end,
            )
            if source is not None and fetched_source != source:
                raise ValueError("exact-pool OHLCV provider source changed across materializations")
            source = fetched_source
            pending.update(fetched)

        if pending:
            self._candles.setdefault(cache_key, {}).update(pending)
        if source is not None:
            self._sources[cache_key] = source
        self._coverage[cache_key] = (min(start_ts, covered_start), max(end_ts, covered_end))

    async def materialize_history(self, target: HistoricalPoolOHLCVTarget) -> int:
        """Load one complete run range in bounded pages before execution."""
        from almanak.framework.backtesting.pnl.providers.perp._gateway_history import run_sync_gateway_call

        descriptor = self._descriptor(target.chain, target.pool_address)
        if descriptor.protocol != target.protocol:
            raise ValueError(
                f"ohlcv unavailable: exact pool {descriptor.address!r} was authenticated as "
                f"{descriptor.protocol!r}, not {target.protocol!r}"
            )
        base_index, quote_index = self._orientation(
            descriptor,
            f"{target.base_token_address}/{target.quote_token_address}",
            target.quote_token_address,
        )
        cache_key = (*descriptor.key, base_index, quote_index, target.timeframe)
        range_start, range_end = materialization_range(
            self._start_ts,
            self._end_ts,
            target.timeframe,
            target.lookback_candles,
        )
        candle_count = materialization_candle_count(
            self._start_ts,
            self._end_ts,
            target.timeframe,
            target.lookback_candles,
        )
        requests = materialization_page_count(candle_count, target.timeframe)
        if requests > MAX_EXACT_POOL_OHLCV_REQUESTS:
            raise ValueError(
                f"ohlcv unavailable: {target.manifest_key} needs {requests} provider requests, exceeding the "
                f"{MAX_EXACT_POOL_OHLCV_REQUESTS}-request materialization limit"
            )
        binding, client = await run_sync_gateway_call(self._binding, descriptor)
        await run_sync_gateway_call(
            self._ensure_range,
            cache_key=cache_key,
            binding=binding,
            client=client,
            start_ts=range_start,
            end_ts=range_end,
        )
        return candle_count

    @staticmethod
    def _frame(
        candles: Iterable[OHLCVCandle],
        *,
        descriptor: PoolDescriptor,
        binding: VerifiedVenueBinding,
        base_index: int,
        quote_index: int,
        timeframe: OHLCVTimeframe,
        source: str,
        confidence: str = "exact_pool",
    ) -> Any:
        import pandas as pd

        frame = pd.DataFrame(
            (
                {
                    "timestamp": candle.timestamp,
                    "open": float(candle.open),
                    "high": float(candle.high),
                    "low": float(candle.low),
                    "close": float(candle.close),
                    "volume": float(candle.volume) if candle.volume is not None else float("nan"),
                }
                for candle in candles
            ),
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        assets = binding.binding.ordered_assets
        frame.attrs = {
            "base_asset": assets[base_index].asset_reference,
            "quote_asset": assets[quote_index].asset_reference,
            "timeframe": timeframe.value,
            "source": source,
            "confidence": confidence,
            "pool_address": descriptor.address,
            "chain": descriptor.chain,
            "protocol": descriptor.protocol,
            "binding_hash": binding.binding.binding_hash,
            "capacity_truncated": False,
        }
        return frame

    def get_pool_ohlcv(
        self,
        *,
        pool_address: str,
        chain: str,
        timestamp: datetime,
        timeframe: OHLCVTimeframe,
        limit: int,
        requested_symbol: str | None,
        requested_quote: str | None = None,
    ) -> Any:
        timeframe = parse_ohlcv_timeframe(timeframe, field_name="SnapshotExactPoolOHLCVSource.timeframe")
        limit_value = int(limit)
        descriptor = self._descriptor(chain, pool_address)
        base_index, quote_index = self._orientation(descriptor, requested_symbol, requested_quote)
        cache_key = (*descriptor.key, base_index, quote_index, timeframe)
        if limit_value <= 0:
            binding, _client = self._binding(descriptor)
            return self._frame(
                (),
                descriptor=descriptor,
                binding=binding,
                base_index=base_index,
                quote_index=quote_index,
                timeframe=timeframe,
                source="",
                confidence="identity_only",
            )
        binding_entry = self._verified.get(descriptor.key)
        covered = self._coverage.get(cache_key)
        if binding_entry is None or covered is None:
            raise ValueError(
                "ohlcv unavailable: exact pool orientation/timeframe was not declared and prewarmed; "
                "declare a HistoricalPoolOHLCVTarget through get_backtest_pool_ohlcv_targets()"
            )
        binding, _client = binding_entry
        step = timeframe.seconds
        normalized_tick = timestamp.replace(tzinfo=UTC) if timestamp.tzinfo is None else timestamp.astimezone(UTC)
        tick_ts = int(normalized_tick.timestamp())
        request_end = tick_ts - tick_ts % step
        request_start = request_end - limit_value * step
        covered_start, covered_end = covered
        if request_start < covered_start or request_end > covered_end:
            raise ValueError(
                f"ohlcv unavailable: requested exact-pool range [{request_start}, {request_end}) exceeds "
                f"prewarmed coverage [{covered_start}, {covered_end})"
            )

        requested_timestamps = tuple(range(request_start, request_end, step))
        candles = self._candles[cache_key]
        if any(ts not in candles for ts in requested_timestamps):
            raise ValueError("ohlcv unavailable: exact-pool candle cache lacks complete requested coverage")
        selected = tuple(candles[ts] for ts in requested_timestamps)
        return self._frame(
            selected,
            descriptor=descriptor,
            binding=binding,
            base_index=base_index,
            quote_index=quote_index,
            timeframe=timeframe,
            source=self._sources[cache_key],
        )


__all__ = [
    "HistoricalPoolOHLCVTarget",
    "MAX_EXACT_POOL_OHLCV_REQUESTS",
    "SnapshotExactPoolOHLCVSource",
    "declared_historical_pool_ohlcv_targets",
    "materialization_candle_count",
    "materialization_page_count",
    "materialization_range",
    "page_candle_capacity",
]
