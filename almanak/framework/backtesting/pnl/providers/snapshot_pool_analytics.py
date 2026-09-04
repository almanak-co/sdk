"""Declared historical exact-pool analytics dependencies for backtests."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, cast

from almanak.framework.backtesting.pnl.data_provider import is_address_like

_SUPPORTED_FIELDS = frozenset({"tvl_usd", "volume_24h_usd", "volume_7d_usd", "fee_apr", "fee_apy"})


@dataclass(frozen=True, slots=True)
class HistoricalPoolAnalyticsTarget:
    """Required analytics fields for one exact pool over a historical run."""

    chain: str
    protocol: str
    pool_address: str
    required_fields: frozenset[str] = field(default_factory=lambda: frozenset({"tvl_usd"}))
    max_staleness_seconds: int | None = None

    def __post_init__(self) -> None:
        chain = self.chain.strip().lower()
        protocol = self.protocol.strip().lower().replace("-", "_")
        pool = self.pool_address.strip().lower()
        fields = frozenset(str(value).strip() for value in self.required_fields)
        if not chain or not protocol:
            raise ValueError("HistoricalPoolAnalyticsTarget chain and protocol are required")
        if not is_address_like(pool):
            raise ValueError(f"HistoricalPoolAnalyticsTarget.pool_address is not an EVM address: {pool!r}")
        if not fields:
            raise ValueError("HistoricalPoolAnalyticsTarget.required_fields cannot be empty")
        unknown = fields - _SUPPORTED_FIELDS
        if unknown:
            raise ValueError(f"unsupported historical pool-analytics fields: {sorted(unknown)!r}")
        if self.max_staleness_seconds is not None and self.max_staleness_seconds <= 0:
            raise ValueError("HistoricalPoolAnalyticsTarget.max_staleness_seconds must be positive when provided")
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "protocol", protocol)
        object.__setattr__(self, "pool_address", pool)
        object.__setattr__(self, "required_fields", fields)

    @property
    def key(self) -> tuple[str, str, str]:
        return self.chain, self.protocol, self.pool_address

    @property
    def manifest_key(self) -> str:
        fields = ",".join(sorted(self.required_fields))
        return f"{self.chain}:{self.protocol}:{self.pool_address}:fields={fields}"


def _typed_declarations(strategy: Any) -> object | None:
    getter = getattr(strategy, "get_backtest_pool_analytics_targets", None)
    if callable(getter):
        return getter()
    return getattr(strategy, "backtest_pool_analytics_targets", None)


def declared_historical_pool_analytics_targets(
    strategy: Any,
    strategy_config: Mapping[str, Any],
    *,
    default_chain: str,
) -> tuple[HistoricalPoolAnalyticsTarget, ...]:
    """Return the strategy's typed pool-analytics declarations (config keys are never read)."""
    raw = _typed_declarations(strategy)
    if raw is not None:
        if isinstance(raw, HistoricalPoolAnalyticsTarget):
            values: Iterable[object] = (raw,)
        elif isinstance(raw, Iterable) and not isinstance(raw, str | bytes | Mapping):
            values = raw
        else:
            raise ValueError("pool-analytics declarations must be HistoricalPoolAnalyticsTarget values")
        targets = tuple(values)
        if not all(isinstance(target, HistoricalPoolAnalyticsTarget) for target in targets):
            raise ValueError("every pool-analytics declaration must be a HistoricalPoolAnalyticsTarget")
        return tuple(dict.fromkeys(cast(tuple[HistoricalPoolAnalyticsTarget, ...], targets)))
    # No config-key discovery: identity comes from the typed hook above or, at
    # first use, from the analytics read that names the pool.
    return ()


def validate_historical_pool_analytics(
    reader: Any,
    targets: tuple[HistoricalPoolAnalyticsTarget, ...],
    timestamp: datetime,
) -> int:
    """Validate every required field at one tick and return check count."""
    checked = 0
    for target in targets:
        envelope = reader.get_pool_analytics(
            pool_address=target.pool_address,
            chain=target.chain,
            protocol=target.protocol,
        )
        unmeasured = target.required_fields & envelope.value.unmeasured_fields
        if unmeasured:
            raise ValueError(
                f"historical pool analytics unavailable for {target.manifest_key} at {timestamp.isoformat()}: "
                f"required fields are unmeasured: {sorted(unmeasured)!r}"
            )
        for field_name in target.required_fields:
            value = getattr(envelope.value, field_name)
            if not Decimal(str(value)).is_finite():
                raise ValueError(
                    f"historical pool analytics invalid for {target.manifest_key} at {timestamp.isoformat()}: "
                    f"{field_name}={value!r}"
                )
        if target.max_staleness_seconds is not None:
            freshness_unmeasured = target.required_fields != frozenset({"tvl_usd"})
            freshness_unmeasured |= getattr(envelope.meta, "block_number", None) is None
            if freshness_unmeasured:
                raise ValueError(
                    f"historical pool analytics freshness is unmeasured for {target.manifest_key} "
                    f"at {timestamp.isoformat()}: freshness limits require exact-state tvl_usd provenance"
                )
            if envelope.meta.staleness_ms >= target.max_staleness_seconds * 1000:
                raise ValueError(
                    f"historical pool analytics stale for {target.manifest_key} at {timestamp.isoformat()}: "
                    f"age={envelope.meta.staleness_ms}ms, limit={target.max_staleness_seconds * 1000}ms"
                )
        checked += 1
    return checked
