"""Declared historical guard requirements shared by readiness and execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from almanak.core.constants import canonical_chain_name
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError


class HistoricalCoverageState(StrEnum):
    UNSUPPORTED = "unsupported_capability"
    INCOMPLETE = "incomplete_historical_coverage"
    TRANSIENT_FAILURE = "transient_provider_failure"
    VERIFIED = "verified_coverage"


@dataclass(frozen=True)
class HistoricalDataDependency:
    """A guard's exact historical data contract for the simulation window."""

    dependency_id: str
    lane: str
    chain: str
    pool_address: str
    protocol: str
    start_time: datetime
    end_time: datetime
    required_fidelity: str
    max_staleness_seconds: int | None = None

    def __post_init__(self) -> None:
        for name in ("dependency_id", "lane", "chain", "pool_address", "protocol", "required_fidelity"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Historical dependency {name} must be nonempty")
        if self.start_time.tzinfo is None or self.end_time.tzinfo is None:
            raise ValueError("Historical dependency windows must be timezone aware")
        if self.end_time <= self.start_time:
            raise ValueError("Historical dependency end_time must follow start_time")
        if self.max_staleness_seconds is not None and self.max_staleness_seconds <= 0:
            raise ValueError("Historical dependency max_staleness_seconds must be positive")

    def to_dict(self) -> dict[str, Any]:
        return {
            "dependency_id": self.dependency_id,
            "lane": self.lane,
            "chain": self.chain,
            "pool_address": self.pool_address,
            "protocol": self.protocol,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "required_fidelity": self.required_fidelity,
            "max_staleness_seconds": self.max_staleness_seconds,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> HistoricalDataDependency:
        return cls(
            **{
                **value,
                "start_time": datetime.fromisoformat(value["start_time"]),
                "end_time": datetime.fromisoformat(value["end_time"]),
            }
        )


@dataclass(frozen=True)
class HistoricalCoverage:
    dependency: HistoricalDataDependency
    state: HistoricalCoverageState | None
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "dependency": self.dependency.to_dict(),
            "provenance": "declared",
            "state": self.state.value if self.state is not None else None,
            "detail": self.detail,
        }


class HistoricalDependencyError(PreflightValidationError):
    code = "HISTORICAL_DATA_DEPENDENCY"

    def __init__(
        self, coverage: tuple[HistoricalCoverage, ...], *, all_coverage: tuple[HistoricalCoverage, ...] | None = None
    ) -> None:
        self.coverage = all_coverage if all_coverage is not None else coverage
        super().__init__(
            message="; ".join(
                f"{item.dependency.dependency_id}: {item.state or 'unverified'}: {item.detail}" for item in coverage
            ),
            failed_checks=["historical_data_dependencies"],
            recommendations=[
                "Use a supported historical data contract or explicitly declare an altered-guard backtest variant."
            ],
            error_count=len(coverage),
            code="HISTORICAL_DATA_DEPENDENCY",
            details={"dependencies": [item.to_dict() for item in coverage]},
        )


class HistoricalDeclarationError(ValueError):
    """Invalid typed declarations retain their unverified coverage evidence."""

    def __init__(self, coverage: tuple[HistoricalCoverage, ...]) -> None:
        self.coverage = coverage
        super().__init__("; ".join(item.detail for item in coverage))


class HistoricalGridCoverage:
    """Bounded-memory completeness check for the simulation's time grid."""

    def __init__(self, config: Any) -> None:
        self.next_time = config.start_time if config.start_time.tzinfo else config.start_time.replace(tzinfo=UTC)
        self.end_time = config.end_time if config.end_time.tzinfo else config.end_time.replace(tzinfo=UTC)
        self.interval = timedelta(seconds=config.interval_seconds)

    def observe(self, timestamp: datetime) -> None:
        timestamp = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=UTC)
        if timestamp != self.next_time or timestamp > self.end_time:
            raise ValueError(f"Historical coverage missing or out of order at {self.next_time.isoformat()}")
        self.next_time += self.interval

    def finish(self) -> None:
        if self.next_time <= self.end_time:
            raise ValueError(f"Historical coverage incomplete at {self.next_time.isoformat()}")


def declared_dependencies(strategy: Any, config: Any) -> tuple[HistoricalDataDependency, ...]:
    """Read declarations without executing strategy decision logic."""
    config.validate_altered_backtest_guards()
    declare = getattr(strategy, "backtest_data_dependencies", None)
    dependencies = tuple(declare(config) if callable(declare) else ())
    if any(not isinstance(item, HistoricalDataDependency) for item in dependencies):
        raise ValueError("backtest_data_dependencies must return HistoricalDataDependency values")
    ids = [item.dependency_id for item in dependencies]
    if len(ids) != len(set(ids)):
        raise ValueError("Historical dependency IDs must be unique")
    for item in dependencies:
        if canonical_chain_name(item.chain.strip()) != canonical_chain_name(config.chain.strip()):
            raise ValueError(f"Historical dependency {item.dependency_id} has a different chain than the run")
    unknown = set(config.altered_backtest_guards) - set(ids)
    if unknown:
        raise ValueError(f"Altered guards must name declared dependencies: {sorted(unknown)}")
    return dependencies


def analytics_target(dependency: HistoricalDataDependency) -> Any | None:
    from almanak.framework.backtesting.pnl.providers.snapshot_pool_analytics import HistoricalPoolAnalyticsTarget

    if dependency.lane != "pool_analytics" or dependency.required_fidelity == "fee_apr":
        return None
    return HistoricalPoolAnalyticsTarget(
        canonical_chain_name(dependency.chain.strip()),
        dependency.protocol,
        dependency.pool_address,
        frozenset({dependency.required_fidelity}),
        max_staleness_seconds=dependency.max_staleness_seconds,
    )


def declared_analytics_targets(
    strategy: Any, config: Any, dependencies: tuple[HistoricalDataDependency, ...] | None = None
) -> tuple[Any, ...]:
    return tuple(
        target
        for item in (declared_dependencies(strategy, config) if dependencies is None else dependencies)
        if item.dependency_id not in config.altered_backtest_guards and (target := analytics_target(item)) is not None
    )


async def check_declared_dependencies(
    strategy: Any, config: Any, dependencies: tuple[HistoricalDataDependency, ...] | None = None
) -> tuple[HistoricalCoverage, ...]:
    """Refuse unwired snapshot lanes independently of optional price preflight.

    A provider's scalar TVL availability is not evidence of a snapshot's ability
    to serve historical tick arrays. New lanes must bind their canonical serving
    source and range validator here before they can certify coverage.
    """
    config.validate_altered_backtest_guards()
    dependencies = declared_dependencies(strategy, config) if dependencies is None else dependencies
    invalid = False

    def resolve(item: HistoricalDataDependency) -> HistoricalCoverage:
        nonlocal invalid
        try:
            target = analytics_target(item)
        except ValueError as exc:
            invalid = True
            return HistoricalCoverage(item, None, f"Invalid declaration {item.dependency_id}: {exc}")
        if item.dependency_id in config.altered_backtest_guards:
            return HistoricalCoverage(
                item,
                None,
                f"Guard explicitly altered; coverage was not validated: {config.altered_backtest_guards[item.dependency_id]}",
            )
        if (
            item.max_staleness_seconds is not None
            and target is not None
            and target.required_fields != frozenset({"tvl_usd"})
        ):
            return HistoricalCoverage(
                item,
                HistoricalCoverageState.UNSUPPORTED,
                "Freshness limits require exact-state tvl_usd provenance; this historical field cannot certify observation age",
            )
        if item.start_time != config.start_time or item.end_time != config.end_time:
            return HistoricalCoverage(
                item,
                HistoricalCoverageState.UNSUPPORTED,
                "The historical serving validator only certifies the run window; "
                "the declared dependency window requires separate historical coverage",
            )
        return HistoricalCoverage(
            dependency=item,
            state=HistoricalCoverageState.UNSUPPORTED if target is None else None,
            detail=(
                f"No historical {item.lane} serving plane certifies {item.required_fidelity} for this snapshot"
                if target is None
                else "Historical analytics coverage has not yet been validated"
            ),
        )

    coverage = tuple(resolve(item) for item in dependencies)
    if invalid:
        raise HistoricalDeclarationError(coverage)
    blockers = tuple(
        item
        for item in coverage
        if item.state == HistoricalCoverageState.UNSUPPORTED
        and item.dependency.dependency_id not in config.altered_backtest_guards
    )
    if blockers:
        raise HistoricalDependencyError(blockers, all_coverage=coverage)
    return coverage
