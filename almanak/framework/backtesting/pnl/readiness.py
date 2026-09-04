"""Canonical pre-launch data-readiness check for historical PnL backtests."""

from __future__ import annotations

import copy
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

from almanak.framework.backtesting.pnl.logging_utils import BacktestLogger

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
    from almanak.framework.backtesting.pnl.engine import BacktestableStrategy, PnLBacktester

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BacktestReadinessResult:
    """Versioned, user-facing verdict produced without executing strategy logic."""

    status: Literal["ready", "ready_with_warnings", "not_ready"]
    checked_at: datetime
    checks: tuple[str, ...]
    blockers: tuple[dict[str, Any], ...] = ()
    warnings: tuple[str, ...] = ()
    observations_checked: int = 0
    schema_version: int = field(default=1, init=False)

    @property
    def ready(self) -> bool:
        return self.status != "not_ready"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "ready": self.ready,
            "checked_at": self.checked_at.isoformat(),
            "checks": list(self.checks),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "observations_checked": self.observations_checked,
        }


def _blocker(exc: BaseException) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "code": getattr(exc, "code", type(exc).__name__),
        "message": str(exc),
    }
    for field_name in ("failed_checks", "recommendations", "error_count", "warning_count", "details"):
        value = getattr(exc, field_name, None)
        if field_name == "details" and not value:
            continue
        if value is not None:
            payload[field_name] = list(value) if isinstance(value, list | tuple) else value
    return payload


def _strategy_intent_names(strategy: Any) -> set[str]:
    metadata = getattr(strategy, "STRATEGY_METADATA", None)
    if metadata is None:
        get_metadata = getattr(strategy, "get_metadata", None)
        metadata = get_metadata() if callable(get_metadata) else None
    names: set[str] = set()
    for intent_type in getattr(metadata, "intent_types", None) or ():
        value = getattr(intent_type, "value", intent_type)
        names.add(str(value).strip().upper())
    return names


def _dynamic_dependency_warnings(
    strategy: Any,
    *,
    perp_targets: tuple[Any, ...],
    declared_perp_targets: tuple[Any, ...],
    twap_source: Any | None,
    pool_state_source: Any | None,
) -> list[str]:
    intents = _strategy_intent_names(strategy)
    unverified: list[str] = []
    if intents.intersection({"PERP_OPEN", "PERP_CLOSE"}) and not perp_targets:
        unverified.append("potential connector-native perp markets selected by emitted intents")
    elif perp_targets and not declared_perp_targets:
        unverified.append("complete funding coverage for config-hinted perp markets")
    if intents.intersection({"SWAP", "LP_OPEN", "LP_CLOSE"}) and twap_source is None:
        unverified.append("potential undeclared exact-pool TWAP reads inside decide()")
    if intents.intersection({"LP_OPEN", "LP_CLOSE"}) and pool_state_source is None:
        unverified.append("potential exact pools selected by emitted LP intents")
    if not unverified:
        return []
    return [
        "Not discoverable without executing strategy.decide(): "
        + "; ".join(unverified)
        + ". Readiness did not verify these first-use paths; the runner resolves or refuses them before use."
    ]


def _readiness_warnings(
    preflight_report: Any,
    strategy: Any,
    *,
    perp_targets: tuple[Any, ...],
    declared_perp_targets: tuple[Any, ...],
    twap_source: Any | None,
    pool_state_source: Any | None,
) -> tuple[str, ...]:
    warnings: list[str] = []
    if preflight_report is not None:
        warnings.extend(check.message for check in preflight_report.failed_checks if check.severity != "error")
        if preflight_report.support is not None:
            warnings.extend(preflight_report.support.warnings)
    warnings.extend(
        _dynamic_dependency_warnings(
            strategy,
            perp_targets=perp_targets,
            declared_perp_targets=declared_perp_targets,
            twap_source=twap_source,
            pool_state_source=pool_state_source,
        )
    )
    return tuple(dict.fromkeys(warnings))


async def check_backtest_readiness(
    backtester: PnLBacktester,
    strategy: BacktestableStrategy,
    config: PnLBacktestConfig,
) -> BacktestReadinessResult:
    """Validate every discoverable data dependency without calling ``decide``.

    This deliberately shares the runner's canonical discovery and provider
    preparation functions. Declared and generated/config-shaped exact-pool OHLCV
    dependencies are authenticated and range-validated before tick 1. Runner-side
    strict preflight remains authoritative in case provider state changes between
    this check and execution.
    """
    from almanak.framework.backtesting.pnl import _engine_helpers
    from almanak.framework.backtesting.pnl.data_broker import data_broker_scope
    from almanak.framework.backtesting.pnl.providers.perp.snapshot_funding import SnapshotFundingRateSource

    checked_at = datetime.now(UTC)
    checks = (
        "support_matrix",
        "funded_price_coverage",
        "perp_price_history",
        "funding_history",
        "historical_exact_pool_twap",
        "historical_exact_pool_state",
        "historical_pool_analytics",
    )
    readiness_config = copy.deepcopy(config)
    readiness_config.preflight_validation = True
    readiness_config.fail_on_preflight_error = True
    bt_logger = BacktestLogger(
        backtest_id=f"readiness-{uuid.uuid4()}",
        json_format=False,
        logger=logger,
    )

    original_strict_historical = (
        backtester.data_config.strict_historical_mode if backtester.data_config is not None else None
    )
    backtester._reset_run_scoped_perp_routes()
    try:
        try:
            await _engine_helpers.prepare_perp_price_history(
                backtester=backtester,
                strategy=strategy,
                config=readiness_config,
                bt_logger=bt_logger,
            )
            perp_targets = backtester._prepared_perp_price_history_targets
            preflight_report, _ = await _engine_helpers.run_preflight(
                backtester=backtester,
                config=readiness_config,
                bt_logger=bt_logger,
                strategy=strategy,
            )
            state = _engine_helpers.initialize_backtest(
                backtester=backtester,
                strategy=strategy,
                config=readiness_config,
                bt_logger=bt_logger,
            )
            assert state.data_broker is not None
            observations_checked = 0
            token_addresses = _engine_helpers._registered_token_addresses(backtester)
            with data_broker_scope(state.data_broker):
                funding_source = SnapshotFundingRateSource(
                    chain=readiness_config.chain,
                    start_time=readiness_config.start_time,
                    end_time=readiness_config.end_time,
                    data_config=backtester.data_config,
                    manifest=state.data_broker.manifest,
                )
                backtester._bind_funding_history_source(funding_source)
                await _engine_helpers._prewarm_funding_history(
                    funding_source,
                    strategy,
                    state.strategy_config,
                    require_complete=True,
                    prepared_targets=backtester._prepared_perp_declared_targets,
                )
                await _engine_helpers._prewarm_funding_history(
                    funding_source,
                    strategy,
                    state.strategy_config,
                    require_complete=False,
                    prepared_targets=backtester._prepared_perp_hint_targets,
                )
                twap_source = await _engine_helpers._prepare_declared_historical_twap(
                    strategy,
                    state.strategy_config,
                    readiness_config,
                    state.data_broker.manifest,
                )
                pool_state_source = await _engine_helpers._prepare_declared_historical_pool_state(
                    strategy,
                    state.strategy_config,
                    readiness_config,
                    state.data_broker.manifest,
                )
                await _engine_helpers._prepare_declared_historical_pool_ohlcv(
                    strategy,
                    state.strategy_config,
                    readiness_config,
                    pool_state_source,
                    token_addresses=token_addresses,
                )
                analytics_targets = _engine_helpers._declared_historical_pool_analytics(
                    strategy,
                    state.strategy_config,
                    readiness_config,
                )
                from almanak.framework.backtesting.pnl.data_broker import pool_history_provider
                from almanak.framework.backtesting.pnl.engine import BacktestPoolAnalyticsReader

                analytics_reader = BacktestPoolAnalyticsReader(pool_history_provider(), readiness_config.chain)

                async for timestamp, market_state in backtester.data_provider.iterate(state.data_config):
                    if token_addresses:
                        market_state.register_symbol_aliases(token_addresses)
                    for token in state.data_config.tokens:
                        price_token = _engine_helpers._normalize_token(
                            token,
                            readiness_config.chain,
                            token_addresses,
                        )
                        if state.portfolio.is_cash_equivalent(price_token):
                            continue
                        try:
                            price = market_state.get_price(price_token)
                        except KeyError as exc:
                            raise ValueError(
                                f"No historical USD price for {token!r} at {timestamp.isoformat()} "
                                f"in requested range {readiness_config.start_time.isoformat()} -> "
                                f"{readiness_config.end_time.isoformat()} with cadence "
                                f"{readiness_config.interval_seconds}s"
                            ) from exc
                        if not price.is_finite() or price <= 0:
                            raise ValueError(
                                f"Invalid historical USD price for {token!r} at {timestamp.isoformat()}: {price!r}"
                            )
                        observations_checked += 1
                    exact_pool_view = pool_state_source.view_at(timestamp) if pool_state_source is not None else None
                    analytics_reader.bind(
                        timestamp,
                        market_state=market_state,
                        pool_state_view=exact_pool_view,
                    )
                    observations_checked += _engine_helpers._validate_declared_historical_pool_analytics(
                        analytics_reader,
                        analytics_targets,
                        timestamp,
                    )
            warnings = _readiness_warnings(
                preflight_report,
                strategy,
                perp_targets=perp_targets,
                declared_perp_targets=backtester._prepared_perp_declared_targets,
                twap_source=twap_source,
                pool_state_source=pool_state_source,
            )
            return BacktestReadinessResult(
                status="ready_with_warnings" if warnings else "ready",
                checked_at=checked_at,
                checks=checks,
                warnings=warnings,
                observations_checked=observations_checked,
            )
        except Exception as exc:  # noqa: BLE001 - structured fail-closed boundary
            return BacktestReadinessResult(
                status="not_ready",
                checked_at=checked_at,
                checks=checks,
                blockers=(_blocker(exc),),
            )
    finally:
        if backtester.data_config is not None and original_strict_historical is not None:
            backtester.data_config.strict_historical_mode = original_strict_historical
        await backtester._release_engine_perp_providers()
        backtester._reset_run_scoped_perp_routes()
