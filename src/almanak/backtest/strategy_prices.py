"""Strategy-declared price series in backtests: declaration, acquisition scope and delivery.

A strategy declares its series in `data_dependencies`; a backtest never repeats them.
`with_strategy_prices` folds the declared bindings into the resolved settings, beside
the risk and valuation prices the backtest configures. Acquisition then reads each
series from its warm-up start through the preparation's historical gateway, and
`StrategySeries` hands every decision the window available at its time. A
historical_path run has no prepared dataset: `HistoricalSeries` reads the same series
through the run's gateway when the run opens.
"""

import json
import time

from pydantic import Field, model_validator

from almanak.backtest.pool_prices import (
    last_boundary,
    pool_requirement,
    series_records,
    series_start,
    validate_pool_requirement,
    validate_pool_row,
    verify_rows,
)
from almanak.backtest.pricing import PoolPriceRecord, PoolSeriesSource, PriceBinding
from almanak.models import Model
from almanak.price_series import series_at


class StrategyPriceData(Model):
    """A data policy that delivers the strategy's declared price series.

    `prices` is folded in from the strategy at resolution (`with_strategy_prices`),
    never authored, and stays out of the policy identity when empty, so policies
    without series keep their datasets.
    """

    prices: tuple[PriceBinding, ...] = Field(default=(), exclude_if=lambda value: not value)

    @model_validator(mode="before")
    @classmethod
    def price_arrays(cls, value):
        if isinstance(value, dict) and isinstance(value.get("prices"), list):
            value = dict(value) | {"prices": tuple(value["prices"])}
        return value

    @model_validator(mode="after")
    def strategy_prices(self):
        if any(binding.purpose != "strategy" for binding in self.prices):
            raise ValueError("this backtest model takes only strategy price series")
        return self


def declared_prices(strategy, parameters, *, economic=True) -> tuple[PriceBinding, ...]:
    """The strategy's declared prices, from its hook run in the worker sandbox."""
    from almanak.backtest.dependencies import DataDependencies
    from almanak.runtime import strategy_worker
    from almanak.strategy import DataDependencyRequest, EconomicDataDependencyRequest

    if strategy is None:
        return ()
    kind = EconomicDataDependencyRequest if economic else DataDependencyRequest
    request = kind(strategy=strategy, config=parameters)
    prices = DataDependencies.model_validate_json(strategy_worker(request)).prices
    if any(binding.purpose != "strategy" for binding in prices):
        raise ValueError("strategy dependencies must identify strategy-purpose prices")
    series = [binding.asset for binding in prices if is_series(binding)]
    if len(set(series)) != len(series):
        raise ValueError("a strategy declares at most one price series per asset")
    return prices


def is_series(binding: PriceBinding) -> bool:
    """A pool price series, delivered as `PriceSeries` in every run mode.

    Any other strategy price is a single value (a coin price or a protocol oracle),
    which only the Aave V4 economic backtest delivers, as a `PriceObservation`.
    """
    return isinstance(binding.source, PoolSeriesSource)


def live_bindings(strategy, config) -> tuple[PriceBinding, ...]:
    """The declared series a fork or live gateway serves: pool series only.

    Single strategy prices are a backtest-only Aave V4 input and are not read.
    A strategy without a `data_dependencies` hook declares nothing, so the hook and
    its parameter codec are not needed for it.
    """
    from almanak.configuration.parameter_codec import hook_parameters

    # The hook must be named in the source to be defined; skip the worker otherwise.
    if strategy is None or "data_dependencies" not in strategy.source:
        return ()
    # The hook sees the strategy parameters, as at check time, without the
    # deployment's execution envelope.
    prices = declared_prices(strategy, hook_parameters(config), economic=False)
    return tuple(binding for binding in prices if is_series(binding))


def require_history_chains(settings):
    """Every declared series reads its pool through its chain's historical provider."""
    chains = {binding.chain for binding in settings.history}
    for binding in strategy_bindings(settings):
        if is_series(binding) and binding.chain not in chains:
            raise ValueError(
                f"a strategy price series on {binding.chain.chain_id} requires a historical "
                "provider for its chain; bind that chain in the operator configuration"
            )


def with_strategy_prices(settings, strategy, parameters, vendors):
    """The settings with the strategy's declared prices folded in.

    A backtest file may still list the strategy's single prices (the Aave V4 economic
    contract); they must then equal the hook's declaration exactly, and their vendor
    is bound from the operator configuration.
    """
    from almanak.backtest.config import EconomicBacktestConfig

    economic = getattr(settings, "market_model", "economic") != "historical_path"
    prices = declared_prices(strategy, parameters, economic=economic)
    configured = strategy_bindings(settings)
    if configured and set(configured) != set(prices):
        raise ValueError("strategy price sources differ from declared backtest bindings")
    single = isinstance(settings, EconomicBacktestConfig)
    if not single and any(not is_series(binding) for binding in prices):
        raise ValueError(
            "only Aave V4 economic backtests deliver single strategy prices; "
            "declare a pool price series (PoolPrice or PoolSwapCandles)"
        )
    if not prices:
        return settings
    data = getattr(settings, "data", None)
    if data is None or "prices" not in type(data).model_fields:
        raise ValueError("this backtest model does not deliver strategy price series")
    wire = json.loads(settings.model_dump_json())
    # A historical_path policy without series is omitted from the settings wire.
    wire.setdefault("data", {})
    wire["data"]["prices"] = [
        json.loads(b.model_dump_json()) for b in data.prices if b.purpose != "strategy"
    ] + [json.loads(b.model_dump_json()) for b in prices]
    if single:
        wire["data"]["vendors"] = [
            json.loads(b.model_dump_json()) for b in single_vendors(data, prices, vendors)
        ]
    return type(settings).model_validate_json(json.dumps(wire))


def single_vendors(data, prices, vendors):
    """The data policy's vendors plus each operator vendor a single strategy price names."""
    available = {binding.name: binding for binding in vendors}
    bound = {binding.name: binding for binding in data.vendors}
    for binding in prices:
        name = getattr(binding.source, "provider", None)
        if name is None or name in bound:
            continue
        if name not in available:
            raise ValueError(f"operator configuration has no vendor binding named {name}")
        bound[name] = available[name]
    return tuple(bound.values())


def strategy_bindings(settings) -> tuple[PriceBinding, ...]:
    data = getattr(settings, "data", None)
    prices = getattr(data, "prices", ()) if data is not None else ()
    return tuple(b for b in prices if b.purpose == "strategy")


def series_bindings(settings) -> tuple[PriceBinding, ...]:
    """One binding per declared pool series key."""
    unique = {b.key: b for b in strategy_bindings(settings) if is_series(b)}
    return tuple(unique.values())


def strategy_requirements(settings):
    """One prepared requirement per declared series, from its warm-up start."""
    start, end = settings.schedule.start, settings.schedule.deadline
    return tuple(pool_requirement(binding, start, end) for binding in series_bindings(settings))


def point_times(binding, start, end) -> range:
    """The boundaries of the points a backtest from `start` to `end` can show."""
    return range(
        series_start(binding, start), last_boundary(binding, end) + 1, binding.source.seconds
    )


def acquire_series(history, binding, start, end):
    """A series' prepared rows and the compact evidence they name, read through `history`."""
    from almanak.backtest.pool_reads import GatewayReads, acquire

    times = point_times(binding, start, end)
    evidence = acquire(GatewayReads(history), binding, times)
    rows = series_records(binding, evidence, times, time.time())
    return rows, tuple(evidence.bodies.values())


def gateway_authority(settings, config):
    """A historical gateway configuration that also admits the declared pools' reads.

    Each pool's price contract joins its chain's contracts; the pools themselves
    bound which pools the gateway resolves and whose swap logs it reads.
    """
    from almanak.backtest.gateway import ContractBinding
    from almanak.backtest.pool_reads import series_scopes

    scopes = series_scopes(series_bindings(settings))
    if not scopes:
        return config
    bindings = {binding.chain: binding for binding in config.contracts}
    for scope in scopes:
        current = bindings.get(scope.chain, ContractBinding(chain=scope.chain, contracts=()))
        contracts = tuple(sorted({*current.contracts, scope.state}))
        bindings[scope.chain] = current.model_copy(update={"contracts": contracts})
    return config.model_copy(
        update={
            "contracts": tuple(bindings.values()),
            "price_pools": tuple(dict.fromkeys((*config.price_pools, *scopes))),
        }
    )


class StrategySeries:
    """The declared series of one prepared dataset, sliced per decision.

    Opening a dataset rebuilds every point from the evidence it names.
    """

    def __init__(self, reader, manifest, settings):
        start, end = settings.schedule.start, settings.schedule.deadline
        self.timelines = {}
        for binding in series_bindings(settings):
            requirement = manifest.preparation.requirement(binding.key)
            validate_pool_requirement(requirement, binding, start, end)
            timeline = reader.timeline(requirement, PoolPriceRecord)
            for row in timeline.rows:
                validate_pool_row(row, binding)
            verify_rows(binding, timeline.rows, reader.evidence)
            self.timelines[binding] = timeline

    def at(self, time):
        return tuple(
            series_at(binding, timeline.rows, time) for binding, timeline in self.timelines.items()
        )


class HistoricalSeries:
    """The declared series of a historical_path run, read through its gateway at open.

    The gateway retains each read, so a resumed or reproduced run reads the same rows
    offline; each decision still sees only rows available by its time.
    """

    def __init__(self, histories, settings):
        from almanak.backtest.evm import EvmHistory

        start, end = settings.schedule.start, settings.schedule.deadline
        self.rows = {}
        for binding in series_bindings(settings):
            if binding.chain not in histories:
                raise ValueError("a strategy price series requires its chain's historical gateway")
            history = histories[binding.chain]
            if not isinstance(history, EvmHistory):
                history = EvmHistory(history.reader)
            self.rows[binding], _ = acquire_series(history, binding, start, end)

    def at(self, time):
        return tuple(series_at(binding, rows, time) for binding, rows in self.rows.items())
