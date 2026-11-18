"""Bounded canonical liquidity history proves fixed-growth tick continuity."""

from pydantic import model_validator

from almanak.backtest.history_logs import (
    MAX_LOG_BLOCKS,
    HistoricalLog,
    LogScope,
    read_log_windows,
)
from almanak.connectors.v3_pool_models import Target
from almanak.connectors.v3_pool_receipts import POOL_BURN, POOL_MINT
from almanak.connectors.v3_prepared_acquisition import read_tick
from almanak.gateway.evm_logs import data
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import Block, Model, UInt


class BoundaryEvidence[T: Target](Model):
    target: T
    first: Block
    last: Block
    initial_gross: tuple[UInt, UInt] | None
    final_gross: tuple[UInt, UInt] | None
    events: tuple[HistoricalLog, ...]
    contracts: tuple[ContractIdentity, ...]
    inputs: tuple[str, ...]

    @model_validator(mode="after")
    def scope(self):
        if self.first.chain != self.target.fund.chain or self.last.chain != self.first.chain:
            raise ValueError("boundary evidence requires its exact execution chain")
        if self.last.number < self.first.number:
            raise ValueError("boundary evidence requires an increasing interval")
        self.validate_events()
        self.validate_continuity()
        return self

    def validate_events(self):
        orders = tuple(event.order for event in self.events)
        if orders != tuple(sorted(set(orders))):
            raise ValueError("boundary evidence requires distinct canonical event order")
        for event in self.events:
            self.validate_event_bounds(event)

    def validate_event_bounds(self, event):
        if (
            event.block.chain != self.first.chain
            or not self.first.number < event.block.number <= self.last.number
        ):
            raise ValueError("liquidity event is outside its retained historical interval")

    def validate_continuity(self):
        if self.target.range is None:
            self.validate_unbounded_events()
            return
        self.validate_liquidity_continuity()

    def validate_unbounded_events(self):
        # A band has no fixed boundary to measure, and a trading target holds no range.
        # The book reconciles a band's retained events against each pair of consecutive
        # samples; for both, the events show collection which ticks changed liquidity.
        if (self.initial_gross, self.final_gross) != (None, None):
            raise ValueError("only a fixed range can claim boundary measurements")
        for event in self.events:
            liquidity_event(self.target, event)

    def validate_liquidity_continuity(self):
        if self.initial_gross is None or self.final_gross is None:
            raise ValueError("fixed historical LP growth requires boundary evidence")
        if continuous_gross(self.target, self.initial_gross, self.events) != self.final_gross:
            raise ValueError("liquidity history does not reconcile with boundary measurements")


def liquidity_event(target, event):
    if event.address != target.pool.pool or len(event.topics) != 4:
        raise ValueError("liquidity event requires the configured V3 pool")
    lower, upper = tuple(data({"data": word}, ("int24",))[0] for word in event.topics[2:])
    if not -887272 <= lower < upper <= 887272:
        raise ValueError("liquidity event has invalid tick boundaries")
    if event.topics[0] == POOL_MINT:
        _, change, _, _ = data({"data": event.data}, ("address", "uint128", "uint256", "uint256"))
    elif event.topics[0] == POOL_BURN:
        value, _, _ = data({"data": event.data}, ("uint128", "uint256", "uint256"))
        change = -value
    else:
        raise ValueError("liquidity history contains another protocol event")
    return lower, upper, change


def continuous_gross(target, initial, events):
    if target.range is None or min(initial) <= 0 or max(initial) >= 2**128:
        raise ValueError("fixed historical LP growth requires initialized initial boundaries")
    gross = list(initial)
    for event in events:
        lower, upper, change = liquidity_event(target, event)
        for index, boundary in enumerate((target.range.lower, target.range.upper)):
            if boundary in (lower, upper):
                gross[index] += change
                if not 0 < gross[index] < 2**128:
                    raise ValueError(
                        "LP boundary cleared or reinitialized inside historical window"
                    )
    return tuple(gross)


def tick_gross(reader, target, block, *, tick_reader=read_tick):
    if target.range is None:
        raise ValueError("tick continuity requires an explicit LP range")
    return tuple(
        tick_reader(reader, target.pool, block, tick)[0]
        for tick in (target.range.lower, target.range.upper)
    )


def log_requests(first, last):
    return (last.number - first.number + MAX_LOG_BLOCKS - 1) // MAX_LOG_BLOCKS


def require_log_requests(first, last, policy):
    requests = log_requests(first, last)
    if requests > policy.max_liquidity_log_requests:
        raise ValueError(
            f"V3 window needs {requests} liquidity log requests, above the explicit "
            "max_liquidity_log_requests limit"
        )


def liquidity_logs(history, target, first, last, policy):
    require_log_requests(first, last, policy)
    scope = LogScope(
        address=target.pool.pool,
        topics=(POOL_MINT, POOL_BURN),
        first_block=first.number,
        last_block=last.number,
    )
    events: list[HistoricalLog] = []
    for chunk in read_log_windows(history, scope, first.number + 1, last.number):
        events.extend(chunk)
        if len(events) > policy.max_liquidity_events:
            raise ValueError("V3 window exceeds the explicit max_liquidity_events limit")
    return tuple(events)


def preflight(
    history, target, schedule, policy, *, contract_identities, evidence_type, tick_reader=read_tick
):
    first, last = history.floor(schedule.start), history.floor(schedule.deadline)
    reader = history.reader
    reader.used.clear()
    history.pin(first)
    contracts = contract_identities(reader, target, first)
    initial = tick_gross(reader, target, first, tick_reader=tick_reader) if target.range else None
    # Every target retains the pool's liquidity events: a range or band reconciles its
    # continuity with them, and collection reads only the ticks they or the price changed.
    events = liquidity_logs(history, target, first, last, policy)
    history.pin(last)
    final = tick_gross(reader, target, last, tick_reader=tick_reader) if target.range else None
    if contract_identities(reader, target, last) != contracts:
        raise ValueError("V3 historical implementation changed during the window")
    return evidence_type(
        target=target,
        first=first,
        last=last,
        initial_gross=initial,
        final_gross=final,
        events=events,
        contracts=contracts,
        inputs=tuple(sorted(reader.used)),
    )
