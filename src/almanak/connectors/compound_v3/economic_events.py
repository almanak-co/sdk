"""Durable Comet absorption and independent missing-market handling in the simulator."""

from datetime import UTC, datetime

from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.engine import Transition
from almanak.backtest.types import UnixTime
from almanak.connectors.compound_v3.economic_data import MODEL, key
from almanak.connectors.compound_v3.economic_liquidation import liquidate
from almanak.connectors.compound_v3.economic_model import EconomicModel, blocked, changes, scope
from almanak.connectors.compound_v3.economic_state import (
    AbsorptionEffect,
    Event,
    EventInput,
    FundInputGap,
    Position,
)
from almanak.effects import Effects
from almanak.runtime import strategy_worker
from almanak.strategy import EventRequest
from almanak.strategy_api import Memory, SimulationEvidence


def groups(state):
    return tuple(
        dict(
            (scope(p.payload.target), p.payload.target) for p in state.portfolio.positions
        ).values()
    )


def indebted(state, target):
    return any(
        p.payload.principal < 0 and scope(p.payload.target) == scope(target)
        for p in state.portfolio.positions
    )


class EconomicDriver(EconomicModel):
    def preview(self, state, target, time):
        try:
            held, market = self.group(state, target, time)
            return liquidate(self.config, tuple(p.payload for p in held), market)
        except InputUnavailable:
            raise
        except ValueError as error:
            raise InputUnavailable(key(target.reserve.market, "risk"), time, str(error)) from error

    def needs_event(self, state, targets, time):
        for target in targets:
            try:
                if self.preview(state, target, time).absorptions:
                    return True
            except InputUnavailable:
                return True
        return False

    def next_event(self, state, until):
        targets = tuple(
            t for t in groups(state) if indebted(state, t) and blocked(state, t) is None
        )
        if not targets:
            return None
        start = max(
            state.time, state.event_cursor + 1 if state.event_cursor is not None else state.time
        )
        if start > until:
            return None
        boundary = self.book.next_boundary(state.time, until)
        stop = min(until, boundary) if boundary is not None else until
        # Scan integer simulation seconds for interest-only risk crossings. This
        # scan does not remove any configured strategy evaluation opportunities.
        for at in range(start, stop + 1):
            if self.needs_event(state, targets, at):
                return UnixTime(at)
        return UnixTime(boundary) if boundary is not None else None

    def external_event(self, state, time):
        positions = tuple(p.payload for p in state.portfolio.positions)
        absorptions: list[AbsorptionEffect] = []
        unavailable, affected, evidence = [], [], set()
        for target in groups(state):
            if not indebted(state, target) or blocked(state, target) is not None:
                continue
            try:
                result = self.preview(state, target, time)
                if (
                    len(absorptions) + len(result.absorptions)
                    > self.settings.data.max_liquidations_per_event
                ):
                    raise InputUnavailable(
                        key(target.reserve.market, "risk"),
                        time,
                        "Comet absorption exceeds the explicit event limit",
                    )
                evidence.update(self.book.evidence(target.reserve.market, time))
            except InputUnavailable as error:
                unavailable.append(error.gap)
                affected.append(FundInputGap(fund=target.fund, gap=error.gap))
                continue
            replaced = {p.target: p for p in result.positions}
            positions = tuple(replaced.get(p.target, p) for p in positions)
            absorptions.extend(result.absorptions)
        if not absorptions and not unavailable:
            return None
        return Transition(
            effects=Effects[Position](positions=changes(state.portfolio.positions, positions)),
            result=Event(
                kind="liquidation" if absorptions else "input_unavailable",
                effective_at=datetime.fromtimestamp(time, UTC),
                absorptions=tuple(absorptions),
                unavailable=tuple(dict.fromkeys(unavailable)),
                affected_funds=tuple(dict.fromkeys(affected)),
                simulation=SimulationEvidence(
                    execution_id=f"{state.run_id}:event:{time}",
                    model=MODEL,
                    inputs=tuple(sorted(evidence)),
                ),
            ),
        )

    def deliver_event(self, state, result):
        request = EventRequest(
            strategy=self.strategy,
            context=EventInput(config=self.config, memory=state.memory, event=result),
        )
        return Memory.model_validate_json(strategy_worker(request))
