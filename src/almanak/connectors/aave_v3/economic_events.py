"""Durable V3 liquidation events and independent handling of missing Pool history."""

from datetime import UTC, datetime

from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.engine import Transition
from almanak.backtest.types import UnixTime
from almanak.connectors.aave_v3.economic_data import MODEL, key
from almanak.connectors.aave_v3.economic_liquidation import liquidate
from almanak.connectors.aave_v3.economic_model import EconomicModel, blocked, changes, scope
from almanak.connectors.aave_v3.economic_state import Event, EventInput, Position
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
        p.payload.debt_scaled and scope(p.payload.target) == scope(target)
        for p in state.portfolio.positions
    )


class EconomicDriver(EconomicModel):
    def preview(self, state, target, time, pool_positions=None):
        try:
            held, markets = self.group(state, target, time)
            positions = tuple(p.payload for p in held)
            return liquidate(
                self.config,
                positions,
                markets,
                tuple(p.payload for p in state.portfolio.positions)
                if pool_positions is None
                else pool_positions,
                self.settings.data.max_liquidations_per_event,
            )
        except InputUnavailable:
            raise
        except ValueError as error:
            raise InputUnavailable(key(target.reserve, "risk"), time, str(error)) from error

    def needs_event(self, state, targets, time):
        for target in targets:
            try:
                result = self.preview(state, target, time)
            except InputUnavailable:
                return True
            if result.liquidations or result.deficits:
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
        # The simulator's clock is integer seconds. Check that grid between
        # retained market boundaries, including interest-only health crossings.
        # No strategy observation or decision is skipped by this event scan.
        for at in range(start, stop + 1):
            if self.needs_event(state, targets, at):
                return UnixTime(at)
        return UnixTime(boundary) if boundary is not None else None

    def external_event(self, state, time):
        positions = tuple(p.payload for p in state.portfolio.positions)
        liquidations, deficits, unavailable, evidence = [], [], [], set()
        blocked_reasons: list[str] = []
        for target in groups(state):
            if not indebted(state, target) or blocked(state, target) is not None:
                continue
            try:
                result = self.preview(state, target, time, positions)
                for p in result.positions:
                    evidence.update(self.book.evidence(p.target.reserve, time))
            except InputUnavailable as error:
                unavailable.append(error.gap)
                continue
            replaced = {p.target: p for p in result.positions}
            positions = tuple(replaced.get(p.target, p) for p in positions)
            liquidations.extend(result.liquidations)
            deficits.extend(result.deficits)
            blocked_reasons.extend(target.account + ": " + reason for reason in result.blocked)
        if not liquidations and not unavailable:
            return None
        return Transition(
            effects=Effects[Position](positions=changes(state.portfolio.positions, positions)),
            result=Event(
                kind="liquidation" if liquidations else "input_unavailable",
                effective_at=datetime.fromtimestamp(time, UTC),
                liquidations=tuple(liquidations),
                deficits=tuple(deficits),
                unavailable=tuple(dict.fromkeys(unavailable)),
                blocked=tuple(dict.fromkeys(blocked_reasons)),
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
