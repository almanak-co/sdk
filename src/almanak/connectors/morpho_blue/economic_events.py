"""Persist archival net flows and liquidations without merging fund ownership."""

from datetime import UTC, datetime

from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.engine import Transition
from almanak.backtest.types import UnixTime
from almanak.connectors.morpho_blue.economic_data import MODEL, key
from almanak.connectors.morpho_blue.economic_journal import anchor, retained
from almanak.connectors.morpho_blue.economic_liquidation import LiquidationChange
from almanak.connectors.morpho_blue.economic_liquidation import apply as liquidate
from almanak.connectors.morpho_blue.economic_model import EconomicModel, blocked, changes
from almanak.connectors.morpho_blue.economic_positions import funds, validate_positions
from almanak.connectors.morpho_blue.economic_state import (
    Event,
    EventInput,
    MarketInputGap,
    Position,
)
from almanak.effects import Effects
from almanak.runtime import strategy_worker
from almanak.strategy import EventRequest
from almanak.strategy_api import Memory, SimulationEvidence


def market_event(book, state, source, time, limit):
    ledger = retained(book, state, source)
    market = book.at(source, time)
    positions = tuple(
        p.payload for p in state.portfolio.positions if p.payload.target.reserve.market == source
    )
    updated = anchor(book, ledger, time)
    market_changes = [] if updated is None else [updated]
    ledger = ledger if updated is None else updated.after
    validate_positions(positions, market, ledger, time)
    liquidations: list[LiquidationChange] = []
    affected = []
    for fund in funds(positions):
        group = tuple(p for p in positions if p.target.fund == fund)
        if blocked(state, group[0].target) is not None:
            continue
        try:
            result = liquidate(group, market, ledger, fund, time=time)
        except ValueError as error:
            gap = InputUnavailable(key(source, "risk"), time, str(error)).gap
            affected.append(MarketInputGap(source=source, fund=fund, gap=gap))
            continue
        if result is None:
            continue
        if len(liquidations) >= limit:
            raise ValueError("Morpho liquidation exceeds the explicit event limit")
        changed = {p.target: p for p in result.positions}
        positions = tuple(changed.get(p.target, p) for p in positions)
        ledger = result.market.after
        market_changes.append(result.market)
        liquidations.append(result)
    validate_positions(positions, market, ledger, time)
    return positions, tuple(market_changes), tuple(liquidations), tuple(affected)


class EconomicDriver(EconomicModel):
    def sources(self, state):
        return tuple(
            dict.fromkeys(
                p.payload.target.reserve.market
                for p in state.portfolio.positions
                if blocked(state, p.payload.target) is None
            )
        )

    def preview(self, state, source, time):
        try:
            return market_event(
                self.book, state, source, time, self.settings.data.max_liquidations_per_event
            )
        except InputUnavailable:
            raise
        except ValueError as error:
            raise InputUnavailable(key(source, "risk"), time, str(error)) from error

    def next_event(self, state, until):
        sources = self.sources(state)
        if not sources:
            return None
        start = max(
            state.time, state.event_cursor + 1 if state.event_cursor is not None else state.time
        )
        boundary = self.book.next_boundary(state.time, until)
        stop = min(until, boundary) if boundary is not None else until
        for at in range(start, stop + 1):
            for source in sources:
                try:
                    _, updates, liquidations, gaps = self.preview(state, source, UnixTime(at))
                    if updates or liquidations or gaps:
                        return UnixTime(at)
                except InputUnavailable:
                    return UnixTime(at)
        return UnixTime(boundary) if boundary is not None else None

    def external_event(self, state, time):
        positions = tuple(p.payload for p in state.portfolio.positions)
        liquidations: list[LiquidationChange] = []
        updates, unavailable, affected, evidence = [], [], [], set()
        for source in self.sources(state):
            try:
                changed, market_changes, losses, fund_gaps = self.preview(state, source, time)
                if len(liquidations) + len(losses) > self.settings.data.max_liquidations_per_event:
                    raise InputUnavailable(
                        key(source, "risk"),
                        time,
                        "Morpho liquidation exceeds the explicit event limit",
                    )
                evidence.update(self.book.evidence(source, time))
            except InputUnavailable as error:
                unavailable.append(error.gap)
                affected.append(MarketInputGap(source=source, gap=error.gap))
                continue
            affected.extend(fund_gaps)
            unavailable.extend(item.gap for item in fund_gaps)
            replaced = {p.target: p for p in changed}
            positions = tuple(replaced.get(p.target, p) for p in positions)
            updates.extend(market_changes)
            liquidations.extend(losses)
        if not updates and not unavailable:
            return None
        return Transition(
            effects=Effects[Position](positions=changes(state.portfolio.positions, positions)),
            result=Event(
                kind="liquidation"
                if liquidations
                else "input_unavailable"
                if unavailable
                else "accounting",
                effective_at=datetime.fromtimestamp(time, UTC),
                market_changes=tuple(updates),
                liquidations=tuple(liquidations),
                unavailable=tuple(dict.fromkeys(unavailable)),
                affected_markets=tuple(affected),
                simulation=SimulationEvidence(
                    execution_id=f"{state.run_id}:event:{time}",
                    model=MODEL,
                    inputs=tuple(sorted(evidence)),
                ),
            ),
        )

    def deliver_event(self, state, result):
        if result.kind == "accounting":
            return state.memory
        request = EventRequest(
            strategy=self.strategy,
            context=EventInput(config=self.config, memory=state.memory, event=result),
        )
        return Memory.model_validate_json(strategy_worker(request))
