"""Input changes and earliest whole-second risk breaches between strategy calls."""

from datetime import UTC, datetime

from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.economic_availability import blocked, scope
from almanak.backtest.economic_data import BPS, RAY
from almanak.backtest.economic_liquidation import settle_liquidations
from almanak.backtest.economic_math import risk_view
from almanak.backtest.economic_model import EconomicModel, position_effects
from almanak.backtest.economic_state import (
    EconomicEvent,
    EconomicEventInput,
    EconomicPosition,
    UnavailableGroup,
)
from almanak.backtest.engine import Transition
from almanak.backtest.types import UnixTime
from almanak.effects import Effects
from almanak.models import AssetAmount
from almanak.runtime import strategy_worker
from almanak.strategy import EventRequest
from almanak.strategy_api import Memory


def first_breach(positions, market_at, start, end):
    """Prune only with a conservative bound, including integer rounding reversals.

    Within a fixed input segment, collateral claims and debt indices are monotonic.
    The initial weighted collateral over final debt bounds every intermediate HF.
    """
    if start > end or not any(p.drawn_shares for p in positions):
        return None
    initial = risk_view(positions, market_at(start))
    if initial.active_collateral_count == 0:
        return None
    final = risk_view(positions, market_at(end))
    if initial.weighted_collateral_value * RAY >= final.debt_value_ray * BPS:
        return None
    if initial.weighted_collateral_value * RAY < initial.debt_value_ray * BPS:
        return start
    if start == end:
        return None
    middle = (start + end) // 2
    left = first_breach(positions, market_at, start, middle)
    return left if left is not None else first_breach(positions, market_at, middle + 1, end)


class EconomicDriver(EconomicModel):
    def next_event(self, state, until):
        start = max(
            state.time, (state.event_cursor + 1) if state.event_cursor is not None else state.time
        )
        if start > until:
            return None
        # The current timestamp can contain newly visible input even after a quiet
        # time jump. It is consumed once; hypothetical action effects stay separate.
        if state.event_cursor is None:
            return UnixTime(start)
        boundary = self.book.next(state.event_cursor)
        limit = min(until, boundary - 1) if boundary is not None else until
        earliest = []
        targets = {scope(p.payload.target): p.payload.target for p in state.portfolio.positions}
        for target in targets.values():
            breach = self.group_event(state, target, start, limit)
            if breach is not None:
                earliest.append(breach)
        if boundary is not None and start <= boundary <= until:
            earliest.append(boundary)
        return UnixTime(min(earliest)) if earliest else None

    def group_event(self, state, target, start, limit):
        if blocked(state, target) is not None:
            return None
        try:
            group, _ = self.group(state, target, start)
            positions = tuple(p.payload for p in group)
            return first_breach(
                positions,
                lambda time: tuple(self.book.at(p.target.reserve, time) for p in positions),
                start,
                limit,
            )
        except InputUnavailable:
            return UnixTime(start)

    def settle_group(self, state, target, time):
        group = tuple(
            p for p in state.portfolio.positions if scope(p.payload.target) == scope(target)
        )
        original = tuple(p.payload for p in group)
        if blocked(state, target) is not None:
            return group, original, (), (), ()
        try:
            _, markets = self.group(state, target, time)
            updated, movements = settle_liquidations(original, markets)
            return group, updated, movements, (), group
        except InputUnavailable as error:
            gap = UnavailableGroup(targets=tuple(p.target for p in original), input=error.gap)
            return group, original, (), (gap,), ()

    def external_event(self, state, time):
        positions = {p.payload.target: p.payload for p in state.portfolio.positions}
        movements = []
        deficits = []
        unavailable = []
        evidenced = []
        targets = {scope(p.payload.target): p.payload.target for p in state.portfolio.positions}
        for target in targets.values():
            group, updated, effects, gaps, ready = self.settle_group(state, target, time)
            movements.extend(effects)
            unavailable.extend(gaps)
            evidenced.extend(ready)
            for old, new in zip(group, updated, strict=True):
                positions[new.target] = new
                if new.deficit > old.payload.deficit:
                    deficits.append(
                        AssetAmount(
                            asset=new.target.asset, amount=new.deficit - old.payload.deficit
                        )
                    )
        if not movements and not deficits and not unavailable:
            return None
        after = tuple(positions[p.payload.target] for p in state.portfolio.positions)
        effects = Effects[EconomicPosition](
            positions=position_effects(state.portfolio.positions, after)
        )
        return Transition(
            effects=effects,
            result=EconomicEvent(
                kind="liquidation" if movements or deficits else "input_unavailable",
                effective_at=datetime.fromtimestamp(time, UTC),
                liquidations=tuple(movements),
                deficits=tuple(deficits),
                unavailable=tuple(unavailable),
                simulation=self.evidence(
                    evidenced, time, f"{state.run_id}:event:{len(state.events)}"
                ),
            ),
        )

    def deliver_event(self, state, result):
        recorded = state.events[state.events_delivered]
        request = EventRequest(
            strategy=self.strategy,
            context=EconomicEventInput(
                config=self.config,
                memory=state.memory,
                event=result,
                effects=recorded.transition.effects,
            ),
        )
        return Memory.model_validate_json(strategy_worker(request))
