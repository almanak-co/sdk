"""Native economic calculations share one portfolio, schedule and sandboxed strategy."""

from almanak.backtest.economic_state import EconomicPosition as AaveV4Position
from almanak.backtest.strategy_prices import strategy_bindings
from almanak.connectors.aave_v3.economic_events import EconomicDriver as LendingDriver
from almanak.connectors.aave_v3.economic_state import Position as LendingPosition
from almanak.connectors.aave_v3.observations import Ownership as LendingOwnership
from almanak.connectors.aave_v4.models import Ownership as AaveV4Ownership
from almanak.connectors.across.economic_model import EconomicDriver as AcrossDriver
from almanak.connectors.across.economic_state import Position as AcrossPosition
from almanak.connectors.aerodrome.economic_model import EconomicModel as AerodromeDriver
from almanak.connectors.aerodrome_slipstream.economic_model import (
    EconomicModel as SlipstreamDriver,
)
from almanak.connectors.composed.aave_v4_economic import Driver as AaveV4Driver
from almanak.connectors.composed.authority import selected
from almanak.connectors.composed.economic_inventory import shared_transition, validate_views
from almanak.connectors.composed.economic_state import (
    DecisionInput,
    EconomicOwnership,
    EventInput,
    OutcomeInput,
)
from almanak.connectors.composed.inventory import project_cash
from almanak.connectors.composed.state import Decision
from almanak.connectors.compound_v3.economic_events import EconomicDriver as CompoundDriver
from almanak.connectors.compound_v3.economic_state import Position as CompoundPosition
from almanak.connectors.compound_v3.observations import Ownership as CompoundOwnership
from almanak.connectors.curve.economic_model import EconomicModel as CurveDriver
from almanak.connectors.gmx_v2.economic_model import EconomicModel as GmxDriver
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.economic_events import EconomicDriver as MorphoDriver
from almanak.connectors.morpho_blue.economic_positions import Position as MorphoPosition
from almanak.connectors.morpho_blue.observations import Ownership as MorphoOwnership
from almanak.connectors.pancakeswap_v3.economic_model import EconomicModel as PancakeDriver
from almanak.connectors.sparklend.economic_events import EconomicDriver as SparkDriver
from almanak.connectors.sparklend.economic_state import Position as SparkPosition
from almanak.connectors.sparklend.observations import Ownership as SparkOwnership
from almanak.connectors.target_routing import target_connector
from almanak.connectors.uniswap_v3.economic_model import EconomicModel as ExchangeDriver
from almanak.connectors.uniswap_v4.economic_model import EconomicModel as V4Driver
from almanak.runtime import strategy_capabilities, strategy_worker
from almanak.strategy import EconomicDecisionRequest, EconomicOutcomeRequest, EventRequest
from almanak.strategy_api import Memory
from almanak.work import ChainAvailability, ChainFault


def component_state(state, identity):
    values = {
        "portfolio": state.portfolio.model_copy(
            update={
                "positions": tuple(
                    p for p in state.portfolio.positions if p.key.connector == identity
                )
            }
        ),
        "completed": tuple(
            component_completed(c, identity)
            for c in state.completed
            if target_connector(c.pending.intent.target) == identity
        ),
        "delivered": sum(
            target_connector(c.pending.intent.target) == identity
            for c in state.completed[: state.delivered]
        ),
        "pending": local_pending(state.pending, identity),
        "continuation": local_pending(state.continuation, identity),
        "events": component_events(state.events, identity),
        "events_delivered": len(component_events(state.events[: state.events_delivered], identity)),
    }
    from almanak.backtest.economic_state import EconomicState as AaveV4State
    from almanak.connectors.aave_v3.economic_state import State as LendingState
    from almanak.connectors.across.economic_state import State as AcrossState
    from almanak.connectors.aerodrome.economic_state import State as AerodromeState
    from almanak.connectors.aerodrome_slipstream.economic_state import State as SlipstreamState
    from almanak.connectors.compound_v3.economic_state import State as CompoundState
    from almanak.connectors.curve.economic_state import State as CurveState
    from almanak.connectors.gmx_v2.economic_state import State as GmxState
    from almanak.connectors.morpho_blue.economic_state import State as MorphoState
    from almanak.connectors.pancakeswap_v3.economic_state import State as PancakeState
    from almanak.connectors.sparklend.economic_state import State as SparkState
    from almanak.connectors.uniswap_v3.economic_state import State as ExchangeState
    from almanak.connectors.uniswap_v4.economic_state import State as V4State

    factories: dict[
        ConnectorId,
        type[LendingState]
        | type[AaveV4State]
        | type[SparkState]
        | type[ExchangeState]
        | type[PancakeState]
        | type[AerodromeState]
        | type[SlipstreamState]
        | type[V4State]
        | type[MorphoState]
        | type[CompoundState]
        | type[CurveState]
        | type[GmxState]
        | type[AcrossState],
    ] = {
        ConnectorId("aave_v3"): LendingState,
        ConnectorId("aave_v4"): AaveV4State,
        ConnectorId("sparklend"): SparkState,
        ConnectorId("uniswap_v3"): ExchangeState,
        ConnectorId("pancakeswap_v3"): PancakeState,
        ConnectorId("aerodrome"): AerodromeState,
        ConnectorId("aerodrome_slipstream"): SlipstreamState,
        ConnectorId("uniswap_v4"): V4State,
        ConnectorId("morpho_blue"): MorphoState,
        ConnectorId("compound_v3"): CompoundState,
        ConnectorId("curve"): CurveState,
        ConnectorId("gmx_v2"): GmxState,
        ConnectorId("across"): AcrossState,
    }
    return factories[identity].model_validate(
        {name: getattr(state, name) for name in type(state).model_fields} | values
    )


def component_completed(completed, identity):
    effects = completed.transition.effects
    positions = tuple(
        change
        for change in effects.positions
        if (change.after or change.before).key.connector == identity
    )
    transition = completed.transition.model_copy(
        update={"effects": effects.model_copy(update={"positions": positions})}
    )
    return completed.model_copy(update={"transition": transition})


def component_events(events, identity):
    return tuple(
        event.model_copy(update={"transition": component.transition})
        for event in events
        for component in event.transition.result.components
        if component.connector == identity
    )


# Event kinds that only move accounting state; the author hook sees them as observations.
# A bridge arrival is delivered to `on_outcome` instead of the event hook.
ACCOUNTING_EVENTS = frozenset({"accounting", "perpetual_liquidations", "bridge_arrivals"})


def arrivals(event):
    """The modeled bridge arrivals one composed event carries, in order."""
    return tuple(
        arrived
        for component in event.components
        if component.transition.result.kind == "bridge_arrivals"
        for arrived in component.transition.result.arrivals
    )


def local_pending(pending, identity):
    if pending is None or target_connector(pending.intent.target) != identity:
        return None
    return pending


def ownership(state):
    result: list[EconomicOwnership] = []
    for held in state.portfolio.positions:
        payload = held.payload
        if isinstance(payload, AaveV4Position):
            result.append(
                AaveV4Ownership(
                    target=payload.target,
                    shares=payload.shares,
                    drawn_shares=payload.drawn_shares,
                    premium_shares=payload.premium_shares,
                    premium_offset_ray=payload.premium_offset_ray,
                    collateral_enabled=payload.collateral,
                )
            )
        elif isinstance(payload, (LendingPosition, SparkPosition)):
            result.append(
                (
                    SparkOwnership if isinstance(payload, SparkPosition) else LendingOwnership
                ).model_validate(
                    {
                        "target": payload.target,
                        "supply_scaled": payload.supply_scaled,
                        "debt_scaled": payload.debt_scaled,
                        "collateral_enabled": payload.collateral_enabled,
                    }
                )
            )
        elif isinstance(payload, (MorphoPosition, CompoundPosition)):
            cls = MorphoOwnership if isinstance(payload, MorphoPosition) else CompoundOwnership
            result.append(cls(**{name: getattr(payload, name) for name in cls.model_fields}))
        else:
            result.append(payload.owned)
    return project_cash(tuple(result), state.portfolio.balances, modeled_transfers(state))


def modeled_transfers(state):
    """Every modeled bridge transfer of the run; the decision view keeps the open ones."""
    return tuple(
        flight.transfer
        for held in state.portfolio.positions
        if isinstance(held.payload, AcrossPosition)
        for flight in held.payload.flights
    )


def availability(state, observations):
    observed = {o.target for o in observations}
    result = []
    for chain in dict.fromkeys(p.payload.target.fund.chain for p in state.portfolio.positions):
        missing = tuple(
            p.payload.target.allocation
            for p in state.portfolio.positions
            if p.key.chain == chain and p.payload.target not in observed
        )
        result.append(
            ChainAvailability(
                chain=chain,
                status="unavailable" if missing else "ready",
                fault=ChainFault(
                    kind="prepared_input_unavailable",
                    message="Missing prepared inputs: " + ", ".join(missing),
                )
                if missing
                else None,
            )
        )
    return tuple(result)


class EconomicDriver:
    def __init__(self, config, settings, strategy, book):
        self.config, self.settings, self.strategy, self.book = config, settings, strategy, book
        if set(settings.models) != set(config.parameter_components):
            raise ValueError("economic model policies must match configured connectors exactly")
        self.authority = strategy_capabilities(strategy, config)
        factories: dict[
            ConnectorId,
            type[LendingDriver]
            | type[AaveV4Driver]
            | type[SparkDriver]
            | type[ExchangeDriver]
            | type[PancakeDriver]
            | type[AerodromeDriver]
            | type[SlipstreamDriver]
            | type[V4Driver]
            | type[MorphoDriver]
            | type[CompoundDriver]
            | type[CurveDriver]
            | type[GmxDriver]
            | type[AcrossDriver],
        ] = {
            ConnectorId("aave_v3"): LendingDriver,
            ConnectorId("aave_v4"): AaveV4Driver,
            ConnectorId("sparklend"): SparkDriver,
            ConnectorId("uniswap_v3"): ExchangeDriver,
            ConnectorId("pancakeswap_v3"): PancakeDriver,
            ConnectorId("aerodrome"): AerodromeDriver,
            ConnectorId("aerodrome_slipstream"): SlipstreamDriver,
            ConnectorId("uniswap_v4"): V4Driver,
            ConnectorId("morpho_blue"): MorphoDriver,
            ConnectorId("compound_v3"): CompoundDriver,
            ConnectorId("curve"): CurveDriver,
            ConnectorId("gmx_v2"): GmxDriver,
            ConnectorId("across"): AcrossDriver,
        }
        self.components: dict[
            ConnectorId,
            LendingDriver
            | AaveV4Driver
            | SparkDriver
            | V4Driver
            | ExchangeDriver
            | PancakeDriver
            | AerodromeDriver
            | SlipstreamDriver
            | MorphoDriver
            | CompoundDriver
            | CurveDriver
            | GmxDriver
            | AcrossDriver,
        ] = {
            identity: factories[identity](
                parameters,
                settings.component(identity, parameters),
                strategy,
                book.components[identity],
                authority=selected(self.authority, identity),
            )
            for identity, parameters in config.parameter_components.items()
        }

    def component(self, target):
        if self.config.target(target.allocation) != target:
            raise ValueError("economic action differs from its configured connector target")
        return self.components[target_connector(target)]

    def route(self, intent):
        return self.component(intent.target).route(intent)

    def eligible_at(self, intent, time, latency):
        return self.component(intent.target).eligible_at(intent, time, latency)

    def continuation_at(self, state, pending):
        from almanak.backtest.engine import ContinuationTiming

        driver = self.component(pending.intent.target)
        if isinstance(driver, ContinuationTiming):
            return driver.continuation_at(
                component_state(state, target_connector(pending.intent.target)), pending
            )
        return driver.eligible_at(pending.intent, state.time, state.schedule.latency_seconds)

    def cancel_at_end(self, intent):
        return self.component(intent.target).cancel_at_end(intent)

    def validate_state(self, state):
        validate_views(state.portfolio)
        expected = tuple(self.config.target(a.name) for a in self.config.positions)
        if tuple(p.payload.target for p in state.portfolio.positions) != expected:
            raise ValueError("economic ownership differs from configured composed targets")
        for identity, driver in self.components.items():
            driver.validate_state(component_state(state, identity))

    def observe(self, state):
        self.validate_state(state)
        observed = {
            item.target: item
            for identity, driver in self.components.items()
            for item in driver.observe(component_state(state, identity))
        }
        return tuple(
            observed[p.payload.target]
            for p in state.portfolio.positions
            if p.payload.target in observed
        )

    def decide(self, state, observations, trigger):
        context = DecisionInput(
            config=self.config,
            memory=state.memory,
            owned=ownership(state),
            observations=observations,
            availability=availability(state, observations),
            trigger=trigger,
            prices=self.book.prices.at(state.time) if strategy_bindings(self.settings) else (),
        )
        return Decision.model_validate_json(
            strategy_worker(EconomicDecisionRequest(strategy=self.strategy, context=context))
        )

    def execute(self, state, pending):
        self.validate_state(state)
        identity = target_connector(pending.intent.target)
        transition = self.component(pending.intent.target).execute(
            component_state(state, identity), pending
        )
        return shared_transition(state, transition)

    def deliver(self, state, result):
        context = OutcomeInput(config=self.config, memory=state.memory, result=result)
        return Memory.model_validate_json(
            strategy_worker(EconomicOutcomeRequest(strategy=self.strategy, context=context))
        )

    def next_event(self, state, until):
        from almanak.connectors.composed.economic_events import next_event

        return next_event(self, state, until)

    def external_event(self, state, time):
        from almanak.connectors.composed.economic_events import external_event

        return external_event(self, state, time)

    def deliver_event(self, state, result):
        # A modeled bridge arrival reaches `on_outcome`, as a measured one does live.
        memory = state.memory
        for arrived in arrivals(result):
            memory = self.deliver(state.model_copy(update={"memory": memory}), arrived)
        # Pure accounting events, including GMX liquidation batches, never reach the hook.
        if all(c.transition.result.kind in ACCOUNTING_EVENTS for c in result.components):
            return memory
        if memory is not state.memory:
            # An arrival was delivered above; the hook sees the rest of a mixed event.
            rest = tuple(
                c for c in result.components if c.transition.result.kind != "bridge_arrivals"
            )
            result = result.model_copy(update={"components": rest})
        context = EventInput(config=self.config, memory=memory, event=result)
        return Memory.model_validate_json(
            strategy_worker(EventRequest(strategy=self.strategy, context=context))
        )

    def closed(self, state, observations):
        self.validate_state(state)
        if len(observations) != len(state.portfolio.positions):
            return False
        for identity, driver in self.components.items():
            view = component_state(state, identity)
            if identity == ConnectorId("gmx_v2"):
                from almanak.connectors.gmx_v2.economic_component import protocol_closed

                if not all(protocol_closed(p.payload) for p in view.portfolio.positions):
                    return False
            elif identity in (
                ConnectorId("uniswap_v3"),
                ConnectorId("pancakeswap_v3"),
                ConnectorId("aerodrome"),
                ConnectorId("aerodrome_slipstream"),
                ConnectorId("uniswap_v4"),
                ConnectorId("curve"),
            ):
                if any(p.payload.owned.position is not None for p in view.portfolio.positions):
                    return False
            elif not driver.closed(
                view, tuple(o for o in observations if target_connector(o.target) == identity)
            ):
                return False
        return True
