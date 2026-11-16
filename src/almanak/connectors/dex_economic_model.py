"""Exchange economic orchestration uses the shared durable simulator and author sandbox."""

from collections.abc import Callable
from datetime import UTC, datetime
from types import ModuleType
from typing import ClassVar

from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.engine import Transition
from almanak.backtest.types import UnixTime
from almanak.connectors.dex_economic_parameters import payer_wallet, wallet
from almanak.connectors.v3_pool_lifecycle import increases_exposure
from almanak.dex_intents import ExitExchange
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount, Model
from almanak.runtime import strategy_capabilities, strategy_worker
from almanak.strategy import EconomicDecisionRequest, EconomicOutcomeRequest
from almanak.strategy_api import Memory, SimulationEvidence
from almanak.work import ChainAvailability, ChainFault


def remaining(owned):
    return owned.position is not None or any(
        a.amount
        for a in (owned.inventory0, owned.inventory1)
        if a.asset != owned.target.funding_currency
    )


def validate_model_variants(connector, config, model):
    from almanak.connectors.support import RunMode

    for allocation in config.allocations:
        connector.require_target(
            allocation.chain.chain_id,
            config.target(allocation.name),
            RunMode.BACKTEST,
            model=model,
        )


class EconomicModel:
    remaining = staticmethod(remaining)
    increases_exposure = staticmethod(increases_exposure)
    admit: ClassVar[Callable]
    validate: ClassVar[Callable]
    # Exit obligations are clear at an observation, including measured residual dust.
    exit_complete: ClassVar[Callable]
    quotes_for: ClassVar[Callable]
    allowances: ClassVar[Callable]
    delta_field: ClassVar[str]
    data: ModuleType
    state_types: ModuleType
    parameters: ModuleType
    simulation: ModuleType
    authoring: ModuleType
    decision_type: type[Model]
    revoke_type: type
    effects_type: type
    validate_capabilities: Callable

    def exit_leg(self, config, observed):
        allocation = config.allocation(observed.allocation)
        if observed.position is not None:
            return self.authoring.close_liquidity(allocation, observed)
        if observed.exit_quote is not None:
            return self.authoring.swap(allocation, observed, observed.exit_quote)
        return self.revoke_type(target=observed.target)

    @property
    def model_name(self):
        return self.data.MODEL

    def __init__(self, config, settings, strategy, book, *, authority=None):
        from almanak.connectors.composition import connector_for
        from almanak.connectors.support import historical_capability

        self.config, self.settings, self.strategy, self.book = config, settings, strategy, book
        connector = connector_for(config)
        self.capabilities = tuple(
            historical_capability(
                connector.support_for(chain.chain_id), chain, self.data.CONNECTOR, self.model_name
            )
            for chain in dict.fromkeys(a.chain for a in config.allocations)
        )
        self.authority = strategy_capabilities(strategy, config) if authority is None else authority
        validate_model_variants(connector, config, self.model_name)
        self.validate_capabilities(config, self.authority)

    def route(self, intent):
        if intent.target != self.config.target(intent.target.allocation):
            raise ValueError("economic intent has another configured exchange target")
        self.admit(self.authority, intent)
        return next(
            c for c in self.capabilities if c.chain == intent.target.fund.chain
        ), intent.kind

    def eligible_at(self, intent, time, latency):
        return UnixTime(time + max(1, latency))

    def cancel_at_end(self, intent):
        return self.increases_exposure(intent)

    def validate_state(self, state):
        if (
            state.schedule != self.settings.schedule
            or state.interval.effective != self.config.decision_trigger
        ):
            raise ValueError("economic exchange state differs from frozen scheduling")
        if tuple(p.payload.target for p in state.portfolio.positions) != tuple(
            self.config.target(a.name) for a in self.config.allocations
        ):
            raise ValueError("economic exchange ownership differs from configured targets")
        for held in state.portfolio.positions:
            if held.key != self.parameters.position_key(held.payload.target):
                raise ValueError("economic exchange position has another retained identity")

    def snapshot(self, state, payload, time, *, quotes=True, entry=True):
        """`entry=False`, or any mode but running, omits the entry quote no entry can use."""
        target = payload.target
        record = self.book.at(target, time)
        allocation = self.config.allocation(target.allocation)
        return self.state_types.EconomicSnapshot(
            target=target,
            pool=record.payload.pool,
            owned=payload.owned,
            **self.snapshot_assets(state, payload, record),
            native_balance=AssetAmount(
                asset=allocation.native_asset,
                amount=state.portfolio.balance(
                    payer_wallet(self.settings, target, allocation.native_asset)
                ),
            ),
            payer=self.config.accounts.binding(target.account).profile.submitter,
            allowances=self.allowances(target),
            position=payload.owned.position,
            simulation_time=UnixTime(time),
            quotes_included=quotes,
            **(
                self.snapshot_quotes(record, payload, time, entry=entry and state.mode == "running")
                if quotes
                else {}
            ),
        )

    def snapshot_quotes(self, record, payload, time, *, entry=True):
        return self.quotes_for(
            self.config, payload.owned, self.snapshot_quote(record, payload, time), entry=entry
        )

    def snapshot_quote(self, record, payload, time):
        return lambda amount: self.simulation.quote_at(record, payload.target, amount, time)

    def snapshot_assets(self, state, payload, record):
        from almanak.configuration.metadata import verify

        verify(self.config, record.payload.pool.currency0)
        verify(self.config, record.payload.pool.currency1)
        target = payload.target
        return dict(
            wallet0=AssetAmount(
                asset=target.assets[0],
                amount=state.portfolio.balance(wallet(target, target.assets[0])),
            ),
            wallet1=AssetAmount(
                asset=target.assets[1],
                amount=state.portfolio.balance(wallet(target, target.assets[1])),
            ),
        )

    def observe(self, state):
        self.validate_state(state)
        observed = []
        for held in state.portfolio.positions:
            try:
                observed.append(self.snapshot(state, held.payload, state.time))
            except InputUnavailable:
                continue
        return tuple(observed)

    def availability(self, state, observations):
        observed = {o.target for o in observations}
        result = []
        for chain in dict.fromkeys(p.payload.target.fund.chain for p in state.portfolio.positions):
            missing = tuple(
                p.payload.target.allocation
                for p in state.portfolio.positions
                if p.payload.target.fund.chain == chain and p.payload.target not in observed
            )
            result.append(
                ChainAvailability(
                    chain=chain,
                    status="unavailable" if missing else "ready",
                    fault=ChainFault(
                        kind="prepared_input_unavailable",
                        message="Missing prepared pools: " + ", ".join(missing),
                    )
                    if missing
                    else None,
                )
            )
        return tuple(result)

    def decide(self, state, observations, trigger):
        context = self.state_types.DecisionInput(
            config=self.config,
            memory=state.memory,
            trigger=trigger,
            owned=tuple(p.payload.owned for p in state.portfolio.positions),
            observations=observations,
            availability=self.availability(state, observations),
            prices=self.book.prices.at(state.time) if hasattr(self.book, "prices") else (),
        )
        return self.decision_type.model_validate_json(
            strategy_worker(EconomicDecisionRequest(strategy=self.strategy, context=context))
        )

    def execute(self, state, pending):
        self.validate_state(state)
        self.route(pending.intent)
        try:
            return self.apply(state, pending)
        except (ValueError, DataError) as error:
            return Transition(
                effects=self.effects_type(),
                result=self.state_types.EconomicFailure(
                    intent=pending.intent,
                    effective_at=datetime.fromtimestamp(pending.eligible_at, UTC),
                    simulation=SimulationEvidence(
                        execution_id=pending.identity, model=self.model_name, inputs=()
                    ),
                    error=str(error),
                    unavailable=error.gap if isinstance(error, InputUnavailable) else None,
                ),
            )

    def apply(self, state, pending):
        target = pending.intent.target
        held = next(
            p for p in state.portfolio.positions if p.key == self.parameters.position_key(target)
        )
        is_exit = isinstance(pending.intent, ExitExchange)
        # The exit observation plans the exit leg only, so it measures no entry quote.
        observed = self.snapshot(
            state, held.payload, pending.eligible_at, quotes=is_exit, entry=False
        )
        decision = self.snapshot(state, held.payload, pending.decision_time)
        self.validate(self.config, state, pending, decision, observed)
        if is_exit and self.exit_complete(observed):
            return self.settled_exit(held, pending, observed)
        actual = self.exit_leg(self.config, observed) if is_exit else pending.intent
        self.admit(self.authority, actual)
        record = self.book.at(target, pending.eligible_at)
        payload, deltas, fees, delta, kind, fill = self.simulate(
            actual, held.payload, record, pending
        )
        cost = 0 if self.settings.costs.kind == "excluded" else getattr(self.settings.costs, kind)
        allocation = self.config.allocation(target.allocation)
        if (
            state.portfolio.balance(payer_wallet(self.settings, target, allocation.native_asset))
            < cost
        ):
            raise ValueError("economic exchange payer cannot cover configured operation cost")
        complete = not is_exit or not self.remaining(payload.owned)
        result = self.state_types.EconomicResult(
            intent=pending.intent,
            complete=complete,
            effective_at=datetime.fromtimestamp(pending.eligible_at, UTC),
            simulation=self.evidence(target, pending),
            before=observed,
            after=payload.owned,
            **self.result_measurements(target, deltas, fees, delta),
            steps=(kind,),
            fill=fill,
            gas_paid=AssetAmount(asset=allocation.native_asset, amount=cost),
        )
        return Transition(
            effects=self.parameters.transition_effects(
                self.settings, held, payload, deltas, allocation.native_asset, cost
            ),
            result=result,
            complete=complete,
        )

    def evidence(self, target, pending):
        inputs = self.book.at(target, pending.eligible_at).evidence
        inputs += self.book.at(target, pending.decision_time).evidence
        return SimulationEvidence(
            execution_id=pending.identity, model=self.model_name, inputs=tuple(sorted(set(inputs)))
        )

    def settled_exit(self, held, pending, observed):
        """Only measured residual dust remains, so the exit completes without a step.

        A leg that leaves counter inventory completes at the next exit observation, whose
        exit quote already measures it; no quote is added after the leg.
        """
        target = pending.intent.target
        zero = tuple(0 for _ in target.assets)
        native = self.config.allocation(target.allocation).native_asset
        result = self.state_types.EconomicResult(
            intent=pending.intent,
            complete=True,
            effective_at=datetime.fromtimestamp(pending.eligible_at, UTC),
            simulation=self.evidence(target, pending),
            before=observed,
            after=held.payload.owned,
            **self.result_measurements(target, zero, zero, 0),
            steps=(),
            fill=None,
            gas_paid=AssetAmount(asset=native, amount=0),
        )
        return Transition(effects=self.effects_type(), result=result, complete=True)

    def simulate(self, intent, payload, record, pending):
        return self.simulation.simulate(
            self.config, intent, payload, record, pending.eligible_at, pending.identity
        )

    def result_measurements(self, target, deltas, fees, delta):
        return dict(
            delta0=deltas[0],
            delta1=deltas[1],
            **{self.delta_field: delta},
            fees0=fees[0],
            fees1=fees[1],
        )

    def deliver(self, state, result):
        return Memory.model_validate_json(
            strategy_worker(
                EconomicOutcomeRequest(
                    strategy=self.strategy,
                    context=self.state_types.OutcomeInput(
                        config=self.config, memory=state.memory, result=result
                    ),
                )
            )
        )

    def closed(self, state, observations):
        """Every retained position is observed with its own inventory and no obligation."""
        observed = {o.target: o for o in observations}
        return all(
            p.payload.target in observed
            and observed[p.payload.target].owned == p.payload.owned
            and self.exit_complete(observed[p.payload.target])
            for p in state.portfolio.positions
        )
