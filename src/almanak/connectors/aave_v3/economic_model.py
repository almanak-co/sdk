"""V3 economic actions through the shared simulator and sandboxed author hooks."""

from datetime import UTC, datetime

from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.engine import Transition
from almanak.backtest.types import UnixTime
from almanak.connectors.aave_v3.authority import admit
from almanak.connectors.aave_v3.authority import validate as validate_capabilities
from almanak.connectors.aave_v3.economic_actions import apply
from almanak.connectors.aave_v3.economic_data import CONNECTOR, MODEL
from almanak.connectors.aave_v3.economic_math import risk, validate_sources
from almanak.connectors.aave_v3.economic_parameters import payer_wallet, position_key, wallet
from almanak.connectors.aave_v3.economic_state import (
    ActionResult,
    DecisionInput,
    OutcomeInput,
    Position,
    Snapshot,
)
from almanak.connectors.aave_v3.economic_views import bitmap, position_view
from almanak.connectors.aave_v3.lifecycle import AaveLifecycle
from almanak.connectors.aave_v3.observations import Ownership
from almanak.connectors.aave_v3.planning import economic_kind
from almanak.connectors.aave_v3.state import Decision
from almanak.effects import AssetDelta, Effects, PositionChange
from almanak.effects import Position as HeldPosition
from almanak.models import AssetAmount
from almanak.runtime import strategy_capabilities, strategy_worker
from almanak.strategy import EconomicDecisionRequest, EconomicOutcomeRequest
from almanak.strategy_api import Memory, SimulationEvidence
from almanak.work import ChainAvailability, ChainFault


def scope(target):
    return target.fund, target.reserve.pool


def gaps(state):
    return tuple(gap for event in state.events for gap in event.transition.result.unavailable)


def blocked(state, target):
    return next(
        (
            gap
            for gap in gaps(state)
            if gap.key.chain == target.fund.chain
            and gap.key.source.startswith(target.reserve.pool + "/")
        ),
        None,
    )


def changes(before, positions):
    by_target = {p.target: p for p in positions}
    return tuple(
        PositionChange[Position](
            before=item,
            after=HeldPosition[Position](key=item.key, payload=by_target[item.payload.target]),
        )
        for item in before
        if item.payload.target in by_target and item.payload != by_target[item.payload.target]
    )


class EconomicModel:
    def __init__(self, config, settings, strategy, book, *, authority=None):
        from almanak.connectors.composition import connector_for
        from almanak.connectors.support import historical_capability

        self.config, self.settings, self.strategy, self.book = config, settings, strategy, book
        connector = connector_for(config)
        self.capabilities = tuple(
            historical_capability(connector.support_for(chain.chain_id), chain, CONNECTOR, MODEL)
            for chain in dict.fromkeys(a.chain for a in config.positions)
        )
        self.authority = strategy_capabilities(strategy, config) if authority is None else authority
        validate_capabilities(config, self.authority)

    def route(self, intent):
        if self.config.target(intent.target.allocation) != intent.target:
            raise ValueError("economic intent differs from its configured fund and reserve")
        admit(self.authority, intent)
        return next(
            c for c in self.capabilities if c.chain == intent.target.fund.chain
        ), intent.kind

    def eligible_at(self, intent, time, latency):
        return UnixTime(time + max(1, latency))

    def cancel_at_end(self, intent):
        return AaveLifecycle.increases_exposure(intent)

    def group(self, state, target, time):
        if (gap := blocked(state, target)) is not None:
            raise InputUnavailable(gap.key, gap.time, gap.reason)
        held = tuple(
            p for p in state.portfolio.positions if scope(p.payload.target) == scope(target)
        )
        return held, tuple(self.book.at(p.payload.target.reserve, time) for p in held)

    def observe(self, state):
        self.validate_state(state)
        observed = []
        for item in state.portfolio.positions:
            try:
                observed.append(self.snapshot(state, item.payload))
            except InputUnavailable:
                continue
        return tuple(observed)

    def validate_state(self, state):
        if (
            state.schedule != self.settings.schedule
            or state.interval.effective != self.config.decision_trigger
        ):
            raise ValueError("economic state differs from its frozen scheduling")
        expected = tuple(self.config.target(a.name) for a in self.config.positions)
        if tuple(p.payload.target for p in state.portfolio.positions) != expected:
            raise ValueError("economic ownership differs from its configured targets")
        if any(p.key != position_key(p.payload.target) for p in state.portfolio.positions):
            raise ValueError("economic position key differs from its fund and reserve")

    def snapshot(self, state, position):
        held, markets = self.group(state, position.target, state.time)
        positions = tuple(p.payload for p in held)
        validate_sources(self.config, positions, markets)
        account_risk = risk(positions, markets).risk
        market = next(m for m in markets if m.reserve.source == position.target.reserve)
        allocation = self.config.allocation(position.target.allocation)
        payer = self.config.accounts.binding(position.target.account).profile.submitter
        native = payer_wallet(self.settings, position.target, allocation.native_asset)
        return Snapshot(
            target=position.target,
            position=position_view(position, market, bitmap(positions, markets), account_risk),
            wallet_balance=state.portfolio.balance(wallet(position.target)),
            native_balance=AssetAmount(
                asset=allocation.native_asset, amount=state.portfolio.balance(native)
            ),
            payer=payer,
            effective_at=datetime.fromtimestamp(state.time, UTC),
            simulation=self.evidence(
                positions, state.time, f"{state.run_id}:observation:{state.time}"
            ),
        )

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
                        message="Missing prepared V3 history: " + ", ".join(missing),
                    )
                    if missing
                    else None,
                )
            )
        return tuple(result)

    def decide(self, state, observations, trigger):
        context = DecisionInput(
            config=self.config,
            memory=state.memory,
            observations=observations,
            owned=tuple(
                Ownership(
                    target=p.payload.target,
                    supply_scaled=p.payload.supply_scaled,
                    debt_scaled=p.payload.debt_scaled,
                    collateral_enabled=p.payload.collateral_enabled,
                )
                for p in state.portfolio.positions
            ),
            availability=self.availability(state, observations),
            trigger=trigger,
            prices=self.book.prices.at(state.time),
        )
        request = EconomicDecisionRequest(strategy=self.strategy, context=context)
        return Decision.model_validate_json(strategy_worker(request))

    def execute(self, state, pending):
        try:
            return self.apply(state, pending)
        except ValueError as error:
            return Transition(
                effects=Effects[Position](),
                result=ActionResult(
                    intent=pending.intent,
                    success=False,
                    assets=None,
                    supply_delta=0,
                    debt_delta=0,
                    effective_at=datetime.fromtimestamp(pending.eligible_at, UTC),
                    simulation=SimulationEvidence(
                        execution_id=pending.identity, model=MODEL, inputs=()
                    ),
                    error=str(error),
                    unavailable=error.gap if isinstance(error, InputUnavailable) else None,
                ),
            )

    def apply(self, state, pending):
        intent = pending.intent
        self.route(intent)
        self.validate_entry(state, intent)
        held, markets = self.group(state, intent.target, pending.eligible_at)
        positions = tuple(p.payload for p in held)
        result = apply(
            self.config,
            intent,
            positions,
            markets,
            state.portfolio.balance(wallet(intent.target)),
            tuple(p.payload for p in state.portfolio.positions),
        )
        effects = Effects[Position](
            assets=(
                AssetDelta(key=wallet(intent.target), amount=result.inventory_delta),
                *self.costs(state, intent),
            ),
            positions=changes(held, result.positions),
        )
        return Transition(
            effects=effects,
            result=ActionResult(
                intent=intent,
                success=True,
                assets=AssetAmount(asset=intent.target.asset, amount=result.assets)
                if result.assets
                else None,
                supply_delta=result.supply_delta,
                debt_delta=result.debt_delta,
                effective_at=datetime.fromtimestamp(pending.eligible_at, UTC),
                simulation=self.evidence(positions, pending.eligible_at, pending.identity),
            ),
        )

    def validate_entry(self, state, intent):
        if not self.cancel_at_end(intent):
            return
        failed = any(
            not step.transition.result.success
            and scope(step.pending.intent.target) == scope(intent.target)
            for step in state.completed
        )
        if state.mode != "running" or failed:
            raise ValueError("new exposure requires running mode and successful prior Pool effects")
        allocation = self.config.allocation(intent.target.allocation)
        native = payer_wallet(self.settings, intent.target, allocation.native_asset)
        if state.portfolio.balance(native) < allocation.minimum_native_balance:
            raise ValueError("economic gas payer is below the configured native minimum")

    def costs(self, state, intent):
        if self.settings.costs.kind == "excluded":
            return ()
        amount = getattr(self.settings.costs, economic_kind(intent))
        native = self.config.allocation(intent.target.allocation).native_asset
        key = payer_wallet(self.settings, intent.target, native)
        if state.portfolio.balance(key) < amount:
            raise ValueError("insufficient explicit economic payer inventory")
        return (AssetDelta(key=key, amount=-amount),)

    def evidence(self, positions, time, identity):
        inputs = {
            digest for p in positions for digest in self.book.evidence(p.target.reserve, time)
        }
        return SimulationEvidence(execution_id=identity, model=MODEL, inputs=tuple(sorted(inputs)))

    def deliver(self, state, result):
        request = EconomicOutcomeRequest(
            strategy=self.strategy,
            context=OutcomeInput(config=self.config, memory=state.memory, result=result),
        )
        return Memory.model_validate_json(strategy_worker(request))

    def closed(self, state, observations):
        return (
            not gaps(state)
            and len(observations) == len(state.portfolio.positions)
            and all(
                not (
                    p.payload.supply_scaled or p.payload.debt_scaled or p.payload.collateral_enabled
                )
                for p in state.portfolio.positions
            )
        )
