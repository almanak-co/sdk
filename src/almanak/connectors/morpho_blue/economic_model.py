"""Morpho economic intents through the shared simulator and isolated author hooks."""

from datetime import UTC, datetime

from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.engine import Transition
from almanak.backtest.types import UnixTime
from almanak.connectors.morpho_blue.authority import admit
from almanak.connectors.morpho_blue.authority import validate as validate_capabilities
from almanak.connectors.morpho_blue.economic_actions import apply
from almanak.connectors.morpho_blue.economic_data import CONNECTOR, MODEL
from almanak.connectors.morpho_blue.economic_journal import require_anchor, retained
from almanak.connectors.morpho_blue.economic_market import project
from almanak.connectors.morpho_blue.economic_parameters import payer_wallet, position_key, wallet
from almanak.connectors.morpho_blue.economic_positions import account_view
from almanak.connectors.morpho_blue.economic_state import (
    ActionResult,
    DecisionInput,
    OutcomeInput,
    Position,
    Snapshot,
)
from almanak.connectors.morpho_blue.lifecycle import MorphoLifecycle
from almanak.connectors.morpho_blue.observations import Ownership
from almanak.connectors.morpho_blue.planning import economic_kind
from almanak.connectors.morpho_blue.state import Decision
from almanak.effects import AssetDelta, Effects, PositionChange
from almanak.effects import Position as HeldPosition
from almanak.models import AssetAmount
from almanak.runtime import strategy_capabilities, strategy_worker
from almanak.strategy import EconomicDecisionRequest, EconomicOutcomeRequest
from almanak.strategy_api import Memory, SimulationEvidence
from almanak.work import ChainAvailability, ChainFault


def scope(target):
    return target.fund, target.reserve.market


def gaps(state):
    return tuple(gap for event in state.events for gap in event.transition.result.unavailable)


def blocked(state, target):
    return next(
        (
            affected.gap
            for event in state.events
            for affected in event.transition.result.affected_markets
            if affected.source == target.reserve.market and affected.fund in (None, target.fund)
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
            raise ValueError("economic Morpho intent differs from its configured fund and market")
        admit(self.authority, intent)
        return next(
            c for c in self.capabilities if c.chain == intent.target.fund.chain
        ), intent.kind

    def eligible_at(self, intent, time, latency):
        return UnixTime(time + max(1, latency))

    def cancel_at_end(self, intent):
        return MorphoLifecycle.increases_exposure(intent)

    def group(self, state, target, time):
        if (gap := blocked(state, target)) is not None:
            raise InputUnavailable(gap.key, gap.time, gap.reason)
        held = tuple(
            p for p in state.portfolio.positions if scope(p.payload.target) == scope(target)
        )
        ledger = retained(self.book, state, target.reserve.market)
        require_anchor(self.book, ledger, time)
        return held, self.book.at(target.reserve.market, time), ledger

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
            raise ValueError("economic Morpho state differs from its frozen schedule")
        expected = tuple(self.config.target(a.name) for a in self.config.positions)
        if tuple(p.payload.target for p in state.portfolio.positions) != expected:
            raise ValueError("economic Morpho ownership differs from its configured targets")
        if any(p.key != position_key(p.payload.target) for p in state.portfolio.positions):
            raise ValueError("economic Morpho position key differs from its fund and market")

    def snapshot(self, state, position):
        held, market, ledger = self.group(state, position.target, state.time)
        positions = tuple(p.payload for p in held)
        account = account_view(positions, market, project(ledger, state.time), position.target.fund)
        allocation = self.config.allocation(position.target.allocation)
        payer = self.config.accounts.binding(position.target.account).profile.submitter
        native = payer_wallet(self.settings, position.target, allocation.native_asset)
        return Snapshot(
            target=position.target,
            account=account,
            wallet_balance=state.portfolio.balance(wallet(position.target)),
            native_balance=AssetAmount(
                asset=allocation.native_asset, amount=state.portfolio.balance(native)
            ),
            payer=payer,
            effective_at=datetime.fromtimestamp(state.time, UTC),
            simulation=self.evidence(
                position.target, state.time, f"{state.run_id}:observation:{state.time}"
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
                        message="Missing prepared Morpho history: " + ", ".join(missing),
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
                    supply_shares=p.payload.supply_shares,
                    borrow_shares=p.payload.borrow_shares,
                    collateral=p.payload.collateral,
                )
                for p in state.portfolio.positions
            ),
            availability=self.availability(state, observations),
            trigger=trigger,
            prices=self.book.prices.at(state.time),
        )
        return Decision.model_validate_json(
            strategy_worker(EconomicDecisionRequest(strategy=self.strategy, context=context))
        )

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
                    market_change=None,
                    effective_at=datetime.fromtimestamp(pending.eligible_at, UTC),
                    simulation=SimulationEvidence(
                        execution_id=pending.identity, model=MODEL, inputs=()
                    ),
                    error=str(error),
                    unavailable=error.gap if isinstance(error, InputUnavailable) else None,
                ),
            )

    def validate_market_positions(self, state, result, market, time):
        from almanak.connectors.morpho_blue.economic_positions import validate_positions

        updated = {p.target: p for p in result.positions}
        positions = tuple(
            updated.get(p.payload.target, p.payload)
            for p in state.portfolio.positions
            if p.payload.target.reserve.market == market.source
        )
        validate_positions(positions, market, result.market.after, time)

    def apply(self, state, pending):
        intent = pending.intent
        self.route(intent)
        self.validate_entry(state, intent)
        held, market, ledger = self.group(state, intent.target, pending.eligible_at)
        positions = tuple(p.payload for p in held)
        result = apply(
            self.config,
            intent,
            positions,
            market,
            ledger,
            state.portfolio.balance(wallet(intent.target)),
            time=pending.eligible_at,
        )
        self.validate_market_positions(state, result, market, pending.eligible_at)
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
                assets=AssetAmount(asset=intent.target.asset, amount=result.assets),
                market_change=result.market,
                effective_at=datetime.fromtimestamp(pending.eligible_at, UTC),
                simulation=self.evidence(intent.target, pending.eligible_at, pending.identity),
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
            raise ValueError(
                "new exposure requires running mode and successful prior Morpho effects"
            )
        allocation = self.config.allocation(intent.target.allocation)
        native = payer_wallet(self.settings, intent.target, allocation.native_asset)
        if state.portfolio.balance(native) < allocation.minimum_native_balance:
            raise ValueError("economic Morpho payer is below the configured native minimum")

    def costs(self, state, intent):
        if self.settings.costs.kind == "excluded":
            return ()
        amount = getattr(self.settings.costs, economic_kind(intent))
        native = self.config.allocation(intent.target.allocation).native_asset
        key = payer_wallet(self.settings, intent.target, native)
        if state.portfolio.balance(key) < amount:
            raise ValueError("insufficient explicit economic Morpho payer inventory")
        return (AssetDelta(key=key, amount=-amount),)

    def evidence(self, target, time, identity):
        return SimulationEvidence(
            execution_id=identity,
            model=MODEL,
            inputs=self.book.evidence(target.reserve.market, time),
        )

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
            and all(p.payload.empty for p in state.portfolio.positions)
        )
