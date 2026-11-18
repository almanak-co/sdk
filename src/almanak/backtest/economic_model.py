"""Aave economic driver consuming prepared inputs through the shared simulator."""

import json
from datetime import UTC, datetime
from uuid import uuid4

from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.economic_actions import apply_action
from almanak.backtest.economic_availability import (
    availability,
    require_history,
    scope,
    unavailable,
)
from almanak.backtest.economic_data import CONNECTOR, MODEL, require_range
from almanak.backtest.economic_math import debts, risk_view
from almanak.backtest.economic_state import (
    EconomicActionResult,
    EconomicDecision,
    EconomicDecisionInput,
    EconomicOutcomeInput,
    EconomicParameters,
    EconomicPosition,
    EconomicSnapshot,
    EconomicState,
)
from almanak.backtest.engine import Transition
from almanak.backtest.types import UnixTime
from almanak.connectors.aave_v4.backtest import position_key, wallet
from almanak.connectors.aave_v4.domain import PositionObservation
from almanak.connectors.aave_v4.models import LendingParameters
from almanak.effects import AssetDelta, Balance, Effects, Portfolio, Position, PositionChange
from almanak.lending import CollateralObservation, DebtAllocation, Ownership, SupplyTarget
from almanak.lending_intents import BorrowIntent, CollateralIntent, RepayIntent, SupplyIntent
from almanak.models import AccountBinding, AssetAmount, Eoa, StrategyAccounts
from almanak.runtime import strategy_worker
from almanak.strategy import EconomicDecisionRequest, EconomicOutcomeRequest
from almanak.strategy_api import Memory, SimulationEvidence


def parameters(settings, author, interval):
    if isinstance(author, LendingParameters):
        # Resolved parameters are authoritative; only the effective cadence changes.
        wire = author.model_dump(mode="json")
        wire["decision_trigger"] = interval.effective.model_dump(mode="json")
        return EconomicParameters.model_validate_json(json.dumps(wire))
    return EconomicParameters(
        decision_trigger=interval.effective,
        asset_metadata=author.asset_metadata,
        parameters=author.parameters,
        accounts=StrategyAccounts(
            bindings=tuple(
                AccountBinding(name=f.name, profile=Eoa(kind="eoa", fund=f.fund))
                for f in settings.funds
            )
        ),
        allocations=author.allocations,
        debts=author.debts,
    )


def initial_state(settings, config, interval):
    for position in config.positions:
        require_range(position.reserve, settings.schedule.start, settings.schedule.deadline)
    balances = {b.key: b for f in settings.funds for b in f.inventory()}
    for debt in config.debts:
        key = wallet(config.target(debt.name))
        balances.setdefault(key, Balance(key=key, amount=0))
    positions = tuple(
        Position[EconomicPosition](
            key=position_key(config.target(a.name)),
            payload=EconomicPosition(target=config.target(a.name)),
        )
        for a in config.positions
    )
    portfolio = Portfolio[EconomicPosition](balances=tuple(balances.values()), positions=positions)
    for allocation in config.allocations:
        target = config.target(allocation.name)
        required = sum(
            a.capital
            for a in config.allocations
            if a.account == allocation.account and a.reserve.asset == target.asset
        )
        if portfolio.balance(wallet(target)) < required:
            raise ValueError("initial inventory cannot fund the combined allocation capital")
        portfolio.balance(wallet(target, allocation.native_asset))
    return EconomicState(
        run_id=uuid4().hex,
        time=settings.schedule.start,
        schedule=settings.schedule,
        interval=interval,
        portfolio=portfolio,
    )


def position_effects(before, after):
    return tuple(
        PositionChange[EconomicPosition](
            before=old, after=Position[EconomicPosition](key=old.key, payload=new)
        )
        for old, new in zip(before, after, strict=True)
        if old.payload != new
    )


class EconomicModel:
    def __init__(self, config, settings, strategy, book):
        self.config, self.settings, self.strategy, self.book = config, settings, strategy, book
        from almanak.connectors.composition import connector_for
        from almanak.connectors.support import historical_capability

        connector = connector_for(config)
        self.capabilities = tuple(
            historical_capability(connector.support_for(chain.chain_id), chain, CONNECTOR, MODEL)
            for chain in dict.fromkeys(a.reserve.chain for a in config.positions)
        )

    def route(self, intent):
        if self.config.target(intent.target.allocation) != intent.target:
            raise ValueError("intent differs from its declared fund and reserve")
        return next(
            c for c in self.capabilities if c.chain == intent.target.fund.chain
        ), intent.kind

    def cancel_at_end(self, intent):
        return isinstance(intent, (SupplyIntent, BorrowIntent)) or (
            isinstance(intent, CollateralIntent) and intent.enabled
        )

    def eligible_at(self, intent, time, latency):
        return UnixTime(time + max(1, latency))

    def group(self, state, target, time):
        require_history(state, target)
        positions = tuple(
            p for p in state.portfolio.positions if scope(p.payload.target) == scope(target)
        )
        markets = tuple(self.book.at(p.payload.target.reserve, time) for p in positions)
        return positions, markets

    def observe(self, state):
        if (
            state.schedule != self.settings.schedule
            or state.interval.effective != self.config.decision_trigger
        ):
            raise ValueError("economic state differs from frozen scheduling")
        expected = tuple(self.config.target(a.name) for a in self.config.positions)
        if tuple(p.payload.target for p in state.portfolio.positions) != expected:
            raise ValueError("economic positions differ from their declared ownership")
        if any(p.key != position_key(p.payload.target) for p in state.portfolio.positions):
            raise ValueError("economic position key differs from its fund and reserve")
        observations = []
        for p in state.portfolio.positions:
            try:
                observations.append(self.snapshot(state, p.payload))
            except InputUnavailable:
                # Missing one reserve invalidates the joint fund/Spoke risk view.
                continue
        return tuple(observations)

    def snapshot(self, state, p):
        target = p.target
        group, markets = self.group(state, target, state.time)
        market = self.book.at(target.reserve, state.time)
        from almanak.configuration.metadata import verify

        verify(self.config, market.risk.reserve.asset)
        drawn, premium = debts(p, market.borrow_index)
        allocation = self.config.allocation(target.allocation)
        native = allocation.native_asset
        times, evidence = self.book.evidence(target.reserve, state.time)
        collateral: tuple[CollateralObservation, ...] = ()
        if isinstance(allocation, DebtAllocation):
            collateral = tuple(
                CollateralObservation(
                    target=q.payload.target,
                    enabled=q.payload.collateral,
                    supplied_shares=q.payload.shares,
                )
                for q in group
                if q.payload.target.allocation in allocation.collateral
            )
        return EconomicSnapshot(
            allocation=target.allocation,
            account=target.account,
            position=PositionObservation(
                reserve=market.risk.reserve,
                account=target.fund,
                supplied_assets=market.supplied(p.shares),
                supplied_shares=p.shares,
                drawn_debt=drawn,
                premium_debt=premium,
                total_debt=drawn + premium,
            ),
            wallet_balance=state.portfolio.balance(wallet(target)),
            native_balance=AssetAmount(
                asset=native, amount=state.portfolio.balance(wallet(target, native))
            ),
            effective_at=datetime.fromtimestamp(state.time, UTC),
            collateral_enabled=p.collateral,
            collateral=collateral,
            drawn_shares=p.drawn_shares,
            premium_shares=p.premium_shares,
            premium_offset_ray=p.premium_offset_ray,
            dynamic_config_key=p.dynamic_config_key,
            risk_premium_bps=p.risk_premium_bps,
            risk=risk_view(tuple(q.payload for q in group), markets),
            input_times=times,
            evidence=evidence,
        )

    def decide(self, state, observations, trigger):
        request = EconomicDecisionRequest(
            strategy=self.strategy,
            context=EconomicDecisionInput(
                config=self.config,
                memory=state.memory,
                prices=self.book.strategy_prices(state.time)
                if any(b.purpose == "strategy" for b in self.settings.data.prices)
                else (),
                observations=observations,
                availability=availability(state, observations),
                trigger=trigger,
                owned=tuple(
                    Ownership(target=p.payload.target, shares=p.payload.shares)
                    for p in state.portfolio.positions
                ),
            ),
        )
        return EconomicDecision.model_validate_json(strategy_worker(request))

    def execute(self, state, pending):
        try:
            return self.apply(state, pending)
        except InputUnavailable as error:
            return Transition(
                effects=Effects[EconomicPosition](),
                result=EconomicActionResult(
                    intent=pending.intent,
                    success=False,
                    effective_at=datetime.fromtimestamp(pending.eligible_at, UTC),
                    assets=None,
                    shares=0,
                    error=str(error),
                    unavailable=error.gap,
                    simulation=SimulationEvidence(
                        execution_id=pending.identity, model=MODEL, inputs=()
                    ),
                ),
            )

    def apply(self, state, pending):
        target = pending.intent.target
        before, markets = self.group(state, target, pending.eligible_at)
        positions = tuple(p.payload for p in before)
        positions = self.select_collateral(pending.intent, positions, markets)
        inventory = state.portfolio.balance(wallet(target))
        changed = apply_action(pending.intent, positions, markets, inventory)
        self.validate_borrow_health(pending.intent, changed.positions, markets)
        positions = self.release_collateral(pending.intent, changed.positions, markets)
        effects = Effects[EconomicPosition](
            assets=(
                AssetDelta(key=wallet(target), amount=changed.inventory_delta),
                *self.costs(state, pending.intent),
            ),
            positions=position_effects(before, positions),
        )
        result = EconomicActionResult(
            intent=pending.intent,
            success=True,
            effective_at=datetime.fromtimestamp(pending.eligible_at, UTC),
            assets=AssetAmount(asset=target.asset, amount=changed.assets)
            if changed.assets is not None
            else None,
            shares=changed.shares,
            simulation=self.evidence(before, pending.eligible_at, pending.identity),
        )
        return Transition(effects=effects, result=result)

    def validate_borrow_health(self, intent, positions, markets):
        if not isinstance(intent, BorrowIntent):
            return
        debt = self.config.allocation(intent.target.allocation)
        health = risk_view(positions, markets).health_factor_wad
        if health is None or health < debt.minimum_health_factor:
            raise ValueError("projected account health is below its configured threshold")

    def select_collateral(self, intent, positions, markets):
        if not isinstance(intent, BorrowIntent):
            return positions
        debt = self.config.allocation(intent.target.allocation)
        if not isinstance(debt, DebtAllocation):
            raise ValueError("borrowing requires a declared debt reserve and collateral selection")
        for p in positions:
            if p.target.allocation in debt.collateral:
                action = CollateralIntent[SupplyTarget](target=p.target, enabled=True)
                positions = apply_action(action, positions, markets, 0).positions
        return positions

    def release_collateral(self, intent, positions, markets):
        if not isinstance(intent, RepayIntent) or any(p.drawn_shares for p in positions):
            return positions
        debt = self.config.allocation(intent.target.allocation)
        if not isinstance(debt, DebtAllocation):
            return positions
        for p in positions:
            if p.target.allocation in debt.collateral:
                positions = apply_action(
                    CollateralIntent[SupplyTarget](target=p.target, enabled=False),
                    positions,
                    markets,
                    0,
                ).positions
        return positions

    def costs(self, state, intent):
        if self.settings.costs.kind == "excluded":
            return ()
        action = {
            "withdraw_amount": "withdraw",
            "repay_amount": "repay",
            "aave_refresh": "refresh",
        }.get(intent.kind, intent.kind)
        cost = getattr(self.settings.costs, action)
        key = wallet(intent.target, self.config.allocation(intent.target.allocation).native_asset)
        if state.portfolio.balance(key) < cost:
            raise ValueError("insufficient local inventory for the declared economic cost")
        return (AssetDelta(key=key, amount=-cost),)

    def evidence(self, positions, time, identity):
        inputs = {
            e for p in positions for e in self.book.evidence(p.payload.target.reserve, time)[1]
        }
        return SimulationEvidence(execution_id=identity, model=MODEL, inputs=tuple(sorted(inputs)))

    def deliver(self, state, result):
        request = EconomicOutcomeRequest(
            strategy=self.strategy,
            context=EconomicOutcomeInput(
                config=self.config,
                memory=state.memory,
                result=result,
            ),
        )
        return Memory.model_validate_json(strategy_worker(request))

    def closed(self, state, observations):
        return (
            not unavailable(state)
            and len(observations) == len(state.portfolio.positions)
            and all(
                not (
                    p.payload.shares
                    or p.payload.drawn_shares
                    or p.payload.premium_shares
                    or p.payload.premium_offset_ray
                    or p.payload.collateral
                    or p.payload.deficit
                )
                for p in state.portfolio.positions
            )
        )
