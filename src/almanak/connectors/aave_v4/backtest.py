"""Supply-only Aave V4 specialization of historical intent simulation."""

from typing import cast
from uuid import uuid4

from almanak.backtest.config import ExcludedCosts
from almanak.backtest.engine import Pending, RunState, Transition
from almanak.backtest.evm import EvmHistory
from almanak.backtest.history import HistoricalReader, ReadRequest
from almanak.backtest.strategy_prices import HistoricalSeries
from almanak.backtest.types import ConnectorId, UnixTime
from almanak.connectors.aave_v4.authority import admit_intent, validate_capabilities
from almanak.connectors.aave_v4.deployments import historical_revision_supported
from almanak.connectors.aave_v4.domain import PositionObservation
from almanak.connectors.aave_v4.history import (
    Market,
    read_market,
    verify_conversion,
)
from almanak.connectors.aave_v4.history_config import AaveBacktestConfig
from almanak.connectors.aave_v4.models import (
    AaveCapabilities,
    ActionResult,
    DecisionInput,
    Intent,
    LendingParameters,
    LendingSnapshot,
    Memory,
    OutcomeInput,
    Ownership,
    SupplyTarget,
)
from almanak.connectors.aave_v4.operations import (
    ApproveOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.aave_v4.reader import plan_operation
from almanak.effects import (
    AssetDelta,
    Effects,
    Portfolio,
    Position,
    PositionChange,
    PositionKey,
    WalletAsset,
)
from almanak.gateway.ethereum import DataError
from almanak.models import AccountBinding, AssetAmount, Chain, Eoa, Model, StrategyAccounts, UInt
from almanak.runtime import strategy_capabilities, strategy_decision, strategy_worker
from almanak.strategy import OutcomeRequest, StrategyDefinition
from almanak.strategy_api import SimulationEvidence
from almanak.triggers import IntervalSelection, IntervalTrigger

MODEL = "aave-v4-supply-historical-shares-v1"
CONNECTOR = ConnectorId("aave_v4")
# Hub conversion probe for an allocation without capital; any nonzero asset amount works.
CONVERSION_PROBE = 10**18


def capability(chain):
    from almanak.connectors.support import aave_support, historical_capability

    support = next((s for s in aave_support() if s.chain == chain.chain_id), None)
    if support is None:
        raise ValueError("connector has no historical support for the requested chain")
    return historical_capability(support, chain, CONNECTOR, MODEL)


class AavePosition(Model):
    target: SupplyTarget
    shares: UInt = 0
    allowance: UInt = 0
    supplied: UInt = 0
    withdrawn: UInt = 0


AaveState = RunState[AavePosition, Intent, ActionResult]


def position_key(target: SupplyTarget) -> PositionKey:
    return PositionKey(
        connector=CONNECTOR,
        chain=target.fund.chain,
        account=target.account,
        reference=f"{target.reserve.spoke}/{target.reserve.reserve_id}",
    )


def wallet(target: SupplyTarget, asset=None) -> WalletAsset:
    return WalletAsset(account=target.account, chain=target.fund.chain, asset=asset or target.asset)


def parameters(
    settings: AaveBacktestConfig, allocations, trigger: IntervalTrigger, debts=()
) -> LendingParameters:
    for debt in debts:
        capability(debt.reserve.chain).require(settings.schedule.start, "borrow")
    return LendingParameters(
        decision_trigger=trigger,
        accounts=StrategyAccounts(
            bindings=tuple(
                AccountBinding(name=fund.name, profile=Eoa(kind="eoa", fund=fund.fund))
                for fund in settings.funds
            )
        ),
        allocations=allocations,
    )


def initial_state(
    settings: AaveBacktestConfig, config: LendingParameters, interval: IntervalSelection
) -> AaveState:
    if config.decision_trigger != interval.effective:
        raise ValueError("strategy and simulator require the same effective interval")
    positions = tuple(
        Position[AavePosition](
            key=position_key(config.target(allocation.name)),
            payload=AavePosition(target=config.target(allocation.name)),
        )
        for allocation in config.allocations
    )
    portfolio = Portfolio[AavePosition](
        balances=tuple(balance for fund in settings.funds for balance in fund.inventory()),
        positions=positions,
    )
    for allocation in config.allocations:
        target = config.target(allocation.name)
        required = sum(
            a.capital
            for a in config.allocations
            if a.account == allocation.account and a.reserve.asset == target.asset
        )
        if portfolio.balance(wallet(target)) < required:
            raise ValueError("initial inventory cannot fund the combined Aave allocation capital")
        portfolio.balance(wallet(target, allocation.native_asset))
    return AaveState(
        run_id=uuid4().hex,
        time=settings.schedule.start,
        schedule=settings.schedule,
        interval=interval,
        portfolio=portfolio,
    )


class AaveModel:
    def __init__(
        self,
        config: LendingParameters,
        settings: AaveBacktestConfig,
        strategy: StrategyDefinition,
        histories: dict[Chain, EvmHistory],
        baselines: tuple[Market, ...] = (),
        authority: AaveCapabilities | None = None,
    ):
        for debt in config.debts:
            capability(debt.reserve.chain).require(settings.schedule.start, "borrow")
        self.config = config
        self.settings = settings
        self.strategy = strategy
        self.histories = histories
        self.baselines = baselines
        self.read_templates: dict[str, tuple[int, tuple[ReadRequest, ...]]] = {}
        self.capabilities = tuple(capability(chain) for chain in histories)
        self.prices = HistoricalSeries(histories, settings)
        self.authority = (
            authority if authority is not None else strategy_capabilities(strategy, config)
        )
        validate_capabilities(config, self.authority)

    def route(self, intent):
        admit_intent(self.authority, intent)
        target = self.config.target(intent.target.allocation)
        if target != intent.target:
            raise ValueError("intent target is not bound to this historical run")
        cap = next(c for c in self.capabilities if c.chain == target.fund.chain)
        return cap, intent.kind

    def market(self, target, timestamp: UnixTime, shares: int) -> Market:
        history = self.histories[target.fund.chain]
        anchor = history.floor(timestamp)
        # Outcome evidence names this market's inputs, independent of previous
        # observations and the resolver's cache state after a runtime restart.
        cast(HistoricalReader, history.reader).used.clear()
        block = history.pin(anchor)
        cap = next(c for c in self.capabilities if c.chain == target.fund.chain)
        cap.require(UnixTime(block.timestamp))
        reader = cast(HistoricalReader, history.reader)
        self.prefetch_market(reader, target, block, shares)
        observed = read_market(reader, target.reserve, block)
        self.retain_template(reader, observed)
        self.validate_revision(observed)
        verify_conversion(history.reader, observed, self.probe(target), shares)
        return observed

    def probe(self, target) -> int:
        """Assets for the Hub conversion check: the allocation's capital, else a nonzero
        fixed probe so the check never degenerates to converting zero."""
        allocation = self.config.allocation(target.allocation)
        return getattr(allocation, "capital", 0) or CONVERSION_PROBE

    def retain_template(self, reader, market):
        methods = {"eth_call", "eth_getCode", "eth_getStorageAt"}
        requests = tuple(
            reader.cache[key].request
            for key in sorted(reader.used)
            if reader.cache[key].request.method in methods
        )
        self.read_templates[market.reserve.source.model_dump_json()] = (
            market.reserve.hub_asset_id,
            requests,
        )

    def prefetch_market(self, reader, target, block, shares):
        from almanak.gateway.safe_fixture import calldata

        template = self.read_templates.get(target.reserve.model_dump_json())
        if template is None:
            return
        asset_id, requests = template
        selector = {"blockHash": block.hash, "requireCanonical": True}
        prepared = [r.model_copy(update={"params": (*r.params[:-1], selector)}) for r in requests]
        for signature, amount in (
            ("previewAddByAssets(uint256,uint256)", self.probe(target)),
            ("previewRemoveByShares(uint256,uint256)", shares),
        ):
            call = {
                "to": target.reserve.hub,
                "data": calldata(signature, ("uint256", "uint256"), (asset_id, amount)),
            }
            prepared.append(
                ReadRequest(chain=reader.chain, method="eth_call", params=(call, selector))
            )
        reader.prefetch(tuple(prepared))

    def validate_revision(self, market):
        if any(not historical_revision_supported(revision) for revision in market.revisions):
            raise DataError(
                "unsupported_revision", "Aave contract revision has no model validation"
            )
        baseline = next(
            (b for b in self.baselines if b.reserve.source == market.reserve.source), None
        )
        if baseline is not None and market.revisions != baseline.revisions:
            raise DataError("unsupported_revision", "Aave code or proxy implementation changed")

    def observe(self, state):
        self.validate_state(state)
        return tuple(
            self.snapshot(state, position.payload, state.time)
            for position in state.portfolio.positions
        )

    def validate_state(self, state):
        if (
            state.interval.effective != self.config.decision_trigger
            or state.schedule != self.settings.schedule
        ):
            raise ValueError("backtest configuration differs from its frozen scheduling")
        targets = tuple(self.config.target(a.name) for a in self.config.allocations)
        if tuple(p.payload.target for p in state.portfolio.positions) != targets:
            raise ValueError("historical positions do not match the configured allocations")
        for position in state.portfolio.positions:
            if position.key != position_key(position.payload.target):
                raise ValueError("historical position key differs from its connector ownership")

    def snapshot(self, state, position: AavePosition, timestamp):
        target = position.target
        market = self.market(target, timestamp, position.shares)
        from almanak.configuration.metadata import verify

        verify(self.config, market.reserve.asset)
        native = self.config.allocation(target.allocation).native_asset
        return LendingSnapshot(
            allocation=target.allocation,
            account=target.account,
            position=PositionObservation(
                reserve=market.reserve,
                account=target.fund,
                supplied_assets=market.supplied_assets(position.shares),
                supplied_shares=position.shares,
                drawn_debt=0,
                premium_debt=0,
                total_debt=0,
            ),
            wallet_balance=state.portfolio.balance(wallet(target)),
            allowance=position.allowance,
            native_balance=AssetAmount(
                asset=native, amount=state.portfolio.balance(wallet(target, native))
            ),
            simulated=True,
        )

    def decide(self, state, observations, trigger):
        context = DecisionInput(
            config=self.config,
            memory=state.memory,
            owned=tuple(
                Ownership(target=p.payload.target, shares=p.payload.shares)
                for p in state.portfolio.positions
            ),
            observations=observations,
            prices=self.prices.at(state.time),
            trigger=trigger,
        )
        return strategy_decision(self.strategy, context)

    def eligible_at(self, intent, time: UnixTime, latency: int) -> UnixTime:
        # after(T + latency - 1) admits an exact latency boundary, but never T itself.
        floor = UnixTime(time + max(latency, 1) - 1)
        return UnixTime(self.histories[intent.target.fund.chain].after(floor).timestamp)

    def execute(self, state, pending: Pending[Intent]) -> Transition[AavePosition, ActionResult]:
        target = pending.intent.target
        before = next(p for p in state.portfolio.positions if p.key == position_key(target))
        snapshot = self.snapshot(state, before.payload, pending.eligible_at)
        operation = plan_operation(pending.intent, snapshot)
        market = self.market(target, pending.eligible_at, before.payload.shares)
        position, deltas, assets, shares = self.apply_operation(
            state, before.payload, operation, market
        )
        deltas = (*self.cost_effects(state, target, operation.kind), *deltas)
        effects = Effects[AavePosition](
            assets=deltas,
            positions=(
                PositionChange[AavePosition](
                    before=before, after=Position[AavePosition](key=before.key, payload=position)
                ),
            ),
        )
        reader = cast(HistoricalReader, self.histories[target.fund.chain].reader)
        result = ActionResult(
            intent=pending.intent,
            success=True,
            complete=completion(operation, position),
            assets=AssetAmount(asset=target.asset, amount=assets),
            shares=shares,
            block=market.reserve.block,
            simulation=SimulationEvidence(
                execution_id=pending.identity, model=MODEL, inputs=tuple(sorted(reader.used))
            ),
        )
        return Transition[AavePosition, ActionResult](
            effects=effects, result=result, complete=result.complete
        )

    def apply_operation(self, state, before, operation, market):
        if isinstance(operation, (ApproveOperation, SupplyOperation)):
            self.validate_supply_entry(state, before, operation.amount.amount)
        if isinstance(operation, ApproveOperation):
            return before.model_copy(update={"allowance": operation.amount.amount}), (), 0, 0
        if isinstance(operation, RevokeOperation):
            return before.model_copy(update={"allowance": 0}), (), 0, 0
        validate_active(market)
        if isinstance(operation, SupplyOperation):
            return supply_effects(state, before, operation.amount.amount, market)
        if isinstance(operation, WithdrawOperation):
            return withdrawal_effects(state, before, market)
        raise ValueError("unsupported Aave modeled operation")

    def validate_supply_entry(self, state, position, amount):
        allocation = self.config.allocation(position.target.allocation)
        native = wallet(position.target, allocation.native_asset)
        if state.portfolio.balance(native) < allocation.minimum_native_balance:
            raise ValueError("native balance is below the configured new-risk threshold")
        if state.portfolio.balance(wallet(position.target)) < amount:
            raise ValueError("insufficient WETH inventory")

    def cost_effects(self, state, target, action):
        policy = self.settings.costs
        if isinstance(policy, ExcludedCosts):
            return ()
        key = wallet(target, self.config.allocation(target.allocation).native_asset)
        cost = getattr(policy, action)
        if cost > state.portfolio.balance(key):
            raise ValueError("insufficient modeled native execution funding")
        return (AssetDelta(key=key, amount=-cost),)

    def deliver(self, state, result):
        return Memory.model_validate_json(
            strategy_worker(
                OutcomeRequest(
                    strategy=self.strategy,
                    context=OutcomeInput(config=self.config, memory=state.memory, result=result),
                )
            )
        )

    def closed(self, state, observations):
        return all(
            not p.payload.shares and not p.payload.allowance for p in state.portfolio.positions
        )


def completion(operation, position):
    if isinstance(operation, ApproveOperation):
        return False
    if isinstance(operation, (WithdrawOperation, RevokeOperation)):
        return position.shares == 0 and position.allowance == 0
    return True


def validate_active(market: Market):
    if market.reserve.paused or not market.active or market.halted:
        raise ValueError("Aave market is paused, inactive or halted at execution")


def shared_positions(state, target, *, spoke=False):
    positions = [p.payload for p in state.portfolio.positions]
    return tuple(
        p
        for p in positions
        if p.target.fund.chain == target.fund.chain
        and p.target.reserve.hub == target.reserve.hub
        and p.target.asset == target.asset
        and (not spoke or p.target.reserve.spoke == target.reserve.spoke)
    )


def supply_effects(state, before: AavePosition, amount: int, market: Market):
    if market.reserve.frozen:
        raise ValueError("Aave supply is frozen at execution")
    local_shares = sum(p.shares for p in shared_positions(state, before.target, spoke=True))
    exposure = market.cap_exposure(market.spoke_shares + local_shares) + amount
    if (
        market.add_cap != 2**40 - 1
        and exposure > market.add_cap * 10**market.reserve.asset.decimals
    ):
        raise ValueError("Aave historical add cap has insufficient headroom")
    shares = market.supplied_shares(amount)
    hub_positions = shared_positions(state, before.target)
    hub_shares = sum(p.shares for p in hub_positions)
    flow = sum(p.supplied - p.withdrawn for p in hub_positions)
    totals = (
        market.added_shares + hub_shares + shares,
        market.spoke_shares + local_shares + shares,
        before.shares + shares,
        market.liquidity + flow + amount,
    )
    if shares == 0 or min(totals) < 0 or max(totals) >= 2**120:
        raise ValueError("Aave supply share quantity is outside supported bounds")
    after = before.model_copy(
        update={
            "shares": before.shares + shares,
            "allowance": 0,
            "supplied": before.supplied + amount,
        }
    )
    return after, (AssetDelta(key=wallet(before.target), amount=-amount),), amount, shares


def withdrawal_effects(state, before: AavePosition, market: Market):
    amount = market.supplied_assets(before.shares)
    shares = market.burned_shares(amount)
    flow = sum(p.supplied - p.withdrawn for p in shared_positions(state, before.target))
    if amount > market.liquidity + flow:
        raise ValueError("Aave historical Hub liquidity cannot fund this withdrawal")
    if amount == 0 or shares == 0 or shares > before.shares:
        raise ValueError("Aave withdrawal cannot clear the remaining share dust")
    after = before.model_copy(
        update={
            "shares": before.shares - shares,
            "withdrawn": before.withdrawn + amount,
        }
    )
    return after, (AssetDelta(key=wallet(before.target), amount=amount),), amount, shares
