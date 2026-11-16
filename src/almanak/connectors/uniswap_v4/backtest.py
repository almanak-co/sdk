"""The same author strategy, with durable hypothetical balances and positions."""

from types import SimpleNamespace
from uuid import uuid4

from pydantic import Field

from almanak.backtest.engine import RunState, Transition
from almanak.backtest.strategy_prices import HistoricalSeries
from almanak.backtest.types import UnixTime
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.uniswap_v4.authority import admit_intent, validate_capabilities
from almanak.connectors.uniswap_v4.backtest_config import OperationKind
from almanak.connectors.uniswap_v4.calibration import validate_costs
from almanak.connectors.uniswap_v4.history import market, refuse_band
from almanak.connectors.uniswap_v4.lifecycle import increases_exposure
from almanak.connectors.uniswap_v4.models import Intent, Parameters
from almanak.connectors.uniswap_v4.observations import decision_quotes, initial_owned
from almanak.connectors.uniswap_v4.pool import pool_id
from almanak.connectors.uniswap_v4.reader import Quote
from almanak.connectors.uniswap_v4.simulation import simulate_intent
from almanak.connectors.uniswap_v4.state import (
    ActionResult,
    Allowance,
    DecisionInput,
    OutcomeInput,
    Ownership,
    SimulatedLiquidity,
    Snapshot,
)
from almanak.connectors.uniswap_v4.validation import (
    close_limits,
    deadline,
    mint_limits,
    swap_limits,
)
from almanak.dex_intents import CloseLiquidity, OpenLiquidity, SwapExactInput
from almanak.effects import (
    AssetDelta,
    Effects,
    Portfolio,
    Position,
    PositionChange,
    PositionKey,
    WalletAsset,
)
from almanak.models import AccountBinding, AssetAmount, Model, StrategyAccounts, UInt
from almanak.runtime import strategy_capabilities, strategy_decision, strategy_worker
from almanak.strategy import OutcomeRequest
from almanak.strategy_api import Memory, SimulationEvidence
from almanak.triggers import DecisionSchedule

MODEL = "uniswap-v4-exact-quoter-fixed-path-lp-v1"
CONNECTOR = ConnectorId("uniswap_v4")


def capability(connector, chain):
    # Resolve the declaration installed on the connector binding, never the shared
    # declaration function: a connector bound with a different chain support would
    # otherwise be unreachable from the historical paths.
    from almanak.connectors.support import historical_capability

    if not any(support.chain == chain.chain_id for support in connector.chain_support):
        raise ValueError("connector has no historical support for the requested chain")
    return historical_capability(connector.support_for(chain.chain_id), chain, CONNECTOR, MODEL)


class HistoricalPosition(Model):
    owned: Ownership
    allowances: tuple[Allowance, ...]
    # Cumulative inputs by pool currency, retained for reports and checkpoint replay.
    # They bound nothing: entries repeat while funding inventory remains.
    swap0: UInt = 0
    swap1: UInt = 0
    mint0: UInt = 0
    mint1: UInt = 0
    fees0: UInt = 0
    fees1: UInt = 0
    benchmark: tuple[AssetAmount, AssetAmount] | None = None
    # Collected growth the upper band bracket admits beyond the credited lower bracket.
    unattributed0: UInt = Field(default=0, exclude_if=lambda v: v == 0)
    unattributed1: UInt = Field(default=0, exclude_if=lambda v: v == 0)

    @property
    def target(self):
        return self.owned.target


class Result(ActionResult):
    before: Snapshot
    after: Ownership
    steps: tuple[OperationKind, ...]
    delta0: int
    delta1: int
    fees0: UInt
    fees1: UInt
    gas_paid: AssetAmount
    fill: Quote | None


State = RunState[HistoricalPosition, Intent, Result]


def position_key(target):
    return PositionKey(
        connector=CONNECTOR,
        chain=target.fund.chain,
        account=target.account,
        reference=f"allocation:{target.allocation}:{pool_id(target.pool)}",
    )


def wallet(target, asset):
    return WalletAsset(account=target.account, chain=target.fund.chain, asset=asset)


def payer_wallet(settings, target, asset):
    payer = settings.payer(target.account)
    return WalletAsset(account=payer.name, chain=payer.fund.chain, asset=asset)


# Banded allocations credit the fee growth continuously initialized ticks prove accrued
# inside each concrete range and report the upper bracket; fixed ranges stay exact.
BAND_LIQUIDITY_MODEL = "bracketed_historical_fee_growth"


def parameters(author, settings, interval):
    """Detailed historical parameters: archival range growth has no band model."""
    for allocation in author.allocations:
        refuse_band(allocation)
    return bound_parameters(author, settings, interval)


def economic_parameters(author, settings, interval):
    """Prepared economic parameters: a band requires the bracketed liquidity model."""
    banded = any(a.band is not None for a in author.allocations)
    selected = settings.liquidity_model == BAND_LIQUIDITY_MODEL
    if banded and not selected:
        raise ValueError(
            f"banded liquidity allocations require liquidity_model {BAND_LIQUIDITY_MODEL}"
        )
    if selected and not banded:
        raise ValueError("bracketed fee growth requires a banded liquidity allocation")
    return bound_parameters(author, settings, interval)


def bound_parameters(author, settings, interval):
    if any(a.liquidity_enabled for a in author.allocations) and settings.liquidity_model is None:
        raise ValueError("LP allocations require an explicit liquidity model")
    return Parameters(
        decision_trigger=interval.effective,
        asset_metadata=author.asset_metadata,
        parameters=author.parameters,
        accounts=StrategyAccounts(
            bindings=tuple(AccountBinding(name=f.name, profile=f.profile) for f in settings.funds)
        ),
        allocations=author.allocations,
    )


def empty_allowances(target):
    result = []
    for asset in target.assets:
        result.append(Allowance(asset=asset, spender=target.contracts.permit2, amount=0))
        for spender in (target.contracts.router, target.contracts.position_manager):
            result.append(Allowance(asset=asset, spender=spender, amount=0, expiration=0, nonce=0))
    return tuple(result)


def initial_state(settings, config, interval):
    balances = tuple(b for f in settings.funds for b in f.inventory()) + tuple(
        b for p in settings.payers for b in p.inventory()
    )
    positions = tuple(
        Position[HistoricalPosition](
            key=position_key(config.target(a.name)),
            payload=HistoricalPosition(
                owned=initial_owned(config, a.name),
                allowances=empty_allowances(config.target(a.name)),
            ),
        )
        for a in config.allocations
    )
    portfolio = Portfolio[HistoricalPosition](balances=balances, positions=positions)
    for allocation in config.allocations:
        target = config.target(allocation.name)
        if portfolio.balance(wallet(target, target.funding_currency)) < allocation.capital.amount:
            raise ValueError("initial funding inventory does not cover declared capital")
        portfolio.balance(wallet(target, target.counter_currency))
        if settings.payers:
            portfolio.balance(payer_wallet(settings, target, allocation.native_asset))
    return State(
        interval=interval,
        decision_schedule=DecisionSchedule(),
        run_id=uuid4().hex,
        time=settings.schedule.start,
        schedule=settings.schedule,
        portfolio=portfolio,
    )


class UniswapModel:
    def __init__(self, config, settings, strategy, histories, baselines=(), authority=None):
        from almanak.connectors.composition import connector_for

        validate_costs(settings)
        self.config, self.settings, self.strategy = config, settings, strategy
        self.histories, self.baselines = histories, baselines
        connector = connector_for(config)
        self.capabilities = tuple(capability(connector, chain) for chain in histories)
        self.prices = HistoricalSeries(histories, settings)
        self.authority = (
            authority if authority is not None else strategy_capabilities(strategy, config)
        )
        validate_capabilities(config, self.authority)

    def route(self, intent):
        admit_intent(self.authority, intent)
        if intent.target != self.config.target(intent.target.allocation):
            raise ValueError("historical intent targets another configured fund or pool")
        return next(
            c for c in self.capabilities if c.chain == intent.target.fund.chain
        ), intent.kind

    def eligible_at(self, intent, time, latency):
        history = self.histories[intent.target.fund.chain]
        return UnixTime(history.after(UnixTime(time + max(latency, 1) - 1)).timestamp)

    def validate_state(self, state):
        if (
            state.schedule != self.settings.schedule
            or state.interval.effective != self.config.decision_trigger
        ):
            raise ValueError("historical checkpoint differs from the retained interval or window")
        self.validate_baselines(state.schedule)
        if tuple(p.payload.target for p in state.portfolio.positions) != tuple(
            self.config.target(a.name) for a in self.config.allocations
        ):
            raise ValueError("historical ownership differs from the retained target bindings")
        for position in state.portfolio.positions:
            if position.key != position_key(position.payload.target):
                raise ValueError("historical position has another durable ownership identity")
            validate_position(self.config, position.payload)

    def validate_baselines(self, schedule):
        targets = tuple(self.config.target(a.name) for a in self.config.allocations)
        if tuple(b.target for b in self.baselines) != targets:
            raise ValueError(
                "historical execution requires retained preflight evidence for every target"
            )
        for baseline in self.baselines:
            history = self.histories[baseline.target.fund.chain]
            if baseline.first != history.floor(schedule.start) or baseline.last != history.floor(
                schedule.deadline
            ):
                raise ValueError(
                    "historical boundary evidence does not cover the complete configured window"
                )

    def validate_limits(self, state, pending, payload, observed):
        intent = pending.intent
        if not isinstance(intent, (SwapExactInput, OpenLiquidity, CloseLiquidity)):
            return
        decision = self.snapshot(
            state, payload, pending.decision_time, quotes=isinstance(intent, SwapExactInput)
        )
        context = SimpleNamespace(decision=SimpleNamespace(observations=(decision,)))
        allocation = self.config.allocation(intent.target.allocation)
        if isinstance(intent, SwapExactInput):
            swap_limits(context, intent, observed, allocation)
        elif isinstance(intent, OpenLiquidity):
            mint_limits(context, intent, observed, allocation)
        else:
            close_limits(context, intent, allocation)

    def snapshot(self, state, payload, timestamp, *, quotes=True, entry=True):
        """`entry=False`, or any mode but running, omits the entry quote no entry can use."""
        target = payload.target
        pool, growth = market(self.histories[target.fund.chain], target, timestamp)
        from almanak.configuration.metadata import verify

        verify(self.config, pool.currency0)
        verify(self.config, pool.currency1)
        allocation = self.config.allocation(target.allocation)
        position = payload.owned.position
        payer = self.settings.payer(target.account)
        return Snapshot(
            target=target,
            pool=pool,
            owned=payload.owned,
            wallet0=AssetAmount(
                asset=target.assets[0],
                amount=state.portfolio.balance(wallet(target, target.assets[0])),
            ),
            wallet1=AssetAmount(
                asset=target.assets[1],
                amount=state.portfolio.balance(wallet(target, target.assets[1])),
            ),
            allowances=payload.allowances,
            native_balance=AssetAmount(
                asset=allocation.native_asset,
                amount=state.portfolio.balance(
                    payer_wallet(self.settings, target, allocation.native_asset)
                ),
            ),
            payer=payer.fund,
            position_liquidity=position.liquidity if position else 0,
            fee_growth=growth,
            position_growth=position.fee_growth if position else None,
            simulated=True,
            quotes_included=quotes,
            **(
                decision_quotes(
                    self.histories[target.fund.chain].reader,
                    self.config,
                    payload.owned,
                    pool,
                    entry=entry and state.mode == "running",
                )
                if quotes
                else {}
            ),
        )

    def observe(self, state):
        self.validate_state(state)
        return tuple(self.snapshot(state, p.payload, state.time) for p in state.portfolio.positions)

    def decide(self, state, observations, trigger):
        context = DecisionInput(
            config=self.config,
            memory=state.memory,
            owned=tuple(p.payload.owned for p in state.portfolio.positions),
            observations=observations,
            prices=self.prices.at(state.time),
            trigger=trigger,
        )
        return strategy_decision(self.strategy, context)

    def execute(self, state, pending):
        self.validate_state(state)
        target = pending.intent.target
        before = next(p for p in state.portfolio.positions if p.key == position_key(target))
        is_exit = pending.intent.kind == "exit_exchange"
        # The exit observation plans the exit leg only, so it measures no entry quote.
        observed = self.snapshot(
            state, before.payload, pending.eligible_at, quotes=is_exit, entry=False
        )
        if is_exit and self.exit_settled(observed):
            return self.settle(before, pending, observed)
        actual = pending
        if is_exit:
            from almanak.connectors.uniswap_v4.exit import leg

            actual = pending.model_copy(update={"intent": leg(self.config, observed)})
        allocation = self.config.allocation(target.allocation)
        deadline(pending.intent, observed, allocation)
        eligible_inputs = self.histories[target.fund.chain].reader.used.copy()
        if not is_exit:
            self.validate_limits(state, pending, before.payload, observed)
        reader = self.histories[target.fund.chain].reader
        eligible_inputs.update(reader.used)
        self.histories[target.fund.chain].pin(observed.block)
        reader.used.update(eligible_inputs)
        if increases_exposure(pending.intent) and (
            state.mode != "running"
            or observed.native_balance.amount < allocation.minimum_native_balance
        ):
            raise ValueError("historical entry lacks running authority or explicit gas funding")
        payload, deltas, fees, steps, fill = simulate_intent(
            reader, self.config, actual.intent, observed, before.payload, pending.identity
        )
        validate_position(self.config, payload)
        gas = sum(self.settings.costs.per_operation[kind] for kind in steps)
        effects = transition_effects(
            self.settings, before, payload, deltas, allocation.native_asset, gas
        )
        from almanak.connectors.uniswap_v4.exit import remaining

        cleanup = any(
            a.needs_cleanup(int(observed.effective_at.timestamp())) for a in payload.allowances
        )
        complete = not is_exit or not remaining(payload.owned, cleanup)
        result = Result(
            intent=pending.intent,
            success=True,
            complete=complete,
            block=observed.block,
            simulation=SimulationEvidence(
                execution_id=pending.identity, model=MODEL, inputs=tuple(sorted(reader.used))
            ),
            before=observed,
            after=payload.owned,
            steps=steps,
            delta0=deltas[0],
            delta1=deltas[1],
            fees0=fees[0],
            fees1=fees[1],
            gas_paid=AssetAmount(asset=allocation.native_asset, amount=gas),
            fill=fill,
        )
        return Transition[HistoricalPosition, Result](
            effects=effects, result=result, complete=complete
        )

    def deliver(self, state, result):
        return Memory.model_validate_json(
            strategy_worker(
                OutcomeRequest(
                    strategy=self.strategy,
                    context=OutcomeInput(config=self.config, memory=state.memory, result=result),
                )
            )
        )

    @staticmethod
    def exit_settled(observed):
        from almanak.connectors.uniswap_v4.exit import cleared

        return cleared(observed)

    def settle(self, before, pending, observed):
        reader = self.histories[pending.intent.target.fund.chain].reader
        result = Result(
            intent=pending.intent,
            success=True,
            block=observed.block,
            simulation=SimulationEvidence(
                execution_id=pending.identity, model=MODEL, inputs=tuple(sorted(reader.used))
            ),
            **settled_exit(self.config, before, observed),
        )
        return Transition[HistoricalPosition, Result](
            effects=Effects[HistoricalPosition](), result=result, complete=True
        )

    def closed(self, state, observations):
        from almanak.connectors.uniswap_v4.exit import cleared

        return settled_positions(state, observations, cleared)


def settled_positions(state, observations, cleared):
    """Every retained position is observed with its own inventory and no exit obligation.

    The observation supplies the exit residual measurement; the retained position
    remains the durable inventory it must describe.
    """
    observed = {o.target: o for o in observations}
    return all(
        p.payload.target in observed
        and observed[p.payload.target].owned == p.payload.owned
        and cleared(observed[p.payload.target])
        for p in state.portfolio.positions
    )


def settled_exit(config, before, observed):
    """Result fields of an exit whose observation leaves only measured residual dust.

    A leg that leaves counter inventory completes at the next exit observation, whose
    exit quote already measures it, so no quote is added after the leg and the exit
    completes without a step, a cost or a change to the retained position.
    """
    native = config.allocation(before.payload.target.allocation).native_asset
    return dict(
        complete=True,
        before=observed,
        after=before.payload.owned,
        steps=(),
        delta0=0,
        delta1=0,
        fees0=0,
        fees1=0,
        gas_paid=AssetAmount(asset=native, amount=0),
        fill=None,
    )


def validate_position(config, payload):
    if payload.owned.position is not None and not isinstance(
        payload.owned.position, SimulatedLiquidity
    ):
        raise ValueError(
            "historical liquidity must have a simulation identity, never an on-chain NFT"
        )


def transition_effects(settings, before, payload, deltas, native_asset, gas):
    target = payload.target
    changes = tuple(
        AssetDelta(key=wallet(target, asset), amount=delta)
        for asset, delta in zip(target.assets, deltas, strict=True)
    )
    return Effects[HistoricalPosition](
        assets=(
            *changes,
            *(
                (AssetDelta(key=payer_wallet(settings, target, native_asset), amount=-gas),)
                if gas
                else ()
            ),
        ),
        positions=(
            PositionChange[HistoricalPosition](
                before=before, after=Position[HistoricalPosition](key=before.key, payload=payload)
            ),
        ),
    )
