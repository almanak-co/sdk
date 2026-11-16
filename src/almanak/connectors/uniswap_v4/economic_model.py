"""Offline economic intents over external pool samples and durable owned inventory."""

from datetime import UTC, datetime

from almanak.backtest.engine import Transition
from almanak.backtest.types import UnixTime
from almanak.connectors.uniswap_v4.authority import validate_capabilities
from almanak.connectors.uniswap_v4.backtest import (
    HistoricalPosition,
    UniswapModel,
    payer_wallet,
    position_key,
    settled_exit,
    transition_effects,
    validate_position,
    wallet,
)
from almanak.connectors.uniswap_v4.backtest import (
    initial_state as historical_initial,
)
from almanak.connectors.uniswap_v4.economic_band import band_inputs, band_settled
from almanak.connectors.uniswap_v4.economic_data import MODEL
from almanak.connectors.uniswap_v4.economic_prepare import capability
from almanak.connectors.uniswap_v4.economic_state import (
    DecisionInput,
    EconomicResult,
    EconomicSnapshot,
    OutcomeInput,
    State,
)
from almanak.connectors.uniswap_v4.lifecycle import increases_exposure
from almanak.connectors.uniswap_v4.observations import quotes_for
from almanak.connectors.uniswap_v4.planning import liquidity_operation
from almanak.connectors.uniswap_v4.reader import EconomicQuoteSource, Quote
from almanak.connectors.uniswap_v4.simulation import liquidity_exit, minted, swap_fill
from almanak.connectors.uniswap_v4.state import Decision, Swap
from almanak.connectors.uniswap_v4.swap_math import exact_input
from almanak.connectors.uniswap_v4.validation import deadline
from almanak.dex_intents import (
    CloseLiquidity,
    CollectLiquidityFees,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.effects import Effects
from almanak.models import AssetAmount
from almanak.runtime import strategy_capabilities, strategy_worker
from almanak.strategy import EconomicDecisionRequest, EconomicOutcomeRequest
from almanak.strategy_api import Memory, SimulationEvidence


def initial_state(settings, config, interval):
    return State.model_validate_json(
        historical_initial(settings, config, interval).model_dump_json()
    )


def quote_at(record, target, amount, time):
    if record.payload.pool.source != target.pool or amount.asset not in target.assets:
        raise ValueError("economic quote has another pool or input asset")
    if record.available_at > time:
        raise ValueError("economic quote cannot use a future pool sample")
    direction = amount.asset == target.assets[0]
    result = exact_input(record.payload, amount.amount, direction)
    return Quote(
        block=record.reference,
        amount=amount,
        output=AssetAmount(asset=target.assets[1 if direction else 0], amount=result.output),
        quoter_gas=None,
        source=EconomicQuoteSource(simulation_time=UnixTime(time), evidence=record.evidence),
    )


class EconomicModel(UniswapModel):
    def __init__(self, config, settings, strategy, book, *, authority=None):
        from almanak.connectors.composition import connector_for

        self.config, self.settings, self.strategy, self.book = config, settings, strategy, book
        self.baselines = tuple(book.baselines.values())
        connector = connector_for(config)
        self.capabilities = tuple(capability(connector, h.chain) for h in settings.history)
        self.authority = strategy_capabilities(strategy, config) if authority is None else authority
        validate_capabilities(config, self.authority)

    def eligible_at(self, intent, time, latency):
        return UnixTime(time + max(1, latency))

    def validate_baselines(self, schedule):
        if schedule != self.book.settings.schedule:
            raise ValueError("economic pool checkpoint differs from the prepared window")

    def snapshot(self, state, payload, timestamp, *, quotes=True, entry=True):
        """`entry=False`, or any mode but running, omits the entry quote no entry can use."""
        target = payload.target
        record = self.book.at(target, timestamp)
        pool, growth = record.payload.pool, self.position_growth(record, payload)
        from almanak.configuration.metadata import verify

        verify(self.config, pool.currency0)
        verify(self.config, pool.currency1)
        allocation = self.config.allocation(target.allocation)
        position = payload.owned.position
        return EconomicSnapshot(
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
            )
            if self.settings.payers
            else None,
            payer=self.settings.payer(target.account).fund if self.settings.payers else None,
            position_liquidity=position.liquidity if position else 0,
            fee_growth=growth,
            position_growth=position.fee_growth if position else None,
            simulation_time=UnixTime(timestamp),
            quotes_included=quotes,
            hook_terms=record.payload.hook,
            **(
                quotes_for(
                    self.config,
                    payload.owned,
                    pool,
                    lambda amount: quote_at(record, target, amount, timestamp),
                    entry=entry and state.mode == "running",
                )
                if quotes
                else {}
            ),
        )

    def position_growth(self, record, payload):
        """Fixed ranges read sampled growth; a held band position reads its lower bracket."""
        position = payload.owned.position
        if payload.target.band is None or position is None:
            return record.payload.fee_growth
        return self.band_growth(record, payload.target, position.range).lower

    def band_growth(self, record, target, selected):
        return self.book.band_bounds(target, record, selected)

    def decide(self, state, observations, trigger):
        request = EconomicDecisionRequest(
            strategy=self.strategy,
            context=DecisionInput(
                config=self.config,
                memory=state.memory,
                owned=tuple(p.payload.owned for p in state.portfolio.positions),
                observations=observations,
                trigger=trigger,
                prices=self.book.prices.at(state.time),
            ),
        )
        return Decision.model_validate_json(strategy_worker(request))

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
        if not is_exit:
            self.validate_limits(state, pending, before.payload, observed)
        if increases_exposure(pending.intent) and state.mode != "running":
            raise ValueError("economic entry requires running authority")
        record = self.book.at(target, pending.eligible_at)
        acted, bounds = band_inputs(self.band_growth, record, actual.intent, observed)
        payload, deltas, fees, steps, fill = simulate(actual, acted, before.payload, record)
        payload = band_settled(payload, before.payload, bounds, actual.intent, fees)
        validate_position(self.config, payload)
        cost = economic_cost(self.settings.costs, actual.intent)
        effects = transition_effects(
            self.settings, before, payload, deltas, allocation.native_asset, cost
        )
        from almanak.connectors.uniswap_v4.exit import remaining

        cleanup = any(
            a.needs_cleanup(int(observed.effective_at.timestamp())) for a in payload.allowances
        )
        complete = not is_exit or not remaining(payload.owned, cleanup)
        result = EconomicResult(
            complete=complete,
            intent=pending.intent,
            effective_at=datetime.fromtimestamp(pending.eligible_at, UTC),
            simulation=self.evidence(target, pending),
            before=observed,
            after=payload.owned,
            steps=steps,
            delta0=deltas[0],
            delta1=deltas[1],
            fees0=fees[0],
            fees1=fees[1],
            gas_paid=AssetAmount(asset=allocation.native_asset, amount=cost),
            fill=fill,
        )
        return Transition[HistoricalPosition, EconomicResult](
            effects=effects, result=result, complete=complete
        )

    def evidence(self, target, pending):
        inputs = self.book.at(target, pending.eligible_at).evidence
        inputs += self.book.at(target, pending.decision_time).evidence
        return SimulationEvidence(
            execution_id=pending.identity, model=MODEL, inputs=tuple(sorted(set(inputs)))
        )

    def settle(self, before, pending, observed):
        result = EconomicResult(
            intent=pending.intent,
            effective_at=datetime.fromtimestamp(pending.eligible_at, UTC),
            simulation=self.evidence(pending.intent.target, pending),
            **settled_exit(self.config, before, observed),
        )
        return Transition[HistoricalPosition, EconomicResult](
            effects=Effects[HistoricalPosition](), result=result, complete=True
        )

    def deliver(self, state, result):
        return Memory.model_validate_json(
            strategy_worker(
                EconomicOutcomeRequest(
                    strategy=self.strategy,
                    context=OutcomeInput(config=self.config, memory=state.memory, result=result),
                )
            )
        )


def simulate(pending, observed, payload, record):
    intent = pending.intent
    if isinstance(intent, SwapExactInput):
        operation = Swap(
            target=intent.target,
            amount=intent.amount,
            minimum=intent.minimum_output,
            deadline=intent.deadline,
        )
        fill = quote_at(record, intent.target, intent.amount, pending.eligible_at)
        payload, deltas, _ = swap_fill(operation, observed, payload, fill)
        return payload, deltas, (0, 0), ("swap",), fill
    if isinstance(intent, OpenLiquidity):
        payload, deltas = minted(
            liquidity_operation(intent, observed), observed, payload, pending.identity
        )
        return payload, deltas, (0, 0), ("mint",), None
    if isinstance(intent, (CollectLiquidityFees, CloseLiquidity)):
        payload, deltas, fees = liquidity_exit(intent, observed, payload)
        return (
            payload,
            deltas,
            fees,
            ("burn" if isinstance(intent, CloseLiquidity) else "collect",),
            None,
        )
    if isinstance(intent, RevokeExchange):
        if observed.owned.position is not None:
            raise ValueError("economic cleanup requires position closure")
        return payload, (0, 0), (0, 0), (), None
    raise ValueError("unsupported economic exchange intent")


def economic_cost(costs, intent):
    if costs.kind == "excluded":
        return 0
    operation = {
        "swap_exact_input": "swap",
        "open_liquidity": "mint",
        "collect_liquidity_fees": "collect",
        "close_liquidity": "burn",
        "revoke_exchange": "revoke",
    }[intent.kind]
    return getattr(costs, operation)
