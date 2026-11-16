"""Drive a HyperCore perp strategy through the shared economic simulator (section 12).

The venue's history is trade candles and hourly funding rates only (D4), so the model
states how it stands in for the rest:

- **Fills.** An `Ioc` order is priced like the live runtime prices it, from a reference
  and the author's slippage (`trading.limit_price`). The reference is the close of the
  latest candle available at the order's time, in the book's place. The order fills
  whole at that close moved against it by the fill model's slippage, or not at all when
  that price is past its limit, as an `Ioc` order that reaches no level. It pays the
  taker fee. An entry needs the configured cross leverage, an order value of at least
  10 USDC at its limit price (the venue's `MinTradeNtl`), and initial margin on the
  account the fill leaves, valued at the reference, so its fee and slippage count; a
  position above the market's first margin tier or 100,000 USDC of notional is
  refused, since HyperCore liquidates those in steps the model does not simulate.
- **Funding.** At each hourly rate, an open position is paid `-size x price x rate`,
  with the latest trade close standing in for the oracle price.
- **Liquidation.** Each candle that opens after the position last changed is a mark
  proxy: when its low (long) or high (short) crosses the price at which account value
  meets maintenance margin, the position closes there, or at the candle's open when it
  opened past it, paying the taker fee. It is applied when the candle closes.
- **Custody.** A deposit moves USDC from the fund on HyperEVM to perp; the account's
  first deposit pays the 1 USDC activation fee (assumption A7, confirmed by the
  canary). A withdrawal returns at most the withdrawable USDC. HyperEVM gas and the
  HYPE fee of a withdrawal are costs, which phase 5 excludes.

A refusal is the venue's or the gateway's answer: it leaves the account unchanged and
is delivered to the strategy as an unsuccessful result.
"""

from datetime import UTC, datetime
from decimal import ROUND_DOWN, ROUND_UP, Decimal

from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.engine import Transition
from almanak.backtest.types import UnixTime
from almanak.connectors.hyperliquid import economic_state as states
from almanak.connectors.hyperliquid import perp_math as math
from almanak.connectors.hyperliquid.assets import USDC
from almanak.connectors.hyperliquid.authority import admit, validate
from almanak.connectors.hyperliquid.economic_data import CONNECTOR, MODEL
from almanak.connectors.hyperliquid.economic_state import (
    FundingPayment,
    HyperCoreAccount,
    HyperCoreEvents,
    HyperCoreResult,
    Liquidation,
    OrderFill,
    Reference,
    Snapshot,
)
from almanak.connectors.hyperliquid.models import (
    MAXIMUM_NOTIONAL,
    MINIMUM_ORDER_VALUE,
    ClosePosition,
    Deposit,
    IncreasePosition,
    SetLeverage,
    Withdraw,
)
from almanak.connectors.hyperliquid.trading import limit_price
from almanak.effects import AssetDelta, Effects, Position, PositionChange, PositionKey, WalletAsset
from almanak.models import AssetAmount
from almanak.runtime import strategy_capabilities, strategy_worker
from almanak.strategy import EconomicDecisionRequest, EconomicOutcomeRequest
from almanak.strategy_api import Memory, SimulationEvidence
from almanak.work import ChainAvailability, ChainFault

ACTIVATION_FEE = 1_000_000
NO_MATCH = "Order could not immediately match against any resting orders."
NEW_RISK = ("deposit", "set_leverage", "increase_position")
Effect = Effects[HyperCoreAccount]


class Refused(Exception):
    """The venue's or the gateway's refusal of one action."""


def position_key(target) -> PositionKey:
    return PositionKey(
        connector=CONNECTOR,
        chain=target.fund.chain,
        account=target.account,
        reference=f"allocation:{target.allocation}:{target.market.coin}",
    )


def wallet(target) -> WalletAsset:
    return WalletAsset(account=target.account, chain=target.fund.chain, asset=USDC)


def instant(time) -> datetime:
    return datetime.fromtimestamp(int(time), UTC)


class EconomicModel:
    model_name = MODEL

    def __init__(self, config, settings, strategy, book, *, authority=None):
        from almanak.connectors.composition import connector_for
        from almanak.connectors.support import historical_capability

        self.config, self.settings, self.strategy, self.book = config, settings, strategy, book
        connector = connector_for(config)
        self.capabilities = tuple(
            historical_capability(connector.support_for(chain.chain_id), chain, CONNECTOR, MODEL)
            for chain in dict.fromkeys(a.chain for a in config.allocations)
        )
        self.fills = settings.data.fills
        self.authority = strategy_capabilities(strategy, config) if authority is None else authority
        validate(config, self.authority)

    # The simulator's driver.

    def validate_state(self, state):
        if (
            state.schedule != self.settings.schedule
            or state.interval.effective != self.config.decision_trigger
        ):
            raise ValueError("HyperCore economic state differs from its frozen scheduling")
        expected = tuple(self.config.target(a.name) for a in self.config.allocations)
        if tuple(held.payload.target for held in state.portfolio.positions) != expected:
            raise ValueError("HyperCore economic state changed its allocation targets")
        if any(held.key != position_key(held.payload.target) for held in state.portfolio.positions):
            raise ValueError("HyperCore economic state changed its position identity")

    def observe(self, state):
        """A snapshot of every account whose candles are available now."""
        self.validate_state(state)
        observed = []
        for held in state.portfolio.positions:
            try:
                observed.append(self.snapshot(state, held.payload, state.time))
            except InputUnavailable:
                continue
        return tuple(observed)

    def snapshot(self, state, account, time) -> Snapshot:
        target = account.target
        record = self.book.candle(target, time)
        price = Decimal(record.payload.close)
        return Snapshot(
            allocation=target.allocation,
            target=target,
            effective_at=instant(time),
            wallet=AssetAmount(asset=USDC, amount=state.portfolio.balance(wallet(target))),
            owned=account,
            reference=Reference(
                price=record.payload.close,
                candle_open=record.source_time,
                available_at=record.available_at,
            ),
            account_value=math.micro(math.account_value(account, price)),
            unrealized_pnl=math.micro(math.unrealized(account, price)),
            initial_margin=math.micro(math.initial_margin(account, price)),
            maintenance_margin=math.micro(math.maintenance_margin(account, price)),
            withdrawable=withdrawable(account, price),
        )

    def availability(self, state, observations):
        """Each chain's readiness: unavailable while a target has no candle."""
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
                        message="Missing prepared HyperCore candles: " + ", ".join(missing),
                    )
                    if missing
                    else None,
                )
            )
        return tuple(result)

    def decide(self, state, observations, trigger):
        """The strategy's decision, evaluated in the sandboxed worker."""
        context = states.DecisionInput(
            config=self.config,
            memory=state.memory,
            trigger=trigger,
            owned=tuple(p.payload for p in state.portfolio.positions),
            observations=observations,
            availability=self.availability(state, observations),
        )
        return states.Decision.model_validate_json(
            strategy_worker(EconomicDecisionRequest(strategy=self.strategy, context=context))
        )

    def route(self, intent):
        """The intent's historical capability and action, after admission."""
        if intent.target != self.config.target(intent.target.allocation):
            raise ValueError("HyperCore intent names another configured target")
        admit(self.authority, intent)
        capability = next(c for c in self.capabilities if c.chain == intent.target.fund.chain)
        return capability, intent.kind

    @staticmethod
    def eligible_at(intent, time, latency):
        return UnixTime(time + max(1, latency))

    @staticmethod
    def cancel_at_end(intent):
        return intent.kind in NEW_RISK

    def execute(self, state, pending):
        """Apply one eligible intent, or record the venue's refusal with no effect."""
        self.validate_state(state)
        self.route(pending.intent)
        held = self.held(state, pending.intent.target)
        time = int(pending.eligible_at)
        common = dict(
            intent=pending.intent,
            effective_at=instant(time),
            before=held.payload,
            simulation=self.evidence(pending, held.payload.target, time),
        )
        try:
            after, delta, fill = self.apply(state, pending.intent, held.payload, time)
        except Refused as refusal:
            result = HyperCoreResult(
                **common, success=False, after=held.payload, error=str(refusal)
            )
            return Transition(effects=Effect(), result=result)
        except InputUnavailable as error:
            result = HyperCoreResult(
                **common, success=False, after=held.payload, error=str(error), unavailable=error.gap
            )
            return Transition(effects=Effect(), result=result)
        result = HyperCoreResult(**common, success=True, after=after, fill=fill)
        assets = (AssetDelta(key=wallet(after.target), amount=delta),) if delta else ()
        return Transition(
            effects=Effect(assets=assets, positions=(changed(held, after),)), result=result
        )

    def deliver(self, state, result):
        return Memory.model_validate_json(
            strategy_worker(
                EconomicOutcomeRequest(
                    strategy=self.strategy,
                    context=states.OutcomeInput(
                        config=self.config, memory=state.memory, result=result
                    ),
                )
            )
        )

    def closed(self, state, observations):
        """Every account is flat with no USDC left on HyperCore."""
        return all(
            held.payload.holding is None and held.payload.perp_usdc == 0
            for held in state.portfolio.positions
        )

    # Intents.

    def held(self, state, target):
        key = position_key(target)
        return next(p for p in state.portfolio.positions if p.key == key)

    def apply(self, state, intent, account, time):
        """The account after the intent, its HyperEVM USDC delta and any fill."""
        if isinstance(intent, Deposit):
            return deposit(account, intent.amount, state.portfolio.balance(wallet(account.target)))
        if isinstance(intent, SetLeverage):
            return self.set_leverage(account, intent.leverage)
        if isinstance(intent, Withdraw):
            return self.withdraw(account, intent.amount, time)
        if isinstance(intent, IncreasePosition):
            return self.increase(account, intent, time)
        if isinstance(intent, ClosePosition):
            return self.close(account, time)
        raise ValueError("unsupported HyperCore intent")

    def set_leverage(self, account, leverage):
        """Set the configured cross leverage on an activated, flat account."""
        allocation = self.config.allocation(account.target.allocation)
        if not account.activated:
            raise Refused("HyperCore holds no account for the fund before its first deposit")
        if leverage != allocation.leverage:
            raise Refused("leverage must equal the configured cross leverage")
        if account.holding is not None:
            raise Refused("leverage changes only while the account holds no position")
        return math.validated(account.model_copy(update={"leverage": leverage})), 0, None

    def withdraw(self, account, amount, time):
        """Move at most the withdrawable USDC back to the fund on HyperEVM."""
        if not account.activated:
            raise Refused("HyperCore holds no account for the fund")
        price = Decimal(self.book.candle(account.target, time).payload.close)
        available = withdrawable(account, price)
        amount = available if amount is None else amount
        if amount == 0 or amount > available:
            raise Refused(f"withdrawal exceeds the {available} withdrawable USDC base units")
        after = account.model_copy(
            update={
                "perp_usdc": account.perp_usdc - amount,
                "withdrawn": account.withdrawn + amount,
            }
        )
        return math.validated(after), amount, None

    def increase(self, account, intent, time):
        """Fill an `Ioc` entry on its own side under leverage, margin and notional limits."""
        allocation = self.config.allocation(account.target.allocation)
        if not account.activated:
            raise Refused("HyperCore holds no account for the fund before its first deposit")
        if account.leverage != allocation.leverage:
            raise Refused("an entry needs the configured cross leverage set first")
        is_buy = intent.side == "long"
        held = math.szi(account)
        if held != 0 and (held > 0) != is_buy:
            raise Refused("an entry adds on its own side; close the position first")
        market = account.target.market
        try:
            size = math.order_size(Decimal(intent.size), market)
        except ValueError as error:
            # Live planning refuses an order whose size rounds away (`trading.order_size`).
            raise Refused(str(error)) from error
        reference, limit, price = self.priced(account, is_buy, time)
        # The order's value at its own limit price, as the venue reads the order.
        if size * limit < MINIMUM_ORDER_VALUE:
            raise Refused(f"Order must have minimum value of $10. asset={market.asset}")
        after_notional = (abs(held) + size) * reference
        if after_notional >= MAXIMUM_NOTIONAL or (
            market.tier_bound is not None and after_notional >= Decimal(market.tier_bound)
        ):
            raise Refused("the position would exceed what the model liquidates in one step")
        fee = math.fee(size, price, Decimal(self.fills.taker_fee_bps))
        after, pnl = math.trade(account, size if is_buy else -size, price, fee, time)
        # Initial margin is checked on the account the fill leaves, valued at the reference:
        # its fee and the fill's slippage against the reference are already lost.
        if math.account_value(after, reference) < math.initial_margin(after, reference):
            raise Refused(f"Insufficient margin to place order. asset={market.asset}")
        side = "buy" if is_buy else "sell"
        return after, 0, order_fill(side, size, price, reference, limit, fee, pnl)

    def close(self, account, time):
        """Close the whole position with a reduce-only `Ioc` order."""
        held = math.szi(account)
        if held == 0:
            raise Refused("the account holds no position to close")
        is_buy = held < 0
        reference, limit, price = self.priced(account, is_buy, time)
        fee = math.fee(held, price, Decimal(self.fills.taker_fee_bps))
        after, pnl = math.trade(account, -held, price, fee, time)
        side = "buy" if is_buy else "sell"
        return after, 0, order_fill(side, abs(held), price, reference, limit, fee, pnl)

    def priced(self, account, is_buy, time):
        """The reference trade close, the order's `Ioc` limit and its modeled fill price."""
        allocation = self.config.allocation(account.target.allocation)
        market = account.target.market
        reference = Decimal(self.book.candle(account.target, time).payload.close)
        limit = limit_price(reference, is_buy, allocation.slippage_bps, market.size_decimals)
        price = math.moved(
            reference, is_buy, Decimal(self.fills.slippage_bps), market.size_decimals
        )
        if price > limit if is_buy else price < limit:
            raise Refused(NO_MATCH)
        return reference, limit, price

    def evidence(self, pending, target, time):
        """The prepared answers the decision's and the execution's candles came from."""
        inputs = set()
        for at in (int(pending.decision_time), time):
            try:
                inputs.update(self.book.candle(target, at).evidence)
            except InputUnavailable:
                continue
        return SimulationEvidence(
            execution_id=pending.identity, model=MODEL, inputs=tuple(sorted(inputs))
        )

    # External events: funding and liquidation.

    def next_event(self, state, until):
        start = (
            state.time if state.event_cursor is None else max(state.time, state.event_cursor + 1)
        )
        if start > until:
            return None
        times = [
            time
            for held in state.portfolio.positions
            if held.payload.holding is not None
            and (time := self.next_for(held.payload, start, int(until))) is not None
        ]
        return UnixTime(min(times)) if times else None

    def next_for(self, account, start, until):
        """The first funding rate or liquidating candle in `[start, until]`. Candles are
        scanned only up to the next funding rate, which changes the account's cash."""
        target = account.target
        funding = self.book.funding_between(target, start - 1, until)
        horizon = int(funding[0].available_at) if funding else until
        for row in self.book.candles_between(target, start - 1, horizon):
            if liquidating(account, row) is not None:
                return int(row.available_at)
        return horizon if funding else None

    def external_event(self, state, time):
        """Pay funding and apply liquidations that fall at `time`, or None."""
        changes, results = [], []
        for held in state.portfolio.positions:
            account = held.payload
            if account.holding is None:
                continue
            for row in self.book.funding_between(account.target, time - 1, time):
                account, payment = self.pay_funding(state, account, row, time)
                results.append(payment)
            for row in self.book.candles_between(account.target, time - 1, time):
                if (trigger := liquidating(account, row)) is not None:
                    account, liquidation = self.liquidate(state, account, row, trigger, time)
                    results.append(liquidation)
            if account != held.payload:
                changes.append(changed(held, account))
        if not results:
            return None
        return Transition(
            effects=Effect(positions=tuple(changes)), result=HyperCoreEvents(results=tuple(results))
        )

    def pay_funding(self, state, account, row, time):
        size = math.szi(account)
        price = Decimal(self.book.candle(account.target, time).payload.close)
        payment = math.micro(-size * price * Decimal(row.payload.rate))
        after = math.validated(
            account.model_copy(
                update={
                    "perp_usdc": account.perp_usdc + payment,
                    "funding": account.funding + payment,
                }
            )
        )
        return after, FundingPayment(
            allocation=account.target.allocation,
            effective_at=instant(time),
            rate=row.payload.rate,
            price=math.text(price),
            szi=account.holding.szi,
            payment=payment,
            simulation=self.event_evidence(state, row.evidence),
        )

    def liquidate(self, state, account, row, trigger, time):
        """Close the position at the trigger, or the candle's open past it."""
        held = math.szi(account)
        opened = Decimal(row.payload.open)
        market = account.target.market
        if held > 0:
            price = math.on_grid(min(trigger, opened), market.size_decimals, ROUND_DOWN)
        else:
            price = math.on_grid(max(trigger, opened), market.size_decimals, ROUND_UP)
        fee = math.fee(held, price, Decimal(self.fills.taker_fee_bps))
        closed, pnl = math.trade(account, -held, price, fee, time)
        after = math.validated(closed.model_copy(update={"liquidations": account.liquidations + 1}))
        side = "buy" if held < 0 else "sell"
        return after, Liquidation(
            allocation=account.target.allocation,
            effective_at=instant(time),
            trigger=math.text(trigger),
            fill=order_fill(side, abs(held), price, Decimal(row.payload.close), None, fee, pnl),
            before=account,
            after=after,
            simulation=self.event_evidence(state, row.evidence),
        )

    def event_evidence(self, state, inputs):
        return SimulationEvidence(
            execution_id=f"{state.run_id}:event:{len(state.events)}",
            model=MODEL,
            inputs=tuple(inputs),
        )

    @staticmethod
    def deliver_event(state, result):
        # Funding and liquidation are accounting events; the strategy sees their effects
        # in its next observation.
        return state.memory


def liquidating(account, row) -> Decimal | None:
    """The liquidation price `row`'s range crosses, or None when it does not."""
    holding = account.holding
    if holding is None or row.source_time < holding.changed_at:
        return None
    trigger = math.liquidation_price(account)
    if trigger is None:
        return None
    if Decimal(holding.szi) > 0:
        return trigger if Decimal(row.payload.low) < trigger else None
    return trigger if Decimal(row.payload.high) > trigger else None


def deposit(account, amount, available):
    if amount > available:
        raise Refused("the fund holds less USDC on HyperEVM than the deposit")
    fee = 0 if account.activated else ACTIVATION_FEE
    if amount <= fee:
        raise Refused("a first deposit must exceed the 1 USDC activation fee")
    after = account.model_copy(
        update={
            "activated": True,
            "perp_usdc": account.perp_usdc + amount - fee,
            "deposited": account.deposited + amount,
            "activation_fee": account.activation_fee + fee,
        }
    )
    return math.validated(after), -amount, None


def withdrawable(account, price) -> int:
    value = math.withdrawable(account, price)
    return int((value / math.USDC).to_integral_value(ROUND_DOWN))


def order_fill(side, size, price, reference, limit, fee, pnl) -> OrderFill:
    return OrderFill(
        side=side,
        size=math.text(size),
        price=math.text(price),
        reference=math.text(reference),
        limit=None if limit is None else math.text(limit),
        fee=fee,
        closed_pnl=pnl,
    )


def changed(held, after) -> PositionChange[HyperCoreAccount]:
    return PositionChange[HyperCoreAccount](
        before=held, after=Position[HyperCoreAccount](key=held.key, payload=after)
    )
