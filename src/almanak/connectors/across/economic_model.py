"""The bridge model: the author's whole fee ceiling is charged and arrival takes a stated time.

A modeled bridge debits the source wallet when its deposit executes. The value is then
in flight: in no balance, and never spendable. The destination wallet is credited the
amount sent less the fee ceiling at the deposit's second plus the stated latency. The
model reads no history and asks no venue, so every amount it produces is modeled, and
it fills every transfer: no refund is modeled.
"""

import hashlib
import json
from datetime import UTC, datetime
from uuid import uuid4

from almanak.backtest.engine import Transition
from almanak.backtest.types import UnixTime
from almanak.bridge_intents import BridgeIntent, arrival_wallet
from almanak.connectors.across.authority import admit
from almanak.connectors.across.authority import validate as validate_capabilities
from almanak.connectors.across.economic_state import (
    ActionResult,
    ArrivalEvent,
    DecisionInput,
    Flight,
    ModeledArrival,
    OutcomeInput,
    Position,
    Snapshot,
    State,
)
from almanak.connectors.across.models import Parameters
from almanak.connectors.across.observations import Ownership
from almanak.connectors.across.quote import fee_ceiling
from almanak.connectors.across.settlement_types import CONNECTOR
from almanak.connectors.across.state import Decision
from almanak.connectors.across.support import BACKTEST_MODEL
from almanak.effects import (
    AssetDelta,
    Balance,
    Effects,
    Portfolio,
    PositionChange,
    PositionKey,
    WalletAsset,
)
from almanak.effects import Position as HeldPosition
from almanak.models import AccountBinding, AssetAmount, StrategyAccounts
from almanak.strategy_api import Memory, SimulationEvidence
from almanak.transfers import Arrived, Transfer
from almanak.work import ChainAvailability

MODEL = BACKTEST_MODEL
ASSUMPTIONS = (
    "full fee ceiling charged: every bridge pays the market's whole max_fee_bps, a model "
    "assumption, not a measured fee; the venue was not asked and no historical quote is read",
    "the fee ceiling stands in for every relayer cost; no destination gas is charged",
    "arrival is credited at the deposit's second plus the stated arrival latency, a model "
    "assumption, not a measured fill time",
    "every transfer arrives: no refund, no unfilled transfer and no slow fill is modeled",
    "a transfer in flight is in neither wallet's balance and states no received amount",
    "the modeled fill deadline of a transfer is its modeled arrival time",
    "approvals, transaction inclusion and fee-market mechanics are excluded; the model keeps "
    "no approval, so it never produces a revoke",
    "hypothetical deposits do not change later venue liquidity, fees or fill times",
)


def parameters(author, settings, interval):
    return Parameters(
        decision_trigger=interval.effective,
        asset_metadata=author.asset_metadata,
        parameters=author.parameters,
        allocations=author.allocations,
        accounts=StrategyAccounts(
            bindings=tuple(AccountBinding(name=f.name, profile=f.profile) for f in settings.funds)
        ),
    )


def routes(config):
    return tuple(config.target(a.name) for a in config.positions)


def identity(config, settings):
    value = {
        "model": MODEL,
        "routes": [t.model_dump(mode="json") for t in routes(config)],
        "data": settings.data.model_dump(mode="json"),
        "bridge_model": settings.bridge_model,
        "start": int(settings.schedule.start),
        "end": int(settings.schedule.deadline),
    }
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def position_key(target):
    return PositionKey(
        connector=CONNECTOR,
        chain=target.fund.chain,
        account=target.account,
        reference=f"{target.asset}>{target.destination_asset}",
    )


def wallet(target, asset=None):
    return WalletAsset(
        account=target.account,
        chain=target.fund.chain,
        asset=target.asset if asset is None else asset,
    )


def payer_wallet(settings, target, asset):
    profile = next(f.profile for f in settings.funds if f.name == target.account)
    if profile.submitter == profile.fund:
        return wallet(target, asset)
    payer = settings.payer(target.account)
    return WalletAsset(account=payer.name, chain=payer.fund.chain, asset=asset)


def initial(settings, config, interval):
    balances = {b.key: b for f in (*settings.funds, *settings.payers) for b in f.inventory()}
    positions = []
    for allocation in config.positions:
        target = config.target(allocation.name)
        for key in (wallet(target), arrival_wallet(target)):
            balances.setdefault(key, Balance(key=key, amount=0))
        if payer_wallet(settings, target, allocation.native_asset) not in balances:
            raise ValueError("a modeled bridge requires explicit payer inventory")
        if balances[wallet(target)].amount < allocation.capital:
            raise ValueError("initial inventory cannot fund the bridge allocation's capital")
        positions.append(
            HeldPosition[Position](key=position_key(target), payload=Position(target=target))
        )
    return State(
        run_id=uuid4().hex,
        time=settings.schedule.start,
        schedule=settings.schedule,
        interval=interval,
        portfolio=Portfolio[Position](
            balances=tuple(balances.values()), positions=tuple(positions)
        ),
    )


def instant(time) -> datetime:
    return datetime.fromtimestamp(time, UTC)


def evidence(identity):
    return SimulationEvidence(execution_id=identity, model=MODEL, inputs=())


def modeled_transfer(target, amount: int, max_fee_bps: int, reference: str, arrives_at) -> Transfer:
    """The transfer a modeled deposit opens. Its offer is the amount less the whole ceiling."""
    received = amount - fee_ceiling(amount, max_fee_bps)
    if received <= 0:
        raise ValueError("the fee ceiling leaves nothing to receive from this bridge")
    return Transfer(
        connector=CONNECTOR,
        allocation=target.allocation,
        source=wallet(target),
        sent=AssetAmount(asset=target.asset, amount=amount),
        destination=arrival_wallet(target),
        offered=AssetAmount(asset=target.destination_asset, amount=received),
        reference=reference,
        fill_deadline=instant(arrives_at),
    )


def arrival(target, flight: Flight, identity: str) -> ModeledArrival:
    transfer = flight.transfer
    return ModeledArrival(
        target=target,
        transfer=transfer.reference,
        received=transfer.offered,
        offered=transfer.offered,
        sent=transfer.sent,
        modeled_fee=AssetAmount(
            asset=transfer.sent.asset, amount=transfer.sent.amount - transfer.offered.amount
        ),
        effective_at=instant(flight.arrives_at),
        simulation=evidence(identity),
    )


def landed(position: Position, due: tuple[Flight, ...]) -> Position:
    """The position with each due transfer resolved by its modeled amount."""
    references = {f.transfer.reference for f in due}
    flights = tuple(
        f.model_copy(
            update={
                "transfer": Transfer(
                    **{**dict(f.transfer), "resolution": Arrived(received=f.transfer.offered)}
                )
            }
        )
        if f.transfer.reference in references
        else f
        for f in position.flights
    )
    return Position(target=position.target, flights=flights)


class EconomicModel:
    def __init__(self, config, settings, strategy, book, *, authority=None):
        from almanak.connectors.composition import connector_for
        from almanak.connectors.support import historical_capability
        from almanak.runtime import strategy_capabilities

        self.config, self.settings, self.strategy, self.book = config, settings, strategy, book
        connector = connector_for(config)
        self.capabilities = tuple(
            historical_capability(connector.support_for(chain.chain_id), chain, CONNECTOR, MODEL)
            for chain in dict.fromkeys(a.chain for a in config.positions)
        )
        self.authority = strategy_capabilities(strategy, config) if authority is None else authority
        validate_capabilities(config, self.authority)

    @property
    def latency(self) -> int:
        return self.settings.data.arrival_latency_seconds

    def route(self, intent):
        if self.config.target(intent.target.allocation) != intent.target:
            raise ValueError("modeled bridge intent differs from its configured route")
        admit(self.authority, intent)
        return next(
            c for c in self.capabilities if c.chain == intent.target.fund.chain
        ), intent.kind

    def eligible_at(self, intent, time, latency):
        return UnixTime(time + max(1, latency))

    def cancel_at_end(self, intent):
        return isinstance(intent, BridgeIntent)

    def validate_state(self, state):
        if (
            state.schedule != self.settings.schedule
            or state.interval.effective != self.config.decision_trigger
        ):
            raise ValueError("modeled bridge state differs from its frozen schedule")
        expected = tuple(routes(self.config))
        if tuple(p.payload.target for p in state.portfolio.positions) != expected:
            raise ValueError("modeled bridge ownership differs from its configured routes")
        if any(p.key != position_key(p.payload.target) for p in state.portfolio.positions):
            raise ValueError("modeled bridge position key differs from its fund and route")

    def observe(self, state):
        self.validate_state(state)
        return tuple(self.snapshot(state, p.payload) for p in state.portfolio.positions)

    def snapshot(self, state, position):
        target = position.target
        allocation = self.config.allocation(target.allocation)
        native = payer_wallet(self.settings, target, allocation.native_asset)
        return Snapshot(
            target=target,
            wallet_balance=state.portfolio.balance(wallet(target)),
            native_balance=AssetAmount(
                asset=allocation.native_asset, amount=state.portfolio.balance(native)
            ),
            payer=self.config.accounts.binding(target.account).profile.submitter,
            effective_at=instant(state.time),
        )

    def owned(self, state):
        return tuple(
            Ownership(
                target=p.payload.target,
                inventory=AssetAmount(
                    asset=p.payload.target.asset,
                    amount=state.portfolio.balance(wallet(p.payload.target)),
                ),
                in_flight=p.payload.owned.in_flight,
            )
            for p in state.portfolio.positions
        )

    def decide(self, state, observations, trigger):
        from almanak.runtime import strategy_worker
        from almanak.strategy import EconomicDecisionRequest

        context = DecisionInput(
            config=self.config,
            memory=state.memory,
            observations=observations,
            owned=self.owned(state),
            availability=tuple(
                ChainAvailability(chain=chain, status="ready")
                for chain in dict.fromkeys(o.target.fund.chain for o in observations)
            ),
            trigger=trigger,
        )
        return Decision.model_validate_json(
            strategy_worker(EconomicDecisionRequest(strategy=self.strategy, context=context))
        )

    def execute(self, state, pending):
        intent = pending.intent
        try:
            return self.apply(state, pending)
        except ValueError as error:
            return Transition(
                effects=Effects[Position](),
                result=ActionResult(
                    intent=intent,
                    success=False,
                    sent=AssetAmount(asset=intent.target.asset, amount=0),
                    effective_at=instant(pending.eligible_at),
                    simulation=evidence(pending.identity),
                    error=str(error),
                ),
            )

    def apply(self, state, pending):
        intent = pending.intent
        self.route(intent)
        if not isinstance(intent, BridgeIntent):
            raise ValueError("the bridge model keeps no approval; there is nothing to revoke")
        self.validate_entry(state, intent)
        target, amount = intent.target, intent.amount.amount
        if state.portfolio.balance(wallet(target)) < amount:
            raise ValueError("the source wallet does not hold the amount to bridge")
        held = next(p for p in state.portfolio.positions if p.payload.target == target)
        arrives_at = UnixTime(pending.eligible_at + self.latency)
        transfer = modeled_transfer(
            target, amount, target.route.max_fee_bps, f"modeled:{pending.identity}", arrives_at
        )
        flight = Flight(transfer=transfer, sent_at=pending.eligible_at, arrives_at=arrives_at)
        after = Position(target=target, flights=(*held.payload.flights, flight))
        effects = Effects[Position](
            assets=(AssetDelta(key=wallet(target), amount=-amount), *self.costs(state, intent)),
            positions=(
                PositionChange[Position](
                    before=held, after=HeldPosition[Position](key=held.key, payload=after)
                ),
            ),
        )
        return Transition(
            effects=effects,
            result=ActionResult(
                intent=intent,
                success=True,
                sent=intent.amount,
                transfer=transfer,
                effective_at=instant(pending.eligible_at),
                simulation=evidence(pending.identity),
            ),
        )

    def validate_entry(self, state, intent):
        if state.mode != "running":
            raise ValueError("a new bridge requires running mode; teardown sends nothing")
        allocation = self.config.allocation(intent.target.allocation)
        native = payer_wallet(self.settings, intent.target, allocation.native_asset)
        if state.portfolio.balance(native) < allocation.minimum_native_balance:
            raise ValueError("the modeled bridge payer is below the configured native minimum")

    def costs(self, state, intent):
        if self.settings.costs.kind == "excluded":
            return ()
        amount = self.settings.costs.bridge
        native = self.config.allocation(intent.target.allocation).native_asset
        key = payer_wallet(self.settings, intent.target, native)
        if state.portfolio.balance(key) < amount:
            raise ValueError("insufficient explicit payer inventory for the modeled bridge")
        return (AssetDelta(key=key, amount=-amount),)

    def deliver(self, state, result):
        from almanak.runtime import strategy_worker
        from almanak.strategy import EconomicOutcomeRequest

        request = EconomicOutcomeRequest(
            strategy=self.strategy,
            context=OutcomeInput(config=self.config, memory=state.memory, result=result),
        )
        return Memory.model_validate_json(strategy_worker(request))

    def next_event(self, state, until):
        """The second of the earliest modeled arrival not later than `until`."""
        due = [f.arrives_at for p in state.portfolio.positions for f in p.payload.in_flight]
        if not due or min(due) > until:
            return None
        return UnixTime(min(due))

    def external_event(self, state, time):
        changes, arrivals, credits = [], [], []
        for held in state.portfolio.positions:
            due = tuple(f for f in held.payload.in_flight if f.arrives_at <= time)
            if not due:
                continue
            if any(f.arrives_at != time for f in due):
                raise ValueError("a modeled arrival was not credited at its stated second")
            after = landed(held.payload, due)
            changes.append(
                PositionChange[Position](
                    before=held, after=HeldPosition[Position](key=held.key, payload=after)
                )
            )
            for flight in due:
                identity = f"{state.run_id}:arrival:{flight.transfer.reference}"
                arrivals.append(arrival(held.payload.target, flight, identity))
                credits.append(
                    AssetDelta(
                        key=flight.transfer.destination, amount=flight.transfer.offered.amount
                    )
                )
        if not arrivals:
            return None
        return Transition(
            effects=Effects[Position](assets=tuple(credits), positions=tuple(changes)),
            result=ArrivalEvent(effective_at=instant(time), arrivals=tuple(arrivals)),
        )

    def deliver_event(self, state, result):
        """Each arrival reaches `on_outcome`, as a measured one does in a fork or live run."""
        memory = state.memory
        for item in result.arrivals:
            memory = self.deliver(state.model_copy(update={"memory": memory}), item)
        return memory

    def closed(self, state, observations):
        # Teardown waits for every transfer to arrive; it does not bridge back.
        return len(observations) == len(state.portfolio.positions) and not any(
            p.payload.in_flight for p in state.portfolio.positions
        )


EconomicDriver = EconomicModel
