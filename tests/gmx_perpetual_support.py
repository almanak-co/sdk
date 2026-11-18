"""Synthetic perpetual settlement inputs and a scripted driver for offline lifecycle checks.

Nothing here is captured chain evidence. Prices, funding indexes and borrowing
accumulators are invented so that the connector's own validators accept them.
They prove wiring, deployed ordering and durable-state behavior offline; they
establish no historical coverage, Reader comparison or public support.
"""

import json
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

from almanak.backtest.dataset import Timeline
from almanak.backtest.engine import Journal, Pending, Simulator
from almanak.backtest.types import UnixTime
from almanak.connectors.gmx_v2 import economic_data as data
from almanak.connectors.gmx_v2.authority import declared
from almanak.connectors.gmx_v2.economic_config import EconomicConfig
from almanak.connectors.gmx_v2.economic_parameters import initial
from almanak.connectors.gmx_v2.economic_state import State
from almanak.connectors.gmx_v2.intents import (
    ClosePosition,
    DecreasePosition,
    IncreasePosition,
    RevokePosition,
)
from almanak.connectors.gmx_v2.models import Parameters
from almanak.connectors.gmx_v2.state import Decision
from almanak.models import (
    Account,
    AccountBinding,
    AssetAmount,
    SimulationAccount,
    StrategyAccounts,
)
from almanak.triggers import IntervalEvent, IntervalSelection, TeardownEvent
from tests import gmx_synthetic_frames as synthetic

PAYER = "0x" + "78" * 20
# Allocation name, author account name and hypothetical fund address per allocation.
ACCOUNTS = (("Main", "fund", synthetic.FUND), ("Second", "second", "0x" + "79" * 20))
START = synthetic.TIMESTAMP
NETWORKS = {"eip155:42161": "arb-mainnet", "eip155:43114": "avax-mainnet"}
# Synthetic index prices that breach a 5x long's leverage bound, wipe it out entirely,
# and the mirror moves for a 5x short. Collateral is the index token, so it devalues
# with the position: a long opened near 2000 with 0.0997 collateral and 0.49975 size
# tokens breaks even near 1674 and its 1% leverage bound near 1691. Every value is
# invented.
CRASH = (1_685 * 10**12, 1_686 * 10**12)
INSOLVENT = (1_620 * 10**12, 1_621 * 10**12)
PUMP = (2_480 * 10**12, 2_481 * 10**12)
PUMP_INSOLVENT = (2_600 * 10**12, 2_601 * 10**12)
PROFIT_FOR_SHORT = (1_900 * 10**12, 1_901 * 10**12)
COLLATERAL = 10**17
SIZE = 10**33


def liquidation_prices(side, *, insolvent=False):
    if side == "long":
        return INSOLVENT if insolvent else CRASH
    return PUMP_INSOLVENT if insolvent else PUMP


def parameters(name="arbitrum", *, side="long", allocations=1, **overrides):
    author = synthetic.author(name, "position")
    reference = author.allocations[0]
    # The reference prices are long-oriented; a short accepts any entry and a bounded exit.
    bounds = (
        {}
        if side == "long"
        else {"increase_acceptable_price": 0, "decrease_acceptable_price": 10**18}
    )
    chain = reference.market.chain
    selected, bindings = [], []
    for label, account, address in ACCOUNTS[:allocations]:
        selected.append(
            reference.model_copy(
                update={
                    "name": label,
                    "account": account,
                    "side": side,
                    **bounds,
                    **overrides,
                }
            )
        )
        profile = SimulationAccount(
            fund=Account(chain=chain, address=address),
            payer=Account(chain=chain, address=PAYER),
        )
        bindings.append(AccountBinding(name=account, profile=profile))
    return Parameters(
        decision_trigger=author.decision_trigger,
        asset_metadata=author.asset_metadata,
        allocations=tuple(selected),
        accounts=StrategyAccounts(bindings=tuple(bindings)),
    )


def amount(asset, value):
    return {"asset": str(asset), "amount": value}


def fund_entry(config, allocation):
    target = config.target(allocation.name)
    market = target.market
    other = next(
        asset
        for asset in (market.long_asset, market.short_asset)
        if asset != allocation.capital.asset
    )
    return {
        "name": allocation.account,
        "profile": {
            "kind": "simulation",
            "fund": target.fund.model_dump(mode="json"),
            "payer": {"chain": market.chain.model_dump(mode="json"), "address": PAYER},
        },
        "balances": [
            amount(allocation.capital.asset, allocation.capital.amount),
            amount(other, 0),
            amount(allocation.native_asset, 0),
            amount(market.share_asset, 0),
        ],
    }


def settings(config, *, start=START, end=START + 60, cleanup=60, costs="fixed_native"):
    allocation = config.allocations[0]
    market = allocation.market
    chain = market.chain.model_dump(mode="json")
    chain_id = str(market.chain.chain_id)
    native = allocation.native_asset
    if costs == "excluded":
        cost = {"kind": "excluded"}
    else:
        cost = {
            "kind": "fixed_native",
            "chains": [
                {
                    "chain": chain_id,
                    "acceptance": amount(native, 3 * 10**14),
                    "cancellation": amount(native, 2 * 10**14),
                    "revocation": amount(native, 10**14),
                    "claim": amount(native, 5 * 10**13),
                }
            ],
        }
    raw = {
        "schedule": {
            "start": start,
            "end": end,
            "end_policy": "teardown",
            "cleanup_seconds": cleanup,
            "latency_seconds": 1,
        },
        "history": [
            {
                "chain": chain,
                "provider": {
                    "kind": "alchemy",
                    "network": NETWORKS[chain_id],
                    "api_key_env": "ALCHEMY_API_KEY",
                },
            }
        ],
        "funds": [fund_entry(config, value) for value in config.allocations],
        "payers": [
            {
                "name": "operator",
                "fund": {"chain": chain, "address": PAYER},
                "balances": [amount(native, 10**18)],
            }
        ],
        "market_model": "economic",
        "swap_model": "prepared_integer_market",
        "liquidity_model": None,
        "position_model": "prepared_position_accounting",
        "costs": cost,
        "keeper": {
            "model": "published_opportunity_fixed_native_payment",
            "minimum_latency_seconds": 1,
            "payments": [{"chain": chain_id, "payment": amount(native, 10**15)}],
        },
        "data": {
            "source": "gmx_published_signed_reports",
            "publications": [
                {"allocation": value.name, "source": synthetic.source().model_dump(mode="json")}
                for value in config.allocations
            ],
            "max_age_seconds": 60,
            "maximum_risk_blocks": 1500,
            "maximum_report_age_seconds": 60,
            "publication_limits": {
                "page_blocks": 500,
                "maximum_blocks": 1500,
                "maximum_pages": 3,
                "maximum_logs": 10000,
                "maximum_transactions": 500,
            },
        },
    }
    return EconomicConfig.model_validate_json(json.dumps(raw))


@contextmanager
def priced(long_price):
    saved = synthetic.LONG_PRICE
    synthetic.LONG_PRICE = long_price
    try:
        yield
    finally:
        synthetic.LONG_PRICE = saved


def accrued(opportunity, seconds):
    """Advance stored funding indexes and borrowing accumulators by invented accrual.

    The increments dominate the price-dependent accrual the funding component adds
    on top of stored indexes, so accumulators stay monotonic across synthetic
    price moves exactly as canonical indexes do.
    """
    indexes = opportunity.funding_indexes
    values = {}
    for field in type(indexes).model_fields:
        stored = getattr(indexes, field)
        values[field] = stored.model_copy(
            update={
                "long": stored.long + seconds * 10**25,
                "short": stored.short + seconds * 10**13,
            }
        )
    accounting = opportunity.accounting
    sides = {
        name: getattr(accounting, name).model_copy(
            update={
                "cumulative_factor": getattr(accounting, name).cumulative_factor + seconds * 10**20
            }
        )
        for name in ("long", "short")
    }
    return opportunity.model_copy(
        update={
            "funding_indexes": type(indexes)(**values),
            "accounting": accounting.model_copy(update=sides),
        }
    )


def thinned(opportunity):
    """A market whose physical balances are fully reserved: no token can leave the pool."""
    solvency = opportunity.solvency
    tokens = {
        name: getattr(solvency, name).model_copy(
            update={"collateral_amount": getattr(solvency, name).balance}
        )
        for name in ("long", "short")
    }
    return opportunity.model_copy(update={"solvency": solvency.model_copy(update=tokens)})


def frame(target, timestamp, *, publication=True, long_price=None, risk=None, thin=False):
    market = target.market
    at = synthetic.block(market, number=synthetic.NUMBER + (timestamp - START), timestamp=timestamp)
    controls = risk or synthetic.perpetual_risk(market, at)
    opportunities = ()
    if publication:
        with priced(long_price or synthetic.LONG_PRICE):
            opportunity = synthetic.perpetual_opportunity(market, at)
        opportunity = accrued(opportunity, timestamp - START)
        if thin:
            opportunity = thinned(opportunity)
        opportunities = (opportunity.model_copy(update={"risk": controls}),)
    return data.MarketFrame(target=target, risk=(controls,), opportunities=opportunities)


def record(settings, target, frame):
    time = frame.block.timestamp
    return data.Record(
        key=data.key(target),
        unit=data.UNIT,
        payload=frame,
        reference=frame.block,
        source_time=UnixTime(time),
        available_at=UnixTime(max(settings.schedule.start, time)),
        retrieved_at=float(settings.schedule.deadline + 1),
        evidence=(f"{time:064x}",),
    )


def record_at(target, timestamp, **changes):
    value = frame(target, timestamp, **changes)
    return SimpleNamespace(payload=value, reference=value.block, evidence=(f"{timestamp:064x}",))


class SyntheticBook:
    """The book surface the driver needs: one frozen timeline per configured target."""

    def __init__(self, settings, frames_by_target):
        self.timelines = {
            target: Timeline(
                tuple(record(settings, target, value) for value in frames),
                data.requirement(target, settings),
            )
            for target, frames in frames_by_target.items()
        }

    def at(self, target, time):
        return self.timelines[target].at(time)

    def close(self):
        pass


def frames_for(target, settings, *, crash_at=None, crash=CRASH, thin=(), skip=()):
    """One frame per second; ``skip`` leaves a coverage gap and ``thin`` empties the pool."""
    frames = []
    for time in range(settings.schedule.start, settings.schedule.deadline + 1):
        if time in skip:
            continue
        price = crash if crash_at is not None and time >= crash_at else None
        frames.append(frame(target, time, long_price=price, thin=time in thin))
    return frames


def book(settings, target, **changes):
    return SyntheticBook(settings, {target: frames_for(target, settings, **changes)})


def pending_for(intent, time, identity="admission"):
    at = datetime.fromtimestamp(time, UTC)
    return Pending(
        identity=identity,
        decision_identity="decision",
        trigger=IntervalEvent(started_at=at, due_at=at),
        intent=intent,
        decision_time=UnixTime(time),
        eligible_at=UnixTime(time),
    )


def select(observations, name):
    """Pick one observation or payload; unit checks pass a single payload directly."""
    if not isinstance(observations, (tuple, list)):
        return observations
    if name is None:
        return observations[0]
    return next(observed for observed in observations if observed.allocation == name)


def increase(collateral=COLLATERAL, size=SIZE, name=None):
    def plan(observations):
        target = select(observations, name).target
        return IncreasePosition(
            target=target,
            collateral=AssetAmount(asset=target.collateral_asset, amount=collateral),
            size_delta_usd_e30=size,
        )

    return plan


def decrease(withdrawal, size, name=None):
    def plan(observations):
        target = select(observations, name).target
        return DecreasePosition(
            target=target,
            collateral_withdrawal=AssetAmount(asset=target.collateral_asset, amount=withdrawal),
            size_delta_usd_e30=size,
        )

    return plan


def close(name=None):
    return lambda observations: ClosePosition(target=select(observations, name).target)


def hold():
    return lambda observations: None


def exit_intent(observed):
    if observed.position is not None:
        return ClosePosition(target=observed.target)
    return RevokePosition(target=observed.target) if observed.needs_cleanup else None


def scripted(model_type, plan):
    """A driver whose decisions follow ``plan`` from durable memory, then exit; no sandbox."""

    class Scripted(model_type):
        def decide(self, state, observations, trigger):
            stage = int(state.memory.values.get("stage", 0))
            memory = state.memory
            if stage < len(plan) and not isinstance(trigger, TeardownEvent):
                intent = plan[stage](observations)
                memory = memory.updated(values={**memory.values, "stage": stage + 1})
            else:
                intent = next(
                    (exit for observed in observations if (exit := exit_intent(observed))), None
                )
            return Decision(memory=memory, intent=intent)

        def deliver(self, state, result):
            return state.memory

    return Scripted


def driver(model_type, config, settings, book, plan):
    strategy = SimpleNamespace(sha256="0" * 64)
    return scripted(model_type, plan)(config, settings, strategy, book, authority=declared(config))


def seed(config, settings):
    return initial(settings, config, IntervalSelection(configured=config.decision_trigger))


def run(path, model, initial_state, *, fault=None):
    """Run or resume the journal at ``path``; a shared seed keeps run identities equal."""
    journal = Journal(path, State)
    if not path.exists():
        journal.create(initial_state)
    return Simulator(journal, model, fault=fault).run()
