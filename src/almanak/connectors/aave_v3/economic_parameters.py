"""Bind author parameters to explicit hypothetical accounts and data requirements."""

import hashlib
import json
from uuid import uuid4

from almanak.backtest.strategy_prices import strategy_requirements
from almanak.connectors.aave_v3.domain import Reserve
from almanak.connectors.aave_v3.economic_data import CONNECTOR, MODEL, REVISION, requirement
from almanak.connectors.aave_v3.economic_state import Position, State
from almanak.connectors.aave_v3.models import Allocation, Parameters
from almanak.effects import (
    Balance,
    Portfolio,
    PositionKey,
    WalletAsset,
)
from almanak.effects import (
    Position as HeldPosition,
)
from almanak.models import AccountBinding, StrategyAccounts


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


def allocations(config):
    sources: dict[Reserve, Allocation] = {}
    for allocation in config.positions:
        prior = sources.setdefault(allocation.reserve, allocation)
        if (prior.oracle, prior.price_source) != (allocation.oracle, allocation.price_source):
            raise ValueError("shared reserve requires the same explicit historical price source")
    return tuple(sources.values())


def requirements(config, settings):
    return tuple(
        requirement(a.reserve, settings, product)
        for a in allocations(config)
        for product in ("accounting", "risk")
    ) + strategy_requirements(settings)


def identity(config, settings):
    value = {
        "sources": [
            {
                "reserve": a.reserve.model_dump(mode="json"),
                "oracle": a.oracle,
                "price_source": a.price_source,
            }
            for a in allocations(config)
        ],
        "data": settings.data.model_dump(mode="json"),
        "start": int(settings.schedule.start),
        "end": int(settings.schedule.deadline),
        "providers": [h.model_dump(mode="json", exclude={"collection"}) for h in settings.history],
        "model": MODEL,
        "acquisition_revision": REVISION,
    }
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def position_key(target):
    return PositionKey(
        connector=CONNECTOR,
        chain=target.fund.chain,
        account=target.account,
        reference=f"{target.reserve.pool}/{target.asset}",
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
    positions = tuple(
        HeldPosition[Position](
            key=position_key(config.target(a.name)), payload=Position(target=config.target(a.name))
        )
        for a in config.positions
    )
    for allocation in config.positions:
        target = config.target(allocation.name)
        balances.setdefault(wallet(target), Balance(key=wallet(target), amount=0))
        if payer_wallet(settings, target, allocation.native_asset) not in balances:
            raise ValueError("economic observations require explicit payer inventory")
    portfolio = Portfolio[Position](balances=tuple(balances.values()), positions=positions)
    validate_inventory(config, portfolio)
    return State(
        run_id=uuid4().hex,
        time=settings.schedule.start,
        schedule=settings.schedule,
        interval=interval,
        portfolio=portfolio,
    )


def validate_inventory(config, portfolio):
    requirements: dict[WalletAsset, int] = {}
    for allocation in config.positions:
        target = config.target(allocation.name)
        needed = allocation.capital
        requirements[wallet(target)] = requirements.get(wallet(target), 0) + needed
    if any(portfolio.balance(key) < amount for key, amount in requirements.items()):
        raise ValueError("initial inventory cannot fund the combined allocation capital")
