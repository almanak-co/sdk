"""Bind HyperCore author parameters to hypothetical funds and their first account state."""

from uuid import uuid4

from almanak.connectors.hyperliquid.economic_model import position_key, wallet
from almanak.connectors.hyperliquid.economic_state import HyperCoreAccount, State
from almanak.connectors.hyperliquid.models import Parameters
from almanak.effects import Portfolio, Position
from almanak.models import AccountBinding, StrategyAccounts


def parameters(author, settings, interval):
    """The author parameters bound to the backtest's hypothetical funds."""
    return Parameters(
        decision_trigger=interval.effective,
        asset_metadata=author.asset_metadata,
        parameters=author.parameters,
        allocations=author.allocations,
        accounts=StrategyAccounts(
            bindings=tuple(AccountBinding(name=f.name, profile=f.profile) for f in settings.funds)
        ),
    )


def initial(settings, config, interval):
    """The first state: every account inactive, funded by the fund's inventory."""
    balances = tuple(b for f in (*settings.funds, *settings.payers) for b in f.inventory())
    targets = tuple(config.target(a.name) for a in config.allocations)
    portfolio = Portfolio[HyperCoreAccount](
        balances=balances,
        positions=tuple(
            Position[HyperCoreAccount](key=position_key(t), payload=HyperCoreAccount(target=t))
            for t in targets
        ),
    )
    for allocation, target in zip(config.allocations, targets, strict=True):
        if portfolio.balance(wallet(target)) < allocation.capital.amount:
            raise ValueError("initial inventory cannot cover configured HyperCore capital")
    return State(
        run_id=uuid4().hex,
        time=settings.schedule.start,
        schedule=settings.schedule,
        interval=interval,
        portfolio=portfolio,
    )
