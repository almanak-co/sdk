"""Explicit exchange fixture funding preserves market CAIP identity across fork routing."""

from collections.abc import Callable
from typing import ClassVar, Literal

from pydantic import model_validator

from almanak.asset_amounts import AssetAmounts
from almanak.connectors.aerodrome.models import Allocation as AerodromeAllocation
from almanak.connectors.curve.models import Allocation as CurveAllocation
from almanak.connectors.v3_pool_models import Allocation
from almanak.gateway.environments import ForkBinding
from almanak.gateway.fork_config import (
    ForkConfigBase,
    ForkEoaFund,
    ForkSafeFund,
    require_unique,
)
from almanak.models import StrategyAccounts


class ForkConfig[A: Allocation | AerodromeAllocation | CurveAllocation](ForkConfigBase[A]):
    connector: Literal["uniswap_v3", "pancakeswap_v3", "aerodrome_slipstream", "aerodrome", "curve"]

    @model_validator(mode="before")
    @classmethod
    def arrays(cls, value):
        return fork_arrays(value)

    @model_validator(mode="after")
    def scope(self):
        self.validate_accounts()
        chains = {target.chain for target in self.forks}
        for allocation in self.allocations:
            if allocation.chain not in chains:
                raise ValueError("exchange pool requires an explicit fork target")
            self.validate_funding(allocation)
        return self

    def validate_accounts(self):
        require_unique([a.name for a in self.allocations], "allocation names")
        accounts = [a.account for a in self.allocations]
        require_unique(accounts, "exchange allocation accounts")
        if set(accounts) != {f.account for f in self.funds}:
            raise ValueError("exchange allocations must identify exactly the funded accounts")

    def validate_funding(self, allocation):
        fund = next(f for f in self.funds if f.account == allocation.account)
        if not isinstance(fund, (ForkSafeFund, ForkEoaFund)):
            raise ValueError("exchange fixtures require explicit asset funding")
        payer = self.payer(fund)
        if payer.native_balance.asset != allocation.native_asset:
            raise ValueError("exchange fund and gas payer must share the configured pool chain")
        validate_assets(fund, allocation.chain.chain_id)
        validate_capital(fund, allocation.capital)

    def payer(self, fund):
        if isinstance(fund, ForkSafeFund):
            return next(o for o in self.operators if o.name == fund.operator)
        return fund


def fork_arrays(value):
    if not isinstance(value, dict):
        return value
    raw = dict(value)
    for field in ("forks", "funds", "operators", "allocations"):
        if isinstance(raw.get(field), list):
            raw[field] = tuple(raw[field])
    for fund in raw.get("funds", ()):
        if isinstance(fund, dict) and isinstance(fund.get("assets"), list):
            fund["assets"] = tuple(fund["assets"])
    return raw


def validate_assets(fund, chain_id):
    assets = [a.balance.asset for a in fund.assets]
    require_unique(assets, "funding assets")
    if any((asset.chain, asset.namespace) != (chain_id, "erc20") for asset in assets):
        raise ValueError("funded ERC-20 assets require the exchange pool chain")


def validate_capital(fund, capital):
    balances = {a.balance.asset: a.balance.amount for a in fund.assets}
    values = capital.root if isinstance(capital, AssetAmounts) else (capital,)
    if any(balances.get(value.asset, 0) < value.amount for value in values):
        raise ValueError("exchange fork funding must cover its configured capital")


def fork_allocation(allocation, chain):
    if chain.chain_id != allocation.chain.chain_id:
        raise ValueError("exchange fork routing must preserve the origin CAIP identity")
    return type(allocation).model_validate(
        {
            **allocation.model_dump(),
            "pool": allocation.pool.model_copy(update={"chain": chain}),
        }
    )


class Fork:
    config_type: ClassVar[Callable]

    @classmethod
    def prepare_chains(cls, config, bindings, scopes):
        chains = {binding.origin.chain: binding.chain for binding in scopes}
        return cls.config_type(
            decision_trigger=config.decision_trigger,
            asset_metadata=config.asset_metadata,
            accounts=StrategyAccounts(bindings=tuple(bindings)),
            allocations=tuple(fork_allocation(a, chains[a.chain]) for a in config.allocations),
            parameters=config.parameters,
            fork_bindings=scopes,
        )

    @classmethod
    def prepare(cls, config, bindings, chain, instance, origin):
        return cls.prepare_chains(
            config, bindings, (ForkBinding(chain=chain, origin=origin, instance=instance),)
        )

    @staticmethod
    def funding_asset(config, account):
        raise ValueError("exchange fixtures require explicit assets, not an inferred funding token")
