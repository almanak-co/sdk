"""Explicit V3 fork targets, asset funding and environment-only remapping."""

from typing import Literal

from pydantic import model_validator

from almanak.caip import AssetType
from almanak.connectors.aave_v3.models import Allocation, Config
from almanak.gateway.environments import ForkBinding
from almanak.gateway.fork_config import (
    ForkConfigBase,
    ForkEoaFund,
    ForkSafeFund,
    remap_asset,
    require_unique,
)
from almanak.models import StrategyAccounts


class ForkConfig(ForkConfigBase[Allocation]):
    connector: Literal["aave_v3"] = "aave_v3"

    @model_validator(mode="before")
    @classmethod
    def arrays(cls, value):
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

    @model_validator(mode="after")
    def scope(self):
        require_unique([a.name for a in self.allocations], "allocation names")
        require_unique(
            [(a.account, a.reserve) for a in self.allocations], "fund/Pool/asset positions"
        )
        if {a.account for a in self.allocations} != {f.account for f in self.funds}:
            raise ValueError("reserve allocations must identify exactly the funded accounts")
        chains = {target.chain for target in self.forks}
        if any(a.chain not in chains for a in self.allocations):
            raise ValueError("reserve requires an explicit fork target")
        for fund in self.funds:
            self.validate_funding(fund)
        return self

    def validate_funding(self, fund):
        if not isinstance(fund, (ForkSafeFund, ForkEoaFund)):
            raise ValueError("Aave V3 fixtures require explicit asset funding")
        allocations = [a for a in self.allocations if a.account == fund.account]
        payer = (
            next(o for o in self.operators if o.name == fund.operator)
            if isinstance(fund, ForkSafeFund)
            else fund
        )
        if len({a.chain for a in allocations}) != 1 or any(
            a.native_asset != payer.native_balance.asset for a in allocations
        ):
            raise ValueError("fund allocations and gas payer must share one explicit chain")
        require_unique([a.balance.asset for a in fund.assets], "funding assets")
        if any(
            a.balance.asset.chain != allocations[0].chain.chain_id
            or a.balance.asset.namespace != "erc20"
            for a in fund.assets
        ):
            raise ValueError("funded ERC-20 assets require the fund's explicit chain")
        required: dict[AssetType, int] = {}
        for allocation in allocations:
            asset = allocation.reserve.asset
            required[asset] = required.get(asset, 0) + allocation.capital
        balances = {a.balance.asset: a.balance.amount for a in fund.assets}
        if any(balances.get(asset, 0) < amount for asset, amount in required.items()):
            raise ValueError("fork funding must cover the allocation capital")


def fork_allocation(allocation, chain):
    if chain.chain_id != allocation.chain.chain_id:
        raise ValueError("V3 fork setup must preserve the origin CAIP identity")
    return allocation.model_copy(
        update={
            "reserve": allocation.reserve.model_copy(update={"chain": chain}),
            "native_asset": remap_asset(allocation.native_asset, chain.chain_id),
        }
    )


class AaveFork:
    @staticmethod
    def prepare_chains(config, bindings, scopes):
        chains = {binding.origin.chain: binding.chain for binding in scopes}
        return Config(
            decision_trigger=config.decision_trigger,
            asset_metadata=config.asset_metadata,
            accounts=StrategyAccounts(bindings=tuple(bindings)),
            allocations=tuple(fork_allocation(a, chains[a.chain]) for a in config.allocations),
            parameters=config.parameters,
            fork_bindings=scopes,
        )

    @staticmethod
    def prepare(config, bindings, chain, instance, origin):
        return AaveFork.prepare_chains(
            config, bindings, (ForkBinding(chain=chain, origin=origin, instance=instance),)
        )

    @staticmethod
    def funding_asset(config, account):
        raise ValueError("Aave V3 fixtures require explicit assets, not an inferred funding token")
