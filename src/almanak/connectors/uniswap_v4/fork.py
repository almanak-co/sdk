"""Explicit USDC funding and origin-to-fork remapping for the Uniswap samples."""

from typing import Literal

from pydantic import model_validator

from almanak.connectors.uniswap_v4.models import Allocation, Config
from almanak.gateway.fork_config import (
    ForkConfigBase,
    ForkEoaFund,
    ForkOperator,
    ForkSafeFund,
    remap_asset,
    require_unique,
)
from almanak.models import AssetAmount, StrategyAccounts


class ForkConfig(ForkConfigBase[Allocation]):
    connector: Literal["uniswap_v4"] = "uniswap_v4"

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
        if {a.account for a in self.allocations} != {f.account for f in self.funds}:
            raise ValueError("allocations must identify exactly the funded accounts")
        require_unique([a.name for a in self.allocations], "allocation names")
        require_unique([a.account for a in self.allocations], "Uniswap allocation accounts")
        chains = {t.chain for t in self.forks}
        for allocation in self.allocations:
            if allocation.pool.chain not in chains:
                raise ValueError("pool must identify an explicit fork target")
            self.validate_funding(allocation)
        return self

    def validate_funding(self, allocation):
        fund = next(f for f in self.funds if f.account == allocation.account)
        operator: ForkOperator | ForkEoaFund
        if isinstance(fund, ForkSafeFund):
            operator = next(o for o in self.operators if o.name == fund.operator)
        elif isinstance(fund, ForkEoaFund):
            operator = fund
        else:
            raise ValueError("Uniswap fixtures require explicit asset funding")
        if operator.native_balance.asset != allocation.native_asset:
            raise ValueError("operator funding requires the pool chain native asset")
        require_unique([f.balance.asset for f in fund.assets], "funding asset identities")
        if any(f.balance.asset.chain != allocation.pool.chain.chain_id for f in fund.assets):
            raise ValueError("fixture funding has another chain identity")
        available = {f.balance.asset: f.balance.amount for f in fund.assets}
        if available.get(allocation.capital.asset, 0) < allocation.capital.amount:
            raise ValueError("fixture USDC balance does not cover its declared capital")


def fork_allocation(allocation, chain):
    pool = allocation.pool.model_copy(
        update={
            "chain": chain,
            "currency0": remap_asset(allocation.pool.currency0, chain.chain_id),
            "currency1": remap_asset(allocation.pool.currency1, chain.chain_id),
        }
    )
    return Allocation.model_validate(
        {
            **allocation.model_dump(),
            "pool": pool,
            "capital": AssetAmount(
                asset=remap_asset(allocation.capital.asset, chain.chain_id),
                amount=allocation.capital.amount,
            ),
            "native_asset": remap_asset(allocation.native_asset, chain.chain_id),
        }
    )


class UniswapFork:
    @staticmethod
    def prepare(config, bindings, chain, instance, origin):
        return Config(
            decision_trigger=config.decision_trigger,
            asset_metadata=config.asset_metadata,
            accounts=StrategyAccounts(bindings=tuple(bindings)),
            allocations=tuple(fork_allocation(a, chain) for a in config.allocations),
            parameters=config.parameters,
            fork_instance=instance,
            fork_origin=origin,
        )

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
    def funding_asset(config, account):
        return next(a.capital.asset for a in config.allocations if a.account == account)
