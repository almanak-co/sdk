"""Aave author fork binding and connector-owned remapping."""

import json
from typing import Self

from pydantic import model_validator

from almanak.caip import AssetType
from almanak.connectors.aave_v4.models import Allocation, DebtAllocation, LendingConfig
from almanak.gateway.fork_config import (
    ForkConfigBase,
    ForkEoaFund,
    ForkSafeFund,
    remap_asset,
    require_unique,
)
from almanak.models import Block, Chain, StrategyAccounts


class ForkConfig(ForkConfigBase[Allocation]):
    debts: tuple[DebtAllocation, ...] = ()

    @model_validator(mode="before")
    @classmethod
    def legacy(cls, value):
        if not isinstance(value, dict):
            return value
        raw = dict(value)
        if "origin" in raw:
            if "forks" in raw:
                raise ValueError("choose legacy origin or forks, not both")
            try:
                raw = convert_legacy(raw)
            except (KeyError, TypeError) as exc:
                raise ValueError(
                    "legacy origin requires complete fund and asset declarations"
                ) from exc
        return normalize_arrays(raw)

    @model_validator(mode="after")
    def scope(self) -> Self:
        require_unique([a.name for a in (*self.allocations, *self.debts)], "allocation names")
        positions = [
            (a.account, a.reserve.chain, a.reserve.spoke, a.reserve.reserve_id)
            for a in self.allocations
        ]
        require_unique(positions, "fund/Spoke/reserve positions; allocations cannot alias")
        self.validate_allocations()
        return self

    def validate_allocations(self):
        if {a.account for a in self.allocations} != {f.account for f in self.funds}:
            raise ValueError("allocations must identify exactly the funded accounts")
        targets: dict[Chain, set[AssetType]] = {target.chain: set() for target in self.forks}
        for allocation in self.allocations:
            chain = allocation.reserve.chain
            if chain not in targets:
                raise ValueError("fork allocation must identify a configured target chain")
            targets[chain].add(allocation.native_asset)
        for assets in targets.values():
            if len(assets) != 1:
                raise ValueError("each target requires funds with one explicit native asset")
        for fund in self.funds:
            allocations = [a for a in self.allocations if a.account == fund.account]
            if isinstance(fund, ForkSafeFund):
                operator = next(o for o in self.operators if o.name == fund.operator)
                validate_safe_funding(fund, operator, allocations)
            elif isinstance(fund, ForkEoaFund):
                validate_safe_funding(fund, fund, allocations)
            else:
                validate_funding(fund, allocations)


def validate_funding(fund, allocations):
    if len({a.reserve.chain for a in allocations}) != 1:
        raise ValueError("each fund must identify one chain and environment")
    if any(a.native_asset != fund.native_balance.asset for a in allocations):
        raise ValueError("funding must identify the allocation's native gas asset")
    if fund.funding == "wrap_native":
        validate_token_funding(fund, allocations)


def validate_token_funding(fund, allocations):
    if len({a.reserve.asset for a in allocations}) != 1:
        raise ValueError("wrapped-native funding requires one explicit asset per fund")
    if fund.token_balance < sum(a.capital for a in allocations):
        raise ValueError("fork token funding is smaller than the combined allocations")


def validate_safe_funding(fund, operator, allocations):
    require_unique([a.balance.asset for a in fund.assets], "funding asset identities")
    chains = {a.reserve.chain for a in allocations}
    if len(chains) != 1 or any(
        a.native_asset != operator.native_balance.asset for a in allocations
    ):
        raise ValueError("Safe allocations and operator gas must share an explicit chain scope")
    chain = next(iter(chains))
    if operator.native_balance.asset.namespace != "slip44":
        raise ValueError("operator funding must identify the native gas asset")
    for funding in fund.assets:
        if (
            funding.balance.asset.chain != chain.chain_id
            or funding.balance.asset.namespace != "erc20"
        ):
            raise ValueError("Safe fixture asset funding requires a scoped ERC-20 asset")
    available = {a.balance.asset: a.balance.amount for a in fund.assets}
    for asset in {a.reserve.asset for a in allocations}:
        if available.get(asset, 0) < sum(
            a.capital for a in allocations if a.reserve.asset == asset
        ):
            raise ValueError("Safe asset funding is smaller than its combined supply allocations")


def convert_legacy(value):
    raw = dict(value)
    origin = raw.pop("origin")
    block = origin if isinstance(origin, Block) else Block.model_validate_json(json.dumps(origin))
    if str(block.chain.chain_id) != "eip155:1" or block.chain.environment != "mainnet":
        raise ValueError("legacy origin supports Ethereum mainnet only; declare explicit forks")
    funds = [dict(fund) for fund in raw["funds"]]
    allocations = []
    for fund in funds:
        validate_legacy_fund(fund)
        allocation = dict(fund.pop("allocation"))
        allocation["name"] = allocation["account"]
        # A legacy supply `amount` was the allocation's entry supply, which is its capital.
        if "amount" in allocation:
            allocation["capital"] = allocation.pop("amount")
        allocations.append(allocation)
        fund["account"] = allocation["account"]
        fund["funding"] = "wrap_native"
    raw["funds"] = tuple(funds)
    raw["allocations"] = tuple(allocations)
    target = {
        "chain": origin.chain if isinstance(origin, Block) else origin["chain"],
        "provider": {"kind": "alchemy", "network": "eth-mainnet", "api_key_env": "ALCHEMY_API_KEY"},
        "block": origin,
    }
    raw["forks"] = (target,)
    return raw


def validate_legacy_fund(fund):
    if "funding" in fund:
        raise ValueError("legacy origin has implicit WETH funding; use forks for explicit funding")
    allocation = fund["allocation"]
    asset = str(allocation["reserve"]["asset"])
    if asset != "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2":
        raise ValueError("legacy funding supports Ethereum WETH only; declare explicit funding")
    if str(fund["native_balance"]["asset"]) != "eip155:1/slip44:60":
        raise ValueError("legacy funding requires Ethereum native ETH")


def normalize_arrays(raw):
    for field in ("forks", "funds", "operators", "allocations", "debts"):
        if isinstance(raw.get(field), list):
            raw[field] = tuple(raw[field])
    for fund in raw.get("funds", ()):
        if isinstance(fund, dict) and isinstance(fund.get("assets"), list):
            fund["assets"] = tuple(fund["assets"])
    for debt in raw.get("debts", ()):
        if isinstance(debt, dict):
            for field in ("collateral", "collateral_price_sources"):
                if isinstance(debt.get(field), list):
                    debt[field] = tuple(debt[field])
    return raw


def fork_allocation[A: Allocation](allocation: A, chain: Chain) -> A:
    # Origin asset identity changes only at the privileged fork setup boundary.
    source = allocation.reserve
    reserve = source.model_copy(
        update={
            "chain": chain,
            "asset": AssetType(
                f"{chain.chain_id}/{source.asset.namespace}:{source.asset.reference}"
            ),
        }
    )
    return allocation.model_copy(
        update={
            "reserve": reserve,
            "native_asset": remap_asset(allocation.native_asset, chain.chain_id),
        }
    )


class AaveFork:
    @staticmethod
    def prepare(config, bindings, chain, instance, origin):
        return LendingConfig(
            decision_trigger=config.decision_trigger,
            asset_metadata=config.asset_metadata,
            accounts=StrategyAccounts(bindings=tuple(bindings)),
            allocations=tuple(fork_allocation(a, chain) for a in config.allocations),
            debts=tuple(fork_allocation(d, chain) for d in config.debts),
            parameters=config.parameters,
            fork_instance=instance,
            fork_origin=origin,
        )

    @staticmethod
    def prepare_chains(config, bindings, scopes):
        chains = {binding.origin.chain: binding.chain for binding in scopes}
        return LendingConfig(
            decision_trigger=config.decision_trigger,
            asset_metadata=config.asset_metadata,
            accounts=StrategyAccounts(bindings=tuple(bindings)),
            allocations=tuple(fork_allocation(a, chains[a.chain]) for a in config.allocations),
            debts=tuple(fork_allocation(d, chains[d.chain]) for d in config.debts),
            parameters=config.parameters,
            fork_bindings=scopes,
        )

    @staticmethod
    def funding_asset(config, account):
        return next(a.reserve.asset for a in config.allocations if a.account == account)
