"""Explicit fixture funding and shared owned cash remain separate quantities."""

from pydantic import model_validator

from almanak.configuration.composed import Allocation
from almanak.connectors.aave_v3.fork import fork_allocation as lending_allocation
from almanak.connectors.aave_v4.fork import fork_allocation as aave_v4_allocation
from almanak.connectors.aave_v4.models import Allocation as AaveV4Allocation
from almanak.connectors.aerodrome_slipstream.fork import fork_allocation as slipstream_allocation
from almanak.connectors.composed.models import Config
from almanak.connectors.compound_v3.fork import fork_allocation as compound_allocation
from almanak.connectors.compound_v3.models import Allocation as CompoundAllocation
from almanak.connectors.curve.fork import curve_arrays
from almanak.connectors.curve.models import Allocation as CurveAllocation
from almanak.connectors.dex_fork import fork_allocation as aerodrome_allocation
from almanak.connectors.dex_fork import fork_allocation as curve_allocation
from almanak.connectors.gmx_v2.exchange_models import SwapAllocation as GmxSwapAllocation
from almanak.connectors.gmx_v2.fork import fork_allocation as gmx_allocation
from almanak.connectors.gmx_v2.liquidity_models import (
    LiquidityAllocation as GmxLiquidityAllocation,
)
from almanak.connectors.gmx_v2.models import Allocation as GmxAllocation
from almanak.connectors.hyperliquid.fork import fork_allocation as hyperliquid_allocation
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.fork import fork_allocation as morpho_allocation
from almanak.connectors.morpho_blue.models import Allocation as MorphoAllocation
from almanak.connectors.pancakeswap_v3.fork import fork_allocation as pancake_allocation
from almanak.connectors.sparklend.fork import fork_allocation as spark_allocation
from almanak.connectors.target_routing import allocation_connector
from almanak.connectors.uniswap_v3.fork import fork_allocation as exchange_allocation
from almanak.connectors.uniswap_v3.fork import validate_assets
from almanak.connectors.uniswap_v4.custody import custody_assets
from almanak.connectors.uniswap_v4.fork import fork_allocation as v4_allocation
from almanak.connectors.uniswap_v4.models import Allocation as UniswapV4Allocation
from almanak.effects import Balance, WalletAsset
from almanak.gateway.environments import ForkBinding
from almanak.gateway.fork_config import (
    ForkConfigBase,
    ForkEoaFund,
    ForkSafeFund,
    require_unique,
)
from almanak.models import StrategyAccounts


class ForkConfig(ForkConfigBase[Allocation]):
    connector: None = None
    inventory: tuple[Balance, ...]

    @model_validator(mode="before")
    @classmethod
    def arrays(cls, value):
        if not isinstance(value, dict):
            return value
        raw = dict(value)
        for field in ("forks", "funds", "operators", "allocations", "inventory"):
            if isinstance(raw.get(field), list):
                raw[field] = tuple(raw[field])
        raw["funds"] = tuple(
            {**fund, "assets": tuple(fund["assets"])}
            if isinstance(fund, dict) and isinstance(fund.get("assets"), list)
            else fund
            for fund in raw.get("funds", ())
        )
        raw["allocations"] = tuple(debt_arrays(a) for a in raw["allocations"])
        return curve_arrays(raw)

    @model_validator(mode="after")
    def scope(self):
        require_unique([a.name for a in self.allocations], "allocation names")
        if {a.account for a in self.allocations} != {f.account for f in self.funds}:
            raise ValueError("composed allocations must identify exactly the funded accounts")
        require_unique([b.key for b in self.inventory], "owned fund assets")
        keys = {
            WalletAsset(account=a.account, chain=a.chain, asset=asset)
            for a in self.allocations
            for asset in allocation_assets(a)
        }
        if {b.key for b in self.inventory} != keys:
            raise ValueError("composed fixture inventory must cover every configured fund asset")
        for fund in self.funds:
            self.validate_funding(fund)
        return self

    def validate_funding(self, fund):
        if not isinstance(fund, (ForkSafeFund, ForkEoaFund)):
            raise ValueError("composed fixtures require explicit asset funding")
        allocations = tuple(a for a in self.allocations if a.account == fund.account)
        chains = {a.chain for a in allocations}
        if len(chains) != 1:
            raise ValueError("each named fund requires one explicit execution chain")
        chain = next(iter(chains))
        if chain not in {target.chain for target in self.forks}:
            raise ValueError("composed fund requires an explicit fork target")
        payer = (
            next(o for o in self.operators if o.name == fund.operator)
            if isinstance(fund, ForkSafeFund)
            else fund
        )
        if any(a.native_asset != payer.native_balance.asset for a in allocations):
            raise ValueError("fund allocations and gas payer must share an explicit chain")
        validate_assets(fund, chain.chain_id)
        balances = {a.balance.asset: a.balance.amount for a in fund.assets}
        if isinstance(fund, ForkEoaFund):
            balances[fund.native_balance.asset] = fund.native_balance.amount
        if any(
            balances.get(b.key.asset, 0) < b.amount
            for b in self.inventory
            if b.key.account == fund.account
        ):
            raise ValueError("fixture funding must cover explicitly owned initial inventory")


def debt_arrays(allocation):
    if not isinstance(allocation, dict):
        return allocation
    return {
        key: tuple(value)
        if key in ("collateral", "collateral_price_sources") and isinstance(value, list)
        else value
        for key, value in allocation.items()
    }


def allocation_assets(allocation):
    from almanak.connectors.aave_v3.models import Allocation as LendingAllocation
    from almanak.connectors.sparklend.models import Allocation as SparkAllocation

    if isinstance(
        allocation,
        (
            LendingAllocation,
            AaveV4Allocation,
            SparkAllocation,
            MorphoAllocation,
            CompoundAllocation,
        ),
    ):
        return (allocation.reserve.asset,)
    if isinstance(allocation, CurveAllocation):
        return allocation.pool.coins
    if isinstance(allocation, (GmxAllocation, GmxSwapAllocation, GmxLiquidityAllocation)):
        market = allocation.market
        return tuple(
            dict.fromkeys(
                (
                    market.long_asset,
                    market.short_asset,
                    market.wrapped_native,
                    allocation.native_asset,
                    *(
                        (market.share_asset,)
                        if not isinstance(allocation, GmxAllocation)
                        else (market.index_asset,)
                    ),
                )
            )
        )
    if isinstance(allocation, UniswapV4Allocation):
        return custody_assets(allocation.pool)
    return allocation.pool.currency0, allocation.pool.currency1


def remap_allocation(allocation, chain):
    factories = {
        ConnectorId("aave_v3"): lending_allocation,
        ConnectorId("aave_v4"): aave_v4_allocation,
        ConnectorId("sparklend"): spark_allocation,
        ConnectorId("uniswap_v3"): exchange_allocation,
        ConnectorId("pancakeswap_v3"): pancake_allocation,
        ConnectorId("aerodrome"): aerodrome_allocation,
        ConnectorId("aerodrome_slipstream"): slipstream_allocation,
        ConnectorId("uniswap_v4"): v4_allocation,
        ConnectorId("morpho_blue"): morpho_allocation,
        ConnectorId("compound_v3"): compound_allocation,
        ConnectorId("curve"): curve_allocation,
        ConnectorId("gmx_v2"): gmx_allocation,
        ConnectorId("hyperliquid"): hyperliquid_allocation,
    }
    factory = factories[allocation_connector(allocation)]
    return factory(allocation, chain)


class ComposedFork:
    @staticmethod
    def prepare_chains(config, bindings, scopes):
        chains = {binding.origin.chain: binding.chain for binding in scopes}
        return Config(
            decision_trigger=config.decision_trigger,
            asset_metadata=config.asset_metadata,
            accounts=StrategyAccounts(bindings=tuple(bindings)),
            allocations=tuple(remap_allocation(a, chains[a.chain]) for a in config.allocations),
            inventory=tuple(
                Balance(
                    key=balance.key.model_copy(update={"chain": chains[balance.key.chain]}),
                    amount=balance.amount,
                )
                for balance in config.inventory
            ),
            parameters=config.parameters,
            fork_bindings=scopes,
        )

    @staticmethod
    def prepare(config, bindings, chain, instance, origin):
        return ComposedFork.prepare_chains(
            config, bindings, (ForkBinding(chain=chain, origin=origin, instance=instance),)
        )

    @staticmethod
    def funding_asset(config, account):
        raise ValueError("composed fixtures require explicit assets, not an inferred funding token")
