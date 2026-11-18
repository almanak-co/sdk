"""Explicit GMX fork funding preserves chain identity and names each operator and fund."""

from typing import Literal

from pydantic import model_validator

from almanak.connectors.dex_fork import fork_arrays, validate_assets, validate_capital
from almanak.connectors.gmx_v2.exchange_models import ExchangeAllocation, SwapAllocation
from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation
from almanak.connectors.gmx_v2.models import Allocation, Config
from almanak.gateway.environments import ForkBinding
from almanak.gateway.fork_config import ForkConfigBase, ForkEoaFund, ForkSafeFund, require_unique
from almanak.models import StrategyAccounts


class ForkConfig(ForkConfigBase[Allocation | SwapAllocation | LiquidityAllocation]):
    connector: Literal["gmx_v2"] = "gmx_v2"

    @model_validator(mode="before")
    @classmethod
    def arrays(cls, value):
        return fork_arrays(value)

    @model_validator(mode="after")
    def scope(self):
        require_unique([a.name for a in self.allocations], "GMX allocation names")
        require_unique([a.account for a in self.allocations], "GMX allocation accounts")
        if {a.account for a in self.allocations} != {f.account for f in self.funds}:
            raise ValueError("GMX allocations must identify exactly their funded accounts")
        for allocation in self.allocations:
            if allocation.chain not in {t.chain for t in self.forks}:
                raise ValueError("GMX market requires an explicit matching fork target")
            self.validate_funding(allocation)
        return self

    def validate_funding(self, allocation):
        fund = next(f for f in self.funds if f.account == allocation.account)
        if not isinstance(fund, (ForkSafeFund, ForkEoaFund)):
            raise ValueError("GMX fixtures require explicit asset funding")
        payer = (
            next(o for o in self.operators if o.name == fund.operator)
            if isinstance(fund, ForkSafeFund)
            else fund
        )
        if payer.native_balance.asset != allocation.native_asset:
            raise ValueError("GMX fund and submitter must share the configured execution chain")
        validate_assets(fund, allocation.chain.chain_id)
        amounts = (
            allocation.initial_inventory.root
            if isinstance(allocation, ExchangeAllocation)
            else (allocation.capital,)
        )
        for capital in amounts:
            if capital.amount:
                validate_capital(fund, capital)


def fork_allocation(allocation, chain):
    if chain.chain_id != allocation.chain.chain_id:
        raise ValueError("GMX fork routing must preserve the origin CAIP identity")
    return allocation.model_copy(
        update={"market": allocation.market.model_copy(update={"chain": chain})}
    )


class Fork:
    # A fork test replays keeper executions published after each request, so fork time must
    # stay behind the head. Each decision may move it to the next publication, minutes apart
    # on a quiet market: ten decisions and teardown used 85 minutes on Avalanche.
    anchor_lag_seconds = 10800

    @staticmethod
    def anchor_time(reader, selections, latest):
        """A fork start at or before `latest` where the market's usable keeper publications
        are dense, so the first decision after admission finds reports. None when there is
        none, or several markets whose dense stretches need not coincide."""
        from almanak.connectors.gmx_v2.report_timeline import Timeline
        from almanak.connectors.gmx_v2.resolution import resolve_market

        if len(selections) != 1:
            return None
        market = resolve_market(reader, selections[0], reader.pin_block("latest")).market
        return Timeline(reader, market).anchor(latest)

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

    @classmethod
    def prepare(cls, config, bindings, chain, instance, origin):
        return cls.prepare_chains(
            config, bindings, (ForkBinding(chain=chain, origin=origin, instance=instance),)
        )

    @staticmethod
    def funding_asset(config, account):
        raise ValueError("GMX funding requires explicitly configured ERC-20 balances")
