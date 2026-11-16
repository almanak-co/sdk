"""Explicit HyperEVM fork funding names the Safe's USDC and the operator's HYPE.

A fork funds the Safe with the USDC it deposits and its operator with HYPE for gas, as
the Morpho HyperEVM reference does. HyperCore has no fork: the synthesized HyperCore
model of blueprint 30, section 9 answers for it, labeled as synthesized.

The model is the fork workspace's `hypercore-model` process beside the HyperEVM node
(`fork_venue.py`, section 9.1), which also holds the Safe owners' HyperCore HYPE for
withdrawals. The workspace names it, with the deployment's agent key file, as the fork
route's `hypercore` venue (`gateway/fork_venues.py`), and the gateway's fork adapter for
the chain reads it there (`gateway/hypercore_gateway.py`).
"""

from typing import Literal

from pydantic import model_validator

from almanak.connectors.dex_fork import fork_arrays, validate_assets, validate_capital
from almanak.connectors.hyperliquid.models import Allocation, Config
from almanak.gateway.environments import ForkBinding
from almanak.gateway.fork_config import ForkConfigBase, ForkSafeFund, require_unique
from almanak.models import StrategyAccounts

# A Hyperliquid fork route's venue: the model's loopback endpoint and the agent key file.
VENUE = "hypercore"
MODEL_VENUE = frozenset({"model", "agent_keys"})


class ForkConfig(ForkConfigBase[Allocation]):
    connector: Literal["hyperliquid"] = "hyperliquid"

    @model_validator(mode="before")
    @classmethod
    def arrays(cls, value):
        return fork_arrays(value)

    @model_validator(mode="after")
    def scope(self):
        require_unique([a.name for a in self.allocations], "Hyperliquid allocation names")
        require_unique([a.account for a in self.allocations], "Hyperliquid allocation accounts")
        if {a.account for a in self.allocations} != {f.account for f in self.funds}:
            raise ValueError("Hyperliquid allocations must identify exactly their funded Safes")
        for allocation in self.allocations:
            if allocation.chain not in {t.chain for t in self.forks}:
                raise ValueError("Hyperliquid market requires an explicit HyperEVM fork target")
            self.validate_funding(allocation)
        return self

    def validate_funding(self, allocation):
        fund = next(f for f in self.funds if f.account == allocation.account)
        if not isinstance(fund, ForkSafeFund):
            raise ValueError("Hyperliquid fixtures fund a Safe, the HyperCore account owner")
        payer = next(o for o in self.operators if o.name == fund.operator)
        if payer.native_balance.asset != allocation.native_asset:
            raise ValueError("Hyperliquid Safe and operator must share HyperEVM")
        validate_assets(fund, allocation.chain.chain_id)
        validate_capital(fund, allocation.capital)


def fork_allocation(allocation, chain):
    if chain.chain_id != allocation.chain.chain_id:
        raise ValueError("Hyperliquid fork routing must preserve the origin CAIP identity")
    return allocation.model_copy(update={"chain": chain})


class Fork:
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
        raise ValueError("Hyperliquid funding requires explicitly configured USDC balances")

    # The fork workspace's venue (`gateway/fork_venues.py`).

    @staticmethod
    def venues(deployment, host):
        from almanak.connectors.hyperliquid.fork_venue import start

        return start(deployment, host)

    @staticmethod
    def fork_adapter(config, endpoint, key_file, *, chain=None, venues):
        from almanak.gateway.hypercore_gateway import fork_adapter

        return fork_adapter(config, endpoint, key_file, chain=chain, venues=venues)

    @staticmethod
    def serve_venue(spec, listen):
        from almanak.connectors.hyperliquid.fork_venue import serve

        return serve(spec, listen)
