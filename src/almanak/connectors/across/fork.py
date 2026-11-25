"""Across on identified forks: the quote a fork run signs is constructed, never asked.

No relayer quotes a fork, and a live quote names the venue's clock and a fee for mainnet
liquidity, neither of which a fork shares. A fork run therefore asks no venue. The fork
binding names a constructed quote in the source chain's fork route, and the chain's fork
gateway declares a transport that answers each quote request from it: a stated fee, the
fork's own pinned block time and the pinned SpokePools. That answer body takes the same
fetch, size cap, parser and admission as the venue's, and its quote is retained under
its own source, which admission accepts on an identified fork only. A constructed quote
is never evidence of what the venue would have quoted.
"""

import json
from itertools import product
from typing import Literal

import httpx
from pydantic import model_validator

from almanak.connectors.across import quote as venue
from almanak.connectors.across.domain import FeeBps, Target, Uint32, spoke_pool
from almanak.connectors.across.models import Allocation, Config
from almanak.connectors.across.quote import QUOTE_VENUE
from almanak.connectors.support import RunMode
from almanak.gateway.environments import is_fork
from almanak.gateway.ethereum import DataError
from almanak.gateway.fork_config import ForkConfigBase, ForkEoaFund, require_unique
from almanak.models import ZERO_ADDRESS, Chain, Model, StrategyAccounts

# What a constructed quote charges, in basis points of the amount sent.
FORK_FEE_BPS = 1
# How long after the pinned block a constructed quote lets a relayer fill.
FORK_FILL_SECONDS = 3600
LABEL = (
    "fork-constructed quote: built by the fork gateway from a stated fee and the fork's "
    "own block time; no venue was asked"
)


class ForkQuote(Model):
    """The constructed quote a fork route names for deposits from its chain."""

    kind: Literal["fork_constructed"] = "fork_constructed"
    fee_bps: FeeBps
    fill_seconds: Uint32

    def answer(self, target: Target, amount: int, block_timestamp: int) -> dict:
        """A quote in the venue's own answer shape, so the same parser reads it."""
        route = target.route
        return {
            "forkConstructed": LABEL,
            "feeBps": self.fee_bps,
            "outputAmount": str(amount - amount * self.fee_bps // 10_000),
            "timestamp": block_timestamp,
            "fillDeadline": block_timestamp + self.fill_seconds,
            "exclusiveRelayer": ZERO_ADDRESS,
            "exclusivityDeadline": 0,
            "spokePoolAddress": route.spoke_pool,
            "destinationSpokePoolAddress": spoke_pool(route.destination_chain).address,
            "inputToken": {"address": route.asset.reference, "decimals": route.decimals},
            "outputToken": {
                "address": route.destination_asset.reference,
                "decimals": route.decimals,
            },
            "isAmountTooLow": False,
        }


class ForkTransport:
    """Answers a quote request with the constructed quote, in place of the venue's API."""

    source: venue.Source = venue.FORK_SOURCE

    def __init__(self, constructed: ForkQuote):
        self.constructed = constructed

    def get(self, target: Target, amount: int, block_timestamp: int) -> httpx.Response:
        if not is_fork(target.fund.chain):
            raise DataError(
                "quote_unavailable",
                "a constructed quote answers an identified fork only; "
                f"{target.fund.chain.chain_id} is not one",
            )
        body = self.constructed.answer(target, amount, block_timestamp)
        return httpx.Response(200, content=json.dumps(body).encode())


class ForkConfig(ForkConfigBase[Allocation]):
    """The in-process fixture of a bridge: one fork per chain a route leaves or reaches,
    and one funded EOA per named account, the receiving one included."""

    connector: Literal["across"] = "across"

    @model_validator(mode="before")
    @classmethod
    def arrays(cls, value):
        if not isinstance(value, dict):
            return value
        raw = dict(value)
        for field in ("forks", "funds", "operators", "allocations"):
            if isinstance(raw.get(field), list):
                raw[field] = tuple(raw[field])
        raw["funds"] = tuple(
            {**fund, "assets": tuple(fund["assets"])}
            if isinstance(fund, dict) and isinstance(fund.get("assets"), list)
            else fund
            for fund in raw.get("funds", ())
        )
        return raw

    @model_validator(mode="after")
    def scope(self):
        require_unique([a.name for a in self.allocations], "allocation names")
        named = {name for a in self.allocations for name in (a.account, a.destination_account)}
        if named != {fund.account for fund in self.funds}:
            raise ValueError("bridge allocations must identify exactly the funded accounts")
        funds = {fund.account: fund for fund in self.funds if isinstance(fund, ForkEoaFund)}
        if len(funds) != len(self.funds):
            # Every fork run of a standalone bridge reads this fixture, a supervised one
            # with a hand-written operator file included, so none can bind a Safe.
            raise ValueError(
                "Across fork fixtures fund EOA accounts only: no fork fixture of a "
                "standalone bridge provisions a Safe, so a Safe/Roles bridge has no fork run"
            )
        for allocation in self.allocations:
            validate_funding(allocation, funds[allocation.account])
        return self

    def validate_support(self):
        """A bridge's forks are its source and its destination chain. Only the source
        chain declares a run mode: a route is planned and signed on the chain it leaves."""
        from almanak.connectors.composition import connector_from_wire

        if route_chains(self.allocations) != {target.chain.chain_id for target in self.forks}:
            raise ValueError("fork targets must match each route's source and destination chain")
        connector = connector_from_wire({"connector": "across"})
        modes = (RunMode.FORK,) if self.production is None else (RunMode.FORK, RunMode.PRODUCTION)
        for allocation, mode in product(self.allocations, modes):
            connector.require_target(allocation.route.chain.chain_id, allocation, mode)
        if self.production is not None and {t.chain for t in self.forks} != {
            n.chain for n in self.production.networks
        }:
            raise ValueError("production and fork profiles must cover the same author chains")


def validate_funding(allocation, fund: ForkEoaFund) -> None:
    if fund.native_balance.asset != allocation.native_asset:
        raise ValueError("a bridge's source fund pays gas on its source chain")
    balances = {a.balance.asset: a.balance.amount for a in fund.assets}
    if balances.get(allocation.route.asset, 0) < allocation.capital:
        raise ValueError("fork funding must cover the allocation capital")


def route_chains(allocations) -> set:
    return {
        chain for a in allocations for chain in (a.route.chain.chain_id, a.route.destination_chain)
    }


def fork_allocation(allocation, chain: Chain):
    """The allocation on its source chain's fork. The destination follows its account."""
    route = allocation.route.model_copy(update={"chain": chain})
    return allocation.model_copy(update={"route": route})


def source_chains(deployment) -> tuple[Chain, ...]:
    return tuple(dict.fromkeys(position.chain for position in deployment.positions))


def route_transports(venues) -> dict:
    """The quote transport a fork route's descriptors declare for its chain's gateway."""
    if set(venues) - {QUOTE_VENUE}:
        raise ValueError("an Across fork route names only its constructed quote")
    descriptor = venues.get(QUOTE_VENUE)
    # A chain that sends no deposit names no quote, and its gateway declares none.
    if descriptor is None:
        return {}
    return {QUOTE_VENUE: ForkTransport(ForkQuote.model_validate(descriptor))}


class AcrossFork:
    # A generated fork operator file provisions plain EOAs for a bridge although the
    # connector declares Safe/Roles accounts: no fork run has installed the bridge's
    # grants, so the unrun profile is never the default. A standalone bridge has no
    # fork fixture for a Safe at all (`ForkConfig.scope` refuses one). A composed
    # deployment's fixture takes Safe funds from a hand-written operator file, which
    # would be that path's first run.
    plain_fixture_accounts = True

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
    def venues(deployment, host) -> dict[Chain, dict]:
        """Each source chain's route names its constructed quote; no process serves it."""
        descriptor = ForkQuote(fee_bps=FORK_FEE_BPS, fill_seconds=FORK_FILL_SECONDS)
        return {
            chain: {QUOTE_VENUE: descriptor.model_dump(mode="json")}
            for chain in source_chains(deployment)
        }

    @staticmethod
    def fork_adapter(config, endpoint, key_file, *, chain=None, venues):
        from almanak.gateway.fork_execution import ForkGateway

        return ForkGateway(
            config, endpoint, key_file, chain=chain, venue_transports=route_transports(venues)
        )
