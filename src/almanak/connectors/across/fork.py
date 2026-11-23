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
from typing import Literal

import httpx

from almanak.connectors.across import quote as venue
from almanak.connectors.across.domain import FeeBps, Target, Uint32, spoke_pool
from almanak.connectors.across.quote import QUOTE_VENUE
from almanak.gateway.environments import is_fork
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS, Chain, Model

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


def fork_allocation(allocation, chain: Chain):
    """The allocation on its source chain's fork. The destination follows its account."""
    route = allocation.route.model_copy(update={"chain": chain})
    return allocation.model_copy(update={"route": route})


def source_chains(deployment) -> tuple[Chain, ...]:
    return tuple(dict.fromkeys(position.chain for position in deployment.positions))


class AcrossFork:
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

        if set(venues) - {QUOTE_VENUE}:
            raise ValueError("an Across fork route names only its constructed quote")
        descriptor = venues.get(QUOTE_VENUE)
        # A chain that sends no deposit names no quote, and its gateway declares none.
        transports = (
            {}
            if descriptor is None
            else {QUOTE_VENUE: ForkTransport(ForkQuote.model_validate(descriptor))}
        )
        return ForkGateway(config, endpoint, key_file, chain=chain, venue_transports=transports)
