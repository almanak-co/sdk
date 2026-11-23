"""Across on identified forks: the quote a fork run signs is constructed, never asked.

No relayer quotes a fork, and a live quote names the venue's clock and a fee for mainnet
liquidity, neither of which a fork shares. A fork run therefore asks no venue. The fork
binding names a constructed quote in the source chain's fork route, and the chain's
gateway builds each quote from it: a stated fee, the fork's own pinned block time and
the pinned SpokePools. The gateway's admission checks run on it unchanged, and the
retained answer carries its label. A constructed quote is never evidence of what the
venue would have quoted.
"""

from typing import Literal

from almanak.connectors.across import quote as venue
from almanak.connectors.across.domain import FeeBps, Target, Uint32, spoke_pool
from almanak.models import ZERO_ADDRESS, Chain, Model

QUOTE_VENUE = "across-quote"
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

    def quote(self, target: Target, amount: int, block_timestamp: int) -> venue.Quote:
        return venue.parse(self.answer(target, amount, block_timestamp), amount)


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
        gateway = ForkGateway(config, endpoint, key_file, chain=chain)
        descriptor = venues.get(QUOTE_VENUE)
        # Read by the connector's planning; a chain that sends no deposit names none.
        quote = None if descriptor is None else ForkQuote.model_validate(descriptor)
        setattr(gateway, "fork_quote", quote)  # noqa: B010 - the fork gateway declares none
        return gateway
