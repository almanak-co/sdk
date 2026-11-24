"""Across chains with a pinned SpokePool, and the run modes proven on each.

A mode is declared only for a chain a reference lifecycle has left from. A chain
without one names the connector's scope and admits no run that sends from it; it can
still be the destination of a route from a declared chain.
"""

from almanak.caip import ChainId
from almanak.connectors.across.domain import SPOKE_POOLS
from almanak.connectors.support import ConnectorChainSupport, EconomicAction, RunMode

# Chains a reference lifecycle has left from, on identified forks.
FORK_SOURCES = (ChainId("eip155:42161"),)


SCOPE = (
    "ERC-20 routes between two bound accounts; no wrapped native asset is sent",
    "pinned SpokePool proxy and implementation",
    "the output amount is the venue's quote under the author's fee ceiling, a gateway check",
    "arrival and refund are read from the SpokePools; a refund is read only "
    "once the destination chain has proven the transfer unfilled",
)
FORK_SCOPE = (
    "fork runs only, as the source of a route to eip155:8453; no production run and no backtest",
    "a fork run asks no venue: the fork gateway constructs the quote from a stated fee "
    "and the fork's block time and retains it as across_fork_constructed; it is no "
    "evidence of a venue quote",
    "a fork refund is a fork-constructed bundle, never Fork Qualification evidence",
)
UNPROVEN = (
    "no run mode: no reference lifecycle sends from this chain; it is the proven "
    "destination of a fork route from eip155:42161 only",
)


def candidate_support():
    return tuple(
        ConnectorChainSupport(
            chain=chain,
            modes=(RunMode.FORK,) if chain in FORK_SOURCES else (),
            actions=(EconomicAction.BRIDGE, EconomicAction.REVOKE_BRIDGE),
            account_profiles=("eoa",),
            restrictions=SCOPE + (FORK_SCOPE if chain in FORK_SOURCES else UNPROVEN),
        )
        for chain in SPOKE_POOLS
    )
