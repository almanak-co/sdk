"""Across chains with a pinned SpokePool, and the run modes proven on each.

A mode is declared only for a chain a reference lifecycle has left from. A chain
without one names the connector's scope and admits no run that sends from it; it can
still be the destination of a route from a declared chain.
"""

from datetime import UTC, datetime, timedelta
from typing import Literal

from almanak.caip import ChainId
from almanak.connectors.across.domain import SPOKE_POOLS
from almanak.connectors.support import (
    ConnectorChainSupport,
    EconomicAction,
    HistoricalSupport,
    RunMode,
)

# Chains a reference lifecycle has left from, on identified forks.
FORK_SOURCES = (ChainId("eip155:42161"),)
# Chains declared for production as the source of a route. None is: the declaration is
# the maintainer's, and it waits for the rules it would rest on to be accepted and for
# the cell's evidence package. The production EOA rehearsal and the hosted Fork
# Qualification select a bridge reference from this declaration alone.
PRODUCTION_SOURCES: tuple[ChainId, ...] = ()
# The bridge model of economic backtests. It reads no history: it charges the author's
# whole fee ceiling and credits the arrival after a latency the backtest file states.
BACKTEST_MODEL: Literal["across-full-fee-ceiling-fixed-latency-v1"] = (
    "across-full-fee-ceiling-fixed-latency-v1"
)
# When each pinned SpokePool implementation was installed: the first block whose proxy
# EIP-1967 implementation slot holds the pinned address, found by a binary search over
# `eth_getStorageAt` on an archive node on 24 November 2026: Arbitrum block 494538558
# and Base block 49971488.
PIN_INSTALLED = {
    ChainId("eip155:42161"): datetime(2026, 8, 14, 18, 24, 47, tzinfo=UTC),
    ChainId("eip155:8453"): datetime(2026, 8, 14, 18, 32, 3, tzinfo=UTC),
}
# The destination each declared source chain's reference route reaches.
ROUTE_DESTINATIONS = {ChainId("eip155:42161"): ChainId("eip155:8453")}


def backtest_start(source: ChainId) -> datetime:
    """The first whole day on which both pinned implementations of a route were in place.

    A backtest resolves the route at its window start, on both chains, and first
    resolution refuses another implementation, so no earlier window resolves.
    """
    installed = max(PIN_INSTALLED[source], PIN_INSTALLED[ROUTE_DESTINATIONS[source]])
    return datetime(installed.year, installed.month, installed.day, tzinfo=UTC) + timedelta(days=1)


BACKTEST_STARTS = {source: backtest_start(source) for source in ROUTE_DESTINATIONS}


SCOPE = (
    "ERC-20 routes between two bound accounts; no wrapped native asset is sent",
    "pinned SpokePool proxy and implementation",
    "the output amount is the venue's quote under the author's fee ceiling, a gateway check",
    "arrival and refund are read from the SpokePools; a refund is read only "
    "once the destination chain has proven the transfer unfilled",
)
# What each declared run mode means on a source chain, stated once per mode.
MODE_NAMES = {
    RunMode.FORK: "fork runs",
    RunMode.PRODUCTION: "production runs",
    RunMode.BACKTEST: "economic backtests",
}
MODE_SCOPE = {
    RunMode.FORK: (
        "a fork run asks no venue: the fork gateway constructs the quote from a stated fee "
        "and the fork's block time and retains it as across_fork_constructed; it is no "
        "evidence of a venue quote",
        "a fork refund is a fork-constructed bundle, never Fork Qualification evidence",
    ),
    RunMode.PRODUCTION: (
        "a production run signs the venue's own quote from its API, retained as across_api; "
        "only the gateway holds it to the author's fee ceiling",
    ),
    RunMode.BACKTEST: (
        "a backtest models the bridge and reads no history: it charges the market's whole "
        "max_fee_bps, credits the arrival after the latency the backtest file states, fills "
        "every transfer and models no refund; it keeps no approval, so it serves no "
        "revoke_bridge",
    ),
}
UNPROVEN = (
    "no run mode: no reference lifecycle sends from this chain; it is the proven "
    "destination of a fork route from eip155:42161 only",
)


def restrictions(chain: ChainId) -> tuple[str, ...]:
    """The chain's scope in words, from the modes it declares and nothing else."""
    declared = modes(chain)
    if not declared:
        return SCOPE + UNPROVEN
    names = [MODE_NAMES[mode] for mode in declared]
    listed = names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]
    absent = "" if RunMode.PRODUCTION in declared else "; no production run"
    source = f"{listed} only, as the source of a route to {ROUTE_DESTINATIONS[chain]}{absent}"
    return SCOPE + (source, *(text for mode in declared for text in MODE_SCOPE[mode]))


def historical(chain: ChainId) -> tuple[HistoricalSupport, ...]:
    start = BACKTEST_STARTS.get(chain)
    if start is None:
        return ()
    return (
        HistoricalSupport(
            model=BACKTEST_MODEL,
            mode="economic",
            # The model keeps no approval, so a revocation never arises in it.
            actions=(EconomicAction.BRIDGE,),
            backtest_start_at=start,
        ),
    )


def modes(chain: ChainId) -> tuple[RunMode, ...]:
    if chain not in FORK_SOURCES:
        return ()
    declared = {
        RunMode.FORK: True,
        RunMode.PRODUCTION: chain in PRODUCTION_SOURCES,
        RunMode.BACKTEST: chain in BACKTEST_STARTS,
    }
    return tuple(mode for mode, held in declared.items() if held)


def candidate_support():
    return tuple(
        ConnectorChainSupport(
            chain=chain,
            modes=modes(chain),
            actions=(EconomicAction.BRIDGE, EconomicAction.REVOKE_BRIDGE),
            account_profiles=("eoa",),
            restrictions=restrictions(chain),
            historical=historical(chain) if chain in FORK_SOURCES else (),
        )
        for chain in SPOKE_POOLS
    )
