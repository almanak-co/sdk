"""Across chains with a pinned SpokePool, and the run modes proven on each.

A mode is declared only for a chain a reference lifecycle has left from. A chain
without one names the connector's scope and admits no run that sends from it; it can
still be the destination of a route from a declared chain.
"""

from datetime import UTC, datetime
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
# The first whole day after the pinned SpokePool implementation was installed on the
# source chain. Read on 24 November 2026 by a binary search over `eth_getStorageAt` of
# the proxy's EIP-1967 implementation slot on an archive node: Arbitrum block
# 494538558, 14 August 2026 18:24:47 UTC, is the first whose slot holds the pinned
# implementation. A backtest resolves the route at its window start and refuses
# another implementation, so no earlier window resolves.
BACKTEST_STARTS = {ChainId("eip155:42161"): datetime(2026, 8, 15, tzinfo=UTC)}


SCOPE = (
    "ERC-20 routes between two bound accounts; no wrapped native asset is sent",
    "pinned SpokePool proxy and implementation",
    "the output amount is the venue's quote under the author's fee ceiling, a gateway check",
    "arrival and refund are read from the SpokePools; a refund is read only "
    "once the destination chain has proven the transfer unfilled",
)
FORK_SCOPE = (
    "fork runs and economic backtests only, as the source of a route to eip155:8453; "
    "no production run",
    "a fork run asks no venue: the fork gateway constructs the quote from a stated fee "
    "and the fork's block time and retains it as across_fork_constructed; it is no "
    "evidence of a venue quote",
    "a fork refund is a fork-constructed bundle, never Fork Qualification evidence",
    "a backtest models the bridge and reads no history: it charges the market's whole "
    "max_fee_bps, credits the arrival after the latency the backtest file states, fills "
    "every transfer and models no refund; it keeps no approval, so it serves no revoke_bridge",
)
UNPROVEN = (
    "no run mode: no reference lifecycle sends from this chain; it is the proven "
    "destination of a fork route from eip155:42161 only",
)


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
            restrictions=SCOPE + (FORK_SCOPE if chain in FORK_SOURCES else UNPROVEN),
            historical=historical(chain) if chain in FORK_SOURCES else (),
        )
        for chain in SPOKE_POOLS
    )
