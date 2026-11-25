"""Across chains with a pinned SpokePool, and the run modes each declares.

Every pinned chain is the source of routes to the other pinned chains and declares
its modes from the pin table. One chain's modes rest on a reference lifecycle; the
others rest on read-only mainnet known answers and offline tests, and say so.
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

# Every pinned chain is the source and the destination of routes to the others.
FORK_SOURCES = tuple(SPOKE_POOLS)
# Chains declared for production as the source of a route. The
# production EOA rehearsal and the hosted Fork Qualification select a bridge reference
# from this declaration. No cell's evidence package is complete.
PRODUCTION_SOURCES: tuple[ChainId, ...] = FORK_SOURCES
# What `almanak ax` runs directly from every production source chain. Declared on
# offline evidence: no fork rehearsal of the direct command has run on any cell.
DIRECT_ACTIONS = (EconomicAction.BRIDGE, EconomicAction.REVOKE_BRIDGE)

# The one chain a reference lifecycle, a refund proof and a production rehearsal have
# left from. Every other chain declares its modes on read-only mainnet known answers
# and offline tests alone, an exception accepted for this connector only: nobody has
# run a deposit, an arrival or a refund from there. The QA catalog and the hosted Fork
# Qualification selection leave those cells out by this constant, and `almanak support`
# states it on each of them.
PROVEN_SOURCES = (ChainId("eip155:42161"),)
# The account profiles a source chain declares. A Safe deposits through generated Roles
# grants (`permissions.py`); the receiving account may be an EOA or a Safe on any chain.
ACCOUNT_PROFILES: tuple[Literal["eoa", "safe_roles"], ...] = ("eoa", "safe_roles")
# Profiles declared with no run behind them, on every chain, the proven one included:
# the maintainer declared the Safe/Roles bridge without fork proofs. Its grants were
# never installed in any Roles module, and no Safe has bridged on a fork or on mainnet.
# The QA catalog leaves these profiles out by this constant, the generated fork operator
# file keeps provisioning plain accounts for a bridge, and `almanak support` states it
# on each row. The generated authority is new, so every production cell owes a hosted
# Fork Qualification that exercises it, and none has run.
UNPROVEN_PROFILES: tuple[Literal["eoa", "safe_roles"], ...] = ("safe_roles",)
# The bridge model of economic backtests. It reads no history: it charges the author's
# whole fee ceiling and credits the arrival after a latency the backtest file states.
BACKTEST_MODEL: Literal["across-full-fee-ceiling-fixed-latency-v1"] = (
    "across-full-fee-ceiling-fixed-latency-v1"
)
# When each pinned SpokePool implementation was installed: the first block whose proxy
# EIP-1967 implementation slot holds the pinned address, found by bisection over
# `eth_getStorageAt` on an archive node (`scripts/across_pins.py`, 25 November 2026).
# Blocks: Arbitrum 494538558, Base 49971488, Ethereum 25754941, OP Mainnet 155565570,
# BNB Smart Chain 115920366, Polygon 92015968, Robinhood Chain 35828194 and Avalanche
# 92752373.
PIN_INSTALLED = {
    ChainId("eip155:42161"): datetime(2026, 8, 14, 18, 24, 47, tzinfo=UTC),
    ChainId("eip155:8453"): datetime(2026, 8, 14, 18, 32, 3, tzinfo=UTC),
    ChainId("eip155:1"): datetime(2026, 8, 14, 18, 2, 35, tzinfo=UTC),
    ChainId("eip155:10"): datetime(2026, 8, 14, 17, 51, 57, tzinfo=UTC),
    ChainId("eip155:56"): datetime(2026, 8, 14, 16, 27, 26, tzinfo=UTC),
    ChainId("eip155:137"): datetime(2026, 8, 14, 17, 36, 19, tzinfo=UTC),
    ChainId("eip155:4663"): datetime(2026, 8, 14, 0, 39, 28, tzinfo=UTC),
    ChainId("eip155:43114"): datetime(2026, 8, 14, 0, 0, 31, tzinfo=UTC),
}


def declared_without_proof() -> tuple[ChainId, ...]:
    """The declared source chains no reference lifecycle has left from."""
    return tuple(chain for chain in FORK_SOURCES if chain not in PROVEN_SOURCES)


def profiles_without_proof() -> tuple[str, ...]:
    """The declared account profiles no bridge has run with, on any chain."""
    return tuple(profile for profile in ACCOUNT_PROFILES if profile in UNPROVEN_PROFILES)


def backtest_start(source: ChainId) -> datetime | None:
    """The first whole day on which every pinned implementation was in place.

    A backtest resolves a route at its window start, on both chains, and first
    resolution refuses another implementation. A route may reach any pinned chain, so
    the latest installation among all of them decides, whichever chain it is. While
    any pinned chain's installation is unread no chain declares a start: its routes
    could not be dated.
    """
    if source not in PIN_INSTALLED or set(SPOKE_POOLS) - set(PIN_INSTALLED):
        return None
    installed = max(PIN_INSTALLED.values())
    return datetime(installed.year, installed.month, installed.day, tzinfo=UTC) + timedelta(days=1)


BACKTEST_STARTS = {
    source: start for source in FORK_SOURCES if (start := backtest_start(source)) is not None
}


SCOPE = (
    "ERC-20 routes between two bound accounts; no wrapped native asset is sent",
    "a Safe/Roles account deposits under scope-only grants that pin the SpokePool, both "
    "tokens, the destination chain, the Safe as depositor, the destination fund as "
    "recipient and an empty message; Roles cannot relate the output amount to the input",
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
# A fork refund impersonates the SpokePool's admin, which each chain's SpokePool variant
# admits differently. Only this chain's is built.
FORK_REFUND_SOURCES = (ChainId("eip155:42161"),)
FORK_REFUND = {
    True: "a fork refund is a fork-constructed bundle, never Fork Qualification evidence",
    False: "no fork refund is defined for a transfer sent from this chain",
}
# What an author or operator meets on one chain and on no other.
CHAIN_NOTES = {
    ChainId("eip155:8453"): (
        "a fork refuses authored gas ceilings on this chain, so a fork run uses an "
        "author file without a gas policy",
    ),
    ChainId("eip155:10"): (
        "a fork refuses authored gas ceilings on this chain, so a fork run uses an "
        "author file without a gas policy",
    ),
    ChainId("eip155:137"): (
        "the network states no finality margin, so a signed step whose nonce another "
        "transaction consumed is never released on this chain",
    ),
}
WITHOUT_PROOF = (
    "declared on read-only mainnet known answers and offline tests: no reference "
    "lifecycle, refund proof or production rehearsal has run from this chain",
)


PROFILE_WITHOUT_PROOF = {
    "safe_roles": (
        "Safe/Roles accounts are declared with no run behind them: the generated grants "
        "were never installed, and no Safe has bridged on a fork or on mainnet"
    ),
}


def mode_scope(mode: RunMode, chain: ChainId) -> tuple[str, ...]:
    if mode is RunMode.FORK:
        return (*MODE_SCOPE[mode], FORK_REFUND[chain in FORK_REFUND_SOURCES])
    return MODE_SCOPE[mode]


def restrictions(chain: ChainId) -> tuple[str, ...]:
    """The chain's scope in words, from the modes it declares and nothing else."""
    declared = modes(chain)
    names = [MODE_NAMES[mode] for mode in declared]
    listed = names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]
    absent = "" if RunMode.PRODUCTION in declared else "; no production run"
    source = f"{listed} only, as the source of a route to another pinned chain{absent}"
    scoped = tuple(text for mode in declared for text in mode_scope(mode, chain))
    unproven = WITHOUT_PROOF if chain in declared_without_proof() else ()
    profiles = tuple(PROFILE_WITHOUT_PROOF[profile] for profile in profiles_without_proof())
    return SCOPE + (source, *scoped, *CHAIN_NOTES.get(chain, ()), *unproven, *profiles)


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
    declared = {
        RunMode.FORK: chain in FORK_SOURCES,
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
            account_profiles=ACCOUNT_PROFILES,
            direct_actions=DIRECT_ACTIONS if chain in PRODUCTION_SOURCES else (),
            restrictions=restrictions(chain),
            historical=historical(chain),
        )
        for chain in SPOKE_POOLS
    )
