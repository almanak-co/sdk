"""Revision-specific V3 execution and bounded prepared pool capabilities."""

from datetime import UTC, datetime

from almanak.caip import ChainId
from almanak.connectors.support import (
    ONE_ALLOCATION_PER_FUND,
    ConnectorChainSupport,
    EconomicAction,
    HistoricalSupport,
    RunMode,
    op_fee_restrictions,
)
from almanak.connectors.uniswap_v3.economic_data import MODEL

ACTIONS = (
    EconomicAction.SWAP,
    EconomicAction.EXIT_EXCHANGE,
    EconomicAction.REVOKE,
    EconomicAction.OPEN_LIQUIDITY,
    EconomicAction.INCREASE_LIQUIDITY,
    EconomicAction.DECREASE_LIQUIDITY,
    EconomicAction.COLLECT,
    EconomicAction.CLOSE_LIQUIDITY,
)
RESTRICTIONS = (
    "production unavailable pending separately verified production-path accounting",
    "configured verified factory, pool and periphery revisions",
    "explicit token pair and fee; exact-input swaps only",
    ONE_ALLOCATION_PER_FUND,
    "economic execution assumes access; approval and revocation mechanics excluded",
    "broader NFT manager grants can affect compatible owned or approved NFTs; "
    "gateway operations require the retained strategy-owned NFT",
)
CANDIDATE_RESTRICTION = "candidate fork and backtest verification only; not promoted"
# Each boundary is the earliest retained backtest window start among the chain's
# references. Robinhood's composed Morpho reference starts before its standalone
# trading and LP windows, so its boundary follows the composed window.
PROMOTED_STARTS = {
    "eip155:1": datetime(2026, 9, 6, 23, 57, 23, tzinfo=UTC),
    "eip155:8453": datetime(2026, 9, 6, 23, 57, 29, tzinfo=UTC),
    "eip155:42161": datetime(2026, 9, 8, 8, 17, 7, tzinfo=UTC),
    "eip155:10": datetime(2026, 9, 8, 4, 34, 47, tzinfo=UTC),
    "eip155:137": datetime(2026, 9, 13, 17, 51, 22, tzinfo=UTC),
    "eip155:43114": datetime(2026, 9, 9, 3, 42, 27, tzinfo=UTC),
    "eip155:56": datetime(2026, 9, 8, 18, 10, 37, tzinfo=UTC),
    "eip155:4663": datetime(2026, 9, 8, 5, 28, tzinfo=UTC),
}
PROMOTED_CHAINS = tuple(PROMOTED_STARTS)
# Every production cell serves `almanak ax swap` as a direct operator action; each has
# a fork rehearsal of the whole command (tests/fork/test_ax_swap.py).
DIRECT_ACTIONS = (EconomicAction.SWAP,)
PROMOTED_MODES = (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
PRODUCTION_RESTRICTIONS = tuple(
    restriction for restriction in RESTRICTIONS if "production-path accounting" not in restriction
)
# Unregistered chains admitted for private qualification tests only; they advertise
# no public support.
CANDIDATE_STARTS: tuple[tuple[str, datetime], ...] = ()


def declaration(chain, start, restrictions, modes=(RunMode.FORK, RunMode.BACKTEST), direct=()):
    return ConnectorChainSupport(
        chain=ChainId(chain),
        modes=modes,
        actions=ACTIONS,
        account_profiles=("eoa", "safe_roles"),
        historical=(
            HistoricalSupport(
                model=MODEL,
                mode="economic",
                actions=tuple(a for a in ACTIONS if a != EconomicAction.REVOKE),
                backtest_start_at=start,
            ),
        ),
        restrictions=restrictions,
        direct_actions=direct,
    )


def verified_support():
    return tuple(
        declaration(
            chain,
            start,
            PRODUCTION_RESTRICTIONS + op_fee_restrictions(ChainId(chain)),
            PROMOTED_MODES,
            DIRECT_ACTIONS,
        )
        for chain, start in PROMOTED_STARTS.items()
    )


def candidate_support():
    """Verified entries plus unregistered chains for private qualification tests only."""
    return (
        *verified_support(),
        *(
            declaration(chain, start, (CANDIDATE_RESTRICTION, *RESTRICTIONS))
            for chain, start in CANDIDATE_STARTS
        ),
    )
