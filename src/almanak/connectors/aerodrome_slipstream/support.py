"""Qualified Base support and unregistered declarations for candidate verification."""

from datetime import UTC, datetime

from almanak.caip import ChainId
from almanak.connectors.aerodrome_slipstream.economic_data import MODEL
from almanak.connectors.support import (
    ONE_ALLOCATION_PER_FUND,
    ConnectorChainSupport,
    EconomicAction,
    HistoricalSupport,
    RunMode,
    op_fee_restrictions,
)


def candidate_support():
    return (
        ConnectorChainSupport(
            chain=ChainId(chain),
            modes=(RunMode.FORK,),
            actions=(
                EconomicAction.SWAP,
                EconomicAction.OPEN_LIQUIDITY,
                EconomicAction.INCREASE_LIQUIDITY,
                EconomicAction.DECREASE_LIQUIDITY,
                EconomicAction.COLLECT,
                EconomicAction.CLOSE_LIQUIDITY,
                EconomicAction.EXIT_EXCHANGE,
                EconomicAction.REVOKE,
            ),
            account_profiles=("eoa", "safe_roles"),
            restrictions=(
                "candidate fork verification only; not promoted",
                "configured compiled Slipstream factory, clone, periphery and fee modules",
                ONE_ALLOCATION_PER_FUND,
                "unstaked NFTs and undiscounted submitter fee context",
                "Safe LP grants allow broader NFT scope; gateway binds retained ownership",
            ),
        )
        for chain in ("eip155:8453",)
    )


PROMOTED_START = datetime(2026, 9, 7, 10, 0, tzinfo=UTC)
PROMOTED_MODES = (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
PRODUCTION_RESTRICTIONS = (
    "configured compiled Slipstream factory, clone, periphery and fee modules",
    ONE_ALLOCATION_PER_FUND,
    "unstaked NFTs and undiscounted submitter fee context",
    "economic execution assumes access; approval and revocation mechanics excluded",
    "broader NFT manager grants can affect compatible owned or approved NFTs; "
    "gateway operations require the retained strategy-owned NFT",
)


def verified_support():
    return tuple(
        ConnectorChainSupport.model_validate(
            {
                **candidate.model_dump(),
                "modes": PROMOTED_MODES,
                "historical": (
                    HistoricalSupport(
                        model=MODEL,
                        mode="economic",
                        actions=tuple(a for a in candidate.actions if a != EconomicAction.REVOKE),
                        backtest_start_at=PROMOTED_START,
                    ),
                ),
                "restrictions": PRODUCTION_RESTRICTIONS + op_fee_restrictions(candidate.chain),
            }
        )
        for candidate in candidate_support()
    )
