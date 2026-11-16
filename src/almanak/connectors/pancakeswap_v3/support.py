"""Qualified anchor support and unregistered declarations for candidate verification."""

from datetime import UTC, datetime

from almanak.caip import ChainId
from almanak.connectors.pancakeswap_v3.economic_data import MODEL
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
                "configured verified Pancake original router, factory, deployer and pool",
                ONE_ALLOCATION_PER_FUND,
                "absent or compiled verified liquidity-mining callback; unstaked NFTs",
                "Safe LP grants allow broader NFT scope; gateway binds retained ownership",
            ),
        )
        for chain in ("eip155:56", "eip155:1", "eip155:8453", "eip155:42161")
    )


PROMOTED_STARTS = {
    ChainId("eip155:56"): datetime(2026, 9, 7, 7, 30, tzinfo=UTC),
    ChainId("eip155:1"): datetime(2026, 9, 7, 9, 40, tzinfo=UTC),
    ChainId("eip155:8453"): datetime(2026, 9, 7, 9, 20, tzinfo=UTC),
    ChainId("eip155:42161"): datetime(2026, 9, 7, 9, 40, tzinfo=UTC),
}
PROMOTED_MODES = (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
PRODUCTION_RESTRICTIONS = (
    "configured verified Pancake original router, factory, deployer and pool",
    ONE_ALLOCATION_PER_FUND,
    "absent or compiled verified liquidity-mining callback; unstaked NFTs",
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
                        backtest_start_at=PROMOTED_STARTS[candidate.chain],
                    ),
                ),
                "restrictions": PRODUCTION_RESTRICTIONS + op_fee_restrictions(candidate.chain),
            }
        )
        for candidate in candidate_support()
    )
