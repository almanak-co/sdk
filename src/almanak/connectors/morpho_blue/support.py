"""Verified Morpho fork, production and prepared economic declarations on qualified EVM chains."""

from datetime import UTC, datetime

from almanak.caip import ChainId
from almanak.connectors.morpho_blue.economic_data import MODEL
from almanak.connectors.support import (
    ConnectorChainSupport,
    EconomicAction,
    HistoricalSupport,
    RunMode,
    op_fee_restrictions,
)

NON_VAULT_ORACLE_RESTRICTION = (
    "verified Morpho Blue core, adaptive IRM and explicit non-vault oracle revision"
)
ERC4626_ORACLE_RESTRICTION = (
    "verified Morpho Blue core, adaptive IRM and explicit ERC-4626 conversion oracle revision"
)
# Promoted by the 24 September 2026 early-promotion acceptance (fourth batch), and
# HyperEVM by its 20 October 2026 acceptance.
PROMOTED_MODES = (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
FOURTH_BATCH_START = datetime(2026, 9, 7, 15, tzinfo=UTC)
# Each chain's historical boundary is the start of its first retained prepared window.
BACKTEST_STARTS = {
    "eip155:1": FOURTH_BATCH_START,
    "eip155:8453": FOURTH_BATCH_START,
    "eip155:42161": FOURTH_BATCH_START,
    "eip155:10": FOURTH_BATCH_START,
    "eip155:137": FOURTH_BATCH_START,
    "eip155:4663": FOURTH_BATCH_START,
    "eip155:56": FOURTH_BATCH_START,
    "eip155:43114": FOURTH_BATCH_START,
    "eip155:999": datetime(2026, 10, 20, 4, tzinfo=UTC),
}


def candidate_support():
    actions = (
        EconomicAction.SUPPLY,
        EconomicAction.WITHDRAW,
        EconomicAction.WITHDRAW_AMOUNT,
        EconomicAction.BORROW,
        EconomicAction.REPAY,
        EconomicAction.REPAY_AMOUNT,
    )
    return tuple(
        ConnectorChainSupport(
            chain=ChainId(chain),
            modes=PROMOTED_MODES,
            actions=actions,
            account_profiles=("eoa", "safe_roles"),
            historical=(
                HistoricalSupport(
                    model=MODEL,
                    mode="economic",
                    actions=actions,
                    backtest_start_at=start,
                ),
            ),
            restrictions=(
                (
                    ERC4626_ORACLE_RESTRICTION
                    if chain == "eip155:43114"
                    else NON_VAULT_ORACLE_RESTRICTION
                ),
                "separate supply shares, borrow shares and non-interest-bearing collateral",
                "no rewards, callbacks, bundlers or delegated managers",
                "scope-only supply and borrow grants with no amount bound; "
                "health and ownership remain gateway checks",
            )
            + op_fee_restrictions(ChainId(chain)),
        )
        for chain, start in BACKTEST_STARTS.items()
    )
