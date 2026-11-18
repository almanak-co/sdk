"""Verified Comet fork, production and prepared economic declarations on qualified EVM chains."""

from datetime import UTC, datetime

from almanak.caip import ChainId
from almanak.connectors.compound_v3.economic_data import MODEL
from almanak.connectors.support import (
    ConnectorChainSupport,
    EconomicAction,
    HistoricalSupport,
    RunMode,
    op_fee_restrictions,
)

# Promoted by the 24 September 2026 early-promotion acceptance (fourth batch).
PROMOTED_MODES = (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)


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
                    backtest_start_at=datetime(2026, 9, 7, 15, tzinfo=UTC),
                ),
            ),
            restrictions=(
                "verified Comet implementation, extension and immutable asset-list revisions",
                "signed base principal and non-interest-bearing collateral",
                "no rewards or manager authority",
                "scope-only multipurpose ABI grants; economic distinctions remain gateway checks",
            )
            + op_fee_restrictions(ChainId(chain)),
        )
        for chain in ("eip155:1", "eip155:8453", "eip155:42161", "eip155:10")
    )
