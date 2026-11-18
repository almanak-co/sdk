"""Ethereum fork, production and prepared economic scope with explicit revision restrictions."""

from datetime import UTC, datetime

from almanak.caip import ChainId
from almanak.connectors.sparklend.economic_data import MODEL
from almanak.connectors.support import (
    ConnectorChainSupport,
    EconomicAction,
    HistoricalSupport,
    RunMode,
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
        EconomicAction.COLLATERAL,
    )
    return (
        ConnectorChainSupport(
            chain=ChainId("eip155:1"),
            modes=PROMOTED_MODES,
            actions=actions,
            account_profiles=("eoa", "safe_roles"),
            restrictions=(
                "Ethereum Pool revision 4 and token revision 1",
                "ordinary collateral mode and variable-rate debt",
                "isolated and siloed reserves remain unsupported",
                "configured Pool, assets and oracle feeds; no default markets",
            ),
            historical=(
                HistoricalSupport(
                    model=MODEL,
                    mode="economic",
                    actions=actions,
                    backtest_start_at=datetime(2026, 9, 7, 5, 33, 47, tzinfo=UTC),
                ),
            ),
        ),
    )
