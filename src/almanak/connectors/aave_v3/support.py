"""Revision-specific support backed by retained execution and prepared-model proofs."""

from datetime import UTC, datetime

from almanak.caip import ChainId
from almanak.connectors.aave_v3.economic_data import MODEL
from almanak.connectors.support import (
    ConnectorChainSupport,
    EconomicAction,
    HistoricalSupport,
    RunMode,
)


def verified_support():
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
            modes=(RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST),
            actions=actions,
            account_profiles=("eoa", "safe_roles"),
            restrictions=(
                "verified Pool and reserve-token revision 11 implementations",
                "ordinary collateral mode and variable-rate debt",
                "configured Pool, reserve assets and oracle feeds; no default markets",
            ),
            historical=(
                HistoricalSupport(
                    model=MODEL,
                    mode="economic",
                    actions=actions,
                    backtest_start_at=datetime(2026, 9, 6, 19, tzinfo=UTC),
                ),
            ),
        ),
        ConnectorChainSupport(
            chain=ChainId("eip155:8453"),
            modes=(RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST),
            actions=actions,
            account_profiles=("eoa", "safe_roles"),
            restrictions=(
                "verified Pool and reserve-token revision 11 implementations",
                "ordinary collateral mode and variable-rate debt",
                "pinned Jovian L1 and operator fee accounting",
            ),
            historical=(
                HistoricalSupport(
                    model=MODEL,
                    mode="economic",
                    actions=actions,
                    backtest_start_at=datetime(2026, 9, 6, 22, 28, tzinfo=UTC),
                ),
            ),
        ),
        ConnectorChainSupport(
            chain=ChainId("eip155:42161"),
            modes=(RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST),
            actions=actions,
            account_profiles=("eoa", "safe_roles"),
            restrictions=(
                "verified revision-11 L2Pool, aToken and variable-debt semantics",
                "full runtime fingerprints retain documented proxy metadata differences",
                "ordinary collateral mode and explicit configured oracle sources",
            ),
            historical=(
                HistoricalSupport(
                    model=MODEL,
                    mode="economic",
                    actions=actions,
                    backtest_start_at=datetime.fromtimestamp(1788855427, UTC),
                ),
            ),
        ),
        ConnectorChainSupport(
            chain=ChainId("eip155:10"),
            modes=(RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST),
            actions=actions,
            account_profiles=("eoa", "safe_roles"),
            restrictions=(
                "verified revision-11 L2Pool and reserve-token implementations",
                "complete proxy fingerprints and explicitly configured oracle sources",
            ),
            historical=(
                HistoricalSupport(
                    model=MODEL,
                    mode="economic",
                    actions=actions,
                    backtest_start_at=datetime.fromtimestamp(1788884891, UTC),
                ),
            ),
        ),
        ConnectorChainSupport(
            chain=ChainId("eip155:56"),
            modes=(RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST),
            actions=actions,
            account_profiles=("eoa", "safe_roles"),
            restrictions=(
                "verified revision-11 PoolInstance and reserve-token implementations",
                "complete proxy fingerprints; reserve proxy metadata difference retained",
            ),
            historical=(
                HistoricalSupport(
                    model=MODEL,
                    mode="economic",
                    actions=actions,
                    backtest_start_at=datetime.fromtimestamp(1788891037, UTC),
                ),
            ),
        ),
        ConnectorChainSupport(
            chain=ChainId("eip155:137"),
            modes=(RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST),
            actions=actions,
            account_profiles=("eoa", "safe_roles"),
            restrictions=(
                "verified revision-11 PoolInstance and reserve-token implementations",
                "complete proxy fingerprints and explicitly configured oracle sources",
            ),
            historical=(
                HistoricalSupport(
                    model=MODEL,
                    mode="economic",
                    actions=actions,
                    backtest_start_at=datetime.fromtimestamp(1788895753, UTC),
                ),
            ),
        ),
        ConnectorChainSupport(
            chain=ChainId("eip155:43114"),
            modes=(RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST),
            actions=actions,
            account_profiles=("eoa", "safe_roles"),
            restrictions=(
                "verified revision-11 PoolInstance and reserve-token implementations",
                "complete proxy fingerprints and explicitly configured oracle sources",
            ),
            historical=(
                HistoricalSupport(
                    model=MODEL,
                    mode="economic",
                    actions=actions,
                    backtest_start_at=datetime.fromtimestamp(1788925347, UTC),
                ),
            ),
        ),
    )
