"""Qualified Base invariant variants and unregistered candidate declarations."""

from datetime import UTC, datetime

from almanak.caip import ChainId
from almanak.connectors.aerodrome.economic_data import MODEL
from almanak.connectors.identifiers import ProtocolVariant
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
            variants=(ProtocolVariant("stable"), ProtocolVariant("volatile")),
            restrictions=(
                "candidate fork verification only; not promoted",
                "configured verified Aerodrome factory, clone, router and fee contracts",
                "fungible pool shares and separate account-wide fee claims",
                ONE_ALLOCATION_PER_FUND,
                "stable and volatile pools; no staking or reward authority",
            ),
        )
        for chain in ("eip155:8453",)
    )


PROMOTED_START = datetime(2026, 9, 7, 11, 0, tzinfo=UTC)
PROMOTED_MODES = (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
# The production cell serves `almanak ax swap`, the liquidity commands and
# `ax revoke-exchange` as direct operator actions, in stable and in volatile pools;
# each has a fork rehearsal of the whole command (tests/fork/test_ax_swap_aerodrome.py
# and tests/fork/test_ax_liquidity_aerodrome.py).
DIRECT_ACTIONS = (
    EconomicAction.SWAP,
    EconomicAction.OPEN_LIQUIDITY,
    EconomicAction.INCREASE_LIQUIDITY,
    EconomicAction.DECREASE_LIQUIDITY,
    EconomicAction.COLLECT,
    EconomicAction.CLOSE_LIQUIDITY,
    EconomicAction.REVOKE,
)
PRODUCTION_RESTRICTIONS = (
    "configured verified Aerodrome factory, clone, router and fee contracts",
    "fungible pool shares and separate account-wide fee claims",
    "one allocation per fund; no unrelated initial shares, fees or pool LP deposit",
    "stable and volatile pools; no staking or reward authority",
    "economic execution assumes access; approval and revocation mechanics excluded",
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
                        variants=candidate.variants,
                    ),
                ),
                "restrictions": PRODUCTION_RESTRICTIONS + op_fee_restrictions(candidate.chain),
                "direct_actions": DIRECT_ACTIONS,
            }
        )
        for candidate in candidate_support()
    )
