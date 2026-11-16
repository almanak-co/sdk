"""Per-connector transaction counts of the gas lifecycle, one table per connector.

Each connector's `Execution.gas_lifecycle` names its allocation kinds. A kind's
`entry` is the funding count for one entry with a single entry slice and none of its
optional items; `payer_entry_transactions` reads it through the connector's own
`entry_transactions`, and `almanak support` prints the same table, so the counts an
author sizes gas with before a configuration exists are the ones `almanak check`
then reports. `exit` is the transactions one exit submits after that entry: closing a
position, the swap back to the funding asset with its approvals, repaying a debt
with its approval and the revocation of what the repayment leaves, and revoking the
allowance the exit or the mint leaves. `almanak check` sizes `exit_reserve` with it.
`exit_per_unwind_cycle` is what one deleverage cycle (withdraw part of the collateral,
swap it, repay part of the debt) submits on the lending connector; the configuration
cannot say how many cycles a strategy's unwind takes, so `check` counts none.
"""

from pydantic import Field

from almanak.models import Model

# Lower bounds on the transactions one allocation submits to enter, from the V3 pool
# planner (connectors/v3_pool_planning.py) on a fresh fund account: a trade approves
# the router and swaps; a liquidity allocation also approves the position manager at
# least once and mints. Any other allocation or debt needs at least one transaction
# in and one out. `almanak check` sizes `total_budget` with these entry floors and
# `exit_reserve` with each kind's table `exit`.
TRADE_ENTRY = 2
LIQUIDITY_ENTRY = 4
MINIMUM_LIFECYCLE = (1, 1)
MINIMUM_EXIT = MINIMUM_LIFECYCLE[1]


class KindCounts(Model):
    """Transactions of one allocation kind.

    Entry is `entry`, plus `per_entry_slice` for each entry slice past the first,
    plus `per_item` for each counted item the connector names, plus
    `when_granted` for each named permission the allocation enables. Exit is `exit`,
    plus `exit_per_item` and `exit_when_granted` read the same way.
    `exit_per_unwind_cycle` is the lending side of one partial deleverage cycle:
    a partial withdrawal, then the approval and the partial repayment.
    """

    entry: int = Field(ge=0)
    exit: int = Field(ge=0)
    per_entry_slice: int = Field(default=0, ge=0)
    per_item: dict[str, int] = {}
    when_granted: dict[str, int] = {}
    exit_per_item: dict[str, int] = {}
    exit_when_granted: dict[str, int] = {}
    exit_per_unwind_cycle: int = Field(default=0, ge=0)

    def entries(self, *, slices: int = 1, items=None, granted=()) -> int:
        return (
            self.entry
            + self.per_entry_slice * (slices - 1)
            + sum(self.per_item[name] * count for name, count in (items or {}).items())
            + sum(self.when_granted[name] for name in granted)
        )

    def exits(self, *, items=None, granted=()) -> int:
        return (
            self.exit
            + sum(self.exit_per_item.get(name, 0) * count for name, count in (items or {}).items())
            + sum(self.exit_when_granted.get(name, 0) for name in granted)
        )


class GasLifecycle(Model):
    kinds: dict[str, KindCounts]

    def kind(self, name: str) -> KindCounts:
        return self.kinds[name]


def exchange_lifecycle(
    trade: int,
    liquidity: int,
    *,
    exits=(MINIMUM_EXIT, MINIMUM_EXIT),
    liquidity_items=None,
) -> GasLifecycle:
    """A swap-and-pool connector: every entry slice submits `trade` transactions."""
    return GasLifecycle(
        kinds={
            "trade": KindCounts(entry=trade, exit=exits[0], per_entry_slice=trade),
            "liquidity": KindCounts(
                entry=liquidity,
                exit=exits[1],
                per_entry_slice=trade,
                per_item=liquidity_items or {},
            ),
        }
    )


# A borrowing allocation exits by approving its held balance, repaying the whole debt
# and revoking what the repayment leaves of that approval: 1 + 2. A partial
# deleverage cycle withdraws part of the collateral and approves the exact partial
# repayment, which the repayment consumes: 3, besides its swap. Aave V3, SparkLend,
# Compound V3 and Morpho Blue plan these alike (their planning.py repayment and
# withdrawal steps); the rig's leverage-loop fork verdicts submit exactly these.
BORROW_EXIT = 2
UNWIND_CYCLE = 3


def lending_lifecycle(*granted: str) -> GasLifecycle:
    """Supply approval and supply, then one transaction for each granted permission.

    The exit withdraws; a borrowing allocation repays instead, with its approval and
    revocation, and can unwind in partial cycles.
    """
    return GasLifecycle(
        kinds={
            "lending": KindCounts(
                entry=2,
                exit=MINIMUM_EXIT,
                when_granted=dict.fromkeys(granted, 1),
                exit_when_granted={"borrow": BORROW_EXIT},
                exit_per_unwind_cycle=UNWIND_CYCLE,
            )
        }
    )


# Exits, as the fork verdicts of the rig's reference runs record them:
# V3-style pools: a trade approves and swaps back; a liquidity allocation closes,
# approves and swaps, then revokes each position-manager allowance the mint left
# (v3_pool_planning.Planner.cleanup, one per call). The mint approves both currencies
# for their maxima and pulls amounts rounded from its liquidity, so it can leave a
# residual on both; the borrow-LP forks revoked two, a banded Slipstream fork one.
# A reserve must cover the case it cannot rule out, so the exit counts both.
V3_POOL_LIFECYCLE = exchange_lifecycle(2, 5, exits=(2, 5))
# Uniswap V4: the swap back approves Permit2 and the router's Permit2 allowance, swaps
# and revokes that unexpired Permit2 allowance; a liquidity allocation burns first.
UNISWAP_V4_LIFECYCLE = exchange_lifecycle(3, 8, exits=(4, 5))
# Aerodrome classic: a liquidity allocation approves its pool shares to the router,
# removes them, approves and swaps back, then revokes the mint's residual allowance.
AERODROME_LIFECYCLE = exchange_lifecycle(2, 5, exits=(2, 5))
# Curve: a liquidity allocation removes to its configured exit coin, then approves and
# swaps it back to the funding asset; a trade approves and swaps back.
CURVE_LIFECYCLE = exchange_lifecycle(2, 3, exits=(2, 3), liquidity_items={"coins": 1})
