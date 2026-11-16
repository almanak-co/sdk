"""Entry funding requirements, kept separate from the balances they are compared with.

A requirement is a pure function of resolved parameters, so a check can report it
before any account is observed. Preflight compares the same requirements with
measured wallet balances; a missing measurement is never treated as zero.
"""

from collections.abc import Iterable

from almanak.caip import AssetType
from almanak.connectors.gas_lifecycle import lending_lifecycle
from almanak.models import AssetAmount, Model, UInt


class TokenRequirement(Model):
    """Base units of one asset a fund account must hold before its first entry."""

    asset: AssetType
    amount: UInt
    basis: str


def summed(amounts: Iterable[AssetAmount], basis: str) -> tuple[TokenRequirement, ...]:
    """Total each asset once, in first-seen order, so shared balances are not double counted."""
    totals: dict[AssetType, int] = {}
    for value in amounts:
        totals[value.asset] = totals.get(value.asset, 0) + value.amount
    return tuple(
        TokenRequirement(asset=asset, amount=amount, basis=basis)
        for asset, amount in totals.items()
    )


def lending_requirements(config, account) -> tuple[TokenRequirement, ...]:
    """Each allocation's configured `capital` of its reserve asset, per asset.

    Interest is not prefunded: a repayment the fund cannot cover refuses with its
    shortfall, and the runtime reports that shortfall while debt is open.
    """
    return summed(
        (
            AssetAmount(asset=position.reserve.asset, amount=position.capital)
            for position in config.positions
            if position.account == account
        ),
        "configured allocation capital",
    )


def capital_requirements(config, account) -> tuple[TokenRequirement, ...]:
    """The single configured capital amount of each position allocation in the account."""
    return summed(
        (allocation.capital for allocation in config.allocations if allocation.account == account),
        "configured position capital",
    )


def payer_accounts(config, payer) -> tuple[str, ...]:
    """Bound account names whose submissions this payer signs and pays for."""
    return tuple(b.name for b in config.accounts.bindings if b.profile.submitter == payer)


def admission_reserve(policy) -> int:
    """Native units fee admission reserves for an exposure-increasing submission."""
    if policy is None:
        return 0
    return policy.max_transaction_fee.amount + policy.exit_reserve.amount


def entry_reserve(policy, entries: int) -> int:
    """Native units that let `entries` entry transactions all pass fee admission.

    Every transaction pays its fee from the payer, and admission refuses an
    exposure-increasing one unless the payer still holds its fee ceiling plus
    `exit_reserve`. When the last of `entries` adds exposure, it needs every earlier
    fee spent on top of its own fee and the reserve.
    """
    if policy is None:
        return 0
    return max(1, entries) * policy.max_transaction_fee.amount + policy.exit_reserve.amount


def payer_entry_transactions(config, accounts) -> int:
    """Transactions entering the payer's accounts can submit, through the last that adds exposure.

    Each connector states, per position, an upper bound for one pass through every
    entry action the position is granted, from a fresh fund account with no
    allowance or position: every transaction its planner emits up to and including
    the last one its `increases_exposure` classifies as adding exposure. A
    transaction that adds no exposure (an approval of the other pool currency) still
    pays its fee from the payer before the mint that follows it, so it counts. Adding
    to a position, re-entering after an exit or rebalancing submits more; those are
    not part of entry.
    """
    from almanak.connectors.composition import connector_for

    execution = connector_for(config).execution
    return sum(execution.entry_transactions(config, account) for account in accounts)


def position_entries(config, account, count) -> int:
    """Sum `count(position)` over the positions of one account."""
    return sum(count(position) for position in config.positions if position.account == account)


def lifecycle_entries(lifecycle, classify):
    """An account's entry count, each position counted by its kind in `lifecycle`.

    `classify(position)` returns the position's kind and the keyword arguments of
    `KindCounts.entries`: its entry slices, item counts and granted permissions.
    """

    def count(position) -> int:
        kind, counts = classify(position)
        return lifecycle.kind(kind).entries(**counts)

    def entries(config, account) -> int:
        return position_entries(config, account, count)

    return entries


def lending_entries(lifecycle):
    """Supply approval and supply, then each granted permission the lifecycle counts.

    Supply is always granted, so every allocation counts it; Aave V3 and SparkLend
    count an explicit collateral enablement when `collateral` is granted.
    """
    granted = tuple(lifecycle.kind("lending").when_granted)
    return lifecycle_entries(
        lifecycle,
        lambda position: (
            "lending",
            {"granted": tuple(name for name in granted if getattr(position, name, False))},
        ),
    )


LENDING_LIFECYCLE = lending_lifecycle("collateral", "borrow")
lending_entry_transactions = lending_entries(LENDING_LIFECYCLE)


def entry_slices(capital: int, entry_swap_bps: int) -> int:
    """Entry swaps an allocation's `capital` pays for.

    Each entry quote offers `capital x entry_swap_bps / 10000`, floored, of the funding
    asset while funding inventory remains, so spending the capital takes the ceiling
    of capital over that slice. A zero slice is never offered, so it swaps nothing.
    Inventory beyond `capital` (a composed strategy's shared or borrowed funding
    balance) is offered in further slices; those draw on what entry leaves.
    """
    size = capital * entry_swap_bps // 10000
    return -(-capital // size) if size else 0


def exchange_entry_transactions(lifecycle, capital=None, items=None):
    """An account's entry count for exchange connectors.

    An entry slice submits the trade count: its approvals and the swap. A trade
    allocation repeats them per slice. A liquidity allocation is offered the same
    slices before it mints, then submits the rest of its liquidity count; `items`
    reads the allocation's per-item counts. `capital` reads an allocation's funding
    capital in base units. A banded allocation enters through one `rebalance_quote`
    swap sized from the centred position, so it counts at least one swap even when
    `entry_swap_bps` offers no entry slice.
    """
    capital = capital or (lambda position: position.capital.amount)

    def classify(position):
        slices = entry_slices(capital(position), position.entry_swap_bps)
        if getattr(position, "band", None) is not None:
            slices = max(1, slices)
        if not position.liquidity_enabled:
            return "trade", {"slices": slices}
        return "liquidity", {"slices": slices, "items": items(position) if items else None}

    return lifecycle_entries(lifecycle, classify)


def native_threshold(config, accounts) -> int:
    """The largest allocation `minimum_native_balance` among the payer's accounts."""
    return max(
        (
            getattr(position, "minimum_native_balance", 0)
            for position in config.positions
            if position.account in accounts
        ),
        default=0,
    )


def native_entry_minimum(policy, config, accounts, entries: int) -> int:
    """Native units a payer must hold to complete its accounts' entry.

    `entries` is `payer_entry_transactions` for the same accounts. Each of those
    transactions pays its fee, and the last adds exposure and must pass fee admission,
    so the payer needs one fee ceiling per entry transaction plus `exit_reserve`. Each allocation's
    `minimum_native_balance` is an independent check on the same payer balance, so
    the requirement is the larger of the two.
    """
    return max(entry_reserve(policy, entries), native_threshold(config, accounts))


def funding_rows(requirements, balances: dict) -> tuple[dict, ...]:
    """Preflight rows comparing each requirement with its measured base-unit balance."""
    return tuple(funding_row(value, balances.get(value.asset)) for value in requirements)


def funding_row(requirement: TokenRequirement, observed: int | None) -> dict:
    return {
        "asset": str(requirement.asset),
        "required_token_base_units": str(requirement.amount),
        "observed_token_base_units": None if observed is None else str(observed),
        "funded": observed is not None and observed >= requirement.amount,
        "requirement": requirement.basis,
    }
