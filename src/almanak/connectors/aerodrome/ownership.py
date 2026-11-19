"""Account-wide LP fee claims cannot be partitioned by allocation names."""

from almanak.connectors.aerodrome.math import accrued
from almanak.connectors.aerodrome.positions import Position
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount


def at_indices(position, pool):
    """Calculate current claims for display or settlement without changing ownership."""
    if position is None:
        return None
    fees = tuple(
        AssetAmount(
            asset=fee.asset,
            amount=accrued(position.shares.amount, current, previous, fee.amount),
        )
        for fee, current, previous in (
            (position.fees0, pool.index0, position.index0),
            (position.fees1, pool.index1, position.index1),
        )
    )
    return Position(
        shares=position.shares,
        fees0=fees[0],
        fees1=fees[1],
        index0=pool.index0,
        index1=pool.index1,
    )


def validate_owned(owned, before):
    """Admit work that is not yet signed against this observation."""
    if owned != before.owned or owned.position != before.position:
        raise DataError("ownership", "observed pool shares or fee claims differ from owned amounts")
    require_inventory(owned, before)


def validate_retained(owned, before):
    """What signed bytes of an adopting allocation still need of an observation: its own
    ownership and its inputs.

    The fund's stored position is not among them. Anyone can send the fund a share, and
    that also moves the fee indices the pool stores for it, for good. Signed bytes held
    to the stored position could then never be sent: an approval or a fee claim carries
    no deadline to release it by. They are sent, and the difference is an accounting
    issue on their outcome.
    """
    if owned != before.owned:
        raise DataError("ownership", "observation differs from retained fund ownership")
    require_inventory(owned, before)


def require_inventory(owned, before):
    if any(
        wallet.amount < inventory.amount
        for wallet, inventory in zip(
            (before.wallet0, before.wallet1), (owned.inventory0, owned.inventory1), strict=True
        )
    ):
        raise DataError("inventory", "fund wallet does not cover attributed inventory")


def require_empty_pool_deposit(pool):
    # Pool.burn consumes its entire own LP balance, including unrelated deposits.
    if pool.deposited_shares:
        raise DataError("ownership", "pool already holds LP shares from another transfer")


def validate_initial(before):
    if before.position is not None or before.owned.position is not None:
        raise DataError("ownership", "initial fund must have no existing pool shares or fee claims")
    validate_owned(before.owned, before)
    require_empty_pool_deposit(before.pool)
