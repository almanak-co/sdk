"""An allocation cannot partition the same fund's fungible pool share balance."""

from almanak.gateway.ethereum import DataError


def validate_owned(owned, before):
    """Admit work that is not yet signed against this observation."""
    refuse_unowned_shares(before)
    validate_retained(owned, before)


def validate_retained(owned, before):
    if owned != before.owned or owned.position != before.position:
        raise DataError("ownership", "Curve observed LP shares differ from retained fund ownership")
    if any(
        wallet.amount < inventory.amount
        for wallet, inventory in zip(before.wallets.root, owned.inventory.root, strict=True)
    ):
        raise DataError("inventory", "Curve fund wallet does not cover attributed inventory")


def unowned_shares(shares) -> DataError:
    return DataError(
        "ownership",
        f"the fund wallet holds {shares.amount} base units of this pool's LP token "
        f"{shares.asset}, which an allocation without liquidity authority does not own; "
        "move them out of the wallet before acting in this pool",
    )


def refuse_unowned_shares(before):
    """Refuse to start, plan or sign beside LP shares the allocation cannot own.

    Nothing is at stake before a signature, so the fund is refused until the shares
    are gone. It bounds nothing after one: anyone can send the fund the pool's LP
    token, so signed bytes are still sent and a mined step is still reconciled, with
    the shares recorded as an accounting issue on its outcome.
    """
    if before.unowned_shares is not None:
        raise unowned_shares(before.unowned_shares)


def validate_initial(before):
    if before.position is not None or before.owned.position is not None:
        raise DataError("ownership", "initial Curve fund must have no existing shares in this pool")
    if before.needs_cleanup:
        raise DataError(
            "authority", "initial Curve fund must have no existing allowance to this pool"
        )
    validate_owned(before.owned, before)
