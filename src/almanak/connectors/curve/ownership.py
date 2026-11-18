"""An allocation cannot partition the same fund's fungible pool share balance."""

from almanak.gateway.ethereum import DataError


def validate_owned(owned, before):
    if owned != before.owned or owned.position != before.position:
        raise DataError("ownership", "Curve observed LP shares differ from retained fund ownership")
    if any(
        wallet.amount < inventory.amount
        for wallet, inventory in zip(before.wallets.root, owned.inventory.root, strict=True)
    ):
        raise DataError("inventory", "Curve fund wallet does not cover attributed inventory")


def validate_initial(before):
    if before.position is not None or before.owned.position is not None:
        raise DataError("ownership", "initial Curve fund must have no existing shares in this pool")
    if before.needs_cleanup:
        raise DataError(
            "authority", "initial Curve fund must have no existing allowance to this pool"
        )
    validate_owned(before.owned, before)
