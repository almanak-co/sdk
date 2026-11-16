"""Economic closure of a V3 inventory and position, independent of connector encoding."""

from almanak.connectors.dex_residual import counter_settled


def exit_complete(after):
    """No position, cleanup or counter inventory beyond the residual measured here."""
    return (
        after.owned.position is None
        and not after.needs_cleanup
        and counter_settled(after.owned, after.exit_residual)
    )
