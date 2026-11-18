"""Exit stages derive from retained ownership and newly eligible quote evidence."""

from almanak.connectors.uniswap_v4.authoring import close_liquidity, swap
from almanak.connectors.uniswap_v4.models import Target
from almanak.connectors.uniswap_v4.observation_models import counter_settled
from almanak.dex_intents import RevokeExchange


def remaining(owned, cleanup, residual=None):
    """Exit obligations; counter inventory is clear only as a residual measured with it."""
    return owned.position is not None or cleanup or not counter_settled(owned, residual)


def cleared(observed):
    return not remaining(observed.owned, observed.needs_cleanup, observed.exit_residual)


def obligations(observed):
    if observed is None:
        return "unknown"
    return "clear" if cleared(observed) else "remaining"


def leg(config, observed):
    allocation = config.allocation(observed.allocation)
    if observed.owned.position is not None:
        return close_liquidity(allocation, observed)
    if not counter_settled(observed.owned, observed.exit_residual):
        return swap(allocation, observed, observed.exit_quote)
    if observed.needs_cleanup:
        return RevokeExchange[Target](target=observed.target)
    raise ValueError("exchange allocation already has verified economic closure")
