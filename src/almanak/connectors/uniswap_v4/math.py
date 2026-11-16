"""V4 entry sizing using the shared, contract-compared integer arithmetic."""

from almanak.connectors.concentrated_math import (
    Q96 as Q96,
)
from almanak.connectors.concentrated_math import (
    Q128 as Q128,
)
from almanak.connectors.concentrated_math import (
    TICK_FACTORS as TICK_FACTORS,
)
from almanak.connectors.concentrated_math import (
    UINT256 as UINT256,
)
from almanak.connectors.concentrated_math import (
    accrued as accrued,
)
from almanak.connectors.concentrated_math import (
    amounts as amounts,
)
from almanak.connectors.concentrated_math import (
    divide as divide,
)
from almanak.connectors.concentrated_math import (
    liquidity_for as liquidity_for,
)
from almanak.connectors.concentrated_math import (
    sqrt_at_tick as sqrt_at_tick,
)


def entry_split(
    budget: int, price: int, lower: int, upper: int, quote, *, funding0: bool = True
) -> int:
    """One entry funded by either pool currency; quote calls are pinned to one decision block.

    Compare each leg's liquidity at a fixed observed price. This is sizing, not
    a promise of the eventual mint ratio; actual fills and mint state prevail.
    """
    if not 0 < budget < 2**128:
        raise ValueError("entry budget must be positive uint128")
    if price <= lower:
        return 0 if funding0 else budget
    if price >= upper:
        return budget if funding0 else 0
    lo, hi = 0, budget
    while hi - lo > 1:
        middle = (lo + hi) // 2
        output = quote(middle)
        left, right = entry_liquidity(budget - middle, output, price, lower, upper, funding0)
        if left > right:
            lo = middle
        else:
            hi = middle
    return hi


def entry_liquidity(remaining, output, price, lower, upper, funding0):
    scale0 = price * upper // Q96
    if funding0:
        return remaining * scale0 // (upper - price), output * Q96 // (price - lower)
    return remaining * Q96 // (price - lower), output * scale0 // (upper - price)
