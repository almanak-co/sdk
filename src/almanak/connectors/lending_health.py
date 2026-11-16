"""The minimum health factor of a reserve allocation, required only where debt is possible.

Aave V3, SparkLend, Morpho and Comet risk checks share one policy per fund and market
scope. An allocation that cannot borrow, in a scope where no allocation borrows, may
omit it: the floor then tolerates no debt at all.
"""

from almanak.configuration.input_values import Quantity
from almanak.configuration.positions import converted

# A debt-free Aave V3 or SparkLend position reports the uint256 maximum, and Morpho and
# Comet report no health factor without debt, so every position carrying debt is below it.
NO_DEBT_FLOOR = 2**256 - 1

MINIMUM_HEALTH_FACTOR_DESCRIPTION = (
    "Minimum projected health factor for an action that adds risk. Required when borrow "
    "is true; every allocation sharing that fund and market then states the same value. "
    "Omit it on an allocation that cannot borrow in a scope where none borrows (lend- or "
    "supply-only): there is no debt for it to bound, and while the fund carries any debt "
    "in that market an action the connector risk-checks is refused."
)


def health_floor(minimum: int | None) -> int:
    """The floor risk checks compare against; an omitted minimum tolerates no debt."""
    return NO_DEBT_FLOOR if minimum is None else minimum


def require_borrow_floor(borrow: bool, minimum: int | None) -> None:
    if borrow and minimum is None:
        raise ValueError("an allocation with borrow: true requires minimum_health_factor")


def health_factor_input(value: Quantity | None) -> int | None:
    """Convert the author ratio at wad precision; it is never an amount of an asset."""
    return None if value is None else converted(value, 18, "minimum_health_factor")
