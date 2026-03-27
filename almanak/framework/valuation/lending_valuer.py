"""Pure math for Aave V3 lending position valuation.

Values supply (aToken) and borrow (debt) positions using on-chain balances
and live USD prices. No I/O — all data is passed in.

Supply value = aToken balance (in human units) * token price USD
Debt value   = (stable debt + variable debt) (in human units) * token price USD
Net value    = supply value - debt value
"""

import logging
from dataclasses import dataclass
from decimal import Decimal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LendingPositionValue:
    """Valued lending position for a single asset reserve.

    A user can have both supply and borrow on the same asset.
    Net value = supply_value_usd - debt_value_usd.
    """

    asset: str
    supply_balance: Decimal  # Human-readable (e.g., 1.5 WETH)
    supply_value_usd: Decimal
    stable_debt_balance: Decimal
    variable_debt_balance: Decimal
    debt_value_usd: Decimal
    net_value_usd: Decimal
    collateral_enabled: bool
    decimals: int


def value_lending_position(
    *,
    atoken_balance: int,
    stable_debt: int,
    variable_debt: int,
    token_price_usd: Decimal,
    token_decimals: int,
    collateral_enabled: bool = True,
    asset: str = "",
) -> LendingPositionValue:
    """Value a single Aave V3 reserve position.

    Args:
        atoken_balance: Raw aToken balance (wei). Includes accrued interest.
        stable_debt: Raw stable debt balance (wei).
        variable_debt: Raw variable debt balance (wei).
        token_price_usd: Current USD price for the underlying token.
        token_decimals: Decimals for the underlying token.
        collateral_enabled: Whether this asset is used as collateral.
        asset: Asset symbol for labeling.

    Returns:
        LendingPositionValue with computed USD values.
    """
    divisor = Decimal(10**token_decimals)

    supply_balance = Decimal(atoken_balance) / divisor
    stable_balance = Decimal(stable_debt) / divisor
    variable_balance = Decimal(variable_debt) / divisor

    supply_value = supply_balance * token_price_usd
    debt_value = (stable_balance + variable_balance) * token_price_usd
    net_value = supply_value - debt_value

    return LendingPositionValue(
        asset=asset,
        supply_balance=supply_balance,
        supply_value_usd=supply_value,
        stable_debt_balance=stable_balance,
        variable_debt_balance=variable_balance,
        debt_value_usd=debt_value,
        net_value_usd=net_value,
        collateral_enabled=collateral_enabled,
        decimals=token_decimals,
    )


def value_lending_portfolio(
    positions: list[LendingPositionValue],
) -> tuple[Decimal, Decimal, Decimal]:
    """Aggregate multiple lending positions into portfolio totals.

    Args:
        positions: List of valued lending positions across assets.

    Returns:
        (total_supply_usd, total_debt_usd, total_net_usd)
    """
    total_supply = sum((p.supply_value_usd for p in positions), Decimal("0"))
    total_debt = sum((p.debt_value_usd for p in positions), Decimal("0"))
    total_net = total_supply - total_debt
    return total_supply, total_debt, total_net
