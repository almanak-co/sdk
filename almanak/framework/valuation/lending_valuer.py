"""Pure math for Aave V3 lending position valuation.

Values supply (aToken) and borrow (debt) positions using on-chain balances
and live USD prices. No I/O — all data is passed in.

Supply value = aToken balance (in human units) * token price USD
Debt value   = (stable debt + variable debt) (in human units) * token price USD
Net value    = supply value - debt value

USD outputs are :class:`MeasuredMoney` (VIB-5216 / US-006): a price-unavailable
position yields **unmeasured** USD values, never a fabricated ``Decimal("0")``
(the #2866 placeholder class). Balances stay plain ``Decimal`` because they come
from on-chain reads that are always measured here. Callers serialize the USD
fields back to the existing persisted representation via the MeasuredMoney
payload codec (``to_payload`` / ``value_or``), so persistence stays byte-compatible.
"""

import logging
from dataclasses import dataclass
from decimal import Decimal

from almanak.framework.accounting.measured import MeasuredMoney

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LendingPositionValue:
    """Valued lending position for a single asset reserve.

    A user can have both supply and borrow on the same asset.
    Net value = supply_value_usd - debt_value_usd.

    USD fields are :class:`MeasuredMoney` so a price-unavailable reserve is
    *unmeasured* rather than a fabricated zero (Empty≠Zero, blueprint 27 §10.10).
    Balances are plain ``Decimal`` (always measured from the on-chain read).
    """

    asset: str
    supply_balance: Decimal  # Human-readable (e.g., 1.5 WETH)
    supply_value_usd: MeasuredMoney
    stable_debt_balance: Decimal
    variable_debt_balance: Decimal
    debt_value_usd: MeasuredMoney
    net_value_usd: MeasuredMoney
    collateral_enabled: bool
    decimals: int


def value_lending_position(
    *,
    atoken_balance: int,
    stable_debt: int,
    variable_debt: int,
    token_price_usd: MeasuredMoney | Decimal | str | None,
    token_decimals: int,
    collateral_enabled: bool = True,
    asset: str = "",
) -> LendingPositionValue:
    """Value a single Aave V3 reserve position.

    Args:
        atoken_balance: Raw aToken balance (wei). Includes accrued interest.
        stable_debt: Raw stable debt balance (wei).
        variable_debt: Raw variable debt balance (wei).
        token_price_usd: Current USD price for the underlying token. Accepts a
            :class:`MeasuredMoney` or any legacy raw form (``Decimal`` / ``str`` /
            ``None``); a non-measured price makes the USD outputs unmeasured
            rather than fabricating a ``Decimal("0")``.
        token_decimals: Decimals for the underlying token.
        collateral_enabled: Whether this asset is used as collateral.
        asset: Asset symbol for labeling.

    Returns:
        LendingPositionValue with computed USD values (MeasuredMoney).
    """
    price = token_price_usd if isinstance(token_price_usd, MeasuredMoney) else MeasuredMoney.from_raw(token_price_usd)

    divisor = Decimal(10**token_decimals)

    supply_balance = Decimal(atoken_balance) / divisor
    stable_balance = Decimal(stable_debt) / divisor
    variable_balance = Decimal(variable_debt) / divisor

    if price.is_measured:
        px = price.value
        supply_value = MeasuredMoney.measured(supply_balance * px)
        debt_value = MeasuredMoney.measured((stable_balance + variable_balance) * px)
    else:
        # Price unavailable ⇒ USD values are unmeasured, NEVER a fabricated $0
        # (Empty≠Zero / #2866). Propagate the price's non-measured state via the
        # intention-revealing classmethods (never the raw constructor).
        supply_value = MeasuredMoney.absent() if price.is_absent else MeasuredMoney.unmeasured()
        debt_value = MeasuredMoney.absent() if price.is_absent else MeasuredMoney.unmeasured()
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
) -> tuple[MeasuredMoney, MeasuredMoney, MeasuredMoney]:
    """Aggregate multiple lending positions into portfolio totals.

    Seeds each accumulator with ``MeasuredMoney.measured(Decimal("0"))`` and folds
    with MeasuredMoney addition, so a single unmeasured leg poisons the whole
    total (Empty≠Zero / §10.10 information lattice) instead of silently summing a
    fabricated zero.

    Args:
        positions: List of valued lending positions across assets.

    Returns:
        (total_supply_usd, total_debt_usd, total_net_usd) as MeasuredMoney.
    """
    total_supply = MeasuredMoney.measured(Decimal("0"))
    total_debt = MeasuredMoney.measured(Decimal("0"))
    for p in positions:
        total_supply = total_supply + p.supply_value_usd
        total_debt = total_debt + p.debt_value_usd
    total_net = total_supply - total_debt
    return total_supply, total_debt, total_net
