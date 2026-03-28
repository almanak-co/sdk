"""Pure math for GMX V2 perpetual position valuation.

Values open perp positions using on-chain position data and live market prices.
No I/O — all data is passed in.

GMX V2 position value (mark-to-market):
  For LONG:  pnl = size_in_tokens * (mark_price - entry_price)
  For SHORT: pnl = size_in_tokens * (entry_price - mark_price)
  net_value = collateral_value + pnl - pending_fees

GMX V2 uses 30 decimals for USD-denominated values (size_in_usd, entry_price).
Token amounts use the index token's native decimals (e.g. 18 for ETH, 8 for BTC).
"""

import logging
from dataclasses import dataclass
from decimal import Decimal

logger = logging.getLogger(__name__)

# GMX V2 stores USD values with 30 decimal places
GMX_USD_DECIMALS = 30
_USD_DIVISOR = Decimal(10**GMX_USD_DECIMALS)


@dataclass(frozen=True)
class PerpsPositionValue:
    """Valued perpetual position for a single GMX V2 market.

    All USD values are human-readable (not raw 30-decimal).
    """

    market: str
    is_long: bool
    size_usd: Decimal  # Notional size in USD
    collateral_value_usd: Decimal  # Collateral marked to market
    entry_price_usd: Decimal  # Average entry price
    mark_price_usd: Decimal  # Current market price
    unrealized_pnl_usd: Decimal  # Position PnL before fees
    pending_fees_usd: Decimal  # Funding + borrowing fees owed
    net_value_usd: Decimal  # collateral + pnl - fees (what you'd get closing)
    leverage: Decimal  # size / collateral


def value_perps_position(
    *,
    size_in_usd: int,
    size_in_tokens: int,
    collateral_amount: int,
    is_long: bool,
    mark_price_usd: Decimal,
    collateral_token_price_usd: Decimal,
    collateral_token_decimals: int,
    index_token_decimals: int,
    pending_funding_fees_usd: Decimal = Decimal("0"),
    pending_borrowing_fees_usd: Decimal = Decimal("0"),
    market: str = "",
) -> PerpsPositionValue:
    """Value a single GMX V2 perpetual position at current market price.

    This is the GMX V2 mark-to-market formula used by the frontend and keepers.

    Args:
        size_in_usd: Position notional size (30 decimals, raw from chain).
        size_in_tokens: Position size in index tokens (index token decimals, raw).
        collateral_amount: Collateral balance (collateral token decimals, raw).
        is_long: True for long, False for short.
        mark_price_usd: Current index token price in USD (human-readable).
        collateral_token_price_usd: Current collateral token price in USD.
        collateral_token_decimals: Decimals of the collateral token (e.g. 6 for USDC).
        index_token_decimals: Decimals of the index token (e.g. 18 for ETH).
        pending_funding_fees_usd: Pending funding fees in USD (human-readable).
        pending_borrowing_fees_usd: Pending borrowing fees in USD (human-readable).
        market: Market identifier for labeling.

    Returns:
        PerpsPositionValue with computed mark-to-market values.
    """
    # Convert raw values to human-readable
    size_usd = Decimal(size_in_usd) / _USD_DIVISOR
    tokens = Decimal(size_in_tokens) / Decimal(10**index_token_decimals)
    collateral = Decimal(collateral_amount) / Decimal(10**collateral_token_decimals)

    # Collateral value at current price
    collateral_value = collateral * collateral_token_price_usd

    # Entry price: size_in_usd / size_in_tokens (both in raw, result in USD)
    if size_in_tokens > 0:
        # entry_price = (size_in_usd / 10^30) / (size_in_tokens / 10^index_decimals)
        entry_price = size_usd / tokens
    else:
        entry_price = Decimal("0")

    # Unrealized PnL: price movement * token quantity
    if tokens > 0 and mark_price_usd > 0:
        if is_long:
            unrealized_pnl = tokens * (mark_price_usd - entry_price)
        else:
            unrealized_pnl = tokens * (entry_price - mark_price_usd)
    else:
        unrealized_pnl = Decimal("0")

    # Pending fees (funding + borrowing)
    pending_fees = pending_funding_fees_usd + pending_borrowing_fees_usd

    # Net value: what the trader would receive if closing now
    # collateral_value + pnl - fees
    net_value = collateral_value + unrealized_pnl - pending_fees

    # Leverage: notional / collateral
    if collateral_value > 0:
        leverage = size_usd / collateral_value
    else:
        leverage = Decimal("0")

    return PerpsPositionValue(
        market=market,
        is_long=is_long,
        size_usd=size_usd,
        collateral_value_usd=collateral_value,
        entry_price_usd=entry_price,
        mark_price_usd=mark_price_usd,
        unrealized_pnl_usd=unrealized_pnl,
        pending_fees_usd=pending_fees,
        net_value_usd=net_value,
        leverage=leverage,
    )
