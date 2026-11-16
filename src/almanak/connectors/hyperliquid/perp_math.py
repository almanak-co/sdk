"""Cross-margin account arithmetic of the HyperCore backtest model (blueprint 30, 12).

The rules are the fork model's (section 9.4), applied to one market per account: initial
margin is notional over the cross leverage, maintenance margin is half the initial
margin at the market's maximum leverage, withdrawable USDC keeps the larger of initial
margin and a tenth of notional, and a trade settles its closed PnL less a taker fee
rounded to the micro-USDC. Every price here is the caller's: the backtest passes trade
candles where HyperCore would use its book, mark or oracle.
"""

from decimal import ROUND_DOWN, ROUND_HALF_EVEN, ROUND_UP, Decimal

from almanak.backtest.types import UnixTime
from almanak.connectors.hyperliquid.economic_state import Holding, HyperCoreAccount
from almanak.connectors.hyperliquid.positions import text
from almanak.connectors.hyperliquid.resolution import PerpMarket

USDC = Decimal("0.000001")
BASIS_POINTS = Decimal(10_000)
# HyperCore perp prices carry at most five significant figures and 6 - szDecimals places.
PRICE_DECIMALS = 6
SIGNIFICANT = 5


def micro(value: Decimal) -> int:
    return int(value.quantize(USDC, ROUND_HALF_EVEN) / USDC)


def usdc(amount: int) -> Decimal:
    return Decimal(amount) * USDC


def on_grid(raw: Decimal, size_decimals: int, rounding: str) -> Decimal:
    """`raw` on the market's price grid, rounded the given way (the `Ioc` limit rule)."""
    places = PRICE_DECIMALS - size_decimals
    price = raw.quantize(Decimal(1).scaleb(-places), rounding)
    if price != price.to_integral_value():
        whole = price.adjusted() + 1
        places = min(places, max(0, SIGNIFICANT - whole))
        price = raw.quantize(Decimal(1).scaleb(-places), rounding)
    return price


def moved(reference: Decimal, is_buy: bool, bps: Decimal, size_decimals: int) -> Decimal:
    """The reference moved against the taker by `bps`, rounded against the taker."""
    factor = 1 + bps / BASIS_POINTS * (1 if is_buy else -1)
    return on_grid(reference * factor, size_decimals, ROUND_UP if is_buy else ROUND_DOWN)


def szi(account: HyperCoreAccount) -> Decimal:
    return Decimal(account.holding.szi) if account.holding is not None else Decimal(0)


def unrealized(account: HyperCoreAccount, price: Decimal) -> Decimal:
    holding = account.holding
    if holding is None:
        return Decimal(0)
    return Decimal(holding.szi) * (price - Decimal(holding.entry_px))


def account_value(account: HyperCoreAccount, price: Decimal) -> Decimal:
    return usdc(account.perp_usdc) + unrealized(account, price)


def notional(account: HyperCoreAccount, price: Decimal) -> Decimal:
    return abs(szi(account)) * price


def initial_margin(account: HyperCoreAccount, price: Decimal) -> Decimal:
    if account.holding is None:
        return Decimal(0)
    if account.leverage is None:
        raise ValueError("a HyperCore position needs its cross leverage")
    return notional(account, price) / account.leverage


def maintenance_margin(account: HyperCoreAccount, price: Decimal) -> Decimal:
    return notional(account, price) * account.target.market.maintenance_rate


def withdrawable(account: HyperCoreAccount, price: Decimal) -> Decimal:
    kept = max(initial_margin(account, price), notional(account, price) / 10)
    return max(Decimal(0), account_value(account, price) - kept)


def liquidation_price(account: HyperCoreAccount) -> Decimal | None:
    """The price at which account value meets maintenance margin, or None when no
    positive price reaches it (a position its cash fully backs)."""
    holding = account.holding
    if holding is None:
        return None
    size, entry = Decimal(holding.szi), Decimal(holding.entry_px)
    cash, rate = usdc(account.perp_usdc), account.target.market.maintenance_rate
    if size > 0:
        price = (size * entry - cash) / (size * (1 - rate))
    else:
        price = (cash - size * entry) / (-size * (1 + rate))
    return price if price > 0 else None


def under_maintenance(account: HyperCoreAccount, price: Decimal) -> bool:
    return account.holding is not None and account_value(account, price) < maintenance_margin(
        account, price
    )


def order_size(size: Decimal, market: PerpMarket) -> Decimal:
    rounded = size.quantize(Decimal(1).scaleb(-market.size_decimals), ROUND_DOWN)
    if rounded <= 0:
        raise ValueError("the order size rounds to nothing at the market's size decimals")
    return rounded


def fee(size: Decimal, price: Decimal, taker_bps: Decimal) -> int:
    return micro(abs(size) * price * taker_bps / BASIS_POINTS)


def trade(account: HyperCoreAccount, signed: Decimal, price: Decimal, fee_paid: int, time: int):
    """The account after moving its position by `signed` coins at `price`, and the PnL
    the trade closed. The fee is charged in the same step."""
    start = szi(account)
    entry = Decimal(account.holding.entry_px) if account.holding is not None else Decimal(0)
    closing = start != 0 and (start > 0) != (signed > 0)
    closed = min(abs(signed), abs(start)) if closing else Decimal(0)
    pnl = micro(closed * (price - entry) * (1 if start > 0 else -1)) if closing else 0
    after = start + signed
    if after == 0:
        holding = None
    else:
        if not closing:
            entry = (abs(start) * entry + abs(signed) * price) / abs(after)
        elif (after > 0) != (start > 0):
            entry = price
        holding = Holding(szi=text(after), entry_px=text(entry), changed_at=UnixTime(time))
    updated = account.model_copy(
        update={
            "holding": holding,
            "perp_usdc": account.perp_usdc + pnl - fee_paid,
            "fees": account.fees + fee_paid,
            "realized_pnl": account.realized_pnl + pnl,
        }
    )
    return validated(updated), pnl


def validated(account: HyperCoreAccount) -> HyperCoreAccount:
    """`model_copy` skips validation; every changed account is validated again."""
    return HyperCoreAccount.model_validate_json(account.model_dump_json())
