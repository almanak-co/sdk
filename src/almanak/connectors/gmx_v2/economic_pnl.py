"""Position PnL preserves side-specific rounding and the captured market profit cap."""

from pydantic import model_validator

from almanak.connectors.gmx_v2.domain import Market
from almanak.connectors.gmx_v2.economic_position_math import signed_div
from almanak.connectors.gmx_v2.economic_precision import FLOAT, apply_factor
from almanak.connectors.gmx_v2.economic_spot_types import Price
from almanak.models import Block, Model, UInt


class PnlSide(Model):
    open_interest_usd_e30: UInt
    open_interest_tokens: UInt
    pool_amount: UInt
    pool_price: Price
    maximum_trader_pnl_factor: UInt


class PnlState(Model):
    market: Market
    block: Block
    index_price: Price
    long: PnlSide
    short: PnlSide

    @model_validator(mode="after")
    def scope(self):
        if self.block.chain != self.market.chain or (
            self.index_price.asset,
            self.long.pool_price.asset,
            self.short.pool_price.asset,
        ) != (self.market.index_asset, self.market.long_asset, self.market.short_asset):
            raise ValueError("GMX position PnL requires the market's canonical asset prices")
        return self


class PositionPnl(Model):
    pnl_usd_e30: int
    uncapped_pnl_usd_e30: int
    size_delta_tokens: UInt


def market_pnl(side, price, is_long):
    if not side.open_interest_usd_e30 or not side.open_interest_tokens:
        return 0
    value = side.open_interest_tokens * (price.maximum if is_long else price.minimum)
    return value - side.open_interest_usd_e30 if is_long else side.open_interest_usd_e30 - value


def capped_position_profit(state, value, is_long):
    if value <= 0:
        return value
    side = state.long if is_long else state.short
    pool_pnl = market_pnl(side, state.index_price, is_long)
    cap = apply_factor(side.pool_amount * side.pool_price.minimum, side.maximum_trader_pnl_factor)
    # The deployed position helper does not scale profit when the capped pool PnL is zero.
    if 0 < cap < pool_pnl:
        return value * cap // pool_pnl
    return value


def position_pnl(state, size_usd_e30, size_tokens, size_delta_usd_e30, is_long):
    if min(size_usd_e30, size_tokens) <= 0 or not 0 <= size_delta_usd_e30 <= size_usd_e30:
        raise ValueError("GMX PnL requires a nonempty position and a bounded size decrease")
    price = state.index_price.minimum if is_long else state.index_price.maximum
    worth = size_tokens * price
    uncapped = worth - size_usd_e30 if is_long else size_usd_e30 - worth
    capped = capped_position_profit(state, uncapped, is_long)
    numerator = size_tokens * size_delta_usd_e30
    tokens = (
        (numerator + size_usd_e30 - 1) // size_usd_e30 if is_long else numerator // size_usd_e30
    )
    return PositionPnl(
        pnl_usd_e30=signed_div(capped * tokens, size_tokens),
        uncapped_pnl_usd_e30=signed_div(uncapped * tokens, size_tokens),
        size_delta_tokens=tokens,
    )


def funding_amount(latest_per_size, position_per_size, size_usd_e30, *, payment):
    if min(position_per_size, size_usd_e30) < 0 or latest_per_size < position_per_size:
        raise ValueError("GMX funding accumulators cannot decrease")
    denominator = FLOAT * 10**15
    numerator = (latest_per_size - position_per_size) * size_usd_e30
    return (numerator + denominator - 1) // denominator if payment else numerator // denominator


def borrowing_fee_usd(latest_factor, position_factor, size_usd_e30):
    if min(position_factor, size_usd_e30) < 0 or latest_factor < position_factor:
        raise ValueError("GMX borrowing accumulator cannot decrease")
    return apply_factor(size_usd_e30, latest_factor - position_factor)
