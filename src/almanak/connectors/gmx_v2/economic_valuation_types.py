"""Raw GM valuation inputs retain borrowing, impact distribution and action-specific PnL caps."""

from pydantic import model_validator

from almanak.connectors.gmx_v2.economic_spot_types import Factor, Price, SpotState
from almanak.models import Model, UInt


class BorrowingSide(Model):
    open_interest_usd_e30: UInt
    open_interest_tokens: UInt
    cumulative_factor: UInt
    updated_at: UInt
    total_borrowing_usd_e30: UInt
    optimal_usage_factor: UInt
    base_factor: UInt
    above_optimal_factor: UInt
    factor: UInt
    exponent: UInt
    reserve_factor: UInt
    maximum_pnl_factor: UInt


class ImpactDistribution(Model):
    pool_amount: UInt
    minimum_amount: UInt
    rate: UInt
    distributed_at: UInt
    lent_amount: UInt


class AccountingState(Model):
    spot: SpotState
    index_price: Price
    long: BorrowingSide
    short: BorrowingSide
    skip_borrowing_for_smaller_side: bool
    use_open_interest_tokens_for_balance: bool
    borrowing_fee_receiver_factor: Factor
    impact: ImpactDistribution
    share_supply: UInt

    @model_validator(mode="after")
    def scope(self):
        if self.index_price.asset != self.spot.market.index_asset:
            raise ValueError("GM valuation index price differs from its configured market")
        if (
            max(self.long.updated_at, self.short.updated_at, self.impact.distributed_at)
            > self.spot.block.timestamp
        ):
            raise ValueError(
                "GM valuation accumulator timestamps cannot follow their canonical block"
            )
        return self


class ValuationState(AccountingState):
    @model_validator(mode="after")
    def liquidity_action(self):
        if self.spot.pricing_type not in ("DEPOSIT", "WITHDRAWAL"):
            raise ValueError("GM valuation requires action-specific deposit or withdrawal inputs")
        return self


class PoolValue(Model):
    pool_usd_e30: int
    long_pnl_usd_e30: int
    short_pnl_usd_e30: int
    net_pnl_usd_e30: int
    long_amount: UInt
    short_amount: UInt
    long_usd_e30: UInt
    short_usd_e30: UInt
    borrowing_fees_usd_e30: UInt
    borrowing_pool_factor: Factor
    impact_pool_amount: UInt
    lent_impact_amount: UInt

    def reader_fields(self):
        return tuple(self.model_dump().values())
