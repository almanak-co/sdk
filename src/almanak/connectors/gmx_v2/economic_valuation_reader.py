"""Acquire raw GM valuation terms without using the Reader's resulting pool value as input."""

from almanak.connectors.gmx_v2.economic_position_reader import open_interest
from almanak.connectors.gmx_v2.economic_spot_reader import market_uint
from almanak.connectors.gmx_v2.economic_swap_risk_reader import maximum_pnl_factor
from almanak.connectors.gmx_v2.economic_valuation_types import (
    AccountingState,
    BorrowingSide,
    ImpactDistribution,
    ValuationState,
)
from almanak.connectors.gmx_v2.oracle_reader import stored

SIDE_FIELDS = {
    "cumulative_factor": "CUMULATIVE_BORROWING_FACTOR",
    "updated_at": "CUMULATIVE_BORROWING_FACTOR_UPDATED_AT",
    "total_borrowing_usd_e30": "TOTAL_BORROWING",
    "optimal_usage_factor": "OPTIMAL_USAGE_FACTOR",
    "base_factor": "BASE_BORROWING_FACTOR",
    "above_optimal_factor": "ABOVE_OPTIMAL_USAGE_BORROWING_FACTOR",
    "factor": "BORROWING_FACTOR",
    "exponent": "BORROWING_EXPONENT_FACTOR",
    "reserve_factor": "OPEN_INTEREST_RESERVE_FACTOR",
}
IMPACT_FIELDS = {
    "pool_amount": "POSITION_IMPACT_POOL_AMOUNT",
    "minimum_amount": "MIN_POSITION_IMPACT_POOL_AMOUNT",
    "rate": "POSITION_IMPACT_POOL_DISTRIBUTION_RATE",
    "distributed_at": "POSITION_IMPACT_POOL_DISTRIBUTED_AT",
    "lent_amount": "LENT_POSITION_IMPACT_POOL_AMOUNT",
}


def borrowing_side(reader, spot, is_long, pnl_type):
    market, block = spot.market, spot.block
    return BorrowingSide(
        **{
            field: market_uint(reader, market, block, name, positive=is_long)
            for field, name in SIDE_FIELDS.items()
        },
        open_interest_usd_e30=open_interest(reader, market, block, is_long, in_tokens=False),
        open_interest_tokens=open_interest(reader, market, block, is_long, in_tokens=True),
        maximum_pnl_factor=maximum_pnl_factor(reader, market, block, is_long, pnl_type),
    )


def prepared_valuation(reader, spot, index_price):
    state = prepared_accounting(reader, spot, index_price, pnl_type=spot.pricing_type + "S")
    return ValuationState.model_validate_json(state.model_dump_json())


def prepared_accounting(reader, spot, index_price, *, pnl_type):
    market, block = spot.market, spot.block
    impact = ImpactDistribution(
        **{field: market_uint(reader, market, block, name) for field, name in IMPACT_FIELDS.items()}
    )
    flags = tuple(
        stored(reader, block, market.contracts.data_store, "Bool", name)
        for name in (
            "SKIP_BORROWING_FEE_FOR_SMALLER_SIDE",
            "USE_OPEN_INTEREST_IN_TOKENS_FOR_BALANCE",
        )
    )
    return AccountingState(
        spot=spot,
        index_price=index_price,
        long=borrowing_side(reader, spot, True, pnl_type),
        short=borrowing_side(reader, spot, False, pnl_type),
        skip_borrowing_for_smaller_side=flags[0],
        use_open_interest_tokens_for_balance=flags[1],
        borrowing_fee_receiver_factor=stored(
            reader, block, market.contracts.data_store, "Uint", "BORROWING_FEE_RECEIVER_FACTOR"
        ),
        impact=impact,
        share_supply=reader.call(
            market.share_asset.reference, "totalSupply()", block, outputs=("uint256",)
        )[0],
    )
