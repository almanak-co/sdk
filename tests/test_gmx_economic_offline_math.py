"""Offline GM pricing, valuation and admission refusals on synthetic market state.

The market state comes from tests/gmx_synthetic_frames.py: invented values that are
internally consistent under the connector's validators. Nothing here compares with a
historical Reader call; the retained-evidence modules own that comparison.
"""

import pytest

from almanak.connectors.gmx_v2.economic_accounting import apply_fund_gas
from almanak.connectors.gmx_v2.economic_data import accounting_market
from almanak.connectors.gmx_v2.economic_deposit_execution import deposit_leg, execute_deposit
from almanak.connectors.gmx_v2.economic_liquidity import (
    LiquidityState,
    quote_deposit,
    quote_withdrawal,
)
from almanak.connectors.gmx_v2.economic_liquidity_risk import (
    LiquidityRiskState,
    RequestControls,
    validate_deposit,
    validate_market,
    validate_withdrawal,
)
from almanak.connectors.gmx_v2.economic_spot_math import quote_swap
from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint
from almanak.connectors.gmx_v2.economic_state import EconomicPosition
from almanak.connectors.gmx_v2.economic_swap_risk import validate_swap_risk, validate_swap_side
from almanak.connectors.gmx_v2.economic_valuation import pool_value, side_pnl
from almanak.connectors.gmx_v2.economic_valuation_types import ValuationState
from almanak.connectors.gmx_v2.exchange_observations import (
    ExchangeOwnership,
    allowance_assets,
    inventory_assets,
)
from almanak.models import Account, AssetAmount
from tests import gmx_synthetic_frames as frames

TARGET = frames.exchange_target()
MARKET = accounting_market(TARGET.market)
AT = frames.block(MARKET)
ETHER = 10**18
USDC = 10**6


def accounting(**updates):
    return frames.accounting(MARKET, AT).model_copy(update=updates)


def valuation(action, **updates):
    base = frames.accounting(MARKET, AT)
    spot = base.spot.model_copy(update={"pricing_type": action})
    return ValuationState(**{**dict(base), "spot": spot, **updates})


def quote_state(action, **updates):
    state = valuation(action, **updates)
    value = pool_value(state, maximize=action == "DEPOSIT").pool_usd_e30
    return LiquidityState(
        action=action, spot=state.spot, pool_value_usd_e30=value, share_supply=state.share_supply
    )


def liquidity_risk(**updates):
    swap = frames.swap_risk(MARKET, AT)
    controls = RequestControls(
        creation_disabled=False, execution_disabled=False, cancellation_disabled=False
    )
    values = {
        "market": MARKET,
        "block": AT,
        "long": swap.long,
        "short": swap.short,
        "deposits": controls,
        "withdrawals": controls,
        "market_disabled": False,
        "request_expiration_seconds": 300,
        "maximum_oracle_age_seconds": 300,
        "maximum_long_deposit_usd_e30": 10**45,
        "maximum_short_deposit_usd_e30": 10**45,
        "minimum_first_deposit_shares": 0,
        "maximum_lent_impact_withdrawal_factor": 10**30,
    }
    return LiquidityRiskState(**{**values, **updates})


# Pool valuation


def test_pool_value_bounds_trader_pnl_against_the_pool_in_both_directions():
    state = accounting()
    high, low = pool_value(state, maximize=True), pool_value(state, maximize=False)
    assert high.pool_usd_e30 > low.pool_usd_e30 > 0
    assert high.long_usd_e30 == frames.POOL["long"] * frames.LONG_PRICE[1]
    assert low.long_usd_e30 == frames.POOL["long"] * frames.LONG_PRICE[0]
    assert high.net_pnl_usd_e30 == high.long_pnl_usd_e30 + high.short_pnl_usd_e30
    assert high.borrowing_fees_usd_e30 > 0
    assert high.impact_pool_amount == frames.IMPACT_POOL


def test_side_pnl_is_zero_without_open_interest_and_capped_when_positive():
    state = accounting()
    empty = state.long.model_copy(update={"open_interest_tokens": 0})
    assert side_pnl(state.model_copy(update={"long": empty}), True, True, 10**40) == 0
    # A long side whose tokens are worth far more than its entry value is in profit.
    rich = state.long.model_copy(
        update={"open_interest_tokens": 10**18, "maximum_pnl_factor": 10**29}
    )
    profitable = state.model_copy(update={"long": rich})
    uncapped = 10**18 * frames.LONG_PRICE[0] - rich.open_interest_usd_e30
    assert side_pnl(profitable, True, True, 10**40) == uncapped
    assert side_pnl(profitable, True, True, 10**33) == 10**32
    # Short traders lose when the index is worth more than their entry value.
    poor = state.short.model_copy(update={"open_interest_tokens": 10**18})
    losing = state.model_copy(update={"short": poor})
    assert side_pnl(losing, False, False, 10**40) == poor.open_interest_usd_e30 - (
        10**18 * frames.LONG_PRICE[0]
    )
    assert side_pnl(losing, False, True, 10**40) < 0


# Share quotes


def test_quote_deposit_refuses_other_actions_and_nonpositive_amounts():
    with pytest.raises(ValueError, match="deposit-specific"):
        quote_deposit(quote_state("WITHDRAWAL"), ETHER, 0)
    state = quote_state("DEPOSIT")
    with pytest.raises(ValueError, match="nonnegative amounts"):
        quote_deposit(state, -1, USDC)
    with pytest.raises(ValueError, match="nonnegative amounts"):
        quote_deposit(state, 0, 0)


def test_quote_deposit_splits_impact_between_both_currencies():
    state = quote_state("DEPOSIT")
    single = quote_deposit(state, ETHER, 0)
    assert single.short_fees.after_fees == 0 and single.price_impact_usd_e30 < 0
    both = quote_deposit(state, ETHER, 2_000 * USDC)
    assert both.long_amount == ETHER and both.short_amount == 2_000 * USDC
    assert both.shares > single.shares > 0
    assert both.long_fees.after_fees == single.long_fees.after_fees


def test_quote_deposit_credits_positive_impact_when_it_rebalances_the_pool():
    # The synthetic pool holds slightly more long USD; a small short deposit improves it.
    state = quote_state("DEPOSIT")
    quote = quote_deposit(state, 0, 100 * USDC)
    assert quote.price_impact_usd_e30 > 0
    assert quote.shares > 0


def test_quote_withdrawal_refuses_invalid_shares_and_empty_inventory():
    with pytest.raises(ValueError, match="withdrawal-specific"):
        quote_withdrawal(quote_state("DEPOSIT"), ETHER)
    state = quote_state("WITHDRAWAL")
    for shares in (0, state.share_supply + 1):
        with pytest.raises(ValueError, match="within the captured supply"):
            quote_withdrawal(state, shares)
    empty = state.model_copy(
        update={
            "spot": state.spot.model_copy(
                update={
                    "long": state.spot.long.model_copy(update={"pool_amount": 0}),
                    "short": state.spot.short.model_copy(update={"pool_amount": 0}),
                }
            )
        }
    )
    with pytest.raises(ValueError, match="positive inventory"):
        quote_withdrawal(empty, ETHER)


def test_quote_withdrawal_returns_both_currencies_in_pool_proportion():
    state = quote_state("WITHDRAWAL")
    half = quote_withdrawal(state, state.share_supply // 2)
    assert half.shares == state.share_supply // 2
    assert 0 < half.long_amount < frames.POOL["long"] // 2
    assert 0 < half.short_amount < frames.POOL["short"] // 2
    assert half.long_fees.after_fees == half.long_amount


# Swap quotes


def test_quote_swap_refuses_other_fees_assets_and_nonpositive_amounts():
    spot = accounting().spot
    with pytest.raises(ValueError, match="swap-specific"):
        quote_swap(spot.model_copy(update={"pricing_type": "DEPOSIT"}), MARKET.long_asset, ETHER)
    with pytest.raises(ValueError, match="configured market currency"):
        quote_swap(spot, MARKET.share_asset, ETHER)
    with pytest.raises(ValueError, match="configured market currency"):
        quote_swap(spot, MARKET.long_asset, 0)


def test_quote_swap_prices_both_directions():
    spot = accounting().spot
    forward = quote_swap(spot, MARKET.long_asset, ETHER)
    assert forward.asset_out == MARKET.short_asset
    assert forward.price_impact_usd_e30 < 0 and 0 < forward.amount_out < 2_000 * USDC
    backward = quote_swap(spot, MARKET.short_asset, 100 * USDC)
    assert backward.asset_out == MARKET.long_asset
    assert backward.price_impact_usd_e30 > 0 and backward.amount_out > 0


# Swap risk


def test_validate_swap_risk_admits_a_bounded_swap():
    state = accounting()
    risk = frames.swap_risk(MARKET, AT)
    validate_swap_risk(
        state.spot, risk, state.index_price, quote_swap(state.spot, MARKET.long_asset, ETHER)
    )


def test_validate_swap_risk_refuses_mismatched_disabled_and_empty_swaps():
    state = accounting()
    risk = frames.swap_risk(MARKET, AT)
    calculation = quote_swap(state.spot, MARKET.long_asset, ETHER)
    later = frames.block(MARKET, number=frames.NUMBER + 1)
    with pytest.raises(ValueError, match="same canonical market"):
        validate_swap_risk(
            state.spot, risk.model_copy(update={"block": later}), state.index_price, calculation
        )
    long_price = frames.prices(MARKET)[1]
    if long_price.asset != MARKET.index_asset:
        with pytest.raises(ValueError, match="same canonical market"):
            validate_swap_risk(state.spot, risk, long_price, calculation)
    for field in ("market_disabled", "execution_disabled"):
        with pytest.raises(SwapConstraint, match="disabled"):
            validate_swap_risk(
                state.spot, risk.model_copy(update={field: True}), state.index_price, calculation
            )
    with pytest.raises(SwapConstraint, match="no positive output"):
        validate_swap_risk(
            state.spot, risk, state.index_price, calculation.model_copy(update={"amount_out": 0})
        )
    drained = calculation.model_copy(update={"pool_amount_out": frames.POOL["short"] + 1})
    with pytest.raises(SwapConstraint, match="available pool inventory"):
        validate_swap_risk(state.spot, risk, state.index_price, drained)


def test_validate_swap_side_refuses_pool_cap_reserve_and_trader_pnl():
    index = accounting().index_price
    side = frames.swap_risk(MARKET, AT).long
    with pytest.raises(SwapConstraint, match="maximum pool amount"):
        validate_swap_side(
            side.model_copy(update={"maximum_pool_amount": 1}),
            index,
            2,
            10**36,
            True,
            incoming=True,
        )
    with pytest.raises(SwapConstraint, match="position reserve"):
        validate_swap_side(side, index, 1, 1, True, incoming=False)
    profitable = side.model_copy(
        update={"open_interest_tokens": 10**21, "maximum_deposit_pnl_factor": 0}
    )
    with pytest.raises(SwapConstraint, match="trader-PnL"):
        validate_swap_side(profitable, index, 1, 10**36, True, incoming=True)
    validate_swap_side(side, index, 1, 10**40, False, incoming=False)


# GM deposit execution and risk


def test_execute_deposit_refuses_swap_pricing_and_nonpositive_amounts():
    with pytest.raises(ValueError, match="deposit-specific accounting"):
        execute_deposit(accounting(), ETHER, 0)
    state = valuation("DEPOSIT")
    with pytest.raises(ValueError, match="nonnegative amounts"):
        execute_deposit(state, ETHER, -1)
    with pytest.raises(ValueError, match="nonnegative amounts"):
        execute_deposit(state, 0, 0)


def test_execute_deposit_applies_legs_in_order_and_mints_their_sum():
    state = valuation("DEPOSIT")
    result = execute_deposit(state, ETHER, 2_000 * USDC)
    assert [leg.is_long for leg in result.legs] == [True, False]
    assert result.shares == sum(leg.minted for leg in result.legs) > 0
    assert result.after == result.legs[-1].after
    assert result.after.share_supply == state.share_supply + result.shares
    long_leg = result.legs[0]
    assert long_leg.after.spot.long.pool_amount == (
        state.spot.long.pool_amount
        + long_leg.fees.after_fees
        + long_leg.input_impact_amount
        + long_leg.fees.pool_amount
    )
    single = execute_deposit(state, 0, 100 * USDC)
    assert [leg.is_long for leg in single.legs] == [False]
    assert single.price_impact_usd_e30 > 0
    assert single.legs[0].output_impact_amount > 0


def test_deposit_leg_refuses_impact_above_its_incoming_amount():
    state = valuation("DEPOSIT")
    with pytest.raises(SwapConstraint, match="exceeds its incoming amount"):
        deposit_leg(state, True, ETHER, -(10**40), False)


def test_validate_market_refuses_another_block_and_disabled_execution():
    state = valuation("DEPOSIT")
    later = frames.block(MARKET, number=frames.NUMBER + 1)
    with pytest.raises(ValueError, match="same canonical market"):
        validate_market(state, liquidity_risk(block=later), deposit=True)
    with pytest.raises(SwapConstraint, match="disabled"):
        validate_market(state, liquidity_risk(market_disabled=True), deposit=False)
    disabled = RequestControls(
        creation_disabled=False, execution_disabled=True, cancellation_disabled=False
    )
    with pytest.raises(SwapConstraint, match="disabled"):
        validate_market(state, liquidity_risk(deposits=disabled), deposit=True)
    validate_market(state, liquidity_risk(deposits=disabled), deposit=False)


def test_validate_deposit_admits_and_refuses_each_historical_limit():
    state = valuation("DEPOSIT")
    result = execute_deposit(state, ETHER, 2_000 * USDC)
    validate_deposit(state, liquidity_risk(), result)
    empty = state.model_copy(update={"share_supply": 0})
    with pytest.raises(SwapConstraint, match="locked-share receiver"):
        validate_deposit(empty, liquidity_risk(minimum_first_deposit_shares=1), result)
    capped = frames.swap_risk(MARKET, AT).long.model_copy(update={"maximum_pool_amount": 1})
    with pytest.raises(SwapConstraint, match="maximum pool amount"):
        validate_deposit(state, liquidity_risk(long=capped), result)
    with pytest.raises(SwapConstraint, match="incoming-currency USD cap"):
        validate_deposit(state, liquidity_risk(maximum_long_deposit_usd_e30=1), result)
    with pytest.raises(SwapConstraint, match="incoming-currency USD cap"):
        validate_deposit(state, liquidity_risk(maximum_short_deposit_usd_e30=1), result)


def test_validate_withdrawal_reduces_the_pool_and_refuses_historical_limits():
    state = valuation("WITHDRAWAL")
    calculation = quote_withdrawal(quote_state("WITHDRAWAL"), state.share_supply // 10)
    after = validate_withdrawal(state, liquidity_risk(), calculation)
    assert after.spot.long.pool_amount < state.spot.long.pool_amount
    assert after.spot.short.pool_amount < state.spot.short.pool_amount
    lent = state.model_copy(
        update={"impact": state.impact.model_copy(update={"lent_amount": 10**18})}
    )
    with pytest.raises(SwapConstraint, match="lent-impact"):
        validate_withdrawal(
            lent, liquidity_risk(maximum_lent_impact_withdrawal_factor=0), calculation
        )
    reserved = frames.swap_risk(MARKET, AT).short.model_copy(update={"reserve_factor": 0})
    with pytest.raises(SwapConstraint, match="position reserve"):
        validate_withdrawal(state, liquidity_risk(short=reserved), calculation)
    with pytest.raises(SwapConstraint, match="available pool inventory"):
        validate_withdrawal(
            state,
            liquidity_risk(),
            calculation.model_copy(
                update={
                    "long_fees": calculation.long_fees.model_copy(
                        update={"after_fees": frames.POOL["long"] + 1}
                    )
                }
            ),
        )


# Fund-paid gas


def position(inventory):
    owned = ExchangeOwnership(
        target=TARGET,
        inventory=tuple(
            AssetAmount(asset=asset, amount=amount)
            for asset, amount in zip(inventory_assets(TARGET), inventory, strict=True)
        ),
        market_tokens=AssetAmount(asset=TARGET.market.share_asset, amount=0),
    )
    return EconomicPosition(
        target=TARGET,
        owned=owned,
        spent=(),
        allowances=tuple(AssetAmount(asset=a, amount=0) for a in allowance_assets(TARGET)),
        escrow=None,
    )


class Before:
    def __init__(self, held, wallets, payer=frames.FUND):
        self.target = TARGET
        self.owned = held.owned
        self.payer = Account(chain=TARGET.fund.chain, address=payer)
        self.wallets = tuple(
            AssetAmount(asset=asset, amount=amount)
            for asset, amount in zip(inventory_assets(TARGET), wallets, strict=True)
        )


def native(amount):
    return AssetAmount(asset=inventory_assets(TARGET)[-1], amount=amount)


def test_apply_fund_gas_charges_owned_native_only_after_the_unallocated_reserve():
    count = len(inventory_assets(TARGET))
    held = position((0,) * (count - 1) + (10,))
    # The fund holds 15 native units, 10 of them owned: a 7-unit charge takes 5 free first.
    before = Before(held, (0,) * (count - 1) + (15,))
    charged = apply_fund_gas(before, held, native(7))
    assert charged.owned.inventory[-1].amount == 8
    assert apply_fund_gas(before, held, native(0)) is held
    elsewhere = Before(held, (0,) * (count - 1) + (15,), payer="0x" + "12" * 20)
    assert apply_fund_gas(elsewhere, held, native(7)) is held


def test_apply_fund_gas_refuses_charges_beyond_measured_cash_or_ownership():
    count = len(inventory_assets(TARGET))
    held = position((0,) * (count - 1) + (10,))
    with pytest.raises(ValueError, match="exceeds measured native cash"):
        apply_fund_gas(Before(held, (0,) * (count - 1) + (9,)), held, native(1))
    with pytest.raises(ValueError, match="exceeds measured native cash"):
        apply_fund_gas(Before(held, (0,) * (count - 1) + (15,)), held, native(16))
