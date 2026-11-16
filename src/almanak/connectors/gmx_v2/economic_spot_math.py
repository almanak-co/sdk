"""Size-dependent swap pricing on a frozen external market with exact deployed rounding."""

from almanak.connectors.gmx_v2.economic_precision import apply_exponent, apply_factor, ceil_div
from almanak.connectors.gmx_v2.economic_spot_types import (
    Fees,
    Impact,
    SwapCalculation,
    SwapConstraint,
)


def impact_curve(difference, factor, exponent):
    return apply_factor(apply_exponent(difference, exponent), factor)


def imbalance_impact(before, after, parameters):
    if min(*before, *after) < 0:
        raise SwapConstraint("GMX requested USD delta exceeds available pool value")
    initial, following = abs(before[0] - before[1]), abs(after[0] - after[1])
    improved = following < initial
    positive = min(parameters.positive_factor, parameters.negative_factor)
    if (before[0] <= before[1]) == (after[0] <= after[1]):
        factor = positive if improved else parameters.negative_factor
        magnitude = abs(
            impact_curve(initial, factor, parameters.exponent)
            - impact_curve(following, factor, parameters.exponent)
        )
        impact = magnitude if improved else -magnitude
    else:
        impact = impact_curve(initial, positive, parameters.exponent) - impact_curve(
            following, parameters.negative_factor, parameters.exponent
        )
    return Impact(usd_e30=impact, balance_improved=improved)


def inventory_impact(state, inventory, deltas):
    prices = (state.long.price.midpoint, state.short.price.midpoint)
    before = tuple(amount * price for amount, price in zip(inventory, prices, strict=True))
    after = tuple(amount + delta for amount, delta in zip(before, deltas, strict=True))
    return imbalance_impact(before, after, state.impact)


def spot_impact(state, deltas, *, virtual=True):
    actual = inventory_impact(state, (state.long.pool_amount, state.short.pool_amount), deltas)
    if actual.usd_e30 >= 0 or not virtual or state.virtual_inventory is None:
        return actual
    combined = inventory_impact(state, state.virtual_inventory, deltas)
    return combined if combined.usd_e30 < actual.usd_e30 else actual


def fees_for(amount, parameters, improved):
    factor = parameters.positive_factor if improved else parameters.negative_factor
    fee = apply_factor(amount, factor)
    receiver = apply_factor(fee, parameters.receiver_factor)
    ui = apply_factor(amount, parameters.ui_factor)
    if fee + ui > amount:
        raise SwapConstraint("GMX fee amount exceeds its gross input")
    return Fees(
        receiver_amount=receiver,
        pool_amount=fee - receiver,
        ui_amount=ui,
        after_fees=amount - fee - ui,
    )


def impact_amount(currency, impact_usd):
    if impact_usd <= 0:
        return -ceil_div(-impact_usd, currency.price.minimum), 0
    requested = impact_usd // currency.price.maximum
    granted = min(requested, currency.impact_pool_amount)
    return granted, (requested - granted) * currency.price.maximum


def output_amount(incoming, outgoing, net_input, impact_usd):
    if impact_usd > 0:
        output_impact, leftover = impact_amount(outgoing, impact_usd)
        input_impact, _ = impact_amount(incoming, leftover)
        pool_output = (net_input + input_impact) * incoming.price.minimum // outgoing.price.maximum
        return pool_output + output_impact, output_impact, input_impact, pool_output
    input_impact, _ = impact_amount(incoming, impact_usd)
    if net_input + input_impact < 0:
        raise SwapConstraint("GMX negative impact exceeds input after fees")
    pool_output = (net_input + input_impact) * incoming.price.minimum // outgoing.price.maximum
    return pool_output, input_impact, 0, pool_output


def quote_swap(state, asset_in, amount):
    if state.pricing_type != "SWAP":
        raise ValueError("GMX swap requires swap-specific fees")
    if asset_in not in (state.market.long_asset, state.market.short_asset) or amount <= 0:
        raise ValueError("GMX swap requires a positive amount of a configured market currency")
    forward = asset_in == state.market.long_asset
    incoming, outgoing = (state.long, state.short) if forward else (state.short, state.long)
    usd = amount * incoming.price.midpoint
    impact = spot_impact(state, (usd, -usd) if forward else (-usd, usd))
    fees = fees_for(amount, state.fees, impact.balance_improved)
    output, adjusted, input_impact, pool_output = output_amount(
        incoming, outgoing, fees.after_fees, impact.usd_e30
    )
    return SwapCalculation(
        asset_in=asset_in,
        asset_out=outgoing.price.asset,
        amount_in=amount,
        amount_out=output,
        price_impact_usd_e30=impact.usd_e30,
        impact_amount=adjusted,
        input_impact_amount=input_impact,
        pool_amount_out=pool_output,
        fees=fees,
    )
