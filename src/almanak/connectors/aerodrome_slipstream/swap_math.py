"""Slipstream's verified swap steps use the sampled undiscounted dynamic fee."""

from almanak.connectors.concentrated_swap import v3_step as swap_step


def exact_input(sample, amount, zero_for_one):
    from almanak.connectors.concentrated_swap import exact_input as calculate

    return calculate(sample, amount, zero_for_one, sample.pool.swap_fee, swap_step)
