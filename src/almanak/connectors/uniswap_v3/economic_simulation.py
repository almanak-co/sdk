"""Connector-owned quote construction with verified V3 hypothetical effects."""

from almanak.connectors import v3_economic_simulation as family
from almanak.connectors.uniswap_v3.positions import Quote
from almanak.connectors.uniswap_v3.swap_math import exact_input
from almanak.connectors.v3_economic_simulation import pending_fees as pending_fees


def quote_at(record, target, amount, time):
    return family.quote_at(record, target, amount, time, quote_type=Quote, exact_input=exact_input)


def simulate(config, intent, payload, record, time, identity, growth=None):
    return family.simulate(config, intent, payload, record, time, identity, quote_at, growth)
