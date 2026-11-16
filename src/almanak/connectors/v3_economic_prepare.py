"""Concentrated pools require explicit tick coverage of every LP range and band."""

from almanak.connectors.band_fee_growth import validate_envelope
from almanak.connectors.dex_economic_dataset import targets
from almanak.connectors.dex_economic_prepare import PreparationBinding as ExchangePreparation


def validate_tick_coverage(target, data):
    bound = data.for_target(target)
    spacing = target.pool.tick_spacing
    words = ((bound.upper // spacing) >> 8) - ((bound.lower // spacing) >> 8) + 1
    if words > data.max_bitmap_words:
        raise ValueError("V3 tick coverage exceeds the explicit bitmap word limit")
    if target.range is not None and not (
        bound.lower <= target.range.lower < target.range.upper <= bound.upper
    ):
        raise ValueError("V3 LP range must lie inside explicit prepared tick coverage")
    if target.band is not None:
        validate_envelope(target.band, bound.lower, bound.upper, spacing)


def validate_policy(config, settings):
    if {a.name for a in config.allocations} != {c.allocation for c in settings.data.coverage}:
        raise ValueError("V3 tick coverage must identify every allocation exactly")
    for target in targets(config):
        validate_tick_coverage(target, settings.data)


def PreparationBinding(**kwargs):
    return ExchangePreparation(validate_policy=validate_policy, **kwargs)
