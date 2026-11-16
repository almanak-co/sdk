"""Bind exact GMX publication acquisition to the shared immutable dataset writer."""

from almanak.connectors.dex_economic_prepare import PreparationBinding as SharedPreparation
from almanak.connectors.gmx_v2 import economic_collection, economic_data
from almanak.connectors.gmx_v2.economic_authority import authorize_ranges
from almanak.connectors.gmx_v2.economic_book import MarketBook
from almanak.connectors.gmx_v2.economic_parameters import parameters
from almanak.connectors.gmx_v2.exchange_models import SwapAllocation
from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation
from almanak.connectors.gmx_v2.models import Allocation


def validate_policy(config, settings):
    kinds = {type(allocation) for allocation in config.allocations}
    if kinds not in ({SwapAllocation}, {LiquidityAllocation}, {Allocation}):
        raise ValueError("GMX prepared publication datasets require one supported request family")
    if bool(settings.liquidity_model) != (kinds == {LiquidityAllocation}) or bool(
        settings.position_model
    ) != (kinds == {Allocation}):
        raise ValueError("GMX prepared model selection differs from its exact request family")
    if settings.data.maximum_report_age_seconds > settings.data.max_age_seconds:
        raise ValueError("GMX report age bound cannot exceed the explicit carried input age")


class PreparationBinding(SharedPreparation):
    def authorize_ranges(self, directory, config, settings, env_file):
        return authorize_ranges(directory, config, settings, env_file)


binding = PreparationBinding(
    data=economic_data,
    collection=economic_collection,
    book_type=MarketBook,
    parameters=parameters,
    validate_policy=validate_policy,
    model_identity=economic_data.revisions,
)
selected_parameters = binding.selected_parameters
validate_sources = binding.validate_sources
dataset_directory = binding.dataset_directory
prepare = binding.prepare
prepare_locked = binding.prepare_locked
