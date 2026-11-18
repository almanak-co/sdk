"""Concrete model identity and acquisition bound to shared private preparation."""

from almanak.connectors.uniswap_v3 import economic_collection, economic_data
from almanak.connectors.uniswap_v3.economic_book import PoolBook
from almanak.connectors.uniswap_v3.economic_parameters import parameters
from almanak.connectors.v3_economic_prepare import PreparationBinding

_binding = PreparationBinding(
    data=economic_data, collection=economic_collection, book_type=PoolBook, parameters=parameters
)
selected_parameters = _binding.selected_parameters
validate_sources = _binding.validate_sources
dataset_directory = _binding.dataset_directory
provision = _binding.provision
authorize_ranges = _binding.authorize_ranges
prepare = _binding.prepare
prepare_locked = _binding.prepare_locked
acquire = _binding.acquire
