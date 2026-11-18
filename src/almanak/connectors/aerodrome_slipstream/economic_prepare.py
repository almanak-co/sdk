"""Discover historical dependencies from the configured compiled factory."""

from almanak.backtest.gateway import ContractBinding
from almanak.connectors.aerodrome_slipstream import economic_collection, economic_data
from almanak.connectors.aerodrome_slipstream.deployments import (
    address_word,
    fee_modules,
    read_code,
)
from almanak.connectors.aerodrome_slipstream.economic_book import PoolBook
from almanak.connectors.aerodrome_slipstream.economic_parameters import parameters
from almanak.connectors.dex_economic_dataset import targets
from almanak.connectors.v3_economic_prepare import PreparationBinding
from almanak.models import ZERO_ADDRESS


def dependency_bindings(histories, config, boundaries, bindings):
    result = []
    for binding in bindings:
        history = histories[binding.chain]
        first, _ = boundaries[binding.chain]
        history.pin(first)
        dependencies = set()
        for target in targets(config):
            if target.fund.chain != binding.chain:
                continue
            values, _ = read_code(history.reader, target.pool.factory, first, "factory")
            dependencies.add(address_word(values["poolImplementation"]))
            dependencies.update(fee_modules(history.reader, target.pool, first).values())
        dependencies.discard(ZERO_ADDRESS)
        result.append(
            ContractBinding(
                chain=binding.chain,
                contracts=tuple(sorted(set(binding.contracts) | dependencies)),
                logs=binding.logs,
            )
        )
    return tuple(result)


_binding = PreparationBinding(
    data=economic_data,
    collection=economic_collection,
    book_type=PoolBook,
    parameters=parameters,
    dependencies=dependency_bindings,
)
selected_parameters = _binding.selected_parameters
validate_sources = _binding.validate_sources
dataset_directory = _binding.dataset_directory
provision = _binding.provision
authorize_ranges = _binding.authorize_ranges
prepare = _binding.prepare
prepare_locked = _binding.prepare_locked
acquire = _binding.acquire
