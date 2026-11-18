"""Prepare Aerodrome history through explicit read-only gateway authority."""

from almanak.backtest.gateway import ContractBinding
from almanak.connectors.aerodrome import economic_collection, economic_data
from almanak.connectors.aerodrome.deployments import address_word, read_code
from almanak.connectors.aerodrome.economic_book import PoolBook
from almanak.connectors.aerodrome.economic_collection import sample_count
from almanak.connectors.aerodrome.economic_parameters import parameters
from almanak.connectors.dex_economic_dataset import targets
from almanak.connectors.dex_economic_prepare import PreparationBinding


def dependencies_at(history, target, block):
    factory, _ = read_code(history.reader, target.pool.factory, block, "factory")
    router, _ = read_code(history.reader, target.contracts.router, block, "router")
    return (
        address_word(factory["implementation"]),
        address_word(router["factoryRegistry"]),
        history.reader.call(target.pool.pool, "poolFees()", block, outputs=("address",))[0],
    )


def dependency_bindings(histories, config, boundaries, bindings):
    result = []
    for binding in bindings:
        history = histories[binding.chain]
        first, _ = boundaries[binding.chain]
        history.pin(first)
        dependencies = {
            address
            for target in targets(config)
            if target.fund.chain == binding.chain
            for address in dependencies_at(history, target, first)
        }
        result.append(
            ContractBinding(
                chain=binding.chain,
                contracts=tuple(sorted(set(binding.contracts) | dependencies)),
                logs=binding.logs,
            )
        )
    return tuple(result)


def validate_policy(config, settings):
    if sample_count(settings) > settings.data.max_samples:
        raise ValueError("Aerodrome sampling exceeds its explicit per-pool sample limit")


_binding = PreparationBinding(
    data=economic_data,
    collection=economic_collection,
    book_type=PoolBook,
    parameters=parameters,
    dependencies=dependency_bindings,
    validate_policy=validate_policy,
)
selected_parameters = _binding.selected_parameters
validate_sources = _binding.validate_sources
dataset_directory = _binding.dataset_directory
prepare = _binding.prepare
provision = _binding.provision
authorize_ranges = _binding.authorize_ranges
acquire = _binding.acquire
