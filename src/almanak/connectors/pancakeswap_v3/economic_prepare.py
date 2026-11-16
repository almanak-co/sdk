"""Concrete model identity and acquisition bound to shared private preparation."""

from almanak.backtest.gateway import ContractBinding
from almanak.connectors.dex_economic_dataset import targets
from almanak.connectors.pancakeswap_v3 import economic_collection, economic_data
from almanak.connectors.pancakeswap_v3.deployments import factory_deployer, verify_pool_code
from almanak.connectors.pancakeswap_v3.economic_book import PoolBook
from almanak.connectors.pancakeswap_v3.economic_parameters import parameters
from almanak.connectors.v3_economic_prepare import PreparationBinding
from almanak.models import ZERO_ADDRESS


def deployer_bindings(histories, config, boundaries, bindings):
    result = []
    for binding in bindings:
        history = histories[binding.chain]
        first, _ = boundaries[binding.chain]
        history.pin(first)
        dependencies = set()
        for target in targets(config):
            if target.fund.chain != binding.chain:
                continue
            dependencies.add(factory_deployer(history.reader, target.pool, first)[0])
            verify_pool_code(history.reader, target.pool, first)
            callback = history.reader.call(
                target.pool.pool, "lmPool()", first, outputs=("address",)
            )[0]
            if callback != ZERO_ADDRESS:
                dependencies.add(callback)
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
    dependencies=deployer_bindings,
)
selected_parameters = _binding.selected_parameters
validate_sources = _binding.validate_sources
dataset_directory = _binding.dataset_directory
provision = _binding.provision
authorize_ranges = _binding.authorize_ranges
prepare = _binding.prepare
prepare_locked = _binding.prepare_locked
acquire = _binding.acquire
