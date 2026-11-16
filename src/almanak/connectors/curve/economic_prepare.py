"""Prepare Curve history through explicit read-only gateway authority."""

from almanak.backtest.gateway import ContractBinding
from almanak.connectors.curve import economic_collection, economic_data
from almanak.connectors.curve.deployment import read_stable_code, registered
from almanak.connectors.curve.economic_book import PoolBook
from almanak.connectors.curve.economic_collection import grid
from almanak.connectors.curve.economic_parameters import parameters
from almanak.connectors.dex_economic_dataset import targets
from almanak.connectors.dex_economic_prepare import PreparationBinding
from almanak.connectors.evm_revision import read_code


def dependencies_at(history, target, block):
    if target.pool.family == "tricrypto_ng":
        return tri_dependencies(history.reader, target.pool, block)
    if target.pool.family == "twocrypto_ng":
        return crypto_dependencies(history.reader, target.pool, block)
    read_stable_code(history.reader, target.pool.factory, block, "factory")
    return (
        registered(
            history.reader, target.pool, "get_implementation_address(address)", block, ("address",)
        ),
        history.reader.call(
            target.pool.factory, "views_implementation()", block, outputs=("address",)
        )[0],
    )


def crypto_dependencies(reader, source, block):
    from almanak.connectors.curve import crypto_deployment

    compiled, _ = crypto_deployment.factory_revision(reader, source, block)
    values, _ = read_code(reader, source.pool, block, compiled["pool"])
    crypto_deployment.verify_immutables(source, values)
    return (
        crypto_deployment.original_blueprint(
            reader, source, block, pool_data_slot=crypto_deployment.blueprint_slot(compiled)
        ),
        "0x" + values["MATH"].to_bytes(20, "big").hex(),
        reader.call(source.factory, "views_implementation()", block, outputs=("address",))[0],
    )


def tri_dependencies(reader, source, block):
    from almanak.connectors.curve import tricrypto_deployment

    selected = source.implementation_revision
    tricrypto_deployment.verified_component(reader, source.factory, block, "factory", selected)
    values, _ = tricrypto_deployment.verified_component(
        reader, source.pool, block, "pool", selected
    )
    tricrypto_deployment.verify_immutables(source, values)
    return (
        "0x" + values["MATH"].to_bytes(20, "big").hex(),
        reader.call(source.factory, "views_implementation()", block, outputs=("address",))[0],
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
    if len(grid(settings)) > settings.data.max_samples:
        raise ValueError("Curve sampling exceeds its explicit per-pool sample limit")


_binding = PreparationBinding(
    data=economic_data,
    collection=economic_collection,
    book_type=PoolBook,
    parameters=parameters,
    dependencies=dependency_bindings,
    validate_policy=validate_policy,
    model_identity=economic_data.model_identity,
)
selected_parameters = _binding.selected_parameters
validate_sources = _binding.validate_sources
dataset_directory = _binding.dataset_directory
prepare = _binding.prepare
provision = _binding.provision
authorize_ranges = _binding.authorize_ranges
acquire = _binding.acquire
