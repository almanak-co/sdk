"""Publication evidence bounds each staged provider, verifier and target-Oracle grant."""

from contextlib import ExitStack

from eth_utils import keccak

from almanak.backtest.cli import GatewayProcess, write_json
from almanak.backtest.history_authority import CallerScope
from almanak.connectors.dex_economic_dataset import targets
from almanak.connectors.dex_economic_prepare import gateway_config, open_histories
from almanak.connectors.gmx_v2.economic_collection import (
    contract_bindings,
    dependency_bindings,
    publication_candidates,
    resolve_boundaries,
)
from almanak.connectors.gmx_v2.oracle_reader import stored, validate_provider
from almanak.connectors.gmx_v2.published_source import report_vectors
from almanak.gateway.ethereum import DataError
from almanak.models import Address, Chain


def selected_source_blocks(history, target, settings, bounds):
    source = settings.data.publication(target.allocation)
    candidates = publication_candidates(history, target, settings, *bounds)
    blocks = {}
    for transaction, (number, block_hash, _) in candidates.items():
        block = history.reader.pin_block(number)
        if block.hash != block_hash:
            raise DataError("reorganization", "GMX source discovery changed its canonical block")
        value = history.reader._rpc("eth_getTransactionByHash", [transaction])
        if value is None or value.get("hash") != transaction:
            raise DataError("identity", "GMX source discovery requires its exact transaction")
        if report_vectors(value, source) is None:
            continue
        if value.get("blockHash") != block_hash:
            raise DataError("reorganization", "GMX source discovery changed its canonical block")
        blocks[number] = block
    return tuple(blocks.values())


def provider_scopes(histories, config, settings, boundaries):
    result = set()
    for target in targets(config):
        history, market = histories[target.fund.chain], target.market
        source = settings.data.publication(target.allocation)
        for block in selected_source_blocks(
            history, target, settings, boundaries[target.fund.chain]
        ):
            reader = history.reader
            if reader.pin_block(block.number) != block:
                raise DataError("reorganization", "GMX source discovery changed its pinned block")
            binding = tuple(
                reader.call(source.handler, name + "()", block, outputs=("address",))[0]
                for name in ("dataStore", "oracle")
            )
            if binding != (market.contracts.data_store, source.oracle):
                raise ValueError("GMX published source handler changed its selected Oracle binding")
            for oracle in (source.oracle, market.contracts.oracle):
                for asset in dict.fromkeys(
                    (market.index_asset, market.long_asset, market.short_asset)
                ):
                    provider = stored(
                        reader,
                        block,
                        market.contracts.data_store,
                        "Address",
                        "ORACLE_PROVIDER_FOR_TOKEN",
                        ("address", "address"),
                        (oracle, asset.reference),
                    )
                    result.add((block, market.contracts.data_store, oracle, provider))
    return tuple(
        sorted(
            result, key=lambda value: (str(value[0].chain.chain_id), value[0].number, *value[1:])
        )
    )


def expanded(contracts, addresses, config=None):
    result = []
    for binding in contracts:
        callers = (
            ()
            if config is None
            else tuple(
                dict.fromkeys(
                    CallerScope(
                        sender=target.market.contracts.order_handler,
                        target=target.market.contracts.oracle,
                        selectors=("0x" + keccak(text=signature)[:4].hex(),),
                    )
                    for target in targets(config)
                    if target.fund.chain == binding.chain
                    for signature in (
                        "validatePrices((address[],address[],bytes[]),bool)",
                        "setPrices((address[],address[],bytes[]))",
                    )
                )
            )
        )
        result.append(
            binding.model_copy(
                update={
                    "contracts": tuple(
                        sorted(set(binding.contracts) | addresses.get(binding.chain, set()))
                    ),
                    "callers": callers,
                }
            )
        )
    return tuple(result)


def configure(directory, settings, contracts):
    write_json(directory / "history.json", gateway_config(settings, contracts))


def authorize_ranges(directory, config, settings, env_file):
    contracts = contract_bindings(settings, config)
    configure(directory, settings, contracts)
    with ExitStack() as stack:
        gateway = stack.enter_context(GatewayProcess(directory, env_file))
        histories = open_histories(stack, directory, settings, gateway.endpoint)
        boundaries = resolve_boundaries(histories, config, settings)
        # Perpetual fee reads consult the ReferralStorage the OrderHandler selects.
        contracts = dependency_bindings(
            histories, config, boundaries, contract_bindings(settings, config, boundaries)
        )
    configure(directory, settings, contracts)
    with ExitStack() as stack:
        gateway = stack.enter_context(GatewayProcess(directory, env_file))
        histories = open_histories(stack, directory, settings, gateway.endpoint)
        providers = provider_scopes(histories, config, settings, boundaries)
    addresses: dict[Chain, set[Address]] = {}
    for block, _, _, provider in providers:
        addresses.setdefault(block.chain, set()).add(provider)
    contracts = expanded(contracts, addresses)
    configure(directory, settings, contracts)
    with ExitStack() as stack:
        gateway = stack.enter_context(GatewayProcess(directory, env_file))
        histories = open_histories(stack, directory, settings, gateway.endpoint)
        for block, store, oracle, provider in providers:
            reader = histories[block.chain].reader
            if reader.pin_block(block.number) != block:
                raise ValueError("GMX provider discovery changed its canonical publication block")
            verifier = validate_provider(reader, block, provider, store, oracle)
            addresses[block.chain].add(verifier)
    configure(directory, settings, expanded(contracts, addresses, config))
    return boundaries
