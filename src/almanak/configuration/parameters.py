"""Construct complete parameters only from matching, replay-checked resolution."""

from pydantic import ValidationError

from almanak.caip import AssetType
from almanak.configuration.authoring import binding_for
from almanak.configuration.bootstrap import ResolutionProgress
from almanak.configuration.input_values import resolve_quantity
from almanak.configuration.parameter_types import parameter_type
from almanak.configuration.unresolved import InputDocument
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.support import RunMode
from almanak.effects import Balance, WalletAsset
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_metadata
from almanak.gateway.fixture_accounts import FixtureAccounts
from almanak.gateway.resolution_evm import RetainedResolutionReader, asset_symbols
from almanak.models import AssetMetadata, Model
from almanak.triggers import IntervalTrigger

GROUPS = ("allocations", "debts")


def replay_facts(result):
    for anchor in result.anchors.checkpoints:
        markets = tuple(m for m in result.markets if m.resolved.block.chain == anchor.chain)
        evidence = tuple(row for market in markets for row in market.evidence)
        with RetainedResolutionReader(anchor.chain, evidence) as reader:
            for market in markets:
                selection = market.resolved.selection
                resolve = binding_for(selection.connector).resolve
                observed = resolve(reader, selection, market.resolved.block)
                if observed != market.resolved:
                    raise DataError(
                        "resolution_evidence", "resolved facts differ from retained reads"
                    )
                block = market.resolved.block
                if asset_symbols(reader, market.resolved.assets, block) != market.symbols:
                    raise DataError(
                        "resolution_evidence", "asset symbols differ from retained reads"
                    )


def admit_new_positions(document: InputDocument, result):
    """Preflight for a new run only: connector refusals that need the resolved reads.

    Retained parameters (resume, exit) and backtests never pass through here, so a check
    added later cannot strand a deployment whose parameters are already frozen.
    """
    for anchor in result.anchors.checkpoints:
        markets = tuple(m for m in result.markets if m.resolved.block.chain == anchor.chain)
        evidence = tuple(row for market in markets for row in market.evidence)
        with RetainedResolutionReader(anchor.chain, evidence) as reader:
            check_resolved_positions(document, reader, {m.name: m.resolved for m in markets})


def check_resolved_positions(document, reader, markets):
    """Connector checks that need the market's resolved reads, not only its frozen facts."""
    for group in GROUPS:
        for name, selection in getattr(document.inputs.strategy, group).items():
            check = getattr(selection, "check_resolved", None)
            if check is None or selection.market not in markets:
                continue
            try:
                check(reader, markets[selection.market])
            except ConfigurationError as error:
                raise document.error(error, f"strategy.{group}.{name}") from error


def construct_parameters(
    document: InputDocument, progress: ResolutionProgress, *, fixture: FixtureAccounts | None = None
) -> Model:
    inputs = document.inputs
    try:
        inputs.require_context(progress.context)
    except ConfigurationError as error:
        raise document.error(error) from error
    result = progress.result
    if result is None:
        raise document.error(ConfigurationError("market resolution is incomplete", field="markets"))
    replay_facts(result)
    markets = {market.name: market.resolved for market in result.markets}
    symbols = {a: name for market in result.markets for a, name in market.symbols.items()}
    metadata = resolved_metadata(document, markets, symbols)
    groups = grouped_positions(document, markets)
    gas = gas_policies(document, progress.context.mode)
    accounts = result.binding.accounts
    if fixture is not None:
        fixture.require_resolution(progress)
        accounts = fixture.accounts
        groups = fork_rescope(groups, fixture)
    return assemble(document, groups, accounts, metadata, gas)


def grouped_positions(document, markets) -> dict[ConnectorId, dict[str, list]]:
    """Resolved positions grouped by the connector of their named market."""
    inputs = document.inputs
    groups: dict[ConnectorId, dict[str, list]] = {}
    for group in GROUPS:
        for name, selection in getattr(inputs.strategy, group).items():
            identity = ConnectorId(inputs.markets[selection.market].connector)
            resolved = resolve_position(document, name, selection, markets, group)
            groups.setdefault(identity, {g: [] for g in GROUPS})[group].append(resolved)
    return groups


def resolve_position(document, name, selection, markets, group):
    try:
        return selection.resolve(name, markets[selection.market])
    except (ConfigurationError, ValidationError) as error:
        raise document.error(error, f"strategy.{group}.{name}") from error


def fork_rescope(groups, fixture):
    """Positions move to the generated fixture chains; targets keep their origin identity."""
    from almanak.connectors.composed.fork import remap_allocation

    chains = {b.origin.chain: b.chain for b in fixture.bindings}
    return {
        identity: {
            group: [remap_allocation(item, chains[item.chain]) for item in items]
            for group, items in parts.items()
        }
        for identity, parts in groups.items()
    }


def assemble(document, groups, accounts, metadata, gas) -> Model:
    """Build one authoritative parameter instance across all named markets."""
    if len(groups) > 1:
        by_name = {
            item.name: item
            for parts in groups.values()
            for group in GROUPS
            for item in parts[group]
        }
        names = (
            *document.inputs.strategy.allocations,
            *document.inputs.strategy.debts,
        )
        positions = tuple(by_name[name] for name in names)
        composed_values = {
            "connector": None,
            "allocations": positions,
            "inventory": resolved_inventory(document, accounts, metadata),
        }
        return validate_parameters(document, None, composed_values, accounts, metadata, gas)
    if document.inputs.inventory:
        raise document.error(
            ConfigurationError(
                "inventory requires positions from more than one connector", field="inventory"
            )
        )
    ((identity, parts),) = groups.items()
    values: dict[str, object] = {"allocations": tuple(parts["allocations"])}
    if identity == ConnectorId("aave_v4"):
        values["debts"] = tuple(parts["debts"])
    return validate_parameters(document, identity, values, accounts, metadata, gas)


def validate_parameters(document, identity, values, accounts, metadata, gas):
    trigger = IntervalTrigger(kind="interval", seconds=document.inputs.strategy.decision_interval)
    values.update(
        decision_trigger=trigger,
        accounts=accounts,
        asset_metadata=metadata,
        gas=gas,
        parameters=document.inputs.strategy.parameters,
    )
    try:
        return parameter_type(identity).model_validate(values)
    except ValidationError as error:
        raise document.error(error, "strategy") from error


def resolved_inventory(document, accounts, metadata):
    """Resolve shared owned cash once, using verified precision and bound fund scope."""
    result = []
    for index, item in enumerate(document.inputs.inventory):
        try:
            binding = accounts.binding(item.account)
            result.append(
                Balance(
                    key=WalletAsset(
                        account=item.account,
                        chain=binding.profile.fund.chain,
                        asset=item.balance.asset,
                    ),
                    amount=resolve_quantity(
                        item.balance.quantity, metadata[item.balance.asset].decimals
                    ),
                )
            )
        except (ConfigurationError, KeyError, ValidationError) as error:
            raise document.error(error, f"inventory.{index}") from error
    return tuple(result)


def resolved_metadata(document, markets, symbols=None):
    """Verified precision per asset, named by the author or else by the token itself."""
    symbols = symbols or {}
    metadata: dict[AssetType, AssetMetadata] = {}
    for market in markets.values():
        for asset in market.assets:
            previous = metadata.setdefault(
                asset.identifier,
                AssetMetadata(decimals=asset.decimals, symbol=symbols.get(asset.identifier)),
            )
            if previous.decimals != asset.decimals:
                raise document.error(
                    ConfigurationError("resolved asset precision is inconsistent", field="markets")
                )
    for account in document.inputs.accounts.values():
        identity, native = native_metadata(account.chain)
        metadata[identity] = native
    for asset, expected in document.inputs.assets.items():
        field = f"assets.{asset}"
        if asset not in metadata:
            raise document.error(
                ConfigurationError("asset metadata has no verified resolution", field=field)
            )
        if expected.decimals is not None and metadata[asset].decimals != expected.decimals:
            raise document.error(
                ConfigurationError(
                    "asset precision assertion differs from verified metadata",
                    field=field + ".decimals",
                )
            )
        metadata[asset] = AssetMetadata(
            decimals=metadata[asset].decimals, symbol=expected.symbol or metadata[asset].symbol
        )
    return metadata


def gas_policies(document, mode):
    if mode == RunMode.PRODUCTION and not document.inputs.strategy.gas:
        raise document.error(
            ConfigurationError(
                "production requires explicit author gas policy", field="strategy.gas"
            )
        )
    result = []
    for name, policy in document.inputs.strategy.gas.items():
        try:
            result.append(policy.resolve(name))
        except (ConfigurationError, ValidationError) as error:
            raise document.error(error, f"strategy.gas.{name}") from error
    return tuple(result)
