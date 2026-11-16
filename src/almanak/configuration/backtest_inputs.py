"""Public backtest settings: syntax-checked inputs that resolve with verified parameters.

Historical provider bindings come from the operator configuration, never from
the author's backtest file, so retained runs carry no provider identity.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BeforeValidator, Field, JsonValue, ValidationError

from almanak.backtest.config import HistoryBinding
from almanak.backtest.pricing import VendorBinding
from almanak.caip import AssetType, ChainId
from almanak.configuration.anchors import AnchorSelection
from almanak.configuration.input_values import Duration, Quantity
from almanak.configuration.parameter_types import COMPOSED, parameter_tag
from almanak.configuration.resolution import simulation_binding
from almanak.configuration.unresolved import AuthorInputs, diagnostic, located
from almanak.configuration.values import amount, duration_fields, instant
from almanak.configuration.yaml import ConfigurationError, Document, load
from almanak.gateway.operator_config import Name, OperatorConfig
from almanak.models import Chain, Model, UInt

Instant = Annotated[UInt, BeforeValidator(instant)]


class BalanceInput(Model):
    asset: AssetType
    quantity: Quantity


class FundInput(Model):
    balances: Annotated[tuple[BalanceInput, ...], Field(min_length=1)]
    payer: Name | None = Field(default=None, exclude_if=lambda value: value is None)


class PayerInput(Model):
    chain: ChainId
    balances: Annotated[tuple[BalanceInput, ...], Field(min_length=1)]


class InventoryInput(Model):
    funds: Annotated[dict[Name, FundInput], Field(min_length=1)]
    payers: dict[Name, PayerInput] = Field(default_factory=dict, exclude_if=lambda v: not v)


class WindowInput(Model):
    start: Instant
    end: Instant
    end_policy: Literal["mark_open", "teardown"]
    cleanup: Duration


class ExecutionInput(Model):
    latency: Duration
    decision_interval: Annotated[Duration, Field(gt=0)] | None = None
    models: dict[str, JsonValue] = Field(default_factory=dict)


class VendorSelection(Model):
    name: Name
    kind: Literal["coingecko"]


class BacktestInputs(Model):
    mode: Literal["historical_path", "economic"]
    window: WindowInput
    inventory: InventoryInput
    execution: ExecutionInput
    data: dict[str, JsonValue] = Field(default_factory=dict)
    costs: dict[str, JsonValue]

    def schedule(self) -> dict:
        result: dict = {
            "start": self.window.start,
            "end": self.window.end,
            "end_policy": self.window.end_policy,
            "cleanup_seconds": self.window.cleanup,
            "latency_seconds": self.execution.latency,
        }
        if self.execution.decision_interval is not None:
            result["decision_interval_override_seconds"] = self.execution.decision_interval
        return result

    def require_accounts(self, inputs: AuthorInputs):
        for name, payer in self.inventory.payers.items():
            require_payer(name, payer, inputs)
        for name, fund in self.inventory.funds.items():
            self.require_fund(name, fund, inputs)

    def require_fund(self, name, fund: FundInput, inputs: AuthorInputs):
        field = f"inventory.funds.{name}"
        account = inputs.accounts.get(name)
        if account is None:
            raise ConfigurationError("hypothetical fund has no author account", field=field)
        require_balance_chain(account.chain, fund.balances, field, "fund")
        if fund.payer is not None:
            self.require_fund_payer(name, fund.payer, account.chain)

    def require_fund_payer(self, name, payer: str, chain: ChainId):
        declared = self.inventory.payers.get(payer)
        if declared is None or declared.chain != chain:
            raise ConfigurationError(
                "fund payer must name a declared payer on the fund chain",
                field=f"inventory.funds.{name}.payer",
            )

    def anchors(self, inputs: AuthorInputs) -> tuple[AnchorSelection, ...]:
        """One anchor per account chain: the last block at the window start."""
        chains = dict.fromkeys(account.chain for account in inputs.accounts.values())
        return tuple(AnchorSelection(chain=chain, at=self.window.start) for chain in chains)


def require_payer(name, payer: PayerInput, inputs: AuthorInputs):
    field = f"inventory.payers.{name}"
    if name in inputs.accounts:
        raise ConfigurationError(
            "payer name collides with an author account", field=f"{field}.chain"
        )
    require_balance_chain(payer.chain, payer.balances, field, "payer")


def require_balance_chain(chain: ChainId, balances, field: str, scope: str):
    for index, balance in enumerate(balances):
        if balance.asset.chain != chain:
            raise ConfigurationError(
                f"balance asset belongs to another chain than its {scope}",
                field=f"{field}.balances.{index}.asset",
            )


@dataclass(frozen=True)
class BacktestDocument:
    inputs: BacktestInputs
    source: Document

    def error(self, error, prefix="") -> ConfigurationError:
        return diagnostic(self.source, error, prefix)

    def require_accounts(self, inputs: AuthorInputs):
        try:
            self.inputs.require_accounts(inputs)
        except ConfigurationError as error:
            raise self.error(error) from error


def parse_backtest(document: Document) -> BacktestDocument:
    data = document.value.get("data")
    if isinstance(data, dict) and "history" in data:
        raise located(
            document,
            ConfigurationError(
                "historical providers come from the operator configuration; remove data.history",
                field="data.history",
            ),
        )
    if document.value.get("mode") == "historical_path" and data:
        raise located(
            document,
            ConfigurationError(
                "historical_path takes no data policy; a strategy declares its price "
                "series in data_dependencies",
                field="data",
            ),
        )
    refuse_strategy_prices(document, data)
    validate_authored_vendors(document, data)
    try:
        inputs = BacktestInputs.model_validate_json(json.dumps(document.value))
    except ValidationError as error:
        raise diagnostic(document, error) from error
    return BacktestDocument(inputs, document)


def read_backtest_inputs(path: Path) -> BacktestDocument:
    return parse_backtest(load(path))


def history_bindings(operator: OperatorConfig, chains) -> tuple[HistoryBinding, ...]:
    return tuple(
        HistoryBinding(
            chain=Chain(chain_id=chain, environment="mainnet"),
            provider=operator.network(chain).binding(),
        )
        for chain in chains
    )


def resolve_backtest(
    document: BacktestDocument,
    parameters: Model,
    history: tuple[HistoryBinding, ...],
    vendors: tuple[VendorBinding, ...] = (),
):
    """The settings wire of whichever connector the resolved parameters name."""
    from almanak.connectors.composition import connector_for

    builders = {"aave_v4": aave_wire, COMPOSED: composed_wire}
    builder = builders.get(parameter_tag(parameters), connector_wire)
    wire = builder(document, parameters, history)
    bind_vendor_access(document, wire, vendors)
    settings_type = connector_for(parameters).backtest(document.inputs.mode).settings_type
    try:
        return settings_type.model_validate_json(json.dumps(wire))
    except ValidationError as error:
        raise document.error(error) from error


def refuse_strategy_prices(document: Document, value, prefix="data"):
    """Only the strategy declares a pool price series; a backtest never repeats one.

    A single strategy price (an Aave V4 economic input) may still be listed here, and
    must then equal the strategy's declaration.
    """
    if not isinstance(value, dict):
        return
    prices = value.get("prices")
    for index, binding in enumerate(prices if isinstance(prices, list) else ()):
        if isinstance(binding, dict) and binding.get("purpose") == "strategy":
            source = binding.get("source")
            kind = source.get("kind") if isinstance(source, dict) else None
            if kind not in ("pool_state", "pool_swaps") and binding.get("window", 1) == 1:
                continue
            raise located(
                document,
                ConfigurationError(
                    "strategy price series are declared by the strategy's data_dependencies "
                    "hook, not the backtest configuration; remove this entry",
                    field=f"{prefix}.prices.{index}.purpose",
                ),
            )
    connectors = value.get("connectors")
    if isinstance(connectors, dict):
        for name, policy in connectors.items():
            refuse_strategy_prices(document, policy, f"{prefix}.connectors.{name}")


def validate_authored_vendors(document: Document, value, prefix="data"):
    if not isinstance(value, dict):
        return
    declarations = value.get("vendors")
    if declarations is not None and not isinstance(declarations, list):
        raise located(
            document,
            ConfigurationError("vendors must be a list", field=f"{prefix}.vendors"),
        )
    if declarations is not None:
        for index, declaration in enumerate(declarations):
            field = f"{prefix}.vendors.{index}"
            if not isinstance(declaration, dict):
                raise located(
                    document,
                    ConfigurationError(
                        "vendor selection must contain a name and kind", field=field
                    ),
                )
            if isinstance(declaration, dict) and "provider" in declaration:
                raise located(
                    document,
                    ConfigurationError(
                        "vendor access belongs in the operator configuration; "
                        "declare only its name and kind here",
                        field=f"{field}.provider",
                    ),
                )
            try:
                VendorSelection.model_validate(declaration)
            except ValidationError as error:
                raise diagnostic(document, error, field) from error
    connectors = value.get("connectors")
    if isinstance(connectors, dict):
        for name, policy in connectors.items():
            validate_authored_vendors(document, policy, f"{prefix}.connectors.{name}")


def bind_vendor_access(
    document: BacktestDocument, wire: dict, available: tuple[VendorBinding, ...]
):
    """Bind only author-selected vendors to operator-owned access settings."""
    selected = selected_vendors(document.inputs.data)
    if not selected:
        return
    bindings = {binding.name: binding for binding in available}
    for field, name, kind in selected:
        binding = bindings.get(name)
        if binding is None:
            raise document.error(
                ConfigurationError(
                    f"operator configuration has no vendor binding named {name}",
                    field=f"{field}.name",
                )
            )
        if binding.provider.kind != kind:
            raise document.error(
                ConfigurationError(
                    f"operator vendor {name} is {binding.provider.kind}, not {kind}",
                    field=f"{field}.kind",
                )
            )
    inject_vendor_access(wire.get("data"), bindings)
    for model in wire.get("models", {}).values():
        inject_vendor_access(model.get("data"), bindings)


def selected_vendors(value, prefix="data") -> list[tuple[str, str, str]]:
    selected: list[tuple[str, str, str]] = []
    if not isinstance(value, dict):
        return selected
    declarations = value.get("vendors", ())
    if isinstance(declarations, (list, tuple)):
        for index, declaration in enumerate(declarations):
            if isinstance(declaration, dict):
                name, kind = declaration.get("name"), declaration.get("kind")
                if isinstance(name, str) and isinstance(kind, str):
                    selected.append((f"{prefix}.vendors.{index}", name, kind))
    connectors = value.get("connectors")
    if isinstance(connectors, dict):
        for name, policy in connectors.items():
            selected.extend(selected_vendors(policy, f"{prefix}.connectors.{name}"))
    return selected


def inject_vendor_access(value, bindings):
    if not isinstance(value, dict):
        return
    declarations = value.get("vendors")
    if isinstance(declarations, list):
        value["vendors"] = [bindings[item["name"]].model_dump(mode="json") for item in declarations]


def aave_wire(document: BacktestDocument, parameters, history) -> dict:
    """Aave V4 keeps the flat fund identity and models no separate gas payer."""
    refuse_payers(document)
    funds = document.inputs.inventory.funds
    return single_wire(
        document,
        history,
        [fund_wire(document, name, fund, parameters) for name, fund in funds.items()],
    )


def connector_wire(document: BacktestDocument, parameters, history) -> dict:
    """Every other connector carries a simulation profile and its own gas payers."""
    funds, payers = profiled_inventory(document, parameters)
    return single_wire(document, history, funds, payers)


def composed_wire(document: BacktestDocument, parameters, history) -> dict:
    """A composed run pairs one data policy, one cost policy and one model per connector."""
    funds, payers = profiled_inventory(document, parameters)
    return {
        **head_wire(document, history),
        "funds": funds,
        "payers": payers,
        "models": composed_models(document),
    }


def profiled_inventory(document: BacktestDocument, parameters) -> tuple[list, list]:
    inventory = document.inputs.inventory
    payers = {
        name: payer_wire(document, name, payer, parameters)
        for name, payer in inventory.payers.items()
    }
    funds = [
        profile_wire(document, name, fund, parameters, payers)
        for name, fund in inventory.funds.items()
    ]
    return funds, list(payers.values())


def head_wire(document: BacktestDocument, history) -> dict:
    return {
        "market_model": document.inputs.mode,
        "schedule": document.inputs.schedule(),
        "history": [binding.model_dump(mode="json") for binding in history],
    }


def single_wire(document: BacktestDocument, history, funds, payers=None) -> dict:
    inputs = document.inputs
    wire = {
        **head_wire(document, history),
        "funds": funds,
        "costs": inputs.costs,
        **inputs.execution.models,
    }
    if payers is not None:
        wire["payers"] = payers
    data = duration_fields(inputs.data)
    if data:
        wire["data"] = data
    return wire


def composed_models(document: BacktestDocument) -> dict:
    inputs = document.inputs
    policies = composed_policies(document)
    costs, models = inputs.costs, inputs.execution.models
    require_connectors(document, policies, costs, models)
    return {
        name: connector_policy(models[name], policy, costs[name])
        for name, policy in policies.items()
    }


def connector_policy(model, data, costs) -> dict:
    return {**model, "data": duration_fields(data), "costs": costs}


def composed_policies(document: BacktestDocument) -> dict:
    policies = document.inputs.data.get("connectors")
    if set(document.inputs.data) != {"connectors"} or not isinstance(policies, dict):
        raise document.error(
            ConfigurationError(
                "composed backtests declare one data policy per connector", field="data"
            )
        )
    return policies


def require_connectors(document: BacktestDocument, policies, costs, models):
    expected = [set(policies)] * 3
    named = [set(value) for value in (policies, costs, models) if isinstance(value, dict)]
    if not policies or named != expected:
        raise document.error(
            ConfigurationError(
                "composed data, costs and models must name the same connectors",
                field="data.connectors",
            )
        )


def refuse_payers(document: BacktestDocument):
    """Aave V4 simulates one account identity, so its inputs carry no payer."""
    inventory = document.inputs.inventory
    located_fields = [f"inventory.payers.{name}.chain" for name in inventory.payers]
    located_fields += [
        f"inventory.funds.{name}.payer" for name, fund in inventory.funds.items() if fund.payer
    ]
    if located_fields:
        raise document.error(
            ConfigurationError(
                "this connector models no separate gas payer", field=located_fields[0]
            )
        )


def fund_wire(document, name, fund, parameters):
    field = f"inventory.funds.{name}"
    return {
        "name": name,
        "fund": fund_account(document, name, parameters),
        "balances": balances_wire(document, field, fund.balances, parameters),
    }


def profile_wire(document, name, fund, parameters, payers) -> dict:
    field = f"inventory.funds.{name}"
    profile = {"kind": "simulation", "fund": fund_account(document, name, parameters)}
    if fund.payer is not None:
        profile["payer"] = declared_payer(document, name, fund.payer, payers)
    return {
        "name": name,
        "profile": profile,
        "balances": balances_wire(document, field, fund.balances, parameters),
    }


def payer_wire(document, name, payer, parameters) -> dict:
    """A payer keeps the same deterministic identity as a hypothetical fund."""
    field = f"inventory.payers.{name}"
    return {
        "name": name,
        "fund": simulation_binding(name, payer.chain).profile.fund.model_dump(mode="json"),
        "balances": balances_wire(document, field, payer.balances, parameters),
    }


def fund_account(document, name, parameters) -> dict:
    try:
        return parameters.accounts.binding(name).profile.fund.model_dump(mode="json")
    except KeyError:
        raise document.error(
            ConfigurationError(
                "hypothetical fund has no bound account", field=f"inventory.funds.{name}"
            )
        ) from None


def declared_payer(document, name, payer, payers) -> dict:
    try:
        return payers[payer]["fund"]
    except KeyError:
        raise document.error(
            ConfigurationError(
                "fund payer must name a declared payer", field=f"inventory.funds.{name}.payer"
            )
        ) from None


def balances_wire(document, field, balances, parameters) -> list:
    return [
        balance_wire(document, f"{field}.balances.{index}", balance, parameters.asset_metadata)
        for index, balance in enumerate(balances)
    ]


def balance_wire(document, field, balance, metadata):
    try:
        return amount(balance.model_dump(mode="json"), metadata)
    except ConfigurationError as error:
        raise document.error(error, field) from error
