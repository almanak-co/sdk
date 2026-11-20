"""The public author root, with connector-owned inputs and gateway-resolved facts.

These inputs are never strategy parameters. Supported connector bindings share
this loader, which has no legacy-format fallback.
"""

import json
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from difflib import get_close_matches
from pathlib import Path
from typing import Annotated
from uuid import uuid4

from pydantic import Field, ValidationError

from almanak.caip import AssetType, ChainId
from almanak.configuration.authoring import (
    BINDINGS,
    AllocationInput,
    DebtInput,
    MarketSelection,
    binding_for,
)
from almanak.configuration.gas import GasInput
from almanak.configuration.input_values import AmountInput, Duration
from almanak.configuration.resolution import (
    AccountRequirement,
    AnchorSelection,
    NamedMarket,
    ReadContext,
)
from almanak.configuration.selection import require_connector
from almanak.configuration.strategy_parameters import (
    StrategyParameters,
    apply_overrides,
    test_values,
)
from almanak.configuration.yaml import ConfigurationError, Document, load
from almanak.connectors.support import RunMode
from almanak.gateway.operator_config import Name, OperatorConfig, content_digest
from almanak.models import Model, refusals

TAGS = frozenset(str(binding.connector) for binding in BINDINGS)


class AccountInput(Model):
    chain: ChainId


class AssetExpectation(Model):
    decimals: Annotated[int, Field(ge=0, le=255)] | None = None
    symbol: str | None = None


class InventoryInput(Model):
    """Owned cash a composed root declares; never spending authority."""

    account: Name
    balance: AmountInput


class StrategyInput(Model):
    decision_interval: Annotated[Duration, Field(gt=0)]
    allocations: Annotated[dict[Name, AllocationInput], Field(min_length=1)]
    debts: dict[Name, DebtInput] = Field(default_factory=dict)
    gas: dict[Name, GasInput] = Field(default_factory=dict)
    parameters: StrategyParameters = Field(
        default_factory=StrategyParameters, exclude_if=lambda value: not value
    )
    # Fork-test values for declared parameters; parse_inputs moves them off the inputs.
    test_parameters: StrategyParameters = Field(
        default_factory=StrategyParameters, exclude_if=lambda value: not value
    )


class AuthorInputs(Model):
    strategy: StrategyInput
    markets: Annotated[dict[Name, MarketSelection], Field(min_length=1)]
    accounts: Annotated[dict[Name, AccountInput], Field(min_length=1)]
    assets: dict[AssetType, AssetExpectation] = Field(default_factory=dict)
    inventory: tuple[InventoryInput, ...] = Field(default=(), exclude_if=lambda value: not value)

    @property
    def digest(self):
        return content_digest(self)

    @property
    def requirements(self):
        return tuple(
            AccountRequirement(name=name, chain=value.chain)
            for name, value in self.accounts.items()
        )

    @property
    def selections(self):
        return tuple(
            NamedMarket(name=name, selection=value) for name, value in self.markets.items()
        )

    def context(
        self, operator: OperatorConfig, mode: RunMode, anchors: tuple[AnchorSelection, ...]
    ) -> ReadContext:
        result = ReadContext(
            identity=uuid4().hex,
            authored_digest=self.digest,
            operator_digest=operator.digest,
            mode=mode,
            accounts=self.requirements,
            markets=self.selections,
            anchors=anchors,
        )
        result.match_operator(operator, mode)
        return result

    def require_context(self, context: ReadContext):
        if (context.authored_digest, context.accounts, context.markets) != (
            self.digest,
            self.requirements,
            self.selections,
        ):
            raise ConfigurationError("retained resolution differs from the authored choices")


@dataclass(frozen=True)
class InputDocument:
    inputs: AuthorInputs
    source: Document
    # `strategy.test_parameters` in the declared types; only `almanak test run` reads them.
    test_parameters: dict = dataclass_field(default_factory=dict)

    def error(self, error, prefix="") -> ConfigurationError:
        return diagnostic(self.source, error, prefix)


def diagnostic(source: Document, error, prefix="") -> ConfigurationError:
    """Locate a validation or configuration failure in the author's own file."""
    field = error.field if isinstance(error, ConfigurationError) else ""
    message = str(error)
    if isinstance(error, ValidationError):
        detail = refusals(error)[0]
        field = author_field(detail["loc"])
        message = detail["msg"]
    field = ".".join(part for part in (prefix, field) if part)
    return located(source, ConfigurationError(message, field=field))


def author_field(location) -> str:
    """A discriminated union inserts its connector tag, which the author never wrote."""
    return ".".join(str(part) for part in location if str(part) not in TAGS)


def located(document, error):
    location = error.field
    while location not in document.locations and "." in location:
        location = location.rsplit(".", 1)[0]
    return error.at(file=document.file, field=error.field, line=document.locations.get(location))


def public_root(value) -> bool:
    """Public roots carry no execution block and identify every market chain by CAIP-2 text."""
    if not isinstance(value, dict) or "execution" in value:
        return False
    strategy = value.get("strategy")
    if isinstance(strategy, dict) and "connector" in strategy:
        return False
    markets = value.get("markets")
    return (
        isinstance(markets, dict)
        and bool(markets)
        and all(isinstance(m, dict) and isinstance(m.get("chain"), str) for m in markets.values())
    )


def parse_inputs(document: Document, fields: frozenset[str] | None = None) -> InputDocument:
    try:
        require_bindings(document)
        inputs = AuthorInputs.model_validate_json(json.dumps(inject_connectors(document)))
        validate_references(inputs)
        tests = test_values(inputs.strategy, fields)
        strategy = inputs.strategy.model_copy(update={"test_parameters": StrategyParameters()})
        return InputDocument(inputs.model_copy(update={"strategy": strategy}), document, tests)
    except ConfigurationError as error:
        raise located(document, error) from error
    except ValidationError as error:
        raise diagnostic(document, error) from error


def require_bindings(document: Document):
    """Every market names a canonical connector that has a public author binding."""
    markets = document.value.get("markets", {})
    if not isinstance(markets, dict):
        raise ConfigurationError("named markets must be a mapping", field="markets")
    for name, market in markets.items():
        if not isinstance(market, dict) or "connector" not in market:
            continue
        try:
            binding_for(require_connector(market["connector"]))
        except ConfigurationError as error:
            raise error.at(
                file=document.file,
                field=f"markets.{name}.connector",
                line=document.locations.get(f"markets.{name}.connector"),
            ) from error


def inject_connectors(document: Document) -> dict:
    """Positions take the connector tag from their named market; the author never writes it."""
    value = json.loads(json.dumps(document.value))
    markets = value.get("markets")
    strategy = value.get("strategy")
    if not isinstance(markets, dict) or not isinstance(strategy, dict):
        return value
    for group in ("allocations", "debts"):
        positions = strategy.get(group)
        for name, position in positions.items() if isinstance(positions, dict) else ():
            if isinstance(position, dict):
                tag = position_connector(position, markets, f"strategy.{group}.{name}")
                if tag is not None:
                    position["connector"] = tag
    return value


def position_connector(position: dict, markets: dict, field: str) -> str | None:
    if "connector" in position:
        raise ConfigurationError(
            "position connector comes from its named market", field=f"{field}.connector"
        )
    if "market" not in position:
        return None
    reference = position["market"]
    if not isinstance(reference, str):
        raise ConfigurationError(
            "named market reference must be a string",
            field=f"{field}.market",
            code="invalid_named_reference",
            remedy="Choose a configured market name.",
        )
    market = markets.get(reference)
    if not isinstance(market, dict):
        raise unknown_reference("market", reference, markets, f"{field}.market")
    tag = market.get("connector") if isinstance(market, dict) else None
    return tag if isinstance(tag, str) else None


def read_inputs(path: Path, overrides=None, fields: frozenset[str] | None = None) -> InputDocument:
    """The author inputs; `fields` names the strategy's typed parameter model's fields."""
    document = load(path)
    if overrides:
        try:
            value = apply_overrides(document.value, overrides, fields)
        except ConfigurationError as error:
            raise located(document, error) from error
        document = Document(value, document.locations, document.file)
    return parse_inputs(document, fields)


def read_frozen_inputs(path: Path) -> InputDocument:
    """A workspace's inputs as frozen in JSON; YAML would read `1e-05` back as a string."""
    return parse_inputs(Document(json.loads(path.read_text()), {}, str(path)))


def validate_references(inputs: AuthorInputs):
    positions = inputs.strategy.allocations
    if positions.keys() & inputs.strategy.debts.keys():
        raise ConfigurationError(
            "allocation and debt names must be distinct", field="strategy.debts"
        )
    for group, values in (("allocations", positions), ("debts", inputs.strategy.debts)):
        for name, value in values.items():
            field = f"strategy.{group}.{name}"
            if value.market not in inputs.markets:
                raise unknown_reference("market", value.market, inputs.markets, f"{field}.market")
            if value.account not in inputs.accounts:
                raise unknown_reference(
                    "account", value.account, inputs.accounts, f"{field}.account"
                )
            if inputs.markets[value.market].chain != inputs.accounts[value.account].chain:
                raise ConfigurationError(
                    "account and market chains differ", field=f"{field}.account"
                )
    for name, debt in inputs.strategy.debts.items():
        unknown = next((item for item in debt.collateral if item not in positions), None)
        if unknown is not None:
            raise unknown_reference(
                "supply allocation",
                unknown,
                positions,
                f"strategy.debts.{name}.collateral",
            )
    for name, policy in inputs.strategy.gas.items():
        if any(
            a not in inputs.accounts or inputs.accounts[a].chain != policy.chain
            for a in policy.accounts
        ):
            raise ConfigurationError(
                "gas accounts must exist on the policy chain", field=f"strategy.gas.{name}.accounts"
            )
    validate_inventory(inputs)
    require_one_allocation_per_fund(inputs)
    require_composed_inventory(inputs)
    require_chain_support(inputs)


def require_one_allocation_per_fund(inputs: AuthorInputs):
    """The DEX connectors' own per-fund rule, refused here before any resolution."""
    from almanak.connectors.support import ONE_ALLOCATION_PER_FUND_CONNECTORS

    seen: dict[tuple[str, str], str] = {}
    for name, value in inputs.strategy.allocations.items():
        connector = str(inputs.markets[value.market].connector)
        if connector not in ONE_ALLOCATION_PER_FUND_CONNECTORS:
            continue
        first = seen.setdefault((connector, value.account), name)
        if first != name:
            raise ConfigurationError(
                f"{connector} takes one allocation per fund; {first!r} and {name!r} "
                f"both use fund {value.account!r}",
                field=f"strategy.allocations.{name}.account",
                code="one_allocation_per_fund",
                remedy="Use another DEX connector or another fund for the second allocation.",
            )


def known_assets(inputs: AuthorInputs, value) -> tuple[AssetType, ...]:
    """The position assets the author file names; pool coins need resolution."""
    market = inputs.markets[value.market]
    named = (
        getattr(market, "asset", None),
        getattr(getattr(value, "capital", None), "asset", None),
    )
    return tuple(asset for asset in named if isinstance(asset, AssetType))


def require_composed_inventory(inputs: AuthorInputs):
    """Composed inventory states every fund asset; check the ones known before resolution."""
    positions = {**inputs.strategy.allocations, **inputs.strategy.debts}
    if len({str(inputs.markets[p.market].connector) for p in positions.values()}) < 2:
        return
    declared = {(item.account, item.balance.asset) for item in inputs.inventory}
    for name, value in positions.items():
        for asset in known_assets(inputs, value):
            if (value.account, asset) not in declared:
                raise ConfigurationError(
                    "composed inventory must explicitly cover every configured fund asset; "
                    f"{name!r} uses {asset} in fund {value.account!r}",
                    field="inventory",
                    code="composed_inventory_incomplete",
                    remedy=(
                        f"Add an inventory entry for {asset} on account {value.account!r}, "
                        "with quantity '0' when the strategy starts without it."
                    ),
                )


def require_chain_support(inputs: AuthorInputs):
    """Offline checks refuse a market chain its connector never declared, in any mode."""
    from almanak.connectors.composition import connector_from_wire

    for name, market in inputs.markets.items():
        connector = connector_from_wire({"connector": str(market.connector)})
        try:
            connector.support_for(market.chain)
        except ValueError as error:
            raise ConfigurationError(str(error), field=f"markets.{name}.chain") from error


def validate_inventory(inputs: AuthorInputs):
    seen = set()
    for index, item in enumerate(inputs.inventory):
        field = f"inventory.{index}"
        account = inputs.accounts.get(item.account)
        if account is None:
            raise unknown_reference("account", item.account, inputs.accounts, f"{field}.account")
        if item.balance.asset.chain != account.chain:
            raise ConfigurationError(
                "inventory asset belongs to another chain than its account",
                field=f"{field}.balance.asset",
            )
        if (item.account, item.balance.asset) in seen:
            raise ConfigurationError("inventory repeats an account asset", field=field)
        seen.add((item.account, item.balance.asset))


def unknown_reference(kind, value, choices, field):
    """Report local-name typos without selecting a replacement automatically."""
    names = tuple(name for name in choices if isinstance(name, str))
    matches = get_close_matches(value, names, n=1) if isinstance(value, str) else []
    hint = f" Did you mean {matches[0]!r}?" if matches else ""
    available = ", ".join(names)
    return ConfigurationError(
        f"Unknown named {kind} {value!r}.{hint} Configured {kind}s: {available}.",
        field=field,
        code="unknown_named_reference",
        remedy=(
            f"Choose one of the configured {kind} names; suggestions never change the selection."
        ),
    )
