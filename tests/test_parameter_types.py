"""Retained parameter sets decode to their connector type; gas and preparation are shared."""

import json

import pytest
from pydantic import TypeAdapter, ValidationError

from almanak.configuration import parameter_types
from almanak.configuration.gas_parameters import AuthorGas, PreparedConfig
from almanak.configuration.parameter_types import PARAMETERS, parameter_tag, parameter_type
from almanak.connectors.aave_v4.models import LendingParameters
from almanak.connectors.composition import CONNECTOR_IDS
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.uniswap_v3.models import Parameters as UniswapV3Parameters
from almanak.gateway.production_config import ForkGasExecution, ProductionExecution
from almanak.models import StrategyAccounts
from tests.test_author_resolution import FROZEN, expected_parameters, frozen_production_gas
from tests.test_author_resolution import archive as archive
from tests.test_author_resolution import rpc as rpc
from tests.test_author_resolution import setup as setup
from tests.test_resolution_context import revalidate


def declared_tag(cls):
    """The tag a parameter class writes on the wire; Aave V4 parameters carry none."""
    field = cls.model_fields.get("connector")
    return parameter_tag({} if field is None else {"connector": field.default})


def test_tags_follow_the_wire_convention():
    assert parameter_tag({"connector": "uniswap_v3"}) == "uniswap_v3"
    assert parameter_tag({}) == "aave_v4"
    assert parameter_tag({"connector": None}) == "composed"
    assert declared_tag(UniswapV3Parameters) == "uniswap_v3"
    assert declared_tag(LendingParameters) == "aave_v4"
    assert declared_tag(parameter_types.ComposedParameters) == "composed"


def test_every_connector_and_the_composed_root_have_one_parameter_type():
    assert set(parameter_types.PARAMETER_TYPES) == {*map(str, CONNECTOR_IDS), "composed"}
    for identity in CONNECTOR_IDS:
        cls = parameter_type(identity)
        assert declared_tag(cls) == str(identity)
        assert isinstance(cls, type) and issubclass(cls, AuthorGas)
    assert parameter_type(None) is parameter_types.ComposedParameters
    with pytest.raises(ValueError, match="no reviewed parameter binding"):
        parameter_type(ConnectorId("unbound_protocol"))


def test_retained_parameters_decode_to_their_connector_type(setup):
    parameters = expected_parameters(setup)
    adapter = TypeAdapter(PARAMETERS)
    wire = parameters.model_dump(mode="json")
    assert "connector" not in wire and "gas" not in wire
    decoded = adapter.validate_json(json.dumps(wire))
    assert type(decoded) is LendingParameters and decoded == parameters
    assert adapter.validate_python(parameters) == parameters
    with pytest.raises(ValidationError):
        adapter.validate_json(json.dumps({**wire, "connector": "uniswap_v3"}))
    with pytest.raises(ValidationError):
        adapter.validate_json(json.dumps({**wire, "connector": "unknown"}))


class Owner(AuthorGas):
    accounts: StrategyAccounts
    positions: tuple[()] = ()


class OwnerConfig(Owner, PreparedConfig):
    production: ProductionExecution | None = None
    fork_bindings: tuple[str, ...] = ()


def test_author_gas_is_excluded_when_empty_and_checked_against_admitted_policy(setup):
    example = json.loads((FROZEN / "production.example.json").read_text())["production"]
    execution = ProductionExecution.model_validate_json(
        json.dumps({**example, "gas": list(frozen_production_gas())})
    )
    policy = execution.gas[0]
    treasury = revalidate(setup.operator.accounts.bindings[1], name="treasury")
    accounts = StrategyAccounts(bindings=(treasury,))
    assert Owner(accounts=accounts).model_dump() == {
        "accounts": accounts.model_dump(),
        "positions": (),
    }
    assert Owner(accounts=accounts, gas=(policy,)).gas == (policy,)
    with pytest.raises(ValidationError, match="exactly the bound accounts"):
        Owner(accounts=setup.operator.accounts, gas=(policy,))
    unprepared = OwnerConfig(accounts=accounts)
    assert "preparation_digest" not in unprepared.model_dump()
    prepared = OwnerConfig(
        accounts=accounts, gas=(policy,), production=execution, preparation_digest="0" * 64
    )
    assert prepared.model_dump()["preparation_digest"] == "0" * 64
    with pytest.raises(ValidationError, match="matching admitted execution policy"):
        OwnerConfig(accounts=accounts, gas=(policy,))
    with pytest.raises(ValidationError, match="matching admitted execution policy"):
        OwnerConfig(
            accounts=accounts,
            gas=(policy,),
            production=execution.model_copy(
                update={"gas": (policy.model_copy(update={"max_gas_limit": 999999}),)}
            ),
        )
    # Retained production artifacts may predate top-level author gas retention.
    assert OwnerConfig(accounts=accounts, production=execution).production == execution
    with pytest.raises(ValidationError, match="must equal the author gas policy"):
        OwnerConfig(
            accounts=accounts,
            fork_gas=ForkGasExecution(gas=(policy,)),
            preparation_digest="0" * 64,
            fork_bindings=("identified",),
        )
