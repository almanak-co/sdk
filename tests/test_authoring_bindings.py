"""The public author aliases and the binding registry name the same connector types."""

import typing

import pytest

from almanak.configuration import authoring
from almanak.configuration.parameter_types import parameter_type
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.composition import CONNECTOR_IDS
from tests.test_parameter_types import declared_tag


def members(alias):
    origin = typing.get_origin(alias)
    if origin is typing.Annotated:
        alias = typing.get_args(alias)[0]
        origin = typing.get_origin(alias)
    return set(typing.get_args(alias)) if origin is not None else {alias}


def test_aliases_and_registry_stay_in_lockstep():
    bindings = authoring.BINDINGS
    assert members(authoring.MarketSelection) == {b.selection for b in bindings}
    assert members(authoring.ResolvedMarketFacts) == {b.resolved for b in bindings}
    assert members(authoring.AllocationInput) == {b.allocation for b in bindings}
    assert members(authoring.DebtInput) == {b.debt for b in bindings if b.debt is not None}
    assert len({b.connector for b in bindings}) == len(bindings)
    resolvers = set()
    for binding in bindings:
        assert binding.connector in CONNECTOR_IDS
        # A connector binds one position type or a union of its position kinds.
        positions = members(binding.allocation)
        for model in (binding.selection, *positions, *filter(None, (binding.debt,))):
            tag = model.model_fields["connector"]
            assert typing.get_args(tag.annotation) == (str(binding.connector),)
        # Shared verified facts are cached without assertions, then replayed with them.
        assert "assertions" in binding.selection.model_fields
        assert all(m.model_fields["connector"].exclude is True for m in positions)
        resolvers |= set(typing.get_args(binding.resolved.model_fields["resolver"].annotation))
        assert callable(binding.resolved.summary) and "assets" in dir(binding.resolved)
        assert authoring.binding_for(binding.connector) is binding
        assert declared_tag(parameter_type(binding.connector)) == str(binding.connector)
    assert len(resolvers) == len(bindings)


def test_every_public_connector_is_bound():
    assert {b.connector for b in authoring.BINDINGS} == set(CONNECTOR_IDS)


def test_unknown_connectors_are_refused_with_the_bound_list():
    assert str(authoring.binding_for("uniswap_v3").connector) == "uniswap_v3"
    with pytest.raises(ConfigurationError, match="no public author binding") as error:
        authoring.binding_for("aave_v5")
    assert error.value.code == "unconverted_connector"
    assert "aave_v4" in str(error.value) and "gmx_v2" in str(error.value)
