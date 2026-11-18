"""Candidate declarations stay test-local and route each family to its own model id."""

from types import SimpleNamespace

import pytest

from almanak.backtest.types import AvailabilityError, UnixTime
from almanak.caip import ChainId
from almanak.connectors import composition
from almanak.connectors.gmx_v2.authority import declared
from almanak.connectors.gmx_v2.connector import connector
from almanak.connectors.gmx_v2.economic_data import CONNECTOR, revisions
from almanak.connectors.support import RunMode, historical_capability
from tests import gmx_candidates as candidates
from tests.gmx_candidates import (
    candidate_liquidity_model as candidate_liquidity_model,
)
from tests.gmx_candidates import (
    candidate_model as candidate_model,
)
from tests.gmx_candidates import (
    candidate_perpetual_model as candidate_perpetual_model,
)
from tests.test_gmx_economic_perpetual_data import parameters

CHAINS = (ChainId("eip155:42161"), ChainId("eip155:43114"))


@pytest.mark.parametrize(
    "family,kind", [("swap", "swap"), ("liquidity", "liquidity"), ("perpetual", "position")]
)
def test_each_family_declares_only_its_own_model_and_actions(family, kind):
    model, actions = candidates.CANDIDATES[family]
    candidate = candidates.candidate_connector(family)
    config = parameters("arbitrum", kind)
    assert revisions(config)[0] == model
    for chain in CHAINS:
        support = candidate.support_for(chain)
        assert RunMode.BACKTEST in support.modes
        for action in actions:
            support.require(RunMode.BACKTEST, action, model=model)
        foreign = next(
            other
            for other_family, (_, other_actions) in candidates.CANDIDATES.items()
            if other_family != family
            for other in other_actions
            if other not in actions
        )
        with pytest.raises(ValueError, match="unsupported"):
            support.require(RunMode.BACKTEST, foreign, model=model)
        for other_family, (other_model, _) in candidates.CANDIDATES.items():
            if other_family != family:
                with pytest.raises(ValueError):
                    support.require(RunMode.BACKTEST, model=other_model)
        capability = historical_capability(
            support, config.allocations[0].market.chain, CONNECTOR, model
        )
        capability.require(UnixTime(1))
        with pytest.raises(AvailabilityError, match="unsupported backtest action"):
            capability.require(UnixTime(1), foreign)


def test_private_declaration_is_limited_to_arbitrum_gm(candidate_perpetual_model):
    private = connector()
    arbitrum = private.support_for(CHAINS[0])
    avalanche = private.support_for(CHAINS[1])
    assert arbitrum.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
    assert tuple(model.model for model in arbitrum.historical) == (candidates.LIQUIDITY_MODEL,)
    assert avalanche.modes == (RunMode.FORK,)
    assert avalanche.historical == ()
    for model in (candidates.MODEL, candidates.PERPETUAL_MODEL):
        with pytest.raises(ValueError, match="unsupported"):
            arbitrum.require(RunMode.BACKTEST, model=model)


@pytest.mark.parametrize(
    "fixture,kind",
    [
        ("candidate_model", "swap"),
        ("candidate_liquidity_model", "liquidity"),
        ("candidate_perpetual_model", "position"),
    ],
)
def test_candidate_fixtures_construct_the_driver_for_their_family(request, fixture, kind):
    driver = request.getfixturevalue(fixture)
    config = parameters("arbitrum", kind)
    settings = SimpleNamespace(schedule=SimpleNamespace(start=UnixTime(1)))
    model = driver(config, settings, None, None, authority=declared(config))
    assert model.model_name == revisions(config)[0]
    assert composition.connector_for(config).support_for(CHAINS[0]).historical[0].model == (
        model.model_name
    )
    assert tuple(capability.model for capability in model.capabilities) == (model.model_name,)


def test_candidate_fixtures_refuse_another_family(candidate_perpetual_model):
    config = parameters("arbitrum", "swap")
    settings = SimpleNamespace(schedule=SimpleNamespace(start=UnixTime(1)))
    with pytest.raises(ValueError):
        candidate_perpetual_model(config, settings, None, None, authority=declared(config))
