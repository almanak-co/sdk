"""Fresh composed GMX runs must select post-origin publications instead of pinned fixtures."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from almanak.connectors.identifiers import ConnectorId
from scripts.legacy_configuration.author import read_author
from tests.composed_public import resolved_parameters
from tests.legacy_references import harness_directory
from tests.qa.catalog import Case, ProofScope
from tests.qa.gmx_fresh import fresh_market
from tests.qa.worker import uses_fresh_gmx_harness


@pytest.mark.parametrize("chain", ("arbitrum", "avalanche"))
def test_fresh_composition_selects_its_gmx_market_among_lending_allocations(chain):
    _, author = resolved_parameters(f"{chain}_aave_borrow_and_trade_gmx")
    assert len(author.allocations) == 3
    routed = SimpleNamespace(forks=(object(),), allocations=author.allocations)
    assert fresh_market(routed) == author.allocations[-1].market
    harness = harness_directory(f"gmx_v2_{chain}_swap") / "config.yaml"
    standalone = read_author(harness).fork_config()
    assert fresh_market(standalone) == standalone.allocations[0].market
    with pytest.raises(ValueError, match="one explicitly routed"):
        fresh_market(SimpleNamespace(forks=(object(),), allocations=author.allocations[:2]))


def test_fresh_gmx_scope_uses_authentic_publication_harness():
    case = Case(
        id="composed-fresh",
        lane="fork",
        input_policy="fresh",
        scopes=(
            ProofScope(connector=ConnectorId("aave_v3"), chains=(), actions=()),
            ProofScope(connector=ConnectorId("gmx_v2"), chains=(), actions=()),
        ),
    )
    assert uses_fresh_gmx_harness(case)
    assert not uses_fresh_gmx_harness(replace(case, input_policy="pinned"))
    assert not uses_fresh_gmx_harness(replace(case, scopes=case.scopes[:1]))
