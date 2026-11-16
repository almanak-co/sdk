"""Recorded Compound V3 preflight rows equal the pure funding requirements."""

import pytest

from almanak.connectors.compound_v3.execution import CompoundExecution
from tests.compound_v3_helpers import candidate_compound_support as candidate_compound_support
from tests.compound_v3_helpers import recorded_compound as recorded_compound


@pytest.mark.usefixtures("candidate_compound_support")
def test_compound_requirements_match_preflight_and_aggregate_a_shared_asset(recorded_compound):
    config, baseline = recorded_compound.config, recorded_compound.baseline
    for binding in config.accounts.bindings:
        rows, _ = CompoundExecution.preflight(config, binding.name, baseline)
        required = CompoundExecution.requirements(config, binding.name)
        assert [(r["asset"], r["required_token_base_units"]) for r in rows] == [
            (str(r.asset), str(r.amount)) for r in required
        ]
    first = config.allocations[0]
    twin = first.model_copy(update={"name": first.name + "_twin"})
    doubled = config.model_copy(update={"allocations": (*config.allocations, twin)})
    (row,) = (
        r
        for r in CompoundExecution.requirements(doubled, first.account)
        if r.asset == first.reserve.asset
    )
    single = next(
        r
        for r in CompoundExecution.requirements(config, first.account)
        if r.asset == first.reserve.asset
    )
    own = first.capital
    assert own > 0 and row.amount == single.amount + own
