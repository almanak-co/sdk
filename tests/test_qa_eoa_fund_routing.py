"""The selected EOA QA harness uses the exact fund indices qualified at fork origins.

A converted reference takes its fixture accounts from the case operator file, so the
QA route no longer applies the transform. The transform still qualifies the harness
configuration the dedicated EOA fork lane builds, from the frozen pre-conversion copy.
"""

import pytest

from almanak.gateway.fork_config import ForkEoaFund
from scripts.legacy_configuration.author import read_author
from tests.legacy_references import harness_directory
from tests.qa.catalog import catalog, converted
from tests.qa.operators import example_operator
from tests.qa.worker import uses_curve_harness, uses_lending_eoa_harness
from tests.strategy_fork.supervised import eoa_inputs


def qualified(reference, source):
    from tests.fork.test_aave_v3_base_eoa import eoa_configuration

    if reference.startswith("aerodrome_slipstream_"):
        from tests.fork.test_slipstream_eoa import qualified_fund_indices
    else:
        from tests.fork.test_aerodrome_eoa import qualified_fund_indices

    return qualified_fund_indices(eoa_configuration(source), lp=reference.endswith("_lp"))


@pytest.mark.parametrize(
    "reference,start",
    (
        ("aerodrome_slipstream_base_trading", 845309200),
        ("aerodrome_slipstream_base_lp", 845309201),
        ("aerodrome_base_stable_trading", 845309300),
        ("aerodrome_base_stable_lp", 845309301),
        ("aerodrome_base_volatile_trading", 845309302),
        ("aerodrome_base_volatile_lp", 845309303),
    ),
)
def test_selected_eoa_route_preserves_origin_qualified_funds(reference, start):
    case = next(c for c in catalog() if c.id == f"fork/pinned/{reference}/eoa/configured")
    assert uses_lending_eoa_harness(case) and not uses_curve_harness(case)
    source = read_author(harness_directory(reference) / "config.yaml").fork_config()
    author = qualified(reference, source)
    assert tuple(f.foundry_account_index for f in author.funds) == tuple(
        range(start, start + len(source.funds))
    )
    assert all(isinstance(f, ForkEoaFund) for f in author.funds)
    assert tuple((f.account, f.assets) for f in author.funds) == tuple(
        (f.account, f.assets) for f in source.funds
    )


@pytest.mark.parametrize(
    "reference", ("aerodrome_slipstream_base_trading", "aerodrome_base_stable_lp")
)
def test_the_converted_qa_route_takes_its_eoa_fixture_from_the_operator_file(reference):
    """The operator fixture keeps the frozen fund topology and pins no Foundry index."""
    case = next(c for c in catalog() if c.id == f"fork/pinned/{reference}/eoa/configured")
    assert converted(case)
    source = read_author(harness_directory(reference) / "config.yaml").fork_config()
    fixture = eoa_inputs(example_operator(case, "fork").fork)
    assert [f.account for f in fixture.funds] == [f.account for f in source.funds]
    assert all(not hasattr(f, "foundry_account_index") for f in fixture.funds)
