"""The reusable Comet and V3 trading reference uses the qualified shared path."""

from pathlib import Path
from uuid import uuid4

import pytest

from almanak.connectors.identifiers import ConnectorId
from tests.qa.catalog import Case
from tests.qa.worker import lending_eoa_case
from tests.strategy_fork.harness import discover, run_case

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize(
    "reference",
    (
        "ethereum_compound_curve_borrow_and_trade",
        "base_compound_borrow_and_trade",
        "arbitrum_compound_borrow_and_trade",
        "optimism_compound_curve_borrow_and_trade",
    ),
)
@pytest.mark.parametrize(
    "profile,operator",
    (("eoa", "configured"), ("safe_roles", "configured"), ("safe_roles", "distinct")),
)
def test_compound_borrow_funds_trading_and_recovers_through_the_shared_gateway(
    request, tmp_path, profile, operator, reference
):
    source = next(
        case
        for case in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if case.name == reference
    )
    env = request.config.getoption("--fork-env").resolve()
    temporary = tmp_path / "workspace"
    temporary.mkdir()
    artifact = (
        ROOT / "artifacts/compound-composition" / f"{reference}-{profile}-{operator}-{uuid4().hex}"
    )
    artifact.parent.mkdir(parents=True, exist_ok=True)
    if profile == "eoa":
        case = Case(
            id="composition/compound/eoa",
            lane="fork",
            connector=ConnectorId("compound_v3"),
            reference=source.name,
            profile=profile,
        )
        lending_eoa_case(case, source, source.config, env, temporary, artifact)
    else:
        run_case(source, env, {}, temporary, artifact, operator)
