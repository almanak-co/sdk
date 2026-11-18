"""The reusable Spark and Curve trading reference uses the qualified shared path."""

from pathlib import Path
from uuid import uuid4

import pytest

from almanak.connectors.identifiers import ConnectorId
from tests.qa.catalog import Case
from tests.qa.worker import lending_eoa_case
from tests.strategy_fork.harness import discover, run_case

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize("reference", ("ethereum_spark_curve_borrow_and_trade",))
@pytest.mark.parametrize(
    "profile,operator",
    (("eoa", "configured"), ("safe_roles", "configured"), ("safe_roles", "distinct")),
)
def test_spark_borrow_funds_trading_and_recovers_through_the_shared_gateway(
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
        ROOT / "artifacts/spark-composition" / f"{reference}-{profile}-{operator}-{uuid4().hex}"
    )
    artifact.parent.mkdir(parents=True, exist_ok=True)
    if profile == "eoa":
        case = Case(
            id="composition/spark/eoa",
            lane="fork",
            connector=ConnectorId("sparklend"),
            reference=source.name,
            profile=profile,
        )
        lending_eoa_case(case, source, source.config, env, temporary, artifact)
    else:
        run_case(source, env, {}, temporary, artifact, operator)
