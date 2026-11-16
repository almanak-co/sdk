from pathlib import Path

import pytest

from tests.strategy_fork.harness import discover, run_case

ROOT = Path(__file__).resolve().parents[2]
CASES = discover(ROOT / "strategies", Path(__file__).parent / "scenarios")
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.fixture(scope="session")
def resolved_targets():
    return {}


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
@pytest.mark.parametrize("operator_mode", ["configured", "distinct"])
def test_strategy_lifecycle(case, request, tmp_path, resolved_targets, operator_mode):
    run_case(
        case,
        request.config.getoption("--fork-env").resolve(),
        resolved_targets,
        tmp_path,
        ROOT / "artifacts/strategy-fork",
        operator_mode,
    )
