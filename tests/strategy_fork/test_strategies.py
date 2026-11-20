from pathlib import Path

import pytest

from tests.fork.aave_v3_revision12 import (
    ANCHORS,
    COLLATERAL,
    anchored_revision,
    operator_at_revision_12,
)
from tests.strategy_fork.harness import discover, run_case
from tests.strategy_fork.supervised import run_supervised_case

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


# The Avalanche reference supplies WAVAX, which the Pool has held above its supply cap
# since the upgrade, so its entry is refused with `supply_cap` and it has no run here.
@pytest.mark.parametrize("reference", sorted(set(ANCHORS) - set(COLLATERAL)))
def test_aave_v3_lifecycle_at_pool_revision_12(reference, request, tmp_path):
    """The Safe/Roles reference lifecycle on a fork where its Pool runs revision 12.

    The reference's own fork fixture pins a block before the upgrade, so this run uses a
    copy of that operator file with only the anchor moved.
    """
    env = request.config.getoption("--fork-env").resolve()
    assert anchored_revision(env, reference) == 12
    run_supervised_case(
        next(case for case in CASES if case.name == reference),
        operator_at_revision_12(reference, tmp_path / "operator"),
        env,
        tmp_path,
        ROOT / "artifacts/strategy-fork",
    )
