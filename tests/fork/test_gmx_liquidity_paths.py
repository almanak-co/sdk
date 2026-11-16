"""GM liquidity references execute with generated Safe authority and explicit EOA funds."""

from pathlib import Path
from uuid import uuid4

import pytest

from tests.fork.test_gmx_collateral_paths import run_path
from tests.fork.test_gmx_requests import case, configuration
from tests.strategy_fork.harness import resolve_config, retain

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("profile", ["safe_roles", "eoa"])
def test_gmx_liquidity_path(request, tmp_path, chain, profile):
    selected = case(f"gmx_v2_{chain}_liquidity")
    env = request.config.getoption("--fork-env").resolve()
    config = resolve_config(configuration(selected, profile, env, tmp_path / "resolution"), env, {})
    artifact = (
        ROOT / "artifacts/strategy-fork" / (selected.name + "-" + profile + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {"strategy": selected.name, "profile": profile, "status": "failed", "phases": []}
    try:
        run_path(selected, config, env, temporary, record)
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("GMX liquidity path evidence:", artifact)
