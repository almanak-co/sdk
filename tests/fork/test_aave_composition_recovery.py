"""The real composed reference survives three privileged borrow boundaries."""

from pathlib import Path
from uuid import uuid4

import pytest

from tests.fork.test_aave_v3_base_eoa import eoa_configuration
from tests.fork.test_aave_v3_recovery import exercise
from tests.strategy_fork.harness import case_fork_config, discover, resolve_config, retain

ROOT = Path.cwd()
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]
RECOVERY_REFERENCES = (
    "arbitrum_aave_borrow_and_trade",
    "optimism_aave_borrow_and_trade",
    "bnb_aave_borrow_and_trade",
    "polygon_aave_borrow_and_trade",
    "avalanche_aave_borrow_and_trade",
    "base_borrow_and_trade",
    "ethereum_aave_v4_borrow_and_trade",
    "avalanche_aave_v4_borrow_and_trade",
    "bnb_aave_borrow_and_trade_v3",
    "bnb_aave_borrow_and_trade_v4",
    "bnb_aave_pancake_borrow_and_trade",
    "robinhood_morpho_borrow_and_trade_v4",
    "robinhood_morpho_borrow_and_trade_v3",
    "robinhood_morpho_curve_borrow_and_trade",
    "base_aave_curve_borrow_and_trade",
    "arbitrum_aave_curve_borrow_and_trade",
    "arbitrum_aave_borrow_and_trade_gmx",
    "avalanche_aave_borrow_and_trade_gmx",
)


@pytest.mark.parametrize(
    "reference",
    RECOVERY_REFERENCES,
)
@pytest.mark.parametrize("profile", ("eoa", "safe_roles"))
def test_composed_borrow_recovers_all_privileged_boundaries(request, tmp_path, profile, reference):
    case = next(
        c
        for c in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if c.name == reference
    )
    env = request.config.getoption("--fork-env").resolve()
    config = case_fork_config(case, env, tmp_path / "resolution")
    if profile == "eoa":
        config = eoa_configuration(config)
    config = resolve_config(config, env, {})
    artifact = (
        ROOT
        / "artifacts/aave-composition-recovery"
        / (reference + "-" + profile + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {"strategy": case.name, "account_profile": profile, "status": "failed", "phases": []}
    try:
        exercise(case, config, env, temporary, record)
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("Composed borrow process evidence:", artifact)
