"""Pre-entry native checks against installed Robinhood Safe/Roles grants."""

import hashlib
import subprocess
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import pytest

from almanak.connectors.composition import connector_for
from almanak.gateway.client import GatewayClient
from tests.fork.support import processes
from tests.strategy_fork.harness import (
    bind_workspace,
    case_fork_config,
    discover,
    install_permissions,
    require_safe_accounts,
    resolve_config,
    retain,
    start_gateway,
)
from tests.strategy_fork.robinhood_joint_authority import run

ROOT = Path(__file__).parents[2]
REFERENCES = (
    "morpho_blue_robinhood_lending",
    "uniswap_v4_robinhood_lp",
    "uniswap_v4_robinhood_trading",
    "robinhood_morpho_borrow_and_trade_v4",
    "robinhood_morpho_curve_borrow_and_trade",
    "uniswap_v3_robinhood_trading",
    "uniswap_v3_robinhood_lp",
    "robinhood_morpho_borrow_and_trade_v3",
)
CASES = {
    case.name: case
    for case in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
    if case.name in REFERENCES
}
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def runner_hashes():
    paths = (
        Path(__file__),
        ROOT / "tests/strategy_fork/robinhood_joint_authority.py",
    )
    return {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths
    }


def execution_source():
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert not status.stdout, "authority evidence requires a clean tracked execution source"
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def exact_authority_state(manifest, verification, accounts):
    """Installed scope for every partition; scope-only grants carry no quota (blueprint 27)."""
    assert verification["manifest"] == manifest.digest
    assert tuple(partition.account for partition in manifest.partitions) == accounts
    observed = verification["partitions"]
    assert tuple(partition["account"] for partition in observed) == accounts
    assert all("quotas" not in partition for partition in observed)
    return observed


@pytest.mark.parametrize("reference", REFERENCES)
def test_robinhood_generated_grants_reject_pre_entry_parameter_attacks(
    request, tmp_path, reference
):
    case = CASES[reference]
    env = request.config.getoption("--fork-env").resolve()
    config = resolve_config(case_fork_config(case, env, tmp_path / "resolution"), env, {})
    artifact = ROOT / "artifacts/robinhood-joint-authority" / f"{reference}-{uuid4().hex}"
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": reference,
        "source_revision": execution_source(),
        "status": "failed",
        "scope": "private_unqualified_candidate",
        "foreign_nft_native_proof": "not_claimed; retain current-source generic fixture",
        "runner_sha256": runner_hashes(),
    }
    try:
        with ExitStack() as stack:
            nodes, node = bind_workspace(config, env, stack, record)
            launch = stack.enter_context(processes(temporary))
            directory = temporary / "deployment"
            deployment = nodes.prepare(config, directory)
            require_safe_accounts(deployment)
            assert deployment.production is None
            _, arguments = start_gateway(node, directory, launch, lease_seconds=30)
            install_permissions(case, nodes, directory, deployment, arguments)
            manifest = connector_for(deployment).permissions.manifest_type.model_validate_json(
                (directory / "permissions/manifest.json").read_text()
            )
            manifest_path = directory / "permissions/manifest.json"
            manifest_bytes = manifest_path.read_bytes()
            accounts = tuple(binding.name for binding in deployment.accounts.bindings)
            client = GatewayClient(arguments[3], Path(arguments[5]))
            try:
                record["initial_authority"] = exact_authority_state(
                    manifest, client.verify_permissions(manifest), accounts
                )
                record["authority"] = run(node, deployment)
                record["final_authority"] = exact_authority_state(
                    manifest, client.verify_permissions(manifest), accounts
                )
            finally:
                client.close()
            assert record["final_authority"] == record["initial_authority"]
            assert manifest_path.read_bytes() == manifest_bytes
            record["manifest_digest"] = manifest.digest
            record["manifest_sha256"] = hashlib.sha256(manifest_bytes).hexdigest()
            record["manifest_source_hash"] = manifest.source_hash
            record["manifest_configuration_hash"] = manifest.configuration_hash
            record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("Robinhood installed-authority evidence:", artifact)
