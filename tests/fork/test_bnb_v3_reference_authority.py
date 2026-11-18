"""Pristine generated Safe authority for the actual BNB V3 references."""

import json
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import pytest

from almanak.connectors.composition import connector_for
from almanak.connectors.identifiers import ConnectorId
from almanak.gateway.client import GatewayClient
from almanak.models import SafeRoles
from almanak.strategy import StrategyDefinition
from tests.fork.support import processes, start_gateway
from tests.strategy_fork.bnb_v3_authority import exchange_permissions, lending_permissions
from tests.strategy_fork.harness import (
    bind_workspace,
    case_fork_config,
    discover,
    install_permissions,
    require_safe_accounts,
    resolve_config,
    retain_context,
)

ROOT = Path(__file__).parents[2]
SCENARIOS = ROOT / "tests/strategy_fork/scenarios"
REFERENCES = (
    "uniswap_v3_bnb_trading",
    "uniswap_v3_bnb_lp",
    "bnb_aave_borrow_and_trade_v3",
)
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def _case(reference):
    return next(case for case in discover(ROOT / "strategies", SCENARIOS) if case.name == reference)


def _probe(node, deployment, reference):
    if reference == "bnb_aave_borrow_and_trade_v3":
        lending = deployment.component(ConnectorId("aave_v3"))
        exchange = deployment.component(ConnectorId("uniswap_v3"))
        return {
            "aave_v3": lending_permissions(node, lending),
            "uniswap_v3": exchange_permissions(node, exchange, "trade"),
        }
    return {"uniswap_v3": exchange_permissions(node, deployment, "position")}


def _verified_partitions(manifest, result):
    """Installed verification covers each generated partition once; scope-only, no quotas."""
    accounts = [partition.account for partition in manifest.partitions]
    if not accounts:
        raise AssertionError("generated permission manifest has no account partitions")
    if len(set(accounts)) != len(accounts):
        raise AssertionError("generated permission manifest repeats an account partition")
    if result.get("manifest") != manifest.digest:
        raise AssertionError("permission verification returned a different manifest identity")
    partitions = result.get("partitions")
    if not isinstance(partitions, list) or len(partitions) != len(accounts):
        raise AssertionError("permission verification did not cover every manifest partition")
    observed = []
    for partition in partitions:
        if not isinstance(partition, dict) or partition.get("account") not in accounts:
            raise AssertionError("permission verification returned an unknown account partition")
        if partition["account"] in observed:
            raise AssertionError("permission verification repeated an account partition")
        if "quotas" in partition:
            raise AssertionError("scope-only verification reported Roles allowances")
        observed.append(partition["account"])
    return tuple(sorted(observed))


def _verify_pristine(manifest, arguments):
    client = GatewayClient(arguments[3], Path(arguments[5]))
    try:
        result = client.verify_permissions(manifest)
    finally:
        client.close()
    return result, _verified_partitions(manifest, result)


@pytest.mark.parametrize("reference", REFERENCES)
def test_bnb_v3_generated_safe_authority_before_runtime_entry(request, tmp_path, reference):
    """Install from the real strategy, probe fresh grants, and never start its coordinator."""
    case = _case(reference)
    env = request.config.getoption("--fork-env").resolve()
    config = resolve_config(case_fork_config(case, env, tmp_path / "resolution"), env, {})
    artifact = ROOT / "artifacts/bnb-v3-pristine-authority" / f"{reference}-{uuid4().hex}"
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    directory = temporary / "deployment"
    record = {
        "strategy": reference,
        "strategy_sha256": StrategyDefinition.read(case.strategy).sha256,
        "account_profile": "safe_roles",
        "authority": "identified Anvil fixture accounts only; no production authority",
        "runtime_started": False,
        "status": "failed",
        "phases": [],
    }
    try:
        with ExitStack() as stack:
            nodes, node = bind_workspace(config, env, stack, record)
            launch = stack.enter_context(processes(temporary))
            deployment = nodes.prepare(config, directory)
            require_safe_accounts(deployment)
            assert deployment.production is None
            assert all(
                isinstance(binding.profile, SafeRoles) for binding in deployment.accounts.bindings
            )
            _, arguments = start_gateway(node, directory, launch, lease_seconds=30)
            install_permissions(case, nodes, directory, deployment, arguments)
            record["phases"].append("generated_strategy_permissions_installed_and_verified")
            manifest_type = connector_for(deployment).permissions.manifest_type
            manifest = manifest_type.model_validate_json(
                (directory / "permissions/manifest.json").read_text()
            )
            initial, initial_partitions = _verify_pristine(manifest, arguments)
            observations = {"verification_before_probes": initial}
            observation_path = directory / "permission-budget-observations.json"
            observation_path.write_text(json.dumps(observations, indent=2) + "\n")
            record["phases"].append("initial_generated_permissions_verified")
            observations.update(_probe(node, deployment, reference))
            after, after_partitions = _verify_pristine(manifest, arguments)
            if after_partitions != initial_partitions:
                raise AssertionError("read-only authority probes changed installed authority")
            observations["verification_after_probes"] = after
            (directory / "permission-budget-observations.json").write_text(
                json.dumps(observations, indent=2) + "\n"
            )
            record["phases"].extend(
                (
                    "pristine_roles_controls_and_predicates_probed",
                    "installed_authority_verified_after_eth_calls",
                )
            )
            record["status"] = "passed"
    finally:
        if directory.is_dir():
            retain_context(directory, artifact)
        (artifact / "result.json").write_text(json.dumps(record, indent=2) + "\n")
        print("BNB V3 pristine authority evidence:", artifact)
