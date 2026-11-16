"""The HyperCore perp reference through the production adapter on a fork (blueprint 30,
section 14.3).

The fork workspace's HyperEVM node and `hypercore-model` process are prepared as for a fork
run, and its fork gateway installs the Safe's generated grants. The deployment then runs
under a production policy instead: accounts on the mainnet chain, encrypted
operator keystores, the production adapter generating encrypted agent keys, and no A4.
The coordinator enters, is restarted, the gateway is replaced (so the agent keys are read
back from their encrypted file), and teardown closes and withdraws.

The HyperEVM side is fork evidence; every HyperCore effect is the synthesized model's
(section 9), so nothing here is evidence of HyperCore's behaviour.
"""

import json
import signal
import socket
import sys
import time
from pathlib import Path

import pytest

from almanak.connectors.hyperliquid.connector import support
from almanak.connectors.hyperliquid.fork_venue import FIXTURE, ModelKeeper
from almanak.connectors.support import RunMode
from almanak.gateway.credentials import write_private
from almanak.gateway.evm import chain_number
from almanak.gateway.fork_workspace import ForkSet
from almanak.gateway.hypercore_gateway import AGENT_KEY_FILE
from almanak.gateway.provisioning import production_configuration
from almanak.models import Account, AccountBinding
from almanak.state import Journal, release_claims
from tests.fork.support import CLI, processes, request_teardown, resume, start_gateway, wait_state
from tests.fork.test_production import encrypt_test_keys
from tests.strategy_fork.adapter import fork_config_for, resolve_reference
from tests.strategy_fork.harness import Case, install_permissions, load_assertions, stop_runtime
from tests.strategy_fork.test_production_rehearsals import rehearsal_policy

pytestmark = [
    pytest.mark.fork,
    # Configuration refuses production for a connector that does not declare it, so the
    # rehearsal runs only where D7's acceptance declares it (section 14.4).
    pytest.mark.skipif(
        RunMode.PRODUCTION not in support()[0].modes,
        reason="Hyperliquid production is declared once D7 is accepted",
    ),
]

ROOT = Path(__file__).resolve().parents[2]
NAME = "hyperliquid_hyperevm_perp"
CASE = Case(
    NAME,
    ROOT / "strategies" / NAME / "strategy.py",
    ROOT / "strategies" / NAME / "config.yaml",
    ROOT / "tests/strategy_fork/scenarios" / NAME / "assertions.py",
)
OPERATOR = ROOT / "examples/operators" / (NAME + ".fork.yaml")
DRIVER = Path(__file__).with_name("hypercore_production_driver.py")
LIFECYCLE_SECONDS = 900
ANVIL_CHAIN_ID = 31337
ASSERTIONS = load_assertions(CASE)


def mainnet_bindings(nodes, deployment):
    """The fork's Safe and operator, bound on the mainnet chain as production binds them."""
    bindings = []
    for binding in deployment.accounts.bindings:
        profile = binding.profile
        origin = nodes.for_chain(profile.fund.chain).origin.chain
        rebound = profile.model_copy(
            update={
                "fund": Account(chain=origin, address=profile.fund.address),
                "operator": Account(chain=origin, address=profile.operator.address),
            }
        )
        bindings.append(AccountBinding(name=binding.name, profile=rebound))
    return bindings


def production_deployment(nodes, config, deployment, directory):
    """The rehearsal's production policy over the prepared fork (the shared HyperEVM one,
    `tests/strategy_fork/rehearsal-policy.json`, since the reference authors no gas), and
    its gateway's inputs."""
    production = production_configuration(
        rehearsal_policy(config), mainnet_bindings(nodes, deployment)
    )
    write_private(directory / "production.json", production.model_dump_json(indent=2))
    encrypt_test_keys(directory)
    (node,) = nodes.nodes.values()
    origin = node.origin.chain
    node.rpc("anvil_setChainId", [chain_number(origin.chain_id)])
    node.rpc.signing_chain = origin.chain_id
    write_private(directory / "identity.json", json.dumps(node.rpc("anvil_metadata")))
    return node


def launch_gateway(directory, node, model, launch):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    arguments = [sys.executable, str(DRIVER), "--port", str(port)]
    arguments += ["--fork-url", node.rpc.endpoint, "--model", model]
    for option, filename in (
        ("config", "production.json"),
        ("key-file", "keystores.json"),
        ("password-file", "password"),
        ("database", "production.sqlite"),
        ("auth-token-file", "gateway-auth.token"),
        ("identity", "identity.json"),
    ):
        arguments += ["--" + option, str(directory / filename)]
    process = launch(arguments, ready="gateway_ready")
    control = [
        "--config",
        str(directory / "production.json"),
        "--gateway-url",
        f"http://127.0.0.1:{port}",
        "--gateway-auth-file",
        str(directory / "gateway-auth.token"),
    ]
    return process, arguments, control


def owners_send_hype(node, model):
    """Checklist step 5 (section 14.5): once the first deposit has activated the Safe's
    HyperCore account, its owners send HyperCore HYPE for withdrawals' payouts. The model
    process sends it as the fixture's owners, who sign only on Anvil's own chain ID
    (`safe_fixture.owner_transaction`), so with the coordinator stopped the fork answers
    that ID until the owners' transfer is sent, then production's again."""
    keeper, origin = ModelKeeper(model), int(node.rpc("eth_chainId"), 16)
    node.rpc("anvil_setChainId", [ANVIL_CHAIN_ID])
    try:
        deadline = time.monotonic() + 60
        while fixture_status(keeper) != "sent":
            assert time.monotonic() < deadline, "the owners' HyperCore HYPE was never sent"
            time.sleep(0.5)
    finally:
        node.rpc("anvil_setChainId", [origin])


def fixture_status(keeper):
    return (
        keeper.control({"op": "state"})["model"].get("fixture", {}).get(FIXTURE, {}).get("status")
    )


def test_the_reference_runs_through_the_production_adapter(request, tmp_path):
    env_file = request.config.getoption("--fork-env").resolve()
    progress = resolve_reference(CASE.config, OPERATOR, env_file, tmp_path / "resolution")
    config = fork_config_for(CASE.config, OPERATOR, progress)
    try:
        rehearse(config, env_file, tmp_path)
    finally:
        # Claims on the mainnet chain outlive the fork: a stopped run's must not block the
        # next, since the fixture's Safe address repeats.
        release_claims(tmp_path / "deployment" / "production.sqlite")


def rehearse(config, env_file, tmp_path):
    with ForkSet(config, env_file) as nodes, processes(tmp_path) as launch:
        directory = tmp_path / "deployment"
        deployment = nodes.prepare(config, directory)
        _, arguments = start_gateway(nodes, directory, launch, lease_seconds=5)
        install_permissions(CASE, nodes, directory, deployment, arguments)
        nodes.gateway_process.send_signal(signal.SIGINT)
        assert nodes.gateway_process.wait(timeout=60) == 0
        (route,) = json.loads((directory / "fork-routes.json").read_text())
        model = route["venues"]["hypercore"]["model"]
        node = production_deployment(nodes, config, deployment, directory)
        gateway, gateway_args, control = launch_gateway(directory, node, model, launch)
        journal = Journal(directory / "production.sqlite")
        closed = production_lifecycle(launch, journal, node, model, control, gateway, gateway_args)
        ASSERTIONS.assert_closed(node, closed)


def production_lifecycle(launch, journal, node, model, control, gateway, gateway_args):
    """Enter, stop the coordinator while the owners send HyperCore HYPE, replace the
    gateway, resume, then tear down to closure."""
    runtime = launch([CLI, "service", "start", "--strategy", str(CASE.strategy), *control])
    held = wait_state(journal, ASSERTIONS.entered, runtime, timeout=LIFECYCLE_SECONDS)
    ASSERTIONS.assert_holding(node, held)
    assert held.config.production is not None
    # The agent key the production adapter generated is kept encrypted, never bare.
    (keys,) = json.loads((journal.path.parent / AGENT_KEY_FILE).read_text()).values()
    assert keys and all("crypto" in entry for entry in keys.values())
    stop_runtime(runtime, timeout=60)
    owners_send_hype(node, model)
    gateway.send_signal(signal.SIGINT)
    assert gateway.wait(timeout=60) == 0
    launch(gateway_args, ready="gateway_ready")
    runtime = resume(launch, journal, control)
    ASSERTIONS.assert_holding(node, wait_state(journal, ASSERTIONS.entered, runtime))
    request_teardown(control)
    closed = wait_state(journal, lambda s: s.mode == "closed", runtime, timeout=LIFECYCLE_SECONDS)
    assert runtime.wait(timeout=60) == 0
    return closed
