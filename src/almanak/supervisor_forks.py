"""Retain original fork nodes and reconcile pre-execution fixture setup."""

import json
import secrets
import shutil
from contextlib import ExitStack
from pathlib import Path

from almanak.configuration.admission import PreparedDeployment
from almanak.configuration.bootstrap import read_progress
from almanak.configuration.fixture import prepare_fixture
from almanak.configuration.fork_preparation import prepare_fork, require_fork_policy
from almanak.gateway import fork_venues
from almanak.gateway.ethereum import DataError
from almanak.gateway.fixture_accounts import (
    provision_accounts,
    publish,
    require_fixture_nodes,
)
from almanak.gateway.fork_compatibility import check_node_code
from almanak.gateway.fork_config import provider_endpoint
from almanak.gateway.fork_fixture import Fork, Rpc
from almanak.gateway.fork_permissions import prepare_permissions
from almanak.gateway.fork_workspace import ForkSet
from almanak.models import Block, SafeRoles
from almanak.supervisor_process import identity, loopback_listener, running, spawn
from almanak.workspace import save


def record(supervisor, chain, node):
    name = "fork-" + str(chain.chain_id)
    entry = {
        "pid": node.process.pid,
        "identity": identity(node.process.pid),
        "origin": node.origin.model_dump(mode="json"),
        "metadata": node.metadata,
        "endpoint": node.rpc.endpoint,
        "proxy_port": node.proxy.server_port,
        "snapshot": node.rpc("evm_snapshot"),
    }
    supervisor.persist(processes={**supervisor.state["processes"], name: entry})


def reconnect(supervisor, config):
    nodes = ForkSet(config, Path(supervisor.state["credentials"]["env_file"]))
    nodes.stack = ExitStack()
    try:
        for target in config.forks:
            entry = supervisor.state["processes"].get("fork-" + str(target.chain.chain_id))
            if not running(entry):
                raise RuntimeError("original fork node is stopped or unidentified; cannot resume")
            node = Fork(
                nodes.env_file,
                Block.model_validate_json(json.dumps(entry["origin"])),
                target=target,
            )
            node.rpc, node.metadata = Rpc(entry["endpoint"]), entry["metadata"]
            node._validate_origin()
            node.cleanup = ExitStack()
            node.start_proxy(
                provider_endpoint(target.provider, nodes.env_file), entry["proxy_port"]
            )
            nodes.stack.callback(node.cleanup.close)
            nodes.nodes[target.chain] = node
        return nodes
    except BaseException:
        nodes.stack.close()
        raise


def rollback_node(supervisor, nodes, name, entry):
    node = next(n for n in nodes.nodes.values() if n.rpc.endpoint == entry["endpoint"])
    node._validate_origin()
    if entry.get("snapshot") is None:
        raise RuntimeError("fixture rollback outcome is uncertain; retain evidence")
    snapshot = entry["snapshot"]
    supervisor.persist(
        processes={**supervisor.state["processes"], name: {**entry, "snapshot": None}}
    )
    if not node.rpc("evm_revert", [snapshot]):
        raise RuntimeError("retained fixture snapshot is unavailable")
    supervisor.persist(
        processes={
            **supervisor.state["processes"],
            name: {**entry, "snapshot": node.rpc("evm_snapshot")},
        }
    )


def fixture_directory(supervisor):
    return supervisor.deployment / "fixture"


def fork_entries(supervisor):
    return {
        name: entry
        for name, entry in supervisor.state["processes"].items()
        if name.startswith("fork-")
    }


def attach_nodes(supervisor, plan):
    """Reattach the retained nodes or start the first ones; a lost fork is never recreated."""
    if supervisor.nodes is not None:
        if supervisor.nodes.config != plan or any(
            not running(entry) for entry in fork_entries(supervisor).values()
        ):
            raise RuntimeError("attached fork nodes differ from the retained plan or stopped")
        return supervisor.nodes
    if fork_entries(supervisor):
        nodes = reconnect(supervisor, plan)
    else:
        retained = (
            fixture_directory(supervisor),
            supervisor.root / "private/parameters.json",
            supervisor.deployment / "preparation.json",
            supervisor.root / "state.sqlite",
        )
        if any(path.exists() for path in retained):
            raise RuntimeError(
                "fixture nodes were never retained or were lost; a fork is never recreated"
            )
        nodes = ForkSet(
            plan,
            Path(supervisor.state["credentials"]["env_file"]),
            on_ready=lambda chain, node: record(supervisor, chain, node),
        )
        nodes.__enter__()
    supervisor.nodes = nodes
    return nodes


def reconcile_fixture(supervisor, nodes):
    """Decide reuse, rollback or refusal from retained evidence before any funding.

    A complete account record is reused and verified by provisioning; it is never
    reverted. An interrupted setup is reverted only through the original nodes'
    retained snapshots. Frozen hooks or execution state without a complete fixture
    cannot be repaired automatically.
    """
    directory = fixture_directory(supervisor)
    complete = (directory / "fixture-accounts.json").is_file()
    frozen = (supervisor.root / "private/parameters.json").exists() or (
        supervisor.deployment / "preparation.json"
    ).exists()
    if (supervisor.root / "state.sqlite").exists() and not (complete and frozen):
        raise ValueError("existing deployment requires its original preparation artifacts")
    if complete:
        return
    if frozen:
        raise RuntimeError("frozen preparation has no complete fixture; retain evidence")
    if directory.exists():
        rollback_fixture(supervisor, nodes, directory)
    elif any(entry.get("snapshot") is None for entry in fork_entries(supervisor).values()):
        raise RuntimeError("fixture rollback outcome is uncertain; retain evidence")


def rollback_fixture(supervisor, nodes, directory):
    """Revert interrupted funding through retained snapshots and archive its evidence."""
    if (supervisor.root / "state.sqlite").exists():
        raise RuntimeError("cannot roll back a fork with authoritative execution state")
    if (directory / "fixture-accounts.json").exists():
        raise RuntimeError("completed fixture setup is never rolled back")
    for name, entry in fork_entries(supervisor).items():
        rollback_node(supervisor, nodes, name, entry)
    archive = supervisor.root / "private" / ("interrupted-" + secrets.token_hex(8))
    shutil.move(str(directory), archive)


def prepare_fixture_bundle(supervisor, document, definition, context, operator):
    """Resolve, retain nodes, provision accounts and freeze hooks; reuse repeats nothing."""
    from almanak.supervisor_preparation import resolve_progress

    require_fork_policy(document, operator)
    root = supervisor.root
    if (supervisor.deployment / "preparation.json").exists():
        progress = read_progress(root / "private/resolution.json")
    else:
        if (root / "state.sqlite").exists():
            raise ValueError("existing deployment requires its original preparation artifacts")
        progress = resolve_progress(supervisor, context)
    if progress.result is None:
        raise ValueError("fork preparation requires complete resolution")
    plan = prepare_fixture(
        root / "private/fixture-plan.json", context, operator, progress.result.anchors
    )
    nodes = attach_nodes(supervisor, plan)
    # Before funding, and again on reuse since a fixture funded before this check never
    # ran it: a token the fork cannot execute would otherwise fail a funding route or the
    # first decision read with only the node's opcode error. It is a few local reads.
    check_node_code(plan, progress.result.markets, nodes)
    reconcile_fixture(supervisor, nodes)
    supervisor.persist(stage="provisioning")
    accounts = provision_accounts(fixture_directory(supervisor), plan, context, operator, nodes)
    supervisor.persist(stage="preparing")
    preparation = prepare_fork(
        root / "private/parameters.json", document, definition, progress, accounts, operator
    )
    return PreparedDeployment(resolution=progress, preparation=preparation, fixture=accounts)


class SupervisedVenues:
    """The venue host of a supervised workspace (`gateway/fork_venues.py`). Each venue is
    recorded as `venue-<name>` with its endpoint, which the fork route names, so a later
    launch reattaches it, or restarts it at that endpoint over its retained state, and
    shutdown stops it with the nodes."""

    def __init__(self, supervisor, nodes, directory):
        self.supervisor, self.nodes, self.directory = supervisor, nodes, directory

    def node(self, chain):
        if self.nodes is None:
            raise RuntimeError("fork nodes are not attached; relaunch the workspace to resume")
        return self.nodes.for_chain(chain)

    def launch(self, name, spec):
        key = "venue-" + name
        entry = self.supervisor.state["processes"].get(key)
        if running(entry):
            # A live process can still have stopped serving: reattach only one that answers.
            fork_venues.wait_ready(entry["endpoint"], lambda: running(entry))
            return entry["endpoint"]
        with loopback_listener(entry["port"] if entry else 0) as listener:
            port = listener.getsockname()[1]
            arguments = fork_venues.arguments(spec, listener)
            process = spawn(self.supervisor.root, key, arguments, listener=listener)
        endpoint = f"http://127.0.0.1:{port}"
        record = {**process, "endpoint": endpoint, "port": port}
        self.supervisor.persist(processes={**self.supervisor.state["processes"], key: record})
        fork_venues.wait_ready(endpoint, lambda: running(record))
        return endpoint


def start_venues(supervisor, bundle, nodes):
    """Start or reattach the venues the deployment's connector runs beside the fork nodes
    and name them in the fork routes; a connector without venues changes nothing."""
    directory = fixture_directory(supervisor)
    host = SupervisedVenues(supervisor, nodes, directory)
    venues = fork_venues.start(bundle.configuration(), host)
    if not venues:
        return
    path = directory / "fork-routes.json"
    routes = json.loads(path.read_text())
    updated = fork_venues.routed(routes, venues)
    if updated != routes:
        publish(path, updated)


def require_venues(supervisor, bundle):
    """Before a coordinator starts: a venue that exited while execution was paused restarts
    at its endpoint over its retained state, and every venue must answer."""
    try:
        start_venues(supervisor, bundle, supervisor.nodes)
    # OSError: a restart rebinds the recorded port, which another process may hold.
    except (RuntimeError, OSError) as error:
        raise DataError("venue_unavailable", str(error)) from None


def safe_profiles(fixture):
    return any(isinstance(b.profile, SafeRoles) for b in fixture.accounts.bindings)


def install_permissions(supervisor, bundle, nodes):
    """Install generated grants once; an interrupted installation is never repeated."""
    if not safe_profiles(bundle.fixture):
        return
    directory = fixture_directory(supervisor)
    if (directory / "permission-verification.json").is_file():
        return
    if (directory / "permissions").exists():
        raise RuntimeError(
            "permission installation was interrupted; retain evidence and reconcile explicitly"
        )
    prepare_permissions(
        nodes,
        bundle.configuration(),
        supervisor.strategy_source,
        directory,
        prepared=bundle.preparation.prepared,
    )


def fork_admission(supervisor):
    """Admit execution only on the identified nodes with verified generated authority."""
    from almanak.supervisor_preparation import checked_preparation

    bundle = checked_preparation(supervisor.root)
    directory = fixture_directory(supervisor)
    try:
        require_fixture_nodes(bundle.fixture, directory / "fork-routes.json")
        require_venues(supervisor, bundle)
        if (
            safe_profiles(bundle.fixture)
            and not (directory / "permission-verification.json").is_file()
        ):
            raise DataError("authority", "generated permissions are not verified as installed")
        result = {
            "ready": True,
            "execution_environment": "identified_anvil",
            "preparation_digest": bundle.digest,
        }
    except DataError as error:
        result = {"ready": False, "reason": f"{error.kind}: {error}"}
    save(supervisor.root / "preflight.json", result)
    if not result["ready"]:
        supervisor.persist(stage="action_required", diagnostic=result["reason"])
    return result["ready"]
