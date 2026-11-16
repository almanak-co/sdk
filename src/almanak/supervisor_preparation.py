"""Supervised fork and production preparation before the public author-loader cutover."""

import os
import secrets
import time
from contextlib import contextmanager
from pathlib import Path

from almanak.configuration.admission import PreparedDeployment
from almanak.configuration.bootstrap import read_progress, resolve_configuration
from almanak.configuration.fork_preparation import require_fork_policy
from almanak.configuration.prepared import prepare_production, read_preparation
from almanak.configuration.read_gateway import wait_read_gateway
from almanak.configuration.resolution import AnchorSelection, ReadContext
from almanak.configuration.unresolved import read_frozen_inputs
from almanak.connectors.composition import configuration_from_json
from almanak.connectors.support import RunMode
from almanak.gateway.client import GatewayClient
from almanak.gateway.fork_workspace import write_private
from almanak.gateway.operator_config import read_operator_config
from almanak.sandbox import readable_paths, validate_key_path
from almanak.strategy import StrategyDefinition
from almanak.supervisor_process import loopback_listener, running, spawn, stop, timing
from almanak.workspace import read, save

PREPARED = frozenset({"prepared-fork-v1", "prepared-production-v1"})


def configuration_token(mode: RunMode) -> str:
    token = f"prepared-{mode.value}-v1"
    if token not in PREPARED:
        raise ValueError("supervised preparation supports explicit fork or production mode")
    return token


def publish(path, value):
    save(path, value)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


CLOCK_OFFSET_FILE = "clock-offset"


def create_workspace(
    root, document, definition, context, gateway_config, env_file, *, test_clock=False
):
    """Persist provisional identity and inputs before a supervisor can perform I/O."""
    root, gateway_config, env_file = (path.resolve() for path in (root, gateway_config, env_file))
    operator = read_operator_config(gateway_config)
    if context.mode == RunMode.FORK:
        require_fork_policy(document, operator)
    token = configuration_token(context.mode)
    context.match_operator(operator, context.mode)
    document.inputs.require_context(context)
    signing = mode_settings(operator, context.mode)
    if Path(definition.filename).name != definition.filename or not definition.filename.endswith(
        ".py"
    ):
        raise ValueError("strategy filename must be a single Python filename")
    for path in (root / "control.token", gateway_config, env_file, *signing):
        validate_key_path(path, readable_paths())
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    for name in ("logs", "private", "private/deployment"):
        (root / name).mkdir(mode=0o700)
    write_private(root / "control.token", secrets.token_hex(32))
    write_private(root / "private/deployment/gateway-auth.token", secrets.token_hex(32))
    # strategy.py with the siblings it was read with (parameters.py, dashboard.py), so the
    # supervisor reads back the same definition.
    from almanak.strategy import DASHBOARD_FILE

    files = {
        definition.filename: definition.source,
        **{f"{name}.py": text for name, text in definition.modules.items()},
        **({DASHBOARD_FILE: definition.dashboard} if definition.dashboard is not None else {}),
    }
    for name, text in files.items():
        descriptor = os.open(root / name, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
    if test_clock:
        if context.mode != RunMode.FORK:
            raise ValueError("a test clock offset is available on identified forks only")
        write_private(root / CLOCK_OFFSET_FILE, "0")
    publish(root / "private/author-inputs.json", document.inputs.model_dump(mode="json"))
    publish(root / "private/read-context.json", context.model_dump(mode="json"))
    publish(
        root / "workspace.json",
        {
            "version": 1,
            "identity": secrets.token_hex(32),
            "configuration": token,
            "strategy_file": definition.filename,
            "mode": context.mode.value,
            "stage": "created",
            "credentials": {"gateway_config": str(gateway_config), "env_file": str(env_file)},
            "processes": {},
            # Only `almanak test run` prepares a test clock; its verdict reads decision facts.
            **(
                {"clock_offset_file": CLOCK_OFFSET_FILE, "decision_facts": True}
                if test_clock
                else {}
            ),
        },
    )
    return root


def selected_anchors(operator, mode, inputs):
    """Production reads fresh anchors; a fork reads exactly the operator fixture anchors."""
    chains = sorted({requirement.chain for requirement in inputs.requirements}, key=str)
    if mode == RunMode.PRODUCTION:
        return tuple(AnchorSelection(chain=chain, fresh=True) for chain in chains)
    if operator.fork is None:
        raise ValueError("supervised fork requires operator fixture inputs")
    anchors = operator.fork.anchors
    if sorted((anchor.chain for anchor in anchors), key=str) != chains:
        raise ValueError("operator fixture anchors must cover exactly the authored account chains")
    return anchors


def mode_settings(operator, mode):
    """Signing references stay in the operator file; a fork workspace never holds any."""
    if mode == RunMode.FORK:
        if operator.fork is None:
            raise ValueError("supervised fork requires operator fixture inputs")
        return ()
    if operator.production is None or operator.production.signing is None:
        raise ValueError("supervised production requires operator signing configuration")
    signing = operator.production.signing
    return tuple(
        path
        for path in (signing.key_file, signing.password_file, signing.signer_file)
        if path is not None
    )


def operator_for(root):
    state = read(root)
    context = ReadContext.model_validate_json((root / "private/read-context.json").read_text())
    operator = read_operator_config(Path(state["credentials"]["gateway_config"]))
    if (state.get("configuration"), state["mode"]) != (
        configuration_token(context.mode),
        context.mode.value,
    ):
        raise ValueError("workspace mode differs from its retained read context")
    context.match_operator(operator, context.mode)
    return operator, context


def retained(root):
    return PreparedDeployment.model_validate_json(
        (root / "private/deployment/preparation.json").read_text()
    )


def checked_preparation(root):
    record = retained(root)
    operator, context = operator_for(root)
    if record.resolution.context != context:
        raise ValueError("workspace context differs from frozen preparation")
    record.require_operator(operator)
    return record


def provision(supervisor):
    root = supervisor.root
    operator, context = operator_for(root)
    document = read_frozen_inputs(root / "private/author-inputs.json")
    document.inputs.require_context(context)
    definition = StrategyDefinition.read(supervisor.strategy_source)
    path = supervisor.deployment / "preparation.json"
    if context.mode == RunMode.FORK:
        from almanak.supervisor_forks import (
            install_permissions,
            prepare_fixture_bundle,
            start_venues,
        )

        record = prepare_fixture_bundle(supervisor, document, definition, context, operator)
        # Before publishing: a retained preparation would refuse corrected inputs later.
        require_price_access(record)
        if path.exists():
            if retained(root) != record:
                raise ValueError("workspace preparation differs from retained inputs and progress")
        else:
            publish(path, record.model_dump(mode="json"))
        record.require_operator(operator)
        # Before anything builds a fork adapter, which reads the venues from the routes.
        start_venues(supervisor, record, supervisor.nodes)
        install_permissions(supervisor, record, supervisor.nodes)
    else:
        record = production_bundle(supervisor, document, definition, context, path)
        record.require_operator(operator)
    config = record.configuration()
    config_path = supervisor.deployment / "deployment.json"
    if config_path.exists():
        if configuration_from_json(config_path.read_text()) != config:
            raise ValueError("retained deployment differs from prepared configuration")
    else:
        publish(config_path, config.model_dump(mode="json"))
    supervisor.persist(stage="provisioned", provisioned=True, preparation_digest=record.digest)


def require_price_access(record):
    """Refuse, before launch, declared series on a chain the deployment cannot read.

    The gateway resolves each pool on its chain before it serves.
    """
    from almanak.chains import configured_chains
    from almanak.gateway.strategy_prices import require_price_chains, strategy_bindings

    require_price_chains(
        strategy_bindings(record.preparation.prepared),
        set(configured_chains(record.configuration())),
    )


def production_bundle(supervisor, document, definition, context, path):
    root = supervisor.root
    if path.exists():
        record = retained(root)
        record.preparation.prepared.require(document, definition, record.resolution)
        if context != record.resolution.context or (
            read_progress(root / "private/resolution.json"),
            read_preparation(root / "private/parameters.json"),
        ) != (record.resolution, record.preparation):
            raise ValueError("workspace preparation differs from retained inputs and progress")
        return record
    if (root / "state.sqlite").exists():
        raise ValueError("existing deployment requires its original preparation artifacts")
    record = prepare(supervisor, document, definition, context)
    # Before publishing: a retained preparation would refuse corrected inputs later.
    require_price_access(record)
    publish(path, record.model_dump(mode="json"))
    return record


def prepare(supervisor, document, definition, context):
    supervisor.persist(stage="resolving")
    with read_client(supervisor, context) as client:
        resolution_path = supervisor.root / "private/resolution.json"
        resolve_configuration(resolution_path, context, client)
        progress = read_progress(resolution_path)
        supervisor.persist(stage="preparing")
        preparation = prepare_production(
            supervisor.root / "private/parameters.json", document, definition, progress, client
        )
        return PreparedDeployment(resolution=progress, preparation=preparation)


def resolve_progress(supervisor, context):
    supervisor.persist(stage="resolving")
    with read_client(supervisor, context) as client:
        resolution_path = supervisor.root / "private/resolution.json"
        resolve_configuration(resolution_path, context, client)
        return read_progress(resolution_path)


@contextmanager
def read_client(supervisor, context):
    client = read_gateway(supervisor, context)
    try:
        yield client
    finally:
        client.close()
        stop(supervisor.state["processes"].get("read-gateway"))


def read_gateway(supervisor, context):
    process = supervisor.state["processes"].get("read-gateway")
    spawned = not running(process)
    # Timed from before the spawn, so read_gateway_ready covers the whole cold start.
    started = time.monotonic()
    if spawned:
        with loopback_listener() as listener:
            port = listener.getsockname()[1]
            arguments = gateway_arguments(supervisor, listener.fileno(), read_only=True)
            process = spawn(supervisor.root, "read-gateway", arguments, listener=listener)
        try:
            supervisor.persist(
                read_gateway_url=f"http://127.0.0.1:{port}",
                processes={**supervisor.state["processes"], "read-gateway": process},
            )
        except BaseException:
            stop(process)
            raise
    client = GatewayClient(
        supervisor.state["read_gateway_url"], supervisor.deployment / "gateway-auth.token"
    )
    try:
        wait_read_gateway(client, context, process)
        if spawned:
            # The read gateway starts on the same cold path as the execution gateway.
            timing("read_gateway_ready", started)
        return client
    except BaseException:
        client.close()
        stop(process)
        raise


def gateway_arguments(supervisor, listen_fd, *, read_only=False):
    credentials = supervisor.state["credentials"]
    mode = supervisor.state["mode"]
    arguments = [
        "service",
        "gateway",
        "--gateway-config",
        credentials["gateway_config"],
        "--auth-token-file",
        str(supervisor.deployment / "gateway-auth.token"),
        "--listen-fd",
        str(listen_fd),
    ]
    if read_only:
        arguments.extend(["--" + mode, "--env-file", credentials["env_file"]])
        arguments.extend(["--read-context", str(supervisor.root / "private/read-context.json")])
        return arguments
    if mode == "production":
        arguments.extend(["--production", "--env-file", credentials["env_file"]])
    else:
        # The fork execution gateway receives generated transport and keys only.
        fixture = supervisor.deployment / "fixture"
        arguments.extend(["--fork-routes", str(fixture / "fork-routes.json")])
        arguments.extend(["--fork-key-file", str(fixture / "fork-keys.json")])
    arguments.extend(
        [
            "--preparation-file",
            str(supervisor.deployment / "preparation.json"),
            "--database",
            str(supervisor.root / "state.sqlite"),
        ]
    )
    return arguments
