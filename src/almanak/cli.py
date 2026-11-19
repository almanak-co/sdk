import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from almanak import native


def lifecycle(args) -> int:
    from almanak.runtime import report
    from almanak.state import Journal

    if args.product == "status" and args.database:
        if args.gateway_url or args.gateway_auth_file:
            raise ValueError("choose offline database audit or gateway status")
        print(json.dumps(Journal(args.database).audit(), indent=2))
        return 0
    if args.product in ("status", "teardown"):
        state = control_lifecycle(args)
    else:
        state = run_lifecycle(args)
    print(json.dumps(report(state), indent=2))
    return 0


def main() -> int:
    args = parse_arguments()
    try:
        return dispatch(args)
    except KeyboardInterrupt:
        print(interrupted_message(args), file=sys.stderr)
        return 130
    except Exception as exc:
        document = error_document(exc)
        if document is None:
            raise
        print(json.dumps(document), file=sys.stderr)
    return 1


def interrupted_message(args) -> str:
    if str(getattr(args, "product", "")).startswith("operator-"):
        # A direct action is never resumed; only the chain says what it did.
        return "Interrupted; read the position before acting again."
    return "Interrupted; the checkpoint is preserved for resume or teardown."


def error_document(exc: Exception) -> dict | None:
    """The one-line JSON error for a refused command, or None for an unexpected failure.

    The error types are imported here, not at module level: a supervisor may run `almanak
    status` every few seconds, and its import cost was mostly these types' dependencies.
    """
    from pydantic import ValidationError

    from almanak.backtest.types import AvailabilityError
    from almanak.configuration.yaml import ConfigurationError
    from almanak.gateway.ethereum import DataError
    from almanak.workspace import Unanswered

    if isinstance(exc, Unanswered):
        return exc.document
    if isinstance(exc, ConfigurationError):
        return {"error": "invalid_config", **exc.diagnostic()}
    if isinstance(exc, AvailabilityError):
        return {"error": "backtest_unavailable", **exc.failure.model_dump(mode="json")}
    if isinstance(exc, ValidationError):
        problems = [
            {"field": ".".join(map(str, e["loc"])), "message": e["msg"]}
            for e in exc.errors(include_input=False, include_url=False)
        ]
        return {"error": "invalid_config", "problems": problems}
    if isinstance(exc, DataError):
        document: dict[str, Any] = {"error": exc.kind, "message": str(exc)}
        # A source marks a failure transient when the same request can pass unchanged.
        if exc.transient:
            document["transient"] = True
        if exc.reverted:
            document["reverted"] = True
        if exc.details:
            document["details"] = exc.details
        return document
    from almanak.backtest.recent import RecentUnsupported

    if isinstance(exc, RecentUnsupported):
        return {"error": "recent_unsupported", "reason": exc.code, "message": str(exc)}
    if isinstance(exc, (ValueError, OSError, SyntaxError)):
        return {"error": "invalid_input", "message": str(exc)}
    if isinstance(exc, (RuntimeError, sqlite3.Error)):
        return {"error": "runtime", "message": str(exc)}
    return None


def dispatch(args) -> int:
    if (
        args.product in ("run", "pause", "shutdown")
        or getattr(args, "run", None) is not None
        and args.product in ("resume", "teardown", "status")
    ):
        from almanak.workspace import command

        return command(args)
    if args.product in ("report", "support"):
        from almanak.reports import command

        return command(args)
    if args.product in ("wallet-create", "production-check"):
        from almanak.gateway.production_cli import command

        return command(args)
    if args.product == "test":
        from almanak.testflow import command as lifecycle_test

        return lifecycle_test(args)
    if args.product == "test-config":
        from almanak.fork_operator import command as fork_config

        return fork_config(args)
    if args.product.startswith("backtest"):
        from almanak.backtest.cli import run

        return run(args)
    if args.product == "markets":
        from almanak.markets import command as markets

        return markets(args)
    if args.product == "operator-action":
        from almanak.ax_actions import command as operator_action

        return operator_action(args)
    if args.product == "operator-recover":
        from almanak.ax_actions import recover_command

        return recover_command(args)
    if args.product.startswith("ax-"):
        from almanak.ax import command

        return command(args)
    commands = {
        "check": check_config,
        "gateway": run_gateway,
        "read-gateway": run_read_gateway,
        "fork-venue": run_fork_venue,
        "permissions": run_permissions,
    }
    if args.product in commands:
        return commands[args.product](args)
    if args.product in ("start", "resume", "teardown", "status"):
        return lifecycle(args)
    raise ValueError(f"unknown product: {args.product}")


def check_config(args):
    from almanak.checks import check
    from almanak.presentation import emit

    result = check(args)
    emit(result, args)
    return 0


def run_gateway(args):
    from almanak.connectors.composition import configuration_from_json
    from almanak.gateway.auth import read_token
    from almanak.gateway.service import GatewayService, listen_address, serve
    from almanak.runtime import worker_command

    if args.port is not None and not 1 <= args.port <= 65535:
        raise ValueError("gateway port must be between 1 and 65535")
    if getattr(args, "gateway_config", None) is not None:
        if getattr(args, "preparation_file", None) is not None:
            from almanak.gateway.prepared_cli import run as run_prepared

            return run_prepared(args)
    if getattr(args, "gateway_config", None) is not None:
        from almanak.gateway.bootstrap_cli import run

        return run(args)
    if (
        args.database is None
        or getattr(args, "read_context", None) is not None
        or getattr(args, "preparation_file", None) is not None
        or getattr(args, "fork", False)
        or getattr(args, "backtest", False)
    ):
        raise ValueError("execution gateway requires --database and its execution transport mode")
    worker_command(key_file=args.auth_token_file)
    read_token(args.auth_token_file)
    config = configuration_from_json(args.config.read_text())
    if getattr(config, "preparation_digest", None) is not None:
        raise ValueError(
            "prepared deployment requires operator gateway config and preparation file"
        )
    if args.production:
        from almanak.gateway.production_cli import production_backend

        backend = production_backend(args, config)
    else:
        backend = configured_fork_backend(args, config)
    try:
        service = GatewayService(config, args.database, backend, lease_seconds=args.lease_seconds)
        serve(service, args.auth_token_file, listen_address(args))
    finally:
        backend.close()
    return 0


def configured_fork_backend(args, config):
    from almanak.gateway.fork_venues import fork_adapter
    from almanak.runtime import worker_command

    if args.fork_key_file is None or any((args.key_file, args.password_file, args.env_file)):
        raise ValueError(
            "fork gateway requires --fork-key-file and cannot load production credentials"
        )
    worker_command(key_file=args.fork_key_file)
    if args.fork_routes is not None:
        from almanak.gateway.fork_workspace import fork_routes

        return fork_routes(config, args.fork_routes, args.fork_key_file)
    # A connector whose fork runs a venue beside the node needs it from a route.
    return fork_adapter(config, args.fork_url, args.fork_key_file)


def parse_arguments():
    from almanak.cli_arguments import build_parser, parse_validated, product_names

    parser = build_parser()
    native.forward_unrecognized(sys.argv[1:], product_names(parser))
    return parse_validated(parser)


def run_fork_venue(args) -> int:
    from almanak.gateway.fork_venues import serve
    from almanak.gateway.service import inherited_listener

    return serve(args.spec, inherited_listener(args.listen_fd))


def run_read_gateway(args) -> int:
    from almanak.gateway.observation_gateway import command

    return command(args)


def permissions_arguments(products):
    permissions = products.add_parser(
        "permissions", help="generate or verify strategy-derived permissions"
    )
    commands = permissions.add_subparsers(dest="permission_command", required=True)
    for name in ("generate", "verify"):
        command = commands.add_parser(name)
        command.add_argument("--gateway-url", required=True)
        command.add_argument("--gateway-auth-file", type=Path, required=True)
        if name == "generate":
            command.add_argument("--strategy", type=Path, required=True)
            command.add_argument("--config", type=Path, required=True)
            command.add_argument("--output", type=Path, required=True)
        else:
            command.add_argument("--manifest", type=Path, required=True)


def run_permissions(args):
    import time

    from almanak.connectors.composition import configuration_from_json
    from almanak.gateway.client import GatewayClient
    from almanak.gateway.permissions import write_artifacts
    from almanak.strategy import StrategyDefinition

    client = GatewayClient(args.gateway_url, args.gateway_auth_file)
    started = time.monotonic()
    try:
        if args.permission_command == "generate":
            config = configuration_from_json(args.config.read_text())
            manifest = client.generate_permissions(config, StrategyDefinition.read(args.strategy))
            write_artifacts(manifest, args.output)
            result = {
                "manifest": str((args.output / "manifest.json").resolve()),
                "manifest_bytes": (args.output / "manifest.json").stat().st_size,
                "artifact_bytes": sum(path.stat().st_size for path in args.output.iterdir()),
            }
        else:
            client.configuration()
            manifest = client.connector.permissions.manifest_type.model_validate_json(
                args.manifest.read_text()
            )
            result = client.verify_permissions(manifest)
        result["elapsed_seconds"] = time.monotonic() - started
        print(json.dumps(result, indent=2))
        return 0
    finally:
        client.close()


def control_lifecycle(args):
    from almanak.gateway.client import GatewayClient

    if not args.gateway_url or not args.gateway_auth_file:
        raise ValueError("gateway URL and authentication file are required")
    client = GatewayClient(args.gateway_url, args.gateway_auth_file)
    try:
        return client.teardown() if args.product == "teardown" else client.load()
    finally:
        client.close()


def run_lifecycle(args):
    from almanak.interrupts import deferred_sigint

    with deferred_sigint() as interrupt:
        return coordinated_lifecycle(args, interrupt)


def coordinated_lifecycle(args, interrupt):
    from almanak.connectors.composition import configuration_from_json
    from almanak.runtime import Runtime
    from almanak.strategy import StrategyDefinition
    from almanak.triggers import offset_clock, utc_now

    config = configuration_from_json(args.config.read_text()) if args.config else None
    offset = getattr(args, "clock_offset_file", None)
    runner = Runtime(
        args.gateway_url,
        args.gateway_auth_file,
        create=config if args.product == "start" else None,
        strategy=StrategyDefinition.read(args.strategy) if args.product == "start" else None,
        expected=config if args.product == "resume" else None,
        export=getattr(args, "export", None),
        clock=utc_now if offset is None else offset_clock(offset),
    )
    runner.decision_facts = bool(getattr(args, "decision_facts", False))
    state = runner.journal.load()
    print(
        json.dumps({"event": "ready", "deployment_id": state.deployment_id}),
        file=sys.stderr,
        flush=True,
    )
    if runner.decision_facts:
        from almanak import run_facts

        run_facts.log(state)
    return runner.run(interrupt=interrupt)


if __name__ == "__main__":
    raise SystemExit(main())
