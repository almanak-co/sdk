"""Launch the existing gateway HTTP service with read authority only."""

from almanak.configuration.resolution import ReadContext
from almanak.connectors.support import RunMode
from almanak.gateway.auth import read_token
from almanak.gateway.bootstrap import BootstrapService, serve
from almanak.gateway.operator_config import read_operator_config
from almanak.gateway.service import listen_address
from almanak.sandbox import readable_paths, validate_key_path


def run(args):
    forbidden = (
        args.accounts,
        args.database,
        args.key_file,
        args.password_file,
        args.password_stdin,
        args.fork_key_file,
        args.fork_url,
        args.fork_routes,
    )
    if any(forbidden):
        raise ValueError(
            "read bootstrap cannot load accounts, execution storage, signing keys or fork routes"
        )
    if args.read_context is None or args.env_file is None:
        raise ValueError("read bootstrap requires --read-context and operator --env-file")
    mode = selected_mode(args)
    for path in (args.env_file, args.auth_token_file, args.gateway_config, args.read_context):
        validate_key_path(path, readable_paths())
    read_token(args.auth_token_file)
    context = ReadContext.model_validate_json(args.read_context.read_text())
    operator = read_operator_config(args.gateway_config)
    service = BootstrapService(context, operator, mode, args.env_file)
    serve(service, args.auth_token_file, listen_address(args))
    return 0


def selected_mode(args) -> RunMode:
    selected = [mode for mode in RunMode if getattr(args, mode.value, False)]
    if len(selected) != 1:
        raise ValueError("read bootstrap requires explicit --fork, --production or --backtest")
    return selected[0]
