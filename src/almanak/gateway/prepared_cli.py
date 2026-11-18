"""Operator-controlled gateway launch from a generated fork or production preparation."""

import threading

from almanak.configuration.admission import PreparedDeployment
from almanak.connectors.support import RunMode
from almanak.gateway.auth import read_token
from almanak.gateway.fixture_accounts import require_fixture_nodes
from almanak.gateway.fork_workspace import fork_routes
from almanak.gateway.operator_config import read_operator_config
from almanak.gateway.production_cli import production_routes, read_password
from almanak.gateway.service import (
    GatewayService,
    listen_address,
    require_preparation,
    serve,
)
from almanak.sandbox import readable_paths, validate_key_path, worker_command
from almanak.state import Journal


def launch_inputs(args):
    if not 2 <= args.lease_seconds <= 120:
        raise ValueError("lease policy must be between 2 and 120 seconds")
    if args.production:
        return production_inputs(args)
    return fork_inputs(args)


def production_inputs(args):
    forbidden = (
        args.config,
        args.read_context,
        args.accounts,
        args.key_file,
        args.password_file,
        args.fork_key_file,
        args.fork_url,
        args.fork_routes,
        args.fork,
        args.backtest,
    )
    if any(forbidden):
        raise ValueError(
            "prepared production uses operator signing settings and explicit --production"
        )
    if any(
        value is None
        for value in (args.gateway_config, args.preparation_file, args.database, args.env_file)
    ):
        raise ValueError(
            "prepared production requires operator config, preparation, database and env file"
        )
    for path in (
        args.gateway_config,
        args.preparation_file,
        args.database,
        args.env_file,
        args.auth_token_file,
    ):
        validate_key_path(path, readable_paths())
    read_token(args.auth_token_file)
    preparation = PreparedDeployment.model_validate_json(args.preparation_file.read_text())
    if preparation.resolution.context.mode != RunMode.PRODUCTION:
        raise ValueError("prepared production launch requires a production preparation")
    operator = read_operator_config(args.gateway_config)
    config = preparation.configuration()
    require_preparation(config, preparation, operator, Journal(args.database))
    signing = operator.production.signing if operator.production is not None else None
    if signing is None:
        raise ValueError("operator has no production signing configuration")
    if args.password_stdin and signing.password_file is not None:
        raise ValueError("operator password file and ephemeral password cannot both be selected")
    for path in (signing.key_file, signing.password_file, signing.signer_file):
        if path is not None:
            validate_key_path(path, readable_paths())
    return preparation, operator, config, signing


def fork_inputs(args):
    """Generated fixture transport and keys only; author and production inputs are refused."""
    forbidden = (
        args.config,
        args.read_context,
        args.accounts,
        args.key_file,
        args.password_file,
        args.password_stdin,
        args.env_file,
        args.fork_url,
        args.fork,
        args.backtest,
    )
    if any(forbidden) or args.fork_routes is None or args.fork_key_file is None:
        raise ValueError(
            "prepared fork uses generated fixture routes and keys without author, "
            "production or single-node transport inputs"
        )
    if any(value is None for value in (args.gateway_config, args.preparation_file, args.database)):
        raise ValueError("prepared fork requires operator config, preparation and database")
    for path in (
        args.gateway_config,
        args.preparation_file,
        args.database,
        args.fork_routes,
        args.fork_key_file,
        args.auth_token_file,
    ):
        validate_key_path(path, readable_paths())
    worker_command(key_file=args.fork_key_file)
    read_token(args.auth_token_file)
    preparation = PreparedDeployment.model_validate_json(args.preparation_file.read_text())
    if preparation.fixture is None or preparation.resolution.context.mode != RunMode.FORK:
        raise ValueError("prepared fork launch requires a fork preparation with generated accounts")
    operator = read_operator_config(args.gateway_config)
    config = preparation.configuration()
    require_preparation(config, preparation, operator, Journal(args.database))
    require_fixture_nodes(preparation.fixture, args.fork_routes)
    return preparation, operator, config, None


def run(args):
    preparation, operator, config, signing = launch_inputs(args)
    if signing is None:
        backend = fork_routes(config, args.fork_routes, args.fork_key_file)
    else:
        password = read_password() if args.password_stdin else None
        backend = production_routes(
            config,
            args.env_file,
            signing.key_file,
            signing.password_file,
            password=password,
            signer_file=signing.signer_file,
        )
    try:
        prices = price_source(args, backend, preparation)
        service = GatewayService(
            config,
            args.database,
            backend,
            lease_seconds=args.lease_seconds,
            preparation=preparation,
            operator=operator,
            prices=prices,
        )
        serve(service, args.auth_token_file, listen_address(args))
    finally:
        backend.close()
    return 0


def price_source(args, backend, preparation):
    """The gateway's reader of declared strategy price series, from its own chain routes.

    A preparation whose pools cannot be resolved on their chains, or do not hold the
    declared asset, is refused before the gateway serves; each declared window is then
    read once in the background, so decisions read only their newest boundaries.
    """
    from almanak.gateway.strategy_prices import (
        StrategyPriceSource,
        resolve_price_pools,
        strategy_bindings,
        warm_price_series,
    )

    bindings = strategy_bindings(preparation.preparation.prepared)
    if not bindings:
        return None
    source = StrategyPriceSource(backend, args.database.with_name("strategy-prices.sqlite"))
    resolve_price_pools(source, bindings)
    # In the background: start-up must report ready within the supervisor's deadline.
    threading.Thread(target=warm_price_series, args=(source, bindings), daemon=True).start()
    return source
