"""Commands follow author tasks; service mechanics remain explicitly grouped."""

import argparse
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path

from almanak.connectors.discovery import (
    lending_discovery,
    perp_discovery,
    pool_connectors,
    pool_discovery,
)


def output_options(parser, *, html=False):
    parser.add_argument("--format", choices=("text", "json", "html") if html else ("text", "json"))
    parser.add_argument("--json", dest="format", action="store_const", const="json")


def author_options(parser):
    parser.add_argument("directory", type=Path, nargs="?", default=Path("."))
    parser.add_argument("--config", type=Path)
    parser.add_argument("--strategy", type=Path)


def credentials(parser):
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--accounts", type=Path, help="operator-generated account bindings")
    parser.add_argument("--key-file", type=Path)
    parser.add_argument("--password-file", type=Path)


def operator_context(parser, *, required):
    parser.add_argument(
        "--gateway-config",
        type=Path,
        required=required,
        help="operator provider, account and execution settings",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        required=required,
        help="private provider credentials read only by the operator gateway",
    )


def observation_transport(parser):
    # Exactly one read transport: a local operator env file, or a read-only
    # observation gateway that holds the provider credentials instead. Without
    # either flag, parse_validated takes the gateway from the session environment.
    transport = parser.add_mutually_exclusive_group()
    transport.add_argument(
        "--env-file", type=Path, help="private provider credentials read by this process"
    )
    transport.add_argument(
        "--gateway-url", help="read-only observation gateway on loopback (service read-gateway)"
    )
    parser.add_argument(
        "--gateway-auth-file", type=Path, help="authentication token file for --gateway-url"
    )


def depth_bps(value: str) -> int:
    # Mirrors MAX_DEPTH_BPS in connectors/v3_pool_request.py without importing it here.
    if not (value.isascii() and value.isdigit()) or not 0 < int(value) <= 5_000:
        raise argparse.ArgumentTypeError("expected whole basis points from 1 to 5000")
    return int(value)


def decimal_value(value: str) -> Decimal | None:
    try:
        parsed = Decimal(value) if value.isascii() else None
    except InvalidOperation:
        return None
    return parsed if parsed is not None and parsed.is_finite() else None


def borrow_fraction(value: str) -> Decimal:
    parsed = decimal_value(value)
    if parsed is None or not 0 < parsed <= 1:
        raise argparse.ArgumentTypeError("expected a decimal fraction above 0 and at most 1")
    return parsed


def health_floor(value: str) -> Decimal:
    # A floor at or below 1 is the liquidation line itself, not a safety margin.
    parsed = decimal_value(value)
    if parsed is None or not parsed > 1:
        raise argparse.ArgumentTypeError("expected a decimal health factor above 1")
    return parsed


def risk_options(command) -> None:
    command.add_argument(
        "--borrow-fraction",
        type=borrow_fraction,
        action="append",
        help="project the health factor after borrowing this fraction (0 to 1] of one"
        " collateral's value: liquidation_threshold / f; repeatable",
    )
    command.add_argument(
        "--health-floor",
        type=health_floor,
        help="report the largest borrow fraction keeping the health factor at or above this"
        " floor (above 1): min(ltv, liquidation_threshold / floor)",
    )


def block(value: str) -> int | str:
    # Every ax read pins one header first; a number or tag resolves to its
    # concrete number and hash, and each later read names that hash.
    if value in ("finalized", "latest"):
        return value
    if value.isascii() and value.isdigit():
        return int(value)
    raise argparse.ArgumentTypeError(
        f"invalid block {value!r}: expected a decimal block number, 'finalized' or 'latest'"
    )


BLOCK_HELP = (
    "block to pin: a decimal block number, 'finalized' or 'latest'; the output"
    " names the resolved number and hash"
)


def execution_mode(parser, *, required=False):
    modes = parser.add_mutually_exclusive_group(required=required)
    modes.add_argument("--fork", action="store_true")
    modes.add_argument("--production", action="store_true")


def primary(products):
    check = products.add_parser("check", help="validate author inputs and selected-mode readiness")
    author_options(check)
    execution_mode(check)
    operator_context(check, required=False)
    check.add_argument(
        "--online", action="store_true", help="resolve current facts through a read-only gateway"
    )
    check.add_argument("--normalized", action="store_true")
    check.add_argument("--backtest-config", type=Path)
    output_options(check)
    run = products.add_parser("run", help="start a supervised fork or production workspace")
    author_options(run)
    execution_mode(run, required=True)
    operator_context(run, required=True)
    run.add_argument("--output", type=Path, required=True)
    output_options(run)
    for name, help_text in {
        "status": "show retained run state and service availability",
        "pause": "pause the coordinator; positions and services remain",
        "resume": "resume captured execution through its original services",
        "teardown": "request and execute economic closure",
        "shutdown": "stop local services, preserving recorded evidence",
    }.items():
        command = products.add_parser(name, help=help_text)
        command.add_argument("--run", type=Path, required=True)
        output_options(command)
        if name == "shutdown":
            command.add_argument("--force", action="store_true")
    lifecycle_test(products)
    report = products.add_parser("report", help="render recorded evidence without network access")
    report.add_argument("--run", type=Path, required=True)
    report.add_argument("--output", type=Path)
    report.add_argument(
        "--dashboard",
        action="store_true",
        help="print the strategy's dashboard response for the run (a fork test: its last running"
        " state)",
    )
    report.add_argument(
        "--workspace",
        type=Path,
        help="with --dashboard: draw this workspace's current strategy and dashboard.py over the"
        " run's data instead of the source the run captured",
    )
    output_options(report, html=True)
    support = products.add_parser(
        "support", help="show declared connector and chain support and gas lifecycle counts"
    )
    support.add_argument("--connector", help="filter by an installed canonical connector ID")
    support.add_argument("--chain", help="filter by a strict CAIP-2 chain identity")
    listing = support.add_mutually_exclusive_group()
    listing.add_argument("--schema", choices=("strategy", "backtest", "dashboard"))
    listing.add_argument(
        "--references",
        action="store_true",
        help="list the installed read-only reference strategies with their connector, chain"
        " and path, filtered by --connector and --chain",
    )
    output_options(support)


def whole(value: str) -> int:
    if not (value.isascii() and value.isdigit()):
        raise argparse.ArgumentTypeError("expected a non-negative whole number")
    return int(value)


def positive(value: str) -> int:
    if whole(value) == 0:
        raise argparse.ArgumentTypeError("expected a positive whole number")
    return int(value)


def lifecycle_test(products):
    group = products.add_parser("test", help="fork test tooling: operator file and one lifecycle")
    commands = group.add_subparsers(dest="test_command", required=True)
    test = commands.add_parser(
        "run", help="one supervised fork lifecycle: admission, decisions, teardown, verdict"
    )
    test.set_defaults(product="test", fork=True, production=False)
    author_options(test)
    operator_context(test, required=True)
    test.add_argument("--output", type=Path, required=True, help="new fork workspace directory")
    test.add_argument(
        "--decisions", type=positive, default=3, help="most decisions before teardown"
    )
    test.add_argument(
        "--advance",
        type=positive,
        help="seconds of fork time between decisions (default: the decision interval; "
        "a shorter step is raised to reach the next due decision)",
    )
    test.add_argument(
        "--through-exit",
        action="store_true",
        help="keep deciding after the first entry, up to --decisions, so the strategy's own "
        "later intents and exit execute before teardown",
    )
    test.add_argument(
        "--param",
        action="append",
        metavar="KEY=VALUE",
        help="set a strategy.parameters value in the fork workspace's copy of the config "
        "(read as a YAML scalar, on top of strategy.test_parameters); repeatable",
    )
    test.add_argument(
        "--production-parameters",
        action="store_true",
        help="do not apply strategy.test_parameters: fork with the production "
        "strategy.parameters values plus any --param",
    )
    test.add_argument(
        "--idle",
        type=positive,
        default=3,
        help="with --through-exit, consecutive holds after the entry that end the decisions "
        "early (default 3); raise it, or --advance, when the strategy waits longer between steps",
    )
    output_options(test)
    config = commands.add_parser(
        "config", help="generate a fork operator file from the author configuration"
    )
    config.set_defaults(product="test-config")
    author_options(config)
    config.add_argument("--env-file", type=Path, required=True, help="private provider credentials")
    config.add_argument(
        "--block", type=whole, help="anchor block (default: latest; one chain only)"
    )
    config.add_argument("--output", type=Path, help="write the operator file here")
    config.add_argument("--force", action="store_true", help="replace an existing --output")
    output_options(config)


def progress_options(parser):
    parser.add_argument(
        "--progress",
        choices=("auto", "text", "json", "none"),
        default="auto",
        help="terminal or JSON progress on stderr; snapshot file is always written",
    )
    parser.add_argument(
        "--progress-file",
        type=Path,
        help="atomic progress snapshot (default: OUTPUT.progress.json)",
    )
    parser.add_argument(
        "--timing-history", type=Path, help="local completed phase timings for ETA calibration"
    )
    parser.add_argument(
        "--timing-scope", help="comparable worker environment; defaults to this host"
    )


def backtests(products):
    group = products.add_parser(
        "backtest", help="prepare, run and reproduce historical simulations"
    )
    commands = group.add_subparsers(dest="backtest_command", required=True)
    for name in ("run", "prepare", "resume", "reproduce"):
        command = commands.add_parser(name)
        command.set_defaults(product="backtest" if name == "run" else "backtest-" + name)
        if name in ("run", "prepare"):
            author_options(command)
            command.add_argument("--backtest-config", type=Path, required=True)
            command.add_argument(
                "--gateway-config",
                type=Path,
                help="operator provider, account and execution settings",
            )
            command.add_argument("--output", type=Path, required=True)
            command.add_argument("--env-file", type=Path)
            if name == "run":
                command.add_argument("--dataset", type=Path)
        else:
            command.add_argument("--run", type=Path, required=True)
            if name == "resume":
                command.add_argument("--env-file", type=Path)
            else:
                command.set_defaults(offline=True)
        progress_options(command)
        output_options(command)
    recent = commands.add_parser(
        "recent",
        help="rewrite an economic backtest file for the most recent window, with measured gas",
    )
    recent.set_defaults(product="backtest-recent")
    author_options(recent)
    recent.add_argument("--backtest-config", type=Path, required=True)
    recent.add_argument("--env-file", type=Path, required=True)
    recent.add_argument("--days", type=int, default=30)
    recent.add_argument("--end", help="ISO window end; defaults to the latest served hour")
    recent.add_argument("--output", type=Path, required=True)


def services(products):
    group = products.add_parser("service", help="advanced explicit process operation")
    commands = group.add_subparsers(dest="service_command", required=True)
    gateway = commands.add_parser("gateway")
    gateway.set_defaults(product="gateway")
    configuration = gateway.add_mutually_exclusive_group(required=True)
    configuration.add_argument("--config", type=Path)
    configuration.add_argument(
        "--gateway-config", type=Path, help="operator provider, account and execution settings"
    )
    context = gateway.add_mutually_exclusive_group()
    context.add_argument("--read-context", type=Path, help="generated provisional read context")
    context.add_argument(
        "--preparation-file", type=Path, help="generated frozen deployment preparation"
    )
    gateway.add_argument("--database", type=Path)
    mode = gateway.add_mutually_exclusive_group(required=True)
    mode.add_argument("--fork-url")
    mode.add_argument("--fork-routes", type=Path)
    mode.add_argument("--production", action="store_true")
    mode.add_argument("--fork", action="store_true", help="read-only fork origin preparation")
    mode.add_argument("--backtest", action="store_true", help="read-only historical preparation")
    gateway.add_argument("--fork-key-file", type=Path)
    credentials(gateway)
    gateway.add_argument("--password-stdin", action="store_true", help=argparse.SUPPRESS)
    gateway.add_argument("--auth-token-file", type=Path, required=True)
    listen = gateway.add_mutually_exclusive_group(required=True)
    listen.add_argument("--port", type=int)
    listen.add_argument(
        "--listen-fd",
        type=int,
        help="inherited loopback socket, already bound by the supervising process",
    )
    gateway.add_argument("--lease-seconds", type=float, default=30)
    venue = commands.add_parser(
        "fork-venue", help="a connector's synthesized venue beside an identified fork node"
    )
    venue.set_defaults(product="fork-venue")
    venue.add_argument("--spec", type=Path, required=True, help="the workspace's venue spec")
    venue.add_argument(
        "--listen-fd",
        type=int,
        required=True,
        help="inherited loopback socket, already bound by the workspace",
    )
    observation = commands.add_parser(
        "read-gateway", help="read-only observation gateway holding the provider credentials"
    )
    observation.set_defaults(product="read-gateway")
    observation.add_argument(
        "--env-file",
        type=Path,
        required=True,
        help="private provider credentials read only by this gateway",
    )
    observation.add_argument("--auth-token-file", type=Path, required=True)
    observation.add_argument("--port", type=int, required=True)
    for name in ("start", "resume", "status", "teardown"):
        command = commands.add_parser(name)
        command.set_defaults(product=name, run=None)
        command.add_argument("--gateway-url", required=name != "status")
        command.add_argument("--gateway-auth-file", type=Path, required=name != "status")
        if name == "status":
            command.add_argument("--database", type=Path)
        if name in ("start", "resume"):
            command.add_argument("--config", type=Path, required=name == "start")
            command.add_argument(
                "--export", type=Path, help="retain read-only dashboard input snapshots"
            )
            command.add_argument(
                "--clock-offset-file",
                type=Path,
                help="fork test clock: whole seconds added to wall time for decision timing",
            )
            command.add_argument(
                "--decision-facts",
                action="store_true",
                help="fork test run: log what each decision saw for the test verdict",
            )
        if name == "start":
            command.add_argument("--strategy", type=Path, required=True)


def advanced(products):
    from almanak.cli import permissions_arguments

    permissions_arguments(products)
    wallet = products.add_parser("wallet", help="provision encrypted operator credentials")
    create = wallet.add_subparsers(required=True).add_parser("create")
    create.set_defaults(product="wallet-create")
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--account", required=True)
    ax = products.add_parser("ax", help="read-only protocol observations at a pinned block")
    commands = ax.add_subparsers(dest="ax_command", required=True)
    for name, (help_text, connectors) in AX_REQUESTS.items():
        command = commands.add_parser(name, help=help_text, description=help_text)
        command.set_defaults(product="ax-" + name)
        command.add_argument(
            "--connector", required=True, choices=connectors, help="canonical connector ID"
        )
        if name in INLINE_SOURCES:
            INLINE_SOURCES[name](command)
        elif name in LENDING_REQUESTS:
            lending_source(command)
        else:
            command.add_argument(
                "--config", type=Path, required=True, help="JSON request file for the connector"
            )
        block_option(command)
        observation_transport(command)
        if name in ("lending-position", "perp-position"):
            command.add_argument("--account", required=True, help="EVM account address")
        if name == "lending-reserve":
            risk_options(command)
        if name == "pool":
            command.add_argument(
                "--depth-bps",
                type=depth_bps,
                help="also report pool output within this price move, 1 to 5000 basis points",
            )
    balance = commands.add_parser(
        "balance", help="native or ERC-20 balance", description="native or ERC-20 balance"
    )
    balance.set_defaults(product="ax-balance")
    balance.add_argument("--asset", required=True, help="CAIP-19 asset type")
    balance.add_argument(
        "--account",
        required=True,
        action="append",
        help="EVM account address; repeat to read several accounts at the same block",
    )
    block_option(balance)
    observation_transport(balance)
    discovery(commands)
    hypercore(commands)


def block_option(command):
    command.add_argument("--block", type=block, default="finalized", help=BLOCK_HELP)


def discovery(commands):
    resolve = commands.add_parser(
        "resolve",
        help="ERC-20 token candidates for a symbol or address, verified at a pinned block",
        description=(
            "CoinGecko on-chain names candidates; each is verified by reading its decimals and"
            " symbol on chain. A symbol is never identity: state the chosen CAIP-19 asset."
        ),
    )
    resolve.set_defaults(product="ax-resolve")
    resolve.add_argument("--chain", required=True, help="strict CAIP-2 chain")
    resolve.add_argument("--query", required=True, help="token symbol or contract address")
    resolve.add_argument(
        "--limit", type=bounded(1, 30), default=10, help="candidates to report (1 to 30)"
    )
    pools = commands.add_parser(
        "dex-pools",
        help="pools that trade a token, verified through each connector's resolver",
        description=(
            "CoinGecko on-chain lists the token's pools with its liquidity and volume; each"
            " pool reported is resolved by its connector, which refuses unreviewed deployments."
            " A hooked Uniswap V4 pool is verified only with a reviewed hook profile whose code"
            " matches its pin, and reports the profile and terms. A Uniswap V4 pool holding the"
            " native coin is held as its wrapped token (WETH, WPOL, WBNB, WAVAX) for trading and"
            " liquidity, so a wrapped-native --token or --quote, or the native coin, which asks"
            " for its wrapped token, matches both wrapped and native-coin pools. A native pool's"
            " market names the native coin (slip44) as its currency: keep it native, since the"
            " wrapped-token key is a different pool."
        ),
    )
    pools.set_defaults(product="ax-dex-pools")
    pools.add_argument("--chain", required=True, help="strict CAIP-2 chain")
    pools.add_argument(
        "--token", required=True, help="CAIP-19 ERC-20 asset or native coin the pools trade"
    )
    pools.add_argument(
        "--quote",
        help="CAIP-19 ERC-20 asset or native coin; only pools pairing --token with it",
    )
    # A filter, not a request connector: the handler table maps --connector requests.
    pools.add_argument(
        "--connector",
        dest="connector_filter",
        choices=DISCOVERY_CONNECTORS,
        help="only pools of this connector",
    )
    # Every reported pool is resolved on chain, so --limit also bounds the reads per call.
    pools.add_argument("--limit", type=bounded(1, 20), default=10, help="pools to report (1 to 20)")
    pools.add_argument(
        "--offset", type=bounded(0, 90), default=0, help="matching pools to skip, for paging"
    )
    pools.add_argument(
        "--depth-bps",
        type=depth_bps,
        help="also report each verified V3-style pool's depth within this price move",
    )
    markets = commands.add_parser(
        "lending-markets",
        help="a lending protocol's markets on a chain, verified through its connector's resolver",
        description=(
            "The protocol's own records name the markets: an Aave V3 or SparkLend Pool's reserve"
            " list, the reserves of the Spokes an Aave V4 Hub serves, the Comets a Compound V3"
            " Configurator deployed, the markets a Morpho Blue core created. Each reported"
            " market is resolved as lending-reserve resolves it. Morpho markets are listed for"
            " one loan asset, most supplied first; where Morpho's API indexes the chain, only"
            " the markets Morpho lists, unless --include-unlisted. Each verified market reports"
            " its supply and borrow APR and APY and available liquidity as lending-reserve does."
        ),
    )
    markets.set_defaults(product="ax-lending-markets")
    markets.add_argument("--connector", required=True, choices=LISTED_LENDING_CONNECTORS)
    markets.add_argument("--chain", required=True, help="strict CAIP-2 chain")
    markets.add_argument(
        "--asset",
        help=(
            "CAIP-19 ERC-20 asset: an Aave or SparkLend reserve asset, a Compound base or"
            " collateral asset, the Morpho loan asset (required for morpho_blue)"
        ),
    )
    markets.add_argument(
        "--collateral", help="CAIP-19 ERC-20 asset; only Compound or Morpho markets accepting it"
    )
    # Every reported market is resolved on chain, so --limit also bounds the reads per call.
    markets.add_argument(
        "--limit", type=bounded(1, 20), default=10, help="markets to report (1 to 20)"
    )
    markets.add_argument(
        "--include-unlisted",
        action="store_true",
        help=(
            "morpho_blue: also list markets Morpho's API does not mark listed;"
            " they keep its warnings"
        ),
    )
    markets.add_argument(
        "--offset", type=bounded(0, 500), default=0, help="matching markets to skip, for paging"
    )
    for command in (resolve, pools, markets):
        block_option(command)
        observation_transport(command)


def hypercore(commands):
    # HyperCore answers at read time: these commands take no --block.
    markets = commands.add_parser(
        "hypercore-markets",
        help="HyperCore perpetual markets from Hyperliquid's meta, with their asset index",
        description=(
            "Hyperliquid's /info API names each perpetual market of the first dex with its"
            " asset index, size decimals, maximum leverage and margin tiers, and its mark,"
            " oracle and mid prices, funding and open interest as of the moment it answers;"
            " HyperCore has no block to pin. Without --query, tradable markets are listed by"
            " 24-hour notional volume, the venue's own figure."
        ),
    )
    markets.set_defaults(product="ax-hypercore-markets")
    markets.add_argument("--query", help="report one market: its coin as meta names it, e.g. BTC")
    markets.add_argument(
        "--limit", type=bounded(1, 50), default=20, help="markets to report (1 to 50)"
    )
    markets.add_argument(
        "--offset", type=bounded(0, 500), default=0, help="listed markets to skip, for paging"
    )
    markets.add_argument(
        "--include-delisted", action="store_true", help="also list delisted markets"
    )
    account = commands.add_parser(
        "hypercore-account",
        help="a HyperCore account: its role, abstraction mode, perp margin and spot balances",
        description=(
            "Hyperliquid's /info API reports the account's role (missing until HyperCore"
            " first credits it), its account abstraction mode, its perp margin and positions"
            " and its spot balances, each identified by its linked HyperEVM asset, as of the"
            " moment it answers. Under a unified account or portfolio margin the perp state"
            " is not meaningful and spot holds every balance."
        ),
    )
    account.set_defaults(product="ax-hypercore-account")
    account.add_argument(
        "--account",
        required=True,
        action="append",
        help="EVM address of the account; repeat to read several",
    )
    for command in (markets, account):
        observation_transport(command)


def bounded(low: int, high: int):
    def parse(value: str) -> int:
        if not (value.isascii() and value.isdigit()) or not low <= int(value) <= high:
            raise argparse.ArgumentTypeError(f"expected a whole number from {low} to {high}")
        return int(value)

    return parse


# Help text and --connector choices per ax request subcommand; the choices must
# cover exactly the (product, connector) pairs of ax.CONNECTOR_REQUESTS.
# Lending choices equal lending_lookup.LENDING_CONNECTORS; tests assert they match.
LENDING = ("aave_v3", "sparklend", "aave_v4", "compound_v3", "morpho_blue")
LENDING_REQUESTS = ("lending-reserve", "lending-position")
LISTED_LENDING_CONNECTORS = tuple(lending_discovery())
DISCOVERY_CONNECTORS = tuple(pool_discovery())
AX_REQUESTS = {
    "pool-price": ("Uniswap V4 pool spot price", pool_connectors("keyed")),
    "lending-reserve": (
        "lending reserve state, its supply and borrow APR and APY, its collateral risk"
        " parameters, its price sources and a config-ready market entry",
        LENDING,
    ),
    "lending-position": ("account position in one lending reserve", LENDING),
    "perp-position": ("GMX V2 account position", tuple(perp_discovery())),
    "perp-markets": ("GMX V2 market lookup", tuple(perp_discovery())),
    "pool": (
        "V3-style pool state and optional liquidity depth, or an Aerodrome or Curve pool's"
        " resolution",
        pool_connectors("concentrated", "resolved"),
    ),
}


def perp_market_source(command):
    source = command.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--config", type=Path, help='JSON request file: {"chain": "<CAIP-2>", "query": "..."}'
    )
    source.add_argument("--chain", help="strict CAIP-2 chain, given with --query")
    command.add_argument("--query", help='GM share address or a label such as "BTC/USD"')


def pool_price_source(command):
    # The inline key names a pool on the reviewed deployment for the chain; the
    # connector admits a hooked key only with a reviewed hook profile.
    source = command.add_mutually_exclusive_group(required=True)
    source.add_argument("--config", type=Path, help="JSON request file with the full pool source")
    source.add_argument("--chain", help="strict CAIP-2 chain, given with the pool key flags")
    command.add_argument("--currency0", help="CAIP-19 currency0 of the pool key")
    command.add_argument("--currency1", help="CAIP-19 currency1 of the pool key")
    command.add_argument("--fee", type=int, help="pool fee in hundredths of a basis point")
    command.add_argument("--tick-spacing", type=int, help="pool tick spacing")
    command.add_argument(
        "--hooks",
        help="reviewed hook address of the pool key; omit for a hookless pool"
        " (a dynamic-fee hook takes --fee 8388608)",
    )
    command.add_argument("--base", help="CAIP-19 currency to price; defaults to currency0")


def pool_source(command):
    # One request, named either inline or by file; both validate as PoolRequest.
    source = command.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--config", type=Path, help='JSON request file: {"chain": "<CAIP-2>", "pool": "0x..."}'
    )
    source.add_argument("--chain", help="strict CAIP-2 chain of the pool, given with --pool")
    command.add_argument("--pool", help="pool contract address, given with --chain")


INLINE_SOURCES = {
    "pool": pool_source,
    "perp-markets": perp_market_source,
    "pool-price": pool_price_source,
}


# Inline selector flags per lending connector, in the author `markets:` field names.
LENDING_SELECTOR_FLAGS = (
    "asset",
    "pool",
    "spoke",
    "reserve_id",
    "comet",
    "market_id",
    "leg",
    "core",
)


def lending_source(command):
    # One market selection, named either inline or by file; both validate as the
    # connector's own MarketSelection, the same model the author loader uses.
    source = command.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--config", type=Path, help="JSON market selection, as written under `markets:`"
    )
    source.add_argument("--chain", help="strict CAIP-2 chain, given with the selector flags")
    command.add_argument("--asset", help="CAIP-19 reserve asset (aave_v3, sparklend, compound_v3)")
    command.add_argument("--pool", help="Aave V3 or SparkLend Pool; omit for the reviewed Pool")
    command.add_argument("--spoke", help="Aave V4 Spoke address")
    command.add_argument("--reserve-id", type=int, help="Aave V4 reserve ID on the Spoke")
    command.add_argument("--comet", help="Compound V3 Comet address")
    command.add_argument("--market-id", help="Morpho Blue market ID")
    command.add_argument("--leg", choices=("loan", "collateral"), help="Morpho Blue market side")
    command.add_argument("--core", help="Morpho Blue core; omit for the reviewed deployment")


# Inline request flags per ax subcommand, exclusive with --config.
INLINE_FLAGS = {
    "ax-pool": ("chain", "pool"),
    "ax-perp-markets": ("chain", "query"),
    "ax-pool-price": ("chain", "currency0", "currency1", "fee", "tick_spacing"),
}
# Optional inline flags: never required, but refused with --config like the rest.
INLINE_OPTIONS = {"ax-pool-price": ("base", "hooks")}
# The session environment that names the read gateway.
GATEWAY_ENV = ("ALMANAK_AX_GATEWAY_URL", "ALMANAK_AX_GATEWAY_AUTH_FILE")


def parse_validated(parser, argv=None):
    # argparse cannot tie one option to another; refuse a half pair here so a
    # wrong invocation fails before any transport opens.
    args = parser.parse_args(argv)
    if (
        str(getattr(args, "product", "")).startswith("ax-")
        or getattr(args, "product", None) == "markets"
    ):
        session_transport(parser, args)
        require_pair(parser, args, "gateway_url", "gateway_auth_file")
    product = getattr(args, "product", None)
    if product in INLINE_FLAGS:
        require_inline(parser, args, INLINE_FLAGS[product], INLINE_OPTIONS.get(product, ()))
    if getattr(args, "ax_command", None) in LENDING_REQUESTS and args.config is not None:
        given = [name for name in LENDING_SELECTOR_FLAGS if getattr(args, name) is not None]
        if given:
            flags = ", ".join("--" + name.replace("_", "-") for name in given)
            parser.error(f"{flags} name an inline request; they cannot be combined with --config")
    return args


def session_transport(parser: argparse.ArgumentParser, args) -> None:
    """Take the read gateway from the session when no transport flag is given.

    The flags always win, so a command line stays explicit about its transport.
    """
    if any(
        value is not None for value in (args.env_file, args.gateway_url, args.gateway_auth_file)
    ):
        return
    url, token = (os.environ.get(name) for name in GATEWAY_ENV)
    if not url or not token:
        parser.error(
            "give --env-file, or --gateway-url with --gateway-auth-file "
            "(or set ALMANAK_AX_GATEWAY_URL and ALMANAK_AX_GATEWAY_AUTH_FILE)"
        )
    args.gateway_url, args.gateway_auth_file = url, Path(token)


def require_inline(parser, args, names: tuple[str, ...], options: tuple[str, ...] = ()) -> None:
    given = [name for name in names[1:] if getattr(args, name) is not None]
    if args.config is not None:
        given += [name for name in options if getattr(args, name) is not None]
        if given:
            flags = ", ".join("--" + name.replace("_", "-") for name in given)
            parser.error(f"{flags} name an inline request; they cannot be combined with --config")
        return
    if len(given) != len(names) - 1:
        flags = " and ".join("--" + name.replace("_", "-") for name in names)
        parser.error(f"{flags} must be given together")


def require_pair(parser, args, first: str, second: str) -> None:
    if (getattr(args, first) is None) != (getattr(args, second) is None):
        flags = " and ".join("--" + name.replace("_", "-") for name in (first, second))
        parser.error(f"{flags} must be given together")


def markets(products):
    parser = products.add_parser(
        "markets",
        help="what each market in config.yaml resolves to: its contracts, tokens and ids, with"
        " each token's symbol and decimals, for checking before approval (no prices read)",
    )
    parser.set_defaults(product="markets")
    parser.add_argument("--config", type=Path, default=Path("config.yaml"))
    parser.add_argument("--out", type=Path, default=Path(".almanak/markets.json"))
    parser.add_argument("--block", type=block, default="latest", help=BLOCK_HELP)
    observation_transport(parser)


def build_parser():
    parser = argparse.ArgumentParser(
        prog="almanak",
        description=(
            "Define, check, run and explain a strategy. A bare almanak, and any "
            "command this CLI does not define, run the bundled Almanak Code "
            "assistant when the installed wheel carries a binary for this platform."
        ),
    )
    products = parser.add_subparsers(dest="product", required=True)
    primary(products)
    backtests(products)
    markets(products)
    services(products)
    advanced(products)
    return parser


def product_names(parser) -> frozenset[str]:
    """Top-level command names the parser accepts.

    Read from the built parser so the native fallback cannot disagree with it
    about which names belong to this CLI.
    """
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return frozenset(action.choices or ())
    raise RuntimeError("the CLI parser defines no products")
