import argparse
import os
from decimal import Decimal

import pytest

from almanak import native
from almanak.cli_arguments import build_parser, parse_validated, product_names

ADDRESS = "0x" + "11" * 20
ASSET = "eip155:1/erc20:0x" + "aa" * 20


class Exec(Exception):
    """Stands in for a process replacement so a test can inspect the argv."""


@pytest.fixture
def execs(monkeypatch):
    calls: list[list[str]] = []

    def record(path, argv, environ=None):
        calls.append(list(argv))
        raise Exec

    monkeypatch.setattr(os, "execve", record)
    monkeypatch.setattr(os, "execv", record)
    monkeypatch.delenv(native.ACTIVE, raising=False)
    return calls


@pytest.fixture
def installed(monkeypatch, tmp_path):
    binary = tmp_path / "almanak-code"
    binary.write_bytes(b"#!/bin/sh\n")
    binary.chmod(0o644)
    monkeypatch.setattr(native, "binary_path", lambda: binary)
    return binary


def parse(argv):
    return parse_validated(build_parser(), argv)


CONNECTORS = {
    "pool-price": "uniswap_v4",
    "lending-reserve": "aave_v4",
    "lending-position": "aave_v4",
    "perp-position": "gmx_v2",
    "perp-markets": "gmx_v2",
    "pool": "uniswap_v3",
}


def config_args(tmp_path, subcommand, *extra):
    return [
        "--connector",
        CONNECTORS[subcommand],
        "--config",
        str(tmp_path / "source.json"),
        "--block",
        "finalized",
        "--env-file",
        str(tmp_path / "operator.env"),
        *extra,
    ]


@pytest.mark.parametrize(
    ("subcommand", "extra"),
    [
        ("pool-price", []),
        ("lending-reserve", []),
        ("lending-position", ["--account", ADDRESS]),
        ("perp-position", ["--account", ADDRESS]),
        ("perp-markets", []),
        ("pool", []),
    ],
)
def test_ax_config_subcommands_parse(tmp_path, subcommand, extra):
    args = parse(["ax", subcommand, *config_args(tmp_path, subcommand, *extra)])
    assert args.product == "ax-" + subcommand
    assert args.ax_command == subcommand
    assert args.connector == CONNECTORS[subcommand]


def test_ax_config_subcommands_require_connector(tmp_path):
    for subcommand in CONNECTORS:
        extra = (
            ["--account", ADDRESS] if subcommand in ("lending-position", "perp-position") else []
        )
        argv = ["ax", subcommand, *config_args(tmp_path, subcommand, *extra)]
        argv.remove("--connector")
        argv.remove(CONNECTORS[subcommand])
        with pytest.raises(SystemExit):
            parse(argv)


def test_parser_connector_choices_match_the_handler_table():
    from almanak.ax import CONNECTOR_REQUESTS
    from almanak.ax_lending_markets import LISTINGS
    from almanak.connectors.direct import connectors_for
    from almanak.connectors.lending_lookup import LENDING_CONNECTORS

    parser = build_parser()
    choices = set()
    for action in parser._subparsers._group_actions:  # noqa: SLF001
        if not isinstance(action, argparse._SubParsersAction):  # noqa: SLF001
            continue
        ax_parser = action.choices.get("ax")
        if ax_parser is None:
            continue
        for sub in ax_parser._subparsers._group_actions:  # noqa: SLF001
            if not isinstance(sub, argparse._SubParsersAction):  # noqa: SLF001
                continue
            for sub_parser in sub.choices.values():
                product = sub_parser.get_default("product")
                # `ax supply` and `ax withdraw` are operator actions, not read handlers.
                if not product.startswith("ax-"):
                    continue
                for argument in sub_parser._actions:
                    if argument.dest == "connector":
                        choices.update((product, connector) for connector in argument.choices)
    lending = {
        (product, connector)
        for product in ("ax-lending-reserve", "ax-lending-position")
        for connector in LENDING_CONNECTORS
    }
    listed = {("ax-lending-markets", connector) for connector in LISTINGS}
    # The status of a bridge transfer is read for the connectors that bridge.
    bridges = {("ax-bridge-status", connector) for connector in connectors_for("bridge")}
    assert choices == set(CONNECTOR_REQUESTS) | lending | listed | bridges


def test_lending_requests_accept_inline_selectors(tmp_path):
    transport = ["--block", "finalized", "--env-file", str(tmp_path / "operator.env")]
    args = parse(
        ["ax", "lending-reserve", "--connector", "aave_v3", "--chain", "eip155:1"]
        + ["--asset", ASSET, *transport]
    )
    assert (args.chain, args.asset, args.config) == ("eip155:1", ASSET, None)
    args = parse(
        ["ax", "lending-position", "--connector", "aave_v4", "--chain", "eip155:1"]
        + ["--spoke", ADDRESS, "--reserve-id", "7", "--account", ADDRESS, *transport]
    )
    assert (args.spoke, args.reserve_id) == (ADDRESS, 7)
    with pytest.raises(SystemExit):
        parse(["ax", "lending-reserve", "--connector", "morpho_blue", "--leg", "both", *transport])


def test_lending_config_refuses_inline_selectors(tmp_path, capsys):
    argv = ["ax", "lending-reserve", *config_args(tmp_path, "lending-reserve")]
    with pytest.raises(SystemExit):
        parse([*argv, "--reserve-id", "7"])
    assert "--reserve-id name an inline request" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        parse([*argv, "--chain", "eip155:1"])


def test_lending_reserve_takes_repeated_borrow_fractions_and_a_health_floor(tmp_path, capsys):
    argv = ["ax", "lending-reserve", *config_args(tmp_path, "lending-reserve")]
    args = parse(
        [*argv, "--borrow-fraction", "0.3", "--borrow-fraction", "1", "--health-floor", "1.5"]
    )
    assert args.borrow_fraction == [Decimal("0.3"), Decimal("1")]
    assert args.health_floor == Decimal("1.5")
    plain = parse(argv)
    assert (plain.borrow_fraction, plain.health_floor) == (None, None)
    for flag, value in (
        ("--borrow-fraction", "0"),
        ("--borrow-fraction", "1.01"),
        ("--borrow-fraction", "-0.2"),
        ("--borrow-fraction", "NaN"),
        ("--borrow-fraction", "half"),
        ("--health-floor", "1"),
        ("--health-floor", "0.9"),
        ("--health-floor", "Infinity"),
    ):
        with pytest.raises(SystemExit):
            parse([*argv, flag, value])
    assert "expected a decimal health factor above 1" in capsys.readouterr().err
    position = ["ax", "lending-position", *config_args(tmp_path, "lending-position")]
    with pytest.raises(SystemExit):
        parse([*position, "--account", ADDRESS, "--health-floor", "1.5"])


def test_ax_balance_parses(tmp_path):
    args = parse(
        [
            "ax",
            "balance",
            "--asset",
            ASSET,
            "--account",
            ADDRESS,
            "--block",
            "25908472",
            "--env-file",
            str(tmp_path / "operator.env"),
        ]
    )
    assert args.product == "ax-balance"
    assert args.asset == ASSET
    assert args.account == [ADDRESS]


def test_ax_reads_accept_a_gateway_instead_of_an_env_file(tmp_path):
    argv = ["ax", "perp-markets", *config_args(tmp_path, "perp-markets")]
    argv[argv.index("--env-file")] = "--gateway-url"
    argv[argv.index(str(tmp_path / "operator.env"))] = "http://127.0.0.1:8300"
    args = parse([*argv, "--gateway-auth-file", str(tmp_path / "token")])
    assert args.env_file is None
    assert args.gateway_url == "http://127.0.0.1:8300"
    assert args.gateway_auth_file == tmp_path / "token"
    with pytest.raises(SystemExit):
        parse([*argv, "--env-file", str(tmp_path / "operator.env")])
    with pytest.raises(SystemExit):
        parse(["ax", "perp-markets", *config_args(tmp_path, "perp-markets")][:-2])
    # A half pair is refused at parse time, before any transport opens.
    with pytest.raises(SystemExit):
        parse(argv)
    with pytest.raises(SystemExit):
        parse(
            [
                "ax",
                "perp-markets",
                *config_args(tmp_path, "perp-markets"),
                "--gateway-auth-file",
                str(tmp_path / "token"),
            ]
        )


def test_read_gateway_service_parses(tmp_path):
    args = parse(
        [
            "service",
            "read-gateway",
            "--env-file",
            str(tmp_path / "operator.env"),
            "--auth-token-file",
            str(tmp_path / "token"),
            "--port",
            "8300",
        ]
    )
    assert args.product == "read-gateway"
    assert (args.env_file, args.auth_token_file, args.port) == (
        tmp_path / "operator.env",
        tmp_path / "token",
        8300,
    )
    with pytest.raises(SystemExit):
        parse(["service", "read-gateway", "--env-file", str(tmp_path / "operator.env")])


@pytest.mark.parametrize("connector", ["uniswap_v3", "pancakeswap_v3", "aerodrome_slipstream"])
def test_ax_pool_takes_each_v3_family_and_an_optional_depth_band(tmp_path, connector):
    argv = ["ax", "pool", *config_args(tmp_path, "pool")]
    argv[argv.index("uniswap_v3")] = connector
    args = parse(argv)
    assert (args.product, args.connector, args.depth_bps) == ("ax-pool", connector, None)
    assert parse([*argv, "--depth-bps", "200"]).depth_bps == 200
    for invalid in ("0", "5001", "-5", "1.5", "two"):
        with pytest.raises(SystemExit):
            parse([*argv, "--depth-bps", invalid])


def test_ax_pool_refuses_other_connectors_and_transport_flags_before_it(tmp_path):
    argv = ["ax", "pool", *config_args(tmp_path, "pool")]
    argv[argv.index("uniswap_v3")] = "uniswap_v4"
    with pytest.raises(SystemExit):
        parse(argv)
    # Transport flags belong to the subcommand, not to `ax` itself.
    with pytest.raises(SystemExit):
        parse(["ax", "--env-file", str(tmp_path / "operator.env"), "pool"])


def test_depth_band_bound_matches_the_reader():
    from almanak.cli_arguments import depth_bps
    from almanak.connectors.v3_pool_request import MAX_DEPTH_BPS

    assert depth_bps(str(MAX_DEPTH_BPS)) == MAX_DEPTH_BPS
    with pytest.raises(argparse.ArgumentTypeError):
        depth_bps(str(MAX_DEPTH_BPS + 1))
    # Unicode digits pass str.isdigit(); Arabic-Indic 200 would otherwise parse.
    for unicode_digits in ("\u0662\u0660\u0660", "\u00b2"):
        with pytest.raises(argparse.ArgumentTypeError, match="whole basis points"):
            depth_bps(unicode_digits)


@pytest.mark.parametrize("subcommand", ["pool-price", "lending-reserve", "perp-markets", "pool"])
def test_ax_without_positions_rejects_account(tmp_path, subcommand):
    with pytest.raises(SystemExit):
        parse(["ax", subcommand, *config_args(tmp_path, subcommand, "--account", ADDRESS)])


@pytest.mark.parametrize("subcommand", ["lending-position", "perp-position"])
def test_ax_position_subcommands_require_account(tmp_path, subcommand):
    with pytest.raises(SystemExit):
        parse(["ax", subcommand, *config_args(tmp_path, subcommand)])


def test_the_old_read_group_forwards_to_the_native_binary(installed, execs):
    known = product_names(build_parser())
    assert "read" not in known
    with pytest.raises(Exec):
        native.forward_unrecognized(["read", "pool-price"], known)
    assert execs == [[str(installed), "read", "pool-price"]]


def test_unknown_commands_still_forward(installed, execs):
    known = product_names(build_parser())
    with pytest.raises(Exec):
        native.forward_unrecognized(["code", "-p", "hi"], known)
    assert execs == [[str(installed), "code", "-p", "hi"]]


def every_ax_invocation(tmp_path, block):
    for subcommand in CONNECTORS:
        extra = (
            ["--account", ADDRESS] if subcommand in ("lending-position", "perp-position") else []
        )
        argv = ["ax", subcommand, *config_args(tmp_path, subcommand, *extra)]
        argv[argv.index("finalized")] = block
        yield argv
    yield [
        "ax",
        "balance",
        "--asset",
        ASSET,
        "--account",
        ADDRESS,
        "--block",
        block,
        "--env-file",
        str(tmp_path / "operator.env"),
    ]


@pytest.mark.parametrize(
    ("value", "selector"),
    [("finalized", "finalized"), ("latest", "latest"), ("0", 0), ("25908472", 25908472)],
)
def test_every_ax_block_accepts_a_number_finalized_or_latest(tmp_path, value, selector):
    for argv in every_ax_invocation(tmp_path, value):
        assert parse(argv).block == selector


@pytest.mark.parametrize("value", ["Latest", "safe", "pending", "-1", "0x10", "1.5", "٢", ""])
def test_every_ax_block_refuses_other_forms_naming_the_accepted_ones(tmp_path, value, capsys):
    for argv in every_ax_invocation(tmp_path, value):
        with pytest.raises(SystemExit) as refused:
            parse(argv)
        assert refused.value.code == 2
        assert "expected a decimal block number, 'finalized' or 'latest'" in (
            capsys.readouterr().err
        )


def test_ax_help_names_the_block_forms(capsys):
    for subcommand in [*CONNECTORS, "balance"]:
        with pytest.raises(SystemExit):
            parse(["ax", subcommand, "--help"])
        assert "'finalized' or 'latest'" in " ".join(capsys.readouterr().out.split())


POOL = "0x69bfaf19c9f377bb306a89aed9f6b07e2c1a8d9a"


def pool_args(tmp_path, *source):
    return [
        "ax",
        "pool",
        "--connector",
        "uniswap_v3",
        *source,
        "--block",
        "latest",
        "--env-file",
        str(tmp_path / "operator.env"),
    ]


def test_ax_pool_names_its_chain_and_pool_inline(tmp_path):
    args = parse(pool_args(tmp_path, "--chain", "eip155:4663", "--pool", POOL))
    assert (args.chain, args.pool, args.config) == ("eip155:4663", POOL, None)
    by_file = parse(pool_args(tmp_path, "--config", str(tmp_path / "pool.json")))
    assert (by_file.chain, by_file.pool) == (None, None)


REQUIRED = "one of the arguments --config --chain is required"
PAIRED = "--chain and --pool must be given together"


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ([], REQUIRED),
        (["--pool", POOL], REQUIRED),
        (["--chain", "eip155:4663"], PAIRED),
        (["--config", "pool.json", "--pool", POOL], "cannot be combined with --config"),
        (["--config", "pool.json", "--chain", "eip155:4663", "--pool", POOL], "not allowed with"),
    ],
)
def test_ax_pool_takes_exactly_one_request_form(tmp_path, source, message, capsys):
    with pytest.raises(SystemExit) as refused:
        parse(pool_args(tmp_path, *source))
    assert refused.value.code == 2
    assert message in capsys.readouterr().err


def test_inline_pool_flags_belong_to_ax_pool_only(tmp_path):
    argv = ["ax", "perp-markets", *config_args(tmp_path, "perp-markets")]
    with pytest.raises(SystemExit):
        parse([*argv, "--chain", "eip155:42161"])
    with pytest.raises(SystemExit):
        parse([*argv, "--pool", POOL])


GATEWAY = ["--gateway-url", "http://127.0.0.1:8300", "--gateway-auth-file", "/tmp/token"]


def test_ax_takes_the_read_gateway_from_the_session(monkeypatch, tmp_path):
    monkeypatch.setenv("ALMANAK_AX_GATEWAY_URL", "http://127.0.0.1:8400")
    monkeypatch.setenv("ALMANAK_AX_GATEWAY_AUTH_FILE", str(tmp_path / "session.token"))
    args = parse(["ax", "balance", "--asset", ASSET, "--account", ADDRESS])
    assert (args.gateway_url, args.gateway_auth_file) == (
        "http://127.0.0.1:8400",
        tmp_path / "session.token",
    )
    # --block defaults to the finalized header.
    assert args.block == "finalized"
    # Flags always win over the session.
    flagged = parse(["ax", "balance", "--asset", ASSET, "--account", ADDRESS, *GATEWAY])
    assert flagged.gateway_url == "http://127.0.0.1:8300"
    local = parse(["ax", "balance", "--asset", ASSET, "--account", ADDRESS, "--env-file", "op.env"])
    assert (local.gateway_url, local.gateway_auth_file) == (None, None)
    # An explicit auth file is never paired with the session URL.
    with pytest.raises(SystemExit):
        parse(["ax", "balance", "--asset", ASSET, "--account", ADDRESS, "--gateway-auth-file", "t"])


def test_ax_without_any_transport_names_every_way_to_give_one(monkeypatch, capsys):
    monkeypatch.delenv("ALMANAK_AX_GATEWAY_URL", raising=False)
    monkeypatch.setenv("ALMANAK_AX_GATEWAY_AUTH_FILE", "/tmp/token")
    with pytest.raises(SystemExit):
        parse(["ax", "balance", "--asset", ASSET, "--account", ADDRESS])
    assert "ALMANAK_AX_GATEWAY_URL" in capsys.readouterr().err


def test_perp_markets_and_pool_price_name_their_request_inline(capsys):
    markets = parse(
        ["ax", "perp-markets", "--connector", "gmx_v2", "--chain", "eip155:42161"]
        + ["--query", "BTC/USD", *GATEWAY]
    )
    assert (markets.chain, markets.query, markets.config) == ("eip155:42161", "BTC/USD", None)
    key = ["--currency0", ASSET, "--currency1", ASSET, "--fee", "500", "--tick-spacing", "10"]
    price = parse(
        ["ax", "pool-price", "--connector", "uniswap_v4", "--chain", "eip155:1", *key, *GATEWAY]
    )
    assert (price.fee, price.tick_spacing, price.base, price.hooks) == (500, 10, None, None)
    hooked = parse(
        ["ax", "pool-price", "--connector", "uniswap_v4", "--chain", "eip155:1", *key]
        + ["--hooks", ADDRESS, *GATEWAY]
    )
    assert hooked.hooks == ADDRESS
    with pytest.raises(SystemExit):
        parse(["ax", "pool-price", "--connector", "uniswap_v4", "--chain", "eip155:1", *GATEWAY])
    assert "must be given together" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        parse(
            ["ax", "perp-markets", "--connector", "gmx_v2", "--config", "r.json"]
            + ["--query", "BTC/USD", *GATEWAY]
        )
    assert "--query name an inline request" in capsys.readouterr().err


def test_discovery_commands_parse_with_bounded_counts():
    resolved = parse(["ax", "resolve", "--chain", "eip155:8453", "--query", "cbBTC", *GATEWAY])
    assert (resolved.product, resolved.limit) == ("ax-resolve", 10)
    pools = parse(
        ["ax", "dex-pools", "--chain", "eip155:8453", "--token", ASSET, "--connector", "curve"]
        + ["--limit", "20", "--depth-bps", "200", *GATEWAY]
    )
    assert (pools.connector_filter, pools.limit, pools.depth_bps) == ("curve", 20, 200)
    default = parse(["ax", "dex-pools", "--chain", "eip155:8453", "--token", ASSET, *GATEWAY])
    assert (default.limit, default.offset) == (10, 0)
    paged = parse(
        ["ax", "dex-pools", "--chain", "eip155:8453", "--token", ASSET, "--offset", "90", *GATEWAY]
    )
    assert paged.offset == 90
    for flag, value in (
        ("--limit", "21"),
        ("--limit", "0"),
        ("--limit", "x"),
        ("--offset", "١"),
        ("--offset", "91"),
    ):
        with pytest.raises(SystemExit):
            parse(["ax", "dex-pools", "--chain", "eip155:8453", "--token", ASSET, flag, value])


def test_lending_markets_parse_with_bounded_counts():
    markets = ["ax", "lending-markets", "--connector", "morpho_blue", "--chain", "eip155:8453"]
    default = parse([*markets, "--asset", ASSET, *GATEWAY])
    assert (default.product, default.limit, default.offset) == ("ax-lending-markets", 10, 0)
    assert (default.asset, default.collateral, default.block) == (ASSET, None, "finalized")
    assert default.include_unlisted is False
    assert parse([*markets, "--include-unlisted", *GATEWAY]).include_unlisted is True
    paged = parse([*markets, "--collateral", ASSET, "--limit", "20", "--offset", "500", *GATEWAY])
    assert (paged.collateral, paged.limit, paged.offset) == (ASSET, 20, 500)
    for flag, value in (("--limit", "21"), ("--limit", "0"), ("--offset", "501")):
        with pytest.raises(SystemExit):
            parse([*markets, flag, value, *GATEWAY])
    with pytest.raises(SystemExit):
        parse(["ax", "lending-markets", "--connector", "aave_v4", "--chain", "eip155:1"])


def test_pool_price_base_is_refused_with_a_config_file(tmp_path, capsys):
    # The file names its own base; a --base beside it would be silently ignored.
    argv = ["ax", "pool-price", *config_args(tmp_path, "pool-price"), "--base", "x"]
    with pytest.raises(SystemExit) as refused:
        parse(argv)
    assert refused.value.code == 2
    assert "--base name an inline request" in capsys.readouterr().err
    hooked = ["ax", "pool-price", *config_args(tmp_path, "pool-price"), "--hooks", ADDRESS]
    with pytest.raises(SystemExit) as refused:
        parse(hooked)
    assert refused.value.code == 2
    assert "--hooks name an inline request" in capsys.readouterr().err
