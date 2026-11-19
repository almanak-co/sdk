"""`python -m scripts.live_qa reads|writes`: the opt-in live QA entry point."""

import argparse
import os
import shlex
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path

from almanak.gateway.providers import PUBLIC_RPC
from scripts.live_qa import cli, reads, writes
from scripts.live_qa.read_cases import CASES
from scripts.live_qa.rpc import Rpc, endpoint_url


def whole(value: str) -> int:
    if not (value.isascii() and value.isdigit()):
        raise argparse.ArgumentTypeError("expected a whole number in base units")
    return int(value)


def command_option(parser: argparse.ArgumentParser, *, output_required: bool) -> None:
    parser.add_argument(
        "--almanak-command",
        type=shlex.split,
        default=list(cli.default_command()),
        help="how to start the CLI under test; default is this checkout's `almanak`."
        ' For the published package: "uvx --from almanak==<version> almanak"',
    )
    parser.add_argument(
        "--output", type=Path, required=output_required, help="a new directory for the report"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m scripts.live_qa", description=__doc__)
    levels = parser.add_subparsers(dest="level", required=True)
    read = levels.add_parser(
        "reads", help="free read checks on public endpoints; no key, wallet or funds"
    )
    # A run needs --output; --list writes nothing and takes none.
    command_option(read, output_required=False)
    read.add_argument(
        "--endpoint",
        action="append",
        choices=sorted(PUBLIC_RPC),
        help="public endpoint to check; repeat for several (default: all)",
    )
    read.add_argument("--case", action="append", help="only cases whose ID contains this text")
    read.add_argument("--attempts", type=int, default=3, help="tries per case before blocked")
    read.add_argument(
        "--backoff-seconds", type=float, default=5.0, help="wait before the second try"
    )
    read.add_argument(
        "--timeout-seconds", type=float, default=90.0, help="longest one command may take"
    )
    read.add_argument(
        "--known-issues-ok",
        action="store_true",
        help="exit 0 or 2 when the only failures reproduce known issues; they stay in the report",
    )
    read.add_argument("--list", action="store_true", help="print the planned cases and exit")
    write = levels.add_parser(
        "writes",
        help="SPENDS REAL FUNDS: a tiny Aave V4 supply and full withdrawal on mainnet",
        description=writes.WARNING,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    command_option(write, output_required=True)
    # None of these is required by the parser: a missing one is a gate with its own reason.
    write.add_argument("--market", help=f"one of {', '.join(sorted(writes.MARKETS))}")
    write.add_argument("--amount", type=whole, help="base units of the market's asset; tiny")
    write.add_argument(
        "--wallet", type=Path, help="your own funded wallet directory (`almanak wallet create`)"
    )
    write.add_argument("--max-gas-price-wei", type=whole, help="as `almanak ax supply` takes it")
    write.add_argument("--max-transaction-fee", type=whole, help="as `almanak ax supply` takes it")
    write.add_argument("--public-rpc", choices=sorted(PUBLIC_RPC))
    write.add_argument("--env-file", type=Path, help="private provider credentials")
    write.add_argument(
        "--execute", action="store_true", help="sign and send after the confirmation"
    )
    write.add_argument("--confirm", help="the confirmation phrase the preview printed")
    return parser


def new_directory(path: Path) -> Path:
    """Reports are never mixed with an earlier run's."""
    if path.exists() and any(path.iterdir()):
        raise SystemExit(f"{path} is not empty; use a new output directory")
    path.mkdir(parents=True, exist_ok=True)
    return path


def selected_cases(filters: Sequence[str] | None) -> list[reads.ReadCase]:
    return [case for case in CASES if not filters or any(text in case.id for text in filters)]


def announce(result: reads.Result) -> None:
    line = f"{result.status:7} {result.endpoint:10} {result.case.id}"
    print(line if result.status == "pass" else f"{line}: {result.detail}", flush=True)


def command_label(command: Sequence[str]) -> str:
    """The CLI under test, without this machine's paths when it is the checkout's."""
    if tuple(command) == cli.default_command():
        return f"this checkout's `almanak` ({version('almanak')})"
    return f"`{shlex.join(command)}`"


def read_level(args: argparse.Namespace) -> int:
    endpoints = args.endpoint or sorted(PUBLIC_RPC)
    plan = reads.planned(
        selected_cases(args.case),
        endpoints,
        lambda endpoint, chain: endpoint_url(endpoint, chain) is not None,
    )
    if args.list:
        print("\n".join(f"{endpoint:10} {case.id}" for case, endpoint in plan))
        return 0
    if args.output is None:
        raise SystemExit("--output is required: a new directory for the report")
    output = new_directory(args.output)
    workdir = output / "work"
    workdir.mkdir()
    session = reads.Session(
        run=cli.runner(args.almanak_command, workdir),
        rpc=lambda endpoint, chain: Rpc(endpoint_url(endpoint, chain)),
        workdir=workdir,
        policy=reads.RetryPolicy(args.attempts, args.backoff_seconds, args.timeout_seconds),
        report=announce,
    )
    started = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    results = reads.run_plan(plan, session)
    header = {
        "Started": started,
        "Command under test": command_label(args.almanak_command),
        "Endpoints": ", ".join(endpoints),
    }
    reads.write_reports(results, output, header)
    tally = reads.counts(results)
    print(
        f"{tally['pass']} pass, {tally['fail']} fail, {tally['blocked']} blocked;"
        f" report in {output}"
    )
    return reads.exit_code(results, args.known_issues_ok)


def write_level(args: argparse.Namespace) -> int:
    request = writes.WriteRequest(
        market=args.market,
        amount=args.amount,
        wallet=args.wallet,
        max_gas_price_wei=args.max_gas_price_wei,
        max_transaction_fee=args.max_transaction_fee,
        public_rpc=args.public_rpc,
        env_file=args.env_file,
        execute=args.execute,
        confirm=args.confirm,
    )
    # The gates run first, inside run_writes: the output directory is made only after.
    return writes.run_writes(
        request,
        os.environ,
        cli.runner(args.almanak_command, Path.cwd()),
        args.output,
        ask=writes.terminal_prompt(),
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return read_level(args) if args.level == "reads" else write_level(args)


if __name__ == "__main__":
    sys.exit(main())
