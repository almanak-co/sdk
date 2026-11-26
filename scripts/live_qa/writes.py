"""Costly live write check: a tiny Aave V4 supply and full withdrawal on mainnet.

THIS SPENDS REAL FUNDS. It is for a release or the days after a protocol upgrade,
run by a person at a terminal with their own funded wallet. It must not run in CI,
on a schedule or often.

Every gate is checked before anything else happens: no command runs and no
endpoint is contacted until all of them hold. The gates look at the wallet
directory only as far as `wallet.json`, which holds the address and file paths.
The check never creates a wallet and never opens a keystore or a password file;
`almanak ax` does the signing, with the wallet directory the operator names.
"""

import json
import os
import shlex
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from almanak.caip import ChainId
from almanak.gateway.providers import PUBLIC_RPC
from almanak.models import address
from scripts.live_qa.cli import Invocation, Run

SPEND_ENV = "ALMANAK_QA_SPEND_REAL_FUNDS"
# Set by hosted runners. The write check refuses under either, whatever else is set.
CI_ENV = ("CI", "GITHUB_ACTIONS")

WARNING = """\
==========================================================================
 THIS SPENDS REAL FUNDS ON MAINNET.
 It sends up to four transactions (approve, supply, withdraw, and a revoke
 when an allowance is left) from the wallet you named, and pays gas for each.
 Run it for a release or after a protocol upgrade. Do not run it in CI, on a
 schedule or often. Nothing is signed before you confirm.
=========================================================================="""


@dataclass(frozen=True)
class WriteMarket:
    """A reviewed Aave V4 market the write check may use, with its size limit."""

    chain: ChainId
    # The Spoke's EVM address, as `almanak ax` takes it with `--spoke`.
    spoke: str
    reserve_id: int
    symbol: str
    decimals: int
    # Base units. `suggested` is the documented size; `maximum` refuses anything larger.
    suggested: int
    maximum: int


@dataclass(frozen=True)
class FeeLimit:
    """The largest ceilings the check accepts on a chain, in wei of its native asset."""

    gas_price: int
    transaction_fee: int


ETHEREUM = ChainId("eip155:1")
AVALANCHE = ChainId("eip155:43114")
GWEI = 10**9
# Validated when this module loads; the commands are also parsed by the real CLI in tests.
ETHEREUM_SPOKE = address("0x94e7a5dcbe816e498b89ab752661904e2f56c485")
AVALANCHE_SPOKE = address("0x435272ceff93a1e657e8abfdf0a13e95900a3a56")

MARKETS: dict[str, WriteMarket] = {
    "ethereum-usdc": WriteMarket(ETHEREUM, ETHEREUM_SPOKE, 7, "USDC", 6, 100_000, 1_000_000),
    "ethereum-weth": WriteMarket(ETHEREUM, ETHEREUM_SPOKE, 0, "WETH", 18, 10**14, 10**15),
    "avalanche-usdc": WriteMarket(AVALANCHE, AVALANCHE_SPOKE, 2, "USDC", 6, 100_000, 1_000_000),
    "avalanche-wavax": WriteMarket(AVALANCHE, AVALANCHE_SPOKE, 0, "WAVAX", 18, 10**16, 10**17),
}

# A ceiling above these is refused: a QA round trip is never worth more gas than this.
FEE_LIMITS: dict[ChainId, FeeLimit] = {
    ETHEREUM: FeeLimit(gas_price=100 * GWEI, transaction_fee=2 * 10**16),
    AVALANCHE: FeeLimit(gas_price=200 * GWEI, transaction_fee=2 * 10**17),
}


@dataclass(frozen=True)
class WriteRequest:
    """The operator's choices. Every field is optional so that a missing one is a gate."""

    market: str | None = None
    amount: int | None = None
    wallet: Path | None = None
    max_gas_price_wei: int | None = None
    max_transaction_fee: int | None = None
    public_rpc: str | None = None
    env_file: Path | None = None
    execute: bool = False
    confirm: str | None = None


def environment_gates(environment: Mapping[str, str]) -> list[str]:
    # Present with any value, an empty one included: the gate fails closed.
    refusals = [
        f"{name} is set: the write check never runs in CI" for name in CI_ENV if name in environment
    ]
    if environment.get(SPEND_ENV) != "1":
        refusals.append(f"{SPEND_ENV}=1 is required: this check spends real funds")
    return refusals


def password_file(wallet: Path) -> Path | None:
    """The password file `wallet.json` names, if any. The file itself is never opened."""
    try:
        record = json.loads((wallet / "wallet.json").read_text())
    except (OSError, ValueError):
        return None
    named = record.get("password_file") if isinstance(record, dict) else None
    return Path(named) if isinstance(named, str) and named else None


def wallet_gates(request: WriteRequest, interactive: bool) -> list[str]:
    if request.wallet is None:
        return ["--wallet is required: the directory of your own funded wallet"]
    if not (request.wallet / "wallet.json").is_file():
        return ["--wallet holds no wallet.json from `almanak wallet create`"]
    # `almanak ax` asks for the keystore password on the terminal when the wallet
    # names no password file. Without a terminal that fails after the confirmation,
    # in the middle of the round trip, so it is refused here instead.
    named = password_file(request.wallet)
    if not interactive and (named is None or not named.is_file()):
        return [
            "without a terminal the wallet must name an existing password file: create it"
            " with `almanak wallet create --generate-password` or `--password-file`"
        ]
    return []


# Created with exclusive creation once the gates pass, so of two runs that found the
# same directory empty, only one proceeds; the other would overwrite its report.
RUN_MARKER = "run.lock"


def reserve_output(output: Path) -> str | None:
    """Make the output directory and claim it for this run, or say why not."""
    try:
        # Made before the first command, so a report can be written whatever follows.
        output.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return f"--output {output} cannot be created: {exc}"
    try:
        marker = os.open(output / RUN_MARKER, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        return f"--output {output} is claimed by another run; use a new output directory"
    except OSError as exc:
        return f"--output {output} cannot be claimed for this run: {exc}"
    os.close(marker)
    return None


def output_gates(output: Path | None) -> list[str]:
    """A report is never written over an earlier run's, which may hold transaction hashes."""
    if output is None or not (output.exists() or output.is_symlink()):
        return []
    if not output.is_dir():
        return [f"--output {output} is not a directory; name a new output directory"]
    if any(output.iterdir()):
        return [f"--output {output} is not empty; use a new output directory"]
    return []


def transport_gates(request: WriteRequest) -> list[str]:
    if (request.public_rpc is None) == (request.env_file is None):
        return ["exactly one of --public-rpc and --env-file is required"]
    market = MARKETS.get(request.market or "")
    if request.public_rpc is None or market is None:
        return []
    if market.chain not in PUBLIC_RPC.get(request.public_rpc, {}):
        serving = sorted(name for name, chains in PUBLIC_RPC.items() if market.chain in chains)
        return [
            f"--public-rpc {request.public_rpc} does not serve {market.chain};"
            f" choose from {', '.join(serving) or 'none: use --env-file'}"
        ]
    return []


def market_gates(request: WriteRequest) -> list[str]:
    market = MARKETS.get(request.market or "")
    if market is None:
        return [f"--market is required: one of {', '.join(sorted(MARKETS))}"]
    if request.amount is None:
        return [f"--amount is required in base units; suggested {market.suggested}"]
    if not 0 < request.amount <= market.maximum:
        return [
            f"--amount must be above 0 and at most {market.maximum} base units of"
            f" {market.symbol}: tiny amounts only"
        ]
    return []


def fee_gates(request: WriteRequest) -> list[str]:
    market = MARKETS.get(request.market or "")
    # Without a known market there is no chain to bound the ceilings by; they must
    # still be given, and the market gate refuses the request anyway.
    limit = FEE_LIMITS[market.chain] if market is not None else FeeLimit(0, 0)
    ceilings = (
        ("--max-gas-price-wei", request.max_gas_price_wei, limit.gas_price or None),
        ("--max-transaction-fee", request.max_transaction_fee, limit.transaction_fee or None),
    )
    refusals = []
    for option, value, maximum in ceilings:
        if value is None:
            refusals.append(f"{option} is required: a fee ceiling the wallet will not exceed")
        elif value <= 0 or (maximum is not None and value > maximum):
            refusals.append(f"{option} must be above 0 and at most {maximum}")
    return refusals


def gates(
    request: WriteRequest,
    environment: Mapping[str, str],
    output: Path | None = None,
    *,
    interactive: bool = True,
) -> list[str]:
    """Every reason the check may not start.

    Runs no command and contacts nothing. On disk it lists the output directory and
    reads `wallet.json` for the password file's path; it opens no keystore and no
    password file. `interactive` says whether a terminal can be asked for the
    keystore password.
    """
    return [
        *environment_gates(environment),
        *wallet_gates(request, interactive),
        *fee_gates(request),
        *market_gates(request),
        *transport_gates(request),
        *output_gates(output),
    ]


def confirmation_phrase(request: WriteRequest) -> str:
    """What the person types or passes to approve this exact round trip."""
    return f"supply {request.amount} {request.market} and withdraw"


def action(request: WriteRequest, name: str, *, execute: bool) -> tuple[str, ...]:
    """The `almanak ax supply` or `ax withdraw` command for the request's market."""
    market = MARKETS[request.market or ""]
    arguments = ["ax", name, "--connector", "aave_v4", "--chain", str(market.chain)]
    arguments += ["--spoke", market.spoke, "--reserve-id", str(market.reserve_id)]
    if name == "supply":
        arguments += ["--amount", str(request.amount)]
    arguments += ["--wallet", str(request.wallet)]
    if request.public_rpc is not None:
        arguments += ["--public-rpc", request.public_rpc]
    else:
        arguments += ["--env-file", str(request.env_file)]
    arguments += ["--max-gas-price-wei", str(request.max_gas_price_wei)]
    arguments += ["--max-transaction-fee", str(request.max_transaction_fee)]
    return (*arguments, "--execute") if execute else tuple(arguments)


@dataclass
class Step:
    name: str
    invocation: Invocation

    @property
    def output(self) -> dict[str, Any]:
        return self.invocation.output() or {}

    def record(self) -> dict[str, Any]:
        return {
            "step": self.name,
            "command": self.invocation.command,
            "exit_code": self.invocation.exit_code,
            "output": self.invocation.output(),
            "refusal": self.invocation.refusal(),
        }


def planned(step: Step) -> bool:
    return step.invocation.exit_code == 0 and "first_step" in step.output


def completed(step: Step) -> bool:
    return step.invocation.exit_code == 0 and step.output.get("completed") is True


# The fields of a position report that a full withdrawal takes, as `almanak ax` prints them.
SHARE_FIELDS = ("supplied_shares", "debt_shares")


def shares_held(position: object) -> bool | None:
    """Whether a position report shows shares: False when it reports both as zero, True
    when it reports a share field other than zero, and None when it shows neither.

    A missing report or share field is not a measured zero, and is no evidence of a
    position either.
    """
    reported = [position.get(name) for name in SHARE_FIELDS] if isinstance(position, dict) else []
    if any(value is not None and value != "0" for value in reported):
        return True
    if len(reported) == len(SHARE_FIELDS) and None not in reported:
        return False
    return None


# What a refusal to withdraw says about the funds, whatever the shares showed: the
# check cannot tell from the reports alone where the supplied amount now is.
POSITION_UNCERTAIN = (
    " THE SUPPLIED AMOUNT MAY STILL BE IN THE MARKET. Read the position with"
    " `almanak ax lending-position` before recovering it with `almanak ax withdraw`"
)


def share_refusal(supplied: object, before_withdrawal: object) -> str | None:
    """Why the withdrawal is not run, from the share fields of the two position reports.

    None only when both reports carry every share field and they agree. A report that is
    missing or lacks a share field is no evidence of a change, so it is named as shares
    that could not be compared; a real difference is reported as observed, in either
    direction, never as funds that reached the position.
    """
    after, now = (
        {name: report.get(name) for name in SHARE_FIELDS} if isinstance(report, dict) else {}
        for report in (supplied, before_withdrawal)
    )
    if None in (*after.values(), *now.values()) or not (after and now):
        return (
            "the withdrawal was not run: a position report lacks its shares, so the check"
            " could not compare the position the supply left with the one the withdrawal"
            f" would take (after the supply {after}, now {now})." + POSITION_UNCERTAIN
        )
    changed = [
        f"{name} {after[name]} after the supply, {now[name]} now"
        for name in SHARE_FIELDS
        if after[name] != now[name]
    ]
    if not changed:
        return None
    return (
        "the withdrawal was not run: the position it would take differs from the one the"
        " supply left (" + "; ".join(changed) + "), and the confirmation covered only"
        " this check's supply." + POSITION_UNCERTAIN
    )


class WriteCheck:
    """One round trip. `steps` holds every command it ran, in order, for the report."""

    def __init__(self, request: WriteRequest, run: Run, say: Callable[[str], None]):
        self.request = request
        self.run = run
        self.say = say
        self.steps: list[Step] = []
        # The command running now, and whether it signs; kept so that an interrupted
        # run's report can name a command that returned nothing.
        self.running: tuple[str, bool] | None = None

    def step(self, name: str, action_name: str, *, execute: bool) -> Step:
        # An executing command waits for its transactions; it is never timed out or killed.
        arguments = action(self.request, action_name, execute=execute)
        self.running = (shlex.join(arguments), execute)
        invocation = self.run(arguments, None) if execute else self.run(arguments)
        self.running = None
        step = Step(name, invocation)
        self.steps.append(step)
        self.say(f"\n$ {invocation.command}\n{invocation.stdout}{invocation.stderr}".rstrip())
        return step

    def signing_started(self) -> bool:
        """Whether an executing command has started, so a transaction may have been sent."""
        executing = [step for step in self.steps if "--execute" in step.invocation.arguments]
        return bool(executing) or (self.running is not None and self.running[1])

    def interrupted(self, error: BaseException) -> str:
        """The verdict of a run stopped by an exception, such as the operator's Ctrl-C."""
        where = f" while running `{self.running[0]}`" if self.running else ""
        verdict = f"interrupted{where} ({type(error).__name__})"
        if not self.signing_started():
            return f"{verdict}; nothing was signed"
        return (
            f"{verdict}, after signing began: transactions may have been sent, and the"
            " outcome of an executing command that returned nothing is not known."
            " THE SUPPLIED AMOUNT MAY STILL BE IN THE MARKET. Do not run this again: read"
            " the steps above, resolve a kept record with `almanak ax recover`, then read"
            " the position"
        )

    def preview(self) -> str | None:
        """Plan the supply. Returns why the round trip may not go on, or None."""
        step = self.step("supply preview", "supply", execute=False)
        if not planned(step):
            return "the supply preview was refused; nothing was signed"
        held = shares_held(step.output.get("position_before"))
        if held:
            return (
                "the wallet already holds a position in this market, and the withdrawal"
                " would withdraw all of it; use a wallet with none. Nothing was signed"
            )
        if held is None:
            return (
                "the supply preview's report of the position lacks its shares, so the check"
                " could not tell whether the wallet already holds a position in this market,"
                " all of which the withdrawal would take. Nothing was signed"
            )
        return None

    def round_trip(self) -> str | None:
        """Supply, then withdraw everything. Returns what went wrong, or None."""
        supply = self.step("supply", "supply", execute=True)
        if not completed(supply):
            return (
                "the supply did not complete. Do not run this again: read `outcomes` and"
                " `refused` above, then `almanak ax recover` if a record was kept. Recovery"
                " can complete the supply, so read the position afterwards and withdraw"
                " with `almanak ax withdraw` if the amount is supplied"
            )
        # The supply adopted what the wallet held when it ran, after the preview.
        held = shares_held(supply.output.get("position_before"))
        if held:
            return (
                "the wallet's position changed between the preview and the supply, so the"
                " withdrawal was not run: it would withdraw more than this check supplied."
                " THE SUPPLIED AMOUNT IS STILL IN THE MARKET, with what was there before"
            )
        if held is None:
            # Missing shares are no evidence of a change, nor that the position was empty.
            return (
                "the withdrawal was not run: the supply's report of the position before it"
                " lacks its shares, so the check could not compare it with the preview, and"
                " a full withdrawal takes whatever the position holds." + POSITION_UNCERTAIN
            )
        withdraw_preview = self.step("withdraw preview", "withdraw", execute=False)
        if not planned(withdraw_preview):
            return (
                "the withdrawal preview was refused. THE SUPPLIED AMOUNT IS STILL IN THE"
                " MARKET; withdraw it with `almanak ax withdraw` once the reason is resolved"
            )
        # A full withdrawal takes whatever the wallet holds when it is planned, and the
        # confirmation covered only this check's supply. Something that reached the
        # position after the supply is not withdrawn here. The position can still change
        # between this preview and the withdrawal; that window is not closed.
        refusal = share_refusal(
            supply.output.get("position_after"), withdraw_preview.output.get("position_before")
        )
        if refusal is not None:
            return refusal
        withdrawal = self.step("withdraw", "withdraw", execute=True)
        if not completed(withdrawal):
            return (
                "the withdrawal did not complete, and its outcome is not known: THE SUPPLIED"
                " AMOUNT MAY STILL BE IN THE MARKET, or the withdrawal may have been mined."
                " Do not withdraw again yet: read `outcomes` and `refused` above, resolve a"
                " kept record with `almanak ax recover`, then read the position"
            )
        held = shares_held(withdrawal.output.get("position_after"))
        if held:
            return "the withdrawal completed but the position is not empty"
        if held is None:
            return (
                "the withdrawal completed, but its report of the position after it lacks its"
                " shares, so the check could not confirm the position is empty. Read the"
                " position with `almanak ax lending-position`"
            )
        return None


def transactions(steps: Sequence[Step]) -> list[str]:
    return [
        f"{step.name}: {outcome.get('operation')} {outcome.get('transaction')}"
        f" ({'succeeded' if outcome.get('success') else 'did not succeed'})"
        for step in steps
        for outcome in step.output.get("outcomes", ())
    ]


def transaction_lines(check: WriteCheck) -> list[str]:
    """The report's transactions: every recorded outcome, and what no outcome shows.

    An executing command that was interrupted, or returned no outcome, may still have
    sent a transaction, so "none sent" is said only when no command signed.
    """
    lines = transactions(check.steps)
    if check.running is not None and check.running[1]:
        lines.append(
            f"`{check.running[0]}` was interrupted before it returned: whether it sent a"
            " transaction is not known"
        )
    if lines:
        return lines
    if check.signing_started():
        return ["no transaction outcomes recorded; a transaction may still have been sent"]
    return ["none sent"]


def write_report(directory: Path, request: WriteRequest, check: WriteCheck, verdict: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    document = {
        "market": request.market,
        "amount": request.amount,
        "executed": request.execute,
        "verdict": verdict,
        "steps": [step.record() for step in check.steps],
    }
    (directory / "writes.json").write_text(json.dumps(document, indent=2))
    lines = ["# Live write check", "", f"- Market: {request.market}"]
    lines += [f"- Amount: {request.amount} base units", f"- Verdict: {verdict}", ""]
    lines += ["## Transactions", ""]
    lines += [f"- {line}" for line in transaction_lines(check)]
    lines += ["", "## Commands", ""]
    lines += [
        f"- `{step.invocation.command}` (exit {step.invocation.exit_code})" for step in check.steps
    ]
    (directory / "summary.md").write_text("\n".join(lines) + "\n")


def confirmed(request: WriteRequest, ask: Callable[[str], str] | None) -> bool:
    """The exact phrase, passed with --confirm or typed at a terminal."""
    phrase = confirmation_phrase(request)
    if request.confirm is not None:
        return request.confirm == phrase
    if ask is None:
        return False
    return ask(f'\nType "{phrase}" to sign and send, anything else to stop: ') == phrase


def terminal_prompt() -> Callable[[str], str] | None:
    return input if sys.stdin.isatty() else None


def run_writes(
    request: WriteRequest,
    environment: Mapping[str, str],
    run: Run,
    output: Path,
    *,
    ask: Callable[[str], str] | None = None,
    say: Callable[[str], None] = lambda text: print(text, file=sys.stderr, flush=True),
) -> int:
    """0: done as asked. 1: a step failed or was refused. 2: a gate or the confirmation."""
    refusals = gates(request, environment, output, interactive=ask is not None)
    if refusals:
        say("The write check refuses to start:\n" + "\n".join(f"- {line}" for line in refusals))
        return 2
    if (problem := reserve_output(output)) is not None:
        say(f"The write check refuses to start:\n- {problem}")
        return 2
    say(WARNING)
    check = WriteCheck(request, run, say)
    try:
        return finish(request, check, output, ask, say)
    except BaseException as error:
        # Ctrl-C or a crash after a transaction was sent must not lose its record: the
        # steps already returned, and the command that was running, go into the report.
        verdict = check.interrupted(error)
        write_report(output, request, check, verdict)
        say(f"\n{verdict}\nReport: {output}")
        raise


def finish(
    request: WriteRequest,
    check: WriteCheck,
    output: Path,
    ask: Callable[[str], str] | None,
    say: Callable[[str], None],
) -> int:
    """Preview, confirm and run the round trip, then write the report."""
    problem = check.preview()
    if problem is None and request.execute:
        if not confirmed(request, ask):
            say(
                "Not confirmed; nothing was signed. To approve this round trip pass"
                f' --confirm "{confirmation_phrase(request)}".'
            )
            write_report(output, request, check, "not confirmed; nothing was signed")
            return 2
        problem = check.round_trip()
    elif problem is None:
        say(
            "\nPreview only; nothing was signed. Show this preview to the person paying,"
            f' then run again with --execute --confirm "{confirmation_phrase(request)}".'
        )
    verdict = problem or ("round trip completed" if request.execute else "preview only")
    write_report(output, request, check, verdict)
    say(f"\n{verdict}\nReport: {output}")
    return 1 if problem else 0
