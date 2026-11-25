"""Free live read checks: `almanak ax` reads on named public endpoints, cross-checked.

Three outcomes. `pass`: the command behaved as the case states and every direct
read agreed. `fail`: it did not, and repeating it would not help. `blocked`: the
endpoint did not answer (rate limit, rejected read, timeout) after every retry;
a blocked case is not a pass and says nothing about the SDK.
"""

import csv
import json
import time
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from eth_abi.exceptions import EncodingError

from almanak.caip import ChainId
from scripts.live_qa.checks import Check
from scripts.live_qa.cli import Invocation, Run
from scripts.live_qa.rpc import Rpc, RpcReverted, RpcUnavailable

# Refusals that name the endpoint's availability, not the SDK or the chain's state.
TRANSIENT_ERRORS = frozenset({"rpc", "http", "transport", "unavailable", "stale", "block_mismatch"})

Status = Literal["pass", "fail", "blocked"]
STATUSES: tuple[Status, ...] = ("pass", "fail", "blocked")


@dataclass(frozen=True)
class Refusal:
    """A refusal a case accepts: its `error` and a part of its `message`."""

    error: str
    contains: str = ""

    def matches(self, refusal: dict[str, Any]) -> bool:
        message = str(refusal.get("message", ""))
        return refusal.get("error") == self.error and self.contains in message


@dataclass(frozen=True)
class KnownIssue:
    """A defect found by hand that the case keeps visible until it is fixed.

    The case still states the correct behavior and still fails. `signature` is the
    part of the failure detail that identifies this defect, so the report can tell
    it from a new one. `refusal` is the refusal kind the defect shows as, or None
    where it shows as a check's finding; a failure of any other kind is not it,
    whatever its text holds.
    """

    summary: str
    signature: str
    refusal: str | None = None


@dataclass(frozen=True)
class ReadCase:
    id: str
    chain: ChainId
    # Arguments after `almanak ax`, without the transport.
    arguments: tuple[str, ...]
    check: Check | None = None
    # Whether a successful read is a correct result, and which refusals are.
    succeeds: bool = True
    refusals: tuple[Refusal, ...] = ()
    known_issue: KnownIssue | None = None
    # A JSON request written to a file and passed as `--config`.
    config: dict[str, Any] | None = None
    # Endpoints to run on; None means every selected endpoint that serves the chain.
    endpoints: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        # The planner, the breaker and the direct clients key on the chain's identity.
        if not isinstance(self.chain, ChainId):
            raise TypeError(f"case {self.id}: chain must be a ChainId, not {self.chain!r}")


@dataclass(frozen=True)
class Verdict:
    status: Literal["pass", "fail", "transient"]
    detail: str
    # Each separate thing a failed check found; `detail` joins them for the report.
    findings: tuple[str, ...] = ()
    # The refusal's `error` kind where the command refused, else None.
    refusal: str | None = None


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 3
    # Seconds before the second attempt; each later wait is three times the last.
    backoff_seconds: float = 5.0

    # Seconds one command may take before it counts as unanswered.
    timeout_seconds: float = 90.0

    def wait(self, attempt: int) -> float:
        return self.backoff_seconds * 3 ** (attempt - 1)


DEFAULT_POLICY = RetryPolicy()


@dataclass(frozen=True)
class Result:
    case: ReadCase
    endpoint: str
    status: Status
    attempts: int
    seconds: float
    detail: str
    command: str
    findings: tuple[str, ...] = ()
    refusal: str | None = None

    @property
    def known(self) -> str:
        """How the result relates to the case's known issue, for the report.

        It reproduces only when every finding of the failure is the known issue.
        One finding beside it is a new failure, and is not excused by the marker.
        """
        issue = self.case.known_issue
        if issue is None:
            return ""
        if self.status == "pass":
            return f"no longer reproduces, remove the marker: {issue.summary}"
        if self.status != "fail" or self.refusal != issue.refusal:
            return f"not observed this run: {issue.summary}"
        matched = [issue.signature in finding for finding in self.findings or (self.detail,)]
        if all(matched):
            return f"reproduces: {issue.summary}"
        if any(matched):
            return f"other failures beside the known issue: {issue.summary}"
        return f"not observed this run: {issue.summary}"


def judge_refusal(case: ReadCase, refusal: dict[str, Any]) -> Verdict:
    kind = refusal.get("error")
    if not isinstance(kind, str):
        return Verdict("fail", f"malformed refusal, its error is not a kind: {refusal!r}")
    # A refusal carries a `message`, or `problems` when a request did not validate.
    reason = refusal.get("message") or json.dumps(refusal.get("problems", ""))
    text = f"{kind}: {reason}"
    if any(accepted.matches(refusal) for accepted in case.refusals):
        return Verdict("pass", f"refused as the case states: {text}", refusal=kind)
    if kind in TRANSIENT_ERRORS:
        return Verdict("transient", text, refusal=kind)
    return Verdict("fail", text, refusal=kind)


def partial_errors(output: dict[str, Any]) -> list[str]:
    """Parts a command could not read although it exited 0, as `kind: message`.

    A reserve read reports its resolution and puts a failed observation under
    `observation_error`, with `observation` null.
    """
    return [str(value) for key, value in output.items() if key.endswith("_error") and value]


def judge_partial(errors: list[str]) -> Verdict:
    text = "the command exited 0 but reported: " + "; ".join(errors)
    if all(error.partition(":")[0] in TRANSIENT_ERRORS for error in errors):
        return Verdict("transient", text)
    return Verdict("fail", text, tuple(errors))


def judge_output(case: ReadCase, output: dict[str, Any], rpc: Rpc) -> Verdict:
    if not case.succeeds:
        return Verdict("fail", "the case states a refusal, but the command succeeded")
    errors = partial_errors(output)
    if errors:
        return judge_partial(errors)
    if case.check is None:
        return Verdict("pass", "read succeeded")
    try:
        mismatches = case.check(output, rpc)
    except RpcReverted as exc:
        # The endpoint answered: a read the output should make possible reverts.
        return Verdict("fail", f"a direct read of what the command reported reverted: {exc}")
    except RpcUnavailable as exc:
        return Verdict("transient", f"cross-check blocked: {exc}")
    except (AttributeError, KeyError, IndexError, TypeError, ValueError, EncodingError) as exc:
        # AttributeError: a null or a list where the check reads an object's fields.
        # EncodingError: a reported value, such as a null address, cannot be a call argument.
        return Verdict("fail", f"output is not shaped as the check reads it: {exc!r}")
    if mismatches:
        return Verdict("fail", "; ".join(mismatches), tuple(mismatches))
    return Verdict("pass", "read succeeded and the direct reads agree")


def judge(case: ReadCase, invocation: Invocation, rpc: Rpc) -> Verdict:
    if invocation.timed_out:
        return Verdict("transient", "the command timed out")
    output = invocation.output()
    if invocation.exit_code == 0 and output is not None:
        return judge_output(case, output, rpc)
    refusal = invocation.refusal()
    if refusal is None:
        tail = " ".join(invocation.stderr.split())[-300:]
        return Verdict("fail", f"exit {invocation.exit_code} without a JSON refusal: {tail}")
    return judge_refusal(case, refusal)


def case_arguments(case: ReadCase, endpoint: str, workdir: Path) -> tuple[str, ...]:
    arguments = ("ax", *case.arguments)
    if case.config is not None:
        # Endpoints run side by side in one directory, so each writes its own file.
        name = f"{endpoint}-{case.id}".replace("/", "-").replace(":", "-")
        path = workdir / f"{name}.json"
        path.write_text(json.dumps(case.config))
        arguments = (*arguments, "--config", str(path))
    return (*arguments, "--public-rpc", endpoint)


def run_case(
    case: ReadCase,
    endpoint: str,
    run: Run,
    rpc: Rpc,
    workdir: Path,
    policy: RetryPolicy = DEFAULT_POLICY,
    sleep: Callable[[float], None] = time.sleep,
) -> Result:
    """Run one case on one endpoint, repeating only what the endpoint failed to answer."""
    arguments = case_arguments(case, endpoint, workdir)
    started = time.monotonic()
    attempt = 0
    while True:
        attempt += 1
        invocation = run(arguments, policy.timeout_seconds)
        verdict = judge(case, invocation, rpc)
        if verdict.status != "transient" or attempt >= policy.attempts:
            break
        sleep(policy.wait(attempt))
    status: Status = "blocked" if verdict.status == "transient" else verdict.status
    seconds = round(time.monotonic() - started, 1)
    return Result(
        case,
        endpoint,
        status,
        attempt,
        seconds,
        verdict.detail,
        invocation.command,
        verdict.findings,
        verdict.refusal,
    )


def planned(
    cases: Iterable[ReadCase],
    endpoints: Sequence[str],
    serves: Callable[[str, ChainId], bool],
) -> list[tuple[ReadCase, str]]:
    """Each case on each selected endpoint it names, or on each that serves its chain.

    A case that states a refusal for an unserved chain names its endpoints itself.
    """

    def runs_on(case: ReadCase, endpoint: str) -> bool:
        if case.endpoints is not None:
            return endpoint in case.endpoints
        return serves(endpoint, case.chain)

    return [(case, endpoint) for case in cases for endpoint in endpoints if runs_on(case, endpoint)]


# After this many consecutive blocked cases on one chain, an endpoint's remaining
# cases there are reported blocked without being run: it is down or limiting, and
# each further case would only spend every retry to say so again.
BREAKER_LIMIT = 3


class Breaker:
    """Consecutive blocked cases per endpoint and chain."""

    def __init__(self) -> None:
        self.streak: dict[tuple[str, ChainId], int] = {}

    def open(self, endpoint: str, chain: ChainId) -> bool:
        return self.streak.get((endpoint, chain), 0) >= BREAKER_LIMIT

    def record(self, result: "Result") -> None:
        key = (result.endpoint, result.case.chain)
        self.streak[key] = self.streak.get(key, 0) + 1 if result.status == "blocked" else 0


@dataclass
class Session:
    """What a run needs besides its plan: the command, direct clients and retry policy."""

    run: Run
    rpc: Callable[[str, ChainId], Rpc]
    workdir: Path
    policy: RetryPolicy = DEFAULT_POLICY
    report: Callable[[Result], None] = lambda result: None
    breaker: Breaker = field(default_factory=Breaker)

    def one(self, case: ReadCase, endpoint: str) -> Result:
        """Run the case unless the breaker is open for its endpoint and chain."""
        if self.breaker.open(endpoint, case.chain):
            result = not_run(case, endpoint)
        else:
            result = run_case(
                case, endpoint, self.run, self.rpc(endpoint, case.chain), self.workdir, self.policy
            )
            self.breaker.record(result)
        self.report(result)
        return result


def not_run(case: ReadCase, endpoint: str) -> Result:
    detail = f"not run: {endpoint} blocked the previous {BREAKER_LIMIT} cases on {case.chain}"
    return Result(case, endpoint, "blocked", 0, 0.0, detail, "")


def run_lane(cases: Sequence[ReadCase], endpoint: str, session: Session) -> list[Result]:
    """One endpoint's cases, one after another."""
    return [session.one(case, endpoint) for case in cases]


def run_plan(plan: Sequence[tuple[ReadCase, str]], session: Session) -> list[Result]:
    """Endpoints run side by side; the cases of one endpoint run one after another."""
    endpoints = sorted({endpoint for _, endpoint in plan})
    if not endpoints:
        return []

    def lane(endpoint: str) -> list[Result]:
        return run_lane([case for case, named in plan if named == endpoint], endpoint, session)

    with ThreadPoolExecutor(max_workers=len(endpoints)) as pool:
        lanes = list(pool.map(lane, endpoints))
    order = {(case.id, endpoint): index for index, (case, endpoint) in enumerate(plan)}
    results = [result for results in lanes for result in results]
    return sorted(results, key=lambda result: order[result.case.id, result.endpoint])


def counts(results: Sequence[Result]) -> dict[str, int]:
    return {status: sum(result.status == status for result in results) for status in STATUSES}


def exit_code(results: Sequence[Result], known_issues_ok: bool = False) -> int:
    """0: every case passed. 1: a case failed. 2: none failed, but one was blocked.

    With `known_issues_ok`, a failure that reproduces its case's known issue does not
    count as failed here; the report still lists it as a failure.
    """
    failed = [
        result
        for result in results
        if result.status == "fail"
        and not (known_issues_ok and result.known.startswith("reproduces"))
    ]
    if failed or not results:
        return 1
    return 2 if counts(results)["blocked"] else 0


Expectation = Literal["pass", "skip", "xfail", "fail"]


def expectation(result: Result) -> tuple[Expectation, str]:
    """How a test lane treats a result, strictly about known issues.

    A known issue that reproduces is an expected failure. One that no longer
    reproduces fails, so that its marker is removed. Blocked is a skip, never a pass.
    """
    if result.status == "blocked":
        return "skip", f"blocked after {result.attempts} attempts: {result.detail}"
    if result.known.startswith("reproduces"):
        return "xfail", f"{result.known}: {result.detail}"
    if result.known.startswith("no longer reproduces"):
        return "fail", result.known
    if result.status == "pass":
        return "pass", result.detail
    return "fail", f"{result.known or 'unexpected'}: {result.detail}"


COLUMNS = (
    "case",
    "chain",
    "endpoint",
    "status",
    "attempts",
    "seconds",
    "known_issue",
    "detail",
    "command",
)


def row(result: Result) -> dict[str, Any]:
    return {
        "case": result.case.id,
        "chain": str(result.case.chain),
        "endpoint": result.endpoint,
        "status": result.status,
        "attempts": result.attempts,
        "seconds": result.seconds,
        "known_issue": result.known,
        "detail": result.detail,
        "command": result.command,
    }


def write_csv(results: Sequence[Result], path: Path) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(row(result) for result in results)


def cell(text: object) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def markdown(results: Sequence[Result], header: dict[str, str]) -> str:
    tally = counts(results)
    known = sum(result.known.startswith("reproduces") for result in results)
    lines = ["# Live public read checks", ""]
    lines += [f"- {name}: {value}" for name, value in header.items()]
    lines += [
        f"- Result: {tally['pass']} pass, {tally['fail']} fail"
        f" ({known} with a known issue), {tally['blocked']} blocked",
        "",
        "A blocked case is one the public endpoint did not answer after every retry."
        " It is not a pass and not a finding about the SDK.",
        "",
        "| Case | Endpoint | Status | Attempts | Known issue | Detail |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    lines += [
        f"| `{cell(item['case'])}` | {item['endpoint']} | {item['status']} | {item['attempts']}"
        f" | {cell(item['known_issue'])} | {cell(item['detail'])} |"
        for item in map(row, results)
    ]
    for status in ("fail", "blocked"):
        selected = [result for result in results if result.status == status]
        if selected:
            lines += ["", f"## Commands that ended {status}", ""]
            lines += [f"- `{result.command}`" for result in selected]
    return "\n".join(lines) + "\n"


def write_reports(results: Sequence[Result], directory: Path, header: dict[str, str]) -> None:
    write_csv(results, directory / "results.csv")
    (directory / "summary.md").write_text(markdown(results, header))
