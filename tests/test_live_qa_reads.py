"""The live read runner: verdicts, retries, the plan and the report.

Offline: every command and every direct read here is a fake. The live cases
themselves run only with `--live-public` or `python -m scripts.live_qa reads`.
"""

import csv
import json
import sys
from pathlib import Path

import httpx
import pytest
import yaml

from almanak.caip import ChainId
from almanak.cli_arguments import build_parser, parse_validated
from almanak.gateway.providers import PUBLIC_RPC
from scripts.live_qa import __main__ as entry
from scripts.live_qa import checks, cli, reads
from scripts.live_qa.read_cases import CASES
from scripts.live_qa.rpc import Rpc, RpcReverted, RpcUnavailable, asset_address, endpoint_url
from tests.live import conftest as live_conftest

ROOT = Path(__file__).parents[1]
BLOCK = {"number": 7, "hash": "0xaa"}
USDC = "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
ACCOUNT = "0x" + "11" * 20


class FakeRpc:
    """Direct reads answered from a table keyed by the call's signature."""

    def __init__(self, answers=None, balance=5, block_hash="0xaa", down=False, reverting=()):
        self.answers = answers or {}
        self.reverting = reverting
        self.native = balance
        self.hash = block_hash
        self.down = down

    def call(self, to, signature, block, outputs, args=()):
        if self.down:
            raise RpcUnavailable("down")
        if signature in self.reverting:
            raise RpcReverted(f"eth_call {signature}: execution reverted")
        return self.answers[signature]

    def reverts(self, to, signature, block):
        if self.down:
            raise RpcUnavailable("down")
        return to in self.reverting

    def balance(self, account, block):
        if self.down:
            raise RpcUnavailable("down")
        return self.native

    def block_hash(self, block):
        return self.hash


def invocation(exit_code=0, stdout="", stderr="", timed_out=False):
    return cli.Invocation(("almanak", "ax"), exit_code, stdout, stderr, timed_out)


def succeeded(document):
    return invocation(stdout=json.dumps(document))


def refused(error, message=""):
    return invocation(1, stderr=json.dumps({"error": error, "message": message}))


def scripted(*invocations):
    """A command runner that returns the given invocations in turn, recording its calls."""
    calls = []
    remaining = list(invocations)

    def run(arguments, timeout=None):
        calls.append(tuple(arguments))
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    return run, calls


def native_output(amount="5"):
    observation = {
        "asset": "eip155:1/slip44:60",
        "account": {"address": ACCOUNT},
        "block": BLOCK,
        "amount": amount,
        "decimals": 18,
    }
    return {"product": "ax-balance", "observations": [observation]}


NATIVE = "eip155:1/slip44:60"
CASE = reads.ReadCase(
    "balance/native", "eip155:1", ("balance",), checks.native_balance(NATIVE, ACCOUNT)
)


def test_a_read_that_agrees_with_the_direct_read_passes():
    verdict = reads.judge(CASE, succeeded(native_output()), FakeRpc())
    assert verdict.status == "pass"


def test_a_value_that_differs_from_the_direct_read_fails_and_names_both():
    verdict = reads.judge(CASE, succeeded(native_output("6")), FakeRpc())
    assert verdict.status == "fail"
    assert "balance: CLI 6, direct read 5" in verdict.detail


def test_a_block_the_endpoint_reports_differently_fails():
    verdict = reads.judge(CASE, succeeded(native_output()), FakeRpc(block_hash="0xbb"))
    assert verdict.status == "fail" and "block hash" in verdict.detail


def test_an_unanswered_direct_read_blocks_instead_of_failing():
    verdict = reads.judge(CASE, succeeded(native_output()), FakeRpc(down=True))
    assert verdict == reads.Verdict("transient", "cross-check blocked: down")


def test_a_direct_read_that_reverts_is_an_answer_so_the_case_fails_without_a_retry():
    """The endpoint answered. Only a read it did not answer is blocked."""
    output = {"observations": [{**native_output()["observations"][0], "asset": USDC}]}
    check = checks.erc20_balance(USDC, ACCOUNT)
    case = reads.ReadCase("balance/erc20", "eip155:1", ("balance",), check)
    run, calls = scripted(succeeded(output))
    rpc = FakeRpc(reverting=("balanceOf(address)",))
    result = reads.run_case(case, "publicnode", run, rpc, Path("."), sleep=pytest.fail)
    assert (result.status, result.attempts, len(calls)) == ("fail", 1, 1)
    assert "a direct read of what the command reported reverted" in result.detail
    assert reads.expectation(result)[0] == "fail"


def test_a_balance_read_without_observations_compares_nothing_and_fails():
    for check in (checks.native_balance(NATIVE, ACCOUNT), checks.erc20_balance(USDC, ACCOUNT)):
        case = reads.ReadCase("balance", "eip155:1", ("balance",), check)
        verdict = reads.judge(case, succeeded({"observations": []}), FakeRpc())
        assert verdict.status == "fail" and "no observations" in verdict.detail


def test_a_command_that_exits_0_without_its_observation_is_not_a_pass_or_a_wrong_fail():
    """A reserve read keeps its resolution and reports the failed observation beside it."""
    limited = {"resolution": {}, "observation": None, "observation_error": "http: HTTP 429"}
    run, calls = scripted(succeeded(limited))
    blocked = reads.run_case(CASE, "drpc", run, FakeRpc(), Path("."), sleep=lambda seconds: None)
    assert (blocked.status, blocked.attempts, len(calls)) == ("blocked", 3, 3)
    assert "exited 0 but reported: http: HTTP 429" in blocked.detail
    # Anything but an endpoint's refusal is the SDK's answer, and fails.
    changed = {"observation": None, "observation_error": "deployment_changed: unverified"}
    verdict = reads.judge(CASE, succeeded(changed), FakeRpc())
    assert verdict.status == "fail" and verdict.findings == ("deployment_changed: unverified",)
    # A case without a check does not pass on such output either.
    bare = reads.ReadCase("bare", "eip155:1", ("lending-reserve",))
    assert reads.judge(bare, succeeded(limited), FakeRpc()).status == "transient"
    assert reads.judge(bare, succeeded({"observation": {}, "observation_error": None}), FakeRpc())


def test_output_the_check_cannot_read_fails():
    verdict = reads.judge(CASE, succeeded({"product": "ax-balance"}), FakeRpc())
    assert verdict.status == "fail" and "not shaped" in verdict.detail


@pytest.mark.parametrize("error", sorted(reads.TRANSIENT_ERRORS))
def test_an_endpoint_refusal_is_transient(error):
    assert reads.judge(CASE, refused(error, "HTTP 429"), FakeRpc()).status == "transient"


@pytest.mark.parametrize("error", ["deployment_changed", "not_found", "chain_mismatch", "abi"])
def test_any_other_refusal_fails_without_a_retry(error):
    run, calls = scripted(refused(error, "no"))
    result = reads.run_case(CASE, "publicnode", run, FakeRpc(), Path("."), sleep=pytest.fail)
    assert (result.status, result.attempts, len(calls)) == ("fail", 1, 1)


def test_a_request_that_did_not_validate_reports_its_problems():
    problems = [{"field": "market", "message": "Field required"}]
    stderr = json.dumps({"error": "invalid_config", "problems": problems})
    verdict = reads.judge(CASE, invocation(1, stderr=stderr), FakeRpc())
    assert verdict.status == "fail" and "Field required" in verdict.detail


def test_a_timeout_is_transient_and_a_crash_fails():
    assert reads.judge(CASE, invocation(None, timed_out=True), FakeRpc()).status == "transient"
    crashed = reads.judge(CASE, invocation(1, stderr="Traceback\nValueError: x"), FakeRpc())
    assert crashed.status == "fail" and "without a JSON refusal" in crashed.detail


def test_an_event_line_on_standard_error_is_not_the_refusal():
    stderr = '{"error": "rpc", "message": "m"}\n{"event": "steps_sent"}\nnot json'
    assert invocation(1, stderr=stderr).refusal() == {"error": "rpc", "message": "m"}
    assert invocation(0, stdout="[1]").output() is None


def test_a_stated_refusal_passes_and_its_absence_fails():
    case = reads.ReadCase(
        "refusal",
        "eip155:1",
        ("resolve",),
        succeeds=False,
        refusals=(reads.Refusal("unavailable", "vendor key"),),
    )
    stated = reads.judge(case, refused("unavailable", "this lookup uses a vendor key"), FakeRpc())
    assert stated.status == "pass"
    # The same error without the stated text is the endpoint's, not the stated refusal.
    assert reads.judge(case, refused("unavailable", "no data"), FakeRpc()).status == "transient"
    assert reads.judge(case, succeeded({}), FakeRpc()).status == "fail"


def test_flake_is_retried_with_growing_waits_and_then_passes():
    run, calls = scripted(refused("http", "HTTP 429"), refused("rpc"), succeeded(native_output()))
    waits = []
    policy = reads.RetryPolicy(attempts=3, backoff_seconds=2.0)
    result = reads.run_case(CASE, "drpc", run, FakeRpc(), Path("."), policy, waits.append)
    assert (result.status, result.attempts, waits) == ("pass", 3, [2.0, 6.0])
    assert calls[0] == ("ax", "balance", "--public-rpc", "drpc")


def test_flake_on_every_attempt_is_blocked_not_failed():
    run, calls = scripted(refused("http", "HTTP 429"))
    result = reads.run_case(CASE, "drpc", run, FakeRpc(), Path("."), sleep=lambda seconds: None)
    assert (result.status, result.attempts, len(calls)) == ("blocked", 3, 3)
    assert "HTTP 429" in result.detail


def test_a_config_case_writes_its_request_file(tmp_path):
    case = reads.ReadCase("perp-position/x:y", "eip155:43114", ("perp-position",), config={"a": 1})
    arguments = reads.case_arguments(case, "publicnode", tmp_path)
    path = Path(arguments[arguments.index("--config") + 1])
    assert json.loads(path.read_text()) == {"a": 1} and path.parent == tmp_path
    # Endpoints run side by side, so each writes its own request file.
    other = reads.case_arguments(case, "drpc", tmp_path)
    assert path.name.startswith("publicnode-") and other[other.index("--config") + 1] != str(path)
    assert arguments[-2:] == ("--public-rpc", "publicnode")


ISSUE = reads.KnownIssue("pinned revision", "deployment_changed")
KNOWN = reads.ReadCase("known", "eip155:1", ("lending-reserve",), known_issue=ISSUE)


def result(case, status, detail="", endpoint="publicnode"):
    return reads.Result(case, endpoint, status, 1, 0.1, detail, "almanak ax")


def test_a_known_issue_still_fails_and_the_report_says_whether_it_reproduced():
    run, _ = scripted(refused("deployment_changed", "Aave V3 pool has an unverified revision"))
    reproduced = reads.run_case(KNOWN, "publicnode", run, FakeRpc(), Path("."))
    assert reproduced.status == "fail"
    assert reproduced.known == "reproduces: pinned revision"
    assert result(KNOWN, "pass").known.startswith("no longer reproduces, remove the marker")
    assert result(KNOWN, "fail", "abi: other").known.startswith("not observed this run")
    assert result(KNOWN, "blocked").known.startswith("not observed this run")
    assert result(CASE, "fail").known == ""


def test_a_new_finding_beside_a_known_issue_is_not_excused_by_the_marker():
    """Only a failure made of nothing but the known issue reproduces it."""
    known, new = "8 Spokes revert getReserveCount()", "market 0 asset: CLI 'a', direct read 'b'"
    issue = reads.KnownIssue("empty Spokes", "revert getReserveCount()")
    case = reads.ReadCase("listing", "eip155:43114", ("lending-markets",), known_issue=issue)

    def failed(*findings):
        return reads.Result(case, "publicnode", "fail", 1, 0.1, "; ".join(findings), "", findings)

    alone, mixed = failed(known), failed(known, new)
    assert alone.known == "reproduces: empty Spokes"
    assert mixed.known == "other failures beside the known issue: empty Spokes"
    assert reads.expectation(alone)[0] == "xfail" and reads.expectation(mixed)[0] == "fail"
    assert reads.exit_code([alone], known_issues_ok=True) == 0
    assert reads.exit_code([mixed], known_issues_ok=True) == 1
    # Every finding being the known issue still reproduces it, as an Aave V3 listing does.
    assert failed(known, known).known.startswith("reproduces")
    assert failed(new).known.startswith("not observed this run")


def test_a_check_reports_its_findings_separately_to_the_result():
    listed = listing(
        [market(spoke="0xspoke", reserve_id=3, hub=HUB)],
        [{"spoke": "0x1", "reason": "rpc: eth_call: RPC rejected the requested read"}],
    )
    issue = reads.KnownIssue("empty Spokes", "revert getReserveCount()")
    case = reads.ReadCase(
        "listing", "eip155:43114", ("lending-markets",), checks.aave_v4_listing, known_issue=issue
    )
    wrong_asset = {"getReserve(uint256)": ("0x" + "c3" * 20, HUB, 4, 18)}
    run, _ = scripted(succeeded(listed))
    rpc = FakeRpc(wrong_asset)
    rpc.reverts = lambda to, signature, block: True
    result = reads.run_case(case, "publicnode", run, rpc, Path("."))
    assert len(result.findings) == 2 and result.detail == "; ".join(result.findings)
    assert result.known.startswith("other failures beside the known issue")


def test_the_plan_runs_a_case_where_the_endpoint_serves_its_chain():
    only = reads.ReadCase("named", "eip155:8453", ("balance",), endpoints=("drpc",))
    other = reads.ReadCase("avalanche", "eip155:43114", ("balance",))
    serves = lambda endpoint, chain: (endpoint, chain) != ("drpc", "eip155:43114")  # noqa: E731
    plan = reads.planned((CASE, other, only), ("drpc", "publicnode"), serves)
    assert [(case.id, endpoint) for case, endpoint in plan] == [
        ("balance/native", "drpc"),
        ("balance/native", "publicnode"),
        ("avalanche", "publicnode"),
        ("named", "drpc"),
    ]


def session(run, tmp_path, reported=None):
    return reads.Session(
        run=run,
        rpc=lambda endpoint, chain: FakeRpc(),
        workdir=tmp_path,
        policy=reads.RetryPolicy(attempts=1),
        report=(reported if reported is not None else []).append,
    )


def test_results_come_back_in_plan_order_across_endpoints(tmp_path):
    run, _ = scripted(succeeded(native_output()))
    plan = [(CASE, "publicnode"), (CASE, "drpc"), (KNOWN, "publicnode")]
    reported = []
    results = reads.run_plan(plan, session(run, tmp_path, reported))
    assert [(item.case.id, item.endpoint) for item in results] == [
        ("balance/native", "publicnode"),
        ("balance/native", "drpc"),
        ("known", "publicnode"),
    ]
    assert len(reported) == 3 and reads.run_plan([], session(run, tmp_path)) == []


def test_an_endpoint_that_keeps_blocking_a_chain_is_not_asked_again(tmp_path):
    run, calls = scripted(refused("http", "HTTP 429"))
    cases = [reads.ReadCase(f"case-{index}", "eip155:43114", ("balance",)) for index in range(5)]
    cases.append(reads.ReadCase("other-chain", "eip155:1", ("balance",)))
    results = reads.run_lane(cases, "drpc", session(run, tmp_path))
    assert [item.status for item in results] == ["blocked"] * 6
    assert [item.attempts for item in results] == [1, 1, 1, 0, 0, 1]
    assert results[3].detail == "not run: drpc blocked the previous 3 cases on eip155:43114"
    assert len(calls) == 4


def test_a_passing_case_resets_the_breaker(tmp_path):
    blocked, passed = refused("http", "HTTP 429"), succeeded({})
    run, calls = scripted(blocked, blocked, passed, blocked, blocked, blocked)
    cases = [reads.ReadCase(f"case-{index}", "eip155:1", ("balance",)) for index in range(6)]
    results = reads.run_lane(cases, "drpc", session(run, tmp_path))
    assert [item.attempts for item in results] == [1] * 6 and len(calls) == 6


def test_the_exit_code_separates_failed_from_blocked_from_passed():
    passed, failed, blocked = (result(CASE, status) for status in reads.STATUSES)
    assert reads.exit_code([passed]) == 0
    assert reads.exit_code([passed, blocked]) == 2
    assert reads.exit_code([passed, blocked, failed]) == 1
    # A run that checked nothing is not a pass.
    assert reads.exit_code([]) == 1


def test_known_issues_can_be_kept_out_of_the_exit_code_but_nothing_else_can():
    reproduced = result(KNOWN, "fail", "deployment_changed: unverified revision")
    other = result(KNOWN, "fail", "abi: something new")
    passed, blocked = result(CASE, "pass"), result(CASE, "blocked")
    assert reads.exit_code([passed, reproduced]) == 1
    assert reads.exit_code([passed, reproduced], known_issues_ok=True) == 0
    assert reads.exit_code([blocked, reproduced], known_issues_ok=True) == 2
    # A known-issue case that fails another way, and any other failure, still count.
    assert reads.exit_code([passed, other], known_issues_ok=True) == 1
    assert reads.exit_code([reproduced, result(CASE, "fail")], known_issues_ok=True) == 1
    assert reads.exit_code([], known_issues_ok=True) == 1


def test_a_test_lane_expects_a_known_failure_and_refuses_a_stale_marker():
    reproduced = result(KNOWN, "fail", "deployment_changed: unverified revision")
    assert reads.expectation(reproduced)[0] == "xfail"
    stale = reads.expectation(result(KNOWN, "pass"))
    assert stale[0] == "fail" and "remove the marker" in stale[1]
    assert reads.expectation(result(KNOWN, "fail", "abi: new"))[0] == "fail"
    assert reads.expectation(result(CASE, "fail", "abi: new")) == ("fail", "unexpected: abi: new")
    assert reads.expectation(result(CASE, "pass", "ok")) == ("pass", "ok")
    assert reads.expectation(result(KNOWN, "blocked", "http: HTTP 429"))[0] == "skip"


def test_the_breaker_is_shared_by_every_case_a_session_runs(tmp_path):
    """The pytest lane runs cases one by one through the session, with the same breaker."""
    run, calls = scripted(refused("http", "HTTP 429"))
    shared = session(run, tmp_path)
    case = reads.ReadCase("case", "eip155:43114", ("balance",))
    attempts = [shared.one(case, "drpc").attempts for _ in range(5)]
    assert attempts == [1, 1, 1, 0, 0] and len(calls) == 3
    # Another endpoint, and another chain on the same endpoint, are still asked.
    assert shared.one(case, "publicnode").attempts == 1
    assert shared.one(CASE, "drpc").attempts == 1


def test_the_report_is_written_as_csv_and_markdown(tmp_path):
    results = [
        result(CASE, "pass", "read succeeded"),
        result(KNOWN, "fail", "deployment_changed: a | b"),
        result(CASE, "blocked", "http: HTTP 429", endpoint="drpc"),
    ]
    reads.write_reports(results, tmp_path, {"Started": "now"})
    with (tmp_path / "results.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    assert [row["status"] for row in rows] == ["pass", "fail", "blocked"]
    assert tuple(rows[0]) == reads.COLUMNS
    assert rows[1]["known_issue"] == "reproduces: pinned revision"
    summary = (tmp_path / "summary.md").read_text()
    assert "- Started: now" in summary
    assert "- Result: 1 pass, 1 fail (1 with a known issue), 1 blocked" in summary
    assert "deployment_changed: a \\| b" in summary
    assert "## Commands that ended fail" in summary and "## Commands that ended blocked" in summary
    assert "It is not a pass" in summary


# The catalog


def test_case_ids_are_unique_and_every_public_chain_is_covered():
    ids = [case.id for case in CASES]
    assert len(ids) == len(set(ids))
    served = {str(chain) for chains in PUBLIC_RPC.values() for chain in chains}
    assert served <= {case.chain for case in CASES}
    # Every listed endpoint has at least one case that reads through it.
    plan = reads.planned(CASES, sorted(PUBLIC_RPC), live_conftest.served)
    assert {endpoint for _, endpoint in plan} == set(PUBLIC_RPC)
    # A case's chain is a CAIP-2 reference, and no case is left without an endpoint to
    # run on, as a mistyped chain would leave it without failing anything.
    assert all(str(ChainId(case.chain)) == case.chain for case in CASES)
    assert {case.id for case, _ in plan} == set(ids)
    balances = {case.chain for case in CASES if case.id.startswith("balance/native/")}
    assert served <= balances


def test_a_chain_without_its_own_cases_gets_them_from_its_network_record():
    from scripts.live_qa.read_cases import chain_cases

    balance, reserve = chain_cases(ChainId("eip155:8453"))
    wrapped = "0x4200000000000000000000000000000000000006"
    assert balance.arguments[:5] == (
        "balance",
        "--asset",
        "eip155:8453/slip44:60",
        "--account",
        wrapped,
    )
    assert reserve.arguments[-1] == f"eip155:8453/erc20:{wrapped}"
    assert reserve.check is checks.pooled_reserve
    # Aave V3 has no Pool there, so only the balance is read.
    (only,) = chain_cases(ChainId("eip155:4663"))
    assert only.id == "balance/native/eip155:4663"


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.id)
def test_every_case_is_a_command_the_cli_accepts(case, tmp_path, monkeypatch):
    """A case is what a user types: it must parse, on a public endpoint, with no key."""
    for name in cli.WITHHELD:
        monkeypatch.delenv(name, raising=False)
    arguments = reads.case_arguments(case, "publicnode", tmp_path)
    args = parse_validated(build_parser(), list(arguments))
    assert args.public_rpc == "publicnode" and args.env_file is None


def test_the_bugs_found_by_hand_stay_in_the_catalog_as_known_issues():
    known = {case.id: case.known_issue for case in CASES if case.known_issue is not None}
    # Main admits the Aave V3 Pool revision the reads refused, so that marker is gone.
    assert not [name for name in known if "/aave_v3/" in name]
    gmx = [name for name in known if name.startswith("perp-markets/gmx_v2/avalanche")]
    assert known["lending-markets/aave_v4/avalanche"].signature == "revert getReserveCount()"
    assert len(gmx) >= 2
    queries = {case.arguments[-1] for case in CASES if case.id in gmx}
    assert {"BTC/USD", "AVAX/USD"} <= queries
    # The names the venue really uses are held as passing controls beside them.
    controls = {case.arguments[-1] for case in CASES if "gmx_v2" in case.id} - queries
    assert {"BTC.b/USD [BTC.b-USDC]", "WAVAX/USD [WAVAX-USDC]"} <= controls


def test_a_known_issue_case_states_the_correct_behavior_not_the_bug():
    for case in CASES:
        if case.known_issue is None:
            continue
        accepted = {refusal.error for refusal in case.refusals}
        assert case.succeeds and case.known_issue.signature not in accepted, case.id


# Checks against a direct read


def test_an_erc20_balance_is_compared_with_balance_of_and_decimals():
    observation = {
        "asset": USDC,
        "account": {"address": ACCOUNT},
        "block": BLOCK,
        "amount": "9",
        "decimals": 6,
    }
    output = {"observations": [observation]}
    agreeing = FakeRpc({"balanceOf(address)": (9,), "decimals()": (6,)})
    check = checks.erc20_balance(USDC, ACCOUNT)
    assert check(output, agreeing) == []
    differing = FakeRpc({"balanceOf(address)": (9,), "decimals()": (18,)})
    assert check(output, differing) == ["decimals: CLI 6, direct read 18"]


def test_a_balance_of_another_account_or_asset_fails_before_any_direct_read():
    """A correct balance of the wrong account would agree with a direct read of it."""
    other = "0x" + "22" * 20
    unread = FakeRpc(down=True)
    observation = native_output()["observations"][0]
    for reported, check, finding in (
        ({"account": {"address": other}}, CASE.check, f"account: CLI '{other}'"),
        ({"account": {"address": None}}, CASE.check, "account: CLI None"),
        ({}, checks.erc20_balance(USDC, ACCOUNT), "asset: CLI 'eip155:1/slip44:60'"),
    ):
        (difference,) = check({"observations": [{**observation, **reported}]}, unread)
        assert difference.startswith(finding) and ", requested " in difference
    # The same address in another letter case is the same account.
    checksummed = {**observation, "account": {"address": "0x" + "AB" * 20}}
    check = checks.native_balance(NATIVE, "0x" + "ab" * 20)
    assert check({"observations": [checksummed]}, FakeRpc()) == []


MARKET_ASSET = "0x" + "a1" * 20
HUB = "0x" + "b2" * 20


def listing(markets, unreadable=()):
    source = {"contract": "0xpool", "listed": 2, "unreadable": list(unreadable)}
    return {"source": source, "block": BLOCK, "markets": list(markets)}


def market(status="verified", **fields):
    asset = {"asset": f"eip155:1/erc20:{MARKET_ASSET}", "symbol": "TKN"}
    return {"status": status, "asset": asset, **fields}


def test_a_listing_is_complete_or_it_is_blocked_or_failed():
    refused_market = market("refused", reason="deployment_changed: unverified revision")
    assert checks.listed_markets(listing([market()]), FakeRpc()) == []
    assert checks.listed_markets(listing([]), FakeRpc()) == ["the listing reports no markets"]
    assert checks.listed_markets(listing([market(), refused_market]), FakeRpc()) == [
        "market 1 is refused: deployment_changed: unverified revision"
    ]
    # What the endpoint did not answer is not a finding about the listing.
    for partial in (
        listing([market()], [{"spoke": "0x1", "reason": "rpc: eth_call: rejected"}]),
        listing([market()], [{"hub": "0x2", "reason": "rpc: eth_call: rejected"}]),
        listing([market("refused", reason="http: HTTP 429")]),
    ):
        with pytest.raises(RpcUnavailable, match="1 entries unreadable"):
            checks.listed_markets(partial, FakeRpc())


def test_a_spoke_called_unreadable_that_reverts_on_a_direct_read_is_a_finding():
    """The endpoint answered: the Spoke holds no reserves. That is not a blocked read."""
    unreadable = [
        {"spoke": "0x1", "reason": "rpc: eth_call: RPC rejected the requested read"},
        {"spoke": "0x2", "reason": "rpc: eth_call: RPC rejected the requested read"},
    ]
    partial = listing([market()], unreadable)
    (finding,) = checks.listed_markets(partial, FakeRpc(reverting=("0x1", "0x2")))
    assert finding.startswith("2 Spokes are reported unreadable, but they revert getReserveCount()")
    assert finding.endswith("0x1, 0x2")
    # One Spoke the endpoint really did not answer still blocks the case.
    with pytest.raises(RpcUnavailable, match="1 entries unreadable"):
        checks.listed_markets(partial, FakeRpc(reverting=("0x1",)))
    with pytest.raises(RpcUnavailable):
        checks.listed_markets(partial, FakeRpc(down=True))


def test_a_pool_listing_is_held_to_the_reserves_the_pool_itself_lists():
    output = listing([market()])
    two = FakeRpc({"getReservesList()": ((MARKET_ASSET.upper().replace("0X", "0x"), "0xb"),)})
    assert checks.pool_reserve_listing(output, two) == []
    three = FakeRpc({"getReservesList()": (("0xa", "0xb", "0xc"),)})
    assert checks.pool_reserve_listing(output, three) == [
        "listed reserves: CLI 2, direct read 3",
        f"market 0 asset eip155:1/erc20:{MARKET_ASSET} is not on the Pool's reserve list",
    ]


def test_an_aave_v4_market_is_held_to_what_its_spoke_records():
    """A listing or reserve naming the wrong Spoke or reserve ID does not pass on decimals."""
    listed = listing([market(spoke="0xspoke", reserve_id=3, hub=HUB)])
    agreeing = FakeRpc({"getReserve(uint256)": (MARKET_ASSET, HUB, 4, 18), "decimals()": (18,)})
    other = FakeRpc({"getReserve(uint256)": ("0x" + "c3" * 20, HUB, 4, 18), "decimals()": (18,)})
    assert checks.aave_v4_listing(listed, agreeing) == []
    (difference,) = checks.aave_v4_listing(listed, other)
    assert difference.startswith("market 0 asset: CLI")
    reserve = {
        "resolution": {
            "selection": {"spoke": "0xspoke", "reserve_id": 3},
            "asset": f"eip155:1/erc20:{MARKET_ASSET}",
            "hub": HUB,
            "hub_asset_id": 4,
            "decimals": 18,
        },
        "observation": {"block": BLOCK},
    }
    assert checks.aave_v4_reserve(reserve, agreeing) == []
    (difference,) = checks.aave_v4_reserve(reserve, other)
    assert difference.startswith("asset: CLI")
    wrong_slot = FakeRpc({"getReserve(uint256)": (MARKET_ASSET, HUB, 9, 18), "decimals()": (18,)})
    assert checks.aave_v4_reserve(reserve, wrong_slot) == ["hub_asset_id: CLI 4, direct read 9"]


def test_a_morpho_listing_is_held_to_the_core_and_may_refuse_some_markets_not_all():
    check = checks.morpho_listing("0xcore")
    verified = market(market_id="0x" + "0d" * 32, loan_asset=market()["asset"])
    refused_market = market("refused", reason="revision: runtime differs")
    agreeing = FakeRpc({"idToMarketParams(bytes32)": (MARKET_ASSET,)})
    assert check(listing([verified, refused_market]), agreeing) == []
    assert check(listing([refused_market]), agreeing) == ["the listing reports no verified market"]
    other = FakeRpc({"idToMarketParams(bytes32)": ("0x" + "c3" * 20,)})
    (difference,) = check(listing([verified]), other)
    assert difference.startswith("loan asset of 0x0d0d0d0d: CLI")
    with pytest.raises(RpcUnavailable):
        check(listing([market("refused", reason="http: HTTP 429")]), agreeing)


def test_a_gmx_lookup_must_name_the_expected_share_and_symbol():
    share = "0xfb02132333a79c8b5bd0b64e3abcca5f7faf2937"
    observation = {
        "share": f"eip155:43114/erc20:{share}",
        "index": "eip155:43114/erc20:0x152b9d0fdc40c096757f570a51e494bd4b943e50",
        "index_symbol": "BTC.b",
        "block": 7,
    }
    check = checks.gmx_market(share, "BTC.b")
    assert check({"observation": observation}, FakeRpc({"symbol()": ("BTC.b",)})) == []
    assert check({"observation": observation}, FakeRpc({"symbol()": ("WBTC",)})) == [
        "index symbol(): CLI 'BTC.b', direct read 'WBTC'"
    ]
    assert checks.hypercore_market("BTC")({"markets": [{"target": {"coin": "ETH"}}]}, FakeRpc())


def test_a_uniswap_v4_pool_id_is_the_hash_of_its_key():
    source = {
        "currency0": USDC,
        "currency1": "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "fee": 500,
        "tick_spacing": 10,
        "hooks": "0x" + "00" * 20,
    }
    # The hookless USDC/WETH 0.05% pool, as the SDK's own fixtures record its ID.
    assert checks.v4_pool_id(source).hex() == (
        "4f88f7c99022eace4740c6898f59ce6a2e798a1e64ce54589720b7153eb224a7"
    )


# Direct RPC client


def rpc_client(handler):
    return Rpc("https://rpc.invalid", client=httpx.Client(transport=httpx.MockTransport(handler)))


def test_a_direct_call_encodes_the_selector_and_decodes_the_answer():
    seen = []

    def handler(request):
        body = json.loads(request.content)
        seen.append(body)
        results = {"eth_call": "0x" + "00" * 31 + "06", "eth_getBalance": "0x10"}
        result = results.get(body["method"], {"hash": "0xcc"})
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": result})

    rpc = rpc_client(handler)
    assert rpc.call("0xtoken", "decimals()", 255, ("uint8",)) == (6,)
    assert seen[0]["params"] == [{"to": "0xtoken", "data": "0x313ce567"}, "0xff"]
    rpc.call("0xtoken", "balanceOf(address)", 1, ("uint256",), (ACCOUNT,))
    assert seen[1]["params"][0]["data"] == "0x70a08231" + "00" * 12 + "11" * 20
    assert rpc.balance(ACCOUNT, 1) == 16 and rpc.block_hash(1) == "0xcc"


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(429, text="slow down"),
        httpx.Response(200, text="<html>"),
        httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "error": {"code": -32000}}),
        httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "0x"}),
    ],
)
def test_an_endpoint_that_does_not_answer_is_unavailable_not_wrong(response):
    with pytest.raises(RpcUnavailable):
        rpc_client(lambda request: response).call("0xtoken", "decimals()", 1, ("uint8",))


@pytest.mark.parametrize(
    "error",
    [
        # Avalanche's own API, then publicnode and dRPC, for the same reverting call.
        {"code": 3, "message": "execution reverted", "data": "0x"},
        {"code": -32000, "message": "execution reverted"},
    ],
)
def test_a_revert_is_an_answer_whichever_code_the_endpoint_gives_it(error):
    answer = httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "error": error})
    rpc = rpc_client(lambda request: answer)
    with pytest.raises(RpcReverted):
        rpc.call("0xspoke", "getReserveCount()", 1, ("uint256",))
    assert rpc.reverts("0xspoke", "getReserveCount()", 1) is True


def test_only_a_revert_counts_as_reverting():
    count = httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "0x" + "00" * 32})
    assert rpc_client(lambda request: count).reverts("0xspoke", "getReserveCount()", 1) is False
    limited = {"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "rate limited"}}
    throttled = rpc_client(lambda request: httpx.Response(200, json=limited))
    with pytest.raises(RpcUnavailable) as raised:
        throttled.reverts("0xspoke", "getReserveCount()", 1)
    assert not isinstance(raised.value, RpcReverted)


def test_a_run_keeps_one_direct_client_per_endpoint_and_chain():
    from scripts.live_qa.rpc import direct_clients

    client = direct_clients()
    first = client("publicnode", "eip155:1")
    assert client("publicnode", "eip155:1") is first
    assert client("drpc", "eip155:1") is not first
    assert client("publicnode", "eip155:43114") is not first
    assert first.url == "https://ethereum-rpc.publicnode.com"
    assert client("publicnode", "eip155:999").url is None


def test_a_chain_the_endpoint_does_not_serve_has_no_direct_client():
    assert endpoint_url("publicnode", "eip155:1") == "https://ethereum-rpc.publicnode.com"
    assert endpoint_url("publicnode", "eip155:8453") is None
    with pytest.raises(RpcUnavailable, match="does not serve"):
        Rpc(None).balance(ACCOUNT, 1)
    assert asset_address(USDC) == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    with pytest.raises(ValueError, match="not an ERC-20 asset"):
        asset_address("eip155:1/slip44:60")


# The command under test and the entry point


def test_the_command_runs_without_a_provider_key_or_the_session_gateway(tmp_path, monkeypatch):
    monkeypatch.setenv("ALCHEMY_API_KEY", "test-key")
    monkeypatch.setenv("ALMANAK_AX_GATEWAY_URL", "http://127.0.0.1:1")
    script = f"import os, json; print(json.dumps(sorted(set(os.environ) & set({cli.WITHHELD!r}))))"
    run = cli.runner((sys.executable, "-c", script), tmp_path)
    done = run(())
    assert (done.exit_code, done.stdout.strip(), done.timed_out) == (0, "[]", False)
    slow = cli.runner((sys.executable, "-c", "import time; time.sleep(30)"), tmp_path)
    assert slow((), 0.2).timed_out
    assert Path(cli.default_command()[0]).name == "almanak"


def test_listing_the_plan_contacts_nothing_and_writes_nothing(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(reads, "run_plan", lambda plan, session: pytest.fail("must not run"))
    output = tmp_path / "report"
    code = entry.main(["reads", "--list", "--output", str(output), "--case", "gmx_v2"])
    lines = capsys.readouterr().out.splitlines()
    assert code == 0 and not output.exists()
    # The plan writes nothing, so it needs no output directory; a run does.
    assert entry.main(["reads", "--list", "--case", "gmx_v2"]) == 0
    assert capsys.readouterr().out.splitlines() == lines
    with pytest.raises(SystemExit, match="--output is required"):
        entry.main(["reads", "--case", "gmx_v2"])
    assert lines and all("gmx_v2" in line for line in lines)
    # GMX cases are on Avalanche, which one listed endpoint serves.
    assert {line.split()[0] for line in lines} == {"publicnode"}
    assert entry.main(["reads", "--list"]) == 0
    assert {line.split()[0] for line in capsys.readouterr().out.splitlines()} == set(PUBLIC_RPC)


def test_a_run_writes_its_report_into_a_new_directory_only(tmp_path, capsys, monkeypatch):
    seen = {}

    def run_plan(plan, session):
        seen["policy"], seen["plan"] = session.policy, plan
        return [result(case, "pass", endpoint=endpoint) for case, endpoint in plan]

    monkeypatch.setattr(reads, "run_plan", run_plan)
    output = tmp_path / "report"
    arguments = ["reads", "--output", str(output), "--endpoint", "publicnode"]
    code = entry.main([*arguments, "--case", "balance/native", "--attempts", "2"])
    assert code == 0 and seen["policy"] == reads.RetryPolicy(2, 5.0, 90.0)
    assert {endpoint for _, endpoint in seen["plan"]} == {"publicnode"}
    assert (output / "results.csv").is_file() and (output / "summary.md").is_file()
    assert "2 pass, 0 fail, 0 blocked" in capsys.readouterr().out
    # The report names the checkout by version, not by this machine's path.
    assert "this checkout's `almanak` (" in (output / "summary.md").read_text()
    assert entry.command_label(["uvx", "almanak"]) == "`uvx almanak`"
    with pytest.raises(SystemExit, match="is not empty"):
        entry.main(arguments)


def test_the_entry_point_can_leave_known_issues_out_of_its_exit_code(tmp_path, monkeypatch):
    reproduced = result(KNOWN, "fail", "deployment_changed: unverified revision")
    monkeypatch.setattr(reads, "run_plan", lambda plan, session: [reproduced])
    arguments = ["reads", "--case", "balance/native"]
    assert entry.main([*arguments, "--output", str(tmp_path / "strict")]) == 1
    lenient = tmp_path / "lenient"
    assert entry.main([*arguments, "--output", str(lenient), "--known-issues-ok"]) == 0
    # The report does not hide what the exit code leaves out.
    assert "1 fail (1 with a known issue)" in (lenient / "summary.md").read_text()


def test_progress_lines_name_the_reason_only_when_a_case_did_not_pass(capsys):
    entry.announce(result(CASE, "pass", "read succeeded"))
    entry.announce(result(CASE, "blocked", "http: HTTP 429"))
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].split() == ["pass", "publicnode", "balance/native"]
    assert lines[1].endswith("balance/native: http: HTTP 429")


# Opt-in: never part of the default suite or of hosted CI


class Config:
    """The pytest configuration as the live conftest asks it for the opt-in flag."""

    def __init__(self, live):
        self.live = live

    def getoption(self, name):
        assert name == "--live-public"
        return self.live


def test_the_live_directory_is_not_collected_without_the_flag():
    assert live_conftest.pytest_ignore_collect(ROOT / "tests/live", Config(False)) is True
    assert live_conftest.pytest_ignore_collect(ROOT / "tests/live", Config(True)) is None
    assert live_conftest.served("publicnode", "eip155:1")
    assert not live_conftest.served("publicnode", "eip155:999")


def test_a_live_test_named_by_its_path_is_skipped_without_the_flag():
    """pytest collects a file named on its command line without asking the ignore hook."""
    with pytest.raises(pytest.skip.Exception, match="need --live-public"):
        live_conftest.require_opt_in(Config(False))
    assert live_conftest.require_opt_in(Config(True)) is None
    source = (ROOT / "tests/live/conftest.py").read_text()
    fixture = source.split("def live_session(")[1]
    # The check comes before anything that could reach an endpoint.
    assert fixture.index("require_opt_in(request.config)") < fixture.index("reads.Session(")


def test_default_runs_and_the_makefile_never_pass_the_live_flag():
    assert "--live-public" not in (ROOT / "Makefile").read_text()
    assert "live_qa" not in (ROOT / "Makefile").read_text()
    addopts = (ROOT / "pyproject.toml").read_text().split("addopts = ")[1].splitlines()[0]
    assert "live" not in addopts


WORKFLOWS = ROOT / ".github/workflows"
OPTIONAL = "live_public_reads.yml"


def test_no_other_workflow_runs_the_live_checks():
    for path in WORKFLOWS.glob("*.yml"):
        if path.name != OPTIONAL:
            text = path.read_text()
            assert "live_qa" not in text and "--live-public" not in text, path.name


def test_the_optional_workflow_is_manual_only_and_reads_only():
    text = (WORKFLOWS / OPTIONAL).read_text()
    triggers = yaml.load(text, Loader=yaml.BaseLoader)["on"]
    assert set(triggers) == {"workflow_dispatch"}
    assert "scripts.live_qa reads" in text
    # A dispatched run is red for a new failure, not for the known ones it also reports.
    assert text.count("--known-issues-ok") == text.count("scripts.live_qa reads") == 2
    # No write level, no spend flag, no wallet and no secret reaches a hosted runner.
    for forbidden in ("live_qa writes", "ALMANAK_QA_SPEND_REAL_FUNDS", "--wallet", "secrets."):
        assert forbidden not in text, forbidden
