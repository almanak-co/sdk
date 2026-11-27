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

from almanak.caip import AssetType, ChainId
from almanak.cli_arguments import build_parser, parse_validated
from almanak.gateway.providers import PUBLIC_RPC
from scripts.live_qa import __main__ as entry
from scripts.live_qa import checks, cli, reads
from scripts.live_qa.read_cases import CASES
from scripts.live_qa.rpc import Rpc, RpcReverted, RpcUnavailable, asset_address, endpoint_url
from tests.live import conftest as live_conftest

ROOT = Path(__file__).parents[1]
ETHEREUM, AVALANCHE = ChainId("eip155:1"), ChainId("eip155:43114")
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
        answer = self.answers[signature]
        if isinstance(answer, Exception):
            raise answer
        return answer

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
    "balance/native", ETHEREUM, ("balance",), checks.native_balance(AssetType(NATIVE), ACCOUNT)
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
    check = checks.erc20_balance(AssetType(USDC), ACCOUNT)
    case = reads.ReadCase("balance/erc20", ETHEREUM, ("balance",), check)
    run, calls = scripted(succeeded(output))
    rpc = FakeRpc(reverting=("balanceOf(address)",))
    result = reads.run_case(case, "publicnode", run, rpc, Path("."), sleep=pytest.fail)
    assert (result.status, result.attempts, len(calls)) == ("fail", 1, 1)
    assert "a direct read of what the command reported reverted" in result.detail
    assert reads.expectation(result)[0] == "fail"


def test_a_balance_read_without_observations_compares_nothing_and_fails():
    for check in (
        checks.native_balance(AssetType(NATIVE), ACCOUNT),
        checks.erc20_balance(AssetType(USDC), ACCOUNT),
    ):
        case = reads.ReadCase("balance", ETHEREUM, ("balance",), check)
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
    bare = reads.ReadCase("bare", ETHEREUM, ("lending-reserve",))
    assert reads.judge(bare, succeeded(limited), FakeRpc()).status == "transient"
    valid = succeeded({"observation": {}, "observation_error": None})
    assert reads.judge(bare, valid, FakeRpc()).status == "pass"


def test_output_the_check_cannot_read_fails():
    verdict = reads.judge(CASE, succeeded({"product": "ax-balance"}), FakeRpc())
    assert verdict.status == "fail" and "not shaped" in verdict.detail


def test_a_reported_value_no_call_can_encode_fails_instead_of_stopping_the_run():
    """A null hook reaches the ABI encoder of the pool ID, before any request."""
    source = {
        "currency0": USDC,
        "currency1": "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "fee": 500,
        "tick_spacing": 10,
        "hooks": None,
        "state_view": "0x" + "33" * 20,
    }
    output = {"observation": {"source": source, "block": BLOCK}}
    # Requested with the same null hook, so the identity check lets it reach the encoder.
    check = checks.v4_pool_price(source["currency0"], source["currency1"], 500, 10, None)
    case = reads.ReadCase("pool-price", ETHEREUM, ("pool-price",), check)
    verdict = reads.judge(case, succeeded(output), Rpc(None))
    assert verdict.status == "fail" and "EncodingTypeError" in verdict.detail


@pytest.mark.parametrize("source", [None, [], "listing"])
def test_a_listing_whose_source_is_not_an_object_fails_instead_of_stopping_the_run(source):
    """A null source would raise AttributeError inside the check, past the case boundary."""
    output = {"source": source, "block": BLOCK, "markets": []}
    for check in (checks.aave_v4_listing(ETHEREUM), checks.pool_reserve_listing(ETHEREUM, 3)):
        case = reads.ReadCase("listing", ETHEREUM, ("lending-markets",), check)
        run, _ = scripted(succeeded(output))
        result = reads.run_case(case, "publicnode", run, FakeRpc(), Path("."), sleep=pytest.fail)
        assert (result.status, result.attempts) == ("fail", 1)
        assert "output is not shaped as the check reads it" in result.detail


SPOKE = "0x" + "33" * 20


def position_output(spoke=SPOKE, reserve_id=0, account=ACCOUNT, supplied=0):
    return {
        "resolution": {"selection": {"spoke": spoke, "reserve_id": reserve_id}},
        "observation": {
            "reserve": {"block": BLOCK},
            "account": {"address": account},
            "supplied_assets": supplied,
        },
    }


def test_an_aave_v4_position_is_read_for_the_requested_account_spoke_and_reserve():
    """Another account's zero position would agree with a direct read of that account."""
    asked = []

    class Recording(FakeRpc):
        def call(self, to, signature, block, outputs, args=()):
            asked.append((to, args))
            return super().call(to, signature, block, outputs, args)

    rpc = Recording({"getUserSuppliedAssets(uint256,address)": (0,)})
    check = checks.aave_v4_position(SPOKE, 0, ACCOUNT)
    # A zero position of the requested account passes, in any letter case.
    assert check(position_output(account=ACCOUNT.upper().replace("0X", "0x")), rpc) == []
    assert asked == [(SPOKE, (0, ACCOUNT))]
    other = "0x" + "22" * 20
    for reported, finding in (
        (position_output(account=other), "account: CLI"),
        (position_output(account=None), "account: CLI None"),
        (position_output(spoke=other), "spoke: CLI"),
        (position_output(reserve_id=1), "reserve_id: CLI 1, requested 0"),
    ):
        (difference,) = check(reported, FakeRpc(down=True))
        assert difference.startswith(finding) and ", requested " in difference
    held = FakeRpc({"getUserSuppliedAssets(uint256,address)": (5,)})
    assert check(position_output(), held) == ["supplied_assets: CLI 0, direct read 5"]


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
        ETHEREUM,
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
    case = reads.ReadCase("perp-position/x:y", AVALANCHE, ("perp-position",), config={"a": 1})
    arguments = reads.case_arguments(case, "publicnode", tmp_path)
    path = Path(arguments[arguments.index("--config") + 1])
    assert json.loads(path.read_text()) == {"a": 1} and path.parent == tmp_path
    # Endpoints run side by side, so each writes its own request file.
    other = reads.case_arguments(case, "drpc", tmp_path)
    assert path.name.startswith("publicnode-") and other[other.index("--config") + 1] != str(path)
    assert arguments[-2:] == ("--public-rpc", "publicnode")


ISSUE = reads.KnownIssue("pinned revision", "deployment_changed", refusal="deployment_changed")
KNOWN = reads.ReadCase("known", ETHEREUM, ("lending-reserve",), known_issue=ISSUE)


def result(case, status, detail="", endpoint="publicnode"):
    """A failure written `kind: message` is a refusal of that kind, as `judge_refusal` writes it."""
    refusal = detail.partition(":")[0] if status == "fail" and ":" in detail else None
    return reads.Result(case, endpoint, status, 1, 0.1, detail, "almanak ax", (), refusal)


def test_a_known_issue_still_fails_and_the_report_says_whether_it_reproduced():
    run, _ = scripted(refused("deployment_changed", "Aave V3 pool has an unverified revision"))
    reproduced = reads.run_case(KNOWN, "publicnode", run, FakeRpc(), Path("."))
    assert reproduced.status == "fail"
    assert reproduced.known == "reproduces: pinned revision"
    assert result(KNOWN, "pass").known.startswith("no longer reproduces, remove the marker")
    assert result(KNOWN, "fail", "abi: other").known.startswith("not observed this run")
    assert result(KNOWN, "blocked").known.startswith("not observed this run")
    assert result(CASE, "fail").known == ""


def test_a_known_issue_is_matched_by_its_refusal_kind_not_by_text_alone():
    """A refusal of another kind whose message holds the signature is a new failure."""
    issue = reads.KnownIssue("unindexed labels", "not_found", refusal="not_found")
    case = reads.ReadCase("perp-markets", AVALANCHE, ("perp-markets",), known_issue=issue)

    def ran(error, message):
        run, _ = scripted(refused(error, message))
        return reads.run_case(case, "publicnode", run, FakeRpc(), Path("."), sleep=pytest.fail)

    reproduced = ran("not_found", "no GMX market matches BTC/USD")
    assert reproduced.refusal == "not_found" and reproduced.known.startswith("reproduces")
    other = ran("malformed", "label not_found in the index")
    assert other.status == "fail" and other.known.startswith("not observed this run")
    assert reads.exit_code([other], known_issues_ok=True) == 1
    # A check's finding is not a refusal, so an issue stated as a refusal never matches it.
    finding = reads.Result(case, "publicnode", "fail", 1, 0.1, "not_found", "", ("not_found",))
    assert finding.known.startswith("not observed this run")
    # An issue stated as a finding is not reproduced by a refusal holding its text.
    spokes = reads.KnownIssue("empty Spokes", "revert getReserveCount()")
    listing_case = reads.ReadCase("listing", AVALANCHE, ("lending-markets",), known_issue=spokes)
    run, _ = scripted(refused("abi", "revert getReserveCount() undecodable"))
    refusal = reads.run_case(listing_case, "publicnode", run, FakeRpc(), Path("."))
    assert refusal.known.startswith("not observed this run")


def test_a_refusal_whose_error_is_not_a_kind_fails_instead_of_stopping_the_lane():
    run, calls = scripted(invocation(1, stderr=json.dumps({"error": [], "message": "bad"})))
    result = reads.run_case(CASE, "publicnode", run, FakeRpc(), Path("."), sleep=pytest.fail)
    assert (result.status, len(calls)) == ("fail", 1) and "malformed refusal" in result.detail


def test_a_new_finding_beside_a_known_issue_is_not_excused_by_the_marker():
    """Only a failure made of nothing but the known issue reproduces it."""
    known, new = "8 Spokes revert getReserveCount()", "market 0 asset: CLI 'a', direct read 'b'"
    issue = reads.KnownIssue("empty Spokes", "revert getReserveCount()")
    case = reads.ReadCase("listing", AVALANCHE, ("lending-markets",), known_issue=issue)

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
        "listing",
        AVALANCHE,
        ("lending-markets",),
        checks.aave_v4_listing(ETHEREUM),
        known_issue=issue,
    )
    wrong_asset = {"getReserve(uint256)": ("0x" + "c3" * 20, HUB, 4, 18)}
    run, _ = scripted(succeeded(listed))
    rpc = FakeRpc(wrong_asset)
    rpc.reverts = lambda to, signature, block: True
    result = reads.run_case(case, "publicnode", run, rpc, Path("."))
    assert len(result.findings) == 2 and result.detail == "; ".join(result.findings)
    assert result.known.startswith("other failures beside the known issue")


def test_the_plan_runs_a_case_where_the_endpoint_serves_its_chain():
    only = reads.ReadCase("named", ChainId("eip155:8453"), ("balance",), endpoints=("drpc",))
    other = reads.ReadCase("avalanche", AVALANCHE, ("balance",))
    serves = lambda endpoint, chain: (endpoint, chain) != ("drpc", AVALANCHE)  # noqa: E731
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
    cases = [reads.ReadCase(f"case-{index}", AVALANCHE, ("balance",)) for index in range(5)]
    cases.append(reads.ReadCase("other-chain", ETHEREUM, ("balance",)))
    results = reads.run_lane(cases, "drpc", session(run, tmp_path))
    assert [item.status for item in results] == ["blocked"] * 6
    assert [item.attempts for item in results] == [1, 1, 1, 0, 0, 1]
    assert results[3].detail == "not run: drpc blocked the previous 3 cases on eip155:43114"
    assert len(calls) == 4


def test_a_passing_case_resets_the_breaker(tmp_path):
    blocked, passed = refused("http", "HTTP 429"), succeeded({})
    run, calls = scripted(blocked, blocked, passed, blocked, blocked, blocked)
    cases = [reads.ReadCase(f"case-{index}", ETHEREUM, ("balance",)) for index in range(6)]
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
    case = reads.ReadCase("case", AVALANCHE, ("balance",))
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
    assert served <= {str(case.chain) for case in CASES}
    # Every listed endpoint has at least one case that reads through it.
    plan = reads.planned(CASES, sorted(PUBLIC_RPC), live_conftest.served)
    assert {endpoint for _, endpoint in plan} == set(PUBLIC_RPC)
    # A case's chain is a CAIP-2 reference, and no case is left without an endpoint to
    # run on, as a mistyped chain would leave it without failing anything.
    assert all(isinstance(case.chain, ChainId) for case in CASES)
    assert {case.id for case, _ in plan} == set(ids)
    balances = {str(case.chain) for case in CASES if case.id.startswith("balance/native/")}
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
    # The check is bound to the requested asset and to the Pool Aave V3 pins on Base.
    other = {
        "resolution": {"pool": "0xa238dd80c259a72e81d7e4664a9801593f98d1c5", "asset": USDC},
        "observation": {"block": BLOCK},
    }
    assert reserve.check is not None
    (difference,) = reserve.check(other, FakeRpc(down=True))
    assert difference.startswith("asset: CLI") and wrapped in difference
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


CATALOG = {case.id: case for case in CASES}
GMX_BTC_USDC = "0xfb02132333a79c8b5bd0b64e3abcca5f7faf2937"
GMX_WAVAX_USDC = "0x913c1f46b48b3ed35e7dc3cf754d4ae8499f31cf"


def test_the_defects_main_fixed_carry_no_known_issue_marker():
    """Main admits the Aave V3 Pool revision, reads the Aave V4 Spokes without reserves
    and resolves the GMX Avalanche venue labels, so none of those cases carries a marker."""
    known = {case.id: case.known_issue for case in CASES if case.known_issue is not None}
    fixed = ("/aave_v3/", "lending-markets/aave_v4/", "perp-markets/gmx_v2/avalanche")
    assert not [name for name in known if any(part in name for part in fixed)]
    signatures = {issue.signature for issue in known.values()}
    assert not signatures & {"revert getReserveCount()", "not_found", "deployment_changed"}


def test_the_aave_v4_avalanche_listing_is_an_ordinary_complete_listing():
    """A Spoke that reverts `getReserveCount()` is counted in `source.without_reserves`;
    the listing names nothing unreadable and every market it lists is verified."""
    case = CATALOG["lending-markets/aave_v4/avalanche"]
    assert case.succeeds and case.refusals == () and case.known_issue is None
    assert case.check is not None
    avalanche_asset = {"asset": f"eip155:43114/erc20:{MARKET_ASSET}", "symbol": "TKN"}
    listed = listing([market(spoke="0xspoke", reserve_id=3, hub=HUB, asset=avalanche_asset)])
    listed["source"]["without_reserves"] = 4
    agreeing = FakeRpc({"getReserve(uint256)": (MARKET_ASSET, HUB, 4, 18), "decimals()": (18,)})
    assert case.check(listed, agreeing) == []
    # An empty Spoke named unreadable again is the defect main fixed, and fails as a finding.
    misread = {**listed, "source": {**listed["source"], "unreadable": [{"spoke": "0x1"}]}}
    (finding,) = case.check(misread, FakeRpc(agreeing.answers, reverting=("0x1",)))
    assert "revert getReserveCount()" in finding and "0x1" in finding


def gmx_observation(share, symbol):
    index = {"BTC.b": "0x" + "15" * 20, "WAVAX": "0x" + "b3" * 20}[symbol]
    return {
        "observation": {
            "share": f"eip155:43114/erc20:{share}",
            "index": f"eip155:43114/erc20:{index}",
            "index_symbol": symbol,
            "block": 7,
        }
    }


@pytest.mark.parametrize(
    ("case_id", "query", "share", "symbol"),
    [
        ("avalanche-btc-usd-label", "BTC/USD [BTC.b-USDC]", GMX_BTC_USDC, "BTC.b"),
        ("avalanche-avax-usd-venue-label", "AVAX/USD [AVAX-USDC]", GMX_WAVAX_USDC, "WAVAX"),
        # The on-chain symbols stay passing controls beside the venue's labels.
        ("avalanche-btc.b-label", "BTC.b/USD [BTC.b-USDC]", GMX_BTC_USDC, "BTC.b"),
        ("avalanche-wavax-label", "WAVAX/USD [WAVAX-USDC]", GMX_WAVAX_USDC, "WAVAX"),
    ],
)
def test_a_gmx_avalanche_label_with_its_collateral_resolves_the_one_market(
    case_id, query, share, symbol
):
    case = CATALOG[f"perp-markets/gmx_v2/{case_id}"]
    assert case.arguments[-1] == query and case.succeeds and case.known_issue is None
    assert case.check is not None
    named = FakeRpc({"symbol()": (symbol,)})
    assert case.check(gmx_observation(share, symbol), named) == []
    other = GMX_WAVAX_USDC if share == GMX_BTC_USDC else GMX_BTC_USDC
    assert case.check(gmx_observation(other, symbol), named)
    # "Nothing matches" is the defect main fixed, and fails.
    nothing = {"error": "not_found", "message": f"no GMX V2 market matches {query!r}"}
    assert reads.judge_refusal(case, nothing).status == "fail"


@pytest.mark.parametrize(
    ("case_id", "query", "share", "collateral"),
    [
        ("avalanche-btc-usd-bare-label", "BTC/USD", GMX_BTC_USDC, "BTC.b-USDC"),
        ("avalanche-avax-usd-bare-label", "AVAX/USD", GMX_WAVAX_USDC, "WAVAX-USDC"),
    ],
)
def test_a_bare_gmx_avalanche_label_is_refused_with_its_candidates(
    case_id, query, share, collateral
):
    """Each bare label names a USDC market and a single-collateral one, so binding either
    silently, or finding nothing, is wrong."""
    case = CATALOG[f"perp-markets/gmx_v2/{case_id}"]
    assert case.arguments[-1] == query and not case.succeeds and case.known_issue is None
    assert {refusal.error for refusal in case.refusals} == {"ambiguous"}
    message = (
        f"GMX V2 market query {query!r} is ambiguous: matches 2 markets:"
        f" {share} [{collateral}], 0x{'3c' * 20} [{collateral.split('-')[0]}-"
        f"{collateral.split('-')[0]}]"
    )
    assert reads.judge_refusal(case, {"error": "ambiguous", "message": message}).status == "pass"
    elsewhere = message.replace(share, "0x" + "99" * 20)
    assert reads.judge_refusal(case, {"error": "ambiguous", "message": elsewhere}).status == "fail"
    nothing = {"error": "not_found", "message": f"no GMX V2 market matches {query!r}"}
    assert reads.judge_refusal(case, nothing).status == "fail"
    bound = reads.judge_output(case, gmx_observation(share, "BTC.b"), FakeRpc())
    assert bound.status == "fail"


def test_the_qa_guide_states_the_defects_main_fixed_as_fixed():
    text = (ROOT / "docs/qa.md").read_text()
    section = text[text.index("Known issues. A defect") : text.index("Not covered: `ax lending")]
    assert "The catalog holds none now" in section and "fixed on main in 3.4.2" in section
    for stale in ("currently holds", "labels match nothing", "recognizes a revert only by"):
        assert stale not in section, stale


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
    check = checks.erc20_balance(AssetType(USDC), ACCOUNT)
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
        ({}, checks.erc20_balance(AssetType(USDC), ACCOUNT), "asset: CLI 'eip155:1/slip44:60'"),
    ):
        (difference,) = check({"observations": [{**observation, **reported}]}, unread)
        assert difference.startswith(finding) and ", requested " in difference
    # The same address in another letter case is the same account.
    checksummed = {**observation, "account": {"address": "0x" + "AB" * 20}}
    check = checks.native_balance(AssetType(NATIVE), "0x" + "ab" * 20)
    assert check({"observations": [checksummed]}, FakeRpc()) == []


MARKET_ASSET = "0x" + "a1" * 20
LOAN = f"eip155:1/erc20:{MARKET_ASSET}"
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
    # An empty listing beside an entry the endpoint did not answer blocks, so it is retried:
    # the unanswered Hub may hold every market. Beside an answered finding it still fails.
    limited_hub = {"hub": "0x2", "reason": "http: HTTP 429"}
    with pytest.raises(RpcUnavailable, match="1 entries unreadable, first: http: HTTP 429"):
        checks.listed_markets(listing([], [limited_hub]), FakeRpc())
    broken = {"comet": "0x3", "reason": "invalid: no base token"}
    assert checks.listed_markets(listing([], [limited_hub, broken]), FakeRpc()) == [
        "comet 0x3 is unreadable: invalid: no base token"
    ]
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
    # A Spoke the endpoint really did not answer does not hide the misread beside it.
    (finding,) = checks.listed_markets(partial, FakeRpc(reverting=("0x1",)))
    assert finding.startswith("1 Spokes are reported unreadable") and finding.endswith("0x1")
    # Alone, it blocks the case.
    with pytest.raises(RpcUnavailable, match="1 entries unreadable"):
        checks.listed_markets(listing([market()], unreadable[1:]), FakeRpc())
    with pytest.raises(RpcUnavailable):
        checks.listed_markets(partial, FakeRpc(down=True))


def test_an_entry_unreadable_for_a_reason_that_is_not_the_endpoint_fails_the_listing():
    """Only an endpoint that did not answer blocks; anything else is a finding."""
    unreadable = [
        {"spoke": "0x1", "status": "refused", "reason": "malformed: reserve count"},
        {"comet": "0x2", "status": "refused", "reason": "invalid: no base token"},
    ]
    assert checks.listed_markets(listing([market()], unreadable), FakeRpc()) == [
        "spoke 0x1 is unreadable: malformed: reserve count",
        "comet 0x2 is unreadable: invalid: no base token",
    ]
    verified = market(market_id="0x" + "0d" * 32, loan_asset=market()["asset"])
    core = FakeRpc({"idToMarketParams(bytes32)": (MARKET_ASSET,)})
    morpho = checks.morpho_listing("0xcore", LOAN)(listing([verified], unreadable[1:]), core)
    assert morpho == ["comet 0x2 is unreadable: invalid: no base token"]
    # An endpoint reason beside an answered finding does not hide it: the case fails.
    rejected = {"spoke": "0x3", "reason": "rpc: eth_call: rejected"}
    assert checks.listed_markets(listing([market()], [*unreadable, rejected]), FakeRpc()) == [
        "spoke 0x1 is unreadable: malformed: reserve count",
        "comet 0x2 is unreadable: invalid: no base token",
    ]
    limited = market("refused", reason="http: HTTP 429")
    assert checks.listed_markets(listing([limited], unreadable[:1]), FakeRpc()) == [
        "spoke 0x1 is unreadable: malformed: reserve count"
    ]
    # With nothing else found, the endpoint reason alone blocks, so the run retries.
    with pytest.raises(RpcUnavailable, match="1 entries unreadable"):
        checks.listed_markets(listing([market()], [rejected]), FakeRpc())


OTHER_ASSET = "0x" + "b0" * 20
THIRD_ASSET = "0x" + "c0" * 20
FOURTH_ASSET = "0x" + "d0" * 20
OTHER_LOAN = f"eip155:1/erc20:{OTHER_ASSET}"


def test_a_pool_listing_is_held_to_the_reserves_the_pool_itself_lists():
    check = checks.pool_reserve_listing(ETHEREUM, 3)
    output = listing(
        [market(), other_market := market(asset={"asset": f"eip155:1/erc20:{OTHER_ASSET}"})]
    )
    two = FakeRpc({"getReservesList()": ((MARKET_ASSET.upper().replace("0X", "0x"), OTHER_ASSET),)})
    assert check(output, two) == []
    three = FakeRpc({"getReservesList()": ((THIRD_ASSET, OTHER_ASSET, FOURTH_ASSET),)})
    assert check(output, three) == [
        "listed reserves: CLI 2, direct read 3",
        "the listing reports 2 distinct markets, where a limit of 3 on 3 reserves gives 3",
        f"market 0 asset {LOAN} is not on the Pool's reserve list",
    ]
    # A limit below the reserve count is a valid shorter listing.
    assert checks.pool_reserve_listing(ETHEREUM, 1)(listing([other_market]), two) == []


def test_a_pool_listing_that_repeats_an_asset_fails():
    """Three rows of one reserve would pass the count and the membership checks."""
    reserves = FakeRpc({"getReservesList()": ((MARKET_ASSET, OTHER_ASSET, FOURTH_ASSET),)})
    output = {**listing([market()] * 3), "source": {"contract": "0xpool", "listed": 3}}
    assert checks.pool_reserve_listing(ETHEREUM, 3)(output, reserves) == [
        f"asset {LOAN} is reported 3 times",
        "the listing reports 1 distinct markets, where a limit of 3 on 3 reserves gives 3",
    ]


def test_an_aave_v4_market_is_held_to_what_its_spoke_records():
    """A listing or reserve naming the wrong Spoke or reserve ID does not pass on decimals."""
    listed = listing([market(spoke="0xspoke", reserve_id=3, hub=HUB)])
    agreeing = FakeRpc({"getReserve(uint256)": (MARKET_ASSET, HUB, 4, 18), "decimals()": (18,)})
    other = FakeRpc({"getReserve(uint256)": ("0x" + "c3" * 20, HUB, 4, 18), "decimals()": (18,)})
    assert checks.aave_v4_listing(ETHEREUM)(listed, agreeing) == []
    (difference,) = checks.aave_v4_listing(ETHEREUM)(listed, other)
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
    requested = checks.aave_v4_reserve(ETHEREUM, "0xspoke", 3)
    assert requested(reserve, agreeing) == []
    (difference,) = requested(reserve, other)
    assert difference.startswith("asset: CLI")
    wrong_slot = FakeRpc({"getReserve(uint256)": (MARKET_ASSET, HUB, 9, 18), "decimals()": (18,)})
    assert requested(reserve, wrong_slot) == ["hub_asset_id: CLI 4, direct read 9"]


def test_a_reserve_or_pool_read_of_another_selector_fails_before_any_direct_read():
    """Correct data for another reserve or pool would agree with a direct read of it."""
    other = "0x" + "22" * 20
    weth = "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    block = {"block": BLOCK}
    v4_source = {
        "currency0": USDC,
        "currency1": weth,
        "fee": 500,
        "tick_spacing": 10,
        "hooks": checks.ZERO_ADDRESS,
    }
    cases = (
        (
            checks.aave_v4_reserve(ETHEREUM, SPOKE, 0),
            {"resolution": {"selection": {"spoke": SPOKE, "reserve_id": 1}}, "observation": block},
            "reserve_id: CLI 1, requested 0",
        ),
        (
            checks.aave_v4_reserve(ETHEREUM, SPOKE, 0),
            {"resolution": {"selection": {"spoke": other, "reserve_id": 0}}, "observation": block},
            f"spoke: CLI {other!r}",
        ),
        (
            checks.pooled_reserve(SPOKE, USDC),
            {"resolution": {"pool": SPOKE, "asset": weth}, "observation": block},
            f"asset: CLI {weth!r}, requested {USDC!r}",
        ),
        (
            checks.pooled_reserve(SPOKE, USDC),
            {"resolution": {"pool": other, "asset": USDC}, "observation": block},
            f"pool: CLI {other!r}",
        ),
        (
            checks.comet_reserve(SPOKE, USDC),
            {"resolution": {"comet": other, "asset": USDC}, "observation": block},
            f"comet: CLI {other!r}",
        ),
        (
            checks.morpho_reserve(ETHEREUM, SPOKE, "0x" + "0d" * 32),
            {"resolution": {"core": SPOKE, "market_id": "0x" + "0e" * 32}, "observation": block},
            "market_id: CLI '0x0e0e",
        ),
        (
            checks.v3_pool(ETHEREUM, SPOKE),
            {"resolution": {"pool": other}, "observation": block},
            f"pool: CLI {other!r}",
        ),
        (
            checks.v4_pool_price(USDC, weth, 500, 10),
            {"observation": {"source": {**v4_source, "fee": 3000}, "block": BLOCK}},
            "fee: CLI 3000, requested 500",
        ),
        (
            checks.v4_pool_price(USDC, weth, 500, 10),
            {"observation": {"source": {**v4_source, "hooks": other}, "block": BLOCK}},
            f"hooks: CLI {other!r}",
        ),
    )
    for check, output, finding in cases:
        # Any direct read would raise: the identity is refused before one is made.
        (difference,) = check(output, FakeRpc(down=True))
        assert difference.startswith(finding) and ", requested " in difference, finding
    # The same address in another letter case is the same selector.
    upper = {"resolution": {"pool": SPOKE.upper().replace("0X", "0x"), "asset": USDC}}
    read = FakeRpc({"getReserveData(address)": (0,) * 8 + (MARKET_ASSET,), "decimals()": (6,)})
    reserve = {**upper, "observation": block}
    reserve["resolution"] |= {"a_token": f"eip155:1/erc20:{MARKET_ASSET}", "decimals": 6}
    assert checks.pooled_reserve(SPOKE, USDC)(reserve, read) == []


AVALANCHE_LOAN = f"eip155:43114/erc20:{MARKET_ASSET}"
UNANSWERED = RpcUnavailable("eth_call decimals(): HTTP 429")


def reserve_output(**resolution):
    """A lending reserve whose every reported fact agrees with `RESERVE_READS`."""
    fields = {
        "selection": {"spoke": SPOKE, "reserve_id": 3},
        "pool": SPOKE,
        "comet": SPOKE,
        "core": SPOKE,
        "market_id": "0x" + "0d" * 32,
        "asset": LOAN,
        "a_token": LOAN,
        "base_asset": LOAN,
        "loan_asset": LOAN,
        "collateral_asset": OTHER_LOAN,
        "hub": HUB,
        "hub_asset_id": 4,
        "oracle": "0x" + "0c" * 20,
        "lltv": 860,
        "decimals": 18,
    }
    return {"resolution": fields | resolution, "observation": {"block": BLOCK}}


RESERVE_READS = {
    "getReserve(uint256)": (MARKET_ASSET, HUB, 4, 18),
    "getReserveData(address)": (0,) * 8 + (MARKET_ASSET,),
    "baseToken()": (MARKET_ASSET,),
    "idToMarketParams(bytes32)": (MARKET_ASSET, OTHER_ASSET, "0x" + "0c" * 20, "0x0", 860),
    "decimals()": (18,),
}
RESERVE_CHECKS = {
    "asset": checks.aave_v4_reserve(ETHEREUM, SPOKE, 3),
    "a_token": checks.pooled_reserve(SPOKE, LOAN),
    "base_asset": checks.comet_reserve(SPOKE, LOAN),
    "loan_asset": checks.morpho_reserve(ETHEREUM, SPOKE, "0x" + "0d" * 32),
    "collateral_asset": checks.morpho_reserve(ETHEREUM, SPOKE, "0x" + "0d" * 32),
}


def test_a_reserve_asset_of_another_chain_fails_though_its_address_agrees():
    """The right contract address under another chain is another CAIP-19 asset."""
    for field, check in RESERVE_CHECKS.items():
        assert check(reserve_output(), FakeRpc(RESERVE_READS)) == [], field
        moved = AssetType(reserve_output()["resolution"][field]).reference
        output = reserve_output(**{field: f"eip155:43114/erc20:{moved}"})
        findings = check(output, FakeRpc(RESERVE_READS))
        assert findings and "eip155:43114/erc20:" in findings[0], (field, findings)
        # The same asset in another letter case is still the same asset.
        upper = f"eip155:1/erc20:0x{moved[2:].upper()}"
        assert check(reserve_output(**{field: upper}), FakeRpc(RESERVE_READS)) == [], field
    # A Morpho reserve's own asset is the loan asset the core records.
    morpho = RESERVE_CHECKS["loan_asset"]
    (finding,) = morpho(reserve_output(asset=OTHER_LOAN), FakeRpc(RESERVE_READS))
    assert finding.startswith("asset: CLI")
    # A reported value that is no CAIP-19 reference fails as unreadable output.
    case = reads.ReadCase("reserve", ETHEREUM, ("lending-reserve",), RESERVE_CHECKS["asset"])
    verdict = reads.judge_output(case, reserve_output(asset="WETH"), FakeRpc(RESERVE_READS))
    assert verdict.status == "fail" and "not shaped" in verdict.detail


def test_a_listing_or_pool_asset_of_another_chain_fails_though_its_address_agrees():
    elsewhere = {"asset": AVALANCHE_LOAN, "symbol": "TKN"}
    spoke = FakeRpc({"getReserve(uint256)": (MARKET_ASSET, HUB, 4, 18)})
    listed = listing([market(spoke="0xspoke", reserve_id=3, hub=HUB, asset=elsewhere)])
    (finding,) = checks.aave_v4_listing(ETHEREUM)(listed, spoke)
    assert finding == f"market 0 asset: CLI {AVALANCHE_LOAN!r}, direct read {LOAN!r}"
    pool = FakeRpc({"getReservesList()": ((MARKET_ASSET, OTHER_ASSET),)})
    two = listing([market(asset=elsewhere), market(asset={"asset": OTHER_LOAN})])
    assert checks.pool_reserve_listing(ETHEREUM, 3)(two, pool) == [
        f"market 0 asset {AVALANCHE_LOAN} is not on the Pool's reserve list"
    ]
    verified = market(market_id="0x" + "0d" * 32, loan_asset=elsewhere)
    core = FakeRpc({"idToMarketParams(bytes32)": (MARKET_ASSET,)})
    (finding,) = checks.morpho_listing("0xcore", LOAN)(listing([verified]), core)
    assert finding == f"loan asset of 0x0d0d0d0d: CLI {AVALANCHE_LOAN!r}, requested {LOAN!r}"
    pool_state = {
        "slot0()": (1, 2),
        "liquidity()": (3,),
        "token0()": (MARKET_ASSET,),
        "fee()": (500,),
    }
    observation = {
        "sqrt_price_x96": 1,
        "tick": 2,
        "liquidity": 3,
        "currency0": {"identifier": AVALANCHE_LOAN},
        "block": BLOCK,
    }
    pool_read = {"resolution": {"pool": SPOKE, "fee": 500}, "observation": observation}
    (finding,) = checks.v3_pool(ETHEREUM, SPOKE)(pool_read, FakeRpc(pool_state))
    assert finding.startswith(f"currency0: CLI {AVALANCHE_LOAN!r}")
    observation["currency0"]["identifier"] = LOAN
    assert checks.v3_pool(ETHEREUM, SPOKE)(pool_read, FakeRpc(pool_state)) == []


def test_a_gmx_share_or_index_of_another_chain_fails():
    share = "0x" + "fb" * 20
    index = "0x" + "15" * 20
    check = checks.gmx_market(AVALANCHE, share, "BTC.b")
    symbol = FakeRpc({"symbol()": ("BTC.b",)})
    observation = {
        "share": f"eip155:43114/erc20:{share}",
        "index": f"eip155:43114/erc20:{index}",
        "index_symbol": "BTC.b",
        "block": 7,
    }
    assert check({"observation": observation}, symbol) == []
    # The identity is held to the request before the symbol() read, so a lookup that
    # names another asset fails even when that read goes unanswered.
    unanswered = FakeRpc({"symbol()": RpcUnavailable("eth_call symbol(): HTTP 429")})
    case = reads.ReadCase("gmx", AVALANCHE, ("perp-markets",), check)
    for field in ("share", "index"):
        moved = {**observation, field: observation[field].replace("43114", "1")}
        (finding,) = check({"observation": moved}, symbol)
        assert "'eip155:1" in finding and "requested" in finding, field
        verdict = reads.judge_output(case, {"observation": moved}, unanswered)
        assert verdict.status == "fail" and "'eip155:1" in verdict.findings[0], field
    other_share = {**observation, "share": f"eip155:43114/erc20:0x{'fc' * 20}"}
    verdict = reads.judge_output(case, {"observation": other_share}, unanswered)
    assert verdict.status == "fail" and verdict.findings[0].startswith("share:")
    other_symbol = {**observation, "index_symbol": "btc.b"}
    verdict = reads.judge_output(case, {"observation": other_symbol}, unanswered)
    assert verdict.findings == ("index_symbol: CLI 'btc.b', direct read 'BTC.b'",)
    # Matching identities still get the symbol() read, so they block when it goes unanswered.
    verdict = reads.judge_output(case, {"observation": observation}, unanswered)
    assert verdict.status == "transient", verdict


def test_an_answered_reserve_mismatch_is_kept_when_the_decimals_read_is_unanswered():
    """A wrong reported Hub the Spoke answered is a failure, not a blocked case."""
    unanswered = FakeRpc(RESERVE_READS | {"decimals()": UNANSWERED})
    wrong = {
        "asset": {"hub": "0x" + "ee" * 20},
        "a_token": {"a_token": OTHER_LOAN},
        "base_asset": {"base_asset": OTHER_LOAN},
        "loan_asset": {"lltv": 945},
    }
    for field, changes in wrong.items():
        check = RESERVE_CHECKS[field]
        case = reads.ReadCase("reserve", ETHEREUM, ("lending-reserve",), check)
        verdict = reads.judge_output(case, reserve_output(**changes), unanswered)
        assert verdict.status == "fail" and verdict.findings, (field, verdict)
        # Matching reserve data still gets the decimals read, so it blocks when that read
        # goes unanswered, and fails when the token's own decimals differ.
        verdict = reads.judge_output(case, reserve_output(), unanswered)
        assert verdict.status == "transient", (field, verdict)
        other_decimals = FakeRpc(RESERVE_READS | {"decimals()": (6,)})
        verdict = reads.judge_output(case, reserve_output(), other_decimals)
        assert verdict.findings[0] == "decimals: CLI 18, direct read 6", field


class InTurn(FakeRpc):
    """Direct reads of one signature answered in turn, one answer per call."""

    def call(self, to, signature, block, outputs, args=()):
        answer = self.answers[signature].pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def test_an_answered_listing_mismatch_is_kept_when_a_later_market_read_is_unanswered():
    """A market the endpoint answered is compared whatever a later market's read does."""
    unanswered = RpcUnavailable("eth_call getReserve(uint256): HTTP 429")
    other_asset = ("0x" + "c3" * 20, HUB, 4, 18)
    agreeing = (MARKET_ASSET, HUB, 4, 18)
    two = listing(
        [
            market(spoke="0xspoke", reserve_id=3, hub=HUB),
            market(spoke="0xspoke", reserve_id=4, hub=HUB),
        ]
    )
    case = reads.ReadCase(
        "aave-v4", ETHEREUM, ("lending-markets",), checks.aave_v4_listing(ETHEREUM)
    )
    rpc = InTurn({"getReserve(uint256)": [other_asset, unanswered]})
    verdict = reads.judge_output(case, two, rpc)
    assert verdict.status == "fail", verdict
    (finding,) = verdict.findings
    assert finding.startswith("market 0 asset: CLI")
    # Without an answered mismatch the unanswered read still blocks the case.
    rpc = InTurn({"getReserve(uint256)": [agreeing, unanswered]})
    verdict = reads.judge_output(case, two, rpc)
    assert verdict.status == "transient" and "market 1: eth_call" in verdict.detail, verdict
    # The same holds for a Morpho listing's markets, each read from the core.
    morpho = reads.ReadCase(
        "morpho", ETHEREUM, ("lending-markets",), checks.morpho_listing("0xcore", LOAN)
    )
    ids = ("0x" + "0d" * 32, "0x" + "0e" * 32)
    verified = listing([market(market_id=i, loan_asset=market()["asset"]) for i in ids])
    rpc = InTurn({"idToMarketParams(bytes32)": [("0x" + "c3" * 20,), unanswered]})
    verdict = reads.judge_output(morpho, verified, rpc)
    assert verdict.status == "fail", verdict
    (finding,) = verdict.findings
    assert finding.startswith("loan asset of 0x0d0d0d0d: CLI")
    rpc = InTurn({"idToMarketParams(bytes32)": [(MARKET_ASSET,), unanswered]})
    assert reads.judge_output(morpho, verified, rpc).status == "transient"


def test_a_listing_asset_of_another_chain_fails_when_its_market_read_is_unanswered():
    """The reported chain is wrong before any read, so no unanswered read can block it."""
    unanswered = RpcUnavailable("eth_call getReserve(uint256): HTTP 429")
    elsewhere = {"asset": AVALANCHE_LOAN, "symbol": "TKN"}
    case = reads.ReadCase(
        "aave-v4", ETHEREUM, ("lending-markets",), checks.aave_v4_listing(ETHEREUM)
    )
    two = listing(
        [
            market(spoke="0xspoke", reserve_id=3, hub=HUB, asset=elsewhere),
            market(spoke="0xspoke", reserve_id=4, hub=HUB),
        ]
    )
    for answers in ([unanswered, unanswered], [unanswered, (MARKET_ASSET, HUB, 4, 18)]):
        verdict = reads.judge_output(case, two, InTurn({"getReserve(uint256)": answers}))
        assert verdict.status == "fail", verdict
        assert verdict.findings == (f"market 0 asset {AVALANCHE_LOAN} is not an asset of eip155:1",)
    # A matching asset whose read went unanswered still blocks, so the run retries it.
    matching = listing([market(spoke="0xspoke", reserve_id=3, hub=HUB)] * 2)
    rpc = InTurn({"getReserve(uint256)": [unanswered, unanswered]})
    assert reads.judge_output(case, matching, rpc).status == "transient"
    # A value that is no CAIP-19 reference fails as output the check cannot read.
    unshaped = listing([market(spoke="0xspoke", reserve_id=3, hub=HUB, asset={"asset": "WETH"})])
    verdict = reads.judge_output(case, unshaped, InTurn({"getReserve(uint256)": [unanswered]}))
    assert verdict.status == "fail" and "not shaped" in verdict.detail, verdict


def test_a_pool_listing_keeps_what_needs_no_reserve_list_when_that_read_is_unanswered():
    unanswered = FakeRpc({"getReservesList()": RpcUnavailable("eth_call: HTTP 429")})
    case = reads.ReadCase(
        "aave-v3", ETHEREUM, ("lending-markets",), checks.pool_reserve_listing(ETHEREUM, 3)
    )
    elsewhere = market(asset={"asset": AVALANCHE_LOAN, "symbol": "TKN"})
    verdict = reads.judge_output(case, listing([elsewhere, market()]), unanswered)
    assert verdict.status == "fail", verdict
    assert verdict.findings == (f"market 0 asset {AVALANCHE_LOAN} is not an asset of eip155:1",)
    repeated = reads.judge_output(case, listing([market(), market()]), unanswered)
    assert repeated.findings == (f"asset {LOAN} is reported 2 times",), repeated
    other = market(asset={"asset": OTHER_LOAN})
    verdict = reads.judge_output(case, listing([market(), other]), unanswered)
    assert verdict.status == "transient" and "getReservesList()" in verdict.detail, verdict


class SilentSpokes(FakeRpc):
    """A direct read of these Spokes' `getReserveCount()` goes unanswered; others answer."""

    def __init__(self, silent, **fields):
        super().__init__(**fields)
        self.silent = silent

    def reverts(self, to, signature, block):
        if to in self.silent:
            raise RpcUnavailable(f"eth_call {signature}: HTTP 429")
        return super().reverts(to, signature, block)


def test_an_unanswered_spoke_read_keeps_what_the_listing_check_already_found():
    unreadable = [
        {"spoke": "0x1", "reason": "rpc: eth_call: RPC rejected the requested read"},
        {"spoke": "0x2", "reason": "rpc: eth_call: RPC rejected the requested read"},
    ]
    partial = listing([market()], unreadable)
    # A misread of one Spoke stands beside another Spoke the endpoint did not answer.
    (finding,) = checks.listed_markets(partial, SilentSpokes(("0x2",), reverting=("0x1",)))
    assert finding.startswith("1 Spokes are reported unreadable") and finding.endswith("0x1")
    # So does a mismatch the connector's own check found in the same listing.
    spoke = SilentSpokes(("0x1", "0x2"), answers={"getReserve(uint256)": (OTHER_ASSET, HUB, 4, 18)})
    listed = listing([market(spoke="0xspoke", reserve_id=3, hub=HUB)], unreadable)
    (finding,) = checks.aave_v4_listing(ETHEREUM)(listed, spoke)
    assert finding.startswith("market 0 asset: CLI")
    pool = SilentSpokes(("0x1",), answers={"getReservesList()": ((MARKET_ASSET, OTHER_ASSET),)})
    elsewhere = market(asset={"asset": AVALANCHE_LOAN, "symbol": "TKN"})
    found = checks.pool_reserve_listing(ETHEREUM, 3)(listing([elsewhere], unreadable[:1]), pool)
    assert found == [
        "the listing reports 1 distinct markets, where a limit of 3 on 2 reserves gives 2",
        f"market 0 asset {AVALANCHE_LOAN} is not on the Pool's reserve list",
    ]
    # Alone, the unanswered Spoke read blocks the case.
    with pytest.raises(RpcUnavailable, match="2 entries unreadable"):
        checks.listed_markets(listing([market()], unreadable[1:]), SilentSpokes(("0x2",)))


def test_a_morpho_listing_is_held_to_the_core_and_may_refuse_some_markets_not_all():
    check = checks.morpho_listing("0xcore", LOAN)
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


def test_a_morpho_listing_lends_the_requested_asset():
    """A listing that ignored --asset would pass on the core agreeing with its own ID."""
    other = market(
        market_id="0x" + "0e" * 32, loan_asset={"asset": f"eip155:1/erc20:{OTHER_ASSET}"}
    )
    core = FakeRpc({"idToMarketParams(bytes32)": (OTHER_ASSET,)})
    (difference,) = checks.morpho_listing("0xcore", LOAN)(listing([other]), core)
    assert difference == (f"loan asset of 0x0e0e0e0e: CLI {OTHER_LOAN!r}, requested {LOAN!r}")


def test_a_gmx_lookup_must_name_the_expected_share_and_symbol():
    share = "0xfb02132333a79c8b5bd0b64e3abcca5f7faf2937"
    observation = {
        "share": f"eip155:43114/erc20:{share}",
        "index": "eip155:43114/erc20:0x152b9d0fdc40c096757f570a51e494bd4b943e50",
        "index_symbol": "BTC.b",
        "block": 7,
    }
    check = checks.gmx_market(AVALANCHE, share, "BTC.b")
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
    first = client("publicnode", ChainId("eip155:1"))
    assert client("publicnode", ChainId("eip155:1")) is first
    assert client("drpc", ChainId("eip155:1")) is not first
    assert client("publicnode", ChainId("eip155:43114")) is not first
    assert first.url == "https://ethereum-rpc.publicnode.com"
    assert client("publicnode", ChainId("eip155:999")).url is None


def test_a_chain_the_endpoint_does_not_serve_has_no_direct_client():
    assert endpoint_url("publicnode", ChainId("eip155:1")) == "https://ethereum-rpc.publicnode.com"
    assert endpoint_url("publicnode", ChainId("eip155:8453")) is None
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
    assert live_conftest.served("publicnode", ChainId("eip155:1"))
    assert not live_conftest.served("publicnode", ChainId("eip155:999"))


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
