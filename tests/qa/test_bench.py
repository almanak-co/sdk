"""A missing proof, failed worker or skipped test cannot produce a green bench."""

import json
import subprocess
import sys
from dataclasses import replace
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.configuration.backtest_inputs import read_backtest_inputs
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.composition import installed_connectors
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.support import EconomicAction, RunMode
from scripts import qa_bench
from tests.qa.catalog import ROOT, Case, catalog, validate
from tests.qa.inputs import resolved_author, resolved_history, write_json
from tests.qa.process import networkless, run
from tests.qa.results import assert_junit, export_evidence, redact, row, save_results, secrets_for


def test_declared_matrix_and_new_chain_cannot_silently_lose_coverage():
    cases = catalog()
    expected = validate(cases)
    assert len(expected) > 0
    # Every reference proves EOA beside Safe/Roles, unless its connector declares only
    # Safe/Roles: HyperCore's Safe owns the account (blueprint 30).
    # Across declares Safe/Roles accounts with no fork run behind them, so the catalog
    # leaves that profile out by the connector's own list and owes it nothing.
    # A composed reference that bridges proves EOA accounts alone for the same reason.
    from almanak.connectors.across.support import ACCOUNT_PROFILES
    from tests.qa.catalog import profiles_without_proof, required

    assert "safe_roles" in ACCOUNT_PROFILES
    assert profiles_without_proof() == {("across", "safe_roles")}
    owed = {o.profile for o in required() if o.connector == ConnectorId("across") and o.profile}
    assert owed == {"eoa"}
    eoa_only = {
        c.reference
        for c in cases
        if any(check.connector == ConnectorId("across") for check in c.checks())
    }
    assert eoa_only == {"across_usdc_arbitrum_base_bridge", "arbitrum_base_bridge_and_lend"}
    assert {c.reference for c in cases if c.profile == "eoa"} - eoa_only == {
        c.reference
        for c in cases
        if c.profile == "safe_roles" and c.connector != ConnectorId("hyperliquid")
    }
    assert {
        (c.profile, c.operator_mode)
        for c in cases
        if c.lane == "fork" and c.connector == ConnectorId("hyperliquid")
    } == {("safe_roles", "configured"), ("safe_roles", "distinct")}
    bindings = installed_connectors()
    name, connector = bindings[0]
    support = connector.chain_support[0].model_copy(update={"chain": ChainId("eip155:8453")})
    changed = (
        (name, replace(connector, chain_support=(*connector.chain_support, support))),
        *bindings[1:],
    )
    with pytest.raises(ValueError, match="coverage gaps"):
        validate(cases, changed)
    with pytest.raises(ValueError, match="coverage gaps"):
        validate([c for c in cases if c.profile != "eoa"])
    with pytest.raises(ValueError, match="duplicate"):
        validate([*cases, cases[0]])


def test_the_across_revoke_is_claimed_by_its_own_proof_and_by_no_clean_lifecycle():
    import ast

    from tests.qa.catalog import ACROSS_PROOFS, ROOT

    cases = catalog()
    across = [c for c in cases if c.connector == ConnectorId("across")]
    revoke = [c for c in across if EconomicAction.REVOKE_BRIDGE in c.actions]
    assert [c.id for c in revoke] == ["cleanup/across_usdc_arbitrum_base_bridge/eoa"]
    clean = [c for c in across if c.lane == "fork"]
    assert clean and all(c.actions == (EconomicAction.BRIDGE,) for c in clean)
    # Without the revoke proof the declared action has no proof, under either policy.
    with pytest.raises(ValueError, match="coverage gaps.*revoke_bridge"):
        validate([c for c in cases if c not in revoke])
    # Every supervised Across proof is listed by a bench case, each exactly once.
    faults = next(c for c in cases if c.id == "fork/faults")
    listed = [n for n in (*faults.nodes, *revoke[0].nodes) if n.startswith(ACROSS_PROOFS)]
    module = ast.parse((ROOT / ACROSS_PROOFS).read_text())
    proofs = [
        f"{ACROSS_PROOFS}::{node.name}"
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")
    ]
    assert sorted(listed) == sorted(proofs) and len(proofs) == 4


def test_unsupported_action_claim_is_not_a_proof():
    cases = catalog()
    extra = replace(
        next(c for c in cases if c.lane == "fork"), id="invalid", actions=(EconomicAction.REFRESH,)
    )
    with pytest.raises(ValueError, match="unsupported proof claims"):
        validate([*cases, extra])


def test_explicit_selection_retains_the_full_denominator(tmp_path):
    cases = catalog()
    args = SimpleNamespace(case=[cases[0].id], lane=None)
    assert qa_bench.select(cases, args) == [cases[0]]
    args.case = ["missing"]
    with pytest.raises(ValueError, match="unknown"):
        qa_bench.select(cases, args)
    rows = [{**row(c), "artifact": str(i)} for i, c in enumerate(cases)]
    rows[0].update(status="passed")
    result = save_results(tmp_path, {"execution": "run"}, rows)
    assert result["status"] == "incomplete_or_failed" and len(result["cases"]) == len(cases)
    rows = [{**row(cases[0]), "artifact": "0", "status": "passed"}]
    assert save_results(tmp_path, {}, rows)["status"] == "passed"


@pytest.mark.parametrize(
    "body",
    [
        "",
        '<testcase name="x"><skipped/></testcase>',
        '<testcase name="x"><failure/></testcase>',
        '<testcase name="x"><error/></testcase>',
    ],
)
def test_empty_or_skipped_junit_is_not_green(tmp_path, body):
    path = tmp_path / "results.xml"
    path.write_text(f"<testsuite>{body}</testsuite>")
    with pytest.raises(ValueError):
        assert_junit(path)
    path.write_text('<testsuite><testcase name="x"/></testsuite>')
    assert_junit(path)


def test_redaction_preserves_binary_evidence_or_refuses_to_export(tmp_path):
    private, public = tmp_path / "private", tmp_path / "public"
    private.mkdir()
    env = tmp_path / "provider.env"
    env.write_text('RPC_URL="https://rpc.example/v2/very-secret-key"\n')
    (private / "gateway-auth.token").write_text("private-auth-token")
    secrets = secrets_for(env, private)
    (private / "process.log").write_text(
        "https://rpc.example/v2/very-secret-key private-auth-token"
    )
    (private / "dataset.json").write_text('{"digest":"retained-original"}')
    (private / "bad.sqlite").write_bytes(b"private-auth-token")
    errors = export_evidence(private, public, secrets)
    assert errors == ["bad.sqlite"]
    assert (public / "dataset.json").read_bytes() == (private / "dataset.json").read_bytes()
    assert "very-secret-key" not in (public / "process.log").read_text()
    assert not (public / "gateway-auth.token").exists()
    assert "secret" not in redact("https://unknown.example/path?key=secret", ())


def converted_case(case_id):
    return next(c for c in catalog() if c.id == case_id)


def schedule_of(case, tmp_path):
    return read_backtest_inputs(resolved_history(case, tmp_path, 2000000000)).inputs.schedule()


def test_frozen_and_native_configurations_are_explicit(tmp_path):
    case = converted_case("backtest/native/aave_v4_weth_supply/historical_path")
    assert "historical-shares" in case.model
    inputs = read_inputs(resolved_author(case, tmp_path, {})).inputs
    assert inputs.strategy.debts == {}
    assert set(inputs.markets) == {"main_weth_market", "bluechip_weth_market"}
    assert inputs.strategy.allocations["main_weth"].hold_seconds == 120
    schedule = schedule_of(case, tmp_path)
    assert "decision_interval_override_seconds" not in schedule
    assert schedule["end"] - schedule["start"] == 600
    schedule = schedule_of(replace(case, input_policy="fresh"), tmp_path)
    assert schedule["end"] + schedule["cleanup_seconds"] == 2000000000 - 7200
    assert schedule["decision_interval_override_seconds"] == 3600


# A scenario that needs an internal driver must outrank the supervised lane, so a
# converted reference appears on both sides of this table.
ROUTES = (
    ("converted", "supervised", lambda c: c.reference == "aave_v4_weth_supply"),
    (
        "converted_v3",
        "supervised",
        lambda c: (
            c.reference == "uniswap_v3_ethereum_lp" and c.profile == "safe_roles" and not c.scenario
        ),
    ),
    (
        "converted_v3_eoa",
        "supervised",
        lambda c: c.reference == "uniswap_v3_ethereum_lp" and c.profile == "eoa" and not c.scenario,
    ),
    (
        "converted_v4",
        "supervised",
        lambda c: c.reference == "uniswap_v4_lp" and c.profile == "safe_roles" and not c.scenario,
    ),
    (
        "converted_v4_eoa",
        "supervised",
        lambda c: c.reference == "uniswap_v4_lp" and c.profile == "eoa" and not c.scenario,
    ),
    ("gmx_cleanup", "gmx", lambda c: c.scenario == "gmx_cleanup"),
    (
        "gmx_fresh",
        "gmx_fresh",
        lambda c: c.input_policy == "fresh" and not c.scenario and "gmx_v2_" in c.reference,
    ),
    ("curve", "curve_case", lambda c: c.connector == ConnectorId("curve") and c.profile == "eoa"),
    (
        "converted_curve",
        "supervised",
        lambda c: (
            c.connector == ConnectorId("curve") and c.profile == "safe_roles" and not c.scenario
        ),
    ),
    ("uniswap_cleanup", "uniswap_cleanup_case", lambda c: c.scenario == "uniswap_cleanup"),
    (
        "converted_lending",
        "supervised",
        lambda c: c.reference == "aave_v3_base_lending" and c.profile == "safe_roles",
    ),
    (
        "converted_lending_eoa",
        "supervised",
        lambda c: c.reference == "aave_v3_base_lending" and c.profile == "eoa",
    ),
    (
        "lending_eoa",
        "supervised",
        lambda c: c.reference == "base_borrow_and_trade" and c.profile == "eoa" and not c.scenario,
    ),
    (
        "account",
        "supervised",
        lambda c: (
            c.reference == "base_borrow_and_trade"
            and c.profile == "safe_roles"
            and c.input_policy == "pinned"
            and not c.scenario
        ),
    ),
)


def routed(name, tmp_path, monkeypatch):
    """Record which harness one fork case selects, without running any of them."""
    from tests.qa import gmx, gmx_fresh, worker
    from tests.strategy_fork import supervised
    from tests.strategy_fork.harness import Case as ForkCase

    calls = []

    def record(label):
        return lambda *args, **kwargs: calls.append(label)

    monkeypatch.setattr(supervised, "run_supervised_case", record("supervised"))
    monkeypatch.setattr(gmx, "cleanup_case", record("gmx"))
    monkeypatch.setattr(gmx_fresh, "execute", record("gmx_fresh"))
    for attribute in (
        "curve_case",
        "uniswap_cleanup_case",
        "lending_eoa_case",
        "execute_account_case",
    ):
        monkeypatch.setattr(worker, attribute, record(attribute))
    predicate = {label: check for label, _, check in ROUTES}[name]
    case = next(c for c in catalog() if c.lane == "fork" and predicate(c))
    source = ForkCase(
        case.reference,
        ROOT / "strategies" / case.reference / "strategy.py",
        tmp_path / "config.yaml",
        ROOT / "tests/strategy_fork/scenarios" / case.reference / "assertions.py",
    )
    worker.execute_fork_case(case, source, source.config, None, tmp_path, tmp_path / "evidence")
    return case, calls


@pytest.mark.parametrize("name,expected", [(n, e) for n, e, _ in ROUTES])
def test_every_fork_case_reaches_exactly_one_harness(tmp_path, monkeypatch, name, expected):
    case, calls = routed(name, tmp_path, monkeypatch)
    assert calls == [expected]
    if name == "converted":
        assert case.reference == "aave_v4_weth_supply"


def synthetic_outcomes(names, kinds):
    return {
        "outcomes": [
            {"operation": {"kind": kind, "target": {"allocation": name}}, "success": True}
            for name in names
            for kind in kinds
        ]
    }


def test_author_scopes_read_both_root_formats_and_credit_their_actions(tmp_path):
    from tests.qa.worker import assert_fork_actions, author_scopes

    case = converted_case("fork/pinned/aave_v4_weth_supply/safe_roles/configured")
    config = resolved_author(case, tmp_path, {})
    scopes = author_scopes(config)
    assert {name for name, _, _ in scopes} == {
        "main_weth",
        "bluechip_weth",
        "main_weth_1",
        "bluechip_weth_1",
        "main_usdc",
        "main_usdc_1",
    }
    assert {(chain, str(connector)) for _, chain, connector in scopes} == {
        (ChainId("eip155:1"), "aave_v4")
    }
    state = synthetic_outcomes(
        ["main_weth", "main_usdc"], ["supply", "withdraw", "borrow", "repay"]
    )
    assert_fork_actions(case, state, config)
    with pytest.raises(AssertionError, match="missing actions"):
        assert_fork_actions(case, synthetic_outcomes(["main_weth"], ["supply"]), config)
    # A composed reference stays a harness root until the composed phase converts it.
    legacy = author_scopes(ROOT / "strategies/arbitrum_aave_borrow_and_trade_gmx/config.yaml")
    assert "gmx_v2" in {str(connector) for _, _, connector in legacy}


def test_worker_timeout_and_failure_are_retained(tmp_path):
    with pytest.raises(RuntimeError, match="blocked"):
        run([sys.executable, "-c", "import time; time.sleep(30)"], tmp_path, "timeout", timeout=0.1)
    assert json.loads((tmp_path / "timeout.json").read_text())["category"] == "timeout"
    with pytest.raises(RuntimeError, match="failed"):
        run([sys.executable, "-c", "raise SystemExit(3)"], tmp_path, "failed")
    assert run([sys.executable, "-c", "print('complete')"], tmp_path, "pass")["status"] == "passed"


def test_network_isolation_blocks_external_access_but_allows_local_gateway(tmp_path):
    code = """
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
s.listen()
c = socket.create_connection(s.getsockname(), timeout=1)
c.close()
s.close()
try:
    socket.create_connection(('1.1.1.1', 443), timeout=1)
except OSError:
    pass
else:
    raise AssertionError('external networking was available')
"""
    result = subprocess.run(
        networkless([sys.executable, "-c", code]), capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0, result.stderr


def test_missing_env_leaves_required_case_blocked(tmp_path):
    case = Case(
        id="fork/example",
        lane="fork",
        connector=ConnectorId("aave_v4"),
        chains=(ChainId("eip155:1"),),
    )
    result = {**row(case), "artifact": "case"}
    qa_bench.execute(case, result, SimpleNamespace(output=tmp_path, env_file=None), 0, {})
    assert result["status"] == "blocked" and result["category"] == "missing_provider_env"
    write_json(tmp_path / "wire.json", {"chain": case.chains[0]})
    assert json.loads((tmp_path / "wire.json").read_text())["chain"] == "eip155:1"


def test_source_resolution_is_once_per_chain_and_keeps_healthy_targets(tmp_path, monkeypatch):
    from almanak.gateway.ethereum import DataError
    from almanak.gateway.fork_config import ForkTarget
    from almanak.models import Block
    from tests.qa.inputs import fresh_targets, resolve_origins

    cases = catalog()
    targets = list(fresh_targets(cases))
    # A converted reference selects its origin through the operator anchors, so no
    # fresh target carries a pinned block; the stand-in resolution is built here.
    blocks = {
        t.chain.chain_id: Block(chain=t.chain, number=1, hash="0x" + "11" * 32, timestamp=1)
        for t in targets
    }
    calls = []

    def resolve(target, env_file):
        assert target.block == "latest"
        calls.append(target.chain.chain_id)
        if str(target.chain.chain_id) == "eip155:43114":
            raise DataError("transport", "provider unavailable")
        return blocks[target.chain.chain_id]

    monkeypatch.setattr(ForkTarget, "resolve", resolve)
    resolved, failures = resolve_origins(cases, tmp_path / "env", tmp_path)
    assert len(calls) == len(blocks)
    assert set(calls) == set(blocks)
    assert set(resolved) == set(blocks) - {ChainId("eip155:43114")}
    assert set(failures) == {ChainId("eip155:43114")}
    resolved, failures = resolve_origins(cases, None, tmp_path)
    assert not resolved and set(failures) == set(blocks)


def test_source_snapshot_preserves_bytes_and_rejects_changed_or_secret_inputs(
    tmp_path, monkeypatch
):
    import tarfile

    from tests.qa import source
    from tests.qa.results import digest

    monkeypatch.setattr(source, "ROOT", tmp_path)
    path = tmp_path / "strategy.py"
    path.write_bytes(b"def decide(context): return None\n")
    identity = {"files": {path.name: digest(path)}}
    output = tmp_path / "source.tar.gz"
    source.snapshot(identity, output, ())
    with tarfile.open(output) as archive:
        assert archive.extractfile(path.name).read() == path.read_bytes()
    with pytest.raises(ValueError, match="credential"):
        source.source_bytes(path.name, digest(path), ("decide",))
    path.write_text("changed")
    with pytest.raises(ValueError, match="changed"):
        source.source_bytes(path.name, identity["files"][path.name], ())
    (tmp_path / "link.py").symlink_to(path)
    with pytest.raises(ValueError, match="links"):
        source.source_bytes("link.py", digest(path), ())


@pytest.mark.parametrize(
    "status,hours,conclusion,expected",
    [
        ("completed", 1, "success", "healthy"),
        ("completed", 31, "success", "stale"),
        ("completed", 1, "failure", "failed"),
        ("in_progress", 1, None, "running"),
        ("in_progress", 7, None, "stalled"),
    ],
)
def test_watchdog_detects_missing_stale_and_failed_runs(status, hours, conclusion, expected):
    from datetime import UTC, datetime, timedelta

    from scripts.qa_watchdog import check

    now = datetime(2026, 9, 7, tzinfo=UTC)
    record = {
        "event": "schedule",
        "status": status,
        "created_at": (now - timedelta(hours=hours)).isoformat(),
        "conclusion": conclusion,
    }
    assert check([record], now, 30)[0] == expected
    assert check([], now, 30)[0] == "missing"
    record["event"] = "workflow_dispatch"
    assert check([record], now, 30)[0] == "missing"


def test_runner_continues_after_child_failure_and_retains_unselected_cases(tmp_path, monkeypatch):
    from tests.qa.results import digest

    source = tmp_path / "strategy.py"
    source.write_text("pass\n")
    cases = [Case(id=name, lane="pytest") for name in ("fail", "pass", "unselected")]
    args = SimpleNamespace(
        output=tmp_path / "run",
        env_file=None,
        previous=None,
        case=["fail", "pass"],
        lane=None,
        workers=1,
        timeout=10,
        plan=False,
        frozen_inputs=None,
    )
    monkeypatch.setattr(qa_bench, "arguments", lambda: args)
    monkeypatch.setattr(qa_bench, "catalog", lambda: cases)
    monkeypatch.setattr(qa_bench, "validate", lambda cases: set())
    monkeypatch.setattr(qa_bench, "identity", lambda: {"files": {}, "tree_sha256": digest(source)})
    monkeypatch.setattr(
        qa_bench,
        "worker_command",
        lambda c, *args: [
            sys.executable,
            "-c",
            "raise SystemExit(2)" if c.id == "fail" else "print('completed')",
        ],
    )
    assert qa_bench.main() == 1
    result = json.loads((args.output / "public/results.json").read_text())
    assert [r["status"] for r in result["cases"]] == ["failed", "passed", "skipped"]
    assert result["status"] == "incomplete_or_failed"
    assert len(json.loads((args.output / "public/changes.json").read_text())) == 3


def test_source_changes_invalidate_success_and_recovery_is_reported(monkeypatch):
    from tests.qa.source import changes

    rows = [{"id": "case", "status": "passed", "category": ""}]
    metadata = {"identity": {"tree_sha256": "before"}}
    monkeypatch.setattr(qa_bench, "identity", lambda: {"tree_sha256": "after"})
    qa_bench.invalidate_changed_sources(metadata, rows)
    assert rows[0]["status"] == "blocked"
    previous = {"cases": [{"id": "case", "status": "failed", "category": "case_failed"}]}
    assert changes(rows, previous)[0]["after"] == ("blocked", "source_changed")
    assert changes(rows, {"cases": rows}) == []


def test_model_revision_labels_do_not_select_backtest_mode(tmp_path):
    case = next(
        c for c in catalog() if c.reference == "uniswap_v4_trading" and c.model_mode == "economic"
    )
    case = replace(case, model="opaque_revision_2")
    settings = resolved_history(case, tmp_path, 2000000000)
    from almanak.configuration.yaml import load

    assert load(settings).value["mode"] == "economic"


def test_case_mode_must_match_the_declared_model():
    cases = catalog()
    original = next(c for c in cases if c.model_mode == "economic")
    wrong = replace(original, model_mode="historical_path")
    with pytest.raises(ValueError, match="mode differs"):
        validate([wrong if c is original else c for c in cases])


def test_unpromoted_references_are_explicit_blockers_in_the_full_denominator(tmp_path, monkeypatch):
    from tests.qa import catalog as module
    from tests.qa import composed_catalog

    pending = {
        **module.CANDIDATES,
        "base_borrow_and_trade": (None, ("eip155:8453",)),
        "ethereum_base_borrow_and_trade": (None, ("eip155:1", "eip155:8453")),
    }
    monkeypatch.setattr(module, "CANDIDATES", pending)
    monkeypatch.setattr(
        composed_catalog,
        "COMPOSED_REFERENCES",
        {k: v for k, v in composed_catalog.COMPOSED_REFERENCES.items() if k not in pending},
    )
    cases = catalog()
    candidates = [c for c in cases if c.lane == "blocked"]
    assert {c.reference for c in candidates} == set(pending)
    results = [{**row(c), "status": "passed", "artifact": str(i)} for i, c in enumerate(cases)]
    args = SimpleNamespace(env_file=None)
    for case, result in zip(cases, results, strict=True):
        if case in candidates:
            assert not qa_bench.prerequisites(case, result, args, {})
            assert result["status"] == "blocked"
            assert result["category"] == "unproved_candidate"
            assert not case.obligations()
    assert save_results(tmp_path, {}, results)["status"] == "incomplete_or_failed"
    with pytest.raises(ValueError, match="candidate blockers"):
        validate([c for c in cases if c not in candidates])
    changed = replace(candidates[0], blocked_reason="")
    with pytest.raises(ValueError, match="candidate blockers"):
        validate([changed if c is candidates[0] else c for c in cases])
    from tests.qa.worker import dispatch

    with pytest.raises(ValueError, match="rollout proof gates"):
        dispatch(candidates[0], None)


def test_promoted_candidate_requires_real_proof_mappings(monkeypatch):
    from tests.qa import catalog as module

    reference = "pancakeswap_v3_bnb_lp"
    monkeypatch.setattr(
        module, "REFERENCES", {k: v for k, v in module.REFERENCES.items() if k != reference}
    )
    monkeypatch.setattr(
        module, "CANDIDATES", {**module.CANDIDATES, reference: ("pancakeswap_v3", ("eip155:56",))}
    )
    with pytest.raises(ValueError, match="promoted support requires"):
        module.catalog()


def test_public_eoa_cases_keep_the_fixture_of_their_operator_file(monkeypatch):
    """The case operator file already holds the EOA fixture; no harness transform applies."""
    from tests.qa import worker

    case = next(
        c
        for c in catalog()
        if c.lane == "fork" and c.profile == "eoa" and c.reference == "uniswap_v3_ethereum_lp"
    )
    composed = next(
        c
        for c in catalog()
        if c.lane == "fork" and c.profile == "eoa" and c.reference == "base_borrow_and_trade"
    )
    adapted = object()
    monkeypatch.setattr(worker, "author_fork_configuration", lambda *args: adapted)
    assert worker.eoa_fork_configuration(case, None, None) is adapted
    assert worker.eoa_fork_configuration(composed, None, None) is adapted


def test_legacy_composed_eoa_indices_apply_primary_then_uniswap_v4(monkeypatch):
    from tests.fork import test_aave_v3_base_eoa, test_pancakeswap_v3_eoa, test_uniswap_v4_eoa
    from tests.qa import worker

    calls = []

    def stage(label):
        def apply(value, **options):
            calls.append((label, value, options))
            return value + ":" + label

        return apply

    monkeypatch.setattr(test_aave_v3_base_eoa, "eoa_configuration", stage("eoa"))
    monkeypatch.setattr(test_pancakeswap_v3_eoa, "qualified_fund_indices", stage("primary"))
    monkeypatch.setattr(test_uniswap_v4_eoa, "qualified_fund_indices", stage("uniswap_v4"))

    case = SimpleNamespace(
        connector=ConnectorId("pancakeswap_v3"),
        reference="combined_lp",
        checks=lambda: (SimpleNamespace(connector=ConnectorId("uniswap_v4")),),
    )
    result = worker.indexed_eoa_configuration(case, "source")
    assert result == "source:eoa:primary:uniswap_v4"
    assert calls == [
        ("eoa", "source", {}),
        ("primary", "source:eoa", {"lp": True}),
        ("uniswap_v4", "source:eoa:primary", {"lp": True}),
    ]


def test_worker_command_carries_the_case_timeout(tmp_path):
    case = Case(id="backtest/pinned/reference/economic", lane="backtest")
    args = SimpleNamespace(output=tmp_path, env_file=None, workers=4, timeout=5400)
    command = qa_bench.worker_command(case, tmp_path / "case", args, 1)
    assert command[command.index("--timeout") + 1] == "5400"
    assert command[command.index("--workers") + 1] == "1"


def test_interrupted_case_is_retained_as_attempted_and_stops_the_batch(tmp_path, monkeypatch):
    case = Case(id="offline/support-refusals", lane="pytest", nodes=("tests/qa/test_support.py",))
    result = {**row(case), "artifact": "cases/0"}
    args = SimpleNamespace(output=tmp_path, env_file=None, workers=1, timeout=10)

    def interrupt(command, directory, *args, **kwargs):
        directory.mkdir(parents=True)
        (directory / "partial.json").write_text('{"phase":"started"}')
        raise KeyboardInterrupt

    monkeypatch.setattr(qa_bench, "run", interrupt)
    with pytest.raises(KeyboardInterrupt):
        qa_bench.execute(case, result, args, 0, {})
    assert result["status"] == "blocked" and result["category"] == "interrupted"
    assert result["wall_seconds"] >= 0
    assert (tmp_path / "public/cases/0/partial.json").is_file()


def test_fault_worker_fails_fast_without_changing_nodes_or_serialization(tmp_path, monkeypatch):
    from tests.qa import worker

    calls = []

    def measured(command, directory, name, **options):
        calls.append((command, options["timeout"]))
        if name == "pytest":
            (directory / "pytest.xml").write_text('<testsuite><testcase name="proof"/></testsuite>')

    monkeypatch.setattr(worker, "run", measured)
    nodes = ("tests/fork", "tests/strategy_fork/test_composed_outages.py")
    case = Case(id="fork/faults", lane="faults", nodes=nodes)
    worker.pytest_case(case, tmp_path, tmp_path / "operator.env", 1, timeout=7200)
    command, timeout = calls[0]
    assert timeout == 7200
    assert command[3:5] == list(nodes)
    assert command[command.index("-n") + 1] == "1"
    assert command.count("--maxfail=1") == 1


def test_offline_worker_keeps_full_test_scope_without_fail_fast(tmp_path, monkeypatch):
    from tests.qa import worker

    calls = []

    def measured(command, directory, name, **options):
        calls.append((name, command))
        if name == "pytest":
            (directory / "pytest.xml").write_text('<testsuite><testcase name="proof"/></testsuite>')

    monkeypatch.setattr(worker, "run", measured)
    nodes = ("tests", "strategies")
    case = Case(id="offline/quality", lane="offline", nodes=nodes)
    worker.pytest_case(case, tmp_path, None, "auto")
    assert [name for name, _ in calls] == ["lint", "format", "types", "pytest"]
    command = calls[-1][1]
    assert command[2:4] == list(nodes)
    assert command[command.index("-n") + 1] == "auto"
    assert "--maxfail=1" not in command


def test_a_composed_reference_is_fork_only_exactly_when_a_component_declares_no_backtest(
    monkeypatch, tmp_path
):
    from tests.qa import composed_catalog
    from tests.qa.catalog import ROOT, discover

    sources = {
        source.name: source
        for source in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if source.name in composed_catalog.COMPOSED_REFERENCES
    }

    def lanes(found):
        return {c.lane for c in found}

    def fork_only_references():
        produced = {name: list(composed_catalog.composed_cases(s)) for name, s in sources.items()}
        assert all(lanes(found) - {"backtest"} for found in produced.values())
        return {name for name, found in produced.items() if "backtest" not in lanes(found)}

    # Every component of every composed reference declares backtest on its chain, the
    # bridge included, so none is fork-only and the bridge reference owes backtest cases.
    assert fork_only_references() == set()
    bridged = list(composed_catalog.composed_cases(sources["arbitrum_base_bridge_and_lend"]))
    backtests = [c for c in bridged if c.lane == "backtest"]
    assert {c.input_policy for c in backtests} == {"pinned", "fresh", "native"}
    assert all(
        {scope.connector: scope.model for scope in c.scopes}[ConnectorId("across")]
        == "across-full-fee-ceiling-fixed-latency-v1"
        for c in backtests
    )
    # The category is derived from the declaration: with Across declaring fork mode
    # alone, the bridge reference is fork-only and no other reference changes.
    installed = dict(composed_catalog.installed_connectors())
    across = installed[ConnectorId("across")]
    fork_alone = replace(
        across,
        chain_support=tuple(
            s.model_copy(
                update={
                    "modes": tuple(m for m in s.modes if m != RunMode.BACKTEST),
                    "historical": (),
                }
            )
            for s in across.chain_support
        ),
    )
    with monkeypatch.context() as patched:
        patched.setattr(
            composed_catalog,
            "installed_connectors",
            lambda: tuple({**installed, ConnectorId("across"): fork_alone}.items()),
        )
        assert fork_only_references() == {"arbitrum_base_bridge_and_lend"}
    fork_only = set()
    # Deriving the category skips no obligation. The bridge is proven by this reference
    # and by the standalone one; without both the catalog has a coverage gap.
    cases = catalog()
    bridge = [
        c
        for c in cases
        if any(
            check.connector == ConnectorId("across") and EconomicAction.BRIDGE in check.actions
            for check in c.checks()
        )
    ]
    assert {c.reference for c in bridge} == {
        "across_usdc_arbitrum_base_bridge",
        "arbitrum_base_bridge_and_lend",
    }
    with pytest.raises(ValueError, match="coverage gaps"):
        validate([c for c in cases if c not in bridge])
    # A reference whose components all declare backtest still owes its configuration.
    other = next(s for name, s in sources.items() if name not in fork_only)
    moved = tmp_path / "config.yaml"
    moved.write_bytes(other.config.read_bytes())
    with pytest.raises(ValueError, match="missing backtest-economic.yaml"):
        list(composed_catalog.composed_cases(replace(other, config=moved)))
    # Components that share no account profile leave no fork case; that is refused.
    source = sources["arbitrum_base_bridge_and_lend"]
    profiles = iter(({"eoa"}, {"safe_roles"}))
    monkeypatch.setattr(composed_catalog, "declared_profiles", lambda *scope: next(profiles))
    with pytest.raises(ValueError, match="no common account profile"):
        list(composed_catalog.composed_cases(source))
