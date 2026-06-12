"""Tests for nightly-test-builds/probe_runner.py — deterministic mainnet probes.

The probe runner replaces the Claude Code session that previously drove the
nightly mainnet probe job. These tests pin the deterministic pieces the LLM
used to do ad hoc: probe discovery (skip flags, chain filtering), lifecycle
run-flag derivation from intent_types, failure-type classification, and a
full_report.md whose Detailed Results table stays parseable by
post_results_to_slack.py's table parser (the Slack summary contract).
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

_NIGHTLY_DIR = Path(__file__).resolve().parents[2] / "nightly-test-builds"


def _load_script_module(name: str):
    script_path = _NIGHTLY_DIR / f"{name}.py"
    # probe_runner does `import direct_runner` (same-dir sibling); make the
    # nightly dir importable the same way `python nightly-test-builds/...` does.
    if str(_NIGHTLY_DIR) not in sys.path:
        sys.path.insert(0, str(_NIGHTLY_DIR))
    spec = importlib.util.spec_from_file_location(f"{name}_module", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {name} from {script_path}")  # noqa: TRY003
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def probe_runner():
    return _load_script_module("probe_runner")


@pytest.fixture(scope="module")
def slack_helper():
    return _load_script_module("post_results_to_slack")


def _write_probe(
    probes_dir: Path,
    name: str,
    config: dict | str,
    intent_types: list[str] | None = None,
) -> Path:
    probe_dir = probes_dir / name
    probe_dir.mkdir(parents=True)
    config_path = probe_dir / "config.json"
    if isinstance(config, str):
        config_path.write_text(config)
    else:
        config_path.write_text(json.dumps(config))
    if intent_types is not None:
        types_repr = ", ".join(f'"{t}"' for t in intent_types)
        (probe_dir / "strategy.py").write_text(
            "@almanak_strategy(\n"
            f"    intent_types=[{types_repr}],\n"
            '    default_chain="base",\n'
            ")\n"
            "class Probe: ...\n"
        )
    return probe_dir


# ──────────────────────────────────────────────────────────────────────────
# USDC balance RPC
# ──────────────────────────────────────────────────────────────────────────


class _FakeResponse:
    def __init__(self, body: dict):
        self._body = json.dumps(body).encode()

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_fetch_usdc_base_balance_success(probe_runner, monkeypatch):
    # 5 USDC = 5_000_000 raw units (6 decimals)
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda req, timeout: _FakeResponse({"jsonrpc": "2.0", "id": 1, "result": hex(5_000_000)}),
    )
    balance = probe_runner.fetch_usdc_base_balance("0x" + "11" * 20, "key")
    assert balance == 5.0


def test_fetch_usdc_base_balance_surfaces_rpc_error(probe_runner, monkeypatch):
    # JSON-RPC errors come back 200 OK with an "error" payload; the message
    # must survive the retries into the final RuntimeError, not be swallowed
    # as KeyError('result').
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda req, timeout: _FakeResponse(
            {"jsonrpc": "2.0", "id": 1, "error": {"code": -32005, "message": "rate limited"}}
        ),
    )
    sleeps: list[float] = []
    monkeypatch.setattr(probe_runner.time, "sleep", sleeps.append)

    with pytest.raises(RuntimeError, match="rate limited"):
        probe_runner.fetch_usdc_base_balance("0x" + "11" * 20, "key")
    # All attempts exhausted, with backoff between them
    assert len(sleeps) == probe_runner.MAX_RPC_RETRIES - 1


# ──────────────────────────────────────────────────────────────────────────
# Run-flag derivation (lifecycle budgets from the retired test-probes prompt)
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("intent_types", "expected"),
    [
        (["BRIDGE", "SWAP", "PERP_OPEN", "PERP_CLOSE"], (10, 8)),
        (["BRIDGE", "SWAP", "LP_OPEN", "LP_CLOSE"], (5, 7)),
        (["LP_OPEN", "LP_CLOSE", "HOLD"], (5, 7)),
        (["BRIDGE", "SWAP", "SUPPLY", "BORROW", "REPAY", "WITHDRAW"], (5, 6)),
        (["SWAP"], (5, 3)),
        (["SWAP", "SUPPLY", "WITHDRAW"], (5, 3)),
        ([], (5, 3)),
    ],
)
def test_derive_run_flags(probe_runner, intent_types, expected):
    assert probe_runner.derive_run_flags(intent_types) == expected


def test_perp_takes_precedence_over_lp_and_bridge(probe_runner):
    flags = probe_runner.derive_run_flags(["BRIDGE", "LP_OPEN", "PERP_OPEN"])
    assert flags == (10, 8)


def test_extract_intent_types_from_decorator(probe_runner, tmp_path):
    probe_dir = _write_probe(
        tmp_path, "perp_probe", {"chain": "base"},
        intent_types=["BRIDGE", "SWAP", "PERP_OPEN", "PERP_CLOSE"],
    )
    types = probe_runner.extract_intent_types(probe_dir)
    assert types == ["BRIDGE", "SWAP", "PERP_OPEN", "PERP_CLOSE"]


def test_extract_intent_types_missing_strategy_py(probe_runner, tmp_path):
    probe_dir = tmp_path / "empty"
    probe_dir.mkdir()
    assert probe_runner.extract_intent_types(probe_dir) == []


# ──────────────────────────────────────────────────────────────────────────
# Probe discovery
# ──────────────────────────────────────────────────────────────────────────


def test_discover_probes_basic(probe_runner, tmp_path):
    _write_probe(tmp_path, "base_probe", {"chain": "base"})
    _write_probe(tmp_path, "arb_probe", {"chains": ["base", "arbitrum"], "target_chain": "arbitrum"})
    runnable, config_skipped = probe_runner.discover_probes(tmp_path, set())
    assert [p["name"] for p in runnable] == ["arb_probe", "base_probe"]
    assert runnable[0]["chain"] == "arbitrum"  # target_chain wins over chains
    assert config_skipped == []


def test_discover_probes_honors_skip_flag(probe_runner, tmp_path):
    _write_probe(tmp_path, "gmx_probe", {
        "skip": True, "skip_reason": "keeper fees too high", "chain": "base",
    })
    _write_probe(tmp_path, "live_probe", {"chain": "base"})
    runnable, config_skipped = probe_runner.discover_probes(tmp_path, set())
    assert [p["name"] for p in runnable] == ["live_probe"]
    assert len(config_skipped) == 1
    assert config_skipped[0]["name"] == "gmx_probe"
    assert config_skipped[0]["skip_reason"] == "keeper fees too high"


def test_discover_probes_chain_filter_omits_mismatches(probe_runner, tmp_path):
    _write_probe(tmp_path, "avax_probe", {"target_chain": "avalanche", "chains": ["base", "avalanche"]})
    _write_probe(tmp_path, "arb_probe", {"target_chain": "arbitrum", "chains": ["base", "arbitrum"]})
    runnable, config_skipped = probe_runner.discover_probes(tmp_path, {"arbitrum", "base"})
    assert [p["name"] for p in runnable] == ["arb_probe"]
    assert config_skipped == []


def test_discover_probes_broken_config_is_a_failure_not_a_skip(probe_runner, tmp_path):
    _write_probe(tmp_path, "broken_probe", "{not json")
    runnable, config_skipped = probe_runner.discover_probes(tmp_path, set())
    assert len(runnable) == 1
    assert runnable[0]["config_error"] is not None
    assert config_skipped == []


def test_discover_probes_ignores_non_probe_dirs(probe_runner, tmp_path):
    (tmp_path / "__pycache__").mkdir()
    (tmp_path / "no_config").mkdir()
    runnable, config_skipped = probe_runner.discover_probes(tmp_path, set())
    assert runnable == []
    assert config_skipped == []


def test_probe_chain_defaults_to_base(probe_runner):
    assert probe_runner.probe_chain({}) == "base"
    assert probe_runner.probe_chain({"chain": "Base"}) == "base"
    assert probe_runner.probe_chain({"chain": "base", "target_chain": "Arbitrum"}) == "arbitrum"


# ──────────────────────────────────────────────────────────────────────────
# Failure-type classification
# ──────────────────────────────────────────────────────────────────────────


def _fail_result(probe_runner, **overrides) -> dict:
    result = probe_runner._empty_result("probe", "base")
    result.update(overrides)
    return result


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"config_error": "bad json"}, "config_error"),
        ({"outcome": "SKIPPED"}, "funding_failure"),
        ({"outcome": "PARTIAL"}, "stranded_funds"),
        ({"outcome": "TIMEOUT"}, "timeout"),
        ({"stderr": "Transaction reverted: tx_hash=abc"}, "execution_revert"),
        ({"stdout": "BridgeError: relay request failed"}, "bridge_failure"),
        ({"stderr": "keeper did not execute order"}, "keeper_timeout"),
        ({"stderr": "grpc StatusCode.UNAVAILABLE: failed to connect to all addresses"}, "gateway_error"),
        ({"stderr": "insufficient funds for gas * price + value"}, "funding_failure"),
        ({"stderr": "something inexplicable"}, "other"),
    ],
)
def test_classify_failure(probe_runner, overrides, expected):
    assert probe_runner.classify_failure(_fail_result(probe_runner, **overrides)) == expected


def test_all_failure_types_are_in_the_enum(probe_runner):
    for failure_type, _ in probe_runner._FAILURE_MARKERS:
        assert failure_type in probe_runner.FAILURE_TYPES


# ──────────────────────────────────────────────────────────────────────────
# Report generation + Slack-parser round trip
# ──────────────────────────────────────────────────────────────────────────


def _sample_results(probe_runner) -> list[dict]:
    ok = probe_runner._empty_result("base_swap_relay", "base")
    ok.update({
        "status": "PASS", "outcome": "EXECUTED",
        "tx_hashes": ["0x" + "ab" * 32, "0x" + "cd" * 32],
        "reason": "Strategy executed successfully with on-chain transactions",
        "next_action": "Monitor for regressions",
        "budget_in": 10.00, "budget_out": 9.95, "loss": 0.05,
    })
    fail = probe_runner._empty_result("arbitrum_full_cycle", "arbitrum")
    fail.update({
        "status": "FAIL", "outcome": "ERROR",
        "reason": "Error: execution reverted | with a pipe",
        "next_action": "Investigate error logs and fix root cause",
        "error_snippet": "Transaction reverted: tx_hash=ff",
        "budget_in": 9.95, "budget_out": 8.10, "loss": 1.85,
        "failure_type": "execution_revert",
    })
    skipped = probe_runner._empty_result("avalanche_lp_lifecycle", "avalanche")
    skipped.update({
        "status": "FAIL", "outcome": "SKIPPED",
        "reason": "Skipped: USDC on Base $1.20 below $3.00 minimum",
        "next_action": "Top up nightly wallet with USDC on Base",
        "budget_in": 1.20, "budget_out": 1.20, "loss": 0.0,
        "failure_type": "funding_failure",
    })
    return [ok, fail, skipped]


def test_full_report_round_trips_through_slack_parser(
    probe_runner, slack_helper, tmp_path,
):
    report_path = tmp_path / "full_report.md"
    probe_runner.write_full_report(
        report_path,
        _sample_results(probe_runner),
        config_skipped=[{"name": "gmx_probe", "chain": "arbitrum", "skip_reason": "fees | high"}],
        wallet="0x" + "11" * 20,
        git_commit="a" * 40,
        repo_url="https://github.com/almanak-co/almanak-sdk-private",
        min_balance_usd=3.0,
    )
    report = report_path.read_text()

    rows = slack_helper._parse_report_rows(report)
    assert len(rows) == 3
    by_name = {r["strategy"]: r for r in rows}
    assert by_name["base_swap_relay"]["status"] == "PASS"
    assert by_name["base_swap_relay"]["source"] == "probes"
    assert by_name["arbitrum_full_cycle"]["status"] == "FAIL"
    assert by_name["arbitrum_full_cycle"]["chain"] == "arbitrum"
    assert by_name["avalanche_lp_lifecycle"]["outcome"] == "SKIPPED"
    # Pipe characters in reasons must not break the table shape
    assert "with a pipe" in by_name["arbitrum_full_cycle"]["reason"]


def test_full_report_sections(probe_runner, tmp_path):
    report_path = tmp_path / "full_report.md"
    probe_runner.write_full_report(
        report_path,
        _sample_results(probe_runner),
        config_skipped=[{"name": "gmx_probe", "chain": "arbitrum", "skip_reason": "fees"}],
        wallet="0x" + "11" * 20,
        git_commit="a" * 40,
        repo_url="https://github.com/almanak-co/almanak-sdk-private",
        min_balance_usd=3.0,
    )
    report = report_path.read_text()

    assert "# Nightly Probe Test Report -- mainnet" in report
    assert "| Pass | 1 |" in report
    assert "| Fail | 1 |" in report
    assert "| Skip (balance) | 1 |" in report
    assert "| Skip (config) | 1 |" in report
    # Failures become P0 action items with their failure type
    assert "| P0 | Fix execution_revert in arbitrum_full_cycle (arbitrum) | execution_revert |" in report
    assert "| P0 | Fix funding_failure in avalanche_lp_lifecycle (avalanche) | funding_failure |" in report
    # Primary blocker names the first failure
    assert "**Primary blocker:** execution_revert in arbitrum_full_cycle" in report
    # Config-skipped probes are listed but outside the Detailed Results table
    assert "## Skipped Probes (config)" in report
    assert "gmx_probe" in report
    # Full TX hashes and error snippets are present
    assert "0x" + "ab" * 32 in report
    assert "Transaction reverted: tx_hash=ff" in report
    # The commit line the entrypoint greps for
    assert "a" * 40 in report


def test_full_report_all_pass_has_no_action_items(probe_runner, tmp_path):
    ok = probe_runner._empty_result("base_swap_relay", "base")
    ok.update({
        "status": "PASS", "outcome": "EXECUTED", "tx_hashes": ["0x" + "ab" * 32],
        "reason": "ok", "next_action": "none", "budget_in": 10.0, "budget_out": 9.9, "loss": 0.1,
    })
    report_path = tmp_path / "full_report.md"
    probe_runner.write_full_report(
        report_path, [ok], config_skipped=[],
        wallet="0x" + "11" * 20, git_commit="a" * 40,
        repo_url="https://github.com/x/y", min_balance_usd=3.0,
    )
    report = report_path.read_text()
    assert "No action items -- all probes passed." in report
    assert "**Primary blocker:** none" in report
    assert "## Skipped Probes" not in report


def test_debug_log_contains_budget_lines(probe_runner, tmp_path):
    log_path = tmp_path / "debug.log"
    probe_runner.write_debug_log(log_path, _sample_results(probe_runner), "a" * 40)
    log = log_path.read_text()
    assert "## base_swap_relay [chain=base]: PASS (EXECUTED)" in log
    assert "Budget: $10.00 -> $9.95 (loss: $0.05)" in log
    assert "Failure type: execution_revert" in log
