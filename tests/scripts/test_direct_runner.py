"""Tests for nightly-test-builds/direct_runner.py — harness status determination.

VIB-3755: the harness wall-clock timeout previously short-circuited every run
that hadn't returned within the budget to ``FAIL / TIMEOUT``, even when the
strategy had successfully submitted on-chain transactions and only the
``iteration_summary`` log line failed to drain before the kill. This produced
false-negative regressions for slow-but-successful runs in the QA April 29
batches.

These tests pin the new behaviour: with ``timed_out=True`` we trust ``tx_hashes``
as a signal of success, while still respecting any explicit error status the
strategy logged before the timeout fired.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_direct_runner_module():
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "nightly-test-builds" / "direct_runner.py"
    spec = importlib.util.spec_from_file_location("direct_runner_module", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load direct_runner from {script_path}")  # noqa: TRY003
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def direct_runner():
    return _load_direct_runner_module()


# A canonical-looking 32-byte hex transaction hash for fixture data.
_TX_HASH_OK = "0x" + "ab" * 32


# ──────────────────────────────────────────────────────────────────────────
# Pre-VIB-3755 invariants — must keep working
# ──────────────────────────────────────────────────────────────────────────


def test_clean_success_status_line_returns_executed(direct_runner):
    status, outcome = direct_runner.determine_status(
        exit_code=0,
        tx_hashes=[_TX_HASH_OK],
        stdout="Status: SUCCESS | Intent: SWAP | Duration: 1234ms\n",
        stderr="",
        timed_out=False,
    )
    assert (status, outcome) == ("PASS", "EXECUTED")


def test_clean_hold_status_line_returns_hold(direct_runner):
    status, outcome = direct_runner.determine_status(
        exit_code=0,
        tx_hashes=[],
        stdout="Status: HOLD | Intent: HOLD | Duration: 80ms\n",
        stderr="",
        timed_out=False,
    )
    assert (status, outcome) == ("PASS", "HOLD")


def test_execution_failed_returns_error(direct_runner):
    status, outcome = direct_runner.determine_status(
        exit_code=1,
        tx_hashes=[],
        stdout="Status: EXECUTION_FAILED | Intent: SWAP | Error: revert | Duration: 40ms\n",
        stderr="",
        timed_out=False,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_non_zero_exit_with_blocked_pattern(direct_runner):
    status, outcome = direct_runner.determine_status(
        exit_code=2,
        tx_hashes=[],
        stdout="",
        stderr="RiskGuard blocked by leverage cap",
        timed_out=False,
    )
    assert (status, outcome) == ("FAIL", "BLOCKED")


# ──────────────────────────────────────────────────────────────────────────
# VIB-3755 — tx_hash heuristic on timeout
# ──────────────────────────────────────────────────────────────────────────


def test_timeout_without_tx_hashes_is_still_failure(direct_runner):
    """No tx_hashes + timeout = real failure; do not silently flip to PASS."""
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[],
        stdout="",
        stderr="",
        timed_out=True,
    )
    assert (status, outcome) == ("FAIL", "TIMEOUT")


def test_timeout_with_tx_hashes_is_executed(direct_runner):
    """VIB-3755: tx_hash present at timeout = the strategy executed."""
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[_TX_HASH_OK],
        stdout="tx_hash=" + _TX_HASH_OK[2:] + "\n",
        stderr="",
        timed_out=True,
    )
    assert (status, outcome) == ("PASS", "EXECUTED")


def test_timeout_with_tx_hashes_but_explicit_failure_status_wins(direct_runner):
    """If the strategy logged an explicit failure status before the timeout,
    we trust that signal even when tx_hashes were emitted earlier in the run."""
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[_TX_HASH_OK],
        stdout=(
            "tx_hash=" + _TX_HASH_OK[2:] + "\n"
            "Status: EXECUTION_FAILED | Intent: SWAP | Error: simulator timeout | Duration: 40ms\n"
        ),
        stderr="",
        timed_out=True,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_timeout_with_compilation_failure_is_error(direct_runner):
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[],
        stdout="Status: COMPILATION_FAILED | Intent: SWAP | Error: bad pool | Duration: 12ms\n",
        stderr="",
        timed_out=True,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_timeout_with_multiple_tx_hashes_is_executed(direct_runner):
    """Lifecycle strategies submit multiple txs; any tx_hash + timeout = PASS."""
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[_TX_HASH_OK, "0x" + "cd" * 32],
        stdout="",
        stderr="",
        timed_out=True,
    )
    assert (status, outcome) == ("PASS", "EXECUTED")


# ──────────────────────────────────────────────────────────────────────────
# VIB-3755 — failure-precedence on timeout (Codex P2 + pr-auditor Important #1)
# ──────────────────────────────────────────────────────────────────────────


def test_timeout_with_tx_hash_then_revert_log_is_failure(direct_runner):
    """Submitted-then-reverted: tx_hash present + ``Transaction reverted: tx_hash=``
    log line in stdout must classify FAIL, not silently flip to PASS.

    Without this guard, a strategy whose tx submits successfully but reverts
    on-chain (then spins/retries until wall-clock timeout) would be marked
    PASS/EXECUTED — hiding a real regression in nightly runs.
    """
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[_TX_HASH_OK],
        stdout=(
            f"Transaction submitted: tx_hash={_TX_HASH_OK[2:]}, latency=1500ms\n"
            f"Transaction reverted: tx_hash={_TX_HASH_OK[2:]}, reason=ERC20: insufficient allowance\n"
        ),
        stderr="",
        timed_out=True,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_timeout_with_transaction_not_found_log_is_failure(direct_runner):
    """``Transaction not found: tx_hash=`` is a hard failure signal even with
    other tx_hashes in the run (the submitter logs this when receipt
    polling exhausts retries)."""
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[_TX_HASH_OK],
        stdout="",
        stderr=f"Transaction not found: tx_hash={_TX_HASH_OK[2:]}\n",
        timed_out=True,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_timeout_with_transaction_reverted_error_traceback_is_failure(direct_runner):
    """Tracebacks containing ``TransactionRevertedError`` (the exception class
    name) also count as a hard failure signal, since the exception fires
    when the submitter raises after a confirmed revert."""
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[_TX_HASH_OK],
        stdout="",
        stderr=(
            "Traceback (most recent call last):\n"
            "  File \"submitter/public.py\", line 1018, in _confirm\n"
            "    raise TransactionRevertedError(tx_hash=...)\n"
            "TransactionRevertedError: tx reverted on chain\n"
        ),
        timed_out=True,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_timeout_with_iteration_failed_log_is_failure(direct_runner):
    """``Iteration failed: ...`` is parsed by ``_parse_status_line`` as an
    error message even when no formal ``Status:`` line was emitted. The
    timeout heuristic must honor it (Codex P2 finding)."""
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[_TX_HASH_OK],
        stdout=(
            f"Transaction submitted: tx_hash={_TX_HASH_OK[2:]}, latency=900ms\n"
            "Iteration failed: state machine wedged in COMPILE\n"
        ),
        stderr="",
        timed_out=True,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_tx_failure_log_markers_are_documented_and_complete(direct_runner):
    """Pin the exact set of failure-log markers the heuristic recognises so
    a future edit cannot accidentally drop one. If you add a new submitter
    error log line, update both the constant and this test."""
    assert direct_runner._TX_FAILURE_LOG_MARKERS == (
        "Transaction reverted: tx_hash=",
        "Transaction not found: tx_hash=",
        "TransactionRevertedError",
    )


# ──────────────────────────────────────────────────────────────────────────
# tx_hash extraction (sanity)
# ──────────────────────────────────────────────────────────────────────────


def test_extract_tx_hashes_normalizes_prefix(direct_runner):
    raw = "submitted tx_hash=" + ("ab" * 32) + " ok"
    hashes = direct_runner.extract_tx_hashes(raw)
    assert hashes == ["0x" + "ab" * 32]


def test_extract_tx_hashes_dedupes_in_order(direct_runner):
    h1 = "ab" * 32
    h2 = "cd" * 32
    raw = f"tx_hash={h1} ... tx_hash={h2} ... tx_hash={h1}"
    hashes = direct_runner.extract_tx_hashes(raw)
    assert hashes == ["0x" + h1, "0x" + h2]


# ──────────────────────────────────────────────────────────────────────────
# Timestamped Status lines + ACCOUNTING_FAILED (June 12 mainnet probe run)
# ──────────────────────────────────────────────────────────────────────────

# Real shape from the arbitrum_full_cycle failure: the CLI timestamps the
# Status line, and the anchored ^Status: pattern missed it — the harness then
# reported an unrelated stderr deprecation warning as the failure reason.
_TIMESTAMPED_ACCOUNTING_FAILED = (
    "[2026-06-12 06:09:52] Status: ACCOUNTING_FAILED | Intent: BRIDGE | "
    "Error: Accounting persistence failed (snapshot): native-gas append failed "
    "in live mode (gas_native_status='price_missing') | Duration: 8078ms\n"
)


def test_timestamped_status_line_is_parsed(direct_runner):
    keyword, error = direct_runner._parse_status_line(_TIMESTAMPED_ACCOUNTING_FAILED)
    assert keyword == "ACCOUNTING_FAILED"
    assert "native-gas append failed" in error


def test_accounting_failed_keyword_is_a_failure(direct_runner):
    status, outcome = direct_runner.determine_status(
        exit_code=1,
        tx_hashes=[],
        stdout=_TIMESTAMPED_ACCOUNTING_FAILED,
        stderr="",
        timed_out=False,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_extract_error_prefers_status_line_over_stderr_warnings(direct_runner):
    stderr = (
        "/app/almanak/cli/cli.py:151: UserWarning: GATEWAY_HOST is deprecated. "
        "Use ALMANAK_GATEWAY_HOST instead. Legacy unprefixed gateway env vars "
        "will be removed in a future release.\n"
        "  warn_legacy_gateway_envvars()\n"
    )
    error = direct_runner.extract_error(_TIMESTAMPED_ACCOUNTING_FAILED, stderr)
    assert "native-gas append failed" in error
    assert "deprecated" not in error


def test_extract_error_filters_deprecation_warnings_as_noise(direct_runner):
    # No Status line at all: the stderr fallback must not surface the
    # deprecation warnings as the failure reason.
    stderr = (
        "/app/almanak/cli/cli.py:151: UserWarning: GATEWAY_PORT is deprecated. "
        "Use ALMANAK_GATEWAY_PORT instead. Legacy unprefixed gateway env vars "
        "will be removed in a future release.\n"
        "  warn_legacy_gateway_envvars()\n"
    )
    error = direct_runner.extract_error("", stderr)
    assert error == "No error output"


# ──────────────────────────────────────────────────────────────────────────
# VIB-5373(d) manifest preflight + (a) scope-aware JSON artifact
# ──────────────────────────────────────────────────────────────────────────


def _make_strategy(tmp_path, name, chain, protocols, intents):
    """Write a minimal strategy folder (strategy.py + config.json) on disk."""
    d = tmp_path / name
    d.mkdir(parents=True)
    proto_lit = ", ".join(f'"{p}"' for p in protocols)
    intent_lit = ", ".join(f'"{i}"' for i in intents)
    (d / "strategy.py").write_text(
        "from almanak import almanak_strategy\n\n"
        "@almanak_strategy(\n"
        f"    supported_protocols=[{proto_lit}],\n"
        f"    intent_types=[{intent_lit}],\n"
        ")\n"
        "class S:\n    pass\n"
    )
    (d / "config.json").write_text(f'{{"chain": "{chain}"}}\n')
    return d


def test_preflight_partition_skips_unsupported_runs_supported(direct_runner, tmp_path):
    """A structurally-impossible combo (PancakeSwap V3 on Optimism) is skipped;
    a supported one (Uniswap V3 on Arbitrum) is kept. This is the runner-level
    proof that an unsupported (protocol, chain, intent) is NOT enqueued."""
    bad = _make_strategy(
        tmp_path, "pancakeswap_v3_swap_optimism", "optimism", ["pancakeswap_v3"], ["SWAP"]
    )
    good = _make_strategy(
        tmp_path, "uniswap_rsi_arbitrum", "arbitrum", ["uniswap_v3"], ["SWAP"]
    )
    runnable, skipped = direct_runner.preflight_partition([bad, good])
    assert [p.name for p in runnable] == ["uniswap_rsi_arbitrum"]
    assert len(skipped) == 1
    assert skipped[0]["name"] == "pancakeswap_v3_swap_optimism"
    assert "pancakeswap_v3" in skipped[0]["reason"]


def test_preflight_partition_fails_open_on_unknown(direct_runner, tmp_path):
    """Unresolvable protocol -> never skipped (fail-open)."""
    s = _make_strategy(tmp_path, "mystery", "optimism", ["not_a_connector"], ["SWAP"])
    runnable, skipped = direct_runner.preflight_partition([s])
    assert [p.name for p in runnable] == ["mystery"]
    assert skipped == []


def test_results_artifact_carries_scope_and_skip_signals(direct_runner, tmp_path):
    """The machine-readable results.json must carry the in-scope fingerprint
    allowlist (a), per-result fingerprints + root cause (b/c), and the
    skipped-unsupported combos (d) — the merge step drops everything else."""
    import json

    results = [
        {
            "name": "foo_arb",
            "source": "demo",
            "status": "PASS",
            "outcome": "EXECUTED",
            "tx_hashes": [_TX_HASH_OK],
            "failure_type": "",
            "root_cause": "",
            "fingerprint": "foo_arb:arbitrum",
        },
        {
            "name": "bar_arb",
            "source": "incubating",
            "status": "FAIL",
            "outcome": "ERROR",
            "tx_hashes": [],
            "failure_type": "anvil_fork_start_failure",
            "root_cause": "Managed Anvil fork failed to start",
            "fingerprint": "bar_arb:arbitrum",
        },
        {
            # A FAIL whose error matched no root-cause rule: root_cause is the
            # empty string (the run_strategy `or ""` coercion). Locks the JSON
            # type so a future regression to None/null is caught here.
            "name": "baz_arb",
            "source": "demo",
            "status": "FAIL",
            "outcome": "ERROR",
            "tx_hashes": [],
            "failure_type": "other",
            "root_cause": "",
            "fingerprint": "baz_arb:arbitrum",
        },
    ]
    skipped = [{"name": "pancake_opt", "chain": "optimism", "reason": "unsupported (protocol, chain)"}]

    direct_runner.write_results_artifact(
        tmp_path, "arbitrum", "anvil", results, skipped, "deadbeef",
    )
    raw = (tmp_path / "results.json").read_text()
    data = json.loads(raw)

    assert data["schema_version"] == 1
    assert data["chain"] == "arbitrum"
    # (a) in-scope allowlist = every evaluated fingerprint.
    assert set(data["in_scope_fingerprints"]) == {
        "foo_arb:arbitrum", "bar_arb:arbitrum", "baz_arb:arbitrum",
    }
    # (b/c) per-result fingerprint + root cause survive.
    bar = next(r for r in data["results"] if r["name"] == "bar_arb")
    assert bar["fingerprint"] == "bar_arb:arbitrum"
    assert bar["root_cause"] == "Managed Anvil fork failed to start"
    assert bar["failure_type"] == "anvil_fork_start_failure"
    # No-root-cause failure stays an empty string, never JSON null.
    baz = next(r for r in data["results"] if r["name"] == "baz_arb")
    assert baz["root_cause"] == ""
    assert baz["root_cause"] is not None
    assert '"root_cause": null' not in raw
    # (d) skipped-unsupported list is present and NOT in results.
    assert data["skipped_unsupported"] == skipped
    assert "pancake_opt" not in {r["name"] for r in data["results"]}
