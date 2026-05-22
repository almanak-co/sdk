"""Tests for iteration_summary CLOB field surfacing (VIB-3709).

Off-chain Polymarket CLOB orders (PREDICTION_BUY / PREDICTION_SELL) succeed
without producing a tx_hash, so the existing iteration_summary fields
(``txs_planned=0``, ``txs_sent=0``, ``tx_hashes=[]``, ``gas_used=0``,
``status=SUCCESS``) leave operators without an actionable identifier when
triaging from logs. The CLOB ``order_id`` and matcher ``clob_status`` are
already attached to ``ExecutionResult.extracted_data`` in
``_single_chain_execute_clob``; this module asserts they're surfaced on the
``iteration_summary`` log event for prediction intents only, additively, and
without changing any existing field.
"""

from dataclasses import dataclass
from unittest.mock import MagicMock

import structlog.testing

from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)


# Existing iteration_summary fields that MUST remain present on every
# emission. Keep this in lockstep with ``emit_iteration_summary``'s
# ``logger.info`` keyword arguments — the regression test below pins them.
_BASELINE_FIELDS = {
    "event_type",
    "event",
    "deployment_id",
    "chain",
    "iteration",
    "decision",
    "intents",
    "dry_run",
    "txs_planned",
    "txs_sent",
    "tx_hashes",
    "gas_used",
    "status",
    "duration_ms",
    "hold_reason",
    "hold_reason_code",
    "reconciliation_ok",
    "error",
    "log_level",
}


def _make_runner(**config_overrides) -> StrategyRunner:
    """Create a minimal StrategyRunner with mocked dependencies."""
    config = RunnerConfig(
        default_interval_seconds=0,
        enable_state_persistence=False,
        enable_alerting=False,
        **config_overrides,
    )
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=MagicMock(),
        config=config,
    )
    return runner


def _capture_summary(runner, result, chain=None):
    """Run _emit_iteration_summary and return the captured structlog event dict."""
    with structlog.testing.capture_logs() as cap:
        runner._emit_iteration_summary(result, chain=chain)

    summaries = [e for e in cap if e.get("event") == "iteration_summary"]
    assert len(summaries) == 1, f"Expected 1 iteration_summary record, got {len(summaries)}"
    return summaries[0]


def _make_clob_intent(intent_type_value: str):
    """Build a MagicMock intent whose intent_type.value matches ``intent_type_value``."""
    intent = MagicMock()
    intent.intent_type.value = intent_type_value
    intent.serialize.return_value = {"intent_type": intent_type_value}
    return intent


def _make_clob_execution_result(*, order_id: str | None, clob_status: str | None):
    """Build a mock ExecutionResult mirroring _single_chain_execute_clob output.

    Off-chain CLOB executions have ``transaction_results=[]``,
    ``tx_hashes=None``, no ``receipts`` worth counting, and surface the
    matcher fields on ``extracted_data``.
    """
    exec_result = MagicMock()
    exec_result.transaction_results = []
    exec_result.tx_hashes = None
    exec_result.receipts = []
    exec_result.total_gas_used = 0
    extracted: dict[str, str] = {}
    if clob_status is not None:
        extracted["clob_status"] = clob_status
    if order_id is not None:
        extracted["order_id"] = order_id
    exec_result.extracted_data = extracted
    return exec_result


# ---------------------------------------------------------------------------
# (a) PREDICTION_BUY surfaces order_id + clob_status
# ---------------------------------------------------------------------------


def test_prediction_buy_surfaces_order_id_and_clob_status():
    runner = _make_runner()
    runner._total_iterations = 7

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_make_clob_intent("PREDICTION_BUY"),
        execution_result=_make_clob_execution_result(
            order_id="0xabc123def456",
            clob_status="MATCHED",
        ),
        deployment_id="poly-buy-strat",
        duration_ms=512.0,
    )

    record = _capture_summary(runner, result, chain="polygon")

    # CLOB fields must be surfaced.
    assert record["order_id"] == "0xabc123def456"
    assert record["clob_status"] == "MATCHED"
    # Off-chain is accurate: no tx hashes, no gas, but status is still SUCCESS.
    assert record["status"] == "SUCCESS"
    assert record["tx_hashes"] == []
    assert record["txs_sent"] == 0
    assert record["txs_planned"] == 0
    assert record["gas_used"] == 0
    assert record["decision"] == "PREDICTION_BUY"


# ---------------------------------------------------------------------------
# (b) PREDICTION_SELL surfaces order_id + clob_status
# ---------------------------------------------------------------------------


def test_prediction_sell_surfaces_order_id_and_clob_status():
    runner = _make_runner()
    runner._total_iterations = 8

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_make_clob_intent("PREDICTION_SELL"),
        execution_result=_make_clob_execution_result(
            order_id="0xfeedbeef0001",
            clob_status="MATCHED",
        ),
        deployment_id="poly-sell-strat",
        duration_ms=341.0,
    )

    record = _capture_summary(runner, result, chain="polygon")

    assert record["order_id"] == "0xfeedbeef0001"
    assert record["clob_status"] == "MATCHED"
    assert record["decision"] == "PREDICTION_SELL"
    assert record["status"] == "SUCCESS"


# ---------------------------------------------------------------------------
# (c) PREDICTION_REDEEM is on-chain → tx_hashes, no order_id / clob_status
# ---------------------------------------------------------------------------


def test_prediction_redeem_keeps_tx_hashes_and_omits_clob_fields():
    runner = _make_runner()
    runner._total_iterations = 9

    @dataclass
    class FakeTxResult:
        tx_hash: str

    exec_result = MagicMock()
    exec_result.transaction_results = [FakeTxResult(tx_hash="0xdeadbeef")]
    exec_result.tx_hashes = None
    exec_result.receipts = [{"status": 1}]
    exec_result.total_gas_used = 12345
    # On-chain redeems would not populate CLOB fields, but even if a stale
    # entry slipped through extracted_data we should NOT surface it for
    # REDEEM — that intent type is on-chain and tx_hashes is the identifier.
    exec_result.extracted_data = {
        "order_id": "should-not-surface",
        "clob_status": "MATCHED",
    }

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_make_clob_intent("PREDICTION_REDEEM"),
        execution_result=exec_result,
        deployment_id="poly-redeem-strat",
        duration_ms=900.0,
    )

    record = _capture_summary(runner, result, chain="polygon")

    assert record["tx_hashes"] == ["0xdeadbeef"]
    assert record["txs_sent"] == 1
    assert record["txs_planned"] == 1
    assert record["gas_used"] == 12345
    assert record["decision"] == "PREDICTION_REDEEM"
    # CLOB-only fields must be absent for on-chain REDEEM.
    assert "order_id" not in record
    assert "clob_status" not in record


# ---------------------------------------------------------------------------
# (d) Non-prediction intent (SWAP) → no order_id / clob_status
# ---------------------------------------------------------------------------


def test_swap_intent_omits_clob_fields():
    runner = _make_runner()
    runner._total_iterations = 10

    @dataclass
    class FakeTxResult:
        tx_hash: str

    exec_result = MagicMock()
    exec_result.transaction_results = [FakeTxResult(tx_hash="0xswap1")]
    exec_result.tx_hashes = None
    exec_result.receipts = [{"status": 1}]
    exec_result.total_gas_used = 21000
    # Even if extracted_data happened to contain unrelated keys, we must
    # not surface CLOB fields for non-prediction intents.
    exec_result.extracted_data = {"some_swap_meta": "value"}

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_make_clob_intent("SWAP"),
        execution_result=exec_result,
        deployment_id="swap-strat",
        duration_ms=120.0,
    )

    record = _capture_summary(runner, result, chain="arbitrum")

    assert record["decision"] == "SWAP"
    assert "order_id" not in record
    assert "clob_status" not in record


# ---------------------------------------------------------------------------
# (e) PREDICTION_BUY where extraction failed → graceful degradation
# ---------------------------------------------------------------------------


def test_prediction_buy_without_extracted_data_is_graceful():
    runner = _make_runner()
    runner._total_iterations = 11

    # Mirrors the failure mode where the CLOB execution returns a result
    # but extracted_data is empty (e.g. order id never came back). We must
    # NOT emit empty/None CLOB fields — omit them entirely.
    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_make_clob_intent("PREDICTION_BUY"),
        execution_result=_make_clob_execution_result(order_id=None, clob_status=None),
        deployment_id="poly-buy-degraded",
        duration_ms=200.0,
    )

    record = _capture_summary(runner, result, chain="polygon")

    assert record["decision"] == "PREDICTION_BUY"
    assert "order_id" not in record
    assert "clob_status" not in record
    assert record["tx_hashes"] == []
    # VIB-3754: SUCCESS without tx_hash AND without CLOB order_id is exactly
    # the faux-SUCCESS scenario the trade-effective gate guards against. A
    # PREDICTION_BUY where the matcher never returned an identifier produced
    # no trade-effective output, so the iteration_summary log status must be
    # re-classified to EXECUTION_NOOP. (in-memory result.status remains
    # SUCCESS so circuit-breaker / metrics wiring is untouched.)
    assert record["status"] == "EXECUTION_NOOP"
    assert "noop_reason" in record


def test_prediction_buy_with_only_clob_status_omits_order_id():
    """If only one of the two CLOB fields is set, surface only that one."""
    runner = _make_runner()
    runner._total_iterations = 12

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_make_clob_intent("PREDICTION_BUY"),
        execution_result=_make_clob_execution_result(
            order_id=None,
            clob_status="FAILED",
        ),
        deployment_id="poly-buy-failed",
        duration_ms=180.0,
    )

    record = _capture_summary(runner, result, chain="polygon")

    assert record["clob_status"] == "FAILED"
    assert "order_id" not in record
    # VIB-3754: ``clob_status="FAILED"`` is not trade-effective (no order_id,
    # no tx_hash) — the matcher rejected the order. Re-classify to
    # EXECUTION_NOOP at the log layer.
    assert record["status"] == "EXECUTION_NOOP"


# ---------------------------------------------------------------------------
# (f) Regression: existing summary shape preserved
# ---------------------------------------------------------------------------


def test_iteration_summary_baseline_fields_preserved_for_prediction_buy():
    """Adding order_id/clob_status must not drop or mutate any existing field."""
    runner = _make_runner()
    runner._total_iterations = 13

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_make_clob_intent("PREDICTION_BUY"),
        execution_result=_make_clob_execution_result(
            order_id="0x1",
            clob_status="MATCHED",
        ),
        deployment_id="baseline-prediction",
        duration_ms=10.0,
    )

    record = _capture_summary(runner, result, chain="polygon")

    missing = _BASELINE_FIELDS - set(record.keys())
    assert not missing, f"Iteration summary lost baseline fields: {sorted(missing)}"
    # And the additive fields are present too.
    assert "order_id" in record
    assert "clob_status" in record


def test_iteration_summary_baseline_fields_preserved_for_swap():
    """Non-prediction intents keep the exact prior shape (no new keys leaking)."""
    runner = _make_runner()
    runner._total_iterations = 14

    @dataclass
    class FakeTxResult:
        tx_hash: str

    exec_result = MagicMock()
    exec_result.transaction_results = [FakeTxResult(tx_hash="0xs")]
    exec_result.tx_hashes = None
    exec_result.receipts = [{"status": 1}]
    exec_result.total_gas_used = 21000
    exec_result.extracted_data = {}

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_make_clob_intent("SWAP"),
        execution_result=exec_result,
        deployment_id="baseline-swap",
        duration_ms=10.0,
    )

    record = _capture_summary(runner, result)

    missing = _BASELINE_FIELDS - set(record.keys())
    assert not missing, f"Iteration summary lost baseline fields: {sorted(missing)}"
    assert "order_id" not in record
    assert "clob_status" not in record
