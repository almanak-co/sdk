"""Tests for the iteration_summary trade-effective gate (VIB-3754).

The runner's success path returns ``IterationStatus.SUCCESS`` whenever the
post-execution wiring (slippage, reconciliation, ledger write) finishes
without raising. Several real failure modes reach that path with no
on-chain transaction hash and no CLOB order id — in dashboards those rows
look identical to a healthy SUCCESS, so operators silently accept "deployed
strategy with 0 events" as real activity.

The gate re-classifies the iteration_summary log status to
``EXECUTION_NOOP`` when:

  - status is SUCCESS, AND
  - ``runner.config.dry_run`` is False, AND
  - the intent is present and not HOLD, AND
  - no tx_hash was sent, AND
  - no CLOB order_id was captured.

The in-memory ``IterationResult.status`` is never mutated — the gate is a
log-layer re-classification only, so circuit-breaker / metrics /
state-persistence wiring keeps treating the iteration as SUCCESS.
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock

import structlog.testing

from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)


def _make_runner(**config_overrides) -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=0,
        enable_state_persistence=False,
        enable_alerting=False,
        **config_overrides,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=MagicMock(),
        config=config,
    )


def _capture_summary(runner, result, chain=None) -> dict:
    with structlog.testing.capture_logs() as cap:
        runner._emit_iteration_summary(result, chain=chain)
    summaries = [e for e in cap if e.get("event") == "iteration_summary"]
    assert len(summaries) == 1, f"Expected 1 iteration_summary record, got {len(summaries)}"
    return summaries[0]


def _make_intent(intent_type_value: str):
    intent = MagicMock()
    intent.intent_type.value = intent_type_value
    intent.serialize.return_value = {"intent_type": intent_type_value}
    return intent


def _exec_result_no_tx() -> MagicMock:
    """Mock ExecutionResult with no transactions and no extracted_data."""
    er = MagicMock()
    er.transaction_results = []
    er.tx_hashes = None
    er.receipts = []
    er.total_gas_used = 0
    er.extracted_data = {}
    return er


# ─── Gate fires (faux SUCCESS detected) ──────────────────────────────────────


def test_success_with_no_tx_and_non_hold_intent_is_reclassified_noop():
    """Real bug: PERP_OPEN reports SUCCESS but produced zero transactions."""
    runner = _make_runner()
    runner._total_iterations = 42

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_make_intent("PERP_OPEN"),
        execution_result=_exec_result_no_tx(),
        strategy_id="bb-perps",
        duration_ms=812.0,
    )

    record = _capture_summary(runner, result, chain="arbitrum")

    # Log-only invariant: the in-memory IterationResult.status MUST stay SUCCESS.
    # Circuit-breaker, metrics, and state-persistence wiring all read .status
    # downstream; mutating it would silently break those code paths.
    assert result.status is IterationStatus.SUCCESS, (
        "Trade-effective gate must reclassify only the log record, never the "
        "in-memory IterationResult.status (would break circuit-breaker / metrics)"
    )

    assert record["status"] == "EXECUTION_NOOP", (
        "SUCCESS with no tx_hash and no CLOB order_id must be re-classified"
    )
    assert record["decision"] == "PERP_OPEN"
    assert record["txs_sent"] == 0
    assert record["tx_hashes"] == []
    assert "noop_reason" in record
    assert "no on-chain tx_hash" in record["noop_reason"].lower()


def test_success_with_supply_intent_no_tx_is_reclassified_noop():
    """SUPPLY (lending) faux-SUCCESS — covers ethena_morpho_double_yield class."""
    runner = _make_runner()
    runner._total_iterations = 5

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_make_intent("SUPPLY"),
        execution_result=_exec_result_no_tx(),
        strategy_id="ethena-double-yield",
        duration_ms=410.0,
    )

    record = _capture_summary(runner, result, chain="ethereum")

    assert result.status is IterationStatus.SUCCESS  # log-only invariant
    assert record["status"] == "EXECUTION_NOOP"
    assert record["decision"] == "SUPPLY"


def test_success_with_no_execution_result_and_real_intent_is_reclassified_noop():
    """SUCCESS with intent but no ExecutionResult — most pathological case."""
    runner = _make_runner()
    runner._total_iterations = 3

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_make_intent("LP_OPEN"),
        execution_result=None,  # <-- nothing executed at all
        strategy_id="lp-no-exec",
        duration_ms=50.0,
    )

    record = _capture_summary(runner, result, chain="base")

    assert result.status is IterationStatus.SUCCESS  # log-only invariant
    assert record["status"] == "EXECUTION_NOOP"
    assert record["txs_sent"] == 0


# ─── Gate skipped (legitimate cases) ─────────────────────────────────────────


def test_success_with_tx_hash_keeps_success_status():
    """Real on-chain SWAP — gate must NOT fire."""
    runner = _make_runner()
    runner._total_iterations = 1

    @dataclass
    class FakeTx:
        tx_hash: str

    er = MagicMock()
    er.transaction_results = [FakeTx(tx_hash="0xabc")]
    er.tx_hashes = None
    er.receipts = [{"status": 1}]
    er.total_gas_used = 21000
    er.extracted_data = {}

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_make_intent("SWAP"),
        execution_result=er,
        strategy_id="real-swap",
        duration_ms=120.0,
    )

    record = _capture_summary(runner, result, chain="arbitrum")

    assert record["status"] == "SUCCESS"
    assert record["txs_sent"] == 1
    assert "noop_reason" not in record


def test_success_with_clob_order_id_keeps_success_status():
    """Off-chain PREDICTION_BUY with matched CLOB order — gate must NOT fire."""
    runner = _make_runner()
    runner._total_iterations = 1

    er = MagicMock()
    er.transaction_results = []
    er.tx_hashes = None
    er.receipts = []
    er.total_gas_used = 0
    er.extracted_data = {"order_id": "0xfeedbeef", "clob_status": "MATCHED"}

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_make_intent("PREDICTION_BUY"),
        execution_result=er,
        strategy_id="poly-buy",
        duration_ms=120.0,
    )

    record = _capture_summary(runner, result, chain="polygon")

    assert record["status"] == "SUCCESS"
    assert record["order_id"] == "0xfeedbeef"
    assert "noop_reason" not in record


def test_hold_intent_is_not_reclassified():
    """HOLD is legitimately a no-op — gate must NOT fire."""
    from almanak.framework.intents.vocabulary import HoldIntent

    runner = _make_runner()
    runner._total_iterations = 1

    result = IterationResult(
        status=IterationStatus.HOLD,
        intent=HoldIntent(reason="Waiting for entry"),
        strategy_id="rsi-strat",
        duration_ms=42.5,
    )

    record = _capture_summary(runner, result, chain="arbitrum")

    assert record["status"] == "HOLD"
    assert "noop_reason" not in record


def test_dry_run_success_is_not_reclassified():
    """DRY_RUN is intentional — gate is skipped via runner.config.dry_run."""
    runner = _make_runner(dry_run=True)
    runner._total_iterations = 1

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_make_intent("SWAP"),
        execution_result=_exec_result_no_tx(),
        strategy_id="dry-strat",
        duration_ms=10.0,
    )

    record = _capture_summary(runner, result)

    assert record["status"] == "SUCCESS"
    assert record["dry_run"] is True
    assert "noop_reason" not in record


def test_failure_status_is_not_reclassified():
    """Non-SUCCESS statuses are already correctly classified — gate skipped."""
    runner = _make_runner()
    runner._total_iterations = 1

    result = IterationResult(
        status=IterationStatus.EXECUTION_FAILED,
        intent=_make_intent("LP_OPEN"),
        error="reverted",
        strategy_id="failed-strat",
        duration_ms=200.0,
    )

    record = _capture_summary(runner, result, chain="base")

    assert record["status"] == "EXECUTION_FAILED"
    assert "noop_reason" not in record


def test_success_no_intent_is_not_reclassified():
    """Some runner code paths return SUCCESS with intent=None (e.g. callback
    flows that didn't compile an intent). The gate skips those — there is no
    intent type to classify, so EXECUTION_NOOP would be ambiguous.
    """
    runner = _make_runner()
    runner._total_iterations = 1

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=None,
        execution_result=None,
        strategy_id="callback-strat",
        duration_ms=10.0,
    )

    record = _capture_summary(runner, result)

    assert record["status"] == "SUCCESS"
    assert "noop_reason" not in record


def test_teardown_status_is_not_reclassified():
    """TEARDOWN is its own success-class status; never re-classified."""
    runner = _make_runner()
    runner._total_iterations = 1

    result = IterationResult(
        status=IterationStatus.TEARDOWN,
        intent=_make_intent("SWAP"),
        execution_result=_exec_result_no_tx(),
        strategy_id="teardown-strat",
        duration_ms=10.0,
    )

    record = _capture_summary(runner, result)

    assert record["status"] == "TEARDOWN"
    assert "noop_reason" not in record
