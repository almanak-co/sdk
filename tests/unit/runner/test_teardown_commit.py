"""Tests for ``runner.teardown_commit.commit_teardown_intent`` (VIB-3773 Phase 0).

Covers T1–T5 from ``docs/internal/AccountingTeardown.md`` §6:

* T1 — Full pipeline runs for an LP_CLOSE intent (enrich called; ledger,
       outbox+fire, sidecar all called).
* T2 — Same for a SWAP intent.
* T3 — Live ledger write raising ``AccountingPersistenceError`` does NOT
       propagate; outcome reports degraded; deferred-log row appended.
* T4 — Outbox raising in live mode does NOT propagate; outcome captures
       both succeeded ledger_entry_id and degraded outbox.
* T5 — Same as T3/T4 but in paper/dry-run mode (degraded contract is
       lane-shaped, not mode-shaped — teardown lane never raises).

Plus a few smaller invariants:

* Cycle-id contextvar is set to ``teardown_cycle_id`` for the duration of
  the helper and restored afterwards (P1-4 — runner_state reads
  ``runner._last_cycle_id`` first, but the contextvar restoration matters
  for the outbox writer which reads via :func:`get_cycle_id`).
* Sidecar failure is captured in the outcome, never propagated.
* Enrichment failure does not block the ledger / outbox / sidecar steps.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.accounting.deferred_log import DEFERRED_LOG_FILENAME
from almanak.framework.observability.context import (
    clear_cycle_id,
    get_cycle_id,
    set_cycle_id,
)
from almanak.framework.runner.teardown_commit import (
    TeardownCommitOutcome,
    commit_teardown_intent,
)
from almanak.framework.state.exceptions import (
    AccountingPersistenceError,
    AccountingWriteKind,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def local_db_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Pin the deferred-log to ``tmp_path`` for assertions."""
    monkeypatch.delenv("AGENT_ID", raising=False)
    monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "state.db"))
    return tmp_path


@pytest.fixture
def fake_strategy() -> SimpleNamespace:
    return SimpleNamespace(
        strategy_id="strat-1",
        deployment_id="dep-1",
        chain="arbitrum",
        wallet_address="0xWALLET",
    )


def _make_intent(intent_type_value: str) -> SimpleNamespace:
    """Build a duck-typed intent with the minimum surface the helper reads."""
    intent_type = SimpleNamespace(value=intent_type_value)
    return SimpleNamespace(
        intent_type=intent_type,
        protocol="uniswap_v3",
        chain="arbitrum",
    )


def _make_execution_result(tx_hash: str = "0xabc", success: bool = True) -> SimpleNamespace:
    """Minimal ExecutionResult-ish object: success + transaction_results[0].tx_hash."""
    return SimpleNamespace(
        success=success,
        transaction_results=[SimpleNamespace(tx_hash=tx_hash)],
        total_gas_used=120_000,
        gas_cost_usd="0.50",
        extracted_data={},
        error="",
    )


def _make_runner(*, live_mode: bool = True) -> MagicMock:
    """Construct a fake runner satisfying the helper's protocol surface."""
    runner = MagicMock(name="StrategyRunner")
    runner._is_live_mode.return_value = live_mode
    runner._write_ledger_entry = AsyncMock(return_value="ledger-1")
    runner._write_outbox_and_fire_processor = AsyncMock(return_value=None)
    runner.config = SimpleNamespace(chain="arbitrum")
    return runner


@pytest.fixture
def patch_enricher_and_sidecar(monkeypatch: pytest.MonkeyPatch):
    """Replace ResultEnricher + AccountingSidecarWriter with spies that never
    actually parse / write disk. Returns ``(enricher_spy, sidecar_spy)``.
    """
    enricher_calls: list[dict] = []

    class _SpyEnricher:
        def __init__(self, live_mode: bool = True) -> None:
            self.live_mode = live_mode

        def enrich(self, result, intent, context, *, bundle_metadata=None):
            enricher_calls.append(
                {
                    "intent_type": getattr(intent.intent_type, "value", str(intent.intent_type)),
                    "live_mode": self.live_mode,
                    "bundle_metadata": bundle_metadata,
                }
            )
            # Tag the result so callers can verify the enriched object flows
            # forward into ledger / sidecar.
            result.enriched = True
            return result

    sidecar_calls: list[dict] = []

    class _SpySidecar:
        def append(self, *, strategy_id, intent, result, chain, price_oracle=None):
            sidecar_calls.append(
                {
                    "strategy_id": strategy_id,
                    "intent_type": getattr(
                        intent.intent_type, "value", str(intent.intent_type)
                    ),
                    "chain": chain,
                    "result_enriched": getattr(result, "enriched", False),
                    "price_oracle_passed": price_oracle is not None,
                }
            )

    monkeypatch.setattr(
        "almanak.framework.execution.result_enricher.ResultEnricher", _SpyEnricher
    )
    monkeypatch.setattr(
        "almanak.framework.accounting.sidecar.AccountingSidecarWriter", _SpySidecar
    )

    return enricher_calls, sidecar_calls


# ---------------------------------------------------------------------------
# T1 — full pipeline runs for an LP_CLOSE intent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t1_lp_close_full_pipeline(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    enricher_calls, sidecar_calls = patch_enricher_and_sidecar
    runner = _make_runner(live_mode=True)
    intent = _make_intent("LP_CLOSE")
    result = _make_execution_result(tx_hash="0xc1ose")
    context = SimpleNamespace(protocol="uniswap_v3", chain="arbitrum")

    outcome = await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        bundle_metadata={"expected_output_human": "1.0"},
        teardown_cycle_id="teardown-uuid-1",
    )

    assert isinstance(outcome, TeardownCommitOutcome)
    assert outcome.ledger_entry_id == "ledger-1"
    assert outcome.accounting_degraded is False
    assert outcome.degraded_reason is None
    assert outcome.degraded_writes == ()

    # All four steps fired.
    assert len(enricher_calls) == 1
    assert enricher_calls[0]["intent_type"] == "LP_CLOSE"
    assert enricher_calls[0]["bundle_metadata"] == {"expected_output_human": "1.0"}
    runner._write_ledger_entry.assert_awaited_once()
    runner._write_outbox_and_fire_processor.assert_awaited_once_with(
        fake_strategy, intent, "ledger-1"
    )
    assert len(sidecar_calls) == 1
    assert sidecar_calls[0]["intent_type"] == "LP_CLOSE"
    assert sidecar_calls[0]["chain"] == "arbitrum"
    # Enriched object flows forward.
    assert sidecar_calls[0]["result_enriched"] is True

    # No deferred-log rows on the happy path.
    log = local_db_dir / DEFERRED_LOG_FILENAME
    assert not log.exists()


# ---------------------------------------------------------------------------
# T2 — same for SWAP intent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t2_swap_full_pipeline(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    enricher_calls, sidecar_calls = patch_enricher_and_sidecar
    runner = _make_runner(live_mode=True)
    intent = _make_intent("SWAP")
    result = _make_execution_result(tx_hash="0xswap")
    context = SimpleNamespace(protocol="uniswap_v3", chain="arbitrum")

    outcome = await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        teardown_cycle_id="teardown-uuid-2",
    )

    assert outcome.accounting_degraded is False
    assert outcome.ledger_entry_id == "ledger-1"
    runner._write_ledger_entry.assert_awaited_once()
    runner._write_outbox_and_fire_processor.assert_awaited_once()
    assert sidecar_calls[0]["intent_type"] == "SWAP"
    assert enricher_calls[0]["intent_type"] == "SWAP"


# ---------------------------------------------------------------------------
# T3 — ledger write fails in live mode → degraded, no raise
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t3_live_ledger_failure_degrades_does_not_raise(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    runner = _make_runner(live_mode=True)
    runner._write_ledger_entry.side_effect = AccountingPersistenceError(
        write_kind=AccountingWriteKind.LEDGER,
        strategy_id="strat-1",
        message="forced ledger fail",
    )
    intent = _make_intent("LP_CLOSE")
    result = _make_execution_result(tx_hash="0xfail-ledger")
    context = SimpleNamespace(protocol="uniswap_v3", chain="arbitrum")

    outcome = await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        teardown_cycle_id="teardown-fail-1",
    )

    # The defining contract: never raises on writer failure.
    assert outcome.accounting_degraded is True
    assert outcome.ledger_entry_id is None
    assert "ledger" in (outcome.degraded_reason or "")
    assert len(outcome.degraded_writes) == 1
    assert outcome.degraded_writes[0].kind == "ledger"
    assert outcome.degraded_writes[0].cycle_id == "teardown-fail-1"
    assert outcome.degraded_writes[0].tx_hash == "0xfail-ledger"
    assert outcome.degraded_writes[0].intent_type == "LP_CLOSE"

    # Outbox + processor are gated on a successful ledger write — must be
    # skipped when ledger failed.
    runner._write_outbox_and_fire_processor.assert_not_awaited()

    # Deferred-log file was written.
    log = local_db_dir / DEFERRED_LOG_FILENAME
    assert log.exists()
    rows = [json.loads(line) for line in log.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["kind"] == "ledger"
    assert rows[0]["tx_hash"] == "0xfail-ledger"


# ---------------------------------------------------------------------------
# T4 — outbox fails in live → ledger_id preserved, no raise
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t4_live_outbox_failure_degrades_does_not_raise(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    runner = _make_runner(live_mode=True)
    runner._write_outbox_and_fire_processor.side_effect = AccountingPersistenceError(
        write_kind=AccountingWriteKind.ACCOUNTING,
        strategy_id="strat-1",
        message="forced outbox fail",
    )
    intent = _make_intent("LP_CLOSE")
    result = _make_execution_result(tx_hash="0xfail-outbox")
    context = SimpleNamespace(protocol="uniswap_v3", chain="arbitrum")

    outcome = await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        teardown_cycle_id="teardown-fail-2",
    )

    assert outcome.accounting_degraded is True
    # Ledger succeeded — outcome carries the id so the caller knows the
    # ledger row landed even if the outbox didn't.
    assert outcome.ledger_entry_id == "ledger-1"
    runner._write_ledger_entry.assert_awaited_once()
    runner._write_outbox_and_fire_processor.assert_awaited_once()

    assert "outbox" in (outcome.degraded_reason or "")
    assert len(outcome.degraded_writes) == 1
    assert outcome.degraded_writes[0].kind == "outbox"
    assert outcome.degraded_writes[0].ledger_entry_id == "ledger-1"

    log = local_db_dir / DEFERRED_LOG_FILENAME
    rows = [json.loads(line) for line in log.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["kind"] == "outbox"
    assert rows[0]["ledger_entry_id"] == "ledger-1"


# ---------------------------------------------------------------------------
# T5 — paper / dry-run: same degraded-but-continue contract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t5_paper_mode_failure_degrades_does_not_raise(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    runner = _make_runner(live_mode=False)
    # Paper-mode writer raises a generic exception (live-fail-closed branch
    # not reached) — helper still must not propagate.
    runner._write_ledger_entry.side_effect = RuntimeError("paper failure")
    intent = _make_intent("SWAP")
    result = _make_execution_result(tx_hash="0xpaper")
    context = SimpleNamespace(protocol="uniswap_v3", chain="arbitrum")

    outcome = await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        teardown_cycle_id="teardown-paper-1",
    )

    assert outcome.accounting_degraded is True
    assert outcome.ledger_entry_id is None
    assert "ledger" in (outcome.degraded_reason or "")
    runner._write_outbox_and_fire_processor.assert_not_awaited()

    log = local_db_dir / DEFERRED_LOG_FILENAME
    rows = [json.loads(line) for line in log.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["kind"] == "ledger"


# ---------------------------------------------------------------------------
# Cycle-id contextvar swap + restore
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cycle_id_set_during_helper_and_restored(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    runner = _make_runner(live_mode=True)

    # Capture cycle_id seen by ledger/outbox writers.
    seen_cycle_ids: list[str | None] = []

    async def _capture_ledger(strategy, intent, *, result, success, error="", price_oracle=None):
        seen_cycle_ids.append(get_cycle_id())
        return "ledger-1"

    async def _capture_outbox(strategy, intent, ledger_entry_id):
        seen_cycle_ids.append(get_cycle_id())

    runner._write_ledger_entry = AsyncMock(side_effect=_capture_ledger)
    runner._write_outbox_and_fire_processor = AsyncMock(side_effect=_capture_outbox)

    # Pre-set an outer cycle id so we can verify restoration.
    set_cycle_id("outer-cycle")
    try:
        outcome = await commit_teardown_intent(
            runner,
            fake_strategy,
            _make_intent("SWAP"),
            execution_result=_make_execution_result(),
            execution_context=SimpleNamespace(protocol="uniswap_v3", chain="arbitrum"),
            teardown_cycle_id="teardown-cyc-1",
        )
        # During the helper, writers saw the teardown cycle_id.
        assert seen_cycle_ids == ["teardown-cyc-1", "teardown-cyc-1"]
        # After the helper, restored to the outer value.
        assert get_cycle_id() == "outer-cycle"
        assert outcome.accounting_degraded is False
    finally:
        clear_cycle_id()


@pytest.mark.asyncio
async def test_cycle_id_restored_to_none_when_no_outer(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    """When no outer cycle id is set, the helper must restore to None,
    not leave its teardown value lingering on the contextvar.
    """
    runner = _make_runner(live_mode=True)
    clear_cycle_id()
    assert get_cycle_id() is None

    await commit_teardown_intent(
        runner,
        fake_strategy,
        _make_intent("SWAP"),
        execution_result=_make_execution_result(),
        execution_context=SimpleNamespace(protocol="uniswap_v3", chain="arbitrum"),
        teardown_cycle_id="teardown-cyc-2",
    )
    assert get_cycle_id() is None


# ---------------------------------------------------------------------------
# Enrichment failure must not block downstream steps
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enrich_failure_does_not_block_ledger_outbox_sidecar(
    fake_strategy, monkeypatch: pytest.MonkeyPatch, local_db_dir: Path
):
    runner = _make_runner(live_mode=True)

    class _BoomEnricher:
        def __init__(self, live_mode: bool = True) -> None: ...
        def enrich(self, *args, **kwargs):
            raise RuntimeError("parser explosion")

    sidecar_seen: list[bool] = []

    class _SpySidecar:
        def append(self, *, strategy_id, intent, result, chain, price_oracle=None):
            sidecar_seen.append(True)

    monkeypatch.setattr(
        "almanak.framework.execution.result_enricher.ResultEnricher", _BoomEnricher
    )
    monkeypatch.setattr(
        "almanak.framework.accounting.sidecar.AccountingSidecarWriter", _SpySidecar
    )

    outcome = await commit_teardown_intent(
        runner,
        fake_strategy,
        _make_intent("SWAP"),
        execution_result=_make_execution_result(),
        execution_context=SimpleNamespace(protocol="uniswap_v3", chain="arbitrum"),
        teardown_cycle_id="teardown-enrich-fail",
    )

    # Ledger + outbox + sidecar all still fired.
    runner._write_ledger_entry.assert_awaited_once()
    runner._write_outbox_and_fire_processor.assert_awaited_once()
    assert sidecar_seen == [True]
    # But the outcome is degraded, with an "enrich" deferred row.
    assert outcome.accounting_degraded is True
    assert any(w.kind == "enrich" for w in outcome.degraded_writes)


# ---------------------------------------------------------------------------
# Sidecar failure: outcome captures it, never propagates
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sidecar_failure_captured_not_raised(
    fake_strategy, monkeypatch: pytest.MonkeyPatch, local_db_dir: Path
):
    runner = _make_runner(live_mode=True)

    class _SpyEnricher:
        def __init__(self, live_mode: bool = True) -> None: ...
        def enrich(self, result, *args, **kwargs):
            return result

    class _BoomSidecar:
        def append(self, **kwargs):
            raise OSError("disk full")

    monkeypatch.setattr(
        "almanak.framework.execution.result_enricher.ResultEnricher", _SpyEnricher
    )
    monkeypatch.setattr(
        "almanak.framework.accounting.sidecar.AccountingSidecarWriter", _BoomSidecar
    )

    outcome = await commit_teardown_intent(
        runner,
        fake_strategy,
        _make_intent("SWAP"),
        execution_result=_make_execution_result(),
        execution_context=SimpleNamespace(protocol="uniswap_v3", chain="arbitrum"),
        teardown_cycle_id="teardown-sidecar-fail",
    )

    assert outcome.accounting_degraded is True
    assert outcome.ledger_entry_id == "ledger-1"  # ledger + outbox succeeded
    assert any(w.kind == "sidecar" for w in outcome.degraded_writes)
