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


def _make_execution_result(
    tx_hash: str = "0xabc", success: bool = True, block_number: int | None = None
) -> SimpleNamespace:
    """Minimal ExecutionResult-ish object: success + transaction_results[0].tx_hash.

    When ``block_number`` is provided, also stamps a ``receipt`` on the
    transaction so :func:`strategy_runner._last_receipt_block` can extract
    it for VIB-4589 post-state-read pinning.
    """
    receipt = SimpleNamespace(block_number=block_number) if block_number is not None else None
    return SimpleNamespace(
        success=success,
        transaction_results=[SimpleNamespace(tx_hash=tx_hash, success=success, receipt=receipt)],
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
        def __init__(
            self,
            live_mode: bool = True,
            *,
            # VIB-4477 (T08): teardown_commit now threads a sync
            # pool_key_lookup bridge so V4 LP_CLOSE receipts can resolve
            # PoolKey via gateway. The spy records it but does not exercise
            # it (the spy never invokes a parser).
            pool_key_lookup=None,
        ) -> None:
            self.live_mode = live_mode
            self.pool_key_lookup = pool_key_lookup

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

    async def _capture_ledger(strategy, intent, *, result, success, error="", price_oracle=None, **_kwargs):
        # Absorb extra kwargs (pre_state / post_state added by VIB-3918) so
        # the test signature stays aligned with the real writer.
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
        def __init__(self, live_mode: bool = True, *, pool_key_lookup=None) -> None: ...
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
        def __init__(self, live_mode: bool = True, *, pool_key_lookup=None) -> None: ...
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


# ---------------------------------------------------------------------------
# VIB-3934 — lending pre/post state threaded through the teardown commit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_vib3934_lending_pre_state_threaded_into_pre_state_json(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    """The teardown commit pipeline must serialize ``lending_pre_state``
    (captured by the teardown manager BEFORE submission) into
    ``transaction_ledger.pre_state_json``. Without this REPAY/WITHDRAW rows
    can't carry collateral/debt/HF on the pre-side, lane-asymmetric with
    iteration.
    """
    captured_pre_state: list[dict | None] = []
    captured_post_state: list[dict | None] = []

    runner = _make_runner(live_mode=True)
    runner._get_gateway_client = MagicMock(return_value=None)
    runner._capture_lending_state_safe = MagicMock(return_value=None)

    async def _capture_ledger(strategy, intent, *, result, success, error="", price_oracle=None, **kwargs):
        captured_pre_state.append(kwargs.get("pre_state"))
        captured_post_state.append(kwargs.get("post_state"))
        return "ledger-1"

    runner._write_ledger_entry = AsyncMock(side_effect=_capture_ledger)

    # A duck-typed AaveAccountState — just needs the fields lending_state_to_dict reads.
    from decimal import Decimal

    lending_pre_state = SimpleNamespace(
        collateral_usd=Decimal("100.50"),
        debt_usd=Decimal("40.25"),
        health_factor=Decimal("2.50"),
        liquidation_threshold_bps=8500,
    )

    intent = _make_intent("REPAY")
    intent.protocol = "aave_v3"
    result = _make_execution_result(tx_hash="0xrepay-pre")
    context = SimpleNamespace(protocol="aave_v3", chain="arbitrum")

    outcome = await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        teardown_cycle_id="teardown-vib3934-pre",
        lending_pre_state=lending_pre_state,
    )

    assert outcome.accounting_degraded is False
    assert len(captured_pre_state) == 1
    pre = captured_pre_state[0]
    # Lending fields merged into the pre_state dict.
    assert pre is not None
    assert pre.get("collateral_usd") is not None
    assert Decimal(str(pre["collateral_usd"])) == Decimal("100.50")
    assert Decimal(str(pre["debt_usd"])) == Decimal("40.25")
    assert Decimal(str(pre["health_factor"])) == Decimal("2.50")


@pytest.mark.asyncio
async def test_vib3934_lending_post_state_captured_inside_commit(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    """The teardown commit pipeline must call
    ``runner._capture_lending_state_safe(phase="post", ...)`` and serialize
    the result into ``transaction_ledger.post_state_json`` so lending
    accounting events get HIGH confidence on the teardown lane.
    """
    captured_post_state: list[dict | None] = []
    capture_calls: list[dict] = []

    runner = _make_runner(live_mode=True)
    runner._get_gateway_client = MagicMock(return_value="fake-gateway")

    from decimal import Decimal

    fake_post_state = SimpleNamespace(
        collateral_usd=Decimal("80.00"),
        debt_usd=Decimal("0.00"),
        health_factor=Decimal("999999"),
        liquidation_threshold_bps=8500,
    )

    def _capture_lending(*, intent, chain, wallet_address, gateway_client, price_oracle, phase, block=None):
        # VIB-4589 / F7 — ``block`` is the new receipt-anchored read param.
        # Capture it alongside the phase so the regression test below pins
        # that the teardown commit pipeline passes ``receipt.block_number``
        # (extracted from ``execution_result``) for the post-state read.
        capture_calls.append(
            {
                "phase": phase,
                "chain": chain,
                "wallet_address": wallet_address,
                "gateway_client": gateway_client,
                "block": block,
            }
        )
        return fake_post_state if phase == "post" else None

    runner._capture_lending_state_safe = MagicMock(side_effect=_capture_lending)
    runner._teardown_price_oracle = {"USDT": Decimal("1.00"), "WETH": Decimal("2000.0")}

    async def _capture_ledger(strategy, intent, *, result, success, error="", price_oracle=None, **kwargs):
        captured_post_state.append(kwargs.get("post_state"))
        return "ledger-1"

    runner._write_ledger_entry = AsyncMock(side_effect=_capture_ledger)

    intent = _make_intent("WITHDRAW")
    intent.protocol = "aave_v3"
    # VIB-4589 / F7 — stamp a specific receipt block so we can assert below
    # that the teardown commit pipeline forwards EXACTLY that block to the
    # post-state read. Anything else (None, "latest", a different int) would
    # reintroduce the indexer-race this test is the regression guard for.
    receipt_block = 19_876_543
    result = _make_execution_result(tx_hash="0xwithdraw-post", block_number=receipt_block)
    context = SimpleNamespace(protocol="aave_v3", chain="arbitrum")

    outcome = await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        teardown_cycle_id="teardown-vib3934-post",
    )

    assert outcome.accounting_degraded is False
    # Only the POST-state read happens inside commit_teardown_intent —
    # PRE-state is captured by the teardown manager before submission.
    post_phases = [c["phase"] for c in capture_calls]
    assert "post" in post_phases
    assert "pre" not in post_phases
    # The captured state was serialized into the ledger row's post_state.
    assert len(captured_post_state) == 1
    post = captured_post_state[0]
    assert post is not None
    assert Decimal(str(post["collateral_usd"])) == Decimal("80.00")
    assert Decimal(str(post["debt_usd"])) == Decimal("0.00")
    # Reads happened with the teardown's stash oracle (Accounting-AttemptNo17 §A4).
    assert capture_calls[0]["gateway_client"] == "fake-gateway"
    # VIB-4589 / F7 regression guard — the forwarded ``block`` MUST equal
    # the receipt's block_number. A None / "latest" / mismatched int here
    # would re-open the stale-collateral race that this PR closed.
    assert capture_calls[0]["block"] == receipt_block


@pytest.mark.asyncio
async def test_vib3934_lending_post_capture_failure_never_propagates(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    """A gateway hiccup during lending post-state capture must not crash
    the teardown commit pipeline — degraded-but-continue semantics
    (VIB-3773) trump ESTIMATED-vs-HIGH delta. The ledger row still lands.
    """
    runner = _make_runner(live_mode=True)
    runner._get_gateway_client = MagicMock(return_value="fake-gateway")

    def _explode(**_kwargs):
        raise ConnectionError("gateway dropped")

    runner._capture_lending_state_safe = MagicMock(side_effect=_explode)

    intent = _make_intent("REPAY")
    intent.protocol = "aave_v3"
    result = _make_execution_result(tx_hash="0xrepay-explode")
    context = SimpleNamespace(protocol="aave_v3", chain="arbitrum")

    outcome = await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        teardown_cycle_id="teardown-vib3934-explode",
    )

    # Helper swallowed the error — pipeline still ran end-to-end.
    assert outcome.ledger_entry_id == "ledger-1"
    runner._write_ledger_entry.assert_awaited_once()
    runner._write_outbox_and_fire_processor.assert_awaited_once()


# ---------------------------------------------------------------------------
# VIB-4318 — intent-token prices merged into the teardown ledger oracle
# ---------------------------------------------------------------------------


def _make_swap_intent(*, from_token: str, to_token: str) -> SimpleNamespace:
    """SwapIntent shape consumed by ``_extract_tokens_from_intent``."""
    return SimpleNamespace(
        intent_type=SimpleNamespace(value="SWAP"),
        protocol="uniswap_v3",
        chain="arbitrum",
        from_token=from_token,
        to_token=to_token,
    )


def _make_price_oracle(
    quotes: dict[str, str | None],
    *,
    source: str = "gateway",
    confidence: str = "HIGH",
) -> MagicMock:
    """Build a fake :class:`PriceOracle` whose ``get_aggregated_price`` returns
    a structured result for each symbol in ``quotes``. ``None`` value ⇒ the
    oracle returns a result with ``price=None`` (price unknown — should NOT
    be inserted per Empty ≠ Zero).
    """
    calls: list[dict] = []

    async def _get_aggregated_price(symbol: str, quote: str, *, chain: str):
        calls.append({"symbol": symbol, "quote": quote, "chain": chain})
        if symbol not in quotes:
            raise KeyError(f"no quote for {symbol}")
        return SimpleNamespace(
            price=quotes[symbol],
            source=source,
            confidence=confidence,
            timestamp=None,
        )

    oracle = MagicMock(name="PriceOracle")
    oracle.get_aggregated_price = AsyncMock(side_effect=_get_aggregated_price)
    oracle._calls = calls  # expose for assertions
    return oracle


@pytest.mark.asyncio
async def test_vib4318_intent_token_merged_into_teardown_ledger_oracle(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    """Smoking-gun row from the VIB-4316 matrix: teardown WETH→USDC swap on
    ``loop_lp_diff`` left ``transaction_ledger.price_inputs_json`` without
    WETH because the pre-teardown stash only contained held assets
    (USDC + USDT). The merge helper fetches WETH at close-time and merges
    it into the stash before the ledger write, so the ledger row carries
    every price the SWAP handler needs.
    """
    runner = _make_runner(live_mode=True)
    # Pre-teardown stash carries USDC + USDT (held assets) but NOT WETH.
    runner._teardown_price_oracle = {
        "USDC": {
            "price_usd": "1.0",
            "oracle_source": "portfolio_valuer",
            "fetched_at": "2026-05-12T00:00:00+00:00",
            "confidence": "HIGH",
        },
        "USDT": {
            "price_usd": "1.0",
            "oracle_source": "portfolio_valuer",
            "fetched_at": "2026-05-12T00:00:00+00:00",
            "confidence": "HIGH",
        },
    }
    runner.price_oracle = _make_price_oracle({"WETH": "2295.62"})

    intent = _make_swap_intent(from_token="WETH", to_token="USDC")
    result = _make_execution_result(tx_hash="0xweth_swap")
    context = SimpleNamespace(protocol="uniswap_v3", chain="arbitrum")

    outcome = await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        teardown_cycle_id="teardown-vib4318",
    )

    assert outcome.accounting_degraded is False
    assert outcome.ledger_entry_id == "ledger-1"
    runner._write_ledger_entry.assert_awaited_once()

    # WETH is now present in the stash AND in the price_oracle passed to
    # _write_ledger_entry, alongside the pre-existing USDC / USDT.
    assert "WETH" in runner._teardown_price_oracle
    assert runner._teardown_price_oracle["WETH"]["price_usd"] == "2295.62"
    assert "USDC" in runner._teardown_price_oracle
    assert "USDT" in runner._teardown_price_oracle
    passed_oracle = runner._write_ledger_entry.await_args.kwargs["price_oracle"]
    assert "WETH" in passed_oracle


@pytest.mark.asyncio
async def test_vib4318_pre_teardown_quote_wins_on_collision(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    """A HIGH-confidence pre-teardown ``portfolio_valuer`` quote is NOT
    overwritten by a STALE gateway-aggregated quote on the same symbol.
    Same precedence as :meth:`StrategyRunner._merge_oracle_for_ledger`.
    """
    runner = _make_runner(live_mode=True)
    runner._teardown_price_oracle = {
        "USDC": {
            "price_usd": "1.0",
            "oracle_source": "portfolio_valuer",
            "fetched_at": "2026-05-12T00:00:00+00:00",
            "confidence": "HIGH",
        },
    }
    # If our helper queried USDC, the gateway would return a wrong / stale
    # quote. The assertion is that the helper does NOT overwrite the
    # pre-teardown entry on collision.
    runner.price_oracle = _make_price_oracle(
        {"USDC": "0.99"}, source="gateway", confidence="STALE"
    )

    intent = _make_swap_intent(from_token="USDC", to_token="USDT")
    result = _make_execution_result(tx_hash="0xusdc_usdt")
    context = SimpleNamespace(protocol="uniswap_v3", chain="arbitrum")

    await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        teardown_cycle_id="teardown-vib4318-collision",
    )

    # The pre-teardown HIGH-confidence quote is preserved.
    assert runner._teardown_price_oracle["USDC"]["price_usd"] == "1.0"
    assert runner._teardown_price_oracle["USDC"]["oracle_source"] == "portfolio_valuer"
    assert runner._teardown_price_oracle["USDC"]["confidence"] == "HIGH"


@pytest.mark.asyncio
async def test_vib4318_unknown_price_not_fabricated_as_zero(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    """Empty ≠ Zero (CLAUDE.md): when the gateway has no quote for a token,
    the helper MUST NOT insert a fabricated zero. The SWAP handler then
    keeps its fail-closed behaviour for that leg — which is correct, since
    "price was genuinely unavailable" is a different signal from "price
    was available but never queried".
    """
    runner = _make_runner(live_mode=True)
    runner._teardown_price_oracle = {
        "USDC": {
            "price_usd": "1.0",
            "oracle_source": "portfolio_valuer",
            "fetched_at": "2026-05-12T00:00:00+00:00",
            "confidence": "HIGH",
        },
    }
    # Gateway returns price=None for the unknown token.
    runner.price_oracle = _make_price_oracle({"XYZ": None})

    intent = _make_swap_intent(from_token="XYZ", to_token="USDC")
    result = _make_execution_result(tx_hash="0xxyz_swap")
    context = SimpleNamespace(protocol="uniswap_v3", chain="arbitrum")

    await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        teardown_cycle_id="teardown-vib4318-unknown",
    )

    # XYZ was queried (proving the helper tried), but NOT inserted.
    assert any(c["symbol"] == "XYZ" for c in runner.price_oracle._calls)
    assert "XYZ" not in runner._teardown_price_oracle


@pytest.mark.asyncio
async def test_vib4318_gateway_failure_does_not_block_ledger_write(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    """A gateway error on intent-token price fetch is logged at DEBUG and
    the teardown ledger row still lands (degraded-but-continue per
    VIB-3773). The pre-teardown stash is preserved.
    """
    runner = _make_runner(live_mode=True)
    runner._teardown_price_oracle = {
        "USDC": {
            "price_usd": "1.0",
            "oracle_source": "portfolio_valuer",
            "fetched_at": "2026-05-12T00:00:00+00:00",
            "confidence": "HIGH",
        },
    }

    async def _boom(symbol: str, quote: str, *, chain: str):
        raise ConnectionError("gateway dropped")

    runner.price_oracle = MagicMock()
    runner.price_oracle.get_aggregated_price = AsyncMock(side_effect=_boom)

    intent = _make_swap_intent(from_token="WETH", to_token="USDC")
    result = _make_execution_result(tx_hash="0xboom")
    context = SimpleNamespace(protocol="uniswap_v3", chain="arbitrum")

    outcome = await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        teardown_cycle_id="teardown-vib4318-boom",
    )

    # The ledger write still succeeded — the merge helper never raises.
    assert outcome.ledger_entry_id == "ledger-1"
    # The pre-teardown stash is untouched on every-token-failure.
    assert runner._teardown_price_oracle["USDC"]["price_usd"] == "1.0"
    assert "WETH" not in runner._teardown_price_oracle


@pytest.mark.asyncio
async def test_vib4318_none_stash_initialised_when_intent_tokens_priced(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    """When the pre-teardown bracket failed (stash starts as ``None``) but
    the gateway can quote at least one intent token, the merge helper
    must initialise the stash so the ledger row carries
    ``price_inputs_json`` instead of empty.

    Without this, returning ``None`` early on a ``None`` input would
    propagate to the ledger writer and defeat the fix.
    """
    runner = _make_runner(live_mode=True)
    runner._teardown_price_oracle = None
    runner.price_oracle = _make_price_oracle({"WETH": "2300.0", "USDC": "1.0"})

    intent = _make_swap_intent(from_token="WETH", to_token="USDC")
    result = _make_execution_result(tx_hash="0xnone_stash")
    context = SimpleNamespace(protocol="uniswap_v3", chain="arbitrum")

    await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        teardown_cycle_id="teardown-vib4318-none-stash",
    )

    assert runner._teardown_price_oracle is not None
    assert "WETH" in runner._teardown_price_oracle
    assert "USDC" in runner._teardown_price_oracle


@pytest.mark.asyncio
async def test_vib4318_address_shaped_intent_tokens_resolved_to_symbols(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    """gemini review on PR #2260 (2026-05-13): some connectors (Aerodrome
    confirmed) populate intent token fields with CONTRACT ADDRESSES rather
    than symbols. ``_extract_tokens_from_intent`` filters those out via
    ``_is_symbol`` (anything starting with ``"0x"`` is dropped), so the
    symbol-only loop never fetches a price for them — the original
    VIB-4318 fix would silently regress for any address-based connector.

    The address-resolution helper resolves address-shaped intent fields
    to symbols via the singleton :class:`TokenResolver` (skip_gateway=True)
    and merges the resolved symbol into the teardown stash so the ledger
    row carries a SYMBOL-keyed ``price_inputs_json`` for the downstream
    ``swap_handler`` lookup.
    """
    # Base USDC (well-known 6-decimal address) — guaranteed in the static
    # token resolver so ``skip_gateway=True`` resolves it without a gateway.
    usdc_base_address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"

    runner = _make_runner(live_mode=True)
    runner._teardown_price_oracle = {
        "WETH": {
            "price_usd": "2295.62",
            "oracle_source": "portfolio_valuer",
            "fetched_at": "2026-05-12T00:00:00+00:00",
            "confidence": "HIGH",
        },
    }
    runner.price_oracle = _make_price_oracle({"USDC": "1.0"})

    # Aerodrome-style intent: ``to_token`` is an ADDRESS, not a symbol.
    # Override the swap intent's chain to ``base`` so the resolver can
    # disambiguate the USDC address (USDC exists on multiple chains).
    intent = SimpleNamespace(
        intent_type=SimpleNamespace(value="SWAP"),
        protocol="aerodrome",
        chain="base",
        from_token="WETH",
        to_token=usdc_base_address,
    )
    # Strategy's chain is what the merge helper reads — point it at Base.
    fake_strategy.chain = "base"

    result = _make_execution_result(tx_hash="0xaerodrome_swap")
    context = SimpleNamespace(protocol="aerodrome", chain="base")

    outcome = await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        teardown_cycle_id="teardown-vib4318-address-resolution",
    )

    assert outcome.accounting_degraded is False
    # The address resolved to ``USDC`` and the price was fetched + merged.
    # Pre-fix the helper would have skipped the address leg entirely and
    # ``USDC`` would NOT appear in the stash.
    assert "USDC" in runner._teardown_price_oracle, (
        "Address-shaped intent token must resolve to its symbol and land "
        "in the stash — otherwise the ledger row's price_inputs_json is "
        "missing the close-side leg."
    )
    assert runner._teardown_price_oracle["USDC"]["price_usd"] == "1.0"
    # Pre-existing WETH quote is preserved.
    assert runner._teardown_price_oracle["WETH"]["price_usd"] == "2295.62"
    # The gateway was queried by SYMBOL, not by address — keys are canonical.
    queried_symbols = {call["symbol"] for call in runner.price_oracle._calls}
    assert "USDC" in queried_symbols
    assert usdc_base_address not in queried_symbols
    assert usdc_base_address.upper() not in queried_symbols


@pytest.mark.asyncio
async def test_vib4318_unresolvable_address_dropped_not_fabricated(
    fake_strategy, patch_enricher_and_sidecar, local_db_dir: Path
):
    """Empty ≠ Zero: an address that the local token resolver cannot resolve
    (no entry in the static catalogue) must be SILENTLY DROPPED, not
    substituted with the raw address as a phantom symbol. Mirrors
    ``swap_handler._resolve_price_lookup_key``'s fall-through semantics —
    the downstream ``has_price_*`` check then correctly reports
    "missing prices: <address>" rather than fabricating a symbol that
    never appears in any ``price_inputs_json``.
    """
    unknown_address = "0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead"

    runner = _make_runner(live_mode=True)
    runner._teardown_price_oracle = {
        "WETH": {
            "price_usd": "2295.62",
            "oracle_source": "portfolio_valuer",
            "fetched_at": "2026-05-12T00:00:00+00:00",
            "confidence": "HIGH",
        },
    }
    # price_oracle would raise KeyError on any symbol query — the
    # assertion is that no such query happens for the unresolvable address.
    runner.price_oracle = _make_price_oracle({})

    intent = SimpleNamespace(
        intent_type=SimpleNamespace(value="SWAP"),
        protocol="aerodrome",
        chain="base",
        from_token="WETH",
        to_token=unknown_address,
    )
    fake_strategy.chain = "base"

    result = _make_execution_result(tx_hash="0xunknown_address")
    context = SimpleNamespace(protocol="aerodrome", chain="base")

    outcome = await commit_teardown_intent(
        runner,
        fake_strategy,
        intent,
        execution_result=result,
        execution_context=context,
        teardown_cycle_id="teardown-vib4318-unknown-address",
    )

    assert outcome.accounting_degraded is False
    # Empty ≠ Zero — no phantom-keyed entry inserted.
    assert unknown_address not in runner._teardown_price_oracle
    assert unknown_address.upper() not in runner._teardown_price_oracle
    # WETH (symbol leg) was still attempted but the existing pre-teardown
    # quote on WETH is authoritative, so the helper short-circuits the
    # symbol leg via the case-insensitive _already_present check.
    queried_symbols = {call["symbol"] for call in runner.price_oracle._calls}
    # The unresolvable address must NOT have hit the gateway as a phantom symbol.
    assert unknown_address not in queried_symbols
    assert unknown_address.upper() not in queried_symbols
