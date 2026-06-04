"""``AccountingProcessor._dispatch`` stage-1 connector-treatment routing (VIB-4931 PR-A commit 3).

Stage 1 runs the strategy-side ``AccountingTreatmentRegistry`` before the generic
``classify`` / ``HANDLERS`` path: a connector that claims an event has its treatment
run in place of the generic category handler. These tests pin the precedence
(claimed → treatment, before classify), the decline path (unclaimed → generic), the
loud missing-treatment guard, and the real Pendle end-to-end route. Event-level
behaviour preservation is additionally covered by the frozen
``legacy_dispatch_truth_table.json`` parity tests, which re-run ``_dispatch`` and
still match (stage-1 produces the identical Pendle events).
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.accounting_treatment_base import AccountingCategoryDecision
from almanak.connectors._strategy_base.accounting_treatment_registry import (
    AccountingTreatmentRegistry,
)
from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.models import PendleEventType
from almanak.framework.accounting.processor import AccountingProcessor
from almanak.framework.primitives.types import AccountingCategory


@pytest.fixture(autouse=True)
def _reset_registry():
    AccountingTreatmentRegistry.reset_cache()
    yield
    AccountingTreatmentRegistry.reset_cache()


def _processor() -> AccountingProcessor:
    return AccountingProcessor(state_manager=None, basis_store=FIFOBasisStore(), deployment_id="dep-test")


def _rows(intent_type: str = "LP_OPEN", protocol: str = "pendle_v2", token_out: str = "PT-WETH"):
    outbox = {"id": "ob", "wallet_address": "0xwallet", "market_id": "0xmarket", "position_key": ""}
    ledger = {
        "id": "led",
        "intent_type": intent_type,
        "protocol": protocol,
        "token_out": token_out,
        "chain": "arbitrum",
        "tx_hash": "0xdeadbeef",
        "timestamp": "2026-01-02T03:04:05+00:00",
        "extracted_data_json": "",
    }
    return outbox, ledger


def test_stage1_claimed_event_runs_treatment_before_classify(monkeypatch):
    sentinel = object()
    treatment = MagicMock(return_value=sentinel)
    classify_spy = MagicMock()

    monkeypatch.setattr(
        AccountingTreatmentRegistry,
        "categorize",
        lambda _it, _p, _t: AccountingCategoryDecision(category=AccountingCategory.LP, treatment_key="k"),
    )
    monkeypatch.setattr(AccountingTreatmentRegistry, "treatment_for", lambda _k: treatment)
    monkeypatch.setattr("almanak.framework.accounting.processor.classify", classify_spy)

    outbox, ledger = _rows()
    result = _processor()._dispatch(outbox, ledger)

    assert result is sentinel
    treatment.assert_called_once()
    # The generic taxonomy path is never consulted for a claimed event.
    classify_spy.assert_not_called()
    # Treatment receives the same HandlerContext shape the generic handlers get.
    ctx = treatment.call_args.args[0]
    assert ctx.outbox_row is outbox and ctx.ledger_row is ledger


def test_stage1_declines_falls_through_to_generic(monkeypatch):
    sentinel = object()
    handler = MagicMock(return_value=sentinel)

    monkeypatch.setattr(AccountingTreatmentRegistry, "categorize", lambda _it, _p, _t: None)
    monkeypatch.setattr("almanak.framework.accounting.processor.classify", lambda *_a: AccountingCategory.LP)
    monkeypatch.setattr("almanak.framework.accounting.processor.HANDLERS", {AccountingCategory.LP: handler})

    result = _processor()._dispatch(*_rows(protocol="uniswap_v3", token_out="USDC"))

    assert result is sentinel
    handler.assert_called_once()


def test_stage1_missing_treatment_falls_through_to_generic(monkeypatch, caplog):
    # A connector claims the event but has no treatment for the key it returned (a
    # stale/typoed treatment_key — a wiring bug): _dispatch logs loudly and FALLS
    # THROUGH to the generic stage-2 path rather than silently dropping the accounting
    # event (CodeRabbit review on #2598).
    sentinel = object()
    generic_handler = MagicMock(return_value=sentinel)
    monkeypatch.setattr(
        AccountingTreatmentRegistry,
        "categorize",
        lambda _it, _p, _t: AccountingCategoryDecision(category=AccountingCategory.LP, treatment_key="orphan"),
    )
    monkeypatch.setattr(AccountingTreatmentRegistry, "treatment_for", lambda _k: None)
    monkeypatch.setattr("almanak.framework.accounting.processor.classify", lambda *_a: AccountingCategory.LP)
    monkeypatch.setattr("almanak.framework.accounting.processor.HANDLERS", {AccountingCategory.LP: generic_handler})

    with caplog.at_level(logging.ERROR):
        result = _processor()._dispatch(*_rows())

    assert result is sentinel  # fell through to the generic handler, not dropped
    generic_handler.assert_called_once()
    assert any("falling back to generic" in r.message for r in caplog.records)


def test_pendle_lp_routes_through_stage1_end_to_end():
    # Real registry + real Pendle spec: a Pendle LP_OPEN dispatches to the connector
    # treatment and yields a PENDLE_LP_OPEN event (the same event the legacy
    # PENDLE_LP handler produced).
    event = _processor()._dispatch(*_rows("LP_OPEN", "pendle_v2", "PT-WETH"))
    assert event is not None
    assert event.event_type == PendleEventType.PENDLE_LP_OPEN


def test_pendle_pt_routes_through_stage1_end_to_end():
    event = _processor()._dispatch(*_rows("SWAP", "pendle_v2", "PT-wstETH-25JUN2030"))
    assert event is not None
    assert event.event_type == PendleEventType.PT_BUY
