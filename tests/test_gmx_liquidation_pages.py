"""Immutable page replay and canonicality audits cover scanner progress and reorganization."""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import liquidation_binding as binding
from almanak.connectors.gmx_v2 import liquidation_reader as reader
from almanak.connectors.gmx_v2 import liquidation_reconciliation as accounting
from almanak.gateway.ethereum import DataError
from tests import test_gmx_liquidation_replay

retained = test_gmx_liquidation_replay.retained


def provider(page):
    pinned = {page.start.number: page.start, page.end.number: page.end}
    return SimpleNamespace(
        chain=page.target.fund.chain,
        pin_block=lambda number: pinned[number],
        _rpc=lambda method, *args, **kwargs: (
            page.outcome.receipt if method == "eth_getTransactionReceipt" else page.outcome.trace
        ),
    )


def test_page_requeries_the_exact_prefix_ending_at_liquidation(monkeypatch, retained):
    before, page = retained
    later = page.end.model_copy(update={"number": page.end.number + 5})
    monkeypatch.setattr(reader, "bounds", lambda *args: (page.start, later))
    queried = []

    def events(backend, target, start, end):
        queried.append((start, end))
        return page.events

    monkeypatch.setattr(reader, "logs", events)
    monkeypatch.setattr(accounting, "reconcile", lambda *args: page.outcome)
    result = reader.page(SimpleNamespace(reader=provider(page)), before, page.target)
    assert result == page
    assert queried == [(page.start, later), (page.start, page.end)]


def test_empty_page_retains_cursor_evidence_without_inventing_effect(monkeypatch, retained):
    before, page = retained
    monkeypatch.setattr(reader, "bounds", lambda *args: (page.start, page.end))
    monkeypatch.setattr(reader, "logs", lambda *args: ())
    result = reader.page(SimpleNamespace(reader=provider(page)), before, page.target)
    assert result.outcome is None and result.events == ()
    assert result.start == page.start and result.end == page.end


def test_no_new_block_has_no_page(monkeypatch, retained):
    before, page = retained
    monkeypatch.setattr(reader, "bounds", lambda *args: None)
    assert reader.page(SimpleNamespace(reader=None), before, page.target) is None


def test_prior_request_progress_does_not_consume_external_cursor(monkeypatch, retained):
    before, page = retained
    monkeypatch.setattr(reader, "bounds", lambda *args: (page.start, page.end))
    monkeypatch.setattr(reader, "logs", lambda *args: page.events)
    monkeypatch.setattr(reader, "selected_liquidation", lambda *args: (None, True))
    assert reader.page(SimpleNamespace(reader=provider(page)), before, page.target) is None
    assert before.external_scans == ()


def test_audit_replays_canonical_bounds_events_receipt_and_trace(monkeypatch, retained):
    before, page = retained
    state = before.model_copy(update={"external_scans": (page,)})
    monkeypatch.setattr(reader, "logs", lambda *args: page.events)
    binding.audit(SimpleNamespace(reader=provider(page)), state)
    monkeypatch.setattr(reader, "logs", lambda *args: ())
    with pytest.raises(DataError, match="events are no longer canonical"):
        binding.audit(SimpleNamespace(reader=provider(page)), state)


def test_audit_refuses_changed_canonical_boundary(retained):
    before, page = retained
    state = before.model_copy(update={"external_scans": (page,)})
    changed = provider(page)
    changed.pin_block = lambda _: page.end
    with pytest.raises(DataError, match="bounds are no longer canonical"):
        binding.audit(SimpleNamespace(reader=changed), state)


def test_audit_refuses_changed_receipt_or_trace(retained):
    _, page = retained
    changed = provider(page)
    changed._rpc = lambda *args, **kwargs: {}
    with pytest.raises(DataError, match="receipt or trace has changed"):
        binding.audit_outcome(changed, page.outcome)
