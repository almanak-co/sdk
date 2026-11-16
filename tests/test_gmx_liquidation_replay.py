"""Replay the retained real liquidation outcome against fixed evidence without provider I/O."""

import gzip
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import liquidation_reconciliation as reconciliation
from almanak.connectors.gmx_v2.liquidation_reader import anchor, bounds, logs
from almanak.connectors.gmx_v2.state import Checkpoint
from almanak.gateway.ethereum import DataError
from tests.gmx_retired import current_json
from tests.gmx_retired import retired_closure as retired_closure

EVIDENCE = Path(__file__).parents[1] / (
    "docs/internal/reports/evidence/2026-09-09-gmx-liquidation/arbitrum-eoa-runtime-first"
)


@pytest.fixture
def retained():
    final = Checkpoint.model_validate_json(
        current_json(gzip.decompress((EVIDENCE / "checkpoint.json.gz").read_bytes()))
    )
    outcome = final.outcomes[-1]
    before = final.model_copy(
        update={
            "mode": "running",
            "closure": (),
            "external_scans": (),
            "outcomes": final.outcomes[:-1],
            "delivered": len(final.outcomes) - 1,
            "owned": (outcome.before.owned,),
        }
    )
    return before, final.external_scans[-1]


# The handler identity is stubbed below; the receipt's emitters are checked against the
# retired closure that emitted it, because the claim is the financial result.
@pytest.mark.usefixtures("retired_closure")
def test_reconcile_real_native_capture_preserves_exact_financial_result(monkeypatch, retained):
    before, page = retained
    expected = page.outcome
    monkeypatch.setattr(
        reconciliation, "verify_handler", lambda reader, target, block: expected.handler
    )
    monkeypatch.setattr(reconciliation, "snapshot", lambda *args: expected.after)
    provider = SimpleNamespace(
        _rpc=lambda method, *args, **kwargs: (
            expected.receipt if method == "eth_getTransactionReceipt" else expected.trace
        )
    )
    result = reconciliation.reconcile(
        SimpleNamespace(reader=provider), before, page.target, expected.position_event, page.end
    )
    assert result == expected
    assert result.strategy_result().kind == "external_liquidation"
    assert not hasattr(result, "operation") and not hasattr(result, "request")


def test_reference_page_never_uses_moving_latest(retained):
    before, page = retained
    page = page.model_copy(update={"start": anchor(before, page.target)})
    pinned = {page.start.number: page.start, page.end.number: page.end}
    provider = SimpleNamespace(pin_block=lambda selector: pinned[selector])
    assert bounds(provider, before, page.target, page) == (page.start, page.end)
    with pytest.raises(DataError, match="bounded cursor"):
        bounds(
            provider,
            before,
            page.target,
            page.model_copy(
                update={"start": page.start.model_copy(update={"number": page.start.number + 1})}
            ),
        )


def test_reference_page_reorganization_refuses_retained_bounds(retained):
    before, page = retained
    page = page.model_copy(update={"start": anchor(before, page.target)})
    provider = SimpleNamespace(
        pin_block=lambda number: page.end.model_copy(
            update={"number": number, "hash": "0x" + "12" * 32}
        )
    )
    with pytest.raises(DataError, match="no longer canonical"):
        bounds(provider, before, page.target, page)


def event_logs(page):
    from almanak.connectors.gmx_v2.events import decode_event

    return [
        log
        for log in page.outcome.receipt["logs"]
        if (event := decode_event(log, emitter=page.target.market.contracts.event_emitter))
        is not None
        and event.name in ("PositionIncrease", "PositionDecrease")
    ]


def test_scan_query_checks_fund_emitter_bounds_and_deduplicates(retained):
    _, page = retained
    selected = event_logs(page)
    calls = []

    def rpc(method, args):
        calls.append((method, args))
        return selected

    provider = SimpleNamespace(_rpc=rpc)
    assert logs(provider, page.target, page.start, page.end) == page.events
    assert calls[0][1][0]["address"] == page.target.market.contracts.event_emitter
    assert calls[0][1][0]["fromBlock"] == hex(page.start.number)
    selected.extend(selected)
    with pytest.raises(DataError, match="duplicate"):
        logs(provider, page.target, page.start, page.end)


def test_empty_receipt_cannot_be_substituted_for_liquidation(monkeypatch, retained):
    before, page = retained
    monkeypatch.setattr(
        reconciliation, "verify_handler", lambda reader, target, block: page.outcome.handler
    )
    provider = SimpleNamespace(_rpc=lambda *args, **kwargs: None)
    with pytest.raises(DataError, match="absent from canonical"):
        reconciliation.reconcile(
            SimpleNamespace(reader=provider),
            before,
            page.target,
            page.outcome.position_event,
            page.end,
        )


def test_opening_block_is_scanned_for_same_block_external_effect(retained):
    before, page = retained
    opening = anchor(before, page.target)
    provider = SimpleNamespace(
        pin_block=lambda selector: (
            page.end
            if selector == "latest"
            else opening
            if selector == opening.number
            else page.end
        )
    )
    first, _ = bounds(provider, before, page.target, None)
    assert first == opening
    with pytest.raises(DataError, match="cannot precede its financial reference"):
        reconciliation.reconcile(
            SimpleNamespace(reader=provider),
            before,
            page.target,
            page.outcome.position_event,
            opening,
        )


def test_scan_rotation_allows_other_owned_allocation_to_progress(monkeypatch, retained):
    from almanak.connectors.gmx_v2 import liquidation_binding, liquidation_reader

    before, page = retained
    owned = before.owned[0]
    other = owned.model_copy(
        update={"target": owned.target.model_copy(update={"allocation": "Other"})}
    )
    state = before.model_copy(update={"owned": (owned, other), "external_scans": (page,)})
    visited = []

    def scan(backend, state, target, reference):
        visited.append(target)
        return page

    monkeypatch.setattr(liquidation_reader, "page", scan)
    assert liquidation_binding.reconcile(None, state) == page
    assert visited == [other.target]
