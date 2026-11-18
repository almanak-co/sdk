"""Source publication evidence cannot bypass its provider, time or target keeper authority."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.connectors.gmx_v2 import report_source as source
from almanak.connectors.gmx_v2.fresh_reports import FreshReports
from almanak.gateway.ethereum import DataError
from tests.test_gmx_fresh_reports import captured as captured


def source_arguments(captured):
    vectors = source.source_transaction(captured.row["oracle_source"], captured.selection)
    selected, original = source.reports_for(vectors, captured.destination, captured.original)
    market = SimpleNamespace(
        contracts=SimpleNamespace(
            event_emitter=captured.row["original_receipt"]["logs"][-1]["address"]
        )
    )
    return selected, original, market


def test_real_source_oracle_events_match_original_provider_report_bounds(captured):
    _, original, market = source_arguments(captured)
    events = source.source_events(captured.row["original_receipt"], market, captured.selection)
    assert events
    for binding, report in zip(captured.original, original, strict=True):
        source.verify_report_event(events, binding, report)
    changed = original[0].model_copy(update={"observed_at": original[0].observed_at + 1})
    with pytest.raises(DataError, match="signed report"):
        source.verify_report_event(events, captured.original[0], changed)
    with pytest.raises(DataError, match="one exact"):
        source.verify_report_event((), captured.original[0], original[0])


def test_source_oracle_logs_cannot_be_transplanted_from_another_receipt(captured):
    _, _, market = source_arguments(captured)
    receipt = captured.row["original_receipt"] | {"transactionHash": "0x" + "12" * 32}
    with pytest.raises(DataError, match="canonical receipt"):
        source.source_events(receipt, market, captured.selection)


@pytest.mark.parametrize("authorized", [False, True])
def test_existing_target_keeper_role_is_required_at_the_frozen_origin(captured, authorized):
    market = SimpleNamespace(contracts=SimpleNamespace(order_handler="target-handler"))
    reader = SimpleNamespace(
        pin_block=Mock(return_value=captured.origin),
        call=Mock(side_effect=[("target-role-store",), (authorized,)]),
    )
    if authorized:
        assert (
            source.keeper_authorized(reader, market, captured.origin, "source-keeper")
            == "target-role-store"
        )
    else:
        with pytest.raises(DataError, match="existing keeper role"):
            source.keeper_authorized(reader, market, captured.origin, "source-keeper")
    reader.pin_block.assert_called_once_with(captured.origin.number)
    assert reader.call.call_args.kwargs["args"][0] == "source-keeper"


def test_declared_calldata_suffix_format_keeps_identifier_bytes_opaque_and_bounded():
    suffix = source.CalldataSuffix(prefix="0xdeadbeef00010203", length=15)
    assert suffix.matches(bytes.fromhex("deadbeef00010203") + bytes(7))
    assert suffix.matches(bytes.fromhex("deadbeef00010203") + bytes.fromhex("02330c61d7eaaa"))
    assert not suffix.matches(bytes.fromhex("deadbeef00010203") + bytes(8))
    assert not suffix.matches(bytes.fromhex("deadbeef00010204") + bytes(7))
    with pytest.raises(ValueError, match="declared length"):
        source.CalldataSuffix(prefix="0xdeadbeef", length=3)


def test_explicit_source_wait_exhaustion_does_not_select_another_source(monkeypatch):
    from almanak.connectors.gmx_v2 import fresh_reports as gmx_fresh_reports

    collector = object.__new__(FreshReports)
    collector.inspect_candidates = Mock(return_value=None)
    collector.read_page = Mock(return_value=False)
    collector.limits = SimpleNamespace(poll_seconds=1)
    monkeypatch.setattr(gmx_fresh_reports.time, "monotonic", Mock(side_effect=[0, 2]))
    sleep = Mock()
    monkeypatch.setattr(gmx_fresh_reports.time, "sleep", sleep)
    with pytest.raises(DataError, match="explicit source within its wait bound"):
        collector.wait_for_source(100, 101, 1)
    collector.read_page.assert_called_once_with()
    sleep.assert_called_once_with(1)


def test_transaction_inspection_budget_fails_before_another_provider_call():
    collector = object.__new__(FreshReports)
    collector.transactions, collector.candidates = 1, ["retained-source-transaction"]
    collector.limits = SimpleNamespace(maximum_transactions=1)
    collector.reader = SimpleNamespace(_rpc=Mock())
    with pytest.raises(DataError, match="transaction bound"):
        collector.inspect_candidates(100, 101)
    collector.reader._rpc.assert_not_called()
