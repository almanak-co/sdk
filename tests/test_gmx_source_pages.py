"""Fresh scan pages keep their selected Oracle, canonical blocks and deduplicated transactions."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.connectors.gmx_v2 import report_source as source
from almanak.connectors.gmx_v2.fresh_reports import FreshReports
from almanak.gateway.ethereum import DataError
from tests.test_gmx_fresh_reports import captured as captured
from tests.test_gmx_report_events import source_arguments


def collector_and_event(captured):
    _, _, market = source_arguments(captured)
    event = source.source_events(captured.row["original_receipt"], market, captured.selection)[0]
    collector = object.__new__(FreshReports)
    collector.selection = captured.selection
    collector.reader = SimpleNamespace(
        pin_block=Mock(return_value=SimpleNamespace(hash=event.block_hash))
    )
    collector.seen, collector.candidates = set(), []
    return collector, event


def test_selected_source_transaction_is_canonical_and_admitted_once(captured):
    collector, event = collector_and_event(captured)
    for _ in range(2):
        collector.admit_event(event, event.block_number, event.block_number)
    assert collector.candidates == [event.transaction_hash]
    assert collector.seen == {event.transaction_hash}


def test_unselected_oracle_is_not_an_implicit_report_source(captured):
    collector, event = collector_and_event(captured)
    other = event.model_copy(update={"sender": "0x" + "12" * 20})
    collector.admit_event(other, event.block_number, event.block_number)
    collector.admit_event(None, event.block_number, event.block_number)
    assert collector.candidates == []
    collector.reader.pin_block.assert_not_called()


def test_out_of_page_or_reorganized_event_cannot_enter_the_source_queue(captured):
    collector, event = collector_and_event(captured)
    with pytest.raises(DataError, match="outside the requested page"):
        collector.admit_event(event, event.block_number + 1, event.block_number + 2)
    collector.reader.pin_block.return_value = SimpleNamespace(hash="replacement-block")
    with pytest.raises(DataError, match="no longer canonical"):
        collector.admit_event(event, event.block_number, event.block_number)
    assert collector.candidates == []
