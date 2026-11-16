"""The fresh report source binds its handlers and inspects candidates without retained evidence."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.caip import ChainId
from almanak.connectors.gmx_v2 import fresh_reports
from almanak.connectors.gmx_v2.fresh_reports import FreshReports, Limits, bound_selections
from almanak.connectors.gmx_v2.report_source import CalldataSuffix, SourceSelection
from almanak.gateway.ethereum import DataError
from almanak.models import Chain
from tests.test_gmx_fork_keeper import ARBITRUM, block

ORACLE = "0x" + "26" * 20
LIMITS = Limits(
    page_blocks=10,
    maximum_blocks=100,
    maximum_transactions=2,
    maximum_logs=10,
    wait_seconds=1,
    poll_seconds=1,
)


def selection(handler, oracle=ORACLE, kind="order"):
    return SourceSelection(
        handler="0x" + handler * 20,
        oracle=oracle,
        kind=kind,
        calldata_suffix=CalldataSuffix(prefix="0x", length=0),
    )


DESTINATION = Mock(model_dump=Mock(return_value={"stream": "destination"}))
ORIGINAL = Mock(model_dump=Mock(return_value={"stream": "original"}))


@pytest.fixture
def bound(monkeypatch):
    calls = []

    def bindings(reader, market, origin, selected):
        calls.append(selected.handler)
        return (DESTINATION,), (ORIGINAL,)

    monkeypatch.setattr(fresh_reports.source, "bindings", bindings)
    return calls


def test_one_or_several_handlers_sharing_an_oracle_bind_once_each(bound):
    order, deposit = selection("a1"), selection("a2", kind="deposit")
    assert bound_selections(None, None, None, order) == (
        (order,),
        ((DESTINATION,), (ORIGINAL,)),
    )
    selections, binding = bound_selections(None, None, None, [order, deposit])
    assert selections == (order, deposit) and binding == ((DESTINATION,), (ORIGINAL,))
    assert bound == [order.handler, order.handler, deposit.handler]


def test_handlers_of_different_oracles_or_streams_are_refused(bound, monkeypatch):
    with pytest.raises(ValueError, match="share one selected Oracle"):
        bound_selections(
            None, None, None, [selection("a1"), selection("a2", oracle="0x" + "27" * 20)]
        )
    streams = iter([(("a",), ("a",)), (("b",), ("b",))])
    monkeypatch.setattr(fresh_reports.source, "bindings", lambda *args: next(streams))
    with pytest.raises(DataError, match="bind different streams"):
        bound_selections(None, None, None, [selection("a1"), selection("a2")])


class Reader:
    chain = ARBITRUM

    def pin_block(self, number):
        return block(number, 1000 + number)


def built(tmp_path, monkeypatch, bound, origin=None):
    monkeypatch.setattr(fresh_reports.source, "identities", lambda *args: {"0x01": "0xcode"})
    monkeypatch.setattr(fresh_reports, "verify_deployments", lambda reader, market, origin: ())
    monkeypatch.setattr(fresh_reports, "stored", lambda *args: 300)
    market = SimpleNamespace(chain=ARBITRUM, contracts=SimpleNamespace(data_store="0x" + "99" * 20))
    return FreshReports(
        Reader(),
        market,
        origin or block(50, 1050),
        [selection("a1"), selection("a2", kind="withdrawal")],
        LIMITS,
        tmp_path / "source.json",
    )


def test_a_source_starts_after_its_origin_and_retains_its_handlers(tmp_path, monkeypatch, bound):
    reports = built(tmp_path, monkeypatch, bound)
    assert (reports.cursor, reports.start, reports.expiration) == (51, 50, 300)
    assert reports.selection.kind == "order" and reports.identities == {"0x01": "0xcode"}
    retained = json.loads((tmp_path / "source.json").read_text())
    assert [s["kind"] for s in retained["selections"]] == ["order", "withdrawal"]


def test_a_source_refuses_another_chain_or_a_reorganized_origin(tmp_path, monkeypatch, bound):
    other = block(50, 1050).model_copy(update={"hash": "0x" + "ff" * 32})
    with pytest.raises(DataError, match="no longer canonical"):
        built(tmp_path, monkeypatch, bound, origin=other)
    mainnet = block(50, 1050, Chain(chain_id=ChainId("eip155:1"), environment="mainnet"))
    with pytest.raises(ValueError, match="selected market chain"):
        built(tmp_path, monkeypatch, bound, origin=mainnet)


def inspector(transactions, accepted):
    collector = object.__new__(FreshReports)
    collector.limits, collector.transactions, collector.examined = LIMITS, 0, []
    collector.candidates = list(transactions)
    collector.selections, collector.destination, collector.original = (), (), ()
    collector.reader = SimpleNamespace(_rpc=lambda method, params: {"hash": params[0]})
    collector.accept = Mock(side_effect=accepted)
    return collector


def test_candidates_skip_other_handlers_and_missing_assets(monkeypatch):
    vectors = {"0x1": None, "0x2": {"t": 1}, "0x3": {"t": 1}}
    reports = {"0x2": None, "0x3": ("selected", "original")}
    collector = inspector(["0x1", "0x2", "0x3"], [None])
    collector.limits = LIMITS.model_copy(update={"maximum_transactions": 5})
    monkeypatch.setattr(
        fresh_reports.source, "transaction_vectors", lambda tx, selections: vectors[tx["hash"]]
    )
    monkeypatch.setattr(
        fresh_reports.source,
        "reports_for",
        lambda found, destination, original: reports[collector.examined[-1]["hash"]],
    )
    assert collector.inspect_candidates(1, 1) is None
    assert [t["hash"] for t in collector.examined] == ["0x1", "0x2", "0x3"]
    collector.accept.assert_called_once()
    collector = inspector(["0x3"], ["row"])
    assert collector.inspect_candidates(1, 1) == "row"


def test_candidate_inspection_is_bounded(monkeypatch):
    monkeypatch.setattr(fresh_reports.source, "transaction_vectors", lambda tx, selections: None)
    collector = inspector(["0x1", "0x2", "0x3"], [])
    with pytest.raises(DataError, match="transaction bound"):
        collector.inspect_candidates(1, 1)
