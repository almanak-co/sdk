"""Fixture source selection, unchanged signatures and post-origin eligibility fail closed."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from eth_abi import decode, encode

from almanak.connectors.gmx_v2 import report_source as source
from almanak.connectors.gmx_v2.fresh_reports import FreshReports, Limits
from almanak.connectors.gmx_v2.oracle_reader import StreamConfiguration
from almanak.gateway.ethereum import DataError
from almanak.models import Block

ROOT = Path(__file__).parents[1]


@pytest.fixture(params=["arbitrum", "avalanche"])
def captured(request):
    folder = ROOT / "docs/internal/reports/evidence"
    proof = json.loads(
        gzip.decompress(
            (
                folder
                / "2026-09-08-gmx-position-paths"
                / (request.param + "-signed-reports.json.gz")
            ).read_bytes()
        )
    )
    binding = json.loads(
        (
            folder / "2026-09-09-gmx-fresh-source" / (request.param + "-explicit-bindings.json")
        ).read_text()
    )
    return SimpleNamespace(
        row=proof["selected"][0],
        origin=Block.model_validate_json(json.dumps(proof["origin"])),
        selection=source.SourceSelection.model_validate(
            binding["selection"]
            | {
                "calldata_suffix": {
                    "prefix": "0xdeadbeef00010203" if request.param == "arbitrum" else "0x",
                    "length": 15 if request.param == "arbitrum" else 0,
                }
            }
        ),
        destination=tuple(
            StreamConfiguration.model_validate_json(json.dumps(b)) for b in binding["bindings"]
        ),
        original=tuple(
            StreamConfiguration.model_validate_json(json.dumps(b))
            for b in binding["source_bindings"]
        ),
    )


def test_explicit_source_preserves_payload_and_separate_provider_binding(captured):
    vectors = source.source_transaction(captured.row["oracle_source"], captured.selection)
    selected, original = source.reports_for(vectors, captured.destination, captured.original)
    assert [r.signed_payload for r in selected] == [r.signed_payload for r in original]
    assert tuple(r.provider for r in selected) == tuple(b.provider for b in captured.destination)
    assert tuple(r.provider for r in original) == tuple(b.provider for b in captured.original)
    assert any(a.provider != b.provider for a, b in zip(selected, original, strict=True))


def test_other_handler_cannot_silently_supply_reports(captured):
    selection = captured.selection.model_copy(update={"handler": "0x" + "12" * 20})
    assert source.source_transaction(captured.row["oracle_source"], selection) is None
    transaction = captured.row["oracle_source"] | {"input": "0x12345678"}
    assert source.source_transaction(transaction, captured.selection) is None


def test_noncanonical_or_duplicate_calldata_cannot_supply_reports(captured):
    transaction = captured.row["oracle_source"]
    with pytest.raises(DataError, match="canonical encoding"):
        source.source_transaction(
            transaction | {"input": transaction["input"] + "00" * 32}, captured.selection
        )
    key, (tokens, providers, payloads) = decode(
        source.PARAMETERS, bytes.fromhex(transaction["input"][10:])
    )
    invalid = (key, (tokens + tokens[:1], providers + providers[:1], payloads + payloads[:1]))
    suffix = (
        transaction["input"][-2 * captured.selection.calldata_suffix.length :]
        if captured.selection.calldata_suffix.length
        else ""
    )
    malformed = transaction | {
        "input": captured.selection.selector + encode(source.PARAMETERS, invalid).hex() + suffix
    }
    with pytest.raises(DataError, match="duplicate or incomplete"):
        source.source_transaction(malformed, captured.selection)


def test_source_provider_and_required_asset_are_not_substituted(captured):
    vectors = source.source_transaction(captured.row["oracle_source"], captured.selection)
    asset = captured.destination[0].asset.reference
    wrong = vectors | {asset: ("0x" + "12" * 20, vectors[asset][1])}
    with pytest.raises(DataError, match="selected stream provider"):
        source.reports_for(wrong, captured.destination, captured.original)
    del vectors[asset]
    assert source.reports_for(vectors, captured.destination, captured.original) is None


def test_source_timestamp_requires_strict_post_request_eligibility(captured):
    vectors = source.source_transaction(captured.row["oracle_source"], captured.selection)
    selected, _ = source.reports_for(vectors, captured.destination, captured.original)
    times = [
        r.observed_at - b.timestamp_adjustment
        for r, b in zip(selected, captured.destination, strict=True)
    ]
    assert source.eligible(selected, captured.destination, min(times) - 1, 300)
    assert not source.eligible(selected, captured.destination, min(times), 300)
    assert not source.eligible(selected, captured.destination, max(times) - 301, 300)


def test_publication_must_be_canonical_success_after_origin(captured):
    transaction, receipt = captured.row["oracle_source"], captured.row["original_receipt"]
    block = captured.origin.model_copy(
        update={"number": int(transaction["blockNumber"], 16), "hash": transaction["blockHash"]}
    )
    reader = SimpleNamespace(pin_block=Mock(return_value=block), _rpc=Mock(return_value=receipt))
    assert source.canonical_receipt(reader, transaction, captured.origin) == (block, receipt)
    with pytest.raises(DataError, match="strictly after"):
        source.canonical_receipt(reader, transaction, block)
    reader._rpc.return_value = receipt | {"blockHash": "0x" + "12" * 32}
    with pytest.raises(DataError, match="no longer canonical"):
        source.canonical_receipt(reader, transaction, captured.origin)


def test_polling_bounds_refuse_instead_of_backshifting_origin():
    collector = object.__new__(FreshReports)
    collector.origin = SimpleNamespace(number=100)
    collector.cursor, collector.start = 101, 100
    collector.limits = Limits(
        page_blocks=10,
        maximum_blocks=20,
        maximum_transactions=5,
        maximum_logs=10,
        wait_seconds=1,
        poll_seconds=1,
    )
    collector.reader = SimpleNamespace(pin_block=Mock(return_value=SimpleNamespace(number=100)))
    collector.read_events = Mock()
    assert collector.read_page() is False
    collector.reader.pin_block.return_value = SimpleNamespace(number=110)
    assert collector.read_page() is True
    collector.read_events.assert_called_once_with(101, 110)
    collector.cursor = 121
    collector.reader.pin_block.return_value = SimpleNamespace(number=121)
    with pytest.raises(DataError, match="block bound"):
        collector.read_page()
