"""Explicit relay sources decode full canonical batches before accepting signed prices."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode

from almanak.connectors.gmx_v2.published_prices import PublishedPrices
from almanak.connectors.gmx_v2.published_source import (
    RELAY_BATCH_PARAMETERS,
    CalldataSuffix,
    PublishedSource,
    report_vectors,
    selected_reports,
)
from almanak.gateway.ethereum import DataError

ROOT = Path(__file__).parents[1] / "docs/internal/reports/evidence"


@pytest.fixture(params=[0, 1])
def relay(request):
    rows = json.loads((ROOT / "2026-09-09-gmx-finish/arbitrum-relay-publications.json").read_text())
    transactions = [
        r["result"] for r in rows if r["request"]["method"] == "eth_getTransactionByHash"
    ]
    tx = transactions[request.param]
    receipt = next(
        r["result"]
        for r in rows
        if r["request"]["method"] == "eth_getTransactionReceipt"
        and r["request"]["params"] == [tx["hash"]]
    )
    source = PublishedSource(
        handler=tx["to"],
        oracle="0x26c02f221e8db5a821e12347c7ea8a6b6e10842f",
        request_kind="relay_batch",
        calldata_suffix=CalldataSuffix(prefix="0x", length=0),
    )
    return SimpleNamespace(transaction=tx, receipt=receipt, source=source)


def test_retained_relay_batches_are_not_signed_report_opportunities(relay):
    root = ROOT / "2026-09-09-gmx-prepared"
    prices = PublishedPrices.model_validate_json(
        (root / "arbitrum-published-prices.json").read_text()
    )
    assert relay.source.selector == "0x0427ef5f"
    vectors = report_vectors(relay.transaction, relay.source)
    assert len(vectors) == 2
    assert all(payload == b"" for _, payload in vectors.values())
    with pytest.raises(DataError, match="selected source provider"):
        selected_reports(vectors, prices.target_bindings, prices.source_bindings)


def test_relay_batch_requires_full_canonical_encoding_and_selected_handler(relay):
    assert report_vectors(relay.transaction | {"to": "0x" + "12" * 20}, relay.source) is None
    assert report_vectors(relay.transaction | {"input": "0x12345678"}, relay.source) is None
    with pytest.raises(DataError, match="canonical shape"):
        report_vectors(
            relay.transaction | {"input": relay.transaction["input"] + "00"}, relay.source
        )
    raw = bytes.fromhex(relay.transaction["input"][10:])
    values = decode(RELAY_BATCH_PARAMETERS, raw)
    assert encode(RELAY_BATCH_PARAMETERS, values) == raw
    vectors = values[0][0]
    duplicate = tuple(items + items[:1] for items in vectors)
    params = ((duplicate, *values[0][1:]), *values[1:])
    with pytest.raises(DataError, match="duplicate or incomplete"):
        report_vectors(
            relay.transaction
            | {"input": relay.source.selector + encode(RELAY_BATCH_PARAMETERS, params).hex()},
            relay.source,
        )
