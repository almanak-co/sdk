"""Raw signed stream evidence cannot be relabeled as a different price or time."""

import gzip
import json
from pathlib import Path

import pytest
from eth_abi import decode, encode

from almanak.caip import AssetType
from almanak.connectors.gmx_v2.events import decode_event
from almanak.connectors.gmx_v2.oracle import ENVELOPE_TYPES, StreamReport, decode_report
from almanak.connectors.gmx_v2.oracle_reader import StreamConfiguration, normalized_bounds

EVIDENCE = (
    Path(__file__).parents[1] / "docs/internal/reports/evidence/2026-09-08-gmx-request-fixtures"
)


def report():
    proof = json.loads(
        gzip.decompress((EVIDENCE / "arbitrum-eoa-open-close-cancel.json.gz").read_bytes())
    )
    captured = next(row for row in proof["actions"] if row["function"] == "executeOrder")[
        "reports"
    ][0]
    return decode_report(
        AssetType(captured["asset"]),
        captured["provider"],
        bytes.fromhex(captured["signed_payload"][2:]),
    )


def test_captured_signed_report_preserves_original_stream_and_timestamp():
    value = report()
    assert value.feed_id == "0x000362205e10b3a147d02792eccee483dca6c7b44ecce7012cb8c6e0b68b3ae9"
    assert value.observed_at == 1788827815
    assert value.bid == 2491135546658762600000
    assert value.price_unit == "feed_report_integer"
    assert StreamReport.model_validate_json(value.model_dump_json()) == value


@pytest.mark.parametrize("field", ["bid", "observed_at", "native_verification_fee"])
def test_fields_cannot_diverge_from_signed_payload(field):
    value = report()
    with pytest.raises(ValueError):
        StreamReport.model_validate({**value.model_dump(), field: getattr(value, field) + 1})


def test_missing_signatures_and_trailing_payload_are_not_valid_captures():
    value = report()
    original = bytes.fromhex(value.signed_payload[2:])
    envelope = list(decode(ENVELOPE_TYPES, original))
    envelope[2] = ()
    with pytest.raises(ValueError, match="signature vectors"):
        decode_report(value.asset, value.provider, encode(ENVELOPE_TYPES, envelope))
    with pytest.raises(ValueError, match="canonical ABI"):
        decode_report(value.asset, value.provider, original + bytes(32))


def captured_prices():
    proof = json.loads(
        gzip.decompress((EVIDENCE / "arbitrum-eoa-open-close-cancel.json.gz").read_bytes())
    )
    for row in proof["actions"]:
        if row["function"] == "executeOrder":
            for captured in row["reports"]:
                yield row["receipt"], StreamReport.model_validate_json(json.dumps(captured))


@pytest.mark.parametrize("receipt,captured", list(captured_prices()))
def test_stream_normalization_matches_real_oracle_execution(receipt, captured):
    bindings = json.loads(
        gzip.decompress((EVIDENCE / "arbitrum-stream-bindings.json.gz").read_bytes())
    )
    config = next(
        StreamConfiguration.model_validate_json(json.dumps(c))
        for c in bindings["streams"]
        if c["asset"] == str(captured.asset)
    )
    emitted = [
        event
        for log in receipt["logs"]
        if (event := decode_event(log, emitter="0xc8ee91a54287db53897056e12d9819156d3822fb"))
        and event.name == "OraclePriceUpdate"
        and event.value("address", "token") == captured.asset.reference
    ]
    assert len(emitted) == 1
    event = emitted[0]
    assert event.sender == config.oracle
    assert event.value("address", "provider") == config.provider
    assert normalized_bounds(config, captured) == (
        event.value("uint256", "minPrice"),
        event.value("uint256", "maxPrice"),
    )
    assert event.value("uint256", "timestamp") == captured.observed_at
    other = config.model_copy(update={"provider": "0x" + "11" * 20})
    with pytest.raises(ValueError, match="explicitly configured"):
        normalized_bounds(other, captured)
