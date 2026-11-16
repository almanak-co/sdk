"""Avalanche signed prices reproduce the actual configured provider's emitted bounds."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2.events import decode_event
from almanak.connectors.gmx_v2.oracle import StreamReport
from almanak.connectors.gmx_v2.oracle_reader import StreamConfiguration, normalized_bounds

ROOT = Path(__file__).parents[1] / "docs/internal/reports/evidence/2026-09-08-gmx-avalanche-runtime"


def read(name):
    return json.loads(gzip.decompress((ROOT / name).read_bytes()))


def prices():
    for row in read("final-safe-runtime/external-execution.json.gz"):
        for report in row["reports"]:
            yield row["receipt"], report


@pytest.mark.parametrize("receipt,wire", list(prices()))
def test_avalanche_signed_reports_match_actual_oracle_bounds(receipt, wire):
    report = StreamReport.model_validate_json(json.dumps(wire))
    config = next(
        StreamConfiguration.model_validate_json(json.dumps(binding))
        for binding in read("signed-reports.json.gz")["bindings"]
        if binding["asset"] == str(report.asset)
    )
    events = [
        event
        for log in receipt["logs"]
        if (event := decode_event(log, emitter="0xdb17b211c34240b014ab6d61d4a31fa0c0e20c26"))
        and event.name == "OraclePriceUpdate"
        and event.value("address", "token") == report.asset.reference
    ]
    assert len(events) == 1
    event = events[0]
    assert event.sender == config.oracle
    assert event.value("address", "provider") == config.provider
    assert event.value("uint256", "timestamp") == report.observed_at - config.timestamp_adjustment
    assert normalized_bounds(config, report) == (
        event.value("uint256", "minPrice"),
        event.value("uint256", "maxPrice"),
    )
