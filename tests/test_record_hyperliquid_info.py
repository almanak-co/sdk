"""The HyperCore recorder sends only whitelisted bodies and keeps answers as received."""

import gzip
import json

import httpx

from almanak.gateway.hypercore_info import InfoRequest, body
from scripts import record_hyperliquid_info
from tests.hypercore_recordings import RECORDINGS

CLIENT = httpx.Client


def recorded_answer(sent: dict):
    # The recording of the same query for the same account; ranges move with the clock.
    for row in RECORDINGS.values():
        info = row["request"]["info"]
        if info["type"] == sent["type"] and info.get("user") == sent.get("user"):
            return row["result"]
    raise AssertionError(f"unexpected query {sent}")


def test_the_recorder_sends_only_whitelisted_bodies(monkeypatch, tmp_path):
    monkeypatch.setattr(record_hyperliquid_info, "PAUSE", 0)
    sent = []

    def api(request):
        query = json.loads(request.content)
        sent.append(query)
        return httpx.Response(200, json=recorded_answer(query))

    monkeypatch.setattr(
        record_hyperliquid_info.httpx,
        "Client",
        lambda **_: CLIENT(transport=httpx.MockTransport(api)),
    )
    output = tmp_path / "info.json.gz"
    assert record_hyperliquid_info.main(["--output", str(output)]) == 0
    fixture = json.loads(gzip.decompress(output.read_bytes()))
    labels = [row["label"] for row in fixture["recordings"]]
    assert sorted(labels) == sorted(RECORDINGS)
    for row in fixture["recordings"]:
        request = InfoRequest.model_validate_json(json.dumps(row["request"]))
        assert body(request.info) in sent
        assert row["result"] == recorded_answer(body(request.info))
    # Every query sent is one the fixture records.
    assert len(sent) == len(labels)
