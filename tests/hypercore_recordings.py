"""Recorded mainnet HyperCore `/info` answers (`scripts/record_hyperliquid_info.py`)."""

import gzip
import json
from pathlib import Path

from almanak.gateway.hypercore_info import InfoRequest, InfoResult

FIXTURE = Path(__file__).parent / "fixtures/hyperliquid-mainnet-info.json.gz"
RECORDINGS: dict[str, dict] = {
    row["label"]: row for row in json.loads(gzip.decompress(FIXTURE.read_bytes()))["recordings"]
}
SUBJECTS = {
    "leader": "0x677d831aef5328190852e24f13c46cac05f984e7",
    "fresh": "0x5c5368732f2a5b11506ef99305b68dbcf7c989dd",
    "hlp_child": "0x010461c14e146ac35fe42271bdc1134ee31c703a",
    "unified": "0xdf9ea6ec3b7109935ccb4fb267e15ac1fb077ab1",
    "cloid_trader": "0xf5d81a135f756ca16544e53c20fc20643ec3ad53",
}


def recorded_request(label: str) -> InfoRequest:
    return InfoRequest.model_validate_json(json.dumps(RECORDINGS[label]["request"]))


def answer(label: str):
    return json.loads(json.dumps(RECORDINGS[label]["result"]))


class RecordedInfo:
    """Answers each query with the recording made for exactly that request."""

    def __init__(self, overrides: dict[str, object] | None = None):
        self.by_request = {recorded_request(label): label for label in RECORDINGS}
        # Replacement answers by label, for cases the recordings do not show.
        self.overrides = overrides or {}
        self.sent: list[InfoRequest] = []

    def fetch(self, request: InfoRequest) -> InfoResult:
        self.sent.append(request)
        label = self.by_request[request]
        row = RECORDINGS[label]
        result = self.overrides.get(label, row["result"])
        return InfoResult(request=request, received_at=row["received_at"], result=result)

    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        pass
