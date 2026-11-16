"""Retained Comet process outcomes replay offline without promoting candidate support."""

import json

import pytest

from almanak.connectors.compound_v3.models import Config
from almanak.connectors.compound_v3.state import Checkpoint
from almanak.connectors.compound_v3.support import candidate_support
from almanak.connectors.interfaces import Connector
from tests.lending_captures import read_capture
from tests.retained_fixtures import retained

EVIDENCE = retained("2026-09-07-rollout/compound-v3-native-initial")
CASES = tuple(json.loads((EVIDENCE / "manifest.json").read_text())["cases"])
# The same six lifecycles recaptured after the held-balance repay approval (blueprint 25)
# and scope-only authority (blueprint 27); read lazily so collection never depends on it.
RECAPTURED = retained("2026-09-07-rollout/compound-v3-native-initial-2026-10-20")


@pytest.fixture
def candidate_compound_support(monkeypatch):
    original = Connector.support_for
    supports = {row.chain: row for row in candidate_support()}
    monkeypatch.setattr(
        Connector,
        "support_for",
        lambda self, chain: (
            supports[chain]
            if self.codec.config is Config and chain in supports
            else original(self, chain)
        ),
    )


@pytest.fixture(
    params=CASES,
    ids=lambda row: row["reference"] + "-" + row["profile"] + "-" + row["operator_mode"],
)
def recorded_compound(request, candidate_compound_support):
    path = EVIDENCE / request.param["name"] / "checkpoint.json.gz"
    return Checkpoint.model_validate_json(read_capture(path))


@pytest.fixture(
    params=CASES,
    ids=lambda row: row["reference"] + "-" + row["profile"] + "-" + row["operator_mode"],
)
def recaptured_compound(request, candidate_compound_support):
    identity = tuple(request.param[key] for key in ("reference", "profile", "operator_mode"))
    rows = json.loads((RECAPTURED / "manifest.json").read_text())["cases"]
    (name,) = [
        row["name"]
        for row in rows
        if tuple(row[key] for key in ("reference", "profile", "operator_mode")) == identity
    ]
    return Checkpoint.model_validate_json(read_capture(RECAPTURED / name / "checkpoint.json.gz"))
