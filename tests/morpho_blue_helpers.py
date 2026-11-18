"""Retained Morpho process outcomes replay offline without promoting candidate support."""

import json

import pytest

from almanak.connectors.interfaces import Connector
from almanak.connectors.morpho_blue.models import Config
from almanak.connectors.morpho_blue.state import Checkpoint
from almanak.connectors.morpho_blue.support import candidate_support
from tests.lending_captures import read_capture
from tests.retained_fixtures import retained

EVIDENCE = retained("2026-09-07-rollout/morpho-blue-native-final")
CASES = tuple(json.loads((EVIDENCE / "manifest.json").read_text())["cases"])


@pytest.fixture
def candidate_morpho_support(monkeypatch):
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
def recorded_morpho(request, candidate_morpho_support):
    path = EVIDENCE / request.param["name"] / "checkpoint.json.gz"
    return Checkpoint.model_validate_json(read_capture(path))
