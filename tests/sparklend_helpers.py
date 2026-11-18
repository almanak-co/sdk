"""Explicit candidate scope for component tests, without runtime promotion."""

from pathlib import Path

import pytest

from almanak.connectors.interfaces import Connector
from almanak.connectors.sparklend.models import Config
from almanak.connectors.sparklend.state import Checkpoint
from almanak.connectors.sparklend.support import candidate_support
from tests.lending_captures import read_capture


@pytest.fixture
def candidate_spark_support(monkeypatch):
    original = Connector.support_for

    def support(self, chain):
        if self.codec.config is Config:
            for declaration in candidate_support():
                if declaration.chain == chain:
                    return declaration
        return original(self, chain)

    monkeypatch.setattr(Connector, "support_for", support)


@pytest.fixture
def recorded_spark(candidate_spark_support):
    path = Path(__file__).parent / "fixtures/sparklend-safe-lifecycle.json.gz"
    return Checkpoint.model_validate_json(read_capture(path))
