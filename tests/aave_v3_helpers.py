"""Captured contracts exercise the trusted registered V3 support boundary."""

from pathlib import Path

import pytest

from almanak.connectors.aave_v3.state import Checkpoint
from tests.lending_captures import read_capture


@pytest.fixture
def recorded_v3():
    path = Path(__file__).parent / "fixtures/aave-v3-safe-lifecycle.json.gz"
    return Checkpoint.model_validate_json(read_capture(path))
