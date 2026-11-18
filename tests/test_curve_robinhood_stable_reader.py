"""Robinhood standard-coin pool identity and integer quotes replay without HTTP."""

import gzip
import json
from pathlib import Path

import httpx
import pytest

from almanak.connectors.curve.domain import PoolObservation
from almanak.connectors.curve.reader import read_pool
from tests.test_curve_arbitrum_reader import stable_prediction
from tests.test_curve_reader import CapturedReader

DIRECTORY = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-robinhood-stable-discovery"
)
DATA = json.loads(
    gzip.decompress((DIRECTORY / "curve-robinhood-agent-stable-reader.json.gz").read_bytes())
)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("Robinhood captured evidence cannot access HTTP")

    monkeypatch.setattr(httpx.Client, "send", refuse)


def test_pool_identity_matches_captured_canonical_revision():
    observed = PoolObservation.model_validate_json(json.dumps(DATA["observation"]))
    with CapturedReader(observed.source, DATA["records"]) as reader:
        block = reader.pin_block(observed.block.number)
        assert block == observed.block
        assert read_pool(reader, observed.source, block) == observed
        assert observed.asset_types == (0, 0)
        assert str(observed.source.chain.chain_id) == "eip155:4663"


@pytest.mark.parametrize("case", DATA["comparisons"])
def test_integer_swap_and_liquidity_quotes_match_archive(case):
    observed = PoolObservation.model_validate_json(json.dumps(DATA["observation"]))
    assert stable_prediction(observed.math_state(), case) == case["actual"] == case["modeled"]


@pytest.mark.parametrize("index", range(6))
def test_pinned_protocol_transitions_match_full_accounting(index):
    evidence = json.loads(
        gzip.decompress(
            (DIRECTORY / "curve-robinhood-agent-stable-transitions.json.gz").read_bytes()
        )
    )
    case = evidence["cases"][index]
    assert case["equal"] and case["expected"] == case["actual"]
    assert case["receipt"]["status"] == "0x1"
    assert case["before"]["source"] == case["after"]["source"]
    assert case["after"]["block"]["number"] > case["before"]["block"]["number"]
