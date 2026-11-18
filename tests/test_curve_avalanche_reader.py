"""Avalanche standard-coin pool identity and integer quotes replay without HTTP."""

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
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-avalanche-discovery"
)
DATA = json.loads(
    gzip.decompress((DIRECTORY / "curve-avalanche-agent-stable-reader.json.gz").read_bytes())
)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("Avalanche captured evidence cannot access HTTP")

    monkeypatch.setattr(httpx.Client, "send", refuse)


def test_pool_identity_matches_captured_canonical_revision():
    observed = PoolObservation.model_validate_json(json.dumps(DATA["observation"]))
    with CapturedReader(observed.source, DATA["records"]) as reader:
        block = reader.pin_block(observed.block.number)
        assert block == observed.block
        assert read_pool(reader, observed.source, block) == observed
        assert observed.asset_types == (0, 0)
        assert str(observed.source.chain.chain_id) == "eip155:43114"


@pytest.mark.parametrize("case", DATA["comparisons"])
def test_integer_swap_and_liquidity_quotes_match_archive(case):
    observed = PoolObservation.model_validate_json(json.dumps(DATA["observation"]))
    assert stable_prediction(observed.math_state(), case) == case["actual"] == case["modeled"]


def test_crypto_discovery_keeps_zero_current_liquidity_explicit():
    evidence = json.loads(
        gzip.decompress(
            (DIRECTORY / "curve-avalanche-agent-crypto-availability.json.gz").read_bytes()
        )
    )
    assert [family["pool_count"] for family in evidence["families"]] == [2, 1]
    for family in evidence["families"]:
        assert len(family["pools"]) == family["pool_count"]
        for pool in family["pools"]:
            assert pool["current_supply"] == 0
            assert not any(pool["current_balances"])
            assert len(pool["historical_samples"]) == 5


@pytest.mark.parametrize("index", range(6))
def test_pinned_protocol_transitions_match_full_accounting(index):
    evidence = json.loads(
        gzip.decompress(
            (DIRECTORY / "curve-avalanche-agent-stable-transitions.json.gz").read_bytes()
        )
    )
    case = evidence["cases"][index]
    assert case["equal"] and case["expected"] == case["actual"]
    assert case["receipt"]["status"] == "0x1"
    assert case["before"]["source"] == case["after"]["source"]
    assert case["after"]["block"]["number"] > case["before"]["block"]["number"]
