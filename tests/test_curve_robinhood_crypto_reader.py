"""Both Robinhood crypto families replay exact captured states without HTTP."""

import gzip
import json
from pathlib import Path

import httpx
import pytest

from almanak.connectors.curve import (
    tricrypto_liquidity,
    tricrypto_quotes,
    twocrypto_liquidity,
    twocrypto_quotes,
)
from almanak.connectors.curve.crypto_observation import CryptoObservation
from almanak.connectors.curve.reader import read_pool
from almanak.connectors.curve.tricrypto_observation import TriCryptoObservation
from tests.test_curve_arbitrum_reader import crypto_prediction
from tests.test_curve_reader import CapturedReader

DIRECTORY = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-robinhood-crypto-discovery"
)


def captured(name):
    return json.loads(gzip.decompress((DIRECTORY / (name + ".json.gz")).read_bytes()))


DATA = captured("curve-robinhood-agent-crypto-quotes")
MODELS = (CryptoObservation, TriCryptoObservation)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("Robinhood captured evidence cannot access HTTP")

    monkeypatch.setattr(httpx.Client, "send", refuse)


@pytest.mark.parametrize("index", range(2))
def test_registered_revision_reproduces_captured_observation(index):
    observed = MODELS[index].model_validate_json(json.dumps(DATA["families"][index]["observation"]))
    with CapturedReader(observed.source, DATA["records"]) as reader:
        block = reader.pin_block(observed.block.number)
        assert block == observed.block
        assert read_pool(reader, observed.source, block) == observed
        assert str(observed.source.chain.chain_id) == "eip155:4663"


@pytest.mark.parametrize(
    "index,case",
    [(i, c) for i, family in enumerate(DATA["families"]) for c in family["comparisons"]],
)
def test_integer_swap_and_liquidity_quotes_match_archive(index, case):
    observed = MODELS[index].model_validate_json(json.dumps(DATA["families"][index]["observation"]))
    quotes, liquidity = (
        (twocrypto_quotes, twocrypto_liquidity)
        if index == 0
        else (tricrypto_quotes, tricrypto_liquidity)
    )
    if case["kind"] == "withdraw_one":
        case = {**case, "index": case["i"], "shares": case["amount"]}
    assert (
        crypto_prediction(observed.math_state(), case, quotes, liquidity)
        == case["actual"]
        == case["modeled"]
    )


@pytest.mark.parametrize(
    "family,index", [("two", i) for i in range(6)] + [("tri", i) for i in range(7)]
)
def test_pinned_transitions_match_pool_token_and_admin_accounting(family, index):
    case = captured(f"curve-robinhood-agent-{family}-transitions")["cases"][index]
    assert case["equal"] and case["expected"] == case["actual"]
    assert case["receipt"]["status"] == "0x1"
    assert case["before"]["source"] == case["after"]["source"]
    assert case["after"]["block"]["number"] > case["before"]["block"]["number"]


def test_core_twocrypto_pool_storage_matches_the_integer_model_revision():
    from almanak.connectors.curve.crypto_deployment import revision

    assert (
        revision("twocrypto-core-v2.1.0.json")["pool_storage_layout"]
        == revision()["pool_storage_layout"]
    )
