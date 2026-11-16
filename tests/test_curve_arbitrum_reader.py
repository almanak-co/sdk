"""All three configured Arbitrum families replay captured archive inputs offline."""

import gzip
import json
from pathlib import Path

import httpx
import pytest

from almanak.connectors.curve import (
    stable_math,
    tricrypto_liquidity,
    tricrypto_quotes,
    twocrypto_liquidity,
    twocrypto_quotes,
)
from almanak.connectors.curve.crypto_observation import CryptoObservation
from almanak.connectors.curve.domain import PoolObservation
from almanak.connectors.curve.reader import read_pool
from almanak.connectors.curve.tricrypto_observation import TriCryptoObservation
from tests.test_curve_reader import CapturedReader

DIRECTORY = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-arbitrum-discovery"
)


def capture(name):
    return json.loads(gzip.decompress((DIRECTORY / (name + ".json.gz")).read_bytes()))


CAPTURES = (
    (PoolObservation, capture("curve-arbitrum-stable-reader")),
    (CryptoObservation, capture("curve-arbitrum-two-quotes")),
    (TriCryptoObservation, capture("curve-arbitrum-tri-quotes")),
)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("captured Arbitrum evidence cannot access HTTP")

    monkeypatch.setattr(httpx.Client, "send", refuse)


@pytest.mark.parametrize("model,data", CAPTURES)
def test_registered_revisions_reproduce_the_captured_pool(model, data):
    observed = model.model_validate_json(json.dumps(data.get("observation", data.get("pool"))))
    with CapturedReader(observed.source, data["records"]) as reader:
        block = reader.pin_block(observed.block.number)
        assert block == observed.block
        assert read_pool(reader, observed.source, block) == observed


def stable_prediction(state, case):
    if case["kind"] == "swap":
        return stable_math.swap(state, case["first"], case["second"], case["amount"]).output
    if case["kind"] == "withdraw_one":
        return stable_math.removal_one(state, case["coin"], case["amount"]).output
    if case["kind"] == "virtual_price":
        return state.invariant(state.normalized) * 10**18 // state.total_supply
    return stable_math.addition(state, tuple(case["amounts"])).minted


def crypto_prediction(state, case, quotes, liquidity):
    if case["kind"] == "swap":
        return quotes.swap(state, case["i"], case["j"], case["amount"])
    if case["kind"] == "addition":
        return liquidity.addition(state, case["amounts"], contract_view=True)[0]
    return liquidity.single_coin(state, case["index"], case["shares"])[0]


@pytest.mark.parametrize(
    "index,case",
    [(i, c) for i, (_, d) in enumerate(CAPTURES) for c in d.get("comparisons", d.get("cases"))],
)
def test_archival_integer_quotes_match_the_family_model(index, case):
    model, data = CAPTURES[index]
    observed = model.model_validate_json(json.dumps(data.get("observation", data.get("pool"))))
    state = observed.math_state()
    if index == 0:
        result = stable_prediction(state, case)
    elif index == 1:
        result = crypto_prediction(state, case, twocrypto_quotes, twocrypto_liquidity)
    else:
        result = crypto_prediction(state, case, tricrypto_quotes, tricrypto_liquidity)
    assert result == case.get("expected", case.get("actual"))


def test_paris_compilation_preserves_verified_pool_storage():
    from almanak.connectors.curve.tricrypto_deployment import revision

    assert (
        revision("tricrypto-v2.0.0-paris.json")["pool_storage_layout"]
        == revision()["pool_storage_layout"]
    )
