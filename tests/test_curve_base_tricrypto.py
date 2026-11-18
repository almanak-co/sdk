"""The configured Base revision reproduces archived observations and integer quotes."""

import gzip
import json
from pathlib import Path

import httpx
import pytest

from almanak.connectors.curve.tricrypto_liquidity import addition, single_coin
from almanak.connectors.curve.tricrypto_observation import TriCryptoObservation
from almanak.connectors.curve.tricrypto_quotes import swap
from almanak.connectors.curve.tricrypto_reader import read_pool
from tests.test_curve_reader import CapturedReader

DIRECTORY = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-base-tricrypto-discovery"
)
CAPTURE = json.loads(gzip.decompress((DIRECTORY / "curve-base-tri-quotes.json.gz").read_bytes()))


@pytest.fixture
def observed(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("retained Base quotes cannot access HTTP")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    return TriCryptoObservation.model_validate_json(json.dumps(CAPTURE["pool"]))


def test_base_l2_factory_math_and_views_replay_without_provider(observed):
    with CapturedReader(observed.source, CAPTURE["records"]) as reader:
        block = reader.pin_block(observed.block.number)
        assert block == observed.block
        assert read_pool(reader, observed.source, block) == observed


def test_preparation_derives_the_same_verified_historical_dependencies(observed):
    from almanak.connectors.curve.economic_prepare import tri_dependencies

    with CapturedReader(observed.source, CAPTURE["records"]) as reader:
        block = reader.pin_block(observed.block.number)
        assert tri_dependencies(reader, observed.source, block) == (observed.math, observed.views)


@pytest.mark.parametrize("case", CAPTURE["cases"])
def test_base_model_equals_archival_contract_integer_quotes(observed, case):
    pool = observed.math_state()
    if case["kind"] == "swap":
        predicted = swap(pool, case["i"], case["j"], case["amount"])
    elif case["kind"] == "addition":
        predicted = addition(pool, case["amounts"], contract_view=True)[0]
    else:
        predicted = single_coin(pool, case["index"], case["shares"])[0]
    assert predicted == case["expected"]


def test_compiler_variants_preserve_the_shared_pool_storage_layout():
    from almanak.connectors.curve.tricrypto_deployment import revision

    assert (
        revision()["pool_storage_layout"]
        == revision("tricrypto-v2.0.0-l2.json")["pool_storage_layout"]
    )
