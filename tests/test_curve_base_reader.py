"""Base captures distinguish contract revisions, oracle pools and integer rounding."""

import gzip
import json
from pathlib import Path

import httpx
import pytest

from almanak.connectors.curve.deployment import revisions
from almanak.connectors.curve.domain import CurvePool, PoolObservation
from almanak.connectors.curve.reader import quote, read_pool
from almanak.connectors.curve.stable_math import DECEMBER, NOVEMBER, invariant, removal_one, swap
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.test_curve_reader import CapturedReader, request_key

EVIDENCE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-base-discovery"
)


def capture(name):
    return json.loads(gzip.decompress((EVIDENCE / (name + ".json.gz")).read_bytes()))


@pytest.fixture
def captured(monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("Base Curve evidence must reproduce without HTTP")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    data = capture("curve-base-stable-reader")
    source = CurvePool.model_validate_json(json.dumps(data["source"]))
    with CapturedReader(source, data["records"]) as reader:
        block = reader.pin_block(data["observation"]["block"]["number"])
        yield data, source, reader, block


def test_base_pool_retains_its_exact_deployed_revision_and_ordered_assets(captured):
    data, source, reader, block = captured
    observed = read_pool(reader, source, block)
    assert observed == PoolObservation.model_validate_json(json.dumps(data["observation"]))
    assert observed.revision == observed.math_state().revision == DECEMBER
    assert observed.asset_types == (0, 0)
    assert observed.math_state().invariant(observed.math_state().normalized) > 0


@pytest.mark.parametrize("index", range(25))
def test_base_integer_model_matches_archived_pool_quotes(captured, index):
    data, source, reader, block = captured
    case = data["comparisons"][index]
    state = read_pool(reader, source, block).math_state()
    if case["kind"] == "swap":
        output = swap(state, case["first"], case["second"], case["amount"]).output
        actual = quote(
            reader,
            source,
            block,
            AssetAmount(asset=source.coins[case["first"]], amount=case["amount"]),
            source.coins[case["second"]],
        )
    elif case["kind"] == "withdraw_one":
        output = removal_one(state, case["coin"], case["amount"]).output
        actual = reader.call(
            source.pool,
            "calc_withdraw_one_coin(uint256,int128)",
            block,
            inputs=("uint256", "int128"),
            args=(case["amount"], case["coin"]),
        )[0]
    else:
        output = state.invariant(state.normalized) * 10**18 // state.total_supply
        actual = reader.call(source.pool, "get_virtual_price()", block)[0]
    assert output == actual == case["actual"] == case["modeled"]


@pytest.mark.parametrize("case", capture("curve-base-stable-math")["cases"])
def test_division_order_matches_deployed_math_and_preserves_november_results(case):
    assert invariant(case["values"], case["amplification"], revision=DECEMBER) == case["actual"]
    assert invariant(case["values"], case["amplification"], revision=NOVEMBER) == case["november"]
    assert case["november"] != case["actual"]
    with pytest.raises(ValueError, match="unsupported StableSwap integer revision"):
        invariant(case["values"], case["amplification"], revision="unknown")


def replace_immutable(reader, source, block, name, value):
    key = request_key(
        "eth_getCode", [source.pool, {"blockHash": block.hash, "requireCanonical": True}]
    )
    code = bytearray.fromhex(reader.records[key][2:])
    for reference in revisions()[DECEMBER]["pool"]["immutables"][name]:
        start, size = reference["start"], reference["length"]
        code[start : start + size] = value.to_bytes(size, "big")
    reader.records[key] = "0x" + code.hex()


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("CACHED_CHAIN_ID", 1, "deployment chain"),
        ("pool_contains_rebasing_tokens", 1, "rebasing flag"),
    ],
)
def test_normalized_constructor_fields_still_require_semantic_validation(
    captured, name, value, message
):
    _, source, reader, block = captured
    replace_immutable(reader, source, block, name, value)
    with pytest.raises(DataError, match=message):
        read_pool(reader, source, block)


def test_rate_oracle_pool_cannot_be_misrepresented_as_standard_coins(captured):
    data = capture("curve-base-reader-probe")
    source = CurvePool.model_validate_json(json.dumps(data["pools"][0]["source"]))
    with CapturedReader(source, data["records"]) as reader:
        block = reader.pin_block(data["block"]["number"])
        with pytest.raises(DataError, match="standard non-rebasing coins"):
            read_pool(reader, source, block)
