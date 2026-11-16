"""Pinned Crypto observations and ramp calls remain reproducible without HTTP."""

import json

import httpx
import pytest

from almanak.connectors.curve.crypto_observation import CryptoObservation, CryptoRamp
from almanak.connectors.curve.crypto_reader import read_pool
from almanak.connectors.curve.domain import CurvePool
from almanak.connectors.curve.twocrypto_quotes import swap
from almanak.gateway.ethereum import DataError
from tests.test_curve_crypto_revisions import captured
from tests.test_curve_reader import CapturedReader, request_key


@pytest.fixture
def observation(monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("captured Crypto state cannot access HTTP")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    data = captured("curve-two-observation-capture.json")
    source = CurvePool.model_validate_json(json.dumps(data["source"]))
    with CapturedReader(source, data["records"]) as reader:
        block = reader.pin_block(data["observation"]["block"]["number"])
        yield data, source, reader, block


def test_stored_prices_profit_and_contract_identities_survive_json(observation):
    data, source, reader, block = observation
    result = read_pool(reader, source, block)
    assert result == CryptoObservation.model_validate_json(json.dumps(data["observation"]))
    assert CryptoObservation.model_validate_json(result.model_dump_json()) == result
    assert result.cached_price_oracle != result.price_oracle
    assert result.accounting.math_state().receiver_enabled
    assert result.ramp_invariant(timestamp=block.timestamp) > 0
    assert result.accounting.virtual_shares > 0
    quotes = captured("curve-two-math-comparison.json")["cases"]
    for row in quotes:
        if row["kind"] == "swap":
            assert swap(result.math_state(), row["i"], row["j"], row["dx"]) == row["expected"]


def test_captured_contract_ramps_include_decreasing_weighted_sum_rounding(observation):
    data, _, _, _ = observation
    assert len(data["ramp_cases"]) == 8
    for case in data["ramp_cases"]:
        ramp = CryptoRamp.model_validate(case["ramp"])
        assert ramp.at(case["timestamp"]) == tuple(case["actual"])
        with pytest.raises(ValueError, match="future ramp"):
            ramp.at(ramp.initial_time - 1)


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("precisions", [10, 1], "precisions"),
        ("packed_rebalancing", 0, "duration"),
        ("math", "0x" + "00" * 20, "executable"),
        ("last_timestamp", 2**63, "future"),
    ],
)
def test_invalid_observation_cannot_become_prepared_state(observation, field, value, message):
    data, _, _, _ = observation
    raw = dict(data["observation"], **{field: value})
    with pytest.raises(ValueError, match=message):
        CryptoObservation.model_validate_json(json.dumps(raw))


def test_wrong_family_order_environment_and_units_are_refused(observation):
    data, _, _, _ = observation
    original = data["observation"]
    for change, message in (
        ({"source": dict(original["source"], family="stableswap_ng")}, "family"),
        ({"currencies": original["currencies"][::-1]}, "coin order"),
        (
            {
                "block": dict(
                    original["block"], chain={"chain_id": "eip155:1", "environment": "fork"}
                )
            },
            "scope",
        ),
        ({"shares": dict(original["shares"], decimals=17)}, "decimals"),
    ):
        with pytest.raises(ValueError, match=message):
            CryptoObservation.model_validate_json(json.dumps(dict(original, **change)))


def test_ramp_mismatch_is_refused_at_the_reader_boundary(observation):
    _, source, reader, block = observation
    from eth_utils import keccak

    key = request_key(
        "eth_call",
        [
            {"to": source.pool, "data": "0x" + keccak(text="A()")[:4].hex()},
            {"blockHash": block.hash, "requireCanonical": True},
        ],
    )
    reader.records[key] = "0x" + (4000).to_bytes(32, "big").hex()
    with pytest.raises(DataError, match="retained ramp"):
        read_pool(reader, source, block)


def test_ramp_requires_ordered_times_and_revision_parameters():
    raw = dict(
        initial_amplification=4000,
        initial_gamma=10**10,
        future_amplification=4000,
        future_gamma=10**10,
        initial_time=2,
        future_time=1,
    )
    with pytest.raises(ValueError, match="reversed"):
        CryptoRamp.model_validate(raw)
    with pytest.raises(ValueError, match="domain"):
        CryptoRamp.model_validate(dict(raw, initial_amplification=1))
