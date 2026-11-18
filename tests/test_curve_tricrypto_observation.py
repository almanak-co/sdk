"""TriCrypto identity and custody capture reproduces without provider access."""

import json

import httpx
import pytest

from almanak.connectors.curve.tricrypto_deployment import verify_pool
from almanak.connectors.curve.tricrypto_observation import TriCryptoObservation, TriCryptoRamp
from almanak.connectors.curve.tricrypto_reader import read_pool
from almanak.gateway.ethereum import DataError
from tests.test_curve_reader import CapturedReader
from tests.test_curve_tricrypto_transitions import captured


@pytest.fixture
def observation(monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("retained TriCrypto state cannot access HTTP")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    original = TriCryptoObservation.model_validate_json(
        json.dumps(captured("curve-tricrypto-observation.json"))
    )
    with CapturedReader(
        original.source, captured("curve-tricrypto-observation-capture.json")
    ) as reader:
        block = reader.pin_block(original.block.number)
        yield original, reader, block


def test_identity_prices_and_actual_custody_survive_reproduction(observation):
    original, reader, block = observation
    result = read_pool(reader, original.source, block)
    assert result == original
    assert TriCryptoObservation.model_validate_json(result.model_dump_json()) == result
    assert result.ramp_invariant(timestamp=block.timestamp) > 0
    assert result.accounting.math_state().receiver_enabled
    assert result.actual_balances == result.balances
    assert result.cached_price_oracle != result.price_oracle
    assert len(verify_pool(reader, original.source, block).identities) == 4


@pytest.mark.parametrize(
    "changes",
    [
        {"precisions": [10, 1, 1]},
        {"price_oracle": [1, 1]},
        {"packed_rebalancing": 0},
        {"math": "0x" + "00" * 20},
        {"last_timestamp": 2**63},
        {"actual_balances": [1, 1]},
    ],
)
def test_incomplete_or_inconsistent_state_is_refused(observation, changes):
    original, _, _ = observation
    data = original.model_dump(mode="json")
    data.update(changes)
    with pytest.raises(ValueError):
        TriCryptoObservation.model_validate_json(json.dumps(data))


def test_identity_and_units_are_refused(observation):
    original, reader, block = observation
    raw = original.model_dump(mode="json")
    for change in (
        {"source": dict(raw["source"], family="stableswap_ng")},
        {"currencies": raw["currencies"][::-1]},
        {"shares": dict(raw["shares"], decimals=17)},
        {"block": dict(raw["block"], chain={"chain_id": "eip155:1", "environment": "other"})},
    ):
        with pytest.raises(ValueError):
            TriCryptoObservation.model_validate_json(json.dumps(dict(raw, **change)))
    with pytest.raises(DataError, match="revision"):
        verify_pool(reader, original.source.model_copy(update={"family": "stableswap_ng"}), block)


def test_ramps_validate_domain_times_and_decreasing_rounding():
    raw = dict(
        initial_amplification=2701,
        initial_gamma=10000000001,
        future_amplification=2700,
        future_gamma=10000000000,
        initial_time=10,
        future_time=20,
    )
    ramp = TriCryptoRamp.model_validate(raw)
    assert ramp.at(15) == (2700, 10000000000)
    assert ramp.at(20) == (2700, 10000000000)
    with pytest.raises(ValueError, match="future ramp"):
        ramp.at(9)
    with pytest.raises(ValueError, match="reversed"):
        TriCryptoRamp.model_validate(dict(raw, future_time=9))
    with pytest.raises(ValueError, match="domain"):
        TriCryptoRamp.model_validate(dict(raw, initial_amplification=1))
