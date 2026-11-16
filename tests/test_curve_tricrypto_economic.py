"""Prepared TriCrypto actions preserve exact native math and declared share overlay."""

import json
from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import UnixTime
from almanak.connectors.curve import crypto_history, economic_pool
from almanak.connectors.curve.economic_data import unit
from almanak.connectors.curve.tricrypto_observation import TriCryptoObservation
from tests.test_curve_tricrypto_transitions import CASES


@pytest.mark.parametrize("case", CASES)
def test_prepared_transition_at_typed_time_matches_native_fund_effect(case):
    pool = TriCryptoObservation.model_validate_json(json.dumps(case["observation"]))
    state = economic_pool.virtual_pool(
        pool, SimpleNamespace(position=None), UnixTime(case["timestamp"])
    )
    args = case.get("inputs", case)
    if case["mode"] == "exchange":
        output = economic_pool.swap(
            state, args["input_index"], args["output_index"], args["amount"]
        )
        assert output == case["predicted"]["amounts"][args["output_index"]]
    elif case["mode"].startswith("add"):
        assert economic_pool.minted(state, args["amounts"]) == case["actual_lp_change"]
    else:
        index = None if case["mode"] == "proportional" else args["index"]
        assert economic_pool.removed(state, args["shares"], index) == tuple(
            case["predicted"]["amounts"]
        )


def test_hypothetical_lp_scales_custody_and_stored_balances_together():
    case = next(c for c in CASES if c.get("tuning") == "native_donation")
    pool = TriCryptoObservation.model_validate_json(json.dumps(case["observation"]))
    shares = pool.total_supply // 10
    owned = SimpleNamespace(position=SimpleNamespace(shares=SimpleNamespace(amount=shares)))
    state = economic_pool.virtual_pool(pool, owned, UnixTime(case["timestamp"]))
    assert state.pool.block == pool.block
    assert state.pool.price_scales == pool.price_scales
    assert state.pool.accounting == pool.accounting
    assert state.pool.total_supply == pool.total_supply + shares
    assert state.pool.invariant == pool.invariant + pool.invariant * shares // pool.total_supply
    for field in ("balances", "actual_balances"):
        values = getattr(pool, field)
        assert getattr(state.pool, field) == tuple(
            value + value * shares // pool.total_supply for value in values
        )
    assert unit(pool.source) == "curve_tricrypto_integer_pool"


def test_prepared_parameter_path_refuses_unrecorded_changes():
    pool = TriCryptoObservation.model_validate_json(json.dumps(CASES[0]["observation"]))
    crypto_history.validate_parameters(pool, pool, ())
    with pytest.raises(ValueError, match="complete retained event"):
        crypto_history.validate_parameters(
            pool, pool.model_copy(update={"packed_fees": pool.packed_fees + 1}), ()
        )
