"""TwoCrypto LP quotes are checked separately from hypothetical accounting."""

import gzip
import json
from dataclasses import replace
from pathlib import Path

import pytest

from almanak.connectors.curve.twocrypto_liquidity import (
    AdminState,
    LiquidityState,
    addition,
    admin_claim,
    proportional,
    single_coin,
    xcp,
)

EVIDENCE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-crypto-discovery"
)
CAPTURE = json.loads(
    gzip.decompress((EVIDENCE / "curve-two-lp-view-comparison.json.gz").read_bytes())
)
TRANSACTIONS = json.loads(
    gzip.decompress((EVIDENCE / "curve-two-withdraw-comparison.json.gz").read_bytes())
)


@pytest.fixture
def state():
    pool = CAPTURE["pool"]
    return LiquidityState(
        tuple(pool["balances"]),
        tuple(pool["precisions()"]),
        pool["price_scale"][0],
        pool["A()"],
        pool["gamma()"],
        pool["D()"],
        pool["packed_fee_params()"],
        pool["totalSupply()"],
    )


@pytest.mark.parametrize("case", CAPTURE["cases"])
def test_public_pool_lp_quotes_match_exactly(state, case):
    if case["kind"] == "addition_view":
        result = addition(state, case["amounts"], contract_view=True)[0]
    else:
        result = single_coin(state, case["index"], case["shares"])[0]
    assert result == case["expected"]


@pytest.mark.parametrize("case", TRANSACTIONS["cases"])
def test_read_only_transaction_payouts_with_explicit_synthetic_lp_ownership(state, case):
    if case["kind"] == "proportional_transaction_call":
        result = list(proportional(state, case["shares"]))
    else:
        admin = AdminState(**TRANSACTIONS["admin"])
        timestamp = TRANSACTIONS["block"]["timestamp"]
        if case["defer_claim"]:
            admin = replace(admin, last_claim_time=timestamp)
        changed, debits = admin_claim(
            state,
            admin,
            timestamp=timestamp,
            ramp_end=TRANSACTIONS["pool"]["future_A_gamma_time()"],
        )
        assert list(debits) == case["admin_amounts"]
        result = single_coin(changed, case["index"], case["shares"])[0]
    assert result == case["expected"]


def test_proportional_burn_preserves_contract_one_share_rounding(state):
    shares = state.total_supply // 3
    for balance, output in zip(state.balances, proportional(state, shares), strict=True):
        assert (
            output * state.total_supply
            <= balance * (shares - 1)
            < (output + 1) * state.total_supply
        )
    assert proportional(state, state.total_supply) == state.balances
    assert proportional(state, 1) == (0, 0)


def test_admin_claim_timing_ramps_and_profit_are_independent_guards(state):
    admin = AdminState(10**18, 10**18 + 10000, 10**18, 0, 100, True)
    assert admin_claim(state, admin, timestamp=101, ramp_end=0) == (state, (0, 0))
    assert admin_claim(state, admin, timestamp=90000, ramp_end=90001) == (state, (0, 0))
    assert admin_claim(state, replace(admin, profit=10**18), timestamp=90000, ramp_end=0) == (
        state,
        (0, 0),
    )
    with pytest.raises(ValueError, match="future"):
        admin_claim(state, admin, timestamp=99, ramp_end=0)


def test_admin_claim_preserves_user_share_count_and_debits_pool_reserves(state):
    virtual_price = 10**18 * xcp(state.invariant, state.price_scale) // state.total_supply
    admin = AdminState(virtual_price, 10**18 + 10**15, 10**18, 0, 0, True)
    changed, amounts = admin_claim(state, admin, timestamp=90000, ramp_end=0)
    assert changed.total_supply == state.total_supply
    assert all(amount > 0 for amount in amounts)
    assert (
        tuple(value + debit for value, debit in zip(changed.balances, amounts, strict=True))
        == state.balances
    )
    assert changed.invariant < state.invariant


@pytest.mark.parametrize("shares", [0, -1, 2**256])
def test_invalid_share_quantities_refuse_payouts(state, shares):
    with pytest.raises(ValueError):
        proportional(state, shares)


def test_empty_or_negative_addition_and_foreign_coin_refuse(state):
    for amounts in ((0, 0), (1,), (-1, 1000)):
        with pytest.raises(ValueError):
            addition(state, amounts)
    with pytest.raises(ValueError, match="coin index"):
        single_coin(state, 2, state.total_supply // 100)
