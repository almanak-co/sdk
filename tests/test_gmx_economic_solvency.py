"""Market balances must cover pool accounting, collateral and funding independently."""

import pytest

from almanak.caip import AssetType
from almanak.connectors.gmx_v2.economic_solvency import TokenSolvency, validate_token_transfer


def token(**changes):
    values = dict(
        asset=AssetType("eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1"),
        balance=100,
        minimum_balance=70,
        collateral_amount=80,
        claimable_funding_amount=60,
    )
    return TokenSolvency(**(values | changes))


def test_withdrawal_preserves_independent_collateral_floor():
    validate_token_transfer(token(), -20)
    with pytest.raises(ValueError, match="physical market token balance"):
        validate_token_transfer(token(), -21)


def test_funding_floor_does_not_get_subtracted_with_pool_inventory():
    state = token(collateral_amount=0, claimable_funding_amount=95)
    validate_token_transfer(state, -5)
    with pytest.raises(ValueError, match="physical market token balance"):
        validate_token_transfer(state, -6)


def test_fees_and_pool_liabilities_move_with_the_exact_transfer():
    state = token(minimum_balance=100, collateral_amount=0, claimable_funding_amount=0)
    validate_token_transfer(state, 500)
    validate_token_transfer(state, -100)
    with pytest.raises(ValueError, match="physical market token balance"):
        validate_token_transfer(state, -101)
    with pytest.raises(ValueError, match="physical market token balance"):
        validate_token_transfer(token(minimum_balance=101), 500)


def test_incoming_amount_can_restore_an_independent_collateral_floor():
    state = token(collateral_amount=110)
    with pytest.raises(ValueError, match="physical market token balance"):
        validate_token_transfer(state, 9)
    validate_token_transfer(state, 10)
