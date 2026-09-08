"""Regression tests for Safe intent-test outer gas sizing."""

from unittest.mock import Mock

import pytest
from web3.exceptions import ContractLogicError

from tests.intents._permission_onchain_harness import (
    _estimate_zodiac_outer_gas,
    _inner_gas_hint,
    _zodiac_outer_gas,
)



def test_action_bundle_gas_estimate_is_preserved_with_wrapper_headroom() -> None:
    """A large inner hint must not become the complete Roles tx limit."""
    inner = _inner_gas_hint({"gas_estimate": 1_500_000})

    assert inner == 1_500_000
    assert _zodiac_outer_gas(inner) == 2_000_000


def test_wrapper_minimum_still_covers_transactions_without_a_hint() -> None:
    assert _inner_gas_hint({"to": "0x0"}) is None
    assert _zodiac_outer_gas(None) == 1_500_000




def _web3(estimate=2_000_000, block_limit=30_000_000):
    web3 = Mock()
    web3.eth.get_block.return_value = {"gasLimit": block_limit}
    web3.eth.estimate_gas.return_value = estimate
    return web3


def test_estimates_wrapped_transaction_without_inner_gas_cap():
    web3 = _web3()
    tx = {"to": "roles", "from": "member", "data": "wrapped_swap", "value": 0, "gas": 1_500_000}
    assert _estimate_zodiac_outer_gas(web3, tx, 200_000) == 2_400_000
    web3.eth.estimate_gas.assert_called_once_with({k: v for k, v in tx.items() if k != "gas"})
    assert tx["gas"] == 1_500_000


def test_buffer_is_capped_by_block_limit():
    assert _estimate_zodiac_outer_gas(_web3(29_000_000), {}, None) == 30_000_000


def test_revert_keeps_mined_negative_test_evidence():
    web3 = _web3()
    web3.eth.estimate_gas.side_effect = ContractLogicError("execution reverted")
    assert _estimate_zodiac_outer_gas(web3, {}, None) == 1_500_000


def test_rpc_failure_is_not_hidden_by_static_fallback():
    web3 = _web3()
    web3.eth.estimate_gas.side_effect = TimeoutError("RPC unavailable")
    with pytest.raises(TimeoutError, match="RPC unavailable"):
        _estimate_zodiac_outer_gas(web3, {}, None)


@pytest.mark.parametrize("estimate", [0, -1, 30_000_001])
def test_invalid_estimate_fails(estimate):
    with pytest.raises(ValueError, match="Invalid Roles gas estimate"):
        _estimate_zodiac_outer_gas(_web3(estimate), {}, None)
