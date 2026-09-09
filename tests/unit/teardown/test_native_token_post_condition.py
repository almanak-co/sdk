"""Native inventory reads cannot call ERC-20 balanceOf or certify gas as sold inventory."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.teardown.token_post_condition import token_balance_teardown_post_condition

WALLET = "0x" + "1" * 40
SENTINEL = "0x" + "e" * 40


def position(token=SENTINEL, chain="base"):
    return SimpleNamespace(protocol="uniswap_v4", position_id="held", chain=chain, details={"token": token})


@pytest.mark.parametrize(
    "chain,token",
    [("base", SENTINEL), ("base", "0x" + "0" * 40), ("polygon", "0x0000000000000000000000000000000000001010")],
)
def test_native_identity_reads_pinned_native_balance(chain, token):
    gateway = MagicMock()
    gateway.query_native_balance.return_value = 0
    result = token_balance_teardown_post_condition(position(token, chain), WALLET, gateway, block=1234)
    assert result.closed and not result.unmeasured
    gateway.query_native_balance.assert_called_once_with(chain=chain, wallet_address=WALLET, block=1234)
    gateway.query_erc20_balance.assert_not_called()


@pytest.mark.parametrize("balance", [None, "0x", -1, True, Decimal("0.2"), "nonsense"])
def test_invalid_native_read_cannot_become_measured_zero(balance):
    gateway = MagicMock()
    gateway.query_native_balance.return_value = balance
    result = token_balance_teardown_post_condition(position(), WALLET, gateway)
    assert result.unmeasured and not result.closed
    assert not result.residual


def test_native_gas_principal_is_not_position_closure_or_attributed_residual():
    gateway = MagicMock()
    gateway.query_native_balance.return_value = 100 * 10**18
    held = position()
    held.details.update(base_before="100", acquired="0", balance="0", baseline="100")
    result = token_balance_teardown_post_condition(held, WALLET, gateway)
    assert result.unmeasured and not result.closed
    assert not result.residual
    assert "transaction-bound" in result.error


@pytest.mark.parametrize(
    "chain,token",
    [("base", "0x4200000000000000000000000000000000000006"), ("base", "0x0000000000000000000000000000000000001010")],
)
def test_wrapped_native_and_other_chain_alias_remain_erc20(chain, token):
    gateway = MagicMock()
    gateway.query_erc20_balance.return_value = 11
    result = token_balance_teardown_post_condition(position(token, chain), WALLET, gateway, block=1234)
    assert not result.closed and not result.unmeasured
    assert result.residual["balance"] == "11"
    gateway.query_native_balance.assert_not_called()
    gateway.query_erc20_balance.assert_called_once_with(
        chain=chain, token_address=token, wallet_address=WALLET, block=1234
    )


def test_unregistered_chain_cannot_assign_native_identity():
    gateway = MagicMock()
    result = token_balance_teardown_post_condition(position(chain="unknown"), WALLET, gateway)
    assert result.unmeasured and not result.closed
    gateway.query_native_balance.assert_not_called()
    gateway.query_erc20_balance.assert_not_called()
