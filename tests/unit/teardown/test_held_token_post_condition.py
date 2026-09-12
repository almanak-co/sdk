"""Held-token closure reads follow the actual fixture and registry declarations."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.teardown.registry_enumeration import _position_info_from_pendle_registry_row
from almanak.framework.teardown.token_post_condition import token_balance_teardown_post_condition
from strategies.accounting.pendle_pt.strategy import AccountingQuantPendlePtStrategy

PT = "PT-stETH-30DEC2027"
PT_ADDRESS = "0xb253Eff1104802b97aC7E3aC9FdD73AecE295a2c"
UNDERLYING = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"
WALLET = "0x" + "1" * 40


@pytest.fixture
def resolver(monkeypatch):
    def resolve(token, chain):
        assert chain == "ethereum"
        addresses = {PT.upper(): PT_ADDRESS, "WSTETH": UNDERLYING}
        return SimpleNamespace(address=addresses[token.upper()])

    resolver = MagicMock()
    resolver.resolve.side_effect = resolve
    monkeypatch.setattr("almanak.framework.data.tokens.get_token_resolver", lambda: resolver)
    return resolver


def fixture_position():
    strategy = AccountingQuantPendlePtStrategy(
        config={"pt_token": PT, "base_token": "wstETH"}, chain="ethereum", wallet_address=WALLET
    )
    strategy._phase = "OPEN"
    return strategy.get_open_positions().positions[0]


def registry_position():
    return _position_info_from_pendle_registry_row(
        {
            "chain": "ethereum",
            "primitive": "swap",
            "payload": {"kind": "pt", "market_id": PT.lower(), "protocol": "pendle", "pt_symbol": PT.lower()},
        }
    )


@pytest.mark.parametrize("producer", [fixture_position, registry_position])
@pytest.mark.parametrize("pt_balance", [0, 11, 10**18])
def test_real_producers_verify_pt_not_retained_underlying(producer, pt_balance, resolver):
    position = producer()
    gateway = MagicMock()
    gateway.query_erc20_balance.side_effect = lambda **kwargs: (
        pt_balance if kwargs["token_address"] == PT_ADDRESS else 20 * 10**18
    )
    result = token_balance_teardown_post_condition(position, WALLET, gateway, block=1234)
    assert result.closed is (pt_balance == 0)
    assert not result.unmeasured
    if pt_balance:
        assert result.residual == {"token": PT_ADDRESS, "balance": str(pt_balance)}
    gateway.query_erc20_balance.assert_called_once_with(
        chain="ethereum", token_address=PT_ADDRESS, wallet_address=WALLET, block=1234
    )
    gateway.query_native_balance.assert_not_called()


@pytest.mark.parametrize("key", ["token_address", "asset", "asset_symbol", "token", "pt_token", "pt_symbol"])
@pytest.mark.parametrize("value", ["PT-unknown-maturity", "", None])
def test_unresolved_held_identity_never_borrows_underlying_or_position_id(key, value, resolver):
    position = SimpleNamespace(
        protocol="pendle",
        chain="ethereum",
        position_id=UNDERLYING,
        details={key: value, "base_token": "wstETH"},
    )
    gateway = MagicMock()
    result = token_balance_teardown_post_condition(position, WALLET, gateway)
    assert result.unmeasured and not result.closed
    gateway.query_erc20_balance.assert_not_called()
    gateway.query_native_balance.assert_not_called()
    assert all(call.args[0] != "wstETH" for call in resolver.resolve.call_args_list)


@pytest.mark.parametrize("details,position_id", [({"base_token": "wstETH"}, "legacy"), ({}, UNDERLYING)])
def test_legacy_base_only_and_address_identity_remain_measurable(details, position_id, resolver):
    position = SimpleNamespace(protocol="test", chain="ethereum", position_id=position_id, details=details)
    gateway = MagicMock()
    gateway.query_erc20_balance.return_value = 0
    result = token_balance_teardown_post_condition(position, WALLET, gateway)
    assert result.closed and not result.unmeasured
    assert gateway.query_erc20_balance.call_args.kwargs["token_address"] == UNDERLYING


def test_explicit_canonical_address_precedes_display_alias(resolver):
    position = fixture_position()
    position.details["token_address"] = PT_ADDRESS
    position.details["asset_symbol"] = "unknown-display-label"
    gateway = MagicMock()
    gateway.query_erc20_balance.return_value = 0
    result = token_balance_teardown_post_condition(position, WALLET, gateway)
    assert result.closed and not result.unmeasured
    resolver.resolve.assert_not_called()
    assert gateway.query_erc20_balance.call_args.kwargs["token_address"] == PT_ADDRESS


def test_pool_key_currency_in_address_is_not_read_as_the_held_token(resolver):
    """A V4 pool-key currency is not a held-token declaration.

    ``details["address"]`` carries the pool-key currency at real TOKEN producers,
    which is the zero address on a native pool. Reading it as the held token
    would measure the wallet's gas balance and report it as position residual.
    """
    position = SimpleNamespace(
        protocol="uniswap_v4",
        chain="ethereum",
        position_id="v4_roundtrip_wsteth",
        details={"address": "0x" + "0" * 40, "base_token": "wstETH"},
    )
    gateway = MagicMock()
    gateway.query_erc20_balance.return_value = 0
    result = token_balance_teardown_post_condition(position, WALLET, gateway)
    assert result.closed and not result.unmeasured
    assert gateway.query_erc20_balance.call_args.kwargs["token_address"] == UNDERLYING
    gateway.query_native_balance.assert_not_called()
