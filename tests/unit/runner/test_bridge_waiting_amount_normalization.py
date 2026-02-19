"""Tests for bridge waiting amount normalization in StrategyRunner."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.runner.strategy_runner import StrategyRunner


class _MockResolver:
    def __init__(self, resolved_by: dict[str, SimpleNamespace]):
        self._resolved_by = resolved_by

    def resolve(self, token: str, chain: str) -> SimpleNamespace:
        key = f"{token.lower()}@{chain.lower()}"
        if key not in self._resolved_by:
            raise ValueError("not found")
        return self._resolved_by[key]


def test_normalization_prefers_token_address_from_bridge_status(monkeypatch: pytest.MonkeyPatch) -> None:
    address = "0x9999999999999999999999999999999999999999"
    resolver = _MockResolver(
        {
            f"{address.lower()}@arbitrum": SimpleNamespace(decimals=8, address=address),
        }
    )

    import almanak.framework.data.tokens as tokens_module

    monkeypatch.setattr(tokens_module, "get_token_resolver", lambda: resolver)

    amount, metadata = StrategyRunner._normalize_bridge_balance_increase(
        balance_increase_wei="123456789",
        destination_chain="arbitrum",
        token_symbol="USDC",
        bridge_status={"destination_token_address": address},
    )

    assert amount == Decimal("1.23456789")
    assert metadata["decimals"] == 8
    assert metadata["resolved_from"] == address


def test_normalization_falls_back_to_symbol_when_no_address(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _MockResolver(
        {
            "usdc@base": SimpleNamespace(decimals=6, address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
        }
    )

    import almanak.framework.data.tokens as tokens_module

    monkeypatch.setattr(tokens_module, "get_token_resolver", lambda: resolver)

    amount, metadata = StrategyRunner._normalize_bridge_balance_increase(
        balance_increase_wei=1500000,
        destination_chain="base",
        token_symbol="USDC",
        bridge_status={},
    )

    assert amount == Decimal("1.5")
    assert metadata["decimals"] == 6
    assert metadata["resolved_from"] == "USDC"


def test_normalization_returns_none_with_raw_wei_when_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _MockResolver({})

    import almanak.framework.data.tokens as tokens_module
    from almanak.framework.data.tokens.exceptions import TokenNotFoundError

    monkeypatch.setattr(tokens_module, "get_token_resolver", lambda: resolver)

    with pytest.raises(TokenNotFoundError):
        StrategyRunner._normalize_bridge_balance_increase(
            balance_increase_wei=10**18,
            destination_chain="optimism",
            token_symbol="UNKNOWN",
            bridge_status={"token_address": "0xabc"},
        )
