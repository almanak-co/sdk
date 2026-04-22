"""Tests for VaultPositionReader and the PortfolioValuer vault valuation hook."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.connectors.vaults import (
    register_vault_adapter,
)
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer
from almanak.framework.valuation.vault_position_reader import VaultPositionReader


# ---------------------------------------------------------------------------
# Stub adapter / SDK that satisfies the ERC-4626 surface used by the reader
# ---------------------------------------------------------------------------


class _StubSdk:
    """Minimal SDK that implements the methods VaultPositionReader calls."""

    def __init__(
        self,
        *,
        asset_address: str,
        shares_wei: int,
        asset_amount_wei: int,
        decimals: int,
    ) -> None:
        self._asset_address = asset_address
        self._shares_wei = shares_wei
        self._asset_amount_wei = asset_amount_wei
        self._decimals = decimals

    def get_balance_of(self, vault_address: str, user: str) -> int:  # noqa: ARG002
        return self._shares_wei

    def get_vault_asset(self, vault_address: str) -> str:  # noqa: ARG002
        return self._asset_address

    def convert_to_assets(self, vault_address: str, shares: int) -> int:  # noqa: ARG002
        # Honour the actual share count (covers the empty-position branch).
        if shares == 0:
            return 0
        ratio = self._asset_amount_wei / self._shares_wei if self._shares_wei else 0
        return int(shares * ratio)

    def get_decimals(self, vault_address: str) -> int:  # noqa: ARG002
        return self._decimals


class _StubAdapter:
    def __init__(self, sdk: _StubSdk) -> None:
        self.sdk = sdk


@pytest.fixture
def register_stub_adapter():
    """Register a stub vault protocol for the duration of one test."""
    name = "stubvault"
    sdk_holder: dict[str, _StubSdk] = {}

    def _factory(**_kwargs: Any) -> _StubAdapter:
        # Allow per-test mutation of the SDK via the holder
        return _StubAdapter(sdk_holder["sdk"])

    register_vault_adapter(name, _factory)
    yield name, sdk_holder

    from almanak.framework.connectors.vaults import _REGISTRY

    _REGISTRY.pop(name, None)


# ---------------------------------------------------------------------------
# VaultPositionReader
# ---------------------------------------------------------------------------


class TestVaultPositionReader:
    def test_returns_none_without_gateway(self):
        reader = VaultPositionReader(gateway_client=None)
        assert reader.read_position(
            protocol="metamorpho",
            chain="base",
            vault_address="0x" + "a" * 40,
            wallet_address="0x" + "b" * 40,
        ) is None

    def test_happy_path(self, register_stub_adapter):
        name, holder = register_stub_adapter
        holder["sdk"] = _StubSdk(
            asset_address="0x" + "c" * 40,
            shares_wei=2_000_000,
            asset_amount_wei=2_100_000,  # 1.05 PPFS appreciation
            decimals=6,
        )

        reader = VaultPositionReader(gateway_client=object())
        result = reader.read_position(
            protocol=name,
            chain="base",
            vault_address="0x" + "a" * 40,
            wallet_address="0x" + "b" * 40,
        )
        assert result is not None
        assert result.shares_wei == 2_000_000
        assert result.asset_amount_wei == 2_100_000
        assert result.asset_decimals == 6
        assert result.is_active

    def test_empty_position_returns_zero_struct(self, register_stub_adapter):
        name, holder = register_stub_adapter
        holder["sdk"] = _StubSdk(
            asset_address="0x" + "c" * 40,
            shares_wei=0,
            asset_amount_wei=0,
            decimals=6,
        )

        reader = VaultPositionReader(gateway_client=object())
        result = reader.read_position(
            protocol=name,
            chain="base",
            vault_address="0x" + "a" * 40,
            wallet_address="0x" + "b" * 40,
        )
        assert result is not None
        assert result.shares_wei == 0
        assert not result.is_active

    def test_unknown_protocol_returns_none(self):
        reader = VaultPositionReader(gateway_client=object())
        result = reader.read_position(
            protocol="totally_unregistered",
            chain="base",
            vault_address="0x" + "a" * 40,
            wallet_address="0x" + "b" * 40,
        )
        assert result is None


# ---------------------------------------------------------------------------
# PortfolioValuer routing
# ---------------------------------------------------------------------------


class _StaticMarket:
    """Minimal MarketDataSource for valuer tests."""

    def __init__(self, prices: dict[str, Decimal]) -> None:
        self._prices = prices

    def price(self, token: str, quote: str = "USD") -> Decimal:  # noqa: ARG002
        return self._prices[token]

    def balance(self, token: str) -> Decimal:  # noqa: ARG002
        return Decimal("0")


class TestPortfolioValuerVaultRouting:
    def test_vault_route_uses_on_chain_assets_x_price(self, register_stub_adapter, monkeypatch):
        name, holder = register_stub_adapter
        # 2.1 USDC underlying (6 decimals, 2_100_000 wei) at $1 = $2.10
        holder["sdk"] = _StubSdk(
            asset_address="0x" + "c" * 40,
            shares_wei=2_000_000,
            asset_amount_wei=2_100_000,
            decimals=6,
        )

        valuer = PortfolioValuer(gateway_client=object())
        # The reader needs the gateway_client wired through set_gateway_client
        # since fixture order means __init__ ran with object() — confirm it.
        assert valuer._vault_reader._gateway is not None

        # Patch the symbol resolver so we don't need a real TokenResolver.
        monkeypatch.setattr(
            valuer,
            "_resolve_token_symbol",
            lambda *args, **kwargs: "USDC",
        )

        position = PositionInfo(
            position_type=PositionType.VAULT,
            position_id="pos-vault-1",
            chain="base",
            protocol=name,
            value_usd=Decimal("0"),  # stale strategy-reported value (silent zero today)
            details={
                "vault_address": "0x" + "a" * 40,
                "wallet_address": "0x" + "b" * 40,
                "asset": "USDC",
            },
        )
        market = _StaticMarket({"USDC": Decimal("1.00")})

        value = valuer._reprice_position(position, "base", market)
        assert value == Decimal("2.10")

    def test_vault_falls_back_to_strategy_value_on_failure(self):
        valuer = PortfolioValuer(gateway_client=None)  # no gateway → reader returns None
        position = PositionInfo(
            position_type=PositionType.VAULT,
            position_id="pos-vault-2",
            chain="base",
            protocol="metamorpho",
            value_usd=Decimal("123.45"),
            details={
                "vault_address": "0x" + "a" * 40,
                "wallet_address": "0x" + "b" * 40,
            },
        )
        market = _StaticMarket({})
        value = valuer._reprice_position(position, "base", market)
        assert value == Decimal("123.45")
