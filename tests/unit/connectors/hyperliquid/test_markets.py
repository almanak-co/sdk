"""Unit tests for Hyperliquid market resolution."""

from __future__ import annotations

import pytest

from almanak.connectors.hyperliquid.markets import PerpMarket, normalize_symbol, resolve_market


class TestNormalize:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("BTC", "BTC"),
            ("btc", "BTC"),
            ("BTC-USD", "BTC"),
            ("BTC/USD", "BTC"),
            ("eth-perp", "ETH"),
            ("SOL/USDC", "SOL"),
        ],
    )
    def test_normalize(self, raw: str, expected: str) -> None:
        assert normalize_symbol(raw) == expected

    def test_empty_rejected(self) -> None:
        with pytest.raises(ValueError):
            normalize_symbol("  ")


class TestResolve:
    def test_btc_resolves_to_index_0(self) -> None:
        m = resolve_market("BTC")
        assert (m.asset_index, m.sz_decimals) == (0, 5)

    def test_sol_is_index_5_not_2(self) -> None:
        # Guards the exact bug V1 shipped: SOL hardcoded as 2 (which is ATOM).
        assert resolve_market("SOL").asset_index == 5
        assert resolve_market("ATOM").asset_index == 2

    def test_suffix_forms_resolve(self) -> None:
        assert resolve_market("ETH-USD").asset_index == 1

    def test_unknown_fails_closed(self) -> None:
        with pytest.raises(ValueError, match="not in the resolvable set"):
            resolve_market("NOTACOIN")

    def test_dynamic_universe_takes_precedence(self) -> None:
        # The seam: a dynamic universe entry is tried before the seed.
        dyn = {"BTC": PerpMarket("BTC", 999, 3, 10)}
        assert resolve_market("BTC", universe=dyn).asset_index == 999
        # Falls back to seed for symbols the dynamic source lacks.
        assert resolve_market("ETH", universe=dyn).asset_index == 1
