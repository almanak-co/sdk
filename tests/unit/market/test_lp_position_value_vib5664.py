"""VIB-5664 — direct tests for ``MarketSnapshot.lp_position_value``.

Sibling of ``position_health``: builds an ``LPPositionReader`` from the
snapshot's gateway client and reuses the shared ``lp_repricer`` engine. Empty ≠
Zero throughout: no gateway / reader miss → ``None`` (never a fabricated $0).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.market.snapshot import MarketSnapshot
from almanak.framework.valuation.lp_position_reader import LPPositionOnChain, PoolSlot0


def _snap() -> MarketSnapshot:
    snap = MarketSnapshot(chain="base", wallet_address="0x" + "11" * 20)
    snap.set_price("WETH", Decimal("3500"))
    snap.set_price("USDC", Decimal("1"))
    snap._gateway_client = object()  # non-None ⇒ reader is buildable
    return snap


# Real Base token addresses so ``default_decimals_fn`` resolves 18 / 6.
_WETH = "0x4200000000000000000000000000000000000006"
_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
_POOL = "0x1111111111111111111111111111111111111111"


def _install_reader(monkeypatch, *, position, slot0=PoolSlot0(sqrt_price_x96=2**96, tick=0)):
    reader = MagicMock()
    reader.read_position.return_value = position
    reader.read_pool_slot0.return_value = slot0
    monkeypatch.setattr(
        "almanak.framework.valuation.lp_position_reader.LPPositionReader",
        lambda *_a, **_k: reader,
    )
    return reader


def _slipstream_position(*, liquidity=10_000_000_000, owed0=0, owed1=0):
    # fee=None, tick_spacing set — the Slipstream CL layout.
    return LPPositionOnChain(
        token_id=12345,
        token0=_WETH,
        token1=_USDC,
        fee=None,
        tick_lower=-100,
        tick_upper=100,
        liquidity=liquidity,
        tokens_owed0=owed0,
        tokens_owed1=owed1,
        tick_spacing=100,
    )


class TestLpPositionValue:
    def test_values_open_position_and_adds_fees(self, monkeypatch):
        reader = _install_reader(monkeypatch, position=_slipstream_position(owed0=1_000_000_000_000_000, owed1=2_000_000))
        snap = _snap()

        result = snap.lp_position_value("12345", "aerodrome_slipstream", pool_address=_POOL)

        assert result is not None
        assert result.value_usd > 0
        assert result.fees_usd > 0  # tokens_owed0/1 > 0 folded into fees
        assert result.total_usd == result.value_usd + result.fees_usd
        assert result.in_range is True  # tick 0 within [-100, 100]
        # slot0 read happened (pool_address supplied) — exact tick, not the fallback.
        reader.read_pool_slot0.assert_called_once()

    def test_slipstream_fee_none_path_values_fine(self, monkeypatch):
        """fee=None (Slipstream CL) is not consumed by the math — still values."""
        _install_reader(monkeypatch, position=_slipstream_position())
        snap = _snap()
        result = snap.lp_position_value("12345", "aerodrome_slipstream", pool_address=_POOL)
        assert result is not None
        assert result.fees_usd == Decimal("0")  # no uncollected fees

    def test_reader_miss_returns_none(self, monkeypatch):
        _install_reader(monkeypatch, position=None)
        snap = _snap()
        result = snap.lp_position_value("12345", "aerodrome_slipstream", pool_address=_POOL)
        assert result is None  # Empty ≠ Zero

    def test_no_gateway_returns_none(self, monkeypatch):
        snap = _snap()
        snap._gateway_client = None
        result = snap.lp_position_value("12345", "aerodrome_slipstream", pool_address=_POOL)
        assert result is None

    def test_zero_liquidity_measured_zero(self, monkeypatch):
        _install_reader(monkeypatch, position=_slipstream_position(liquidity=0, owed0=0, owed1=0))
        snap = _snap()
        result = snap.lp_position_value("12345", "aerodrome_slipstream", pool_address=_POOL)
        assert result is not None
        assert result.total_usd == Decimal("0")
        assert result.liquidity == 0


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
