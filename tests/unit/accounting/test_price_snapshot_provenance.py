"""ALM-3191 — ledger price snapshots fail closed on invalid measurements."""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.accounting.price_snapshot import PriceSnapshot


@pytest.mark.parametrize("price", ["NaN", "Infinity", "-Infinity"])
def test_usd_rejects_non_finite_prices(price: str) -> None:
    snapshot = PriceSnapshot(raw={"WETH": {"price_usd": price}})

    assert snapshot.usd("WETH") is None


def test_usd_preserves_finite_price() -> None:
    snapshot = PriceSnapshot(raw={"WETH": {"price_usd": "3421.25"}})

    assert snapshot.usd("WETH") == Decimal("3421.25")
