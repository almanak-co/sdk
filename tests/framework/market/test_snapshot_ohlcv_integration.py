"""VIB-4347: builder-wired live MarketSnapshot can call market.ohlcv(...).

End-to-end integration: a strategy stub with ``_ohlcv_router`` set is fed
through ``MarketSnapshotBuilder.for_strategy_runner(...)``. The resulting
snapshot's ``market.ohlcv(...)`` call must reach the fake router (and NOT
the legacy ``ohlcv_module`` fallback path that rejects pool_address).

This is the assertion that closes the latent gap the original design doc
flagged in §1: "The ``ohlcv_router`` is never wired onto
``MarketSnapshot._ohlcv_router`` in the live runner path."
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak.framework.data.interfaces import OHLCVCandle
from almanak.framework.data.models import DataEnvelope, DataMeta
from almanak.framework.market import MarketSnapshotBuilder


def _envelope() -> DataEnvelope[list[OHLCVCandle]]:
    return DataEnvelope(
        value=[
            OHLCVCandle(
                timestamp=datetime(2026, 5, 13, 12, 0, 0, tzinfo=UTC),
                open=Decimal("1890"),
                high=Decimal("1910"),
                low=Decimal("1880"),
                close=Decimal("1900"),
                volume=Decimal("1.5"),
            ),
        ],
        meta=DataMeta(
            source="geckoterminal",
            observed_at=datetime.now(UTC),
            confidence=1.0,
        ),
    )


def test_builder_wired_snapshot_calls_router_for_ohlcv() -> None:
    """The router stamped by run_helpers reaches market.ohlcv at runtime."""
    fake_router = MagicMock(name="OHLCVRouter")
    fake_router.get_ohlcv.return_value = _envelope()

    strategy = SimpleNamespace()
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x" + "0" * 40
    strategy._ohlcv_router = fake_router

    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, chain="arbitrum")
    df = snap.ohlcv("WETH", timeframe="1h", limit=24, pool_address="0xabc")

    fake_router.get_ohlcv.assert_called_once()
    kwargs = fake_router.get_ohlcv.call_args.kwargs
    assert kwargs["chain"] == "arbitrum"
    assert kwargs["pool_address"] == "0xabc"
    assert kwargs["timeframe"] == "1h"
    assert kwargs["limit"] == 24
    # Returned a DataFrame-like object (snapshot.ohlcv wraps the envelope).
    assert df is not None
