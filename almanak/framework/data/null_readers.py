"""Null readers for deterministic backtest / paper-fork factories.

VIB-4728 / POOL-7 (VIB-4755) — moving pool history egress to the
gateway introduced the same backtest-determinism risk VIB-4727
identified for pool analytics: a live gateway call at backtest time
makes "works in backtest" silently diverge from production behaviour
(upstream providers can revise historical bars; multi-tenant gateway
caches can be poisoned by other strategies). The agreed contract is
that backtest factories inject a Null reader that always raises
``DataSourceUnavailable("backtest")`` so strategies that depend on
``pool_history(...)`` take a deterministic code path (a static
assumption, a fixture-backed reader, or HOLD) inside backtests.

Public surface:

- ``NullPoolHistoryReader`` — deterministic stub injected by
  ``MarketSnapshotBuilder.for_pnl_backtest_state`` and ``for_paper_fork``
  (D-4). Raises ``DataSourceUnavailable("backtest")`` on every call;
  constructs NO network / subprocess / FFI primitives at any point
  (verified by the D2.M6 38-primitive monkeypatch determinism proof
  in `docs/internal/uat-cards/VIB-4755.md` §D2.M6).

Why a new module (not co-located in ``pools/history.py`` like
``NullPoolAnalyticsReader`` is in ``pools/analytics.py``): the
VIB-4755 UAT card §D-3 documents the choice — keeping the Null
reader in a separate module makes the D3.F10 source-inspection
guard's scope explicit (``null_readers`` is its own row in the
Scan A table) AND avoids the import-graph noise of having the Null
stub live next to the gateway-talking reader (a tester reading
``pools/history.py`` does NOT immediately see a "null" stub that
might look like a fallback).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import DataEnvelope

if TYPE_CHECKING:
    from almanak.framework.data.defi.pools import PoolReserves
    from almanak.framework.data.pools.history import PoolSnapshot
    from almanak.framework.data.pools.liquidity import LiquidityDepth, SlippageEstimate
    from almanak.framework.data.rates.history import (
        FundingRateSnapshot,
        LendingRateSnapshot,
    )


class NullPoolHistoryReader:
    """Always-raises stub used by backtest factories (VIB-4755).

    Live gateway HTTP at backtest time = nondeterministic results
    across runs — strategies that "work in backtest" then silently
    change behaviour in production. The agreed contract is: backtest
    factories inject this null reader; strategies that depend on
    ``pool_history(...)`` must take a deterministic code path inside
    backtests.

    Any call raises ``DataSourceUnavailable("backtest")`` so the
    runner's HOLD inference path is exercised identically to a real
    gateway outage. The class is intentionally a thin shell — NO
    primitives are constructed in ``__init__`` or anywhere else.
    This is verified by the D2.M6 38-primitive monkeypatch
    determinism test (`tests/framework/market/test_backtest_pool_history_determinism.py
    ::test_null_reader_constructs_no_network_primitives`).
    """

    def get_pool_history(
        self,
        pool_address: str,  # noqa: ARG002
        chain: str,  # noqa: ARG002
        start_date: datetime,  # noqa: ARG002
        end_date: datetime | None = None,  # noqa: ARG002
        resolution: str = "1h",  # noqa: ARG002
        *,
        protocol: str,  # noqa: ARG002  # REQUIRED (VIB-4755 D-2)
    ) -> DataEnvelope[list[PoolSnapshot]]:
        raise DataSourceUnavailable(
            source="pool_history",
            reason="backtest",
        )

    def health(self) -> dict[str, dict[str, int]]:
        """Compat shim — mirrors the live reader's health() shape.

        The live reader's ``health()`` returns ``{}`` (per-provider
        stats are now server-side). The null reader returns the same
        empty dict so any caller that polls ``.health()`` during the
        cut-over gets the same non-throwing response in both backtest
        and live paths.
        """
        return {}


class NullPriceAggregator:
    """Always-raises ``twap()`` / ``lwap()`` stub for backtest factories (VIB-4924).

    ``MarketSnapshotBuilder.for_strategy_runner`` injects a real
    ``GatewayMarketPriceAggregator`` (twap over the gateway ``GetDexTwap``
    service, lwap over the gateway ``eth_call`` proxy). Doing the same on the
    backtest / paper-fork surfaces would make replay nondeterministic — a live
    gateway call at backtest time produces different results across runs. So the
    backtest factories inject this null aggregator instead; strategies that
    depend on ``twap(...)`` / ``lwap(...)`` must take a deterministic code path
    (a static assumption, a fixture, or HOLD) inside backtests.

    Mirrors ``NullPoolHistoryReader``: a thin shell that constructs NO
    network / subprocess / FFI primitives. ``requires_decimals=False`` so
    ``MarketSnapshot.twap`` does not attempt decimal-resolution eth_calls before
    reaching the raising ``twap()``.
    """

    requires_decimals: bool = False

    def twap(
        self,
        pool_address: str,  # noqa: ARG002
        chain: str,  # noqa: ARG002
        window_seconds: int = 300,  # noqa: ARG002
        token0_decimals: int | None = None,  # noqa: ARG002
        token1_decimals: int | None = None,  # noqa: ARG002
        protocol: str = "uniswap_v3",  # noqa: ARG002
    ) -> None:
        raise DataSourceUnavailable(source="twap", reason="backtest")

    def lwap(
        self,
        token_a: str,  # noqa: ARG002
        token_b: str,  # noqa: ARG002
        chain: str,  # noqa: ARG002
        fee_tiers: list[int] | None = None,  # noqa: ARG002
        protocols: list[str] | None = None,  # noqa: ARG002
    ) -> None:
        raise DataSourceUnavailable(source="lwap", reason="backtest")


class NullPoolReaderRegistry:
    """Always-raises pool-resolution stub for backtest factories (VIB-4924).

    ``MarketSnapshot.twap`` resolves the pool via the registry *before* calling
    the aggregator, so a ``None`` registry would raise a bare ``ValueError``
    instead of the deterministic ``DataSourceUnavailable`` the backtest contract
    expects. Injecting this stub makes ``twap()`` fail with the same
    backtest-determinism signal as the other Null readers.

    Thin shell — constructs NO primitives. ``supported_protocols`` returns an
    empty list so the ``lwap`` protocol pre-check never silently passes a
    backtest call through to a live resolution path.
    """

    def get_reader(self, chain: str, protocol: str) -> None:  # noqa: ARG002
        raise DataSourceUnavailable(source="pool_reader_registry", reason="backtest")

    @property
    def supported_protocols(self) -> list[str]:
        return []

    def protocols_for_chain(self, chain: str) -> list[str]:  # noqa: ARG002
        # ``MarketSnapshot.lwap``'s protocol pre-check calls this when explicit
        # protocols are passed. An empty list means any explicitly-requested
        # protocol is reported unsupported in a backtest (the lwap call then
        # fails closed via the Null aggregator) rather than raising AttributeError.
        return []


class NullPoolReserveReader:
    """Always-raises ``get_pool_reserves`` stub for backtest factories (VIB-4845).

    ``MarketSnapshotBuilder.for_strategy_runner`` injects a real
    ``GatewayPoolReserveReader`` (slot0 / liquidity / balanceOf reads over the
    gateway eth_call proxy). On the backtest / paper-fork surfaces a live read
    at replay time makes "works in backtest" silently diverge from production —
    so the backtest factories inject this null reader instead. Strategies that
    depend on ``pool_reserves(...)`` must take a deterministic code path inside
    backtests.

    Mirrors ``NullPoolHistoryReader``: a thin shell that constructs NO
    network / subprocess / FFI primitives.
    """

    async def get_pool_reserves(
        self,
        pool_address: str,  # noqa: ARG002
        chain: str,  # noqa: ARG002
    ) -> PoolReserves:
        raise DataSourceUnavailable(source="pool_reserves", reason="backtest")


class NullLiquidityDepthReader:
    """Always-raises ``read_liquidity_depth`` stub for backtest factories (VIB-4845).

    The live reader scans tick bitmaps over the gateway eth_call proxy; a live
    read at replay time would be nondeterministic. Backtest factories inject
    this stub so ``liquidity_depth(...)`` fails with the backtest-determinism
    signal. Thin shell — constructs NO primitives.
    """

    def read_liquidity_depth(
        self,
        pool_address: str,  # noqa: ARG002
        chain: str,  # noqa: ARG002
        current_tick: int | None = None,  # noqa: ARG002
        current_liquidity: int | None = None,  # noqa: ARG002
        current_price: Decimal | None = None,  # noqa: ARG002
        token0_decimals: int = 18,  # noqa: ARG002
        token1_decimals: int = 6,  # noqa: ARG002
        tick_spacing: int | None = None,  # noqa: ARG002
        fee_tier: int | None = None,  # noqa: ARG002
    ) -> DataEnvelope[LiquidityDepth]:
        raise DataSourceUnavailable(source="liquidity_depth", reason="backtest")


class NullSlippageEstimator:
    """Always-raises ``estimate_slippage`` stub for backtest factories (VIB-4845).

    The live estimator reads pool price + tick depth over the gateway eth_call
    proxy and simulates the swap; a live read at replay time would be
    nondeterministic. Backtest factories inject this stub so
    ``estimate_slippage(...)`` fails with the backtest-determinism signal.
    Thin shell — constructs NO primitives.
    """

    def estimate_slippage(
        self,
        token_in: str,  # noqa: ARG002
        token_out: str,  # noqa: ARG002
        amount: Decimal,  # noqa: ARG002
        chain: str,  # noqa: ARG002
        protocol: str | None = None,  # noqa: ARG002
        pool_address: str | None = None,  # noqa: ARG002
        fee_tier: int = 3000,  # noqa: ARG002
    ) -> DataEnvelope[SlippageEstimate]:
        raise DataSourceUnavailable(source="estimate_slippage", reason="backtest")


class NullRateHistoryReader:
    """Always-raises rate-history stub for backtest factories (VIB-4845).

    ``MarketSnapshotBuilder.for_strategy_runner`` injects the real
    ``RateHistoryReader`` (thin gRPC client of the gateway ``RateHistoryService``
    — TheGraph / DefiLlama / Hyperliquid egress all gateway-side). A live gateway
    call at backtest time produces nondeterministic results across runs (upstream
    providers revise historical bars), so the backtest factories inject this stub
    instead. Strategies that depend on ``lending_rate_history(...)`` /
    ``funding_rate_history(...)`` must take a deterministic code path (a fixture or
    HOLD) inside backtests.

    Mirrors ``NullPoolHistoryReader``: a thin shell that constructs NO
    network / subprocess / FFI primitives. ``health()`` returns ``{}`` to match
    the live reader's non-throwing shape.
    """

    def get_lending_rate_history(
        self,
        protocol: str,  # noqa: ARG002
        token: str,  # noqa: ARG002
        chain: str,  # noqa: ARG002
        days: int = 90,  # noqa: ARG002
    ) -> DataEnvelope[list[LendingRateSnapshot]]:
        raise DataSourceUnavailable(source="lending_rate_history", reason="backtest")

    def get_funding_rate_history(
        self,
        venue: str,  # noqa: ARG002
        market_symbol: str,  # noqa: ARG002
        hours: int = 168,  # noqa: ARG002
    ) -> DataEnvelope[list[FundingRateSnapshot]]:
        raise DataSourceUnavailable(source="funding_rate_history", reason="backtest")

    def health(self) -> dict[str, object]:
        return {}


__all__ = [
    "NullLiquidityDepthReader",
    "NullPoolHistoryReader",
    "NullPoolReaderRegistry",
    "NullPoolReserveReader",
    "NullPriceAggregator",
    "NullRateHistoryReader",
    "NullSlippageEstimator",
]
