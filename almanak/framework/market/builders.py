"""MarketSnapshotBuilder — VIB-4062.

Direct ``MarketSnapshot(...)`` calls are limited to allow-listed locations
(see ``tests/contracts/marketsnapshot_constructor_allowlist.txt``). Runtime
code uses one of the named factories below.

Each factory:

1. Normalizes async data-layer providers into the canonical *sync* service
   Protocols at the builder boundary (``framework.market.services``).
2. Records ``runtime_surface`` ∈ {"local_sdk", "hosted", "pnl_backtest",
   "paper_fork", "http_backtest", "unit_test"} on the snapshot. The
   behavioral contract suite (PRD §5.3) verifies all surfaces produce the
   same class object.
3. Never reads deployment-mode env vars directly — deployment mode is resolved upstream
   and passed in as ``runtime_context``.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from .snapshot import MarketSnapshot

if TYPE_CHECKING:
    from .models import PriceData, TokenBalance


class MarketSnapshotBuilder:
    """Factory class — never instantiated. All entry points are classmethods."""

    @classmethod
    def for_strategy_runner(
        cls,
        *,
        strategy: Any,
        runtime_context: Any | None = None,
        gateway_client: Any | None = None,
        chain: str | None = None,
        wallet_address: str | None = None,
        runtime_surface: str | None = None,
        chains: tuple[str, ...] | list[str] | None = None,
        multi_chain_price_oracle: Any | None = None,
        multi_chain_balance_provider: Any | None = None,
        aave_health_factor_provider: Any | None = None,
        default_timeframe: str | None = None,
    ) -> MarketSnapshot:
        """Build a snapshot for the live / hosted runner.

        Pulls providers off the strategy's wired services (price oracle,
        balance provider, indicator provider, …) and wraps the gateway client
        into the snapshot. Returns a snapshot with
        ``runtime_surface="local_sdk"`` or ``"hosted"`` based on
        ``runtime_context`` (the caller resolves which is which — the snapshot
        does not read env vars).

        For multi-chain strategies the caller passes ``chains=`` and the
        ``multi_chain_*`` providers; the resulting snapshot has the same
        canonical class as a single-chain build.
        """
        if runtime_surface is None:
            runtime_surface = _resolve_runtime_surface(runtime_context, default="local_sdk")
        # Strict resolution. CodeRabbit (2026-05-06): a multi-chain strategy
        # MUST pass an explicit primary `chain=`. Picking the first item of
        # `chains` reintroduces the silent chain selection PRD §4.2 removes —
        # a downstream `price()` call without `chain=` would then route to
        # whichever chain happened to be listed first instead of raising
        # AmbiguousChainError.
        resolved_chain = chain or getattr(strategy, "chain", None)
        if resolved_chain is None:
            from .errors import ChainNotConfiguredError

            raise ChainNotConfiguredError(
                "for_strategy_runner: caller must pass an explicit primary "
                "chain= (or the strategy must expose a `chain` attribute). "
                "For multi-chain strategies pass BOTH chain= (the primary) "
                "and chains=. The builder no longer defaults to 'ethereum' "
                "or to the first item of chains= silently.",
            )
        wallet_address = wallet_address or getattr(strategy, "wallet_address", "") or ""

        # VIB-4347: thread the OHLCVRouter that ``run_helpers`` stamped on the
        # strategy during indicator wiring through to MarketSnapshot so live
        # ``market.ohlcv(...)`` resolves to the same routed gateway-backed pipes
        # the indicator path uses. ``None`` when ``indicators=False`` strategies
        # opted out — the snapshot falls back to its legacy OHLCV module path.
        ohlcv_router = getattr(strategy, "_ohlcv_router", None)

        # VIB-4727: pool analytics reader is a thin gRPC client over the
        # gateway's PoolAnalyticsService. The strategy container never
        # makes its own HTTP calls — all DefiLlama / GeckoTerminal egress
        # happens server-side. Strategies may inject their own reader via
        # `strategy._pool_analytics_reader` (e.g. for tests); otherwise
        # build one from the wired gateway_client. When no gateway_client
        # is available (unusual — typically only in misconfigured tests),
        # leave it None so MarketSnapshot.pool_analytics(...) raises a
        # clear "not configured" error instead of silently degrading.
        pool_analytics_reader = getattr(strategy, "_pool_analytics_reader", None)
        if pool_analytics_reader is None and gateway_client is not None:
            from almanak.framework.data.pools.analytics import PoolAnalyticsReader

            pool_analytics_reader = PoolAnalyticsReader(gateway_client=gateway_client)

        # T3-B (VIB-4844): stateless calculators are pure Python — no gateway,
        # no egress, no secrets. Construct them directly so the documented
        # MarketSnapshot surface (`il_exposure`, `projected_il`, `realized_vol`,
        # `vol_cone`, `portfolio_risk`, `rolling_sharpe`) is live instead of
        # dead. A strategy may inject its own (e.g. an ILCalculator seeded with
        # tracked positions) via the private attribute; otherwise the default
        # stateless instance is built. The same calculators are injected on the
        # backtest factories below — they are pure math over series the snapshot
        # already holds, so they stay deterministic under replay.
        il_calculator, volatility_calculator, risk_calculator = _build_stateless_calculators(strategy)

        if chains:
            # Multi-chain path: lift the multi-chain providers and the
            # aave_health_factor_provider off the strategy.
            return MarketSnapshot(
                chain=resolved_chain,
                chains=tuple(chains),
                wallet_address=wallet_address,
                price_oracle=multi_chain_price_oracle or getattr(strategy, "_multi_chain_price_oracle", None),
                balance_provider=multi_chain_balance_provider
                or getattr(strategy, "_multi_chain_balance_provider", None),
                aave_health_factor_provider=aave_health_factor_provider
                or getattr(strategy, "_aave_health_factor_provider", None),
                pool_analytics_reader=pool_analytics_reader,
                il_calculator=il_calculator,
                volatility_calculator=volatility_calculator,
                risk_calculator=risk_calculator,
                gateway_client=gateway_client,
                ohlcv_router=ohlcv_router,
                runtime_surface=runtime_surface,
            )

        return MarketSnapshot(
            chain=resolved_chain,
            wallet_address=wallet_address,
            price_oracle=getattr(strategy, "price_oracle", None) or getattr(strategy, "_price_oracle", None),
            rsi_provider=getattr(strategy, "rsi_provider", None) or getattr(strategy, "_rsi_provider", None),
            balance_provider=getattr(strategy, "balance_provider", None)
            or getattr(strategy, "_balance_provider", None),
            wallet_activity_provider=getattr(strategy, "wallet_activity_provider", None)
            or getattr(strategy, "_wallet_activity_provider", None),
            prediction_provider=getattr(strategy, "prediction_provider", None)
            or getattr(strategy, "_prediction_provider", None),
            indicator_provider=getattr(strategy, "indicator_provider", None)
            or getattr(strategy, "_indicator_provider", None),
            multi_dex_service=getattr(strategy, "multi_dex_service", None)
            or getattr(strategy, "_multi_dex_service", None),
            rate_monitor=getattr(strategy, "rate_monitor", None) or getattr(strategy, "_rate_monitor", None),
            funding_rate_provider=getattr(strategy, "funding_rate_provider", None)
            or getattr(strategy, "_funding_rate_provider", None),
            pool_analytics_reader=pool_analytics_reader,
            il_calculator=il_calculator,
            volatility_calculator=volatility_calculator,
            risk_calculator=risk_calculator,
            gateway_client=gateway_client,
            ohlcv_router=ohlcv_router,
            default_timeframe=default_timeframe or getattr(strategy, "default_timeframe", None),
            runtime_surface=runtime_surface,
        )

    @classmethod
    def for_pnl_backtest_state(
        cls,
        *,
        chain: str,
        wallet_address: str,
        state: Any,
    ) -> MarketSnapshot:
        """Build a snapshot wired to a PnL backtest engine's state.

        The state object exposes ``price_oracle`` / ``balance_provider`` /
        ``indicator_provider`` interfaces; the builder forwards them as-is.

        VIB-4727: backtest factories inject ``NullPoolAnalyticsReader`` so
        any ``market.pool_analytics(...)`` call raises ``DataSourceUnavailable``
        deterministically. Live gateway HTTP at backtest time would break
        reproducibility — strategies must take a deterministic code path
        (static fee assumption, fixture-backed analytics, or HOLD) inside
        backtests.

        VIB-4728 / POOL-7 (VIB-4755) extends the same pattern to pool
        history: ``NullPoolHistoryReader`` is injected for the same
        determinism reason. ``for_strategy_runner`` does NOT auto-construct
        the live ``PoolHistoryReader`` (per VIB-4755 D-4: the cut-over is
        gated on VIB-4730 hosted-egress + VIB-4863 TheGraph API key landing).
        """
        from almanak.framework.data.null_readers import NullPoolHistoryReader
        from almanak.framework.data.pools.analytics import NullPoolAnalyticsReader

        # T3-B (VIB-4844): inject the real stateless calculators on the backtest
        # path too. They are pure math over series the snapshot already holds
        # (tracked LP positions, OHLCV candles, a caller-supplied PnL series),
        # so they produce identical output across replays of the same fixture —
        # no determinism risk, unlike a live gateway/HTTP provider. A state
        # object MAY pre-seed its own calculators (e.g. an ILCalculator carrying
        # the backtest's tracked positions) via the same attributes.
        il_calculator, volatility_calculator, risk_calculator = _build_stateless_calculators(state)

        return MarketSnapshot(
            chain=chain,
            wallet_address=wallet_address,
            price_oracle=getattr(state, "price_oracle", None),
            rsi_provider=getattr(state, "rsi_provider", None),
            balance_provider=getattr(state, "balance_provider", None),
            indicator_provider=getattr(state, "indicator_provider", None),
            rate_monitor=getattr(state, "rate_monitor", None),
            funding_rate_provider=getattr(state, "funding_rate_provider", None),
            pool_analytics_reader=NullPoolAnalyticsReader(),
            pool_history_reader=NullPoolHistoryReader(),
            il_calculator=il_calculator,
            volatility_calculator=volatility_calculator,
            risk_calculator=risk_calculator,
            timestamp=getattr(state, "timestamp", None),
            runtime_surface="pnl_backtest",
        )

    @classmethod
    def for_paper_fork(
        cls,
        *,
        chain: str,
        wallet_address: str,
        fork_manager: Any,
        gateway_client: Any | None = None,
    ) -> MarketSnapshot:
        """Build a snapshot for paper-trading on an Anvil fork.

        Strategies must NOT see ``fork_rpc_url`` directly — the fork-aware
        market service adapters consume it internally. PRD §4.7.
        """
        # VIB-4727: paper-fork backtests inject NullPoolAnalyticsReader for
        # the same determinism reason as for_pnl_backtest_state — a live
        # gateway HTTP call at paper-trading time would make replay-of-the-
        # same-fork produce different results across runs.
        # VIB-4728 / POOL-7 (VIB-4755) extends the same injection to pool
        # history via NullPoolHistoryReader.
        from almanak.framework.data.null_readers import NullPoolHistoryReader
        from almanak.framework.data.pools.analytics import NullPoolAnalyticsReader

        # T3-B (VIB-4844): stateless calculators are pure math and deterministic
        # under fork replay — inject the real instances here too (no determinism
        # concern, unlike live gateway/HTTP analytics).
        il_calculator, volatility_calculator, risk_calculator = _build_stateless_calculators(None)

        snapshot = MarketSnapshot(
            chain=chain,
            wallet_address=wallet_address,
            gateway_client=gateway_client,
            pool_analytics_reader=NullPoolAnalyticsReader(),
            pool_history_reader=NullPoolHistoryReader(),
            il_calculator=il_calculator,
            volatility_calculator=volatility_calculator,
            risk_calculator=risk_calculator,
            runtime_surface="paper_fork",
        )
        # Builder owns the fork URL — strategies never see it.
        # RollingForkManager exposes get_rpc_url()/current_block; older fork
        # managers used rpc_url/block_number — read both to stay compatible
        # (see paper/engine.py which already uses the get_rpc_url/current_block API).
        get_rpc_url = getattr(fork_manager, "get_rpc_url", None)
        if callable(get_rpc_url):
            snapshot._fork_rpc_url = get_rpc_url()
        else:
            snapshot._fork_rpc_url = getattr(fork_manager, "rpc_url", None)
        snapshot._fork_block = getattr(
            fork_manager,
            "current_block",
            getattr(fork_manager, "block_number", None),
        )
        return snapshot

    @classmethod
    def for_http_backtest_spec(
        cls,
        *,
        spec: Any,
    ) -> MarketSnapshot:
        """Build a snapshot for the HTTP-backtest service path."""
        # Strict resolution: PRD §4.2 forbids silent ethereum fallback so a
        # caller misconfiguration backtests the wrong market instead of failing
        # fast. Mirrors `for_strategy_runner` above.
        chain = getattr(spec, "chain", None)
        if not chain:
            from .errors import ChainNotConfiguredError

            raise ChainNotConfiguredError(
                "for_http_backtest_spec: spec.chain is required and must be "
                "non-empty. The builder no longer defaults to 'ethereum'.",
            )
        # T3-B (VIB-4844): inject the real stateless calculators — pure math,
        # deterministic over the spec's series.
        il_calculator, volatility_calculator, risk_calculator = _build_stateless_calculators(spec)

        return MarketSnapshot(
            chain=chain,
            wallet_address=getattr(spec, "wallet_address", ""),
            price_oracle=getattr(spec, "price_oracle", None),
            balance_provider=getattr(spec, "balance_provider", None),
            indicator_provider=getattr(spec, "indicator_provider", None),
            il_calculator=il_calculator,
            volatility_calculator=volatility_calculator,
            risk_calculator=risk_calculator,
            timestamp=getattr(spec, "timestamp", None),
            runtime_surface="http_backtest",
        )

    @classmethod
    def seeded(
        cls,
        *,
        chain: str = "arbitrum",
        wallet_address: str = "0x" + "0" * 40,
        prices: Mapping[str, Decimal] | None = None,
        price_data: Mapping[str, PriceData] | None = None,
        balances: Mapping[str, TokenBalance] | None = None,
        indicators: Mapping[str, Any] | None = None,
        timestamp: Any | None = None,
    ) -> MarketSnapshot:
        """Build a snapshot pre-seeded for unit tests.

        Goes through the public ``seed_*`` API on the snapshot — never writes
        private cache attributes directly (PRD §5.6).
        """
        snapshot = MarketSnapshot(
            chain=chain,
            wallet_address=wallet_address,
            timestamp=timestamp,
            runtime_surface="unit_test",
        )
        if prices:
            for token, price in prices.items():
                snapshot.seed_price(token, price)
        if price_data:
            for token, data in price_data.items():
                snapshot.seed_price_data(token, data)
        if balances:
            for token, bal in balances.items():
                snapshot.seed_balance(token, bal)
        if indicators:
            from .models import (
                BollingerBandsData,
                MACDData,
                RSIData,
            )

            for key, data in indicators.items():
                # Key form: "TOKEN:indicator:period:timeframe" (e.g. "ETH:rsi:14:4h")
                parts = key.split(":")
                token = parts[0]
                timeframe = parts[3] if len(parts) > 3 else None
                if isinstance(data, RSIData):
                    snapshot.seed_rsi(token, data, timeframe=timeframe)
                elif isinstance(data, MACDData):
                    snapshot.seed_macd(token, data, timeframe=timeframe)
                elif isinstance(data, BollingerBandsData):
                    snapshot.seed_bollinger_bands(token, data, timeframe=timeframe)
                else:
                    # Generic: store under a private dict so tests can inspect.
                    seeded: dict[str, Any] = getattr(snapshot, "_seeded_indicators", {})
                    seeded[key] = data
                    snapshot._seeded_indicators = seeded  # type: ignore[attr-defined]
        return snapshot


def _build_stateless_calculators(
    source: Any | None,
) -> tuple[Any, Any, Any]:
    """Construct the T3-B (VIB-4844) stateless calculators for a snapshot.

    Returns ``(il_calculator, volatility_calculator, risk_calculator)``.

    These three are pure-Python — they hold no gateway client, open no
    sockets, and read no secrets — so they are wired identically on the live
    and backtest surfaces (the PRD §Epic B T3-B contract). ``source`` (the
    strategy / backtest state object, or ``None``) may pre-seed a custom
    instance via a public-style attribute (``il_calculator`` /
    ``_il_calculator``, etc.); for the IL calculator this is how a strategy
    threads its tracked LP positions in. When nothing is injected we build the
    default stateless instance. The volatility / risk calculators are
    intrinsically stateless, so the default instance is always sufficient.
    """
    from almanak.framework.data.lp import ILCalculator
    from almanak.framework.data.risk import PortfolioRiskCalculator
    from almanak.framework.data.volatility import RealizedVolatilityCalculator

    def _injected(name: str) -> Any:
        if source is None:
            return None
        val = getattr(source, name, None)
        if val is not None:
            return val
        return getattr(source, f"_{name}", None)

    il_calculator = _injected("il_calculator") or ILCalculator()
    volatility_calculator = _injected("volatility_calculator") or RealizedVolatilityCalculator()
    risk_calculator = _injected("risk_calculator") or PortfolioRiskCalculator()
    return il_calculator, volatility_calculator, risk_calculator


def _resolve_runtime_surface(runtime_context: Any | None, *, default: str) -> str:
    """Resolve the runtime surface label without reading env vars."""
    if runtime_context is None:
        return default
    surface = getattr(runtime_context, "runtime_surface", None)
    if isinstance(surface, str) and surface:
        return surface
    is_hosted = getattr(runtime_context, "is_hosted", None)
    if callable(is_hosted) and is_hosted():
        return "hosted"
    if is_hosted is True:
        return "hosted"
    return default
