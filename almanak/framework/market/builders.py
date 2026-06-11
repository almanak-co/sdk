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

from almanak.core.chains import DEFAULT_CHAIN

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
        # makes its own HTTP calls — all DefiLlama / CoinGecko Onchain egress
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

        # VIB-4924 / ALM-2770: wire the twap()/lwap() providers that were unwired
        # in the hosted runner (strategy stuck in perpetual safe-HOLD because
        # MarketSnapshot.twap/lwap raised "No price aggregator configured").
        # twap routes through the gateway GetDexTwap service; lwap runs the
        # existing liquidity-weighted aggregator over the gateway eth_call proxy
        # (source="gateway_rpc"). A strategy may inject its own; otherwise these
        # are built from the wired gateway_client. When no gateway_client is
        # available, both stay None and MarketSnapshot.twap/lwap raise a clear
        # "not configured" error instead of silently degrading.
        price_aggregator, pool_reader_registry = _build_gateway_price_providers(strategy, gateway_client)

        # VIB-4845 (T3-C / T3-D): wire the RPC/gRPC-backed readers that were left
        # unwired by the T3 seam so the documented MarketSnapshot accessors go
        # live instead of dead:
        #   - pool_reserves()                  -> GatewayPoolReserveReader (eth_call proxy)
        #   - liquidity_depth()                -> LiquidityDepthReader (eth_call proxy)
        #   - estimate_slippage()              -> SlippageEstimator (eth_call proxy)
        #   - lending_rate_history() /
        #     funding_rate_history()           -> RateHistoryReader (gateway RateHistoryService)
        # All four route through the gateway (no strategy-container egress). A
        # strategy may inject its own via the private attribute; otherwise these
        # are built from the wired gateway_client and the shared pool registry.
        # pool_history_reader is deliberately NOT wired here — VIB-4755 D-4 gates
        # the live cut-over on VIB-4730 (hosted egress) + VIB-4863 (TheGraph API
        # key); the accessor stays raising "not configured" until those land.
        # yield_opportunities() stays deferred too — there is no gateway Yield
        # service in the proto (a new service, tracked separately).
        pool_reserve_reader, liquidity_depth_reader, slippage_estimator, rate_history_reader = (
            _build_gateway_rpc_readers(strategy, gateway_client, pool_reader_registry)
        )

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
                price_aggregator=price_aggregator,
                pool_reader_registry=pool_reader_registry,
                pool_reader=pool_reserve_reader,
                liquidity_depth_reader=liquidity_depth_reader,
                slippage_estimator=slippage_estimator,
                rate_history_reader=rate_history_reader,
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
            price_aggregator=price_aggregator,
            pool_reader_registry=pool_reader_registry,
            pool_reader=pool_reserve_reader,
            liquidity_depth_reader=liquidity_depth_reader,
            slippage_estimator=slippage_estimator,
            rate_history_reader=rate_history_reader,
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
        from almanak.framework.data.null_readers import (
            NullLiquidityDepthReader,
            NullPoolHistoryReader,
            NullPoolReaderRegistry,
            NullPoolReserveReader,
            NullPriceAggregator,
            NullRateHistoryReader,
            NullSlippageEstimator,
        )
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
            # VIB-4924: twap()/lwap() must fail deterministically in backtests
            # (a live gateway call at replay time = nondeterministic results).
            price_aggregator=NullPriceAggregator(),
            pool_reader_registry=NullPoolReaderRegistry(),
            # VIB-4845: pool_reserves()/liquidity_depth()/estimate_slippage()/
            # rate-history must also fail deterministically under replay — a live
            # gateway eth_call / RateHistoryService call at backtest time =
            # nondeterministic results.
            pool_reader=NullPoolReserveReader(),
            liquidity_depth_reader=NullLiquidityDepthReader(),
            slippage_estimator=NullSlippageEstimator(),
            rate_history_reader=NullRateHistoryReader(),
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
        from almanak.framework.data.null_readers import (
            NullLiquidityDepthReader,
            NullPoolHistoryReader,
            NullPoolReaderRegistry,
            NullPoolReserveReader,
            NullPriceAggregator,
            NullRateHistoryReader,
            NullSlippageEstimator,
        )
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
            # VIB-4924: deterministic twap()/lwap() failure under fork replay.
            price_aggregator=NullPriceAggregator(),
            pool_reader_registry=NullPoolReaderRegistry(),
            # VIB-4845: deterministic pool_reserves()/liquidity_depth()/
            # estimate_slippage()/rate-history failure under fork replay.
            pool_reader=NullPoolReserveReader(),
            liquidity_depth_reader=NullLiquidityDepthReader(),
            slippage_estimator=NullSlippageEstimator(),
            rate_history_reader=NullRateHistoryReader(),
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
        from almanak.framework.data.null_readers import (
            NullLiquidityDepthReader,
            NullPoolReserveReader,
            NullRateHistoryReader,
            NullSlippageEstimator,
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
            # VIB-4845: the HTTP-backtest surface is a replay surface — the
            # RPC/gRPC-backed readers must fail deterministically here too. (The
            # VIB-4924 price aggregator / pool registry are intentionally left
            # unstubbed on this factory per that PR's scope; this PR only adds the
            # four T3-C/T3-D Null readers.)
            pool_reader=NullPoolReserveReader(),
            liquidity_depth_reader=NullLiquidityDepthReader(),
            slippage_estimator=NullSlippageEstimator(),
            rate_history_reader=NullRateHistoryReader(),
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
        chain: str = DEFAULT_CHAIN,
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


def _build_gateway_price_providers(
    strategy: Any,
    gateway_client: Any | None,
) -> tuple[Any, Any]:
    """Build the VIB-4924 twap()/lwap() providers for the live runner.

    Returns ``(price_aggregator, pool_reader_registry)``.

    ``twap()`` routes through the gateway ``GetDexTwap`` service and ``lwap()``
    runs the existing liquidity-weighted aggregator over the gateway ``eth_call``
    proxy — both gateway-boundary compliant (no direct strategy-container
    egress). The aggregator and the registry share one ``eth_call`` closure and
    one ``PoolReaderRegistry`` (constructed with ``source_name="gateway_rpc"``
    so the lwap envelope carries honest provenance, VIB-4924 H3).

    A strategy may inject its own ``price_aggregator`` / ``pool_reader_registry``
    (public or private attribute); those are honored first. When no
    ``gateway_client`` is available (unusual — typically only misconfigured
    tests) the providers stay ``None`` so ``MarketSnapshot.twap/lwap`` raise a
    clear "not configured" error instead of silently degrading.
    """
    price_aggregator = getattr(strategy, "price_aggregator", None) or getattr(strategy, "_price_aggregator", None)
    pool_reader_registry = getattr(strategy, "pool_reader_registry", None) or getattr(
        strategy, "_pool_reader_registry", None
    )

    if price_aggregator is not None and pool_reader_registry is not None:
        return price_aggregator, pool_reader_registry
    if gateway_client is None:
        # Nothing to build from; return whatever (possibly partial) overrides
        # exist. A None aggregator yields the clear "not configured" raise.
        return price_aggregator, pool_reader_registry

    from almanak.framework.data.pools.reader import PoolReaderRegistry
    from almanak.framework.data.tokens import get_token_resolver
    from almanak.framework.market.gateway_price_aggregator import GatewayMarketPriceAggregator

    _rpc_call = _make_gateway_rpc_call(gateway_client)

    if pool_reader_registry is None:
        # VIB-4924 B1: wire the registry-based TokenResolver so the readers can
        # map the canonical *symbols* produced by ``resolve_instrument``
        # ("WETH", "USDC") to pool-key addresses. Without it,
        # ``reader._resolve_to_address("WETH")`` returns None and
        # ``twap("WETH/USDC")`` / ``lwap("WETH/USDC")`` resolve no pool and HOLD
        # forever (ALM-2770's own call site). ``get_token_resolver()`` is
        # registry-backed (no egress) — gateway-boundary safe.
        pool_reader_registry = PoolReaderRegistry(
            rpc_call=_rpc_call,
            token_resolver=get_token_resolver(),
            source_name="gateway_rpc",
        )
    if price_aggregator is None:
        price_aggregator = GatewayMarketPriceAggregator(
            gateway_client=gateway_client,
            pool_registry=pool_reader_registry,
            rpc_call=_rpc_call,
        )
    return price_aggregator, pool_reader_registry


def _make_gateway_rpc_call(gateway_client: Any) -> Any:
    """Return the sanctioned gateway ``eth_call`` proxy closure.

    Mirrors ``dashboard/custom/api_client.py``: a ``Callable(chain, to, data) ->
    bytes`` over ``gateway_client.eth_call`` (no strategy-container egress).
    ``None`` / ``"0x"`` responses become empty bytes so the pure decoders raise
    the typed "response too short" error rather than crashing on ``None``.
    Shared by the price providers (VIB-4924) and the RPC-backed readers
    (VIB-4845) so both speak the same gateway proxy.
    """
    from almanak.framework.data.interfaces import DataSourceError

    def _rpc_call(chain_name: str, to: str, calldata: str) -> bytes:
        # Fail fast at the boundary when the client explicitly reports a dead
        # channel, rather than deferring an opaque failure into the decoders.
        # ``default=True`` (not False): real GatewayClient exposes the property,
        # but test doubles / adapters that omit it must still work — only an
        # explicit ``is_connected is False`` short-circuits.
        if not getattr(gateway_client, "is_connected", True):
            raise DataSourceError(f"gateway client is not connected; cannot eth_call {to} on {chain_name}")
        raw = gateway_client.eth_call(chain=chain_name, to=to, data=calldata)
        if not raw or raw == "0x":
            return b""
        return bytes.fromhex(raw.removeprefix("0x"))

    return _rpc_call


def _build_gateway_rpc_readers(
    strategy: Any,
    gateway_client: Any | None,
    pool_reader_registry: Any | None,
) -> tuple[Any, Any, Any, Any]:
    """Build the VIB-4845 RPC/gRPC-backed readers for the live runner.

    Returns ``(pool_reserve_reader, liquidity_depth_reader, slippage_estimator,
    rate_history_reader)``.

    - ``pool_reserve_reader`` (``pool_reserves()``): a ``GatewayPoolReserveReader``
      over the gateway ``eth_call`` proxy. The legacy ``UniswapV3PoolReader`` is
      boundary-violating (direct ``AsyncWeb3``), so it is NOT used here.
    - ``liquidity_depth_reader`` (``liquidity_depth()``): a ``LiquidityDepthReader``
      over the same ``eth_call`` proxy.
    - ``slippage_estimator`` (``estimate_slippage()``): a ``SlippageEstimator``
      backed by the liquidity reader + the shared ``pool_reader_registry`` (which
      already carries the wired ``TokenResolver`` for symbol→address resolution).
    - ``rate_history_reader`` (``lending_rate_history()`` / ``funding_rate_history()``):
      a ``RateHistoryReader`` — a thin gRPC client of the gateway
      ``RateHistoryService`` (it resolves its own connected gateway client, so it
      needs no constructor arg).

    A strategy may inject any of these via a public/private attribute; those are
    honored first. When no ``gateway_client`` is available (unusual —
    misconfigured tests) the eth_call-backed readers stay ``None`` so the
    corresponding accessor raises a clear "not configured" error instead of
    silently degrading. ``rate_history_reader`` is gateway-client-agnostic at
    construction (it connects lazily on first call), so it is always built.
    """
    from almanak.framework.data.pools.liquidity import LiquidityDepthReader, SlippageEstimator
    from almanak.framework.data.rates.history import RateHistoryReader

    def _injected(*names: str) -> Any:
        for name in names:
            val = getattr(strategy, name, None)
            if val is not None:
                return val
        return None

    pool_reserve_reader = _injected("pool_reader", "_pool_reader")
    liquidity_depth_reader = _injected("liquidity_depth_reader", "_liquidity_depth_reader")
    slippage_estimator = _injected("slippage_estimator", "_slippage_estimator")
    rate_history_reader = _injected("rate_history_reader", "_rate_history_reader") or RateHistoryReader()

    if gateway_client is not None:
        from almanak.framework.data.defi.gateway_pool_reader import GatewayPoolReserveReader
        from almanak.framework.data.tokens import get_token_resolver

        rpc_call = _make_gateway_rpc_call(gateway_client)
        token_resolver = get_token_resolver()

        if pool_reserve_reader is None:
            pool_reserve_reader = GatewayPoolReserveReader(
                rpc_call=rpc_call,
                token_resolver=token_resolver,
                price_oracle=getattr(strategy, "price_oracle", None) or getattr(strategy, "_price_oracle", None),
            )
        if liquidity_depth_reader is None:
            liquidity_depth_reader = LiquidityDepthReader(rpc_call=rpc_call, source_name="gateway_rpc")
        if slippage_estimator is None:
            slippage_estimator = SlippageEstimator(
                liquidity_reader=liquidity_depth_reader,
                pool_reader_registry=pool_reader_registry,
                source_name="gateway_rpc",
            )

    return pool_reserve_reader, liquidity_depth_reader, slippage_estimator, rate_history_reader


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
