"""Nightly contract test for strategy-facing MarketSnapshot API.

Validates the runtime API that users call in `decide(market)` using live
gateway-backed providers, then writes visual artifacts and a JSON summary.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import matplotlib
import pytest

from almanak.framework.cli.run import (
    create_sync_balance_func,
    create_sync_price_oracle_func,
)
from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider
from almanak.framework.data.indicators.adx import ADXCalculator
from almanak.framework.data.indicators.atr import ATRCalculator
from almanak.framework.data.indicators.bollinger_bands import BollingerBandsCalculator
from almanak.framework.data.indicators.cci import CCICalculator
from almanak.framework.data.indicators.ichimoku import IchimokuCalculator
from almanak.framework.data.indicators.macd import MACDCalculator
from almanak.framework.data.indicators.moving_averages import MovingAverageCalculator
from almanak.framework.data.indicators.obv import OBVCalculator
from almanak.framework.data.indicators.rsi import RSICalculator
from almanak.framework.data.indicators.stochastic import StochasticCalculator
from almanak.framework.data.indicators.sync_wrappers import (
    create_sync_adx_func,
    create_sync_atr_func,
    create_sync_bollinger_func,
    create_sync_cci_func,
    create_sync_ema_func,
    create_sync_ichimoku_func,
    create_sync_macd_func,
    create_sync_obv_func,
    create_sync_rsi_func,
    create_sync_sma_func,
    create_sync_stochastic_func,
)
from almanak.framework.data.ohlcv.gateway_data_adapter import GatewayOHLCVDataProvider
from almanak.framework.data.ohlcv.gateway_provider import GatewayOHLCVProvider
from almanak.framework.data.ohlcv.geckoterminal_provider import GeckoTerminalOHLCVProvider
from almanak.framework.data.ohlcv.ohlcv_router import OHLCVRouter
from almanak.framework.data.ohlcv.routing_provider import RoutingOHLCVProvider
from almanak.framework.data.price.gateway_oracle import GatewayPriceOracle
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.framework.strategies.intent_strategy import IndicatorProvider, MarketSnapshot

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

pytestmark = pytest.mark.integration

OUTPUT_DIR = Path(__file__).parent / "output"


def _run_async(coro: Any) -> Any:
    """Run async coroutine synchronously, handling nested loops."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None:
        import nest_asyncio

        nest_asyncio.apply()
        return asyncio.get_event_loop().run_until_complete(coro)
    return asyncio.run(coro)


def _resolve_wallet_address() -> str:
    """Resolve wallet address from env/private key with sensible fallback."""
    explicit = os.getenv("MARKET_CONTRACT_WALLET_ADDRESS") or os.getenv("ALMANAK_WALLET_ADDRESS")
    if explicit:
        return explicit

    private_key = os.getenv("ALMANAK_PRIVATE_KEY")
    if private_key:
        try:
            from eth_account import Account

            return Account.from_key(private_key).address
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("ALMANAK_PRIVATE_KEY is set but invalid; cannot resolve wallet address") from exc

    # Anvil default account #0 fallback.
    return "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


def _sma_series(values: list[float], period: int) -> list[float | None]:
    """Compute SMA series with None for warm-up region."""
    if period <= 0:
        raise ValueError("period must be > 0")

    series: list[float | None] = []
    for idx in range(len(values)):
        if idx + 1 < period:
            series.append(None)
        else:
            window = values[idx + 1 - period : idx + 1]
            series.append(sum(window) / period)
    return series


def _rsi_series(close_prices: list[Decimal], period: int) -> list[float | None]:
    """Compute RSI series using rolling windows."""
    out: list[float | None] = []
    for idx in range(len(close_prices)):
        if idx < period:
            out.append(None)
        else:
            window = close_prices[: idx + 1]
            out.append(RSICalculator.calculate_rsi_from_prices(window, period=period))
    return out


def _record_step(summary: dict[str, Any], name: str, fn: Any) -> Any:
    """Run a step, recording timing and structured pass/fail details."""
    start = time.perf_counter()
    try:
        value = fn()
        summary["checks"][name] = {"ok": True}
        return value
    except Exception as exc:  # noqa: BLE001
        summary["checks"][name] = {"ok": False, "error": str(exc)}
        summary["errors"].append(f"{name}: {exc}")
        return None
    finally:
        summary["timings_ms"][name] = round((time.perf_counter() - start) * 1000, 2)


def test_strategy_market_snapshot_contract() -> None:
    """Validate user-facing MarketSnapshot methods and generate nightly artifacts."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    chain = os.getenv("MARKET_CONTRACT_CHAIN", "arbitrum").lower()
    wallet_address = _resolve_wallet_address()
    gateway_host = os.getenv("GATEWAY_HOST", "127.0.0.1")
    gateway_port = int(os.getenv("GATEWAY_PORT", "50051"))
    gateway_timeout = float(os.getenv("GATEWAY_TIMEOUT", "30.0"))

    summary: dict[str, Any] = {
        "status": "fail",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "chain": chain,
        "wallet_address": wallet_address,
        "gateway": f"{gateway_host}:{gateway_port}",
        "checks": {},
        "timings_ms": {},
        "metrics": {},
        "errors": [],
    }

    start_total = time.perf_counter()
    client: GatewayClient | None = None
    ohlcv_provider: RoutingOHLCVProvider | None = None

    try:
        client = GatewayClient(
            GatewayClientConfig(
                host=gateway_host,
                port=gateway_port,
                timeout=gateway_timeout,
            )
        )
        client.connect()
        if not client.wait_for_ready(timeout=120, interval=2):
            raise RuntimeError(f"Gateway not ready at {gateway_host}:{gateway_port}")

        price_oracle = GatewayPriceOracle(client)
        balance_provider = GatewayBalanceProvider(
            client=client,
            wallet_address=wallet_address,
            chain=chain,
        )

        gateway_ohlcv = GatewayOHLCVProvider(gateway_client=client)
        gateway_adapter = GatewayOHLCVDataProvider(gateway_ohlcv)
        gecko_provider = GeckoTerminalOHLCVProvider()

        router = OHLCVRouter(default_chain=chain)
        router.register_provider(gateway_adapter)
        router.register_provider(gecko_provider)

        ohlcv_provider = RoutingOHLCVProvider(
            router=router,
            chain=chain,
            pool_address=None,
            closeable_providers=[gecko_provider],
        )

        rsi_calculator = RSICalculator(ohlcv_provider=ohlcv_provider)
        macd_calculator = MACDCalculator(ohlcv_provider=ohlcv_provider)
        stoch_calculator = StochasticCalculator(ohlcv_provider=ohlcv_provider)
        atr_calculator = ATRCalculator(ohlcv_provider=ohlcv_provider)
        ma_calculator = MovingAverageCalculator(ohlcv_provider=ohlcv_provider)
        adx_calculator = ADXCalculator(ohlcv_provider=ohlcv_provider)
        obv_calculator = OBVCalculator(ohlcv_provider=ohlcv_provider)
        cci_calculator = CCICalculator(ohlcv_provider=ohlcv_provider)
        ichimoku_calculator = IchimokuCalculator(ohlcv_provider=ohlcv_provider)

        sync_price_oracle = create_sync_price_oracle_func(price_oracle)
        sync_balance_provider = create_sync_balance_func(balance_provider, price_oracle)

        indicator_provider = IndicatorProvider(
            macd=create_sync_macd_func(macd_calculator),
            bollinger=create_sync_bollinger_func(BollingerBandsCalculator(ohlcv_provider=ohlcv_provider)),
            stochastic=create_sync_stochastic_func(stoch_calculator),
            atr=create_sync_atr_func(atr_calculator, sync_price_oracle),
            sma=create_sync_sma_func(ma_calculator, sync_price_oracle),
            ema=create_sync_ema_func(ma_calculator, sync_price_oracle),
            adx=create_sync_adx_func(adx_calculator),
            obv=create_sync_obv_func(obv_calculator),
            cci=create_sync_cci_func(cci_calculator),
            ichimoku=create_sync_ichimoku_func(ichimoku_calculator),
        )

        market = MarketSnapshot(
            chain=chain,
            wallet_address=wallet_address,
            price_oracle=sync_price_oracle,
            rsi_provider=create_sync_rsi_func(rsi_calculator),
            balance_provider=sync_balance_provider,
            indicator_provider=indicator_provider,
        )

        prices: dict[str, Decimal] = (
            _record_step(
                summary,
                "price_sanity",
                lambda: {
                    "WETH": market.price("WETH"),
                    "WBTC": market.price("WBTC"),
                    "USDC": market.price("USDC"),
                },
            )
            or {}
        )
        if prices:
            assert prices["WETH"] > 0
            assert prices["WBTC"] > 0
            assert prices["USDC"] > 0
            assert Decimal("0.90") <= prices["USDC"] <= Decimal("1.10")
            summary["metrics"]["prices"] = {k: str(v) for k, v in prices.items()}

        price_data = _record_step(summary, "price_data_shape", lambda: market.price_data("WETH"))
        if price_data is not None:
            assert price_data.price > 0
            summary["metrics"]["price_data"] = {
                "price": str(price_data.price),
                "change_24h_pct": str(price_data.change_24h_pct),
            }

        def _balance_checks() -> dict[str, Decimal]:
            bal = market.balance("WETH")
            bal_usd = market.balance_usd("WETH")
            portfolio = market.total_portfolio_usd()
            assert bal_usd == bal.balance_usd
            assert bal_usd >= 0
            assert portfolio >= 0
            return {
                "balance": bal.balance,
                "balance_usd": bal_usd,
                "portfolio_usd": portfolio,
            }

        balance_metrics = _record_step(summary, "balance_coherence", _balance_checks) or {}
        if balance_metrics:
            summary["metrics"]["balances"] = {k: str(v) for k, v in balance_metrics.items()}

        def _indicator_checks() -> dict[str, Any]:
            rsi = market.rsi("WETH", period=14, timeframe="4h")
            macd = market.macd("WETH", timeframe="4h")
            bb = market.bollinger_bands("WETH", timeframe="4h")
            stoch = market.stochastic("WETH", timeframe="4h")
            atr = market.atr("WETH", timeframe="4h")
            sma = market.sma("WETH", period=20, timeframe="4h")
            ema = market.ema("WETH", period=12, timeframe="4h")
            adx = market.adx("WETH", period=14, timeframe="4h")
            obv = market.obv("WETH", signal_period=21, timeframe="4h")
            cci = market.cci("WETH", period=20, timeframe="4h")
            ichimoku = market.ichimoku("WETH", timeframe="4h")

            assert Decimal("0") <= rsi.value <= Decimal("100")
            assert Decimal("0") <= stoch.k_value <= Decimal("100")
            assert Decimal("0") <= stoch.d_value <= Decimal("100")
            assert bb.upper_band >= bb.middle_band >= bb.lower_band
            assert atr.value >= 0
            assert sma.value > 0
            assert ema.value > 0
            assert Decimal("0") <= adx.adx <= Decimal("100")
            assert ichimoku.senkou_span_a > 0
            assert ichimoku.senkou_span_b > 0

            return {
                "rsi": rsi,
                "macd": macd,
                "bb": bb,
                "stoch": stoch,
                "atr": atr,
                "sma": sma,
                "ema": ema,
                "adx": adx,
                "obv": obv,
                "cci": cci,
                "ichimoku": ichimoku,
            }

        indicators = _record_step(summary, "indicator_coverage", _indicator_checks) or {}
        if indicators:
            summary["metrics"]["indicators"] = {
                "rsi": str(indicators["rsi"].value),
                "macd_histogram": str(indicators["macd"].histogram),
                "stoch_k": str(indicators["stoch"].k_value),
                "atr_percent": str(indicators["atr"].value_percent),
                "adx": str(indicators["adx"].adx),
                "cci": str(indicators["cci"].value),
            }

        wallet_signals = _record_step(
            summary,
            "wallet_activity_callable",
            lambda: market.wallet_activity(action_types=["SWAP", "LP_OPEN"]),
        )
        if wallet_signals is not None:
            assert isinstance(wallet_signals, list)
            summary["metrics"]["wallet_activity_count"] = len(wallet_signals)

        prediction_price = _record_step(
            summary,
            "prediction_price_graceful",
            lambda: market.prediction_price("market-id-placeholder", "YES"),
        )
        if prediction_price is not None:
            assert isinstance(prediction_price, Decimal)
        else:
            summary["metrics"]["prediction_price"] = None

        def _artifact_step() -> dict[str, str]:
            candles = _run_async(ohlcv_provider.get_ohlcv("WETH", quote="USD", timeframe="4h", limit=120))
            if len(candles) < 60:
                raise AssertionError(f"Need >=60 WETH candles for charts, got {len(candles)}")

            times = [c.timestamp for c in candles]
            closes_float = [float(c.close) for c in candles]
            closes_decimal = [Decimal(str(c.close)) for c in candles]
            sma_20 = _sma_series(closes_float, period=20)
            rsi_14 = _rsi_series(closes_decimal, period=14)

            chart_prices = OUTPUT_DIR / "chart_prices_with_sma.png"
            chart_rsi = OUTPUT_DIR / "chart_weth_rsi.png"
            chart_osc = OUTPUT_DIR / "chart_indicator_oscillators.png"
            chart_trend = OUTPUT_DIR / "chart_indicator_trend_volume.png"
            legacy_chart = OUTPUT_DIR / "chart_indicator_coverage.png"
            if legacy_chart.exists():
                legacy_chart.unlink()

            fig1, ax1 = plt.subplots(figsize=(12, 5))
            ax1.plot(times, closes_float, label="WETH Close", color="#2C7FB8", linewidth=1.8)
            ax1.plot(times, sma_20, label="SMA(20)", color="#E34A33", linewidth=1.5)
            ax1.set_title(f"WETH Price + SMA(20) ({chain})")
            ax1.set_ylabel("Price (USD)")
            ax1.legend()
            ax1.grid(alpha=0.2)
            fig1.autofmt_xdate()
            fig1.tight_layout()
            fig1.savefig(chart_prices, dpi=150)
            plt.close(fig1)

            fig2, ax2 = plt.subplots(figsize=(12, 4))
            ax2.plot(times, rsi_14, color="#31A354", linewidth=1.8, label="RSI(14)")
            ax2.axhline(70, color="#CB181D", linestyle="--", linewidth=1.0, label="Overbought (70)")
            ax2.axhline(30, color="#08519C", linestyle="--", linewidth=1.0, label="Oversold (30)")
            ax2.set_title(f"WETH RSI(14) ({chain})")
            ax2.set_ylim(0, 100)
            ax2.set_ylabel("RSI")
            ax2.legend(loc="upper left")
            ax2.grid(alpha=0.2)
            fig2.autofmt_xdate()
            fig2.tight_layout()
            fig2.savefig(chart_rsi, dpi=150)
            plt.close(fig2)

            oscillator_values = {
                "RSI": float(indicators["rsi"].value),
                "Stoch_%K": float(indicators["stoch"].k_value),
                "BB_%B x100": float(indicators["bb"].percent_b * Decimal("100")),
                "ADX": float(indicators["adx"].adx),
                "CCI": float(indicators["cci"].value),
            }
            osc_labels = list(oscillator_values.keys())
            osc_data = list(oscillator_values.values())
            osc_colors = ["#31A354" if value >= 0 else "#CB181D" for value in osc_data]

            fig3, ax3 = plt.subplots(figsize=(12, 5))
            bars3 = ax3.bar(osc_labels, osc_data, color=osc_colors)
            ax3.set_title("Oscillator Snapshot (raw values)")
            ax3.set_ylabel("Indicator value")
            ax3.axhline(70, color="#CB181D", linestyle="--", linewidth=0.9, alpha=0.8)
            ax3.axhline(30, color="#08519C", linestyle="--", linewidth=0.9, alpha=0.8)
            ax3.axhline(100, color="#636363", linestyle=":", linewidth=0.9, alpha=0.7)
            ax3.axhline(-100, color="#636363", linestyle=":", linewidth=0.9, alpha=0.7)
            ax3.axhline(0, color="#4D4D4D", linewidth=0.8, alpha=0.6)
            ax3.grid(axis="y", alpha=0.2)
            plt.setp(ax3.get_xticklabels(), rotation=25, ha="right")
            ax3.bar_label(bars3, fmt="%.2f", padding=3, fontsize=8)
            fig3.tight_layout()
            fig3.savefig(chart_osc, dpi=150)
            plt.close(fig3)

            trend_raw = {
                "MACD_hist": float(indicators["macd"].histogram),
                "ATR_%": float(indicators["atr"].value_percent),
                "SMA20": float(indicators["sma"].value),
                "EMA12": float(indicators["ema"].value),
                "OBV": float(indicators["obv"].obv),
                "Ichimoku_A": float(indicators["ichimoku"].senkou_span_a),
            }
            trend_transformed = [math.copysign(math.log10(abs(v) + 1), v) for v in trend_raw.values()]

            fig4, ax4 = plt.subplots(figsize=(12, 5))
            bars4 = ax4.bar(
                list(trend_raw.keys()),
                trend_transformed,
                color=["#756BB1" if value >= 0 else "#E34A33" for value in trend_transformed],
            )
            ax4.set_title("Trend/Volume Snapshot (signed log scale)")
            ax4.set_ylabel("Signed log10(|value| + 1)")
            ax4.axhline(0, color="#4D4D4D", linewidth=0.8, alpha=0.6)
            ax4.grid(axis="y", alpha=0.2)
            plt.setp(ax4.get_xticklabels(), rotation=25, ha="right")
            ax4.bar_label(bars4, fmt="%.2f", padding=3, fontsize=8)
            fig4.tight_layout()
            fig4.savefig(chart_trend, dpi=150)
            plt.close(fig4)

            return {
                "chart_prices_with_sma.png": str(chart_prices),
                "chart_weth_rsi.png": str(chart_rsi),
                "chart_indicator_oscillators.png": str(chart_osc),
                "chart_indicator_trend_volume.png": str(chart_trend),
            }

        artifacts = _record_step(summary, "artifact_generation", _artifact_step) or {}
        summary["metrics"]["artifacts"] = artifacts

        summary["status"] = "pass" if not summary["errors"] else "fail"
    finally:
        summary["timings_ms"]["total"] = round((time.perf_counter() - start_total) * 1000, 2)
        summary_path = OUTPUT_DIR / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

        if ohlcv_provider is not None:
            _run_async(ohlcv_provider.close())
        if client is not None:
            client.disconnect()

    if summary["errors"]:
        pytest.fail("\n".join(summary["errors"]))
