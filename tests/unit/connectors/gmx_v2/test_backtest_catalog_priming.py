"""PnL-backtest market-catalog priming regressions.

The process-wide GMX market catalog is populated by the intent compiler's
dynamic gateway verification — a code path a PnL backtest never executes.
Without priming, the fill-pricing lane (``registry_perp_base_symbol`` →
``PerpsReadRegistry.market_metadata``) read an empty catalog and rejected
every address-form PERP_OPEN as unpriceable ("0 trades" backtests). The
candle-lane provider primes the catalog in ``prepare_backtest`` via the
gateway's verified ``GetPerpMarket``; a prime miss stays soft so downstream
pricing keeps its fail-closed named rejection.

Priming happens through exactly ONE path — the provenance-checked
``remember_verified_market``, which runs after ``prepare`` accepts the candle
identity and refuses metadata that disagrees with it. A second, unconditional
pre-``prepare`` prime once inserted records that check could never veto
(the #3660/#3661 double-merge regression); these tests pin the single path.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.connectors.gmx_v2 import market_catalog
from almanak.connectors.gmx_v2.backtest_prices import GMXOracleDataProvider, _GMXOracleMarketSource
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

MARKET_TOKEN = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
INDEX_TOKEN = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
LONG_TOKEN = INDEX_TOKEN
SHORT_TOKEN = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


@pytest.fixture(autouse=True)
def _clean_catalog() -> Iterator[None]:
    market_catalog.clear()
    yield
    market_catalog.clear()


def _verified_market_response() -> SimpleNamespace:
    return SimpleNamespace(
        success=True,
        error="",
        market=SimpleNamespace(
            verified=True,
            label="ETH/USD [WETH-USDC]",
            market_token=MARKET_TOKEN,
            index_token=INDEX_TOKEN,
            index_symbol="ETH",
            index_token_decimals=18,
            long_token=LONG_TOKEN,
            long_token_symbol="WETH",
            short_token=SHORT_TOKEN,
            short_token_symbol="USDC",
        ),
    )


def _fake_gateway_client(rpc: Mock) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(timeout=12.5),
        market=SimpleNamespace(GetPerpMarket=rpc),
    )


def _accept_candle_identity(source: _GMXOracleMarketSource) -> None:
    """Stamp the provenance ``prepare`` would have accepted for this market."""
    source.resolved_market = "ETH/USD [WETH-USDC]"
    source.market_token = MARKET_TOKEN.lower()
    source.index_token = INDEX_TOKEN.lower()
    source.index_symbol = "ETH"


def _provider_with_prepared_candles(monkeypatch: pytest.MonkeyPatch, gateway: object) -> GMXOracleDataProvider:
    monkeypatch.setattr(_GMXOracleMarketSource, "_gateway", staticmethod(gateway))
    provider = GMXOracleDataProvider(fallback=SimpleNamespace(), chain="arbitrum", market=MARKET_TOKEN)

    async def _prepare(**_kwargs: object) -> str:
        _accept_candle_identity(provider._source)
        return "1h"

    provider._source.prepare = _prepare  # type: ignore[method-assign]
    return provider


def _hourly_auto_config() -> PnLBacktestConfig:
    start = datetime(2026, 5, 1, tzinfo=UTC)
    return PnLBacktestConfig(start_time=start, end_time=start + timedelta(days=1), timeframe="auto")


@pytest.mark.asyncio
async def test_prepare_backtest_primes_catalog_with_verified_market(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rpc = Mock(return_value=_verified_market_response())
    provider = _provider_with_prepared_candles(monkeypatch, lambda: (_fake_gateway_client(rpc), None))

    assert await provider.prepare_backtest(_hourly_auto_config()) == "1h"

    record = market_catalog.by_address("arbitrum", MARKET_TOKEN)
    assert record is not None
    assert record.index_symbol == "ETH"
    assert record.index_token_decimals == 18
    rpc.assert_called_once()
    request = rpc.call_args.args[0]
    assert request.protocol == "gmx_v2"
    assert request.chain == "arbitrum"
    assert request.market == MARKET_TOKEN
    assert rpc.call_args.kwargs == {"timeout": 12.5}


@pytest.mark.asyncio
async def test_prime_miss_is_soft_and_keeps_catalog_empty(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No gateway → the run proceeds unprimed; the fill lane stays fail-closed."""

    def _raise() -> tuple[object, object]:
        raise RuntimeError("no gateway reachable")

    provider = _provider_with_prepared_candles(monkeypatch, _raise)

    with caplog.at_level("WARNING"):
        assert await provider.prepare_backtest(_hourly_auto_config()) == "1h"

    assert market_catalog.by_address("arbitrum", MARKET_TOKEN) is None
    assert any("GMX market metadata unavailable" in message for message in caplog.messages)


def test_unverified_market_is_never_remembered(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only venue-verified records may enter the catalog (its one hard rule)."""
    response = _verified_market_response()
    response.market.verified = False
    rpc = Mock(return_value=response)
    monkeypatch.setattr(
        _GMXOracleMarketSource,
        "_gateway",
        staticmethod(lambda: (_fake_gateway_client(rpc), None)),
    )
    source = _GMXOracleMarketSource(chain="arbitrum", market=MARKET_TOKEN, venue="gmx_v2")
    _accept_candle_identity(source)

    source.remember_verified_market()

    assert market_catalog.by_address("arbitrum", MARKET_TOKEN) is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("index_token", "0x" + "8" * 40),
        ("index_symbol", "BTC"),
    ],
)
def test_verified_index_identity_mismatch_is_never_remembered(
    monkeypatch: pytest.MonkeyPatch, field: str, value: str
) -> None:
    """The WHOLE candle-verified identity gates the catalog, not just the market token.

    Candle provenance and ``GetPerpMarket`` serve from the same registry rows,
    and a GMX market token names an immutable on-chain tuple — so metadata that
    keeps the market token but swaps the index identity is a gateway
    contradicting itself. Remembering it would price and close the market
    against an index the accepted candle series never verified.
    """
    response = _verified_market_response()
    setattr(response.market, field, value)
    rpc = Mock(return_value=response)
    monkeypatch.setattr(
        _GMXOracleMarketSource,
        "_gateway",
        staticmethod(lambda: (_fake_gateway_client(rpc), None)),
    )
    source = _GMXOracleMarketSource(chain="arbitrum", market=MARKET_TOKEN, venue="gmx_v2")
    _accept_candle_identity(source)

    source.remember_verified_market()

    assert market_catalog.by_address("arbitrum", MARKET_TOKEN) is None
