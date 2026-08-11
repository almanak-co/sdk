"""Multi-market connector-native price-history regressions (ALM-3234)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from almanak.connectors._strategy_base.perp_price_history_registry import PerpPriceHistoryRegistry
from almanak.connectors.gmx_v2.backtest_prices import (
    GMXOracleDataProvider,
    GMXPriceHistoryCoverageError,
)
from almanak.framework.backtesting.pnl._engine_helpers import (
    _prewarm_declared_funding_history,
    coverage_aware_default_timeframe,
    declared_perp_price_history_targets,
    prepare_perp_price_history,
)
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import OHLCV, normalize_token_key
from almanak.framework.backtesting.pnl.perp_targets import PerpPriceHistoryTarget
from almanak.framework.data.interfaces import DataSourceUnavailable

# Exact generated declaration shape recovered from staging run
# fab7a1df-c4c2-449d-a647-0c5571aa2a40. Unrelated strategy parameters are
# retained to prove that discovery consumes only the closed compatibility
# contract and, specifically, never derives order from market_order.
STAGING_BASKET_CONFIG: dict[str, object] = {
    "chain": "arbitrum",
    "markets": ["SOL/USD", "DOGE/USD", "AVAX/USD"],
    "leverage": "3",
    "protocol": "gmx_v2",
    "rsi_period": 14,
    "force_action": "",
    "market_order": ["SOL", "DOGE", "AVAX"],
    "max_leverage": "3",
    "max_slippage": "0.01",
    "ohlcv_source": "venue_native",
    "perp_markets": {
        "SOL": {
            "index_token": "0x2bcc6d6cdbbdc0a4071e48bb3b969b06b3330c07",
            "index_symbol": "SOL",
            "market_token": "0x09400d9db990d5ed3f35d7be61dfaeb900af03c9",
        },
        "AVAX": {
            "index_token": "0x565609faf65b92f7be02468acf86f8979423e514",
            "index_symbol": "AVAX",
            "market_token": "0x7bbbf946883a5701350007320f525c5379b8178a",
        },
        "DOGE": {
            "index_token": "0xc4da4c24fd591125c3f47b340b6f4f76111883d8",
            "index_symbol": "DOGE",
            "market_token": "0x6853ea96ff216fab11d2d930ce3c508556a4bdc4",
        },
    },
}


def test_generated_basket_decoder_preserves_market_order_and_address_identity() -> None:
    config = dict(STAGING_BASKET_CONFIG)
    # Discovery order comes only from pair labels; this deliberately disagrees.
    config["market_order"] = ["AVAX", "DOGE", "SOL"]

    targets = declared_perp_price_history_targets(SimpleNamespace(), config)

    assert [(target.protocol, target.market, target.market_address) for target in targets] == [
        ("gmx_v2", "SOL/USD", "0x09400d9db990d5ed3f35d7be61dfaeb900af03c9"),
        ("gmx_v2", "DOGE/USD", "0x6853ea96ff216fab11d2d930ce3c508556a4bdc4"),
        ("gmx_v2", "AVAX/USD", "0x7bbbf946883a5701350007320f525c5379b8178a"),
    ]


def test_hosted_omitted_price_timeframe_defaults_basket_to_atomic_auto() -> None:
    strategy = SimpleNamespace(config=STAGING_BASKET_CONFIG)

    assert coverage_aware_default_timeframe(strategy) == "auto"


def test_typed_hook_is_canonical_and_outranks_legacy_config() -> None:
    declared = PerpPriceHistoryTarget(
        protocol="GMX-V2",
        market="SOL/USD",
        market_address="0x09400d9db990d5ed3f35d7be61dfaeb900af03c9",
    )
    strategy = SimpleNamespace(backtest_perp_price_history_targets=lambda: [declared])

    targets = declared_perp_price_history_targets(
        strategy,
        {"protocol": "wrong", "market": "WRONG/USD", "markets": "malformed"},
    )

    assert targets == (declared,)
    assert targets[0].protocol == "gmx_v2"


def test_legacy_scalar_declaration_remains_address_first() -> None:
    targets = declared_perp_price_history_targets(
        SimpleNamespace(),
        {
            "protocol": "gmx_v2",
            "market": "SOL/USD",
            "market_address": "0x09400d9db990d5ed3f35d7be61dfaeb900af03c9",
        },
    )

    assert len(targets) == 1
    assert targets[0].market == "SOL/USD"
    assert targets[0].price_market == "0x09400d9db990d5ed3f35d7be61dfaeb900af03c9"


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda config: config["perp_markets"].pop("DOGE"), "no matching perp_markets"),
        (
            lambda config: config["perp_markets"].update(
                {
                    "ETH": {
                        "index_token": "0x" + "8" * 40,
                        "index_symbol": "ETH",
                        "market_token": "0x" + "9" * 40,
                    }
                }
            ),
            "not present in markets",
        ),
        (lambda config: config["perp_markets"]["SOL"].update({"index_symbol": "DOGE"}), "must match"),
    ],
)
def test_generated_basket_decoder_refuses_partial_or_ambiguous_shapes(mutation: object, message: str) -> None:
    import copy

    config = copy.deepcopy(STAGING_BASKET_CONFIG)
    mutation(config)  # type: ignore[operator]

    with pytest.raises(ValueError, match=message):
        declared_perp_price_history_targets(SimpleNamespace(), config)


class _PreparedBasketProvider:
    def __init__(self, *, fallback: object, chain: str, markets: tuple[str, ...], venue: str) -> None:
        self.fallback = fallback
        self.price_history_targets = tuple((venue, chain, market) for market in markets)

    @classmethod
    def for_backtest_many(
        cls,
        *,
        fallback: object,
        chain: str,
        markets: tuple[str, ...],
        venue: str,
    ) -> _PreparedBasketProvider:
        return cls(fallback=fallback, chain=chain, markets=markets, venue=venue)

    async def prepare_backtest(self, config: PnLBacktestConfig) -> str:
        config.apply_resolved_timeframe("4h", 14_400)
        return "4h"


@pytest.mark.asyncio
async def test_engine_installs_one_provider_for_every_generated_basket_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        PerpPriceHistoryRegistry,
        "backtest_provider",
        classmethod(lambda cls, protocol: _PreparedBasketProvider),
    )
    fallback = object()
    backtester = SimpleNamespace(
        data_provider=fallback,
        _get_strategy_config_dict=lambda strategy: STAGING_BASKET_CONFIG,
    )
    start = datetime(2025, 1, 1, tzinfo=UTC)
    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=200),
        chain="arbitrum",
        timeframe="auto",
    )

    await prepare_perp_price_history(
        backtester,
        SimpleNamespace(),
        config,
        SimpleNamespace(info=Mock(), warning=Mock()),
    )

    assert backtester.data_provider.fallback is fallback
    assert backtester.data_provider.price_history_targets == (
        ("gmx_v2", "arbitrum", "0x09400d9db990d5ed3f35d7be61dfaeb900af03c9"),
        ("gmx_v2", "arbitrum", "0x6853ea96ff216fab11d2d930ce3c508556a4bdc4"),
        ("gmx_v2", "arbitrum", "0x7bbbf946883a5701350007320f525c5379b8178a"),
    )
    assert config.resolved_timeframe == "4h"


@pytest.mark.asyncio
async def test_funding_prewarm_materializes_every_pair_label() -> None:
    source = SimpleNamespace(history_capable=True, materialize_history=AsyncMock(return_value=12))

    await _prewarm_declared_funding_history(source, SimpleNamespace(), STAGING_BASKET_CONFIG)

    assert [call.args for call in source.materialize_history.await_args_list] == [
        ("gmx_v2", "SOL/USD"),
        ("gmx_v2", "DOGE/USD"),
        ("gmx_v2", "AVAX/USD"),
    ]


def _basket_provider() -> GMXOracleDataProvider:
    provider = GMXOracleDataProvider.for_backtest_many(
        fallback=SimpleNamespace(get_price=AsyncMock(return_value=Decimal("1"))),
        chain="arbitrum",
        markets=("SOL-market", "DOGE-market", "AVAX-market"),
        venue="gmx_v2",
    )
    identities = {
        "SOL-market": ("SOL", "0x" + "1" * 40, Decimal("10")),
        "DOGE-market": ("DOGE", "0x" + "2" * 40, Decimal("20")),
        "AVAX-market": ("AVAX", "0x" + "3" * 40, Decimal("30")),
    }
    for source in provider._sources:
        symbol, index_token, _price = identities[source.requested_market]
        source.resolved_market = f"{symbol}/USD"
        source.market_token = "0x" + str(len(symbol)) * 40
        source.index_token = index_token
        source.index_symbol = symbol
        source.remember_verified_market = Mock()  # type: ignore[method-assign]
    return provider


def _series(start: datetime, price: Decimal) -> list[OHLCV]:
    return [OHLCV(timestamp=start, open=price, high=price, low=price, close=price, volume=None)]


@pytest.mark.asyncio
async def test_gmx_basket_auto_commits_only_one_complete_common_cadence() -> None:
    provider = _basket_provider()
    start = datetime(2025, 1, 1, tzinfo=UTC)
    calls: list[tuple[str, str]] = []
    prices = {"SOL-market": Decimal("10"), "DOGE-market": Decimal("20"), "AVAX-market": Decimal("30")}

    for source in provider._sources:

        async def fetch(*, timeframe: str, start: datetime, end: datetime, source=source) -> list[OHLCV]:
            calls.append((source.requested_market, timeframe))
            if timeframe in {"1m", "5m", "15m"}:
                raise GMXPriceHistoryCoverageError("retention")
            if timeframe == "1h" and source.requested_market == "DOGE-market":
                raise GMXPriceHistoryCoverageError("DOGE gap")
            return _series(start, prices[source.requested_market])

        source._fetch_complete = fetch  # type: ignore[method-assign]

    config = PnLBacktestConfig(start_time=start, end_time=start + timedelta(days=200), timeframe="auto")

    assert await provider.prepare_backtest(config) == "4h"
    assert {source.timeframe for source in provider._sources} == {"4h"}
    assert ("SOL-market", "1h") in calls and ("DOGE-market", "1h") in calls
    assert ("AVAX-market", "1h") not in calls
    assert calls[-3:] == [("SOL-market", "4h"), ("DOGE-market", "4h"), ("AVAX-market", "4h")]
    assert provider.required_price_tokens == (
        normalize_token_key("arbitrum", "0x" + "1" * 40),
        normalize_token_key("arbitrum", "0x" + "2" * 40),
        normalize_token_key("arbitrum", "0x" + "3" * 40),
    )
    assert await provider.get_price("DOGE", start) == Decimal("20")
    assert [target[2] for target in provider.price_history_targets] == ["SOL-market", "DOGE-market", "AVAX-market"]
    assert [target["index_symbol"] for target in provider.price_provenance["targets"]] == [
        "SOL",
        "DOGE",
        "AVAX",
    ]


@pytest.mark.asyncio
async def test_gmx_basket_exact_cadence_names_the_uncovered_target() -> None:
    provider = _basket_provider()
    start = datetime(2025, 1, 1, tzinfo=UTC)
    for source in provider._sources:

        async def fetch(*, timeframe: str, start: datetime, end: datetime, source=source) -> list[OHLCV]:
            if source.requested_market == "DOGE-market":
                raise GMXPriceHistoryCoverageError("internal gap")
            return _series(start, Decimal("10"))

        source._fetch_complete = fetch  # type: ignore[method-assign]

    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=1),
        timeframe="1h",
    )

    with pytest.raises(GMXPriceHistoryCoverageError, match="DOGE-market"):
        await provider.prepare_backtest(config)
    assert all(source.timeframe is None for source in provider._sources)


@pytest.mark.asyncio
async def test_gmx_basket_rejects_duplicate_verified_index_identity() -> None:
    provider = _basket_provider()
    start = datetime(2025, 1, 1, tzinfo=UTC)
    provider._sources[1].index_token = provider._sources[0].index_token
    for source in provider._sources:
        source._fetch_complete = AsyncMock(return_value=_series(start, Decimal("10")))  # type: ignore[method-assign]

    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=1),
        timeframe="4h",
    )

    with pytest.raises(DataSourceUnavailable, match="same index token"):
        await provider.prepare_backtest(config)
    assert all(source.timeframe is None for source in provider._sources)
