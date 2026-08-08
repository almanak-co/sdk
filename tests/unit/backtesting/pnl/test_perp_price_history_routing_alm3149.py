"""Engine routing for connector-declared perp price history (ALM-3149)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.connectors._strategy_base.perp_price_history_registry import PerpPriceHistoryRegistry
from almanak.framework.backtesting.pnl._engine_helpers import prepare_perp_price_history
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError


class _PreparedProvider:
    constructed: list[_PreparedProvider] = []

    def __init__(self, *, fallback: object, chain: str, market: str, venue: str) -> None:
        self.fallback = fallback
        self.price_history_target = (venue, chain, market)
        self.constructed.append(self)

    @classmethod
    def for_backtest(
        cls,
        *,
        fallback: object,
        chain: str,
        market: str,
        venue: str,
    ) -> _PreparedProvider:
        return cls(fallback=fallback, chain=chain, market=market, venue=venue)

    async def prepare_backtest(self, config: PnLBacktestConfig) -> str:
        config.apply_resolved_timeframe("4h", 14_400)
        return "4h"


@pytest.mark.asyncio
async def test_engine_installs_manifest_provider_without_changing_strategy_ticks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _PreparedProvider.constructed.clear()
    monkeypatch.setattr(
        PerpPriceHistoryRegistry,
        "backtest_provider",
        classmethod(lambda cls, protocol: _PreparedProvider),
    )
    fallback = object()
    backtester = SimpleNamespace(
        data_provider=fallback,
        _get_strategy_config_dict=lambda strategy: {"protocol": "gmx_v2", "market": "DOGE/USD"},
    )
    strategy = SimpleNamespace()
    start = datetime(2025, 1, 1, tzinfo=UTC)
    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=200),
        chain="arbitrum",
        timeframe="auto",
    )
    logger = SimpleNamespace(info=Mock())

    await prepare_perp_price_history(backtester, strategy, config, logger)

    assert isinstance(backtester.data_provider, _PreparedProvider)
    assert backtester.data_provider.fallback is fallback
    assert backtester.data_provider.price_history_target == ("gmx_v2", "arbitrum", "DOGE/USD")
    assert config.resolved_timeframe == "4h"
    assert config.interval_seconds == 3600
    logger.info.assert_called_once()


@pytest.mark.asyncio
async def test_auto_without_eligible_declared_market_fails_preflight() -> None:
    backtester = SimpleNamespace(
        data_provider=object(),
        _get_strategy_config_dict=lambda strategy: {},
    )
    start = datetime(2025, 1, 1, tzinfo=UTC)
    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=1),
        timeframe="auto",
    )

    with pytest.raises(PreflightValidationError, match="one declared connector-native perp market"):
        await prepare_perp_price_history(
            backtester,
            SimpleNamespace(),
            config,
            SimpleNamespace(info=Mock()),
        )


@pytest.mark.asyncio
async def test_explicit_legacy_interval_does_not_require_native_route() -> None:
    fallback = object()
    backtester = SimpleNamespace(
        data_provider=fallback,
        _get_strategy_config_dict=lambda strategy: {},
    )
    start = datetime(2025, 1, 1, tzinfo=UTC)
    config = PnLBacktestConfig(start_time=start, end_time=start + timedelta(days=1), interval_seconds=3600)

    await prepare_perp_price_history(
        backtester,
        SimpleNamespace(),
        config,
        SimpleNamespace(info=Mock()),
    )

    assert backtester.data_provider is fallback
    assert config.resolved_timeframe is None


GMX_ETH_MARKET_ADDRESS = "0x70d95587d40a2caf56bd97485ab3eec10bee6336"


@pytest.mark.asyncio
async def test_declared_market_address_outranks_the_pair_label(monkeypatch: pytest.MonkeyPatch) -> None:
    """Address-first: a stale/re-labeled pair string cannot fail a valid address."""
    _PreparedProvider.constructed.clear()
    monkeypatch.setattr(
        PerpPriceHistoryRegistry,
        "backtest_provider",
        classmethod(lambda cls, protocol: _PreparedProvider),
    )
    backtester = SimpleNamespace(
        data_provider=object(),
        _get_strategy_config_dict=lambda strategy: {
            "protocol": "gmx_v2",
            "market": "WETH/USD",
            "market_address": GMX_ETH_MARKET_ADDRESS,
        },
    )
    start = datetime(2025, 1, 1, tzinfo=UTC)
    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=200),
        chain="arbitrum",
        timeframe="auto",
    )

    await prepare_perp_price_history(backtester, SimpleNamespace(), config, SimpleNamespace(info=Mock()))

    assert backtester.data_provider.price_history_target == ("gmx_v2", "arbitrum", GMX_ETH_MARKET_ADDRESS)


@pytest.mark.asyncio
async def test_strategy_attribute_market_address_is_discovered(monkeypatch: pytest.MonkeyPatch) -> None:
    _PreparedProvider.constructed.clear()
    monkeypatch.setattr(
        PerpPriceHistoryRegistry,
        "backtest_provider",
        classmethod(lambda cls, protocol: _PreparedProvider),
    )
    backtester = SimpleNamespace(
        data_provider=object(),
        _get_strategy_config_dict=lambda strategy: {"protocol": "gmx_v2", "market": "ETH/USD"},
    )
    strategy = SimpleNamespace(market_address=GMX_ETH_MARKET_ADDRESS)
    start = datetime(2025, 1, 1, tzinfo=UTC)
    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=200),
        chain="arbitrum",
        timeframe="auto",
    )

    await prepare_perp_price_history(backtester, strategy, config, SimpleNamespace(info=Mock()))

    assert backtester.data_provider.price_history_target == ("gmx_v2", "arbitrum", GMX_ETH_MARKET_ADDRESS)


@pytest.mark.asyncio
async def test_malformed_market_address_fails_preflight_instead_of_label_fallback() -> None:
    backtester = SimpleNamespace(
        data_provider=object(),
        _get_strategy_config_dict=lambda strategy: {
            "protocol": "gmx_v2",
            "market": "ETH/USD",
            "market_address": "ETH/USD",
        },
    )
    start = datetime(2025, 1, 1, tzinfo=UTC)
    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=200),
        chain="arbitrum",
        timeframe="auto",
    )

    with pytest.raises(PreflightValidationError, match="address-first market contract"):
        await prepare_perp_price_history(backtester, SimpleNamespace(), config, SimpleNamespace(info=Mock()))


def test_funding_targets_keep_the_pair_label_when_an_address_is_declared() -> None:
    """The funding-history lane is symbol-keyed; the address never leaks into it."""
    from almanak.framework.backtesting.pnl._engine_helpers import declared_perp_price_history_targets

    targets = declared_perp_price_history_targets(
        SimpleNamespace(),
        {"protocol": "gmx_v2", "market": "ETH/USD", "market_address": GMX_ETH_MARKET_ADDRESS},
    )

    assert targets == (("gmx_v2", "ETH/USD"),)


@pytest.mark.asyncio
async def test_manifest_chain_metadata_resolves_strictly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        PerpPriceHistoryRegistry,
        "declared_chains",
        classmethod(lambda cls, protocol: ("arbitrium",)),
    )
    backtester = SimpleNamespace(
        data_provider=object(),
        _get_strategy_config_dict=lambda strategy: {"protocol": "gmx_v2", "market": "DOGE/USD"},
    )
    start = datetime(2025, 1, 1, tzinfo=UTC)
    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=1),
        chain="arbitrum",
        timeframe="auto",
    )

    with pytest.raises(ValueError, match="Unknown chain: 'arbitrium'"):
        await prepare_perp_price_history(
            backtester,
            SimpleNamespace(),
            config,
            SimpleNamespace(info=Mock()),
        )
