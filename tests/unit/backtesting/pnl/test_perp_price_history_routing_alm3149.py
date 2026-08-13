"""Engine routing for connector-declared perp price history (ALM-3149)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.connectors._strategy_base.perp_price_history_registry import PerpPriceHistoryRegistry
from almanak.framework.backtesting.pnl._engine_helpers import prepare_perp_price_history
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import HistoricalCoverage
from almanak.framework.backtesting.pnl.engine import PnLBacktester
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
    logger = SimpleNamespace(info=Mock(), warning=Mock())

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

    with pytest.raises(PreflightValidationError, match="coverage-aware spot token provider"):
        await prepare_perp_price_history(
            backtester,
            SimpleNamespace(),
            config,
            SimpleNamespace(info=Mock()),
        )


@pytest.mark.asyncio
async def test_auto_spot_strategy_uses_coverage_aware_token_provider() -> None:
    class SpotProvider:
        def __init__(self) -> None:
            self.requested_intervals: list[int] = []

        async def get_price_coverage(self, token, start, end, interval_seconds):
            self.requested_intervals.append(interval_seconds)
            return HistoricalCoverage(
                status="partial" if interval_seconds < 3600 else "full",
                requested_start=start,
                requested_end=end,
                first_available_at=start,
                last_available_at=end,
                earliest_contiguous_at=start,
                coverage_ratio=Decimal("0.5") if interval_seconds < 3600 else Decimal("1"),
                provider="spot_fixture",
                source_id=str(token),
                interval_seconds=interval_seconds,
                observed_interval_seconds=3600,
            )

    provider = SpotProvider()
    backtester = PnLBacktester(data_provider=provider, fee_models={}, slippage_models={})
    start = datetime(2025, 1, 1, tzinfo=UTC)
    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=1),
        chain="bsc",
        timeframe="auto",
    )
    logger = SimpleNamespace(info=Mock(), warning=Mock())

    await prepare_perp_price_history(backtester, SimpleNamespace(), config, logger)

    assert backtester.data_provider is provider
    assert config.resolved_timeframe == "1h"
    assert config.interval_seconds == 3600
    assert provider.requested_intervals == [60, 3600]
    logger.info.assert_called_once_with(
        "Resolved timeframe='auto' to provider-validated spot price cadence '1h' "
        "without changing the 3600s simulation tick cadence"
    )

    _, _, check, _ = await backtester._preflight_token_availability(config)

    assert check.passed
    assert provider.requested_intervals == [60, 3600]

    replay_provider = SpotProvider()
    replay = PnLBacktestConfig.from_dict(config.to_dict())
    replay_backtester = PnLBacktester(data_provider=replay_provider, fee_models={}, slippage_models={})

    await prepare_perp_price_history(replay_backtester, SimpleNamespace(), replay, logger)

    _, _, replay_check, _ = await replay_backtester._preflight_token_availability(replay)

    assert replay.resolved_timeframe == "1h"
    assert replay_check.passed
    assert replay_provider.requested_intervals == [3600]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected_code"),
    [("none", "NO_PRICE_HISTORY"), ("unknown", "PRICE_PROVIDER_UNAVAILABLE")],
)
async def test_auto_spot_preserves_terminal_coverage_diagnostics(status: str, expected_code: str) -> None:
    class TerminalCoverageProvider:
        async def get_price_coverage(self, token, start, end, interval_seconds):
            return HistoricalCoverage(
                status=status,
                requested_start=start,
                requested_end=end,
                first_available_at=None,
                last_available_at=None,
                earliest_contiguous_at=None,
                coverage_ratio=Decimal("0"),
                provider="spot_fixture",
                source_id=str(token),
                interval_seconds=interval_seconds,
                observed_interval_seconds=None,
            )

    start = datetime(2025, 1, 1, tzinfo=UTC)
    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=1),
        chain="bsc",
        timeframe="auto",
    )
    backtester = PnLBacktester(data_provider=TerminalCoverageProvider(), fee_models={}, slippage_models={})

    with pytest.raises(PreflightValidationError) as raised:
        await backtester.prepare_spot_price_history(config)

    assert raised.value.code == expected_code


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

    assert [(target.protocol, target.market, target.market_address) for target in targets] == [
        ("gmx_v2", "ETH/USD", GMX_ETH_MARKET_ADDRESS)
    ]


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
            SimpleNamespace(info=Mock(), warning=Mock()),
        )
