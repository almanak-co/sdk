"""Preflight guard for perp strategies with no discoverable market declaration.

A perp-trading strategy on a connector-native price venue whose market is
declared under a non-standard key (found live: ``hedge_market_address``) used
to sail through preflight with no candle route at all — the venue market
catalog was never primed, so every PERP_OPEN was rejected at fill time as
unpriceable. These tests pin the guard that names that misconfiguration in
one line at preflight, and pin that properly declared, non-perp, and
non-registry-venue strategies are unaffected.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.connectors._strategy_base.perp_price_history_registry import PerpPriceHistoryRegistry
from almanak.core.intent_types import IntentType
from almanak.framework.backtesting.pnl._engine_helpers import prepare_perp_price_history
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError

GMX_ETH_MARKET_ADDRESS = "0x70d95587d40a2caf56bd97485ab3eec10bee6336"


class _PreparedProvider:
    def __init__(self, *, fallback: object, chain: str, market: str, venue: str) -> None:
        self.fallback = fallback
        self.price_history_target = (venue, chain, market)

    @classmethod
    def for_backtest(cls, *, fallback: object, chain: str, market: str, venue: str) -> _PreparedProvider:
        return cls(fallback=fallback, chain=chain, market=market, venue=venue)

    async def prepare_backtest(self, config: PnLBacktestConfig) -> str:
        config.apply_resolved_timeframe("4h", 14_400)
        return "4h"


def _perp_metadata(supported_protocols: list[str]) -> SimpleNamespace:
    return SimpleNamespace(
        intent_types=[IntentType.PERP_OPEN, IntentType.PERP_CLOSE, IntentType.HOLD],
        supported_protocols=supported_protocols,
    )


def _backtester(strategy_config: dict[str, object]) -> SimpleNamespace:
    return SimpleNamespace(
        data_provider=object(),
        _get_strategy_config_dict=lambda strategy: strategy_config,
    )


def _config(**overrides: object) -> PnLBacktestConfig:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    kwargs: dict[str, object] = {
        "start_time": start,
        "end_time": start + timedelta(days=30),
        "chain": "arbitrum",
        "interval_seconds": 3600,
    }
    kwargs.update(overrides)
    return PnLBacktestConfig(**kwargs)


@pytest.mark.asyncio
async def test_perp_strategy_without_market_declaration_fails_preflight() -> None:
    """The incident shape: perp intents + gmx_v2 + a bespoke market key only.

    The explicit (non-auto) timeframe is the lane the gap lived in — the
    timeframe='auto' lane already raised.
    """
    strategy = SimpleNamespace(
        STRATEGY_METADATA=_perp_metadata(["gmx_v2"]),
        hedge_market_address=GMX_ETH_MARKET_ADDRESS,
    )
    backtester = _backtester({"hedge_market_address": GMX_ETH_MARKET_ADDRESS})

    with pytest.raises(PreflightValidationError, match="'market_address'"):
        await prepare_perp_price_history(
            backtester,
            strategy,
            _config(),
            SimpleNamespace(info=Mock(), warning=Mock()),
        )


@pytest.mark.asyncio
async def test_properly_declared_perp_strategy_is_unaffected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        PerpPriceHistoryRegistry,
        "backtest_provider",
        classmethod(lambda cls, protocol: _PreparedProvider),
    )
    strategy = SimpleNamespace(STRATEGY_METADATA=_perp_metadata(["gmx_v2"]))
    backtester = _backtester({"market": "ETH/USD", "market_address": GMX_ETH_MARKET_ADDRESS})
    logger = SimpleNamespace(info=Mock(), warning=Mock())

    await prepare_perp_price_history(backtester, strategy, _config(), logger)

    assert backtester.data_provider.price_history_target == ("gmx_v2", "arbitrum", GMX_ETH_MARKET_ADDRESS)
    logger.warning.assert_not_called()


@pytest.mark.asyncio
async def test_non_perp_strategy_without_declaration_is_unaffected() -> None:
    strategy = SimpleNamespace(
        STRATEGY_METADATA=SimpleNamespace(
            intent_types=[IntentType.LP_OPEN, IntentType.HOLD],
            supported_protocols=["gmx_v2"],
        ),
    )
    backtester = _backtester({})
    fallback = backtester.data_provider

    await prepare_perp_price_history(
        backtester,
        strategy,
        _config(),
        SimpleNamespace(info=Mock(), warning=Mock()),
    )

    assert backtester.data_provider is fallback


@pytest.mark.asyncio
async def test_perp_strategy_on_non_registry_venue_is_unaffected() -> None:
    """A venue without a connector-native price plane (hyperliquid) never trips the guard."""
    strategy = SimpleNamespace(STRATEGY_METADATA=_perp_metadata(["hyperliquid"]))
    backtester = _backtester({"market": "ETH/USD"})
    fallback = backtester.data_provider

    await prepare_perp_price_history(
        backtester,
        strategy,
        _config(),
        SimpleNamespace(info=Mock(), warning=Mock()),
    )

    assert backtester.data_provider is fallback


@pytest.mark.asyncio
async def test_explicit_foreign_protocol_masking_the_perp_venue_fails_preflight() -> None:
    """A declared market routed to a non-perp protocol is the same dead end.

    An explicit ``protocol`` declaration outranks strategy metadata in the
    ladder, so a dual-protocol strategy that pins its LP protocol silently
    loses the candle route even with 'market'/'market_address' declared.
    """
    strategy = SimpleNamespace(STRATEGY_METADATA=_perp_metadata(["uniswap_v3", "gmx_v2"]))
    backtester = _backtester(
        {
            "protocol": "uniswap_v3",
            "market": "ETH/USD",
            "market_address": GMX_ETH_MARKET_ADDRESS,
        }
    )

    with pytest.raises(PreflightValidationError, match="did not resolve to a perp-capable protocol"):
        await prepare_perp_price_history(
            backtester,
            strategy,
            _config(),
            SimpleNamespace(info=Mock(), warning=Mock()),
        )


@pytest.mark.asyncio
async def test_disabled_price_plane_seam_disables_the_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """The guard gates on has() — the route filter's own predicate.

    Network-free tiers (the trust matrix's observation-lifecycle cell) disable
    the venue-native price plane by patching ``PerpPriceHistoryRegistry.has``
    to False and price fills off a synthetic provider with a hand-primed
    market catalog. The guard must follow that seam, not re-derive
    perp-capability through ``canonical()`` and fail a tier the filter
    deliberately routed away from the candle lane.
    """
    monkeypatch.setattr(PerpPriceHistoryRegistry, "has", classmethod(lambda cls, protocol: False))
    strategy = SimpleNamespace(STRATEGY_METADATA=_perp_metadata(["gmx_v2"]))
    backtester = _backtester({"market": "ETH/USD"})
    fallback = backtester.data_provider

    await prepare_perp_price_history(
        backtester,
        strategy,
        _config(),
        SimpleNamespace(info=Mock(), warning=Mock()),
    )

    assert backtester.data_provider is fallback


@pytest.mark.asyncio
async def test_label_only_declaration_warns_but_proceeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """'market' without 'market_address' works but earns an address-first nudge."""
    monkeypatch.setattr(
        PerpPriceHistoryRegistry,
        "backtest_provider",
        classmethod(lambda cls, protocol: _PreparedProvider),
    )
    strategy = SimpleNamespace(STRATEGY_METADATA=_perp_metadata(["gmx_v2"]))
    backtester = _backtester({"market": "ETH/USD"})
    logger = SimpleNamespace(info=Mock(), warning=Mock())

    await prepare_perp_price_history(backtester, strategy, _config(), logger)

    assert backtester.data_provider.price_history_target == ("gmx_v2", "arbitrum", "ETH/USD")
    logger.warning.assert_called_once()
    assert "market_address" in logger.warning.call_args.args[0]
