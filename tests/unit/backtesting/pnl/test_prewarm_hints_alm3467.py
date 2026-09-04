"""Config-key prewarm HINTS: they prewarm what is visible and never refuse a run (ALM-3467).

Identity is never *required* from config: pools and perp markets are
authenticated at first use from the intent or read that names them. The
generated shapes (``pool`` / ``swap_pool``, ``market`` +
``market_address``, the perp basket) are read only to prewarm the same venue
planes before tick 1 that a typed declaration would, so previously
config-declared strategies keep identical results. A hint that is malformed,
unsupported or unservable is dropped with a warning.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.connectors._strategy_base.perp_price_history_registry import PerpPriceHistoryRegistry
from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl._engine_helpers import (
    _prewarm_declared_funding_history,
    coverage_aware_default_timeframe,
    hinted_perp_price_history_targets,
    prepare_perp_price_history,
)
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.providers.snapshot_pool_state import (
    HistoricalPoolStatePoint,
    hinted_historical_pool_state_target,
)
from almanak.framework.data.interfaces import DataSourceUnavailable

POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"
GMX_ETH = "0x70d95587d40a2caf56bd97485ab3eec10bee6336"
START = datetime(2024, 1, 1, tzinfo=UTC)


def _lp_metadata(*protocols: str) -> SimpleNamespace:
    return SimpleNamespace(supported_protocols=list(protocols), intent_types=["LP_OPEN", "HOLD"])


# ---------------------------------------------------------------- pool hints


@pytest.mark.parametrize("key", ["pool", "swap_pool"])
def test_pool_hint_reads_supported_generated_address_key(key: str) -> None:
    strategy = SimpleNamespace(STRATEGY_METADATA=_lp_metadata("uniswap_v3"))
    target = hinted_historical_pool_state_target(
        strategy, {key: POOL, "protocol": "uniswap_v3"}, default_chain="arbitrum"
    )
    assert target is not None
    assert target.key == ("arbitrum", "uniswap_v3", POOL)


def test_pool_hint_takes_the_venue_from_metadata_when_protocol_key_is_the_lp_leg_of_a_dual_strategy() -> None:
    """The ALM-3467 shape: LP protocol at top level, pool address under a bespoke key, no hook."""
    strategy = SimpleNamespace(STRATEGY_METADATA=_lp_metadata("uniswap_v3", "gmx_v2"))
    target = hinted_historical_pool_state_target(
        strategy, {"pool": POOL, "protocol": "uniswap_v3", "gmx_protocol": "gmx_v2"}, default_chain="arbitrum"
    )
    assert target is not None and target.protocol == "uniswap_v3"


@pytest.mark.parametrize(
    "config",
    [
        {"pool": "WETH/USDC/500", "protocol": "uniswap_v3"},  # symbolic: nothing to prewarm
        {"pool": POOL},  # no venue anywhere
        {"pool": POOL, "protocol": "curve"},  # venue without historical pool state
        {"pool": "not-an-address", "protocol": "uniswap_v3"},
        {"pool_address": POOL, "protocol": "uniswap_v3"},  # not a bridge key: first use, never a hint
    ],
)
def test_pool_hint_never_raises_on_unusable_shapes(config: dict) -> None:
    # No venue anywhere but the config key under test: metadata is deliberately empty so a
    # config without a usable protocol has nothing to fall back on.
    strategy = SimpleNamespace(STRATEGY_METADATA=_lp_metadata())
    assert hinted_historical_pool_state_target(strategy, config, default_chain="arbitrum") is None


def test_pool_hint_takes_the_venue_from_metadata_alone() -> None:
    strategy = SimpleNamespace(STRATEGY_METADATA=_lp_metadata("uniswap_v3"))
    target = hinted_historical_pool_state_target(strategy, {"pool": POOL}, default_chain="arbitrum")
    assert target is not None and target.protocol == "uniswap_v3"


@pytest.mark.asyncio
async def test_pool_hint_prewarms_before_tick_1(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []

    def fetcher(**kwargs):
        calls.append(kwargs)
        targets = range(kwargs["start_ts"], kwargs["end_ts"] + 1, kwargs["interval_secs"])
        return [
            HistoricalPoolStatePoint(
                timestamp=t - 5,
                block_number=1 + i,
                sqrt_price_x96=2**96,
                tick=0,
                liquidity=10,
                token0="0x" + "1" * 40,
                token1="0x" + "2" * 40,
                token0_decimals=18,
                token1_decimals=6,
                fee_tier=500,
                reserve0_raw=10**18,
                reserve1_raw=10**6,
                source="on_chain_archive",
            )
            for i, t in enumerate(targets)
        ]

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.snapshot_pool_state.fetch_historical_pool_state_points", fetcher
    )
    strategy = SimpleNamespace(STRATEGY_METADATA=_lp_metadata("uniswap_v3"))
    config = PnLBacktestConfig(
        start_time=START, end_time=START + timedelta(hours=2), interval_seconds=3600, chain="arbitrum"
    )
    source = await _engine_helpers._prepare_declared_historical_pool_state(
        strategy, {"pool": POOL, "protocol": "uniswap_v3"}, config, None
    )
    assert source is not None and not source.is_empty
    assert len(calls) == 1 and calls[0]["pool_address"] == POOL
    assert source.pool_descriptor("arbitrum", "uniswap_v3", POOL) is not None


@pytest.mark.asyncio
async def test_pool_hint_that_cannot_be_served_does_not_refuse_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing(**kwargs):
        raise DataSourceUnavailable(source="gateway", reason="archive timed out")

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.snapshot_pool_state.fetch_historical_pool_state_points", failing
    )
    strategy = SimpleNamespace(STRATEGY_METADATA=_lp_metadata("uniswap_v3"))
    config = PnLBacktestConfig(
        start_time=START, end_time=START + timedelta(hours=2), interval_seconds=3600, chain="arbitrum"
    )
    source = await _engine_helpers._prepare_declared_historical_pool_state(
        strategy, {"pool": POOL, "protocol": "uniswap_v3"}, config, None
    )
    assert source is None  # the loop creates an empty plane; first use authenticates later


# ---------------------------------------------------------------- perp hints


def _perp_metadata(*protocols: str) -> SimpleNamespace:
    return SimpleNamespace(supported_protocols=list(protocols), intent_types=["PERP_OPEN", "PERP_CLOSE", "HOLD"])


def test_perp_hint_ignores_top_level_protocol_precedence() -> None:
    """The refusal that hit ALM-3467's second run: LP protocol at top level masked the GMX market."""
    strategy = SimpleNamespace(STRATEGY_METADATA=_perp_metadata("uniswap_v3", "gmx_v2"))
    targets = hinted_perp_price_history_targets(
        strategy, {"protocol": "uniswap_v3", "market": "ETH/USD", "market_address": GMX_ETH}
    )
    assert [(t.protocol, t.market, t.market_address) for t in targets] == [("gmx_v2", "ETH/USD", GMX_ETH)]


def test_perp_hint_reads_strategy_attributes_and_bespoke_free_keys() -> None:
    strategy = SimpleNamespace(STRATEGY_METADATA=_perp_metadata("gmx_v2"), market="DOGE/USD")
    targets = hinted_perp_price_history_targets(strategy, {})
    assert [(t.protocol, t.market, t.market_address) for t in targets] == [("gmx_v2", "DOGE/USD", None)]
    # A bespoke key is not a hint (and not a refusal either): first use handles it.
    assert (
        hinted_perp_price_history_targets(
            SimpleNamespace(STRATEGY_METADATA=_perp_metadata("gmx_v2")), {"hedge_market_address": GMX_ETH}
        )
        == ()
    )


@pytest.mark.parametrize(
    "config",
    [
        {"protocol": "gmx_v2", "markets": [], "perp_markets": {"SOL": {}}},
        {"protocol": "gmx_v2", "markets": "SOL/USD", "perp_markets": {"SOL": {}}},
        {"protocol": "gmx_v2", "markets": ["not-a-pair"], "perp_markets": {"SOL": {}}},
        {"protocol": "gmx_v2", "markets": ["SOL/USD", "SOL-USD"], "perp_markets": {"SOL": {}}},
        {"protocol": "gmx_v2", "markets": ["SOL/USD"], "perp_markets": []},
        {"protocol": "gmx_v2", "markets": ["SOL/USD"], "perp_markets": {"DOGE": {}}},
        {"protocol": "gmx_v2", "markets": ["SOL/USD"], "perp_markets": {"SOL": {}, "DOGE": {}}},
        {"protocol": "gmx_v2", "markets": ["SOL/USD"], "perp_markets": {"SOL": "bad"}},
        {"protocol": "gmx_v2", "markets": ["SOL/USD"], "perp_markets": {"sol": {}, "SOL": {}}},
        {
            "protocol": "gmx_v2",
            "markets": ["SOL/USD"],
            "perp_markets": {"SOL": {"index_symbol": "DOGE", "market_token": GMX_ETH, "index_token": GMX_ETH}},
        },
        {
            "protocol": "gmx_v2",
            "markets": ["SOL/USD"],
            "perp_markets": {"SOL": {"index_symbol": "SOL", "market_token": "bad", "index_token": GMX_ETH}},
        },
        {
            "protocol": "gmx_v2",
            "markets": ["SOL/USD"],
            "perp_markets": {"SOL": {"index_symbol": "SOL", "market_token": GMX_ETH, "index_token": "bad"}},
        },
    ],
)
def test_perp_hint_never_raises_on_malformed_basket(config: dict) -> None:
    assert hinted_perp_price_history_targets(SimpleNamespace(), config) == ()


def test_hinted_market_keeps_the_hosted_auto_cadence_default() -> None:
    strategy = SimpleNamespace(STRATEGY_METADATA=_perp_metadata("gmx_v2"), market="DOGE/USD")
    assert coverage_aware_default_timeframe(strategy) == "auto"


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


class _FailingProvider(_PreparedProvider):
    async def prepare_backtest(self, config: PnLBacktestConfig) -> str:
        raise DataSourceUnavailable(source="gateway", reason="no native cadence covers the window")


def _config() -> PnLBacktestConfig:
    return PnLBacktestConfig(start_time=START, end_time=START + timedelta(days=30), chain="arbitrum", timeframe="auto")


@pytest.mark.asyncio
async def test_perp_hint_prewarms_the_venue_plane_like_a_declaration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        PerpPriceHistoryRegistry, "backtest_provider", classmethod(lambda cls, protocol: _PreparedProvider)
    )
    strategy = SimpleNamespace(STRATEGY_METADATA=_perp_metadata("gmx_v2"))
    backtester = SimpleNamespace(
        data_provider=object(),
        _get_strategy_config_dict=lambda s: {"protocol": "gmx_v2", "market": "ETH/USD", "market_address": GMX_ETH},
    )
    await prepare_perp_price_history(backtester, strategy, _config(), SimpleNamespace(info=Mock(), warning=Mock()))
    assert backtester.data_provider.price_history_target == ("gmx_v2", "arbitrum", GMX_ETH)
    prepared = backtester._prepared_perp_price_history_targets
    assert [(target.protocol, target.market, target.market_address) for target in prepared] == [
        ("gmx_v2", "ETH/USD", GMX_ETH)
    ]

    class _FundingSource:
        history_capable = True

        def __init__(self) -> None:
            self.calls: list[tuple[str, str, str]] = []

        async def materialize_history(self, protocol: str, market: str, market_address: str) -> int:
            self.calls.append((protocol, market, market_address))
            return 2

    source = _FundingSource()
    await _prewarm_declared_funding_history(source, strategy, {}, prepared_targets=prepared)
    assert source.calls == [("gmx_v2", "ETH/USD", GMX_ETH)]


@pytest.mark.asyncio
async def test_perp_hint_that_cannot_be_prepared_keeps_the_spot_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        PerpPriceHistoryRegistry, "backtest_provider", classmethod(lambda cls, protocol: _FailingProvider)
    )
    strategy = SimpleNamespace(STRATEGY_METADATA=_perp_metadata("gmx_v2"))
    fallback = object()
    backtester = SimpleNamespace(
        data_provider=fallback,
        _get_strategy_config_dict=lambda s: {"protocol": "gmx_v2", "market": "ETH/USD", "market_address": GMX_ETH},
    )
    logger = SimpleNamespace(info=Mock(), warning=Mock())
    await prepare_perp_price_history(backtester, strategy, _config(), logger)  # no raise
    assert backtester.data_provider is fallback
    assert backtester._prepared_perp_price_history_targets == ()
    assert "prepared at first use" in logger.warning.call_args.args[0]


@pytest.mark.asyncio
async def test_skipped_perp_hint_still_resolves_the_auto_cadence() -> None:
    """A hint that cannot be prepared must not change the run's documented cadence.

    ``timeframe="auto"`` resolves against the spot plane on the undeclared
    route; a config key nobody meant as a declaration must land in the same
    place rather than leaving the run on the fallback cadence.
    """
    strategy = SimpleNamespace(STRATEGY_METADATA=_perp_metadata("gmx_v2"))
    prepared: list[object] = []

    class _CoverageAwareProvider:
        def get_price_coverage(self, *args: object, **kwargs: object) -> object:  # pragma: no cover - probe only
            return None

    async def prepare_spot_price_history(config: object) -> SimpleNamespace:
        prepared.append(config)
        return SimpleNamespace(resolved_timeframe="1h")

    backtester = SimpleNamespace(
        data_provider=_CoverageAwareProvider(),
        prepare_spot_price_history=prepare_spot_price_history,
        _get_strategy_config_dict=lambda s: {"protocol": "gmx_v2", "market": "ETH/USD", "market_address": GMX_ETH},
    )
    logger = SimpleNamespace(info=Mock(), warning=Mock())

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            PerpPriceHistoryRegistry, "backtest_provider", classmethod(lambda cls, protocol: _FailingProvider)
        )
        await prepare_perp_price_history(backtester, strategy, _config(), logger)

    assert len(prepared) == 1  # the spot cadence was resolved after the hint was skipped
    assert "prepared at first use" in logger.warning.call_args.args[0]
    assert "1h" in logger.info.call_args.args[0]


@pytest.mark.asyncio
async def test_typed_declaration_still_refuses_loudly(monkeypatch: pytest.MonkeyPatch) -> None:
    """A hook is a contract the author wrote; a broken one is still a preflight error."""
    from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError
    from almanak.framework.backtesting.pnl.perp_targets import PerpPriceHistoryTarget

    monkeypatch.setattr(
        PerpPriceHistoryRegistry, "backtest_provider", classmethod(lambda cls, protocol: _FailingProvider)
    )
    strategy = SimpleNamespace(
        STRATEGY_METADATA=_perp_metadata("gmx_v2"),
        backtest_perp_price_history_targets=lambda: [PerpPriceHistoryTarget(protocol="gmx_v2", market="ETH/USD")],
    )
    backtester = SimpleNamespace(data_provider=object(), _get_strategy_config_dict=lambda s: {})
    with pytest.raises(PreflightValidationError):
        await prepare_perp_price_history(backtester, strategy, _config(), SimpleNamespace(info=Mock(), warning=Mock()))
