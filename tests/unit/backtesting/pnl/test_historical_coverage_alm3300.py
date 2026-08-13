"""Range-aware token coverage and exact-contract chain identity (ALM-3300)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from almanak.framework.backtesting.pnl import HistoricalCoverage as PublicHistoricalCoverage
from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import (
    OHLCV,
    HistoricalCoverage,
    HistoricalCoverageProvider,
    HistoricalDataCapability,
    HistoricalDataConfig,
    HistoricalDataProvider,
    token_ref_display,
)
from almanak.framework.backtesting.pnl.engine import PnLBacktester, discover_token_coverage
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError
from almanak.framework.backtesting.pnl.logging_utils import BacktestLogger
from almanak.framework.backtesting.pnl.providers.coingecko import (
    _OHLCV_RANGE_CACHE_MAX_ENTRIES,
    CoinGeckoDataProvider,
)
from almanak.framework.backtesting.pnl.providers.coingecko_gateway import (
    CoinGeckoGatewayProviderError,
    CoinGeckoGatewayUnavailableError,
)
from almanak.framework.backtesting.pnl.readiness import _blocker
from tests.backtesting_funding import pnl_token_funding
from tests.unit.backtesting.pnl._mocks import MockDataProvider

_START = datetime(2026, 7, 15, tzinfo=UTC)
_END = _START + timedelta(hours=6)
_TOKEN_A = "0x1111111111111111111111111111111111111111"
_TOKEN_B = "0x2222222222222222222222222222222222222222"
_USDC_ARBITRUM = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"


def test_legacy_provider_still_satisfies_base_protocol_without_coverage_capability() -> None:
    provider = MockDataProvider()

    assert isinstance(provider, HistoricalDataProvider)
    assert not isinstance(provider, HistoricalCoverageProvider)


def test_historical_coverage_is_exported_with_its_provider_protocol() -> None:
    assert PublicHistoricalCoverage is HistoricalCoverage


def _candles(*hours: int) -> list[OHLCV]:
    return [
        OHLCV(
            timestamp=_START + timedelta(hours=hour),
            open=Decimal("10"),
            high=Decimal("10"),
            low=Decimal("10"),
            close=Decimal("10"),
        )
        for hour in hours
    ]


@pytest.mark.asyncio
async def test_coingecko_mid_window_listing_is_partial_and_range_fetch_is_reused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = CoinGeckoDataProvider(api_key="test", min_request_interval=0, use_gateway=True)

    async def request(endpoint: str, params: dict[str, str]) -> dict:
        if "/contract/" in endpoint:
            return {"id": "spacex-bstocks-tokenized-stock"}
        assert endpoint == "/coins/spacex-bstocks-tokenized-stock/market_chart/range"
        return {
            "prices": [[int((_START + timedelta(hours=hour)).timestamp() * 1000), 10 + hour] for hour in range(2, 7)]
        }

    assert provider._gateway_transport is not None
    gateway_request = AsyncMock(side_effect=request)
    monkeypatch.setattr(provider._gateway_transport, "request", gateway_request)
    token = ("bsc", "0xbe9d156892e55e7154bcd3cb0fea677f9d3103e1")

    coverage = await provider.get_price_coverage(token, _START, _END, 3600)
    range_calls_after_coverage = sum(
        call.args[0].endswith("/market_chart/range") for call in gateway_request.await_args_list
    )
    candles = await provider.get_ohlcv(token, _START, _END, 3600)

    assert coverage.status == "partial"
    assert coverage.source_id == "spacex-bstocks-tokenized-stock"
    assert coverage.first_available_at == _START + timedelta(hours=2)
    assert coverage.earliest_contiguous_at == _START + timedelta(hours=2)
    assert coverage.last_available_at == _END
    assert len(candles) == 5
    requested_endpoints = [call.args[0] for call in gateway_request.await_args_list]
    assert range_calls_after_coverage == 2  # requested range + honest prior-candle seed probe
    assert (
        sum(endpoint.endswith("/market_chart/range") for endpoint in requested_endpoints) == range_calls_after_coverage
    )


@pytest.mark.asyncio
async def test_coingecko_range_cache_is_bounded_lru() -> None:
    provider = CoinGeckoDataProvider(api_key="test", min_request_interval=0, use_gateway=False)
    provider._resolve_token_id = AsyncMock(return_value="asset")

    async def request(endpoint: str, params: dict[str, str]) -> dict:
        assert endpoint == "/coins/asset/market_chart/range"
        return {"prices": [[int(params["from"]) * 1000, 10]]}

    provider._make_request = AsyncMock(side_effect=request)
    for offset in range(_OHLCV_RANGE_CACHE_MAX_ENTRIES):
        start = _START + timedelta(days=offset)
        await provider.get_ohlcv(("bsc", _TOKEN_A), start, start + timedelta(hours=1), 3600)

    first_start = _START
    await provider.get_ohlcv(("bsc", _TOKEN_A), first_start, first_start + timedelta(hours=1), 3600)
    overflow_start = _START + timedelta(days=_OHLCV_RANGE_CACHE_MAX_ENTRIES)
    await provider.get_ohlcv(("bsc", _TOKEN_A), overflow_start, overflow_start + timedelta(hours=1), 3600)

    first_key = (
        "asset",
        int(first_start.timestamp()),
        int((first_start + timedelta(hours=1)).timestamp()),
        0,
    )
    second_start = _START + timedelta(days=1)
    second_key = (
        "asset",
        int(second_start.timestamp()),
        int((second_start + timedelta(hours=1)).timestamp()),
        0,
    )
    assert len(provider._ohlcv_range_cache) == _OHLCV_RANGE_CACHE_MAX_ENTRIES
    assert first_key in provider._ohlcv_range_cache
    assert second_key not in provider._ohlcv_range_cache


@pytest.mark.asyncio
async def test_unaligned_start_uses_prior_candle_without_lookahead() -> None:
    start = _START + timedelta(minutes=30)
    end = start + timedelta(hours=2)
    provider = CoinGeckoDataProvider(api_key="test", min_request_interval=0, use_gateway=True)
    provider._resolve_token_id = AsyncMock(return_value="asset")
    provider._get_ohlcv = AsyncMock(
        side_effect=[
            [
                OHLCV(
                    timestamp=_START + timedelta(hours=1),
                    open=Decimal("10"),
                    high=Decimal("10"),
                    low=Decimal("10"),
                    close=Decimal("10"),
                ),
                OHLCV(
                    timestamp=_START + timedelta(hours=2),
                    open=Decimal("10"),
                    high=Decimal("10"),
                    low=Decimal("10"),
                    close=Decimal("10"),
                ),
            ],
            [
                OHLCV(timestamp=_START, open=Decimal("9"), high=Decimal("9"), low=Decimal("9"), close=Decimal("9")),
            ],
        ]
    )

    coverage = await provider.get_price_coverage(("bsc", _TOKEN_A), start, end, 3600)

    assert coverage.status == "full"
    assert coverage.first_available_at == _START
    assert coverage.last_available_at == _START + timedelta(hours=2)
    assert coverage.coverage_ratio == Decimal("1")


@pytest.mark.asyncio
async def test_prefetch_uses_resolved_price_cadence_without_changing_ticks() -> None:
    provider = CoinGeckoDataProvider(api_key="test", min_request_interval=0, use_gateway=False)
    provider.get_ohlcv = AsyncMock(
        return_value=[
            OHLCV(timestamp=_START, open=Decimal("10"), high=Decimal("10"), low=Decimal("10"), close=Decimal("10")),
            OHLCV(
                timestamp=_START + timedelta(hours=1),
                open=Decimal("11"),
                high=Decimal("11"),
                low=Decimal("11"),
                close=Decimal("11"),
            ),
        ]
    )
    config = HistoricalDataConfig(
        start_time=_START,
        end_time=_END,
        interval_seconds=300,
        price_interval_seconds=3600,
        tokens=[("bsc", _TOKEN_A)],
        chains=["bsc"],
    )

    await provider._prefetch_ohlcv_data(config)

    provider.get_ohlcv.assert_awaited_once_with(("bsc", _TOKEN_A), _START, _END, 3600)
    assert config.interval_seconds == 300


def test_config_rejects_cross_chain_token_refs_direct_and_deserialized() -> None:
    with pytest.raises(ValueError, match="does not match backtest chain"):
        PnLBacktestConfig(
            start_time=_START,
            end_time=_END,
            chain="arbitrum",
            tokens=[("bsc", _TOKEN_A)],
        )

    payload = _config().to_dict()
    payload["tokens"] = [["bsc", _TOKEN_A]]
    with pytest.raises(ValueError, match="does not match backtest chain"):
        PnLBacktestConfig.from_dict(payload)


@pytest.mark.parametrize("token", [("", _TOKEN_A), ("arbitrum", ""), ("arbitrum", "   ")])
def test_config_rejects_empty_chain_qualified_token_components(token: tuple[str, str]) -> None:
    with pytest.raises(ValueError, match="chain and address cannot be empty"):
        PnLBacktestConfig(
            start_time=_START,
            end_time=_END,
            chain="arbitrum",
            tokens=[token],
        )


@pytest.mark.asyncio
async def test_preflight_rejects_cross_chain_token_ref_after_mutation() -> None:
    config = _config()
    config.tokens = [("bsc", _TOKEN_A)]
    backtester = PnLBacktester(data_provider=_CoverageProvider(), fee_models={}, slippage_models={})

    with pytest.raises(PreflightValidationError) as raised:
        await backtester._preflight_token_availability(config)

    assert raised.value.code == "TOKEN_CHAIN_MISMATCH"


def test_coverage_rejects_interior_gaps_missing_end_and_lookahead() -> None:
    gap = CoinGeckoDataProvider._coverage_from_candles(
        _candles(0, 1, 3, 4, 5, 6),
        start=_START,
        end=_END,
        interval_seconds=3600,
        source_id="asset",
    )
    missing_end = CoinGeckoDataProvider._coverage_from_candles(
        _candles(0, 1, 2, 3, 4, 5),
        start=_START,
        end=_END,
        interval_seconds=3600,
        source_id="asset",
    )
    future_first = CoinGeckoDataProvider._coverage_from_candles(
        [
            OHLCV(
                timestamp=_START + timedelta(minutes=30),
                open=Decimal("10"),
                high=Decimal("10"),
                low=Decimal("10"),
                close=Decimal("10"),
            ),
            *_candles(1, 2, 3, 4, 5, 6),
        ],
        start=_START,
        end=_END,
        interval_seconds=3600,
        source_id="asset",
    )
    coarse = CoinGeckoDataProvider._coverage_from_candles(
        _candles(0, 2, 4, 6),
        start=_START,
        end=_END,
        interval_seconds=3600,
        source_id="asset",
    )

    assert gap.status == "partial"
    assert gap.earliest_contiguous_at == _START + timedelta(hours=3)
    assert missing_end.status == "partial"
    assert missing_end.earliest_contiguous_at is None
    assert future_first.status == "partial"
    assert future_first.earliest_contiguous_at == _START + timedelta(hours=1)
    assert coarse.status == "partial"
    assert coarse.earliest_contiguous_at is None


def test_sparse_long_window_coverage_uses_exact_grid_arithmetic() -> None:
    end = _START + timedelta(days=3650)
    candles = [
        OHLCV(timestamp=_START, open=Decimal("10"), high=Decimal("10"), low=Decimal("10"), close=Decimal("10")),
        OHLCV(timestamp=end, open=Decimal("10"), high=Decimal("10"), low=Decimal("10"), close=Decimal("10")),
    ]

    covered, total, last_uncovered = CoinGeckoDataProvider._coverage_grid(
        candles,
        start=_START,
        end=end,
        interval_seconds=60,
    )

    assert covered == 2
    assert total == 3650 * 24 * 60 + 1
    assert last_uncovered == end - timedelta(minutes=1)


@pytest.mark.asyncio
async def test_completely_unknown_contract_has_no_availability_timestamps() -> None:
    provider = CoinGeckoDataProvider(api_key="test", min_request_interval=0, use_gateway=True)
    provider._resolve_token_id = AsyncMock(return_value=None)
    provider._stablecoin_fallback_cache_id = lambda token: None

    coverage = await provider.get_price_coverage(("bsc", _TOKEN_A), _START, _END, 3600)

    assert coverage.status == "none"
    assert coverage.first_available_at is None
    assert coverage.last_available_at is None
    assert coverage.earliest_contiguous_at is None


@pytest.mark.asyncio
async def test_coingecko_coverage_retains_local_operator_direct_fallback() -> None:
    provider = CoinGeckoDataProvider(api_key="test", min_request_interval=0, use_gateway=False)
    provider._resolve_token_id = AsyncMock(return_value="asset")
    provider._make_request = AsyncMock(
        return_value={
            "prices": [
                [int(_START.timestamp() * 1000), 10],
                [int(_END.timestamp() * 1000), 11],
            ]
        }
    )

    coverage = await provider.get_price_coverage(("bsc", _TOKEN_A), _START, _END, 3600)

    assert coverage.status == "partial"
    provider._make_request.assert_awaited_once()


@pytest.mark.asyncio
async def test_coingecko_coverage_fails_closed_when_gateway_cannot_serve_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = CoinGeckoDataProvider(api_key="test", min_request_interval=0, use_gateway=True)
    provider._resolve_token_id = AsyncMock(return_value="asset")
    assert provider._gateway_transport is not None
    gateway_request = AsyncMock(return_value=None)
    direct_session = AsyncMock(side_effect=AssertionError("direct HTTP must not run"))
    monkeypatch.setattr(provider._gateway_transport, "request", gateway_request)
    monkeypatch.setattr(provider, "_get_session", direct_session)

    with pytest.raises(CoinGeckoGatewayUnavailableError, match="unavailable via the configured gateway"):
        await provider.get_price_coverage(("bsc", _TOKEN_A), _START, _END, 3600)

    gateway_request.assert_awaited_once()
    direct_session.assert_not_awaited()


@pytest.mark.asyncio
async def test_coingecko_gateway_application_failure_is_not_missing_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = CoinGeckoDataProvider(api_key="test", min_request_interval=0, use_gateway=True)
    provider._resolve_token_id = AsyncMock(return_value="asset")
    assert provider._gateway_transport is not None
    monkeypatch.setattr(
        provider._gateway_transport,
        "request",
        AsyncMock(side_effect=CoinGeckoGatewayProviderError("gateway upstream 500")),
    )

    with pytest.raises(CoinGeckoGatewayProviderError, match="gateway upstream 500"):
        await provider.get_price_coverage(("bsc", _TOKEN_A), _START, _END, 3600)


@pytest.mark.asyncio
async def test_coingecko_prior_seed_gateway_failure_is_provider_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = CoinGeckoDataProvider(api_key="test", min_request_interval=0, use_gateway=True)
    provider._resolve_token_id = AsyncMock(return_value="asset")
    assert provider._gateway_transport is not None

    async def request(endpoint: str, params: dict[str, str]) -> dict:
        if int(params["from"]) < int(_START.timestamp()):
            raise CoinGeckoGatewayProviderError("seed gateway upstream 500")
        return {
            "prices": [
                [int((_START + timedelta(hours=hour)).timestamp() * 1000), 10 + hour]
                for hour in range(1, 7)
            ]
        }

    monkeypatch.setattr(provider._gateway_transport, "request", AsyncMock(side_effect=request))
    backtester = PnLBacktester(data_provider=provider, fee_models={}, slippage_models={})
    config = PnLBacktestConfig(
        start_time=_START,
        end_time=_END,
        interval_seconds=3600,
        chain="bsc",
        tokens=[_TOKEN_A],
    )

    with pytest.raises(PreflightValidationError) as raised:
        await backtester._preflight_token_availability(config)

    assert raised.value.code == "PRICE_PROVIDER_UNAVAILABLE"
    assert raised.value.details["error_type"] == "CoinGeckoGatewayProviderError"


@pytest.mark.asyncio
async def test_coverage_ranges_remain_pinned_until_large_basket_prefetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = CoinGeckoDataProvider(api_key="test", min_request_interval=0, use_gateway=True)
    provider._resolve_token_id = AsyncMock(side_effect=lambda token: token[1])
    assert provider._gateway_transport is not None

    async def request(endpoint: str, params: dict[str, str]) -> dict:
        return {
            "prices": [
                [int(params["from"]) * 1000, 10],
                [int(params["to"]) * 1000, 11],
            ]
        }

    gateway_request = AsyncMock(side_effect=request)
    monkeypatch.setattr(provider._gateway_transport, "request", gateway_request)
    tokens = [("bsc", f"0x{index:040x}") for index in range(1, _OHLCV_RANGE_CACHE_MAX_ENTRIES + 2)]

    discoveries = await discover_token_coverage(provider, tokens, _START, _END, 3600)

    calls_after_coverage = gateway_request.await_count
    assert all(item.coverage.status == "partial" for item in discoveries)
    assert len(provider._ohlcv_range_cache) == len(tokens)
    assert len(provider._coverage_range_pins) == len(tokens)

    await provider.get_ohlcv(tokens[0], _START, _END, 3600)

    assert gateway_request.await_count == calls_after_coverage
    assert len(provider._ohlcv_range_cache) == _OHLCV_RANGE_CACHE_MAX_ENTRIES
    assert len(provider._coverage_range_pins) == len(tokens) - 1

    provider.begin_price_coverage_batch()

    assert provider._coverage_range_pins == {}
    assert len(provider._ohlcv_range_cache) == _OHLCV_RANGE_CACHE_MAX_ENTRIES


@pytest.mark.asyncio
async def test_large_auto_timeframe_basket_reuses_probe_ranges_across_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = CoinGeckoDataProvider(api_key="test", min_request_interval=0, use_gateway=True)
    provider._resolve_token_id = AsyncMock(side_effect=lambda token: token[1])
    assert provider._gateway_transport is not None

    async def request(endpoint: str, params: dict[str, str]) -> dict:
        start = datetime.fromtimestamp(int(params["from"]), tz=UTC)
        end = datetime.fromtimestamp(int(params["to"]), tz=UTC)
        return {
            "prices": [
                [int((start + timedelta(hours=hour)).timestamp() * 1000), 10 + hour]
                for hour in range(int((end - start).total_seconds() // 3600) + 1)
            ]
        }

    gateway_request = AsyncMock(side_effect=request)
    monkeypatch.setattr(provider._gateway_transport, "request", gateway_request)
    tokens = [("bsc", f"0x{index:040x}") for index in range(1, _OHLCV_RANGE_CACHE_MAX_ENTRIES + 2)]
    config = PnLBacktestConfig(
        start_time=_START,
        end_time=_END,
        interval_seconds=300,
        timeframe="auto",
        chain="bsc",
        tokens=tokens,
    )
    backtester = PnLBacktester(data_provider=provider, fee_models={}, slippage_models={})

    preparation = await backtester.prepare_spot_price_history(config)

    assert preparation.resolved_timeframe == "1h"
    assert gateway_request.await_count == len(tokens)
    assert len(provider._coverage_range_pins) == len(tokens)


class _CoverageProvider:
    provider_name = "coverage_fixture"
    historical_capability = HistoricalDataCapability.FULL
    resolution_based_availability = True

    def __init__(self, *, fail: Exception | None = None) -> None:
        self.fail = fail
        self.tokens_seen: list[object] = []

    async def get_price_coverage(
        self,
        token: object,
        start: datetime,
        end: datetime,
        interval_seconds: int,
    ) -> HistoricalCoverage:
        self.tokens_seen.append(token)
        if self.fail is not None:
            raise self.fail
        label = token_ref_display(token)
        if _TOKEN_A in label:
            first = start + timedelta(hours=2)
            last = end
            status = "partial"
        elif _TOKEN_B in label:
            first = start + timedelta(hours=3)
            last = end - timedelta(hours=1)
            status = "partial"
        else:
            first = start
            last = end
            status = "full"
        return HistoricalCoverage(
            status=status,
            requested_start=start,
            requested_end=end,
            first_available_at=first,
            last_available_at=last,
            earliest_contiguous_at=first,
            coverage_ratio=Decimal("1") if status == "full" else Decimal("0.5"),
            provider=self.provider_name,
            source_id=label,
            interval_seconds=interval_seconds,
        )


@pytest.mark.asyncio
async def test_coverage_discovery_treats_symbol_and_address_usdc_as_cash() -> None:
    provider = _CoverageProvider()

    discoveries = await discover_token_coverage(
        provider,
        ["USDC", ("arbitrum", _USDC_ARBITRUM)],
        _START,
        _END,
        3600,
    )

    assert provider.tokens_seen == []
    assert [item.coverage.status for item in discoveries] == ["full", "full"]
    assert [item.coverage.source_id for item in discoveries] == ["USDC", "USDC"]


def test_required_price_tokens_canonicalize_chain_aliases_once() -> None:
    config = PnLBacktestConfig(
        start_time=_START,
        end_time=_END,
        chain="arb",
        tokens=[_TOKEN_A],
        token_funding=pnl_token_funding(100, chain="arbitrum"),
    )

    tokens = PnLBacktester._required_price_tokens(config)

    assert ("arbitrum", _TOKEN_A) in tokens
    assert ("arbitrum", _USDC_ARBITRUM) in tokens
    assert all(not isinstance(token, tuple) or token[0] == "arbitrum" for token in tokens)


def test_historical_coverage_serializes_naive_datetimes_as_utc() -> None:
    coverage = HistoricalCoverage(
        status="full",
        requested_start=_START.replace(tzinfo=None),
        requested_end=_END.replace(tzinfo=None),
        first_available_at=_START.replace(tzinfo=None),
        last_available_at=_END.replace(tzinfo=None),
        earliest_contiguous_at=_START.replace(tzinfo=None),
        coverage_ratio=Decimal("1"),
        provider="fixture",
        source_id="asset",
        interval_seconds=3600,
    )

    assert coverage.to_dict()["requested_start"] == "2026-07-15T00:00:00Z"


def test_preflight_error_copies_structured_details() -> None:
    details = {"code": "PARTIAL_PRICE_HISTORY"}

    error = PreflightValidationError(message="failed", details=details)
    error.details["mutated"] = True

    assert details == {"code": "PARTIAL_PRICE_HISTORY"}


def _config() -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=_START,
        end_time=_END,
        interval_seconds=3600,
        chain="arbitrum",
        tokens=[_TOKEN_A, _TOKEN_B],
        token_funding=pnl_token_funding(100, chain="arbitrum"),
        include_gas_costs=False,
    )


def test_chain_qualified_tokens_survive_config_serialization_round_trip() -> None:
    config = _config()
    config.tokens = [("arbitrum", _TOKEN_A), ("arbitrum", _TOKEN_B)]

    restored = PnLBacktestConfig.from_dict(config.to_dict())

    assert restored.tokens == [("arbitrum", _TOKEN_A), ("arbitrum", _TOKEN_B)]


@pytest.mark.asyncio
async def test_preflight_preserves_chain_and_intersects_all_required_assets() -> None:
    provider = _CoverageProvider()
    backtester = PnLBacktester(data_provider=provider, fee_models={}, slippage_models={})

    report = await backtester.run_preflight_validation(_config())

    token_check = next(check for check in report.checks if check.check_name == "token_availability")
    assert token_check.details["code"] == "PARTIAL_PRICE_HISTORY"
    assert token_check.details["common_supported_range"] == {
        "start": "2026-07-15T03:00:00Z",
        "end": "2026-07-15T05:00:00Z",
    }
    assert token_check.details["suggested_backtest_config_patch"] == {
        "start_time": "2026-07-15T03:00:00Z",
        "end_time": "2026-07-15T05:00:00Z",
    }
    assert ("arbitrum", _TOKEN_A) in provider.tokens_seen
    assert ("arbitrum", _TOKEN_B) in provider.tokens_seen
    asset_a = next(asset for asset in token_check.details["assets"] if asset.get("address") == _TOKEN_A)
    assert asset_a["chain"] == "arbitrum"


@pytest.mark.asyncio
async def test_structured_partial_patch_survives_readiness_blocker_boundary() -> None:
    provider = _CoverageProvider()
    backtester = PnLBacktester(data_provider=provider, fee_models={}, slippage_models={})

    with pytest.raises(PreflightValidationError) as raised:
        await _engine_helpers.run_preflight(
            backtester,
            _config(),
            BacktestLogger(backtest_id="alm-3300", json_format=False),
        )

    blocker = _blocker(raised.value)
    assert blocker["code"] == "PARTIAL_PRICE_HISTORY"
    assert blocker["details"]["suggested_backtest_config_patch"]["start_time"] == "2026-07-15T03:00:00Z"


@pytest.mark.asyncio
async def test_provider_failure_is_not_mislabeled_as_missing_history() -> None:
    provider = _CoverageProvider(fail=TimeoutError("provider timed out"))
    backtester = PnLBacktester(data_provider=provider, fee_models={}, slippage_models={})

    with pytest.raises(PreflightValidationError) as raised:
        await backtester._preflight_token_availability(_config())

    assert raised.value.code == "PRICE_PROVIDER_UNAVAILABLE"
    assert raised.value.details["error_type"] == "TimeoutError"


@pytest.mark.asyncio
async def test_unknown_coverage_recommends_retrying_the_provider() -> None:
    class UnknownCoverageProvider(_CoverageProvider):
        async def get_price_coverage(
            self,
            token: object,
            start: datetime,
            end: datetime,
            interval_seconds: int,
        ) -> HistoricalCoverage:
            return HistoricalCoverage(
                status="unknown",
                requested_start=start,
                requested_end=end,
                first_available_at=None,
                last_available_at=None,
                earliest_contiguous_at=None,
                coverage_ratio=Decimal("0"),
                provider=self.provider_name,
                source_id=token_ref_display(token),
                interval_seconds=interval_seconds,
            )

    backtester = PnLBacktester(data_provider=UnknownCoverageProvider(), fee_models={}, slippage_models={})

    _, _, check, recommendations = await backtester._preflight_token_availability(_config())

    assert check.details["code"] == "PRICE_PROVIDER_UNAVAILABLE"
    assert recommendations == ["Retry the provider or verify its credentials and availability."]
