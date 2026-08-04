"""Contract tests for the thin gateway-backed Chainlink backtest facade."""

from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig
from almanak.framework.backtesting.pnl.providers.chainlink import (
    CHAINLINK_PRICE_FEEDS,
    ChainlinkDataProvider,
    ChainlinkStaleDataError,
    PersistentCacheConfig,
    PriceCache,
)
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.gateway.proto import gateway_pb2

_REPO_ROOT = Path(__file__).resolve().parents[4]


def _gateway(*, current=None, history=None):
    rate_history = SimpleNamespace(
        GetOraclePrice=Mock(return_value=current),
        GetOraclePriceHistory=Mock(return_value=history),
    )
    return SimpleNamespace(
        is_connected=True,
        rate_history=rate_history,
    )


def test_provider_is_gateway_only_and_ignores_legacy_rpc_url() -> None:
    provider = ChainlinkDataProvider(chain="ethereum", rpc_url="https://must-not-be-used.example")
    assert provider._rpc_url == ""
    source = _REPO_ROOT / "almanak/framework/backtesting/pnl/providers/chainlink.py"
    tree = ast.parse(source.read_text())
    imported_roots = {
        name
        for node in ast.walk(tree)
        for name in (
            [alias.name.split(".", 1)[0] for alias in node.names]
            if isinstance(node, ast.Import)
            else [str(node.module).split(".", 1)[0]]
            if isinstance(node, ast.ImportFrom)
            else []
        )
    }
    assert not imported_roots & {"web3", "aiohttp", "httpx", "requests"}


def test_catalog_configuration_is_preserved() -> None:
    provider = ChainlinkDataProvider(chain="arbitrum")
    assert provider.get_feed_address("WETH") == CHAINLINK_PRICE_FEEDS["arbitrum"]["ETH/USD"]
    assert provider.get_feed_config("USDC").heartbeat_seconds == 86_400
    assert provider.get_feed_address("NOT_A_TOKEN") is None
    assert "ethereum" in provider.supported_chains
    assert "WETH" in provider.supported_tokens


def test_unsupported_chain_fails_closed() -> None:
    with pytest.raises(ValueError, match="Unsupported chain"):
        ChainlinkDataProvider(chain="not-a-chain")


def test_staleness_uses_feed_heartbeat_with_buffer() -> None:
    provider = ChainlinkDataProvider(chain="ethereum")
    now = datetime(2026, 1, 1, tzinfo=UTC)
    assert not provider.is_data_stale(now - timedelta(seconds=3_700), "ETH", current_time=now)
    assert provider.is_data_stale(now - timedelta(seconds=4_000), "ETH", current_time=now)
    assert not provider.is_data_stale(now - timedelta(hours=12), "USDC", current_time=now)
    with pytest.raises(ChainlinkStaleDataError):
        provider._check_staleness("ETH", now - timedelta(hours=2), now, raise_on_stale=True)


def test_price_cache_is_at_or_before_and_case_insensitive() -> None:
    cache = PriceCache(ttl_seconds=60)
    one = datetime(2026, 1, 1, 1, tzinfo=UTC)
    two = datetime(2026, 1, 1, 2, tzinfo=UTC)
    cache.data["ETH"] = [(one, Decimal("100")), (two, Decimal("200"))]
    assert cache.get_price_at("eth", one + timedelta(minutes=30)) == Decimal("100")
    assert cache.get_price_at("ETH", two) == Decimal("200")


def test_latest_price_sync_rejects_unsupported_tokens_without_rpc() -> None:
    provider = ChainlinkDataProvider()
    with patch("almanak.framework.backtesting.pnl.providers.chainlink._connected_gateway") as connect:
        assert provider.get_latest_price_sync("NOT_A_TOKEN") is None
    connect.assert_not_called()


def test_latest_price_sync_uses_case_insensitive_live_cache() -> None:
    provider = ChainlinkDataProvider()
    provider._cache.set_live_price("ETH", Decimal("3123.45"))
    with patch("almanak.framework.backtesting.pnl.providers.chainlink._connected_gateway") as connect:
        assert provider.get_latest_price_sync("eth") == Decimal("3123.45")
    connect.assert_not_called()


def test_latest_price_sync_applies_staleness_policy_to_live_cache() -> None:
    provider = ChainlinkDataProvider()
    stale = datetime.now(UTC) - timedelta(hours=2)
    provider._cache.set_live_price("ETH", Decimal("3123.45"), stale)

    with patch("almanak.framework.backtesting.pnl.providers.chainlink._connected_gateway") as connect:
        with pytest.raises(ChainlinkStaleDataError):
            provider.get_latest_price_sync("eth")
        assert provider.get_latest_price_sync("eth", raise_on_stale=False) is None

    connect.assert_not_called()


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        (
            gateway_pb2.OraclePriceResponse(
                provider="chainlink",
                chain="ethereum",
                token="ETH",
                point=gateway_pb2.OraclePricePoint(
                    timestamp=int(datetime.now(UTC).timestamp()),
                    price="3123.45",
                    observation_id="1",
                ),
                success=True,
            ),
            Decimal("3123.45"),
        ),
    ],
)
def test_latest_price_sync_handles_gateway_price_envelopes(response, expected: Decimal | None) -> None:
    client = _gateway(current=response)
    with patch(
        "almanak.framework.backtesting.pnl.providers.chainlink._connected_gateway",
        return_value=(client, gateway_pb2),
    ):
        provider = ChainlinkDataProvider()
        assert provider.get_latest_price_sync("eth", use_cache=False) == expected

    request = client.rate_history.GetOraclePrice.call_args.args[0]
    assert request.provider == "chainlink"
    assert request.chain == "ethereum"
    assert request.token == "ETH"
    if expected is not None:
        assert provider._cache.get_live_price("ETH").price == expected


def test_latest_price_sync_propagates_unavailable_envelope() -> None:
    client = _gateway(
        current=gateway_pb2.OraclePriceResponse(
            provider="chainlink", chain="ethereum", token="ETH", success=False, error="feed unavailable"
        )
    )
    with patch(
        "almanak.framework.backtesting.pnl.providers.chainlink._connected_gateway",
        return_value=(client, gateway_pb2),
    ):
        with pytest.raises(DataSourceUnavailable, match="feed unavailable"):
            ChainlinkDataProvider().get_latest_price_sync("ETH", use_cache=False)


@pytest.mark.parametrize(
    ("price", "timestamp"),
    [
        ("", 1_735_689_600),
        ("0", 1_735_689_600),
        ("-1", 1_735_689_600),
        ("NaN", 1_735_689_600),
        ("100", 0),
    ],
)
def test_latest_price_rejects_empty_or_invalid_observations(price: str, timestamp: int) -> None:
    client = _gateway(
        current=gateway_pb2.OraclePriceResponse(
            provider="chainlink",
            chain="ethereum",
            token="ETH",
            point=gateway_pb2.OraclePricePoint(timestamp=timestamp, price=price),
            success=True,
        )
    )
    with patch(
        "almanak.framework.backtesting.pnl.providers.chainlink._connected_gateway",
        return_value=(client, gateway_pb2),
    ):
        with pytest.raises(DataSourceUnavailable, match="empty|invalid"):
            ChainlinkDataProvider().get_latest_price_sync("ETH", use_cache=False)


def test_latest_price_sync_returns_none_for_allowed_stale_data() -> None:
    stale = datetime.now(UTC) - timedelta(days=1)
    client = _gateway(
        current=gateway_pb2.OraclePriceResponse(
            provider="chainlink",
            chain="ethereum",
            token="ETH",
            point=gateway_pb2.OraclePricePoint(timestamp=int(stale.timestamp()), price="1000"),
            success=True,
        )
    )
    with patch(
        "almanak.framework.backtesting.pnl.providers.chainlink._connected_gateway",
        return_value=(client, gateway_pb2),
    ):
        assert ChainlinkDataProvider().get_latest_price_sync("ETH", raise_on_stale=False) is None


@pytest.mark.asyncio
async def test_latest_price_uses_provider_exact_rpc() -> None:
    current = gateway_pb2.OraclePriceResponse(
        provider="chainlink",
        chain="ethereum",
        token="ETH",
        point=gateway_pb2.OraclePricePoint(
            timestamp=int(datetime.now(UTC).timestamp()),
            price="3123.45",
            observation_id="18446744073709551617",
        ),
        success=True,
    )
    client = _gateway(current=current)
    with patch(
        "almanak.framework.backtesting.pnl.providers.chainlink._connected_gateway",
        return_value=(client, gateway_pb2),
    ):
        provider = ChainlinkDataProvider(chain="ethereum")
        assert await provider.get_latest_price("ETH") == Decimal("3123.45")

    request = client.rate_history.GetOraclePrice.call_args.args[0]
    assert request.provider == "chainlink"
    assert request.chain == "ethereum"
    assert request.token == "ETH"


@pytest.mark.asyncio
async def test_latest_price_propagates_unavailable_envelope() -> None:
    client = _gateway(
        current=gateway_pb2.OraclePriceResponse(
            provider="chainlink", chain="ethereum", token="ETH", success=False, error="feed unavailable"
        )
    )
    with patch(
        "almanak.framework.backtesting.pnl.providers.chainlink._connected_gateway",
        return_value=(client, gateway_pb2),
    ):
        with pytest.raises(DataSourceUnavailable, match="feed unavailable"):
            await ChainlinkDataProvider().get_latest_price("ETH")


@pytest.mark.asyncio
async def test_historical_price_uses_bounded_history_rpc() -> None:
    requested = datetime(2025, 1, 1, 2, tzinfo=UTC)
    history = gateway_pb2.OraclePriceHistoryResponse(
        provider="chainlink",
        chain="ethereum",
        token="ETH",
        points=[
            gateway_pb2.OraclePricePoint(
                timestamp=int((requested - timedelta(hours=1)).timestamp()),
                price="2000",
                observation_id="1",
            ),
            gateway_pb2.OraclePricePoint(
                timestamp=int(requested.timestamp()),
                price="2100",
                observation_id="2",
            ),
        ],
        success=True,
    )
    client = _gateway(history=history)
    with patch(
        "almanak.framework.backtesting.pnl.providers.chainlink._connected_gateway",
        return_value=(client, gateway_pb2),
    ):
        provider = ChainlinkDataProvider(cache_ttl_seconds=0)
        assert await provider.get_price("ETH", requested) == Decimal("2100")
        assert provider._cache.get_price_at("ETH", requested) == Decimal("2100")

    request = client.rate_history.GetOraclePriceHistory.call_args.args[0]
    assert request.provider == "chainlink"
    assert request.max_points == 10_000
    assert request.end_ts - request.start_ts <= 366 * 86_400


@pytest.mark.asyncio
async def test_history_saturation_is_paginated_until_progress_fails_closed() -> None:
    point = SimpleNamespace(timestamp=1_735_689_600, price="2000")
    client = _gateway(history=SimpleNamespace(success=True, error="", points=[point] * 10_000))
    with patch(
        "almanak.framework.backtesting.pnl.providers.chainlink._connected_gateway",
        return_value=(client, gateway_pb2),
    ):
        provider = ChainlinkDataProvider()
        with pytest.raises(DataSourceUnavailable, match="non-progressing split point"):
            await provider._fetch_history(
                "ETH",
                datetime(2025, 1, 1, tzinfo=UTC),
                datetime(2025, 1, 2, tzinfo=UTC),
            )


@pytest.mark.parametrize(
    ("timestamp", "price"),
    [(1_735_689_600, ""), (1_735_689_600, "0"), (1_735_689_600, "-1"), (1_735_689_600, "NaN"), (0, "1")],
)
def test_history_rejects_empty_or_invalid_proto_points(timestamp: int, price: str) -> None:
    with pytest.raises(DataSourceUnavailable, match="malformed|invalid"):
        ChainlinkDataProvider._decode_history_point(SimpleNamespace(timestamp=timestamp, price=price))


@pytest.mark.asyncio
async def test_history_truncation_signal_bisects_and_reassembles_complete_series() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = start + timedelta(hours=2)
    split_ts = int((start + timedelta(hours=1)).timestamp())
    client = _gateway()

    def history_response(request):
        if request.start_ts == int(start.timestamp()) and request.end_ts == int(end.timestamp()):
            return gateway_pb2.OraclePriceHistoryResponse(
                success=True,
                truncated=True,
                recommended_split_ts=split_ts,
            )
        return gateway_pb2.OraclePriceHistoryResponse(
            success=True,
            points=[
                gateway_pb2.OraclePricePoint(
                    timestamp=request.start_ts,
                    price="100" if request.start_ts == int(start.timestamp()) else "110",
                )
            ],
        )

    client.rate_history.GetOraclePriceHistory.side_effect = history_response
    with patch(
        "almanak.framework.backtesting.pnl.providers.chainlink._connected_gateway",
        return_value=(client, gateway_pb2),
    ):
        points = await ChainlinkDataProvider()._fetch_history("ETH", start, end)

    assert points == [
        (start, Decimal("100")),
        (start + timedelta(hours=1), Decimal("110")),
    ]
    assert client.rate_history.GetOraclePriceHistory.call_count == 3


@pytest.mark.asyncio
async def test_ohlcv_materializes_gateway_history_as_flat_candles() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    provider = ChainlinkDataProvider(cache_ttl_seconds=0)
    points = [
        (start, Decimal("100")),
        (start + timedelta(hours=2), Decimal("120")),
    ]
    with patch.object(provider, "_fetch_history", return_value=points) as fetch:
        rows = await provider.get_ohlcv("eth", start, start + timedelta(hours=2), interval_seconds=3600)

    fetch.assert_awaited_once_with("eth", start, start + timedelta(hours=2))
    assert [row.timestamp for row in rows] == [
        start,
        start + timedelta(hours=1),
        start + timedelta(hours=2),
    ]
    assert [row.close for row in rows] == [Decimal("100"), Decimal("100"), Decimal("120")]
    assert all(row.open == row.high == row.low == row.close for row in rows)
    assert all(row.volume is None for row in rows)
    assert provider._cache.get_price_at("ETH", start + timedelta(hours=2)) == Decimal("120")


@pytest.mark.asyncio
async def test_ohlcv_refetches_when_cached_series_does_not_cover_requested_range() -> None:
    start = datetime(2025, 1, 2, tzinfo=UTC)
    provider = ChainlinkDataProvider()
    provider.set_historical_prices("ETH", [(start - timedelta(days=1), Decimal("90"))])
    fetched = [(start, Decimal("100")), (start + timedelta(hours=1), Decimal("110"))]

    with patch.object(provider, "_fetch_history", return_value=fetched) as fetch:
        rows = await provider.get_ohlcv("ETH", start, start + timedelta(hours=1))

    fetch.assert_awaited_once_with("ETH", start, start + timedelta(hours=1))
    assert [row.close for row in rows] == [Decimal("100"), Decimal("110")]
    assert provider._cache.data["ETH"][0] == (start - timedelta(days=1), Decimal("90"))


@pytest.mark.asyncio
async def test_ohlcv_uses_recorded_fetch_coverage_for_sparse_oracle_series() -> None:
    start = datetime(2025, 1, 2, tzinfo=UTC)
    end = start + timedelta(hours=2)
    provider = ChainlinkDataProvider()

    with patch.object(provider, "_fetch_history", return_value=[(start, Decimal("100"))]) as fetch:
        first = await provider.get_ohlcv("ETH", start, end)
        second = await provider.get_ohlcv("ETH", start, end)

    fetch.assert_awaited_once_with("ETH", start, end)
    assert [row.close for row in first] == [Decimal("100"), Decimal("100"), Decimal("100")]
    assert second == first


@pytest.mark.asyncio
async def test_ohlcv_rejects_tokens_without_a_feed() -> None:
    provider = ChainlinkDataProvider()
    with patch.object(provider, "_fetch_history") as fetch:
        with pytest.raises(ValueError, match="No Chainlink feed available"):
            await provider.get_ohlcv("NOT_A_TOKEN", datetime.now(UTC), datetime.now(UTC))
    fetch.assert_not_called()


@pytest.mark.asyncio
async def test_iteration_materializes_gateway_history() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    provider = ChainlinkDataProvider()
    points = [(start, Decimal("100")), (start + timedelta(hours=1), Decimal("110"))]
    with (
        patch.object(provider, "_verify_archive_access", return_value=True) as verify,
        patch.object(provider, "_fetch_history", return_value=points) as fetch,
    ):
        rows = [
            row
            async for row in provider.iterate(
                HistoricalDataConfig(
                    tokens=["ETH"],
                    start_time=start,
                    end_time=start + timedelta(hours=1),
                    interval_seconds=3600,
                )
            )
        ]
    assert [state.prices["ETH"] for _, state in rows] == [Decimal("100"), Decimal("110")]
    verify.assert_awaited_once()
    fetch.assert_awaited_once()
    assert rows[0][1].metadata == {
        "data_source": "chainlink_historical",
        "historical_price_hits": 1,
        "cache_price_hits": 0,
    }
    assert rows[1][1].metadata["historical_price_hits"] == 1


@pytest.mark.asyncio
async def test_iteration_reports_preloaded_cache_provenance_when_gateway_history_is_unavailable(caplog) -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = start + timedelta(hours=1)
    provider = ChainlinkDataProvider()
    provider.set_historical_prices(
        "ETH",
        [(start, Decimal("100")), (end, Decimal("110"))],
        covered_range=(start, end),
    )

    with caplog.at_level("WARNING"):
        with (
            patch.object(provider, "_verify_archive_access", return_value=False) as verify,
            patch.object(provider, "_fetch_history") as fetch,
        ):
            rows = [
                row
                async for row in provider.iterate(
                    HistoricalDataConfig(tokens=["ETH"], start_time=start, end_time=end, interval_seconds=3600)
                )
            ]

    verify.assert_awaited_once()
    fetch.assert_not_called()
    assert rows[-1][1].metadata == {
        "data_source": "chainlink_cache",
        "historical_price_hits": 0,
        "cache_price_hits": 1,
    }
    assert provider.historical_capability.name == "PRE_CACHE"
    assert "restricted to preloaded cache coverage" in caplog.text


def test_persistent_cache_round_trip(tmp_path: Path) -> None:
    config = PersistentCacheConfig(enabled=True, cache_dir=str(tmp_path))
    timestamp = datetime(2025, 1, 1, tzinfo=UTC)
    writer = ChainlinkDataProvider(persistent_cache_config=config)
    writer.set_historical_prices("ETH", [(timestamp, Decimal("123.4"))])
    reader = ChainlinkDataProvider(persistent_cache_config=config)
    assert reader._cache.get_price_at("ETH", timestamp) == Decimal("123.4")


def test_set_historical_prices_merges_existing_points_and_incoming_wins_duplicates() -> None:
    provider = ChainlinkDataProvider()
    first = datetime(2025, 1, 1, tzinfo=UTC)
    second = first + timedelta(hours=1)
    provider.set_historical_prices("eth", [(first, Decimal("100")), (second, Decimal("110"))])
    provider.set_historical_prices("ETH", [(second, Decimal("111")), (second + timedelta(hours=1), Decimal("120"))])

    assert provider._cache.data["ETH"] == [
        (first, Decimal("100")),
        (second, Decimal("111")),
        (second + timedelta(hours=1), Decimal("120")),
    ]


def test_public_metadata_contract() -> None:
    provider = ChainlinkDataProvider(chain="ethereum", priority=7)
    assert provider.provider_name == "chainlink_ethereum"
    assert provider.priority == 7
    assert provider.min_timestamp == datetime(2020, 1, 1, tzinfo=UTC)


@pytest.mark.asyncio
async def test_ttl_zero_preload_is_offline_capable_and_survives_ttl_changes() -> None:
    timestamp = datetime(2025, 1, 1, tzinfo=UTC)
    provider = ChainlinkDataProvider(cache_ttl_seconds=0)
    provider.set_historical_prices("ETH", [(timestamp, Decimal("123"))])
    provider.set_cache_ttl(60)
    provider.set_cache_ttl(0)

    with patch.object(provider, "_fetch_history") as fetch:
        assert await provider.get_price("ETH", timestamp) == Decimal("123")
        rows = await provider.get_ohlcv("ETH", timestamp, timestamp)
    fetch.assert_not_called()
    assert rows[0].close == Decimal("123")


@pytest.mark.asyncio
async def test_iteration_rejects_address_token_refs_before_fetch() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    provider = ChainlinkDataProvider()
    config = HistoricalDataConfig(
        tokens=[("ethereum", "0x0000000000000000000000000000000000000001")],
        start_time=start,
        end_time=start + timedelta(hours=1),
    )
    with patch.object(provider, "_fetch_history") as fetch:
        with pytest.raises(DataSourceUnavailable, match="symbol tokens only"):
            _ = [row async for row in provider.iterate(config)]
    fetch.assert_not_called()


@pytest.mark.asyncio
async def test_iteration_empty_history_fails_typed() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    provider = ChainlinkDataProvider()
    config = HistoricalDataConfig(tokens=["ETH"], start_time=start, end_time=start + timedelta(hours=1))
    with (
        patch.object(provider, "_verify_archive_access", return_value=True),
        patch.object(provider, "_fetch_history", return_value=[]),
    ):
        with pytest.raises(DataSourceUnavailable, match="No prices"):
            _ = [row async for row in provider.iterate(config)]


@pytest.mark.asyncio
async def test_get_price_validates_token_and_uses_cache_or_live_window() -> None:
    provider = ChainlinkDataProvider(chain="sonic")
    now = datetime.now(UTC)
    with pytest.raises(ValueError, match="Unknown token"):
        await provider.get_price("NOPE", now)
    with pytest.raises(ValueError, match="No Chainlink feed"):
        await provider.get_price("BTC", now)

    provider.set_historical_prices("ETH", [(now - timedelta(hours=1), Decimal("100"))])
    with patch.object(provider, "get_latest_price", return_value=Decimal("101")) as latest:
        assert await provider.get_price("ETH", now) == Decimal("101")
    latest.assert_awaited_once()

    old = now - timedelta(hours=1)
    with patch.object(provider, "_fetch_history") as fetch:
        assert await provider.get_price("ETH", old) == Decimal("100")
    fetch.assert_not_called()


def test_cache_management_reports_and_clears_without_destroying_other_tokens() -> None:
    provider = ChainlinkDataProvider()
    timestamp = datetime(2025, 1, 1, tzinfo=UTC)
    provider.set_historical_prices("ETH", [(timestamp, Decimal("100"))])
    provider.set_historical_prices("USDC", [(timestamp, Decimal("1"))])
    stats = provider.get_cache_stats()
    assert stats["historical_count"] == 2
    assert stats["total_historical_points"] == 2

    provider.clear_cache("ETH")
    assert "ETH" not in provider._cache.data
    assert "USDC" in provider._cache.data
    provider.clear_cache()
    assert provider._cache.data == {}
    with pytest.raises(ValueError, match="non-negative"):
        provider.set_cache_ttl(-1)


def test_malformed_persistent_cache_does_not_partially_hydrate_state(tmp_path: Path) -> None:
    config = PersistentCacheConfig(enabled=True, cache_dir=str(tmp_path))
    path = config.get_cache_path("ethereum")
    assert path is not None
    path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "cached_at": datetime.now(UTC).isoformat(),
                "chain": "ethereum",
                "prices": {"ETH": [[1_735_689_600, "123"]]},
                "coverage": {"ETH": [["not-a-timestamp", 1_735_689_601]]},
            }
        )
    )

    provider = ChainlinkDataProvider(persistent_cache_config=config)
    assert provider._cache.data == {}
    assert provider._cache.coverage == {}


def test_expired_persistent_cache_is_ignored(tmp_path: Path) -> None:
    config = PersistentCacheConfig(enabled=True, cache_dir=str(tmp_path), max_age_days=1)
    path = config.get_cache_path("ethereum")
    assert path is not None
    path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "cached_at": "2020-01-01T00:00:00+00:00",
                "chain": "ethereum",
                "prices": {"ETH": [[1_735_689_600, "123"]]},
                "coverage": {},
            }
        )
    )
    assert ChainlinkDataProvider(persistent_cache_config=config)._cache.data == {}


def test_persistent_cache_save_failure_is_nonfatal_and_atomic(tmp_path: Path) -> None:
    config = PersistentCacheConfig(enabled=True, cache_dir=str(tmp_path))
    provider = ChainlinkDataProvider(persistent_cache_config=config)
    with patch("almanak.framework.backtesting.pnl.providers.chainlink.os.replace", side_effect=OSError("disk")):
        provider.set_historical_prices(
            "ETH",
            [(datetime(2025, 1, 1, tzinfo=UTC), Decimal("123"))],
        )
    path = config.get_cache_path("ethereum")
    assert path is not None and not path.exists()


@pytest.mark.asyncio
async def test_async_context_manager_returns_provider_and_closes() -> None:
    provider = ChainlinkDataProvider()
    with patch.object(provider, "close") as close:
        async with provider as entered:
            assert entered is provider
    close.assert_awaited_once()
