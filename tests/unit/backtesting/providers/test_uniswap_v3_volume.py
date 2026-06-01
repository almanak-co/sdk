"""Unit tests for the gateway-backed Uniswap V3 Volume Provider.

**VIB-4870 / W7**: ``UniswapV3VolumeProvider`` is now a thin gRPC client
of ``RateHistoryService.GetDexVolumeHistory``. These tests assert against
the gateway-client path (a mocked ``rate_history`` stub) rather than the
removed ``SubgraphClient`` egress. Volume VALUES are still asserted
byte-equivalent to the pre-W7 provider (W7 §6); the pre-W7 silent-zero
fallback path is replaced by ``DataSourceUnavailable``.
"""

from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.core.enums import Chain
from almanak.framework.backtesting.pnl.providers.dex.uniswap_v3_volume import (
    DATA_SOURCE,
    SUPPORTED_CHAINS,
    UNISWAP_V3_SUBGRAPH_IDS,
    UniswapV3VolumeProvider,
)
from almanak.framework.backtesting.pnl.types import DataConfidence
from almanak.framework.data.interfaces import DataSourceUnavailable

_GW_MODULE = "almanak.framework.backtesting.pnl.providers.dex._gateway_volume"


def _make_point(timestamp: int, volume_usd: str) -> MagicMock:
    point = MagicMock()
    point.timestamp = timestamp
    point.volume_usd = volume_usd
    return point


def _make_response(
    points: list[MagicMock], *, success: bool = True, source: str = "the_graph", error: str = ""
) -> MagicMock:
    resp = MagicMock()
    resp.success = success
    resp.source = source
    resp.error = error
    resp.points = points
    return resp


def _patch_gateway(response: MagicMock):
    """Patch the shared gateway-client helper to return ``response``.

    Returns a context manager yielding the captured ``GetDexVolumeHistory``
    request object so tests can assert the request the provider built.
    """
    captured: dict[str, object] = {}

    client = MagicMock()
    client.is_connected = True

    def _get_volume_history(request):
        captured["request"] = request
        return response

    client.rate_history.GetDexVolumeHistory = _get_volume_history

    import almanak.gateway.proto.gateway_pb2 as gateway_pb2

    patcher = patch(
        f"{_GW_MODULE}._get_connected_gateway_client",
        return_value=(client, gateway_pb2),
    )
    return patcher, captured


class TestUniswapV3VolumeProviderInitialization:
    """Tests for UniswapV3VolumeProvider initialization (back-compat surface)."""

    def test_init_default(self):
        provider = UniswapV3VolumeProvider()
        assert provider.supported_chains == SUPPORTED_CHAINS
        assert provider._fallback_volume == Decimal("0")

    def test_init_with_custom_fallback_is_accepted(self):
        # fallback_volume kept for back-compat (ignored at runtime).
        provider = UniswapV3VolumeProvider(fallback_volume=Decimal("1000"))
        assert provider._fallback_volume == Decimal("1000")

    def test_init_with_legacy_kwargs_accepted(self):
        # client / requests_per_minute kept for back-compat (ignored).
        provider = UniswapV3VolumeProvider(client=MagicMock(), requests_per_minute=50)
        assert provider.supported_chains == SUPPORTED_CHAINS

    def test_supported_chains_property_returns_copy(self):
        provider = UniswapV3VolumeProvider()
        chains1 = provider.supported_chains
        chains2 = provider.supported_chains
        assert chains1 == chains2
        assert chains1 is not chains2


class TestSupportedChains:
    """Tests for supported chains configuration (preserved tables)."""

    def test_supported_chains_include_required_networks(self):
        assert Chain.ETHEREUM in SUPPORTED_CHAINS
        assert Chain.ARBITRUM in SUPPORTED_CHAINS
        assert Chain.BASE in SUPPORTED_CHAINS
        assert Chain.OPTIMISM in SUPPORTED_CHAINS
        assert Chain.POLYGON in SUPPORTED_CHAINS

    def test_all_supported_chains_have_subgraph_ids(self):
        for chain in SUPPORTED_CHAINS:
            assert chain in UNISWAP_V3_SUBGRAPH_IDS
            assert UNISWAP_V3_SUBGRAPH_IDS[chain]


class TestGetVolume:
    """Tests for the gateway-backed get_volume path."""

    @pytest.mark.asyncio
    async def test_get_volume_success(self):
        # 1705276800 = 2024-01-15 00:00:00 UTC
        response = _make_response([_make_point(1705276800, "1500000.50")])
        patcher, captured = _patch_gateway(response)
        with patcher:
            provider = UniswapV3VolumeProvider()
            volumes = await provider.get_volume(
                pool_address="0x123ABC",
                chain=Chain.ARBITRUM,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )

        assert len(volumes) == 1
        assert volumes[0].value == Decimal("1500000.50")
        assert volumes[0].source_info.source == DATA_SOURCE
        assert volumes[0].source_info.confidence == DataConfidence.HIGH

        # The RPC was built for the right dex / chain / pool / daily interval.
        request = captured["request"]
        assert request.dex == "uniswap_v3"
        assert request.chain == "arbitrum"
        assert request.pool_address == "0x123ABC"
        assert request.interval_secs == 86400
        # Single-day window must satisfy the gateway's strict start < end.
        assert request.start_ts < request.end_ts

    @pytest.mark.asyncio
    async def test_get_volume_multiple_days(self):
        response = _make_response(
            [
                _make_point(1705276800, "1000000"),
                _make_point(1705363200, "1100000"),
                _make_point(1705449600, "1200000"),
            ]
        )
        patcher, _captured = _patch_gateway(response)
        with patcher:
            provider = UniswapV3VolumeProvider()
            volumes = await provider.get_volume(
                pool_address="0x123",
                chain=Chain.ETHEREUM,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 17),
            )

        assert len(volumes) == 3
        assert volumes[0].value == Decimal("1000000")
        assert volumes[1].value == Decimal("1100000")
        assert volumes[2].value == Decimal("1200000")

    @pytest.mark.asyncio
    async def test_get_volume_no_data_raises_unavailable(self):
        # W7 intentional change: empty subgraph -> success=False ->
        # DataSourceUnavailable (NOT a silent Decimal("0") LOW row).
        response = _make_response([], success=False, source="uniswap_v3", error="subgraph returned no poolDayDatas")
        patcher, _captured = _patch_gateway(response)
        with patcher:
            provider = UniswapV3VolumeProvider()
            with pytest.raises(DataSourceUnavailable):
                await provider.get_volume(
                    pool_address="0x123",
                    chain=Chain.ARBITRUM,
                    start_date=date(2024, 1, 15),
                    end_date=date(2024, 1, 17),
                )

    @pytest.mark.asyncio
    async def test_get_volume_unsupported_chain_raises(self):
        provider = UniswapV3VolumeProvider()
        with pytest.raises(ValueError) as exc_info:
            await provider.get_volume(
                pool_address="0x123",
                chain=Chain.AVALANCHE,  # Not supported for Uniswap V3
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        assert "Unsupported chain" in str(exc_info.value)
        assert "AVALANCHE" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_rpc_failure_raises_unavailable(self):
        client = MagicMock()
        client.is_connected = True
        client.rate_history.GetDexVolumeHistory = MagicMock(side_effect=RuntimeError("channel down"))
        import almanak.gateway.proto.gateway_pb2 as gateway_pb2

        with patch(f"{_GW_MODULE}._get_connected_gateway_client", return_value=(client, gateway_pb2)):
            provider = UniswapV3VolumeProvider()
            with pytest.raises(DataSourceUnavailable):
                await provider.get_volume(
                    pool_address="0x123",
                    chain=Chain.ARBITRUM,
                    start_date=date(2024, 1, 15),
                    end_date=date(2024, 1, 15),
                )


class TestContextManager:
    """Tests for async context manager behavior."""

    @pytest.mark.asyncio
    async def test_context_manager_is_noop(self):
        provider = UniswapV3VolumeProvider()
        async with provider:
            pass
        # close() is a no-op (no owned client) — must not raise.
        await provider.close()


class TestDataParsing:
    """Tests for byte-equivalent value + timestamp mapping (W7 §6)."""

    @pytest.mark.asyncio
    async def test_parse_volume_with_decimal_precision(self):
        response = _make_response([_make_point(1705276800, "1234567.89012345")])
        patcher, _captured = _patch_gateway(response)
        with patcher:
            provider = UniswapV3VolumeProvider()
            volumes = await provider.get_volume(
                pool_address="0x123",
                chain=Chain.ARBITRUM,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        assert volumes[0].value == Decimal("1234567.89012345")

    @pytest.mark.asyncio
    async def test_timestamp_conversion(self):
        # 1705276800 = 2024-01-15 00:00:00 UTC
        response = _make_response([_make_point(1705276800, "1000000")])
        patcher, _captured = _patch_gateway(response)
        with patcher:
            provider = UniswapV3VolumeProvider()
            volumes = await provider.get_volume(
                pool_address="0x123",
                chain=Chain.ARBITRUM,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        assert volumes[0].source_info.timestamp == datetime(2024, 1, 15, tzinfo=UTC)


class TestAllSupportedChains:
    """Tests for all supported chains."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("chain", SUPPORTED_CHAINS)
    async def test_can_query_all_supported_chains(self, chain: Chain):
        response = _make_response([_make_point(1705276800, "1000000")])
        patcher, captured = _patch_gateway(response)
        with patcher:
            provider = UniswapV3VolumeProvider()
            volumes = await provider.get_volume(
                pool_address="0x123",
                chain=chain,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        assert len(volumes) == 1
        assert captured["request"].chain == chain.value.lower()
