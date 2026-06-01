"""Unit tests for the gateway-backed SushiSwap V3 Volume Provider.

**VIB-4870 / W7**: ``SushiSwapV3VolumeProvider`` is now a thin gRPC client
of ``RateHistoryService.GetDexVolumeHistory``. Tests assert the gateway-
client path; volume VALUES stay byte-equivalent (W7 §6); the pre-W7
silent-zero fallback is replaced by ``DataSourceUnavailable``.
"""

from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.core.enums import Chain
from almanak.framework.backtesting.pnl.providers.dex.sushiswap_v3_volume import (
    DATA_SOURCE,
    SUPPORTED_CHAINS,
    SUSHISWAP_V3_SUBGRAPH_IDS,
    SushiSwapV3VolumeProvider,
)
from almanak.framework.backtesting.pnl.types import DataConfidence
from almanak.framework.data.interfaces import DataSourceUnavailable

from ._dex_volume_test_helpers import (
    make_point,
    make_response,
    patch_gateway,
    patch_gateway_rpc_error,
)


class TestInitialization:
    def test_init_default(self):
        provider = SushiSwapV3VolumeProvider()
        assert provider.supported_chains == SUPPORTED_CHAINS
        assert provider._fallback_volume == Decimal("0")

    def test_init_legacy_kwargs_accepted(self):
        provider = SushiSwapV3VolumeProvider(
            client=MagicMock(), fallback_volume=Decimal("1000"), requests_per_minute=50
        )
        assert provider._fallback_volume == Decimal("1000")
        assert provider.supported_chains == SUPPORTED_CHAINS

    def test_supported_chains_property_returns_copy(self):
        provider = SushiSwapV3VolumeProvider()
        assert provider.supported_chains is not provider.supported_chains


class TestSupportedChains:
    def test_supported_chains_include_ethereum(self):
        assert Chain.ETHEREUM in SUPPORTED_CHAINS

    def test_all_supported_chains_have_subgraph_ids(self):
        for chain in SUPPORTED_CHAINS:
            assert chain in SUSHISWAP_V3_SUBGRAPH_IDS
            assert SUSHISWAP_V3_SUBGRAPH_IDS[chain]


class TestGetVolume:
    @pytest.mark.asyncio
    async def test_get_volume_success(self):
        response = make_response([make_point(1705276800, "1500000.50")])
        patcher, captured = patch_gateway(response)
        with patcher:
            provider = SushiSwapV3VolumeProvider()
            volumes = await provider.get_volume(
                pool_address="0x123abc",
                chain=Chain.ETHEREUM,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        assert len(volumes) == 1
        assert volumes[0].value == Decimal("1500000.50")
        assert volumes[0].source_info.source == DATA_SOURCE
        assert volumes[0].source_info.confidence == DataConfidence.HIGH
        request = captured["request"]
        assert request.dex == "sushiswap_v3"
        assert request.chain == "ethereum"
        assert request.interval_secs == 86400
        assert request.start_ts < request.end_ts

    @pytest.mark.asyncio
    async def test_get_volume_multiple_days(self):
        response = make_response(
            [
                make_point(1705276800, "1000000"),
                make_point(1705363200, "1100000"),
                make_point(1705449600, "1200000"),
            ]
        )
        patcher, _captured = patch_gateway(response)
        with patcher:
            provider = SushiSwapV3VolumeProvider()
            volumes = await provider.get_volume(
                pool_address="0x123",
                chain=Chain.ETHEREUM,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 17),
            )
        assert [v.value for v in volumes] == [Decimal("1000000"), Decimal("1100000"), Decimal("1200000")]

    @pytest.mark.asyncio
    async def test_get_volume_no_data_raises_unavailable(self):
        response = make_response([], success=False, source="sushiswap_v3", error="no poolDayDatas")
        patcher, _captured = patch_gateway(response)
        with patcher:
            provider = SushiSwapV3VolumeProvider()
            with pytest.raises(DataSourceUnavailable):
                await provider.get_volume(
                    pool_address="0x123",
                    chain=Chain.ETHEREUM,
                    start_date=date(2024, 1, 15),
                    end_date=date(2024, 1, 17),
                )

    @pytest.mark.asyncio
    async def test_get_volume_unsupported_chain_raises(self):
        provider = SushiSwapV3VolumeProvider()
        with pytest.raises(ValueError) as exc_info:
            await provider.get_volume(
                pool_address="0x123",
                chain=Chain.POLYGON,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        assert "Unsupported chain" in str(exc_info.value)
        assert "POLYGON" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_rpc_failure_raises_unavailable(self):
        with patch_gateway_rpc_error(RuntimeError("channel down")):
            provider = SushiSwapV3VolumeProvider()
            with pytest.raises(DataSourceUnavailable):
                await provider.get_volume(
                    pool_address="0x123",
                    chain=Chain.ETHEREUM,
                    start_date=date(2024, 1, 15),
                    end_date=date(2024, 1, 15),
                )


class TestDataParsing:
    @pytest.mark.asyncio
    async def test_decimal_precision_preserved(self):
        response = make_response([make_point(1705276800, "1234567.89012345")])
        patcher, _captured = patch_gateway(response)
        with patcher:
            provider = SushiSwapV3VolumeProvider()
            volumes = await provider.get_volume(
                pool_address="0x123",
                chain=Chain.ETHEREUM,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        assert volumes[0].value == Decimal("1234567.89012345")

    @pytest.mark.asyncio
    async def test_timestamp_conversion(self):
        response = make_response([make_point(1705276800, "1000000")])
        patcher, _captured = patch_gateway(response)
        with patcher:
            provider = SushiSwapV3VolumeProvider()
            volumes = await provider.get_volume(
                pool_address="0x123",
                chain=Chain.ETHEREUM,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        assert volumes[0].source_info.timestamp == datetime(2024, 1, 15, tzinfo=UTC)


class TestContextManager:
    @pytest.mark.asyncio
    async def test_context_manager_is_noop(self):
        provider = SushiSwapV3VolumeProvider()
        async with provider:
            pass
        await provider.close()
