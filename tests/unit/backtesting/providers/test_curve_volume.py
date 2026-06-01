"""Unit tests for the gateway-backed Curve Volume Provider.

**VIB-4870 / W7**: ``CurveVolumeProvider`` is now a thin gRPC client of
``RateHistoryService.GetDexVolumeHistory``. Tests assert the gateway-
client path; volume VALUES stay byte-equivalent (W7 §6); the pre-W7
silent-zero fallback is replaced by ``DataSourceUnavailable``. The
gateway returns Messari ``day`` numbers already converted to unix
seconds, so the timestamp mapping is identical to the other DEXes.
"""

from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.core.enums import Chain
from almanak.framework.backtesting.pnl.providers.dex.curve_volume import (
    CURVE_SUBGRAPH_IDS,
    DATA_SOURCE,
    SUPPORTED_CHAINS,
    CurveVolumeProvider,
)
from almanak.framework.backtesting.pnl.types import DataConfidence
from almanak.framework.data.interfaces import DataSourceUnavailable

from ._dex_volume_test_helpers import (
    make_point,
    make_response,
    patch_gateway,
    patch_gateway_rpc_error,
)

# Messari day 19676 → 19676 * 86400 unix seconds (the gateway converts it).
_DAY_19676_TS = 19676 * 86400


class TestInitialization:
    def test_init_default(self):
        provider = CurveVolumeProvider()
        assert provider.supported_chains == SUPPORTED_CHAINS
        assert provider._fallback_volume == Decimal("0")

    def test_init_legacy_kwargs_accepted(self):
        provider = CurveVolumeProvider(client=MagicMock(), fallback_volume=Decimal("1000"), requests_per_minute=50)
        assert provider._fallback_volume == Decimal("1000")

    def test_supported_chains_property_returns_copy(self):
        provider = CurveVolumeProvider()
        assert provider.supported_chains is not provider.supported_chains


class TestSupportedChains:
    def test_supported_chains_include_required_networks(self):
        assert Chain.ETHEREUM in SUPPORTED_CHAINS
        assert Chain.OPTIMISM in SUPPORTED_CHAINS

    def test_arbitrum_and_polygon_not_yet_supported(self):
        assert Chain.ARBITRUM not in SUPPORTED_CHAINS
        assert Chain.POLYGON not in SUPPORTED_CHAINS

    def test_all_supported_chains_have_subgraph_ids(self):
        for chain in SUPPORTED_CHAINS:
            assert chain in CURVE_SUBGRAPH_IDS
            assert CURVE_SUBGRAPH_IDS[chain]


class TestGetVolume:
    @pytest.mark.asyncio
    async def test_get_volume_success(self):
        response = make_response([make_point(_DAY_19676_TS, "500000")])
        patcher, captured = patch_gateway(response)
        with patcher:
            provider = CurveVolumeProvider()
            volumes = await provider.get_volume(
                pool_address="0xbEbc",
                chain=Chain.ETHEREUM,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        assert len(volumes) == 1
        assert volumes[0].value == Decimal("500000")
        assert volumes[0].source_info.source == DATA_SOURCE
        assert volumes[0].source_info.confidence == DataConfidence.HIGH
        # Messari day → midnight UTC, byte-equivalent to the pre-W7 parse.
        assert volumes[0].source_info.timestamp == datetime.fromtimestamp(_DAY_19676_TS, tz=UTC)
        request = captured["request"]
        assert request.dex == "curve"
        assert request.chain == "ethereum"
        assert request.interval_secs == 86400
        assert request.start_ts < request.end_ts

    @pytest.mark.asyncio
    async def test_get_volume_no_data_raises_unavailable(self):
        response = make_response([], success=False, source="curve", error="no liquidityPoolDailySnapshots")
        patcher, _captured = patch_gateway(response)
        with patcher:
            provider = CurveVolumeProvider()
            with pytest.raises(DataSourceUnavailable):
                await provider.get_volume(
                    pool_address="0x123",
                    chain=Chain.ETHEREUM,
                    start_date=date(2024, 1, 15),
                    end_date=date(2024, 1, 17),
                )

    @pytest.mark.asyncio
    async def test_get_volume_unsupported_chain_raises(self):
        provider = CurveVolumeProvider()
        with pytest.raises(ValueError) as exc_info:
            await provider.get_volume(
                pool_address="0x123",
                chain=Chain.ARBITRUM,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        assert "Unsupported chain" in str(exc_info.value)
        assert "ARBITRUM" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_rpc_failure_raises_unavailable(self):
        with patch_gateway_rpc_error(RuntimeError("channel down")):
            provider = CurveVolumeProvider()
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
        response = make_response([make_point(_DAY_19676_TS, "1234567.89012345")])
        patcher, _captured = patch_gateway(response)
        with patcher:
            provider = CurveVolumeProvider()
            volumes = await provider.get_volume(
                pool_address="0x123",
                chain=Chain.ETHEREUM,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        assert volumes[0].value == Decimal("1234567.89012345")


class TestContextManager:
    @pytest.mark.asyncio
    async def test_context_manager_is_noop(self):
        provider = CurveVolumeProvider()
        async with provider:
            pass
        await provider.close()
