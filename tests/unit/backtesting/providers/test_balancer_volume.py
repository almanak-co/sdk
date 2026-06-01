"""Unit tests for the gateway-backed Balancer V2 Volume Provider.

**VIB-4870 / W7**: ``BalancerVolumeProvider`` is now a thin gRPC client of
``RateHistoryService.GetDexVolumeHistory``. Tests assert the gateway-
client path; volume VALUES stay byte-equivalent (W7 §6); the pre-W7
silent-zero fallback is replaced by ``DataSourceUnavailable``. The RPC
routes on the canonical ``dex_name`` ``"balancer_v2"``.
"""

from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.core.enums import Chain
from almanak.framework.backtesting.pnl.providers.dex.balancer_volume import (
    BALANCER_SUBGRAPH_IDS,
    DATA_SOURCE,
    SUPPORTED_CHAINS,
    BalancerVolumeProvider,
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
        provider = BalancerVolumeProvider()
        assert provider.supported_chains == SUPPORTED_CHAINS
        assert provider._fallback_volume == Decimal("0")

    def test_init_legacy_kwargs_accepted(self):
        provider = BalancerVolumeProvider(client=MagicMock(), fallback_volume=Decimal("1000"), requests_per_minute=50)
        assert provider._fallback_volume == Decimal("1000")

    def test_supported_chains_property_returns_copy(self):
        provider = BalancerVolumeProvider()
        assert provider.supported_chains is not provider.supported_chains


class TestSupportedChains:
    def test_supported_chains_include_required_networks(self):
        assert Chain.ETHEREUM in SUPPORTED_CHAINS
        assert Chain.ARBITRUM in SUPPORTED_CHAINS
        assert Chain.POLYGON in SUPPORTED_CHAINS

    def test_all_supported_chains_have_subgraph_ids(self):
        for chain in SUPPORTED_CHAINS:
            assert chain in BALANCER_SUBGRAPH_IDS
            assert BALANCER_SUBGRAPH_IDS[chain]


class TestGetVolume:
    @pytest.mark.asyncio
    async def test_get_volume_success_routes_to_balancer_v2(self):
        response = make_response([make_point(1705276800, "42.0")])
        patcher, captured = patch_gateway(response)
        with patcher:
            provider = BalancerVolumeProvider()
            volumes = await provider.get_volume(
                pool_address="0xBAL",
                chain=Chain.ETHEREUM,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        assert len(volumes) == 1
        assert volumes[0].value == Decimal("42.0")
        assert volumes[0].source_info.source == DATA_SOURCE
        assert volumes[0].source_info.confidence == DataConfidence.HIGH
        request = captured["request"]
        # Routes on the canonical dex_name, not the legacy "balancer" alias.
        assert request.dex == "balancer_v2"
        assert request.chain == "ethereum"
        assert request.interval_secs == 86400
        assert request.start_ts < request.end_ts

    @pytest.mark.asyncio
    async def test_get_volume_no_data_raises_unavailable(self):
        response = make_response([], success=False, source="balancer_v2", error="no poolSnapshots")
        patcher, _captured = patch_gateway(response)
        with patcher:
            provider = BalancerVolumeProvider()
            with pytest.raises(DataSourceUnavailable):
                await provider.get_volume(
                    pool_address="0x123",
                    chain=Chain.ETHEREUM,
                    start_date=date(2024, 1, 15),
                    end_date=date(2024, 1, 17),
                )

    @pytest.mark.asyncio
    async def test_get_volume_unsupported_chain_raises(self):
        provider = BalancerVolumeProvider()
        with pytest.raises(ValueError) as exc_info:
            await provider.get_volume(
                pool_address="0x123",
                chain=Chain.AVALANCHE,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        assert "Unsupported chain" in str(exc_info.value)
        assert "AVALANCHE" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_rpc_failure_raises_unavailable(self):
        with patch_gateway_rpc_error(RuntimeError("channel down")):
            provider = BalancerVolumeProvider()
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
            provider = BalancerVolumeProvider()
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
            provider = BalancerVolumeProvider()
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
        provider = BalancerVolumeProvider()
        async with provider:
            pass
        await provider.close()
