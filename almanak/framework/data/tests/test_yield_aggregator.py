"""Tests for YieldAggregator and YieldOpportunity dataclass."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import DataClassification, DataEnvelope
from almanak.framework.data.yields.aggregator import (
    YieldAggregator,
    YieldOpportunity,
)

# =============================================================================
# Helper: mock aiohttp response
# =============================================================================


def _mock_aiohttp_response(json_data: dict | list, status: int = 200) -> MagicMock:
    response = MagicMock()
    response.status = status
    response.json = AsyncMock(return_value=json_data)
    response.text = AsyncMock(return_value="error")
    response.__aenter__ = AsyncMock(return_value=response)
    response.__aexit__ = AsyncMock(return_value=False)
    return response


def _mock_session(response: MagicMock) -> MagicMock:
    session = MagicMock()
    session.get = MagicMock(return_value=response)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    return session


# =============================================================================
# Sample DeFi Llama yield pool data
# =============================================================================

_POOL_AAVE_USDC = {
    "pool": "aave-v3-usdc-arb",
    "chain": "Arbitrum",
    "project": "aave-v3",
    "symbol": "USDC",
    "tvlUsd": 50000000,
    "apy": 5.2,
    "apyBase": 4.8,
    "apyReward": 0.4,
    "ilRisk": False,
}

_POOL_COMPOUND_USDC = {
    "pool": "compound-v3-usdc-arb",
    "chain": "Arbitrum",
    "project": "compound-v3",
    "symbol": "USDC",
    "tvlUsd": 30000000,
    "apy": 4.1,
    "apyBase": 3.8,
    "apyReward": 0.3,
    "ilRisk": False,
}

_POOL_UNI_USDC_WETH = {
    "pool": "uniswap-v3-usdc-weth-arb",
    "chain": "Arbitrum",
    "project": "uniswap-v3",
    "symbol": "USDC-WETH",
    "tvlUsd": 20000000,
    "apy": 15.5,
    "apyBase": 14.0,
    "apyReward": 1.5,
    "ilRisk": True,
}

_POOL_LIDO_ETH = {
    "pool": "lido-steth-eth",
    "chain": "Ethereum",
    "project": "lido",
    "symbol": "STETH",
    "tvlUsd": 10000000000,
    "apy": 3.5,
    "apyBase": 3.5,
    "apyReward": None,
    "ilRisk": False,
}

_POOL_LOW_TVL = {
    "pool": "small-pool",
    "chain": "Arbitrum",
    "project": "unknown-dex",
    "symbol": "USDC-ARB",
    "tvlUsd": 50000,  # Below 100k min
    "apy": 50.0,
    "apyBase": 50.0,
    "apyReward": None,
    "ilRisk": True,
}

_ALL_POOLS = [_POOL_AAVE_USDC, _POOL_COMPOUND_USDC, _POOL_UNI_USDC_WETH, _POOL_LIDO_ETH, _POOL_LOW_TVL]


# =============================================================================
# YieldOpportunity dataclass tests
# =============================================================================


class TestYieldOpportunity:
    def test_construction(self):
        opp = YieldOpportunity(
            protocol="aave-v3",
            chain="arbitrum",
            pool_id="aave-v3-usdc",
            symbol="USDC",
            apy=5.2,
            tvl_usd=Decimal("50000000"),
            type="lending",
        )
        assert opp.protocol == "aave-v3"
        assert opp.apy == 5.2
        assert opp.type == "lending"
        assert opp.risk_score is None
        assert opp.il_risk is False

    def test_frozen(self):
        opp = YieldOpportunity(
            protocol="aave-v3",
            chain="arb",
            pool_id="x",
            symbol="USDC",
            apy=5.0,
            tvl_usd=Decimal("100"),
            type="lending",
        )
        with pytest.raises(AttributeError):
            opp.apy = 10.0  # type: ignore[misc]

    def test_with_all_fields(self):
        opp = YieldOpportunity(
            protocol="uniswap-v3",
            chain="arbitrum",
            pool_id="uni-usdc-weth",
            symbol="USDC-WETH",
            apy=15.5,
            apy_base=14.0,
            apy_reward=1.5,
            tvl_usd=Decimal("20000000"),
            type="lp",
            risk_score=0.3,
            il_risk=True,
        )
        assert opp.apy_base == 14.0
        assert opp.apy_reward == 1.5
        assert opp.risk_score == 0.3
        assert opp.il_risk is True


# =============================================================================
# YieldAggregator tests
# =============================================================================


class TestYieldAggregatorBasic:
    def test_get_yield_opportunities_usdc_arbitrum(self):
        agg = YieldAggregator()
        response = _mock_aiohttp_response({"data": _ALL_POOLS})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = agg.get_yield_opportunities("USDC", chains=["arbitrum"])

        assert isinstance(result, DataEnvelope)
        opps = result.value
        assert isinstance(opps, list)
        # Should find: aave USDC, compound USDC, uniswap USDC-WETH
        # Low TVL pool should be filtered out
        assert len(opps) >= 2
        # All should be on arbitrum chain
        for opp in opps:
            assert opp.chain == "arbitrum"
        # Sorted by APY descending
        for i in range(len(opps) - 1):
            assert opps[i].apy >= opps[i + 1].apy

    def test_get_yield_opportunities_all_chains(self):
        agg = YieldAggregator()
        response = _mock_aiohttp_response({"data": _ALL_POOLS})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = agg.get_yield_opportunities("USDC", chains=None)

        opps = result.value
        # Should find USDC across all chains (not low TVL, not STETH-only)
        assert len(opps) >= 2

    def test_get_yield_opportunities_min_tvl_filter(self):
        agg = YieldAggregator()
        response = _mock_aiohttp_response({"data": _ALL_POOLS})
        session = _mock_session(response)

        # Low min_tvl should include the small pool
        with patch("aiohttp.ClientSession", return_value=session):
            result = agg.get_yield_opportunities("USDC", chains=["arbitrum"], min_tvl=10_000)

        opps = result.value
        pool_ids = {o.pool_id for o in opps}
        # The low-TVL USDC-ARB pool should be included since its symbol contains USDC
        # and TVL > 10k
        assert "small-pool" in pool_ids

    def test_no_matching_token(self):
        agg = YieldAggregator()
        response = _mock_aiohttp_response({"data": _ALL_POOLS})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = agg.get_yield_opportunities("SHIBA", chains=["arbitrum"])

        assert result.value == []

    def test_get_yield_opportunities_meta(self):
        agg = YieldAggregator()
        response = _mock_aiohttp_response({"data": _ALL_POOLS})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = agg.get_yield_opportunities("USDC")

        assert result.meta.source == "defillama"
        assert result.meta.confidence == 0.85
        assert result.meta.finality == "off_chain"
        assert result.meta.cache_hit is False


class TestYieldAggregatorSorting:
    def test_sort_by_apy(self):
        agg = YieldAggregator()
        response = _mock_aiohttp_response({"data": _ALL_POOLS})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = agg.get_yield_opportunities("USDC", sort_by="apy")

        opps = result.value
        for i in range(len(opps) - 1):
            assert opps[i].apy >= opps[i + 1].apy

    def test_sort_by_tvl(self):
        agg = YieldAggregator()
        response = _mock_aiohttp_response({"data": _ALL_POOLS})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = agg.get_yield_opportunities("USDC", sort_by="tvl")

        opps = result.value
        for i in range(len(opps) - 1):
            assert float(opps[i].tvl_usd) >= float(opps[i + 1].tvl_usd)

    def test_sort_by_risk(self):
        agg = YieldAggregator()
        response = _mock_aiohttp_response({"data": _ALL_POOLS})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = agg.get_yield_opportunities("USDC", sort_by="risk_score")

        opps = result.value
        # Sorted ascending by risk (safest first)
        for i in range(len(opps) - 1):
            assert (opps[i].risk_score or 0.0) <= (opps[i + 1].risk_score or 0.0)


class TestYieldAggregatorTypeClassification:
    def test_lending_type(self):
        agg = YieldAggregator()
        response = _mock_aiohttp_response({"data": [_POOL_AAVE_USDC]})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = agg.get_yield_opportunities("USDC")

        assert result.value[0].type == "lending"

    def test_lp_type(self):
        agg = YieldAggregator()
        response = _mock_aiohttp_response({"data": [_POOL_UNI_USDC_WETH]})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = agg.get_yield_opportunities("USDC")

        assert result.value[0].type == "lp"
        assert result.value[0].il_risk is True

    def test_staking_type(self):
        agg = YieldAggregator()
        response = _mock_aiohttp_response({"data": [_POOL_LIDO_ETH]})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = agg.get_yield_opportunities("STETH")

        assert result.value[0].type == "staking"


class TestYieldAggregatorRiskScore:
    def test_high_apy_increases_risk(self):
        agg = YieldAggregator()
        high_apy_pool = {**_POOL_AAVE_USDC, "apy": 150.0, "apyBase": 150.0, "tvlUsd": 500000}
        response = _mock_aiohttp_response({"data": [high_apy_pool]})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = agg.get_yield_opportunities("USDC")

        if result.value:
            assert result.value[0].risk_score is not None
            assert result.value[0].risk_score > 0.2

    def test_trusted_protocol_reduces_risk(self):
        agg = YieldAggregator()
        # Aave with good TVL and normal APY
        response = _mock_aiohttp_response({"data": [_POOL_AAVE_USDC]})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = agg.get_yield_opportunities("USDC")

        assert result.value[0].risk_score is not None
        # Trusted protocol with high TVL and normal APY -> low risk
        assert result.value[0].risk_score <= 0.1


class TestYieldAggregatorCaching:
    def test_cache_hit(self):
        agg = YieldAggregator(cache_ttl=300)
        response = _mock_aiohttp_response({"data": _ALL_POOLS})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result1 = agg.get_yield_opportunities("USDC", chains=["arbitrum"])
            result2 = agg.get_yield_opportunities("USDC", chains=["arbitrum"])

        assert result1.meta.cache_hit is False
        assert result2.meta.cache_hit is True


class TestYieldAggregatorErrors:
    def test_api_error(self):
        agg = YieldAggregator()
        response = _mock_aiohttp_response({}, status=500)
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            with pytest.raises(DataSourceUnavailable):
                agg.get_yield_opportunities("USDC")

    def test_health(self):
        agg = YieldAggregator()
        h = agg.health()
        assert h["successes"] == 0
        assert h["failures"] == 0


# =============================================================================
# MarketSnapshot integration tests
# =============================================================================


class TestMarketSnapshotPoolAnalytics:
    def test_pool_analytics_no_reader(self):
        from almanak.framework.data.market_snapshot import MarketSnapshot

        ms = MarketSnapshot(chain="arbitrum", wallet_address="0x123")
        with pytest.raises(ValueError, match="No pool analytics reader"):
            ms.pool_analytics("0xpool")

    def test_pool_analytics_delegates(self):
        from almanak.framework.data.market_snapshot import (
            MarketSnapshot,
        )
        from almanak.framework.data.pools.analytics import PoolAnalytics, PoolAnalyticsReader

        reader = MagicMock(spec=PoolAnalyticsReader)
        analytics = PoolAnalytics(
            pool_address="0xpool",
            chain="arbitrum",
            protocol="uniswap-v3",
            tvl_usd=Decimal("1000000"),
            volume_24h_usd=Decimal("500000"),
            volume_7d_usd=Decimal("3000000"),
            fee_apr=10.0,
            fee_apy=10.5,
        )
        from almanak.framework.data.models import DataEnvelope, DataMeta

        meta = DataMeta(
            source="defillama",
            observed_at=None,
            finality="off_chain",
            confidence=0.85,
            cache_hit=False,
        )
        reader.get_pool_analytics.return_value = DataEnvelope(
            value=analytics,
            meta=meta,
            classification=DataClassification.INFORMATIONAL,
        )

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x123",
            pool_analytics_reader=reader,
        )
        result = ms.pool_analytics("0xpool")

        assert result.value.tvl_usd == Decimal("1000000")
        reader.get_pool_analytics.assert_called_once_with(
            pool_address="0xpool",
            chain="arbitrum",
            protocol=None,
        )

    def test_pool_analytics_error_wrapping(self):
        from almanak.framework.data.market_snapshot import (
            MarketSnapshot,
            PoolAnalyticsUnavailableError,
        )

        reader = MagicMock()
        reader.get_pool_analytics.side_effect = DataSourceUnavailable(source="test", reason="fail")

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x123",
            pool_analytics_reader=reader,
        )
        with pytest.raises(PoolAnalyticsUnavailableError):
            ms.pool_analytics("0xpool")

    def test_pool_analytics_default_chain(self):
        from almanak.framework.data.market_snapshot import MarketSnapshot

        reader = MagicMock()
        reader.get_pool_analytics.return_value = MagicMock()

        ms = MarketSnapshot(
            chain="base",
            wallet_address="0x123",
            pool_analytics_reader=reader,
        )
        ms.pool_analytics("0xpool")
        reader.get_pool_analytics.assert_called_once_with(
            pool_address="0xpool",
            chain="base",
            protocol=None,
        )


class TestMarketSnapshotBestPool:
    def test_best_pool_no_reader(self):
        from almanak.framework.data.market_snapshot import MarketSnapshot

        ms = MarketSnapshot(chain="arbitrum", wallet_address="0x123")
        with pytest.raises(ValueError, match="No pool analytics reader"):
            ms.best_pool("WETH", "USDC")

    def test_best_pool_delegates(self):
        from almanak.framework.data.market_snapshot import MarketSnapshot

        reader = MagicMock()
        reader.best_pool.return_value = MagicMock()

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x123",
            pool_analytics_reader=reader,
        )
        ms.best_pool("WETH", "USDC", metric="fee_apr")
        reader.best_pool.assert_called_once_with(
            token_a="WETH",
            token_b="USDC",
            chain="arbitrum",
            metric="fee_apr",
            protocols=None,
        )


class TestMarketSnapshotYieldOpportunities:
    def test_yield_no_aggregator(self):
        from almanak.framework.data.market_snapshot import MarketSnapshot

        ms = MarketSnapshot(chain="arbitrum", wallet_address="0x123")
        with pytest.raises(ValueError, match="No yield aggregator"):
            ms.yield_opportunities("USDC")

    def test_yield_delegates(self):
        from almanak.framework.data.market_snapshot import MarketSnapshot

        agg = MagicMock()
        agg.get_yield_opportunities.return_value = MagicMock()

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x123",
            yield_aggregator=agg,
        )
        ms.yield_opportunities("USDC", chains=["arbitrum"], min_tvl=50000)
        agg.get_yield_opportunities.assert_called_once_with(
            token="USDC",
            chains=["arbitrum"],
            min_tvl=50000,
            sort_by="apy",
        )

    def test_yield_error_wrapping(self):
        from almanak.framework.data.market_snapshot import (
            MarketSnapshot,
            YieldOpportunitiesUnavailableError,
        )

        agg = MagicMock()
        agg.get_yield_opportunities.side_effect = DataSourceUnavailable(source="test", reason="fail")

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x123",
            yield_aggregator=agg,
        )
        with pytest.raises(YieldOpportunitiesUnavailableError):
            ms.yield_opportunities("USDC")
