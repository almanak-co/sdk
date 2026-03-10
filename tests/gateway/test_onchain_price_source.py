"""Tests for OnChainPriceSource -- Chainlink on-chain pricing."""

import time
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from almanak.gateway.data.price.onchain import OnChainPriceSource


def _build_chainlink_response(answer: int, updated_at: int) -> str:
    """Build a hex-encoded latestRoundData() response.

    Returns 5 x 32-byte words: (roundId, answer, startedAt, updatedAt, answeredInRound)
    """
    round_id = 1000
    started_at = updated_at - 10
    answered_in_round = round_id

    words = [
        round_id.to_bytes(32, byteorder="big"),
        answer.to_bytes(32, byteorder="big", signed=True),
        started_at.to_bytes(32, byteorder="big"),
        updated_at.to_bytes(32, byteorder="big"),
        answered_in_round.to_bytes(32, byteorder="big"),
    ]
    data = b"".join(words)
    return "0x" + data.hex()


class TestStablecoins:
    """Stablecoins return $1.00 without any RPC calls."""

    @pytest.mark.asyncio
    async def test_usdc_returns_one(self):
        source = OnChainPriceSource(chain="arbitrum")
        result = await source.get_price("USDC", "USD")

        assert result.price == Decimal("1.00")
        assert result.source == "onchain"
        assert result.confidence == 0.99
        assert result.stale is False
        await source.close()

    @pytest.mark.asyncio
    async def test_usdt_returns_one(self):
        source = OnChainPriceSource(chain="arbitrum")
        result = await source.get_price("USDT", "USD")

        assert result.price == Decimal("1.00")
        assert result.confidence == 0.99
        await source.close()

    @pytest.mark.asyncio
    async def test_dai_returns_one(self):
        source = OnChainPriceSource(chain="ethereum")
        result = await source.get_price("DAI", "USD")

        assert result.price == Decimal("1.00")
        await source.close()

    @pytest.mark.asyncio
    async def test_stablecoin_case_insensitive(self):
        source = OnChainPriceSource(chain="arbitrum")
        result = await source.get_price("usdc", "USD")

        assert result.price == Decimal("1.00")
        await source.close()


class TestChainlinkPricing:
    """Chainlink latestRoundData() pricing."""

    @pytest.mark.asyncio
    async def test_eth_usd_price(self):
        """ETH/USD price decoded correctly from mocked latestRoundData."""
        source = OnChainPriceSource(chain="arbitrum")

        # ETH at $2500.12345678 with 8 decimals -> answer = 250012345678
        answer = 250012345678
        now = int(time.time())
        mock_response = _build_chainlink_response(answer, now)

        with patch.object(source, "_eth_call", new_callable=AsyncMock, return_value=mock_response):
            result = await source.get_price("ETH", "USD")

        assert result.price == Decimal("2500.12345678")
        assert result.source == "onchain_chainlink"
        assert result.confidence == 0.95
        assert result.stale is False
        await source.close()

    @pytest.mark.asyncio
    async def test_weth_returns_same_as_eth(self):
        """WETH maps to ETH/USD feed (same price)."""
        source = OnChainPriceSource(chain="arbitrum")

        answer = 300000000000  # $3000.00
        now = int(time.time())
        mock_response = _build_chainlink_response(answer, now)

        with patch.object(source, "_eth_call", new_callable=AsyncMock, return_value=mock_response):
            result = await source.get_price("WETH", "USD")

        assert result.price == Decimal("3000.00000000")
        assert result.source == "onchain_chainlink"
        await source.close()

    @pytest.mark.asyncio
    async def test_btc_usd_price(self):
        """BTC/USD price decoded correctly."""
        source = OnChainPriceSource(chain="ethereum")

        answer = 6700000000000  # $67000.00
        now = int(time.time())
        mock_response = _build_chainlink_response(answer, now)

        with patch.object(source, "_eth_call", new_callable=AsyncMock, return_value=mock_response):
            result = await source.get_price("BTC", "USD")

        assert result.price == Decimal("67000.00000000")
        await source.close()

    @pytest.mark.asyncio
    async def test_arb_usd_price(self):
        """ARB/USD price on Arbitrum chain."""
        source = OnChainPriceSource(chain="arbitrum")

        answer = 120000000  # $1.20
        now = int(time.time())
        mock_response = _build_chainlink_response(answer, now)

        with patch.object(source, "_eth_call", new_callable=AsyncMock, return_value=mock_response):
            result = await source.get_price("ARB", "USD")

        assert result.price == Decimal("1.20000000")
        await source.close()


class TestStaleness:
    """Stale Chainlink data returns price with reduced confidence."""

    @pytest.mark.asyncio
    async def test_stale_data_returns_reduced_confidence(self):
        """Data older than threshold returns confidence=0.85 (not an error)."""
        source = OnChainPriceSource(chain="arbitrum")

        answer = 250000000000  # $2500
        # updated_at is 2 hours ago -- stale
        stale_time = int(time.time()) - 7200
        mock_response = _build_chainlink_response(answer, stale_time)

        with patch.object(source, "_eth_call", new_callable=AsyncMock, return_value=mock_response):
            result = await source.get_price("ETH", "USD")

        assert result.price == Decimal("2500.00000000")
        assert result.confidence == 0.85
        assert result.stale is True
        await source.close()

    @pytest.mark.asyncio
    async def test_fresh_data_returns_full_confidence(self):
        """Data within threshold returns confidence=0.95."""
        source = OnChainPriceSource(chain="arbitrum")

        answer = 250000000000
        now = int(time.time())
        mock_response = _build_chainlink_response(answer, now)

        with patch.object(source, "_eth_call", new_callable=AsyncMock, return_value=mock_response):
            result = await source.get_price("ETH", "USD")

        assert result.confidence == 0.95
        assert result.stale is False
        await source.close()


class TestCache:
    """Cache prevents duplicate RPC calls within TTL."""

    @pytest.mark.asyncio
    async def test_cache_prevents_duplicate_rpc(self):
        """Second call within TTL returns cached result, no new RPC call."""
        source = OnChainPriceSource(chain="arbitrum", cache_ttl=60.0)

        answer = 250000000000
        now = int(time.time())
        mock_response = _build_chainlink_response(answer, now)
        mock_call = AsyncMock(return_value=mock_response)

        with patch.object(source, "_eth_call", mock_call):
            # First call -- hits RPC
            result1 = await source.get_price("ETH", "USD")
            # Second call -- should use cache
            result2 = await source.get_price("ETH", "USD")

        assert result1.price == result2.price
        assert mock_call.call_count == 1  # Only one RPC call
        await source.close()

    @pytest.mark.asyncio
    async def test_stablecoin_cache(self):
        """Stablecoin results are cached too."""
        source = OnChainPriceSource(chain="arbitrum", cache_ttl=60.0)

        result1 = await source.get_price("USDC", "USD")
        result2 = await source.get_price("USDC", "USD")

        assert result1.price == result2.price == Decimal("1.00")
        await source.close()


class TestErrorHandling:
    """Error cases raise DataSourceUnavailable."""

    @pytest.mark.asyncio
    async def test_unsupported_token_raises(self):
        """Token not in Chainlink feeds raises DataSourceUnavailable."""
        from almanak.framework.data.interfaces import DataSourceUnavailable

        source = OnChainPriceSource(chain="arbitrum")

        with pytest.raises(DataSourceUnavailable, match="No Chainlink feed"):
            await source.get_price("OBSCURETOKEN123", "USD")

        await source.close()

    @pytest.mark.asyncio
    async def test_rpc_failure_raises(self):
        """RPC call failure raises DataSourceUnavailable."""
        from almanak.framework.data.interfaces import DataSourceUnavailable

        source = OnChainPriceSource(chain="arbitrum")

        with patch.object(source, "_eth_call", new_callable=AsyncMock, side_effect=RuntimeError("RPC down")):
            with pytest.raises(DataSourceUnavailable, match="Chainlink RPC call failed"):
                await source.get_price("ETH", "USD")

        await source.close()

    @pytest.mark.asyncio
    async def test_non_usd_quote_raises(self):
        """Non-USD quote raises DataSourceUnavailable."""
        from almanak.framework.data.interfaces import DataSourceUnavailable

        source = OnChainPriceSource(chain="arbitrum")

        with pytest.raises(DataSourceUnavailable, match="Only USD quote supported"):
            await source.get_price("ETH", "EUR")

        await source.close()

    @pytest.mark.asyncio
    async def test_no_rpc_url_raises(self):
        """No RPC URL available raises DataSourceUnavailable."""
        from almanak.framework.data.interfaces import DataSourceUnavailable

        with patch("almanak.gateway.data.price.onchain.get_rpc_url", side_effect=ValueError("No RPC")):
            source = OnChainPriceSource(chain="nonexistent", network="mainnet")

        # Stablecoins still work (no RPC needed)
        result = await source.get_price("USDC", "USD")
        assert result.price == Decimal("1.00")

        # Non-stablecoins fail
        with pytest.raises(DataSourceUnavailable, match="No RPC URL"):
            await source.get_price("ETH", "USD")

        await source.close()

    @pytest.mark.asyncio
    async def test_short_response_raises(self):
        """Too-short Chainlink response raises DataSourceUnavailable."""
        from almanak.framework.data.interfaces import DataSourceUnavailable

        source = OnChainPriceSource(chain="arbitrum")

        with patch.object(source, "_eth_call", new_callable=AsyncMock, return_value="0x" + "00" * 64):
            with pytest.raises(DataSourceUnavailable, match="response too short"):
                await source.get_price("ETH", "USD")

        await source.close()

    @pytest.mark.asyncio
    async def test_malformed_hex_response_raises(self):
        """Malformed Chainlink hex response raises DataSourceUnavailable."""
        from almanak.framework.data.interfaces import DataSourceUnavailable

        source = OnChainPriceSource(chain="arbitrum")

        with patch.object(source, "_eth_call", new_callable=AsyncMock, return_value="0xabc"):
            with pytest.raises(DataSourceUnavailable, match="Malformed RPC hex"):
                await source.get_price("ETH", "USD")

        await source.close()

    @pytest.mark.asyncio
    async def test_zero_answer_raises(self):
        """Chainlink returning answer=0 raises DataSourceUnavailable."""
        from almanak.framework.data.interfaces import DataSourceUnavailable

        source = OnChainPriceSource(chain="arbitrum")

        now = int(time.time())
        mock_response = _build_chainlink_response(0, now)

        with patch.object(source, "_eth_call", new_callable=AsyncMock, return_value=mock_response):
            with pytest.raises(DataSourceUnavailable, match="non-positive answer"):
                await source.get_price("ETH", "USD")

        await source.close()

    @pytest.mark.asyncio
    async def test_negative_answer_raises(self):
        """Chainlink returning answer<0 raises DataSourceUnavailable."""
        from almanak.framework.data.interfaces import DataSourceUnavailable

        source = OnChainPriceSource(chain="arbitrum")

        now = int(time.time())
        mock_response = _build_chainlink_response(-1, now)

        with patch.object(source, "_eth_call", new_callable=AsyncMock, return_value=mock_response):
            with pytest.raises(DataSourceUnavailable, match="non-positive answer"):
                await source.get_price("ETH", "USD")

        await source.close()


class TestSessionCleanup:
    """Session lifecycle management."""

    @pytest.mark.asyncio
    async def test_close_cleans_up_session(self):
        """close() properly shuts down the aiohttp session."""
        source = OnChainPriceSource(chain="arbitrum")

        # Force session creation via a stablecoin call (no RPC needed)
        await source.get_price("USDC", "USD")

        # No session created for stablecoins, so create one manually
        import aiohttp

        source._session = aiohttp.ClientSession()
        assert not source._session.closed

        await source.close()
        assert source._session is None

    @pytest.mark.asyncio
    async def test_close_without_session(self):
        """close() is safe when no session exists."""
        source = OnChainPriceSource(chain="arbitrum")
        await source.close()  # Should not raise


class _MockChainIdResponse:
    """Async context manager that mimics aiohttp response for chainId calls."""

    def __init__(self, chain_id: int):
        self.status = 200
        self._body = {"result": hex(chain_id)}

    async def json(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class TestChainIdValidation:
    """chainId validation on first use."""

    @pytest.mark.asyncio
    async def test_matching_chain_id_keeps_rpc(self):
        """Correct chainId allows on-chain pricing to proceed."""
        source = OnChainPriceSource(chain="arbitrum")

        mock_session = AsyncMock()
        mock_session.post = lambda *a, **kw: _MockChainIdResponse(42161)

        with patch.object(source, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            await source._validate_chain_id()

        assert source._rpc_url is not None
        assert source._chain_id_validated is True
        await source.close()

    @pytest.mark.asyncio
    async def test_mismatched_chain_id_disables_rpc(self):
        """Wrong chainId disables on-chain pricing."""
        source = OnChainPriceSource(chain="arbitrum")

        mock_session = AsyncMock()
        mock_session.post = lambda *a, **kw: _MockChainIdResponse(1)  # Ethereum, not Arbitrum

        with patch.object(source, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            await source._validate_chain_id()

        assert source._rpc_url is None
        assert source._chain_id_validated is True
        await source.close()

    @pytest.mark.asyncio
    async def test_chain_id_validation_failure_skips(self):
        """Network error during chainId check is non-fatal."""
        source = OnChainPriceSource(chain="arbitrum")
        original_rpc = source._rpc_url

        with patch.object(source, "_get_session", new_callable=AsyncMock, side_effect=Exception("connection refused")):
            await source._validate_chain_id()

        assert source._rpc_url == original_rpc
        assert source._chain_id_validated is True
        await source.close()

    @pytest.mark.asyncio
    async def test_chain_id_validation_runs_once(self):
        """chainId validation only runs on first call."""
        source = OnChainPriceSource(chain="arbitrum")
        source._chain_id_validated = True

        await source._validate_chain_id()
        await source.close()


class TestSourceProperties:
    """Source metadata properties."""

    def test_source_name(self):
        source = OnChainPriceSource(chain="arbitrum")
        assert source.source_name == "onchain"

    def test_supported_tokens_arbitrum(self):
        source = OnChainPriceSource(chain="arbitrum")
        tokens = source.supported_tokens
        assert "ETH" in tokens
        assert "WETH" in tokens
        assert "ARB" in tokens
        assert "GMX" in tokens

    def test_supported_tokens_ethereum(self):
        source = OnChainPriceSource(chain="ethereum")
        tokens = source.supported_tokens
        assert "ETH" in tokens
        assert "BTC" in tokens
        assert "LINK" in tokens

    def test_supported_tokens_unsupported_chain(self):
        """Chain without feeds returns empty list."""
        with patch("almanak.gateway.data.price.onchain.get_rpc_url", side_effect=ValueError("No RPC")):
            source = OnChainPriceSource(chain="nonexistent")
        assert source.supported_tokens == []

    def test_cache_ttl(self):
        source = OnChainPriceSource(chain="arbitrum", cache_ttl=15.0)
        assert source.cache_ttl_seconds == 15


class TestDerivedPricing:
    """Derived pricing: TOKEN/ETH × ETH/USD for tokens without direct USD feeds."""

    @pytest.mark.asyncio
    async def test_wsteth_derived_price_on_arbitrum(self):
        """wstETH uses WSTETH/ETH × ETH/USD when no direct WSTETH/USD feed exists."""
        source = OnChainPriceSource(chain="arbitrum")

        now = int(time.time())
        # WSTETH/ETH = 1.18 (18 decimals) -> 1180000000000000000
        wsteth_eth_response = _build_chainlink_response(1180000000000000000, now)
        # ETH/USD = $2500 (8 decimals) -> 250000000000
        eth_usd_response = _build_chainlink_response(250000000000, now)

        call_count = 0

        async def mock_eth_call(to: str, data: str) -> str:
            nonlocal call_count
            call_count += 1
            # First call is WSTETH/ETH, second is ETH/USD
            if call_count == 1:
                return wsteth_eth_response
            return eth_usd_response

        with patch.object(source, "_eth_call", side_effect=mock_eth_call):
            result = await source.get_price("WSTETH", "USD")

        # 1.18 * 2500 = 2950
        expected = Decimal("1.180000000000000000") * Decimal("2500.00000000")
        assert result.price == expected
        assert result.source == "onchain_derived"
        # Confidence: min(0.95, 0.95) * 0.95 = 0.9025
        assert result.confidence == pytest.approx(0.9025, abs=0.001)
        await source.close()

    @pytest.mark.asyncio
    async def test_steth_derived_price_uses_wsteth_feed(self):
        """stETH maps to WSTETH/ETH feed for derived pricing."""
        source = OnChainPriceSource(chain="arbitrum")

        now = int(time.time())
        wsteth_eth_response = _build_chainlink_response(1180000000000000000, now)
        eth_usd_response = _build_chainlink_response(250000000000, now)

        call_count = 0

        async def mock_eth_call(to: str, data: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return wsteth_eth_response
            return eth_usd_response

        with patch.object(source, "_eth_call", side_effect=mock_eth_call):
            result = await source.get_price("STETH", "USD")

        assert result.price > Decimal("0")
        assert result.source == "onchain_derived"
        await source.close()

    @pytest.mark.asyncio
    async def test_derived_price_not_available_on_ethereum(self):
        """Derived pricing not configured for ethereum -- falls through to error."""
        from almanak.framework.data.interfaces import DataSourceUnavailable

        source = OnChainPriceSource(chain="ethereum")

        # Ethereum has WSTETH/USD in the main feeds dict, so it uses the direct feed.
        # But if we remove it to test derived fallback, there's no ETH-denominated config.
        # This test verifies no crash when derived config is absent.
        with patch.dict(source._feeds, {"WSTETH/USD": None}, clear=False):
            # Remove WSTETH/USD from feeds to force derived path
            del source._feeds["WSTETH/USD"]
            with pytest.raises(DataSourceUnavailable, match="No Chainlink feed"):
                await source.get_price("WSTETH", "USD")

        await source.close()

    @pytest.mark.asyncio
    async def test_derived_price_caches_result(self):
        """Derived price is cached, second call doesn't make RPC calls."""
        source = OnChainPriceSource(chain="arbitrum", cache_ttl=60.0)

        now = int(time.time())
        wsteth_eth_response = _build_chainlink_response(1180000000000000000, now)
        eth_usd_response = _build_chainlink_response(250000000000, now)

        call_count = 0

        async def mock_eth_call(to: str, data: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                return wsteth_eth_response
            return eth_usd_response

        with patch.object(source, "_eth_call", side_effect=mock_eth_call):
            result1 = await source.get_price("WSTETH", "USD")
            result2 = await source.get_price("WSTETH", "USD")

        assert result1.price == result2.price
        assert call_count == 2  # Only 2 RPC calls (WSTETH/ETH + ETH/USD), second get_price cached
        await source.close()
