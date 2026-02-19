"""Tests for token decimals registry and ERC20 fallback."""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.paper.engine import (
    CHAIN_ID_ARBITRUM,
    CHAIN_ID_BASE,
    CHAIN_ID_ETHEREUM,
    ERC20_DECIMALS_CALL_TIMEOUT,
    ERC20_DECIMALS_SELECTOR,
    NATIVE_ETH_ADDRESS,
    TOKEN_DECIMALS,
    _fetch_erc20_decimals,
    get_token_decimals,
    get_token_decimals_with_fallback,
)


class TestGetTokenDecimals:
    """Tests for the synchronous get_token_decimals() function."""

    def test_usdc_on_ethereum(self):
        """Test USDC lookup on Ethereum (6 decimals)."""
        result = get_token_decimals(
            CHAIN_ID_ETHEREUM, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        )
        assert result == 6

    def test_usdc_on_arbitrum(self):
        """Test native USDC lookup on Arbitrum (6 decimals)."""
        result = get_token_decimals(
            CHAIN_ID_ARBITRUM, "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        )
        assert result == 6

    def test_weth_on_ethereum(self):
        """Test WETH lookup on Ethereum (18 decimals)."""
        result = get_token_decimals(
            CHAIN_ID_ETHEREUM, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        )
        assert result == 18

    def test_wbtc_on_ethereum(self):
        """Test WBTC lookup on Ethereum (8 decimals)."""
        result = get_token_decimals(
            CHAIN_ID_ETHEREUM, "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
        )
        assert result == 8

    def test_case_insensitive_lookup(self):
        """Test that address lookup is case-insensitive."""
        # Uppercase
        result1 = get_token_decimals(
            CHAIN_ID_ETHEREUM, "0xA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48"
        )
        # Lowercase
        result2 = get_token_decimals(
            CHAIN_ID_ETHEREUM, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        )
        # Mixed case
        result3 = get_token_decimals(
            CHAIN_ID_ETHEREUM, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        )

        assert result1 == result2 == result3 == 6

    def test_native_eth_on_ethereum(self):
        """Test native ETH sentinel address on Ethereum."""
        result = get_token_decimals(CHAIN_ID_ETHEREUM, NATIVE_ETH_ADDRESS)
        assert result == 18

    def test_native_eth_on_arbitrum(self):
        """Test native ETH sentinel address on Arbitrum."""
        result = get_token_decimals(CHAIN_ID_ARBITRUM, NATIVE_ETH_ADDRESS)
        assert result == 18

    def test_unknown_token_returns_none(self):
        """Test that unknown tokens return None."""
        result = get_token_decimals(
            CHAIN_ID_ETHEREUM, "0x1234567890123456789012345678901234567890"
        )
        assert result is None

    def test_known_token_on_wrong_chain_returns_none(self):
        """Test that a token on wrong chain returns None."""
        # USDC address on Ethereum, but querying Base (not bridged)
        result = get_token_decimals(
            CHAIN_ID_BASE, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        )
        assert result is None


class TestFetchERC20Decimals:
    """Tests for the ERC20 decimals() fallback fetch."""

    @pytest.mark.asyncio
    async def test_successful_fetch(self):
        """Test successful ERC20 decimals() call."""
        mock_response = MagicMock()
        mock_response.json = AsyncMock(
            return_value={"jsonrpc": "2.0", "id": 1, "result": "0x06"}  # 6 decimals
        )

        mock_session_instance = MagicMock()
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)
        mock_session_instance.post = MagicMock(return_value=mock_response)
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession", return_value=mock_session_instance):
            result = await _fetch_erc20_decimals(
                "http://localhost:8545", "0xTokenAddress"
            )

        assert result == 6

    @pytest.mark.asyncio
    async def test_fetch_18_decimals(self):
        """Test ERC20 decimals() call returning 18."""
        mock_response = MagicMock()
        mock_response.json = AsyncMock(
            return_value={"jsonrpc": "2.0", "id": 1, "result": "0x12"}  # 18 decimals
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session_instance = MagicMock()
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)
        mock_session_instance.post = MagicMock(return_value=mock_response)

        with patch("aiohttp.ClientSession", return_value=mock_session_instance):
            result = await _fetch_erc20_decimals(
                "http://localhost:8545", "0xTokenAddress"
            )

        assert result == 18

    @pytest.mark.asyncio
    async def test_rpc_error_returns_none(self):
        """Test that RPC error returns None."""
        mock_response = MagicMock()
        mock_response.json = AsyncMock(
            return_value={
                "jsonrpc": "2.0",
                "id": 1,
                "error": {"code": -32000, "message": "Execution reverted"},
            }
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session_instance = MagicMock()
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)
        mock_session_instance.post = MagicMock(return_value=mock_response)

        with patch("aiohttp.ClientSession", return_value=mock_session_instance):
            result = await _fetch_erc20_decimals(
                "http://localhost:8545", "0xTokenAddress"
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_empty_result_returns_none(self):
        """Test that empty result (0x) returns None."""
        mock_response = MagicMock()
        mock_response.json = AsyncMock(
            return_value={"jsonrpc": "2.0", "id": 1, "result": "0x"}
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session_instance = MagicMock()
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)
        mock_session_instance.post = MagicMock(return_value=mock_response)

        with patch("aiohttp.ClientSession", return_value=mock_session_instance):
            result = await _fetch_erc20_decimals(
                "http://localhost:8545", "0xTokenAddress"
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_timeout_returns_none(self, caplog):
        """Test that timeout returns None and logs warning."""
        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session_class.side_effect = TimeoutError()

            with caplog.at_level(logging.WARNING):
                result = await _fetch_erc20_decimals(
                    "http://localhost:8545", "0xTokenAddress"
                )

        assert result is None
        assert "Timeout" in caplog.text

    @pytest.mark.asyncio
    async def test_invalid_decimals_returns_none(self, caplog):
        """Test that decimals > 255 returns None with warning."""
        mock_response = MagicMock()
        mock_response.json = AsyncMock(
            return_value={"jsonrpc": "2.0", "id": 1, "result": "0x100"}  # 256 - invalid
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session_instance = MagicMock()
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)
        mock_session_instance.post = MagicMock(return_value=mock_response)

        with patch("aiohttp.ClientSession", return_value=mock_session_instance):
            with caplog.at_level(logging.WARNING):
                result = await _fetch_erc20_decimals(
                    "http://localhost:8545", "0xTokenAddress"
                )

        assert result is None
        assert "Invalid decimals" in caplog.text

    @pytest.mark.asyncio
    async def test_connection_error_returns_none(self):
        """Test that connection errors return None."""
        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session_class.side_effect = ConnectionError("Connection refused")

            result = await _fetch_erc20_decimals(
                "http://localhost:8545", "0xTokenAddress"
            )

        assert result is None


class TestGetTokenDecimalsWithFallback:
    """Tests for get_token_decimals_with_fallback() async function."""

    @pytest.mark.asyncio
    async def test_native_eth_returns_18(self):
        """Test that native ETH always returns 18."""
        result = await get_token_decimals_with_fallback(
            CHAIN_ID_ETHEREUM,
            NATIVE_ETH_ADDRESS,
            rpc_url="http://localhost:8545",
        )
        assert result == 18

    @pytest.mark.asyncio
    async def test_native_eth_uppercase(self):
        """Test native ETH with uppercase address."""
        result = await get_token_decimals_with_fallback(
            CHAIN_ID_ETHEREUM,
            NATIVE_ETH_ADDRESS.upper(),
            rpc_url="http://localhost:8545",
        )
        assert result == 18

    @pytest.mark.asyncio
    async def test_known_token_from_registry(self):
        """Test known token returns from registry without RPC call."""
        with patch(
            "almanak.framework.backtesting.paper.engine._fetch_erc20_decimals"
        ) as mock_fetch:
            result = await get_token_decimals_with_fallback(
                CHAIN_ID_ETHEREUM,
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
                rpc_url="http://localhost:8545",
            )

        # Should not call fetch - token is in registry
        mock_fetch.assert_not_called()
        assert result == 6

    @pytest.mark.asyncio
    async def test_unknown_token_fetches_from_chain(self):
        """Test unknown token fetches decimals from chain."""
        unknown_token = "0x1234567890123456789012345678901234567890"

        with patch(
            "almanak.framework.backtesting.paper.engine._fetch_erc20_decimals"
        ) as mock_fetch:
            mock_fetch.return_value = 8  # WBTC-like

            result = await get_token_decimals_with_fallback(
                CHAIN_ID_ETHEREUM,
                unknown_token,
                rpc_url="http://localhost:8545",
            )

        mock_fetch.assert_called_once_with("http://localhost:8545", unknown_token)
        assert result == 8

    @pytest.mark.asyncio
    async def test_caching_after_successful_fetch(self):
        """Test that successful fetch caches result in registry."""
        unknown_token = "0x9999888877776666555544443333222211110000"
        chain_id = 99999  # Unlikely to exist

        # Ensure it's not in cache
        key = (chain_id, unknown_token.lower())
        if key in TOKEN_DECIMALS:
            del TOKEN_DECIMALS[key]

        with patch(
            "almanak.framework.backtesting.paper.engine._fetch_erc20_decimals"
        ) as mock_fetch:
            mock_fetch.return_value = 8

            result = await get_token_decimals_with_fallback(
                chain_id,
                unknown_token,
                rpc_url="http://localhost:8545",
            )

        assert result == 8
        # Verify it was cached
        assert TOKEN_DECIMALS.get((chain_id, unknown_token.lower())) == 8

        # Clean up
        del TOKEN_DECIMALS[(chain_id, unknown_token.lower())]

    @pytest.mark.asyncio
    async def test_no_rpc_url_defaults_to_18(self, caplog):
        """Test that missing RPC URL defaults to 18 with warning."""
        # Use unique address to avoid cache pollution from other tests
        unknown_token = "0xDDDDEEEEFFFF00001111222233334444DDDDEEEE"
        chain_id = 77777  # Unlikely to exist

        # Ensure clean state
        key = (chain_id, unknown_token.lower())
        if key in TOKEN_DECIMALS:
            del TOKEN_DECIMALS[key]

        with caplog.at_level(logging.WARNING):
            result = await get_token_decimals_with_fallback(
                chain_id,
                unknown_token,
                rpc_url=None,
            )

        assert result == 18
        assert "no RPC URL provided" in caplog.text

    @pytest.mark.asyncio
    async def test_failed_fetch_defaults_to_18(self, caplog):
        """Test that failed fetch defaults to 18 with warning."""
        # Use unique address to avoid cache pollution from other tests
        unknown_token = "0xCCCCDDDDEEEEFFFF000011112222333344445555"
        chain_id = 66666  # Unlikely to exist

        # Ensure clean state
        key = (chain_id, unknown_token.lower())
        if key in TOKEN_DECIMALS:
            del TOKEN_DECIMALS[key]

        with patch(
            "almanak.framework.backtesting.paper.engine._fetch_erc20_decimals"
        ) as mock_fetch:
            mock_fetch.return_value = None  # Simulates failure

            with caplog.at_level(logging.WARNING):
                result = await get_token_decimals_with_fallback(
                    chain_id,
                    unknown_token,
                    rpc_url="http://localhost:8545",
                )

        assert result == 18
        assert "Could not fetch decimals" in caplog.text

    @pytest.mark.asyncio
    async def test_second_lookup_uses_cache(self):
        """Test that second lookup uses cached value."""
        unknown_token = "0xAAAABBBBCCCCDDDDEEEEFFFF0000111122223333"
        chain_id = 88888

        # Ensure clean state
        key = (chain_id, unknown_token.lower())
        if key in TOKEN_DECIMALS:
            del TOKEN_DECIMALS[key]

        with patch(
            "almanak.framework.backtesting.paper.engine._fetch_erc20_decimals"
        ) as mock_fetch:
            mock_fetch.return_value = 12

            # First call
            result1 = await get_token_decimals_with_fallback(
                chain_id,
                unknown_token,
                rpc_url="http://localhost:8545",
            )

            # Second call should use cache
            result2 = await get_token_decimals_with_fallback(
                chain_id,
                unknown_token,
                rpc_url="http://localhost:8545",
            )

        # Should only be called once
        assert mock_fetch.call_count == 1
        assert result1 == 12
        assert result2 == 12

        # Clean up
        del TOKEN_DECIMALS[(chain_id, unknown_token.lower())]


class TestConstants:
    """Tests for module constants."""

    def test_native_eth_address_format(self):
        """Test native ETH address is correct format."""
        assert NATIVE_ETH_ADDRESS == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        assert len(NATIVE_ETH_ADDRESS) == 42

    def test_erc20_decimals_selector(self):
        """Test ERC20 decimals() selector is correct."""
        # keccak256("decimals()") first 4 bytes
        assert ERC20_DECIMALS_SELECTOR == "0x313ce567"

    def test_timeout_is_reasonable(self):
        """Test timeout value is reasonable."""
        assert ERC20_DECIMALS_CALL_TIMEOUT == 2.0

    def test_chain_ids(self):
        """Test chain IDs are correct."""
        assert CHAIN_ID_ETHEREUM == 1
        assert CHAIN_ID_ARBITRUM == 42161
        assert CHAIN_ID_BASE == 8453


class TestTokenDecimalsRegistry:
    """Tests for TOKEN_DECIMALS registry content."""

    def test_registry_has_common_tokens(self):
        """Test registry includes common tokens."""
        # USDC on Ethereum
        assert (CHAIN_ID_ETHEREUM, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48") in TOKEN_DECIMALS
        # WETH on Arbitrum
        assert (CHAIN_ID_ARBITRUM, "0x82af49447d8a07e3bd95bd0d56f35241523fbab1") in TOKEN_DECIMALS
        # DAI on Base
        assert (CHAIN_ID_BASE, "0x50c5725949a6f0c72e6c4a641f24049a917db0cb") in TOKEN_DECIMALS

    def test_stablecoins_have_6_decimals(self):
        """Test stablecoins have 6 decimals."""
        # USDC on Ethereum
        assert TOKEN_DECIMALS[(CHAIN_ID_ETHEREUM, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")] == 6
        # USDT on Ethereum
        assert TOKEN_DECIMALS[(CHAIN_ID_ETHEREUM, "0xdac17f958d2ee523a2206206994597c13d831ec7")] == 6
        # USDC on Arbitrum
        assert TOKEN_DECIMALS[(CHAIN_ID_ARBITRUM, "0xaf88d065e77c8cc2239327c5edb3a432268e5831")] == 6

    def test_wbtc_has_8_decimals(self):
        """Test WBTC has 8 decimals."""
        assert TOKEN_DECIMALS[(CHAIN_ID_ETHEREUM, "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599")] == 8
        assert TOKEN_DECIMALS[(CHAIN_ID_ARBITRUM, "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f")] == 8

    def test_weth_has_18_decimals(self):
        """Test WETH has 18 decimals."""
        assert TOKEN_DECIMALS[(CHAIN_ID_ETHEREUM, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")] == 18
        assert TOKEN_DECIMALS[(CHAIN_ID_ARBITRUM, "0x82af49447d8a07e3bd95bd0d56f35241523fbab1")] == 18
        assert TOKEN_DECIMALS[(CHAIN_ID_BASE, "0x4200000000000000000000000000000000000006")] == 18

    def test_registry_addresses_are_lowercase(self):
        """Test all addresses in registry are lowercase."""
        for (_chain_id, address) in TOKEN_DECIMALS.keys():
            assert address == address.lower(), f"Address not lowercase: {address}"
