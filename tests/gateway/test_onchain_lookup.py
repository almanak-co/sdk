"""Tests for OnChainLookup gateway service."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.gateway.services.onchain_lookup import (
    NATIVE_SENTINEL_ADDRESS,
    NATIVE_TOKEN_INFO,
    OnChainLookup,
    TokenMetadata,
)


@pytest.fixture
def mock_web3():
    """Create mock AsyncWeb3 instance."""
    mock = MagicMock()
    mock.to_checksum_address = MagicMock(side_effect=lambda x: x)
    mock.eth = MagicMock()
    mock.eth.contract = MagicMock()
    return mock


@pytest.fixture
def lookup():
    """Create OnChainLookup instance with mock RPC URL."""
    return OnChainLookup(
        rpc_url="https://test.rpc.example.com",
        timeout=5.0,
        max_retries=2,
        backoff_factor=2.0,
    )


class TestTokenMetadata:
    """Tests for TokenMetadata dataclass."""

    def test_create_metadata(self):
        """TokenMetadata can be created with all fields."""
        metadata = TokenMetadata(
            symbol="USDC",
            name="USD Coin",
            decimals=6,
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            is_native=False,
        )

        assert metadata.symbol == "USDC"
        assert metadata.name == "USD Coin"
        assert metadata.decimals == 6
        assert metadata.address == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        assert metadata.is_native is False

    def test_to_dict(self):
        """TokenMetadata converts to dictionary correctly."""
        metadata = TokenMetadata(
            symbol="WETH",
            name="Wrapped Ether",
            decimals=18,
            address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            is_native=False,
        )

        result = metadata.to_dict()

        assert result == {
            "symbol": "WETH",
            "name": "Wrapped Ether",
            "decimals": 18,
            "address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "is_native": False,
        }

    def test_native_token_metadata(self):
        """Native token metadata has is_native=True."""
        metadata = TokenMetadata(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            address=NATIVE_SENTINEL_ADDRESS,
            is_native=True,
        )

        assert metadata.is_native is True


class TestOnChainLookupInit:
    """Tests for OnChainLookup initialization."""

    def test_init_with_defaults(self):
        """OnChainLookup initializes with default values."""
        lookup = OnChainLookup("https://test.rpc.com")

        assert lookup._timeout == 10.0
        assert lookup._max_retries == 3
        assert lookup._backoff_factor == 2.0

    def test_init_with_custom_values(self):
        """OnChainLookup accepts custom configuration."""
        lookup = OnChainLookup(
            rpc_url="https://custom.rpc.com",
            timeout=5.0,
            max_retries=5,
            backoff_factor=1.5,
        )

        assert lookup._timeout == 5.0
        assert lookup._max_retries == 5
        assert lookup._backoff_factor == 1.5


class TestOnChainLookupMaskUrl:
    """Tests for RPC URL masking."""

    def test_mask_url_with_credentials(self):
        """URLs with credentials are masked."""
        url = "https://user:password@rpc.example.com"
        masked = OnChainLookup._mask_rpc_url(url)

        assert "password" not in masked
        assert "***" in masked

    def test_mask_url_with_query_params(self):
        """URLs with query params (API keys) are masked."""
        url = "https://rpc.example.com?apikey=secret123"
        masked = OnChainLookup._mask_rpc_url(url)

        assert "secret123" not in masked
        assert "***" in masked

    def test_mask_url_plain(self):
        """Plain URLs are not modified."""
        url = "https://rpc.example.com"
        masked = OnChainLookup._mask_rpc_url(url)

        assert masked == url


class TestOnChainLookupNativeTokens:
    """Tests for native token handling."""

    @pytest.mark.asyncio
    async def test_lookup_native_token_ethereum(self, lookup):
        """Native token lookup works for Ethereum."""
        result = await lookup.lookup("ethereum", NATIVE_SENTINEL_ADDRESS)

        assert result is not None
        assert result.symbol == "ETH"
        assert result.name == "Ethereum"
        assert result.decimals == 18
        assert result.is_native is True

    @pytest.mark.asyncio
    async def test_lookup_native_token_arbitrum(self, lookup):
        """Native token lookup works for Arbitrum."""
        result = await lookup.lookup("arbitrum", NATIVE_SENTINEL_ADDRESS)

        assert result is not None
        assert result.symbol == "ETH"
        assert result.is_native is True

    @pytest.mark.asyncio
    async def test_lookup_native_token_polygon(self, lookup):
        """Native token lookup works for Polygon."""
        result = await lookup.lookup("polygon", NATIVE_SENTINEL_ADDRESS)

        assert result is not None
        assert result.symbol == "MATIC"
        assert result.name == "Polygon"
        assert result.decimals == 18
        assert result.is_native is True

    @pytest.mark.asyncio
    async def test_lookup_native_token_avalanche(self, lookup):
        """Native token lookup works for Avalanche."""
        result = await lookup.lookup("avalanche", NATIVE_SENTINEL_ADDRESS)

        assert result is not None
        assert result.symbol == "AVAX"
        assert result.name == "Avalanche"
        assert result.is_native is True

    @pytest.mark.asyncio
    async def test_lookup_native_token_bsc(self, lookup):
        """Native token lookup works for BSC."""
        result = await lookup.lookup("bsc", NATIVE_SENTINEL_ADDRESS)

        assert result is not None
        assert result.symbol == "BNB"
        assert result.name == "BNB"
        assert result.is_native is True

    @pytest.mark.asyncio
    async def test_lookup_native_token_lowercase_address(self, lookup):
        """Native token lookup works with lowercase sentinel address."""
        result = await lookup.lookup("ethereum", NATIVE_SENTINEL_ADDRESS.lower())

        assert result is not None
        assert result.symbol == "ETH"
        assert result.is_native is True

    @pytest.mark.asyncio
    async def test_lookup_native_token_unknown_chain(self, lookup):
        """Native token lookup returns None for unknown chain."""
        result = await lookup.lookup("unknown_chain", NATIVE_SENTINEL_ADDRESS)

        assert result is None

    def test_native_token_info_coverage(self):
        """All expected chains have native token info."""
        expected_chains = {"ethereum", "arbitrum", "optimism", "base", "polygon", "avalanche", "bsc", "sonic", "plasma"}
        actual_chains = set(NATIVE_TOKEN_INFO.keys())

        assert expected_chains.issubset(actual_chains)


class TestOnChainLookupAddressValidation:
    """Tests for address validation."""

    @pytest.mark.asyncio
    async def test_lookup_invalid_address_format(self, lookup):
        """Lookup returns None for invalid address format."""
        result = await lookup.lookup("arbitrum", "not_a_valid_address")
        assert result is None

    @pytest.mark.asyncio
    async def test_lookup_short_address(self, lookup):
        """Lookup returns None for too-short address."""
        result = await lookup.lookup("arbitrum", "0x1234")
        assert result is None

    @pytest.mark.asyncio
    async def test_lookup_address_without_0x(self, lookup):
        """Lookup returns None for address without 0x prefix."""
        result = await lookup.lookup("arbitrum", "af88d065e77c8cC2239327C5EDb3A432268e5831")
        assert result is None


class TestOnChainLookupERC20:
    """Tests for ERC20 token lookup with mocked Web3."""

    @pytest.mark.asyncio
    async def test_lookup_standard_erc20(self, lookup):
        """Standard ERC20 lookup returns correct metadata."""
        address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

        # Mock the contract calls
        mock_contract = MagicMock()
        mock_decimals = AsyncMock(return_value=6)
        mock_symbol = AsyncMock(return_value="USDC")
        mock_name = AsyncMock(return_value="USD Coin")

        mock_contract.functions.decimals.return_value.call = mock_decimals
        mock_contract.functions.symbol.return_value.call = mock_symbol
        mock_contract.functions.name.return_value.call = mock_name

        with patch.object(lookup._w3.eth, "contract", return_value=mock_contract):
            result = await lookup.lookup("arbitrum", address)

        assert result is not None
        assert result.symbol == "USDC"
        assert result.name == "USD Coin"
        assert result.decimals == 6
        assert result.is_native is False

    @pytest.mark.asyncio
    async def test_lookup_returns_none_when_no_decimals(self, lookup):
        """Lookup returns None when decimals() call fails."""
        address = "0x1234567890123456789012345678901234567890"

        # Mock contract that raises on decimals()
        mock_contract = MagicMock()
        mock_decimals = AsyncMock(side_effect=Exception("revert"))
        mock_contract.functions.decimals.return_value.call = mock_decimals

        with patch.object(lookup._w3.eth, "contract", return_value=mock_contract):
            result = await lookup.lookup("arbitrum", address)

        assert result is None

    @pytest.mark.asyncio
    async def test_lookup_returns_none_when_no_symbol(self, lookup):
        """Lookup returns None when symbol() call fails."""
        address = "0x1234567890123456789012345678901234567890"

        # Mock contract that succeeds on decimals but fails on symbol
        mock_contract = MagicMock()
        mock_decimals = AsyncMock(return_value=18)
        mock_symbol = AsyncMock(side_effect=Exception("revert"))
        mock_name = AsyncMock(side_effect=Exception("revert"))

        mock_contract.functions.decimals.return_value.call = mock_decimals
        mock_contract.functions.symbol.return_value.call = mock_symbol
        mock_contract.functions.name.return_value.call = mock_name

        with patch.object(lookup._w3.eth, "contract", return_value=mock_contract):
            result = await lookup.lookup("arbitrum", address)

        assert result is None

    @pytest.mark.asyncio
    async def test_lookup_succeeds_without_name(self, lookup):
        """Lookup succeeds when name() is not available."""
        address = "0x1234567890123456789012345678901234567890"

        # Mock contract that succeeds on decimals and symbol, fails on name
        mock_contract = MagicMock()
        mock_decimals = AsyncMock(return_value=18)
        mock_symbol = AsyncMock(return_value="TEST")
        mock_name = AsyncMock(side_effect=Exception("revert"))

        mock_contract.functions.decimals.return_value.call = mock_decimals
        mock_contract.functions.symbol.return_value.call = mock_symbol
        mock_contract.functions.name.return_value.call = mock_name

        with patch.object(lookup._w3.eth, "contract", return_value=mock_contract):
            result = await lookup.lookup("arbitrum", address)

        assert result is not None
        assert result.symbol == "TEST"
        assert result.name is None
        assert result.decimals == 18

    @pytest.mark.asyncio
    async def test_lookup_handles_bytes32_symbol(self, lookup):
        """Lookup handles tokens that return bytes32 symbol."""
        address = "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2"  # MKR address

        # Mock contract that fails string ABI but succeeds with bytes32
        mock_string_contract = MagicMock()
        mock_bytes32_contract = MagicMock()

        mock_decimals = AsyncMock(return_value=18)
        mock_symbol_string = AsyncMock(side_effect=Exception("decode error"))
        mock_symbol_bytes32 = AsyncMock(return_value=b"MKR" + b"\x00" * 29)
        mock_name_string = AsyncMock(side_effect=Exception("decode error"))
        mock_name_bytes32 = AsyncMock(return_value=b"Maker" + b"\x00" * 27)

        # Setup mock contract based on ABI
        def contract_factory(address, abi):
            # Check if this is the bytes32 ABI by looking for bytes32 output type
            if any(func.get("outputs", [{}])[0].get("type") == "bytes32" for func in abi if func.get("outputs")):
                contract = mock_bytes32_contract
                contract.functions.symbol.return_value.call = mock_symbol_bytes32
                contract.functions.name.return_value.call = mock_name_bytes32
            else:
                contract = mock_string_contract
                contract.functions.decimals.return_value.call = mock_decimals
                contract.functions.symbol.return_value.call = mock_symbol_string
                contract.functions.name.return_value.call = mock_name_string
            return contract

        with patch.object(lookup._w3.eth, "contract", side_effect=contract_factory):
            result = await lookup.lookup("ethereum", address)

        assert result is not None
        assert result.symbol == "MKR"
        assert result.name == "Maker"
        assert result.decimals == 18


class TestOnChainLookupRetry:
    """Tests for retry logic."""

    @pytest.mark.asyncio
    async def test_retry_on_timeout(self, lookup):
        """Lookup retries on timeout errors."""
        address = "0x1234567890123456789012345678901234567890"

        mock_contract = MagicMock()
        call_count = 0

        async def flaky_decimals():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise TimeoutError("timeout")
            return 18

        mock_decimals = AsyncMock(side_effect=flaky_decimals)
        mock_symbol = AsyncMock(return_value="TEST")
        mock_name = AsyncMock(return_value="Test Token")

        mock_contract.functions.decimals.return_value.call = mock_decimals
        mock_contract.functions.symbol.return_value.call = mock_symbol
        mock_contract.functions.name.return_value.call = mock_name

        with patch.object(lookup._w3.eth, "contract", return_value=mock_contract):
            result = await lookup.lookup("arbitrum", address)

        assert result is not None
        assert result.decimals == 18
        assert call_count == 2  # First call failed, second succeeded

    @pytest.mark.asyncio
    async def test_returns_none_after_max_retries(self, lookup):
        """Lookup returns None after max retries exhausted."""
        address = "0x1234567890123456789012345678901234567890"

        mock_contract = MagicMock()
        mock_decimals = AsyncMock(side_effect=TimeoutError("timeout"))

        mock_contract.functions.decimals.return_value.call = mock_decimals

        with patch.object(lookup._w3.eth, "contract", return_value=mock_contract):
            result = await lookup.lookup("arbitrum", address)

        assert result is None


class TestOnChainLookupContextManager:
    """Tests for async context manager."""

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """OnChainLookup works as async context manager."""
        async with OnChainLookup("https://test.rpc.com") as lookup:
            assert lookup is not None
            assert lookup._rpc_url == "https://test.rpc.com"


class TestConstants:
    """Tests for module constants."""

    def test_native_sentinel_address(self):
        """Native sentinel address is correct."""
        assert NATIVE_SENTINEL_ADDRESS == "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

    def test_native_token_info_all_have_required_fields(self):
        """All native token info entries have required fields."""
        for chain, info in NATIVE_TOKEN_INFO.items():
            assert "symbol" in info, f"{chain} missing symbol"
            assert "name" in info, f"{chain} missing name"
            assert "decimals" in info, f"{chain} missing decimals"
            assert info["decimals"] == 18, f"{chain} should have 18 decimals"
