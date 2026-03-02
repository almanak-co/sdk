"""Unit tests for RollingForkManager.

Tests cover:
- RollingForkManager configuration and validation
- Chain ID lookup and validation
- URL masking for sensitive data
- Token address and decimal lookups
- RPC URL generation
- Command building for Anvil
- Storage slot calculation

Note: Async methods like start() and stop() require actual Anvil
availability, so those tests are marked with @pytest.mark.integration
and can be skipped in CI without Anvil installed.
"""

from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from almanak.framework.backtesting.paper.fork_manager import (
    CHAIN_IDS,
    KNOWN_BALANCE_SLOTS,
    TOKEN_ADDRESSES,
    TOKEN_DECIMALS,
    ForkManagerConfig,
    RollingForkManager,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def rpc_url() -> str:
    """Mock RPC URL for testing."""
    # Use a 24-char API key to trigger the masking regex (requires 20+ chars)
    return "https://arb-mainnet.g.alchemy.com/v2/test_api_key_12345678901234"


@pytest.fixture
def fork_manager(rpc_url: str) -> RollingForkManager:
    """Create a RollingForkManager for testing."""
    return RollingForkManager(
        rpc_url=rpc_url,
        chain="arbitrum",
        anvil_port=8546,
    )


# =============================================================================
# Constants Tests
# =============================================================================


class TestChainConstants:
    """Tests for chain constants."""

    def test_chain_ids_contains_major_chains(self) -> None:
        """Test that CHAIN_IDS contains all major chains."""
        expected_chains = ["ethereum", "arbitrum", "optimism", "base", "polygon", "avalanche", "bnb"]
        for chain in expected_chains:
            assert chain in CHAIN_IDS

    def test_arbitrum_chain_id(self) -> None:
        """Test Arbitrum chain ID is correct."""
        assert CHAIN_IDS["arbitrum"] == 42161

    def test_ethereum_chain_id(self) -> None:
        """Test Ethereum chain ID is correct."""
        assert CHAIN_IDS["ethereum"] == 1

    def test_base_chain_id(self) -> None:
        """Test Base chain ID is correct."""
        assert CHAIN_IDS["base"] == 8453


class TestTokenConstants:
    """Tests for token address and decimal constants."""

    def test_arbitrum_token_addresses(self) -> None:
        """Test Arbitrum token addresses are present."""
        arb_tokens = TOKEN_ADDRESSES["arbitrum"]
        assert "WETH" in arb_tokens
        assert "USDC" in arb_tokens
        assert "USDT" in arb_tokens
        assert arb_tokens["WETH"] == "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

    def test_token_decimals(self) -> None:
        """Test token decimals are correct."""
        assert TOKEN_DECIMALS["WETH"] == 18
        assert TOKEN_DECIMALS["USDC"] == 6
        assert TOKEN_DECIMALS["USDT"] == 6
        assert TOKEN_DECIMALS["WBTC"] == 8
        assert TOKEN_DECIMALS["DAI"] == 18

    def test_ethereum_token_addresses(self) -> None:
        """Test Ethereum token addresses are present."""
        eth_tokens = TOKEN_ADDRESSES["ethereum"]
        assert "WETH" in eth_tokens
        assert "USDC" in eth_tokens


# =============================================================================
# ForkManagerConfig Tests
# =============================================================================


class TestForkManagerConfig:
    """Tests for ForkManagerConfig dataclass."""

    def test_config_creation(self, rpc_url: str) -> None:
        """Test creating a config with required fields."""
        config = ForkManagerConfig(
            rpc_url=rpc_url,
            chain="arbitrum",
        )
        assert config.chain == "arbitrum"
        assert config.anvil_port == 8546  # default
        assert config.auto_impersonate is True  # default
        assert config.chain_id == 42161

    def test_config_with_custom_port(self, rpc_url: str) -> None:
        """Test config with custom Anvil port."""
        config = ForkManagerConfig(
            rpc_url=rpc_url,
            chain="ethereum",
            anvil_port=9545,
        )
        assert config.anvil_port == 9545

    def test_config_validates_chain(self, rpc_url: str) -> None:
        """Test config validation rejects invalid chain."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            ForkManagerConfig(rpc_url=rpc_url, chain="invalid_chain")

    def test_config_validates_empty_rpc_url(self) -> None:
        """Test config validation rejects empty RPC URL."""
        with pytest.raises(ValueError, match="rpc_url cannot be empty"):
            ForkManagerConfig(rpc_url="", chain="arbitrum")

    def test_config_validates_port_range(self, rpc_url: str) -> None:
        """Test config validation rejects invalid port."""
        with pytest.raises(ValueError, match="Invalid port"):
            ForkManagerConfig(rpc_url=rpc_url, chain="arbitrum", anvil_port=0)

        with pytest.raises(ValueError, match="Invalid port"):
            ForkManagerConfig(rpc_url=rpc_url, chain="arbitrum", anvil_port=70000)

    def test_config_validates_timeout(self, rpc_url: str) -> None:
        """Test config validation rejects non-positive timeout."""
        with pytest.raises(ValueError, match="startup_timeout_seconds must be positive"):
            ForkManagerConfig(rpc_url=rpc_url, chain="arbitrum", startup_timeout_seconds=0)

    def test_config_normalizes_chain_case(self, rpc_url: str) -> None:
        """Test config normalizes chain name to lowercase."""
        config = ForkManagerConfig(rpc_url=rpc_url, chain="ARBITRUM")
        assert config.chain == "arbitrum"

    def test_config_to_dict(self, rpc_url: str) -> None:
        """Test config serialization to dict."""
        config = ForkManagerConfig(
            rpc_url=rpc_url,
            chain="arbitrum",
            anvil_port=8546,
            block_time=2,
        )
        data = config.to_dict()
        assert data["chain"] == "arbitrum"
        assert data["chain_id"] == 42161
        assert data["anvil_port"] == 8546
        assert data["block_time"] == 2
        # URL should be masked (API keys 20+ chars get replaced with ***)
        assert "test_api_key_12345678901234" not in data["rpc_url"]
        assert "***" in data["rpc_url"]

    def test_config_url_masking_alchemy(self) -> None:
        """Test URL masking for Alchemy-style URLs.

        Note: The masking regex requires API keys to be 20+ characters.
        """
        masked = ForkManagerConfig._mask_url("https://eth-mainnet.g.alchemy.com/v2/abcdef12345_67890_extra1234")
        # API key portion (20+ chars) should be masked
        assert "abcdef12345_67890_extra1234" not in masked
        assert "***" in masked

    def test_config_url_masking_infura(self) -> None:
        """Test URL masking for Infura-style URLs."""
        masked = ForkManagerConfig._mask_url("https://mainnet.infura.io/v3/your_infura_key_here12345")
        assert "your_infura_key_here12345" not in masked
        assert "***" in masked

    def test_config_url_masking_query_param(self) -> None:
        """Test URL masking for query parameter API keys."""
        masked = ForkManagerConfig._mask_url("https://rpc.example.com?apikey=secret123&chain=eth")
        assert "secret123" not in masked
        assert "apikey=***" in masked


# =============================================================================
# RollingForkManager Tests
# =============================================================================


class TestRollingForkManagerInit:
    """Tests for RollingForkManager initialization."""

    def test_manager_creation(self, rpc_url: str) -> None:
        """Test creating a manager with required fields."""
        manager = RollingForkManager(
            rpc_url=rpc_url,
            chain="arbitrum",
        )
        assert manager.chain == "arbitrum"
        assert manager.anvil_port == 8546
        assert manager.chain_id == 42161
        assert not manager.is_running
        assert manager.current_block is None

    def test_manager_validates_chain(self, rpc_url: str) -> None:
        """Test manager validation rejects invalid chain."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            RollingForkManager(rpc_url=rpc_url, chain="invalid_chain")

    def test_manager_validates_empty_rpc(self) -> None:
        """Test manager validation rejects empty RPC URL."""
        with pytest.raises(ValueError, match="rpc_url cannot be empty"):
            RollingForkManager(rpc_url="", chain="arbitrum")

    def test_manager_validates_port(self, rpc_url: str) -> None:
        """Test manager validation rejects invalid port."""
        with pytest.raises(ValueError, match="Invalid port"):
            RollingForkManager(rpc_url=rpc_url, chain="arbitrum", anvil_port=-1)

    def test_manager_normalizes_chain_case(self, rpc_url: str) -> None:
        """Test manager normalizes chain to lowercase."""
        manager = RollingForkManager(rpc_url=rpc_url, chain="ETHEREUM")
        assert manager.chain == "ethereum"
        assert manager.chain_id == 1


class TestRollingForkManagerRpcUrl:
    """Tests for RPC URL generation."""

    def test_get_rpc_url_default_port(self, rpc_url: str) -> None:
        """Test RPC URL with default port."""
        manager = RollingForkManager(rpc_url=rpc_url, chain="arbitrum")
        assert manager.get_rpc_url() == "http://127.0.0.1:8546"

    def test_get_rpc_url_custom_port(self, rpc_url: str) -> None:
        """Test RPC URL with custom port."""
        manager = RollingForkManager(rpc_url=rpc_url, chain="arbitrum", anvil_port=9545)
        assert manager.get_rpc_url() == "http://127.0.0.1:9545"


class TestRollingForkManagerCommand:
    """Tests for Anvil command building."""

    def test_build_command_basic(self, fork_manager: RollingForkManager) -> None:
        """Test basic Anvil command building."""
        cmd = fork_manager._build_anvil_command()

        assert "anvil" == cmd[0]
        assert "--fork-url" in cmd
        assert "--port" in cmd
        assert "8546" in cmd
        assert "--chain-id" in cmd
        assert "42161" in cmd
        assert "--silent" in cmd

    def test_build_command_auto_impersonate(self, fork_manager: RollingForkManager) -> None:
        """Test command includes auto-impersonate by default."""
        cmd = fork_manager._build_anvil_command()
        assert "--auto-impersonate" in cmd

    def test_build_command_without_auto_impersonate(self, rpc_url: str) -> None:
        """Test command without auto-impersonate."""
        manager = RollingForkManager(
            rpc_url=rpc_url,
            chain="arbitrum",
            auto_impersonate=False,
        )
        cmd = manager._build_anvil_command()
        assert "--auto-impersonate" not in cmd

    def test_build_command_with_block_time(self, rpc_url: str) -> None:
        """Test command with block time."""
        manager = RollingForkManager(
            rpc_url=rpc_url,
            chain="arbitrum",
            block_time=2,
        )
        cmd = manager._build_anvil_command()
        assert "--block-time" in cmd
        idx = cmd.index("--block-time")
        assert cmd[idx + 1] == "2"

    def test_build_command_with_fork_block(self, rpc_url: str) -> None:
        """Test command with specific fork block."""
        manager = RollingForkManager(
            rpc_url=rpc_url,
            chain="arbitrum",
            fork_block_number=12345678,
        )
        cmd = manager._build_anvil_command()
        assert "--fork-block-number" in cmd
        idx = cmd.index("--fork-block-number")
        assert cmd[idx + 1] == "12345678"


class TestRollingForkManagerHelpers:
    """Tests for helper methods."""

    def test_calculate_mapping_slot(self, fork_manager: RollingForkManager) -> None:
        """Test storage slot calculation for mappings."""
        # This tests the keccak256 hash calculation for mapping storage slots
        slot = fork_manager._calculate_mapping_slot(
            "0x1234567890123456789012345678901234567890",
            0,
        )
        assert slot.startswith("0x")
        assert len(slot) == 66  # 0x + 64 hex chars

    def test_calculate_mapping_slot_uses_keccak256(self, fork_manager: RollingForkManager) -> None:
        """Test that _calculate_mapping_slot uses Ethereum's Keccak-256 (not NIST SHA3-256).

        Verifies against a known keccak256 output computed independently.
        """
        from eth_hash.auto import keccak as keccak256

        address = "0x1234567890123456789012345678901234567890"
        slot_num = 0

        # Compute expected result directly
        key_padded = address.lower().replace("0x", "").zfill(64)
        slot_padded = hex(slot_num)[2:].zfill(64)
        concat = bytes.fromhex(key_padded + slot_padded)
        expected = "0x" + keccak256(concat).hex()

        result = fork_manager._calculate_mapping_slot(address, slot_num)
        assert result == expected, f"Expected keccak256 result {expected}, got {result}"

        # Verify it does NOT match NIST SHA3-256 (the old buggy implementation)
        import hashlib

        sha3_result = "0x" + hashlib.sha3_256(concat).hexdigest()
        assert result != sha3_result, "Result must NOT match NIST SHA3-256 -- must use Keccak-256"

    def test_pad_hex_to_32_bytes_small(self, fork_manager: RollingForkManager) -> None:
        """Test padding small hex value to 32 bytes."""
        padded = fork_manager._pad_hex_to_32_bytes("0x1")
        assert padded == "0x" + "0" * 63 + "1"
        assert len(padded) == 66

    def test_pad_hex_to_32_bytes_no_prefix(self, fork_manager: RollingForkManager) -> None:
        """Test padding hex value without 0x prefix."""
        padded = fork_manager._pad_hex_to_32_bytes("abc")
        assert padded == "0x" + "0" * 61 + "abc"

    def test_pad_hex_to_32_bytes_large(self, fork_manager: RollingForkManager) -> None:
        """Test padding larger hex value."""
        padded = fork_manager._pad_hex_to_32_bytes("0x123456789abcdef")
        assert padded.startswith("0x")
        assert "123456789abcdef" in padded
        assert len(padded) == 66


class TestRollingForkManagerState:
    """Tests for manager state tracking."""

    def test_initial_state(self, fork_manager: RollingForkManager) -> None:
        """Test initial state of manager."""
        assert not fork_manager.is_running
        assert fork_manager.current_block is None

    def test_to_dict(self, fork_manager: RollingForkManager) -> None:
        """Test serialization to dict."""
        data = fork_manager.to_dict()
        assert data["chain"] == "arbitrum"
        assert data["chain_id"] == 42161
        assert data["anvil_port"] == 8546
        assert data["is_running"] is False
        assert data["current_block"] is None
        assert data["fork_rpc_url"] is None  # Not running

    def test_chain_id_property(self, rpc_url: str) -> None:
        """Test chain_id property for different chains."""
        for chain, expected_id in [
            ("arbitrum", 42161),
            ("ethereum", 1),
            ("optimism", 10),
            ("base", 8453),
            ("polygon", 137),
        ]:
            manager = RollingForkManager(rpc_url=rpc_url, chain=chain)
            assert manager.chain_id == expected_id


# =============================================================================
# Known Balance Slots Tests
# =============================================================================


class TestKnownBalanceSlots:
    """Tests for KNOWN_BALANCE_SLOTS constant."""

    def test_known_slots_covers_major_chains(self) -> None:
        """Test that KNOWN_BALANCE_SLOTS covers all major chains."""
        expected_chains = ["arbitrum", "ethereum", "base", "avalanche", "optimism", "polygon", "bnb"]
        for chain in expected_chains:
            assert chain in KNOWN_BALANCE_SLOTS, f"Missing known slots for chain: {chain}"

    def test_known_slots_has_usdc_for_all_chains(self) -> None:
        """Test that every chain has a known USDC slot."""
        for chain, slots in KNOWN_BALANCE_SLOTS.items():
            assert "USDC" in slots, f"Missing USDC slot for chain: {chain}"

    def test_arbitrum_known_slots(self) -> None:
        """Test Arbitrum known slots match verified values from intent tests."""
        arb_slots = KNOWN_BALANCE_SLOTS["arbitrum"]
        assert arb_slots["USDC"] == 9
        assert arb_slots["WETH"] == 51
        assert arb_slots["USDT"] == 51

    def test_ethereum_known_slots(self) -> None:
        """Test Ethereum known slots match verified values."""
        eth_slots = KNOWN_BALANCE_SLOTS["ethereum"]
        assert eth_slots["USDC"] == 9
        assert eth_slots["WETH"] == 3
        assert eth_slots["USDT"] == 2

    def test_base_known_slots(self) -> None:
        """Test Base known slots match verified values."""
        base_slots = KNOWN_BALANCE_SLOTS["base"]
        assert base_slots["USDC"] == 9
        assert base_slots["WETH"] == 3

    def test_all_known_slot_tokens_have_addresses(self) -> None:
        """Every token in KNOWN_BALANCE_SLOTS must have a corresponding address in TOKEN_ADDRESSES."""
        for chain, slots in KNOWN_BALANCE_SLOTS.items():
            chain_addrs = TOKEN_ADDRESSES.get(chain, {})
            for token in slots:
                assert token in chain_addrs, (
                    f"Token '{token}' has known slot for chain '{chain}' but no address in TOKEN_ADDRESSES"
                )


# =============================================================================
# RPC Call Raw Tests (VIB-29: anvil_setBalance null-result fix)
# =============================================================================


class TestRpcCallRaw:
    """Tests for _rpc_call_raw and _rpc_call handling of null JSON-RPC results.

    Anvil methods like anvil_setBalance return {"result": null} on success.
    _rpc_call_raw must return (True, None) for these, not treat null as failure.
    """

    @pytest.mark.asyncio
    async def test_rpc_call_delegates_to_rpc_call_raw(self, fork_manager: RollingForkManager) -> None:
        """Test that _rpc_call returns None for errors and result value for success."""
        # Success with null -> _rpc_call returns None (but _rpc_call_raw says success)
        fork_manager._rpc_call_raw = AsyncMock(return_value=(True, None))
        result = await fork_manager._rpc_call("anvil_setBalance", [])
        assert result is None

        # Success with value -> _rpc_call returns the value
        fork_manager._rpc_call_raw = AsyncMock(return_value=(True, "0x1a4"))
        result = await fork_manager._rpc_call("eth_blockNumber", [])
        assert result == "0x1a4"

        # Error -> _rpc_call returns None
        fork_manager._rpc_call_raw = AsyncMock(return_value=(False, None))
        result = await fork_manager._rpc_call("bad_method", [])
        assert result is None

    @pytest.mark.asyncio
    async def test_rpc_call_raw_success_with_null_result(self, fork_manager: RollingForkManager) -> None:
        """Test that _rpc_call_raw returns (True, None) when result is null.

        This is the core VIB-29 scenario: anvil_setBalance returns null on success.
        """
        import aiohttp

        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.json = AsyncMock(return_value={"jsonrpc": "2.0", "id": 1, "result": None})

        mock_post_ctx = AsyncMock()
        mock_post_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_session.post = lambda *args, **kwargs: mock_post_ctx

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
            success, result = await fork_manager._rpc_call_raw("anvil_setBalance", ["0xabc", "0x1"])

        assert success is True
        assert result is None

    @pytest.mark.asyncio
    async def test_rpc_call_raw_success_with_value(self, fork_manager: RollingForkManager) -> None:
        """Test that _rpc_call_raw returns (True, value) for non-null results."""
        import aiohttp

        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.json = AsyncMock(return_value={"jsonrpc": "2.0", "id": 1, "result": "0x1a4"})

        mock_post_ctx = AsyncMock()
        mock_post_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_session.post = lambda *args, **kwargs: mock_post_ctx

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
            success, result = await fork_manager._rpc_call_raw("eth_blockNumber", [])

        assert success is True
        assert result == "0x1a4"

    @pytest.mark.asyncio
    async def test_rpc_call_raw_error_response(self, fork_manager: RollingForkManager) -> None:
        """Test that _rpc_call_raw returns (False, None) on JSON-RPC error."""
        import aiohttp

        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.json = AsyncMock(
            return_value={"jsonrpc": "2.0", "id": 1, "error": {"code": -32601, "message": "Method not found"}}
        )

        mock_post_ctx = AsyncMock()
        mock_post_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_session.post = lambda *args, **kwargs: mock_post_ctx

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
            success, result = await fork_manager._rpc_call_raw("bad_method", [])

        assert success is False
        assert result is None

    @pytest.mark.asyncio
    async def test_rpc_call_raw_connection_failure(self, fork_manager: RollingForkManager) -> None:
        """Test that _rpc_call_raw returns (False, None) on connection error."""
        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(side_effect=ConnectionRefusedError("Connection refused"))
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
            success, result = await fork_manager._rpc_call_raw("eth_blockNumber", [])

        assert success is False
        assert result is None


class TestFundWalletWithMockedRpc:
    """Tests for fund_wallet using mocked _rpc_call_raw.

    Verifies the VIB-29 fix: fund_wallet must succeed when anvil_setBalance
    returns null (success=True, result=None).
    """

    @pytest.mark.asyncio
    async def test_fund_wallet_succeeds_with_null_result(self, fork_manager: RollingForkManager) -> None:
        """Test fund_wallet returns True when anvil_setBalance succeeds with null result."""
        # Simulate running state
        fork_manager._is_running = True
        fork_manager._process = AsyncMock()
        fork_manager._process.poll = lambda: None  # Process still running

        # anvil_setBalance returns null on success
        fork_manager._rpc_call_raw = AsyncMock(return_value=(True, None))

        result = await fork_manager.fund_wallet("0x1234567890123456789012345678901234567890", Decimal("10"))
        assert result is True

        fork_manager._rpc_call_raw.assert_called_once_with(
            "anvil_setBalance",
            ["0x1234567890123456789012345678901234567890", hex(int(Decimal("10") * Decimal("1e18")))],
        )

    @pytest.mark.asyncio
    async def test_fund_wallet_fails_on_rpc_error(self, fork_manager: RollingForkManager) -> None:
        """Test fund_wallet returns False when anvil_setBalance returns an error."""
        fork_manager._is_running = True
        fork_manager._process = AsyncMock()
        fork_manager._process.poll = lambda: None

        fork_manager._rpc_call_raw = AsyncMock(return_value=(False, None))

        result = await fork_manager.fund_wallet("0x1234567890123456789012345678901234567890", Decimal("10"))
        assert result is False

    @pytest.mark.asyncio
    async def test_fund_wallet_fails_when_not_running(self, fork_manager: RollingForkManager) -> None:
        """Test fund_wallet returns False when fork is not running."""
        result = await fork_manager.fund_wallet("0x1234567890123456789012345678901234567890", Decimal("10"))
        assert result is False


# =============================================================================
# RPC Timeout Tests
# =============================================================================


class TestRpcCallRawTimeout:
    """Tests for _rpc_call_raw with explicit timeout parameter."""

    def test_default_timeout_values(self, rpc_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that default timeout values are set correctly."""
        monkeypatch.delenv("ALMANAK_FORK_RPC_TIMEOUT", raising=False)
        monkeypatch.delenv("ALMANAK_FORK_HEALTH_TIMEOUT", raising=False)
        manager = RollingForkManager(rpc_url=rpc_url, chain="arbitrum")
        assert manager.rpc_timeout_seconds == 8.0
        assert manager.health_timeout_seconds == 2.0

    def test_custom_timeout_values(self, rpc_url: str) -> None:
        """Test custom timeout values can be set."""
        manager = RollingForkManager(
            rpc_url=rpc_url,
            chain="arbitrum",
            rpc_timeout_seconds=15.0,
            health_timeout_seconds=5.0,
        )
        assert manager.rpc_timeout_seconds == 15.0
        assert manager.health_timeout_seconds == 5.0

    @pytest.mark.asyncio
    async def test_rpc_call_raw_timeout_returns_false(self, fork_manager: RollingForkManager) -> None:
        """Test that _rpc_call_raw returns (False, None) on timeout."""

        # Mock a session that raises TimeoutError
        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(side_effect=TimeoutError("Connection timed out"))
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
            success, result = await fork_manager._rpc_call_raw("eth_blockNumber", [], timeout_seconds=0.001)

        assert success is False
        assert result is None

    @pytest.mark.asyncio
    async def test_rpc_call_raw_applies_timeout_to_session(self, fork_manager: RollingForkManager) -> None:
        """Test that _rpc_call_raw passes timeout to aiohttp.ClientSession."""
        import aiohttp

        captured_timeout = None

        class CapturingSession:
            def __init__(self, **kwargs):
                nonlocal captured_timeout
                captured_timeout = kwargs.get("timeout")
                self._mock_response = AsyncMock(spec=aiohttp.ClientResponse)
                self._mock_response.json = AsyncMock(return_value={"jsonrpc": "2.0", "id": 1, "result": "0x1"})

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            def post(self, *args, **kwargs):
                ctx = AsyncMock()
                ctx.__aenter__ = AsyncMock(return_value=self._mock_response)
                ctx.__aexit__ = AsyncMock(return_value=False)
                return ctx

        with patch("aiohttp.ClientSession", CapturingSession):
            await fork_manager._rpc_call_raw("eth_blockNumber", [], timeout_seconds=5.0)

        assert captured_timeout is not None
        assert captured_timeout.total == 5.0


# =============================================================================
# Health Check Tests
# =============================================================================


class TestHealthCheck:
    """Tests for health_check method."""

    @pytest.mark.asyncio
    async def test_health_check_not_running(self, fork_manager: RollingForkManager) -> None:
        """Test health_check returns False when not running."""
        result = await fork_manager.health_check()
        assert result is False

    @pytest.mark.asyncio
    async def test_health_check_success(self, fork_manager: RollingForkManager) -> None:
        """Test health_check returns True when both probes succeed."""
        fork_manager._is_running = True
        fork_manager._process = AsyncMock()
        fork_manager._process.poll = lambda: None

        # Mock both RPC calls to succeed
        call_count = 0

        async def mock_rpc_call_raw(method, params, timeout_seconds=None):
            nonlocal call_count
            call_count += 1
            if method == "eth_chainId":
                return (True, "0xa4b1")
            elif method == "eth_blockNumber":
                return (True, "0x1234")
            return (False, None)

        fork_manager._rpc_call_raw = mock_rpc_call_raw
        result = await fork_manager.health_check()
        assert result is True
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_health_check_chain_id_fails(self, fork_manager: RollingForkManager) -> None:
        """Test health_check returns False when eth_chainId fails."""
        fork_manager._is_running = True
        fork_manager._process = AsyncMock()
        fork_manager._process.poll = lambda: None

        async def mock_rpc_call_raw(method, params, timeout_seconds=None):
            return (False, None)

        fork_manager._rpc_call_raw = mock_rpc_call_raw
        result = await fork_manager.health_check()
        assert result is False

    @pytest.mark.asyncio
    async def test_health_check_block_number_fails(self, fork_manager: RollingForkManager) -> None:
        """Test health_check returns False when eth_blockNumber fails."""
        fork_manager._is_running = True
        fork_manager._process = AsyncMock()
        fork_manager._process.poll = lambda: None

        async def mock_rpc_call_raw(method, params, timeout_seconds=None):
            if method == "eth_chainId":
                return (True, "0xa4b1")
            return (False, None)

        fork_manager._rpc_call_raw = mock_rpc_call_raw
        result = await fork_manager.health_check()
        assert result is False


# =============================================================================
# Restart Tests
# =============================================================================


class TestRestart:
    """Tests for restart method."""

    @pytest.mark.asyncio
    async def test_restart_calls_stop_then_start(self, fork_manager: RollingForkManager) -> None:
        """Test that restart calls stop then start in sequence."""
        call_order = []

        async def mock_stop():
            call_order.append("stop")

        async def mock_start():
            call_order.append("start")
            fork_manager._is_running = True
            fork_manager._process = AsyncMock()
            fork_manager._process.poll = lambda: None
            return True

        async def mock_health_check(timeout_seconds=None):
            call_order.append("health_check")
            return True

        fork_manager.stop = mock_stop
        fork_manager.start = mock_start
        fork_manager.health_check = mock_health_check

        result = await fork_manager.restart()
        assert result is True
        assert call_order == ["stop", "start", "health_check"]

    @pytest.mark.asyncio
    async def test_restart_returns_false_on_start_failure(self, fork_manager: RollingForkManager) -> None:
        """Test that restart returns False when start fails."""

        async def mock_stop():
            pass

        async def mock_start():
            return False

        fork_manager.stop = mock_stop
        fork_manager.start = mock_start

        result = await fork_manager.restart()
        assert result is False


# =============================================================================
# Log Tail Tests
# =============================================================================


class TestAnvilLogTail:
    """Tests for _tail_anvil_log method."""

    def test_tail_no_log_path(self, fork_manager: RollingForkManager) -> None:
        """Test tail returns placeholder when no log path set."""
        assert fork_manager._anvil_log_path is None
        result = fork_manager._tail_anvil_log()
        assert result == "<no log file>"

    def test_tail_reads_log_file(self, fork_manager: RollingForkManager, tmp_path) -> None:
        """Test tail reads the last N lines from a log file."""
        log_file = tmp_path / "test.log"
        lines = [f"line {i}\n" for i in range(200)]
        log_file.write_text("".join(lines))

        fork_manager._anvil_log_path = str(log_file)
        result = fork_manager._tail_anvil_log(num_lines=10)

        # Should contain the last 10 lines
        assert "line 190" in result
        assert "line 199" in result
        # Should not contain early lines
        assert "line 0\n" not in result

    def test_tail_handles_missing_file(self, fork_manager: RollingForkManager) -> None:
        """Test tail handles missing log file gracefully."""
        fork_manager._anvil_log_path = "/tmp/nonexistent-almanak-test.log"
        result = fork_manager._tail_anvil_log()
        assert "<could not read log:" in result

    def test_anvil_log_path_property(self, fork_manager: RollingForkManager) -> None:
        """Test anvil_log_path property."""
        assert fork_manager.anvil_log_path is None
        fork_manager._anvil_log_path = "/tmp/test.log"
        assert fork_manager.anvil_log_path == "/tmp/test.log"


# =============================================================================
# Integration Tests (require Anvil)
# =============================================================================


@pytest.mark.integration
class TestRollingForkManagerIntegration:
    """Integration tests that require Anvil to be installed.

    These tests are marked with @pytest.mark.integration and can be
    skipped in CI environments without Anvil.

    Run with: pytest -m integration
    """

    @pytest.mark.asyncio
    async def test_start_and_stop(self, fork_manager: RollingForkManager) -> None:
        """Test starting and stopping the fork manager.

        This test requires Anvil to be installed.
        """
        # This test would require actual Anvil installation
        # Skip in unit test environments
        pytest.skip("Requires Anvil installation")

    @pytest.mark.asyncio
    async def test_fund_wallet(self, fork_manager: RollingForkManager) -> None:
        """Test funding a wallet with ETH.

        This test requires Anvil to be installed.
        """
        pytest.skip("Requires Anvil installation")

    @pytest.mark.asyncio
    async def test_fund_tokens(self, fork_manager: RollingForkManager) -> None:
        """Test funding a wallet with tokens.

        This test requires Anvil to be installed.
        """
        pytest.skip("Requires Anvil installation")

    @pytest.mark.asyncio
    async def test_reset_to_latest(self, fork_manager: RollingForkManager) -> None:
        """Test resetting fork to latest block.

        This test requires Anvil to be installed.
        """
        pytest.skip("Requires Anvil installation")
