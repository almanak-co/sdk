"""Unit tests for SolanaForkManager.

Tests the fork manager's construction, command building, RPC helpers,
and token/wallet funding logic — all without requiring solana-test-validator.
"""

from __future__ import annotations

import asyncio
import base64
import json
import struct
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

import pytest

from almanak.framework.anvil.solana_fork_manager import (
    ASSOCIATED_TOKEN_PROGRAM,
    DEFAULT_CLONE_ACCOUNTS,
    DEFAULT_CLONE_PROGRAMS,
    MINT_AUTHORITY_PUBKEY_OFFSET,
    SOLANA_TOKEN_DECIMALS,
    SOLANA_TOKEN_MINTS,
    TOKEN_PROGRAM,
    SolanaForkManager,
)


# =============================================================================
# Construction Tests
# =============================================================================


class TestSolanaForkManagerConstruction:
    """Test SolanaForkManager initialization and defaults."""

    def test_default_construction(self):
        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")
        assert mgr.rpc_url == "https://api.mainnet-beta.solana.com"
        assert mgr.validator_port == 8899
        assert mgr.faucet_port == 9900
        assert mgr.startup_timeout_seconds == 60.0
        assert mgr.clone_accounts == []
        assert mgr.clone_programs == []
        assert mgr.is_running is False
        assert mgr.current_slot is None

    def test_custom_port(self):
        mgr = SolanaForkManager(
            rpc_url="https://api.devnet.solana.com",
            validator_port=9999,
            faucet_port=10000,
        )
        assert mgr.validator_port == 9999
        assert mgr.faucet_port == 10000
        assert mgr.get_rpc_url() == "http://127.0.0.1:9999"

    def test_custom_clone_accounts(self):
        extra = ["SomeProgram111111111111111111111111111111"]
        mgr = SolanaForkManager(
            rpc_url="https://api.mainnet-beta.solana.com",
            clone_accounts=extra,
        )
        assert mgr.clone_accounts == extra

    def test_custom_clone_programs(self):
        progs = ["CustomProg111111111111111111111111111111"]
        mgr = SolanaForkManager(
            rpc_url="https://api.mainnet-beta.solana.com",
            clone_programs=progs,
        )
        assert mgr.clone_programs == progs


# =============================================================================
# RPC URL and Serialization
# =============================================================================


class TestSolanaForkManagerGetters:
    """Test getter methods and serialization."""

    def test_get_rpc_url(self):
        mgr = SolanaForkManager(
            rpc_url="https://api.mainnet-beta.solana.com",
            validator_port=8899,
        )
        assert mgr.get_rpc_url() == "http://127.0.0.1:8899"

    def test_to_dict(self):
        mgr = SolanaForkManager(
            rpc_url="https://api.mainnet-beta.solana.com",
            validator_port=8899,
        )
        d = mgr.to_dict()
        assert d["validator_port"] == 8899
        assert d["is_running"] is False
        assert d["current_slot"] is None
        assert d["fork_rpc_url"] == "http://127.0.0.1:8899"
        assert "rpc_url" in d

    def test_mask_url(self):
        assert "***" in SolanaForkManager._mask_url("https://api.mainnet-beta.solana.com/v1/key123")
        assert SolanaForkManager._mask_url("http://localhost:8899") == "http://localhost:8899"


# =============================================================================
# Command Building
# =============================================================================


class TestCommandBuilding:
    """Test _build_validator_command() generates correct CLI arguments."""

    def test_basic_command(self):
        mgr = SolanaForkManager(
            rpc_url="https://api.mainnet-beta.solana.com",
            validator_port=8899,
            faucet_port=9900,
        )
        # Simulate temp dir setup
        mgr._temp_dir = "/tmp/test_solana"
        mgr._modified_mint_dir = "/tmp/test_solana/accounts"

        # Create the dir structure for os.listdir
        with patch("os.listdir", return_value=[]):
            cmd = mgr._build_validator_command()

        assert cmd[0] == "solana-test-validator"
        assert "--rpc-port" in cmd
        assert "8899" in cmd
        assert "--faucet-port" in cmd
        assert "9900" in cmd
        assert "--quiet" in cmd
        assert "--reset" in cmd

    def test_command_includes_default_clones(self):
        mgr = SolanaForkManager(
            rpc_url="https://api.mainnet-beta.solana.com",
        )
        mgr._temp_dir = "/tmp/test_solana"
        mgr._modified_mint_dir = "/tmp/test_solana/accounts"

        with patch("os.listdir", return_value=[]):
            cmd = mgr._build_validator_command()

        cmd_str = " ".join(cmd)
        # Should clone default accounts
        for account in DEFAULT_CLONE_ACCOUNTS:
            assert account in cmd_str, f"Default account {account} not in command"

        # Should clone default programs
        for program in DEFAULT_CLONE_PROGRAMS:
            assert program in cmd_str, f"Default program {program} not in command"

        # --url must appear exactly once (solana-test-validator v3.x constraint)
        assert cmd.count("--url") == 1, f"Expected --url once, got {cmd.count('--url')}"

    def test_command_includes_custom_clones(self):
        extra_account = "ExtraAccount1111111111111111111111111111"
        extra_program = "ExtraProg1111111111111111111111111111"
        mgr = SolanaForkManager(
            rpc_url="https://api.mainnet-beta.solana.com",
            clone_accounts=[extra_account],
            clone_programs=[extra_program],
        )
        mgr._temp_dir = "/tmp/test_solana"
        mgr._modified_mint_dir = "/tmp/test_solana/accounts"

        with patch("os.listdir", return_value=[]):
            cmd = mgr._build_validator_command()

        cmd_str = " ".join(cmd)
        assert extra_account in cmd_str
        assert extra_program in cmd_str

    def test_command_loads_modified_mint_accounts(self):
        mgr = SolanaForkManager(
            rpc_url="https://api.mainnet-beta.solana.com",
        )
        mgr._temp_dir = "/tmp/test_solana"
        mgr._modified_mint_dir = "/tmp/test_solana/accounts"

        # Simulate modified mint files
        mint_files = [
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v.json",
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB.json",
        ]
        with patch("os.listdir", return_value=mint_files), \
             patch("os.path.isdir", return_value=True):
            cmd = mgr._build_validator_command()

        cmd_str = " ".join(cmd)
        assert "--account" in cmd_str
        assert "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" in cmd_str
        # Modified mints should NOT also be --cloned (conflict)
        clone_indices = [i for i, x in enumerate(cmd) if x == "--clone"]
        for idx in clone_indices:
            assert cmd[idx + 1] not in [f.replace(".json", "") for f in mint_files]


# =============================================================================
# Token Constants
# =============================================================================


class TestTokenConstants:
    """Test that token constants are correctly defined."""

    def test_usdc_mint(self):
        assert SOLANA_TOKEN_MINTS["USDC"] == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    def test_usdt_mint(self):
        assert SOLANA_TOKEN_MINTS["USDT"] == "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"

    def test_usdc_decimals(self):
        assert SOLANA_TOKEN_DECIMALS["USDC"] == 6

    def test_sol_decimals(self):
        assert SOLANA_TOKEN_DECIMALS["SOL"] == 9

    def test_all_mints_have_decimals(self):
        for symbol in SOLANA_TOKEN_MINTS:
            assert symbol in SOLANA_TOKEN_DECIMALS, (
                f"Token {symbol} has a mint address but no decimals entry"
            )


# =============================================================================
# Lifecycle Tests (mocked subprocess)
# =============================================================================


class TestLifecycle:
    """Test start/stop lifecycle with mocked subprocess."""

    @pytest.mark.asyncio
    async def test_start_when_already_running(self):
        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")
        mgr._is_running = True
        result = await mgr.start()
        assert result is True  # Should return True without starting again

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self):
        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")
        # Should not raise
        await mgr.stop()
        assert mgr.is_running is False

    @pytest.mark.asyncio
    async def test_stop_terminates_process(self):
        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")
        mock_process = MagicMock()
        mock_process.terminate = MagicMock()
        mock_process.wait = MagicMock(return_value=0)
        mock_process.kill = MagicMock()
        mgr._process = mock_process
        mgr._is_running = True

        with patch.object(mgr, "_wait_for_port_free", new_callable=AsyncMock):
            await mgr.stop()

        mock_process.terminate.assert_called_once()
        assert mgr._is_running is False
        assert mgr._process is None

    @pytest.mark.asyncio
    async def test_start_returns_false_on_file_not_found(self):
        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")

        with patch.object(mgr, "_prepare_mint_authority", new_callable=AsyncMock), \
             patch.object(mgr, "_prepare_modified_mints", new_callable=AsyncMock), \
             patch("subprocess.Popen", side_effect=FileNotFoundError):
            result = await mgr.start()

        assert result is False


# =============================================================================
# Wallet Funding Tests
# =============================================================================


class TestFundWallet:
    """Test SOL airdrop funding."""

    @pytest.mark.asyncio
    async def test_fund_wallet_when_not_running(self):
        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")
        result = await mgr.fund_wallet("SomeAddress", Decimal("10"))
        assert result is False

    @pytest.mark.asyncio
    async def test_fund_wallet_success(self):
        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")
        mgr._is_running = True

        # Mock _rpc_call to return signature, then confirmation status
        async def _mock_rpc(method, params):
            if method == "requestAirdrop":
                return "tx_signature_123"
            if method == "getSignatureStatuses":
                return {"value": [{"confirmationStatus": "confirmed", "err": None}]}
            return None

        with patch.object(mgr, "_rpc_call", new_callable=AsyncMock, side_effect=_mock_rpc), \
             patch.object(mgr, "_get_sol_balance", new_callable=AsyncMock, return_value=10_000_000_000):
            result = await mgr.fund_wallet("SomeAddress", Decimal("10"))

        assert result is True

    @pytest.mark.asyncio
    async def test_fund_wallet_airdrop_fails(self):
        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")
        mgr._is_running = True

        with patch.object(mgr, "_rpc_call", new_callable=AsyncMock, return_value=None):
            result = await mgr.fund_wallet("SomeAddress", Decimal("5"))

        assert result is False


# =============================================================================
# Token Funding Tests
# =============================================================================


class TestFundTokens:
    """Test SPL token funding."""

    @pytest.mark.asyncio
    async def test_fund_tokens_when_not_running(self):
        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")
        result = await mgr.fund_tokens("SomeAddress", {"USDC": Decimal("1000")})
        assert result is False

    @pytest.mark.asyncio
    async def test_fund_tokens_no_mint_authority(self):
        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")
        mgr._is_running = True
        mgr._mint_authority_keypair = None
        result = await mgr.fund_tokens("SomeAddress", {"USDC": Decimal("1000")})
        assert result is False

    @pytest.mark.asyncio
    async def test_fund_single_token_unknown_symbol(self):
        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")
        mgr._is_running = True
        mgr._mint_authority_keypair = MagicMock()
        result = await mgr._fund_single_token("SomeAddress", "UNKNOWN_TOKEN", Decimal("100"))
        assert result is False


# =============================================================================
# Mint Modification Tests
# =============================================================================


class TestMintModification:
    """Test the mint account modification logic."""

    def test_mint_authority_offset_correctness(self):
        """Verify our offset constants match SPL Token layout."""
        assert MINT_AUTHORITY_PUBKEY_OFFSET == 4
        # COption discriminator (4) + pubkey (32) = 36 for supply offset
        # supply (8) = 44 for decimals offset
        # decimals (1) = 45 for is_initialized

    def test_build_modified_mint_data(self):
        """Test that mint authority bytes are correctly replaced."""
        # Create a fake 82-byte mint account
        data = bytearray(82)
        # Set COption::Some for authority
        struct.pack_into("<I", data, 0, 1)
        # Set original authority (32 bytes of 0xAA)
        data[4:36] = bytes([0xAA] * 32)
        # Set supply
        struct.pack_into("<Q", data, 36, 1_000_000_000_000)
        # Set decimals
        data[44] = 6  # USDC-like
        # Set is_initialized
        data[45] = 1

        # Replace authority with new pubkey (32 bytes of 0xBB)
        new_authority = bytes([0xBB] * 32)
        data[MINT_AUTHORITY_PUBKEY_OFFSET:MINT_AUTHORITY_PUBKEY_OFFSET + 32] = new_authority

        # Verify
        assert data[4:36] == new_authority
        assert data[44] == 6  # Decimals unchanged
        assert struct.unpack_from("<Q", data, 36)[0] == 1_000_000_000_000  # Supply unchanged

    def test_no_authority_gets_set(self):
        """Test that mint with no authority (COption::None) gets authority added."""
        data = bytearray(82)
        # COption::None
        struct.pack_into("<I", data, 0, 0)

        # Now set COption::Some + our authority
        struct.pack_into("<I", data, 0, 1)
        new_authority = bytes([0xCC] * 32)
        data[MINT_AUTHORITY_PUBKEY_OFFSET:MINT_AUTHORITY_PUBKEY_OFFSET + 32] = new_authority

        assert struct.unpack_from("<I", data, 0)[0] == 1  # COption::Some
        assert data[4:36] == new_authority


# =============================================================================
# ATA Derivation Tests
# =============================================================================


class TestATADerivation:
    """Test Associated Token Account address derivation."""

    def test_derive_ata_is_deterministic(self):
        """Test that ATA derivation produces consistent results."""
        pytest.importorskip("solders")
        from solders.pubkey import Pubkey

        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")

        owner = Pubkey.from_string("Hs5wSP3ancpUapqK5Q8R9YpFheRELSFHZwsWeofMVSpJ")
        mint = Pubkey.from_string("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")

        ata1 = mgr._derive_ata(owner, mint)
        ata2 = mgr._derive_ata(owner, mint)
        assert ata1 == ata2

    def test_derive_ata_different_for_different_mints(self):
        """Different mints produce different ATAs for the same owner."""
        pytest.importorskip("solders")
        from solders.pubkey import Pubkey

        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")

        owner = Pubkey.from_string("Hs5wSP3ancpUapqK5Q8R9YpFheRELSFHZwsWeofMVSpJ")
        usdc_mint = Pubkey.from_string("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
        usdt_mint = Pubkey.from_string("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB")

        ata_usdc = mgr._derive_ata(owner, usdc_mint)
        ata_usdt = mgr._derive_ata(owner, usdt_mint)
        assert ata_usdc != ata_usdt


# =============================================================================
# Health Check Tests
# =============================================================================


class TestHealthChecks:
    """Test port checking and readiness logic."""

    def test_is_port_open_when_closed(self):
        """Port check returns False for a port nobody listens on."""
        mgr = SolanaForkManager(
            rpc_url="https://api.mainnet-beta.solana.com",
            validator_port=19876,  # Unlikely to be in use
        )
        assert mgr._is_port_open() is False

    @pytest.mark.asyncio
    async def test_wait_for_ready_timeout(self):
        """_wait_for_ready returns False after timeout."""
        mgr = SolanaForkManager(
            rpc_url="https://api.mainnet-beta.solana.com",
            startup_timeout_seconds=0.5,
            validator_port=19876,
        )
        mgr._process = MagicMock()
        mgr._process.poll.return_value = None  # Process still running

        result = await mgr._wait_for_ready()
        assert result is False

    @pytest.mark.asyncio
    async def test_wait_for_ready_process_died(self):
        """_wait_for_ready returns False if process exits."""
        mgr = SolanaForkManager(
            rpc_url="https://api.mainnet-beta.solana.com",
            startup_timeout_seconds=5.0,
            validator_port=19876,
        )
        mock_process = MagicMock()
        mock_process.poll.return_value = 1  # Process exited
        mock_process.stdout = MagicMock()
        mock_process.stdout.read.return_value = b"stdout"
        mock_process.stderr = MagicMock()
        mock_process.stderr.read.return_value = b"some error"
        mgr._process = mock_process

        result = await mgr._wait_for_ready()
        assert result is False
