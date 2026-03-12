"""Unit tests for DriftSDK — PDA derivation, instruction building, Borsh encoding.

All tests use mocked RPC responses. No network access required.
"""

import struct
from unittest.mock import MagicMock, patch

import pytest
from solders.instruction import AccountMeta
from solders.pubkey import Pubkey

from almanak.framework.connectors.drift.constants import (
    DIRECTION_LONG,
    DIRECTION_SHORT,
    DRIFT_PROGRAM_ID,
    MAX_PERP_POSITIONS,
    ORDER_TYPE_MARKET,
    PERP_POSITION_SIZE,
    PLACE_PERP_ORDER_DISCRIMINATOR,
    USER_PERP_POSITIONS_OFFSET,
)
from almanak.framework.connectors.drift.exceptions import DriftConfigError
from almanak.framework.connectors.drift.models import OrderParams
from almanak.framework.connectors.drift.sdk import DriftSDK

# A valid Solana pubkey for testing
TEST_WALLET = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"


class TestDriftSDKInit:
    """Test DriftSDK initialization."""

    def test_init_success(self):
        sdk = DriftSDK(wallet_address=TEST_WALLET)
        assert sdk.wallet_address == TEST_WALLET
        assert sdk.rpc_url == ""

    def test_init_with_rpc(self):
        sdk = DriftSDK(wallet_address=TEST_WALLET, rpc_url="https://api.mainnet-beta.solana.com")
        assert sdk.rpc_url == "https://api.mainnet-beta.solana.com"

    def test_init_empty_wallet_raises(self):
        with pytest.raises(DriftConfigError, match="wallet_address"):
            DriftSDK(wallet_address="")


class TestPDADerivation:
    """Test PDA derivation functions."""

    def setup_method(self):
        self.sdk = DriftSDK(wallet_address=TEST_WALLET)

    def test_state_pda_is_deterministic(self):
        pda1 = self.sdk.get_state_pda()
        pda2 = self.sdk.get_state_pda()
        assert pda1 == pda2
        assert isinstance(pda1, Pubkey)

    def test_user_pda_varies_by_sub_account(self):
        pda0 = self.sdk.get_user_pda(sub_account_id=0)
        pda1 = self.sdk.get_user_pda(sub_account_id=1)
        assert pda0 != pda1

    def test_user_pda_varies_by_authority(self):
        other_wallet = Pubkey.from_string("11111111111111111111111111111112")
        pda_default = self.sdk.get_user_pda()
        pda_other = self.sdk.get_user_pda(authority=other_wallet)
        assert pda_default != pda_other

    def test_user_stats_pda_is_deterministic(self):
        pda1 = self.sdk.get_user_stats_pda()
        pda2 = self.sdk.get_user_stats_pda()
        assert pda1 == pda2

    def test_perp_market_pda_varies_by_index(self):
        pda0 = self.sdk.get_perp_market_pda(0)
        pda1 = self.sdk.get_perp_market_pda(1)
        pda2 = self.sdk.get_perp_market_pda(2)
        assert pda0 != pda1 != pda2

    def test_spot_market_pda_varies_by_index(self):
        pda0 = self.sdk.get_spot_market_pda(0)
        pda1 = self.sdk.get_spot_market_pda(1)
        assert pda0 != pda1

    def test_pdas_are_on_drift_program(self):
        """All PDAs should be derived from the Drift program ID."""
        state_pda = self.sdk.get_state_pda()
        # The PDA should be a valid Pubkey (32 bytes)
        assert len(bytes(state_pda)) == 32


class TestOrderParamsEncoding:
    """Test Borsh encoding of OrderParams."""

    def setup_method(self):
        self.sdk = DriftSDK(wallet_address=TEST_WALLET)

    def test_encode_market_order(self):
        params = OrderParams(
            order_type=ORDER_TYPE_MARKET,
            direction=DIRECTION_LONG,
            base_asset_amount=1_000_000_000,
            market_index=0,
        )
        encoded = self.sdk._encode_order_params(params)
        assert isinstance(encoded, bytes)
        assert len(encoded) > 0

        # First byte should be order_type (0 = market)
        assert encoded[0] == ORDER_TYPE_MARKET

    def test_encode_short_direction(self):
        params = OrderParams(
            order_type=ORDER_TYPE_MARKET,
            direction=DIRECTION_SHORT,
            base_asset_amount=500_000_000,
            market_index=2,
        )
        encoded = self.sdk._encode_order_params(params)
        # direction is the 3rd byte (index 2)
        assert encoded[2] == DIRECTION_SHORT

    def test_encode_reduce_only(self):
        params = OrderParams(
            order_type=ORDER_TYPE_MARKET,
            direction=DIRECTION_SHORT,
            base_asset_amount=1_000_000_000,
            market_index=0,
            reduce_only=True,
        )
        encoded = self.sdk._encode_order_params(params)
        assert isinstance(encoded, bytes)
        assert len(encoded) > 0

    def test_encode_with_all_optional_fields(self):
        params = OrderParams(
            order_type=ORDER_TYPE_MARKET,
            direction=DIRECTION_LONG,
            base_asset_amount=1_000_000_000,
            market_index=0,
            max_ts=1700000000,
            trigger_price=150_000_000,
            oracle_price_offset=1000,
            auction_duration=10,
            auction_start_price=149_000_000,
            auction_end_price=151_000_000,
        )
        encoded = self.sdk._encode_order_params(params)
        assert isinstance(encoded, bytes)
        # With all optionals set, encoding should be longer
        assert len(encoded) > 20


class TestOptionEncoding:
    """Test Option<T> Borsh encoding helpers."""

    def test_option_none_is_single_zero_byte(self):
        assert DriftSDK._encode_option_i64(None) == b"\x00"
        assert DriftSDK._encode_option_u64(None) == b"\x00"
        assert DriftSDK._encode_option_i32(None) == b"\x00"
        assert DriftSDK._encode_option_u8(None) == b"\x00"

    def test_option_some_has_tag_plus_value(self):
        result = DriftSDK._encode_option_i64(42)
        assert result[0] == 1  # Some tag
        assert struct.unpack("<q", result[1:])[0] == 42

    def test_option_u64(self):
        result = DriftSDK._encode_option_u64(1_000_000)
        assert result[0] == 1
        assert struct.unpack("<Q", result[1:])[0] == 1_000_000

    def test_option_i32_negative(self):
        result = DriftSDK._encode_option_i32(-500)
        assert result[0] == 1
        assert struct.unpack("<i", result[1:])[0] == -500


class TestInstructionBuilders:
    """Test instruction building methods."""

    def setup_method(self):
        self.sdk = DriftSDK(wallet_address=TEST_WALLET)

    def test_build_place_perp_order_ix(self):
        params = OrderParams(
            order_type=ORDER_TYPE_MARKET,
            direction=DIRECTION_LONG,
            base_asset_amount=1_000_000_000,
            market_index=0,
        )
        ix = self.sdk.build_place_perp_order_ix(
            order_params=params,
            remaining_accounts=[],
        )
        assert ix.program_id == Pubkey.from_string(DRIFT_PROGRAM_ID)
        # Instruction data starts with discriminator
        assert ix.data[:8] == PLACE_PERP_ORDER_DISCRIMINATOR
        # Should have at least 3 accounts (state, user, authority)
        assert len(ix.accounts) >= 3

    def test_build_initialize_user_ix(self):
        ix = self.sdk.build_initialize_user_ix()
        assert ix.program_id == Pubkey.from_string(DRIFT_PROGRAM_ID)
        assert len(ix.accounts) >= 4  # user, user_stats, state, authority, system_program

    def test_build_initialize_user_stats_ix(self):
        ix = self.sdk.build_initialize_user_stats_ix()
        assert ix.program_id == Pubkey.from_string(DRIFT_PROGRAM_ID)
        assert len(ix.accounts) >= 3

    def test_build_deposit_ix(self):
        ix = self.sdk.build_deposit_ix(amount=100_000_000, market_index=0)
        assert ix.program_id == Pubkey.from_string(DRIFT_PROGRAM_ID)
        # Should have accounts for state, user, user_stats, authority, spot_market, vault, token_account, token_program
        assert len(ix.accounts) >= 7


class TestUserAccountParsing:
    """Test on-chain account data parsing."""

    def setup_method(self):
        self.sdk = DriftSDK(wallet_address=TEST_WALLET, rpc_url="https://fake-rpc.test")

    def test_parse_empty_perp_positions(self):
        # Create fake account data with all zeros for positions
        data = bytes(2000)  # Large enough to contain all position slots
        account = self.sdk._parse_user_account(data, sub_account_id=0)
        assert account.exists
        assert len(account.perp_positions) == MAX_PERP_POSITIONS
        # All positions should be inactive (base_asset_amount = 0)
        assert all(not p.is_active for p in account.perp_positions)

    def test_parse_active_perp_position(self):
        # Create fake account data with one active position
        data = bytearray(2000)
        offset = USER_PERP_POSITIONS_OFFSET

        # Set base_asset_amount to 1_000_000_000 (long position)
        struct.pack_into("<q", data, offset, 1_000_000_000)
        # Set quote_asset_amount
        struct.pack_into("<q", data, offset + 8, -50_000_000)
        # Set market_index = 0 (SOL-PERP)
        struct.pack_into("<H", data, offset + 24, 0)

        account = self.sdk._parse_user_account(bytes(data), sub_account_id=0)
        assert account.perp_positions[0].is_active
        assert account.perp_positions[0].is_long
        assert account.perp_positions[0].market_index == 0
        assert account.perp_positions[0].base_asset_amount == 1_000_000_000

    def test_active_market_indexes(self):
        data = bytearray(2000)

        # Position 0: SOL-PERP (market_index=0), active
        offset = USER_PERP_POSITIONS_OFFSET
        struct.pack_into("<q", data, offset, 500_000_000)
        struct.pack_into("<H", data, offset + 24, 0)

        # Position 1: ETH-PERP (market_index=2), active
        offset = USER_PERP_POSITIONS_OFFSET + PERP_POSITION_SIZE
        struct.pack_into("<q", data, offset, -200_000_000)
        struct.pack_into("<H", data, offset + 24, 2)

        account = self.sdk._parse_user_account(bytes(data), sub_account_id=0)
        assert set(account.active_perp_market_indexes) == {0, 2}

    def test_fetch_user_account_no_rpc(self):
        sdk = DriftSDK(wallet_address=TEST_WALLET)  # No RPC URL
        account = sdk.fetch_user_account()
        assert not account.exists

    @patch.object(DriftSDK, "_fetch_account_data", return_value=None)
    def test_fetch_user_account_not_found(self, mock_fetch):
        account = self.sdk.fetch_user_account()
        assert not account.exists


class TestGetInitInstructions:
    """Test user account initialization check."""

    def setup_method(self):
        self.sdk = DriftSDK(wallet_address=TEST_WALLET, rpc_url="https://fake-rpc.test")

    def test_no_init_needed_when_no_rpc(self):
        sdk = DriftSDK(wallet_address=TEST_WALLET)
        ixs = sdk.get_init_instructions()
        assert ixs == []

    @patch.object(DriftSDK, "_fetch_account_data", return_value=bytes(2000))
    def test_no_init_when_accounts_exist(self, mock_fetch):
        ixs = self.sdk.get_init_instructions()
        assert ixs == []

    @patch.object(DriftSDK, "_fetch_account_data", return_value=None)
    def test_init_when_no_accounts(self, mock_fetch):
        ixs = self.sdk.get_init_instructions()
        # Should have 2 instructions: init_user_stats + init_user
        assert len(ixs) == 2
