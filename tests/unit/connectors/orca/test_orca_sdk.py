"""Tests for OrcaWhirlpoolSDK instruction building (VIB-386).

Verifies:
1. PDA derivation (position, tick array, metadata, ATA)
2. open_position_with_metadata + increase_liquidity instruction building
3. decrease_liquidity instruction building
4. close_position instruction building
5. Pool info fetching (mocked)
"""

import struct
from unittest.mock import MagicMock, patch

import pytest
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from almanak.framework.connectors.orca.constants import (
    CLOSE_POSITION_DISCRIMINATOR,
    DECREASE_LIQUIDITY_DISCRIMINATOR,
    INCREASE_LIQUIDITY_DISCRIMINATOR,
    OPEN_POSITION_WITH_METADATA_DISCRIMINATOR,
    WHIRLPOOL_PROGRAM_ID,
)
from almanak.framework.connectors.orca.exceptions import OrcaConfigError, OrcaPoolError
from almanak.framework.connectors.orca.models import OrcaPool, OrcaPosition
from almanak.framework.connectors.orca.sdk import OrcaWhirlpoolSDK

WALLET = "KUMtRazMP7vwvc2kthnGZ9Cq6ZsGRiYC97snMYepNx9"
SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

MOCK_POOL = OrcaPool(
    address="HJPjoWUrhoZzkNfRpHuieeFk9AnbKnovy8po1NtRSqX2",
    mint_a=SOL_MINT,
    mint_b=USDC_MINT,
    symbol_a="SOL",
    symbol_b="USDC",
    decimals_a=9,
    decimals_b=6,
    tick_spacing=64,
    current_price=150.0,
    vault_a="7GmDCbu7bYiKgcFC1JKzQm8KHr9NvfV2UvVLqtZpGWJt",
    vault_b="86SwKuyfVwFDmHx5GoJKPwB2VVEhPfDp7tTpbz3JxJ3g",
)


class TestSDKInitialization:
    """OrcaWhirlpoolSDK initialization."""

    def test_valid_initialization(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        assert sdk.wallet_address == WALLET

    def test_empty_wallet_raises(self):
        with pytest.raises(OrcaConfigError, match="wallet_address"):
            OrcaWhirlpoolSDK(wallet_address="")


class TestPDADerivation:
    """PDA computation for Orca accounts."""

    def test_position_pda_is_deterministic(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        nft_mint = Keypair().pubkey()

        pda1 = sdk._find_position_pda(nft_mint)
        pda2 = sdk._find_position_pda(nft_mint)
        assert pda1 == pda2

    def test_different_mints_give_different_pdas(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        mint1 = Keypair().pubkey()
        mint2 = Keypair().pubkey()

        pda1 = sdk._find_position_pda(mint1)
        pda2 = sdk._find_position_pda(mint2)
        assert pda1 != pda2

    def test_tick_array_pda_is_deterministic(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        pool = Pubkey.from_string(MOCK_POOL.address)

        pda1 = sdk._find_tick_array_pda(pool, 0)
        pda2 = sdk._find_tick_array_pda(pool, 0)
        assert pda1 == pda2

    def test_different_start_indices_give_different_tick_arrays(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        pool = Pubkey.from_string(MOCK_POOL.address)

        pda1 = sdk._find_tick_array_pda(pool, 0)
        pda2 = sdk._find_tick_array_pda(pool, 5632)
        assert pda1 != pda2

    def test_oracle_pda_is_deterministic(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        pool = Pubkey.from_string(MOCK_POOL.address)

        pda1 = sdk._find_oracle_pda(pool)
        pda2 = sdk._find_oracle_pda(pool)
        assert pda1 == pda2

    def test_metadata_pda_is_deterministic(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        nft_mint = Keypair().pubkey()

        pda1 = sdk._find_metadata_pda(nft_mint)
        pda2 = sdk._find_metadata_pda(nft_mint)
        assert pda1 == pda2

    def test_ata_is_deterministic(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        owner = Pubkey.from_string(WALLET)
        mint = Pubkey.from_string(SOL_MINT)

        ata1 = sdk._get_ata(owner, mint)
        ata2 = sdk._get_ata(owner, mint)
        assert ata1 == ata2


class TestTickArrayStartIndex:
    """_tick_array_start_index computation."""

    def test_positive_tick(self):
        # tick_spacing=64, array_size=88*64=5632
        start = OrcaWhirlpoolSDK._tick_array_start_index(100, 64)
        assert start == 0  # 100 < 5632, so start=0

    def test_large_positive_tick(self):
        start = OrcaWhirlpoolSDK._tick_array_start_index(6000, 64)
        assert start == 5632  # 6000 // 5632 * 5632

    def test_negative_tick(self):
        start = OrcaWhirlpoolSDK._tick_array_start_index(-100, 64)
        assert start == -5632

    def test_exact_boundary(self):
        start = OrcaWhirlpoolSDK._tick_array_start_index(5632, 64)
        assert start == 5632


class TestBuildOpenPositionIx:
    """build_open_position_ix produces valid instructions."""

    def test_open_position_produces_two_instructions(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)

        ixs, nft_mint_kp = sdk.build_open_position_ix(
            pool=MOCK_POOL,
            tick_lower=-640,
            tick_upper=640,
            amount_a_max=1_000_000_000,
            amount_b_max=150_000_000,
            liquidity=1_000_000,
        )

        # Should produce 2 instructions: open_position_with_metadata + increase_liquidity
        assert len(ixs) == 2

        # Verify open_position instruction
        assert bytes(ixs[0].data)[:8] == OPEN_POSITION_WITH_METADATA_DISCRIMINATOR

        # Verify increase_liquidity instruction
        assert bytes(ixs[1].data)[:8] == INCREASE_LIQUIDITY_DISCRIMINATOR

        # Both should target the Whirlpool program
        program_id = Pubkey.from_string(WHIRLPOOL_PROGRAM_ID)
        assert ixs[0].program_id == program_id
        assert ixs[1].program_id == program_id

        # NFT mint keypair should be valid
        assert nft_mint_kp.pubkey() is not None

    def test_open_position_encodes_tick_range(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)

        tick_lower = -1280
        tick_upper = 1280
        ixs, _ = sdk.build_open_position_ix(
            pool=MOCK_POOL,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            amount_a_max=1_000_000_000,
            amount_b_max=150_000_000,
            liquidity=1_000_000,
        )

        # open_position_with_metadata: discriminator(8) + bump(1) + metadata_bump(1) + tick_lower(4) + tick_upper(4)
        data = bytes(ixs[0].data)
        parsed_tick_lower = struct.unpack_from("<i", data, 10)[0]
        parsed_tick_upper = struct.unpack_from("<i", data, 14)[0]
        assert parsed_tick_lower == tick_lower
        assert parsed_tick_upper == tick_upper

    def test_open_position_pool_missing_vaults_raises(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        pool_no_vaults = OrcaPool(
            address=MOCK_POOL.address,
            mint_a=SOL_MINT,
            mint_b=USDC_MINT,
            vault_a="",
            vault_b="",
        )

        with pytest.raises(OrcaPoolError, match="vault"):
            sdk.build_open_position_ix(
                pool=pool_no_vaults,
                tick_lower=-640,
                tick_upper=640,
                amount_a_max=1_000_000_000,
                amount_b_max=150_000_000,
                liquidity=1_000_000,
            )


class TestBuildDecreaseLiquidityIx:
    """build_decrease_liquidity_ix tests."""

    def test_decrease_liquidity_produces_instruction(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        position = OrcaPosition(
            nft_mint=str(Keypair().pubkey()),
            pool_address=MOCK_POOL.address,
            tick_lower=-640,
            tick_upper=640,
            liquidity=1_000_000,
        )

        ixs = sdk.build_decrease_liquidity_ix(
            pool=MOCK_POOL,
            position=position,
            liquidity=500_000,
        )

        assert len(ixs) == 1
        assert bytes(ixs[0].data)[:8] == DECREASE_LIQUIDITY_DISCRIMINATOR
        assert ixs[0].program_id == Pubkey.from_string(WHIRLPOOL_PROGRAM_ID)


class TestBuildClosePositionIx:
    """build_close_position_ix tests."""

    def test_close_position_produces_instruction(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        position = OrcaPosition(
            nft_mint=str(Keypair().pubkey()),
            pool_address=MOCK_POOL.address,
            tick_lower=-640,
            tick_upper=640,
            liquidity=0,
        )

        ixs = sdk.build_close_position_ix(position)

        assert len(ixs) == 1
        assert bytes(ixs[0].data) == CLOSE_POSITION_DISCRIMINATOR
        assert ixs[0].program_id == Pubkey.from_string(WHIRLPOOL_PROGRAM_ID)


class TestBuildHighLevelTransactions:
    """High-level transaction builder tests."""

    def test_open_position_transaction_includes_ata_setup(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)

        ixs, nft_mint_kp, metadata = sdk.build_open_position_transaction(
            pool=MOCK_POOL,
            price_lower=100.0,
            price_upper=200.0,
            amount_a=1_000_000,
            amount_b=150_000_000,
        )

        # Should have ATA setup (2) + open_position (1) + increase_liquidity (1)
        assert len(ixs) >= 4
        assert "tick_lower" in metadata
        assert "tick_upper" in metadata
        assert "liquidity" in metadata
        assert "nft_mint" in metadata

    def test_close_position_transaction_with_liquidity(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        position = OrcaPosition(
            nft_mint=str(Keypair().pubkey()),
            pool_address=MOCK_POOL.address,
            tick_lower=-640,
            tick_upper=640,
            liquidity=1_000_000,
        )

        ixs, metadata = sdk.build_close_position_transaction(
            pool=MOCK_POOL,
            position=position,
        )

        # Should have decrease_liquidity + close_position
        assert len(ixs) == 2
        assert bytes(ixs[0].data)[:8] == DECREASE_LIQUIDITY_DISCRIMINATOR
        assert bytes(ixs[1].data) == CLOSE_POSITION_DISCRIMINATOR

    def test_close_position_transaction_zero_liquidity(self):
        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        position = OrcaPosition(
            nft_mint=str(Keypair().pubkey()),
            pool_address=MOCK_POOL.address,
            tick_lower=-640,
            tick_upper=640,
            liquidity=0,
        )

        ixs, metadata = sdk.build_close_position_transaction(
            pool=MOCK_POOL,
            position=position,
        )

        # Only close_position (no decrease needed)
        assert len(ixs) == 1
        assert bytes(ixs[0].data) == CLOSE_POSITION_DISCRIMINATOR


class TestPoolFetch:
    """Pool info fetching (mocked HTTP)."""

    @patch("almanak.framework.connectors.orca.sdk.OrcaWhirlpoolSDK._make_request")
    def test_get_pool_info_success(self, mock_request):
        mock_request.return_value = {
            "address": MOCK_POOL.address,
            "tokenA": {
                "mint": SOL_MINT,
                "symbol": "SOL",
                "decimals": 9,
                "vault": MOCK_POOL.vault_a,
            },
            "tokenB": {
                "mint": USDC_MINT,
                "symbol": "USDC",
                "decimals": 6,
                "vault": MOCK_POOL.vault_b,
            },
            "tickSpacing": 64,
            "price": 150.0,
            "tvl": 5000000.0,
        }

        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        pool = sdk.get_pool_info(MOCK_POOL.address)

        assert pool.address == MOCK_POOL.address
        assert pool.mint_a == SOL_MINT
        assert pool.mint_b == USDC_MINT
        assert pool.tick_spacing == 64
        assert pool.current_price == 150.0

    @patch("almanak.framework.connectors.orca.sdk.OrcaWhirlpoolSDK._make_request")
    def test_get_pool_info_not_found(self, mock_request):
        mock_request.return_value = {}

        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        with pytest.raises(OrcaPoolError, match="not found"):
            sdk.get_pool_info("nonexistent-pool")

    @patch("almanak.framework.connectors.orca.sdk.OrcaWhirlpoolSDK._make_request")
    def test_get_pool_info_missing_vaults(self, mock_request):
        mock_request.return_value = {
            "address": "some-pool",
            "tokenA": {"mint": SOL_MINT, "decimals": 9, "vault": ""},
            "tokenB": {"mint": USDC_MINT, "decimals": 6, "vault": ""},
            "tickSpacing": 64,
        }

        sdk = OrcaWhirlpoolSDK(wallet_address=WALLET)
        with pytest.raises(OrcaPoolError, match="vault"):
            sdk.get_pool_info("some-pool")
