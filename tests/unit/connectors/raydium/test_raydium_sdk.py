"""Tests for RaydiumCLMMSDK instruction building (VIB-371).

Verifies:
1. PDA derivation (position, tick array, metadata, ATA)
2. openPositionV2 instruction building
3. decreaseLiquidityV2 instruction building
4. closePosition instruction building
5. Pool info fetching (mocked)
"""

import struct
from unittest.mock import MagicMock, patch

import pytest
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from almanak.framework.connectors.raydium.constants import (
    CLMM_PROGRAM_ID,
    CLOSE_POSITION_DISCRIMINATOR,
    DECREASE_LIQUIDITY_V2_DISCRIMINATOR,
    OPEN_POSITION_V2_DISCRIMINATOR,
)
from almanak.framework.connectors.raydium.exceptions import RaydiumConfigError, RaydiumPoolError
from almanak.framework.connectors.raydium.models import RaydiumPool, RaydiumPosition
from almanak.framework.connectors.raydium.sdk import RaydiumCLMMSDK

WALLET = "KUMtRazMP7vwvc2kthnGZ9Cq6ZsGRiYC97snMYepNx9"
SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

MOCK_POOL = RaydiumPool(
    address="2QdhepnKRTLjjSqPL1PtKNwqrUkoLee2mQW2r4iB8F8J",
    mint_a=SOL_MINT,
    mint_b=USDC_MINT,
    symbol_a="SOL",
    symbol_b="USDC",
    decimals_a=9,
    decimals_b=6,
    tick_spacing=60,
    current_price=150.0,
    vault_a="7GmDCbu7bYiKgcFC1JKzQm8KHr9NvfV2UvVLqtZpGWJt",
    vault_b="86SwKuyfVwFDmHx5GoJKPwB2VVEhPfDp7tTpbz3JxJ3g",
    amm_config="CQYbhr6amMFM7P7P1qqGjb5ySYDSiEJ3bBGKjGyPoHrX",
)


class TestSDKInitialization:
    """RaydiumCLMMSDK initialization."""

    def test_valid_initialization(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)
        assert sdk.wallet_address == WALLET

    def test_empty_wallet_raises(self):
        with pytest.raises(RaydiumConfigError, match="wallet_address"):
            RaydiumCLMMSDK(wallet_address="")


class TestPDADerivation:
    """PDA computation for Raydium accounts."""

    def test_position_pda_is_deterministic(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)
        nft_mint = Keypair().pubkey()

        pda1 = sdk._find_position_pda(nft_mint)
        pda2 = sdk._find_position_pda(nft_mint)
        assert pda1 == pda2

    def test_different_mints_give_different_pdas(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)
        mint1 = Keypair().pubkey()
        mint2 = Keypair().pubkey()

        pda1 = sdk._find_position_pda(mint1)
        pda2 = sdk._find_position_pda(mint2)
        assert pda1 != pda2

    def test_tick_array_pda_is_deterministic(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)
        pool = Pubkey.from_string(MOCK_POOL.address)

        pda1 = sdk._find_tick_array_pda(pool, 0)
        pda2 = sdk._find_tick_array_pda(pool, 0)
        assert pda1 == pda2

    def test_different_tick_start_gives_different_pda(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)
        pool = Pubkey.from_string(MOCK_POOL.address)

        pda1 = sdk._find_tick_array_pda(pool, 0)
        pda2 = sdk._find_tick_array_pda(pool, 3600)
        assert pda1 != pda2

    def test_ata_computation(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)
        owner = Pubkey.from_string(WALLET)
        mint = Pubkey.from_string(USDC_MINT)

        ata = sdk._get_ata(owner, mint)
        assert isinstance(ata, Pubkey)
        # ATA should be deterministic
        assert ata == sdk._get_ata(owner, mint)


class TestBuildOpenPositionIx:
    """build_open_position_ix() instruction building."""

    def test_returns_instruction_and_keypair(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)

        ixs, nft_mint_kp = sdk.build_open_position_ix(
            pool=MOCK_POOL,
            tick_lower=-3600,
            tick_upper=3600,
            amount_a_max=1_000_000_000,  # 1 SOL
            amount_b_max=150_000_000,     # 150 USDC
            liquidity=1_000_000,
        )

        assert len(ixs) == 1
        assert isinstance(nft_mint_kp, Keypair)

    def test_instruction_has_correct_program_id(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)

        ixs, _ = sdk.build_open_position_ix(
            pool=MOCK_POOL,
            tick_lower=-3600,
            tick_upper=3600,
            amount_a_max=1_000_000_000,
            amount_b_max=150_000_000,
            liquidity=1_000_000,
        )

        assert str(ixs[0].program_id) == CLMM_PROGRAM_ID

    def test_instruction_data_starts_with_discriminator(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)

        ixs, _ = sdk.build_open_position_ix(
            pool=MOCK_POOL,
            tick_lower=-3600,
            tick_upper=3600,
            amount_a_max=1_000_000_000,
            amount_b_max=150_000_000,
            liquidity=1_000_000,
        )

        data = bytes(ixs[0].data)
        assert data[:8] == OPEN_POSITION_V2_DISCRIMINATOR

    def test_instruction_has_22_accounts(self):
        """openPositionV2 requires 22 accounts (includes Token2022 program + token mints)."""
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)

        ixs, _ = sdk.build_open_position_ix(
            pool=MOCK_POOL,
            tick_lower=-3600,
            tick_upper=3600,
            amount_a_max=1_000_000_000,
            amount_b_max=150_000_000,
            liquidity=1_000_000,
        )

        assert len(ixs[0].accounts) == 22

    def test_first_account_is_payer_signer(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)

        ixs, _ = sdk.build_open_position_ix(
            pool=MOCK_POOL,
            tick_lower=-3600,
            tick_upper=3600,
            amount_a_max=1_000_000_000,
            amount_b_max=150_000_000,
            liquidity=1_000_000,
        )

        payer_account = ixs[0].accounts[0]
        assert str(payer_account.pubkey) == WALLET
        assert payer_account.is_signer is True
        assert payer_account.is_writable is True

    def test_nft_mint_is_signer(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)

        ixs, nft_mint_kp = sdk.build_open_position_ix(
            pool=MOCK_POOL,
            tick_lower=-3600,
            tick_upper=3600,
            amount_a_max=1_000_000_000,
            amount_b_max=150_000_000,
            liquidity=1_000_000,
        )

        nft_mint_account = ixs[0].accounts[2]
        assert str(nft_mint_account.pubkey) == str(nft_mint_kp.pubkey())
        assert nft_mint_account.is_signer is True

    def test_missing_vault_raises(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)
        bad_pool = RaydiumPool(
            address=MOCK_POOL.address,
            mint_a=SOL_MINT,
            mint_b=USDC_MINT,
            vault_a="",  # Missing!
            vault_b="",
        )

        with pytest.raises(RaydiumPoolError, match="vault"):
            sdk.build_open_position_ix(
                pool=bad_pool,
                tick_lower=-3600,
                tick_upper=3600,
                amount_a_max=1_000_000_000,
                amount_b_max=150_000_000,
                liquidity=1_000_000,
            )


class TestBuildDecreaseLiquidityIx:
    """build_decrease_liquidity_ix() instruction building."""

    def test_returns_instruction(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)
        position = RaydiumPosition(
            nft_mint="6RfnQFgLbmfRZGDSxUrPnAiqjg3CtsneHqz2mF7Tpump",
            pool_address=MOCK_POOL.address,
            tick_lower=-3600,
            tick_upper=3600,
            liquidity=1_000_000,
        )

        ixs = sdk.build_decrease_liquidity_ix(
            pool=MOCK_POOL,
            position=position,
            liquidity=500_000,
        )

        assert len(ixs) == 1

    def test_instruction_data_starts_with_discriminator(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)
        position = RaydiumPosition(
            nft_mint="6RfnQFgLbmfRZGDSxUrPnAiqjg3CtsneHqz2mF7Tpump",
            pool_address=MOCK_POOL.address,
            tick_lower=-3600,
            tick_upper=3600,
            liquidity=1_000_000,
        )

        ixs = sdk.build_decrease_liquidity_ix(
            pool=MOCK_POOL,
            position=position,
            liquidity=500_000,
        )

        data = bytes(ixs[0].data)
        assert data[:8] == DECREASE_LIQUIDITY_V2_DISCRIMINATOR

    def test_has_15_accounts(self):
        """decreaseLiquidityV2 requires 15 accounts."""
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)
        position = RaydiumPosition(
            nft_mint="6RfnQFgLbmfRZGDSxUrPnAiqjg3CtsneHqz2mF7Tpump",
            pool_address=MOCK_POOL.address,
            tick_lower=-3600,
            tick_upper=3600,
            liquidity=1_000_000,
        )

        ixs = sdk.build_decrease_liquidity_ix(
            pool=MOCK_POOL,
            position=position,
            liquidity=500_000,
        )

        assert len(ixs[0].accounts) == 15


class TestBuildClosePositionIx:
    """build_close_position_ix() instruction building."""

    def test_returns_instruction(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)
        position = RaydiumPosition(
            nft_mint="6RfnQFgLbmfRZGDSxUrPnAiqjg3CtsneHqz2mF7Tpump",
            pool_address=MOCK_POOL.address,
            tick_lower=-3600,
            tick_upper=3600,
        )

        ixs = sdk.build_close_position_ix(position)

        assert len(ixs) == 1

    def test_instruction_data_is_discriminator_only(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)
        position = RaydiumPosition(
            nft_mint="6RfnQFgLbmfRZGDSxUrPnAiqjg3CtsneHqz2mF7Tpump",
            pool_address=MOCK_POOL.address,
            tick_lower=-3600,
            tick_upper=3600,
        )

        ixs = sdk.build_close_position_ix(position)

        data = bytes(ixs[0].data)
        assert data == CLOSE_POSITION_DISCRIMINATOR

    def test_has_6_accounts(self):
        """closePosition requires 6 accounts."""
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)
        position = RaydiumPosition(
            nft_mint="6RfnQFgLbmfRZGDSxUrPnAiqjg3CtsneHqz2mF7Tpump",
            pool_address=MOCK_POOL.address,
            tick_lower=-3600,
            tick_upper=3600,
        )

        ixs = sdk.build_close_position_ix(position)

        assert len(ixs[0].accounts) == 6


class TestPoolInfo:
    """get_pool_info() API interaction."""

    def test_pool_not_found_raises(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)

        with patch.object(sdk, "_make_request", return_value={"data": []}):
            with pytest.raises(RaydiumPoolError, match="not found"):
                sdk.get_pool_info("nonexistent")

    def test_pool_found_returns_pool(self):
        sdk = RaydiumCLMMSDK(wallet_address=WALLET)

        mock_response = {
            "success": True,
            "data": [
                {
                    "id": MOCK_POOL.address,
                    "mintA": {"address": SOL_MINT, "symbol": "SOL", "decimals": 9},
                    "mintB": {"address": USDC_MINT, "symbol": "USDC", "decimals": 6},
                    "config": {"tickSpacing": 60, "id": "config123", "tradeFeeRate": 3000},
                    "price": 150.5,
                    "tvl": 5000000,
                    "mintVaultA": MOCK_POOL.vault_a,
                    "mintVaultB": MOCK_POOL.vault_b,
                    "programId": CLMM_PROGRAM_ID,
                }
            ],
        }

        with patch.object(sdk, "_make_request", return_value=mock_response):
            pool = sdk.get_pool_info(MOCK_POOL.address)

        assert pool.address == MOCK_POOL.address
        assert pool.symbol_a == "SOL"
        assert pool.symbol_b == "USDC"
        assert pool.tick_spacing == 60
