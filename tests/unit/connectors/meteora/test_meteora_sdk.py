"""Tests for MeteoraSDK instruction building (VIB-384).

Verifies:
1. PDA derivation (position, bin array, event authority, oracle)
2. Bin math (bin_id_to_price, price_to_bin_id, roundtrip)
3. Instruction encoding (struct pack format correctness)
4. Pool fetching (mocked)
"""

import struct
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from almanak.framework.connectors.meteora.constants import (
    ADD_LIQUIDITY_BY_STRATEGY_DISCRIMINATOR,
    BIN_ID_OFFSET,
    CLOSE_POSITION_DISCRIMINATOR,
    DLMM_PROGRAM_ID,
    INITIALIZE_POSITION_DISCRIMINATOR,
    REMOVE_LIQUIDITY_BY_RANGE_DISCRIMINATOR,
    STRATEGY_TYPE_SPOT_BALANCED,
)
from almanak.framework.connectors.meteora.exceptions import MeteoraPoolError
from almanak.framework.connectors.meteora.math import (
    bin_id_to_price,
    get_bin_array_index,
    get_bin_array_pda,
    price_to_bin_id,
)
from almanak.framework.connectors.meteora.models import MeteoraPool, MeteoraPosition
from almanak.framework.connectors.meteora.sdk import MeteoraSDK

WALLET = "KUMtRazMP7vwvc2kthnGZ9Cq6ZsGRiYC97snMYepNx9"
SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

MOCK_POOL = MeteoraPool(
    address="5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6Q",
    mint_x=SOL_MINT,
    mint_y=USDC_MINT,
    symbol_x="SOL",
    symbol_y="USDC",
    decimals_x=9,
    decimals_y=6,
    bin_step=10,
    active_bin_id=BIN_ID_OFFSET + 100,
    current_price=150.0,
    vault_x=str(Keypair().pubkey()),
    vault_y=str(Keypair().pubkey()),
)


class TestSDKInitialization:
    """MeteoraSDK initialization."""

    def test_valid_initialization(self):
        sdk = MeteoraSDK(wallet_address=WALLET)
        assert sdk.wallet_address == WALLET

    def test_empty_wallet_raises(self):
        with pytest.raises(ValueError, match="wallet_address"):
            MeteoraSDK(wallet_address="")


class TestBinMath:
    """Bin ID <-> price conversion tests."""

    def test_center_bin_gives_price_one(self):
        """Bin at BIN_ID_OFFSET should give price ~1.0."""
        price = bin_id_to_price(BIN_ID_OFFSET, bin_step=10)
        assert abs(float(price) - 1.0) < 0.001

    def test_higher_bin_gives_higher_price(self):
        """Bins above offset should give price > 1."""
        price = bin_id_to_price(BIN_ID_OFFSET + 100, bin_step=10)
        assert float(price) > 1.0

    def test_lower_bin_gives_lower_price(self):
        """Bins below offset should give price < 1."""
        price = bin_id_to_price(BIN_ID_OFFSET - 100, bin_step=10)
        assert float(price) < 1.0

    def test_price_to_bin_roundtrip(self):
        """price_to_bin_id(bin_id_to_price(x)) should roundtrip."""
        for bin_id in [BIN_ID_OFFSET, BIN_ID_OFFSET + 50, BIN_ID_OFFSET - 50]:
            price = bin_id_to_price(bin_id, bin_step=10)
            recovered = price_to_bin_id(price, bin_step=10)
            assert abs(recovered - bin_id) <= 1, f"Roundtrip failed for bin_id={bin_id}"

    def test_price_to_bin_with_decimals(self):
        """Price conversion should account for token decimals."""
        # SOL/USDC: decimals_x=9, decimals_y=6
        price = Decimal("150")  # 150 USDC per SOL
        bin_id = price_to_bin_id(price, bin_step=10, decimals_x=9, decimals_y=6)
        # Recover
        recovered_price = bin_id_to_price(bin_id, bin_step=10, decimals_x=9, decimals_y=6)
        assert abs(float(recovered_price) - 150.0) / 150.0 < 0.05  # Within 5%

    def test_negative_price_raises(self):
        with pytest.raises(ValueError, match="positive"):
            price_to_bin_id(Decimal("-1"), bin_step=10)

    def test_zero_price_raises(self):
        with pytest.raises(ValueError, match="positive"):
            price_to_bin_id(Decimal("0"), bin_step=10)

    def test_different_bin_steps(self):
        """Different bin_step values should produce different prices for same bin."""
        price_10 = bin_id_to_price(BIN_ID_OFFSET + 100, bin_step=10)
        price_50 = bin_id_to_price(BIN_ID_OFFSET + 100, bin_step=50)
        assert float(price_50) > float(price_10)


class TestBinArrayIndex:
    """Bin array index computation."""

    def test_zero_bin(self):
        idx = get_bin_array_index(0)
        assert idx == 0

    def test_positive_bin(self):
        idx = get_bin_array_index(69)
        assert idx == 0  # Bins 0-69 are in array 0

    def test_next_array(self):
        idx = get_bin_array_index(70)
        assert idx == 1  # Bin 70 starts array 1

    def test_negative_bin(self):
        idx = get_bin_array_index(-1)
        assert idx == -1

    def test_bin_array_pda_deterministic(self):
        program_id = Pubkey.from_string(DLMM_PROGRAM_ID)
        lb_pair = Keypair().pubkey()

        pda1 = get_bin_array_pda(program_id, lb_pair, 0)
        pda2 = get_bin_array_pda(program_id, lb_pair, 0)
        assert pda1 == pda2

    def test_different_indexes_different_pdas(self):
        program_id = Pubkey.from_string(DLMM_PROGRAM_ID)
        lb_pair = Keypair().pubkey()

        pda1 = get_bin_array_pda(program_id, lb_pair, 0)
        pda2 = get_bin_array_pda(program_id, lb_pair, 1)
        assert pda1 != pda2


class TestPDADerivation:
    """PDA computation for Meteora accounts."""

    def test_position_pda_is_deterministic(self):
        sdk = MeteoraSDK(wallet_address=WALLET)
        lb_pair = Keypair().pubkey()
        base = Keypair().pubkey()

        pda1 = sdk.get_position_pda(lb_pair, base, 100, 10)
        pda2 = sdk.get_position_pda(lb_pair, base, 100, 10)
        assert pda1 == pda2

    def test_different_params_give_different_pdas(self):
        sdk = MeteoraSDK(wallet_address=WALLET)
        lb_pair = Keypair().pubkey()
        base1 = Keypair().pubkey()
        base2 = Keypair().pubkey()

        pda1 = sdk.get_position_pda(lb_pair, base1, 100, 10)
        pda2 = sdk.get_position_pda(lb_pair, base2, 100, 10)
        assert pda1 != pda2

    def test_event_authority_pda_is_deterministic(self):
        sdk = MeteoraSDK(wallet_address=WALLET)
        pda1 = sdk.get_event_authority_pda()
        pda2 = sdk.get_event_authority_pda()
        assert pda1 == pda2

    def test_oracle_pda_is_deterministic(self):
        sdk = MeteoraSDK(wallet_address=WALLET)
        lb_pair = Keypair().pubkey()
        pda1 = sdk.get_oracle_pda(lb_pair)
        pda2 = sdk.get_oracle_pda(lb_pair)
        assert pda1 == pda2


class TestInstructionBuilding:
    """Instruction building tests."""

    def test_initialize_position_instruction(self):
        sdk = MeteoraSDK(wallet_address=WALLET)
        lb_pair = Keypair().pubkey()
        position_kp = Keypair()

        ix = sdk.build_initialize_position_ix(
            lb_pair=lb_pair,
            position_kp=position_kp,
            lower_bin_id=100,
            width=10,
        )

        # Check discriminator
        assert bytes(ix.data)[:8] == INITIALIZE_POSITION_DISCRIMINATOR
        # Check args: lower_bin_id(i32) + width(i32)
        lower, width = struct.unpack_from("<ii", bytes(ix.data), 8)
        assert lower == 100
        assert width == 10
        # Check accounts
        assert len(ix.accounts) == 8

    def test_add_liquidity_by_strategy_instruction(self):
        sdk = MeteoraSDK(wallet_address=WALLET)
        position = Keypair().pubkey()

        ix = sdk.build_add_liquidity_by_strategy_ix(
            pool=MOCK_POOL,
            position=position,
            lower_bin_id=100,
            upper_bin_id=110,
            amount_x=1_000_000_000,
            amount_y=150_000_000,
            active_id=BIN_ID_OFFSET + 100,
            strategy_type=STRATEGY_TYPE_SPOT_BALANCED,
        )

        # Check discriminator
        assert bytes(ix.data)[:8] == ADD_LIQUIDITY_BY_STRATEGY_DISCRIMINATOR
        # Parse args
        offset = 8
        amount_x, amount_y, active_id, max_slippage = struct.unpack_from("<QQii", bytes(ix.data), offset)
        assert amount_x == 1_000_000_000
        assert amount_y == 150_000_000
        assert active_id == BIN_ID_OFFSET + 100

    def test_remove_liquidity_by_range_instruction(self):
        sdk = MeteoraSDK(wallet_address=WALLET)
        position = Keypair().pubkey()

        ix = sdk.build_remove_liquidity_by_range_ix(
            pool=MOCK_POOL,
            position=position,
            from_bin_id=100,
            to_bin_id=110,
            bps_to_remove=10000,
        )

        # Check discriminator
        assert bytes(ix.data)[:8] == REMOVE_LIQUIDITY_BY_RANGE_DISCRIMINATOR
        # Parse args: from_bin_id(i32) + to_bin_id(i32) + bps_to_remove(u16)
        from_bin, to_bin, bps = struct.unpack_from("<iiH", bytes(ix.data), 8)
        assert from_bin == 100
        assert to_bin == 110
        assert bps == 10000

    def test_close_position_instruction(self):
        sdk = MeteoraSDK(wallet_address=WALLET)
        lb_pair = Keypair().pubkey()
        position = Keypair().pubkey()

        ix = sdk.build_close_position_ix(
            lb_pair=lb_pair,
            position=position,
        )

        # Check discriminator only (no args)
        assert bytes(ix.data) == CLOSE_POSITION_DISCRIMINATOR
        assert len(ix.accounts) == 5


class TestHighLevelTransactionBuilders:
    """High-level transaction builder tests."""

    def test_build_open_position_transaction(self):
        sdk = MeteoraSDK(wallet_address=WALLET)

        ixs, position_kp, metadata = sdk.build_open_position_transaction(
            pool=MOCK_POOL,
            lower_bin_id=BIN_ID_OFFSET,
            upper_bin_id=BIN_ID_OFFSET + 10,
            amount_x=1_000_000_000,
            amount_y=150_000_000,
        )

        # Should have ATA setup + initialize + addLiquidity
        assert len(ixs) >= 3
        assert isinstance(position_kp, Keypair)
        assert metadata["lower_bin_id"] == BIN_ID_OFFSET
        assert metadata["upper_bin_id"] == BIN_ID_OFFSET + 10
        assert metadata["width"] == 11
        assert metadata["position_address"] == str(position_kp.pubkey())

    def test_build_close_position_transaction(self):
        sdk = MeteoraSDK(wallet_address=WALLET)
        position = MeteoraPosition(
            position_address=str(Keypair().pubkey()),
            lb_pair=MOCK_POOL.address,
            lower_bin_id=BIN_ID_OFFSET,
            upper_bin_id=BIN_ID_OFFSET + 10,
            total_x=1000,
        )

        ixs, metadata = sdk.build_close_position_transaction(
            pool=MOCK_POOL,
            position=position,
        )

        # Should have removeLiquidity + closePosition
        assert len(ixs) == 2
        assert metadata["position_address"] == position.position_address


class TestPoolFetching:
    """Pool query tests (mocked)."""

    @patch.object(MeteoraSDK, "_make_request")
    def test_get_pool_success(self, mock_request):
        mock_request.return_value = {
            "address": "pool123",
            "mint_x": SOL_MINT,
            "mint_y": USDC_MINT,
            "name": "SOL-USDC",
            "bin_step": 10,
            "active_id": 8388708,
            "current_price": 150.0,
            "liquidity": 1000000,
            "mint_x_decimals": 9,
            "mint_y_decimals": 6,
        }

        sdk = MeteoraSDK(wallet_address=WALLET)
        pool = sdk.get_pool("pool123")
        assert pool.address == "pool123"
        assert pool.mint_x == SOL_MINT
        assert pool.mint_y == USDC_MINT
        assert pool.bin_step == 10

    @patch.object(MeteoraSDK, "_make_request")
    def test_get_pool_not_found(self, mock_request):
        mock_request.return_value = None

        sdk = MeteoraSDK(wallet_address=WALLET)
        with pytest.raises(MeteoraPoolError, match="not found"):
            sdk.get_pool("nonexistent")

    @patch.object(MeteoraSDK, "_make_request")
    def test_find_pool_success(self, mock_request):
        mock_request.return_value = {
            "pairs": [
                {
                    "address": "pool_best",
                    "mint_x": SOL_MINT,
                    "mint_y": USDC_MINT,
                    "name": "SOL-USDC",
                    "bin_step": 10,
                    "active_id": 8388708,
                    "liquidity": 5000000,
                    "mint_x_decimals": 9,
                    "mint_y_decimals": 6,
                },
            ],
        }

        sdk = MeteoraSDK(wallet_address=WALLET)
        pool = sdk.find_pool(SOL_MINT, USDC_MINT)
        assert pool is not None
        assert pool.address == "pool_best"
