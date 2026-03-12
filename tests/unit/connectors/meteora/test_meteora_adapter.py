"""Tests for MeteoraAdapter intent compilation (VIB-384).

Verifies:
1. LPOpenIntent compilation with mocked SDK
2. LPCloseIntent compilation with mocked SDK
3. Error handling
4. ActionBundle shape and metadata
"""

import base64
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from solders.keypair import Keypair

from almanak.framework.connectors.meteora.adapter import MeteoraAdapter, MeteoraConfig
from almanak.framework.connectors.meteora.constants import BIN_ID_OFFSET, STRATEGY_TYPE_SPOT_BALANCED
from almanak.framework.connectors.meteora.models import MeteoraPool, MeteoraPosition
from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent

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
    vault_x="vault_x_address_placeholder_xxxxxxxxxxxxxxxxxxxxxxx",
    vault_y="vault_y_address_placeholder_xxxxxxxxxxxxxxxxxxxxxxx",
)


class TestMeteoraConfig:
    """MeteoraConfig validation."""

    def test_valid_config(self):
        config = MeteoraConfig(wallet_address=WALLET)
        assert config.wallet_address == WALLET
        assert config.rpc_url == ""
        assert config.default_strategy_type == 6

    def test_empty_wallet_raises(self):
        with pytest.raises(ValueError, match="wallet_address"):
            MeteoraConfig(wallet_address="")

    def test_custom_strategy_type(self):
        config = MeteoraConfig(wallet_address=WALLET, default_strategy_type=7)
        assert config.default_strategy_type == 7


class TestLPOpenCompilation:
    """MeteoraAdapter.compile_lp_open_intent tests."""

    @patch("almanak.framework.connectors.meteora.adapter.MeteoraSDK")
    def test_compile_lp_open_success(self, MockSDK):
        """Successful LP open compilation returns valid ActionBundle."""
        # Mock SDK
        position_kp = Keypair()
        mock_sdk = MockSDK.return_value
        mock_sdk.get_pool.return_value = MOCK_POOL
        mock_sdk.build_open_position_transaction.return_value = (
            [],  # Empty instructions for mock — serialization will be mocked
            position_kp,
            {
                "lower_bin_id": BIN_ID_OFFSET,
                "upper_bin_id": BIN_ID_OFFSET + 10,
                "width": 11,
                "position_address": str(position_kp.pubkey()),
                "active_bin_id": BIN_ID_OFFSET + 100,
                "bin_step": 10,
                "slippage_bps": 100,
                "strategy_type": STRATEGY_TYPE_SPOT_BALANCED,
                "amount_x_max": 1_000_000,
                "amount_y_max": 150_000,
            },
        )

        config = MeteoraConfig(wallet_address=WALLET)
        adapter = MeteoraAdapter(config)
        adapter._pool_cache[MOCK_POOL.address] = MOCK_POOL

        # Mock serialization to avoid needing real instructions
        adapter._serialize_transaction = MagicMock(return_value="base64encodedtx")

        intent = LPOpenIntent(
            protocol="meteora_dlmm",
            pool=MOCK_POOL.address,
            amount0=Decimal("1"),
            amount1=Decimal("150"),
            range_lower=Decimal("100"),
            range_upper=Decimal("200"),
        )

        bundle = adapter.compile_lp_open_intent(intent)

        assert bundle.transactions
        assert bundle.metadata["protocol"] == "meteora_dlmm"
        assert bundle.metadata["action"] == "open_position"
        assert bundle.metadata["chain_family"] == "SOLANA"
        assert bundle.metadata["pool"] == MOCK_POOL.address
        assert bundle.sensitive_data is not None
        assert "additional_signers" in bundle.sensitive_data

    @patch("almanak.framework.connectors.meteora.adapter.MeteoraSDK")
    def test_compile_lp_open_error_returns_error_bundle(self, MockSDK):
        """Failed compilation returns error bundle."""
        mock_sdk = MockSDK.return_value
        mock_sdk.get_pool.side_effect = Exception("API unreachable")

        config = MeteoraConfig(wallet_address=WALLET)
        adapter = MeteoraAdapter(config)

        intent = LPOpenIntent(
            protocol="meteora_dlmm",
            pool="nonexistent",
            amount0=Decimal("1"),
            amount1=Decimal("150"),
            range_lower=Decimal("100"),
            range_upper=Decimal("200"),
        )

        bundle = adapter.compile_lp_open_intent(intent)
        assert bundle.metadata.get("error")
        assert not bundle.transactions


class TestLPCloseCompilation:
    """MeteoraAdapter.compile_lp_close_intent tests."""

    @patch("almanak.framework.connectors.meteora.adapter.MeteoraSDK")
    def test_compile_lp_close_no_pool_returns_error(self, MockSDK):
        """LP close without pool returns error."""
        config = MeteoraConfig(wallet_address=WALLET)
        adapter = MeteoraAdapter(config)

        intent = LPCloseIntent(
            protocol="meteora_dlmm",
            pool="",
            position_id="position123",
        )

        bundle = adapter.compile_lp_close_intent(intent)
        assert bundle.metadata.get("error")
        assert "pool address is required" in bundle.metadata["error"]

    @patch("almanak.framework.connectors.meteora.adapter.MeteoraSDK")
    def test_compile_lp_close_success(self, MockSDK):
        """Successful LP close compilation."""
        mock_sdk = MockSDK.return_value
        mock_sdk.get_pool.return_value = MOCK_POOL

        position_addr = str(Keypair().pubkey())
        mock_position = MeteoraPosition(
            position_address=position_addr,
            lb_pair=MOCK_POOL.address,
            lower_bin_id=BIN_ID_OFFSET,
            upper_bin_id=BIN_ID_OFFSET + 10,
        )
        mock_sdk.get_position_state.return_value = mock_position
        mock_sdk.build_close_position_transaction.return_value = (
            [MagicMock()],  # Mock instructions
            {
                "position_address": position_addr,
                "pool": MOCK_POOL.address,
                "lower_bin_id": BIN_ID_OFFSET,
                "upper_bin_id": BIN_ID_OFFSET + 10,
            },
        )

        config = MeteoraConfig(wallet_address=WALLET, rpc_url="http://localhost:8899")
        adapter = MeteoraAdapter(config)
        adapter._pool_cache[MOCK_POOL.address] = MOCK_POOL
        adapter._serialize_transaction = MagicMock(return_value="base64encodedtx")

        intent = LPCloseIntent(
            protocol="meteora_dlmm",
            pool=MOCK_POOL.address,
            position_id=position_addr,
        )

        bundle = adapter.compile_lp_close_intent(intent)

        assert bundle.transactions
        assert bundle.metadata["protocol"] == "meteora_dlmm"
        assert bundle.metadata["action"] == "close_position"
        assert bundle.metadata["position_address"] == position_addr


class TestPoolResolution:
    """Pool resolution logic."""

    @patch("almanak.framework.connectors.meteora.adapter.MeteoraSDK")
    def test_pool_cache(self, MockSDK):
        """Pool lookups should be cached."""
        mock_sdk = MockSDK.return_value
        mock_sdk.get_pool.return_value = MOCK_POOL

        config = MeteoraConfig(wallet_address=WALLET)
        adapter = MeteoraAdapter(config)

        pool1 = adapter._resolve_pool(MOCK_POOL.address)
        pool2 = adapter._resolve_pool(MOCK_POOL.address)
        assert pool1 is pool2
        mock_sdk.get_pool.assert_called_once()
