"""Tests for OrcaAdapter intent compilation (VIB-386).

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

from almanak.framework.connectors.orca.adapter import OrcaAdapter, OrcaConfig
from almanak.framework.connectors.orca.models import OrcaPool, OrcaPosition
from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent

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


class TestOrcaConfig:
    """OrcaConfig validation."""

    def test_valid_config(self):
        config = OrcaConfig(wallet_address=WALLET)
        assert config.wallet_address == WALLET
        assert config.rpc_url == ""

    def test_empty_wallet_raises(self):
        with pytest.raises(ValueError, match="wallet_address"):
            OrcaConfig(wallet_address="")


class TestLPOpenCompilation:
    """OrcaAdapter.compile_lp_open_intent tests."""

    @patch("almanak.framework.connectors.orca.adapter.OrcaWhirlpoolSDK")
    def test_compile_lp_open_success(self, MockSDK):
        """Successful LP open compilation returns valid ActionBundle."""
        nft_mint_kp = Keypair()
        mock_sdk = MockSDK.return_value
        mock_sdk.get_pool_info.return_value = MOCK_POOL
        mock_sdk.build_open_position_transaction.return_value = (
            [],  # Empty instructions for mock
            nft_mint_kp,
            {
                "tick_lower": -640,
                "tick_upper": 640,
                "liquidity": "1000000",
                "amount_a_max": 1_010_000_000,
                "amount_b_max": 151_500_000,
                "nft_mint": str(nft_mint_kp.pubkey()),
                "slippage_bps": 100,
            },
        )

        # Mock the MessageV0 compilation
        with patch("almanak.framework.connectors.orca.adapter.MessageV0") as MockMsg:
            mock_msg = MagicMock()
            mock_msg.header.num_required_signatures = 2
            MockMsg.try_compile.return_value = mock_msg

            with patch("almanak.framework.connectors.orca.adapter.VersionedTransaction") as MockTx:
                mock_tx = MagicMock()
                mock_tx.__bytes__ = MagicMock(return_value=b"\x00" * 200)
                MockTx.populate.return_value = mock_tx

                config = OrcaConfig(wallet_address=WALLET)
                adapter = OrcaAdapter(config=config)
                adapter.sdk = mock_sdk

                intent = LPOpenIntent(
                    protocol="orca_whirlpools",
                    pool=MOCK_POOL.address,
                    amount0=Decimal("1"),
                    amount1=Decimal("150"),
                    range_lower=Decimal("100"),
                    range_upper=Decimal("200"),
                )

                bundle = adapter.compile_lp_open_intent(intent)

                assert bundle.metadata.get("error") is None
                assert bundle.metadata["protocol"] == "orca_whirlpools"
                assert bundle.metadata["chain_family"] == "SOLANA"
                assert bundle.metadata["action"] == "open_position"
                assert bundle.metadata["nft_mint"] == str(nft_mint_kp.pubkey())
                assert bundle.transactions

    @patch("almanak.framework.connectors.orca.adapter.OrcaWhirlpoolSDK")
    def test_compile_lp_open_error_returns_error_bundle(self, MockSDK):
        """SDK error returns error ActionBundle."""
        mock_sdk = MockSDK.return_value
        mock_sdk.get_pool_info.side_effect = Exception("API unreachable")

        config = OrcaConfig(wallet_address=WALLET)
        adapter = OrcaAdapter(config=config)
        adapter.sdk = mock_sdk

        intent = LPOpenIntent(
            protocol="orca_whirlpools",
            pool="invalid-pool",
            amount0=Decimal("1"),
            amount1=Decimal("150"),
            range_lower=Decimal("100"),
            range_upper=Decimal("200"),
        )

        bundle = adapter.compile_lp_open_intent(intent)
        assert "error" in bundle.metadata
        assert "API unreachable" in bundle.metadata["error"]


class TestLPCloseCompilation:
    """OrcaAdapter.compile_lp_close_intent tests."""

    def test_compile_lp_close_no_pool_returns_error(self):
        """LPCloseIntent without pool returns error."""
        config = OrcaConfig(wallet_address=WALLET)
        adapter = OrcaAdapter(config=config)

        intent = LPCloseIntent(
            protocol="orca_whirlpools",
            pool="",
            position_id="fake_nft_mint_address_xxxxxxxxxxxxxxxxxxxxxxxx",
        )

        bundle = adapter.compile_lp_close_intent(intent)
        assert "error" in bundle.metadata
        assert "pool" in bundle.metadata["error"].lower()

    @patch("almanak.framework.connectors.orca.adapter.OrcaWhirlpoolSDK")
    def test_compile_lp_close_success_no_rpc(self, MockSDK):
        """LP close without RPC URL uses zero-liquidity position."""
        mock_sdk = MockSDK.return_value
        mock_sdk.get_pool_info.return_value = MOCK_POOL
        mock_sdk.build_close_position_transaction.return_value = (
            [MagicMock()],  # Fake instruction list
            {"nft_mint": "fake_nft", "pool": MOCK_POOL.address, "liquidity_removed": "0"},
        )

        with patch("almanak.framework.connectors.orca.adapter.MessageV0") as MockMsg:
            mock_msg = MagicMock()
            mock_msg.header.num_required_signatures = 1
            MockMsg.try_compile.return_value = mock_msg

            with patch("almanak.framework.connectors.orca.adapter.VersionedTransaction") as MockTx:
                mock_tx = MagicMock()
                mock_tx.__bytes__ = MagicMock(return_value=b"\x00" * 100)
                MockTx.populate.return_value = mock_tx

                config = OrcaConfig(wallet_address=WALLET)
                adapter = OrcaAdapter(config=config)
                adapter.sdk = mock_sdk

                intent = LPCloseIntent(
                    protocol="orca_whirlpools",
                    pool=MOCK_POOL.address,
                    position_id="fake_nft_mint_xxxxxxxxxxxxxxxxxxxxxxxxx",
                )

                bundle = adapter.compile_lp_close_intent(intent)
                assert bundle.metadata.get("error") is None
                assert bundle.metadata["protocol"] == "orca_whirlpools"
                assert bundle.metadata["action"] == "close_position"
