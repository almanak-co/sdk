"""Unit tests for Fluid DEX connector — adapter, SDK, receipt parser.

Tests compilation logic, encumbrance guard, receipt parsing, and position details
without requiring Anvil or live RPC. Uses mocked Web3 calls.

To run:
    uv run pytest tests/unit/connectors/test_fluid_connector.py -v
"""

from dataclasses import asdict
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.fluid.adapter import (
    FluidAdapter,
    FluidConfig,
    FluidPositionDetails,
    MAX_INT256,
)
from almanak.framework.connectors.fluid.receipt_parser import (
    ERC721_TRANSFER_TOPIC,
    LOG_OPERATE_TOPIC,
    FluidReceiptParser,
)
from almanak.framework.connectors.fluid.sdk import (
    DEFAULT_GAS_ESTIMATES,
    FLUID_ADDRESSES,
    DexPoolData,
    FluidSDK,
    FluidSDKError,
)


# =============================================================================
# FluidPositionDetails Tests
# =============================================================================


class TestFluidPositionDetails:
    """Tests for the FluidPositionDetails dataclass (pre-implementation requirement #2)."""

    def test_basic_creation(self):
        details = FluidPositionDetails(
            fluid_nft_id="42",
            dex_address="0x1234567890abcdef1234567890abcdef12345678",
            token0="0xWETH",
            token1="0xUSDC",
            swap_fee_apr=5.0,
            lending_yield_apr=3.0,
            combined_apr=8.0,
        )
        assert details.fluid_nft_id == "42"
        assert details.swap_fee_apr == 5.0
        assert details.combined_apr == 8.0
        assert details.is_smart_collateral is False
        assert details.is_smart_debt is False

    def test_nft_id_is_string(self):
        """NFT ID must be string (consistent with Uniswap V3 / TraderJoe patterns)."""
        details = FluidPositionDetails(
            fluid_nft_id="123456",
            dex_address="0x0",
            token0="0x0",
            token1="0x0",
        )
        assert isinstance(details.fluid_nft_id, str)

    def test_asdict_for_position_info(self):
        """FluidPositionDetails must serialize to dict for PositionInfo.details."""
        details = FluidPositionDetails(
            fluid_nft_id="1",
            dex_address="0xPool",
            token0="0xA",
            token1="0xB",
            swap_fee_apr=2.5,
            lending_yield_apr=1.5,
            combined_apr=4.0,
            is_smart_collateral=False,
            is_smart_debt=False,
        )
        d = asdict(details)
        assert d["fluid_nft_id"] == "1"
        assert d["combined_apr"] == 4.0
        assert "is_smart_debt" in d

    def test_encumbered_position(self):
        """Encumbered positions have smart collateral or debt flags set."""
        details = FluidPositionDetails(
            fluid_nft_id="99",
            dex_address="0xPool",
            token0="0xA",
            token1="0xB",
            is_smart_collateral=True,
            is_smart_debt=True,
        )
        assert details.is_smart_collateral is True
        assert details.is_smart_debt is True


# =============================================================================
# FluidSDK Tests
# =============================================================================


class TestFluidSDK:
    """Tests for the FluidSDK low-level contract interactions."""

    def test_unsupported_chain_raises(self):
        """SDK rejects non-Arbitrum chains in phase 1."""
        with pytest.raises(FluidSDKError, match="not supported"):
            FluidSDK(chain="ethereum", rpc_url="http://localhost:8545")

    def test_addresses_registered(self):
        """Arbitrum addresses are present in the address registry."""
        assert "arbitrum" in FLUID_ADDRESSES
        arb = FLUID_ADDRESSES["arbitrum"]
        assert "dex_factory" in arb
        assert "dex_resolver" in arb
        assert arb["dex_factory"].startswith("0x")

    def test_gas_estimates_defined(self):
        """Gas estimates are defined for all operations."""
        assert "approve" in DEFAULT_GAS_ESTIMATES
        assert "operate_open" in DEFAULT_GAS_ESTIMATES
        assert "operate_close" in DEFAULT_GAS_ESTIMATES

    @patch("almanak.framework.connectors.fluid.sdk.Web3")
    def test_build_operate_tx_rejects_debt(self, mock_web3_cls):
        """Phase 1 rejects operate() calls with non-zero debt."""
        mock_web3_cls.return_value = MagicMock()
        mock_web3_cls.to_checksum_address = lambda x: x

        sdk = FluidSDK.__new__(FluidSDK)
        sdk.chain = "arbitrum"
        sdk.rpc_url = "http://localhost:8545"
        sdk.w3 = mock_web3_cls.return_value
        sdk._addresses = FLUID_ADDRESSES["arbitrum"]

        with pytest.raises(FluidSDKError, match="smart-debt"):
            sdk.build_operate_tx(
                dex_address="0x1234",
                nft_id=0,
                new_col=1000,
                new_debt=500,  # Non-zero debt — rejected
                to="0xWallet",
            )


# =============================================================================
# FluidAdapter Tests
# =============================================================================


class TestFluidAdapter:
    """Tests for the FluidAdapter high-level LP operations."""

    def _make_adapter(self, mock_sdk=None, mock_resolver=None):
        """Create a FluidAdapter with mocked dependencies."""
        with patch("almanak.framework.connectors.fluid.adapter.FluidSDK") as mock_sdk_cls:
            mock_sdk_instance = mock_sdk or MagicMock()
            mock_sdk_cls.return_value = mock_sdk_instance

            config = FluidConfig(
                chain="arbitrum",
                wallet_address="0x742d35Cc6634C0532925a3b844Bc9e7595f2bD60",
                rpc_url="http://localhost:8545",
            )

            if mock_resolver:
                adapter = FluidAdapter(config, token_resolver=mock_resolver)
            else:
                with patch("almanak.framework.data.tokens.get_token_resolver") as mock_get_resolver:
                    mock_get_resolver.return_value = MagicMock()
                    adapter = FluidAdapter(config)

            adapter._sdk = mock_sdk_instance
            return adapter

    def test_non_arbitrum_chain_rejected(self):
        """Phase 1 only supports Arbitrum."""
        with patch("almanak.framework.connectors.fluid.adapter.FluidSDK"):
            with pytest.raises(FluidSDKError, match="Arbitrum only"):
                FluidAdapter(
                    FluidConfig(chain="ethereum", wallet_address="0x0", rpc_url="http://localhost"),
                )

    def test_lp_close_builds_operate(self):
        """LP_CLOSE builds operate() transaction with encumbrance guard."""
        mock_sdk = MagicMock()
        mock_sdk.is_position_encumbered.return_value = False
        mock_sdk.build_operate_tx.return_value = {
            "to": "0xPool",
            "data": "0x1234",
            "value": 0,
            "gas": 250000,
        }

        adapter = self._make_adapter(mock_sdk=mock_sdk)
        tx = adapter.build_remove_liquidity_transaction(
            dex_address="0xPool",
            nft_id=42,
        )

        assert tx.tx_type == "fluid_operate_close"
        mock_sdk.build_operate_tx.assert_called_once_with(
            dex_address="0xPool",
            nft_id=42,
            new_col=-MAX_INT256,
            new_debt=0,
            to=adapter.config.wallet_address,
        )

    def test_lp_open_raises_not_supported(self):
        """LP_OPEN raises FluidSDKError — deposit not supported in phase 1."""
        adapter = self._make_adapter()

        with pytest.raises(FluidSDKError, match="not yet supported"):
            adapter.build_add_liquidity_transaction(
                dex_address="0xPool",
                amount0=Decimal("1.0"),
                amount1=Decimal("3000"),
                token0_decimals=18,
                token1_decimals=6,
            )

    def test_get_position_details(self):
        """Position details populated from resolver data."""
        mock_sdk = MagicMock()
        mock_sdk.get_dex_data.return_value = DexPoolData(
            dex_address="0xPool",
            token0="0xWETH",
            token1="0xUSDC",
            fee_bps=30,
            is_smart_collateral=False,
            is_smart_debt=False,
        )

        adapter = self._make_adapter(mock_sdk=mock_sdk)
        details = adapter.get_position_details(nft_id=42, dex_address="0xPool")

        assert details.fluid_nft_id == "42"
        assert details.dex_address == "0xPool"
        assert details.token0 == "0xWETH"
        assert details.is_smart_collateral is False
        assert details.swap_fee_apr > 0


# =============================================================================
# FluidReceiptParser Tests
# =============================================================================


class TestFluidReceiptParser:
    """Tests for the FluidReceiptParser."""

    def _make_receipt(
        self,
        status=1,
        nft_id=42,
        token0_amt=10**18,
        token1_amt=3000 * 10**6,
        tx_hash="0xabc123",
    ):
        """Build a mock receipt with a LogOperate event."""
        # Encode nft_id as indexed topic
        nft_topic = "0x" + hex(nft_id)[2:].zfill(64)

        # Encode data: token0_amt (int256) + token1_amt (int256) + timestamp (uint256)
        def encode_int256(val):
            if val >= 0:
                return hex(val)[2:].zfill(64)
            # Two's complement for negative
            return hex(val & (2**256 - 1))[2:].zfill(64)

        data = "0x" + encode_int256(token0_amt) + encode_int256(token1_amt) + hex(1700000000)[2:].zfill(64)

        return {
            "transactionHash": tx_hash,
            "blockNumber": 100,
            "status": status,
            "logs": [
                {
                    "topics": [LOG_OPERATE_TOPIC, nft_topic],
                    "data": data,
                    "address": "0xPool",
                },
            ],
        }

    def test_parse_lp_open_receipt(self):
        """Extract NFT ID and deposit amounts from LP_OPEN receipt."""
        parser = FluidReceiptParser()
        receipt = self._make_receipt(nft_id=42, token0_amt=10**18, token1_amt=3000 * 10**6)

        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert result.nft_id == 42
        assert result.token0_amt == 10**18
        assert result.token1_amt == 3000 * 10**6

    def test_extract_position_id(self):
        """ResultEnricher calls extract_position_id for LP_OPEN."""
        parser = FluidReceiptParser()
        receipt = self._make_receipt(nft_id=99)

        position_id = parser.extract_position_id(receipt)
        assert position_id == 99

    def test_extract_lp_close_data(self):
        """ResultEnricher calls extract_lp_close_data for LP_CLOSE."""
        parser = FluidReceiptParser()
        # Negative amounts = withdrawal
        receipt = self._make_receipt(
            nft_id=42,
            token0_amt=-(10**18),
            token1_amt=-(3000 * 10**6),
        )

        close_data = parser.extract_lp_close_data(receipt)
        assert close_data is not None
        assert close_data.amount0_collected == 10**18
        assert close_data.amount1_collected == 3000 * 10**6

    def test_reverted_tx_returns_failure(self):
        """Reverted transactions return failure result."""
        parser = FluidReceiptParser()
        receipt = self._make_receipt(status=0)

        result = parser.parse_receipt(receipt)
        assert result.success is False
        assert "reverted" in result.error.lower()

    def test_empty_logs_returns_failure(self):
        """Receipt with no relevant logs returns failure."""
        parser = FluidReceiptParser()
        receipt = {"transactionHash": "0x123", "blockNumber": 1, "status": 1, "logs": []}

        result = parser.parse_receipt(receipt)
        assert result.success is False

    def test_erc721_transfer_fallback(self):
        """NFT ID extracted from ERC-721 Transfer event as fallback."""
        parser = FluidReceiptParser()
        nft_id = 55

        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 1,
            "status": 1,
            "logs": [
                {
                    "topics": [
                        ERC721_TRANSFER_TOPIC,
                        "0x" + "0" * 64,  # from = zero (mint)
                        "0x" + "0" * 24 + "abcdef1234567890abcd",  # to
                        "0x" + hex(nft_id)[2:].zfill(64),  # tokenId
                    ],
                    "data": "0x",
                    "address": "0xPool",
                },
            ],
        }

        position_id = parser.extract_position_id(receipt)
        assert position_id == nft_id

    def test_supported_extractions(self):
        """Parser declares supported extraction fields."""
        assert "position_id" in FluidReceiptParser.SUPPORTED_EXTRACTIONS
        assert "lp_close_data" in FluidReceiptParser.SUPPORTED_EXTRACTIONS


# =============================================================================
# Compiler Integration Tests
# =============================================================================


class TestFluidCompilerIntegration:
    """Tests for Fluid routing in the IntentCompiler."""

    def test_fluid_in_lp_position_managers(self):
        """Fluid is registered in LP_POSITION_MANAGERS for Arbitrum."""
        from almanak.framework.intents.compiler import LP_POSITION_MANAGERS

        assert "fluid" in LP_POSITION_MANAGERS.get("arbitrum", {})

    def test_receipt_registry_has_fluid(self):
        """Fluid is registered in the receipt parser registry."""
        from almanak.framework.execution.receipt_registry import ReceiptParserRegistry

        registry = ReceiptParserRegistry()
        parser = registry.get("fluid")
        assert parser is not None
        assert isinstance(parser, FluidReceiptParser)
