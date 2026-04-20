"""Unit tests for LiFi Receipt Parser."""

import pytest

from almanak.framework.connectors.lifi.receipt_parser import (
    TRANSFER_EVENT_SIGNATURE,
    LiFiReceiptParser,
    LiFiSwapResult,
)


# ============================================================================
# Fixtures
# ============================================================================


WALLET_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5eDb3A432268e5831"
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
LIFI_DIAMOND = "0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE"


@pytest.fixture
def parser():
    """Create parser instance."""
    return LiFiReceiptParser()


def _pad_address(addr: str) -> str:
    """Pad address to 32 bytes topic format."""
    return "0x" + addr.lower().replace("0x", "").zfill(64)


def _encode_uint256(value: int) -> str:
    """Encode uint256 to hex data."""
    return "0x" + hex(value)[2:].zfill(64)


def _make_transfer_log(token, from_addr, to_addr, amount, log_index=0):
    """Create a mock ERC-20 Transfer log."""
    return {
        "address": token,
        "topics": [
            TRANSFER_EVENT_SIGNATURE,
            _pad_address(from_addr),
            _pad_address(to_addr),
        ],
        "data": _encode_uint256(amount),
        "logIndex": hex(log_index),
    }


def _make_receipt(status=1, logs=None, tx_hash="0xabc123def456", gas_used=150000):
    """Create a mock transaction receipt."""
    return {
        "transactionHash": tx_hash,
        "status": status,
        "logs": logs or [],
        "gasUsed": gas_used,
        "effectiveGasPrice": 100000000,
        "blockNumber": 12345678,
    }


# ============================================================================
# Basic Parsing Tests
# ============================================================================


class TestBasicParsing:
    """Test basic receipt parsing."""

    def test_parse_successful_swap(self, parser):
        """Parse a successful same-chain swap receipt."""
        logs = [
            # USDC sent from wallet to LiFi Diamond
            _make_transfer_log(USDC_ADDRESS, WALLET_ADDRESS, LIFI_DIAMOND, 1000_000000, 0),
            # WETH received by wallet from router
            _make_transfer_log(WETH_ADDRESS, LIFI_DIAMOND, WALLET_ADDRESS, 500000000000000000, 1),
        ]
        receipt = _make_receipt(logs=logs)

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address=WALLET_ADDRESS,
            token_out=WETH_ADDRESS,
            token_in=USDC_ADDRESS,
        )

        assert result.success is True
        assert result.amount_in == 1000_000000  # 1000 USDC
        assert result.amount_out == 500000000000000000  # 0.5 WETH
        assert result.tx_hash == "0xabc123def456"
        assert result.gas_used == 150000

    def test_parse_failed_transaction(self, parser):
        """Parse a failed (reverted) transaction."""
        receipt = _make_receipt(status=0)

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address=WALLET_ADDRESS,
            token_out=WETH_ADDRESS,
        )

        assert result.success is False
        assert result.error == "Transaction reverted"

    def test_parse_empty_logs(self, parser):
        """Parse receipt with no logs extracts zero amounts."""
        receipt = _make_receipt(logs=[])

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address=WALLET_ADDRESS,
            token_out=WETH_ADDRESS,
        )

        assert result.success is True
        assert result.amount_out == 0

    def test_parse_with_expected_amount_fallback(self, parser):
        """Falls back to expected amount when logs have no match."""
        receipt = _make_receipt(logs=[])

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address=WALLET_ADDRESS,
            token_out=WETH_ADDRESS,
            expected_amount_out=12345,
        )

        assert result.success is True
        assert result.amount_out == 12345


# ============================================================================
# Cross-Chain Parsing Tests
# ============================================================================


class TestCrossChainParsing:
    """Test cross-chain bridge receipt parsing."""

    def test_parse_bridge_receipt(self, parser):
        """Parse a cross-chain bridge receipt (source chain)."""
        logs = [
            # USDC sent from wallet to bridge contract
            _make_transfer_log(USDC_ADDRESS, WALLET_ADDRESS, LIFI_DIAMOND, 1000_000000, 0),
        ]
        receipt = _make_receipt(logs=logs)

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address=WALLET_ADDRESS,
            token_out=USDC_ADDRESS,
            token_in=USDC_ADDRESS,
            tool="across",
            is_cross_chain=True,
        )

        assert result.success is True
        assert result.amount_in == 1000_000000
        # Bridge fallback: from_address=wallet captures amount sent to bridge
        assert result.amount_out == 1000_000000
        assert result.is_cross_chain is True
        assert result.tool == "across"

    def test_bridge_amount_out_fallback_different_tokens(self, parser):
        """Bridge with different token_in/token_out: amount_out falls back to expected."""
        logs = [
            # USDC sent from wallet to bridge (but token_out is WETH on dest chain)
            _make_transfer_log(USDC_ADDRESS, WALLET_ADDRESS, LIFI_DIAMOND, 1000_000000, 0),
        ]
        receipt = _make_receipt(logs=logs)

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address=WALLET_ADDRESS,
            token_out=WETH_ADDRESS,  # Different from token_in - on destination chain
            token_in=USDC_ADDRESS,
            expected_amount_out=500000000000000000,  # Expected WETH amount
            tool="stargate",
            is_cross_chain=True,
        )

        assert result.success is True
        assert result.amount_in == 1000_000000
        # Can't extract WETH amount from source chain, uses expected
        assert result.amount_out == 500000000000000000
        assert result.is_cross_chain is True

    def test_cross_chain_metadata(self, parser):
        """Cross-chain result includes tool and flag."""
        receipt = _make_receipt(logs=[])

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address=WALLET_ADDRESS,
            token_out=USDC_ADDRESS,
            tool="stargate",
            is_cross_chain=True,
        )

        assert result.is_cross_chain is True
        assert result.tool == "stargate"


# ============================================================================
# Transfer Extraction Tests
# ============================================================================


class TestTransferExtraction:
    """Test ERC-20 Transfer event extraction."""

    def test_extract_by_from_address(self, parser):
        """Extract transfer amount by sender."""
        logs = [
            _make_transfer_log(USDC_ADDRESS, WALLET_ADDRESS, LIFI_DIAMOND, 500_000000, 0),
            _make_transfer_log(USDC_ADDRESS, "0xother", LIFI_DIAMOND, 700_000000, 1),
        ]

        amount = parser._extract_transfer_amount(
            logs=logs,
            token_address=USDC_ADDRESS,
            from_address=WALLET_ADDRESS,
        )

        assert amount == 500_000000

    def test_extract_by_to_address(self, parser):
        """Extract transfer amount by recipient."""
        logs = [
            _make_transfer_log(WETH_ADDRESS, LIFI_DIAMOND, "0xother", 300000000000000000, 0),
            _make_transfer_log(WETH_ADDRESS, LIFI_DIAMOND, WALLET_ADDRESS, 500000000000000000, 1),
        ]

        amount = parser._extract_transfer_amount(
            logs=logs,
            token_address=WETH_ADDRESS,
            to_address=WALLET_ADDRESS,
        )

        assert amount == 500000000000000000

    def test_extract_ignores_other_tokens(self, parser):
        """Extraction ignores transfers of other tokens."""
        logs = [
            _make_transfer_log("0xOtherToken", WALLET_ADDRESS, LIFI_DIAMOND, 999_000000, 0),
            _make_transfer_log(USDC_ADDRESS, WALLET_ADDRESS, LIFI_DIAMOND, 100_000000, 1),
        ]

        amount = parser._extract_transfer_amount(
            logs=logs,
            token_address=USDC_ADDRESS,
            from_address=WALLET_ADDRESS,
        )

        assert amount == 100_000000

    def test_extract_sums_multiple_transfers(self, parser):
        """Extraction sums multiple matching Transfer events (split routes)."""
        logs = [
            _make_transfer_log(USDC_ADDRESS, WALLET_ADDRESS, LIFI_DIAMOND, 300_000000, 0),
            _make_transfer_log(USDC_ADDRESS, WALLET_ADDRESS, LIFI_DIAMOND, 700_000000, 1),
        ]

        amount = parser._extract_transfer_amount(
            logs=logs,
            token_address=USDC_ADDRESS,
            from_address=WALLET_ADDRESS,
        )

        assert amount == 1000_000000  # Sum of both transfers

    def test_extract_ignores_non_transfer_events(self, parser):
        """Extraction ignores non-Transfer events."""
        logs = [
            {
                "address": USDC_ADDRESS,
                "topics": [
                    "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",  # Approval
                    _pad_address(WALLET_ADDRESS),
                    _pad_address(LIFI_DIAMOND),
                ],
                "data": _encode_uint256(1000000),
            },
        ]

        amount = parser._extract_transfer_amount(
            logs=logs,
            token_address=USDC_ADDRESS,
            from_address=WALLET_ADDRESS,
        )

        assert amount == 0

    def test_extract_handles_bytes_address(self, parser):
        """Extraction handles bytes-type log addresses."""
        logs = [
            {
                "address": bytes.fromhex(USDC_ADDRESS[2:]),  # bytes instead of string
                "topics": [
                    TRANSFER_EVENT_SIGNATURE,
                    _pad_address(WALLET_ADDRESS),
                    _pad_address(LIFI_DIAMOND),
                ],
                "data": _encode_uint256(250_000000),
            },
        ]

        amount = parser._extract_transfer_amount(
            logs=logs,
            token_address=USDC_ADDRESS,
            from_address=WALLET_ADDRESS,
        )

        assert amount == 250_000000

    def test_extract_handles_bytes_topic(self, parser):
        """Extraction handles bytes-type topics."""
        logs = [
            {
                "address": USDC_ADDRESS,
                "topics": [
                    bytes.fromhex(TRANSFER_EVENT_SIGNATURE[2:]),
                    _pad_address(WALLET_ADDRESS),
                    _pad_address(LIFI_DIAMOND),
                ],
                "data": _encode_uint256(750_000000),
            },
        ]

        amount = parser._extract_transfer_amount(
            logs=logs,
            token_address=USDC_ADDRESS,
            from_address=WALLET_ADDRESS,
        )

        assert amount == 750_000000


# ============================================================================
# Extraction Method Tests (Result Enrichment)
# ============================================================================


class TestExtractionMethods:
    """Test extraction methods for Result Enrichment system."""

    def test_extract_swap_amounts(self, parser):
        """Extract swap amounts for Result Enrichment."""
        logs = [
            _make_transfer_log(USDC_ADDRESS, WALLET_ADDRESS, LIFI_DIAMOND, 1000_000000, 0),
            _make_transfer_log(WETH_ADDRESS, LIFI_DIAMOND, WALLET_ADDRESS, 500000000000000000, 1),
        ]
        receipt = _make_receipt(logs=logs)

        result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_in == 1000_000000
        assert result.amount_out == 500000000000000000
        assert result.token_in == USDC_ADDRESS.lower()
        assert result.token_out == WETH_ADDRESS.lower()

    def test_extract_swap_amounts_failed_receipt(self, parser):
        """Extract returns None for failed receipt."""
        receipt = _make_receipt(status=0)

        result = parser.extract_swap_amounts(receipt)
        assert result is None

    def test_extract_swap_amounts_no_transfers(self, parser):
        """Extract returns None when no transfers found."""
        receipt = _make_receipt(logs=[])

        result = parser.extract_swap_amounts(receipt)
        assert result is None

    def test_extract_position_id_returns_none(self, parser):
        """LiFi does not create LP positions."""
        receipt = _make_receipt(logs=[])
        assert parser.extract_position_id(receipt) is None

    def test_extract_liquidity_returns_none(self, parser):
        """LiFi does not provide liquidity events."""
        receipt = _make_receipt(logs=[])
        assert parser.extract_liquidity(receipt) is None

    def test_extract_lp_close_data_returns_none(self, parser):
        """LiFi does not close LP positions."""
        receipt = _make_receipt(logs=[])
        assert parser.extract_lp_close_data(receipt) is None


# ============================================================================
# Approval Receipt Tests
# ============================================================================


class TestApprovalReceipt:
    """Test approval receipt parsing."""

    def test_parse_successful_approval(self, parser):
        """Parse successful approval receipt."""
        receipt = _make_receipt(status=1)

        result = parser.parse_approval_receipt(receipt)

        assert result["success"] is True
        assert result["error"] is None

    def test_parse_failed_approval(self, parser):
        """Parse failed approval receipt."""
        receipt = _make_receipt(status=0)

        result = parser.parse_approval_receipt(receipt)

        assert result["success"] is False
        assert result["error"] == "Transaction reverted"


# ============================================================================
# Serialization Tests
# ============================================================================


class TestSerialization:
    """Test result serialization."""

    def test_swap_result_to_dict(self):
        """LiFiSwapResult serializes correctly."""
        result = LiFiSwapResult(
            success=True,
            token_in=USDC_ADDRESS,
            token_out=WETH_ADDRESS,
            amount_in=1000000000,
            amount_out=500000000000000000,
            tx_hash="0xabc123",
            gas_used=150000,
            effective_gas_price=100000000,
            tool="across",
            is_cross_chain=True,
        )

        d = result.to_dict()
        assert d["success"] is True
        assert d["amount_in"] == "1000000000"
        assert d["amount_out"] == "500000000000000000"
        assert d["tool"] == "across"
        assert d["is_cross_chain"] is True

    def test_normalize_bytes_tx_hash(self, parser):
        """Normalize bytes transaction hash."""
        result = parser._normalize_tx_hash(bytes.fromhex("abc123"))
        assert result.startswith("0x")

    def test_normalize_string_tx_hash(self, parser):
        """Normalize string transaction hash."""
        result = parser._normalize_tx_hash("0xabc123")
        assert result == "0xabc123"

    def test_normalize_none_tx_hash(self, parser):
        """Normalize None transaction hash."""
        result = parser._normalize_tx_hash(None)
        assert result == ""
