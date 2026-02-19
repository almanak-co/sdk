"""Tests for Enso receipt parser.

These tests verify backward compatibility and correctness of the refactored parser.
"""

from almanak.framework.connectors.enso.receipt_parser import EnsoReceiptParser, SwapResult


class TestEnsoReceiptParserBasic:
    """Basic tests for EnsoReceiptParser."""

    def test_parse_swap_receipt_success(self):
        """Test parsing successful swap receipt."""
        parser = EnsoReceiptParser()

        # Mock receipt with Transfer event
        receipt = {
            "transactionHash": "0xabc123",
            "status": 1,
            "gasUsed": 150000,
            "effectiveGasPrice": 50000000000,
            "logs": [
                {
                    "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",  # Transfer
                        "0x000000000000000000000000" + "1234567890" * 4,  # from
                        "0x000000000000000000000000" + "abcdefabcd" * 4,  # to (wallet)
                    ],
                    "data": "0x" + "00" * 31 + "64",  # 100
                }
            ],
        }

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address="0x" + "abcdefabcd" * 4,
            token_out="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        )

        assert result.success is True
        assert result.amount_out == 100
        assert result.tx_hash == "0xabc123"
        assert result.gas_used == 150000

    def test_parse_swap_receipt_failed_transaction(self):
        """Test parsing failed transaction."""
        parser = EnsoReceiptParser()

        receipt = {
            "transactionHash": "0xfailed",
            "status": 0,  # Failed
            "logs": [],
        }

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address="0xwallet",
            token_out="0xtoken",
        )

        assert result.success is False
        assert result.error == "Transaction reverted"
        assert result.tx_hash == "0xfailed"

    def test_parse_swap_receipt_with_token_in(self):
        """Test parsing swap with both token_in and token_out."""
        parser = EnsoReceiptParser()

        wallet_addr = "0x" + "a" * 40
        token_in_addr = "0x" + "1" * 40
        token_out_addr = "0x" + "2" * 40

        receipt = {
            "transactionHash": "0xswap",
            "status": 1,
            "gasUsed": 200000,
            "effectiveGasPrice": 50000000000,
            "logs": [
                # Transfer from wallet (token_in)
                {
                    "address": token_in_addr,
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x000000000000000000000000" + wallet_addr[2:],  # from wallet
                        "0x000000000000000000000000" + "b" * 40,  # to pool
                    ],
                    "data": "0x" + "00" * 31 + "c8",  # 200
                },
                # Transfer to wallet (token_out)
                {
                    "address": token_out_addr,
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x000000000000000000000000" + "b" * 40,  # from pool
                        "0x000000000000000000000000" + wallet_addr[2:],  # to wallet
                    ],
                    "data": "0x" + "00" * 31 + "96",  # 150
                },
            ],
        }

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address=wallet_addr,
            token_out=token_out_addr,
            token_in=token_in_addr,
        )

        assert result.success is True
        assert result.amount_in == 200
        assert result.amount_out == 150
        assert result.token_in == token_in_addr
        assert result.token_out == token_out_addr

    def test_parse_swap_receipt_with_expected_amount(self):
        """Test using expected amount when transfer not found in logs."""
        parser = EnsoReceiptParser()

        receipt = {
            "transactionHash": "0xswap",
            "status": 1,
            "gasUsed": 150000,
            "effectiveGasPrice": 50000000000,
            "logs": [],  # No Transfer events
        }

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address="0xwallet",
            token_out="0xtoken",
            expected_amount_out=1000,
        )

        assert result.success is True
        assert result.amount_out == 1000  # Used expected amount

    def test_parse_swap_receipt_bytes_tx_hash(self):
        """Test parsing receipt with bytes transaction hash."""
        parser = EnsoReceiptParser()

        receipt = {
            "transactionHash": b"\xab\xcd\xef",  # Bytes
            "status": 1,
            "logs": [],
        }

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address="0xwallet",
            token_out="0xtoken",
        )

        assert result.success is True
        assert result.tx_hash.startswith("0x")
        assert "abcdef" in result.tx_hash.lower()


class TestEnsoReceiptParserApproval:
    """Tests for approval receipt parsing."""

    def test_parse_approval_receipt_success(self):
        """Test parsing successful approval."""
        parser = EnsoReceiptParser()

        receipt = {
            "transactionHash": "0xapproval",
            "status": 1,
            "gasUsed": 50000,
            "effectiveGasPrice": 30000000000,
        }

        result = parser.parse_approval_receipt(receipt)

        assert result["success"] is True
        assert result["tx_hash"] == "0xapproval"
        assert result["gas_used"] == 50000
        assert result["error"] is None

    def test_parse_approval_receipt_failed(self):
        """Test parsing failed approval."""
        parser = EnsoReceiptParser()

        receipt = {
            "transactionHash": "0xfailed",
            "status": 0,
        }

        result = parser.parse_approval_receipt(receipt)

        assert result["success"] is False
        assert result["error"] == "Transaction reverted"


class TestEnsoReceiptParserEdgeCases:
    """Tests for edge cases and error handling."""

    def test_extract_transfer_no_matching_token(self):
        """Test that parser returns 0 when token address doesn't match."""
        parser = EnsoReceiptParser()

        receipt = {
            "transactionHash": "0xswap",
            "status": 1,
            "logs": [
                {
                    "address": "0xwrong_token",  # Different token
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x" + "00" * 12 + "1" * 40,
                        "0x" + "00" * 12 + "2" * 40,
                    ],
                    "data": "0x" + "00" * 31 + "64",
                }
            ],
        }

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address="0x" + "2" * 40,
            token_out="0xcorrect_token",  # Looking for different token
        )

        assert result.success is True
        assert result.amount_out == 0  # No matching transfer found

    def test_extract_transfer_wrong_event_signature(self):
        """Test that parser ignores non-Transfer events."""
        parser = EnsoReceiptParser()

        receipt = {
            "transactionHash": "0xswap",
            "status": 1,
            "logs": [
                {
                    "address": "0xtoken",
                    "topics": [
                        "0xother_event_signature",  # Not Transfer
                        "0x" + "00" * 12 + "1" * 40,
                        "0x" + "00" * 12 + "2" * 40,
                    ],
                    "data": "0x" + "00" * 31 + "64",
                }
            ],
        }

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address="0x" + "2" * 40,
            token_out="0xtoken",
        )

        assert result.success is True
        assert result.amount_out == 0  # Event signature didn't match

    def test_extract_transfer_insufficient_topics(self):
        """Test handling of Transfer event with insufficient topics."""
        parser = EnsoReceiptParser()

        receipt = {
            "transactionHash": "0xswap",
            "status": 1,
            "logs": [
                {
                    "address": "0xtoken",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        # Missing from and to topics
                    ],
                    "data": "0x" + "00" * 31 + "64",
                }
            ],
        }

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address="0xwallet",
            token_out="0xtoken",
        )

        assert result.success is True
        assert result.amount_out == 0  # Insufficient topics

    def test_extract_transfer_invalid_data(self):
        """Test handling of invalid hex data."""
        parser = EnsoReceiptParser()

        receipt = {
            "transactionHash": "0xswap",
            "status": 1,
            "logs": [
                {
                    "address": "0xtoken",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x" + "00" * 12 + "1" * 40,
                        "0x" + "00" * 12 + "wallet" + "0" * 30,
                    ],
                    "data": "",  # Empty data
                }
            ],
        }

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address="0x" + "wallet" + "0" * 30,
            token_out="0xtoken",
        )

        assert result.success is True
        assert result.amount_out == 0  # No data to decode


class TestSwapResult:
    """Tests for SwapResult dataclass."""

    def test_swap_result_to_dict(self):
        """Test converting SwapResult to dictionary."""
        result = SwapResult(
            success=True,
            token_in="0xtoken_in",
            token_out="0xtoken_out",
            amount_in=1000,
            amount_out=950,
            tx_hash="0xhash",
            gas_used=150000,
            effective_gas_price=50000000000,
        )

        dict_result = result.to_dict()

        assert dict_result["success"] is True
        assert dict_result["token_in"] == "0xtoken_in"
        assert dict_result["amount_in"] == "1000"  # Converted to string
        assert dict_result["amount_out"] == "950"
        assert dict_result["tx_hash"] == "0xhash"

    def test_swap_result_to_dict_with_error(self):
        """Test converting failed SwapResult to dictionary."""
        result = SwapResult(
            success=False,
            error="Transaction reverted",
        )

        dict_result = result.to_dict()

        assert dict_result["success"] is False
        assert dict_result["error"] == "Transaction reverted"
