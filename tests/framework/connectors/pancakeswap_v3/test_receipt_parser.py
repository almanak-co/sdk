"""Tests for PancakeSwap V3 receipt parser (refactored version)."""

from almanak.framework.connectors.pancakeswap_v3.receipt_parser import (
    EVENT_TOPICS,
    PancakeSwapV3EventType,
    PancakeSwapV3ReceiptParser,
    ParseResult,
    SwapEventData,
)


class TestPancakeSwapV3ReceiptParserBasic:
    """Basic tests for PancakeSwapV3ReceiptParser."""

    def test_parse_receipt_with_swap(self):
        """Test parsing receipt with Swap event."""
        parser = PancakeSwapV3ReceiptParser()

        receipt = {
            "transactionHash": "0xswap123",
            "blockNumber": 12345,
            "status": 1,
            "logs": [
                {
                    "address": "0xpool_address",
                    "logIndex": 5,
                    "topics": [
                        EVENT_TOPICS["Swap"],
                        "0x000000000000000000000000" + "a" * 40,  # sender
                        "0x000000000000000000000000" + "b" * 40,  # recipient
                    ],
                    "data": (
                        "0x"
                        + "00" * 31
                        + "64"  # amount0 = 100
                        + "ff" * 31
                        + "9c"  # amount1 = -100 (two's complement)
                        + "00" * 12
                        + "01"
                        + "00" * 19  # sqrtPriceX96
                        + "00" * 16
                        + "02"
                        + "00" * 15  # liquidity
                        + "00" * 31
                        + "0a"  # tick = 10
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.swaps) == 1
        assert result.swaps[0].amount0 == 100
        assert result.swaps[0].amount1 == -100
        assert result.swaps[0].tick == 10
        assert result.transaction_hash == "0xswap123"

    def test_parse_receipt_empty_logs(self):
        """Test parsing receipt with no logs."""
        parser = PancakeSwapV3ReceiptParser()

        receipt = {
            "transactionHash": "0xempty",
            "blockNumber": 123,
            "status": 1,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.swaps) == 0
        assert result.transaction_hash == "0xempty"

    def test_parse_receipt_failed_transaction(self):
        """Test parsing failed transaction."""
        parser = PancakeSwapV3ReceiptParser()

        receipt = {
            "transactionHash": "0xfailed",
            "blockNumber": 123,
            "status": 0,  # Failed
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is False
        assert result.error is not None

    def test_parse_receipt_filters_unknown_events(self):
        """Test that unknown events are filtered out."""
        parser = PancakeSwapV3ReceiptParser()

        receipt = {
            "transactionHash": "0xswap",
            "blockNumber": 123,
            "status": 1,
            "logs": [
                {
                    "address": "0xpool",
                    "topics": [EVENT_TOPICS["Swap"]],
                    "data": "0x" + "00" * 160,
                },
                {
                    "address": "0xpool",
                    "topics": ["0xunknown_event"],  # Unknown
                    "data": "0x" + "00" * 160,
                },
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.swaps) == 1  # Only Swap event


class TestPancakeSwapV3SwapEventData:
    """Tests for SwapEventData."""

    def test_token0_in_property(self):
        """Test token0_in property."""
        swap = SwapEventData(
            pool="0xpool",
            sender="0xsender",
            recipient="0xrecipient",
            amount0=100,  # Positive = input
            amount1=-95,
        )

        assert swap.token0_in is True
        assert swap.token1_in is False

    def test_token1_in_property(self):
        """Test token1_in property."""
        swap = SwapEventData(
            pool="0xpool",
            sender="0xsender",
            recipient="0xrecipient",
            amount0=-95,
            amount1=100,  # Positive = input
        )

        assert swap.token0_in is False
        assert swap.token1_in is True

    def test_to_dict(self):
        """Test converting to dictionary."""
        swap = SwapEventData(
            pool="0xpool",
            sender="0xsender",
            recipient="0xrecipient",
            amount0=100,
            amount1=-95,
            sqrt_price_x96=1000,
            liquidity=5000,
            tick=10,
        )

        result = swap.to_dict()

        assert result["pool"] == "0xpool"
        assert result["amount0"] == "100"
        assert result["amount1"] == "-95"
        assert result["token0_in"] is True


class TestPancakeSwapV3BackwardCompatibility:
    """Tests for backward compatibility methods."""

    def test_parse_swap_method(self):
        """Test backward compatible parse_swap method."""
        parser = PancakeSwapV3ReceiptParser()

        log = {
            "address": "0xpool",
            "logIndex": 5,
            "topics": [
                EVENT_TOPICS["Swap"],
                "0x000000000000000000000000" + "a" * 40,
                "0x000000000000000000000000" + "b" * 40,
            ],
            "data": (
                "0x"
                + "00" * 31
                + "64"  # amount0 = 100
                + "ff" * 31
                + "9c"  # amount1 = -100
                + "00" * 32  # sqrtPriceX96
                + "00" * 32  # liquidity
                + "00" * 32  # tick
            ),
        }

        result = parser.parse_swap(log)

        assert result is not None
        assert result.amount0 == 100
        assert result.amount1 == -100

    def test_is_pancakeswap_event(self):
        """Test is_pancakeswap_event method."""
        parser = PancakeSwapV3ReceiptParser()

        assert parser.is_pancakeswap_event(EVENT_TOPICS["Swap"]) is True
        assert parser.is_pancakeswap_event("0xunknown") is False

    def test_get_event_type(self):
        """Test get_event_type method."""
        parser = PancakeSwapV3ReceiptParser()

        event_type = parser.get_event_type(EVENT_TOPICS["Swap"])
        assert event_type == PancakeSwapV3EventType.SWAP

        unknown_type = parser.get_event_type("0xunknown")
        assert unknown_type == PancakeSwapV3EventType.UNKNOWN


class TestPancakeSwapV3ParseResult:
    """Tests for ParseResult."""

    def test_parse_result_to_dict(self):
        """Test converting ParseResult to dictionary."""
        swap = SwapEventData(
            pool="0xpool",
            sender="0xsender",
            recipient="0xrecipient",
            amount0=100,
            amount1=-95,
        )

        result = ParseResult(
            success=True,
            swaps=[swap],
            transaction_hash="0xhash",
            block_number=12345,
        )

        dict_result = result.to_dict()

        assert dict_result["success"] is True
        assert len(dict_result["swaps"]) == 1
        assert dict_result["transaction_hash"] == "0xhash"
        assert dict_result["block_number"] == 12345

    def test_parse_result_failed(self):
        """Test failed ParseResult."""
        result = ParseResult(
            success=False,
            error="Transaction reverted",
            transaction_hash="0xfailed",
            block_number=0,
        )

        dict_result = result.to_dict()

        assert dict_result["success"] is False
        assert dict_result["error"] == "Transaction reverted"


class TestPancakeSwapV3ExtractSwapAmounts:
    """Tests for extract_swap_amounts using Transfer events + token resolver."""

    def _make_transfer_log(self, token_addr: str, from_addr: str, to_addr: str, amount: int) -> dict:
        """Helper to build a Transfer log entry."""
        return {
            "address": token_addr,
            "topics": [
                EVENT_TOPICS["Transfer"],
                "0x000000000000000000000000" + from_addr.lower().replace("0x", ""),
                "0x000000000000000000000000" + to_addr.lower().replace("0x", ""),
            ],
            "data": "0x" + hex(amount)[2:].zfill(64),
        }

    def test_extract_swap_amounts_uses_correct_decimals(self):
        """Test that USDC (6 decimals) is scaled correctly, not divided by 1e18."""
        from unittest.mock import MagicMock, patch

        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        wallet = "0x" + "ab" * 20
        usdc_addr = "0x" + "cc" * 20
        weth_addr = "0x" + "dd" * 20

        # Wallet sends 1 WETH (18 decimals), receives 5 USDC (6 decimals)
        receipt = {
            "from": wallet,
            "status": 1,
            "logs": [
                self._make_transfer_log(weth_addr, wallet, "0x" + "ee" * 20, 10**18),
                self._make_transfer_log(usdc_addr, "0x" + "ee" * 20, wallet, 5_000_000),
            ],
        }

        mock_resolver = MagicMock()

        def mock_resolve(addr, chain):
            result = MagicMock()
            if addr == usdc_addr:
                result.decimals = 6
            else:
                result.decimals = 18
            return result

        mock_resolver.resolve = mock_resolve

        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=mock_resolver,
        ):
            swap = parser.extract_swap_amounts(receipt)

        assert swap is not None
        assert swap.amount_out == 5_000_000
        assert swap.amount_out_decimal == 5  # 5_000_000 / 10^6 = 5, NOT 5e-12
        assert swap.amount_in_decimal == 1  # 10^18 / 10^18 = 1
        assert swap.token_in == weth_addr
        assert swap.token_out == usdc_addr

    def test_extract_swap_amounts_no_wallet(self):
        """Test that missing wallet address returns None."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        receipt = {"status": 1, "logs": []}
        assert parser.extract_swap_amounts(receipt) is None

    def test_extract_swap_amounts_no_transfers(self):
        """Test that receipt with no Transfer events returns None."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        receipt = {"from": "0x" + "ab" * 20, "status": 1, "logs": []}
        assert parser.extract_swap_amounts(receipt) is None

    def test_extract_swap_amounts_reverted_receipt(self):
        """Test that a reverted receipt (status=0) returns None even with Transfer events."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        wallet = "0x" + "ab" * 20
        receipt = {
            "from": wallet,
            "status": 0,
            "logs": [
                self._make_transfer_log("0x" + "cc" * 20, "0x" + "ee" * 20, wallet, 5_000_000),
            ],
        }
        assert parser.extract_swap_amounts(receipt) is None

    def test_extract_swap_amounts_resolver_failure(self):
        """Test that unknown output token (resolver raises) returns None."""
        from unittest.mock import MagicMock, patch

        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        wallet = "0x" + "ab" * 20
        unknown_token = "0x" + "ff" * 20

        receipt = {
            "from": wallet,
            "status": 1,
            "logs": [
                self._make_transfer_log("0x" + "dd" * 20, wallet, "0x" + "ee" * 20, 10**18),
                self._make_transfer_log(unknown_token, "0x" + "ee" * 20, wallet, 5_000_000),
            ],
        }

        mock_resolver = MagicMock()
        mock_resolver.resolve.side_effect = Exception("Token not found")

        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=mock_resolver,
        ):
            swap = parser.extract_swap_amounts(receipt)

        assert swap is None
