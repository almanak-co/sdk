"""Tests for _extract_token_flows with correct token decimals.

This test ensures that token amounts are correctly parsed from receipts
using the actual token decimals (e.g., 6 for USDC, 18 for ETH).

The critical bug this validates: Previously, _extract_token_flows used
hardcoded 10**18 for all tokens, causing USDC amounts to be ~1 million
times too small (since USDC has 6 decimals, not 18).
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.backtesting.paper.engine import (
    CHAIN_ID_ARBITRUM,
    CHAIN_ID_ETHEREUM,
    PaperTrader,
)

# Token addresses (lowercase for registry lookup)
USDC_ETHEREUM = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # 6 decimals
WETH_ETHEREUM = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"  # 18 decimals
USDC_ARBITRUM = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"  # 6 decimals
WETH_ARBITRUM = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"  # 18 decimals


def make_transfer_log(token_address: str, from_addr: str, to_addr: str, value: int) -> dict:
    """Create a mock ERC-20 Transfer event log.

    Args:
        token_address: Token contract address
        from_addr: Sender address (without 0x prefix)
        to_addr: Recipient address (without 0x prefix)
        value: Amount in smallest units (wei for 18 decimals, 10^-6 for USDC)

    Returns:
        Log dict in the format expected by receipt parsers
    """
    # ERC-20 Transfer(address,address,uint256) event signature
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    # Pad addresses to 32 bytes (64 hex chars)
    from_padded = from_addr.lower().replace("0x", "").zfill(64)
    to_padded = to_addr.lower().replace("0x", "").zfill(64)

    # Value encoded as 32-byte hex
    value_hex = hex(value)[2:].zfill(64)

    return {
        "address": token_address,
        "topics": [transfer_topic, f"0x{from_padded}", f"0x{to_padded}"],
        "data": f"0x{value_hex}",
    }


def create_mock_receipt(logs: list[dict], status: int = 1) -> MagicMock:
    """Create a mock TransactionReceipt.

    Args:
        logs: List of log dicts (from make_transfer_log)
        status: Transaction status (1 = success, 0 = failure)

    Returns:
        Mock receipt object with to_dict() method
    """
    receipt = MagicMock()
    receipt.to_dict.return_value = {
        "status": status,
        "logs": logs,
        "block_number": 12345678,
        "gas_used": 150000,
    }
    return receipt


def create_mock_paper_trader(chain_id: int, is_running: bool = True) -> MagicMock:
    """Create a mock PaperTrader with fork manager.

    Args:
        chain_id: Chain ID for the fork
        is_running: Whether the fork is running

    Returns:
        Mock PaperTrader with configured fork_manager
    """
    # Create mock fork manager
    fork_manager = MagicMock()
    fork_manager.chain_id = chain_id
    fork_manager.is_running = is_running
    fork_manager.get_rpc_url.return_value = "http://localhost:8545"

    # Create mock paper trader
    trader = MagicMock(spec=PaperTrader)
    trader.fork_manager = fork_manager
    trader._backtest_id = "test-backtest-id"

    return trader


class TestExtractTokenFlowsDecimals:
    """Tests for _extract_token_flows with correct decimal handling."""

    @pytest.mark.asyncio
    async def test_usdc_swap_correct_decimals(self):
        """Test that USDC amounts are parsed with 6 decimals, not 18.

        This is the critical integration test: swapping 1000 USDC for ETH.
        With 6 decimals, 1000 USDC = 1000 * 10^6 = 1_000_000_000 smallest units.
        With 18 decimals (the old bug), it would be interpreted as:
            1_000_000_000 / 10^18 = 0.000000001 USDC (wrong by ~1 million x)
        """
        wallet = "0x1234567890123456789012345678901234567890"

        # 1000 USDC out = 1000 * 10^6 = 1_000_000_000
        usdc_amount_raw = 1_000_000_000  # 1000 USDC in 6-decimal units
        # 0.5 ETH in = 0.5 * 10^18 = 500_000_000_000_000_000
        weth_amount_raw = 500_000_000_000_000_000  # 0.5 WETH in 18-decimal units

        logs = [
            # USDC out: from wallet to DEX
            make_transfer_log(
                USDC_ETHEREUM,
                from_addr=wallet,
                to_addr="0xDEXADDRESS000000000000000000000000000001",
                value=usdc_amount_raw,
            ),
            # WETH in: from DEX to wallet
            make_transfer_log(
                WETH_ETHEREUM,
                from_addr="0xDEXADDRESS000000000000000000000000000001",
                to_addr=wallet,
                value=weth_amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM)

        # Call the method under test
        # We need to bind the method to our mock and call it
        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        # Create a bound method by using the real class's method with our mock
        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, tokens_out = await method(
            intent=MagicMock(),  # Intent not used when receipt is provided
            receipt=receipt,
            wallet_address=wallet,
        )

        # Verify USDC (6 decimals): 1_000_000_000 / 10^6 = 1000
        # Note: US-065c changed keys from addresses to symbols
        assert "USDC" in tokens_out, f"Expected USDC symbol in tokens_out, got: {tokens_out}"
        usdc_out = tokens_out["USDC"]
        assert usdc_out == Decimal("1000"), f"Expected 1000 USDC, got {usdc_out}"

        # Verify WETH (18 decimals): 500_000_000_000_000_000 / 10^18 = 0.5
        assert "WETH" in tokens_in, f"Expected WETH symbol in tokens_in, got: {tokens_in}"
        weth_in = tokens_in["WETH"]
        assert weth_in == Decimal("0.5"), f"Expected 0.5 WETH, got {weth_in}"

    @pytest.mark.asyncio
    async def test_usdc_amount_not_off_by_million(self):
        """Regression test: Verify USDC is NOT interpreted with 18 decimals.

        Before the fix, 1_000_000_000 would be divided by 10^18 instead of 10^6,
        resulting in 0.000000001 instead of 1000.

        The difference factor is 10^12 (approximately 1 million x).
        """
        wallet = "0x1234567890123456789012345678901234567890"
        usdc_amount_raw = 1_000_000_000  # 1000 USDC in 6-decimal units

        logs = [
            make_transfer_log(
                USDC_ETHEREUM,
                from_addr=wallet,
                to_addr="0xDEXADDRESS000000000000000000000000000001",
                value=usdc_amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        _, tokens_out = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # US-065c: Keys are now symbols, not addresses
        usdc_out = tokens_out.get("USDC", Decimal("0"))

        # This should be 1000, not 0.000000001
        assert usdc_out > Decimal("0.001"), (
            f"USDC amount {usdc_out} is too small - likely using 18 decimals instead of 6"
        )
        assert usdc_out == Decimal("1000"), f"Expected 1000 USDC, got {usdc_out}"

    @pytest.mark.asyncio
    async def test_arbitrum_usdc_swap(self):
        """Test USDC swap on Arbitrum chain."""
        wallet = "0x1234567890123456789012345678901234567890"

        # 500 USDC out = 500 * 10^6 = 500_000_000
        usdc_amount_raw = 500_000_000
        # 0.25 ETH in = 0.25 * 10^18
        weth_amount_raw = 250_000_000_000_000_000

        logs = [
            make_transfer_log(
                USDC_ARBITRUM,
                from_addr=wallet,
                to_addr="0xDEXADDRESS000000000000000000000000000001",
                value=usdc_amount_raw,
            ),
            make_transfer_log(
                WETH_ARBITRUM,
                from_addr="0xDEXADDRESS000000000000000000000000000001",
                to_addr=wallet,
                value=weth_amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        trader = create_mock_paper_trader(CHAIN_ID_ARBITRUM)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, tokens_out = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # Verify USDC (6 decimals): 500_000_000 / 10^6 = 500
        # US-065c: Keys are now symbols, not addresses
        assert tokens_out["USDC"] == Decimal("500")

        # Verify WETH (18 decimals): 250_000_000_000_000_000 / 10^18 = 0.25
        assert tokens_in["WETH"] == Decimal("0.25")

    @pytest.mark.asyncio
    async def test_unknown_token_defaults_to_18_decimals(self):
        """Test that unknown tokens fall back to 18 decimals with warning."""
        wallet = "0x1234567890123456789012345678901234567890"
        unknown_token = "0x1111111111111111111111111111111111111111"

        # 1 token with 18 decimals = 10^18
        amount_raw = 1_000_000_000_000_000_000

        logs = [
            make_transfer_log(
                unknown_token,
                from_addr="0xSOMEADDRESS00000000000000000000000000001",
                to_addr=wallet,
                value=amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        # Fork not running = no RPC to query decimals
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM, is_running=False)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, _ = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # Unknown token should default to 18 decimals
        # US-065c: For unknown tokens, the checksummed address is used as the symbol fallback
        # The fallback will use the checksummed address format
        # Look for any key that contains the address or is the address
        found_key = None
        for key in tokens_in:
            if unknown_token.lower() in key.lower():
                found_key = key
                break
        assert found_key is not None, f"Expected unknown token address in tokens_in, got: {tokens_in}"
        assert tokens_in[found_key] == Decimal("1")

    @pytest.mark.asyncio
    async def test_failed_transaction_returns_empty_flows(self):
        """Test that failed transactions don't have token flows extracted."""
        wallet = "0x1234567890123456789012345678901234567890"

        logs = [
            make_transfer_log(
                USDC_ETHEREUM,
                from_addr=wallet,
                to_addr="0xDEXADDRESS000000000000000000000000000001",
                value=1_000_000_000,
            ),
        ]

        # Failed transaction (status = 0)
        receipt = create_mock_receipt(logs, status=0)
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, tokens_out = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # Failed transactions should have empty flows
        # (extract_token_flows in receipt_utils handles this)
        assert tokens_in == {}, f"Failed tx should have empty tokens_in, got: {tokens_in}"
        assert tokens_out == {}, f"Failed tx should have empty tokens_out, got: {tokens_out}"


class TestDecimalPrecision:
    """Tests for decimal precision in token flow calculations."""

    @pytest.mark.asyncio
    async def test_small_usdc_amount(self):
        """Test small USDC amounts (e.g., $0.01)."""
        wallet = "0x1234567890123456789012345678901234567890"

        # 0.01 USDC = 10_000 (6 decimal units)
        usdc_amount_raw = 10_000

        logs = [
            make_transfer_log(
                USDC_ETHEREUM,
                from_addr="0xSOMEADDRESS00000000000000000000000000001",
                to_addr=wallet,
                value=usdc_amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, _ = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # US-065c: Keys are now symbols, not addresses
        assert tokens_in["USDC"] == Decimal("0.01")

    @pytest.mark.asyncio
    async def test_large_usdc_amount(self):
        """Test large USDC amounts (e.g., $1,000,000)."""
        wallet = "0x1234567890123456789012345678901234567890"

        # 1,000,000 USDC = 1_000_000 * 10^6 = 1_000_000_000_000
        usdc_amount_raw = 1_000_000_000_000

        logs = [
            make_transfer_log(
                USDC_ETHEREUM,
                from_addr="0xSOMEADDRESS00000000000000000000000000001",
                to_addr=wallet,
                value=usdc_amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, _ = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # US-065c: Keys are now symbols, not addresses
        assert tokens_in["USDC"] == Decimal("1000000")

    @pytest.mark.asyncio
    async def test_wei_precision_preserved(self):
        """Test that wei-level precision is preserved for 18-decimal tokens."""
        wallet = "0x1234567890123456789012345678901234567890"

        # 1 wei = smallest ETH unit
        weth_amount_raw = 1

        logs = [
            make_transfer_log(
                WETH_ETHEREUM,
                from_addr="0xSOMEADDRESS00000000000000000000000000001",
                to_addr=wallet,
                value=weth_amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, _ = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # 1 wei = 10^-18 ETH
        # US-065c: Keys are now symbols, not addresses
        expected = Decimal("1") / Decimal(10**18)
        assert tokens_in["WETH"] == expected
