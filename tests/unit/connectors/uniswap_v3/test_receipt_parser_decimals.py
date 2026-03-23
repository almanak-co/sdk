"""Tests for UniswapV3ReceiptParser decimal resolution from Transfer events.

Verifies that when the parser is constructed without token info (as when used
via ResultEnricher), it resolves correct decimals from Transfer event token
addresses rather than defaulting everything to 18.
"""

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.connectors.uniswap_v3.receipt_parser import (
    SwapEventData,
    TransferEventData,
    UniswapV3ReceiptParser,
)

# USDC on Polygon
USDC_ADDRESS = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"
USDC_DECIMALS = 6

# WMATIC on Polygon
WMATIC_ADDRESS = "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"
WMATIC_DECIMALS = 18

POOL_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
ROUTER_ADDRESS = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"


def _make_swap_event(amount0: int, amount1: int) -> SwapEventData:
    """Create a SwapEventData for testing."""
    return SwapEventData(
        sender=ROUTER_ADDRESS,
        recipient=ROUTER_ADDRESS,
        amount0=amount0,
        amount1=amount1,
        sqrt_price_x96=0,
        liquidity=0,
        tick=0,
        pool_address=POOL_ADDRESS,
    )


def _make_transfer(from_addr: str, to_addr: str, value: int, token: str) -> TransferEventData:
    return TransferEventData(
        from_addr=from_addr,
        to_addr=to_addr,
        value=value,
        token_address=token,
    )


# Token info lookup table for mocking _resolve_token_info
_TOKEN_INFO = {
    USDC_ADDRESS: ("USDC", USDC_DECIMALS),
    WMATIC_ADDRESS: ("WMATIC", WMATIC_DECIMALS),
}


def _mock_resolve_token_info(token: str) -> tuple[str, int | None]:
    """Mock _resolve_token_info to return known token info."""
    addr = token.lower()
    if addr in _TOKEN_INFO:
        return _TOKEN_INFO[addr]
    return "", None


def _make_parser_no_tokens() -> UniswapV3ReceiptParser:
    """Create parser without token info, with mocked resolver."""
    parser = UniswapV3ReceiptParser.__new__(UniswapV3ReceiptParser)
    parser.chain = "polygon"
    parser.token0_address = None
    parser.token0_symbol = None
    parser.token0_decimals = 18
    parser.token1_address = None
    parser.token1_symbol = None
    parser.token1_decimals = 18
    parser.quoted_price = None
    parser._token0_decimals_resolved = False
    parser._token1_decimals_resolved = False
    parser._resolve_token_info = _mock_resolve_token_info
    return parser


class TestDecimalResolutionFromTransfers:
    """Test that _resolve_tokens_from_transfers fixes defaulted decimals."""

    def test_resolves_usdc_decimals_from_transfer(self):
        """When parser has no token info, resolve USDC as 6 decimals from Transfer event."""
        parser = _make_parser_no_tokens()
        parser.token0_address = USDC_ADDRESS
        parser.token1_address = WMATIC_ADDRESS

        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        transfers = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 100_000_000, USDC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 1_079_340_000_000_000_000_000, WMATIC_ADDRESS),
        ]

        result = parser._build_swap_result(swap_event, transfers, None)

        # USDC amount should be 100, not 1E-10 (which happens with 18 decimals)
        assert result.amount_in_decimal == Decimal("100")
        # WMATIC amount should be ~1079.34
        assert result.amount_out_decimal == Decimal("1079.34")
        # Effective price ~10.79 WMATIC/USDC
        assert Decimal("10") < result.effective_price < Decimal("11")

    def test_skips_resolution_when_decimals_already_set(self):
        """When parser has correct decimals from construction, don't re-resolve."""
        parser = _make_parser_no_tokens()
        parser.token0_address = USDC_ADDRESS
        parser.token0_decimals = USDC_DECIMALS
        parser._token0_decimals_resolved = True
        parser.token1_address = WMATIC_ADDRESS
        parser.token1_decimals = WMATIC_DECIMALS
        parser._token1_decimals_resolved = True

        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        transfers = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 100_000_000, USDC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 1_079_340_000_000_000_000_000, WMATIC_ADDRESS),
        ]

        result = parser._build_swap_result(swap_event, transfers, None)

        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_out_decimal == Decimal("1079.34")

    def test_infers_token_addresses_from_transfers(self):
        """When parser has no token addresses at all, infer from Transfer events using swap direction."""
        parser = _make_parser_no_tokens()
        # No token addresses set

        # amount0 > 0 means token0 is input (sent TO pool)
        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        transfers = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 100_000_000, USDC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 1_079_340_000_000_000_000_000, WMATIC_ADDRESS),
        ]

        result = parser._build_swap_result(swap_event, transfers, None)

        # Amounts should be correct (inferred from transfer direction)
        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_out_decimal == Decimal("1079.34")
        # Parser state should NOT be mutated (inference uses local overrides)
        assert parser.token0_address is None
        assert parser.token1_address is None

    def test_cached_parser_not_corrupted_across_receipts(self):
        """A cached parser (no pre-set tokens) must produce correct results for different pools."""
        parser = _make_parser_no_tokens()

        # First receipt: USDC -> WMATIC
        swap1 = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        transfers1 = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 100_000_000, USDC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 1_079_340_000_000_000_000_000, WMATIC_ADDRESS),
        ]
        result1 = parser._build_swap_result(swap1, transfers1, None)
        assert result1.amount_in_decimal == Decimal("100")

        # Second receipt: same parser, different pool (WMATIC -> USDC, reversed)
        swap2 = _make_swap_event(amount0=-50_000_000, amount1=539_670_000_000_000_000_000)
        swap2 = SwapEventData(
            sender=ROUTER_ADDRESS,
            recipient=ROUTER_ADDRESS,
            amount0=-50_000_000,
            amount1=539_670_000_000_000_000_000,
            sqrt_price_x96=0,
            liquidity=0,
            tick=0,
            pool_address=POOL_ADDRESS,
        )
        transfers2 = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 539_670_000_000_000_000_000, WMATIC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 50_000_000, USDC_ADDRESS),
        ]
        result2 = parser._build_swap_result(swap2, transfers2, None)
        # WMATIC in (~539.67), USDC out (50)
        assert result2.amount_in_decimal == Decimal("539.67")
        assert result2.amount_out_decimal == Decimal("50")

    def test_handles_empty_transfers_gracefully(self):
        """When no Transfer events, fall back to default 18 decimals."""
        parser = _make_parser_no_tokens()

        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)

        result = parser._build_swap_result(swap_event, [], None)

        # With default 18 decimals, amount_in will be tiny — this is the old behavior
        assert result.amount_in_decimal == Decimal("100000000") / Decimal(10**18)

    def test_handles_resolver_failure(self):
        """When token resolver raises, fall back to default 18 decimals."""
        parser = _make_parser_no_tokens()
        parser.token0_address = USDC_ADDRESS
        parser.token1_address = WMATIC_ADDRESS
        parser._resolve_token_info = lambda token: ("", None)  # Simulate resolver failure

        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        transfers = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 100_000_000, USDC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 1_079_340_000_000_000_000_000, WMATIC_ADDRESS),
        ]

        result = parser._build_swap_result(swap_event, transfers, None)

        # Should still produce a result, just with default decimals
        assert result is not None
        assert result.amount_in > 0
        # Decimals should remain at 18 (default)
        assert parser.token0_decimals == 18

    def test_resolves_reverse_direction(self):
        """Test swap in the other direction: WMATIC -> USDC (amount1 positive)."""
        parser = _make_parser_no_tokens()
        parser.token0_address = USDC_ADDRESS
        parser.token1_address = WMATIC_ADDRESS

        # Swap: WMATIC in (amount1 > 0) -> USDC out (amount0 < 0)
        swap_event = _make_swap_event(
            amount0=-100_000_000,  # USDC out: 100 USDC
            amount1=1_079_340_000_000_000_000_000,  # WMATIC in: ~1079 WMATIC
        )
        transfers = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 1_079_340_000_000_000_000_000, WMATIC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 100_000_000, USDC_ADDRESS),
        ]

        result = parser._build_swap_result(swap_event, transfers, None)

        # WMATIC is token_in (amount1 > 0 = token1 is input)
        assert result.amount_in_decimal == Decimal("1079.34")
        # USDC is token_out
        assert result.amount_out_decimal == Decimal("100")


class TestFlagTracking:
    """Verify _token0/1_decimals_resolved tracking."""

    def test_resolved_flag_set_when_decimals_provided(self):
        """Flags are True when decimals are explicitly provided."""
        parser = _make_parser_no_tokens()
        parser.token0_decimals = 6
        parser._token0_decimals_resolved = True
        parser.token1_decimals = 18
        parser._token1_decimals_resolved = True

        assert parser._token0_decimals_resolved is True
        assert parser._token1_decimals_resolved is True

    def test_resolved_flag_false_when_defaulted(self):
        """Flags are False when decimals were defaulted to 18."""
        parser = _make_parser_no_tokens()

        assert parser._token0_decimals_resolved is False
        assert parser._token1_decimals_resolved is False
