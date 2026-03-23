"""Tests for Aerodrome receipt parser — V1/V2 and Slipstream CL swap events."""

from decimal import Decimal

from almanak.framework.connectors.aerodrome.receipt_parser import (
    EVENT_TOPICS,
    AerodromeReceiptParser,
)

# ---------------------------------------------------------------------------
# Helpers to build mock receipts
# ---------------------------------------------------------------------------


def _pad32(val: int, signed: bool = False) -> str:
    """Encode an integer as a 32-byte hex word (no 0x prefix)."""
    if signed and val < 0:
        val = val + (1 << 256)
    return f"{val:064x}"


def _addr_topic(addr: str) -> str:
    """Pad an address to a 32-byte topic."""
    return "0x" + addr.lower().replace("0x", "").zfill(64)


def _build_v1_swap_receipt(
    amount0_in: int,
    amount1_in: int,
    amount0_out: int,
    amount1_out: int,
    sender: str = "0x" + "aa" * 20,
    to: str = "0x" + "bb" * 20,
    pool: str = "0x" + "cc" * 20,
) -> dict:
    """Build a receipt with a V1/V2 Swap event."""
    data = "0x" + _pad32(amount0_in) + _pad32(amount1_in) + _pad32(amount0_out) + _pad32(amount1_out)
    return {
        "transactionHash": "0x" + "11" * 32,
        "blockNumber": 100,
        "status": 1,
        "gasUsed": 150_000,
        "logs": [
            {
                "address": pool,
                "topics": [EVENT_TOPICS["Swap"], _addr_topic(sender), _addr_topic(to)],
                "data": data,
                "logIndex": 0,
            }
        ],
    }


def _build_cl_swap_receipt(
    amount0: int,
    amount1: int,
    sqrt_price_x96: int = 2**96,
    liquidity: int = 10**18,
    tick: int = 0,
    sender: str = "0x" + "aa" * 20,
    recipient: str = "0x" + "bb" * 20,
    pool: str = "0x" + "dd" * 20,
) -> dict:
    """Build a receipt with a Slipstream CL Swap event (V3-style)."""
    data = (
        "0x"
        + _pad32(amount0, signed=True)
        + _pad32(amount1, signed=True)
        + _pad32(sqrt_price_x96)
        + _pad32(liquidity)
        + _pad32(tick, signed=True)
    )
    return {
        "transactionHash": "0x" + "22" * 32,
        "blockNumber": 200,
        "status": 1,
        "gasUsed": 300_000,
        "logs": [
            {
                "address": pool,
                "topics": [EVENT_TOPICS["SwapCL"], _addr_topic(sender), _addr_topic(recipient)],
                "data": data,
                "logIndex": 0,
            }
        ],
    }


# ---------------------------------------------------------------------------
# Tests — V1/V2 Swap (baseline, should still work)
# ---------------------------------------------------------------------------


class TestV1SwapParsing:
    """Verify V1/V2 Swap event parsing still works correctly."""

    def test_v1_swap_token0_in(self):
        parser = AerodromeReceiptParser(
            chain="base",
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _build_v1_swap_receipt(
            amount0_in=3_000_000,  # 3 USDC
            amount1_in=0,
            amount0_out=0,
            amount1_out=10**15,  # 0.001 WETH
        )
        result = parser.parse_receipt(receipt)
        assert result.success
        assert len(result.swap_events) == 1
        assert result.swap_result is not None
        assert result.swap_result.amount_in == 3_000_000
        assert result.swap_result.amount_out == 10**15
        assert result.swap_result.token_in_symbol == "USDC"
        assert result.swap_result.token_out_symbol == "WETH"

    def test_v1_extract_swap_amounts(self):
        # Real Base addresses so the token resolver can find decimals
        usdc_addr = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        weth_addr = "0x4200000000000000000000000000000000000006"
        wallet = "0x" + "aa" * 20

        parser = AerodromeReceiptParser(
            chain="base",
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _build_v1_swap_receipt(
            amount0_in=5_000_000,
            amount1_in=0,
            amount0_out=0,
            amount1_out=2 * 10**15,
        )
        # Add Transfer events and from address for decimal resolution
        transfer_topic = EVENT_TOPICS["Transfer"]
        receipt["from"] = wallet
        receipt["logs"].insert(0, {
            "address": usdc_addr,
            "topics": [transfer_topic, _addr_topic(wallet), _addr_topic("0x" + "cc" * 20)],
            "data": "0x" + _pad32(5_000_000),
            "logIndex": 10,
        })
        receipt["logs"].append({
            "address": weth_addr,
            "topics": [transfer_topic, _addr_topic("0x" + "cc" * 20), _addr_topic(wallet)],
            "data": "0x" + _pad32(2 * 10**15),
            "logIndex": 11,
        })

        swap_amounts = parser.extract_swap_amounts(receipt)
        assert swap_amounts is not None
        assert swap_amounts.amount_in == 5_000_000
        assert swap_amounts.amount_out == 2 * 10**15
        assert swap_amounts.token_in == "USDC"
        assert swap_amounts.token_out == "WETH"


# ---------------------------------------------------------------------------
# Tests — Slipstream CL Swap (the bug fix)
# ---------------------------------------------------------------------------


class TestCLSwapParsing:
    """Verify Slipstream CL Swap event parsing works (VIB-1632 fix)."""

    def test_cl_swap_token0_in_token1_out(self):
        """User pays token0 (positive), receives token1 (negative)."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _build_cl_swap_receipt(
            amount0=3_000_000,  # +3 USDC into pool
            amount1=-(10**15),  # -0.001 WETH out of pool
        )
        result = parser.parse_receipt(receipt)
        assert result.success
        assert len(result.swap_events) == 1
        assert result.swap_result is not None
        assert result.swap_result.amount_in == 3_000_000
        assert result.swap_result.amount_out == 10**15
        assert result.swap_result.token_in_symbol == "USDC"
        assert result.swap_result.token_out_symbol == "WETH"
        assert result.swap_result.amount_in_decimal == Decimal("3")
        assert result.swap_result.amount_out_decimal == Decimal("0.001")

    def test_cl_swap_token1_in_token0_out(self):
        """User pays token1 (positive), receives token0 (negative)."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _build_cl_swap_receipt(
            amount0=-(5_000_000),  # -5 USDC out of pool
            amount1=2 * 10**15,  # +0.002 WETH into pool
        )
        result = parser.parse_receipt(receipt)
        assert result.success
        assert len(result.swap_events) == 1
        assert result.swap_result is not None
        assert result.swap_result.amount_in == 2 * 10**15
        assert result.swap_result.amount_out == 5_000_000
        assert result.swap_result.token_in_symbol == "WETH"
        assert result.swap_result.token_out_symbol == "USDC"

    def test_cl_swap_extract_swap_amounts(self):
        """extract_swap_amounts() returns SwapAmounts for CL swaps."""
        usdc_addr = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        weth_addr = "0x4200000000000000000000000000000000000006"
        wallet = "0x" + "aa" * 20

        parser = AerodromeReceiptParser(
            chain="base",
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _build_cl_swap_receipt(
            amount0=10_000_000,  # +10 USDC
            amount1=-(4 * 10**15),  # -0.004 WETH
        )
        # Add Transfer events and from address for decimal resolution
        transfer_topic = EVENT_TOPICS["Transfer"]
        receipt["from"] = wallet
        receipt["logs"].insert(0, {
            "address": usdc_addr,
            "topics": [transfer_topic, _addr_topic(wallet), _addr_topic("0x" + "dd" * 20)],
            "data": "0x" + _pad32(10_000_000),
            "logIndex": 10,
        })
        receipt["logs"].append({
            "address": weth_addr,
            "topics": [transfer_topic, _addr_topic("0x" + "dd" * 20), _addr_topic(wallet)],
            "data": "0x" + _pad32(4 * 10**15),
            "logIndex": 11,
        })

        swap_amounts = parser.extract_swap_amounts(receipt)
        assert swap_amounts is not None
        assert swap_amounts.amount_in == 10_000_000
        assert swap_amounts.amount_out == 4 * 10**15
        assert swap_amounts.token_in == "USDC"
        assert swap_amounts.token_out == "WETH"
        assert swap_amounts.effective_price > 0

    def test_cl_swap_effective_price(self):
        """Effective price is computed correctly for CL swaps."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _build_cl_swap_receipt(
            amount0=2500_000_000,  # +2500 USDC
            amount1=-(10**18),  # -1 WETH
        )
        result = parser.parse_receipt(receipt)
        assert result.swap_result is not None
        # price = amount_out / amount_in = 1 WETH / 2500 USDC = 0.0004
        assert result.swap_result.effective_price == Decimal("1") / Decimal("2500")

    def test_cl_swap_no_false_positive_on_v1(self):
        """V1 swap receipts should NOT produce CL swap events."""
        parser = AerodromeReceiptParser(chain="base", token0_decimals=6, token1_decimals=18)
        receipt = _build_v1_swap_receipt(amount0_in=100, amount1_in=0, amount0_out=0, amount1_out=200)
        result = parser.parse_receipt(receipt)
        # Should parse as exactly 1 swap event (V1, not CL)
        assert len(result.swap_events) == 1
        assert result.swap_result is not None


class TestCLSwapEdgeCases:
    """Edge cases for CL swap parsing."""

    def test_empty_receipt_returns_none(self):
        parser = AerodromeReceiptParser(chain="base")
        receipt = {"transactionHash": "0x00", "blockNumber": 1, "status": 1, "logs": []}
        swap_amounts = parser.extract_swap_amounts(receipt)
        assert swap_amounts is None

    def test_failed_tx_returns_none(self):
        parser = AerodromeReceiptParser(chain="base")
        receipt = _build_cl_swap_receipt(amount0=100, amount1=-200)
        receipt["status"] = 0  # reverted
        swap_amounts = parser.extract_swap_amounts(receipt)
        assert swap_amounts is None

    def test_malformed_cl_data_returns_none(self):
        """Malformed CL swap data must NOT produce a zero-amount swap result."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        # Non-hex garbage data that will cause HexDecoder to raise
        receipt = {
            "transactionHash": "0x" + "33" * 32,
            "blockNumber": 1,
            "status": 1,
            "logs": [
                {
                    "address": "0x" + "dd" * 20,
                    "topics": [
                        EVENT_TOPICS["SwapCL"],
                        _addr_topic("0x" + "aa" * 20),
                        _addr_topic("0x" + "bb" * 20),
                    ],
                    "data": "not_valid_hex_data",
                    "logIndex": 0,
                }
            ],
        }
        result = parser.parse_receipt(receipt)
        # Decode failure bubbles up to _parse_log which returns None,
        # so no swap event is appended — prevents silent zero-amount results
        assert len(result.swap_events) == 0
        assert result.swap_result is None
        assert parser.extract_swap_amounts(receipt) is None

    def test_unrecognized_topic_ignored(self):
        parser = AerodromeReceiptParser(chain="base")
        receipt = {
            "transactionHash": "0x00",
            "blockNumber": 1,
            "status": 1,
            "logs": [
                {
                    "address": "0x" + "ff" * 20,
                    "topics": ["0x" + "ab" * 32],
                    "data": "0x" + "00" * 128,
                    "logIndex": 0,
                }
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.success
        assert len(result.swap_events) == 0
        assert result.swap_result is None
