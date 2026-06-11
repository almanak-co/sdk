"""Unit tests for FluidReceiptParser swap extraction (synthetic receipts).

Token identification must be SENDER-INDEPENDENT: under Zodiac Safe execution
``receipt.from`` is the relayer EOA while the ERC-20 transfer legs involve
the Safe, so a wallet-keyed heuristic misses both legs. Fluid custodies all
pool funds in the central Liquidity layer (Phase-0 report §V1, VIB-5028:
input leg = payer → Liquidity, output leg = Liquidity → recipient), so the
parser matches Transfer counterparties against the deterministic Liquidity
address first and the receipt sender only as a secondary signal.

Decimals are seeded into the parser's cache so the tests stay hermetic (no
token-resolver / network dependency). The real-fork behaviour is covered by
``tests/intents/*/test_fluid_swap.py``.
"""

from decimal import Decimal
from unittest.mock import patch

from almanak.connectors.fluid.receipt_parser import (
    _FLUID_NATIVE_SENTINEL,
    ERC721_TRANSFER_TOPIC,
    SWAP_TOPIC,
    FluidReceiptParser,
)

# Real arbitrum addresses (parser compares lowercased strings only).
USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDT = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
POOL = "0x1234567890AbcdEF1234567890aBcdef12345678"
LIQUIDITY = "0x52Aa899454998Be5b000Ad077a46Bbe360F4e497"  # Fluid Liquidity layer
SAFE = "0xAAAAaaaAAaAaAAaaAAAAaAAAaaaaAAaAAAAAaaaA"
RELAYER = "0xBBbBBBbbbBBBbbbbBbBbbbbBBbBbbbbBbBbbbbBB"

AMOUNT_IN = 50_000_000  # 50 USDC (6 dp)
AMOUNT_OUT = 49_975_000  # 49.975 USDT (6 dp)


def _word(value: int) -> str:
    return f"{value:064x}"


def _addr_topic(addr: str) -> str:
    return "0x" + "0" * 24 + addr[2:].lower()


def _swap_log(to: str, swap0to1: bool = True, amount_in: int = AMOUNT_IN, amount_out: int = AMOUNT_OUT) -> dict:
    data = "0x" + _word(int(swap0to1)) + _word(amount_in) + _word(amount_out) + "0" * 24 + to[2:].lower()
    return {"address": POOL, "topics": [SWAP_TOPIC], "data": data}


def _transfer_log(token: str, from_addr: str, to_addr: str, amount: int) -> dict:
    return {
        "address": token,
        "topics": [ERC721_TRANSFER_TOPIC, _addr_topic(from_addr), _addr_topic(to_addr)],
        "data": "0x" + _word(amount),
    }


def _receipt(sender: str, logs: list[dict]) -> dict:
    return {
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 1_000_000,
        "status": 1,
        "from": sender,
        "logs": logs,
    }


def _parser() -> FluidReceiptParser:
    parser = FluidReceiptParser(chain="arbitrum")
    parser._decimals_cache[f"arbitrum:{USDC.lower()}"] = 6
    parser._decimals_cache[f"arbitrum:{USDT.lower()}"] = 6
    return parser


class TestSwapAmountsEoaExecution:
    def test_both_legs_resolved(self):
        receipt = _receipt(
            SAFE,
            [
                _transfer_log(USDC, SAFE, LIQUIDITY, AMOUNT_IN),
                _swap_log(to=SAFE),
                _transfer_log(USDT, LIQUIDITY, SAFE, AMOUNT_OUT),
            ],
        )
        amounts = _parser().extract_swap_amounts(receipt)
        assert amounts is not None
        assert amounts.token_in.lower() == USDC.lower()
        assert amounts.token_out.lower() == USDT.lower()
        assert amounts.amount_in == AMOUNT_IN
        assert amounts.amount_out == AMOUNT_OUT
        assert amounts.amount_in_decimal == Decimal("50")
        assert amounts.amount_out_decimal == Decimal("49.975")


class TestSwapAmountsSafeExecution:
    """receipt.from is the relayer EOA; transfers involve the Safe only."""

    def test_both_legs_resolved_sender_independent(self):
        receipt = _receipt(
            RELAYER,
            [
                _transfer_log(USDC, SAFE, LIQUIDITY, AMOUNT_IN),
                _swap_log(to=SAFE),
                _transfer_log(USDT, LIQUIDITY, SAFE, AMOUNT_OUT),
            ],
        )
        amounts = _parser().extract_swap_amounts(receipt)
        assert amounts is not None, "Safe execution must not break token identification"
        assert amounts.token_in.lower() == USDC.lower()
        assert amounts.token_out.lower() == USDT.lower()
        assert amounts.amount_in == AMOUNT_IN
        assert amounts.amount_out == AMOUNT_OUT

    def test_native_out_falls_back_to_sentinel(self):
        # Native output leg produces no ERC-20 Transfer; pools are strictly
        # per-pair so the missing leg is guaranteed native — and the fallback
        # must not depend on swap.to matching receipt.from.
        receipt = _receipt(
            RELAYER,
            [
                _transfer_log(USDC, SAFE, LIQUIDITY, AMOUNT_IN),
                _swap_log(to=SAFE, amount_out=10**16),
            ],
        )
        amounts = _parser().extract_swap_amounts(receipt)
        assert amounts is not None
        assert amounts.token_in.lower() == USDC.lower()
        assert amounts.token_out == _FLUID_NATIVE_SENTINEL
        assert amounts.amount_out_decimal == Decimal("0.01")  # 18 dp native

    def test_native_in_falls_back_to_sentinel(self):
        receipt = _receipt(
            RELAYER,
            [
                _swap_log(to=SAFE, amount_in=10**16),
                _transfer_log(USDT, LIQUIDITY, SAFE, AMOUNT_OUT),
            ],
        )
        amounts = _parser().extract_swap_amounts(receipt)
        assert amounts is not None
        assert amounts.token_in == _FLUID_NATIVE_SENTINEL
        assert amounts.token_out.lower() == USDT.lower()
        assert amounts.amount_in_decimal == Decimal("0.01")


class TestSwapAmountsFailClosed:
    def test_no_swap_event_returns_none(self):
        receipt = _receipt(SAFE, [_transfer_log(USDC, SAFE, LIQUIDITY, AMOUNT_IN)])
        assert _parser().extract_swap_amounts(receipt) is None

    def test_unresolvable_decimals_returns_none(self):
        # Both legs identified but decimals unknown -> None (fail-closed),
        # never raw-wei amounts dressed as decimals.
        receipt = _receipt(
            SAFE,
            [
                _transfer_log(USDC, SAFE, LIQUIDITY, AMOUNT_IN),
                _swap_log(to=SAFE),
                _transfer_log(USDT, LIQUIDITY, SAFE, AMOUNT_OUT),
            ],
        )
        parser = FluidReceiptParser(chain="arbitrum")  # cache NOT seeded
        with patch("almanak.framework.data.tokens.get_token_resolver", side_effect=RuntimeError("offline")):
            assert parser.extract_swap_amounts(receipt) is None
