"""Characterization tests for AcrossReceiptParser.

These tests pin CURRENT behavior. They are the regression contract for any future
refactor of the parser. Do not change parser source in this PR.

Key pattern: _resolve_decimals imports get_token_resolver inside the method.
All tests that reach that code path MUST stub it via monkeypatch.
"""

from decimal import Decimal

import pytest

from almanak.connectors.across.adapter import ACROSS_SPOKE_POOL_ADDRESSES
from almanak.connectors.across.receipt_parser import (
    TRANSFER_EVENT_SIGNATURE,
    V3_FUNDS_DEPOSITED_TOPIC,
    AcrossReceiptParser,
)


# ---------------------------------------------------------------------------
# Hex helpers
# ---------------------------------------------------------------------------


def word(v: int) -> str:
    """One 32-byte ABI word as 64 hex chars (no 0x)."""
    return f"{v:064x}"


def addr_word(a: str) -> str:
    """Address left-padded to a 32-byte word (no 0x)."""
    return a.lower().replace("0x", "").zfill(64)


def addr_topic(a: str) -> str:
    """Address as an indexed topic (0x-prefixed 32-byte word)."""
    return "0x" + a.lower().replace("0x", "").zfill(64)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
OUTPUT_TOKEN = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
DEPOSITOR = "0x1111111111111111111111111111111111111111"
WALLET = "0x2222222222222222222222222222222222222222"
SPOKE_POOL_ETH = ACROSS_SPOKE_POOL_ADDRESSES[1]
TX_HASH = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

DEST_CHAIN_ARBITRUM = 42161  # maps to "arbitrum"
RANDOM_CONTRACT = "0x9999999999999999999999999999999999999999"


# ---------------------------------------------------------------------------
# Token resolver stub
# ---------------------------------------------------------------------------


class _StubToken:
    decimals = 6


class _StubResolver:
    def resolve(self, value, chain, **kwargs):
        return _StubToken()


class _FailingResolver:
    def resolve(self, value, chain, **kwargs):
        raise ValueError("no such token")


@pytest.fixture
def stub_resolver(monkeypatch):
    monkeypatch.setattr(
        "almanak.framework.data.tokens.get_token_resolver", lambda: _StubResolver()
    )


@pytest.fixture
def failing_resolver(monkeypatch):
    monkeypatch.setattr(
        "almanak.framework.data.tokens.get_token_resolver", lambda: _FailingResolver()
    )


# ---------------------------------------------------------------------------
# Deposit log helper
# ---------------------------------------------------------------------------


def make_deposit_log(
    dest_chain_id: int = DEST_CHAIN_ARBITRUM,
    deposit_id: int = 7,
    input_amount: int = 5_000_000,
    output_amount: int = 4_990_000,
    log_address: str = RANDOM_CONTRACT,  # intentionally random — address is NOT checked
) -> dict:
    """Build a V3FundsDeposited log.

    Indexed topics: [topic0, destinationChainId, depositId, depositor]
    Data: inputToken (32), outputToken (32), inputAmount (32), outputAmount (32)
    """
    return {
        "address": log_address,
        "topics": [
            V3_FUNDS_DEPOSITED_TOPIC,
            "0x" + word(dest_chain_id),   # indexed destinationChainId
            "0x" + word(deposit_id),       # indexed depositId
            addr_topic(DEPOSITOR),          # indexed depositor
        ],
        "data": "0x" + addr_word(USDC) + addr_word(OUTPUT_TOKEN) + word(input_amount) + word(output_amount),
        "logIndex": 0,
    }


def make_receipt(logs: list, status: int = 1) -> dict:
    return {
        "transactionHash": TX_HASH,
        "blockNumber": 12345,
        "status": status,
        "from": WALLET,
        "logs": logs,
    }


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_deposit_event_fully_decoded(self, parser_eth, stub_resolver):
        receipt = make_receipt([make_deposit_log()])
        result = parser_eth.extract_bridge_data(receipt, token="usdc")

        assert result is not None
        assert result.amount_sent_raw == 5_000_000
        assert result.amount_sent == Decimal("5")
        assert result.destination_chain == "arbitrum"
        assert result.source_chain == "ethereum"
        assert result.token_symbol == "USDC"
        assert result.bridge_name == "across"
        assert result.source_token_address == USDC.lower()
        assert result.destination_tx_hash is None

    def test_log_address_is_not_checked(self, parser_eth, stub_resolver):
        """The deposit log's emitting address is NOT verified against SpokePool list.
        Only topic0 is matched. This pins documented behavior."""
        log = make_deposit_log(log_address=RANDOM_CONTRACT)
        receipt = make_receipt([log])
        result = parser_eth.extract_bridge_data(receipt, token="usdc")
        assert result is not None
        assert result.amount_sent_raw == 5_000_000


@pytest.fixture
def parser_eth():
    return AcrossReceiptParser(chain="ethereum")


# ---------------------------------------------------------------------------
# Failure gates
# ---------------------------------------------------------------------------


class TestFailureGates:
    def test_status_not_1_returns_none(self, stub_resolver):
        parser = AcrossReceiptParser(chain="ethereum")
        receipt = make_receipt([make_deposit_log()], status=0)
        assert parser.extract_bridge_data(receipt, token="usdc") is None

    def test_empty_logs_returns_none(self, stub_resolver):
        parser = AcrossReceiptParser(chain="ethereum")
        receipt = make_receipt([])
        assert parser.extract_bridge_data(receipt, token="usdc") is None

    def test_unknown_dest_chain_id_falls_back_to_string(self, stub_resolver):
        """Unknown dest chain id is stringified rather than raising."""
        parser = AcrossReceiptParser(chain="ethereum")
        receipt = make_receipt([make_deposit_log(dest_chain_id=999_999)])
        result = parser.extract_bridge_data(receipt, token="usdc")
        assert result is not None
        assert result.destination_chain == "999999"

    def test_no_source_chain_returns_none(self, stub_resolver):
        """No chain kwarg in constructor + no from_chain hint = None."""
        parser = AcrossReceiptParser()  # no chain
        receipt = make_receipt([make_deposit_log()])
        assert parser.extract_bridge_data(receipt, token="usdc") is None

    def test_decimals_unresolvable_returns_none(self, failing_resolver):
        parser = AcrossReceiptParser(chain="ethereum")
        receipt = make_receipt([make_deposit_log()])
        assert parser.extract_bridge_data(receipt, token="usdc") is None


# ---------------------------------------------------------------------------
# Transfer fallback
# ---------------------------------------------------------------------------


class TestTransferFallback:
    def test_wallet_transfer_to_spoke_pool_used_as_fallback(self, stub_resolver):
        """When no deposit event is present, a Transfer from the wallet to the
        spoke pool provides amount_sent_raw."""
        transfer_log = {
            "address": USDC,
            "topics": [
                TRANSFER_EVENT_SIGNATURE,
                addr_topic(WALLET),
                addr_topic(SPOKE_POOL_ETH),
            ],
            "data": "0x" + word(3_000_000),
            "logIndex": 0,
        }
        receipt = {
            "transactionHash": TX_HASH,
            "blockNumber": 100,
            "status": 1,
            "from": WALLET,
            "logs": [transfer_log],
        }
        parser = AcrossReceiptParser(chain="ethereum")
        result = parser.extract_bridge_data(receipt, token="usdc", to_chain="arbitrum")

        assert result is not None
        assert result.amount_sent_raw == 3_000_000
        assert result.source_token_address == USDC.lower()


# ---------------------------------------------------------------------------
# parse_receipt (minimal shape)
# ---------------------------------------------------------------------------


class TestParseReceiptShape:
    def test_parse_receipt_returns_status_and_tx_hash(self):
        parser = AcrossReceiptParser(chain="ethereum")
        receipt = {"status": 1, "transactionHash": TX_HASH}
        result = parser.parse_receipt(receipt)

        assert result == {"status": 1, "tx_hash": TX_HASH}
