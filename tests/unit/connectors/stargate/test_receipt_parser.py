"""Characterization tests for StargateReceiptParser.

These tests pin CURRENT behavior. They are the regression contract for any future
refactor of the parser. Do not change parser source in this PR.

Key pattern: _resolve_decimals imports get_token_resolver inside the method.
All tests that reach that code path MUST stub it via monkeypatch.
"""

from decimal import Decimal

import pytest

from almanak.connectors.stargate.adapter import STARGATE_ROUTER_ADDRESSES
from almanak.connectors.stargate.receipt_parser import (
    OFT_SENT_TOPIC,
    TRANSFER_EVENT_SIGNATURE,
    StargateReceiptParser,
)


# ---------------------------------------------------------------------------
# Hex helpers
# ---------------------------------------------------------------------------


def word(v: int) -> str:
    """One 32-byte ABI word as 64 hex chars (no 0x)."""
    return f"{v:064x}"


def addr_topic(a: str) -> str:
    """Address as an indexed topic (0x-prefixed 32-byte word)."""
    return "0x" + a.lower().replace("0x", "").zfill(64)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
WALLET = "0x1111111111111111111111111111111111111111"
STARGATE_ETH_USDC_POOL = STARGATE_ROUTER_ADDRESSES[1]["USDC"]
TX_HASH = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

DST_EID_ARBITRUM = 30110  # maps to "arbitrum"
GUID = "0x" + "aa" * 32


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
# Log helpers
# ---------------------------------------------------------------------------


def make_oft_sent_log(
    dst_eid: int = DST_EID_ARBITRUM,
    amount_sent: int = 2_000_000,
    amount_received: int = 1_999_000,
) -> dict:
    """OFTSent(bytes32 indexed guid, uint32 dstEid, address indexed fromAddress,
    uint256 amountSentLD, uint256 amountReceivedLD).

    Indexed: guid (topics[1]), fromAddress (topics[2]).
    Data: dstEid, amountSentLD, amountReceivedLD (3 words).
    """
    return {
        "address": STARGATE_ETH_USDC_POOL,
        "topics": [
            OFT_SENT_TOPIC,
            GUID,              # guid (indexed bytes32)
            addr_topic(WALLET),  # fromAddress (indexed)
        ],
        "data": "0x" + word(dst_eid) + word(amount_sent) + word(amount_received),
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
    def test_oft_sent_fully_decoded(self, stub_resolver):
        parser = StargateReceiptParser(chain="ethereum")
        receipt = make_receipt([make_oft_sent_log()])
        result = parser.extract_bridge_data(receipt, token="usdc")

        assert result is not None
        assert result.amount_sent_raw == 2_000_000
        assert result.amount_sent == Decimal("2")
        assert result.destination_chain == "arbitrum"
        assert result.source_chain == "ethereum"
        assert result.bridge_name == "stargate"
        assert result.destination_token_address is None  # OFTSent does not encode dst token

    def test_source_token_address_is_none_on_oft_sent_only_path(self, stub_resolver):
        """OFTSent does not carry source token address — source_token_address is None
        when there is no Transfer fallback log."""
        parser = StargateReceiptParser(chain="ethereum")
        receipt = make_receipt([make_oft_sent_log()])
        result = parser.extract_bridge_data(receipt, token="usdc")

        # source_token_address is None because OFTSent doesn't carry it
        assert result is not None
        assert result.source_token_address is None


# ---------------------------------------------------------------------------
# Failure gates
# ---------------------------------------------------------------------------


class TestFailureGates:
    def test_status_not_1_returns_none(self, stub_resolver):
        parser = StargateReceiptParser(chain="ethereum")
        receipt = make_receipt([make_oft_sent_log()], status=0)
        assert parser.extract_bridge_data(receipt, token="usdc") is None

    def test_empty_logs_returns_none(self, stub_resolver):
        parser = StargateReceiptParser(chain="ethereum")
        receipt = make_receipt([])
        assert parser.extract_bridge_data(receipt, token="usdc") is None

    def test_unknown_eid_falls_back_to_string(self, stub_resolver):
        """Unknown dst eid is stringified, not rejected."""
        parser = StargateReceiptParser(chain="ethereum")
        receipt = make_receipt([make_oft_sent_log(dst_eid=999_999)])
        result = parser.extract_bridge_data(receipt, token="usdc")
        assert result is not None
        assert result.destination_chain == "999999"

    def test_no_source_chain_returns_none(self, stub_resolver):
        """No chain in constructor + no from_chain hint -> None."""
        parser = StargateReceiptParser()
        receipt = make_receipt([make_oft_sent_log()])
        assert parser.extract_bridge_data(receipt, token="usdc") is None

    def test_decimals_unresolvable_returns_none(self, failing_resolver):
        parser = StargateReceiptParser(chain="ethereum")
        receipt = make_receipt([make_oft_sent_log()])
        assert parser.extract_bridge_data(receipt, token="usdc") is None

    def test_truncated_oft_sent_data_two_words_skipped(self, stub_resolver):
        """OFTSent data requires >= 3 words (192 hex chars). With only 2 words,
        the log is skipped and the result is None (no fallback transfer either)."""
        truncated_log = {
            "address": STARGATE_ETH_USDC_POOL,
            "topics": [OFT_SENT_TOPIC, GUID, addr_topic(WALLET)],
            "data": "0x" + word(DST_EID_ARBITRUM) + word(2_000_000),  # 2 words only
            "logIndex": 0,
        }
        parser = StargateReceiptParser(chain="ethereum")
        receipt = make_receipt([truncated_log])
        assert parser.extract_bridge_data(receipt, token="usdc") is None


# ---------------------------------------------------------------------------
# Transfer fallback
# ---------------------------------------------------------------------------


class TestTransferFallback:
    def test_wallet_transfer_to_stargate_pool_used_as_fallback(self, stub_resolver):
        """When OFTSent is absent, a Transfer from wallet to Stargate pool
        provides amount_sent_raw and source_token_address."""
        transfer_log = {
            "address": USDC,
            "topics": [
                TRANSFER_EVENT_SIGNATURE,
                addr_topic(WALLET),
                addr_topic(STARGATE_ETH_USDC_POOL),
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
        parser = StargateReceiptParser(chain="ethereum")
        result = parser.extract_bridge_data(receipt, token="usdc", to_chain="arbitrum")

        assert result is not None
        assert result.amount_sent_raw == 3_000_000
        assert result.source_token_address == USDC.lower()


# ---------------------------------------------------------------------------
# parse_receipt (minimal shape)
# ---------------------------------------------------------------------------


class TestParseReceiptShape:
    def test_parse_receipt_returns_status_and_tx_hash(self):
        parser = StargateReceiptParser(chain="ethereum")
        receipt = {"status": 1, "transactionHash": TX_HASH}
        result = parser.parse_receipt(receipt)

        assert result == {"status": 1, "tx_hash": TX_HASH}
