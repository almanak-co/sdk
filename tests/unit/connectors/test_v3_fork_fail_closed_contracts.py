"""Cross-fork negative controls for accounting-bearing V3 log contracts."""

from __future__ import annotations

from typing import Any

import pytest

from almanak.connectors.aerodrome.receipt_parser import AerodromeSlipstreamReceiptParser
from almanak.connectors.pancakeswap_v3.receipt_parser import PancakeSwapV3ReceiptParser
from almanak.connectors.sushiswap_v3.receipt_parser import SushiSwapV3ReceiptParser
from almanak.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser
from almanak.framework.execution.extract_result import ExtractError

V3_FORK_PARSERS = [
    UniswapV3ReceiptParser(chain="arbitrum"),
    PancakeSwapV3ReceiptParser(chain="bsc"),
    SushiSwapV3ReceiptParser(chain="arbitrum"),
    AerodromeSlipstreamReceiptParser(chain="base"),
]


@pytest.mark.parametrize(
    ("parser", "event_name"),
    [
        *[
            (UniswapV3ReceiptParser(chain="arbitrum"), event_name)
            for event_name in ("IncreaseLiquidity", "DecreaseLiquidity", "Collect", "Burn")
        ],
        *[
            (PancakeSwapV3ReceiptParser(chain="bsc"), event_name)
            for event_name in ("IncreaseLiquidity", "DecreaseLiquidity", "Collect", "Burn")
        ],
        *[
            (SushiSwapV3ReceiptParser(chain="arbitrum"), event_name)
            for event_name in ("IncreaseLiquidity", "DecreaseLiquidity", "Collect", "Burn")
        ],
        *[
            (AerodromeSlipstreamReceiptParser(chain="base"), event_name)
            for event_name in ("IncreaseLiquidity", "DecreaseLiquidity", "CollectCL", "BurnCL")
        ],
    ],
)
def test_truncated_lp_accounting_event_fails_receipt(parser: Any, event_name: str) -> None:
    """One-byte LP payloads cannot become measured-zero accounting values."""
    spec = parser.v3_fork_spec
    required_topics = spec.strict_topic_counts[event_name]
    log = {
        "address": "0x" + "11" * 20,
        "topics": [spec.event_topics[event_name], *(["0x" + "22" * 32] * (required_topics - 1))],
        "data": "0x01",
        "logIndex": 0,
    }
    receipt = {
        "transactionHash": "0x" + "33" * 32,
        "blockNumber": 1,
        "status": 1,
        "logs": [log],
    }

    parsed = parser.parse_receipt(receipt)
    extracted = parser._parse_receipt_result(receipt)

    assert parsed.success is False
    assert event_name in (parsed.error or "")
    assert isinstance(extracted, ExtractError)


@pytest.mark.parametrize("parser", V3_FORK_PARSERS)
def test_malformed_overloaded_transfer_fails_receipt(parser: Any) -> None:
    """A recognized ERC-721 Transfer cannot accept a truncated token-id topic."""
    spec = parser.v3_fork_spec
    log = {
        "address": "0x" + "11" * 20,
        "topics": [
            spec.event_topics["Transfer"],
            "0x" + "22" * 32,
            "0x" + "33" * 32,
            "0x1234",
        ],
        "data": "0x",
        "logIndex": 0,
    }
    receipt = {
        "transactionHash": "0x" + "44" * 32,
        "blockNumber": 1,
        "status": 1,
        "logs": [log],
    }

    parsed = parser.parse_receipt(receipt)

    assert parsed.success is False
    assert "Transfer decode failed" in (parsed.error or "")
