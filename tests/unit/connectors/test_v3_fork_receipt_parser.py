"""Contract tests for the V3-fork receipt-parser template."""

from __future__ import annotations

from dataclasses import replace
from enum import Enum
from typing import Any

import pytest

from almanak.connectors._strategy_base.v3_fork_receipt_parser import (
    V3_STANDARD_TRANSFER_LAYOUTS,
    V3ForkReceiptParser,
    V3ForkSpec,
    V3ReceiptDecodeError,
)


class _EventType(Enum):
    SWAP = "swap"
    TRANSFER = "transfer"


SWAP_TOPIC = "0x" + "11" * 32
TRANSFER_TOPIC = "0x" + "22" * 32
NPM = "0x" + "33" * 20
DEFAULT_NPM = "0x" + "44" * 20


SPEC = V3ForkSpec(
    protocol_name="Test V3",
    event_topics={"Swap": SWAP_TOPIC.upper(), "Transfer": TRANSFER_TOPIC},
    event_name_to_type={"Swap": _EventType.SWAP, "Transfer": _EventType.TRANSFER},
    position_manager_addresses={"Arbitrum": NPM.upper()},
    strict_decode_fields={"Swap": frozenset({"amount0", "amount1"})},
    strict_topic_counts={"Swap": 1},
    strict_data_words={"Swap": 1},
    strict_event_layouts=V3_STANDARD_TRANSFER_LAYOUTS,
    default_position_manager=DEFAULT_NPM.upper(),
)


class _Parser(V3ForkReceiptParser):
    V3_FORK_SPEC = SPEC

    def __init__(self, decoded: dict[str, Any]) -> None:
        self.chain = "arbitrum"
        self.decoded = decoded

    def _decode_log_data(
        self,
        event_name: str,
        topics: list[Any],
        data: str,
        contract_address: str,
    ) -> dict[str, Any]:
        return self.decoded

    def _create_v3_event(self, **kwargs: Any) -> dict[str, Any]:
        return kwargs


def _log(topic: str = SWAP_TOPIC, *, topics: list[str] | None = None, data: str | None = None) -> dict[str, Any]:
    return {
        "topics": topics or [topic],
        "data": data or "0x" + "12" * 32,
        "address": ("0x" + "AA" * 20),
        "logIndex": 7,
    }


def test_spec_normalizes_topics_managers_and_default() -> None:
    assert SPEC.event_topics["Swap"] == SWAP_TOPIC
    assert SPEC.position_manager("ARBITRUM") == NPM
    assert SPEC.position_manager("base") == DEFAULT_NPM


@pytest.mark.parametrize(
    "layouts",
    [
        frozenset(),
        frozenset({"not-a-tuple"}),
        frozenset({(3,)}),
        frozenset({(3, "one")}),
        frozenset({(0, 1)}),
        frozenset({(3, -1)}),
    ],
)
def test_spec_rejects_malformed_strict_event_layouts(layouts: Any) -> None:
    with pytest.raises(
        ValueError,
        match="Test V3: strict event layouts must contain positive topic/non-negative word pairs",
    ):
        replace(SPEC, strict_event_layouts={"Transfer": layouts})


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            {"strict_event_layouts": {"Unknown": frozenset()}},
            "Test V3: strict decode event missing from topic table: Unknown",
        ),
        (
            {
                "strict_topic_counts": {"Swap": 0},
                "strict_event_layouts": {"Transfer": frozenset()},
            },
            "Test V3: strict topic counts must be positive",
        ),
        (
            {
                "strict_data_words": {"Swap": -1},
                "strict_event_layouts": {"Transfer": frozenset()},
            },
            "Test V3: strict data word counts must be non-negative",
        ),
    ],
)
def test_spec_validation_precedes_strict_layout_validation(overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        replace(SPEC, **overrides)


def test_spec_copies_and_freezes_strict_event_layouts() -> None:
    source_layouts = {(3, 1)}
    spec = replace(SPEC, strict_event_layouts={"Transfer": source_layouts})

    source_layouts.add((4, 0))

    assert spec.strict_event_layouts["Transfer"] == frozenset({(3, 1)})
    with pytest.raises(TypeError):
        spec.strict_event_layouts["Transfer"] = frozenset({(4, 0)})  # type: ignore[index]


def test_template_ignores_unknown_events() -> None:
    parser = _Parser({"amount0": 1, "amount1": -1})

    assert parser._parse_log(_log("0x" + "ff" * 32), "0xtx", 1) is None


def test_template_normalizes_and_creates_known_event() -> None:
    parser = _Parser({"amount0": 1, "amount1": -1})

    event = parser._parse_log(_log(), "0xtx", 9)

    assert event["event_type"] is _EventType.SWAP
    assert event["event_name"] == "Swap"
    assert event["log_index"] == 7
    assert event["block_number"] == 9
    assert event["contract_address"] == "0x" + "aa" * 20
    assert event["raw_data"] == "12" * 32


@pytest.mark.parametrize(
    "decoded",
    [
        {"raw_data": "1234"},
        {"amount0": 1},
    ],
)
def test_template_rejects_malformed_known_money_event(decoded: dict[str, Any]) -> None:
    parser = _Parser(decoded)

    with pytest.raises(V3ReceiptDecodeError, match="Test V3 Swap decode failed"):
        parser._parse_log(_log(), "0xtx", 1)


@pytest.mark.parametrize(
    ("topics", "data"),
    [
        ([TRANSFER_TOPIC, "0x" + "34" * 32, "0x" + "56" * 32], "0x" + "78" * 32),
        ([TRANSFER_TOPIC, "0x" + "34" * 32, "0x" + "56" * 32, "0x" + "78" * 32], "0x"),
    ],
)
def test_overloaded_transfer_accepts_erc20_and_erc721_layouts(topics: list[str], data: str) -> None:
    parser = _Parser({"raw_data": ""})

    event = parser._parse_log(
        _log(topics=topics, data=data),
        "0xtx",
        1,
    )

    assert event["event_name"] == "Transfer"


@pytest.mark.parametrize(
    "log",
    [
        _log(TRANSFER_TOPIC),
        _log(topics=[TRANSFER_TOPIC, "0x" + "34" * 32, "0x1234"]),
        _log(
            topics=[TRANSFER_TOPIC, "0x" + "34" * 32, "0x" + "56" * 32],
            data="0x12",
        ),
        _log(
            topics=[TRANSFER_TOPIC, "0x" + "34" * 32, "0x" + "56" * 32, "0x1234"],
            data="0x",
        ),
    ],
)
def test_structure_only_strict_event_rejects_malformed_abi(log: dict[str, Any]) -> None:
    parser = _Parser({"raw_data": ""})

    with pytest.raises(V3ReceiptDecodeError, match="Test V3 Transfer decode failed"):
        parser._parse_log(log, "0xtx", 1)


def test_parse_logs_uses_template() -> None:
    parser = _Parser({"amount0": 1, "amount1": -1})

    events = parser.parse_logs([_log(), _log("0x" + "ff" * 32)])

    assert len(events) == 1


def test_position_manager_comes_from_spec() -> None:
    parser = _Parser({"amount0": 1, "amount1": -1})
    assert parser._nft_manager_address() == NPM

    parser.chain = "base"
    assert parser._nft_manager_address() == DEFAULT_NPM
