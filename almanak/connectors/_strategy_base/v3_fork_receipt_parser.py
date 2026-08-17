"""Template-method foundation for Uniswap V3-style fork receipt parsers.

The V3-family connectors share receipt plumbing but not a single ABI.  This
module owns the invariant parts—topic dispatch, normalization, strict decode
validation, compiler metadata, token hints, and position-manager lookup—while
leaving venue codecs and result construction in concrete connector modules.

Nothing in this module imports a concrete connector.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, ClassVar, Protocol, cast

from almanak.connectors._strategy_base import v3_receipt_parser_helpers
from almanak.connectors._strategy_base.fail_closed_extract import FailClosedExtractMixin
from almanak.framework.data.tokens import build_swap_token_meta_extract_kwargs


class V3ReceiptDecodeError(ValueError):
    """A recognized accounting event could not be decoded safely."""


_V3_STANDARD_LP_EVENTS = ("Mint", "Burn", "Collect", "IncreaseLiquidity", "DecreaseLiquidity")
V3_STANDARD_LP_TOPIC_COUNTS = MappingProxyType(dict(zip(_V3_STANDARD_LP_EVENTS, (4, 4, 4, 2, 2), strict=True)))
V3_STANDARD_LP_DATA_WORDS = MappingProxyType(dict(zip(_V3_STANDARD_LP_EVENTS, (4, 3, 3, 3, 3), strict=True)))
V3_STANDARD_TRANSFER_LAYOUTS = MappingProxyType({"Transfer": frozenset({(3, 1), (4, 0)})})


class _V3Codec(Protocol):
    def _decode_log_data(
        self,
        event_name: str,
        topics: list[Any],
        data: str,
        contract_address: str,
    ) -> dict[str, Any]: ...


def _validate_strict_event_layouts(
    protocol_name: str,
    strict_layouts: Mapping[str, frozenset[tuple[int, int]]],
) -> None:
    invalid_layouts = {
        name: layouts
        for name, layouts in strict_layouts.items()
        if not layouts
        or any(
            not isinstance(layout, tuple)
            or len(layout) != 2
            or not all(isinstance(value, int) for value in layout)
            or layout[0] < 1
            or layout[1] < 0
            for layout in layouts
        )
    }
    if invalid_layouts:
        raise ValueError(f"{protocol_name}: strict event layouts must contain positive topic/non-negative word pairs")


@dataclass(frozen=True, slots=True)
class V3ForkSpec:
    """Immutable connector facts consumed by :class:`V3ForkReceiptParser`.

    ``strict_decode_fields`` identifies known events whose typed fields are
    money-moving inputs.  ``strict_topic_counts`` and ``strict_data_words``
    also protect accounting events that remain connector-decoded: their ABI
    envelope must still be structurally complete before legacy extraction can
    inspect it. ``strict_event_layouts`` expresses overloaded ABI variants as
    exact ``(topic count, data word count)`` pairs. A recognized event with an
    incomplete contract fails the receipt instead of being silently dropped or
    converted into zero values.
    """

    protocol_name: str
    event_topics: Mapping[str, str]
    event_name_to_type: Mapping[str, Any]
    position_manager_addresses: Mapping[str, str]
    strict_decode_fields: Mapping[str, frozenset[str]]
    strict_topic_counts: Mapping[str, int]
    strict_data_words: Mapping[str, int]
    strict_event_layouts: Mapping[str, frozenset[tuple[int, int]]] = field(default_factory=dict)
    default_position_manager: str = ""

    def __post_init__(self) -> None:
        protocol_name = self.protocol_name.strip()
        if not protocol_name:
            raise ValueError("protocol_name must not be empty")

        topics = {name: self._normalize_topic(topic) for name, topic in self.event_topics.items()}
        if not topics:
            raise ValueError(f"{protocol_name}: event_topics must not be empty")
        missing_types = set(topics).difference(self.event_name_to_type)
        if missing_types:
            missing = ", ".join(sorted(missing_types))
            raise ValueError(f"{protocol_name}: event type missing for {missing}")

        strict_fields = {name: frozenset(fields) for name, fields in self.strict_decode_fields.items()}
        strict_topics = dict(self.strict_topic_counts)
        strict_words = dict(self.strict_data_words)
        strict_layouts = {name: frozenset(layouts) for name, layouts in self.strict_event_layouts.items()}
        unknown_strict_events = set(strict_fields).union(strict_topics, strict_words, strict_layouts).difference(topics)
        if unknown_strict_events:
            unknown = ", ".join(sorted(unknown_strict_events))
            raise ValueError(f"{protocol_name}: strict decode event missing from topic table: {unknown}")
        invalid_topic_counts = {name: count for name, count in strict_topics.items() if count < 1}
        if invalid_topic_counts:
            raise ValueError(f"{protocol_name}: strict topic counts must be positive")
        invalid_word_counts = {name: count for name, count in strict_words.items() if count < 0}
        if invalid_word_counts:
            raise ValueError(f"{protocol_name}: strict data word counts must be non-negative")
        _validate_strict_event_layouts(protocol_name, strict_layouts)

        managers = {
            chain.strip().lower(): address.strip().lower()
            for chain, address in self.position_manager_addresses.items()
            if chain.strip() and address.strip()
        }

        object.__setattr__(self, "protocol_name", protocol_name)
        object.__setattr__(self, "event_topics", MappingProxyType(topics))
        object.__setattr__(self, "event_name_to_type", MappingProxyType(dict(self.event_name_to_type)))
        object.__setattr__(self, "position_manager_addresses", MappingProxyType(managers))
        object.__setattr__(self, "strict_decode_fields", MappingProxyType(strict_fields))
        object.__setattr__(self, "strict_topic_counts", MappingProxyType(strict_topics))
        object.__setattr__(self, "strict_data_words", MappingProxyType(strict_words))
        object.__setattr__(self, "strict_event_layouts", MappingProxyType(strict_layouts))
        object.__setattr__(self, "default_position_manager", self.default_position_manager.strip().lower())

    @staticmethod
    def _normalize_topic(topic: str) -> str:
        normalized = topic.strip().lower()
        return normalized if normalized.startswith("0x") else f"0x{normalized}"

    @property
    def topic_to_event_name(self) -> Mapping[str, str]:
        """Return normalized event-topic dispatch for this fork."""
        return MappingProxyType({topic: name for name, topic in self.event_topics.items()})

    def position_manager(self, chain: str) -> str:
        """Return this fork's chain-specific NPM, or its explicit default."""
        return self.position_manager_addresses.get(chain.strip().lower(), self.default_position_manager)


class V3ForkReceiptParser(FailClosedExtractMixin):
    """Protocol-neutral template for a V3-fork receipt parser.

    Concrete parsers set ``V3_FORK_SPEC`` and provide ``_decode_log_data`` plus
    ``_create_v3_event``.  The template deliberately raises for a malformed
    *known* strict event.  The concrete ``parse_receipt`` boundary may convert
    that exception into ``success=False``; the inherited extraction mixin then
    surfaces it as ``ExtractError``.
    """

    V3_FORK_SPEC: ClassVar[V3ForkSpec]
    chain: str

    @property
    def v3_fork_spec(self) -> V3ForkSpec:
        """Validated immutable specification declared by the connector."""
        spec = getattr(type(self), "V3_FORK_SPEC", None)
        if not isinstance(spec, V3ForkSpec):
            raise TypeError(f"{type(self).__name__} must declare V3_FORK_SPEC")
        return spec

    @property
    def protocol_name(self) -> str:
        """Human-readable protocol name used in decode failures."""
        return self.v3_fork_spec.protocol_name

    def _resolve_token_info(self, token: str) -> tuple[str, int | None]:
        """Resolve token metadata through the shared V3 helper."""
        return v3_receipt_parser_helpers.resolve_token_info(token, self.chain)

    def build_extract_kwargs(
        self,
        *,
        field: str,
        bundle_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Return canonical compiler metadata for receipt extraction."""
        return build_swap_token_meta_extract_kwargs(field=field, bundle_metadata=bundle_metadata, chain=self.chain)

    @staticmethod
    def _build_hint_map(
        swap_token_meta: dict[str, dict[str, Any]] | None,
    ) -> dict[str, tuple[str, int]]:
        """Map compiler token metadata to normalized address hints."""
        return v3_receipt_parser_helpers.build_hint_map(swap_token_meta)

    def _decode_swap_data(self, topics: list[Any], data: str, address: str) -> dict[str, Any]:
        """Decode the canonical five-word V3 Swap layout."""
        return v3_receipt_parser_helpers.decode_swap_data(topics, data, address, log=self._receipt_logger())

    def _decode_transfer_data(self, topics: list[Any], data: str, address: str) -> dict[str, Any]:
        """Decode a fungible ERC-20 Transfer layout."""
        return v3_receipt_parser_helpers.decode_transfer_data(topics, data, address, log=self._receipt_logger())

    def _nft_manager_address(self) -> str:
        """Return the configured NPM address for the parser's chain."""
        return self.v3_fork_spec.position_manager(self.chain)

    def parse_logs(self, logs: list[dict[str, Any]]) -> list[Any]:
        """Parse recognized logs through the common V3 template."""
        events: list[Any] = []
        for log in logs:
            event = self._parse_log(log, "", 0)
            if event is not None:
                events.append(event)
        return events

    def _parse_log(
        self,
        log: dict[str, Any],
        tx_hash: str,
        block_number: int,
    ) -> Any | None:
        """Normalize, dispatch, decode, validate, and create one V3 event."""
        topics = log.get("topics", [])
        if not topics:
            return None

        first_topic = self._normalize_topic(topics[0])
        event_name = self.v3_fork_spec.topic_to_event_name.get(first_topic)
        if event_name is None:
            return None

        raw_topics = [self._normalize_topic(topic) for topic in topics]
        data = self._normalize_data(log.get("data", ""))
        contract_address = self._normalize_address(log.get("address", ""))
        codec = cast(_V3Codec, self)
        decoded_data = codec._decode_log_data(event_name, topics, data, contract_address)
        self._validate_decoded_event(event_name, decoded_data, raw_topics, data)

        return self._create_v3_event(
            event_type=self.v3_fork_spec.event_name_to_type[event_name],
            event_name=event_name,
            log_index=log.get("logIndex", 0),
            tx_hash=tx_hash,
            block_number=block_number,
            contract_address=contract_address,
            decoded_data=decoded_data,
            raw_topics=raw_topics,
            raw_data=data,
        )

    def _validate_decoded_event(
        self,
        event_name: str,
        decoded_data: dict[str, Any],
        raw_topics: list[str],
        raw_data: str,
    ) -> None:
        """Reject an incomplete ABI envelope or typed accounting payload."""
        required_fields = self.v3_fork_spec.strict_decode_fields.get(event_name)
        required_topics = self.v3_fork_spec.strict_topic_counts.get(event_name)
        required_words = self.v3_fork_spec.strict_data_words.get(event_name)
        required_layouts = self.v3_fork_spec.strict_event_layouts.get(event_name)
        if required_fields is None and required_topics is None and required_words is None and required_layouts is None:
            return

        topics_are_words = all(
            len(topic) == 66 and topic.startswith("0x") and self._is_hex(topic[2:]) for topic in raw_topics
        )
        if required_layouts is not None:
            data_is_words = len(raw_data) % 64 == 0 and self._is_hex(raw_data)
            actual_layout = (len(raw_topics), len(raw_data) // 64)
            if not topics_are_words or not data_is_words or actual_layout not in required_layouts:
                expected = " or ".join(
                    f"{topic_count} topics/{data_words} data words"
                    for topic_count, data_words in sorted(required_layouts)
                )
                raise V3ReceiptDecodeError(
                    f"{self.protocol_name} {event_name} decode failed: malformed ABI layout "
                    f"({len(raw_topics)} topics/{len(raw_data) // 64} data words, expected {expected})"
                )

        if required_topics is not None:
            if len(raw_topics) != required_topics or not topics_are_words:
                raise V3ReceiptDecodeError(
                    f"{self.protocol_name} {event_name} decode failed: "
                    f"malformed topics ({len(raw_topics)} topics, expected {required_topics})"
                )

        if required_words is not None and (
            len(raw_data) < required_words * 64 or len(raw_data) % 64 != 0 or not self._is_hex(raw_data)
        ):
            raise V3ReceiptDecodeError(
                f"{self.protocol_name} {event_name} decode failed: "
                f"malformed data ({len(raw_data)} hex chars, expected at least {required_words * 64} in ABI words)"
            )

        if required_fields is not None:
            missing_fields = required_fields.difference(decoded_data)
            if "raw_data" in decoded_data or missing_fields:
                detail = "raw fallback" if "raw_data" in decoded_data else f"missing {sorted(missing_fields)}"
                raise V3ReceiptDecodeError(f"{self.protocol_name} {event_name} decode failed: {detail}")

    def _create_v3_event(
        self,
        *,
        event_type: Any,
        event_name: str,
        log_index: int,
        tx_hash: str,
        block_number: int,
        contract_address: str,
        decoded_data: dict[str, Any],
        raw_topics: list[str],
        raw_data: str,
    ) -> Any | None:
        """Create a connector-owned event value from normalized fields."""
        raise NotImplementedError

    @staticmethod
    def _normalize_topic(topic: Any) -> str:
        if isinstance(topic, bytes):
            return f"0x{topic.hex()}"
        normalized = str(topic).lower() if topic else ""
        return normalized if not normalized or normalized.startswith("0x") else f"0x{normalized}"

    @staticmethod
    def _normalize_address(address: Any) -> str:
        if isinstance(address, bytes):
            return f"0x{address.hex()}"
        return str(address).lower() if address else ""

    @staticmethod
    def _normalize_data(data: Any) -> str:
        if isinstance(data, bytes):
            return data.hex()
        if isinstance(data, str):
            return data[2:] if data.startswith("0x") else data
        return ""

    @staticmethod
    def _is_hex(value: str) -> bool:
        return all(character in "0123456789abcdefABCDEF" for character in value)

    def _receipt_logger(self) -> logging.Logger:
        return logging.getLogger(type(self).__module__)


__all__ = ["V3ForkReceiptParser", "V3ForkSpec", "V3ReceiptDecodeError"]
