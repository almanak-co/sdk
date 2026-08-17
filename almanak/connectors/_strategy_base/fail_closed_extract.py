"""Fail-closed helpers for receipt-parser extraction methods.

Receipt parsers expose legacy ``extract_*`` methods whose ``None`` return value
predates the tagged :class:`~almanak.framework.execution.extract_result.ExtractResult`
contract.  Those methods commonly catch decode exceptions and return ``None``,
which makes a malformed money-moving receipt indistinguishable from a receipt
that simply does not contain the requested event.

``FailClosedExtractMixin`` centralizes the defensive probe used by migrated
``extract_*_result`` methods.  It deliberately leaves event-presence policy to
the connector: the generic wrapper maps a successfully parsed ``None`` to
``ExtractMissing`` while parsers with stronger evidence (for example Curve's
present-but-undecodable checks) can consume ``_parse_receipt_result`` directly.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import Any, Protocol, TypeVar, cast

from almanak.framework.execution.extract_result import ExtractError, ExtractMissing, ExtractOk, ExtractResult


class ParseReceiptResult(Protocol):
    """Minimum result shape required by the fail-closed probe."""

    success: bool
    error: str | None


class _ReceiptParser(Protocol):
    def parse_receipt(self, receipt: dict[str, Any], **kwargs: Any) -> ParseReceiptResult: ...


T = TypeVar("T")


class RecognizedEventDecodeError(ValueError):
    """A recognized money event does not match its declared ABI envelope."""


def validate_recognized_event_layout(
    protocol_name: str,
    event_name: str,
    topics: Sequence[Any],
    data: Any,
    layouts: Mapping[str, Collection[tuple[int, int]]],
) -> None:
    """Reject malformed recognized events before legacy decoders can default fields."""
    expected = layouts.get(event_name)
    if expected is None:
        return

    def is_topic_word(topic: Any) -> bool:
        if isinstance(topic, bytes | bytearray):
            return len(topic) == 32
        value = str(topic)
        value = value[2:] if value.startswith(("0x", "0X")) else value
        return len(value) == 64 and all(char in "0123456789abcdefABCDEF" for char in value)

    if isinstance(data, bytes | bytearray):
        data_hex = bytes(data).hex()
    else:
        data_hex = str(data)
        data_hex = data_hex[2:] if data_hex.startswith(("0x", "0X")) else data_hex

    actual_layout = (len(topics), len(data_hex) // 64)
    data_is_words = len(data_hex) % 64 == 0 and all(char in "0123456789abcdefABCDEF" for char in data_hex)
    if actual_layout not in expected or not all(is_topic_word(topic) for topic in topics) or not data_is_words:
        expected_description = " or ".join(
            f"{topic_count} topics/{word_count} data words" for topic_count, word_count in sorted(expected)
        )
        raise RecognizedEventDecodeError(
            f"{protocol_name} {event_name} decode failed: malformed ABI layout "
            f"({actual_layout[0]} topics/{actual_layout[1]} data words, expected {expected_description})"
        )


class FailClosedExtractMixin:
    """Shared three-variant extraction contract for receipt parsers.

    Subclasses provide ``parse_receipt``.  ``_parse_receipt_result`` converts
    both raised exceptions and reported failures into ``ExtractError`` while
    retaining the original exception when one exists.  ``_wrap_extract`` then
    calls a legacy extractor only after that probe succeeds.

    ``parse_kwargs`` must contain every argument that changes parsing.  The
    extractor's own keyword arguments are passed separately through ``kwargs``;
    this keeps Pendle's intent-dependent parsing and post-parse hints distinct.
    """

    def _parse_receipt_result(
        self,
        receipt: dict[str, Any],
        parse_kwargs: Mapping[str, Any] | None = None,
    ) -> ExtractResult[ParseReceiptResult]:
        """Return a parsed receipt or a tagged accounting-critical failure."""
        try:
            parser = cast(_ReceiptParser, self)
            parsed = parser.parse_receipt(receipt, **dict(parse_kwargs or {}))
            if parsed is None:
                return ExtractError(error="parse_receipt returned None")
            if parsed.success is not True:
                return ExtractError(error=parsed.error or "parse_receipt reported failure")
            return ExtractOk(value=parsed)
        except Exception as exc:  # noqa: BLE001 - malformed receipt shape is accounting-critical
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)

    def _strict_parse(
        self,
        receipt: dict[str, Any],
        parse_kwargs: Mapping[str, Any] | None = None,
    ) -> ExtractError | None:
        """Return ``None`` after a successful probe, otherwise ``ExtractError``.

        This compatibility shape lets connector migrations remove their local
        implementation without changing existing ``*_result`` methods in the
        same commit.  New code that needs the parsed value should use
        :meth:`_parse_receipt_result`.
        """
        parsed = self._parse_receipt_result(receipt, parse_kwargs)
        if isinstance(parsed, ExtractError):
            return parsed
        return None

    def _wrap_extract(
        self,
        fn: Callable[..., T | None],
        receipt: dict[str, Any],
        missing_reason: str,
        parse_kwargs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> ExtractResult[T]:
        """Probe, invoke, and tag a legacy ``T | None`` extractor."""
        error = self._strict_parse(receipt, parse_kwargs)
        if error is not None:
            return error

        try:
            value = fn(receipt, **kwargs)
        except Exception as exc:  # noqa: BLE001 - extractor failures must fail closed
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)

        if value is None:
            return ExtractMissing(reason=missing_reason)
        return ExtractOk(value=value)


__all__ = [
    "FailClosedExtractMixin",
    "ParseReceiptResult",
    "RecognizedEventDecodeError",
    "validate_recognized_event_layout",
]
