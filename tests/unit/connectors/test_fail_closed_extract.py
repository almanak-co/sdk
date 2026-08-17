"""Contract tests for the shared fail-closed extraction mixin."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.fail_closed_extract import FailClosedExtractMixin
from almanak.connectors._strategy_base.v3_fork_receipt_parser import V3ForkReceiptParser
from almanak.connectors.aave_v3.receipt_parser import AaveV3ReceiptParser
from almanak.connectors.aerodrome.receipt_parser import AerodromeSlipstreamReceiptParser
from almanak.connectors.compound_v3.receipt_parser import CompoundV3ReceiptParser
from almanak.connectors.curve.receipt_parser import CurveReceiptParser
from almanak.connectors.gmx_v2.receipt_parser import GMXv2ReceiptParser
from almanak.connectors.morpho_blue.receipt_parser import MorphoBlueReceiptParser
from almanak.connectors.pancakeswap_v3.receipt_parser import PancakeSwapV3ReceiptParser
from almanak.connectors.pendle.receipt_parser import PendleReceiptParser
from almanak.connectors.sushiswap_v3.receipt_parser import SushiSwapV3ReceiptParser
from almanak.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser
from almanak.framework.execution.extract_result import ExtractError, ExtractMissing, ExtractOk


@dataclass
class _ParseResult:
    success: bool
    error: str | None = None


class _Parser(FailClosedExtractMixin):
    def __init__(self) -> None:
        self.parse_calls: list[dict[str, Any]] = []

    def parse_receipt(self, receipt: dict[str, Any], **kwargs: Any) -> _ParseResult:
        self.parse_calls.append(kwargs)
        return _ParseResult(success=True)


MIGRATED_PARSERS = (
    AaveV3ReceiptParser,
    MorphoBlueReceiptParser,
    CompoundV3ReceiptParser,
    GMXv2ReceiptParser,
    PendleReceiptParser,
    CurveReceiptParser,
    PancakeSwapV3ReceiptParser,
    AerodromeSlipstreamReceiptParser,
    UniswapV3ReceiptParser,
    SushiSwapV3ReceiptParser,
)

V3_FORK_PARSERS = (
    PancakeSwapV3ReceiptParser,
    AerodromeSlipstreamReceiptParser,
    UniswapV3ReceiptParser,
    SushiSwapV3ReceiptParser,
)


def test_migrated_parsers_share_fail_closed_foundation() -> None:
    assert all(issubclass(parser, FailClosedExtractMixin) for parser in MIGRATED_PARSERS)


def test_v3_forks_share_template_and_do_not_redeclare_strict_parse() -> None:
    assert all(issubclass(parser, V3ForkReceiptParser) for parser in V3_FORK_PARSERS)
    assert all("_strict_parse" not in parser.__dict__ for parser in MIGRATED_PARSERS)


def test_strict_parse_preserves_raised_exception() -> None:
    parser = _Parser()
    error = ValueError("malformed money event")

    def _raise(_receipt: dict[str, Any], **_kwargs: Any) -> _ParseResult:
        raise error

    parser.parse_receipt = _raise  # type: ignore[method-assign]
    result = parser._strict_parse({"logs": []})

    assert isinstance(result, ExtractError)
    assert result.exception is error
    assert result.error == "ValueError: malformed money event"


def test_strict_parse_rejects_reported_failure() -> None:
    parser = _Parser()
    parser.parse_receipt = lambda _receipt, **_kwargs: _ParseResult(False, "decode failed")  # type: ignore[method-assign]

    result = parser._strict_parse({"logs": []})

    assert isinstance(result, ExtractError)
    assert result.error == "decode failed"


def test_parse_receipt_result_rejects_none() -> None:
    parser = _Parser()
    parser.parse_receipt = lambda _receipt, **_kwargs: None  # type: ignore[method-assign,return-value]

    result = parser._parse_receipt_result({"logs": []})

    assert isinstance(result, ExtractError)
    assert result.error == "parse_receipt returned None"


def test_parse_receipt_result_tags_malformed_result_shape() -> None:
    parser = _Parser()
    parser.parse_receipt = lambda _receipt, **_kwargs: object()  # type: ignore[method-assign,return-value]

    result = parser._parse_receipt_result({"logs": []})

    assert isinstance(result, ExtractError)
    assert "AttributeError" in result.error


def test_wrap_extract_forwards_parse_and_extractor_kwargs() -> None:
    parser = _Parser()
    seen: dict[str, Any] = {}

    def _extract(_receipt: dict[str, Any], **kwargs: Any) -> int:
        seen.update(kwargs)
        return 0

    result = parser._wrap_extract(
        _extract,
        {"logs": []},
        "event absent",
        {"intent_swap_type": "PT_TO_YT"},
        expected_out=7,
    )

    assert isinstance(result, ExtractOk)
    assert result.value == 0
    assert parser.parse_calls == [{"intent_swap_type": "PT_TO_YT"}]
    assert seen == {"expected_out": 7}


def test_wrap_extract_tags_none_as_missing() -> None:
    parser = _Parser()

    result = parser._wrap_extract(lambda _receipt: None, {"logs": []}, "event absent")

    assert result == ExtractMissing(reason="event absent")


def test_wrap_extract_preserves_extractor_exception() -> None:
    parser = _Parser()
    error = RuntimeError("extract failed")

    def _raise(_receipt: dict[str, Any]) -> int:
        raise error

    result = parser._wrap_extract(_raise, {"logs": []}, "event absent")

    assert isinstance(result, ExtractError)
    assert result.exception is error
    assert result.error == "RuntimeError: extract failed"


def test_pancake_and_sushi_swap_variants_preserve_enricher_kwargs() -> None:
    metadata = {
        "token_in": {"address": "0x" + "11" * 20, "symbol": "IN", "decimals": 6},
        "token_out": {"address": "0x" + "22" * 20, "symbol": "OUT", "decimals": 18},
    }

    def recorder(target: dict[str, Any]):
        def extract(_receipt: dict[str, Any], **kwargs: Any) -> int:
            target.update(kwargs)
            return 1

        return extract

    for parser in (
        PancakeSwapV3ReceiptParser(chain="bsc"),
        SushiSwapV3ReceiptParser(chain="arbitrum"),
    ):
        seen: dict[str, Any] = {}
        parser.parse_receipt = lambda _receipt, **_kwargs: _ParseResult(True)  # type: ignore[method-assign]
        parser.extract_swap_amounts = recorder(seen)  # type: ignore[method-assign]
        result = parser.extract_swap_amounts_result(
            {"logs": []},
            expected_out=Decimal("1.25"),
            swap_token_meta=metadata,
        )

        assert isinstance(result, ExtractOk)
        assert seen == {"expected_out": Decimal("1.25"), "swap_token_meta": metadata}
