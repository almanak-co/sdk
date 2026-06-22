"""Tagged-variant (``ExtractResult``) extractor tests for the Pendle parser — VIB-5354.

These tests pin the three-way contract on the four migrated Pendle extractors
(``extract_position_id``, ``extract_lp_open_data``, ``extract_lp_close_data``,
``extract_primitive_money_legs``):

  * happy path  -> ``ExtractOk`` wrapping *exactly* the value the legacy raw
                   method returns (no behaviour drift for the success path);
  * absent event -> ``ExtractMissing`` where the legacy method returned ``None``;
  * parse error -> ``ExtractError`` — distinct from ``ExtractMissing``. A silent
                   parse failure on a money path must never be indistinguishable
                   from "no event" (the ghost-position failure mode VIB-3159 /
                   VIB-5354 closes).

It also asserts the ``ResultEnricher`` consumes the tagged variants directly:
a Pendle parse error is *surfaced* (paper mode -> warning + counter; live mode
-> ``CriticalAccountingError``) rather than dropped, and no backward-compat
``DeprecationWarning`` is emitted for Pendle.
"""

from __future__ import annotations

import warnings

import pytest

from almanak.connectors.pendle import EVENT_TOPICS, PendleReceiptParser
from almanak.framework.execution.extract_result import (
    CriticalAccountingError,
    ExtractError,
    ExtractMissing,
    ExtractOk,
)
from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData

MARKET = "0x" + "cd" * 20
WALLET = "0x" + "11" * 20


def _receipt(logs: list[dict] | None = None, status: int = 1) -> dict:
    return {
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 12345678,
        "status": status,
        "logs": logs or [],
        "gasUsed": 200000,
    }


def _mint_log(net_lp: int, net_sy: int, net_pt: int) -> dict:
    receiver = "0x" + WALLET.lower().replace("0x", "").zfill(64)
    data = "0x" + hex(net_lp)[2:].zfill(64) + hex(net_sy)[2:].zfill(64) + hex(net_pt)[2:].zfill(64)
    return {"topics": [EVENT_TOPICS["Mint"], receiver], "data": data, "logIndex": 0, "address": MARKET}


def _burn_log(net_lp: int, net_sy: int, net_pt: int) -> dict:
    rcv = "0x" + WALLET.lower().replace("0x", "").zfill(64)
    data = "0x" + hex(net_lp)[2:].zfill(64) + hex(net_sy)[2:].zfill(64) + hex(net_pt)[2:].zfill(64)
    return {"topics": [EVENT_TOPICS["Burn"], rcv, rcv], "data": data, "logIndex": 0, "address": MARKET}


@pytest.fixture
def parser() -> PendleReceiptParser:
    return PendleReceiptParser(chain="arbitrum")


# ---------------------------------------------------------------------------
# Happy path: ExtractOk wraps exactly the legacy raw value.
# ---------------------------------------------------------------------------


def test_position_id_result_ok_matches_raw(parser: PendleReceiptParser) -> None:
    receipt = _receipt([_mint_log(10**18, 2 * 10**18, 3 * 10**18)])
    result = parser.extract_position_id_result(receipt)
    assert isinstance(result, ExtractOk)
    assert result.value == parser.extract_position_id(receipt)
    assert result.value == MARKET.lower()


def test_lp_open_data_result_ok_matches_raw(parser: PendleReceiptParser) -> None:
    receipt = _receipt([_mint_log(10**18, 2 * 10**18, 3 * 10**18)])
    result = parser.extract_lp_open_data_result(receipt)
    assert isinstance(result, ExtractOk)
    assert isinstance(result.value, LPOpenData)
    assert result.value == parser.extract_lp_open_data(receipt)


def test_lp_close_data_result_ok_matches_raw(parser: PendleReceiptParser) -> None:
    receipt = _receipt([_burn_log(10**18, 2 * 10**18, 3 * 10**18)])
    result = parser.extract_lp_close_data_result(receipt)
    assert isinstance(result, ExtractOk)
    assert isinstance(result.value, LPCloseData)
    assert result.value == parser.extract_lp_close_data(receipt)


def test_primitive_money_legs_result_ok_forwards_kwargs_and_wraps(parser: PendleReceiptParser) -> None:
    # money_legs is the only migrated method that takes extractor kwargs the
    # enricher threads (pt_address / out_token_*). Pin that the _result wrapper
    # forwards them verbatim and wraps the legacy return in ExtractOk. Stub the
    # legacy method (rather than craft a producing receipt) so the assertion is
    # exact and independent of redeem-receipt shape.
    sentinel = object()
    seen: dict = {}

    def _raw(receipt, **kwargs):  # noqa: ANN001, ANN003
        seen.update(kwargs)
        return sentinel

    parser.extract_primitive_money_legs = _raw  # type: ignore[method-assign]
    result = parser.extract_primitive_money_legs_result(
        _receipt([_burn_log(10**18, 2 * 10**18, 3 * 10**18)]),
        pt_address="0x" + "22" * 20,
        out_token_symbol="USDC",
        out_token_address="0x" + "33" * 20,
        out_token_decimals=6,
    )
    assert isinstance(result, ExtractOk)
    assert result.value is sentinel
    assert seen == {
        "pt_address": "0x" + "22" * 20,
        "out_token_symbol": "USDC",
        "out_token_address": "0x" + "33" * 20,
        "out_token_decimals": 6,
    }


# ---------------------------------------------------------------------------
# Absent event: ExtractMissing where the legacy method returned None.
# ---------------------------------------------------------------------------


def test_position_id_result_missing_on_empty_receipt(parser: PendleReceiptParser) -> None:
    receipt = _receipt([])
    assert parser.extract_position_id(receipt) is None
    assert isinstance(parser.extract_position_id_result(receipt), ExtractMissing)


def test_lp_open_data_result_missing_on_burn_only(parser: PendleReceiptParser) -> None:
    receipt = _receipt([_burn_log(10**18, 2 * 10**18, 3 * 10**18)])
    assert parser.extract_lp_open_data(receipt) is None
    assert isinstance(parser.extract_lp_open_data_result(receipt), ExtractMissing)


def test_lp_close_data_result_missing_on_mint_only(parser: PendleReceiptParser) -> None:
    receipt = _receipt([_mint_log(10**18, 2 * 10**18, 3 * 10**18)])
    assert parser.extract_lp_close_data(receipt) is None
    assert isinstance(parser.extract_lp_close_data_result(receipt), ExtractMissing)


def test_primitive_money_legs_result_missing_on_unresolvable_redeem(parser: PendleReceiptParser) -> None:
    # No Burn (so not an LP close) and an unknown PT address -> the extractor
    # declines to declare typed legs and returns None (legacy contract). That is
    # a benign ExtractMissing, NOT an ExtractError.
    receipt = _receipt([])
    legs = parser.extract_primitive_money_legs(receipt, pt_address="0x" + "22" * 20, out_token_symbol="USDC")
    assert legs is None
    result = parser.extract_primitive_money_legs_result(receipt, pt_address="0x" + "22" * 20, out_token_symbol="USDC")
    assert isinstance(result, ExtractMissing)


# ---------------------------------------------------------------------------
# Parse error: ExtractError, distinct from ExtractMissing.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "method_name",
    [
        "extract_position_id_result",
        "extract_lp_open_data_result",
        "extract_lp_close_data_result",
        "extract_primitive_money_legs_result",
    ],
)
def test_result_variant_returns_extract_error_on_parse_crash(parser: PendleReceiptParser, method_name: str) -> None:
    """A crash inside ``parse_receipt`` must surface as ``ExtractError`` (with the
    original exception attached), NOT ``ExtractMissing`` and NOT a propagated raw
    exception."""

    def _boom(*_args, **_kwargs):
        raise ValueError("decode boom — wrong ABI / malformed data")

    parser.parse_receipt = _boom  # type: ignore[method-assign]

    method = getattr(parser, method_name)
    result = method(_receipt([_mint_log(10**18, 1, 1)]))

    assert isinstance(result, ExtractError), f"{method_name} returned {type(result).__name__}, expected ExtractError"
    assert not isinstance(result, ExtractMissing)
    assert "ValueError" in result.error
    assert isinstance(result.exception, ValueError)


def test_result_variant_returns_extract_error_on_reported_failure(parser: PendleReceiptParser) -> None:
    """``parse_receipt`` swallows decode exceptions internally and returns
    ``ParseResult(success=False)``; that reported failure must also become
    ``ExtractError``, not ``ExtractMissing`` (e.g. a non-dict receipt)."""
    not_a_dict_receipt = ["logs"]  # parse_receipt -> AttributeError -> success=False
    result = parser.extract_position_id_result(not_a_dict_receipt)  # type: ignore[arg-type]
    assert isinstance(result, ExtractError)
    assert not isinstance(result, ExtractMissing)


# ---------------------------------------------------------------------------
# Enricher surfaces the error (does not silently drop the leg) and emits no
# DeprecationWarning for Pendle.
# ---------------------------------------------------------------------------


def _raising_parser() -> PendleReceiptParser:
    parser = PendleReceiptParser(chain="arbitrum")

    def _boom(*_args, **_kwargs):
        raise ValueError("decode boom — wrong ABI / malformed data")

    parser.parse_receipt = _boom  # type: ignore[method-assign]
    return parser


def _exec_result():
    from dataclasses import dataclass, field
    from typing import Any

    @dataclass
    class _Result:
        success: bool = True
        position_id: Any = None
        lp_open_data: Any = None
        lp_close_data: Any = None
        extracted_data: dict = field(default_factory=dict)
        extraction_warnings: list = field(default_factory=list)

    return _Result()


def test_enricher_paper_mode_surfaces_pendle_parse_error() -> None:
    """Paper mode: a Pendle parse error is surfaced as a warning + counter on the
    enricher, never silently dropped."""
    from almanak.framework.execution.result_enricher import ResultEnricher

    enricher = ResultEnricher(live_mode=False)
    result = _exec_result()
    receipt = _receipt([_mint_log(10**18, 1, 1)])

    enricher._extract_field(result, _raising_parser(), [receipt], "position_id", "LP_OPEN", protocol="pendle")

    assert enricher.extract_error_count == 1
    assert any("position_id" in w for w in result.extraction_warnings), result.extraction_warnings
    assert result.position_id is None  # nothing fabricated


def test_enricher_live_mode_raises_on_pendle_parse_error() -> None:
    """Live mode: a Pendle parse error escalates to ``CriticalAccountingError``
    rather than producing a ghost (empty) position."""
    from almanak.framework.execution.result_enricher import ResultEnricher

    enricher = ResultEnricher(live_mode=True)
    result = _exec_result()
    receipt = _receipt([_mint_log(10**18, 1, 1)])

    with pytest.raises(CriticalAccountingError) as excinfo:
        enricher._extract_field(result, _raising_parser(), [receipt], "position_id", "LP_OPEN", protocol="pendle")

    assert excinfo.value.field_name == "position_id"
    assert excinfo.value.protocol == "pendle"


def test_enricher_emits_no_deprecation_warning_for_pendle() -> None:
    """The enricher prefers the migrated ``extract_*_result`` variants, so a
    successful Pendle extraction emits no legacy ``DeprecationWarning``."""
    from almanak.framework.execution.result_enricher import ResultEnricher

    enricher = ResultEnricher(live_mode=False)
    parser = PendleReceiptParser(chain="arbitrum")
    result = _exec_result()
    receipt = _receipt([_mint_log(10**18, 2 * 10**18, 3 * 10**18)])

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        enricher._extract_field(result, parser, [receipt], "position_id", "LP_OPEN", protocol="pendle")
        enricher._extract_field(result, parser, [receipt], "lp_open_data", "LP_OPEN", protocol="pendle")

    assert result.position_id == MARKET.lower()
    assert isinstance(result.extracted_data["lp_open_data"], LPOpenData)
