"""Tests for VIB-3159: fail-closed ResultEnricher + three-variant extract contract.

Verifies:
1. ExtractOk / ExtractMissing / ExtractError dispatch correctly in live and paper modes.
2. Live mode raises CriticalAccountingError (which is an Exception, so it
   is caught by the runner's except-Exception recovery handlers and converted
   to ACCOUNTING_FAILED — VIB-3180).
3. Paper mode downgrades ExtractError to a structured warning + counter.
4. Backward compatibility: parsers that still return raw None / value keep
   working and emit a one-shot DeprecationWarning.
5. Five retrofitted connectors (uniswap_v3, aerodrome, aave_v3, morpho_blue,
   gmx_v2) expose the ``extract_{field}_result`` tagged variant.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pytest

from almanak.connectors.aave_v3.receipt_parser import AaveV3ReceiptParser
from almanak.connectors.aerodrome.receipt_parser import AerodromeReceiptParser
from almanak.connectors.gmx_v2.receipt_parser import GMXv2ReceiptParser
from almanak.connectors.morpho_blue.receipt_parser import MorphoBlueReceiptParser
from almanak.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser
from almanak.framework.execution.extract_result import (
    CriticalAccountingError,
    ExtractError,
    ExtractMissing,
    ExtractOk,
)
from almanak.framework.execution.extracted_data import SwapAmounts
from almanak.framework.execution.receipt_registry import ReceiptParserRegistry
from almanak.framework.execution.result_enricher import ResultEnricher

# ---------------------------------------------------------------------------
# Minimal stubs — mirror tests/unit/execution/test_result_enricher.py
# ---------------------------------------------------------------------------


@dataclass
class _FakeReceipt:
    tx_hash: str = "0xabc123"
    block_number: int = 100
    block_hash: str = "0xblock"
    gas_used: int = 200000
    effective_gas_price: int = 1000000000
    status: int = 1
    logs: list = field(default_factory=list)
    from_address: str | None = None
    to_address: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "tx_hash": self.tx_hash,
            "block_number": self.block_number,
            "block_hash": self.block_hash,
            "gas_used": self.gas_used,
            "effective_gas_price": str(self.effective_gas_price),
            "status": self.status,
            "logs": self.logs,
            "contract_address": None,
            "from_address": self.from_address,
            "to_address": self.to_address,
        }


@dataclass
class _FakeTxResult:
    success: bool = True
    tx_hash: str = "0xabc123"
    receipt: _FakeReceipt | None = None
    gas_used: int = 200000


@dataclass
class _FakeExecResult:
    success: bool = True
    transaction_results: list = field(default_factory=list)
    position_id: int | str | None = None
    swap_amounts: SwapAmounts | None = None
    lp_close_data: Any = None
    extracted_data: dict = field(default_factory=dict)
    extraction_warnings: list = field(default_factory=list)


@dataclass
class _FakeContext:
    chain: str = "arbitrum"
    protocol: str | None = None


@dataclass
class _FakeIntent:
    intent_type: str = "SWAP"
    protocol: str | None = None


def _make_result(receipt: _FakeReceipt | None = None) -> _FakeExecResult:
    """Construct a successful ExecutionResult carrying one receipt."""
    return _FakeExecResult(
        success=True,
        transaction_results=[_FakeTxResult(receipt=receipt or _FakeReceipt())],
    )


# ---------------------------------------------------------------------------
# Fake parsers exercising each variant
# ---------------------------------------------------------------------------


class _OkParser:
    """Returns ExtractOk for swap_amounts."""

    def extract_swap_amounts_result(self, receipt: dict[str, Any]) -> Any:
        return ExtractOk(
            value=SwapAmounts(
                amount_in=1,
                amount_out=2,
                amount_in_decimal=Decimal("0.001"),
                amount_out_decimal=Decimal("0.002"),
            )
        )


class _MissingParser:
    """Returns ExtractMissing — benign."""

    def extract_swap_amounts_result(self, receipt: dict[str, Any]) -> Any:
        return ExtractMissing(reason="no swap event")


class _ErrorReturnParser:
    """Returns ExtractError explicitly."""

    def extract_swap_amounts_result(self, receipt: dict[str, Any]) -> Any:
        return ExtractError(error="malformed log shape")


class _ErrorRaiseParser:
    """Raises a plain exception inside the tagged method."""

    def extract_swap_amounts_result(self, receipt: dict[str, Any]) -> Any:
        raise ValueError("corrupt receipt")


class _LegacyOkParser:
    """Legacy contract: returns the raw value directly."""

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> Any:
        return SwapAmounts(
            amount_in=1,
            amount_out=2,
            amount_in_decimal=Decimal("0.001"),
            amount_out_decimal=Decimal("0.002"),
        )


class _LegacyNoneParser:
    """Legacy contract: returns None — ambiguous between missing and error."""

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> Any:
        return None


class _LegacyRaiseParser:
    """Legacy parser that crashes."""

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> Any:
        raise RuntimeError("legacy parser crashed")


def _registry_with(parser_instance: Any) -> ReceiptParserRegistry:
    """Build a registry whose ``fakeproto`` always returns ``parser_instance``."""

    registry = ReceiptParserRegistry()

    # Monkeypatch get() to always return our instance regardless of kwargs.
    # The real registry re-instantiates on any non-empty kwargs (chain=...),
    # which would bypass a cached instance.
    def _fake_get(protocol: str, **kwargs: Any) -> Any:  # noqa: ARG001
        return parser_instance

    registry.get = _fake_get  # type: ignore[assignment]
    return registry


# ---------------------------------------------------------------------------
# Core enricher behavior
# ---------------------------------------------------------------------------


def test_extract_ok_attaches_value() -> None:
    enricher = ResultEnricher(parser_registry=_registry_with(_OkParser()), live_mode=True)
    intent = _FakeIntent(intent_type="SWAP", protocol="fakeproto")
    result = _make_result()

    enriched = enricher.enrich(result, intent, _FakeContext())

    assert enriched.swap_amounts is not None
    assert enriched.swap_amounts.amount_in == 1
    assert enriched.extraction_warnings == []
    assert enricher.extract_error_count == 0


def test_extract_missing_is_noop() -> None:
    enricher = ResultEnricher(parser_registry=_registry_with(_MissingParser()), live_mode=True)
    intent = _FakeIntent(intent_type="SWAP", protocol="fakeproto")
    result = _make_result()

    enriched = enricher.enrich(result, intent, _FakeContext())

    assert enriched.swap_amounts is None
    assert enriched.extraction_warnings == []
    # Missing is not an error, so the counter stays at 0.
    assert enricher.extract_error_count == 0


def test_extract_error_live_mode_raises_critical() -> None:
    enricher = ResultEnricher(parser_registry=_registry_with(_ErrorReturnParser()), live_mode=True)
    intent = _FakeIntent(intent_type="SWAP", protocol="fakeproto")
    result = _make_result()

    with pytest.raises(CriticalAccountingError) as excinfo:
        enricher.enrich(result, intent, _FakeContext())

    err = excinfo.value
    assert err.field_name == "swap_amounts"
    assert err.intent_type == "SWAP"
    # ``protocol`` on the error must be the resolved slug (what downstream
    # consumers filter on) — NOT the parser class name.
    assert err.protocol == "fakeproto"
    # Parser class name still appears in the human-readable message.
    assert "_ErrorReturnParser" in str(err)
    assert "malformed log shape" in str(err)


def test_extract_error_is_exception_subclass() -> None:
    """CriticalAccountingError must be an Exception subclass.

    VIB-3180: the original implementation used BaseException so the error
    would escape generic ``except Exception`` handlers.  That is wrong —
    it escapes the strategy runner's *recovery* handlers (ACCOUNTING_FAILED
    conversion, operator alerting, finalize_run_loop cleanup), not just
    accidental catch-all swallowers.  The correct contract is:

    1. CriticalAccountingError IS an Exception so run_iteration's outer
       except-Exception block can catch it and return ACCOUNTING_FAILED.
    2. _single_chain_handle_success has an explicit ``except
       CriticalAccountingError: raise`` BEFORE the generic swallowing
       ``except Exception: logger.warning(...)`` so the error is never
       silently downgraded to a warning at the wrong layer.
    """
    assert issubclass(CriticalAccountingError, Exception), (
        "CriticalAccountingError must inherit from Exception so run_iteration "
        "can catch it and return ACCOUNTING_FAILED"
    )
    # Guard against bare BaseException: Exception must be a direct base, not
    # just transitively reachable.
    assert Exception in CriticalAccountingError.__bases__, (
        "CriticalAccountingError.__bases__ must include Exception directly "
        "(not just via BaseException inheritance chain)"
    )

    enricher = ResultEnricher(parser_registry=_registry_with(_ErrorReturnParser()), live_mode=True)
    intent = _FakeIntent(intent_type="SWAP", protocol="fakeproto")
    result = _make_result()

    caught_as_exception = False
    try:
        enricher.enrich(result, intent, _FakeContext())
    except Exception:
        caught_as_exception = True

    assert caught_as_exception, (
        "CriticalAccountingError must be catchable by except Exception so "
        "run_iteration's recovery handler can convert it to ACCOUNTING_FAILED"
    )


def test_extract_error_raised_inside_method_is_caught_as_variant() -> None:
    enricher = ResultEnricher(parser_registry=_registry_with(_ErrorRaiseParser()), live_mode=True)
    intent = _FakeIntent(intent_type="SWAP", protocol="fakeproto")
    result = _make_result()

    with pytest.raises(CriticalAccountingError) as excinfo:
        enricher.enrich(result, intent, _FakeContext())

    assert "corrupt receipt" in str(excinfo.value)
    assert isinstance(excinfo.value.original, ValueError)


def test_extract_error_paper_mode_warns_and_counts() -> None:
    enricher = ResultEnricher(parser_registry=_registry_with(_ErrorReturnParser()), live_mode=False)
    intent = _FakeIntent(intent_type="SWAP", protocol="fakeproto")
    result = _make_result()

    enriched = enricher.enrich(result, intent, _FakeContext())

    assert enriched.swap_amounts is None
    assert enricher.extract_error_count == 1
    assert any("ExtractError[swap_amounts]" in w for w in enriched.extraction_warnings)


def test_extract_error_paper_mode_covers_raised_exception_too() -> None:
    enricher = ResultEnricher(parser_registry=_registry_with(_ErrorRaiseParser()), live_mode=False)
    intent = _FakeIntent(intent_type="SWAP", protocol="fakeproto")
    result = _make_result()

    enriched = enricher.enrich(result, intent, _FakeContext())

    assert enriched.swap_amounts is None
    assert enricher.extract_error_count == 1
    assert any("corrupt receipt" in w for w in enriched.extraction_warnings)


# ---------------------------------------------------------------------------
# Backward compatibility: legacy parsers keep working
# ---------------------------------------------------------------------------


def test_legacy_raw_value_still_works() -> None:
    enricher = ResultEnricher(parser_registry=_registry_with(_LegacyOkParser()), live_mode=True)
    intent = _FakeIntent(intent_type="SWAP", protocol="fakeproto")
    result = _make_result()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        enriched = enricher.enrich(result, intent, _FakeContext())

    assert enriched.swap_amounts is not None
    assert enriched.swap_amounts.amount_in == 1
    # Legacy parsers emit a one-shot DeprecationWarning.
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)


def test_legacy_none_treated_as_missing_not_error() -> None:
    enricher = ResultEnricher(parser_registry=_registry_with(_LegacyNoneParser()), live_mode=True)
    intent = _FakeIntent(intent_type="SWAP", protocol="fakeproto")
    result = _make_result()

    enriched = enricher.enrich(result, intent, _FakeContext())

    assert enriched.swap_amounts is None
    # Legacy None is ambiguous but conservatively treated as missing, so
    # live mode does NOT raise. This is the documented trade-off for
    # un-migrated parsers; the DeprecationWarning flags them for migration.
    assert enricher.extract_error_count == 0


def test_legacy_raise_is_treated_as_error() -> None:
    """Even under the legacy contract, a raised exception is accounting-critical."""
    enricher = ResultEnricher(parser_registry=_registry_with(_LegacyRaiseParser()), live_mode=True)
    intent = _FakeIntent(intent_type="SWAP", protocol="fakeproto")
    result = _make_result()

    with pytest.raises(CriticalAccountingError):
        enricher.enrich(result, intent, _FakeContext())


# ---------------------------------------------------------------------------
# Per-connector coverage — the 5 retrofitted parsers expose the tagged variant
# ---------------------------------------------------------------------------


def test_uniswap_v3_exposes_result_variants() -> None:
    parser = UniswapV3ReceiptParser(chain="arbitrum")
    for name in (
        "extract_position_id_result",
        "extract_swap_amounts_result",
        "extract_lp_close_data_result",
        "extract_liquidity_result",
    ):
        assert hasattr(parser, name), f"Uniswap V3 missing {name}"


def test_uniswap_v3_empty_receipt_is_missing() -> None:
    parser = UniswapV3ReceiptParser(chain="arbitrum")
    assert isinstance(parser.extract_position_id_result({"logs": []}), ExtractMissing)
    assert isinstance(parser.extract_swap_amounts_result({"logs": []}), ExtractMissing)
    assert isinstance(parser.extract_lp_close_data_result({"logs": []}), ExtractMissing)
    assert isinstance(parser.extract_liquidity_result({"logs": []}), ExtractMissing)


def test_uniswap_v3_malformed_receipt_is_error() -> None:
    parser = UniswapV3ReceiptParser(chain="arbitrum")
    # A receipt that is not a dict will crash ``parse_receipt`` at the top
    # level (``.get`` attribute error), which the strict ``_result`` wrapper
    # MUST surface as ``ExtractError`` — not silently degrade to missing.
    # This is the VIB-3159 contract: parser crash != benign "no event".
    bad = ["not", "a", "dict"]  # type: ignore[assignment]
    out = parser.extract_swap_amounts_result(bad)  # type: ignore[arg-type]
    assert isinstance(out, ExtractError)


def test_all_connectors_parse_crash_is_error() -> None:
    """A ``parse_receipt`` crash MUST map to ``ExtractError`` across every
    migrated connector (VIB-3159).

    The previous wrappers relied on the legacy ``extract_*`` methods, which
    catch their own exceptions and return ``None``. That made parse crashes
    look identical to "no event in receipt" — the ghost-position class of
    bug. The fix routes each ``_result`` call through ``parse_receipt`` first
    so a real crash propagates as ``ExtractError``.
    """

    def crashing_parse(_receipt: dict[str, Any]) -> Any:
        raise RuntimeError("induced parse_receipt failure")

    # uniswap_v3
    u = UniswapV3ReceiptParser(chain="arbitrum")
    u.parse_receipt = crashing_parse  # type: ignore[method-assign]
    assert isinstance(u.extract_position_id_result({"logs": []}), ExtractError)
    assert isinstance(u.extract_swap_amounts_result({"logs": []}), ExtractError)
    assert isinstance(u.extract_lp_close_data_result({"logs": []}), ExtractError)
    assert isinstance(u.extract_liquidity_result({"logs": []}), ExtractError)

    # aerodrome
    a = AerodromeReceiptParser(chain="base")
    a.parse_receipt = crashing_parse  # type: ignore[method-assign]
    assert isinstance(a.extract_swap_amounts_result({"logs": []}), ExtractError)
    assert isinstance(a.extract_lp_close_data_result({"logs": []}), ExtractError)
    assert isinstance(a.extract_position_id_result({"logs": []}), ExtractError)
    assert isinstance(a.extract_liquidity_result({"logs": []}), ExtractError)

    # aave_v3
    av = AaveV3ReceiptParser(chain="arbitrum")
    av.parse_receipt = crashing_parse  # type: ignore[method-assign]
    assert isinstance(av.extract_supply_amount_result({"logs": []}), ExtractError)
    assert isinstance(av.extract_withdraw_amount_result({"logs": []}), ExtractError)
    assert isinstance(av.extract_borrow_amount_result({"logs": []}), ExtractError)
    assert isinstance(av.extract_repay_amount_result({"logs": []}), ExtractError)
    assert isinstance(av.extract_a_token_received_result({"logs": []}), ExtractError)

    # morpho_blue
    mb = MorphoBlueReceiptParser()
    mb.parse_receipt = crashing_parse  # type: ignore[method-assign]
    assert isinstance(mb.extract_supply_amount_result({"logs": []}), ExtractError)
    assert isinstance(mb.extract_withdraw_amount_result({"logs": []}), ExtractError)
    assert isinstance(mb.extract_borrow_amount_result({"logs": []}), ExtractError)
    assert isinstance(mb.extract_repay_amount_result({"logs": []}), ExtractError)

    # gmx_v2
    g = GMXv2ReceiptParser(chain="arbitrum")
    g.parse_receipt = crashing_parse  # type: ignore[method-assign]
    assert isinstance(g.extract_swap_amounts_result({"logs": []}), ExtractError)
    assert isinstance(g.extract_position_id_result({"logs": []}), ExtractError)
    assert isinstance(g.extract_size_delta_result({"logs": []}), ExtractError)
    assert isinstance(g.extract_collateral_result({"logs": []}), ExtractError)


def test_all_connectors_parse_reports_failure_is_error() -> None:
    """If ``parse_receipt`` returns ``success=False`` without raising, the
    ``_result`` wrapper MUST still surface ``ExtractError``. This is the
    "graceful failure" path — parser caught the crash internally and reported
    it via ``ParseResult.success=False, error=...``. We MUST NOT downgrade to
    ``ExtractMissing`` (VIB-3159)."""
    from almanak.connectors.aave_v3.receipt_parser import (
        ParseResult as AaveParseResult,
    )
    from almanak.connectors.aerodrome.receipt_parser import (
        ParseResult as AeroParseResult,
    )
    from almanak.connectors.gmx_v2.receipt_parser import (
        ParseResult as GmxParseResult,
    )
    from almanak.connectors.morpho_blue.receipt_parser import (
        ParseResult as MorphoParseResult,
    )
    from almanak.connectors.uniswap_v3.receipt_parser import (
        ParseResult as UniParseResult,
    )

    def failed(cls: Any) -> Any:
        def _parse(_receipt: dict[str, Any]) -> Any:
            return cls(success=False, error="simulated receipt-parse failure")

        return _parse

    u = UniswapV3ReceiptParser(chain="arbitrum")
    u.parse_receipt = failed(UniParseResult)  # type: ignore[method-assign]
    out = u.extract_swap_amounts_result({"logs": []})
    assert isinstance(out, ExtractError)
    assert "simulated receipt-parse failure" in out.error

    a = AerodromeReceiptParser(chain="base")
    a.parse_receipt = failed(AeroParseResult)  # type: ignore[method-assign]
    assert isinstance(a.extract_lp_close_data_result({"logs": []}), ExtractError)

    av = AaveV3ReceiptParser(chain="arbitrum")
    av.parse_receipt = failed(AaveParseResult)  # type: ignore[method-assign]
    assert isinstance(av.extract_supply_amount_result({"logs": []}), ExtractError)

    mb = MorphoBlueReceiptParser()
    mb.parse_receipt = failed(MorphoParseResult)  # type: ignore[method-assign]
    assert isinstance(mb.extract_borrow_amount_result({"logs": []}), ExtractError)

    g = GMXv2ReceiptParser(chain="arbitrum")
    g.parse_receipt = failed(GmxParseResult)  # type: ignore[method-assign]
    assert isinstance(g.extract_fees_paid_result({"logs": []}), ExtractError)


def test_aerodrome_exposes_result_variants() -> None:
    parser = AerodromeReceiptParser(chain="base")
    for name in (
        "extract_swap_amounts_result",
        "extract_lp_close_data_result",
        "extract_position_id_result",
        "extract_liquidity_result",
    ):
        assert hasattr(parser, name), f"Aerodrome missing {name}"


def test_aerodrome_empty_receipt_is_missing() -> None:
    parser = AerodromeReceiptParser(chain="base")
    assert isinstance(parser.extract_swap_amounts_result({"logs": []}), ExtractMissing)
    assert isinstance(parser.extract_lp_close_data_result({"logs": []}), ExtractMissing)


def test_aave_v3_exposes_result_variants() -> None:
    parser = AaveV3ReceiptParser(chain="arbitrum")
    for name in (
        "extract_supply_amount_result",
        "extract_withdraw_amount_result",
        "extract_borrow_amount_result",
        "extract_repay_amount_result",
        "extract_a_token_received_result",
    ):
        assert hasattr(parser, name), f"Aave V3 missing {name}"


def test_aave_v3_empty_receipt_is_missing() -> None:
    parser = AaveV3ReceiptParser(chain="arbitrum")
    assert isinstance(parser.extract_supply_amount_result({"logs": []}), ExtractMissing)
    assert isinstance(parser.extract_borrow_amount_result({"logs": []}), ExtractMissing)


def test_morpho_blue_exposes_result_variants() -> None:
    parser = MorphoBlueReceiptParser()
    for name in (
        "extract_supply_amount_result",
        "extract_withdraw_amount_result",
        "extract_borrow_amount_result",
        "extract_repay_amount_result",
    ):
        assert hasattr(parser, name), f"Morpho Blue missing {name}"


def test_morpho_blue_empty_receipt_is_missing() -> None:
    parser = MorphoBlueReceiptParser()
    assert isinstance(parser.extract_supply_amount_result({"logs": []}), ExtractMissing)
    assert isinstance(parser.extract_repay_amount_result({"logs": []}), ExtractMissing)


def test_gmx_v2_exposes_result_variants() -> None:
    parser = GMXv2ReceiptParser(chain="arbitrum")
    for name in (
        "extract_swap_amounts_result",
        "extract_position_id_result",
        "extract_size_delta_result",
        "extract_collateral_result",
        "extract_entry_price_result",
        "extract_leverage_result",
        "extract_realized_pnl_result",
        "extract_exit_price_result",
        "extract_fees_paid_result",
    ):
        assert hasattr(parser, name), f"GMX V2 missing {name}"


def test_gmx_v2_empty_receipt_is_missing() -> None:
    parser = GMXv2ReceiptParser(chain="arbitrum")
    assert isinstance(parser.extract_position_id_result({"logs": []}), ExtractMissing)
    assert isinstance(parser.extract_size_delta_result({"logs": []}), ExtractMissing)


# ---------------------------------------------------------------------------
# enrich_result() convenience — live_mode override
# ---------------------------------------------------------------------------


def test_enrich_result_live_mode_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Calling enrich_result with live_mode=False must downgrade errors."""
    from almanak.framework.execution import result_enricher as rm

    # Patch ReceiptParserRegistry so the transient enricher finds our fake.
    class _Reg:
        def get(self, protocol: str, **kwargs: Any) -> Any:  # noqa: ARG002
            return _ErrorReturnParser()

    monkeypatch.setattr(rm, "ReceiptParserRegistry", _Reg)

    intent = _FakeIntent(intent_type="SWAP", protocol="fakeproto")
    result = _make_result()

    enriched = rm.enrich_result(result, intent, _FakeContext(), live_mode=False)
    assert enriched.swap_amounts is None
    assert any("ExtractError[swap_amounts]" in w for w in enriched.extraction_warnings)
