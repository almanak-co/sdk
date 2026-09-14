"""Missing native evidence cannot become measured zero or a requested amount."""

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.enso.receipt_parser import EnsoReceiptParser
from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.execution.extracted_data import SwapAmounts


@pytest.fixture
def saved_funding():
    saved = json.loads((Path(__file__).parent / "fixtures/bnb_funding_receipt.json").read_text())
    receipt = saved["receipt"]
    receipt["status"] = int(receipt["status"], 16)
    assert saved["transaction_value_wei"] == 2_000_000_000_000_000
    return receipt


@pytest.fixture
def parser(monkeypatch):
    parser = EnsoReceiptParser(chain="bsc")
    # The saved output is BSC USDT; native effects are not ERC-20 metadata.
    monkeypatch.setattr(
        parser,
        "_resolve_decimals",
        lambda address: 18 if address == "0x55d398326f99059ff775485246999027b3197955" else None,
    )
    return parser


@pytest.mark.parametrize(
    "field, expected",
    [("amount_in_decimal", None), ("amount_in_decimal_resolved", False), ("effective_price", None)],
)
def test_missing_native_effect_is_unmeasured_not_zero(parser, saved_funding, field, expected):
    amounts = parser.extract_swap_amounts(saved_funding)
    assert amounts is not None
    assert amounts.amount_out_decimal == Decimal("1.458978484712884624")
    assert amounts.amount_in is None
    assert amounts.to_dict()["amount_in"] is None
    assert getattr(amounts, field) is expected


def test_ax_does_not_replace_unmeasured_execution_input_with_requested_amount(parser, saved_funding):
    amounts = parser.extract_swap_amounts(saved_funding)
    assert amounts is not None
    truthful = replace(amounts, amount_in_decimal=None, amount_in_decimal_resolved=False, effective_price=None)
    response = ToolExecutor._swap_response_from_enriched(
        None, {}, SimpleNamespace(swap_amounts=truthful), {"amount": "0.002", "token_in": "BNB", "token_out": "USDT"}
    )
    assert response["amount_out"] == "1.458978484712884624"
    assert response["amount_in"] == ""


def test_reverted_receipt_has_no_successful_swap_economics(parser, saved_funding):
    saved_funding["status"] = 0
    assert parser.extract_swap_amounts(saved_funding) is None


@pytest.mark.parametrize("raw, decimals", [(0, 6), (1_250_000, 6), (1_250_000, None)])
def test_erc20_input_preserves_raw_measurement_and_scaling(parser, saved_funding, monkeypatch, raw, decimals):
    token = "0x" + "56" * 20
    saved_funding["logs"].insert(
        0,
        {
            "address": token,
            "topics": [
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x" + saved_funding["from"][2:].lower().zfill(64),
                "0x" + saved_funding["to"][2:].lower().zfill(64),
            ],
            "data": "0x" + f"{raw:064x}",
        },
    )
    monkeypatch.setattr(parser, "_resolve_decimals", lambda address: decimals if address == token else 18)
    result = parser.extract_swap_amounts(saved_funding)
    assert result is not None
    assert result.amount_in == raw
    assert result.token_in == token
    assert result.amount_in_decimal_resolved is (decimals is not None)
    assert result.amount_in_decimal == (Decimal(raw) / 10**decimals if decimals is not None else None)
    if raw and decimals is not None:
        assert result.effective_price == result.amount_out_decimal / result.amount_in_decimal
    else:
        assert result.effective_price is None


@pytest.mark.parametrize("input_ok, output_ok", [(False, True), (True, False), (False, False), (True, True)])
def test_ax_honors_each_decimal_measurement_flag(input_ok, output_ok):
    swap = SwapAmounts(
        amount_in=1,
        amount_out=2,
        amount_in_decimal=Decimal("1"),
        amount_out_decimal=Decimal("2"),
        effective_price=Decimal("2"),
        amount_in_decimal_resolved=input_ok,
        amount_out_decimal_resolved=output_ok,
    )
    response = ToolExecutor._swap_response_from_enriched(None, {}, SimpleNamespace(swap_amounts=swap), {"amount": "9"})
    assert response["amount_in"] == ("1" if input_ok else "")
    assert response["amount_out"] == ("2" if output_ok else "")
    assert response["effective_price"] == ("2" if input_ok and output_ok else "")


def test_ax_missing_measurement_flags_fails_closed():
    swap = SimpleNamespace(
        amount_in_decimal=Decimal("1"),
        amount_out_decimal=Decimal("2"),
        effective_price=Decimal("2"),
        token_in="USDC",
        token_out="WETH",
        slippage_bps=None,
    )
    response = ToolExecutor._swap_response_from_enriched(None, {}, SimpleNamespace(swap_amounts=swap), {})
    assert response["amount_in"] == response["amount_out"] == response["effective_price"] == ""


@pytest.mark.parametrize("input_amount, output_amount", [("1", Decimal("2")), (Decimal("1"), "2")])
def test_ax_malformed_decimal_values_are_unmeasured(input_amount, output_amount):
    swap = SimpleNamespace(
        amount_in_decimal=input_amount,
        amount_out_decimal=output_amount,
        effective_price=Decimal("2"),
        amount_in_decimal_resolved=True,
        amount_out_decimal_resolved=True,
        slippage_bps=None,
        token_in="USDC",
        token_out="WETH",
    )
    response = ToolExecutor._swap_response_from_enriched(None, {}, SimpleNamespace(swap_amounts=swap), {})
    assert response["effective_price"] == ""


def test_ax_without_extraction_does_not_claim_requested_fill():
    result = ToolExecutor._swap_response_from_enriched(
        None, {}, SimpleNamespace(swap_amounts=None), {"amount": "0.002", "token_in": "BNB", "token_out": "USDT"}
    )
    assert result["amount_in"] == result["amount_out"] == result["effective_price"] == ""
    assert result["token_in"] is None
    assert result["token_out"] is None


@pytest.mark.parametrize("amount", [None, Decimal("0")])
def test_ax_legacy_price_without_valid_denominator_is_unmeasured(amount):
    swap = SwapAmounts(
        amount_in=None if amount is None else 0,
        amount_out=2,
        amount_in_decimal=amount,
        amount_out_decimal=Decimal("2"),
        effective_price=Decimal("99"),
    )
    response = ToolExecutor._swap_response_from_enriched(None, {}, SimpleNamespace(swap_amounts=swap), {"amount": "9"})
    assert response["amount_in"] == ("" if amount is None else "0")
    assert response["amount_out"] == "2"
    assert response["effective_price"] == ""


def test_partial_measurement_survives_enricher_ledger_sidecar_and_ax(saved_funding, monkeypatch):
    from almanak.framework.accounting.sidecar import AccountingSidecarWriter
    from almanak.framework.execution.orchestrator import (
        ExecutionContext,
        ExecutionPhase,
        ExecutionResult,
        TransactionResult,
    )
    from almanak.framework.execution.result_enricher import ResultEnricher
    from almanak.framework.observability.ledger import _extract_from_swap_amounts
    from almanak.framework.runner.inner_runner import _DictReceipt, _MinimalIntent

    monkeypatch.setattr(EnsoReceiptParser, "_resolve_decimals", lambda self, address: 18 if address else None)
    intent = _MinimalIntent("SWAP", {"from_token": "BNB", "to_token": "USDT", "amount": "0.002", "protocol": "enso"})
    result = ExecutionResult(
        success=True,
        phase=ExecutionPhase.COMPLETE,
        transaction_results=[
            TransactionResult(
                tx_hash=saved_funding["transactionHash"], success=True, receipt=_DictReceipt(saved_funding)
            )
        ],
    )
    context = ExecutionContext(
        deployment_id="deployment:native-result-contract",
        chain="bsc",
        wallet_address=saved_funding["from"],
        protocol="enso",
    )
    ResultEnricher().enrich(result, intent, context)
    assert result.swap_amounts is not None
    assert result.swap_amounts.amount_in is None
    assert result.swap_amounts.to_dict()["amount_in"] is None
    assert result.swap_amounts.amount_out_decimal == Decimal("1.458978484712884624")
    assert any("effect missing or decimals unresolved" in warning for warning in result.extraction_warnings)
    assert not any("legacy 18-decimal fallback" in warning for warning in result.extraction_warnings)
    ledger = _extract_from_swap_amounts(result.swap_amounts, intent)
    assert ledger[2:5] == ("", "1.458978484712884624", "")
    sidecar = AccountingSidecarWriter._build_line(
        deployment_id=context.deployment_id, intent=intent, result=result, chain="bsc"
    )
    assert sidecar["amount_in"] is None
    assert sidecar["amount_out"] == "1.458978484712884624"
    response = ToolExecutor._swap_response_from_enriched(None, {}, result, {"amount": "0.002"})
    assert response["amount_in"] == ""
    assert response["amount_out"] == "1.458978484712884624"


def test_unmeasured_native_input_degrades_ledger_row_instead_of_claiming_success():
    from almanak.framework.accounting.ledger_guard import apply_degradation, classify_ledger_row
    from almanak.framework.observability.ledger import LedgerEntry

    entry = LedgerEntry(
        intent_type="SWAP",
        success=True,
        token_in="BNB",
        amount_in="",
        token_out="USDT",
        amount_out="1.458978484712884624",
        tx_hash="0x" + "ab" * 32,
        gas_used=1,
    )
    degradation = classify_ledger_row(entry)
    assert degradation is not None
    apply_degradation(entry, degradation)
    assert entry.success is False
    assert "amount_in" in entry.error
    assert entry.amount_in == ""


@pytest.mark.parametrize("outer_value", [0, 2_000_000_000_000_000])
def test_safe_native_input_does_not_infer_fill_from_outer_value(parser, saved_funding, outer_value):
    from almanak.connectors._strategy_base.base.receipt_wallet import stamp_trading_wallet

    trading_wallet = saved_funding["from"]
    saved_funding["from"] = "0x" + "78" * 20
    saved_funding["value"] = outer_value
    result = parser.extract_swap_amounts(stamp_trading_wallet(saved_funding, trading_wallet))
    assert result is not None
    assert result.amount_in is None
    assert result.amount_in_decimal is None
    assert result.amount_out_decimal == Decimal("1.458978484712884624")
