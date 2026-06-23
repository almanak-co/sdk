"""VIB-5319 — generic Accountant Test cells (G1/G3/G6) read the typed PT event.

A Pendle PT trade rides the SWAP intent_type in the ledger but is booked as a
typed ``PendleAccountingEvent`` (PT_BUY / PT_SELL / PT_REDEEM), not a generic
``SWAP`` event. Before VIB-5319 the generic money-trail (G1), yield-ledger (G3)
and reconciliation (G6) cells could not see the PT economics — they false-FAILed
on a "missing SwapEventPayload" / "no realized yield" / "$X reconciliation gap"
even though the PT payoff is fully captured by the PEN cells.

This file pins the three behaviours VIB-5319 ships:

* **G3** PASSES off ``realized_yield_usd`` (the PT disposal's realised payoff).
* **G1** treats a PT-backed SWAP ledger row's USD proof as the Pendle payload,
  and surfaces a measured **XFAIL** (not FAIL) when the disposal's sell-side SY
  price is unmeasured (sy_price=None — VIB-5276 gateway PT price), but only for a
  profile that opts in via ``disposal_usd_unmeasured_is_xfail``.
* **G6** books the PT disposal into the component method and surfaces the same
  measured **XFAIL** when the proceeds leg is unmeasured — never a silent zero,
  never a fabricated PASS.

Empty ≠ Zero is enforced throughout: a null sell-side price is *unmeasured*
(XFAIL on the opted-in profile / FAIL otherwise), never folded to a measured
zero.
"""

from __future__ import annotations

import json
from typing import Any

from almanak.framework.accounting.accountant_test import (
    _cell_g1_money_trail,
    _cell_g3_yield_ledger,
    _cell_g6_reconciliation,
    _typed_acct_payloads,
)

_LEDGER_BUY_ID = "ledger-pt-buy"
_LEDGER_SELL_ID = "ledger-pt-sell"


def _pt_buy_event(*, ledger_id: str, sy_price: str | None) -> dict[str, Any]:
    payload = {
        "event_type": "PT_BUY",
        "pt_token": "PT-wstETH-25JUN2026",
        "pt_amount": "0.0123778",
        "sy_amount": "0.0099995",
        "pt_price": "0.8079",
        "implied_apr_bps": 434056,
        "confidence": "HIGH",
        "matching_policy_version": 4,
    }
    if sy_price is not None:
        payload["sy_price"] = sy_price
    return {
        "id": "ae-pt-buy",
        "event_type": "PT_BUY",
        "ledger_entry_id": ledger_id,
        "timestamp": "2026-06-23T01:11:55+00:00",
        "payload_json": json.dumps(payload),
    }


def _pt_sell_event(
    *,
    ledger_id: str,
    realized_yield_usd: str | None,
    sy_price: str | None,
    sy_amount: str | None = "0.0099994",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event_type": "PT_SELL",
        "pt_token": "PT-wstETH-25JUN2026",
        "pt_amount": "0.0123778",
        "basis_lot_id": "ae-pt-buy",
        "confidence": "HIGH",
        "matching_policy_version": 4,
        "realized_yield_usd": realized_yield_usd,
        "realized_yield_sy": "-6.25e-8",
    }
    if sy_amount is not None:
        payload["sy_amount"] = sy_amount
    if sy_price is not None:
        payload["sy_price"] = sy_price
    return {
        "id": "ae-pt-sell",
        "event_type": "PT_SELL",
        "ledger_entry_id": ledger_id,
        "timestamp": "2026-06-23T01:15:19+00:00",
        "payload_json": json.dumps(payload),
    }


def _ledger_swap_row(ledger_id: str) -> dict[str, Any]:
    return {
        "id": ledger_id,
        "intent_type": "SWAP",
        "success": True,
        "tx_hash": "0x" + "ab" * 32,
        "amount_in": "0.01",
        "amount_out": "0.0123",
        "gas_usd": "0.5",
        "chain": "arbitrum",
    }


# ── G3 — yield ledger reads PT realized_yield_usd ────────────────────────────


def test_g3_pass_on_pt_realized_yield() -> None:
    acct = [
        _pt_buy_event(ledger_id=_LEDGER_BUY_ID, sy_price="2143.8"),
        _pt_sell_event(ledger_id=_LEDGER_SELL_ID, realized_yield_usd="-0.000134", sy_price=None),
    ]
    cell = _cell_g3_yield_ledger([], acct)
    assert cell.status == "PASS", cell.diagnostic
    assert "yield-emitting" in cell.diagnostic


def test_g3_does_not_count_measured_zero_string_pt_yield() -> None:
    # Empty ≠ Zero: raw payload JSON carries realized_yield_usd as the STRING
    # "0" (truthy). A measured-zero yield is a real no-yield disposal and must
    # NOT be counted as yield-emitting — a bare truthiness test would miscount it
    # and flip G3 to a phantom PASS.
    acct = [
        _pt_buy_event(ledger_id=_LEDGER_BUY_ID, sy_price="2143.8"),
        _pt_sell_event(ledger_id=_LEDGER_SELL_ID, realized_yield_usd="0", sy_price=None),
    ]
    cell = _cell_g3_yield_ledger([], acct)
    assert cell.status == "FAIL", cell.diagnostic


def test_g3_does_not_count_null_pt_yield() -> None:
    # Empty ≠ Zero: a disposal with no realized_yield_usd contributes no yield
    # (and here there is no other yield source) → the cell does NOT pass on a
    # phantom zero.
    acct = [
        _pt_buy_event(ledger_id=_LEDGER_BUY_ID, sy_price="2143.8"),
        _pt_sell_event(ledger_id=_LEDGER_SELL_ID, realized_yield_usd=None, sy_price=None),
    ]
    cell = _cell_g3_yield_ledger([], acct)
    assert cell.status == "FAIL", cell.diagnostic


# ── G1 — money trail reads the Pendle payload, XFAILs on unmeasured disposal ──


def test_g1_xfail_on_unmeasured_pt_disposal_for_pendle_pt_profile() -> None:
    ledger = [_ledger_swap_row(_LEDGER_BUY_ID), _ledger_swap_row(_LEDGER_SELL_ID)]
    acct = [
        _pt_buy_event(ledger_id=_LEDGER_BUY_ID, sy_price="2143.8"),
        _pt_sell_event(ledger_id=_LEDGER_SELL_ID, realized_yield_usd="-0.000134", sy_price=None),
    ]
    payloads, _, _ = _typed_acct_payloads(acct)
    cell = _cell_g1_money_trail(ledger, acct, payloads, "pendle_pt")
    assert cell.status == "XFAIL", cell.diagnostic
    assert "VIB-5276" in cell.diagnostic


def test_g1_pass_when_pt_disposal_usd_measured() -> None:
    ledger = [_ledger_swap_row(_LEDGER_BUY_ID), _ledger_swap_row(_LEDGER_SELL_ID)]
    acct = [
        _pt_buy_event(ledger_id=_LEDGER_BUY_ID, sy_price="2143.8"),
        # Sell-side SY price present → money trail fully valued in USD.
        _pt_sell_event(ledger_id=_LEDGER_SELL_ID, realized_yield_usd="-0.000134", sy_price="2143.1"),
    ]
    payloads, _, _ = _typed_acct_payloads(acct)
    cell = _cell_g1_money_trail(ledger, acct, payloads, "pendle_pt")
    assert cell.status == "PASS", cell.diagnostic


def test_g1_fail_on_missing_disposal_amount_even_for_pendle_pt_profile() -> None:
    # The VIB-5276 XFAIL waiver is the *price* gap only. A disposal whose
    # proceeds AMOUNT (sy_amount) is itself unmeasured is a receipt/writer data
    # loss — it must FAIL even on the opted-in pendle_pt profile, never be masked
    # by the price-only waiver.
    ledger = [_ledger_swap_row(_LEDGER_BUY_ID), _ledger_swap_row(_LEDGER_SELL_ID)]
    acct = [
        _pt_buy_event(ledger_id=_LEDGER_BUY_ID, sy_price="2143.8"),
        _pt_sell_event(
            ledger_id=_LEDGER_SELL_ID,
            realized_yield_usd="-0.000134",
            sy_price="2143.1",
            sy_amount=None,
        ),
    ]
    payloads, _, _ = _typed_acct_payloads(acct)
    cell = _cell_g1_money_trail(ledger, acct, payloads, "pendle_pt")
    assert cell.status == "FAIL", cell.diagnostic
    assert "VIB-5276" not in cell.diagnostic


def test_g1_fail_on_empty_string_sy_price_even_for_pendle_pt_profile() -> None:
    # The VIB-5276 waiver is the gateway-None price gap only. sy_price == "" is a
    # parser omission (the field was never emitted), a real serialization defect
    # distinct from the gateway returning None — it must FAIL, not XFAIL.
    ledger = [_ledger_swap_row(_LEDGER_BUY_ID), _ledger_swap_row(_LEDGER_SELL_ID)]
    acct = [
        _pt_buy_event(ledger_id=_LEDGER_BUY_ID, sy_price="2143.8"),
        _pt_sell_event(ledger_id=_LEDGER_SELL_ID, realized_yield_usd="-0.000134", sy_price=""),
    ]
    payloads, _, _ = _typed_acct_payloads(acct)
    cell = _cell_g1_money_trail(ledger, acct, payloads, "pendle_pt")
    assert cell.status == "FAIL", cell.diagnostic
    assert "VIB-5276" not in cell.diagnostic


def test_g1_fail_on_unmeasured_pt_disposal_for_non_optin_profile() -> None:
    # A profile that does NOT opt in keeps the strict FAIL-on-null behaviour:
    # the XFAIL is narrowly scoped to the ticketed pendle_pt gap.
    ledger = [_ledger_swap_row(_LEDGER_SELL_ID)]
    acct = [
        _pt_sell_event(ledger_id=_LEDGER_SELL_ID, realized_yield_usd="-0.000134", sy_price=None),
    ]
    payloads, _, _ = _typed_acct_payloads(acct)
    cell = _cell_g1_money_trail(ledger, acct, payloads, "lp")
    assert cell.status == "FAIL", cell.diagnostic


# ── G6 — reconciliation books the PT disposal, XFAILs on unmeasured proceeds ──


def _snapshots() -> list[dict[str, Any]]:
    return [
        {
            "deployment_id": "deployment:pt",
            "timestamp": "2026-06-23T01:11:55+00:00",
            "total_value_usd": "26.5",
            "available_cash_usd": "100.0",
            "wallet_balances_json": json.dumps(
                [{"symbol": "wstETH", "balance": "0.04", "value_usd": "85.75", "price_usd": "2143.8"}]
            ),
        },
        {
            "deployment_id": "deployment:pt",
            "timestamp": "2026-06-23T01:15:19+00:00",
            "total_value_usd": "26.5",
            "available_cash_usd": "100.0",
            "wallet_balances_json": json.dumps(
                [{"symbol": "wstETH", "balance": "0.05", "value_usd": "107.16", "price_usd": "2143.1"}]
            ),
        },
    ]


def test_g6_xfail_on_unmeasured_pt_proceeds_for_pendle_pt_profile() -> None:
    ledger = [_ledger_swap_row(_LEDGER_SELL_ID)]
    acct = [
        _pt_buy_event(ledger_id=_LEDGER_BUY_ID, sy_price="2143.8"),
        _pt_sell_event(ledger_id=_LEDGER_SELL_ID, realized_yield_usd="-0.000134", sy_price=None),
    ]
    payloads, errors, _ = _typed_acct_payloads(acct)
    cell, decomp = _cell_g6_reconciliation(
        _snapshots(), ledger, [], acct, "pendle_pt", payloads, errors
    )
    assert cell.status == "XFAIL", cell.diagnostic
    assert "VIB-5276" in cell.diagnostic
    # The null bucket is surfaced (not folded to zero).
    assert decomp["Σ_pt_proceeds_usd_null_count"] == "1"


def test_g6_fail_on_unmeasured_pt_proceeds_for_non_optin_profile() -> None:
    # Same unmeasured proceeds, but a profile that does not opt in still FAILs.
    ledger = [_ledger_swap_row(_LEDGER_SELL_ID)]
    acct = [
        _pt_sell_event(ledger_id=_LEDGER_SELL_ID, realized_yield_usd="-0.000134", sy_price=None),
    ]
    payloads, errors, _ = _typed_acct_payloads(acct)
    cell, _ = _cell_g6_reconciliation(_snapshots(), ledger, [], acct, "perp", payloads, errors)
    assert cell.status == "FAIL", cell.diagnostic


def test_g6_fail_on_missing_disposal_amount_even_for_pendle_pt_profile() -> None:
    # A PT disposal whose proceeds AMOUNT is unmeasured is a real data loss, not
    # the VIB-5276 price gap — G6 must FAIL even on the opted-in pendle_pt
    # profile, and the amount-null bucket is surfaced separately (Empty≠Zero).
    ledger = [_ledger_swap_row(_LEDGER_SELL_ID)]
    acct = [
        _pt_buy_event(ledger_id=_LEDGER_BUY_ID, sy_price="2143.8"),
        _pt_sell_event(
            ledger_id=_LEDGER_SELL_ID,
            realized_yield_usd="-0.000134",
            sy_price="2143.1",
            sy_amount=None,
        ),
    ]
    payloads, errors, _ = _typed_acct_payloads(acct)
    cell, decomp = _cell_g6_reconciliation(
        _snapshots(), ledger, [], acct, "pendle_pt", payloads, errors
    )
    assert cell.status == "FAIL", cell.diagnostic
    assert "VIB-5276" not in cell.diagnostic
    assert decomp["Σ_pt_proceeds_amount_null_count"] == "1"
    assert decomp["Σ_pt_proceeds_usd_null_count"] == "0"
