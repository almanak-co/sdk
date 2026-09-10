"""Receipt-bound prices for unregistered tokens and colliding symbols."""

import json
from decimal import Decimal

import pytest

from almanak.framework.accounting.category_handlers.swap_handler import handle_swap
from almanak.framework.execution.extracted_data import SwapAmounts
from tests.unit.framework.accounting.test_swap_accounting import _make_ledger_row, _make_outbox_row, _price_json

TOKEN = "0x29cdaa3a468682573f405f070884eb933cde1e18"


def row(address=TOKEN, prices=None):
    ledger = _make_ledger_row(
        token_in="USDG", amount_in="1.549927", token_out="BELIKEBOB", amount_out="74788.352718368090621084",
        chain="robinhood", price_inputs_json=_price_json(prices or {"USDG": "1", f"robinhood:{TOKEN}": "0.00001997"}),
    )
    ledger["extracted_data_json"] = json.dumps({"swap_amounts": {"token_out_address": address}})
    return ledger


def test_unregistered_receipt_address_retains_usd_and_provider_provenance():
    event = handle_swap(_make_outbox_row(), row())
    assert event.token_out == "BELIKEBOB"
    assert event.amount_out_usd == Decimal("1.49352340378581076970304748")
    assert event.price_out_source == "chainlink"
    assert event.price_out_observed_at.isoformat() == "2026-08-14T12:00:00+00:00"


def test_exact_address_price_wins_over_conflicting_symbol_price():
    event = handle_swap(_make_outbox_row(), row(prices={"USDG": "1", f"robinhood:{TOKEN}": "0.00001997", "BELIKEBOB": "1000"}))
    assert event.amount_out_usd == Decimal("1.49352340378581076970304748")


@pytest.mark.parametrize("prices", [{"USDG": "1", f"base:{TOKEN}": "2"}, {"USDG": "1", "BELIKEBOB": "2"}])
def test_unknown_address_cannot_borrow_a_different_chain_or_unbound_symbol_price(prices):
    event = handle_swap(_make_outbox_row(), row(prices=prices))
    assert event.amount_out_usd is None
    assert event.price_out_source is None


@pytest.mark.parametrize("address", ["not-an-address", 42, True, "0x1234"])
def test_invalid_claimed_address_is_unmeasured(address):
    event = handle_swap(_make_outbox_row(), row(address=address))
    assert event.amount_out_usd is None


def test_missing_legacy_address_preserves_symbol_price_compatibility():
    event = handle_swap(_make_outbox_row(), row(address=None, prices={"USDG": "1", "BELIKEBOB": "0.00001997"}))
    assert event.amount_out_usd == Decimal("1.49352340378581076970304748")


def test_swap_wire_retains_price_addresses_without_replacing_symbol_identity():
    amounts = SwapAmounts(1, 2, Decimal(1), Decimal(2), token_in="USDG", token_out="BELIKEBOB", token_out_address=TOKEN)
    raw = amounts.to_dict()
    assert raw["token_out_address"] == TOKEN and raw["token_out"] == "BELIKEBOB"
    assert raw["token_in_address"] is None
