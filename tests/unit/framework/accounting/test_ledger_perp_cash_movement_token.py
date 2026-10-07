"""Perp-venue cash movements name their token on the ledger fallback row."""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.intents import Intent
from almanak.framework.observability.ledger import _extract_from_intent_fallback


def test_perp_deposit_row_names_the_deposited_asset() -> None:
    intent = Intent.perp_deposit(amount=Decimal("2.5"), asset="USDT", protocol="aster_perps", chain="bsc")
    row = _extract_from_intent_fallback(intent, intent_type="PERP_DEPOSIT")
    assert row[0] == "USDT"


def test_perp_withdraw_row_names_the_withdrawn_asset() -> None:
    intent = Intent.perp_withdraw(amount=Decimal("2.4"), asset="USDT", protocol="aster_perps", chain="bsc")
    assert _extract_from_intent_fallback(intent, intent_type="PERP_WITHDRAW")[0] == "USDT"
