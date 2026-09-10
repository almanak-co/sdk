"""Replay measured dynamic-pool LP events without upgrading their USD provenance."""

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.framework.accounting.category_handlers.lp_confidence import retain_lp_price_confidence
from almanak.framework.accounting.category_handlers.lp_handler import handle_lp
from almanak.framework.accounting.models import AccountingConfidence
from tests.support.token_resolver import FakeToken, FakeTokenResolver

_FIXTURE = Path(__file__).parents[3] / "fixtures/accounting/v4_dynamic_lp_confidence.json"


@pytest.fixture
def captured(monkeypatch):
    resolver = FakeTokenResolver()
    for symbol, address, decimals in [
        ("BELIKEBOB", "0x29cdaa3a468682573f405f070884eb933cde1e18", 18),
        ("USDG", "0x5fc5360d0400a0fd4f2af552add042d716f1d168", 6),
    ]:
        resolver.add(symbol, FakeToken(symbol=symbol, address=address, decimals=decimals, chain="robinhood"))
    monkeypatch.setattr("almanak.framework.data.tokens.resolver.get_token_resolver", lambda: resolver)
    return json.loads(_FIXTURE.read_text())["events"]


def test_mainnet_lp_replay_preserves_money_and_degraded_provenance(captured):
    prior = None
    for record in captured:
        event = handle_lp(record["outbox"], record["ledger"], prior_open_payload=prior)
        assert event is not None
        payload = json.loads(event.to_payload_json())
        assert {key: payload[key] for key in record["original_money"]} == record["original_money"]
        assert event.confidence == AccountingConfidence.STALE
        assert "price provenance degraded" in event.unavailable_reason
        prior = payload


@pytest.mark.parametrize("confidence", ["HIGH", "ESTIMATED", "STALE", "UNAVAILABLE", None, "unknown"])
def test_lp_uses_selected_price_confidence(captured, confidence):
    record = captured[0]
    prices = json.loads(record["ledger"]["price_inputs_json"])
    for value in prices.values():
        value["confidence"] = confidence
    record["ledger"]["price_inputs_json"] = json.dumps(prices)
    event = handle_lp(record["outbox"], record["ledger"])
    assert event.confidence.value == (
        confidence if confidence in {"HIGH", "ESTIMATED", "STALE", "UNAVAILABLE"} else "UNAVAILABLE"
    )
    assert event.cost_basis_usd is not None


def test_provenance_follows_the_price_key_used_by_valuation(captured):
    record = captured[0]
    prices = json.loads(record["ledger"]["price_inputs_json"])
    for key, value in prices.items():
        value["confidence"] = "STALE" if key.startswith("robinhood:0x29") else "HIGH"
    record["ledger"]["price_inputs_json"] = json.dumps(prices)
    event = handle_lp(record["outbox"], record["ledger"])
    assert event.confidence == AccountingConfidence.HIGH


def test_zero_leg_and_unrelated_prices_do_not_degrade_lp(captured):
    record = captured[0]
    extracted = json.loads(record["ledger"]["extracted_data_json"])
    extracted["lp_open_data"]["amount1"] = "0"
    record["ledger"]["extracted_data_json"] = json.dumps(extracted)
    prices = json.loads(record["ledger"]["price_inputs_json"])
    for key, value in prices.items():
        value["confidence"] = "HIGH" if key.startswith("robinhood:0x29") or key == "BELIKEBOB" else "UNAVAILABLE"
    record["ledger"]["price_inputs_json"] = json.dumps(prices)
    event = handle_lp(record["outbox"], record["ledger"])
    assert event.amount1 == 0
    assert event.confidence == AccountingConfidence.HIGH
    assert event.unavailable_reason == ""


def test_close_retains_entry_basis_confidence(captured):
    opened = handle_lp(captured[0]["outbox"], captured[0]["ledger"])
    record = captured[1]
    prices = json.loads(record["ledger"]["price_inputs_json"])
    for value in prices.values():
        value["confidence"] = "HIGH"
    record["ledger"]["price_inputs_json"] = json.dumps(prices)
    event = handle_lp(record["outbox"], record["ledger"], json.loads(opened.to_payload_json()))
    assert event.realized_pnl_usd is not None
    assert event.confidence == AccountingConfidence.STALE
    assert "entry basis=STALE" in event.unavailable_reason


def test_legacy_scalar_price_does_not_invent_high_confidence(captured):
    record = captured[0]
    prices = json.loads(record["ledger"]["price_inputs_json"])
    record["ledger"]["price_inputs_json"] = json.dumps({key: value["price_usd"] for key, value in prices.items()})
    event = handle_lp(record["outbox"], record["ledger"])
    assert event.cost_basis_usd is not None
    assert event.confidence == AccountingConfidence.UNAVAILABLE


def test_n_coin_fee_leg_provenance_is_retained():
    event = SimpleNamespace(
        cost_basis_usd=Decimal(1),
        fees_total_usd=Decimal(2),
        hodl_value_usd=None,
        realized_pnl_usd=None,
        identity=SimpleNamespace(chain="ethereum"),
        confidence=AccountingConfidence.ESTIMATED,
        unavailable_reason="existing gap",
    )
    data = SimpleNamespace(
        coin_symbols=["USDC", "WETH", "DAI"], coin_addresses=None, all_amounts=[1, 0, 0], all_fees=[0, 0, 1]
    )
    prices = {
        key: {"price_usd": "1", "confidence": confidence}
        for key, confidence in [("USDC", "HIGH"), ("WETH", "UNAVAILABLE"), ("DAI", "STALE")]
    }
    result = retain_lp_price_confidence(event, json.dumps(prices), data, None)
    assert result.confidence == AccountingConfidence.STALE
    assert result.unavailable_reason.startswith("existing gap;")
    assert "DAI=STALE" in result.unavailable_reason
    assert "WETH" not in result.unavailable_reason


@pytest.mark.parametrize("amount", ["", "not-a-number", "NaN", "Infinity", None])
def test_malformed_prior_amount_keeps_close_and_unmeasured_hodl(captured, amount):
    opened = handle_lp(captured[0]["outbox"], captured[0]["ledger"])
    prior = json.loads(opened.to_payload_json())
    prior["amount0"] = amount
    closed = handle_lp(captured[1]["outbox"], captured[1]["ledger"], prior)
    assert closed is not None
    assert closed.hodl_value_usd is None
    assert closed.il_usd is None


@pytest.mark.parametrize("amount", ["", "not-a-number", "NaN", "Infinity", None])
def test_malformed_persisted_coin_amount_degrades_confidence_without_losing_event(captured, amount):
    record = captured[0]
    extracted = json.loads(record["ledger"]["extracted_data_json"])
    extracted["lp_open_data"].update(coin_symbols=["BELIKEBOB", "USDG", "OTHER"], additional_amounts={"2": amount})
    record["ledger"]["extracted_data_json"] = json.dumps(extracted)
    event = handle_lp(record["outbox"], record["ledger"])
    assert event is not None
    payload = json.loads(event.to_payload_json())
    assert {key: payload[key] for key in record["original_money"]} == record["original_money"]
    assert event.confidence == AccountingConfidence.UNAVAILABLE
    assert "unmeasured amount" in event.unavailable_reason


@pytest.mark.parametrize("confidence", ["HIGH", "ESTIMATED", "UNAVAILABLE"])
def test_polygon_open_optional_fields_do_not_override_price_provenance(captured, monkeypatch, confidence):
    resolver = FakeTokenResolver()
    usdc = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
    weth = "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619"
    for symbol, address, decimals in [("USDC", usdc, 6), ("WETH", weth, 18)]:
        resolver.add(symbol, FakeToken(symbol=symbol, address=address, decimals=decimals, chain="polygon"))
    monkeypatch.setattr("almanak.framework.data.tokens.resolver.get_token_resolver", lambda: resolver)
    record = captured[0]
    record["ledger"].update(chain="polygon", token_in="WETH", token_out="USDC")
    extracted = json.loads(record["ledger"]["extracted_data_json"])
    # Amounts and position metadata are from the Polygon CI receipt; prices are unit inputs.
    extracted["lp_open_data"].update(
        position_id=137359,
        liquidity="957807728237",
        tick_lower=184200,
        tick_upper=207240,
        currency0=usdc,
        currency1=weth,
        amount0="17752614",
        amount1="9523809523800727",
    )
    record["ledger"]["extracted_data_json"] = json.dumps(extracted)
    record["ledger"]["price_inputs_json"] = json.dumps(
        {token: {"price_usd": price, "confidence": confidence} for token, price in [("USDC", "1"), ("WETH", "2000")]}
    )
    event = handle_lp(record["outbox"], record["ledger"])
    assert event.amount0 == Decimal("17.752614")
    assert event.amount1 == Decimal("0.009523809523800727")
    assert event.cost_basis_usd == Decimal("36.800233047601454")
    assert event.fees0_collected is None and event.fees1_collected is None
    assert event.fees_total_usd is None and event.hodl_value_usd is None
    assert event.confidence.value == confidence
    assert "unmeasured amount" not in event.unavailable_reason
