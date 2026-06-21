"""Tests for the Pendle connector accounting-treatment spec (VIB-4931 PR-A commit 2).

Pins the categorization (which replaces ``taxonomy.classify``'s pendle branches)
and the registry wiring: the strategy-side ``AccountingTreatmentRegistry`` resolves
the connector's treatments, and they produce events byte-identical to calling the
relocated handlers directly. The handlers' own behaviour is pinned by
``tests/unit/framework/accounting/test_pendle_handlers.py`` (repointed to this
connector in the same commit).
"""

from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.connectors._strategy_accounting_treatment_registry import (
    AccountingTreatmentRegistry,
)
from almanak.connectors.pendle.accounting_spec import (
    ACCOUNTING_TREATMENT_SPEC,
    handle_pendle_lp,
    handle_pendle_pt,
)
from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.category_handlers import HandlerContext
from almanak.framework.primitives.types import AccountingCategory


@pytest.fixture(autouse=True)
def _reset_registry():
    AccountingTreatmentRegistry.reset_cache()
    yield
    AccountingTreatmentRegistry.reset_cache()


def _ctx(outbox: dict, ledger: dict, basis: FIFOBasisStore | None = None) -> HandlerContext:
    return HandlerContext(
        outbox_row=outbox,
        ledger_row=ledger,
        basis_store=basis or FIFOBasisStore(),
        prior_open_lookup=lambda _pk, _disc: None,
    )


def _outbox(intent_type: str = "LP_OPEN", market_id: str = "0xmarket") -> dict:
    return {
        "id": "ob-1",
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "intent_type": intent_type,
        "wallet_address": "0xwallet",
        "position_key": "pendle_lp:arbitrum:0xwallet:0xmarket",
        "market_id": market_id,
    }


def _ledger(
    intent_type: str = "LP_OPEN",
    protocol: str = "pendle",
    token_out: str = "",
    token_in: str = "",
    extracted: str = "",
    tx_hash: str = "0xdeadbeef",
    price_inputs_json: str = "",
) -> dict:
    return {
        "id": "led-1",
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "timestamp": "2026-01-02T03:04:05+00:00",  # fixed → deterministic event id/timestamp
        "intent_type": intent_type,
        "protocol": protocol,
        "chain": "arbitrum",
        "token_out": token_out,
        "token_in": token_in,
        "tx_hash": tx_hash,
        "extracted_data_json": extracted,
        "price_inputs_json": price_inputs_json,
    }


# --- categorization (replaces taxonomy.classify's pendle branches) ----------


@pytest.mark.parametrize("intent_type", ["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"])
def test_categorize_pendle_lp_to_generic_lp(intent_type: str):
    decision = ACCOUNTING_TREATMENT_SPEC.categorize(intent_type, "pendle_v2", "")
    assert decision is not None
    assert decision.category is AccountingCategory.LP  # generic, not a protocol-named member
    assert decision.treatment_key == "pendle_lp"


def test_categorize_pendle_pt_buy_to_generic_swap():
    """SWAP with a PT- token_out is a PT BUY → generic SWAP + pendle_pt."""
    decision = ACCOUNTING_TREATMENT_SPEC.categorize("SWAP", "pendle", "PT-wstETH-25JUN2030")
    assert decision is not None
    assert decision.category is AccountingCategory.SWAP
    assert decision.treatment_key == "pendle_pt"


def test_categorize_pendle_pt_sell_via_token_in():
    """SWAP with a PT- token_in is a PT SELL → claimed via the new token_in arg (VIB-4988)."""
    decision = ACCOUNTING_TREATMENT_SPEC.categorize("SWAP", "pendle", "USDC", token_in="PT-wstETH-25JUN2030")
    assert decision is not None
    assert decision.category is AccountingCategory.SWAP
    assert decision.treatment_key == "pendle_pt"


def test_categorize_pendle_pt_redeem_withdraw():
    """WITHDRAW (PT redeem) with a PT- token_in → generic SWAP + pendle_pt (VIB-4988)."""
    decision = ACCOUNTING_TREATMENT_SPEC.categorize("WITHDRAW", "pendle", "USDC", token_in="PT-x")
    assert decision is not None
    assert decision.category is AccountingCategory.SWAP
    assert decision.treatment_key == "pendle_pt"


@pytest.mark.parametrize(
    "token_in",
    [
        "YT-wstETH-25JUN2026",  # YT leg
        "SY-wstETH",  # SY leg
        "WSTETH",  # underlying (pt_address-degrade path)
        "",  # parser emitted no leg
        None,  # None/missing token_in (must not AttributeError — Gemini)
    ],
)
def test_categorize_non_pt_withdraw_declined_vib5330(token_in: str | None):
    """VIB-5330: a Pendle WITHDRAW whose token_in is NOT a PT- symbol must NOT be
    routed to the PT treatment — it would misbook a phantom PT_REDEEM. Declining
    (None) routes it to the generic SWAP path, matching the position-event lane's
    PT/non-PT predicate (``_pendle_pt_event`` declines the same shape)."""
    assert ACCOUNTING_TREATMENT_SPEC.categorize("WITHDRAW", "pendle", "WSTETH", token_in=token_in) is None


def test_dispatch_non_pt_withdraw_returns_none_and_no_fifo_pollution_vib5330():
    """VIB-5330: handle_pendle_pt declines a non-PT WITHDRAW (returns None) and
    records NO FIFO lot, so a real PT redeem's lot match is never polluted."""
    basis = FIFOBasisStore()
    extracted = json.dumps({"redemption_amounts": {"py_redeemed": int(1e18), "sy_received": int(1e18)}})
    ob = _outbox("WITHDRAW", market_id="0xmarket")
    led = _ledger(
        "WITHDRAW",
        token_in="YT-wstETH-25JUN2026",  # non-PT leg
        token_out="WSTETH",
        extracted=extracted,
        price_inputs_json=json.dumps({"WSTETH": "4000.0"}),
    )

    assert handle_pendle_pt(ob, led, basis_store=basis) is None
    assert not basis._lots  # FIFO lane untouched — no phantom redemption


def test_dispatch_pt_withdraw_still_redeems_vib5330():
    """VIB-5330 no-regression: a genuine PT WITHDRAW (PT- token_in) STILL produces
    a PT_REDEEM event via the dispatcher."""
    from almanak.framework.accounting.models import PendleEventType

    basis = FIFOBasisStore()
    extracted = json.dumps({"redemption_amounts": {"py_redeemed": int(1e18), "sy_received": int(1e18)}})
    ob = _outbox("WITHDRAW", market_id="0xmarket")
    led = _ledger(
        "WITHDRAW",
        token_in="PT-wstETH-25JUN2030",
        token_out="WSTETH",
        extracted=extracted,
        price_inputs_json=json.dumps({"WSTETH": "4000.0"}),
    )

    event = handle_pendle_pt(ob, led, basis_store=basis)
    assert event is not None
    assert event.event_type == PendleEventType.PT_REDEEM
    assert event.pt_token == "PT-wstETH-25JUN2030"


def test_categorize_pendle_yt_sy_swap_declined():
    """A pendle SWAP with neither leg a PT- token (YT/SY swap) is DECLINED → generic SWAP."""
    assert ACCOUNTING_TREATMENT_SPEC.categorize("SWAP", "pendle", "YT-x", token_in="SY-x") is None


@pytest.mark.parametrize(
    ("intent_type", "protocol", "token_out", "token_in"),
    [
        ("LP_OPEN", "uniswap_v3", "", ""),  # non-pendle LP
        ("SWAP", "pendle", "USDC", "USDC"),  # pendle swap, neither leg a PT- token
        ("SWAP", "uniswap_v3", "PT-x", "USDC"),  # PT token_out but non-pendle protocol
        ("SWAP", "uniswap_v3", "USDC", "PT-x"),  # PT token_in but non-pendle protocol
        ("SUPPLY", "pendle", "", ""),  # pendle but not an LP/PT/redeem intent
    ],
)
def test_categorize_declines_unclaimed(intent_type: str, protocol: str, token_out: str, token_in: str):
    assert ACCOUNTING_TREATMENT_SPEC.categorize(intent_type, protocol, token_out, token_in) is None


def test_categorize_token_in_defaults_to_empty():
    """token_in is additive with a default → a 3-arg call still works (other connectors)."""
    # 3-arg PT-buy claim still resolves without passing token_in.
    decision = ACCOUNTING_TREATMENT_SPEC.categorize("SWAP", "pendle", "PT-x")
    assert decision is not None and decision.treatment_key == "pendle_pt"


def test_withdraw_in_claims_event_types():
    assert "WITHDRAW" in ACCOUNTING_TREATMENT_SPEC.claims_event_types


def test_registry_routes_pendle_categorization():
    decision = AccountingTreatmentRegistry.categorize("LP_OPEN", "pendle_v2", "")
    assert decision is not None and decision.treatment_key == "pendle_lp"


# --- registry → treatment wiring equals the direct handler (P1 byte-equivalence) ---


def test_registry_treatment_lp_equals_direct_handler():
    sy_raw, pt_raw = int(1.5 * 10**18), int(2.0 * 10**18)
    extracted = json.dumps({"lp_open_data": {"amount0": sy_raw, "amount1": pt_raw}})
    ob, led = _outbox("LP_OPEN"), _ledger("LP_OPEN", extracted=extracted)

    via_registry = AccountingTreatmentRegistry.treatment_for("pendle_lp")(_ctx(ob, led))
    direct = handle_pendle_lp(ob, led)

    assert via_registry is not None and direct is not None
    assert via_registry == direct
    assert via_registry.to_payload_json() == direct.to_payload_json()


def test_registry_treatment_pt_equals_direct_handler():
    sy_in, pt_out = int(0.9 * 10**18), int(1.0 * 10**18)
    extracted = json.dumps({"swap_amounts": {"amount_in": sy_in, "amount_out": pt_out}})
    ob = _outbox("SWAP", market_id="0xmarket")
    led = _ledger("SWAP", token_out="PT-wstETH-25JUN2030", extracted=extracted)

    # The registry path threads ctx.basis_store (a lot is recorded as a side effect);
    # the returned event is identical to the direct call (the lot does not alter it).
    via_registry = AccountingTreatmentRegistry.treatment_for("pendle_pt")(_ctx(ob, led, FIFOBasisStore()))
    direct = handle_pendle_pt(ob, led, basis_store=None)

    assert via_registry is not None and direct is not None
    assert via_registry == direct
    assert via_registry.to_payload_json() == direct.to_payload_json()


# --- position-key relocation byte-equivalence vs the legacy runner helpers ---


def test_position_key_lp():
    # pool "TOKEN/0xMarket" → market parsed + lowercased; key = pendle_lp:chain:wallet:market.
    intent = SimpleNamespace(protocol="pendle_v2", pool="WETH/0xMarketAddr")
    result = ACCOUNTING_TREATMENT_SPEC.position_key(
        protocol="pendle_v2", intent_type="LP_OPEN", chain="Arbitrum", wallet="0xWallet", intent=intent
    )
    assert result == ("pendle_lp:arbitrum:0xwallet:0xmarketaddr", "0xmarketaddr")


def test_position_key_pt_keys_on_symbol():
    """PT identity is the normalized PT symbol, NOT the market address. A real
    Pendle PT SwapIntent carries ``from_token``/``to_token`` and NO pool, so a
    market-derived key would be empty in production — the symbol is the only
    identifier present on both the intent and the ledger row."""
    intent = SimpleNamespace(
        protocol="pendle_v2", from_token="WSTETH", to_token="PT-wstETH-25JUN2026", pool=""
    )
    result = ACCOUNTING_TREATMENT_SPEC.position_key(
        protocol="pendle_v2", intent_type="SWAP", chain="Arbitrum", wallet="0xWallet", intent=intent
    )
    key, _market = result
    assert key == "pendle_pt:arbitrum:0xwallet:pt-wsteth-25jun2026"


def test_position_key_pt_buy_and_sell_share_key():
    """A PT buy (PT in ``to_token``) and a PT sell (PT in ``from_token``) land on
    the SAME pendle_pt key so the FIFO realized-yield match ties."""
    buy = ACCOUNTING_TREATMENT_SPEC.position_key(
        protocol="pendle_v2",
        intent_type="SWAP",
        chain="Arbitrum",
        wallet="0xWallet",
        intent=SimpleNamespace(protocol="pendle_v2", from_token="WSTETH", to_token="PT-wstETH-25JUN2026"),
    )
    sell = ACCOUNTING_TREATMENT_SPEC.position_key(
        protocol="pendle_v2",
        intent_type="SWAP",
        chain="Arbitrum",
        wallet="0xWallet",
        intent=SimpleNamespace(protocol="pendle_v2", from_token="PT-wstETH-25JUN2026", to_token="WSTETH"),
    )
    assert buy[0] == sell[0] == "pendle_pt:arbitrum:0xwallet:pt-wsteth-25jun2026"


def test_position_key_declines_non_pendle():
    intent = SimpleNamespace(protocol="uniswap_v3", pool="0xm")
    assert (
        ACCOUNTING_TREATMENT_SPEC.position_key(
            protocol="uniswap_v3", intent_type="LP_OPEN", chain="arbitrum", wallet="0x1", intent=intent
        )
        is None
    )


def test_position_key_withdraw_is_owned_but_deferred():
    """A Pendle PT redeem (WITHDRAW) is OWNED by the connector but its key is
    DEFERRED to dispatch time (VIB-4988).

    A real ``WithdrawIntent`` names only the underlying ``token`` + YT
    ``market_id`` — it carries no PT symbol — so ``_position_key`` cannot derive
    the ``pendle_pt:`` key at outbox time. It returns a non-None owned-but-empty
    tuple so the runner does NOT fall through to the generic lending branch; the
    canonical key is then derived from the ledger row's PT leg by ``_pt_context``
    (proven in test_pendle_handlers redeem tests). The non-None return is the
    contract that keeps the runner off the lending path."""
    redeem = ACCOUNTING_TREATMENT_SPEC.position_key(
        protocol="pendle_v2",
        intent_type="WITHDRAW",
        chain="Arbitrum",
        wallet="0xWallet",
        intent=SimpleNamespace(protocol="pendle_v2", token="WSTETH", market_id="0xYT"),
    )
    assert redeem is not None  # owned — runner must NOT take the lending branch
    key, market = redeem
    assert key == ""  # deferred to _pt_context (ledger-derived)
    assert market == "0xyt"


# --- version stamp == 5 for PT events after augment (VIB-5316 SWAP bump) ------


@pytest.mark.parametrize("event_type", ["PT_BUY", "PT_SELL", "PT_REDEEM"])
def test_pt_events_stamp_primitive_version_5(event_type: str):
    """PT_BUY / PT_SELL / PT_REDEEM all taxonomy-map to SWAP, now bumped to v5
    (v4→v5 = PT_BUY now populates the buy-time ``sy_price`` the held-PT USD cost
    basis is anchored to; v3→v4 was the raw-18 → human payload-unit move)."""
    import json as _json

    from almanak.framework.accounting.writer import augment_accounting_payload

    decoded = _json.loads(augment_accounting_payload(_json.dumps({"event_type": event_type}), is_live=True))
    assert decoded["primitive_version"] == 5


# --- golden: _build_pt_buy payload is byte-identical to the buy contract -----


def test_pt_buy_payload_byte_identity_golden():
    """Pin the PT_BUY payload bytes so a future builder edit can't silently drift it.

    The buy event PAYLOAD stores HUMAN units (uniform PT convention, VIB-4988
    v3→v4) + the exact field set the handler emits.
    """
    sy_in, pt_out = int(0.9 * 10**18), int(1.0 * 10**18)
    extracted = json.dumps({"swap_amounts": {"amount_in": sy_in, "amount_out": pt_out}})
    ob = _outbox("SWAP", market_id="0xmarket")
    led = _ledger("SWAP", token_out="PT-wstETH-25JUN2030", extracted=extracted)

    event = handle_pendle_pt(ob, led, basis_store=None)
    assert event is not None
    payload = json.loads(event.to_payload_json())

    # Human-unit amounts on the event payload (raw / 1e18).
    assert Decimal(payload["pt_amount"]) == Decimal("1")
    assert Decimal(payload["sy_amount"]) == Decimal("0.9")
    assert payload["event_type"] == "PT_BUY"
    assert payload["realized_yield_usd"] is None
    assert payload["basis_lot_id"] is None
    # Field set is stable.
    assert set(payload.keys()) == {
        "event_type",
        "position_key",
        "market_id",
        "pt_token",
        "maturity_timestamp",
        "pt_amount",
        "sy_amount",
        "pt_price",
        "sy_price",
        "implied_apr_bps",
        "days_to_maturity",
        "realized_yield_usd",
        "basis_lot_id",
        "confidence",
        "unavailable_reason",
        "schema_version",
        "primitive_version",
    }
