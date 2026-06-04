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
from types import SimpleNamespace

import pytest

from almanak.connectors._strategy_base.accounting_treatment_registry import (
    AccountingTreatmentRegistry,
)
from almanak.connectors.pendle.accounting_spec import (
    ACCOUNTING_TREATMENT_SPEC,
    handle_pendle_lp,
    handle_pendle_pt,
)
from almanak.framework.accounting import pendle_accounting as legacy
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
    extracted: str = "",
    tx_hash: str = "0xdeadbeef",
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
        "tx_hash": tx_hash,
        "extracted_data_json": extracted,
    }


# --- categorization (replaces taxonomy.classify's pendle branches) ----------


@pytest.mark.parametrize("intent_type", ["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"])
def test_categorize_pendle_lp_to_generic_lp(intent_type: str):
    decision = ACCOUNTING_TREATMENT_SPEC.categorize(intent_type, "pendle_v2", "")
    assert decision is not None
    assert decision.category is AccountingCategory.LP  # generic, not a protocol-named member
    assert decision.treatment_key == "pendle_lp"


def test_categorize_pendle_pt_to_generic_swap():
    decision = ACCOUNTING_TREATMENT_SPEC.categorize("SWAP", "pendle", "PT-wstETH-25JUN2030")
    assert decision is not None
    assert decision.category is AccountingCategory.SWAP
    assert decision.treatment_key == "pendle_pt"


@pytest.mark.parametrize(
    ("intent_type", "protocol", "token_out"),
    [
        ("LP_OPEN", "uniswap_v3", ""),  # non-pendle LP
        ("SWAP", "pendle", "USDC"),  # pendle swap, but token_out is not a PT- token
        ("SWAP", "uniswap_v3", "PT-x"),  # PT token but non-pendle protocol
        ("SUPPLY", "pendle", ""),  # pendle but not an LP/PT intent
    ],
)
def test_categorize_declines_unclaimed(intent_type: str, protocol: str, token_out: str):
    assert ACCOUNTING_TREATMENT_SPEC.categorize(intent_type, protocol, token_out) is None


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


def test_position_key_lp_matches_legacy_helpers():
    # The relocated _derive_pendle_position_key / _get_market_address must produce
    # the same (position_key, market_id) the runner's pendle LP branch did.
    intent = SimpleNamespace(protocol="pendle_v2", pool="WETH/0xMarketAddr")
    result = ACCOUNTING_TREATMENT_SPEC.position_key(
        protocol="pendle_v2", intent_type="LP_OPEN", chain="Arbitrum", wallet="0xWallet", intent=intent
    )
    market = legacy._get_market_address(intent)
    assert result == (legacy._derive_pendle_position_key("Arbitrum", "0xWallet", market), market)


def test_position_key_pt_matches_legacy_runner_logic():
    intent = SimpleNamespace(protocol="pendle_v2", pool="0xMarketAddr")
    result = ACCOUNTING_TREATMENT_SPEC.position_key(
        protocol="pendle_v2", intent_type="SWAP", chain="Arbitrum", wallet="0xWallet", intent=intent
    )
    market = "0xmarketaddr"
    assert result == (f"pendle_pt:arbitrum:0xwallet:{market}", market)


def test_position_key_declines_non_pendle():
    intent = SimpleNamespace(protocol="uniswap_v3", pool="0xm")
    assert (
        ACCOUNTING_TREATMENT_SPEC.position_key(
            protocol="uniswap_v3", intent_type="LP_OPEN", chain="arbitrum", wallet="0x1", intent=intent
        )
        is None
    )
