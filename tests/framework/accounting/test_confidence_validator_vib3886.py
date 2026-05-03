"""VIB-3886 — confidence ⊕ unavailable_reason exclusivity validator.

The May 2 LP_OPEN payload reported ``confidence=HIGH`` simultaneously
with a non-empty ``unavailable_reason`` ("LP_OPEN cost_basis_usd
unavailable: ..."). The two are mutually exclusive by definition —
HIGH means "all USD fields populated"; an unavailable_reason means
"at least one USD field is missing". The SWAP handler degraded
correctly to ESTIMATED in the same scenario, so the LP path was the
divergent one.

These tests fence the new ``model_validator`` on ``_Versioned`` that
makes the contradiction unrepresentable, plus assert the LP handler
no longer emits the contradiction post-VIB-3886.
"""

from __future__ import annotations

import json
import uuid
from decimal import Decimal

import pytest

from almanak.framework.accounting.payload_schemas import (
    LPOpenEventPayload,
    SupplyEventPayload,
    SwapEventPayload,
    validate_payload,
)


# ──────────────────────────────────────────────────────────────────────────
# Direct payload-schema validator coverage
# ──────────────────────────────────────────────────────────────────────────


def test_high_with_empty_reason_accepted():
    """The valid happy-path: all USD fields populated, no reason needed."""
    payload = LPOpenEventPayload(
        protocol="uniswap_v3",
        position_key="lp:uniswap_v3:arbitrum:0xw:0xpool",
        pool_address="0xpool",
        token0="WETH",
        token1="USDC",
        amount0=Decimal("1"),
        amount1=Decimal("2300"),
        cost_basis_usd=Decimal("4600"),
        confidence="HIGH",
        unavailable_reason=None,
    )
    assert payload.confidence == "HIGH"
    assert payload.unavailable_reason is None


def test_high_with_empty_string_reason_accepted():
    """Empty-string ``unavailable_reason`` is treated as "no reason"."""
    payload = SwapEventPayload(
        protocol="uniswap_v3",
        token_in="USDC",
        token_out="WETH",
        amount_in=Decimal("100"),
        amount_out=Decimal("0.04"),
        amount_in_usd=Decimal("100"),
        amount_out_usd=Decimal("100"),
        confidence="HIGH",
        unavailable_reason="",
    )
    assert payload.confidence == "HIGH"


def test_high_with_non_empty_reason_rejected():
    """The May 2 LP_OPEN regression — must now raise."""
    with pytest.raises(ValueError, match="confidence=HIGH is incompatible"):
        LPOpenEventPayload(
            protocol="uniswap_v3",
            position_key="lp:uniswap_v3:arbitrum:0xw:0xpool",
            pool_address="0xpool",
            token0="WETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2300"),
            cost_basis_usd=None,
            confidence="HIGH",
            unavailable_reason="cost_basis_usd unavailable: missing prices",
        )


@pytest.mark.parametrize("conf", ["ESTIMATED", "STALE", "UNAVAILABLE"])
def test_non_high_with_reason_accepted(conf: str):
    """All non-HIGH confidence levels coexist with a non-empty reason."""
    payload = SupplyEventPayload(
        protocol="aave_v3",
        asset="USDC",
        amount=Decimal("100"),
        amount_usd=None,
        confidence=conf,
        unavailable_reason="missing oracle quote",
    )
    assert payload.confidence == conf
    assert payload.unavailable_reason == "missing oracle quote"


def test_validate_payload_wraps_validator_error():
    """``validate_payload`` raises a ValueError-with-context, not the bare
    Pydantic error — Accountant Test machinery already inspects its message
    format for cell-failure reporting."""
    bad = {
        "event_type": "LP_OPEN",
        "protocol": "uniswap_v3",
        "position_key": "k",
        "pool_address": "p",
        "token0": "WETH",
        "token1": "USDC",
        "amount0": "1",
        "amount1": "1",
        "confidence": "HIGH",
        "unavailable_reason": "cost_basis_usd unavailable",
    }
    with pytest.raises(ValueError, match="payload schema mismatch for LP_OPEN"):
        validate_payload("LP_OPEN", bad)


# ──────────────────────────────────────────────────────────────────────────
# LP handler integration — the May 2 regression scenario in miniature
# ──────────────────────────────────────────────────────────────────────────


def _make_outbox_row(led_id: str) -> dict:
    return {
        "id": led_id,
        "intent_type": "LP_OPEN",
        "wallet_address": "0xwallet",
        "position_key": "lp:uniswap_v3:arbitrum:0xwallet:0xpool",
        "market_id": "0xpool",
    }


def _make_ledger_row(led_id: str, *, price_inputs_json: str) -> dict:
    return {
        "id": led_id,
        "intent_type": "LP_OPEN",
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "token_in": "WETH",
        "token_out": "USDC",
        "amount_in": "0.000891556839636852",
        "amount_out": "2.294332",
        "extracted_data_json": "",
        "price_inputs_json": price_inputs_json,
        "deployment_id": "d",
        "strategy_id": "s",
        "cycle_id": "c",
        "execution_mode": "live",
        "tx_hash": "0xtx",
        "timestamp": "2026-05-02T11:09:19.031997+00:00",
    }


def test_lp_handler_does_not_emit_contradiction_with_nested_oracle():
    """May 2 reproducer (§9.3 of AccountingPost1977.md): nested-shape
    price_inputs_json with two priced tokens — LP handler must produce
    confidence=HIGH and empty unavailable_reason, never the
    HIGH+unavailable_reason contradiction."""
    from almanak.framework.accounting.category_handlers.lp_handler import handle_lp

    nested = json.dumps(
        {
            "WETH": {"price_usd": "2301.69", "oracle_source": "coingecko"},
            "USDC": {"price_usd": "1.0001", "oracle_source": "chainlink"},
        }
    )
    led_id = str(uuid.uuid4())
    result = handle_lp(_make_outbox_row(led_id), _make_ledger_row(led_id, price_inputs_json=nested))

    assert result is not None
    assert result.cost_basis_usd is not None
    assert result.confidence.value == "HIGH"
    assert result.unavailable_reason == ""


def test_lp_handler_degrades_to_estimated_when_pricing_missing():
    """When price_inputs_json is empty, confidence MUST degrade — never
    the HIGH+unavailable_reason contradiction."""
    from almanak.framework.accounting.category_handlers.lp_handler import handle_lp

    led_id = str(uuid.uuid4())
    result = handle_lp(_make_outbox_row(led_id), _make_ledger_row(led_id, price_inputs_json=""))

    assert result is not None
    assert result.cost_basis_usd is None
    assert result.confidence.value == "ESTIMATED"
    assert "no price_inputs_json" in result.unavailable_reason


def test_lp_handler_degrades_to_estimated_when_one_token_unpriceable():
    """Partial pricing degrades just like fully-missing pricing. The
    unit amounts are still trustworthy but the USD field isn't — that's
    the exact ESTIMATED semantic."""
    from almanak.framework.accounting.category_handlers.lp_handler import handle_lp

    half = json.dumps({"WETH": {"price_usd": "2301.69"}})
    led_id = str(uuid.uuid4())
    result = handle_lp(_make_outbox_row(led_id), _make_ledger_row(led_id, price_inputs_json=half))

    assert result is not None
    assert result.cost_basis_usd is None
    assert result.confidence.value == "ESTIMATED"
    assert "USDC" in result.unavailable_reason
