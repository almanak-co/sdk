"""VIB-4087 — sub-transaction detail + slippage_source attribution.

Pre-fix:
* `transaction_ledger` collapsed multi-tx intents into a single row with one
  `tx_hash` and aggregate `gas_used`. Operators couldn't tell approval-leg
  failures from action-leg failures, or audit per-leg gas.
* `slippage_bps` was a bare integer. A 0-bps reading from on-chain log
  decoding was indistinguishable from a 0-bps reading from balance-delta
  fallback or "no source available."

Post-fix:
* `extracted_data_json.sub_transactions` is a typed array with role
  classification (APPROVAL / ACTION / INCIDENTAL).
* `transaction_ledger.tx_hash` always points at the ACTION sub-tx.
* `SwapAmounts.slippage_source` is a `SlippageSource` enum value.
* uniswap_v3 receipt parser stamps RECEIPT_DECODED.
"""

from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.execution.extracted_data import SlippageSource, SwapAmounts
from almanak.framework.observability.ledger import (
    _build_sub_transactions,
    _classify_sub_tx_role,
    _build_extracted_data_json,
    _extract_tx_and_gas,
    deserialize_extracted_data,
    serialize_extracted_data,
)


# ──────────────────────────────────────────────────────────────────────────
# Role classification — APPROVAL / ACTION / INCIDENTAL
# ──────────────────────────────────────────────────────────────────────────

ERC20_APPROVAL_TOPIC = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"


def _make_tx_result(*, logs=None, status=1, gas_used=50_000, tx_hash="0xabc", to_address="0xToken"):
    receipt = SimpleNamespace(
        logs=logs or [],
        status=status,
        to_address=to_address,
    )
    return SimpleNamespace(
        tx_hash=tx_hash,
        success=(status == 1),
        gas_used=gas_used,
        receipt=receipt,
    )


def test_approval_event_only_classifies_as_approval():
    """A pure approve() emits ONLY Approval events ⇒ APPROVAL."""
    tr = _make_tx_result(logs=[{"topics": [ERC20_APPROVAL_TOPIC, "0xowner", "0xspender"]}])
    assert _classify_sub_tx_role(tr) == "APPROVAL"


def test_non_approval_event_classifies_as_action():
    """Swap event (not Approval) is classified ACTION."""
    swap_topic = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
    tr = _make_tx_result(logs=[{"topics": [swap_topic]}])
    assert _classify_sub_tx_role(tr) == "ACTION"


def test_approval_alongside_other_event_classifies_as_action():
    """VIB-4087 — an LP_OPEN mint emits ERC721 Approval(0x0, owner, tokenId)
    as a side-effect of minting the NFT. The naive "any Approval ⇒ APPROVAL"
    rule misclassified the action transaction. The refined rule (ONLY
    Approval events ⇒ APPROVAL; otherwise ACTION) correctly handles this."""
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    tr = _make_tx_result(
        logs=[
            {"topics": [transfer_topic]},
            {"topics": [ERC20_APPROVAL_TOPIC]},  # ERC721 Approval side-effect
        ]
    )
    assert _classify_sub_tx_role(tr) == "ACTION"


def test_no_logs_classifies_as_action():
    """A receipt with no logs at all defaults to ACTION (the safe default —
    we never want to silently elevate an unclassified leg to APPROVAL)."""
    tr = _make_tx_result(logs=[])
    assert _classify_sub_tx_role(tr) == "ACTION"


def test_role_classification_handles_missing_receipt():
    """A TransactionResult with no receipt (e.g. paper/dry_run) still
    classifies — defaults to ACTION."""
    tr = SimpleNamespace(tx_hash="0xabc", success=True, gas_used=21_000, receipt=None)
    assert _classify_sub_tx_role(tr) == "ACTION"


def test_role_case_insensitive_topic_match():
    """Topic comparison must be case-insensitive — receipt parsers vary on
    hex-case conventions."""
    tr = _make_tx_result(logs=[{"topics": [ERC20_APPROVAL_TOPIC.upper()]}])
    assert _classify_sub_tx_role(tr) == "APPROVAL"


# ──────────────────────────────────────────────────────────────────────────
# sub_transactions[] shape
# ──────────────────────────────────────────────────────────────────────────


def test_build_sub_transactions_shape():
    approve_tr = _make_tx_result(
        logs=[{"topics": [ERC20_APPROVAL_TOPIC]}],
        gas_used=46_123,
        tx_hash="0xapprove",
        to_address="0xUSDC",
    )
    action_tr = _make_tx_result(
        logs=[{"topics": ["0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"]}],
        gas_used=240_000,
        tx_hash="0xaction",
        to_address="0xRouter",
    )
    rows = _build_sub_transactions([approve_tr, action_tr])
    assert len(rows) == 2
    assert rows[0]["tx_hash"] == "0xapprove"
    assert rows[0]["target_contract"] == "0xUSDC"
    assert rows[0]["gas_used"] == 46_123
    assert rows[0]["status"] == "success"
    assert rows[0]["role"] == "APPROVAL"
    assert rows[0]["function_selector"] == ""  # plumbing-deferred per docstring
    assert rows[1]["role"] == "ACTION"
    assert rows[1]["target_contract"] == "0xRouter"


def test_build_sub_transactions_failure_status():
    failed_tr = _make_tx_result(status=0, tx_hash="0xfailed")
    rows = _build_sub_transactions([failed_tr])
    assert rows[0]["status"] == "failure"


# ──────────────────────────────────────────────────────────────────────────
# Parent tx_hash invariant — picks ACTION, not APPROVAL
# ──────────────────────────────────────────────────────────────────────────


def test_parent_tx_hash_picks_action_not_approval():
    """``transaction_ledger.tx_hash`` must point at the action transaction.
    Pre-fix the ledger writer always picked tx_results[0], which for a
    SUPPLY = approve+supply bundle pointed at the approval — making the
    parent useless for "what was the action?" audits."""
    approve_tr = _make_tx_result(
        logs=[{"topics": [ERC20_APPROVAL_TOPIC]}], tx_hash="0xapprove", gas_used=46_000
    )
    action_tr = _make_tx_result(
        logs=[{"topics": ["0x123ddd"]}], tx_hash="0xaction", gas_used=240_000
    )
    result = SimpleNamespace(
        transaction_results=[approve_tr, action_tr],
        total_gas_used=286_000,
        total_gas_cost_wei=None,
        gas_cost_usd=None,
    )

    tx_hash, gas_used, gas_usd = _extract_tx_and_gas(result)

    assert tx_hash == "0xaction"
    assert gas_used == 286_000


def test_parent_tx_hash_falls_back_to_first_when_no_action():
    """Pathological case: a bundle of all-APPROVAL legs (shouldn't happen
    in production but defensive). Falls back to first tx_hash so the
    ledger row at least carries a known hash rather than empty string."""
    approve_a = _make_tx_result(
        logs=[{"topics": [ERC20_APPROVAL_TOPIC]}], tx_hash="0xapprove_a"
    )
    approve_b = _make_tx_result(
        logs=[{"topics": [ERC20_APPROVAL_TOPIC]}], tx_hash="0xapprove_b"
    )
    result = SimpleNamespace(
        transaction_results=[approve_a, approve_b],
        total_gas_used=92_000,
        total_gas_cost_wei=None,
        gas_cost_usd=None,
    )
    tx_hash, _, _ = _extract_tx_and_gas(result)
    assert tx_hash == "0xapprove_a"


def test_parent_tx_hash_single_tx():
    """Single-tx intents (already-approved swap) preserve the hash."""
    only_tr = _make_tx_result(tx_hash="0xonly")
    result = SimpleNamespace(
        transaction_results=[only_tr],
        total_gas_used=200_000,
        total_gas_cost_wei=None,
        gas_cost_usd=None,
    )
    tx_hash, _, _ = _extract_tx_and_gas(result)
    assert tx_hash == "0xonly"


# ──────────────────────────────────────────────────────────────────────────
# extracted_data_json — sub_transactions key emission
# ──────────────────────────────────────────────────────────────────────────


def test_extracted_data_json_emits_sub_transactions_for_single_tx():
    """VIB-4087: sub_transactions is emitted for every result with at least
    one transaction, not only multi-tx. Pre-fix only multi-tx results
    had ``all_tx_results`` populated; operators couldn't tell "single
    tx" from "missing data."""
    tr = _make_tx_result(tx_hash="0xonly")
    result = SimpleNamespace(
        extracted_data={"some_field": "value"},
        transaction_results=[tr],
    )
    s = _build_extracted_data_json(result)
    assert s
    parsed = json.loads(s)
    assert "sub_transactions" in parsed
    assert len(parsed["sub_transactions"]) == 1
    assert parsed["sub_transactions"][0]["tx_hash"] == "0xonly"


def test_extracted_data_json_keeps_legacy_all_tx_results_for_multi_tx():
    """Back-compat: any reader still on the pre-VIB-4087 schema can keep
    reading ``all_tx_results``. Strictly cheaper than coordinating a
    cross-repo removal."""
    approve = _make_tx_result(
        logs=[{"topics": [ERC20_APPROVAL_TOPIC]}], tx_hash="0xapprove"
    )
    action = _make_tx_result(tx_hash="0xaction")
    result = SimpleNamespace(
        extracted_data={"k": "v"},
        transaction_results=[approve, action],
    )
    parsed = json.loads(_build_extracted_data_json(result))
    assert "sub_transactions" in parsed
    assert "all_tx_results" in parsed
    assert len(parsed["all_tx_results"]) == 2


# ──────────────────────────────────────────────────────────────────────────
# SwapAmounts.slippage_source — round-trip + default
# ──────────────────────────────────────────────────────────────────────────


def test_slippage_source_default_is_none():
    sa = SwapAmounts(amount_in=1_000_000, amount_out=500_000_000_000_000_000, amount_in_decimal=Decimal("1"), amount_out_decimal=Decimal("0.5"))
    assert sa.slippage_source == SlippageSource.NONE


def test_swap_amounts_to_dict_emits_slippage_source():
    sa = SwapAmounts(
        amount_in=1_000_000,
        amount_out=500_000_000_000_000_000,
        amount_in_decimal=Decimal("1"),
        amount_out_decimal=Decimal("0.5"),
        slippage_bps=15,
        slippage_source=SlippageSource.RECEIPT_DECODED,
    )
    d = sa.to_dict()
    assert d["slippage_source"] == "RECEIPT_DECODED"


def test_swap_amounts_round_trip_preserves_slippage_source():
    """Serialise → deserialise must preserve the typed enum."""
    original = SwapAmounts(
        amount_in=1_000_000,
        amount_out=500_000_000_000_000_000,
        amount_in_decimal=Decimal("1"),
        amount_out_decimal=Decimal("0.5"),
        slippage_bps=15,
        slippage_source=SlippageSource.RECEIPT_DECODED,
    )
    s = serialize_extracted_data({"swap_amounts": original})
    decoded = deserialize_extracted_data(s)["swap_amounts"]
    assert isinstance(decoded.slippage_source, SlippageSource)
    assert decoded.slippage_source == SlippageSource.RECEIPT_DECODED
    assert decoded.slippage_bps == 15


def test_legacy_payload_without_slippage_source_defaults_to_none():
    """A payload serialised before VIB-4087 had no `slippage_source` key.
    Reading it back must default to NONE rather than raise — the
    contract is "slippage_source is always a known value at read
    time"."""
    legacy = {
        "swap_amounts": {
            "_type": "SwapAmounts",
            "amount_in": "1000000",
            "amount_out": "500000000000000000",
            "amount_in_decimal": "1",
            "amount_out_decimal": "0.5",
            "effective_price": None,
            "slippage_bps": None,
            "expected_out_decimal": None,
            "token_in": None,
            "token_out": None,
            "amount_in_decimal_resolved": True,
            "amount_out_decimal_resolved": True,
        }
    }
    json_str = json.dumps(legacy)
    decoded = deserialize_extracted_data(json_str)["swap_amounts"]
    assert decoded.slippage_source == SlippageSource.NONE


def test_unknown_slippage_source_value_degrades_to_none():
    """A hand-crafted payload (or future schema) carrying an unknown
    slippage_source value must NOT crash deserialisation — degrade to
    NONE so older DBs replay cleanly."""
    payload = {
        "swap_amounts": {
            "_type": "SwapAmounts",
            "amount_in": "1000000",
            "amount_out": "500000000000000000",
            "amount_in_decimal": "1",
            "amount_out_decimal": "0.5",
            "effective_price": None,
            "slippage_bps": None,
            "expected_out_decimal": None,
            "token_in": None,
            "token_out": None,
            "amount_in_decimal_resolved": True,
            "amount_out_decimal_resolved": True,
            "slippage_source": "FUTURE_VALUE",
        }
    }
    decoded = deserialize_extracted_data(json.dumps(payload))["swap_amounts"]
    assert decoded.slippage_source == SlippageSource.NONE


# ──────────────────────────────────────────────────────────────────────────
# VIB-5066 — position_events.tx_hash must point at the ACTION sub-tx, not
# the APPROVAL leg. Pre-fix ``_tx_and_gas_details`` always took
# ``transaction_results[0]``, which for an approve+action bundle pointed at
# the approval — so position_events disagreed with transaction_ledger
# (which already picks the ACTION leg per VIB-4087). The fix reuses the same
# ``_classify_sub_tx_role`` heuristic the ledger writer uses.
# ──────────────────────────────────────────────────────────────────────────


def _event_ctx():
    """Minimal IntentEventContext for ``_tx_and_gas_details``.

    ``_tx_and_gas_details`` only reads ``ctx.chain`` / ``ctx.price_oracle``
    on the gas-USD path; the tx_hash branch under test needs neither, so a
    bare context with empty gas inputs is sufficient.
    """
    from almanak.framework.observability.position_events import IntentEventContext

    return IntentEventContext(
        intent=None,
        result=None,
        extracted={},
        deployment_id="deployment:test",
        chain="ethereum",
        ledger_entry_id="ledger:test",
        price_oracle=None,
    )


def test_position_events_tx_hash_picks_action_not_approval():
    """``position_events.tx_hash`` must point at the ACTION transaction.
    Pre-fix the writer always picked tx_results[0], which for a
    SUPPLY = approve+supply bundle pointed at the approval."""
    from almanak.framework.observability.position_events import _tx_and_gas_details

    approve_tr = _make_tx_result(
        logs=[{"topics": [ERC20_APPROVAL_TOPIC]}], tx_hash="0xapprove", gas_used=46_000
    )
    action_tr = _make_tx_result(
        logs=[{"topics": ["0x123ddd"]}], tx_hash="0xaction", gas_used=240_000
    )
    result = SimpleNamespace(
        transaction_results=[approve_tr, action_tr],
        total_gas_used=286_000,
        total_gas_cost_wei=None,
        gas_cost_usd=None,
    )

    tx_hash, _gas_usd = _tx_and_gas_details(_event_ctx(), result)

    assert tx_hash == "0xaction"


def test_position_events_tx_hash_falls_back_to_first_when_no_action():
    """Pathological all-APPROVAL bundle: fall back to the first hash rather
    than emit empty string. Never silently picks an approval *over* an
    available action — but with no action present, first is the safe hash."""
    from almanak.framework.observability.position_events import _tx_and_gas_details

    approve_a = _make_tx_result(
        logs=[{"topics": [ERC20_APPROVAL_TOPIC]}], tx_hash="0xapprove_a"
    )
    approve_b = _make_tx_result(
        logs=[{"topics": [ERC20_APPROVAL_TOPIC]}], tx_hash="0xapprove_b"
    )
    result = SimpleNamespace(
        transaction_results=[approve_a, approve_b],
        total_gas_used=92_000,
        total_gas_cost_wei=None,
        gas_cost_usd=None,
    )

    tx_hash, _gas_usd = _tx_and_gas_details(_event_ctx(), result)

    assert tx_hash == "0xapprove_a"


def test_position_events_tx_hash_single_tx():
    """Single-tx intents (already-approved swap) preserve the hash."""
    from almanak.framework.observability.position_events import _tx_and_gas_details

    only_tr = _make_tx_result(tx_hash="0xonly")
    result = SimpleNamespace(
        transaction_results=[only_tr],
        total_gas_used=200_000,
        total_gas_cost_wei=None,
        gas_cost_usd=None,
    )

    tx_hash, _gas_usd = _tx_and_gas_details(_event_ctx(), result)

    assert tx_hash == "0xonly"
