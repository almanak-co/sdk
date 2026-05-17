"""VIB-4085 — lending position lifecycle in position_events.

Pre-fix: ``almanak/framework/observability/position_events.py`` only mapped
LP_OPEN/LP_CLOSE/LP_COLLECT_FEES/PERP_OPEN/PERP_CLOSE to PositionEvent
rows. Lending intents (SUPPLY/BORROW/REPAY/WITHDRAW) were explicitly
excluded ("Fungible positions ... are excluded.") so a full Aave V3
looping round-trip produced 0 rows in `position_events` despite 10
successful intents in the ledger.

Post-fix:

* ``PositionType`` gains ``LENDING_COLLATERAL`` and ``LENDING_DEBT``.
* ``PositionEventType`` gains ``INCREASE`` / ``DECREASE`` for non-monotonic
  lending lifecycle.
* SUPPLY / BORROW / REPAY / WITHDRAW now produce events.
* ``_apply_lending`` refines the static OPEN/CLOSE default into
  OPEN/INCREASE (cache-driven) or CLOSE/DECREASE (post-state-driven).

These tests pin the lifecycle decision logic without touching disk.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from almanak.framework.observability.position_events import (
    INTENT_TO_EVENT_TYPE,
    LENDING_CLOSE_DUST_USD,
    PositionEventType,
    PositionType,
    _apply_lending,
    _resolve_position_type,
    build_position_event_from_intent,
    lending_position_id,
)

# ──────────────────────────────────────────────────────────────────────────
# Static dispatch — VIB-4085 added five new keys
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "intent_type,expected_event_type,expected_position_type",
    [
        ("SUPPLY", PositionEventType.OPEN, PositionType.LENDING_COLLATERAL),
        ("BORROW", PositionEventType.OPEN, PositionType.LENDING_DEBT),
        ("REPAY", PositionEventType.CLOSE, PositionType.LENDING_DEBT),
        ("WITHDRAW", PositionEventType.CLOSE, PositionType.LENDING_COLLATERAL),
        ("DELEVERAGE", PositionEventType.CLOSE, PositionType.LENDING_DEBT),
    ],
)
def test_lending_intents_in_static_dispatch(intent_type, expected_event_type, expected_position_type):
    assert INTENT_TO_EVENT_TYPE[intent_type] == expected_event_type
    # VIB-4162 (T2): position_type now comes from the taxonomy via
    # _resolve_position_type instead of the deleted INTENT_TO_POSITION_TYPE
    # dict. Behaviour for known position-producing intents is identical.
    assert _resolve_position_type(intent_type) == expected_position_type


def test_lp_and_perp_dispatch_unchanged():
    """Regression guard — lending additions must not displace the existing
    LP/PERP mappings or the SWAP exclusion that the swap_fallback path
    depends on."""
    for k in ("LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES", "PERP_OPEN", "PERP_CLOSE"):
        assert k in INTENT_TO_EVENT_TYPE
        # VIB-4162 (T2): every position-producing intent resolves through
        # the taxonomy. SWAP/BRIDGE are still not in the position-event
        # static dispatch — they bypass the strict lookup at the seed
        # gate.
        assert _resolve_position_type(k) is not None
    assert "SWAP" not in INTENT_TO_EVENT_TYPE
    assert "BRIDGE" not in INTENT_TO_EVENT_TYPE


# ──────────────────────────────────────────────────────────────────────────
# position_id derivation — must match LendingAccountingEvent.position_key
# ──────────────────────────────────────────────────────────────────────────


def test_lending_position_id_canonical_shape():
    pid = lending_position_id(
        chain="ARBITRUM",
        protocol="Aave_V3",
        wallet="0xABCDEF1234567890abcdef1234567890abcdef12",
        asset="USDC",
    )
    assert pid == "lending:arbitrum:aave_v3:0xabcdef1234567890abcdef1234567890abcdef12:usdc"


def test_lending_position_id_handles_missing_segments():
    pid = lending_position_id(chain="", protocol="", wallet="", asset="")
    assert pid == "lending:unknown:unknown:unknown:unknown"


# ──────────────────────────────────────────────────────────────────────────
# OPEN vs INCREASE — cache-driven
# ──────────────────────────────────────────────────────────────────────────


def _make_lending_intent(intent_type: str, *, asset: str = "USDC", protocol: str = "aave_v3", amount: int = 1_000_000):
    return SimpleNamespace(
        intent_type=SimpleNamespace(value=intent_type),
        amount_token=asset,
        token_in=asset,
        amount=amount,
        protocol=protocol,
    )


def _make_result(intent_type: str, *, amount: int = 1_000_000):
    """Build a minimal ExecutionResult-shaped namespace."""
    extracted_key = {
        "SUPPLY": "supply_amount",
        "BORROW": "borrow_amount",
        "REPAY": "repay_amount",
        "WITHDRAW": "withdraw_amount",
    }[intent_type]
    return SimpleNamespace(
        extracted_data={extracted_key: amount},
        transaction_results=[SimpleNamespace(tx_hash=f"0x{intent_type.lower()}deadbeef")],
        gas_cost_usd="0.1",
    )


def test_first_supply_emits_open():
    """Cache miss → OPEN. The first SUPPLY in a fresh deployment opens the
    collateral leg."""
    intent = _make_lending_intent("SUPPLY")
    result = _make_result("SUPPLY", amount=2_000_000)  # 2 USDC

    cache: dict = {}
    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-1",
        chain="arbitrum",
        recent_open_events=cache,
        post_state={"collateral_value_usd": "2.0", "debt_value_usd": "0", "health_factor": "999999"},
        wallet_address="0x1234567890123456789012345678901234567890",
    )

    assert event is not None
    assert event.event_type == PositionEventType.OPEN.value
    assert event.position_type == PositionType.LENDING_COLLATERAL.value
    assert event.position_id == ("lending:arbitrum:aave_v3:0x1234567890123456789012345678901234567890:usdc")
    assert event.token0 == "USDC"
    assert event.amount0 == "2000000"
    assert event.value_usd == "2.0"


def test_second_supply_emits_increase():
    """Cache hit → INCREASE. A subsequent SUPPLY on the same position_id
    grows the existing collateral leg rather than opening a new one."""
    intent = _make_lending_intent("SUPPLY")
    result = _make_result("SUPPLY", amount=3_000_000)

    pid = lending_position_id(
        chain="arbitrum",
        protocol="aave_v3",
        wallet="0x1234567890123456789012345678901234567890",
        asset="USDC",
    )
    cache = {(pid, str(PositionType.LENDING_COLLATERAL)): {"value_usd": "2.0"}}

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-2",
        chain="arbitrum",
        recent_open_events=cache,
        post_state={"collateral_value_usd": "5.0", "debt_value_usd": "0", "health_factor": "999999"},
        wallet_address="0x1234567890123456789012345678901234567890",
    )

    assert event is not None
    assert event.event_type == PositionEventType.INCREASE.value
    assert event.value_usd == "5.0"


def test_borrow_uses_lending_debt_position_type():
    """BORROW maps to LENDING_DEBT, not LENDING_COLLATERAL — the debt leg
    is its own lifecycle."""
    intent = _make_lending_intent("BORROW", asset="USDT")
    result = _make_result("BORROW", amount=1_000_000)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-3",
        chain="arbitrum",
        recent_open_events={},
        post_state={"collateral_value_usd": "5.0", "debt_value_usd": "1.0", "health_factor": "1.5"},
        wallet_address="0xWALLET",
    )

    assert event is not None
    assert event.event_type == PositionEventType.OPEN.value
    assert event.position_type == PositionType.LENDING_DEBT.value
    assert event.token0 == "USDT"
    # value_usd reads the debt leg's post-state (not collateral)
    assert event.value_usd == "1.0"


# ──────────────────────────────────────────────────────────────────────────
# CLOSE vs DECREASE — post-state-driven, dust-aware
# ──────────────────────────────────────────────────────────────────────────


def test_partial_repay_emits_decrease():
    """Debt remains > dust ⇒ DECREASE. The debt leg is partially unwound
    but still active."""
    intent = _make_lending_intent("REPAY", asset="USDT")
    result = _make_result("REPAY", amount=500_000)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-4",
        chain="arbitrum",
        recent_open_events={},
        post_state={"collateral_value_usd": "5.0", "debt_value_usd": "0.5", "health_factor": "10"},
        wallet_address="0xWALLET",
    )

    assert event is not None
    assert event.event_type == PositionEventType.DECREASE.value
    assert event.position_type == PositionType.LENDING_DEBT.value
    assert event.value_usd == "0.5"


def test_full_repay_emits_close():
    """Debt at-or-below dust ⇒ CLOSE."""
    intent = _make_lending_intent("REPAY", asset="USDT")
    result = _make_result("REPAY", amount=1_000_000)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-5",
        chain="arbitrum",
        recent_open_events={},
        post_state={"collateral_value_usd": "5.0", "debt_value_usd": "0", "health_factor": "999999"},
        wallet_address="0xWALLET",
    )

    assert event is not None
    assert event.event_type == PositionEventType.CLOSE.value


def test_withdraw_reads_collateral_value():
    """WITHDRAW lifecycle decision reads collateral_value_usd, NOT
    debt_value_usd. A REPAY decision reading the wrong leg would
    misclassify a partial-collateral-withdraw as CLOSE whenever the debt
    happened to be zero."""
    intent = _make_lending_intent("WITHDRAW", asset="USDC")
    result = _make_result("WITHDRAW", amount=1_000_000)

    # Edge case: debt is zero but collateral > dust — must be DECREASE
    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-6",
        chain="arbitrum",
        recent_open_events={},
        post_state={"collateral_value_usd": "1.0", "debt_value_usd": "0", "health_factor": "999999"},
        wallet_address="0xWALLET",
    )

    assert event is not None
    assert event.event_type == PositionEventType.DECREASE.value
    assert event.position_type == PositionType.LENDING_COLLATERAL.value


def test_dust_threshold_is_one_cent():
    """Sub-cent residual is treated as CLOSE; sub-cent is below the
    declared LENDING_CLOSE_DUST_USD threshold."""
    from decimal import Decimal

    assert Decimal(LENDING_CLOSE_DUST_USD) == Decimal("0.01")


def test_close_at_dust_threshold():
    """leg_value == LENDING_CLOSE_DUST_USD ⇒ CLOSE (boundary inclusive)."""
    intent = _make_lending_intent("WITHDRAW", asset="USDC")
    result = _make_result("WITHDRAW", amount=1_000_000)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-7",
        chain="arbitrum",
        recent_open_events={},
        post_state={"collateral_value_usd": "0.01", "debt_value_usd": "0", "health_factor": "999999"},
        wallet_address="0xWALLET",
    )

    assert event is not None
    assert event.event_type == PositionEventType.CLOSE.value


# ──────────────────────────────────────────────────────────────────────────
# VIB-4493 — lending CLOSE.value_usd stamps pre-close balance, not 0
# ──────────────────────────────────────────────────────────────────────────


def test_repay_close_value_usd_uses_pre_close_debt():
    """A full REPAY drains debt to dust ⇒ post-state ``debt_value_usd`` is 0
    by definition. Stamping post-state into ``PositionEvent.value_usd``
    leaves the dashboard with ``0E-8`` in the "Value (USD)" column, which
    tells the operator nothing about how much was closed. When ``pre_state``
    is supplied, the writer must promote the PRE-close debt (= the closed
    amount) instead. LP_CLOSE already writes market-value-at-close;
    lending CLOSE should match that contract.

    Reproduces the dashboard finding from the VIB-4316 17-row audit
    (loop_lp_same / looping rows showed CLOSE rows with ``0E-8``).
    """
    intent = _make_lending_intent("REPAY", asset="USDT")
    result = _make_result("REPAY", amount=1_200_000)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-repay-close",
        chain="arbitrum",
        recent_open_events={},
        pre_state={"collateral_value_usd": "5.18679622", "debt_value_usd": "1.19944358"},
        post_state={"collateral_value_usd": "5.18679622", "debt_value_usd": "0", "health_factor": "999999"},
        wallet_address="0xWALLET",
    )

    assert event is not None
    assert event.event_type == PositionEventType.CLOSE.value
    assert event.position_type == PositionType.LENDING_DEBT.value
    # Must surface pre-close debt, not post-close (which is 0).
    assert event.value_usd == "1.19944358"


def test_withdraw_close_value_usd_uses_pre_close_collateral():
    """A full WITHDRAW closes the collateral leg. ``pre_state`` carries the
    pre-close collateral; that's what the dashboard should display."""
    intent = _make_lending_intent("WITHDRAW", asset="USDC")
    result = _make_result("WITHDRAW", amount=5_200_000)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-withdraw-close",
        chain="arbitrum",
        recent_open_events={},
        pre_state={"collateral_value_usd": "5.18679622", "debt_value_usd": "0"},
        post_state={"collateral_value_usd": "0", "debt_value_usd": "0", "health_factor": "999999"},
        wallet_address="0xWALLET",
    )

    assert event is not None
    assert event.event_type == PositionEventType.CLOSE.value
    assert event.position_type == PositionType.LENDING_COLLATERAL.value
    assert event.value_usd == "5.18679622"


def test_decrease_stamps_action_delta_not_post_state():
    """Partial WITHDRAW ⇒ DECREASE. The dashboard's "Value (USD)" column
    means the *size of the action*, not the post-state remaining. Stamping
    post-state (= 4.0 in this fixture) reads to an operator as "the
    DECREASE moved 4.0 USDC" — wrong; the action actually moved 1.0.

    Reproduces the dashboard finding from the looping-aave_v3-base row in
    the VIB-4316 batch-2 audit: a 1.39-USDC partial WITHDRAW displayed as
    5.63 USDC (= post-state remaining), which collided visually with the
    subsequent full-close row that also displayed 5.63.
    """
    intent = _make_lending_intent("WITHDRAW", asset="USDC")
    result = _make_result("WITHDRAW", amount=1_000_000)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-withdraw-partial",
        chain="arbitrum",
        recent_open_events={},
        pre_state={"collateral_value_usd": "5.0", "debt_value_usd": "0"},
        post_state={"collateral_value_usd": "4.0", "debt_value_usd": "0", "health_factor": "999999"},
        wallet_address="0xWALLET",
    )

    assert event is not None
    assert event.event_type == PositionEventType.DECREASE.value
    # Action delta (|5.0 - 4.0|), not post-state remaining (4.0).
    assert event.value_usd == "1.0"


def test_increase_stamps_action_delta_not_post_state():
    """Second SUPPLY on the same collateral leg ⇒ INCREASE. The action
    delta = pre - post (with abs) = the supply amount, not the post-state
    new total. Pre-fix the dashboard read "INCREASE 5.0 USDC" when the
    action only added 3.0 to a pre-existing 2.0 balance."""
    intent = _make_lending_intent("SUPPLY")
    result = _make_result("SUPPLY", amount=3_000_000)

    pid = lending_position_id(
        chain="arbitrum",
        protocol="aave_v3",
        wallet="0xWALLET",
        asset="USDC",
    )
    cache = {(pid, str(PositionType.LENDING_COLLATERAL)): {"value_usd": "2.0"}}

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-increase",
        chain="arbitrum",
        recent_open_events=cache,
        pre_state={"collateral_value_usd": "2.0", "debt_value_usd": "0"},
        post_state={"collateral_value_usd": "5.0", "debt_value_usd": "0", "health_factor": "999999"},
        wallet_address="0xWALLET",
    )

    assert event is not None
    assert event.event_type == PositionEventType.INCREASE.value
    # Action delta (|2.0 - 5.0|), not post-state total (5.0).
    assert event.value_usd == "3.0"


def test_open_with_preexisting_balance_stamps_action_delta():
    """OPEN where the wallet had a pre-existing collateral balance from a
    prior run on the same shared Anvil fork. Pre-fix the dashboard stamped
    the post-state (= pre-existing + new contribution), making the OPEN
    look bigger than the action actually was. Reproduces the OPEN
    COLLATERAL row in looping-aave_v3-base where value_usd=7.018 but the
    SUPPLY itself only moved 4.0 (because pre-existing collat was 3.018).
    """
    intent = _make_lending_intent("SUPPLY")
    result = _make_result("SUPPLY", amount=4_000_000)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-open-with-preexisting",
        chain="arbitrum",
        recent_open_events={},  # empty cache → static dispatch keeps OPEN
        pre_state={"collateral_value_usd": "3.01849760", "debt_value_usd": "0"},
        post_state={
            "collateral_value_usd": "7.01789752",
            "debt_value_usd": "0",
            "health_factor": "999999",
        },
        wallet_address="0xWALLET",
    )

    assert event is not None
    assert event.event_type == PositionEventType.OPEN.value
    # Action delta (|3.01849760 - 7.01789752|), not post-state (7.01789752).
    assert event.value_usd == "3.99939992"


def test_close_without_pre_state_falls_back_to_post_state():
    """Backward-compat: callers that don't yet pass ``pre_state`` (legacy
    paper / dry-run paths, third-party harnesses, fixtures) keep the
    pre-fix behaviour — post-state leg value lands in ``value_usd`` even
    if that's 0. The fix is opt-in via ``pre_state``."""
    intent = _make_lending_intent("REPAY", asset="USDT")
    result = _make_result("REPAY", amount=1_000_000)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-no-prestate",
        chain="arbitrum",
        recent_open_events={},
        # pre_state intentionally omitted
        post_state={"collateral_value_usd": "5.0", "debt_value_usd": "0", "health_factor": "999999"},
        wallet_address="0xWALLET",
    )

    assert event is not None
    assert event.event_type == PositionEventType.CLOSE.value
    # No pre_state ⇒ keep legacy post-state stamp (0). Test pins this so
    # we know if a future refactor accidentally tightens the contract.
    assert event.value_usd == "0"


# ──────────────────────────────────────────────────────────────────────────
# attribution_json — v1 lending payload
# ──────────────────────────────────────────────────────────────────────────


def test_attribution_payload_carries_lending_fields():
    intent = _make_lending_intent("WITHDRAW", asset="USDC")
    result = _make_result("WITHDRAW", amount=1_000_000)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-8",
        chain="arbitrum",
        recent_open_events={},
        post_state={
            "collateral_value_usd": "0",
            "debt_value_usd": "0",
            "health_factor": "999999",
            "liquidation_threshold": "0.78",
            "supply_apr_bps": 95,
            "borrow_apr_bps": 385,
        },
        wallet_address="0xWALLET",
    )

    assert event is not None
    assert event.attribution_json
    payload = json.loads(event.attribution_json)
    assert payload["version"] == 1
    assert payload["schema"] == "lending_v1"
    assert payload["health_factor_after"] == "999999"
    assert payload["liquidation_threshold"] == "0.78"
    assert payload["supply_apr_bps"] == 95
    assert payload["borrow_apr_bps"] == 385
    assert payload["asset"] == "USDC"
    assert payload["intent_type"] == "WITHDRAW"


def test_attribution_skipped_when_post_state_empty():
    """No post_state (e.g. dry-run with no on-chain capture) ⇒
    attribution_json stays empty rather than fabricating zeros."""
    intent = _make_lending_intent("SUPPLY")
    result = _make_result("SUPPLY", amount=1_000_000)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-9",
        chain="arbitrum",
        recent_open_events={},
        post_state=None,
        wallet_address="0xWALLET",
    )

    assert event is not None
    # PositionEvent's default attribution_json is "{}" (not ""); _apply_lending
    # must NOT replace it with a populated payload when post_state is missing.
    assert event.attribution_json == "{}"


# ──────────────────────────────────────────────────────────────────────────
# Ensure non-lending intents are unaffected
# ──────────────────────────────────────────────────────────────────────────


def test_swap_still_produces_no_event():
    """SWAP must NOT produce a position_event. This is the regression guard
    that tells us the lending additions didn't accidentally widen the
    dispatch to include generic swaps."""
    intent = SimpleNamespace(intent_type=SimpleNamespace(value="SWAP"))
    result = SimpleNamespace(extracted_data={}, transaction_results=[])

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-10",
        chain="arbitrum",
    )

    assert event is None


def test_borrow_intent_field_name_resolves_asset():
    """Production ``BorrowIntent`` exposes the borrowed asset as
    ``borrow_token`` (not ``token`` / ``amount_token`` / ``token_in``).
    A pre-fix run of the looping harness produced
    ``position_id=lending:...:unknown`` because the resolver fell off
    the end of its fallback chain. Pin the field name lookup here so a
    refactor that removes ``borrow_token`` re-surfaces the same bug
    instead of silently regressing the integration test.
    """
    intent = SimpleNamespace(
        intent_type=SimpleNamespace(value="BORROW"),
        protocol="aave_v3",
        # Mirror BorrowIntent's actual field names — NOT the
        # ``amount_token`` synthetic shorthand the rest of this file uses.
        borrow_token="USDT",
        collateral_token="USDC",
        amount=1_200_000,
    )
    result = _make_result("BORROW", amount=1_200_000)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-borrow",
        chain="arbitrum",
        recent_open_events={},
        post_state={
            "collateral_value_usd": "4.0",
            "debt_value_usd": "1.2",
            "health_factor": "2.6",
        },
        wallet_address="0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266",
    )

    assert event is not None
    assert event.position_type == PositionType.LENDING_DEBT.value
    assert event.token0 == "USDT", "BORROW must read borrow_token (not collateral_token)"
    assert event.position_id == ("lending:arbitrum:aave_v3:0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266:usdt"), (
        "position_id asset segment must come from borrow_token"
    )
    assert event.value_usd == "1.2"


def test_post_state_compact_aliases_normalised():
    """Connectors emit either canonical names
    (``collateral_value_usd`` / ``debt_value_usd`` / ``liquidation_threshold``)
    or compact aliases (``collateral_usd`` / ``debt_usd`` /
    ``liquidation_threshold_bps``). Both must yield identical attribution
    payloads — otherwise the connector that emits aliases produces
    ``cost_basis_usd=null`` snapshots even though the data is present.
    """
    intent = _make_lending_intent("REPAY", asset="USDT")
    result = _make_result("REPAY", amount=1_000_000)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-aliases",
        chain="arbitrum",
        recent_open_events={},
        post_state={
            "collateral_usd": "5.0",
            "debt_usd": "0",
            "health_factor": "999999",
            "liquidation_threshold_bps": 7800,
        },
        wallet_address="0xWALLET",
    )

    assert event is not None
    # Lifecycle decision must read the alias to flip CLOSE.
    assert event.event_type == PositionEventType.CLOSE.value
    payload = json.loads(event.attribution_json)
    # 7800 bps == 0.78 fraction.
    assert payload["liquidation_threshold"] == "0.78"
    assert payload["debt_value_after_usd"] == "0"
    assert payload["collateral_value_after_usd"] == "5.0"


def test_post_state_root_level_metadata_preserved_when_protocol_dict_present():
    """Connectors that wrap canonical lending keys in a protocol-keyed dict
    (``{"aave_v3": {"collateral_value_usd": ...}}``) often still emit
    sibling root-level metadata (``health_factor``, ``borrow_apr``, ...).
    The normalizer must MERGE root + nested rather than discarding root.
    """
    intent = _make_lending_intent("BORROW", asset="USDT")
    result = _make_result("BORROW", amount=2_000_000)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-mixed",
        chain="arbitrum",
        recent_open_events={},
        post_state={
            "aave_v3": {
                "collateral_value_usd": "100",
                "debt_value_usd": "50",
            },
            "health_factor": "1.85",
            "liquidation_threshold": "0.80",
        },
        wallet_address="0xWALLET",
    )

    assert event is not None
    payload = json.loads(event.attribution_json)
    # nested protocol dict promoted
    assert payload["debt_value_after_usd"] == "50"
    # root-level health_factor + liquidation_threshold preserved
    assert payload["health_factor_after"] == "1.85"
    assert payload["liquidation_threshold"] == "0.80"


def test_repay_with_nan_leg_value_defaults_to_decrease():
    """``Decimal('NaN')`` is_finite() is False — a connector that mis-emits
    ``debt_value_usd='NaN'`` must NOT silently route through the dust
    comparison (which raises InvalidOperation, fell back to ``Decimal(0)``,
    and incorrectly classified the row as CLOSE before this fix). The
    behaviour now matches the ``leg_value is None`` branch: log + DECREASE.
    """
    intent = _make_lending_intent("REPAY", asset="USDT")
    result = _make_result("REPAY", amount=500)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-nan",
        chain="arbitrum",
        recent_open_events={},
        post_state={"debt_value_usd": "NaN"},
        wallet_address="0xWALLET",
    )

    assert event is not None
    assert event.event_type == PositionEventType.DECREASE.value


def test_withdraw_with_infinity_leg_value_defaults_to_decrease():
    """``Decimal('Infinity')`` is finite=False; the ``<= dust`` comparison
    silently returns False and would route to DECREASE — harmless today
    but a fragile reliance on Decimal's quirks. The explicit guard
    canonicalises the behaviour and logs the parse failure."""
    intent = _make_lending_intent("WITHDRAW", asset="USDC")
    result = _make_result("WITHDRAW", amount=500)

    event = build_position_event_from_intent(
        deployment_id="dep-1",
        intent=intent,
        result=result,
        ledger_entry_id="le-inf",
        chain="arbitrum",
        recent_open_events={},
        post_state={"collateral_value_usd": "Infinity"},
        wallet_address="0xWALLET",
    )

    assert event is not None
    assert event.event_type == PositionEventType.DECREASE.value


def test_apply_lending_no_op_for_lp_event():
    """``_apply_lending`` must early-return when invoked on an LP or PERP
    event — the helper sequence runs all enrichers regardless of position
    type, so each must be defensive."""
    from almanak.framework.observability.position_events import IntentEventContext, PositionEvent

    event = PositionEvent(
        deployment_id="d",
        position_id="5471740",
        position_type=PositionType.LP.value,
        event_type=PositionEventType.OPEN.value,
        token0="WETH",
        token1="USDC",
    )
    ctx = IntentEventContext(
        intent=SimpleNamespace(intent_type=SimpleNamespace(value="LP_OPEN")),
        result=None,
        extracted={},
        deployment_id="d",
        chain="arbitrum",
        ledger_entry_id="le",
    )
    _apply_lending(event, ctx)

    # LP fields untouched.
    assert event.position_id == "5471740"
    assert event.token0 == "WETH"
    assert event.token1 == "USDC"
    assert event.event_type == PositionEventType.OPEN.value
    # PositionEvent's default attribution_json is "{}" (not ""); _apply_lending
    # must NOT replace it with a populated payload when post_state is missing.
    assert event.attribution_json == "{}"
