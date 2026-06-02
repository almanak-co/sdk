"""Shared Layer-5 accounting helpers for Curve LP intent tests (VIB-4600).

Curve pools are **fungible LP (ERC20 LP-token) venues**, not concentrated-
liquidity NFT systems. The accounting handler
(``almanak/framework/accounting/category_handlers/lp_handler.py``) therefore
emits the *inverse* of the Uniswap-V3 concentrated-liquidity contract — the
same family as the merged Aerodrome-Classic (Solidly) Layer-5 pilot
(``tests/intents/base/test_aerodrome_lp.py``):

  * ``LP_OPEN`` / ``LP_CLOSE`` are the typed event types (no NFT, no
    ``LP_COLLECT_FEES`` standalone path on Curve).
  * ``position_hash`` / ``tick_lower`` / ``tick_upper`` / ``liquidity`` /
    ``current_tick`` / ``in_range`` MUST be ``None`` — Curve has no tick
    bracket and fabricating one would be a correctness regression
    (Empty ≠ Zero ≠ None, docs/internal/blueprints/27).
  * ``position_id`` MUST be ``None`` — fungible LP has no per-position id.
  * ``pool_address`` is the canonical Curve descriptor the position key
    carries. Curve intent pool strings (``"3pool"``, ``"2pool"``,
    ``"crvusd_usdc"``, ``"weth_cbeth"``) contain **no ``/`` separator**, so the
    handler surfaces the bare lowercased label — NOT a ``0x`` address and NOT a
    slash-separated Solidly descriptor.

DOCUMENTED PRODUCTION GAP (VIB-4968, as of 2026-06-02)
-------------------------------------------------------------
Curve LP writes **ZERO typed accounting events** — the on-chain LP_OPEN /
LP_CLOSE land and the ledger row is written, but the LP category handler
(``category_handlers/lp_handler.py:handle_lp``) drops the event entirely and
no ``accounting_events`` row is produced. Root cause: the handler resolves the
pool address via ``_resolve_lp_pool_address`` →
``_clean_pool_address_candidate``, which (VIB-4471) accepts ONLY a
``0x``-prefixed 20-byte address / 32-byte V4 pool-id, or a slash-separated
Solidly descriptor. The Curve position-key tail is the bare pool label
(``3pool`` / ``2pool`` / ``crvusd_usdc`` / ``weth_cbeth`` — no ``0x``, no
``/``), so every candidate is rejected and ``_resolve_lp_pool_address``
returns ``None`` → ``handle_lp`` returns ``None`` → full event drop. Two
compounding upstream causes feed this (also outside the test layer; TESTS-ONLY
task — encoded as a documented gap, NOT fixed):

  * ``CurveReceiptParser`` stamps no ``pool_address`` on ``LPCloseData`` and
    implements no ``extract_lp_open_data``, so the receipt-extraction priority
    (handler step 1) yields no canonical pool address either.
  * The Curve ``LPOpenIntent`` carries no token symbols (``pool="3pool"`` has
    no ``/``; no ``from_token`` / ``token0``), so even if the pool address
    resolved, ``token0`` / ``token1`` and the whole USD money surface would
    collapse to empty / ``None`` (Empty ≠ Zero).

These tests pin that honest **zero-row** contract via
``assert_accounting_persisted_or_gap`` (``pytest.xfail`` on the documented
full-drop signature). The moment a fix lands — Curve pool-address resolution
in the handler + ``extract_lp_open_data`` — the helper stops xfail-ing, the
typed row appears, and the post-row null-contract assertions below run and
force the test to be tightened. Reported under ``VIB-4968``.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from almanak.framework.execution.orchestrator import ExecutionContext
from almanak.framework.execution.result_enricher import enrich_result
from tests.intents.conftest import _persist_and_drain_for_intent_test

# Single xfail reason string shared by every Curve LP Layer-5 assertion so the
# documented full-drop gap reads identically across all five chains. Carries a
# ticket ref (VIB-4968) + date per the intent-test xfail-hygiene rule.
CURVE_LP_EVENT_DROP_GAP = (
    "VIB-4968: Curve LP writes zero typed accounting_events — "
    "lp_handler._resolve_lp_pool_address rejects the bare Curve pool label "
    "(no 0x / slash descriptor) so handle_lp drops the event (as of 2026-06-02)."
)

# Money-surface payload fields that the Curve LP handler would also leave
# unmeasured (None) IF a row were written — the pool label carries no token
# symbols and the parser has no open-data extractor. Checked only on the
# post-fix path (when a row actually appears). Empty ≠ Zero.
_CURVE_UNMEASURED_MONEY_FIELDS = (
    "amount0",
    "amount1",
    "fees0_collected",
    "fees1_collected",
    "cost_basis_usd",
    "realized_pnl_usd",
)


def enrich_for_accounting(
    execution_result: Any,
    intent: Any,
    wallet: str,
    *,
    chain: str,
    bundle_metadata: dict | None = None,
) -> Any:
    """Run the production result enricher in paper mode (live_mode=False).

    Mirrors what the runner does for a non-live cycle so the Layer-5 persist
    path sees the same enriched ``ExecutionResult`` a real deployment would.
    """
    return enrich_result(
        execution_result,
        intent,
        ExecutionContext(
            deployment_id="layer5-curve-lp",
            chain=chain,
            wallet_address=wallet,
            protocol="curve",
        ),
        live_mode=False,
        bundle_metadata=bundle_metadata,
    )


def payload_of(row: dict) -> dict:
    return json.loads(row["payload_json"])


def assert_identity(row: dict, *, event_type: str, wallet: str) -> None:
    assert row["deployment_id"] == "layer5-intent-test"
    assert row["cycle_id"] == "layer5-cycle"
    assert row["execution_mode"] == "paper"
    assert row["event_type"] == event_type
    assert row["tx_hash"], "accounting row must link to an on-chain tx_hash"
    assert row["ledger_entry_id"], "accounting row must link to transaction_ledger"
    assert row["wallet_address"].lower() == wallet.lower()


def assert_no_lot_id(row: dict, payload: dict) -> None:
    assert "lot_id" not in row
    assert "lot_id" not in payload


def assert_curve_lp_null_contract(payload: dict, *, event_type: str) -> None:
    """Assert the Curve (fungible-LP) directional null-contract.

    Curve fungible LP has no NFT / tick model. The handler must persist
    ``None`` for every concentrated-liquidity field rather than fabricate a
    zero or a synthetic bracket (Empty ≠ Zero ≠ None, VIB-4591 decision #5,
    mirrored for Curve under VIB-4600). ``pool_address`` is the bare Curve
    pool label (no ``0x``, no Solidly ``/`` descriptor), and ``token0`` /
    ``token1`` are empty (the label carries no symbols).
    """
    assert payload["event_type"] == event_type
    assert "lot_id" not in payload
    assert payload["position_hash"] is None, "Curve fungible LP must not fabricate a V4 position_hash"
    assert payload.get("position_id") is None, "Curve fungible LP has no per-position id; position_id must be None"
    # The null-contract holds for BOTH LP_OPEN and LP_CLOSE: Curve must never
    # fabricate a tick bracket. LP_CLOSE's payload schema doesn't carry these
    # keys at all (fees/pnl/il instead), so ``.get`` absent → None still
    # satisfies "not fabricated" and future-proofs against a regression that
    # starts injecting them on close rows.
    for field in ("tick_lower", "tick_upper", "liquidity", "current_tick", "in_range"):
        assert payload.get(field) is None, (
            f"Curve {event_type} must not fabricate concentrated-liquidity "
            f"field {field!r}; Curve has no tick model (got {payload.get(field)!r})"
        )
    pool_address = payload["pool_address"]
    assert isinstance(pool_address, str) and pool_address, "Curve must persist a non-empty pool identifier"
    assert not pool_address.startswith("0x"), (
        f"Curve surfaces the bare pool label as pool_address (e.g. '3pool'), not a 0x address; got {pool_address!r}"
    )
    # Curve pool labels carry no token symbols, so the handler cannot derive
    # token0/token1 from the pool string — they remain empty (Empty ≠ Zero).
    assert payload["token0"] == "", (
        f"Curve pool label carries no token symbols; token0 must be empty, got {payload['token0']!r}"
    )
    assert payload["token1"] == "", (
        f"Curve pool label carries no token symbols; token1 must be empty, got {payload['token1']!r}"
    )


def assert_curve_money_surface_unmeasured(payload: dict) -> None:
    """Pin the documented Curve LP money-surface gap (VIB-4968).

    Because the Curve pool label carries no token symbols and the receipt
    parser has no ``extract_lp_open_data``, the handler cannot resolve token
    decimals or USD prices — so every economic field is honestly ``None``
    (unmeasured), NEVER a fabricated zero (Empty ≠ Zero, see module docstring).

    This is the contract the round-trip CURRENTLY produces; it is the inverse
    of a "PnL computed" assertion. When the upstream gap is fixed these
    ``is None`` checks will start failing and force the test to assert the real
    measured values — exactly the regression-surfacing behaviour the
    intent-test rules want from a documented gap.
    """
    for field in _CURVE_UNMEASURED_MONEY_FIELDS:
        assert payload.get(field) is None, (
            f"Curve LP money field {field!r} is currently unmeasured "
            f"(VIB-4968: pool label carries no token symbols + no "
            f"extract_lp_open_data). Expected None (Empty ≠ Zero), got "
            f"{payload.get(field)!r}. If this now carries a value the upstream "
            f"gap is FIXED — tighten this test to assert the real amount."
        )


def is_gap_row(row: dict | None) -> bool:
    """True iff ``row`` is the non-terminal Curve LP gap sentinel.

    A gap sentinel (``{"is_gap": True}``) is what
    :func:`assert_curve_lp_layer5` returns when the documented VIB-4968
    full-event-drop gap is observed, INSTEAD of ``pytest.xfail``-ing
    mid-lifecycle. ``None`` (no leg run) is not a gap.
    """
    return bool(row and row.get("is_gap"))


async def _persist_curve_lp_or_gap(
    harness: Any,
    *,
    intent: Any,
    result: Any,
    chain: str,
    wallet_address: str,
    event_type: str,
    price_oracle: dict | None,
    eth_call_reader: Any,
    resolved_pool: str | None = None,
) -> dict:
    """Persist one Curve LP result through Layer 5; return a row or a gap sentinel.

    This is the **non-terminal** analogue of the shared
    ``conftest.assert_accounting_persisted_or_gap``: it drives the SAME
    ``_persist_and_drain_for_intent_test`` path and applies the IDENTICAL hard
    contract (exactly-one row + idempotent re-drain) when a typed row is
    genuinely written — but on the documented VIB-4968 zero-row drop it returns
    a sentinel ``{"is_gap": True}`` row instead of calling ``pytest.xfail``
    (which raises and would abort a lifecycle test before its LP_CLOSE leg).
    The caller is responsible for finalising the gap via
    :func:`finalize_curve_lp_layer5` once every leg has run on-chain.

    A row that exists for this ledger under a DIFFERENT ``event_type`` is a real
    regression (mis-typed event), NOT the documented gap, and fails loudly here
    — mirroring the conftest helper (CodeRabbit PR #2369).
    """
    persisted = await _persist_and_drain_for_intent_test(
        state_manager=harness.store,
        accounting_processor=harness.processor,
        deployment_id="layer5-intent-test",
        cycle_id="layer5-cycle",
        execution_mode="paper",
        chain=chain,
        wallet_address=wallet_address,
        intent=intent,
        result=result,
        success=bool(getattr(result, "success", False)),
        price_oracle=price_oracle,
        eth_call_reader=eth_call_reader,
        resolved_pool=resolved_pool,
    )
    assert persisted.outbox_id is not None, "Layer-5 helper must write accounting_outbox"
    assert persisted.drained is True, "AccountingProcessor.drain_one must process the row"

    rows = await harness.store.get_accounting_events("layer5-intent-test", limit=50)
    ledger_rows = [row for row in rows if row.get("ledger_entry_id") == persisted.ledger_entry_id]
    matching = [row for row in ledger_rows if row.get("event_type") == event_type]
    if not matching:
        # Only the TRUE zero-row drop is the documented production gap. A row
        # under a different event_type is a mis-typed-event regression and must
        # NOT be masked as the known gap — fail loudly (CodeRabbit PR #2369).
        assert ledger_rows == [], (
            f"expected zero accounting_events rows for the documented Curve LP drop gap, "
            f"but ledger {persisted.ledger_entry_id} has rows under "
            f"{[row.get('event_type') for row in ledger_rows]!r}"
        )
        # Documented production gap (VIB-4968): on-chain action succeeded but the
        # typed event was dropped. Return a NON-terminal sentinel so a lifecycle
        # test can still run its LP_CLOSE leg; the test xfails ONCE at the end
        # via finalize_curve_lp_layer5. Reactivates the moment the gap is fixed
        # (then ``matching`` is non-empty and the full hard asserts below run).
        return {"is_gap": True}

    # Same hard contract as ``conftest.assert_accounting_persisted_or_gap`` —
    # applied to the SAME single persisted entry (no second persist).
    assert len(matching) == 1, (
        f"expected exactly one {event_type} accounting_event for ledger "
        f"{persisted.ledger_entry_id}, got {len(matching)}"
    )
    redrained = await harness.processor.drain_one(persisted.ledger_entry_id)
    assert redrained is True
    rows_after = await harness.store.get_accounting_events("layer5-intent-test", event_type=event_type, limit=20)
    matching_after = [row for row in rows_after if row.get("ledger_entry_id") == persisted.ledger_entry_id]
    assert len(matching_after) == 1, "drain_one must be idempotent for Layer-5 rows"
    return matching_after[0]


async def assert_curve_lp_layer5(
    harness: Any,
    *,
    intent: Any,
    result: Any,
    chain: str,
    wallet_address: str,
    event_type: str,
    price_oracle: dict | None,
    eth_call_reader: Any,
    expected_pool_label: str | None = None,
    prior_open_row: dict | None = None,
    resolved_pool: str | None = None,
) -> dict:
    """Drive the Curve LP Layer-5 contract through the real accounting path.

    Persists ``result`` via :func:`_persist_curve_lp_or_gap`. Today the
    documented full-event-drop gap (``CURVE_LP_EVENT_DROP_GAP``, VIB-4968)
    causes ``lp_handler`` to write no row, so this returns a **non-terminal**
    gap sentinel (``{"is_gap": True}``) — it does NOT ``pytest.xfail`` here.
    That lets a lifecycle test run its LP_CLOSE leg to completion under the
    known gap; the caller raises the xfail ONCE at the very end via
    :func:`finalize_curve_lp_layer5`.

    When the upstream gap is fixed a typed row appears and the full post-row
    contract below runs automatically (gated on ``not row["is_gap"]`` so the
    gap path never weakens it):

      * identity (deployment/cycle/paper-mode/tx_hash/ledger linkage);
      * fungible-LP directional null-contract (no NFT/tick fabrication,
        bare pool label, empty token symbols);
      * money surface honestly unmeasured (None — Empty ≠ Zero);
      * ``expected_pool_label`` match (LP_OPEN) and/or prior-open
        ``position_key`` linkage (LP_CLOSE) when supplied.

    Returns either the persisted row (post-fix) or the gap sentinel (today).
    Pass the returned rows of every leg to :func:`finalize_curve_lp_layer5`.
    """
    row = await _persist_curve_lp_or_gap(
        harness,
        intent=intent,
        result=result,
        chain=chain,
        wallet_address=wallet_address,
        event_type=event_type,
        price_oracle=price_oracle,
        eth_call_reader=eth_call_reader,
        resolved_pool=resolved_pool,
    )
    if is_gap_row(row):
        # Non-terminal gap: the close leg must still run. The real-accounting
        # assertions below are gated off so the gap path never weakens them —
        # they reactivate verbatim once a real row appears post-VIB-4968.
        return row
    assert_identity(row, event_type=event_type, wallet=wallet_address)
    payload = payload_of(row)
    assert payload["position_key"] == row["position_key"]
    assert_curve_lp_null_contract(payload, event_type=event_type)
    assert_curve_money_surface_unmeasured(payload)
    if expected_pool_label is not None:
        assert payload["pool_address"] == expected_pool_label.lower()
    if prior_open_row is not None:
        # Linkage only checks against a real prior-open row; if the open leg was
        # a gap sentinel there is nothing to link to (and this close leg can
        # only be a real row post-fix once open is also a real row).
        assert not is_gap_row(prior_open_row), (
            "LP_CLOSE produced a real accounting row while LP_OPEN was a gap — "
            "open/close fix must land together (VIB-4968)"
        )
        assert_no_lot_id(row, payload)
        assert payload["position_key"] == payload_of(prior_open_row)["position_key"]
    return row


def finalize_curve_lp_layer5(*rows: dict | None) -> None:
    """Raise the documented VIB-4968 xfail ONCE if any leg hit the gap.

    Call at the very END of a Curve LP Layer-5 test, after EVERY leg's on-chain
    execution + receipt-parse + balance-delta assertions AND its
    :func:`assert_curve_lp_layer5` call have run. Because the per-leg helper now
    returns a non-terminal gap sentinel instead of ``pytest.xfail``-ing
    mid-flow, the full lifecycle (LP_OPEN then LP_CLOSE) always completes; this
    is what marks the test ``xfail`` under the gap so it auto-reactivates as a
    hard assertion the moment Curve LP accounting is fixed.

    The reason string carries the VIB-4968 ticket ref + as-of date per the
    intent-test xfail-hygiene contract.
    """
    if any(is_gap_row(row) for row in rows):
        pytest.xfail(CURVE_LP_EVENT_DROP_GAP)
