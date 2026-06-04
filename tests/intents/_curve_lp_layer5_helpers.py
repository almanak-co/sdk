"""Shared Layer-5 accounting helpers for Curve LP intent tests (VIB-4600 / VIB-4968).

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
  * ``pool_address`` is the **canonical 0x Curve pool contract address**
    stamped on chain by the receipt parser (VIB-4968) from the
    AddLiquidity / RemoveLiquidity event emitter — NOT a bare label and NOT
    a slash-separated Solidly descriptor.

VIB-4968 — event drop CLOSED (as of 2026-06-04)
-----------------------------------------------------------
Before VIB-4968 the Curve LP category handler wrote **zero** typed
``accounting_events`` for LP_OPEN / LP_CLOSE: ``_resolve_lp_pool_address``
(VIB-4471) accepts only a ``0x`` address / V4 pool-id / Solidly descriptor,
and the Curve position-key tail is the bare pool label (``3pool`` /
``crvusd_usdc`` / …), so every candidate was rejected and ``handle_lp``
returned ``None`` → full event drop.

The fix makes ``CurveReceiptParser`` stamp the canonical 0x pool address
(the on-chain Add/RemoveLiquidity event emitter) on BOTH ``LPOpenData``
(new ``extract_lp_open_data``) and ``LPCloseData``. The handler's receipt-
extraction priority (``_resolve_lp_pool_address`` step 1) then accepts that
0x address and books the event. The shared ``_resolve_lp_pool_address`` /
``_clean_pool_address_candidate`` seam is UNCHANGED.

These helpers now assert the REAL persisted fungible-LP contract: exactly-one
typed row + idempotent re-drain, the directional null-contract, a canonical
0x pool address, and OPEN↔CLOSE ``position_key`` linkage. The money surface
(amounts / fees / USD) stays honestly ``None`` (unmeasured): the Curve pool
LABEL carries no token symbols so the handler cannot resolve token decimals —
Empty ≠ Zero, not a fabricated zero.
"""

from __future__ import annotations

import json
import re
from typing import Any

from almanak.framework.execution.orchestrator import ExecutionContext
from almanak.framework.execution.result_enricher import enrich_result
from tests.intents.conftest import assert_accounting_persisted

# Canonical 0x EVM address shape (20-byte, lowercased) the Curve receipt
# parser now stamps on the LP event and the handler books as pool_address.
_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")

# Money-surface payload fields that the Curve LP handler leaves unmeasured
# (None) because the pool LABEL carries no token symbols (so token decimals
# cannot be resolved). Empty ≠ Zero — these are honestly None, never a
# fabricated zero. Pinned so a future regression that starts fabricating a
# zero (or a token-symbol resolution improvement) forces the test to be
# tightened to assert the real measured value.
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


def _curve_tokens_resolved(payload: dict) -> bool:
    """True iff the handler derived BOTH token symbols for this Curve event.

    Curve LP has two intent shapes that diverge on token resolution:

    * **Bare label** (``pool="3pool"``): the pool string carries no token
      symbols and the position-key tail has no ``/``, so ``token0`` /
      ``token1`` stay empty — the money surface is honestly unmeasured.
    * **Asset-set** (``pool="USDT/USDC/DAI"``): the ledger derives
      ``token_in`` / ``token_out`` from the slash-separated pool string, so
      the handler CAN resolve decimals — the money surface is measured.

    Either shape is correct (Empty ≠ Zero ≠ None); the assertions below branch
    on which one this row is rather than forcing one universally.
    """
    return bool(payload.get("token0")) and bool(payload.get("token1"))


def assert_curve_lp_null_contract(payload: dict, *, event_type: str) -> None:
    """Assert the Curve (fungible-LP) directional null-contract.

    Curve fungible LP has no NFT / tick model. The handler must persist
    ``None`` for every concentrated-liquidity field rather than fabricate a
    zero or a synthetic bracket (Empty ≠ Zero ≠ None, VIB-4591 decision #5,
    mirrored for Curve under VIB-4600). Post-VIB-4968 ``pool_address`` is the
    canonical 0x Curve pool contract address (the on-chain Add/RemoveLiquidity
    emitter). ``token0`` / ``token1`` are empty for a bare pool label and the
    derived symbols for an asset-set intent (see :func:`_curve_tokens_resolved`).
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
    # VIB-4968 — pool_address is now the canonical on-chain 0x Curve pool
    # contract address (the Add/RemoveLiquidity event emitter), stamped by the
    # receipt parser. This is the chain-data identity the accounting handler
    # books; without it the event was dropped entirely.
    pool_address = payload["pool_address"]
    assert isinstance(pool_address, str) and _ADDRESS_RE.match(pool_address), (
        f"Curve LP pool_address must be a canonical lowercased 0x address "
        f"(VIB-4968: stamped from the on-chain pool contract), got {pool_address!r}"
    )
    # token0/token1: empty for a bare pool label (no symbols to derive), or the
    # ledger-derived symbols for an asset-set intent. Both are valid; assert the
    # pair is internally consistent (both empty or both populated) so a regression
    # that resolves only one leg is caught.
    if _curve_tokens_resolved(payload):
        assert isinstance(payload["token0"], str) and payload["token0"]
        assert isinstance(payload["token1"], str) and payload["token1"]
    else:
        assert payload["token0"] == "", (
            f"Curve bare-label pool carries no token symbols; token0 must be empty, got {payload['token0']!r}"
        )
        assert payload["token1"] == "", (
            f"Curve bare-label pool carries no token symbols; token1 must be empty, got {payload['token1']!r}"
        )


def assert_curve_money_surface(payload: dict, *, event_type: str) -> None:
    """Pin the Curve LP money-surface contract per intent shape (Empty ≠ Zero).

    * **Bare label** (no token symbols): the handler cannot resolve token
      decimals, so every economic field is honestly ``None`` (unmeasured),
      NEVER a fabricated zero. Guards against a regression that starts
      fabricating a zero AND against a future label→symbol resolution silently
      landing untested.
    * **Asset-set** (symbols derived): the handler resolves decimals, so an
      LP_OPEN's principal legs (``amount0`` / ``amount1``) are MEASURED — assert
      they are present (non-None Decimals serialised as strings) rather than
      dropped. This is the positive "money surface now works" assertion.
    """
    if not _curve_tokens_resolved(payload):
        for field in _CURVE_UNMEASURED_MONEY_FIELDS:
            assert payload.get(field) is None, (
                f"Curve bare-label LP money field {field!r} is unmeasured (no "
                f"token symbols → no decimals). Expected None (Empty ≠ Zero), "
                f"got {payload.get(field)!r}."
            )
        return
    # Asset-set: token symbols resolved ⇒ principal amounts are measured on
    # LP_OPEN. (LP_CLOSE principal lives on amount0/amount1 too; both legs of a
    # proportional Curve close return tokens, so they are non-None as well.)
    for field in ("amount0", "amount1"):
        assert payload.get(field) is not None, (
            f"Curve asset-set {event_type} resolved token symbols, so {field!r} "
            f"must be a measured amount, got None"
        )


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
    expected_pool_address: str | None = None,
    prior_open_row: dict | None = None,
    resolved_pool: str | None = None,
) -> dict:
    """Drive the Curve LP Layer-5 contract through the real accounting path.

    Persists ``result`` via the shared ``assert_accounting_persisted`` helper,
    which applies the exactly-one + idempotent-re-drain hard contract and
    returns the single typed ``accounting_events`` row. VIB-4968 closed the
    event-drop gap, so a typed row is now ALWAYS written and the full post-row
    contract runs unconditionally:

      * identity (deployment/cycle/paper-mode/tx_hash/ledger linkage);
      * fungible-LP directional null-contract (no NFT/tick fabrication,
        canonical 0x pool address, empty token symbols);
      * money surface honestly unmeasured (None — Empty ≠ Zero);
      * ``expected_pool_address`` match (when supplied) and/or prior-open
        ``position_key`` linkage (LP_CLOSE).

    ``resolved_pool`` (VIB-3946) is the compiler-resolved canonical pool label
    (``action_bundle.metadata["pool_name"]``); threaded into the position-key
    derivation so a Curve asset-set intent keys off the canonical label.

    Returns the persisted row.
    """
    row = await assert_accounting_persisted(
        harness,
        intent=intent,
        result=result,
        chain=chain,
        wallet_address=wallet_address,
        expected_event_type=event_type,
        price_oracle=price_oracle,
        eth_call_reader=eth_call_reader,
        resolved_pool=resolved_pool,
    )
    assert_identity(row, event_type=event_type, wallet=wallet_address)
    payload = payload_of(row)
    assert payload["position_key"] == row["position_key"]
    assert_curve_lp_null_contract(payload, event_type=event_type)
    assert_curve_money_surface(payload, event_type=event_type)
    if expected_pool_address is not None:
        assert payload["pool_address"] == expected_pool_address.lower(), (
            f"Curve LP pool_address must be the canonical pool contract "
            f"{expected_pool_address.lower()}, got {payload['pool_address']!r}"
        )
    if prior_open_row is not None:
        # OPEN↔CLOSE linkage: both legs must share the SAME position_key (the
        # fungible-LP pool-level key, tail = canonical Curve label) so a CLOSE
        # attributes to its OPEN.
        assert_no_lot_id(row, payload)
        assert payload["position_key"] == payload_of(prior_open_row)["position_key"], (
            "LP_CLOSE position_key must match its prior LP_OPEN (OPEN↔CLOSE linkage)"
        )
        # Both legs book the SAME canonical pool contract address.
        assert payload["pool_address"] == payload_of(prior_open_row)["pool_address"], (
            "LP_CLOSE pool_address must match its prior LP_OPEN's canonical pool address"
        )
    return row
