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
0x pool address, and OPEN↔CLOSE ``position_key`` linkage. Money surface
(VIB-5429): the 2-token per-leg fields stay honestly ``None`` on a bare-label
event (a proportional N-coin close has no token0/token1 direction — Empty ≠ Zero,
not a fabricated zero), while the USD aggregates (cost_basis / realized_pnl) are
MEASURED for a recognized USD-stable pool via the CURVE_POOLS coin resolver and
stay ``None`` (fail-closed) for a non-stable pool. The event carries its true coin
identity in ``coin_symbols`` (not the empty 2-token labels).
"""

from __future__ import annotations

import json
import re
from decimal import Decimal
from typing import Any

from almanak.framework.execution.orchestrator import ExecutionContext
from almanak.framework.execution.result_enricher import enrich_result
from tests.intents.conftest import assert_accounting_persisted

# Canonical 0x EVM address shape (20-byte, lowercased) the Curve receipt
# parser now stamps on the LP event and the handler books as pool_address.
_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")

# VIB-5429 — the Curve LP money surface splits into two families on a bare-label
# (no token0/token1) event, because the close-leg coin resolver (CURVE_POOLS
# registry) now measures the USD *aggregates* from the pool's coins even when the
# 2-token labels are empty, while the 2-token *per-leg* breakdown genuinely does
# not apply to a proportional N-coin pool:
#
#   * 2-TOKEN PER-LEG fields stay None on a bare-label event — a proportional
#     remove_liquidity has no token0/token1 direction, so there is no per-leg
#     amount to scale (the per-coin detail lives on extracted_data, not the
#     2-token payload slots). Empty ≠ Zero — None, never a fabricated zero.
_CURVE_2TOKEN_UNMEASURED_FIELDS = (
    "amount0",
    "amount1",
    "fees0_collected",
    "fees1_collected",
)
#   * PEG-GATED USD aggregates (``cost_basis_usd`` / ``realized_pnl_usd``) are
#     MEASURED for a recognized USD-stable pool ($1-peg numeraire) and None for a
#     non-stable pool (cryptoswap / tricrypto: unpriceable legs → fail-closed).
#     ``realized_pnl_usd`` is realized only on a CLOSE (vs the prior OPEN basis);
#     an OPEN has no realized PnL (None) regardless of pool type. Each test
#     declares its pool's expectation via ``expect_usd_aggregates`` (an
#     INDEPENDENT oracle — the test knows it set up a stable 3pool vs a non-stable
#     tricrypto — so a misclassification BUG in the production stable predicate
#     fails this test instead of moving in lockstep with it).
_CURVE_PEG_GATED_USD_FIELDS = (
    "cost_basis_usd",
    "realized_pnl_usd",
)
# ``fees_total_usd`` is NOT peg-gated: a Curve close's imbalance fee is $0 for a
# balanced removal that emits a fees array (NG pools), and None when the pool's
# RemoveLiquidity carries no fees array (old-style crypto; some NG pools — e.g.
# Optimism crvUSD/USDC — also surface None). Fee-array emission is pool-
# implementation-dependent, NOT a function of the $1-peg or of stable-ness (a
# STABLE pool can legitimately surface fees_total_usd=None). So it is asserted
# for VALIDITY (non-negative or None, never fabricated) plus a non-stable
# fail-closed bound (never a PEGGED positive fee).


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


def assert_curve_money_surface(payload: dict, *, event_type: str, expect_usd_aggregates: bool) -> None:
    """Pin the Curve LP money-surface contract per intent shape (Empty ≠ Zero).

    ``expect_usd_aggregates`` is the TEST'S declaration of its pool type — ``True``
    for a recognized USD-stable pool (3pool, 2pool, crvUSD/USDC, frxUSD/USDT: the
    $1-peg numeraire applies), ``False`` for a non-stable pool (tricrypto,
    weth/cbETH). It is an INDEPENDENT oracle (the test knows what pool it set up),
    NOT the production stable predicate — so a misclassification BUG in
    ``_is_usd_stable_pool`` fails this test rather than moving in lockstep with it.

    * **Asset-set** (``token0``/``token1`` populated — a directional deposit, or a
      slash-pool intent): the handler resolves decimals, so the principal legs
      (``amount0``/``amount1``) are MEASURED — assert present. (USD aggregates are
      not re-checked here; that's the bare-label close's job.)
    * **Bare label** (``token0``/``token1`` empty — a proportional
      ``remove_liquidity`` close returns ALL N coins with no 2-token direction):
        - the 2-TOKEN PER-LEG fields stay ``None`` (no per-leg direction; Empty ≠
          Zero — never a fabricated zero);
        - the PEG-GATED USD aggregates (``cost_basis_usd`` / ``realized_pnl_usd``)
          are MEASURED when ``expect_usd_aggregates`` and ``None`` otherwise
          (non-stable fail-closed). ``realized_pnl_usd`` is realized only on a
          CLOSE; an OPEN carries ``None`` regardless;
        - ``fees_total_usd`` is asserted for VALIDITY (non-negative or ``None``,
          never fabricated) + a non-stable fail-closed bound (never a PEGGED
          positive fee) — see the constant note above.
      The event still carries its identity in ``coin_symbols`` (NOT the empty
      2-token labels), so a measured USD basis is never symbol-less.
    """
    if _curve_tokens_resolved(payload):
        # Asset-set: both legs of a directional deposit / proportional close are
        # measured token amounts.
        for field in ("amount0", "amount1"):
            assert payload.get(field) is not None, (
                f"Curve asset-set {event_type} resolved token symbols, so {field!r} must be a measured amount, got None"
            )
        return

    # Bare label. The 2-token per-leg breakdown is honestly unmeasured.
    for field in _CURVE_2TOKEN_UNMEASURED_FIELDS:
        assert payload.get(field) is None, (
            f"Curve bare-label LP 2-token per-leg field {field!r} has no token "
            f"direction on a proportional N-coin event. Expected None "
            f"(Empty ≠ Zero), got {payload.get(field)!r}."
        )

    coin_symbols = payload.get("coin_symbols")
    is_disposal = event_type in ("LP_CLOSE", "LP_COLLECT_FEES")
    if expect_usd_aggregates:
        # USD-stable pool: the registry coin resolver measures the USD aggregates
        # (cost_basis at the $1-peg) even with empty 2-token labels.
        assert payload.get("cost_basis_usd") is not None, (
            f"Curve USD-stable {event_type} ({coin_symbols}) must measure "
            f"cost_basis_usd via registry coin resolution, got None"
        )
        if is_disposal:
            assert payload.get("realized_pnl_usd") is not None, (
                f"Curve USD-stable {event_type} must realize PnL against the prior "
                f"OPEN basis, got realized_pnl_usd=None"
            )
        else:
            assert payload.get("realized_pnl_usd") is None, (
                f"Curve {event_type} is not a disposal — realized_pnl_usd must be "
                f"None, got {payload.get('realized_pnl_usd')!r}"
            )
    else:
        # Non-stable / unknown pool: fail-closed — the peg-gated aggregates stay
        # None (never a fabricated $1-peg). Preserves the cryptoswap/tricrypto guard.
        for field in _CURVE_PEG_GATED_USD_FIELDS:
            assert payload.get(field) is None, (
                f"Curve non-stable {event_type} ({coin_symbols}) must fail closed: "
                f"{field!r} expected None (no $1-peg for non-stable coins), got "
                f"{payload.get(field)!r}."
            )

    # fees_total_usd (CodeRabbit #4): VALIDITY — a fee USD is non-negative or
    # unmeasured (None), never fabricated/negative — on ANY pool.
    fees_total = payload.get("fees_total_usd")
    assert fees_total is None or Decimal(str(fees_total)) >= 0, (
        f"Curve {event_type} fees_total_usd must be None or a non-negative measured "
        f"value, got {fees_total!r}"
    )
    # Non-stable fail-closed: a $1-peg must never value a non-stable coin's fee, so
    # a non-stable pool may surface fees_total_usd None or a measured ZERO (zero
    # needs no price), but NEVER a pegged positive amount.
    if not expect_usd_aggregates:
        assert fees_total is None or Decimal(str(fees_total)) == 0, (
            f"Curve non-stable {event_type} must not peg a positive fee: "
            f"fees_total_usd expected None or 0, got {fees_total!r}"
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
    expect_usd_aggregates: bool,
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
      * money surface (VIB-5429): 2-token per-leg fields unmeasured (None —
        Empty ≠ Zero), USD aggregates measured for USD-stable pools via registry
        coin resolution and None (fail-closed) for non-stable pools;
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
    assert_curve_money_surface(payload, event_type=event_type, expect_usd_aggregates=expect_usd_aggregates)
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
