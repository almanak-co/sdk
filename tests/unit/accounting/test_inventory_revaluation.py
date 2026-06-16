"""Unit tests for the ambient inventory revaluation lane (G6 component term).

Module under test: ``almanak.framework.accounting.inventory_revaluation``.
Blueprint: ``docs/internal/blueprints/27-accounting.md`` §11.5.

Covers (per the lane's contract):
  * pure ambient revaluation (qty × Δmark, no lots)
  * the double-count guard vs an open swap lot
  * a partially-traded token (70 idle of 100, 30 in an open lot)
  * a swap-touched-then-disposed token excluded (held=0 ⇒ no contribution)
  * native gas via the GENERAL rule (assert NO native branch in the lane)
  * Empty≠Zero (None on missing price, 0 on a measured-zero price)
  * t0-only / t1-only edges
  * symbol/address identity unification (case-insensitive)
  * open-lot mid-position MTM (Q1b)
  * a guard against hardcoded token-symbol literals in the lane source
"""

from __future__ import annotations

import inspect
import json
from decimal import Decimal

from almanak.framework.accounting import inventory_revaluation as inv_mod
from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.inventory_revaluation import (
    compute_inventory_revaluation,
)

_DEP = "deployment:test"


def _snapshot(balances: list[dict], *, deployment_id: str = _DEP) -> dict:
    """Build a portfolio_snapshots-shaped row dict with persisted JSON columns.

    ``balances`` items are ``{symbol, balance, price_usd}`` (the snapshot
    writer's ``wallet_balances_json`` shape; ``value_usd``/``address`` are
    optional and not read by the lane).
    """
    return {
        "deployment_id": deployment_id,
        "wallet_balances_json": json.dumps(
            [
                {
                    "symbol": b["symbol"],
                    "balance": str(b["balance"]) if b.get("balance") is not None else None,
                    "value_usd": b.get("value_usd"),
                    "address": b.get("address"),
                    "price_usd": (None if b.get("price_usd") is None else str(b["price_usd"])),
                }
                for b in balances
            ]
        ),
        "token_prices_json": "{}",
    }


def _swap_event(
    *,
    token_in: str,
    amount_in: str,
    token_out: str,
    amount_out: str,
    amount_out_usd: str | None,
    deployment_id: str = _DEP,
) -> dict:
    """An accounting_events row for a SWAP, in the persisted ``payload_json`` shape.

    Matches the SQLite ``accounting_events`` row shape the Accountant Test reads:
    ``payload_json`` is a JSON string, and the swap lot is keyed under
    ``swap:{chain}:{wallet_address}`` (the live write path's key), which
    ``FIFOBasisStore`` reconstructs from the top-level ``chain`` /
    ``wallet_address`` columns.
    """
    return {
        "deployment_id": deployment_id,
        "event_type": "SWAP",
        "position_key": "",  # SWAP rows persist an empty position_key (VIB-5010)
        "chain": "arbitrum",
        "wallet_address": "0xwallet",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "payload_json": json.dumps(
            {
                "event_type": "SWAP",
                "token_in": token_in,
                "amount_in": amount_in,
                "token_out": token_out,
                "amount_out": amount_out,
                "amount_out_usd": amount_out_usd,
            }
        ),
    }


# ── Pure ambient revaluation ────────────────────────────────────────────────


def test_pure_ambient_revaluation_no_lots() -> None:
    """qty_idle × Δmark for an untraded token, no accounting events at all."""
    si = _snapshot([{"symbol": "WETH", "balance": "2", "price_usd": "2000"}])
    sf = _snapshot([{"symbol": "WETH", "balance": "2", "price_usd": "2500"}])
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[], deployment_id=_DEP
    )
    # 2 WETH × ($2500 − $2000) = $1000.
    assert out.total_usd == Decimal("1000")
    assert out.confidence == "measured"
    assert out.per_token["WETH"] == "1000"


def test_ambient_term_collapses_to_zero_with_no_inventory() -> None:
    """Empty wallets at both endpoints ⇒ term is exactly zero (not None)."""
    out = compute_inventory_revaluation(
        snapshot_initial=_snapshot([]),
        snapshot_final=_snapshot([]),
        accounting_events=[],
        deployment_id=_DEP,
    )
    assert out.total_usd == Decimal("0")
    assert out.confidence == "measured"


# ── Double-count guard vs an open swap lot ──────────────────────────────────


def test_double_count_guard_open_lot_plus_idle_sums_to_full_balance() -> None:
    """A token fully backed by one open swap lot: ambient + open-lot == full × marks.

    Swap 1000 USDC → 1 WETH @ $1000 basis; hold the whole 1 WETH idle. The
    open-lot term is ``1 × mark_final − 1000``; there is NO separate ambient
    contribution for the lot quantity (qty_idle == 0). Counted exactly once.
    """
    si = _snapshot(
        [
            {"symbol": "WETH", "balance": "1", "price_usd": "1000"},
            {"symbol": "USDC", "balance": "0", "price_usd": "1"},
        ]
    )
    sf = _snapshot(
        [
            {"symbol": "WETH", "balance": "1", "price_usd": "1500"},
            {"symbol": "USDC", "balance": "0", "price_usd": "1"},
        ]
    )
    ev = _swap_event(
        token_in="USDC",
        amount_in="1000",
        token_out="WETH",
        amount_out="1",
        amount_out_usd="1000",
    )
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[ev], deployment_id=_DEP
    )
    # Open-lot MTM only: 1 WETH × $1500 − $1000 basis = $500. No ambient term
    # for WETH (qty_idle = 1 − 1 = 0).
    assert out.total_usd == Decimal("500")
    assert out.per_token.get("<open_swap_lots_total>") == "500"
    assert "WETH" not in out.per_token  # ambient WETH did not double-count


def test_partially_traded_token_70_idle_of_100() -> None:
    """30 of 100 in an open lot, 70 idle: open-lot MTM on 30, ambient Δmark on 70."""
    # Swap to acquire 30 WETH @ $30000 basis ($1000/WETH), then hold 100 WETH
    # total in the wallet (70 came from elsewhere / a pre-existing balance).
    si = _snapshot([{"symbol": "WETH", "balance": "100", "price_usd": "1000"}])
    sf = _snapshot([{"symbol": "WETH", "balance": "100", "price_usd": "1100"}])
    ev = _swap_event(
        token_in="USDC",
        amount_in="30000",
        token_out="WETH",
        amount_out="30",
        amount_out_usd="30000",
    )
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[ev], deployment_id=_DEP
    )
    # open-lot: 30 × $1100 − $30000 = $3000.
    # ambient: (100 − 30) × ($1100 − $1000) = 70 × $100 = $7000.
    # total = $10000 — exactly 100 WETH revalued once ($100 each) plus the
    # lot's basis-vs-mark gain folded in.
    assert out.total_usd == Decimal("10000")
    assert out.per_token["WETH"] == "7000"
    assert out.per_token["WETH:open_lot"] == "3000"


# ── Swap-touched token excluded when fully disposed (held == 0) ──────────────


def test_swap_disposed_token_held_zero_contributes_nothing() -> None:
    """A token swapped away (final balance 0) is NOT ambient inventory.

    USDC fully spent into a swap and a WETH lot deployed elsewhere (final WETH
    balance 0): both held balances are 0, so the lane contributes 0 — the
    realized flow is in the SWAP component bucket, not here.
    """
    si = _snapshot([{"symbol": "USDC", "balance": "1000", "price_usd": "1"}])
    sf = _snapshot([{"symbol": "USDC", "balance": "0", "price_usd": "1"}])
    ev = _swap_event(
        token_in="USDC",
        amount_in="1000",
        token_out="WETH",
        amount_out="0.5",
        amount_out_usd="1000",
    )
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[ev], deployment_id=_DEP
    )
    # WETH lot remaining=0.5 but held=0 (deployed elsewhere) ⇒ lot_held=0 ⇒ no
    # open-lot term; USDC held=0 ⇒ no ambient term.
    assert out.total_usd == Decimal("0")


# ── Native gas via the GENERAL rule (no native branch) ──────────────────────


def test_native_gas_revalued_by_general_rule_no_special_case() -> None:
    """The native token's remainder is revalued by the same qty × Δmark rule."""
    si = _snapshot([{"symbol": "ETH", "balance": "3", "price_usd": "2000"}])
    sf = _snapshot([{"symbol": "ETH", "balance": "3", "price_usd": "2100"}])
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[], deployment_id=_DEP
    )
    # 3 ETH × ($2100 − $2000) = $300 — captured with no native-specific code.
    assert out.total_usd == Decimal("300")
    assert out.per_token["ETH"] == "300"


def test_lane_source_has_no_native_token_branch() -> None:
    """Static guard: the lane contains no native-gas-symbol special case."""
    src = inspect.getsource(inv_mod)
    # The lane must not branch on any native-gas symbol; the general rule covers
    # the native row. Tolerate these tokens only inside comments/docstrings, so
    # strip those before scanning for code-level identity checks.
    code_lines = []
    for line in src.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        code_lines.append(line)
    code = "\n".join(code_lines)
    for native in ("ETH", "WETH", "AVAX", "WAVAX", "BNB", "SOL", "MNT", "MONAD"):
        # A native-token equality/membership branch would look like
        # ``== "ETH"`` or ``in ("ETH", ...)`` / ``"ETH" ==`` in code.
        assert f'"{native}"' not in code, f"lane code references native symbol literal {native!r}"


# ── Empty ≠ Zero ────────────────────────────────────────────────────────────


def test_missing_price_makes_term_unmeasured_not_zero() -> None:
    """A held idle token with no mark at an endpoint ⇒ None + unmeasured_price."""
    si = _snapshot([{"symbol": "WETH", "balance": "2", "price_usd": None}])
    sf = _snapshot([{"symbol": "WETH", "balance": "2", "price_usd": "2500"}])
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[], deployment_id=_DEP
    )
    assert out.total_usd is None
    assert out.confidence == "unmeasured_price"


def test_measured_zero_price_contributes_zero() -> None:
    """A persisted price_usd == "0" is a measured zero (contributes 0, not None)."""
    si = _snapshot([{"symbol": "SCAM", "balance": "1000", "price_usd": "0"}])
    sf = _snapshot([{"symbol": "SCAM", "balance": "1000", "price_usd": "0"}])
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[], deployment_id=_DEP
    )
    assert out.total_usd == Decimal("0")
    assert out.confidence == "measured"


def test_missing_open_lot_basis_is_unmeasured_basis() -> None:
    """An open swap lot with no basis ⇒ None + unmeasured_basis (Empty≠Zero)."""
    si = _snapshot([{"symbol": "WETH", "balance": "1", "price_usd": "1000"}])
    sf = _snapshot([{"symbol": "WETH", "balance": "1", "price_usd": "1500"}])
    # amount_out_usd=None ⇒ the lot has cost_usd=None ⇒ cost_for_remaining=None.
    ev = _swap_event(
        token_in="USDC",
        amount_in="1000",
        token_out="WETH",
        amount_out="1",
        amount_out_usd=None,
    )
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[ev], deployment_id=_DEP
    )
    assert out.total_usd is None
    assert out.confidence == "unmeasured_basis"


# ── t0-only / t1-only edges ─────────────────────────────────────────────────


def test_t1_only_token_no_initial_mark_is_unmeasured() -> None:
    """A token that appears only at the final endpoint has no Δ ⇒ unmeasured.

    Without an initial mark there is no defensible Δmark for the held idle
    quantity, so the term is unmeasured rather than assuming "no change".
    """
    si = _snapshot([])
    sf = _snapshot([{"symbol": "ARB", "balance": "50", "price_usd": "1.2"}])
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[], deployment_id=_DEP
    )
    assert out.total_usd is None
    assert out.confidence == "unmeasured_price"


def test_t0_only_token_absent_at_final_contributes_nothing() -> None:
    """A token present only at t0 (gone by t1) is not held inventory ⇒ 0."""
    si = _snapshot([{"symbol": "ARB", "balance": "50", "price_usd": "1.2"}])
    sf = _snapshot([])
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[], deployment_id=_DEP
    )
    # final balance 0 ⇒ qty_idle 0 ⇒ no contribution. Its disposal is a realized
    # event captured by a component bucket.
    assert out.total_usd == Decimal("0")
    assert out.confidence == "measured"


# ── Symbol / address identity unification ───────────────────────────────────


def test_symbol_identity_case_insensitive() -> None:
    """Marks keyed by differently-cased symbols unify to one canonical token."""
    si = _snapshot([{"symbol": "weth", "balance": "1", "price_usd": "2000"}])
    sf = _snapshot([{"symbol": "WETH", "balance": "1", "price_usd": "2200"}])
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[], deployment_id=_DEP
    )
    # Case-insensitive identity: the two rows are the SAME token, Δ = $200.
    assert out.total_usd == Decimal("200")


def test_payload_named_token_with_idle_remainder_still_revalued() -> None:
    """The unspent half of a single-sided swap is ambient even though tracked.

    Swap 0.3 WETH worth out, leaving 0.7 WETH idle; WETH is payload-named
    ("tracked") but the idle 0.7 is genuine ambient inventory.
    """
    si = _snapshot([{"symbol": "WETH", "balance": "0.7", "price_usd": "1000"}])
    sf = _snapshot([{"symbol": "WETH", "balance": "0.7", "price_usd": "1100"}])
    # token_in WETH names WETH as tracked, but the swap recorded NO WETH
    # acquisition lot (token_out is USDC), so there is no open WETH lot.
    ev = _swap_event(
        token_in="WETH",
        amount_in="0.3",
        token_out="USDC",
        amount_out="300",
        amount_out_usd="300",
    )
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[ev], deployment_id=_DEP
    )
    # 0.7 idle WETH × $100 = $70 — the tracked symbol does NOT suppress the
    # idle remainder.
    assert out.total_usd == Decimal("70")
    assert "WETH" in out.excluded_tokens  # diagnostic: WETH was payload-named


# ── Open-lot mid-position MTM (Q1b) ─────────────────────────────────────────


def test_open_lot_mid_position_mtm() -> None:
    """A non-zero open swap lot at t1 contributes remaining × mark − basis (Q1b).

    Two snapshots straddling an OPEN position (no close): swap 1000 USDC → 2 WETH
    @ $1000 basis, still holding all 2 WETH at t1 with the mark up to $600/WETH.
    """
    si = _snapshot([{"symbol": "WETH", "balance": "2", "price_usd": "500"}])
    sf = _snapshot([{"symbol": "WETH", "balance": "2", "price_usd": "600"}])
    ev = _swap_event(
        token_in="USDC",
        amount_in="1000",
        token_out="WETH",
        amount_out="2",
        amount_out_usd="1000",
    )
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[ev], deployment_id=_DEP
    )
    # Open-lot MTM: 2 WETH × $600 − $1000 basis = $200. qty_idle = 2 − 2 = 0.
    assert out.total_usd == Decimal("200")
    assert out.per_token["WETH:open_lot"] == "200"


def test_open_lot_scoped_to_deployment_shared_wallet_isolation() -> None:
    """Open-lot replay is scoped to deployment_id: a sibling's swap is ignored."""
    si = _snapshot([{"symbol": "WETH", "balance": "1", "price_usd": "1000"}])
    sf = _snapshot([{"symbol": "WETH", "balance": "1", "price_usd": "1500"}])
    other = _swap_event(
        token_in="USDC",
        amount_in="1000",
        token_out="WETH",
        amount_out="1",
        amount_out_usd="1000",
        deployment_id="deployment:OTHER",
    )
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[other], deployment_id=_DEP
    )
    # The sibling's WETH lot is NOT replayed (wrong deployment); WETH is then
    # treated as pure ambient: 1 × ($1500 − $1000) = $500.
    assert out.total_usd == Decimal("500")
    assert out.per_token.get("WETH") == "500"
    assert "WETH:open_lot" not in out.per_token


def test_empty_deployment_id_with_events_fails_closed() -> None:
    """Empty deployment_id + trading activity ⇒ fail closed (cannot decompose).

    Without a deployment scope the open-lot decomposition cannot run, so a
    partially-traded token would be silently mis-valued as pure ambient. The
    lane refuses to return a measured-but-wrong total (Empty ≠ Zero).
    """
    si = _snapshot([{"symbol": "WETH", "balance": "1", "price_usd": "1000"}], deployment_id="")
    sf = _snapshot([{"symbol": "WETH", "balance": "1", "price_usd": "1500"}], deployment_id="")
    ev = _swap_event(
        token_in="USDC",
        amount_in="1000",
        token_out="WETH",
        amount_out="1",
        amount_out_usd="1000",
        deployment_id="",
    )
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[ev], deployment_id=""
    )
    assert out.total_usd is None
    assert out.confidence == "unmeasured_basis"


def test_empty_deployment_id_no_events_measures_pure_ambient() -> None:
    """Empty deployment_id but NO events ⇒ ambient term is exact, still measured.

    With no trading activity to attribute there are no open lots to miss, so the
    ambient term is fully correct even without a deployment scope. Failing closed
    here would be needlessly conservative.
    """
    si = _snapshot([{"symbol": "WETH", "balance": "1", "price_usd": "1000"}], deployment_id="")
    sf = _snapshot([{"symbol": "WETH", "balance": "1", "price_usd": "1500"}], deployment_id="")
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[], deployment_id=""
    )
    # 1 WETH × ($1500 − $1000) = $500.
    assert out.total_usd == Decimal("500")
    assert out.confidence == "measured"
    assert out.per_token["WETH"] == "500"


def test_empty_deployment_id_non_swap_events_still_measures() -> None:
    """Empty deployment_id + only NON-swap events ⇒ no open lots to miss, measures.

    A window carrying an LP / lending event but no SWAP creates no FIFO swap
    lots, so ambient-everything is exact even without a deployment scope. The
    fail-closed branch must NOT fire here — the trigger is unattributable SWAP
    activity, not event-list non-emptiness.
    """
    si = _snapshot([{"symbol": "WETH", "balance": "1", "price_usd": "1000"}], deployment_id="")
    sf = _snapshot([{"symbol": "WETH", "balance": "1", "price_usd": "1500"}], deployment_id="")
    lp_event = {
        "deployment_id": "",
        "event_type": "LP_OPEN",
        "payload_json": json.dumps({"event_type": "LP_OPEN", "token0": "USDC", "token1": "WETH"}),
    }
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[lp_event], deployment_id=""
    )
    # 1 WETH × ($1500 − $1000) = $500 — measured, not fail-closed.
    assert out.total_usd == Decimal("500")
    assert out.confidence == "measured"


def test_unmeasured_final_balance_fails_closed_not_zero() -> None:
    """A final wallet ROW present but balance=None ⇒ fail closed, not held=0.

    Empty ≠ Zero: an unmeasured balance is "unknown holdings", not "holds
    nothing". Contrast ``test_t0_only_token_absent_at_final_contributes_nothing``
    where the token has NO final row at all and correctly resolves to held=0.
    """
    si = _snapshot([{"symbol": "WETH", "balance": "2", "price_usd": "2000"}])
    sf = _snapshot([{"symbol": "WETH", "balance": None, "price_usd": "2500"}])
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[], deployment_id=_DEP
    )
    assert out.total_usd is None
    assert out.confidence == "unmeasured_balance"


# ── Source-shape tolerance: payload_json string column ──────────────────────


def test_reads_payload_json_string_column() -> None:
    """The lane tolerates the persisted ``payload_json`` string event shape."""
    si = _snapshot([{"symbol": "WETH", "balance": "0.7", "price_usd": "1000"}])
    sf = _snapshot([{"symbol": "WETH", "balance": "0.7", "price_usd": "1100"}])
    ev = {
        "deployment_id": _DEP,
        "event_type": "SWAP",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "payload_json": json.dumps(
            {"event_type": "SWAP", "token_in": "WETH", "token_out": "USDC", "amount_in": "0.3"}
        ),
    }
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[ev], deployment_id=_DEP
    )
    assert out.total_usd == Decimal("70")
    assert "WETH" in out.excluded_tokens


# ── Hardcoded-symbol-literal guard on the lane source ───────────────────────


def test_lane_has_no_hardcoded_token_symbol_literals() -> None:
    """The lane must not hardcode any token-symbol literal in CODE.

    Field-NAME literals (``token_in`` / ``token0`` / ``asset`` …) are the
    generic allowlist and are allowed; a token SYMBOL literal (WETH/USDC/…)
    would be a scalability regression. Scan code lines only (skip comments).
    """
    src = inspect.getsource(inv_mod)
    code = "\n".join(line for line in src.splitlines() if not line.strip().startswith("#"))
    # Common token symbols that must never appear as code literals.
    forbidden = ("USDC", "USDT", "WETH", "WBTC", "DAI", "ARB", "WAVAX", "WBNB")
    for sym in forbidden:
        assert f'"{sym}"' not in code and f"'{sym}'" not in code, (
            f"lane code hardcodes token symbol literal {sym!r}"
        )


def test_field_name_allowlist_is_generic_not_per_primitive() -> None:
    """The token-field allowlist is the only token-bearing knob — generic."""
    # Sanity: the allowlist is exactly the canonical payload token-symbol fields,
    # no per-primitive branch.
    assert set(inv_mod._TOKEN_SYMBOL_PAYLOAD_FIELDS) == {
        "token_in",
        "token_out",
        "token0",
        "token1",
        "asset",
    }


# ── FIFO parity: lane reuses the same store reconstruction as VIB-4984 ───────


def test_open_lot_term_matches_fifo_store_iter() -> None:
    """The open-lot term equals a direct iter_open_swap_lots MTM on the same store."""
    si = _snapshot([{"symbol": "WETH", "balance": "2", "price_usd": "500"}])
    sf = _snapshot([{"symbol": "WETH", "balance": "2", "price_usd": "600"}])
    ev = _swap_event(
        token_in="USDC",
        amount_in="1000",
        token_out="WETH",
        amount_out="2",
        amount_out_usd="1000",
    )
    store = FIFOBasisStore()
    store.reconstruct_from_events([ev])
    direct = Decimal("0")
    for _pk, _tok, remaining, cost in store.iter_open_swap_lots():
        direct += (remaining * Decimal("600")) - cost
    out = compute_inventory_revaluation(
        snapshot_initial=si, snapshot_final=sf, accounting_events=[ev], deployment_id=_DEP
    )
    assert out.total_usd == direct == Decimal("200")
