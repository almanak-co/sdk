"""VIB-5416 — NO_ACCOUNTING wallet-movement lane in the teardown swap-back clamp.

These tests lock the fund-safety invariants of the synthetic ledger lane that
makes STAKE/WRAP/MINT wallet inventory visible to the ALM-2766 clamp:

* a held NO_ACCOUNTING token becomes TRACKED (the strand fix),
* the two lanes are disjoint by accounting category (no double-count),
* an accounted SWAP disposal AND a NO_ACCOUNTING UNSTAKE disposal both NET a
  prior acquisition (the over-sweep hole — fund loss on a shared wallet — stays
  closed),
* ``ledger_rows=None`` is byte-identical to the pre-VIB-5416 behaviour.
"""

from __future__ import annotations

import json
from decimal import Decimal

from almanak.framework.accounting.basis import (
    sum_open_wallet_basis_by_token,
    synthetic_wallet_movement_events,
)

_DEP = "deployment:abc123"
_CHAIN = "ethereum"
_WALLET = "0xWaLLeT"


def _ledger_row(intent_type, token_in, amount_in, token_out, amount_out, ts, *, success=True, _id="r"):
    return {
        "intent_type": intent_type,
        "token_in": token_in,
        "amount_in": amount_in,
        "token_out": token_out,
        "amount_out": amount_out,
        "timestamp": ts,
        "success": success,
        "id": _id,
        "chain": _CHAIN,
    }


def _swap_event(token_in, amount_in, token_out, amount_out, ts, *, _id="s"):
    """A real SWAP accounting-event row (the lane that already replays)."""
    return {
        "event_type": "SWAP",
        "deployment_id": _DEP,
        "position_key": "",
        "chain": _CHAIN,
        "wallet_address": _WALLET,
        "timestamp": ts,
        "ledger_entry_id": _id,
        "payload_json": json.dumps(
            {
                "swap_position_key": f"swap:{_CHAIN.lower()}:{_WALLET.lower()}",
                "token_in": token_in,
                "amount_in": amount_in,
                "token_out": token_out,
                "amount_out": amount_out,
            }
        ),
    }


def _tracked(events, ledger_rows):
    return sum_open_wallet_basis_by_token(events, _DEP, ledger_rows=ledger_rows, chain=_CHAIN, wallet_address=_WALLET)


# ---------------------------------------------------------------------------
# Projection
# ---------------------------------------------------------------------------


def test_projection_only_no_accounting_success_rows():
    rows = [
        _ledger_row("STAKE", "ETH", "1.0", "wstETH", "0.88", "2026-06-25T00:00:00+00:00", _id="stake"),
        _ledger_row("SWAP", "ETH", "1.0", "USDC", "3000", "2026-06-25T00:10:00+00:00", _id="swap"),
        _ledger_row("UNSTAKE", "wstETH", "0.5", "ETH", "0.55", "2026-06-25T00:20:00+00:00", _id="unstake"),
        _ledger_row("STAKE", "ETH", "9.0", "wstETH", "8.0", "2026-06-25T00:30:00+00:00", success=False, _id="failed"),
    ]
    out = synthetic_wallet_movement_events(rows, _DEP, chain=_CHAIN, wallet_address=_WALLET)
    # SWAP (accounted) and the failed STAKE are excluded; STAKE + UNSTAKE kept.
    ids = {e["ledger_entry_id"] for e in out}
    assert ids == {"stake", "unstake"}
    assert all(e["event_type"] == "WALLET_MOVEMENT" for e in out)
    assert all(e["deployment_id"] == _DEP for e in out)


def test_projection_requires_chain_and_wallet():
    rows = [_ledger_row("STAKE", "ETH", "1.0", "wstETH", "0.88", "2026-06-25T00:00:00+00:00")]
    assert synthetic_wallet_movement_events(rows, _DEP, chain="", wallet_address=_WALLET) == []
    assert synthetic_wallet_movement_events(rows, _DEP, chain=_CHAIN, wallet_address="") == []


# ---------------------------------------------------------------------------
# Tracked-map folding
# ---------------------------------------------------------------------------


def test_held_stake_is_tracked():
    stake = _ledger_row("STAKE", "ETH", "1.0", "wstETH", "0.88", "2026-06-25T00:00:00+00:00")
    assert _tracked([], [stake]) == {"WSTETH": Decimal("0.88")}


def test_wrap_native_is_tracked():
    wrap = _ledger_row("WRAP_NATIVE", "ETH", "2.0", "WETH", "2.0", "2026-06-25T00:00:00+00:00")
    assert _tracked([], [wrap]).get("WETH") == Decimal("2.0")


def test_ledger_rows_none_is_byte_identical():
    # No ledger lane → identical to the pre-VIB-5416 accounting-only result.
    assert sum_open_wallet_basis_by_token([], _DEP) == {}
    assert sum_open_wallet_basis_by_token([], _DEP, ledger_rows=None) == {}


def test_empty_deployment_id_is_unmeasured_sentinel():
    stake = _ledger_row("STAKE", "ETH", "1.0", "wstETH", "0.88", "2026-06-25T00:00:00+00:00")
    assert sum_open_wallet_basis_by_token([], "", ledger_rows=[stake], chain=_CHAIN, wallet_address=_WALLET) is None


# ---------------------------------------------------------------------------
# Over-sweep / netting — the fund-safety core
# ---------------------------------------------------------------------------


def test_stake_then_unstake_nets_wsteth_to_zero():
    # The phantom-balance over-sweep hole: project BOTH legs so the UNSTAKE input
    # disposes the STAKE acquisition. wstETH must net to 0 — never a phantom.
    stake = _ledger_row("STAKE", "ETH", "1.0", "wstETH", "0.88", "2026-06-25T00:00:00+00:00", _id="stake")
    unstake = _ledger_row("UNSTAKE", "wstETH", "0.88", "ETH", "1.0", "2026-06-25T01:00:00+00:00", _id="unstake")
    tracked = _tracked([], [stake, unstake])
    assert tracked.get("WSTETH", Decimal("0")) == Decimal("0")


def test_stake_then_accounted_swap_disposal_nets():
    # Cross-lane: a real SWAP that disposes part of the staked wstETH drains the
    # synthetic STAKE lot via the source-agnostic match_swap_disposal.
    stake = _ledger_row("STAKE", "ETH", "1.0", "wstETH", "0.88", "2026-06-25T00:00:00+00:00")
    swap = _swap_event("wstETH", "0.5", "USDC", "1500", "2026-06-25T02:00:00+00:00")
    tracked = _tracked([swap], [stake])
    assert tracked.get("WSTETH") == Decimal("0.38")  # 0.88 - 0.5
    assert tracked.get("USDC") == Decimal("1500")


def test_partial_unstake_leaves_remainder_tracked():
    stake = _ledger_row("STAKE", "ETH", "1.0", "wstETH", "1.0", "2026-06-25T00:00:00+00:00")
    unstake = _ledger_row("UNSTAKE", "wstETH", "0.4", "ETH", "0.45", "2026-06-25T01:00:00+00:00")
    tracked = _tracked([], [stake, unstake])
    assert tracked.get("WSTETH") == Decimal("0.6")


def test_disposal_before_acquisition_does_not_over_count():
    # Even if rows arrive out of order, the timestamp merge replays the STAKE
    # acquisition before the later UNSTAKE disposal — no phantom.
    unstake = _ledger_row("UNSTAKE", "wstETH", "0.88", "ETH", "1.0", "2026-06-25T01:00:00+00:00", _id="u")
    stake = _ledger_row("STAKE", "ETH", "1.0", "wstETH", "0.88", "2026-06-25T00:00:00+00:00", _id="s")
    tracked = _tracked([], [unstake, stake])  # disposal listed first
    assert tracked.get("WSTETH", Decimal("0")) == Decimal("0")


def test_unknown_intent_is_not_projected():
    # classify() returns NO_ACCOUNTING for UNREGISTERED intents; an unknown intent
    # must NOT be projected into the tracked lane (it would over-count a token the
    # deployment never legitimately acquired). record_for gates on a known primitive.
    unknown = _ledger_row("TOTALLY_UNKNOWN_INTENT", "ETH", "1.0", "wstETH", "0.88", "2026-06-25T00:00:00+00:00")
    assert synthetic_wallet_movement_events([unknown], _DEP, chain=_CHAIN, wallet_address=_WALLET) == []
    assert _tracked([], [unknown]) == {}


def test_equal_timestamp_stake_then_swap_disposal_nets():
    # EVM same-block: a synthetic STAKE acquisition and a real SWAP that disposes it
    # share a block timestamp. The synthetic-first tiebreak must put the STAKE
    # acquisition in the pool BEFORE the real disposal, so it nets (no over-count).
    ts = "2026-06-25T00:00:00+00:00"
    stake = _ledger_row("STAKE", "ETH", "1.0", "wstETH", "0.88", ts)
    swap = _swap_event("wstETH", "0.5", "USDC", "1500", ts)
    tracked = _tracked([swap], [stake])
    assert tracked.get("WSTETH") == Decimal("0.38")  # 0.88 - 0.5, NOT 0.88


def test_same_timestamp_synthetics_preserve_chronological_order():
    # Two NO_ACCOUNTING events in the SAME block (same second): STAKE acquires
    # wstETH, UNSTAKE disposes it. The gateway delivers them chronologically
    # (STAKE first); the stable timestamp-merge must preserve that order so the
    # disposal nets the acquisition (→ 0), never replaying disposal-first (phantom).
    ts = "2026-06-25T00:00:00+00:00"
    stake = _ledger_row("STAKE", "ETH", "1.0", "wstETH", "0.88", ts, _id="stake")
    unstake = _ledger_row("UNSTAKE", "wstETH", "0.88", "ETH", "1.0", ts, _id="unstake")
    tracked = _tracked([], [stake, unstake])  # chronological order, same timestamp
    assert tracked.get("WSTETH", Decimal("0")) == Decimal("0")


def test_combined_swap_and_stake_acquisition_both_count():
    # A token acquired via BOTH an accounted SWAP and a NO_ACCOUNTING STAKE sums
    # without double-counting (disjoint by category) and nets a later disposal.
    swap_in = _swap_event("USDC", "1500", "wstETH", "0.30", "2026-06-25T00:00:00+00:00", _id="swapin")
    stake = _ledger_row("STAKE", "ETH", "1.0", "wstETH", "1.00", "2026-06-25T00:10:00+00:00", _id="stake")
    tracked = _tracked([swap_in], [stake])
    assert tracked.get("WSTETH") == Decimal("1.30")  # 0.30 + 1.00, no double-count
