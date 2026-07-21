"""VIB-5865 PR-2 — the LP measured fold (``FIFOBasisStore._replay_lp``).

PR-1 made the LP family's invisibility VISIBLE (poison → degraded refusal). This
PR makes it MEASURED: LP_OPEN drains the wallet lots it consumed, LP_CLOSE /
LP_COLLECT_FEES credit what came back, so the teardown clamp sweeps exactly the
LP proceeds and nothing else.

The two load-bearing guards here:

1. **The over-sweep trap** (``test_lp_open_disposal_prevents_over_sweep``) — the
   ticket's mutation trap as a permanent exact-equality regression. A handler
   that credits LP_CLOSE proceeds WITHOUT decrementing what LP_OPEN consumed
   double-counts and lets the clamp sweep commingled user funds.
2. **Restart parity** (``test_restart_parity_live_hooks_vs_replay``) — the live
   ``lp_handler._apply_lp_wallet_basis_hooks`` and this replay must produce the
   IDENTICAL pool, or a runner restart silently re-bases wallet inventory.

Fee formula (convention-robust, VIB-5865 design O2 + the CI convention catch):
the per-leg wallet credit is ``max(amount_collected, fees)``. Two writer
conventions, and ``max`` is correct for BOTH because no LPCloseData writer emits
``amount_collected > 0`` AND a disjoint additive ``fees > 0`` (survey:
``tests/reports/vib5865-pr2-fee-convention-survey.md``):

* FEE-INCLUSIVE (V3-family / Curve / Fluid / Pendle / Aerodrome):
  ``amount_collected`` = principal PLUS fees and ``fees`` = ``max(collect-burn,0)``
  is a COMPONENT inside it ⇒ ``amount ≥ fees`` ⇒ ``max = amount``. ``amount+fees``
  double-counts (100% on a fee-only V3 harvest, where ``amount == fees``).
* FEES-SEPARATE (traderjoe_v2 / uniswap_v4 fee-only collect):
  ``amount_collected == 0`` and the real fee lands in ``fees`` on a separate rail
  ⇒ ``max(0, fees) = fees``. Crediting ``amount`` alone dropped it (the CI
  regression this file now guards).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.accounting.basis import (
    FIFOBasisStore,
    _declared_wallet_delta_lane,
    sum_open_wallet_basis_by_token,
)
from almanak.framework.accounting.category_handlers.lp_handler import _apply_lp_wallet_basis_hooks
from almanak.framework.teardown.swap_clamp import decide_swap_clamp

_DEP = "deployment:lp-fold"
_CHAIN = "arbitrum"
_WALLET = "0x1111111111111111111111111111111111111111"
_SWAPKEY = f"swap:{_CHAIN}:{_WALLET}"


def _swap_event(token_out, amount_out, *, ts="2026-07-20T00:00:00+00:00"):
    return {
        "event_type": "SWAP",
        "deployment_id": _DEP,
        "position_key": "",
        "chain": _CHAIN,
        "wallet_address": _WALLET,
        "timestamp": ts,
        "payload_json": json.dumps(
            {
                "swap_position_key": _SWAPKEY,
                "token_in": "USDC",
                "amount_in": "0",
                "token_out": token_out,
                "amount_out": amount_out,
                "amount_out_usd": "1",
            }
        ),
    }


def _lp_event(event_type, *, amount0=None, amount1=None, token0="WETH", token1="USDC", ts, **extra):
    payload = {
        "event_type": event_type,
        "protocol": "uniswap_v3",
        "token0": token0,
        "token1": token1,
        "amount0": amount0,
        "amount1": amount1,
        "position_key": f"lp:uniswap_v3:{_CHAIN}:0x5477:weth/usdc/500",
    }
    payload.update(extra)
    return {
        "event_type": event_type,
        "deployment_id": _DEP,
        "position_key": payload["position_key"],
        "chain": _CHAIN,
        "wallet_address": _WALLET,
        "timestamp": ts,
        "payload_json": json.dumps(payload),
    }


def _tracked(events):
    return sum_open_wallet_basis_by_token(events, _DEP, chain=_CHAIN, wallet_address=_WALLET)


# ---------------------------------------------------------------------------
# 1. THE OVER-SWEEP TRAP (the ticket's mutation trap, made permanent)
# ---------------------------------------------------------------------------


def test_lp_open_disposal_prevents_over_sweep() -> None:
    """[SWAP +1.0 WETH, LP_OPEN -1.0 consumed, LP_CLOSE +1.05] ⇒ tracked == 1.05, NOT 2.05.

    THE fund-safety regression of this epic. The wallet holds 3.05 WETH: 1.05 of
    LP proceeds + 2.0 of the user's own commingled money.

    * Correct (this code): tracked 1.05 ⇒ clamp sweeps 1.05.
    * Naive un-decremented handler: 1.0 (the SWAP lot LP_OPEN never drained)
      + 1.05 (proceeds) = 2.05 ⇒ clamp sweeps 2.05, taking 1.00 WETH of the
      USER'S money.

    MUTATION-VERIFIED: commenting out the ``match_swap_disposal`` branch in
    ``_replay_lp`` makes this exact assertion fail with ``2.05`` — the naive
    handler's number, captured in the PR body.
    """
    events = [
        _swap_event("WETH", "1.0", ts="2026-07-20T00:00:00+00:00"),
        _lp_event("LP_OPEN", amount0="1.0", amount1="0", ts="2026-07-20T00:00:01+00:00"),
        _lp_event("LP_CLOSE", amount0="1.05", amount1="0", ts="2026-07-20T00:00:02+00:00"),
    ]
    tracked = _tracked(events)
    assert tracked == {"WETH": Decimal("1.05")}

    decision = decide_swap_clamp(live_balance=Decimal("3.05"), tracked_map=tracked, from_token="WETH")
    assert decision.reason == "clamped"
    assert decision.skip is False
    assert decision.amount == Decimal("1.05")
    # The user's commingled 2.00 WETH is untouched.
    assert Decimal("3.05") - decision.amount == Decimal("2.00")


def test_lp_open_uses_receipt_confirmed_amount_not_the_request() -> None:
    """The decrement is the CONSUMED amount from the payload, never the requested one.

    Real FLOW-A trace: the strategy requested 1.9006 WETH but tick-alignment made
    the compile consume 1.708743113797863. The payload carries the consumed
    amount; decrementing the request would over-drain by ~0.19 WETH on a normal
    run and under-report tracked inventory (a strand).
    """
    events = [
        _swap_event("WETH", "2.0", ts="2026-07-20T00:00:00+00:00"),
        _lp_event("LP_OPEN", amount0="1.708743113797863", amount1="0", ts="2026-07-20T00:00:01+00:00"),
    ]
    assert _tracked(events) == {"WETH": Decimal("2.0") - Decimal("1.708743113797863")}


def test_lp_open_excess_disposal_drops_and_never_mints_a_negative_lot() -> None:
    """Pre-funded inventory the strategy never acquired through a tracked lane stays untracked."""
    events = [
        _swap_event("WETH", "0.5", ts="2026-07-20T00:00:00+00:00"),
        _lp_event("LP_OPEN", amount0="2.0", amount1="0", ts="2026-07-20T00:00:01+00:00"),
    ]
    tracked = _tracked(events)
    assert tracked.get("WETH", Decimal("0")) == Decimal("0")
    assert all(v is None or v >= 0 for v in tracked.values())


# ---------------------------------------------------------------------------
# 2. The fee formula (pinned)
# ---------------------------------------------------------------------------


def test_close_credit_is_fee_inclusive_amount_not_amount_plus_fees() -> None:
    """``amount_collected`` ALREADY contains the fees — crediting ``amount + fees`` doubles them.

    MUTATION CHECK: restoring ``amount + fees`` in either ``_replay_lp`` or the
    live hook yields 1.10 here and breaks restart parity below.
    """
    events = [
        _lp_event(
            "LP_CLOSE",
            amount0="1.05",  # collect_amount = principal 1.00 + fees 0.05
            amount1="0",
            fees0_collected="0.05",
            ts="2026-07-20T00:00:01+00:00",
        )
    ]
    assert _tracked(events) == {"WETH": Decimal("1.05")}


def test_fee_only_harvest_credits_the_collected_amount_once() -> None:
    """FEE-INCLUSIVE V3 harvest: ``amount_collected == fees`` ⇒ credit X (=max(X,X)), not 2X.

    Real V3 ``LP_COLLECT_FEES`` shape sets BOTH fee legs (``lp_accounting.py``
    writes ``fees{0,1}_collected`` from ``LPCloseData.fees{0,1}``, both computed
    as ``max(collect-burn,0)`` on a collect — never ``None``), so the idle USDC
    leg carries a MEASURED-zero fee and is skipped, not poisoned.
    """
    events = [
        _lp_event(
            "LP_COLLECT_FEES",
            amount0="0.07",
            amount1="0",
            fees0_collected="0.07",
            fees1_collected="0",  # measured zero — the real V3 shape sets both legs
            ts="2026-07-20T00:00:01+00:00",
        )
    ]
    assert _tracked(events) == {"WETH": Decimal("0.07")}


def test_fees_separate_collect_credits_the_fee_amount() -> None:
    """FEES-SEPARATE convention (traderjoe_v2 / uniswap_v4): amount=0, fees>0 ⇒ credit=fees.

    THE CI regression guard. ``max(amount, fees)`` = ``max(0, 0.03)`` = 0.03 — a
    real fee credit the prior ``amount``-alone formula silently DROPPED (leaving
    the swap-back under-tracked and the fee lot's USD basis lost). The fee has
    landed in ``fees0_collected`` via the separate extract-fees rail by replay
    time (VIB-3494 shape).
    """
    events = [
        _lp_event(
            "LP_COLLECT_FEES",
            amount0="0",
            amount1="0",
            fees0_collected="0.03",
            fees1_collected="0",
            ts="2026-07-20T00:00:01+00:00",
        )
    ]
    assert _tracked(events) == {"WETH": Decimal("0.03")}


def test_zero_credit_fee_harvest_is_poisoned_not_credited_zero() -> None:
    """A fees-only collect with the fee UNMEASURED (amount=0, fees=None) is POISONED.

    traderjoe_v2 / uniswap_v4 emit ``amount_collected == 0`` and, when the
    separate extract-fees rail has NOT populated the payload, ``fees == None``.
    The tokens genuinely moved but this payload cannot say how much — Empty ≠
    Zero, so the symbol is poisoned rather than presented as a measured zero.
    (Contrast ``test_fees_separate_collect_credits_the_fee_amount``, where the
    fee IS measured and is credited.)
    """
    events = [_lp_event("LP_COLLECT_FEES", amount0="0", amount1="0", ts="2026-07-20T00:00:01+00:00")]
    tracked = _tracked(events)
    assert tracked["WETH"] is None
    assert tracked["USDC"] is None


def test_unparseable_leg_amount_poisons_that_token() -> None:
    """Token identity known + amount unmeasurable ⇒ poison (an undrained debit over-sweeps)."""
    events = [
        _swap_event("WETH", "5", ts="2026-07-20T00:00:00+00:00"),
        _lp_event("LP_OPEN", amount0=None, amount1="10", ts="2026-07-20T00:00:01+00:00"),
    ]
    tracked = _tracked(events)
    assert tracked["WETH"] is None


# ---------------------------------------------------------------------------
# 3. RESTART PARITY — live hooks vs replay must agree exactly
# ---------------------------------------------------------------------------


def _lots(store: FIFOBasisStore) -> list[tuple[str, str, Decimal]]:
    return sorted(
        (key, token, remaining) for key, token, remaining, _cost in store.iter_open_wallet_basis_lots()
    )


def test_restart_parity_live_hooks_vs_replay() -> None:
    """The same LP history through (a) the live hooks and (b) the replay ⇒ identical lots.

    This is the test that keeps the two implementations of the fee formula from
    drifting: change one side only and the open lots diverge.
    """
    ts = datetime(2026, 7, 20, tzinfo=UTC)
    live = FIFOBasisStore()
    live.record_swap_acquisition(
        deployment_id=_DEP, position_key=_SWAPKEY, token="WETH", amount=Decimal("1.0"), cost_usd=Decimal("1")
    )
    common = {
        "basis_store": live,
        "deployment_id": _DEP,
        "cycle_id": "c1",
        "chain": _CHAIN,
        "wallet_address": _WALLET,
        "token0": "WETH",
        "token1": "USDC",
        "price_oracle": {},
        "timestamp": ts,
        "tx_hash": "",
        "ledger_entry_id": "",
    }
    _apply_lp_wallet_basis_hooks(
        intent_type_str="LP_OPEN",
        amount0=Decimal("1.0"),
        amount1=Decimal("0"),
        fees0=None,
        fees1=None,
        cost_basis_usd=None,
        fees_total_usd=None,
        **common,
    )
    _apply_lp_wallet_basis_hooks(
        intent_type_str="LP_CLOSE",
        amount0=Decimal("1.05"),
        amount1=Decimal("0"),
        fees0=Decimal("0.05"),
        fees1=None,
        cost_basis_usd=None,
        fees_total_usd=None,
        **common,
    )

    replayed = FIFOBasisStore()
    replayed.reconstruct_from_events(
        [
            _swap_event("WETH", "1.0", ts="2026-07-20T00:00:00+00:00"),
            _lp_event("LP_OPEN", amount0="1.0", amount1="0", ts="2026-07-20T00:00:01+00:00"),
            _lp_event(
                "LP_CLOSE",
                amount0="1.05",
                amount1="0",
                fees0_collected="0.05",
                ts="2026-07-20T00:00:02+00:00",
            ),
        ]
    )

    live_lots = [(t, r) for _k, t, r in _lots(live)]
    replay_lots = [(t, r) for _k, t, r in _lots(replayed)]
    # THE restart-parity guarantee: the two paths agree lot-for-lot.
    assert live_lots == replay_lots
    # Both key the lot under the lowercased token (``_key`` lower-cases; the raw
    # ``iter_open_wallet_basis_lots`` yields that key verbatim — the clamp's
    # ``sum_open_wallet_basis_by_token`` is what upper-cases via
    # ``canonical_pt_symbol``). Assert the concrete shape too, so a future change
    # that broke BOTH sides identically still can't pass silently.
    assert replay_lots == [("weth", Decimal("1.05"))]


# ---------------------------------------------------------------------------
# 3b. Convention-aware credit AT THE LIVE HOOK (mirrors the replay pins above)
# ---------------------------------------------------------------------------


def _hook_close_credit(amount0, fees0) -> Decimal | None:
    """Run the live LP_CLOSE/COLLECT hook for one WETH leg; return the credited lot amount."""
    store = FIFOBasisStore()
    _apply_lp_wallet_basis_hooks(
        intent_type_str="LP_COLLECT_FEES",
        basis_store=store,
        deployment_id=_DEP,
        cycle_id="c1",
        chain=_CHAIN,
        wallet_address=_WALLET,
        token0="WETH",
        token1="USDC",
        amount0=amount0,
        amount1=Decimal("0"),
        fees0=fees0,
        fees1=Decimal("0"),
        cost_basis_usd=None,
        fees_total_usd=None,
        price_oracle={},
        timestamp=datetime(2026, 7, 20, tzinfo=UTC),
        tx_hash="",
        ledger_entry_id="",
    )
    lots = store._lots.get(store._key(_DEP, _SWAPKEY, "WETH"), [])
    return lots[0]["amount"] if lots else None


def test_hook_credit_fee_inclusive_harvest_is_not_doubled() -> None:
    """FEE-INCLUSIVE (V3): amount_collected == fees == 0.07 ⇒ hook credits 0.07, never 0.14.

    MUTATION CHECK: restoring ``amount + fees`` credits 0.14 here.
    """
    assert _hook_close_credit(Decimal("0.07"), Decimal("0.07")) == Decimal("0.07")


def test_hook_credit_fee_inclusive_close_keeps_principal_plus_fees() -> None:
    """FEE-INCLUSIVE close: amount_collected == P+F == 1.05, fees == 0.05 ⇒ credit 1.05."""
    assert _hook_close_credit(Decimal("1.05"), Decimal("0.05")) == Decimal("1.05")


def test_hook_credit_fees_separate_collect_is_not_dropped() -> None:
    """FEES-SEPARATE (traderjoe_v2 / uniswap_v4): amount_collected == 0, fees == 0.03 ⇒ credit 0.03.

    MUTATION CHECK: crediting ``amount`` alone mints NO lot here — the CI
    regression this restores. (Mirrors ``test_fees_separate_collect_credits...``
    on the replay side and the hook fixture
    ``test_lp_collect_fees_records_basis_when_principal_zero``.)
    """
    assert _hook_close_credit(Decimal("0"), Decimal("0.03")) == Decimal("0.03")


# ---------------------------------------------------------------------------
# 4. #3349 review deferrals (Gemini robustness nits)
# ---------------------------------------------------------------------------


def test_lane_lookup_survives_a_type_whose_str_raises() -> None:
    """A value whose ``__str__`` raises degrades to "no lane", never escapes.

    Deferral (1) from #3349: the ``str()`` conversion lives inside the guard, so a
    corrupt/hostile payload value cannot abort the whole tracked read.
    """

    class Hostile:
        def __str__(self) -> str:
            raise RuntimeError("boom")

        def __bool__(self) -> bool:
            raise RuntimeError("boom")

    assert _declared_wallet_delta_lane(Hostile()) is None


def test_coin_symbols_accepts_a_tuple() -> None:
    """Deferral (2) from #3349: a TUPLE ``coin_symbols`` is scanned like a list.

    The genuinely reachable tuple path is ``_extra_payload_tokens`` — it reads
    ``payload_json`` as a RAW DICT when it is not a string
    (``json.loads(raw) if isinstance(raw, str) else raw``), so an in-process
    caller that hands a payload dict straight in (never JSON-round-tripped) can
    carry a tuple. A list-only check would silently miss every Curve N-coin leg
    on that path, under-poisoning the tracked map. (The replay's own ``_lp_legs``
    only ever sees a JSON-round-tripped list via ``reconstruct_from_events``, so
    its tuple handling is belt-and-suspenders — but this projection path is
    live.)
    """
    from almanak.framework.accounting.basis import _extra_payload_tokens

    ev_list = {"payload_json": json.dumps({"coin_symbols": ["USDC", "USDT", "DAI"]})}
    ev_tuple = {"payload_json": {"coin_symbols": ("USDC", "USDT", "DAI")}}
    expected = {"USDC", "USDT", "DAI"}
    assert _extra_payload_tokens(ev_list) == expected
    # The tuple (raw-dict, no JSON round trip) must yield the identical footprint.
    assert _extra_payload_tokens(ev_tuple) == expected


def test_coin_symbols_nonstring_entry_preserves_positional_mapping() -> None:
    """Gemini #3350: non-string coins become "" IN PLACE, never filtered out.

    ``_lp_legs`` maps coins positionally (idx 0/1 = token0/token1 fallback
    identity, idx>=2 = unmeasured tail). Filtering a non-string entry would
    shift a tail coin into the token1 slot — a wrong-identity credit, the
    over-credit direction.
    """
    from datetime import UTC, datetime

    from almanak.framework.accounting.basis import FIFOBasisStore, _ReplayContext

    store = FIFOBasisStore()
    ctx = _ReplayContext(
        event_type="LP_CLOSE",
        deployment_id="deployment:abc123def456",
        position_key="lp:curve:arbitrum:0xwallet:POOL",
        # token0/token1 empty (Curve proportional-close shape); identity only
        # in coin_symbols, with a hostile non-string at idx 1.
        payload={"coin_symbols": ["USDC", 123, "DAI"], "token0": "", "token1": "", "amount0": "5", "amount1": "7"},
        timestamp=datetime(2026, 7, 20, tzinfo=UTC),
        swap_wallet_key="swap:arbitrum:0xwallet",
        ledger_entry_id=None,
        chain="arbitrum",
    )
    legs = store._lp_legs(ctx)
    tokens = [t for t, _a, _f in legs]
    # idx0 -> USDC (measured leg), idx1 -> "" (skipped), idx2 -> DAI must stay
    # a TAIL leg (unmeasured amount), never absorb the token1 slot's amount.
    assert "USDC" in tokens
    assert "DAI" in tokens
    dai_leg = next(leg for leg in legs if leg[0] == "DAI")
    assert dai_leg[1] is None, "tail coin must stay unmeasured, not inherit token1's amount"
