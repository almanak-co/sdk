"""VIB-5865 — the UNMEASURED-lane poison fold in ``sum_open_wallet_basis_by_token``.

Contract under test:

* A token touched by a primitive declaring ``WalletDeltaLane.UNMEASURED`` has an
  UNPROVABLE total, so it is set to ``None`` in the tracked map — **overriding**
  any measured quantity for the same symbol (Empty ≠ Zero).
* ``decide_swap_clamp`` then refuses that swap-back with
  ``tracked_qty_unmeasured`` / ``degraded=True`` (VISIBLE) instead of the
  pre-VIB-5865 ``untracked_token`` / ``degraded=False`` (SILENT strand).
* A history containing ONLY measured-lane primitives is byte-identical to the
  pre-VIB-5865 map — this PR changes no sweep amount anywhere.

The event / ledger shapes below are copied from a REAL uniswap_v3 LP round trip
on Arbitrum (deployment ``b3816ff5ddb8``, ``tests/reports/vib5865-defect1-trace-baseline.md``):
LP payloads key their legs ``token0`` / ``token1``, and the ledger rows carry
``token_in`` / ``token_out``.
"""

from __future__ import annotations

import json
import logging
from decimal import Decimal

from almanak.framework.accounting.basis import sum_open_wallet_basis_by_token
from almanak.framework.teardown.swap_clamp import decide_swap_clamp

_DEP = "deployment:b3816ff5ddb8"
_CHAIN = "arbitrum"
_WALLET = "0x1111111111111111111111111111111111111111"
_SWAPKEY = f"swap:{_CHAIN}:{_WALLET}"


def _swap_event(token_out, amount_out, *, token_in="WETH", amount_in="0", ts="2026-07-20T00:00:00+00:00"):
    """A measured SWAP acquisition — the lane that already replays."""
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
                "token_in": token_in,
                "amount_in": amount_in,
                "token_out": token_out,
                "amount_out": amount_out,
                "amount_out_usd": "1",
            }
        ),
    }


def _lp_event(event_type, *, token0="WETH", token1="USDC", ts="2026-07-20T00:00:01+00:00", **extra):
    """A real-shaped LP accounting event (payload keys verbatim from the trace DB)."""
    payload = {
        "event_type": event_type,
        "protocol": "uniswap_v3",
        "token0": token0,
        "token1": token1,
        "amount0": "1.708743113797863",
        "amount1": "3562.499999",
        "position_key": f"lp:uniswap_v3:{_CHAIN}:0x5477:weth/usdc/500",
        "confidence": "HIGH",
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


def _vault_event(token="WETH", ts="2026-07-20T00:00:01+00:00"):
    """A still-UNMEASURED wallet mover (VIB-5865 PR-2 left VAULT_* in that lane)."""
    return {
        "event_type": "VAULT_REDEEM",
        "deployment_id": _DEP,
        "position_key": "vault:erc4626:arbitrum:0xvault",
        "chain": _CHAIN,
        "wallet_address": _WALLET,
        "timestamp": ts,
        "payload_json": json.dumps({"asset_token": token, "assets_amount": "1.5", "confidence": "HIGH"}),
    }


def _ledger_row(intent_type, token_in, token_out, *, success=True, _id="r1"):
    return {
        "intent_type": intent_type,
        "token_in": token_in,
        "amount_in": "1",
        "token_out": token_out,
        "amount_out": "1",
        "timestamp": "2026-07-20T00:00:02+00:00",
        "success": success,
        "id": _id,
        "chain": _CHAIN,
    }


def _tracked(events, ledger_rows=None):
    return sum_open_wallet_basis_by_token(
        events, _DEP, ledger_rows=ledger_rows, chain=_CHAIN, wallet_address=_WALLET
    )


# ---------------------------------------------------------------------------
# 1. Measured lanes are byte-identical (the no-regression contract)
# ---------------------------------------------------------------------------


def test_measured_only_history_is_unchanged() -> None:
    """SWAP-only history → the exact pre-VIB-5865 map, no ``None`` anywhere."""
    out = _tracked([_swap_event("USDC", "42"), _swap_event("DAI", "7")])
    assert out == {"USDC": Decimal("42"), "DAI": Decimal("7")}
    assert all(v is not None for v in out.values())


def test_ledger_projection_lane_still_folds_measured() -> None:
    """A STAKE (LEDGER_PROJECTION) row still projects a MEASURED quantity, not a poison.

    MUTATION CHECK: declaring STAKE ``UNMEASURED`` instead of ``LEDGER_PROJECTION``
    turns this measured ``Decimal`` into ``None`` and fails here.
    """
    # Map keys are canonicalised (upper-cased) by ``canonical_pt_symbol``.
    out = _tracked([], [_ledger_row("STAKE", "ETH", "wstETH")])
    assert out == {"WSTETH": Decimal("1")}


# ---------------------------------------------------------------------------
# 2. The poison — an UNMEASURED lane overrides a measured quantity
# ---------------------------------------------------------------------------


def test_unmeasured_event_poisons_token_even_with_a_measured_swap_lot() -> None:
    """The headline invariant: measured ≠ provable once an UNMEASURED verb touched the symbol.

    A SWAP bought 5 WETH (measured). An LP_OPEN then consumed an unknown amount of
    WETH the replay cannot decrement, so the 5 is a lower bound of unknown
    tightness — sweeping ``min(5, live)`` could still touch commingled funds.

    MUTATION CHECK: removing the ``by_token[sym] = None`` override in
    ``_poison_unmeasured_tokens`` (or applying the poison BEFORE the measured
    folds) leaves ``WETH`` at ``Decimal("5")`` and fails this test.
    """
    out = _tracked([_swap_event("WETH", "5"), _vault_event("WETH")])
    assert out["WETH"] is None


def test_poisoned_token_yields_visible_degraded_refusal() -> None:
    """The clamp turns the ``None`` into ``tracked_qty_unmeasured`` + ``degraded=True``."""
    out = _tracked([_swap_event("WETH", "5"), _vault_event("WETH")])
    decision = decide_swap_clamp(live_balance=Decimal("5"), tracked_map=out, from_token="WETH")
    assert decision.reason == "tracked_qty_unmeasured"
    assert decision.skip is True
    assert decision.degraded is True
    assert decision.amount is None


def test_trace_baseline_now_measures_the_lp_proceeds() -> None:
    """Regression pinned to the real FLOW-A trace (deployment b3816ff5ddb8).

    THE FIX'S SIGNATURE, in three revisions of this one history:

    * pre-VIB-5865:  ``{'USDC': 3726.460265}`` — no ``WETH`` key at all. Clamp
      silently skipped 2.0006 WETH (``untracked_token`` / ``degraded=False``);
      the proceeds stranded.
    * PR-1 (poison):  ``{'WETH': None, 'USDC': None}`` — visible degraded refusal.
    * PR-2 (this):    ``WETH`` is MEASURED at the LP_CLOSE credit, so the clamp
      sweeps exactly the LP proceeds and leaves the pre-funded buffer alone.

    History is the clamp-time one — LP_OPEN then LP_CLOSE, WITHOUT the
    consolidation SWAP, because the clamp is what DECIDES that swap.

    The numbers are the trace's real ones: LP_OPEN consumed 1.708743113797863
    WETH (receipt-confirmed, NOT the 1.9006 requested), LP_CLOSE returned
    1.708743113797862999, and the wallet held 2.000596102324696795 — the
    difference being a 0.291852988526833796 WETH buffer that was NEVER deployed.
    Sweeping that buffer is the ~$546 over-sweep quantified in the trace report
    §5; the clamp now refuses it by construction.
    """
    events = [
        _lp_event("LP_OPEN", ts="2026-07-20T13:47:49+00:00"),
        _lp_event(
            "LP_CLOSE",
            ts="2026-07-20T13:51:02+00:00",
            amount0="1.708743113797862999",
            amount1="3562.499998",
            fees0_collected="0",
            fees1_collected="0",
        ),
    ]
    out = _tracked(events)
    assert out["WETH"] == Decimal("1.708743113797862999")

    live = Decimal("2.000596102324696795")
    decision = decide_swap_clamp(live_balance=live, tracked_map=out, from_token="WETH")
    assert decision.reason == "clamped"
    assert decision.skip is False
    assert decision.degraded is False
    assert decision.amount == Decimal("1.708743113797862999")
    # The never-deployed buffer is left untouched — the over-sweep the manual
    # lane performed for real (~$546) is exactly this delta.
    assert live - decision.amount == Decimal("0.291852988526833796")


def test_unmeasured_ledger_row_poisons_both_legs() -> None:
    """An UNMEASURED-lane ledger row poisons ``token_in`` AND ``token_out``.

    An undrained DEBIT is the over-sweep direction, so the consumed leg matters as
    much as the credited one.
    """
    out = _tracked([_swap_event("USDC", "3726.46")], [_ledger_row("PERP_OPEN", "WETH", "USDC")])
    assert out["WETH"] is None
    assert out["USDC"] is None


def test_failed_unmeasured_ledger_row_does_not_poison() -> None:
    """A reverted tx moved nothing — it must not degrade an otherwise measured map."""
    out = _tracked([_swap_event("USDC", "10")], [_ledger_row("PERP_OPEN", "WETH", "USDC", success=False)])
    assert out == {"USDC": Decimal("10")}


def test_settlement_asset_token_key_is_scanned() -> None:
    """SETTLE_* payloads key their token ``asset_token`` (real fixture shape)."""
    ev = {
        "event_type": "SETTLE_REDEEM",
        "deployment_id": _DEP,
        "position_key": "",
        "chain": _CHAIN,
        "wallet_address": _WALLET,
        "timestamp": "2026-07-20T00:00:05+00:00",
        "payload_json": json.dumps({"asset_token": "USDC", "assets_delta": "100", "confidence": "HIGH"}),
    }
    out = _tracked([_swap_event("USDC", "10"), ev])
    assert out["USDC"] is None


def test_curve_ncoin_tail_is_poisoned_by_the_replay() -> None:
    """Curve N-coin: legs 0/1 are measured by the replay, coins at index >= 2 are POISONED.

    The LP payload persists amounts only for ``amount0``/``amount1``, so a coin at
    index >= 2 has a known identity and an unknowable amount — Empty ≠ Zero. Since
    VIB-5865 PR-2 the LP rows are EVENT_REPLAY, so this poison comes from the
    replay itself (``FIFOBasisStore._mark_token_unmeasured``), not from the
    taxonomy lane.

    A Curve *proportional* close also leaves ``token0``/``token1`` empty, so
    ``coin_symbols`` is the only identity for legs 0/1 too — exercised here.
    """
    out = _tracked(
        [
            _lp_event(
                "LP_CLOSE",
                token0="",
                token1="",
                coin_symbols=["USDC", "USDT", "DAI"],
                amount0="100",
                amount1="200",
            )
        ]
    )
    # Legs 0/1 resolved from coin_symbols and credited at the collected amounts.
    assert out["USDC"] == Decimal("100")
    assert out["USDT"] == Decimal("200")
    # The tail coin is unprovable → visible refusal, never a silent absence.
    assert out["DAI"] is None


# ---------------------------------------------------------------------------
# 3. Unattributable rows — WARN + continue, never a whole-map poison
# ---------------------------------------------------------------------------


def test_unattributable_unmeasured_event_warns_and_leaves_map_measured(caplog) -> None:
    """An UNMEASURED event naming no token cannot poison a symbol.

    Poisoning the WHOLE map on an unattributable row would refuse every swap-back
    on the deployment — a strand of its own. We take the narrower failure and make
    it loud.
    """
    ev = {
        "event_type": "PENDLE_LP_OPEN",
        "deployment_id": _DEP,
        "position_key": "",
        "chain": _CHAIN,
        "wallet_address": _WALLET,
        "timestamp": "2026-07-20T00:00:06+00:00",
        "payload_json": json.dumps({"confidence": "HIGH"}),
    }
    with caplog.at_level(logging.WARNING):
        out = _tracked([_swap_event("USDC", "10"), ev])
    assert out == {"USDC": Decimal("10")}
    assert any("VIB-5865" in r.message for r in caplog.records)


def test_unattributable_unmeasured_ledger_row_warns(caplog) -> None:
    """Same narrow-failure rule on the ledger side."""
    row = _ledger_row("PERP_OPEN", "", "")
    with caplog.at_level(logging.WARNING):
        out = _tracked([_swap_event("USDC", "10")], [row])
    assert out == {"USDC": Decimal("10")}
    assert any("VIB-5865" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# 4. Sentinels unchanged
# ---------------------------------------------------------------------------


def test_unmeasured_sentinel_and_empty_map_unchanged() -> None:
    """Empty deployment id → ``None`` (unmeasured); scoped-but-empty → ``{}`` (measured zero)."""
    assert sum_open_wallet_basis_by_token([_lp_event("LP_OPEN")], "") is None
    assert sum_open_wallet_basis_by_token([], _DEP) == {}


def test_sibling_deployment_lp_does_not_poison_ours() -> None:
    """The poison is deployment-scoped, like every other fold in this function."""
    other = _lp_event("LP_OPEN")
    other["deployment_id"] = "deployment:zzz999"
    out = _tracked([_swap_event("WETH", "5"), other])
    assert out == {"WETH": Decimal("5")}
