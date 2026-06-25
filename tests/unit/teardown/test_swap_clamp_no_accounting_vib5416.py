"""VIB-5416 — clamp wiring of the NO_ACCOUNTING ledger lane (fail-closed semantics).

The NO_ACCOUNTING lane is ADDITIVE: an unmeasured/absent ledger read must drop
ONLY that lane (the token strands — safe), and must NEVER fail the whole tracked
read (which would regress accounted SWAP/BORROW/WITHDRAW/PT swap-backs).
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.teardown.swap_clamp import decide_swap_clamp, read_tracked_swap_inventory

_DEP = "deployment:abc123"
_CHAIN = "ethereum"
_WALLET = "0xWaLLeT"

_STAKE_ROW = {
    "intent_type": "STAKE",
    "token_in": "ETH",
    "amount_in": "1.0",
    "token_out": "wstETH",
    "amount_out": "0.88",
    "timestamp": 1750000000,
    "success": True,
    "id": "stake-1",
    "chain": _CHAIN,
}


class _SM:
    """Fake accounting StateManager exposing both measured readers."""

    def __init__(self, *, acct=([], True), ledger=([_STAKE_ROW], True)):
        self._acct = acct
        self._ledger = ledger

    def read_accounting_events_measured(self, _dep):
        return self._acct

    def read_ledger_entries_measured(self, _dep):
        return self._ledger


class _SMNoLedger:
    """OLD gateway / wrong-flavour manager: no ``read_ledger_entries_measured``."""

    def read_accounting_events_measured(self, _dep):
        return [], True


def _read(sm):
    return read_tracked_swap_inventory(
        state_manager=sm, deployment_id=_DEP, chain=_CHAIN, wallet_address=_WALLET
    )


def test_held_stake_is_clamped_not_stranded():
    tracked = _read(_SM())
    assert tracked == {"WSTETH": Decimal("0.88")}
    decision = decide_swap_clamp(live_balance=Decimal("0.88"), tracked_map=tracked, from_token="wstETH")
    assert decision.reason == "clamped" and not decision.skip


def test_ledger_unmeasured_drops_only_no_accounting_lane():
    # accounting measured-empty + ledger UNMEASURED → wstETH strands (untracked),
    # but the read still returns a measured {} (NOT None) so accounted swap-backs
    # would clamp normally.
    tracked = _read(_SM(ledger=([], False)))
    assert tracked == {}
    decision = decide_swap_clamp(live_balance=Decimal("0.88"), tracked_map=tracked, from_token="wstETH")
    assert decision.reason == "untracked_token" and decision.skip and not decision.degraded


def test_ledger_reader_absent_old_gateway_drops_only_no_accounting_lane():
    tracked = _read(_SMNoLedger())
    assert tracked == {}  # measured accounting read, no ledger lane


def test_accounting_unmeasured_fails_whole_read_closed():
    # The primary accounting lane being unmeasured returns None regardless of the
    # ledger lane — unchanged VIB-5185 fail-closed.
    assert _read(_SM(acct=([], False))) is None


def test_ledger_reader_raises_is_safe():
    class _Boom(_SM):
        def read_ledger_entries_measured(self, _dep):
            raise RuntimeError("ledger backend gone")

    tracked = _read(_Boom())
    assert tracked == {}  # no_accounting lane dropped, accounting lane intact


def test_accounted_swap_back_unaffected_by_ledger_outage():
    # A deployment with a real SWAP-acquired USDC lot must still clamp its USDC
    # swap-back even when the ledger backend is down.
    import json

    swap_ev = {
        "event_type": "SWAP",
        "deployment_id": _DEP,
        "position_key": "",
        "chain": _CHAIN,
        "wallet_address": _WALLET,
        "timestamp": "2026-06-25T00:00:00+00:00",
        "payload_json": json.dumps(
            {
                "swap_position_key": f"swap:{_CHAIN.lower()}:{_WALLET.lower()}",
                "token_in": "ETH",
                "amount_in": "1.0",
                "token_out": "USDC",
                "amount_out": "3000",
            }
        ),
    }
    sm = _SM(acct=([swap_ev], True), ledger=([], False))  # ledger down
    tracked = _read(sm)
    assert tracked.get("USDC") == Decimal("3000")
    decision = decide_swap_clamp(live_balance=Decimal("3000"), tracked_map=tracked, from_token="USDC")
    assert decision.reason == "clamped" and not decision.skip
