"""VIB-5471 — TD-13: the consolidation token universe reconciles to the MEASURED
wallet, not only the accounting-event FIFO.

A NO_ACCOUNTING acquisition (STAKE→wstETH, WRAP→WETH, CDP mint→stablecoin) writes
a ``transaction_ledger`` row but ZERO ``accounting_events``. Before this fix the
teardown token-consolidation universe (``derive_strategy_token_universe``) sourced
its truth from the accounting-event footprint, so a held NO_ACCOUNTING token was
invisible → never a consolidation candidate → stranded at teardown. This
generalises the VIB-5416 swap-back-clamp measured-ledger lane to the consolidation
lane via the SHARED measured-gated ledger reader.

Invariants locked:

* a NO_ACCOUNTING-acquired token (STAKE/WRAP/CDP) enters the universe,
* an UNMEASURED / absent ledger read drops the lane (strand, the safe under-sweep
  direction) — never over-selects on a shared wallet,
* a transport failure never raises (universe derivation must not block the unwind),
* the lane is additive: accounting-event / intent / profile sources are unchanged.
"""

from __future__ import annotations

from types import SimpleNamespace

from almanak.framework.teardown.consolidation import derive_strategy_token_universe

_DEP = "dep-1"


def _ledger_row(intent_type, token_in, token_out, *, success=True, _id="r"):
    return {
        "intent_type": intent_type,
        "token_in": token_in,
        "token_out": token_out,
        "amount_in": "1",
        "amount_out": "1",
        "timestamp": "2026-06-25T00:00:00+00:00",
        "success": success,
        "id": _id,
        "chain": "ethereum",
    }


def _strategy():
    return SimpleNamespace(get_teardown_profile=lambda: SimpleNamespace(natural_exit_assets=[]))


def _measured_sm(rows, *, measured=True):
    """An accounting StateManager exposing only the measured ledger reader (the
    GatewayStateManager production shape carries both; the universe lane only
    needs ``read_ledger_entries_measured``)."""
    return SimpleNamespace(read_ledger_entries_measured=lambda _dep: (rows, measured))


def test_staked_token_enters_universe_when_measured():
    """The held wstETH (acquired via a NO_ACCOUNTING STAKE) is now a consolidation
    candidate — the strand fix, generalised beyond the clamp to consolidation."""
    sm = _measured_sm([_ledger_row("STAKE", "ETH", "wstETH")])
    universe = derive_strategy_token_universe(sm, _DEP, _strategy(), [], None)
    assert "wstETH" in universe


def test_category_complete_wrap_and_cdp():
    """WRAP_NATIVE→WETH and CDP MINT_STABLE→DAI also enter — category-keyed, not
    STAKE-only."""
    rows = [
        _ledger_row("WRAP_NATIVE", "ETH", "WETH", _id="wrap"),
        _ledger_row("MINT_STABLE", "WETH", "DAI", _id="mint"),
    ]
    universe = derive_strategy_token_universe(_measured_sm(rows), _DEP, _strategy(), [], None)
    assert {"WETH", "DAI"} <= universe


def test_unmeasured_ledger_drops_lane():
    """measured=False → the NO_ACCOUNTING token is NOT added (it strands — the
    safe under-sweep direction); a shared wallet is never over-selected."""
    sm = _measured_sm([_ledger_row("STAKE", "ETH", "wstETH")], measured=False)
    universe = derive_strategy_token_universe(sm, _DEP, _strategy(), [], None)
    assert "wstETH" not in universe
    assert universe == set()


def test_old_gateway_without_reader_adds_nothing():
    """An SM lacking ``read_ledger_entries_measured`` (old gateway / wrong-flavour
    manager) contributes no NO_ACCOUNTING tokens — backward compatible."""
    sm = SimpleNamespace(get_accounting_events_sync=lambda _dep: [])
    universe = derive_strategy_token_universe(sm, _DEP, _strategy(), [], None)
    assert universe == set()


def test_ledger_reader_raises_is_safe():
    """A transport failure in the ledger read never raises — universe derivation
    must never block the unwind (degrades to a smaller universe)."""

    def _boom(_dep):
        raise RuntimeError("gateway down")

    sm = SimpleNamespace(read_ledger_entries_measured=_boom)
    closing = [SimpleNamespace(from_token="ARB", to_token=None, token=None, asset=None)]
    universe = derive_strategy_token_universe(sm, _DEP, _strategy(), closing, None)
    # Intent source still contributes; the failed ledger read added nothing.
    assert universe == {"ARB"}


def test_lane_is_additive_to_accounting_footprint():
    """The NO_ACCOUNTING ledger lane unions WITH the accounting-event footprint —
    both an accounted SWAP token and a NO_ACCOUNTING STAKE token appear."""
    sm = SimpleNamespace(
        get_accounting_events_sync=lambda _dep: [{"payload_json": '{"token_in": "USDC", "token_out": "WETH"}'}],
        read_ledger_entries_measured=lambda _dep: ([_ledger_row("STAKE", "ETH", "wstETH")], True),
    )
    universe = derive_strategy_token_universe(sm, _DEP, _strategy(), [], None)
    assert {"USDC", "WETH", "wstETH"} <= universe


def test_accounted_ledger_row_not_double_sourced():
    """A SWAP ledger row is ACCOUNTED — it must NOT enter via the NO_ACCOUNTING
    lane (it already enters via the accounting-event footprint)."""
    sm = _measured_sm([_ledger_row("SWAP", "USDC", "WETH")])
    universe = derive_strategy_token_universe(sm, _DEP, _strategy(), [], None)
    assert universe == set()


def test_none_state_manager_byte_identical():
    """The manager's no-helper fallback passes ``None`` — no ledger read, behaviour
    unchanged from before VIB-5471."""
    closing = [SimpleNamespace(from_token="WETH", to_token="USDC", token=None, asset=None)]
    universe = derive_strategy_token_universe(None, _DEP, _strategy(), closing, None)
    assert universe == {"WETH", "USDC"}
