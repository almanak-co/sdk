"""ALM-2766 — clamp teardown swap-back to the strategy's TRACKED quantity.

A default/automatic teardown's ``amount='all'`` swap-back used to resolve
against the FULL live wallet balance, sweeping commingled funds (a shared
wallet's sibling-strategy balances, or pre-existing holdings the strategy
never owned). This fix clamps the swap to
``min(Σ tracked_lot_remaining, live_balance)`` — the TRACKED quantity, never
``qty_idle`` (the untracked remainder). VIB-5938 applies the same rule to
manual CLI/dashboard consolidation; request provenance cannot bypass it.

Layers tested:
  1. ``decide_swap_clamp`` — the pure fail-closed decision table.
  2. ``basis.iter_open_wallet_basis_lots`` / ``sum_open_wallet_basis_by_token``
     — source-agnostic tracked inventory (SWAP + BORROW + WITHDRAW), deployment
     scoping, UNMEASURED sentinel.
  3. ``read_tracked_swap_inventory`` — never-raises accessor sentinels.
  4. ``_clampable_swap_from_token`` — gating (SWAP only; not WITHDRAW/REPAY).
  5. ``_execute_intents`` integration — clamp / skip end to end, including the
     ignored legacy-consent compatibility argument.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.accounting.basis import (
    FIFOBasisStore,
    canonical_symbol,
    sum_open_wallet_basis_by_token,
)
from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownPositionSummary,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.runner_helpers import TeardownRunnerHelpers
from almanak.framework.teardown.swap_clamp import (
    SwapClampDecision,
    decide_swap_clamp,
    read_tracked_swap_inventory,
)
from almanak.framework.teardown.teardown_manager import (
    TeardownManager,
    _clampable_swap_from_token,
    _read_live_wallet_balance,
    _set_intent_resolved_amount,
)

# ──────────────────────────────────────────────────────────────────────────
# 1. decide_swap_clamp — pure fail-closed decision table
# ──────────────────────────────────────────────────────────────────────────


class TestDecideSwapClamp:
    def test_tracked_below_live_clamps_to_tracked(self):
        d = decide_swap_clamp(live_balance=Decimal("100"), tracked_map={"USDC": Decimal("30")}, from_token="USDC")
        assert d == SwapClampDecision(Decimal("30"), False, False, "clamped")

    def test_tracked_above_live_clamps_to_live(self):
        d = decide_swap_clamp(live_balance=Decimal("100"), tracked_map={"USDC": Decimal("200")}, from_token="USDC")
        assert d.amount == Decimal("100")
        assert d.skip is False and d.degraded is False

    def test_tracked_equals_live_uses_that_value(self):
        d = decide_swap_clamp(live_balance=Decimal("50"), tracked_map={"USDC": Decimal("50")}, from_token="USDC")
        assert d.amount == Decimal("50")
        assert d.skip is False

    def test_unmeasured_map_skips_and_degrades(self):
        d = decide_swap_clamp(live_balance=Decimal("100"), tracked_map=None, from_token="USDC")
        assert d.skip is True and d.degraded is True
        assert d.reason == "tracked_inventory_unmeasured"
        assert d.amount is None

    def test_untracked_token_skips_without_degrading(self):
        d = decide_swap_clamp(live_balance=Decimal("100"), tracked_map={"WETH": Decimal("5")}, from_token="USDC")
        assert d.skip is True and d.degraded is False
        assert d.reason == "untracked_token"

    def test_per_token_none_skips_and_degrades(self):
        # Empty != Zero: an explicitly-unmeasured per-token quantity must NOT
        # be coerced to 0 — fail closed.
        d = decide_swap_clamp(live_balance=Decimal("100"), tracked_map={"USDC": None}, from_token="USDC")
        assert d.skip is True and d.degraded is True
        assert d.reason == "tracked_qty_unmeasured"

    def test_measured_zero_tracked_skips_without_degrading(self):
        d = decide_swap_clamp(live_balance=Decimal("100"), tracked_map={"USDC": Decimal("0")}, from_token="USDC")
        assert d.skip is True and d.degraded is False
        assert d.reason == "zero_tracked"

    def test_canonicalization_is_case_insensitive(self):
        # Map keyed by canonical (upper) symbol; lookup folds the from_token.
        d = decide_swap_clamp(live_balance=Decimal("100"), tracked_map={"USDC.E": Decimal("7")}, from_token="usdc.e")
        assert d.amount == Decimal("7") and d.skip is False

    def test_live_below_tracked_and_zero_live_skips(self):
        d = decide_swap_clamp(live_balance=Decimal("0"), tracked_map={"USDC": Decimal("30")}, from_token="USDC")
        assert d.skip is True and d.reason == "zero_tracked"

    def test_non_finite_live_balance_skips_and_degrades(self):
        # CR#1: NaN / ±Infinity live balance is UNMEASURED — min()/<=0 are
        # undefined on it, so fail closed (skip + degraded), never sweep.
        for bad in (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")):
            d = decide_swap_clamp(live_balance=bad, tracked_map={"USDC": Decimal("5")}, from_token="USDC")
            assert d.skip is True and d.degraded is True
            assert d.reason == "live_balance_unmeasured"

    def test_non_finite_tracked_qty_skips_and_degrades(self):
        # CR#1: a non-finite tracked qty fails closed for THIS token.
        d = decide_swap_clamp(live_balance=Decimal("100"), tracked_map={"USDC": Decimal("Infinity")}, from_token="USDC")
        assert d.skip is True and d.degraded is True
        assert d.reason == "tracked_qty_unmeasured"


# ──────────────────────────────────────────────────────────────────────────
# 2. basis — source-agnostic tracked inventory + deployment scoping
# ──────────────────────────────────────────────────────────────────────────

_DEP = "deployment:abc123"
_OTHER = "deployment:zzz999"
_SWAPKEY = "swap:arbitrum:0x1111111111111111111111111111111111111111"


def _swap_event(deployment_id, token_out, amount_out, *, token_in="WETH", amount_in="0"):
    return {
        "event_type": "SWAP",
        "deployment_id": deployment_id,
        "position_key": "",
        "chain": "arbitrum",
        "wallet_address": "0x1111111111111111111111111111111111111111",
        "timestamp": "2026-06-17T00:00:00+00:00",
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


def _borrow_event(deployment_id, asset, amount):
    return {
        "event_type": "BORROW",
        "deployment_id": deployment_id,
        "position_key": "aave:arbitrum:0x1111111111111111111111111111111111111111",
        "chain": "arbitrum",
        "wallet_address": "0x1111111111111111111111111111111111111111",
        "timestamp": "2026-06-17T00:00:01+00:00",
        "payload_json": json.dumps({"asset": asset, "amount_token": amount, "amount_usd": "1"}),
    }


def _withdraw_event(deployment_id, asset, amount):
    return {
        "event_type": "WITHDRAW",
        "deployment_id": deployment_id,
        "position_key": "aave:arbitrum:0x1111111111111111111111111111111111111111",
        "chain": "arbitrum",
        "wallet_address": "0x1111111111111111111111111111111111111111",
        "timestamp": "2026-06-17T00:00:02+00:00",
        "payload_json": json.dumps({"asset": asset, "amount_token": amount, "amount_usd": "1"}),
    }


class TestSumOpenWalletBasisByToken:
    def test_empty_deployment_id_returns_unmeasured_sentinel(self):
        # Empty deployment id → None (unmeasured), NOT {} (Empty != Zero).
        assert sum_open_wallet_basis_by_token([_swap_event(_DEP, "USDC", "10")], "") is None

    def test_scoped_but_no_events_returns_empty_dict(self):
        # Measured: this deployment has no tracked wallet inventory.
        assert sum_open_wallet_basis_by_token([], _DEP) == {}

    def test_sums_swap_acquisition(self):
        out = sum_open_wallet_basis_by_token([_swap_event(_DEP, "USDC", "42")], _DEP)
        assert out == {"USDC": Decimal("42")}

    def test_swap_acquired_yt_tracked_under_maturity_less_key_and_clamps(self):
        # VIB-5413: a YT bought via a SWAP intent is a plain wallet-basis lot
        # (NOT a separate PT lane). Once the receipt parser stores the FULL
        # maturity-bearing symbol on the ledger row, the lot surfaces under the
        # maturity-LESS canonical key, so the maturity-bearing teardown
        # ``from_token`` matches it → ``clamped`` instead of ``untracked_token``
        # (the strand). End-to-end mirror of the PT path (VIB-5353).
        out = sum_open_wallet_basis_by_token([_swap_event(_DEP, "YT-wstETH-25JUN2026", "16.82")], _DEP)
        assert out == {"YT-WSTETH": Decimal("16.82")}
        decision = decide_swap_clamp(
            live_balance=Decimal("16.82"),
            tracked_map=out,
            from_token="YT-wstETH-25JUN2026",
        )
        assert decision.reason == "clamped"
        assert decision.skip is False
        assert decision.amount == Decimal("16.82")

    def test_counts_borrow_and_withdraw_sourced_lots(self):
        # ALM-2766: borrowed/withdrawn-then-held tokens ARE tracked wallet
        # inventory — a looping teardown's swap-back must not strand them.
        events = [
            _swap_event(_DEP, "USDC", "10"),
            _borrow_event(_DEP, "USDT", "20"),
            _withdraw_event(_DEP, "DAI", "30"),
        ]
        out = sum_open_wallet_basis_by_token(events, _DEP)
        assert out == {"USDC": Decimal("10"), "USDT": Decimal("20"), "DAI": Decimal("30")}

    def test_deployment_scoped_excludes_sibling_lots(self):
        events = [
            _swap_event(_DEP, "USDC", "10"),
            _swap_event(_OTHER, "USDC", "999"),  # sibling on the shared wallet
        ]
        out = sum_open_wallet_basis_by_token(events, _DEP)
        assert out == {"USDC": Decimal("10")}

    def test_supply_disposes_wallet_inventory(self):
        # A SUPPLY of the borrowed token removes it from wallet inventory
        # (match_swap_disposal), so it should no longer be tracked.
        events = [
            _borrow_event(_DEP, "USDT", "20"),
            {
                "event_type": "SUPPLY",
                "deployment_id": _DEP,
                "position_key": "aave:arbitrum:0x1111111111111111111111111111111111111111",
                "chain": "arbitrum",
                "wallet_address": "0x1111111111111111111111111111111111111111",
                "timestamp": "2026-06-17T00:00:03+00:00",
                "payload_json": '{"asset": "USDT", "amount_token": "20", "amount_usd": "1"}',
            },
        ]
        out = sum_open_wallet_basis_by_token(events, _DEP)
        assert out.get("USDT", Decimal("0")) == Decimal("0")


class TestIterOpenWalletBasisLots:
    def test_includes_non_swap_sources_unlike_swap_only_iter(self):
        store = FIFOBasisStore()
        store.record_swap_acquisition(_DEP, _SWAPKEY, "USDC", Decimal("10"), cost_usd=Decimal("10"))
        store.record_swap_acquisition(_DEP, _SWAPKEY, "USDT", Decimal("20"), cost_usd=Decimal("20"), source="BORROW")
        store.record_swap_acquisition(_DEP, _SWAPKEY, "DAI", Decimal("30"), cost_usd=Decimal("30"), source="WITHDRAW")

        wallet = {canonical_symbol(t): r for _pk, t, r, _c in store.iter_open_wallet_basis_lots()}
        assert wallet == {"USDC": Decimal("10"), "USDT": Decimal("20"), "DAI": Decimal("30")}

        # iter_open_swap_lots is SWAP-only by design (VIB-4984).
        swap_only = {canonical_symbol(t): r for _pk, t, r, _c in store.iter_open_swap_lots()}
        assert swap_only == {"USDC": Decimal("10")}

    def test_excludes_supply_keyed_lots(self):
        store = FIFOBasisStore()
        store.record_swap_acquisition(_DEP, _SWAPKEY, "USDC", Decimal("10"))
        # A supply: principal lot is NOT wallet inventory.
        store.record_borrow(_DEP, "supply:aave:arbitrum:x", "USDC", Decimal("100"))
        # Raw token from the composite key is lowercased (same as
        # iter_open_swap_lots); the summer canonicalizes.
        tokens = [canonical_symbol(t) for _pk, t, _r, _c in store.iter_open_wallet_basis_lots()]
        assert tokens == ["USDC"]
        amounts = [r for _pk, _t, r, _c in store.iter_open_wallet_basis_lots()]
        assert amounts == [Decimal("10")]


# ──────────────────────────────────────────────────────────────────────────
# 3. read_tracked_swap_inventory — never-raises accessor sentinels
# ──────────────────────────────────────────────────────────────────────────


def _probe_sm() -> MagicMock:
    """A StateManager-flavoured mock for the VIB-5173 *fallback* path.

    The local ``StateManager`` exposes ``has_accounting_event_backend`` +
    ``get_accounting_events_sync`` but NOT the VIB-5185 per-read measured
    signal, so neutralise ``read_accounting_events_measured`` (a bare MagicMock
    would auto-vivify it as a callable and spuriously take the preferred path).
    """
    sm = MagicMock()
    sm.read_accounting_events_measured = None
    return sm


class TestReadTrackedSwapInventory:
    """VIB-5173 fallback path — local ``StateManager`` structural probe."""

    def test_none_state_manager_returns_sentinel(self):
        assert read_tracked_swap_inventory(state_manager=None, deployment_id=_DEP) is None

    def test_empty_deployment_returns_sentinel(self):
        sm = _probe_sm()
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id="") is None

    def test_state_manager_without_method_returns_sentinel(self):
        sm = SimpleNamespace()  # no get_accounting_events_sync, no measured reader
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id=_DEP) is None

    def test_exception_returns_sentinel(self):
        sm = _probe_sm()
        sm.get_accounting_events_sync.side_effect = RuntimeError("db locked")
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id=_DEP) is None

    def test_success_returns_tracked_map(self):
        sm = _probe_sm()
        sm.has_accounting_event_backend.return_value = True
        sm.get_accounting_events_sync.return_value = [_swap_event(_DEP, "USDC", "12")]
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id=_DEP) == {"USDC": Decimal("12")}

    # ── VIB-5173: Empty ≠ Zero at the accounting-backend boundary ──────────
    # The three cases ``get_accounting_events_sync`` collapses into ``[]``
    # must NOT all map to the same clamp outcome.

    def test_absent_backend_returns_unmeasured_sentinel_not_measured_zero(self):
        # CASE 1 — backend structurally absent (probe False). Even though the
        # shared read would return [] (→ {} measured-zero), the probe short-
        # circuits to the UNMEASURED sentinel so the clamp fails closed + flags
        # accounting_degraded instead of silently under-sweeping.
        sm = _probe_sm()
        sm.has_accounting_event_backend.return_value = False
        sm.get_accounting_events_sync.return_value = []  # would be measured-zero
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id=_DEP) is None
        sm.get_accounting_events_sync.assert_not_called()  # never read past the probe

    def test_read_raising_fails_closed_via_wrapper_guard(self):
        # CASE 2 — the read path raises. This exercises read_tracked_swap_inventory's
        # OWN defensive try/except (the wrapper guard) on the fallback path: the
        # local StateManager swallows read exceptions internally and returns []
        # (measured-zero), so a live backend would yield {} here. The wrapper guard
        # still fails closed to the UNMEASURED sentinel if anything in the
        # read/replay does raise.
        sm = _probe_sm()
        sm.has_accounting_event_backend.return_value = True
        sm.get_accounting_events_sync.side_effect = RuntimeError("db locked")
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id=_DEP) is None

    def test_present_backend_genuinely_empty_returns_measured_zero(self):
        # CASE 3 — backend present (probe True) and genuinely no events for the
        # deployment → measured zero {} (NOT the sentinel). The clamp then
        # treats the from-token as untracked (skip, NOT degraded).
        sm = _probe_sm()
        sm.has_accounting_event_backend.return_value = True
        sm.get_accounting_events_sync.return_value = []
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id=_DEP) == {}

    def test_probe_raising_fails_closed_to_sentinel(self):
        # A probe that itself raises is unmeasured — never block the unwind.
        sm = _probe_sm()
        sm.has_accounting_event_backend.side_effect = RuntimeError("boom")
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id=_DEP) is None

    def test_backcompat_no_probe_no_measured_reader_reads_when_method_present(self):
        # A backend with NEITHER the probe NOR the measured reader (a minimal
        # legacy manager) keeps the prior behaviour — reads when it can supply
        # events.
        sm = SimpleNamespace(
            get_accounting_events_sync=lambda deployment_id, position_key=None: [_swap_event(_DEP, "USDC", "5")]
        )
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id=_DEP) == {"USDC": Decimal("5")}


class TestReadTrackedSwapInventoryMeasuredReader:
    """VIB-5185 production path — ``GatewayStateManager.read_accounting_events_measured``.

    This is the only path that fires on a real ``strat run`` (the runner's
    state_manager is always ``GatewayStateManager``). The ``(events, measured)``
    tuple carries the gateway ``backend_status`` proto signal so absent / errored
    / empty are no longer collapsed into one clamp outcome.
    """

    @staticmethod
    def _reader_sm(result):
        # SimpleNamespace so only the measured reader is present — no
        # auto-vivified has_accounting_event_backend / get_accounting_events_sync.
        return SimpleNamespace(read_accounting_events_measured=lambda deployment_id, position_key=None: result)

    def test_available_nonempty_returns_tracked_map(self):
        # measured=True with events → real tracked inventory.
        sm = self._reader_sm(([_swap_event(_DEP, "USDC", "7")], True))
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id=_DEP) == {"USDC": Decimal("7")}

    def test_available_empty_returns_measured_zero(self):
        # CASE 3 (gateway) — AVAILABLE + genuinely no events → measured zero {},
        # NOT the sentinel. Clamp treats the from-token as untracked (skip, not
        # degraded).
        sm = self._reader_sm(([], True))
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id=_DEP) == {}

    def test_absent_backend_unmeasured_returns_sentinel(self):
        # CASE 1 (gateway) — ABSENT (e.g. hosted pre-metrics-database migration)
        # surfaces as measured=False with []. Empty ≠ Zero → UNMEASURED sentinel,
        # NOT measured-zero. Fails closed + flags accounting_degraded.
        sm = self._reader_sm(([], False))
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id=_DEP) is None

    def test_errored_read_unmeasured_returns_sentinel(self):
        # CASE 2 (gateway) — present-but-errored read surfaces as measured=False
        # (gateway reported ERRORED). The pre-VIB-5185 code collapsed this into []
        # and treated it as measured-zero; now it is the UNMEASURED sentinel.
        sm = self._reader_sm(([], False))
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id=_DEP) is None

    def test_reader_raising_fails_closed_to_sentinel(self):
        # A measured reader that itself raises is unmeasured — never block unwind.
        def _boom(deployment_id, position_key=None):
            raise RuntimeError("rpc gone")

        sm = SimpleNamespace(read_accounting_events_measured=_boom)
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id=_DEP) is None

    def test_measured_reader_preferred_over_probe(self):
        # When BOTH surfaces exist, the per-read measured signal wins (it also
        # catches the errored case a pre-read structural probe cannot).
        sm = MagicMock()
        sm.read_accounting_events_measured.return_value = ([], False)
        sm.has_accounting_event_backend.return_value = True  # would say "present"
        sm.get_accounting_events_sync.return_value = []  # would be measured-zero
        assert read_tracked_swap_inventory(state_manager=sm, deployment_id=_DEP) is None
        sm.get_accounting_events_sync.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────
# 4. _clampable_swap_from_token — gating (SWAP only)
# ──────────────────────────────────────────────────────────────────────────


class TestClampableSwapFromToken:
    def _market(self):
        return MagicMock()

    def test_swap_all_is_clampable(self):
        intent = SimpleNamespace(intent_type="SWAP", amount="all", from_token="USDC")
        assert _clampable_swap_from_token(intent, self._market()) == "USDC"

    def test_dict_swap_all_is_clampable(self):
        intent = {"intent_type": "SWAP", "amount": "all", "from_token": "USDC"}
        assert _clampable_swap_from_token(intent, self._market()) == "USDC"

    def test_withdraw_not_clampable(self):
        intent = SimpleNamespace(intent_type="WITHDRAW", amount="all", token="aUSDC")
        assert _clampable_swap_from_token(intent, self._market()) is None

    def test_repay_not_clampable(self):
        intent = SimpleNamespace(intent_type="REPAY", amount="all", token="USDC")
        assert _clampable_swap_from_token(intent, self._market()) is None

    def test_withdraw_all_flag_not_clampable(self):
        intent = SimpleNamespace(intent_type="SWAP", amount="all", from_token="USDC", withdraw_all=True)
        assert _clampable_swap_from_token(intent, self._market()) is None

    def test_explicit_amount_not_clampable(self):
        intent = SimpleNamespace(intent_type="SWAP", amount=Decimal("1"), from_token="USDC")
        assert _clampable_swap_from_token(intent, self._market()) is None

    def test_none_market_not_clampable(self):
        intent = SimpleNamespace(intent_type="SWAP", amount="all", from_token="USDC")
        assert _clampable_swap_from_token(intent, None) is None


class TestSetIntentResolvedAmount:
    def test_dict_intent_sets_string_amount(self):
        out = _set_intent_resolved_amount({"amount": "all", "from_token": "USDC"}, Decimal("30"))
        assert out["amount"] == "30" and out["from_token"] == "USDC"


class TestReadLiveWalletBalance:
    def test_reads_and_coerces_to_decimal(self):
        market = MagicMock()
        market.balance.return_value = SimpleNamespace(balance=Decimal("1.5"))
        assert _read_live_wallet_balance(market, "USDC") == Decimal("1.5")
        market.invalidate_balance.assert_called_once_with("USDC")

    def test_read_failure_returns_none(self):
        market = MagicMock()
        market.balance.side_effect = RuntimeError("not registered")
        assert _read_live_wallet_balance(market, "USDC") is None


# ──────────────────────────────────────────────────────────────────────────
# 5. _execute_intents integration — clamp / skip / consent end to end
# ──────────────────────────────────────────────────────────────────────────


def _exec_success():
    return SimpleNamespace(
        success=True,
        final_slippage=Decimal("0.005"),
        total_gas_used=21000,
        transaction_results=[],
        status="success",
        error=None,
        approval_request=None,
    )


def _state():
    now = datetime.now(UTC)
    return TeardownState(
        teardown_id="teardown-test",
        deployment_id=_DEP,
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=1,
        completed_intents=0,
        current_intent_index=0,
        started_at=now,
        updated_at=now,
    )


def _positions():
    return TeardownPositionSummary(deployment_id=_DEP, timestamp=datetime.now(UTC), positions=[])


def _strategy():
    s = MagicMock()
    s.deployment_id = _DEP
    s.name = "Test"
    s.chain = "arbitrum"
    del s._framework_record_intent_execution
    del s.on_intent_executed
    del s.save_state
    del s.flush_pending_saves
    return s


def _market(balance: Decimal):
    market = MagicMock()
    market.balance.return_value = SimpleNamespace(balance=balance)
    return market


async def _run_clamp(*, tracked_map, live=Decimal("100"), consent=False, intent=None):
    """Drive ``_execute_intents`` with a single SWAP-all intent. Returns
    ``(result, escalation_mock, inventory_mock)``.
    """
    mgr = TeardownManager()
    mgr.state_manager = None
    escalation = AsyncMock(return_value=_exec_success())
    mgr.slippage_manager.execute_with_escalation = escalation

    inventory = MagicMock(return_value=tracked_map)
    mgr.runner_helpers = TeardownRunnerHelpers(get_tracked_swap_inventory=inventory)

    if intent is None:
        intent = {"intent_type": "SWAP", "amount": "all", "from_token": "USDC", "max_slippage": None}

    result = await mgr._execute_intents(
        teardown_id="teardown-test",
        strategy=_strategy(),
        intents=[intent],
        positions=_positions(),
        mode=TeardownMode.SOFT,
        teardown_state=_state(),
        market=_market(live),
        consolidation_consent=consent,
    )
    return result, escalation, inventory


@pytest.mark.asyncio
async def test_clamp_tracked_below_live_executes_clamped_amount():
    result, escalation, _ = await _run_clamp(tracked_map={"USDC": Decimal("30")}, live=Decimal("100"))
    assert escalation.call_count == 1
    sent_intent = escalation.call_args.kwargs["intent"]
    assert sent_intent["amount"] == "30"  # clamped to tracked, NOT the full 100
    assert result.intents_succeeded == 1 and result.intents_failed == 0


@pytest.mark.asyncio
async def test_clamp_tracked_above_live_executes_live_amount():
    result, escalation, _ = await _run_clamp(tracked_map={"USDC": Decimal("200")}, live=Decimal("100"))
    sent_intent = escalation.call_args.kwargs["intent"]
    assert sent_intent["amount"] == "100"  # clamped to live balance
    assert result.intents_succeeded == 1


@pytest.mark.asyncio
async def test_retired_consent_argument_cannot_skip_clamp():
    # VIB-5938: the compatibility argument is ignored; tracked-only remains.
    result, escalation, inventory = await _run_clamp(
        tracked_map={"USDC": Decimal("30")}, live=Decimal("100"), consent=True
    )
    sent_intent = escalation.call_args.kwargs["intent"]
    assert sent_intent["amount"] == "30"
    inventory.assert_called_once()
    assert result.intents_succeeded == 1


@pytest.mark.asyncio
async def test_untracked_token_skips_swap_as_noop_success():
    result, escalation, _ = await _run_clamp(tracked_map={"WETH": Decimal("5")}, live=Decimal("100"))
    assert escalation.call_count == 0  # swap NOT executed
    assert result.intents_succeeded == 1 and result.intents_failed == 0
    assert result.accounting_degraded is False  # untracked is clean, not degraded


@pytest.mark.asyncio
async def test_unmeasured_inventory_skips_and_flags_degraded():
    result, escalation, _ = await _run_clamp(tracked_map=None, live=Decimal("100"))
    assert escalation.call_count == 0
    assert result.intents_succeeded == 1 and result.intents_failed == 0
    assert result.accounting_degraded is True


@pytest.mark.asyncio
async def test_measured_zero_tracked_skips_without_degraded():
    result, escalation, _ = await _run_clamp(tracked_map={"USDC": Decimal("0")}, live=Decimal("100"))
    assert escalation.call_count == 0
    assert result.intents_succeeded == 1
    assert result.accounting_degraded is False


@pytest.mark.asyncio
async def test_withdraw_intent_is_not_clamped():
    # A WITHDRAW resolves "all" against the protocol balance, not the wallet —
    # the clamp must not touch it and must not even read tracked inventory.
    intent = {"intent_type": "WITHDRAW", "amount": "all", "token": "aUSDC", "max_slippage": None}
    result, escalation, inventory = await _run_clamp(
        tracked_map={"USDC": Decimal("1")}, live=Decimal("100"), intent=intent
    )
    assert escalation.call_count == 1
    inventory.assert_not_called()
    sent_intent = escalation.call_args.kwargs["intent"]
    assert sent_intent["amount"] == "all"  # left for the compiler's protocol resolver
    assert result.intents_succeeded == 1


# ──────────────────────────────────────────────────────────────────────────
# 6. run_token_consolidation — clamp stays on for every request provenance
#
# The VIB-5011 consolidation phase runs on automatic teardowns too (risk-guard
# / auto-protect / config-reload carry a non-None request → consolidation
# enabled → SOFT mode). Manual CLI/dashboard provenance is also not informed
# authorization to consume untracked funds. A planted commingled balance must
# not be swept in either lane.
# ──────────────────────────────────────────────────────────────────────────


def _consolidation_market(balance=Decimal("1"), price=Decimal("3000")):
    """Market double for the planner + clamp: WETH residual above the dust floor."""
    market = MagicMock()

    def _balance(token, chain=None):
        return SimpleNamespace(balance=balance)

    def _price(token, chain=None):
        return price

    market.balance.side_effect = _balance
    market.price.side_effect = _price
    return market


async def _run_consolidation(*, is_auto_mode, tracked_map):
    """Drive run_token_consolidation with a single planted commingled WETH
    residual (NOT in the deployment's tracked inventory). Returns
    (outcome, escalation_mock, inventory_mock).
    """
    from almanak.framework.teardown.config import TeardownConfig

    mgr = TeardownManager(config=TeardownConfig.default())
    mgr.state_manager = None
    escalation = AsyncMock(return_value=_exec_success())
    mgr.slippage_manager.execute_with_escalation = escalation

    inventory = MagicMock(return_value=tracked_map)
    mgr.runner_helpers = TeardownRunnerHelpers(
        get_token_universe=MagicMock(return_value={"WETH"}),
        get_tracked_swap_inventory=inventory,
    )

    outcome = await mgr.run_token_consolidation(
        _strategy(),
        teardown_id="teardown-test",
        teardown_state=_state(),
        mode=TeardownMode.SOFT,
        market=_consolidation_market(),
        positions=_positions(),
        closing_intents=[],
        is_auto_mode=is_auto_mode,
    )
    return outcome, escalation, inventory


@pytest.mark.asyncio
async def test_automatic_teardown_consolidation_does_not_sweep_commingled():
    # is_auto_mode=True → consent=False → clamp ON. WETH is commingled
    # (tracked_map empty) → swap SKIPPED, never reaches execution.
    outcome, escalation, inventory = await _run_consolidation(is_auto_mode=True, tracked_map={})
    assert outcome.planned == 1  # planner DID plan the WETH consolidation swap
    assert escalation.call_count == 0  # ...but the clamp skipped it — NOT swept
    inventory.assert_called()  # clamp engaged (consent disabled)


@pytest.mark.asyncio
async def test_manual_teardown_consolidation_does_not_sweep_commingled():
    # is_auto_mode=False (CLI/dashboard) is request provenance, not consent.
    outcome, escalation, inventory = await _run_consolidation(is_auto_mode=False, tracked_map={})
    assert outcome.planned == 1
    assert escalation.call_count == 0
    inventory.assert_called()


# ──────────────────────────────────────────────────────────────────────────
# 7. _warm_oracle_risk_first — CR#3: clamp-skippable swap-backs must NOT
#    fail-loud the pre-flight oracle warm (they'd block risk-reducing intents).
# ──────────────────────────────────────────────────────────────────────────


class TestWarmOracleRiskFirst:
    def _market(self):
        market = MagicMock()
        market.balance.return_value = SimpleNamespace(balance=Decimal("1"))
        return market

    def test_swap_backs_excluded_from_fail_loud_warm(self, monkeypatch):
        from almanak.framework.teardown import teardown_manager as tm

        captured: dict = {"best_effort": []}

        def _fake_fail_loud(market, intents, chain):
            captured["fail_loud"] = list(intents)
            return {"PRICED": True}

        def _fake_best_effort(market, intents, chain):
            captured["best_effort"].extend(intents)
            return {}

        monkeypatch.setattr(tm, "warm_and_validate_oracle", _fake_fail_loud)
        monkeypatch.setattr(tm, "_warm_oracle_best_effort", _fake_best_effort)

        withdraw = {"intent_type": "WITHDRAW", "amount": "all", "token": "aUSDC", "chain": "arbitrum"}
        swap_back = {"intent_type": "SWAP", "amount": "all", "from_token": "WETH", "chain": "arbitrum"}

        oracle = tm._warm_oracle_risk_first(self._market(), [withdraw, swap_back], fail_loud=True)

        # The risk-reducing WITHDRAW is warmed FAIL-LOUD; the clampable swap-back
        # is warmed BEST-EFFORT only — so an unpriceable swap-back can never raise
        # and block the WITHDRAW.
        assert withdraw in captured["fail_loud"]
        assert swap_back not in captured["fail_loud"]
        assert swap_back in captured["best_effort"]
        assert oracle == {"PRICED": True}

    def test_resume_lane_never_fail_loud(self, monkeypatch):
        from almanak.framework.teardown import teardown_manager as tm

        called = {"fail_loud": 0, "best_effort": 0}

        def _fake_fail_loud(market, intents, chain):
            called["fail_loud"] += 1
            return {}

        def _fake_best_effort(market, intents, chain):
            called["best_effort"] += 1
            return {}

        monkeypatch.setattr(tm, "warm_and_validate_oracle", _fake_fail_loud)
        monkeypatch.setattr(tm, "_warm_oracle_best_effort", _fake_best_effort)

        withdraw = {"intent_type": "WITHDRAW", "amount": "all", "token": "aUSDC", "chain": "arbitrum"}
        tm._warm_oracle_risk_first(self._market(), [withdraw], fail_loud=False)

        # fail_loud=False (resume-past-progress) never invokes the raising warm.
        assert called["fail_loud"] == 0
        assert called["best_effort"] >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
