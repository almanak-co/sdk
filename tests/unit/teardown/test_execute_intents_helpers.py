"""Unit tests for the `_execute_intents` extraction helpers.

Blueprint 14a Stage 3 (uniform SEQUENTIAL dispatch lane + transient-retry
deferred to the tail + resume floor) and 14 §4.5 (swap-back clamp) / §6
(escalation ladder). Each helper has a direct blueprint analogue — no
architectural drift. These tests pin the extracted behaviour so the thinner
`_execute_intents` loop (CC 78 -> <=15) keeps zero behaviour change.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownPositionSummary,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.teardown_manager import TeardownManager


def _mgr() -> TeardownManager:
    mgr = TeardownManager()
    mgr.state_manager = None
    return mgr


def _state(n: int = 1) -> TeardownState:
    now = datetime.now(UTC)
    return TeardownState(
        teardown_id="teardown-test",
        deployment_id="deployment:abc123",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=n,
        completed_intents=0,
        current_intent_index=0,
        started_at=now,
        updated_at=now,
    )


def _positions() -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="deployment:abc123",
        timestamp=datetime.now(UTC),
        positions=[],
    )


def _strategy() -> MagicMock:
    s = MagicMock()
    s.deployment_id = "deployment:abc123"
    s.chain = "arbitrum"
    return s


# -- _extract_intent_slippage (14 §6 ladder floor) ---------------------------


def test_extract_slippage_dict():
    assert _mgr()._extract_intent_slippage({"max_slippage": "0.15"}) == Decimal("0.15")


def test_extract_slippage_object():
    assert _mgr()._extract_intent_slippage(SimpleNamespace(max_slippage=Decimal("0.02"))) == Decimal("0.02")


def test_extract_slippage_missing_is_none():
    assert _mgr()._extract_intent_slippage({}) is None
    assert _mgr()._extract_intent_slippage(SimpleNamespace()) is None


def test_extract_slippage_unparseable_is_none():
    assert _mgr()._extract_intent_slippage({"max_slippage": "bogus"}) is None


def _ladder_result(status="failed", error="Panic 0x11", message="boom"):
    return SimpleNamespace(
        status=status,
        attempts=[SimpleNamespace(error=error)],
        message=message,
    )


def test_transient_paused_never_defers():
    mgr = _mgr()
    res = _ladder_result(status="paused_awaiting_approval", error="Panic 0x11")
    assert mgr._transient_retry_due(res, {"intent_type": "WITHDRAW", "protocol": "morpho"}, 0) is None


def test_transient_exhausted_never_defers():
    mgr = _mgr()
    res = _ladder_result(error="Panic 0x11")
    assert mgr._transient_retry_due(res, {"intent_type": "WITHDRAW", "protocol": "morpho"}, 3) is None


def test_transient_no_text_never_defers():
    mgr = _mgr()
    res = SimpleNamespace(status="failed", attempts=[], message=None)
    assert mgr._transient_retry_due(res, {"intent_type": "X", "protocol": "Y"}, 0) is None


def test_transient_vetted_revert_defers():
    mgr = _mgr()
    with patch(
        "almanak.framework.teardown.teardown_manager.classify_revert_transience",
    ) as cls:
        from almanak.framework.teardown.revert_transience import Transience

        cls.return_value = Transience.TRANSIENT
        text = mgr._transient_retry_due(
            _ladder_result(error="0x1234"),
            {"intent_type": "WITHDRAW", "protocol": "morpho_vault"},
            0,
        )
        assert text == "0x1234"


def test_transient_terminal_never_defers():
    mgr = _mgr()
    with patch(
        "almanak.framework.teardown.teardown_manager.classify_revert_transience",
    ) as cls:
        from almanak.framework.teardown.revert_transience import Transience

        cls.return_value = Transience.UNKNOWN
        assert (
            mgr._transient_retry_due(
                _ladder_result(error="insufficient balance"),
                {"intent_type": "SWAP", "protocol": "uniswap_v3"},
                0,
            )
            is None
        )


def test_fold_success_receipt_max_and_cost():
    mgr = _mgr()
    first = SimpleNamespace(final_slippage=Decimal("0.01"))
    with patch(
        "almanak.framework.teardown.teardown_manager._fold_max_receipt_block",
        side_effect=[100, 200],
    ):
        positions = _positions()
        object.__setattr__(positions, "total_value_usd", Decimal("1000"))
        block, costs = mgr._fold_success_receipt(None, first, positions, 2, Decimal("0"))
        assert block == 100
        assert costs == Decimal("1000") / 2 * Decimal("0.01")
        block2, costs2 = mgr._fold_success_receipt(block, first, positions, 2, costs)
        assert block2 == 200


# -- _clone_intent_with_slippage (14 §6) --------------------------------------


def test_clone_passthrough_without_slippage_field():
    mgr = _mgr()
    sentinel = SimpleNamespace(foo=1)
    assert mgr._clone_intent_with_slippage(sentinel, Decimal("0.05")) is sentinel


def test_clone_model_copy_path():
    mgr = _mgr()
    intent = MagicMock()
    intent.model_copy.return_value = "cloned"
    del intent.to_dict
    del intent.from_dict
    assert mgr._clone_intent_with_slippage(intent, Decimal("0.05")) == "cloned"
    intent.model_copy.assert_called_once_with(update={"max_slippage": Decimal("0.05")})


def test_clone_unclonable_keeps_original():
    mgr = _mgr()
    intent = SimpleNamespace(max_slippage=Decimal("0.01"))
    assert mgr._clone_intent_with_slippage(intent, Decimal("0.05")) is intent


# -- _classify_intent_shape + _resolve_all_amount (14a Stage 2 live markers) ---


def test_classify_dict_withdraw():
    shape = _mgr()._classify_intent_shape({"intent_type": "WITHDRAW", "amount": "all"})
    assert shape["is_withdraw"] and not shape["is_swap"] and not shape["is_repay"]


def test_classify_object_swap():
    shape = _mgr()._classify_intent_shape(SimpleNamespace(intent_type="SWAP", amount="all"))
    assert shape["is_swap"] and not shape["is_withdraw"]


def test_resolve_skips_withdraw_and_repay():
    mgr = _mgr()
    intent = {"intent_type": "WITHDRAW", "amount": "all"}
    shape = mgr._classify_intent_shape(intent)
    out, err = mgr._resolve_all_amount(_strategy(), intent, MagicMock(), shape)
    assert err is None and out is intent


def test_resolve_missing_context_errors():
    mgr = _mgr()
    intent = {"intent_type": "SWAP", "from_token": "USDC", "amount": "all"}
    shape = mgr._classify_intent_shape(intent)
    _, err = mgr._resolve_all_amount(_strategy(), intent, None, shape)
    assert err is not None and "missing" in err


def test_resolve_zero_balance_errors():
    mgr = _mgr()
    market = MagicMock()
    market.balance.return_value = SimpleNamespace(balance=Decimal("0"))
    intent = {"intent_type": "SWAP", "from_token": "USDC", "amount": "all"}
    shape = mgr._classify_intent_shape(intent)
    _, err = mgr._resolve_all_amount(_strategy(), intent, market, shape)
    assert "nothing to teardown" in err


def test_resolve_dict_success():
    mgr = _mgr()
    market = MagicMock()
    market.balance.return_value = SimpleNamespace(balance=Decimal("7"))
    intent = {"intent_type": "SWAP", "from_token": "USDC", "amount": "all"}
    shape = mgr._classify_intent_shape(intent)
    out, err = mgr._resolve_all_amount(_strategy(), intent, market, shape)
    assert err is None and out["amount"] == "7"


def test_find_exact_cancel_hit_and_miss():
    mgr = _mgr()
    cancel = {"intent_type": "PERP_CANCEL_ORDER", "order_key": "0xABC"}
    assert mgr._find_exact_cancel_recovery([cancel], ("0xabc",)) is cancel
    assert mgr._find_exact_cancel_recovery([cancel], ("0xdead",)) is None
    assert mgr._find_exact_cancel_recovery(None, ("0xabc",)) is None


def test_decide_clamp_unmeasured_fails_closed():
    mgr = _mgr()
    decision = mgr._decide_swap_clamp(_strategy(), {}, MagicMock(), "USDC", None)
    assert decision.skip and decision.degraded and decision.reason == "live_balance_unmeasured"


# -- _save_execute_floor (14a Stage 3 resume floor) ----------------------------


@pytest.mark.asyncio
async def test_save_floor_persists_without_state_manager():
    mgr = _mgr()
    st = _state()
    await mgr._save_execute_floor(st, 3)
    assert st.completed_intents == 3 and st.current_intent_index == 3


@pytest.mark.asyncio
async def test_save_floor_calls_state_manager():
    mgr = _mgr()
    mgr.state_manager = AsyncMock()
    st = _state()
    await mgr._save_execute_floor(st, 2)
    assert st.completed_intents == 2
    mgr.state_manager.save_teardown_state.assert_awaited_once_with(st)


@pytest.mark.asyncio
async def test_process_async_not_accepted_returns_none():
    mgr = _mgr()
    assert (
        await mgr._process_async_accepted(
            _strategy(), {"intent_type": "SWAP"}, 0, 1, _state(), "teardown-x", None, lambda: 0
        )
        is None
    )


@pytest.mark.asyncio
async def test_process_async_executed_continues():
    mgr = _mgr()
    intent = {
        "intent_type": "SWAP",
        "_teardown_async_submission_accepted": True,
        "_teardown_async_submission_order_keys": ["0xabc"],
        "_teardown_async_submission_ledger_id": "ledger-1",
        "chain": "arbitrum",
    }
    mgr.runner_helpers = replace(mgr.runner_helpers, check_intent_settlement=AsyncMock(return_value="executed"))
    out = await mgr._process_async_accepted(_strategy(), intent, 0, 1, _state(), "teardown-x", None, lambda: 0)
    assert out is not None
    should_continue, _, s_inc, f_inc, unset = out
    assert (should_continue, s_inc, f_inc, unset) == (True, 1, 0, False)


@pytest.mark.asyncio
async def test_process_async_pending_without_cancel_fails():
    mgr = _mgr()
    intent = {
        "intent_type": "SWAP",
        "_teardown_async_submission_accepted": True,
        "_teardown_async_submission_order_keys": ["0xabc"],
        "_teardown_async_submission_ledger_id": "ledger-1",
        "chain": "arbitrum",
    }
    mgr.runner_helpers = replace(mgr.runner_helpers, check_intent_settlement=AsyncMock(return_value="pending"))
    out = await mgr._process_async_accepted(_strategy(), intent, 0, 1, _state(), "teardown-x", None, lambda: 0)
    assert out is not None
    should_continue, _, s_inc, f_inc, unset = out
    assert (should_continue, s_inc, f_inc, unset) == (True, 0, 1, True)


@pytest.mark.asyncio
async def test_process_async_pending_with_exact_cancel_proceeds():
    mgr = _mgr()
    intent = {
        "intent_type": "SWAP",
        "_teardown_async_submission_accepted": True,
        "_teardown_async_submission_order_keys": ["0xabc"],
        "_teardown_async_submission_ledger_id": "ledger-1",
        "chain": "arbitrum",
    }
    mgr.runner_helpers = replace(mgr.runner_helpers, check_intent_settlement=AsyncMock(return_value="pending"))
    cancel = {"intent_type": "PERP_CANCEL_ORDER", "order_key": "0xABC"}
    out = await mgr._process_async_accepted(_strategy(), intent, 0, 1, _state(), "teardown-x", [cancel], lambda: 0)
    assert out is not None
    should_continue, new_intent, _, _, unset = out
    assert should_continue is False and new_intent is cancel and unset is True


# -- _process_zero_balance_skip + _process_swap_clamp ---------------------------


@pytest.mark.asyncio
async def test_process_zero_balance_skip_hit():
    mgr = _mgr()
    with patch(
        "almanak.framework.teardown.teardown_manager._zero_balance_swap_skip_reason",
        return_value="zero USDC",
    ):
        reason = await mgr._process_zero_balance_skip({}, MagicMock(), 0, 1, 0, None)
        assert reason == "zero USDC"


@pytest.mark.asyncio
async def test_process_zero_balance_skip_miss():
    mgr = _mgr()
    with patch(
        "almanak.framework.teardown.teardown_manager._zero_balance_swap_skip_reason",
        return_value=None,
    ):
        assert await mgr._process_zero_balance_skip({}, MagicMock(), 0, 1, 0, None) is None


@pytest.mark.asyncio
async def test_process_swap_clamp_non_clampable_noop():
    mgr = _mgr()
    with patch(
        "almanak.framework.teardown.teardown_manager._clampable_swap_from_token",
        return_value=None,
    ):
        intent, done, s_inc, k_inc = await mgr._process_swap_clamp(
            _strategy(), {"x": 1}, MagicMock(), "teardown-x", 0, 1, 0, None, []
        )
        assert (done, s_inc, k_inc) == (False, 0, 0) and intent == {"x": 1}


@pytest.mark.asyncio
async def test_process_swap_clamp_skip_counts_noop_success():
    mgr = _mgr()
    with (
        patch(
            "almanak.framework.teardown.teardown_manager._clampable_swap_from_token",
            return_value="USDC",
        ),
        patch(
            "almanak.framework.teardown.teardown_manager._read_live_wallet_balance",
            return_value=None,
        ),
    ):
        degraded: list = []
        _, done, s_inc, k_inc = await mgr._process_swap_clamp(
            _strategy(), {"intent_type": "SWAP"}, MagicMock(), "teardown-x", 0, 1, 0, None, degraded
        )
        assert (done, s_inc, k_inc) == (True, 1, 1)
        assert degraded and degraded[0]["reason"] == "live_balance_unmeasured"


# -- end-to-end `_execute_intents` paths ----------------------------------------


def _exec_ok(**kw):
    base = {
        "success": True,
        "final_slippage": Decimal("0.005"),
        "total_gas_used": 21000,
        "transaction_results": [],
        "status": "success",
        "error": None,
        "approval_request": None,
        "attempts": [],
        "message": "",
    }
    base.update(kw)
    return SimpleNamespace(**base)


def _exec_fail(**kw):
    base = {
        "success": False,
        "final_slippage": Decimal("0"),
        "total_gas_used": 0,
        "transaction_results": [],
        "status": "failed",
        "error": "boom",
        "approval_request": None,
        "attempts": [SimpleNamespace(error="boom")],
        "message": "boom",
    }
    base.update(kw)
    return SimpleNamespace(**base)


def _bare_strategy() -> MagicMock:
    s = MagicMock()
    s.deployment_id = "deployment:abc123"
    s.chain = "arbitrum"
    del s._framework_record_intent_execution
    del s.on_intent_executed
    del s.save_state
    del s.flush_pending_saves
    return s


@pytest.mark.asyncio
async def test_execute_zero_balance_skip_counts_succeeded():
    mgr = _mgr()
    mgr.slippage_manager.execute_with_escalation = AsyncMock()
    with patch(
        "almanak.framework.teardown.teardown_manager._zero_balance_swap_skip_reason",
        return_value="zero USDC",
    ):
        result = await mgr._execute_intents(
            teardown_id="teardown-test",
            strategy=_bare_strategy(),
            intents=[SimpleNamespace(max_slippage=None, intent_type="SWAP")],
            positions=_positions(),
            mode=TeardownMode.SOFT,
            teardown_state=_state(1),
        )
    assert result.success and result.intents_succeeded == 1 and result.intents_skipped == 1
    mgr.slippage_manager.execute_with_escalation.assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_transient_defers_then_succeeds():
    mgr = _mgr()
    mgr.slippage_manager.execute_with_escalation = AsyncMock(side_effect=[_exec_fail(message="Panic 0x11"), _exec_ok()])
    with (
        patch(
            "almanak.framework.teardown.teardown_manager.classify_revert_transience",
        ) as cls,
        patch("asyncio.sleep", new=AsyncMock()),
    ):
        from almanak.framework.teardown.revert_transience import Transience

        cls.return_value = Transience.TRANSIENT
        result = await mgr._execute_intents(
            teardown_id="teardown-test",
            strategy=_bare_strategy(),
            intents=[SimpleNamespace(max_slippage=None, intent_type="WITHDRAW", protocol="morpho")],
            positions=_positions(),
            mode=TeardownMode.SOFT,
            teardown_state=_state(1),
        )
    assert result.success and mgr.slippage_manager.execute_with_escalation.await_count == 2


@pytest.mark.asyncio
async def test_execute_paused_returns_partial():
    mgr = _mgr()
    paused = _exec_fail(status="paused_awaiting_approval", approval_request=SimpleNamespace())
    mgr.slippage_manager.execute_with_escalation = AsyncMock(return_value=paused)
    result = await mgr._execute_intents(
        teardown_id="teardown-test",
        strategy=_bare_strategy(),
        intents=[SimpleNamespace(max_slippage=None, intent_type="SWAP")],
        positions=_positions(),
        mode=TeardownMode.SOFT,
        teardown_state=_state(1),
    )
    assert not result.success and result.error == "Paused awaiting approval"


@pytest.mark.asyncio
async def test_execute_terminal_failure_counts_failed():
    mgr = _mgr()
    mgr.slippage_manager.execute_with_escalation = AsyncMock(return_value=_exec_fail())
    with patch(
        "almanak.framework.teardown.teardown_manager.classify_revert_transience",
    ) as cls:
        from almanak.framework.teardown.revert_transience import Transience

        cls.return_value = Transience.PERMANENT
        state = _state(1)
        result = await mgr._execute_intents(
            teardown_id="teardown-test",
            strategy=_bare_strategy(),
            intents=[SimpleNamespace(max_slippage=None, intent_type="SWAP")],
            positions=_positions(),
            mode=TeardownMode.SOFT,
            teardown_state=state,
        )
    assert not result.success and result.intents_failed == 1
    assert state.status == TeardownStatus.COMPLETED


@pytest.mark.asyncio
async def test_execute_resumed_dict_uses_persisted_chain_and_wallet():
    compiler = MagicMock()
    compiler.compile.return_value = SimpleNamespace(
        status=SimpleNamespace(value="SUCCESS"),
        action_bundle=SimpleNamespace(metadata={}),
        error=None,
        is_transient=False,
        retry_after_seconds=None,
    )
    orchestrator = MagicMock()
    orchestrator.execute = AsyncMock(
        return_value=SimpleNamespace(
            success=True,
            transaction_results=[],
            total_gas_used=21_000,
            error=None,
        )
    )
    strategy = SimpleNamespace(
        deployment_id="deployment:abc123",
        chain="arbitrum",
        wallet_address="0xprimary",
        get_wallet_for_chain=MagicMock(
            side_effect=lambda chain: {"arbitrum": "0xprimary", "base": "0xbase"}.get(chain)
        ),
    )
    mgr = TeardownManager(orchestrator=orchestrator, compiler=compiler)

    result = await mgr._execute_intents(
        teardown_id="teardown-test",
        strategy=strategy,
        intents=[{"intent_type": "WITHDRAW", "chain": "base", "amount": "1"}],
        positions=_positions(),
        mode=TeardownMode.SOFT,
        teardown_state=_state(1),
    )

    assert result.success is True
    context = orchestrator.execute.await_args.args[1]
    assert context.chain == "base"
    assert context.wallet_address == "0xbase"
    strategy.get_wallet_for_chain.assert_called_with("base")
