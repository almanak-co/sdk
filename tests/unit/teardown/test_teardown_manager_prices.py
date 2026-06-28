"""Tests for TeardownManager price threading.

Validates that:
- execute() passes market to generate_teardown_intents
- execute() applies prices to compiler during intent execution
- execute() without market uses placeholder prices (graceful fallback)
- resume() applies prices when regenerating stale intents
- TypeError fallback only catches market-keyword errors, not real bugs
"""

import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.teardown.teardown_manager import TeardownManager
from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownPositionSummary,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.config import TeardownConfig


def _make_strategy(intents=None):
    """Create a mock strategy."""
    strategy = MagicMock()
    strategy.deployment_id = "test_strat"
    strategy.name = "Test Strategy"
    strategy.chain = "arbitrum"
    strategy.uses_safe_wallet = False
    strategy.pause = AsyncMock()

    positions = MagicMock(spec=TeardownPositionSummary)
    positions.positions = []
    positions.total_value_usd = Decimal("10000")
    positions.has_liquidation_risk = False
    positions.chains_involved = {"arbitrum"}
    strategy.get_open_positions.return_value = positions

    if intents is not None:
        strategy.generate_teardown_intents.return_value = intents
    else:
        strategy.generate_teardown_intents.return_value = []

    return strategy


def _make_market(prices=None):
    """Create a mock market snapshot."""
    market = MagicMock()
    market.get_price_oracle_dict.return_value = prices or {"ETH": Decimal("3400"), "USDC": Decimal("1")}
    return market


@pytest.mark.asyncio
async def test_execute_passes_market_to_generate_intents():
    """market is passed to generate_teardown_intents."""
    market = _make_market()
    strategy = _make_strategy(intents=[])

    manager = TeardownManager()

    await manager.execute(strategy=strategy, mode="graceful", market=market)

    strategy.generate_teardown_intents.assert_called_once_with(TeardownMode.SOFT, market=market)


@pytest.mark.asyncio
async def test_execute_applies_prices_to_compiler():
    """compiler.price_oracle is set from market during intent execution."""
    market = _make_market({"ETH": Decimal("3400")})

    # Build an intent mock without max_slippage attribute so slippage
    # cloning is skipped and we can focus on price application
    intent = MagicMock()
    intent.intent_type = "SWAP"
    intent.chain = "arbitrum"
    intent.to_dict.return_value = {"type": "swap"}
    del intent.max_slippage  # remove so hasattr returns False
    strategy = _make_strategy(intents=[intent])

    compiler = MagicMock()
    compiler.price_oracle = None
    compiler._using_placeholders = True

    # Make update_prices/restore_prices functional on the mock
    def _update_prices(prices):
        compiler.price_oracle = prices
        compiler._using_placeholders = False

    def _restore_prices(oracle, placeholders):
        compiler.price_oracle = oracle
        compiler._using_placeholders = placeholders

    compiler.update_prices = _update_prices
    compiler.restore_prices = _restore_prices
    # The real IntentCompiler exposes the VIB-2928 price hard-stop and the
    # teardown SWAP lane fails closed without it; MagicMock auto-attrs cannot
    # stand in (``assert*``-prefixed names are blocked), so model it as a no-op
    # pass (this test supplies real prices, so the production gate would pass).
    compiler.assert_prices_available = MagicMock(return_value=None)

    # Track prices during compile
    seen_prices = []

    def mock_compile(intent_arg):
        seen_prices.append(dict(compiler.price_oracle) if compiler.price_oracle else None)
        result = MagicMock()
        result.status.value = "SUCCESS"
        result.action_bundle = MagicMock()
        return result

    compiler.compile = mock_compile

    orchestrator = MagicMock()
    orchestrator.execute = AsyncMock(return_value=MagicMock(success=True, transaction_results=[], total_gas_used=0))

    manager = TeardownManager(
        orchestrator=orchestrator,
        compiler=compiler,
    )

    # Mock out cancel window and state persistence
    manager.cancel_window.run_cancel_window = AsyncMock(return_value=MagicMock(was_cancelled=False))
    manager.safety_guard.validate_teardown_request = MagicMock(return_value=MagicMock(all_passed=True))
    # VIB-5085: execute() calls the detailed verifier (ClosureVerification).
    from almanak.framework.teardown.models import ClosureVerification

    manager._verify_closure_detailed = AsyncMock(
        return_value=ClosureVerification(all_closed=True, positions_total=1, positions_closed=1)
    )

    await manager.execute(strategy=strategy, mode="graceful", market=market)

    # compiler should have had real prices during compile
    assert len(seen_prices) > 0
    assert seen_prices[0] == {"ETH": Decimal("3400")}

    # After execution, compiler should be restored to original state
    assert compiler.price_oracle is None
    assert compiler._using_placeholders is True


@pytest.mark.asyncio
async def test_execute_without_market_uses_placeholders():
    """Graceful fallback when no market is provided."""
    strategy = _make_strategy(intents=[])
    manager = TeardownManager()

    await manager.execute(strategy=strategy, mode="graceful")

    # Should be called without market kwarg in the fallback
    strategy.generate_teardown_intents.assert_called_once_with(TeardownMode.SOFT, market=None)


@pytest.mark.asyncio
async def test_resume_applies_prices_when_regenerating():
    """When resume() regenerates stale intents, it passes market."""
    market = _make_market()
    strategy = _make_strategy(intents=[])

    state_manager = MagicMock()
    state = TeardownState(
        teardown_id="td_123",
        deployment_id="test_strat",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=1,
        completed_intents=0,
        current_intent_index=0,
        started_at=datetime.now(UTC),
        updated_at=datetime(2020, 1, 1, tzinfo=UTC),  # very old = stale
        pending_intents_json="[]",
        cancel_window_until=datetime.now(UTC),
        config_json="{}",
    )
    state_manager.get_teardown_state = AsyncMock(return_value=state)

    config = TeardownConfig.default()
    config.staleness_threshold_seconds = 1  # 1 second = always stale

    manager = TeardownManager(state_manager=state_manager, config=config)

    await manager.resume(
        deployment_id="test_strat",
        strategy=strategy,
        market=market,
    )

    # generate_teardown_intents should have been called with market
    strategy.generate_teardown_intents.assert_called_once_with(TeardownMode.SOFT, market=market)


@pytest.mark.asyncio
async def test_execute_empty_dict_oracle_not_coerced_to_none():
    """Empty dict {} from get_price_oracle_dict() must NOT be coerced to None.

    Regression test for VIB-1408: `x or None` converts {} to None, which
    triggers $1 placeholder prices on mainnet teardowns. The fix uses
    `is not None` checks to preserve the semantic distinction.

    VIB-4842: the warm+validate seam preserves this distinction — when the
    plan has no warmable tokens (token-less intent, no declared chain), the
    fetched ``{}`` is returned verbatim, not coerced to None.
    """
    market = MagicMock()
    market.get_price_oracle_dict.return_value = {}  # empty but not None
    strategy = _make_strategy(intents=[])

    manager = TeardownManager()

    # Patch _execute_intents to capture the price_oracle arg
    captured_oracle = []
    original_execute = manager._execute_intents

    async def spy_execute(*args, **kwargs):
        captured_oracle.append(kwargs.get("price_oracle"))
        return MagicMock(success=True, results=[], error=None)

    manager._execute_intents = spy_execute
    manager.cancel_window.run_cancel_window = AsyncMock(return_value=MagicMock(was_cancelled=False))
    manager.safety_guard.validate_teardown_request = MagicMock(return_value=MagicMock(all_passed=True))

    # Token-less intent with no declared chain → no warmable tokens, so the
    # warm seam returns the fetched oracle verbatim (tests the {} passthrough,
    # not the warming behaviour, which has dedicated coverage).
    intent = MagicMock()
    intent.intent_type = "SWAP"
    intent.chain = None
    intent.to_dict.return_value = {"type": "swap"}
    del intent.from_token
    del intent.to_token
    del intent.token
    del intent.pool
    strategy.generate_teardown_intents.return_value = [intent]

    await manager.execute(strategy=strategy, mode="graceful", market=market)

    # price_oracle should be {} (empty dict), NOT None
    assert len(captured_oracle) == 1
    assert captured_oracle[0] == {}
    assert captured_oracle[0] is not None


@pytest.mark.asyncio
async def test_execute_none_oracle_stays_none():
    """get_price_oracle_dict() returning None should remain None."""
    market = MagicMock()
    market.get_price_oracle_dict.return_value = None
    strategy = _make_strategy(intents=[])

    manager = TeardownManager()

    captured_oracle = []

    async def spy_execute(*args, **kwargs):
        captured_oracle.append(kwargs.get("price_oracle"))
        return MagicMock(success=True, results=[], error=None)

    manager._execute_intents = spy_execute
    manager.cancel_window.run_cancel_window = AsyncMock(return_value=MagicMock(was_cancelled=False))
    manager.safety_guard.validate_teardown_request = MagicMock(return_value=MagicMock(all_passed=True))

    # Token-less intent with no declared chain → warm seam returns fetched
    # value (None) verbatim.
    intent = MagicMock()
    intent.intent_type = "SWAP"
    intent.chain = None
    intent.to_dict.return_value = {"type": "swap"}
    del intent.from_token
    del intent.to_token
    del intent.token
    del intent.pool
    strategy.generate_teardown_intents.return_value = [intent]

    await manager.execute(strategy=strategy, mode="graceful", market=market)

    assert len(captured_oracle) == 1
    assert captured_oracle[0] is None


@pytest.mark.asyncio
async def test_execute_typeerror_fallback_only_catches_market_keyword():
    """TypeError from real strategy bugs is NOT silently swallowed as a fallback.

    The outer exception handler wraps it in a failed result, but critically
    the code does NOT retry without market (which would hide the bug).
    """
    strategy = _make_strategy()
    # Simulate a real strategy bug: TypeError not related to 'market' keyword
    strategy.generate_teardown_intents.side_effect = TypeError("unsupported operand type(s) for +: 'int' and 'str'")

    manager = TeardownManager()

    result = await manager.execute(strategy=strategy, mode="graceful", market=_make_market())

    # Should fail with the real error, not silently fall back
    assert result.success is False
    assert "unsupported operand" in result.error
    # Should have been called only once (no silent retry without market)
    strategy.generate_teardown_intents.assert_called_once()


@pytest.mark.asyncio
async def test_execute_intents_swap_price_hardstop_is_non_retryable():
    """VIB-2928: a teardown SWAP whose price gate raises HARD-STOPS the leg.

    Directly exercises ``TeardownManager._execute_intents`` (the SWAP branch
    that calls ``compiler.assert_prices_available`` before compiling). When the
    gate raises ``ValueError`` (missing / placeholder / zero price), the leg
    must:

    - be classified **non-retryable** (``ExecutionAttempt.retryable is False``)
      so the slippage manager does NOT escalate to the operator-approval gate
      on a price gap that no slippage bump can fix;
    - short-circuit BEFORE downstream execution — neither ``compiler.compile``
      nor ``orchestrator.execute`` runs on a possibly-fake $1 price; and
    - surface as a failed leg (blueprint 14 inverted semantics: this swap
      fails, the rest of the teardown still proceeds).

    Regression guard: a ``MagicMock`` compiler auto-creates ``assert*``-prefixed
    attributes, which previously masked this gate — so the compiler here models
    the real contract (the gate is a callable that RAISES).
    """
    from almanak.framework.teardown.slippage_manager import ExecutionAttempt

    market = _make_market()
    strategy = _make_strategy()

    positions = MagicMock(spec=TeardownPositionSummary)
    positions.total_value_usd = Decimal("10000")

    state = TeardownState(
        teardown_id="td_hardstop",
        deployment_id="test_strat",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=1,
        completed_intents=0,
        current_intent_index=0,
        started_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        pending_intents_json="[]",
        cancel_window_until=datetime.now(UTC),
        config_json="{}",
    )

    # Concrete-amount SWAP (NOT amount='all') so neither the zero-balance skip
    # nor the ALM-2766 clamp fires and the leg reaches the compile-time gate.
    intent = MagicMock()
    intent.intent_type = "SWAP"
    intent.chain = "arbitrum"
    intent.from_token = "WETH"
    intent.to_token = "USDC"
    intent.amount = "1.0"
    intent.to_dict.return_value = {"type": "swap"}
    del intent.max_slippage  # skip slippage cloning

    compiler = MagicMock()
    compiler.price_oracle = None
    compiler._using_placeholders = True
    compiler.update_prices = MagicMock()
    compiler.restore_prices = MagicMock()
    compiler.compile = MagicMock()

    # The real IntentCompiler's VIB-2928 gate: raise when a token lacks a
    # real USD price. assert*-prefixed attrs cannot be auto-mocked, so wire
    # the contract explicitly — a callable that fails closed.
    def _raise_missing(tokens):
        raise ValueError(f"missing USD price for one of {tokens}")

    compiler.assert_prices_available = MagicMock(side_effect=_raise_missing)

    orchestrator = MagicMock()
    orchestrator.execute = AsyncMock()

    manager = TeardownManager(orchestrator=orchestrator, compiler=compiler)

    # Capture the ExecutionAttempt the gate produces by invoking the real
    # per-slippage closure once (what the slippage manager would do first).
    # The production manager, seeing retryable=False, stops here without
    # escalating slippage.
    captured: dict[str, ExecutionAttempt] = {}

    async def fake_escalation(*, intent, execute_func, **kwargs):
        attempt = await execute_func(intent, Decimal("0.005"))
        captured["attempt"] = attempt
        return MagicMock(success=False, status="failed", final_slippage=Decimal("0"))

    manager.slippage_manager.execute_with_escalation = fake_escalation

    result = await manager._execute_intents(
        teardown_id="td_hardstop",
        strategy=strategy,
        intents=[intent],
        positions=positions,
        mode=TeardownMode.SOFT,
        teardown_state=state,
        price_oracle={"WETH": Decimal("3000"), "USDC": Decimal("1")},
        market=market,
    )

    # Gate fired: the price hard-stop produced a non-retryable failure.
    attempt = captured["attempt"]
    assert attempt.success is False
    assert attempt.retryable is False
    assert "VIB-2928" in (attempt.error or "")

    # And it short-circuited BEFORE any downstream execution.
    compiler.assert_prices_available.assert_called_once_with(["WETH", "USDC"])
    compiler.compile.assert_not_called()
    orchestrator.execute.assert_not_called()

    # The whole leg is reported failed (teardown continues for other legs).
    assert result.success is False
    assert result.intents_failed == 1
    assert result.intents_succeeded == 0


@pytest.mark.asyncio
async def test_execute_intents_swap_hardstops_when_gate_attr_missing():
    """VIB-2928 fail-closed: a compiler that does NOT expose
    ``assert_prices_available`` must NOT compile a teardown SWAP unguarded.

    This pins the audit68 hardening: rather than silently skipping the gate
    when the attribute is absent (the MagicMock masking failure mode), the
    manager raises and marks the leg non-retryable — a swap that cannot be
    price-gated must never reach ``compiler.compile`` on a possibly-fake price.
    """
    from almanak.framework.teardown.slippage_manager import ExecutionAttempt

    market = _make_market()
    strategy = _make_strategy()

    positions = MagicMock(spec=TeardownPositionSummary)
    positions.total_value_usd = Decimal("10000")

    state = TeardownState(
        teardown_id="td_nogate",
        deployment_id="test_strat",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=1,
        completed_intents=0,
        current_intent_index=0,
        started_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        pending_intents_json="[]",
        cancel_window_until=datetime.now(UTC),
        config_json="{}",
    )

    intent = MagicMock()
    intent.intent_type = "SWAP"
    intent.chain = "arbitrum"
    intent.from_token = "WETH"
    intent.to_token = "USDC"
    intent.amount = "1.0"
    intent.to_dict.return_value = {"type": "swap"}
    del intent.max_slippage

    # Compiler WITHOUT the price-gate method (spec restricts the attr surface
    # so getattr(...assert_prices_available) is None, not an auto-mock).
    compiler = MagicMock(spec=["price_oracle", "_using_placeholders", "update_prices", "restore_prices", "compile"])
    compiler.price_oracle = None
    compiler._using_placeholders = True
    compiler.compile = MagicMock()

    orchestrator = MagicMock()
    orchestrator.execute = AsyncMock()

    manager = TeardownManager(orchestrator=orchestrator, compiler=compiler)

    captured: dict[str, ExecutionAttempt] = {}

    async def fake_escalation(*, intent, execute_func, **kwargs):
        attempt = await execute_func(intent, Decimal("0.005"))
        captured["attempt"] = attempt
        return MagicMock(success=False, status="failed", final_slippage=Decimal("0"))

    manager.slippage_manager.execute_with_escalation = fake_escalation

    result = await manager._execute_intents(
        teardown_id="td_nogate",
        strategy=strategy,
        intents=[intent],
        positions=positions,
        mode=TeardownMode.SOFT,
        teardown_state=state,
        price_oracle={"WETH": Decimal("3000"), "USDC": Decimal("1")},
        market=market,
    )

    attempt = captured["attempt"]
    assert attempt.success is False
    assert attempt.retryable is False
    assert "VIB-2928" in (attempt.error or "")
    compiler.compile.assert_not_called()
    orchestrator.execute.assert_not_called()
    assert result.success is False
    assert result.intents_failed == 1


@pytest.mark.asyncio
async def test_execute_typeerror_fallback_catches_old_signature():
    """TypeError about 'market' keyword falls back to call without it."""
    strategy = _make_strategy(intents=[])

    call_count = 0

    def fake_generate(mode, **kwargs):
        nonlocal call_count
        call_count += 1
        if "market" in kwargs:
            raise TypeError("generate_teardown_intents() got an unexpected keyword argument 'market'")
        return []

    strategy.generate_teardown_intents = fake_generate

    manager = TeardownManager()
    await manager.execute(strategy=strategy, mode="graceful", market=_make_market())

    # Should have been called twice: once with market (TypeError), once without
    assert call_count == 2
