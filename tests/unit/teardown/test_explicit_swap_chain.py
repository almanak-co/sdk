"""Explicit swap chains select inventory and do not block independent exits."""

import ast
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.cli.new_strategy import TEMPLATE_CONFIGS, StrategyTemplate, _get_template_teardown
from almanak.framework.teardown.chain_validation import teardown_swap_chain_error
from almanak.framework.teardown.models import TeardownMode
from almanak.framework.teardown.runner_helpers import TeardownRunnerHelpers
from almanak.framework.teardown.swap_clamp import read_tracked_swap_inventory
from almanak.framework.teardown.teardown_manager import TeardownManager
from tests.unit.teardown.test_teardown_swap_clamp import (
    _DEP,
    _exec_success,
    _market,
    _positions,
    _state,
    _strategy,
    _swap_event,
)


@pytest.mark.parametrize(
    "chain,phrase", [(None, "explicit chain"), ("not-a-chain", "unsupported"), ("base", "conflicts")]
)
def test_invalid_chain_is_diagnostic(chain, phrase):
    assert phrase in teardown_swap_chain_error({"intent_type": "SWAP", "chain": chain}, _strategy(), None)


def test_multichain_uses_explicit_position_chain():
    market = SimpleNamespace(chains=("arbitrum", "base"))
    assert teardown_swap_chain_error({"intent_type": "SWAP", "chain": "base"}, _strategy(), market) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("discriminator", ["intent_type", "type"])
async def test_missing_chain_fails_before_balance_read_and_valid_address_exit_continues(discriminator):
    manager = TeardownManager()
    manager.state_manager = None
    manager.slippage_manager.execute_with_escalation = AsyncMock(return_value=_exec_success())
    inventory = MagicMock(return_value={"WETH": Decimal("0.1")})
    manager.runner_helpers = TeardownRunnerHelpers(get_tracked_swap_inventory=inventory)
    market = _market(Decimal("2"))
    address = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
    invalid = {discriminator: "SWAP", "from_token": address, "amount": "all"}
    valid = {**invalid, "chain": "arbitrum"}
    result = await manager._execute_intents(
        teardown_id="teardown-test",
        strategy=_strategy(),
        intents=[invalid, valid],
        positions=_positions(),
        mode=TeardownMode.SOFT,
        teardown_state=_state(),
        market=market,
    )
    assert result.intents_failed == 1
    assert result.intents_succeeded == 1
    sent = manager.slippage_manager.execute_with_escalation.call_args.kwargs["intent"]
    assert sent["amount"] == "0.1"
    assert sent["chain"] == "arbitrum"
    assert inventory.call_args.kwargs["chain"] == "arbitrum"
    assert all(call.kwargs["chain"] == "arbitrum" for call in market.balance.call_args_list)


def test_inventory_does_not_mix_same_symbol_on_other_chain():
    arbitrum = _swap_event(_DEP, "USDC", "7")
    base = {**_swap_event(_DEP, "USDC", "90"), "chain": "base"}
    state = SimpleNamespace(read_accounting_events_measured=lambda *_: ([arbitrum, base], True))
    assert read_tracked_swap_inventory(state_manager=state, deployment_id=_DEP, scope_chain="arbitrum") == {
        "USDC": Decimal("7")
    }
    unknown = {**arbitrum, "chain": ""}
    state.read_accounting_events_measured = lambda *_: ([unknown], True)
    assert read_tracked_swap_inventory(state_manager=state, deployment_id=_DEP, scope_chain="arbitrum") is None


@pytest.mark.parametrize("template", list(StrategyTemplate))
def test_generated_teardown_swaps_all_supply_chain(template):
    import textwrap

    code = _get_template_teardown(template, TEMPLATE_CONFIGS[template], "test")
    tree = ast.parse(textwrap.dedent(code))
    for call in ast.walk(tree):
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == "swap":
            assert any(keyword.arg == "chain" for keyword in call.keywords), template


def test_legacy_swap_type_does_not_bypass_chain_requirement():
    assert "explicit chain" in teardown_swap_chain_error({"intent_type": "IntentType.SWAP"}, _strategy(), None)


@pytest.mark.parametrize("stage", ["idle", "liquidating"])
def test_v4_delegated_teardown_sell_has_valid_chain(stage):
    from strategies.experiments.quant_v4_base_weth_lp_20260908.strategy import V4ExactKeyLPLifecycle

    strategy = V4ExactKeyLPLifecycle(
        config={
            "amount0": "0.0005",
            "protocol_params": {
                "pool_key": {"currency0": "0x4200000000000000000000000000000000000006"},
                "hook_data": "0x",
            },
        },
        chain="base",
        wallet_address="0x" + "1" * 40,
    )
    strategy.stage = stage
    market = MagicMock()
    market.balance.return_value = SimpleNamespace(balance=Decimal("0.01"))
    intents = strategy.generate_teardown_intents(TeardownMode.SOFT, market)
    assert len(intents) == 1
    intent = intents[0]
    assert intent.chain == "base"
    assert teardown_swap_chain_error(intent, strategy, None) is None
    assert intent.amount == Decimal("0.0005")
    assert intent.protocol == "uniswap_v4"
    assert intent.swap_params == strategy.params


def test_inline_fallback_rejects_missing_chain_before_balance_read():
    from datetime import UTC, datetime

    from almanak.framework.intents import Intent
    from almanak.framework.runner.runner_teardown import _prepare_inline_teardown_intent

    market = _market(Decimal("2"))
    runner = MagicMock()
    runner._calculate_duration_ms.return_value = 0
    intent = Intent.swap(from_token="WETH", to_token="USDC", amount="all")
    prepared = _prepare_inline_teardown_intent(runner, _strategy(), intent, market, datetime.now(UTC), 0)
    assert "explicit chain" in prepared.failure_result.error
    market.balance.assert_not_called()


def test_unscoped_ledger_does_not_hide_measured_accounting_inventory():
    state = SimpleNamespace(
        read_accounting_events_measured=lambda *_: ([_swap_event(_DEP, "USDC", "7")], True),
        read_ledger_entries_measured=lambda *_: ([{"chain": ""}], True),
    )
    assert read_tracked_swap_inventory(state_manager=state, deployment_id=_DEP, scope_chain="arbitrum") == {
        "USDC": Decimal("7")
    }


@pytest.mark.parametrize("chain", [None, "invalid", "base"])
def test_serialized_swap_is_validated_before_execution_context(chain):
    from almanak.framework.intents import Intent

    serialized = Intent.swap(from_token="WETH", to_token="USDC", amount="all", chain=chain).serialize()
    assert serialized["type"] == "SWAP"
    assert teardown_swap_chain_error(serialized, _strategy(), None) is not None
    if chain is None:
        with pytest.raises(ValueError, match="explicit chain"):
            TeardownManager()._build_execution_context(_strategy(), serialized, "test", 0)


@pytest.mark.parametrize("fail_loud", [True, False])
def test_invalid_swaps_never_reach_oracle_or_balance_reads(monkeypatch, fail_loud):
    from almanak.framework.teardown import teardown_manager as tm

    invalid = {"type": "SWAP", "from_token": "BAD", "to_token": "USDC", "amount": "all"}
    valid = {"type": "WITHDRAW", "token": "USDC", "chain": "arbitrum"}
    market = _market(Decimal("2"))
    warm = MagicMock(return_value={"USDC": Decimal("1")})
    monkeypatch.setattr(tm, "warm_and_validate_oracle", warm)
    tm._warm_oracle_risk_first(market, [invalid, valid], fail_loud=fail_loud, strategy=_strategy())
    assert warm.call_args.args[1] == [valid]
    market.balance.assert_not_called()


@pytest.mark.asyncio
async def test_inline_chain_rejection_continues_independent_exit_and_remains_failed(monkeypatch):
    from datetime import UTC, datetime

    from almanak.framework.intents import Intent
    from almanak.framework.runner import runner_teardown as rt
    from almanak.framework.runner.runner_models import IterationResult, IterationStatus

    invalid = Intent.swap(from_token="WETH", to_token="USDC", amount="all")
    valid = Intent.withdraw(protocol="aave_v3", token="USDC", amount="all", chain="arbitrum")
    result = IterationResult(status=IterationStatus.SUCCESS, intent=valid, deployment_id=_DEP)
    execute = AsyncMock(return_value=(result, 0))
    monkeypatch.setattr(rt, "_execute_inline_teardown_intent", execute)
    monkeypatch.setattr(rt.Intent, "has_chained_amount", lambda intent: False)
    runner = MagicMock()
    runner._calculate_duration_ms.return_value = 0
    outcome = await rt._dispatch_inline_teardown_intents(
        runner,
        _strategy(),
        [invalid, valid],
        None,
        datetime.now(UTC),
        teardown_cycle_id="test",
        deferred_append=MagicMock(),
    )
    execute.assert_awaited_once()
    assert execute.call_args.args[2] is valid
    assert not outcome.all_success
    assert "explicit chain" in outcome.last_result.error


@pytest.mark.parametrize("serialized", [False, True])
def test_all_amount_resolution_selects_explicit_chain(serialized):
    from almanak.framework.intents import Intent

    manager = TeardownManager()
    intent = Intent.swap(from_token="WETH", to_token="USDC", amount="all", chain="base")
    if serialized:
        intent = intent.serialize()

    def balance(token, *, chain=None):
        assert token == "WETH"
        assert chain == "base"
        return SimpleNamespace(balance=Decimal("0.3"))

    market = SimpleNamespace(chains=("arbitrum", "base"), balance=balance)
    resolved, error = manager._resolve_all_amount(_strategy(), intent, market, manager._classify_intent_shape(intent))
    assert error is None
    amount = resolved["amount"] if serialized else resolved.amount
    assert Decimal(amount) == Decimal("0.3")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chain,chains,valid",
    [
        (None, ("arbitrum",), False),
        ("invalid", ("arbitrum",), False),
        ("base", ("arbitrum",), False),
        ("arbitrum", ("arbitrum",), True),
        ("base", ("arbitrum", "base"), True),
    ],
)
async def test_accepted_swap_validates_chain_before_settlement_and_keeps_resume_floor(chain, chains, valid):
    manager = TeardownManager()
    manager.state_manager = None
    settlement = AsyncMock(return_value="executed")
    manager.runner_helpers = TeardownRunnerHelpers(check_intent_settlement=settlement)
    manager._settle_accepted_executed = AsyncMock()
    intent = {
        "type": "SWAP",
        "chain": chain,
        "_teardown_async_submission_accepted": True,
        "_teardown_async_submission_order_keys": ["0xabc"],
        "_teardown_async_submission_ledger_id": "ledger-1",
    }
    state = _state()
    market = _market(Decimal("2"))
    market.chains = chains
    result = await manager._execute_intents(
        teardown_id="teardown-test",
        strategy=_strategy(),
        intents=[intent],
        positions=_positions(),
        mode=TeardownMode.SOFT,
        teardown_state=state,
        market=market,
    )
    if valid:
        settlement.assert_awaited_once()
        assert result.intents_succeeded == 1
        assert result.intents_failed == 0
    else:
        settlement.assert_not_awaited()
        manager._settle_accepted_executed.assert_not_awaited()
        assert result.intents_succeeded == 0
        assert result.intents_failed == 1
        assert result.async_settlement_pending
        assert state.current_intent_index == 0
