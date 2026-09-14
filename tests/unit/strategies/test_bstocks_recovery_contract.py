"""Real SDK state and callback boundaries for bounded strategy recovery."""

from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.execution.extracted_data import LPOpenData, SwapAmounts
from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult
from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult
from almanak.framework.execution.submission import SubmissionProvenance
from almanak.framework.market.models import PriceData, TokenBalance
from almanak.framework.runner.strategy_runner import StrategyRunner, _ReplayCallbackError
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateData, StateManager, StateManagerConfig
from almanak.framework.state.strategy_state import STRATEGY_USER_STATE_KEY
from almanak.framework.teardown import TeardownMode
from strategies.bstocks_lp.strategy import BStocksLPConfig, BStocksLPStrategy
from strategies.bstocks_spot.strategy import BASE, QUOTE, BStocksSpotStrategy

DEPLOYMENT = "deployment:bstocks-recovery-contract"
HASH = "0x" + "ab" * 32


def _strategy(kind, allocation="6"):
    config = BStocksLPConfig(quote_allocation=Decimal(allocation)) if kind == "lp" else {"quote_allocation": allocation}
    cls = BStocksLPStrategy if kind == "lp" else BStocksSpotStrategy
    return cls(config=config, chain="bsc", wallet_address="0x" + "1" * 40)


def _market(base="0"):
    now = datetime.now(UTC)
    return SimpleNamespace(
        timestamp=now,
        balance=lambda token: TokenBalance(
            symbol=token,
            address=token,
            balance=Decimal("100") if token.lower() == QUOTE else Decimal(base),
            balance_usd=Decimal("0"),
        ),
        price=lambda _: Decimal("300"),
        price_data=lambda token: PriceData(
            price=Decimal("1") if token.lower() == QUOTE else Decimal("300"), timestamp=now
        ),
    )


def _landed_entry(kind):
    amount = Decimal("3") if kind == "lp" else Decimal("6")
    return ExecutionResult(
        success=True,
        phase=ExecutionPhase.COMPLETE,
        swap_amounts=SwapAmounts(
            amount_in=int(amount * 10**18),
            amount_out=10**16,
            amount_in_decimal=amount,
            amount_out_decimal=Decimal("0.01"),
        ),
    )


async def _manager(path):
    store = SQLiteStore(SQLiteConfig(db_path=str(path)))
    manager = StateManager(StateManagerConfig(load_state_on_startup=False), warm_backend=store)
    await manager.initialize()
    return manager


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["spot", "lp"])
async def test_rejected_restore_survives_real_loader_save_and_disk_reopen(tmp_path, kind):
    original = _strategy(kind)
    original.decide(_market())
    raw = deepcopy(original.get_persistent_state())
    path = tmp_path / "state.db"
    manager = await _manager(path)
    try:
        await manager.save_state(StateData(deployment_id=DEPLOYMENT, version=1, state={STRATEGY_USER_STATE_KEY: raw}))
        rejected = _strategy(kind, allocation="7")
        rejected._state_manager = manager
        rejected._deployment_id = DEPLOYMENT
        assert await rejected.load_state_async() is False
        assert rejected.decide(_market()).intent_type.value == "HOLD"
        with pytest.raises((ValueError, RuntimeError)):
            rejected.get_open_positions()
        with pytest.raises((ValueError, RuntimeError)):
            rejected.generate_teardown_intents(TeardownMode.SOFT, _market())
        assert rejected.get_persistent_state() == raw
        rejected.save_state()
        assert rejected._pending_save is not None
        await rejected._pending_save
    finally:
        await manager.close()
    reopened = await _manager(path)
    try:
        saved = await reopened.load_state(DEPLOYMENT)
        assert saved.state[STRATEGY_USER_STATE_KEY] == raw
        recoverable = _strategy(kind)
        recoverable._state_manager = reopened
        recoverable._deployment_id = DEPLOYMENT
        assert await recoverable.load_state_async() is True
        assert recoverable.get_persistent_state() == raw
    finally:
        await reopened.close()


@pytest.mark.parametrize("kind", ["spot", "lp"])
def test_real_strict_runner_callback_rejects_missing_landed_entry_payload(kind):
    strategy = _strategy(kind)
    entry = strategy.decide(_market())
    runner = object.__new__(StrategyRunner)
    with pytest.raises(_ReplayCallbackError, match="replay barrier retained"):
        runner._notify_intent_executed(
            strategy,
            entry,
            True,
            ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE),
            strict=True,
        )
    assert strategy.decide(_market()).intent_type.value == "HOLD"
    assert strategy.get_persistent_state()["pending_intent_id"] == entry.intent_id
    runner._notify_intent_executed(strategy, entry, True, _landed_entry(kind), strict=True)
    assert strategy.get_persistent_state()["pending_intent_id"] is None


@pytest.mark.parametrize("kind", ["spot", "lp"])
def test_policy_failure_after_landing_preserves_receipt_inventory_for_teardown(kind):
    strategy = _strategy(kind)
    entry = strategy.decide(_market())
    runner = object.__new__(StrategyRunner)
    runner._notify_intent_executed(strategy, entry, False, _landed_entry(kind), framework_success=True, strict=True)
    assert strategy.decide(_market(base="0.01")).intent_type.value == "HOLD"
    state = strategy.get_persistent_state()
    if kind == "spot":
        assert Decimal(state["owned_base"]) == Decimal("0.01")
    else:
        assert Decimal(state["owned"]["GOOGLB"]) == Decimal("0.01")
    intents = strategy.generate_teardown_intents(TeardownMode.SOFT, _market(base="0.01"))
    assert len(intents) == 1 and intents[0].intent_type.value == "SWAP"
    assert intents[0].from_token.lower() == BASE


def _failure(case):
    if case == "compile_refusal":
        return SimpleNamespace(error="Compiler refused the requested intent")
    if case == "missing_result":
        return None
    provenance = SubmissionProvenance.UNSPECIFIED
    hashes, receipts = [], []
    if case in {"not_attempted", "contradictory"}:
        provenance = SubmissionProvenance.NOT_ATTEMPTED
    if case in {"reverted", "ambiguous", "contradictory", "partial"}:
        hashes = [HASH]
    if case in {"reverted", "partial"}:
        provenance = SubmissionProvenance.ATTEMPTED
        receipts = [
            {
                "tx_hash": HASH,
                "block_number": 42,
                "block_hash": "0x" + "cd" * 32,
                "gas_used": 21000,
                "effective_gas_price": 1,
                "status": 0,
                "logs": [],
            }
        ]
    if case == "partial":
        other = "0x" + "ef" * 32
        hashes.append(other)
        receipts.append({**receipts[0], "tx_hash": other, "status": 1})
    return GatewayExecutionResult(
        success=False,
        tx_hashes=hashes,
        total_gas_used=0,
        receipts=receipts,
        execution_id="failed",
        submission_provenance=provenance,
    )


@pytest.mark.parametrize("kind", ["spot", "lp"])
@pytest.mark.parametrize(
    "case",
    [
        "not_attempted",
        "reverted",
        "unknown",
        "ambiguous",
        "contradictory",
        "partial",
        "compile_refusal",
        "missing_result",
    ],
)
def test_only_positive_zero_effect_evidence_releases_entry_teardown(kind, case):
    strategy = _strategy(kind)
    entry = strategy.decide(_market())
    strategy.on_intent_executed(entry, False, _failure(case))
    assert strategy.decide(_market()).intent_type.value == "HOLD"
    if case in {"not_attempted", "reverted"}:
        assert strategy.get_persistent_state()["phase"] == "entry_refused"
        assert strategy.generate_teardown_intents(TeardownMode.SOFT, _market()) == []
        resumed = _strategy(kind)
        resumed.load_persistent_state(strategy.get_persistent_state())
        assert resumed.decide(_market()).intent_type.value == "HOLD"
    else:
        assert strategy.get_persistent_state()["pending_intent_id"] == entry.intent_id
        with pytest.raises((ValueError, RuntimeError)):
            strategy.generate_teardown_intents(TeardownMode.SOFT, _market())


def _funded_mint():
    strategy = _strategy("lp")
    entry = strategy.decide(_market())
    strategy.on_intent_executed(entry, True, _landed_entry("lp"))
    mint = strategy.decide(_market(base="0.01"))
    assert mint.intent_type.value == "LP_OPEN"
    return strategy, mint


def test_missing_landed_mint_payload_preserves_known_nft_and_signals_strict_barrier():
    strategy, mint = _funded_mint()
    runner = object.__new__(StrategyRunner)
    with pytest.raises(_ReplayCallbackError, match="replay barrier retained"):
        runner._notify_intent_executed(
            strategy,
            mint,
            True,
            ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE, position_id=42),
            strict=True,
        )
    assert strategy.get_persistent_state()["position_id"] == "42"
    assert strategy.get_persistent_state()["pending_intent_id"] == mint.intent_id
    assert strategy.decide(_market(base="0.01")).intent_type.value == "HOLD"
    runner._notify_intent_executed(
        strategy,
        mint,
        True,
        ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            position_id=42,
            extracted_data={
                "lp_open_data": LPOpenData(
                    position_id=42, amount0=10**15, amount1=10**18, pool_address=strategy.settings.pool_address
                )
            },
        ),
        strict=True,
    )
    recovered = strategy.get_persistent_state()
    assert recovered["phase"] == "holding"
    assert recovered["position_id"] == "42"
    assert recovered["pending_intent_id"] is None
    assert recovered["problem"] is None


@pytest.mark.parametrize("case", ["not_attempted", "reverted", "unknown", "ambiguous", "partial"])
def test_failed_mint_releases_only_proven_unspent_owned_stock(case):
    strategy, mint = _funded_mint()
    strategy.on_intent_executed(mint, False, _failure(case))
    state = strategy.get_persistent_state()
    assert Decimal(state["owned"]["GOOGLB"]) == Decimal("0.01")
    assert Decimal(state["owned"]["USDT"]) == Decimal("3")
    assert strategy.decide(_market(base="0.01")).intent_type.value == "HOLD"
    if case in {"not_attempted", "reverted"}:
        assert state["phase"] == "mint_refused"
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1 and intents[0].intent_type.value == "SWAP"
        assert intents[0].from_token.lower() == BASE
    else:
        assert state["pending_intent_id"] == mint.intent_id
        with pytest.raises((ValueError, RuntimeError)):
            strategy.generate_teardown_intents(TeardownMode.SOFT)
