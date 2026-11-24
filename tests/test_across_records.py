"""Real Across records over two fork nodes: checkpoints, chain projection and the gateway.

No run mode is declared for Across yet, so these tests install a fork declaration for
their own duration. It is a test fixture, never support.
"""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from almanak.connectors.across import execution
from almanak.connectors.across.authority import declared
from almanak.connectors.across.lifecycle import AcrossLifecycle, ownership, validate_closure
from almanak.connectors.across.models import Config
from almanak.connectors.across.operations import DepositOperation, RevokeOperation, plan_call
from almanak.connectors.across.receipts import opened_transfer
from almanak.connectors.across.state import (
    Checkpoint,
    DecisionRecord,
    Outcome,
    Pending,
    TransactionPlan,
)
from almanak.connectors.support import ConnectorChainSupport, EconomicAction, RunMode
from almanak.gateway.accounts import prepare_call
from almanak.gateway.environments import ForkBinding
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.gateway.execution import EvmGateway
from almanak.models import AssetAmount, Block, Chain, StrategyAccounts
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import Memory
from almanak.transfers import Arrived, resolved
from almanak.triggers import IntervalEvent, IntervalTrigger
from almanak.work import merge, project
from tests.across_support import (
    AMOUNT,
    ARBITRUM,
    BASE,
    BASE_USDC,
    BLOCK,
    NOW,
    POOL,
    RECIPIENT,
    SENDER,
    allocation,
    eoa,
    quote,
    route,
)
from tests.test_across_deposit import MINED, receipt

ARB_NODE, BASE_NODE = "0x" + "a1" * 32, "0x" + "b2" * 32
FORK_ARB = Chain(chain_id=ARBITRUM.chain_id, environment="anvil:" + ARB_NODE)
FORK_BASE = Chain(chain_id=BASE.chain_id, environment="anvil:" + BASE_NODE)
FORK_BLOCK = BLOCK.model_copy(update={"chain": FORK_ARB})
STARTED = datetime.fromtimestamp(NOW, UTC)
TX_HASH = "0x" + "cd" * 32
GAS, GAS_PRICE = 200_000, 10**8


def origin(chain, number):
    return Block(chain=chain, number=number, hash="0x" + "0e" * 32, timestamp=NOW - 60)


@pytest.fixture
def config(monkeypatch):
    fork_only = tuple(
        ConnectorChainSupport(
            chain=chain.chain_id,
            modes=(RunMode.FORK,),
            actions=(EconomicAction.BRIDGE, EconomicAction.REVOKE_BRIDGE),
            account_profiles=("eoa",),
            restrictions=("test fixture, not support",),
        )
        for chain in (ARBITRUM, BASE)
    )
    monkeypatch.setattr("almanak.connectors.across.connector.candidate_support", lambda: fork_only)
    return Config(
        decision_trigger=IntervalTrigger(kind="interval", seconds=60),
        accounts=StrategyAccounts(
            bindings=(eoa("arb", FORK_ARB, SENDER), eoa("base", FORK_BASE, RECIPIENT))
        ),
        allocations=(allocation(route=route(chain=FORK_ARB)),),
        fork_bindings=(
            ForkBinding(chain=FORK_ARB, origin=origin(ARBITRUM, 400_000_000), instance=ARB_NODE),
            ForkBinding(chain=FORK_BASE, origin=origin(BASE, 30_000_000), instance=BASE_NODE),
        ),
    )


def observed(config, **changes):
    from almanak.connectors.across.observations import Snapshot

    target = config.target("move")
    values = {
        "target": target,
        "block": FORK_BLOCK,
        "wallet_balance": AMOUNT,
        "allowance": 0,
        "pool": POOL,
        "native_balance": AssetAmount(asset=native_asset(ARBITRUM.chain_id), amount=10**18),
        "payer": target.fund,
        **changes,
    }
    return Snapshot(**values)


def decision(config, intent=None, identity="0" * 32):
    return DecisionRecord(
        trigger=IntervalEvent(started_at=STARTED, due_at=STARTED),
        identity=identity,
        intent=intent or config.bridge("move", AMOUNT),
        observations=(observed(config),),
        memory=Memory(),
    )


def deposit(config):
    return DepositOperation(
        target=config.target("move"),
        amount=config.bridge("move", AMOUNT).amount,
        quote=quote(),
        quote_time_buffer=POOL.quote_time_buffer,
    )


def pending(config, operation, before):
    call = plan_call(operation)
    return Pending(
        attempt_id="attempt-1",
        decision=decision(config),
        operation=operation,
        before=before,
        call=call,
        execution=prepare_call(config.accounts, "arb", call),
        nonce=3,
        gas_estimate=GAS,
        gas=GAS,
        gas_price=GAS_PRICE,
        raw_transaction="0x02",
        tx_hash=TX_HASH,
    )


def mined(operation, status="0x1", **changes):
    logs = receipt(operation)["logs"] if status == "0x1" else []
    return {
        "status": status,
        "logs": logs,
        "transactionHash": TX_HASH,
        "from": SENDER,
        "to": operation.target.route.spoke_pool,
        "gasUsed": hex(GAS // 2),
        "effectiveGasPrice": hex(GAS_PRICE),
        "blockNumber": hex(FORK_BLOCK.number + 1),
        "blockHash": "0x" + "ef" * 32,
        **changes,
    }


def gateway(config, after, mined_at):
    """The real gateway reconciliation, with the chain's own answers fixed."""
    from almanak.connectors.across.connector import connector

    block = FORK_BLOCK.model_copy(
        update={"number": FORK_BLOCK.number + 1, "hash": "0x" + "ef" * 32, "timestamp": mined_at}
    )
    reader = SimpleNamespace(pin_block=lambda number: block, _rpc=None)
    fake = SimpleNamespace(config=config, connector=connector(), reader=reader, guard=lambda: None)
    fake.deadline_revert = lambda operation, at: EvmGateway.deadline_revert(fake, operation, at)
    return fake, lambda: after.model_copy(update={"block": block})


def reconcile(monkeypatch, config, operation, before, after, receipt_fields, mined_at):
    fake, snapshot = gateway(config, after, mined_at)
    monkeypatch.setattr(execution, "snapshot", lambda backend, name, block: snapshot())
    return EvmGateway.reconcile(fake, pending(config, operation, before), receipt_fields)


def spent(snapshot, receipt_fields):
    cost = int(receipt_fields["gasUsed"], 16) * int(receipt_fields["effectiveGasPrice"], 16)
    native = snapshot.native_balance
    return native.model_copy(update={"amount": native.amount - cost})


def test_two_fork_nodes_form_one_route_and_a_fork_never_pairs_with_mainnet(config):
    target = config.target("move")
    assert (target.fund.chain, target.destination_fund.chain) == (FORK_ARB, FORK_BASE)
    assert target.route.destination_chain == BASE.chain_id
    mixed = StrategyAccounts(bindings=(eoa("arb", FORK_ARB, SENDER), eoa("base", BASE, RECIPIENT)))
    with pytest.raises(ValidationError, match="one environment"):
        config.model_copy(update={"accounts": mixed}).target("move")
    elsewhere = StrategyAccounts(
        bindings=(eoa("arb", FORK_ARB, SENDER), eoa("base", FORK_ARB, RECIPIENT))
    )
    with pytest.raises(ValidationError, match="different fund chain"):
        config.model_copy(update={"accounts": elsewhere}).target("move")


def test_a_deposit_reverted_past_its_quote_deadline_is_recorded_with_that_cause(
    monkeypatch, config
):
    operation = deposit(config)
    before = observed(config, allowance=AMOUNT)
    reverted = mined(operation, status="0x0")
    after = before.model_copy(update={"native_balance": spent(before, reverted)})
    late = operation.deadline + 37
    outcome = reconcile(monkeypatch, config, operation, before, after, reverted, late)
    assert isinstance(outcome, Outcome) and not outcome.success and outcome.transfer is None
    assert outcome.revert_reason == (
        f"mined past protocol deadline: block timestamp {late} is 37s after deadline "
        f"{operation.deadline}"
    )
    assert operation.deadline == quote().timestamp + POOL.quote_time_buffer


def test_a_deposit_reverted_before_its_deadline_names_no_deadline(monkeypatch, config):
    operation = deposit(config)
    before = observed(config, allowance=AMOUNT)
    reverted = mined(operation, status="0x0")
    after = before.model_copy(update={"native_balance": spent(before, reverted)})
    monkeypatch.setattr("almanak.gateway.execution.mined_revert", lambda *a: None)
    outcome = reconcile(monkeypatch, config, operation, before, after, reverted, operation.deadline)
    assert not outcome.success and outcome.revert_reason is None


def mined_deposit(monkeypatch, config):
    operation = deposit(config)
    before = observed(config, allowance=AMOUNT)
    fields = mined(operation)
    after = before.model_copy(
        update={"allowance": 0, "wallet_balance": 0, "native_balance": spent(before, fields)}
    )
    return reconcile(monkeypatch, config, operation, before, after, fields, NOW + 12)


def checkpoint(config, **changes):
    initial = AcrossLifecycle.initial(
        config, StrategyDefinition(filename="strategy.py", source=""), declared(config), None
    )
    return initial.model_copy(update=changes)


def test_the_gateway_reconciles_a_mined_deposit_into_an_outcome_with_its_transfer(
    monkeypatch, config
):
    outcome = mined_deposit(monkeypatch, config)
    assert (outcome.success, outcome.assets, outcome.accounting_issues) == (True, AMOUNT, ())
    transfer = outcome.transfer
    assert transfer == opened_transfer(deposit(config), MINED)
    assert (transfer.source.chain, transfer.destination.chain) == (FORK_ARB, FORK_BASE)
    result = outcome.strategy_result()
    assert (result.status, result.transfer) == ("completed", transfer)
    assert Outcome.model_validate_json(outcome.model_dump_json()) == outcome


def test_a_plan_and_its_quote_survive_a_round_trip(config):
    plan = TransactionPlan(
        **pending(config, deposit(config), observed(config)).model_dump(
            exclude={"raw_transaction", "tx_hash", "family"}
        )
    )
    again = TransactionPlan.model_validate_json(plan.model_dump_json())
    assert again == plan and again.operation.quote.response == quote().response


def test_a_checkpoint_keeps_a_transfer_through_a_round_trip_and_opens_it_once(monkeypatch, config):
    outcome = mined_deposit(monkeypatch, config)
    state = checkpoint(config)
    assert tuple(work.chain for work in state.work) == (FORK_ARB, FORK_BASE)
    opened = state.model_copy(update={**ownership(state, outcome), "outcomes": (outcome,)})
    opened = Checkpoint.model_validate_json(opened.model_dump_json())
    assert opened.transfers == (outcome.transfer,)
    with pytest.raises(ValueError, match="retained only once"):
        ownership(opened, outcome)
    with pytest.raises(ValidationError, match="retained only once"):
        Checkpoint.model_validate(
            {**dict(opened), "transfers": (outcome.transfer, outcome.transfer)}
        )


def test_both_chains_see_the_transfer_and_only_the_destination_records_its_arrival(
    monkeypatch, config
):
    outcome = mined_deposit(monkeypatch, config)
    state = checkpoint(config)
    state = state.model_copy(update={**ownership(state, outcome), "outcomes": (outcome,)})
    source, destination = project(state, FORK_ARB), project(state, FORK_BASE)
    assert source.transfers == destination.transfers == state.transfers
    assert (len(source.outcomes), len(destination.outcomes)) == (1, 0)
    assert (len(source.owned), len(destination.owned)) == (1, 0)
    arrival = Arrived(received=AssetAmount(asset=BASE_USDC, amount=AMOUNT - 100_000))
    arrived = resolved(destination.transfers, outcome.transfer.identity, arrival)
    merged = merge(state, FORK_BASE, destination.model_copy(update={"transfers": arrived}))
    assert merged.transfers[0].resolution == arrival
    with pytest.raises(ValueError, match="only on the chain that measured"):
        merge(state, FORK_ARB, source.model_copy(update={"transfers": arrived}))
    assert Checkpoint.model_validate_json(merged.model_dump_json()) == merged


def test_teardown_closes_once_a_standing_approval_is_revoked(config):
    state = checkpoint(config, mode="teardown")
    with pytest.raises(DataError) as error:
        validate_closure(state, (observed(config, allowance=AMOUNT),))
    assert error.value.kind == "remaining_obligations"
    revoke = config.revoke("move")
    assert not AcrossLifecycle.increases_exposure(revoke)
    closing = state.model_copy(update={"decision": decision(config, revoke, "2" * 32)})
    planned = execution.AcrossExecution.operation(
        config, closing, revoke, observed(config, allowance=AMOUNT)
    )
    assert planned == RevokeOperation(target=config.target("move"), final=True)
    validate_closure(state, (observed(config),))
