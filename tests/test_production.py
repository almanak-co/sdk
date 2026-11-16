"""Production admission and recovery checks using public test keys and captured evidence."""

import json
import threading
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import encode
from eth_account import Account as Signer
from pydantic import ValidationError

from almanak.caip import AssetType, ChainId
from almanak.connectors.aave_v4.deployments import validate_markets
from almanak.connectors.aave_v4.execution import validate_balances
from almanak.connectors.aave_v4.models import LendingConfig
from almanak.connectors.aave_v4.state import Checkpoint
from almanak.gateway.accounts import prepare_call
from almanak.gateway.admission import validate_execution_update
from almanak.gateway.credentials import (
    create_wallet,
    decrypt_signer,
    load_production_signers,
    private_text,
    write_private,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.production import (
    ETHEREUM_GENESIS,
    ProductionGateway,
    validate_endpoint,
)
from almanak.gateway.production_fees import (
    decode_quote,
    increases_exposure,
    quoted_fees,
    quoted_price,
    validate_budget,
    validate_receipt_cost,
)
from almanak.gateway.service import GatewayService, Request
from almanak.models import Block, Chain
from almanak.reporting import report
from almanak.runtime import commit_outcome
from almanak.state import FeeQuote, Journal
from tests.test_runtime_recovery import coordinator, pending_for

ROOT = Path(__file__).parents[1]
TEST_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"


def mainnet_value(value):
    if isinstance(value, dict):
        return {
            key: "mainnet" if key == "environment" else mainnet_value(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [mainnet_value(item) for item in value]
    if isinstance(value, str):
        return value.replace("eip155:31337", "eip155:1")
    return value


@pytest.fixture
def production_recorded(recorded):
    return production_state(recorded)


def production_state(recorded):
    raw = mainnet_value(recorded.model_dump(mode="json"))
    policy = json.loads(
        (ROOT / "tests/fixtures/author/aave_v4_weth_supply/production.example.json").read_text()
    )["production"]
    policy["gas"][0]["accounts"] = ["lending_0"]
    raw["config"].pop("fork_instance")
    raw["config"].pop("fork_origin")
    raw["config"]["production"] = policy
    return Checkpoint.model_validate_json(json.dumps(raw))


@pytest.fixture
def production_attempt(production_recorded):
    outcome = production_recorded.outcomes[1]
    pending = pending_for(production_recorded.config, outcome)
    block = pending.before.position.reserve.block
    pending = pending.model_copy(
        update={
            "fee_quote": FeeQuote(block=block, next_base_fee=1, priority_fee=1),
            "execution": prepare_call(
                production_recorded.config.accounts, pending.operation.account, pending.call
            ),
        }
    )
    before = production_recorded.model_copy(
        update={
            "mode": "running",
            "decision": pending.decision,
            "outcomes": production_recorded.outcomes[:1],
            "delivered": 1,
            "pending": pending,
            "closure": (),
        }
    )
    return before, pending, outcome


class Reader:
    def __init__(self, block):
        self.block = block
        self.chain = block.chain
        self.transport_chain = block.chain.chain_id
        self.finalized = block
        self.responses = {"eth_getCode": "0x"}
        self.calls = []

    def pin_block(self, selector):
        self.calls.append(("pin_block", selector))
        if selector == "finalized":
            return self.finalized
        return self.block

    @contextmanager
    def retained_pin(self):
        yield

    def _rpc(self, method, params, **kwargs):
        self.calls.append((method, params))
        value = self.responses.get(method)
        if isinstance(value, Exception):
            raise value
        return value


def backend_for(config, block):
    backend = object.__new__(ProductionGateway)
    backend.config = config
    backend.production = config.production
    backend.reader = Reader(block)
    backend.guard = lambda: None
    backend.node_readings = {}
    backend.node_readings_lock = threading.Lock()
    return backend


@pytest.mark.parametrize("mutation", ["fork", "chain", "environment", "gas", "safe", "source"])
def test_production_authority_is_explicit(production_recorded, mutation):
    raw = production_recorded.config.model_dump(mode="json")
    if mutation == "fork":
        raw["fork_instance"] = "0x" + "01" * 32
    elif mutation in ("chain", "environment"):
        chain = raw["production"]["networks"][0]["chain"]
        chain["chain_id" if mutation == "chain" else "environment"] = (
            "eip155:31337" if mutation == "chain" else "other-mainnet"
        )
    elif mutation == "gas":
        raw["production"]["gas"][0]["accounts"] = ["unknown"]
    elif mutation == "safe":
        raw["accounts"]["bindings"][0]["profile"]["kind"] = "safe_roles"
    else:
        del raw["production"]["gas"][0]["source"]
    with pytest.raises(ValidationError):
        LendingConfig.model_validate_json(json.dumps(raw))


def test_production_accepts_declared_borrowing_capability(production_recorded):
    raw = production_recorded.config.model_dump(mode="json")
    author = json.loads(
        (ROOT / "tests/fixtures/author/aave_v4_weth_supply/config.json").read_text()
    )
    debt = author["debts"][0]
    debt["collateral"] = [raw["allocations"][0]["name"]]
    raw["debts"] = [debt]
    config = LendingConfig.model_validate_json(json.dumps(raw))
    validate_markets(config)
    assert config.debts[0].reserve.asset == config.borrow("main_usdc", 1).amount.asset


def test_supported_markets_and_transport(production_recorded):
    config = production_recorded.config
    validate_markets(config)
    for endpoint in (
        "http://127.0.0.1:8545",
        "https://name:secret@example.com",
        "https://example.com/#x",
    ):
        with pytest.raises(ValueError):
            ProductionGateway(config, endpoint, Path("must-not-read"))
    validate_endpoint("https://explicit.example/rpc")
    allocation = config.allocations[0]
    bad = allocation.model_copy(
        update={"reserve": allocation.reserve.model_copy(update={"spoke": "0x" + "11" * 20})}
    )
    validate_markets(config.model_copy(update={"allocations": (bad,)}))
    duplicate = bad.model_copy(
        update={"reserve": bad.reserve.model_copy(update={"spoke": bad.reserve.hub})}
    )
    with pytest.raises(ValueError, match="distinct"):
        validate_markets(config.model_copy(update={"allocations": (duplicate,)}))


@pytest.mark.parametrize("change", [None, "chain", "client", "genesis"])
def test_live_guard_rechecks_environment(production_recorded, change):
    block = production_recorded.baseline[0].position.reserve.block.model_copy(
        update={"hash": ETHEREUM_GENESIS}
    )
    backend = backend_for(production_recorded.config, block)
    backend.reader.responses = {"eth_chainId": "0x1", "web3_clientVersion": "Geth/v1"}
    if change == "chain":
        backend.reader.responses["eth_chainId"] = "0x7a69"
    elif change == "client":
        backend.reader.responses["web3_clientVersion"] = "anvil/v1"
    elif change == "genesis":
        backend.reader.block = block.model_copy(update={"hash": "0x" + "00" * 32})
    if change:
        with pytest.raises(DataError):
            ProductionGateway.guard(backend)
    else:
        ProductionGateway.guard(backend)
        ProductionGateway.guard(backend)
        assert [method for method, _ in backend.reader.calls].count("eth_chainId") == 2


def test_pinned_fee_quote_is_reproducible(production_recorded):
    block = production_recorded.baseline[0].position.reserve.block
    reader = Reader(block)
    reader.responses = {
        "eth_estimateGas": "0x64",
        "eth_feeHistory": {
            "oldestBlock": hex(block.number),
            "baseFeePerGas": ["0xa", "0xc"],
            "reward": [["0x3"]],
        },
    }
    policy = production_recorded.config.production.gas[0]
    first = quoted_fees(reader, policy, block, {"data": "0x01"})
    assert (first["gas"], first["gas_price"]) == (125, 27)
    assert first == quoted_fees(reader, policy, block, {"data": "0x01"})
    assert reader.calls[0][1][1] == {"blockHash": block.hash, "requireCanonical": True}
    assert first["fee_quote"].block == block
    assert first["fee_quote"].node_gas_price is None
    assert "eth_gasPrice" not in [method for method, _ in reader.calls]


def test_chain_without_fee_history_tips_retains_the_node_gas_price(production_recorded):
    block = production_recorded.baseline[0].position.reserve.block
    bnb_block = block.model_copy(
        update={"chain": Chain(chain_id=ChainId("eip155:56"), environment="mainnet")}
    )
    policy = production_recorded.config.production.gas[0]
    native = AssetType("eip155:56/slip44:714")
    policy = policy.model_copy(
        update={
            "asset": native,
            "max_transaction_fee": policy.max_transaction_fee.model_copy(update={"asset": native}),
            "total_budget": policy.total_budget.model_copy(update={"asset": native}),
            "exit_reserve": policy.exit_reserve.model_copy(update={"asset": native}),
        }
    )
    reader = Reader(bnb_block)
    reader.responses = {
        "eth_feeHistory": {
            "oldestBlock": hex(bnb_block.number),
            "baseFeePerGas": ["0x0", "0x0"],
            "reward": [["0x0"]],
        },
        "eth_gasPrice": "0x2faf080",
    }
    price, quote = quoted_price(reader, policy, bnb_block)
    assert price == 0x2FAF080
    assert quote.node_gas_price == 0x2FAF080
    assert (quote.market_base_fee, quote.priority_fee) == (0, 0)
    # Revalidation reuses the retained value instead of a moving suggestion.
    reader.responses.pop("eth_gasPrice")
    reader.calls.clear()
    retained, requoted = quoted_price(
        reader, policy, bnb_block, node_gas_price=quote.node_gas_price
    )
    assert (retained, requoted) == (price, quote)
    assert "eth_gasPrice" not in [method for method, _ in reader.calls]
    # A higher market quote keeps its own price while still retaining the floor.
    reader.responses["eth_feeHistory"]["baseFeePerGas"] = ["0x3b9aca00", "0x3b9aca00"]
    market, retained_quote = quoted_price(
        reader, policy, bnb_block, node_gas_price=quote.node_gas_price
    )
    assert market == 2 * 0x3B9ACA00
    assert retained_quote.node_gas_price == 0x2FAF080
    # A zero suggestion leaves a zero quote for admission to refuse.
    reader.responses["eth_feeHistory"]["baseFeePerGas"] = ["0x0", "0x0"]
    reader.responses["eth_gasPrice"] = "0x0"
    assert quoted_price(reader, policy, bnb_block)[0] == 0


@pytest.mark.parametrize(
    "history", [{}, {"oldestBlock": "0x0"}, {"oldestBlock": "0x1", "baseFeePerGas": []}]
)
def test_missing_fee_data_has_no_fallback(production_recorded, history):
    with pytest.raises(DataError, match="fee history"):
        decode_quote(history, production_recorded.baseline[0].position.reserve.block)


@pytest.mark.parametrize("failure", ["price", "gas", "transaction", "cumulative", "balance"])
def test_gas_authority_blocks_excess_spending(production_attempt, failure):
    state, pending, _ = production_attempt
    policy = state.config.production.policy(pending.operation.account)
    gas, price, balance = 100, 100, pending.before.native_balance
    changes = {}
    if failure == "price":
        price = policy.max_gas_price_wei + 1
    elif failure == "gas":
        gas = policy.max_gas_limit + 1
    elif failure == "transaction":
        gas, price = policy.max_gas_limit, policy.max_gas_price_wei
        changes["max_transaction_fee"] = policy.max_transaction_fee.model_copy(update={"amount": 1})
    elif failure == "cumulative":
        changes["total_budget"] = policy.total_budget.model_copy(update={"amount": 1})
    else:
        balance = balance.model_copy(update={"amount": 1})
    policy = policy.model_copy(update=changes)
    config = state.config.model_copy(
        update={"production": state.config.production.model_copy(update={"gas": (policy,)})}
    )
    with pytest.raises(DataError):
        validate_budget(
            state.model_copy(update={"config": config}), pending.operation, balance, gas, price
        )


def test_absent_total_budget_is_no_lifetime_cap_but_the_balance_still_binds(production_attempt):
    state, pending, _ = production_attempt
    policy = state.config.production.policy(pending.operation.account)
    spent = policy.total_budget.model_copy(update={"amount": 1})
    capped = policy.model_copy(update={"total_budget": spent})
    uncapped = policy.model_copy(update={"total_budget": None})
    balance = pending.before.native_balance

    def config(selected):
        return state.config.model_copy(
            update={"production": state.config.production.model_copy(update={"gas": (selected,)})}
        )

    with pytest.raises(DataError, match="total_budget"):
        validate_budget(
            state.model_copy(update={"config": config(capped)}),
            pending.operation,
            balance,
            100,
            100,
        )
    uncapped_state = state.model_copy(update={"config": config(uncapped)})
    validate_budget(uncapped_state, pending.operation, balance, 100, 100)
    with pytest.raises(DataError, match="payer balance"):
        validate_budget(
            uncapped_state,
            pending.operation,
            balance.model_copy(update={"amount": 1}),
            100,
            100,
        )


def test_exit_reserve_is_available_for_withdrawal(production_recorded):
    outcome = production_recorded.outcomes[-1]
    balance = outcome.before.native_balance.model_copy(update={"amount": 10000})
    validate_budget(production_recorded, outcome.operation, balance, 100, 100)
    with pytest.raises(DataError, match="exit reserve"):
        validate_budget(
            production_recorded, production_recorded.outcomes[1].operation, balance, 100, 100
        )


@pytest.mark.parametrize("ceiling", ["transaction", "cumulative", "balance"])
def test_op_exposure_counts_against_the_same_payer_ceilings(production_attempt, ceiling):
    state, pending, _ = production_attempt
    policy = state.config.production.policy(pending.operation.account)
    gas, price, exposure = 100, 100, 500
    payer = state.config.accounts.binding(pending.operation.account).profile.submitter
    spent = sum(
        o.gas_paid.amount
        for o in state.outcomes
        if getattr(o, "gas_paid", None) is not None
        and state.config.accounts.binding(o.operation.account).profile.submitter == payer
    )
    reserve = (
        policy.exit_reserve.amount if increases_exposure(state.config, pending.operation) else 0
    )
    required = gas * price + reserve + exposure
    balance = pending.before.native_balance.model_copy(
        update={"amount": required + pending.execution.value - 1}
    )
    changes = {}
    if ceiling == "transaction":
        changes["max_transaction_fee"] = policy.max_transaction_fee.model_copy(
            update={"amount": gas * price + exposure - 1}
        )
    elif ceiling == "cumulative":
        changes["total_budget"] = policy.total_budget.model_copy(
            update={"amount": spent + required - 1}
        )
    policy = policy.model_copy(update=changes)
    config = state.config.model_copy(
        update={"production": state.config.production.model_copy(update={"gas": (policy,)})}
    )
    bounded = state.model_copy(update={"config": config})
    validate_budget(bounded, pending.operation, balance, gas, price, exposure - 1)
    message = {"transaction": "transaction maximum cost", "cumulative": "remaining budget"}.get(
        ceiling, "payer balance"
    )
    with pytest.raises(DataError, match=message):
        validate_budget(bounded, pending.operation, balance, gas, price, exposure)


def test_submission_revalidates_retained_additional_fee_exposure(production_attempt):
    state, pending, _ = production_attempt
    exposure = 500
    policy = state.config.production.policy(pending.operation.account)
    policy = policy.model_copy(
        update={
            "max_transaction_fee": policy.max_transaction_fee.model_copy(
                update={"amount": pending.gas * pending.gas_price + exposure - 1}
            )
        }
    )
    config = state.config.model_copy(
        update={"production": state.config.production.model_copy(update={"gas": (policy,)})}
    )
    pending = pending.model_copy(
        update={
            "fee_quote": pending.fee_quote.model_copy(update={"additional_fee_exposure": exposure})
        }
    )
    state = state.model_copy(update={"config": config, "pending": pending})
    backend = backend_for(config, pending.before.block)
    backend.reader.responses["eth_getTransactionByHash"] = None
    backend.observe = lambda _: (pending.before,)
    backend.op_exposure = lambda *args: exposure
    with pytest.raises(DataError, match="transaction maximum cost"):
        backend.authorize_submission(state)


@pytest.mark.parametrize("condition", ["pending", "unfinalized", "orphan", "finalized"])
def test_receipt_requires_canonical_mining_without_finality(production_attempt, condition):
    state, pending, outcome = production_attempt
    backend = backend_for(state.config, outcome.after.position.reserve.block)
    backend.validate_pending = lambda p: None
    backend.reader.responses["eth_getTransactionReceipt"] = (
        None if condition == "pending" else outcome.receipt
    )
    if condition == "unfinalized":
        backend.reader.finalized = backend.reader.block.model_copy(
            update={"number": backend.reader.block.number - 1}
        )
    elif condition == "orphan":
        backend.reader.block = backend.reader.block.model_copy(update={"hash": "0x" + "00" * 32})
    expected = outcome.receipt if condition in ("unfinalized", "finalized") else None
    assert backend.receipt(pending) == expected
    assert ("pin_block", "finalized") not in backend.reader.calls


@pytest.mark.parametrize("condition", ["canonical", "missing", "changed", "orphan"])
def test_committed_receipts_are_audited_without_finality(production_attempt, condition):
    state, _, _ = production_attempt
    outcome = state.outcomes[0]
    backend = backend_for(state.config, outcome.after.position.reserve.block)
    backend.reader.finalized = backend.reader.block.model_copy(update={"number": 0})
    receipt = dict(outcome.receipt)
    if condition == "changed":
        receipt["blockHash"] = "0x" + "00" * 32
    if condition == "orphan":
        backend.reader.block = backend.reader.block.model_copy(update={"hash": "0x" + "00" * 32})
    backend.reader.responses["eth_getTransactionReceipt"] = (
        None if condition == "missing" else receipt
    )
    if condition == "canonical":
        backend.audit_history(state)
    else:
        with pytest.raises(DataError, match="canonical|absent"):
            backend.audit_history(state)
    assert ("pin_block", "finalized") not in backend.reader.calls


def test_removed_finality_setting_preserves_pending_resume(production_attempt):
    state, pending, _ = production_attempt
    raw = state.model_dump(mode="json")
    raw["config"]["production"]["networks"][0]["finality"] = "finalized"
    restored = Checkpoint.model_validate_json(json.dumps(raw))
    assert restored == state
    assert restored.pending == pending
    assert "finality" not in restored.config.production.networks[0].model_dump()
    assert report(restored)["receipt_confirmation"] == "canonical_mined"


def test_reconciliation_preserves_external_flows_without_losing_ownership(production_attempt):
    state, pending, outcome = production_attempt
    after = outcome.after.model_copy(
        update={
            "native_balance": outcome.after.native_balance.model_copy(
                update={"amount": outcome.after.native_balance.amount + 500}
            )
        }
    )
    issues = validate_balances(
        pending, after, outcome.assets, outcome.shares, outcome.gas_paid.amount
    )
    assert issues == ("gas payer balance includes unexplained external movements",)
    changed = after.model_copy(
        update={"position": after.position.model_copy(update={"supplied_shares": 0})}
    )
    with pytest.raises(DataError, match="owned shares"):
        validate_balances(pending, changed, outcome.assets, outcome.shares, outcome.gas_paid.amount)


def test_outcome_admission_verifies_economics(production_attempt):
    before, pending, outcome = production_attempt
    backend = SimpleNamespace(receipt=lambda p: outcome.receipt, reconcile=lambda p, r: outcome)
    after = commit_outcome(before, pending, outcome)
    validate_execution_update(backend, before, after)
    forged = outcome.model_copy(
        update={"gas_paid": outcome.gas_paid.model_copy(update={"amount": 0})}
    )
    with pytest.raises(ValueError, match="independently verified"):
        validate_execution_update(
            backend, before, after.model_copy(update={"outcomes": (*before.outcomes, forged)})
        )
    with pytest.raises(ValueError, match="durably admitted"):
        validate_execution_update(backend, before.model_copy(update={"pending": None}), after)


def test_invalidated_history_is_durable(tmp_path, production_recorded, reference_strategy):
    def failed_audit(state):
        raise DataError("reorganization", "committed receipt disappeared")

    backend = SimpleNamespace(audit_history=failed_audit)
    config = production_recorded.config
    service = GatewayService(config, tmp_path / "state.sqlite", backend, lease_seconds=30)
    service.journal.create(config, reference_strategy)
    before = service.journal.load()
    identity = "a" * 32
    service.journal.acquire(identity, 30)
    with pytest.raises(DataError, match="disappeared"):
        service.dispatch("audit", Request(coordinator=identity, deployment_id=before.deployment_id))
    state = service.journal.load()
    assert state.integrity_error == "committed receipt disappeared"
    assert report(state)["evidence_status"] == "invalidated"
    with pytest.raises(DataError, match="investigation"):
        service.check_execution_authority("sign", state)


def test_expired_unsigned_plan_can_be_reprepared(tmp_path, production_attempt, reference_strategy):
    state, pending, _ = production_attempt
    from almanak.connectors.aave_v4.state import TransactionPlan

    plan = TransactionPlan.model_validate(
        pending.model_dump(exclude={"raw_transaction", "tx_hash"})
    )
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(state.config, reference_strategy)
    journal.update(lambda _: state.model_copy(update={"pending": None, "plan": plan}))

    def expired():
        raise DataError("stale_plan", "expired")

    runtime = coordinator(journal, SimpleNamespace(sign=expired))
    assert runtime.tick() is False
    assert journal.load().plan is None and journal.load().pending is None
    assert journal.load().decision is None


def test_an_expired_decision_is_dropped_before_anything_is_signed(
    tmp_path, production_attempt, reference_strategy
):
    state, _, _ = production_attempt
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(state.config, reference_strategy)
    deciding = state.model_copy(update={"pending": None, "plan": None})
    journal.update(lambda _: deciding)
    assert deciding.decision is not None

    def refused(kind, message):
        def prepare(state, intent):
            raise DataError(kind, message)

        return SimpleNamespace(prepare=prepare)

    # Any other refusal still stops the coordinator with the decision retained.
    with pytest.raises(DataError, match="JSON-RPC 3"):
        coordinator(journal, refused("rpc", "RPC rejected the read (JSON-RPC 3)")).tick()
    assert journal.load().decision == deciding.decision
    expired = refused("stale_plan", "retained deadline expired or exceeds configured policy")
    assert coordinator(journal, expired).tick() is False
    assert journal.load().plan is None and journal.load().pending is None
    assert journal.load().decision is None


def test_signing_retains_chain_nonce_and_fund(production_attempt, monkeypatch):
    state, pending, _ = production_attempt
    from almanak.connectors.aave_v4.state import TransactionPlan

    plan = TransactionPlan.model_validate(
        pending.model_dump(exclude={"raw_transaction", "tx_hash"})
    )
    state = state.model_copy(update={"plan": plan, "pending": None})
    backend = backend_for(state.config, plan.before.position.reserve.block)
    backend.reader.chain = plan.call.fund.chain
    backend.signers = {plan.operation.account: Signer.from_key(TEST_KEY)}
    backend.audit_history = lambda s: None
    backend.validate_operation = lambda *args: None
    backend.validate_current = lambda p: p.before
    monkeypatch.setattr(
        "almanak.gateway.execution.time.time",
        lambda: plan.before.position.reserve.block.timestamp,
    )
    signed = backend.sign(state)
    assert signed.nonce == pending.nonce
    assert Signer.recover_transaction(signed.raw_transaction).lower() == plan.call.fund.address
    backend.validate_pending_block = lambda p: None
    backend.validate_pending(signed)
    assert backend.sign(state) == signed


def test_no_policy_fork_signing_validates_current_without_a_fee_quote(recorded):
    outcome = next(item for item in recorded.outcomes if item.operation.kind == "supply")
    pending = pending_for(recorded.config, outcome)
    from almanak.connectors.aave_v4.state import TransactionPlan

    plan = TransactionPlan.model_validate(
        pending.model_dump(exclude={"raw_transaction", "tx_hash"})
    ).model_copy(
        update={
            "execution": prepare_call(
                recorded.config.accounts, pending.operation.account, pending.call
            )
        }
    )
    assert plan.fee_quote is None and recorded.config.fork_gas is None
    state = recorded.model_copy(update={"plan": plan, "pending": None, "decision": plan.decision})
    backend = backend_for(recorded.config, plan.before.position.reserve.block)
    backend.reader.chain = plan.call.fund.chain
    backend.signers = {plan.operation.account: Signer.from_key(TEST_KEY)}
    backend.audit_history = lambda _: None
    backend.validate_operation = lambda *args: None
    backend.verify_authority = lambda *args: None
    backend.snapshot = lambda *args: plan.before
    backend.reader.responses["eth_call"] = (
        "0x" + encode(("uint256", "uint256"), (1, plan.operation.amount.amount)).hex()
    )
    assert backend.validate_current(plan) == plan.before
    signed = backend.sign(state)
    assert signed.fee_quote is None and signed.family is None


@pytest.mark.parametrize("stage", ["current", "sign"])
def test_bounded_execution_rejects_a_missing_fee_quote(production_attempt, stage):
    state, pending, _ = production_attempt
    from almanak.connectors.aave_v4.state import TransactionPlan

    plan = TransactionPlan.model_validate(
        pending.model_dump(exclude={"raw_transaction", "tx_hash"})
    ).model_copy(update={"fee_quote": None})
    state = state.model_copy(update={"plan": plan, "pending": None})
    backend = backend_for(state.config, plan.before.position.reserve.block)
    backend.clock = lambda: plan.before.position.reserve.block.timestamp
    backend.snapshot = lambda *args: plan.before
    backend.audit_history = lambda _: None
    if stage == "current":
        backend.reader.responses["eth_call"] = (
            "0x" + encode(("uint256", "uint256"), (1, plan.operation.amount.amount)).hex()
        )
    with pytest.raises(ValueError, match="requires its pinned fee quote"):
        if stage == "current":
            backend.validate_current(plan)
        else:
            backend.sign(state)


def test_current_simulation_checks_position_and_gas(production_attempt, monkeypatch):
    state, pending, _ = production_attempt
    backend = backend_for(state.config, pending.before.position.reserve.block)
    backend.snapshot = lambda *args: pending.before
    monkeypatch.setattr(
        "almanak.gateway.execution.time.time",
        lambda: pending.before.position.reserve.block.timestamp,
    )
    backend.reader.responses["eth_call"] = (
        "0x" + encode(("uint256", "uint256"), (1, pending.operation.amount.amount)).hex()
    )
    assert backend.validate_current(pending) == pending.before
    call = backend.reader.calls[-1]
    assert call[1][0]["gas"] == hex(pending.gas)
    backend.snapshot = lambda *args: pending.before.model_copy(update={"allowance": 0})
    with pytest.raises(DataError, match="allowance changed"):
        backend.validate_current(pending)


@pytest.mark.parametrize("known", [True, False])
def test_broadcast_never_invents_another_transaction(production_attempt, known):
    state, pending, _ = production_attempt
    backend = backend_for(state.config, pending.before.position.reserve.block)
    backend.validate_pending = lambda p: None
    backend.validate_current = lambda p: p.before
    backend.reader.responses = {
        "eth_getTransactionByHash": {} if known else None,
        "eth_getCode": "0x",
        "eth_getTransactionCount": hex(pending.nonce),
        "eth_sendRawTransaction": pending.tx_hash,
    }
    assert backend.broadcast(pending) is (not known)
    sends = [
        params for method, params in backend.reader.calls if method == "eth_sendRawTransaction"
    ]
    assert sends == ([] if known else [[pending.raw_transaction]])
    backend.reader.responses["eth_getTransactionByHash"] = None
    backend.reader.responses["eth_getTransactionCount"] = hex(pending.nonce + 1)
    with pytest.raises(DataError, match="recorded nonce"):
        backend.broadcast(pending)


def test_receipt_cannot_exceed_signed_gas(production_attempt):
    state, pending, outcome = production_attempt
    validate_receipt_cost(state.config, pending, outcome.receipt)
    with pytest.raises(DataError):
        validate_receipt_cost(
            state.config, pending, {**outcome.receipt, "gasUsed": hex(pending.gas + 1)}
        )


def test_private_keystore_files_and_operator_creation(tmp_path, production_recorded, monkeypatch):
    signer = Signer.from_key(TEST_KEY)
    monkeypatch.setattr("almanak.gateway.credentials.Signer.create", lambda: signer)
    monkeypatch.setattr("almanak.gateway.credentials.getpass.getpass", lambda _: "test-password")
    directory = tmp_path / "operator-test"
    result = create_wallet(directory, "lending_0")
    assert result["address"] == signer.address
    text = private_text(directory / "keystores.json")
    assert TEST_KEY[2:] not in text and "test-password" not in text
    keys = load_production_signers(production_recorded.config, directory / "keystores.json", None)
    assert keys["lending_0"].address == signer.address
    profile = production_recorded.config.accounts.bindings[0].profile
    with pytest.raises(ValueError, match="unlocked"):
        decrypt_signer(json.loads(text)["lending_0"], "wrong", profile)
    with pytest.raises(FileExistsError):
        write_private(directory / "keystores.json", "overwrite")
    (directory / "keystores.json").chmod(0o644)
    with pytest.raises(ValueError, match="0600"):
        private_text(directory / "keystores.json")
    link = directory / "link"
    link.symlink_to(directory / "keystores.json")
    with pytest.raises(OSError):
        private_text(link)


def test_preflight_has_no_signer_and_reports_missing_funding(
    tmp_path, production_recorded, monkeypatch
):
    from almanak.gateway.production_cli import preflight

    state = production_recorded
    config_path = tmp_path / "config.json"
    config_path.write_text(state.config.model_dump_json())
    empty = state.baseline[0].model_copy(update={"wallet_balance": 0})
    reader = Reader(empty.position.reserve.block)
    backend = SimpleNamespace(
        observe=lambda: (empty,), reader=reader, require_fresh=lambda b: None, close=lambda: None
    )
    calls = []

    def read_only_constructor(
        config,
        endpoint,
        key_file=None,
        password_file=None,
        *,
        chain=None,
        password=None,
        signer_file=None,
    ):
        assert key_file is password_file is signer_file is None
        calls.append((config, endpoint))
        return backend

    monkeypatch.setattr("almanak.gateway.production_cli.ProductionGateway", read_only_constructor)
    monkeypatch.setattr(
        "almanak.gateway.production_cli.provider_endpoint", lambda *a: "https://rpc.invalid"
    )
    result = preflight(config_path, tmp_path / "unused-env")
    assert len(calls) == 1
    assert result["read_only"] and not result["funded"]
    assert result["funds"][0]["tokens"][0]["observed_token_base_units"] == "0"
    assert result["funds"][0]["tokens"][0]["required_token_base_units"] == str(
        state.config.allocations[0].capital
    )


def test_preflight_requires_the_larger_native_entry_minimum(production_recorded):
    from almanak.gateway.production_cli import fund_preflight

    state = production_recorded
    config, binding = state.config, state.config.accounts.bindings[0]
    policy = config.production.policy(binding.name)
    reserve = policy.max_transaction_fee.amount + policy.exit_reserve.amount
    result = fund_preflight(config, binding, state.baseline)
    assert result["native_admission_reserve_base_units"] == str(reserve)
    assert int(result["native_entry_minimum_base_units"]) >= reserve
    balance = next(
        o.native_balance.amount for o in state.baseline if o.target.account == binding.name
    )
    above = max(balance, reserve) + 1
    raised = config.model_copy(
        update={
            "allocations": tuple(
                a.model_copy(update={"minimum_native_balance": above})
                if a.account == binding.name
                else a
                for a in config.allocations
            )
        }
    )
    result = fund_preflight(raised, binding, state.baseline)
    assert result["native_entry_minimum_base_units"] == str(above)
    assert not result["funded"]


def test_production_launch_refuses_fork_credentials(production_recorded):
    from almanak.gateway.production_cli import validate_launch

    args = SimpleNamespace(
        env_file=Path(".env"), key_file=Path("keystores.json"), fork_key_file=None
    )
    validate_launch(args, production_recorded.config)
    args.fork_key_file = Path("fork-keys.json")
    with pytest.raises(ValueError, match="fork credentials"):
        validate_launch(args, production_recorded.config)


def avalanche_fee_evidence():
    return json.loads((Path(__file__).parent / "fixtures/avalanche-fee-history.json").read_text())


def test_captured_avalanche_observed_fee_quote_reproduces_without_a_next_block_guess():
    from almanak.gateway.production_config import GasPolicy

    captured = avalanche_fee_evidence()
    block = Block.model_validate_json(json.dumps(captured["block"]))
    policy = GasPolicy.model_validate_json(json.dumps(captured["policy"]))
    reader = Reader(block)
    reader.responses = {
        "eth_estimateGas": hex(captured["quote"]["gas_estimate"]),
        "eth_feeHistory": captured["history"],
    }
    quote = quoted_fees(reader, policy, block, captured["transaction"])
    assert {**quote, "fee_quote": quote["fee_quote"].model_dump(mode="json")} == captured["quote"]
    assert quote["fee_quote"].next_base_fee is None
    assert quote["fee_quote"].observed_base_fee == int(captured["history"]["baseFeePerGas"][0], 16)
    with pytest.raises(DataError, match="fee history"):
        decode_quote(captured["history"], block, "next_block")


def test_explicit_observed_fee_basis_is_the_same_on_anvil_and_avalanche():
    captured = avalanche_fee_evidence()
    block = Block.model_validate_json(json.dumps(captured["block"]))
    one = decode_quote(captured["history"], block, "observed_block")
    extra = {
        **captured["history"],
        "baseFeePerGas": [*captured["history"]["baseFeePerGas"], "0xffff"],
    }
    assert decode_quote(extra, block, "observed_block") == one
    with pytest.raises(ValueError, match="exactly one"):
        FeeQuote(block=block, next_base_fee=1, observed_base_fee=2, priority_fee=0)


def test_rate_limited_broadcast_retains_identity_for_retry(production_attempt, monkeypatch):
    import httpx

    from almanak.gateway.ethereum import EthereumReader
    from almanak.gateway.rate_limits import VARIABLES

    state, pending, _ = production_attempt
    monkeypatch.setenv(VARIABLES["rpc"][0], "5")
    monkeypatch.setattr("almanak.gateway.rate_limits._windows", {})
    clock = [100.0]
    monkeypatch.setattr("almanak.gateway.rate_limits.monotonic", lambda: clock[0])
    calls = []

    def respond(request):
        body = json.loads(request.content)
        calls.append(body)
        result = {
            "eth_getTransactionByHash": None,
            "eth_getCode": "0x",
            "eth_getTransactionCount": hex(pending.nonce),
            "eth_sendRawTransaction": pending.tx_hash,
        }[body["method"]]
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": result})

    backend = backend_for(state.config, pending.before.position.reserve.block)
    backend.validate_pending = lambda _: None
    backend.validate_current = lambda p: p.before
    retained = pending.model_dump_json()
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        backend.reader = EthereumReader(
            pending.call.fund.chain, "https://fixture.invalid", client=client
        )
        backend.reader.provider_limit.admit()
        with pytest.raises(DataError) as error:
            backend.broadcast(pending)
        assert error.value.kind == "rate_limit"
        assert not any(r["method"] == "eth_sendRawTransaction" for r in calls)
        assert pending.model_dump_json() == retained
        clock[0] += 1
        assert backend.broadcast(pending)
    sends = [r["params"] for r in calls if r["method"] == "eth_sendRawTransaction"]
    assert sends == [[pending.raw_transaction]]
    assert pending.model_dump_json() == retained
