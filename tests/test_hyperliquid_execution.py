"""The HyperCore gateway adapter offline, against the synthesized model (blueprint 30, 13.2).

The adapter runs its own steps: custody operations through `EvmGateway`'s preparation,
signing and receipt path over a fake HyperEVM chain whose blocks drive the in-process
HyperCore model (as `tests/test_hypercore_trading.py` does), and venue actions through
admission, the agent signer, `/exchange` on the model's loopback service and recovery
from the attempts' own evidence. Each outcome is committed with the runtime's own commit
functions and admitted by the gateway's own verification of it, so every document change
here is one the gateway re-derives. Every HyperCore effect is synthesized.

No `Runtime` tick loop runs here: a decision needs the sandboxed strategy worker and a
reference strategy, which section 13.4's fork harness supplies.
"""

import json
import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from eth_abi import encode
from eth_account import Account as Key
from eth_utils import keccak

from almanak import external_effects
from almanak.caip import ChainId
from almanak.connectors.composition import connector_for
from almanak.connectors.hyperliquid import corewriter as cw
from almanak.connectors.hyperliquid import trading as rules
from almanak.connectors.hyperliquid import venue_reads
from almanak.connectors.hyperliquid.assets import USDC
from almanak.connectors.hyperliquid.authority import declared
from almanak.connectors.hyperliquid.external import ExternalEffects
from almanak.connectors.hyperliquid.fork_model import HyperCoreModel
from almanak.connectors.hyperliquid.fork_service import ModelService
from almanak.connectors.hyperliquid.lifecycle import Lifecycle, inserted
from almanak.connectors.hyperliquid.models import (
    Allocation,
    ClosePosition,
    Config,
    IncreasePosition,
    SetLeverage,
    Withdraw,
)
from almanak.connectors.hyperliquid.models import Deposit as DepositIntent
from almanak.connectors.hyperliquid.operations import (
    AgentApproval,
    Approve,
    ClassTransfer,
    Deposit,
    StandardMode,
    VenueAction,
    Withdrawal,
    core_call,
)
from almanak.connectors.hyperliquid.planning import completes, validate_step
from almanak.connectors.hyperliquid.reporting import Projection
from almanak.connectors.hyperliquid.requests import Requests
from almanak.connectors.hyperliquid.state import (
    CustodyOutcome,
    DecisionRecord,
    TransactionOutcome,
    TransactionPlan,
    VenueOutcome,
    VenuePlan,
)
from almanak.gateway import admission
from almanak.gateway import external_effects as gateway_effects
from almanak.gateway import requests as gateway_requests
from almanak.gateway.environments import ForkBinding
from almanak.gateway.ethereum import DataError
from almanak.gateway.fenced import Writes
from almanak.gateway.fork_venues import fork_adapter
from almanak.gateway.hypercore_agent_keys import LocalAgentKeys
from almanak.gateway.hypercore_exchange import HyperCoreExchange
from almanak.gateway.hypercore_gateway import (
    AgentKeySigner,
    HyperCoreExecution,
    HyperCoreVenue,
    hypercore_venue,
)
from almanak.gateway.hypercore_info import HyperCoreInfo
from almanak.gateway.service import GatewayService
from almanak.gateway.signers import VerifiedSigner
from almanak.models import (
    Account,
    AccountBinding,
    AssetAmount,
    Block,
    SafeRoles,
    StrategyAccounts,
)
from almanak.request_runtime import commit_request
from almanak.runtime import commit_outcome
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import Memory
from almanak.triggers import IntervalEvent, IntervalTrigger
from tests.hypercore_trading_support import seed_model
from tests.test_hypercore_custody import (
    SAFE,
    Chain,
    action_log,
    credit_log,
    hype_log,
    payment,
    usdc_credit,
)
from tests.test_hyperliquid_connector import FORK, INSTANCE, ORIGIN, market

KEY = "0x" + "4f" * 32
OPERATOR = Key.from_key(KEY).address.lower()
ROLES = "0x" + "34" * 20
CAPITAL = 30_000_000
RETURN_DATA = keccak(
    text="execTransactionWithRoleReturnData(address,uint256,bytes,uint8,bytes32,bool)"
)[:4].hex()
APPROVE = keccak(text="approve(address,uint256)")[:4].hex()


def config():
    profile = SafeRoles(
        kind="safe_roles",
        fund=Account(chain=FORK, address=SAFE),
        operator=Account(chain=FORK, address=OPERATOR),
        roles_module=ROLES,
        role_key="0x" + "56" * 32,
    )
    allocation = Allocation(
        name="perp",
        account="safe",
        chain=FORK,
        market=market(),
        capital=AssetAmount(asset=USDC, amount=CAPITAL),
        leverage=10,
        slippage_bps=50,
    )
    return Config(
        decision_trigger=IntervalTrigger(kind="interval", seconds=60),
        accounts=StrategyAccounts(bindings=(AccountBinding(name="safe", profile=profile),)),
        allocations=(allocation,),
        fork_bindings=(ForkBinding(chain=FORK, origin=ORIGIN, instance=INSTANCE),),
    )


class Evm:
    """The HyperEVM fork as the adapter reads it: the custody tests' chain of blocks, the
    Safe's USDC and allowance, and the calls that preparation simulates and estimates."""

    def __init__(self, blocks: Chain):
        self.blocks = blocks
        self.chain = FORK
        self.transport_chain = ChainId("eip155:31337")
        self.wallet = CAPITAL
        self.allowance = 0
        self.nonce = 0
        self.receipts: dict[str, dict] = {}

    def block(self, number: int) -> Block:
        timestamp = self.blocks.blocks[number]["timestamp"]
        return Block(chain=FORK, number=number, hash=f"0x{number:064x}", timestamp=timestamp)

    def pin_block(self, selector) -> Block:
        return self.block(len(self.blocks.blocks) - 1 if selector == "latest" else selector)

    @contextmanager
    def retained_pin(self):
        yield

    def call(self, to, signature, block, *, inputs=(), args=(), outputs=("uint256",)):
        return (self.wallet,) if signature == "balanceOf(address)" else (self.allowance,)

    def _rpc(self, method, params, allow_none=False):
        answers = {
            "eth_call": lambda: self.simulated(params[0]["data"]),
            "eth_getTransactionCount": lambda: hex(self.nonce),
            "eth_estimateGas": lambda: hex(100_000),
            "eth_feeHistory": lambda: {"oldestBlock": params[1], "baseFeePerGas": ["0x1", "0x1"]},
            "eth_getBalance": lambda: hex(10**18),
            "eth_getBlockByNumber": lambda: self.header(params[0]),
            "eth_getTransactionReceipt": lambda: (
                self.receipts.get(params[0]) or self.blocks(method, params)
            ),
        }
        answer = answers.get(method)
        return answer() if answer is not None else self.blocks(method, params)

    def header(self, tag):
        number = len(self.blocks.blocks) - 1 if tag == "latest" else int(tag, 16)
        found = self.blocks("eth_getBlockByNumber", [hex(number), False])
        if found is None:
            return None
        parent = f"0x{max(number - 1, 0):064x}"
        # 0.1 gwei, the base fee the canary paid its payouts at.
        header = {"hash": f"0x{number:064x}", "parentHash": parent, "gasLimit": "0x1"}
        return {**found, **header, "baseFeePerGas": hex(10**8)}

    def simulated(self, data: str) -> str:
        """Roles returns the inner call's success and data; an approve returns true."""
        if not data[2:].startswith(RETURN_DATA):
            return "0x"
        returned = encode(("bool",), (True,)) if APPROVE in data else b""
        return "0x" + encode(("bool", "bytes"), (True, returned)).hex()


class OfflineGateway(HyperCoreExecution):
    """The adapter over the fake chain; Roles storage verification is the fork's to prove."""

    def guard(self):
        return None

    def verify_authority(self, manifest, account):
        return None


class World:
    def __init__(self, tmp_path, service, model, blocks):
        self.model, self.blocks = model, blocks
        self.evm = Evm(blocks)
        self.keys = LocalAgentKeys(tmp_path / "agent-keys.json", SAFE)
        clock = self.now
        self.venue = HyperCoreVenue(
            HyperCoreInfo(model=service.url, clock=clock),
            HyperCoreExchange(model=service.url),
            clock,
        )
        self.config = config()
        self.gateway = gateway_for(self.config, self.evm, self.venue, self.keys)

    def now(self) -> int:
        return self.blocks.blocks[-1]["timestamp"] * 1000

    def mine(self, seconds=1, transactions=(), logs=()):
        timestamp = self.blocks.blocks[-1]["timestamp"] + seconds
        return self.blocks.mine(timestamp, transactions, logs)


def gateway_for(configured, evm, venue, keys):
    gateway = object.__new__(OfflineGateway)
    gateway.config = configured
    gateway.clock = lambda: evm.pin_block("latest").timestamp
    gateway.reader = evm
    signer = AgentKeySigner(Key.from_key(KEY), keys)
    gateway.signers = {"safe": VerifiedSigner(signer, OPERATOR)}
    gateway.verified_authority = {}
    gateway.node_readings = {}
    gateway.node_readings_lock = threading.Lock()
    gateway.authority_reads = {}
    gateway.attach(venue)
    return gateway


@pytest.fixture
def world(tmp_path):
    model = HyperCoreModel(tmp_path / "model.json", 10)
    seed_model(model)
    with ModelService(model) as service:
        found = World(tmp_path, service, model, Chain(model))
        yield found
        found.venue.close()


def initial(world):
    state = Lifecycle.initial(
        world.config,
        StrategyDefinition(filename="strategy.py", source=""),
        declared(world.config),
        None,
    )
    return state.model_copy(update={"initialized": True})


def observed(world, state):
    (snapshot,) = world.gateway.observe(state)
    return snapshot


def decide(world, state, intent):
    started = datetime.fromtimestamp(world.now() / 1000, UTC)
    record = DecisionRecord(
        trigger=IntervalEvent(started_at=started, due_at=started),
        identity=uuid4().hex,
        intent=intent,
        observations=(observed(world, state),),
        memory=Memory(),
    )
    return state.model_copy(update={"decision": record})


def delivered(state):
    return state.model_copy(update={"delivered": len(state.outcomes)})


def planned(world, state):
    """The gateway's plan of the next step, which its own re-preparation reproduces."""
    before = observed(world, state)
    intent = state.decision.intent
    plan = world.gateway.prepare(state, intent, before, uuid4().hex)
    assert world.gateway.prepare(state, intent, before, plan.attempt_id) == plan
    return plan


def signed(world, state, plan):
    reserved = state.model_copy(update={"plan": plan})
    pending = world.gateway.sign(reserved)
    assert world.gateway.unsigned_plan(pending) == plan
    assert world.gateway.verify_signed_admission(reserved, pending)
    return state.model_copy(update={"pending": pending})


def settled(world, state):
    """The pending step's outcome, committed as the runtime commits it and admitted by the
    gateway's independent re-derivation of it."""
    pending = state.pending
    receipt = world.gateway.receipt(pending)
    assert receipt is not None
    outcome = world.gateway.reconcile(pending, receipt)
    after = commit_outcome(state, pending, outcome)
    admission.validate_outcome(world.gateway, state, after)
    return delivered(after), outcome


# Custody: one HyperEVM call per step, mined on the fake chain the model follows.


def mined_logs(world, operation):
    if isinstance(operation, Approve):
        world.evm.allowance = operation.amount.amount
        return []
    if isinstance(operation, Deposit):
        world.evm.wallet -= operation.amount.amount
        world.evm.allowance -= operation.amount.amount
        return [credit_log(SAFE, operation.amount.amount)]
    return [action_log(SAFE, core_call(operation))]


def custody_step(world, state):
    plan = planned(world, state)
    assert isinstance(plan, TransactionPlan)
    state = signed(world, state, plan)
    pending = state.pending
    transaction = {"hash": pending.tx_hash, "from": OPERATOR, "to": ROLES, "input": "0x"}
    mined = world.mine(5, [transaction], mined_logs(world, plan.operation))
    number = int(mined["blockNumber"], 16)
    world.evm.receipts[pending.tx_hash] = {
        "transactionHash": pending.tx_hash,
        "blockHash": world.evm.block(number).hash,
        "blockNumber": hex(number),
        "status": "0x1",
        "from": OPERATOR,
        "to": ROLES,
        "gasUsed": hex(50_000),
        "effectiveGasPrice": hex(pending.gas_price),
        "logs": mined["logs"],
    }
    world.evm.nonce += 1
    if isinstance(plan.operation, Withdrawal):
        pay_withdrawal(world)
    return settled(world, state)


def pay_withdrawal(world):
    """HyperCore pays a withdrawal in the next block from the USDC system address."""
    (owed,) = world.model.doc["deliveries"]
    pay = {"from": cw.USDC_SYSTEM, "to": cw.CORE_DEPOSIT_WALLET, "input": payment(owed["amount"])}
    world.mine(1, [pay], [usdc_credit(SAFE, owed["amount"])])
    world.evm.wallet += owed["amount"]


def resolved(world, state):
    """The custody request's outcome once the model's evidence identifies it."""
    outcome = Requests.reconcile(world.gateway, state)
    for _ in range(3):
        if outcome is not None:
            break
        world.mine(1)
        outcome = Requests.reconcile(world.gateway, state)
    assert isinstance(outcome, CustodyOutcome)
    after = commit_request(state, state, outcome)
    gateway_requests.verified_commit(world.gateway, state, after)
    return delivered(after), outcome


def effects(world, state):
    """The next external-effects page, committed and verified by its ranges read again."""
    page = ExternalEffects.reconcile(world.gateway, state)
    assert page is not None and page.outcome is not None
    after = external_effects.commit(state, state, page)
    gateway_effects.verified_commit(world.gateway, state, after)
    return delivered(after), page.outcome


def run_custody(world, state, kinds):
    """Each custody step the intent plans, in order, with its request resolved."""
    for kind in kinds:
        state, outcome = custody_step(world, state)
        assert outcome.operation.kind == kind and not outcome.complete
        if kind != "hyperliquid_approve":
            state, outcome = resolved(world, state)
            assert outcome.operation.kind == kind
    return state, outcome


def deposited(world):
    state = decide(world, initial(world), DepositIntent(target=target(world), amount=CAPITAL))
    state, outcome = run_custody(
        world,
        state,
        (
            "hyperliquid_approve",
            "hyperliquid_deposit",
            "hyperliquid_standard_mode",
            "hyperliquid_class_transfer",
        ),
    )
    assert outcome.complete and state.decision is None
    # The Safe owners fund HyperCore HYPE for withdrawals' system transactions.
    world.mine(
        1, [{"from": OPERATOR, "to": cw.HYPE_SYSTEM, "input": "0x"}], [hype_log(SAFE, 10**16)]
    )
    return state


def target(world):
    return world.config.target("perp")


def usdc(amount):
    return AssetAmount(asset=USDC, amount=amount)


def test_a_deposit_runs_its_custody_steps_and_resolves_each_request(world):
    state = deposited(world)
    custody = state.custody
    assert [r.kind for r in custody.requests] == ["deposit", "standard_mode", "class_transfer"]
    assert all(r.status == "resolved" for r in custody.requests)
    (owned,) = state.owned
    assert owned.inventory == (usdc(0),) and owned.hypercore == 0
    # The first credit paid HyperCore's 1 USDC activation fee, so 29 USDC moved to perp.
    transfer = next(o for o in state.outcomes if isinstance(o.operation, ClassTransfer))
    assert transfer.operation.amount == usdc(29_000_000)
    state, page = effects(world, state)
    assert {e.entry["delta"]["type"] for e in page.effects} >= {
        "spotTransfer",
        "accountClassTransfer",
    }
    assert state.owned[0].hypercore == Decimal(29) and state.book.cash == Decimal(29)
    assert ExternalEffects.reconcile(world.gateway, state) is None
    # What a backtest's account shows: activated by the resolved deposit, flat, no
    # leverage set yet, and the whole perp balance withdrawable.
    (owned,) = state.owned
    assert (owned.activated, owned.leverage, owned.holding) == (True, None, None)
    assert owned.withdrawable == 29_000_000 == observed(world, state).withdrawable


def entered(world, state, intent):
    """An entry from an account that has only deposited: a fresh agent's approval, then
    cross leverage, which the entry inserts since the market reads another."""
    state = decide(world, state, intent)
    state, approval = run_custody(world, state, ("hyperliquid_agent_approval",))
    assert isinstance(approval.operation, AgentApproval) and approval.strategy_result().applied
    (agent,) = state.custody.agents.approvals
    assert agent.status == "active" and world.keys.held({agent.address})
    world.mine(1)
    state, leverage = venue_step(world, state)
    assert leverage.attempt.purpose == "leverage" and leverage.success
    assert not leverage.complete and state.decision is not None
    return state


def sent(world, state):
    """Record the send durably, then post: as the service does before `submit`."""
    world.gateway.prepare_submission(state)
    pending = state.pending.model_copy(update={"unsent": False})
    return state.model_copy(update={"pending": pending})


def venue_step(world, state, fault=None):
    plan = planned(world, state)
    assert isinstance(plan, VenuePlan) and isinstance(plan.operation, VenueAction)
    state = sent(world, signed(world, state, plan))
    if fault is not None:
        fault()
    state, posted = world.gateway.submit(state)
    assert posted
    while state.pending.resolved is None:
        # A lost answer never posts again: the attempt's own evidence resolves it.
        state, posted = world.gateway.submit(state)
        assert not posted
    return settled(world, state)


def test_an_entry_and_its_exit_trade_through_the_agent_and_withdraw(world):
    state = deposited(world)
    state, _ = effects(world, state)
    intent = IncreasePosition(target=target(world), side="long", size="0.002")
    state = entered(world, state, intent)
    # The entry's answer is withheld; `orderStatus` of its cloid resolves it.
    state, entry = venue_step(world, state, lambda: world.model.withhold("order"))
    assert entry.attempt.status == "applied" and entry.attempt.answer is None
    assert entry.attempt.evidence and entry.complete and state.decision is None
    result = entry.strategy_result()
    assert (result.applied, result.attempt, result.tx_hash) == (True, entry.attempt.id, None)
    assert state.trading.attempt(entry.attempt.id) == entry.attempt
    # The fill is the strategy's own by its cloid: the book, then ownership, hold it.
    assert state.owned[0].leverage == 10 and state.owned[0].holding is None
    state, page = effects(world, state)
    assert any(e.kind == "fill" and e.owned for e in page.effects)
    (owned,) = state.owned
    assert owned.position == Decimal("0.002") and owned.holding.szi == "0.002"
    fills = [e for e in page.effects if e.kind == "fill"]
    paid = sum(Decimal(f.entry["sz"]) * Decimal(f.entry["px"]) for f in fills)
    assert Decimal(owned.holding.entry_px) == paid / Decimal("0.002")
    assert owned.holding.changed_at == max(f.time for f in fills) // 1000
    # Valued at its entry, the position keeps a tenth of its notional (10x cross).
    kept = paid / 10
    assert owned.withdrawable == int((state.book.cash - kept) * 10**6)
    state = decide(world, state, ClosePosition(target=target(world)))
    state, exit_order = venue_step(world, state)
    assert exit_order.attempt.request.action.order.reduce_only and exit_order.complete
    state, _ = effects(world, state)
    assert state.owned[0].position == 0 and state.book.positions == {}
    (owned,) = state.owned
    # Flat, the projection is exactly the venue's own withdrawable perp USDC.
    assert owned.holding is None and owned.leverage == 10
    assert owned.withdrawable == venue_reads.withdrawable(world.gateway.venue, SAFE) > 0
    state = withdrawn(world, state)
    assert state.owned[0].hypercore == 0 and state.owned[0].withdrawable == 0
    assert state.owned[0].activated
    world.gateway.require_empty_venue(state.model_copy(update={"mode": "closed"}))


def withdrawn(world, state):
    state = decide(world, state, Withdraw(target=target(world)))
    state, outcome = run_custody(
        world, state, ("hyperliquid_class_transfer", "hyperliquid_withdrawal")
    )
    assert outcome.complete and state.decision is None
    (paid,) = outcome.assets
    assert paid.amount > 0 and state.owned[0].inventory == (usdc(paid.amount),)
    state, _ = effects(world, state)
    return state


def test_reports_keep_custody_gas_apart_from_venue_attempts_and_request_evidence(world):
    state = deposited(world)
    state, _ = effects(world, state)
    intent = IncreasePosition(target=target(world), side="long", size="0.002")
    state, entry = venue_step(world, entered(world, state, intent))
    report = Projection.report(state)
    # Only custody transactions carry a hash, a nonce and the gas their submitter paid.
    transactions = [o for o in state.outcomes if isinstance(o, TransactionOutcome)]
    (payer,) = report["gas_payers"]
    assert payer["nonces"] == [o.nonce for o in transactions]
    assert payer["gas_paid"]["amount"] == str(sum(o.gas_paid.amount for o in transactions))
    sources = [o["source"] for o in report["outcomes"]]
    assert set(sources) == {"submitter", "venue_action", "hypercore_evidence"}
    last = report["outcomes"][-1]
    assert (last["source"], last["attempt"]) == ("venue_action", entry.attempt.id)
    assert report["transaction_accounting_complete"] and report["accounting_issues"] == []
    assert report["trading"] == state.trading.model_dump(mode="json")
    uninitialized = Projection.report(state.model_copy(update={"initialized": False}))
    assert uninitialized["gas_payers"][0]["gas_paid"]["amount"] is None
    flagged = entry.model_copy(update={"accounting_issues": ("fee unmeasured",)})
    issues = Projection.report(state.model_copy(update={"outcomes": (*state.outcomes, flagged)}))
    assert issues["accounting_issues"] == [
        {"source": "venue_action", "attempt": entry.attempt.id, "issues": ["fee unmeasured"]}
    ]
    assert not issues["transaction_accounting_complete"]
    context, _ = Projection.dashboard(state)
    (position,) = context.positions
    assert [v.amount for v in position.inventory] == ["0"]
    rows = context.datasets["transactions"].rows
    assert len(rows) == len(transactions)
    assert rows[1]["transaction"] == "Deposit USDC to HyperCore"
    assert rows[-1]["transaction"] == "Approve a HyperCore agent"
    with pytest.raises(ValueError, match="invalidated"):
        Projection.dashboard(state.model_copy(update={"integrity_error": "reorg"}))


def test_a_refused_admission_replans_and_an_unmatched_ioc_resolves_cleanly(world):
    state = deposited(world)
    state, _ = effects(world, state)
    intent = IncreasePosition(target=target(world), side="long", size="0.002")
    state = entered(world, state, intent)
    plan = planned(world, state)
    world.mine(10)
    # The book admission priced it from is now older than its 5 second limit.
    with pytest.raises(DataError) as refused:
        world.gateway.sign(state.model_copy(update={"plan": plan}))
    assert refused.value.kind == "price_movement"
    # The market moves away between signing and the post: the Ioc matches nothing.
    state, entry = venue_step(
        world, state, lambda: world.model.move_mark("BTC", "90000", "synthesized jump")
    )
    assert entry.attempt.status == "not_applied" and entry.success and entry.complete
    assert entry.strategy_result().applied is False and state.decision is None


def test_an_attempt_with_no_evidence_waits_then_follows_the_trading_policy(world):
    state = deposited(world)
    state, _ = effects(world, state)
    intent = IncreasePosition(target=target(world), side="long", size="0.002")
    state = entered(world, state, intent)
    plan = planned(world, state)
    state = sent(world, signed(world, state, plan))
    world.model.lose("order")
    lost, posted = world.gateway.submit(state)
    assert posted and lost == state
    # Before `expiresAfter` nothing resolves it, and nothing posts it again.
    assert world.gateway.submit(state) == (state, False)
    world.mine(40)
    # Mainnet keeps T4 open: past its expiry with no record the deployment halts.
    open_t4 = state.trading.policy.model_copy(update={"release_unknown_after_expiry": False})
    strict = state.model_copy(
        update={"trading": state.trading.model_copy(update={"policy": open_t4})}
    )
    with pytest.raises(DataError) as halted:
        world.gateway.submit(strict)
    assert halted.value.kind == "unknown_outcome"
    # The fork model states A4, so the order is non-execution and the intent ends cleanly.
    released, posted = world.gateway.submit(state)
    assert not posted and released.pending.resolved.status == "not_applied"
    after, outcome = settled(world, released)
    assert isinstance(outcome, VenueOutcome) and outcome.success and outcome.complete
    assert after.trading.attempt(outcome.attempt.id).status == "not_applied"


def test_a_never_sent_action_is_released_and_one_maybe_sent_never_is(world):
    state = deposited(world)
    state, _ = effects(world, state)
    intent = IncreasePosition(target=target(world), side="long", size="0.002")
    state = entered(world, state, intent)
    unsent = signed(world, state, planned(world, state))
    world.gateway.require_releasable(unsent.pending)
    world.mine(40)
    with pytest.raises(DataError) as stale:
        world.gateway.prepare_submission(unsent)
    assert stale.value.kind == "stale_plan"
    maybe = unsent.pending.model_copy(update={"unsent": False})
    with pytest.raises(ValueError, match="never released"):
        world.gateway.require_releasable(maybe)
    with pytest.raises(ValueError, match="durably recorded"):
        world.gateway.submit(unsent)


def test_routes_name_the_hypercore_venue_of_a_hyperliquid_deployment(world, tmp_path):
    venue = {"model": "http://127.0.0.1:1", "agent_keys": str(tmp_path / "keys.json")}
    assert hypercore_venue({"hypercore": venue}) == venue
    for refused in ({}, {"hypercore": venue, "other": {}}, {"hypercore": {"model": "x"}}):
        with pytest.raises(ValueError, match="HyperCore model endpoint"):
            hypercore_venue(refused)
    # The generic route seam asks the connector's fork binding for its adapter.
    with pytest.raises(ValueError, match="HyperCore model endpoint"):
        fork_adapter(world.config, "http://127.0.0.1:1", tmp_path / "fork-keys.json")


def test_documents_admit_only_the_rules_next_attempt_and_the_intents_own_steps(world):
    state = deposited(world)
    _, attempt = rules.plan_leverage(state.trading, "BTC", "0x" + "a1" * 20, world.now())
    inserted(state.trading, attempt)
    reused = state.trading.model_copy(update={"nonces": {attempt.agent: attempt.request.nonce}})
    stale = (
        attempt.model_copy(update={"id": "2"}),
        attempt.model_copy(update={"status": "sent"}),
        attempt.model_copy(update={"oids": (1,)}),
    )
    for changed in stale:
        with pytest.raises(ValueError, match="next plan"):
            inserted(state.trading, changed)
    with pytest.raises(ValueError, match="next plan"):
        inserted(reused, attempt)
    with pytest.raises(ValueError, match="unresolved"):
        inserted(inserted(state.trading, attempt), attempt)
    deposit = DepositIntent(target=target(world), amount=CAPITAL)
    again = decide(world, state, deposit)
    for operation, reason in (
        (Deposit(target=target(world), amount=usdc(1)), "sends its own amount once"),
        (Approve(target=target(world), amount=usdc(1)), "approved before it"),
        (StandardMode(target=target(world)), "standard mode is set once"),
        (Withdrawal(target=target(world), amount=usdc(1)), "no step of a deposit"),
        (VenueAction(target=target(world), attempt=attempt), "no step of a deposit"),
    ):
        with pytest.raises(ValueError, match=reason):
            validate_step(again, deposit, operation)
    leverage = SetLeverage(target=target(world), leverage=10)
    with pytest.raises(ValueError, match="no step of a set_leverage"):
        validate_step(decide(world, state, leverage), leverage, StandardMode(target=target(world)))
    close = ClosePosition(target=target(world))
    with pytest.raises(ValueError, match="signs no leverage action"):
        validate_step(
            decide(world, state, close), close, VenueAction(target=target(world), attempt=attempt)
        )


def test_the_service_commits_the_answer_a_post_recorded_under_its_lock(world):
    state = deposited(world)
    state, _ = effects(world, state)
    intent = IncreasePosition(target=target(world), side="long", size="0.002")
    state = entered(world, state, intent)
    state = sent(world, signed(world, state, planned(world, state)))
    service = object.__new__(GatewayService)
    service.route_chain = None
    service.backend = world.gateway
    service.connector = connector_for(world.config)
    # `__init__` always sets the lease; the send refuses checks older than it.
    service.lease_seconds = 30
    writes = Writes(state.model_dump_json())
    assert service.submit_prepared(writes, state, state) is True
    recorded = writes.after.pending
    assert recorded.answer is not None and recorded.resolved.status == "applied"
    assert writes.after == state.model_copy(update={"pending": recorded})
    # Nothing is recorded, and nothing posts again, once the answer is the record.
    again = Writes(writes.after.model_dump_json())
    assert service.submit_prepared(again, writes.after, writes.after) is False
    assert again.after is None


# Phase 5's intents, each planned as the steps its fork run needs.


def test_set_leverage_approves_an_agent_and_completes_once_the_leverage_applies(world):
    state = deposited(world)
    state, _ = effects(world, state)
    state = decide(world, state, SetLeverage(target=target(world), leverage=10))
    state, approval = run_custody(world, state, ("hyperliquid_agent_approval",))
    assert approval.strategy_result().applied and state.decision is not None
    world.mine(1)
    state, leverage = venue_step(world, state)
    assert leverage.attempt.purpose == "leverage" and leverage.attempt.status == "applied"
    assert leverage.complete and state.decision is None
    # The market now reads the configured leverage, so the entry is the intent's one step.
    intent = IncreasePosition(target=target(world), side="short", size="0.002")
    state = decide(world, state, intent)
    state, entry = venue_step(world, state)
    assert entry.attempt.purpose == "entry" and entry.complete and state.decision is None
    assert not entry.attempt.request.action.order.is_buy
    state, _ = effects(world, state)
    assert state.owned[0].position == Decimal("-0.002")


def test_a_leverage_intent_completes_only_once_its_step_applies(world):
    state = deposited(world)
    _, attempt = rules.plan_leverage(state.trading, "BTC", "0x" + "a1" * 20, world.now())
    action = VenueAction(target=target(world), attempt=attempt)
    leverage = SetLeverage(target=target(world), leverage=10)
    assert completes(leverage, action) and not completes(leverage, action, applied=False)
    # Before an entry, the same step never completes the entry's intent.
    entry = IncreasePosition(target=target(world), side="long", size="0.002")
    assert not completes(entry, action)


def test_a_withdrawal_of_an_amount_moves_and_sends_exactly_that_amount(world):
    state = deposited(world)
    state, _ = effects(world, state)
    too_much = decide(world, state, Withdraw(target=target(world), amount=CAPITAL))
    with pytest.raises(DataError, match="withdrawable") as refused:
        planned(world, too_much)
    assert refused.value.kind == "inventory"
    state = decide(world, state, Withdraw(target=target(world), amount=5_000_000))
    state, outcome = run_custody(
        world, state, ("hyperliquid_class_transfer", "hyperliquid_withdrawal")
    )
    assert outcome.complete and state.decision is None
    moved = [
        o.operation
        for o in state.outcomes
        if isinstance(getattr(o, "operation", None), ClassTransfer)
    ]
    assert moved[-1].amount == usdc(5_000_000) and not moved[-1].to_perp
    (paid,) = outcome.assets
    assert paid.amount == 5_000_000 and state.owned[0].inventory == (usdc(5_000_000),)
    state, _ = effects(world, state)
    assert state.owned[0].hypercore == Decimal(24)


def test_a_first_deposit_no_larger_than_the_activation_fee_is_refused_unsent(world):
    state = decide(world, initial(world), DepositIntent(target=target(world), amount=1_000_000))
    with pytest.raises(DataError, match="activation fee") as refused:
        planned(world, state)
    assert refused.value.kind == "inventory" and not state.outcomes


def test_an_entry_whose_size_rounds_to_nothing_is_refused_before_it_is_signed(world):
    state = deposited(world)
    state, _ = effects(world, state)
    tiny = IncreasePosition(target=target(world), side="long", size="0.000001")
    state = entered(world, state, tiny)
    with pytest.raises(DataError, match="rounds to nothing") as refused:
        planned(world, state)
    assert refused.value.kind == "order_size"


def test_a_withdrawal_waits_for_hypercore_hype_to_pay_its_payout(monkeypatch):
    """HyperCore drops a withdrawal its account cannot pay the payout of, so planning one
    needs twice the fee: 200,000 gas at the 0.1 gwei base fee is 0.00002 HYPE."""
    from types import SimpleNamespace

    from almanak.connectors.hyperliquid import planning

    backend = SimpleNamespace(base_fee=lambda block: 10**8, venue=None)
    before = SimpleNamespace(block=None, target=SimpleNamespace(fund=SimpleNamespace(address=SAFE)))
    held = {"hype": Decimal("0.00003999")}
    monkeypatch.setattr(planning.venue_reads, "spot_hype", lambda venue, safe: held["hype"])
    with pytest.raises(DataError) as refused:
        planning.require_payout_fee(backend, before)
    assert refused.value.kind == "funding_unavailable"
    assert refused.value.details == {"hypercore_hype": "0.00003999", "required": "0.00004"}
    held["hype"] = Decimal("0.00004")
    planning.require_payout_fee(backend, before)


def production_gateway(monkeypatch, *args, **fields):
    """The production adapter over a stand-in for the shared EVM production gateway, whose
    provider and keystore checks are its own tests'; this checks what the adapter adds."""
    from almanak.gateway import hypercore_gateway as hg

    passwords = []

    def shared(self, configured, endpoint, key_file, password_file=None, **options):
        self.config = configured
        self.signers = {} if key_file is None else {"safe": Key.from_key(KEY)}
        passwords.append(options.get("password"))

    monkeypatch.setattr(hg.ProductionGateway, "__init__", shared)
    return hg.HyperCoreProductionGateway(config(), "https://rpc", *args, **fields), passwords


def test_production_keeps_agent_keys_encrypted_beside_the_operator_keystores(tmp_path, monkeypatch):
    from almanak.gateway import hypercore_gateway as hg

    gateway, passwords = production_gateway(monkeypatch, tmp_path / "keystores.json", password="pw")
    try:
        assert passwords == ["pw"]
        assert gateway.venue.info.api == "https://api.hyperliquid.xyz/info"
        assert gateway.venue.exchange.api == "https://api.hyperliquid.xyz/exchange"
        signer = gateway.signers["safe"]
        agent = signer.generate_hyperliquid_agent(SAFE)
        stored = json.loads((tmp_path / hg.AGENT_KEY_FILE).read_text())[SAFE][agent]
        # Kept as an encrypted keystore under the operator password, never as the key.
        assert Key.from_key(Key.decrypt(stored, "pw")).address.lower() == agent
        assert signer.hyperliquid_agents(SAFE, frozenset({agent})) == {agent}
        locked = hg.EncryptedAgentKeys(tmp_path / hg.AGENT_KEY_FILE, SAFE, "another")
        with pytest.raises(ValueError, match="could not be unlocked"):
            locked.held(frozenset({agent}))
    finally:
        gateway.venue.close()


def test_production_reads_without_keystores_and_refuses_a_remote_signer(tmp_path, monkeypatch):
    gateway, passwords = production_gateway(monkeypatch, None)
    try:
        # An online check or preflight only reads: no keystore, no password asked.
        assert gateway.signers == {} and passwords == [None]
    finally:
        gateway.venue.close()
    with pytest.raises(ValueError, match="local keystores"):
        production_gateway(monkeypatch, None, signer_file=tmp_path / "signer.json")


def test_production_routes_build_a_connectors_own_gateway(monkeypatch):
    from types import SimpleNamespace

    from almanak.gateway import production_cli

    network = SimpleNamespace(chain=FORK, provider="alchemy")
    configured = SimpleNamespace(production=SimpleNamespace(networks=(network,)))
    built = []
    monkeypatch.setattr(production_cli, "provider_endpoint", lambda provider, env: "https://rpc")
    monkeypatch.setattr(production_cli, "ProductionGateway", lambda *a, **f: ("evm", a, f))

    def own(*args, **fields):
        built.append((args, fields))
        return "hypercore"

    execution = SimpleNamespace(production_gateway=own)
    monkeypatch.setattr(
        production_cli, "connector_for", lambda c: SimpleNamespace(execution=execution)
    )
    assert production_cli.production_routes(configured, None, "keys") == "hypercore"
    assert built == [((configured, "https://rpc", "keys", None), {"chain": FORK, "password": None})]
    plain = SimpleNamespace(execution=SimpleNamespace())
    monkeypatch.setattr(production_cli, "connector_for", lambda c: plain)
    assert production_cli.production_routes(configured, None, "keys")[0] == "evm"
