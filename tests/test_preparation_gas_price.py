"""Re-preparation reproduces a plan's gas price after the node's suggestion moves."""

import json
import threading
from types import MethodType, SimpleNamespace

import httpx
import pytest

from almanak.caip import ChainId
from almanak.gateway import execution as gateway_execution
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.execution import NODE_READINGS, EvmGateway
from almanak.gateway.production_fees import pinned_gas_price
from almanak.gateway.service import GatewayService
from almanak.models import Chain, Eoa

BNB = Chain(chain_id=ChainId("eip155:56"), environment="mainnet")
ETHEREUM = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")


class Node:
    """A node whose head and gas-price suggestion move between plan and update."""

    def __init__(self, chain, head=100, base_fees=True):
        self.chain = chain
        self.head = head
        self.base_fees = base_fees
        self.gas_price = 50_000_000
        self.fee_history = []

    def header(self, number):
        return {
            "number": hex(number),
            "hash": "0x" + number.to_bytes(32).hex(),
            "timestamp": hex(1_700_000_000 + number),
        }

    def base_fee(self, number):
        # Empty blocks lower the base fee, as on an idle fork.
        return 1_000_000_000 - number * 1_000 if self.base_fees else 0

    def mine(self, gas_price):
        self.head += 1
        self.gas_price = gas_price

    def result(self, method, params):
        if method == "eth_getBlockByNumber":
            number = self.head if params[0] == "latest" else int(params[0], 16)
            return self.header(number)
        if method == "eth_feeHistory":
            self.fee_history.append(params)
            number = int(params[1], 16)
            history = {
                "oldestBlock": params[1],
                "baseFeePerGas": [hex(self.base_fee(number)), hex(self.base_fee(number + 1))],
            }
            if params[2]:
                history["reward"] = [["0x0"]]
            return history
        return {
            "eth_chainId": hex(int(str(self.chain.chain_id).split(":")[1])),
            "eth_gasPrice": hex(self.gas_price),
            "eth_getTransactionCount": "0x0",
            "eth_estimateGas": "0x5208",
        }.get(method, "0x")

    def respond(self, request):
        payload = json.loads(request.content)
        result = self.result(payload["method"], payload["params"])
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "result": result})


def gateway(node, monkeypatch, policy):
    """Real preparation, nonce and fee code over a stub connector and authority."""
    reader = EthereumReader(
        node.chain,
        "http://rpc.invalid",
        client=httpx.Client(transport=httpx.MockTransport(node.respond)),
    )
    profile = Eoa.model_construct(kind="eoa", fund=SimpleNamespace(address="0x" + "12" * 20))
    execution = SimpleNamespace(
        submitter=SimpleNamespace(address="0x" + "66" * 20),
        to="0x" + "55" * 20,
        value=0,
        data="0x",
        binding=SimpleNamespace(profile=profile),
    )
    monkeypatch.setattr(gateway_execution, "prepare_call", lambda *_: execution)
    monkeypatch.setattr(gateway_execution, "protocol_simulation", lambda _: {})
    monkeypatch.setattr(gateway_execution, "protocol_result", lambda _, result: result)
    admitted = None if policy is None else SimpleNamespace(policy=lambda _: policy)
    monkeypatch.setattr(gateway_execution, "execution_gas", lambda _: admitted)
    budgets = []
    monkeypatch.setattr(gateway_execution, "validate_budget", lambda *args: budgets.append(args))
    monkeypatch.setattr(
        "almanak.connectors.target_routing.connector_for_target",
        lambda _: SimpleNamespace(execution=SimpleNamespace()),
    )
    adapter = SimpleNamespace(
        operation=lambda *_: SimpleNamespace(account="fund", allocation="pool", target=None),
        call=lambda *_: None,
        plan_evidence=lambda *_: {},
        validate_simulation=lambda *_: None,
    )
    backend = SimpleNamespace(
        reader=reader,
        config=SimpleNamespace(production=None, accounts=None),
        connector=SimpleNamespace(
            execution=adapter,
            codec=SimpleNamespace(plan=lambda **fields: SimpleNamespace(**fields)),
        ),
        audit_history=lambda _: None,
        require_fresh=lambda _: None,
        validate_operation=lambda *_: None,
        verify_authority=lambda *_: None,
        budgets=budgets,
        node_readings={},
        node_readings_lock=threading.Lock(),
    )
    for name in (
        "prepare",
        "require_preparation",
        "prepare_operation",
        "estimate_execution",
        "prepare_fees",
        "estimate_gas",
        "op_exposure",
        "remember_node_reading",
        "node_reading",
    ):
        setattr(backend, name, MethodType(getattr(EvmGateway, name), backend))
    return backend


def bnb_policy():
    return SimpleNamespace(
        asset=SimpleNamespace(chain=BNB.chain_id),
        gas_limit_buffer_bps=2000,
        reward_percentile=50,
        base_fee_basis="next_block",
        base_fee_multiplier_bps=20000,
    )


def planned(backend):
    before = SimpleNamespace(
        block=backend.reader.pin_block("latest"), native_balance=SimpleNamespace(amount=10**18)
    )
    intent = SimpleNamespace(kind="swap")
    state = SimpleNamespace(decision=SimpleNamespace(intent=intent), permissions=None)
    return state, backend.prepare(state, intent, before, "attempt")


def validate(backend, monkeypatch, state, plan):
    """The coordinator's `update`: the gateway re-prepares at the plan's pinned block."""
    monkeypatch.setattr("almanak.request_runtime.unresolved", lambda _: False)
    service = SimpleNamespace(
        adapter=SimpleNamespace(
            snapshot=lambda allocation, block, reference: reference,
            prepare=backend.prepare,
            node_readings=backend.node_readings,
        )
    )
    GatewayService.validate_preparation(service, state, plan)


@pytest.mark.parametrize("chain", (ETHEREUM, BNB))
def test_policy_free_plan_survives_a_mined_block(monkeypatch, chain):
    node = Node(chain)
    backend = gateway(node, monkeypatch, None)
    state, plan = planned(backend)

    # A fork teardown advance between `plan` and `update` moves the node's suggestion.
    node.mine(gas_price=43_750_000)
    validate(backend, monkeypatch, state, plan)

    pinned = plan.before.block.number
    fees = max(node.base_fee(pinned), node.base_fee(pinned + 1))
    assert plan.gas_price == (fees * 9 + 7) // 8
    assert node.fee_history == [["0x1", hex(pinned), []]] * 2


@pytest.mark.parametrize(
    ("history", "headers", "reason"),
    (
        ({"oldestBlock": "0x63", "baseFeePerGas": ["0x1", "0x1"]}, None, "fee history"),
        ({"oldestBlock": "0x64", "baseFeePerGas": []}, None, "fee history"),
        ({"oldestBlock": "0x64", "baseFeePerGas": ["0x1"]}, None, "fee history"),
        ({"oldestBlock": "0x64"}, None, "fee history"),
        (None, "0x" + "ee" * 32, "changed during preparation"),
    ),
)
def test_policy_free_price_has_no_fallback(history, headers, reason):
    node = Node(ETHEREUM)
    reader = EthereumReader(
        node.chain,
        "http://rpc.invalid",
        client=httpx.Client(transport=httpx.MockTransport(node.respond)),
    )
    block = reader.pin_block("latest")
    if history is not None:
        node.result = lambda method, params, result=node.result: (
            history if method == "eth_feeHistory" else result(method, params)
        )
    if headers is not None:
        node.header = lambda number, header=node.header: {**header(number), "hash": headers}
    with pytest.raises(DataError, match=reason):
        pinned_gas_price(reader, block)


def test_the_gateway_remembers_its_node_reading_only_where_fee_history_has_no_tips(
    monkeypatch,
):
    bnb = gateway(Node(BNB, base_fees=False), monkeypatch, bnb_policy())
    _, plan = planned(bnb)
    assert bnb.node_readings == {("attempt", plan.before.block.hash): 50_000_000}
    policy = bnb_policy()
    policy.asset = SimpleNamespace(chain=ETHEREUM.chain_id)
    ethereum = gateway(Node(ETHEREUM), monkeypatch, policy)
    planned(ethereum)
    assert ethereum.node_readings == {}


def test_bnb_revalidation_reuses_the_gateways_own_node_reading(monkeypatch):
    node = Node(BNB, base_fees=False)
    backend = gateway(node, monkeypatch, bnb_policy())
    state, plan = planned(backend)
    assert plan.gas_price == plan.fee_quote.node_gas_price == 50_000_000

    node.mine(gas_price=60_000_000)
    validate(backend, monkeypatch, state, plan)
    # The gateway's reading, not the moving suggestion, reaches the policy's budget check.
    assert [args[4] for args in backend.budgets] == [50_000_000, 50_000_000]

    # The same attempt prepared at the new head is a new reading of the node.
    assert planned(backend)[1].gas_price == 60_000_000


def test_a_submitted_plan_cannot_choose_its_node_gas_price(monkeypatch):
    node = Node(BNB, base_fees=False)
    backend = gateway(node, monkeypatch, bnb_policy())
    state, plan = planned(backend)
    raised = plan.fee_quote.model_copy(update={"node_gas_price": 70_000_000})
    # Consistent with itself and within policy, but not what the gateway read.
    forged = SimpleNamespace(**{**vars(plan), "fee_quote": raised, "gas_price": 70_000_000})
    with pytest.raises(
        ValueError, match="differs from verified gateway preparation: .*gas_price"
    ) as refused:
        validate(backend, monkeypatch, state, forged)
    assert "70000000" not in str(refused.value)


def test_without_its_reading_a_restarted_gateway_reads_the_node_again(monkeypatch):
    node = Node(BNB, base_fees=False)
    state, plan = planned(gateway(node, monkeypatch, bnb_policy()))
    restarted = gateway(node, monkeypatch, bnb_policy())
    validate(restarted, monkeypatch, state, plan)
    node.mine(gas_price=60_000_000)
    with pytest.raises(ValueError, match="differs from verified gateway preparation: .*gas_price"):
        validate(gateway(node, monkeypatch, bnb_policy()), monkeypatch, state, plan)


def test_node_readings_are_bounded_and_keyed_by_attempt_and_block():
    backend = SimpleNamespace(node_readings={}, node_readings_lock=threading.Lock())
    remember = MethodType(EvmGateway.remember_node_reading, backend)
    for index in range(NODE_READINGS + 5):
        remember((f"attempt-{index}", "0xblock"), index)
    assert len(backend.node_readings) == NODE_READINGS
    assert ("attempt-0", "0xblock") not in backend.node_readings
    remember(("attempt-9", "0xother"), 1)
    assert backend.node_readings[("attempt-9", "0xblock")] == 9
    remember((None, "0xblock"), 1)
    remember(("attempt-x", "0xblock"), None)
    assert (None, "0xblock") not in backend.node_readings
    assert ("attempt-x", "0xblock") not in backend.node_readings


def test_retained_floor_is_ignored_where_fee_history_reports_tips(monkeypatch):
    node = Node(ETHEREUM)
    policy = bnb_policy()
    policy.asset = SimpleNamespace(chain=ETHEREUM.chain_id)
    backend = gateway(node, monkeypatch, policy)
    state, plan = planned(backend)
    assert plan.fee_quote.node_gas_price is None
    forged = plan.fee_quote.model_copy(update={"node_gas_price": plan.gas_price * 2})
    altered = SimpleNamespace(
        **{**vars(plan), "fee_quote": forged, "gas_price": plan.gas_price * 2}
    )
    with pytest.raises(ValueError, match="differs from verified gateway preparation: .*gas_price"):
        validate(backend, monkeypatch, state, altered)


def test_concurrent_requests_keep_the_readings_bounded():
    backend = SimpleNamespace(node_readings={}, node_readings_lock=threading.Lock())
    remember = MethodType(EvmGateway.remember_node_reading, backend)

    def flood(worker):
        for index in range(500):
            remember((f"attempt-{worker}-{index}", "0xblock"), index)

    threads = [threading.Thread(target=flood, args=(worker,)) for worker in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(backend.node_readings) == NODE_READINGS


def test_every_route_to_a_chain_reaches_one_backend_and_its_readings():
    from almanak.gateway.routing import ChainRouter, backend_for

    adapters = []
    router = ChainRouter.__new__(ChainRouter)
    router.factories = {BNB: lambda: adapters.append(SimpleNamespace()) or adapters[-1]}
    router.adapters = {}
    router.adapters_lock = threading.Lock()
    first, second = backend_for(router, BNB), backend_for(router, BNB)
    assert first is second and len(adapters) == 1


def test_concurrent_first_use_of_a_chain_builds_one_backend():
    from almanak.gateway.routing import ChainRouter, backend_for

    built, overlap = [], threading.Barrier(8, timeout=1)

    def factory():
        built.append(SimpleNamespace())
        return built[-1]

    def slow_factory():
        # Without serialization every thread passes the existence check before any
        # adapter is stored, so the barrier fills; with it, the wait times out.
        try:
            overlap.wait()
        except threading.BrokenBarrierError:
            pass
        return factory()

    router = ChainRouter.__new__(ChainRouter)
    router.factories = {BNB: slow_factory}
    router.adapters = {}
    router.adapters_lock = threading.Lock()
    reached = []
    threads = [
        threading.Thread(target=lambda: reached.append(backend_for(router, BNB))) for _ in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(built) == 1
    assert all(backend is built[0] for backend in reached)
