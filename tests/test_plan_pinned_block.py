"""Production plan preparation keeps revision checks at the observation's pinned block."""

import json
import threading
from types import MethodType, SimpleNamespace

import httpx
import pytest
from eth_utils import keccak

from almanak.caip import ChainId
from almanak.chains import evm_network
from almanak.connectors.evm_revision import read_code
from almanak.gateway import execution as gateway_execution
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.execution import EvmGateway
from almanak.gateway.production import ProductionGateway
from almanak.models import Chain, Eoa, SafeRoles

CHAIN = Chain(chain_id=ChainId("eip155:4663"), environment="mainnet")
GENESIS = evm_network(CHAIN.chain_id).genesis
POOL = "0x" + "44" * 20
POSITION_MANAGER = "0x" + "55" * 20
CODE = "0x6000600055"
EXPECTED = {
    "runtime_length": len(bytes.fromhex(CODE[2:])),
    "immutables": {},
    "normalized_code_hash": "0x" + keccak(bytes.fromhex(CODE[2:])).hex(),
}


class FastChain:
    """A production chain whose head advances on every `latest` header read."""

    def __init__(self, head):
        self.head = head
        self.code_reads = []

    def header(self, number):
        return {
            "number": hex(number),
            "hash": GENESIS if number == 0 else "0x" + number.to_bytes(32).hex(),
            "timestamp": hex(1_700_000_000 + number),
        }

    def result(self, method, params):
        if method == "eth_getBlockByNumber":
            if params[0] == "latest":
                self.head += 1
                return self.header(self.head)
            return self.header(int(params[0], 16))
        if method == "eth_getCode":
            if params[1] == "latest":
                return "0x"  # the EOA fund's own code check
            self.code_reads.append(params[1]["blockHash"])
            return CODE
        if method == "eth_feeHistory":
            return {"oldestBlock": params[1], "reward": [["0x1"]], "baseFeePerGas": ["0x2", "0x2"]}
        return {
            "eth_chainId": hex(4663),
            "web3_clientVersion": "Nitro/v3",
            "eth_getTransactionCount": "0x0",
            "eth_estimateGas": "0x5208",
            "eth_gasPrice": "0x3",
        }.get(method, "0x")

    def respond(self, request):
        payload = json.loads(request.content)
        result = self.result(payload["method"], payload["params"])
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "result": result})


PROFILES = {
    "safe_roles": SafeRoles.model_construct(kind="safe_roles"),
    "eoa": Eoa.model_construct(kind="eoa", fund=SimpleNamespace(address="0x" + "12" * 20)),
}


@pytest.fixture
def node():
    return FastChain(head=100)


def production_gateway(node, monkeypatch, profile):
    """Real guard, operation validation, authority, gas and fee quoting; stub connector."""
    reader = EthereumReader(
        CHAIN,
        "http://rpc.invalid",
        client=httpx.Client(transport=httpx.MockTransport(node.respond)),
    )
    # Mirrors permissions.verify: Safe authority is verified at a freshly pinned head.
    monkeypatch.setattr(
        "almanak.gateway.permissions.verify",
        lambda config, manifest, backend, **_: backend.reader.pin_block("latest"),
    )
    execution = SimpleNamespace(
        submitter=SimpleNamespace(address="0x" + "66" * 20),
        to=POSITION_MANAGER,
        value=0,
        data="0x",
        binding=SimpleNamespace(profile=profile),
    )
    monkeypatch.setattr(gateway_execution, "prepare_call", lambda *_: execution)
    monkeypatch.setattr(gateway_execution, "protocol_simulation", lambda _: {})
    monkeypatch.setattr(gateway_execution, "protocol_result", lambda _, result: result)
    policy = SimpleNamespace(
        asset=SimpleNamespace(chain=CHAIN.chain_id),
        gas_limit_buffer_bps=2000,
        reward_percentile=50,
        base_fee_basis="next_block",
        base_fee_multiplier_bps=20000,
    )
    monkeypatch.setattr(
        gateway_execution, "execution_gas", lambda _: SimpleNamespace(policy=lambda _: policy)
    )
    monkeypatch.setattr(gateway_execution, "validate_budget", lambda *_: None)
    target_connector = SimpleNamespace(require_target=lambda *_: None, execution=SimpleNamespace())
    monkeypatch.setattr(
        "almanak.connectors.target_routing.connector_for_target", lambda _: target_connector
    )
    adapter = SimpleNamespace(
        # A Uniswap V3 liquidity entry mints through the position manager.
        operation=lambda *_: SimpleNamespace(
            account="fund", target=SimpleNamespace(fund=SimpleNamespace(chain=CHAIN))
        ),
        validate_operation=lambda *_: None,
        call=lambda *_: None,
        # The V3 pool and periphery checks read runtime code at the observation's block.
        plan_evidence=lambda backend, operation, call, before: {
            "pool": read_code(backend.reader, POOL, before.block, EXPECTED, "pool"),
            "manager": read_code(
                backend.reader, POSITION_MANAGER, before.block, EXPECTED, "position_manager"
            ),
        },
        validate_simulation=lambda *_: None,
    )
    gateway = SimpleNamespace(
        reader=reader,
        config=SimpleNamespace(
            production=SimpleNamespace(),
            accounts=SimpleNamespace(binding=lambda _: SimpleNamespace(profile=profile)),
        ),
        connector=SimpleNamespace(
            execution=adapter, permissions=None, codec=SimpleNamespace(plan=lambda **fields: fields)
        ),
        audit_history=lambda _: None,
        require_fresh=lambda _: None,
        verified_authority={},
        authority_reads={},
        node_readings={},
        node_readings_lock=threading.Lock(),
    )
    for name in (
        "prepare",
        "require_preparation",
        "prepare_operation",
        "validate_operation",
        "verify_authority",
        "estimate_execution",
        "prepare_fees",
        "estimate_gas",
        "op_exposure",
        "node_reading",
        "remember_node_reading",
    ):
        setattr(gateway, name, MethodType(getattr(EvmGateway, name), gateway))
    gateway.guard = MethodType(ProductionGateway.guard, gateway)
    gateway.verify_provider = MethodType(ProductionGateway.verify_provider, gateway)
    return gateway


@pytest.mark.parametrize("profile", sorted(PROFILES))
def test_production_plan_reads_revision_code_at_the_observation_block(node, monkeypatch, profile):
    backend = production_gateway(node, monkeypatch, PROFILES[profile])
    before = SimpleNamespace(
        block=backend.reader.pin_block("latest"),
        native_balance=SimpleNamespace(amount=10**18),
    )
    intent = SimpleNamespace(kind="open_liquidity")
    state = SimpleNamespace(
        decision=SimpleNamespace(intent=intent), permissions=SimpleNamespace(digest="manifest")
    )

    plan = backend.prepare(state, intent, before, "attempt")

    # The production guard pins genesis and Safe verification pins a newer head, but
    # both revision checks still read code at the observation's block.
    assert node.code_reads == [before.block.hash] * 2
    assert plan["fee_quote"].block == before.block
    assert backend.reader._pinned == before.block


def test_block_mismatch_names_the_role_and_both_blocks(node):
    reader = EthereumReader(
        CHAIN,
        "http://rpc.invalid",
        client=httpx.Client(transport=httpx.MockTransport(node.respond)),
    )
    observed = reader.pin_block("latest")
    genesis = reader.pin_block(0)
    with pytest.raises(DataError) as error:
        read_code(reader, POOL, observed, EXPECTED, "pool")
    assert error.value.kind == "block_mismatch"
    assert str(error.value) == (
        "revision checks require the reader's pinned block: pool "
        f"{POOL} checked at eip155:4663 block {observed.number} ({observed.hash}), "
        f"reader pinned eip155:4663 block 0 ({genesis.hash})"
    )
