"""Signing revalidates OP Stack connectors at the fresh head, not the fee-quote block.

On a live OP Stack chain the head moves between plan preparation and signing.
Sign-time revalidation pins that newer head, reads the retained OP fee
parameters at the older quote block and then checks connector revisions at the
head. The fee read must not leave the reader pinned at the quote block.
"""

import gzip
import json
from pathlib import Path
from types import MethodType, SimpleNamespace

import httpx
import pytest
from eth_utils import keccak

from almanak.caip import ChainId
from almanak.connectors.evm_revision import read_code
from almanak.gateway import execution as gateway_execution
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.execution import EvmGateway
from almanak.gateway.op_fees import FjordParameters, exposure_bound, read_parameters
from almanak.models import Eoa, SafeRoles

COMET = "0x" + "44" * 20
CODE = "0x6000600055"
EXPECTED = {
    "runtime_length": len(bytes.fromhex(CODE[2:])),
    "immutables": {},
    "normalized_code_hash": "0x" + keccak(bytes.fromhex(CODE[2:])).hex(),
}
CALLDATA = "0x" + "ab" * 196
GAS = 250_000
MULTIPLIER_BPS = 20_000


class MovingOpChain:
    """Retained Base fee reads at the quote block, with a head that advances on every read."""

    def __init__(self):
        path = Path(__file__).parent / "fixtures/base-fjord-fee-reads.json.gz"
        raw = json.loads(gzip.decompress(path.read_bytes()))
        self.quote = FjordParameters.model_validate_json(
            json.dumps(raw["cases"][0]["parameters"])
        ).block
        self.rows = {
            json.dumps([r["method"], r["params"]], sort_keys=True): r["result"]
            for r in raw["reads"]
        }
        self.head = max(c["parameters"]["block"]["number"] for c in raw["cases"])
        self.code_reads = []

    def header(self, number):
        return {
            "number": hex(number),
            "hash": "0x" + keccak(number.to_bytes(32)).hex(),
            "timestamp": hex(self.quote.timestamp + number - self.quote.number),
        }

    def result(self, method, params):
        if method == "eth_getBlockByNumber" and params[0] == "latest":
            self.head += 1
            return self.header(self.head)
        if method == "eth_getBlockByNumber" and int(params[0], 16) > self.quote.number + 4:
            return self.header(int(params[0], 16))
        key = json.dumps([method, params], sort_keys=True)
        if key in self.rows:
            return self.rows[key]
        if method == "eth_getCode" and params[1] == "latest":
            return "0x"  # the EOA fund's own code check
        if method == "eth_getCode":
            self.code_reads.append(params[1]["blockHash"])
            return CODE
        if method == "eth_call":
            return "0x"
        raise AssertionError(f"unexpected read {method} {params}")

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
    return MovingOpChain()


def reader_for(node):
    return EthereumReader(
        node.quote.chain,
        "http://rpc.invalid",
        client=httpx.Client(transport=httpx.MockTransport(node.respond)),
    )


def signing_gateway(node, monkeypatch, profile):
    """Real signing revalidation and OP fee exposure; a Comet-like connector stub."""
    reader = reader_for(node)
    monkeypatch.setattr(
        "almanak.gateway.permissions.verify",
        lambda config, manifest, backend, **_: backend.reader.pin_block("latest"),
    )
    execution = SimpleNamespace(
        submitter=SimpleNamespace(address="0x" + "66" * 20),
        to=COMET,
        value=0,
        data=CALLDATA,
        binding=SimpleNamespace(profile=profile),
    )
    monkeypatch.setattr(gateway_execution, "prepare_call", lambda *_: execution)
    monkeypatch.setattr(gateway_execution, "protocol_simulation", lambda _: {})
    monkeypatch.setattr(gateway_execution, "protocol_result", lambda _, result: result)
    monkeypatch.setattr(gateway_execution, "validate_signing_state", lambda _: None)
    monkeypatch.setattr(gateway_execution, "validate_budget", lambda *_: None)
    # Base selects the OP Stack fee family; the retained reads come from a Base fork.
    policy = SimpleNamespace(
        asset=SimpleNamespace(chain=ChainId("eip155:8453")), base_fee_multiplier_bps=MULTIPLIER_BPS
    )
    monkeypatch.setattr(
        gateway_execution, "execution_gas", lambda _: SimpleNamespace(policy=lambda _: policy)
    )
    target_connector = SimpleNamespace(require_target=lambda *_: None)
    monkeypatch.setattr(
        "almanak.connectors.target_routing.connector_for_target", lambda _: target_connector
    )

    def current_checks(backend, plan, block=None):
        # Mirrors Compound V3 and Morpho Blue: revision checks at the caller's pinned head.
        block = block or backend.reader.pin_block("latest")
        read_code(backend.reader, COMET, block, EXPECTED, "comet")

    adapter = SimpleNamespace(
        call=lambda *_: "call",
        validate_operation=lambda *_: None,
        snapshot=lambda backend, name, block, reference: SimpleNamespace(
            block=block, native_balance=SimpleNamespace(amount=10**18)
        ),
        validate_current=lambda *_: None,
        current_checks=current_checks,
        validate_simulation=lambda *_: None,
    )
    gateway = SimpleNamespace(
        reader=reader,
        config=SimpleNamespace(
            production=SimpleNamespace(),
            accounts=SimpleNamespace(binding=lambda _: SimpleNamespace(profile=profile)),
        ),
        connector=SimpleNamespace(execution=adapter, permissions=None),
        audit_history=lambda _: None,
        require_fresh=lambda _: None,
        guard=lambda: None,
        verified_authority={},
        authority_reads={},
    )
    for name in (
        "authorize_signing",
        "validate_plan_budget",
        "validate_fee_budget",
        "retained_fee_exposure",
        "op_exposure",
        "validate_operation",
        "verify_authority",
        "validate_current",
        "own_observer",
        "current_snapshot",
        "snapshot",
        "simulate_current",
    ):
        setattr(gateway, name, MethodType(getattr(EvmGateway, name), gateway))
    return gateway, execution


@pytest.mark.parametrize("profile", sorted(PROFILES))
def test_op_signing_checks_revisions_at_the_head_after_the_fee_quote_block(
    node, monkeypatch, profile
):
    backend, execution = signing_gateway(node, monkeypatch, PROFILES[profile])
    quote = node.quote
    exposure = exposure_bound(backend.reader, quote, GAS, (len(CALLDATA) - 2) // 2, MULTIPLIER_BPS)
    before = SimpleNamespace(block=quote, native_balance=SimpleNamespace(amount=10**18))
    plan = SimpleNamespace(
        operation=SimpleNamespace(
            account="fund",
            allocation="supply",
            target=SimpleNamespace(fund=SimpleNamespace(chain=quote.chain)),
        ),
        before=before,
        call="call",
        execution=execution,
        nonce=7,
        gas=GAS,
        gas_price=3,
        fee_quote=SimpleNamespace(block=quote, additional_fee_exposure=exposure),
    )
    state = SimpleNamespace(
        plan=plan,
        decision=SimpleNamespace(intent=SimpleNamespace(kind="supply")),
        permissions=SimpleNamespace(digest="manifest"),
    )

    assert backend.authorize_signing(state) == (plan.operation, execution, 7, GAS, 3)

    # Both revision checks ran at heads newer than the quote block; revalidation's
    # check ran at the last head it pinned, after the OP fee read at the quote block.
    assert len(node.code_reads) == 2
    assert quote.hash not in node.code_reads
    assert node.code_reads[-1] == node.header(node.head)["hash"]


def test_op_fee_parameters_restore_the_callers_pin(node):
    reader = reader_for(node)
    head = reader.pin_block("latest")
    parameters = read_parameters(reader, node.quote)
    assert parameters.block == node.quote
    assert reader._pinned == head
