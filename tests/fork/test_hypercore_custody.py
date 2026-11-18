"""Hyperliquid phase 2 on a HyperEVM mainnet fork: generated custody grants and requests.

The HyperEVM side is authentic fork evidence: the Safe, Roles, the generated grants and
the custody calls run against mainnet contract code. Every HyperCore effect comes from
the synthesized model (blueprint 30, section 9), so nothing here is evidence of
HyperCore's behaviour, Fork Qualification or promotion evidence.
"""

import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode
from eth_utils import keccak, to_checksum_address

from almanak.caip import AssetType, ChainId
from almanak.connectors.hyperliquid import agents as agent_rules
from almanak.connectors.hyperliquid import corewriter as cw
from almanak.connectors.hyperliquid.agents import AgentNames, AgentPolicy, Agents
from almanak.connectors.hyperliquid.assets import CORE_DEPOSIT_WALLET
from almanak.connectors.hyperliquid.custody import (
    Custody,
    CustodyRequests,
    approve_agent,
    raw_actions,
    record_action,
    record_deposit,
    require_activated,
    require_standard_mode,
)
from almanak.connectors.hyperliquid.custody_evidence import Reads
from almanak.connectors.hyperliquid.custody_grants import custody_grants, withdrawal_branch
from almanak.connectors.hyperliquid.fork_model import REGISTER, HyperCoreModel, Keeper
from almanak.connectors.hyperliquid.fork_service import ModelService
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.fork_config import AlchemyProvider, ForkTarget
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.fork_funding import fund_erc20
from almanak.gateway.hypercore_agent_keys import LocalAgentKeys
from almanak.gateway.hypercore_info import (
    HyperCoreInfo,
    InfoRequest,
    SpotClearinghouseState,
    UserAbstraction,
    UserRole,
)
from almanak.gateway.permissions import installation_bundle, verify_partition
from almanak.gateway.providers import provider_endpoint
from almanak.gateway.roles_conditions import FunctionGrant, either, every, matches, pinned
from almanak.gateway.roles_storage import AuthorityReader
from almanak.gateway.safe_fixture import calldata, deploy_profile, owner_transaction
from almanak.models import Chain

pytestmark = pytest.mark.fork

ROOT = Path(__file__).resolve().parents[2]
# The HyperEVM reference's identified anchor: a 3,000,000-gas small block.
ANCHOR = (49017682, "0xd6e25a1a30734e9231ff1256c22710f7574fbc563202c8e97b6668babd5da229")
PROVIDER = AlchemyProvider(
    kind="alchemy", network="hyperliquid-mainnet", api_key_env="ALCHEMY_API_KEY"
)
USDC = AssetType("eip155:999/erc20:" + cw.USDC_CONTRACT)
POLICY = AgentPolicy(lifetime=2 * 3600 * 1000, renewal_lead=3600 * 1000, window=120 * 1000)
EXEC_WITH_ROLE = "execTransactionWithRole(address,uint256,bytes,uint8,bytes32,bool)"
CONDITION_VIOLATION = "0x" + keccak(text="ConditionViolation(uint8,bytes32)")[:4].hex()


@pytest.fixture(scope="module")
def custody_fork(request, tmp_path_factory):
    env_file = request.config.getoption("--fork-env")
    mainnet = Chain(chain_id=ChainId("eip155:999"), environment="mainnet")
    with EthereumReader(mainnet, provider_endpoint(PROVIDER, env_file)) as reader:
        origin = reader.pin_block(ANCHOR[0])
    assert origin.hash == ANCHOR[1]
    target = ForkTarget(chain=mainnet, provider=PROVIDER, block=origin)
    with Fork(env_file, origin, target=target) as node:
        yield setup(node, tmp_path_factory.mktemp("hypercore"))


def setup(node, directory):
    chain = Chain(
        chain_id=ChainId("eip155:999"), environment="anvil:" + node.metadata["instanceId"]
    )
    owner = node.funded_signer("", 0, 10**18, 83)
    operator = node.funded_signer("", 0, 10**18, 84)
    funder = node.funded_signer("", 0, 10**18, 85)
    profile, _ = deploy_profile(node.rpc, chain, owner, operator, "hypercore")
    safe = profile.fund.address
    grants = custody_grants(safe, AgentNames())
    partition = SimpleNamespace(account="hypercore", profile=profile, grants=grants, contracts=())
    installation = [
        int(owner_transaction(node.rpc, owner, safe, c.to, c.data)["gasUsed"], 16)
        for c in installation_bundle(partition)
    ]
    with EthereumReader(chain, node.rpc.endpoint) as reader:
        verify_partition(AuthorityReader(reader, reader.pin_block("latest")), partition)
    fund_erc20(node, safe, USDC, 100_000_000)
    fund_erc20(node, funder.address, USDC, 100_000_000)
    node.rpc("anvil_setBalance", [safe, hex(10**17)])
    model = HyperCoreModel(directory / "hypercore-model.json", int(node.rpc("eth_blockNumber"), 16))
    return SimpleNamespace(
        node=node,
        chain=chain,
        owner=owner,
        operator=operator,
        funder=funder,
        profile=profile,
        safe=safe,
        grants=grants,
        installation_gas=installation,
        model=model,
        keeper=Keeper(node.rpc, model),
        keys=LocalAgentKeys(directory / "agent-keys.json", "hypercore-fork"),
        report={"register": REGISTER, "installation_gas": installation},
    )


def clock(node):
    return int(node.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16) * 1000


def warp(node, seconds):
    node.rpc("evm_increaseTime", [seconds])
    node.rpc("evm_mine")


def role_call(fixture, call, profile=None):
    profile = profile or fixture.profile
    inner = bytes.fromhex(call.data[2:])
    role = bytes.fromhex(profile.role_key[2:])
    return calldata(
        EXEC_WITH_ROLE,
        ("address", "uint256", "bytes", "uint8", "bytes32", "bool"),
        (call.to, 0, inner, 0, role, True),
    )


def as_safe(fixture, call, profile=None):
    """The operator's call through Roles, and its block's timestamp."""
    profile = profile or fixture.profile
    roles = to_checksum_address(profile.roles_module)
    receipt = fixture.node.rpc.send(fixture.operator, roles, role_call(fixture, call, profile))
    block = fixture.node.rpc("eth_getBlockByNumber", [receipt["blockNumber"], False])
    return receipt, int(block["timestamp"], 16)


# Roles 2.1 `ConditionViolation(Status, bytes32)` statuses these refusals report.
STATUSES = {3: "FunctionNotAllowed", 5: "OrViolation", 7: "ParameterNotAllowed"}


def refused(fixture, call) -> str:
    """The Roles status that refuses the operator's call, read without sending it."""
    tx = {
        "from": fixture.operator.address,
        "to": fixture.profile.roles_module,
        "data": role_call(fixture, call),
    }
    with pytest.raises(RuntimeError) as error:
        fixture.node.rpc("eth_call", [tx, "latest"])
    # The revert data, not the node's rendering of it in the message.
    found = re.search(CONDITION_VIOLATION + "([0-9a-f]{64})", str(error.value))
    assert found is not None, str(error.value)
    return STATUSES[int(found.group(1), 16)]


class Backend:
    def __init__(self, fixture, info):
        self.fixture = fixture
        self.reads = Reads(fixture.node.rpc, info)
        self.keys = fixture.keys

    def now(self):
        return clock(self.fixture.node)


def settle(fixture, backend, custody):
    fixture.keeper.advance()
    return CustodyRequests.reconcile(backend, custody) or custody


def observe(backend, query):
    return backend.reads.info.fetch(InfoRequest(info=query))


def action(fixture, backend, custody, call):
    receipt, timestamp = as_safe(fixture, call)
    custody = record_action(custody, call, receipt, timestamp)
    return settle(fixture, backend, custody), receipt


def core(payload: bytes) -> cw.CoreCall:
    return cw.CoreCall(
        to=cw.CORE_WRITER, data=calldata(cw.SEND_RAW_ACTION, ("bytes",), (payload,)), note="probe"
    )


def moved_name(agent, label):
    """Action 9 whose name offset skips a decoy of an admitted label to an empty name."""
    decoy = label.encode()
    body = (
        encode(("address",), (agent,))
        + (0xA0).to_bytes(32, "big")
        + len(decoy).to_bytes(32, "big")
        + decoy.ljust(64, b"\x00")
        + bytes(32)
    )
    return cw.header(cw.ADD_API_WALLET) + body


def literal_pins(names):
    """Blueprint 30's first action 9 pins, which leave the name's offset word free."""
    length = names.label_length.to_bytes(15, "big")
    return every(
        *pinned(0, cw.header(cw.ADD_API_WALLET) + bytes(12)),
        *pinned(85, length),
        either(*(every(*pinned(100, names.prefix(n)[:15])) for n in names.both)),
    )


def probe_cases(fixture, fresh, expiry):
    names = AgentNames()
    first = names.label(names.first, expiry)
    zero = "0x" + "00" * 20
    order = ("uint32", "bool", "uint64", "uint64", "bool", "uint8", "uint128")
    return {
        "empty name": cw.agent_approval(fresh, ""),
        "other name": cw.agent_approval(fresh, f"almanak3 valid_until {expiry}"),
        "longer name": cw.agent_approval(fresh, f"almanak10 valid_until {expiry}"),
        "name without expiry": cw.agent_approval(fresh, names.first),
        "uppercase name": cw.agent_approval(fresh, f"ALMANAK1 valid_until {expiry}"),
        "letters for the expiry": cw.agent_approval(fresh, "almanak1 valid_until abcdefghijklm"),
        "colon in the expiry": cw.agent_approval(fresh, f"almanak1 valid_until {expiry // 10}:"),
        "moved empty name": core(moved_name(fresh, first)),
        "version 2": core(bytes([2]) + cw.payload(cw.ADD_API_WALLET, (fresh, first))[1:]),
        "standard mode for another user": cw.standard_mode(fresh),
        "unified account mode": cw.raw_action(cw.SET_ABSTRACTION, (fixture.safe, 2), "probe"),
        "send to another destination": cw.raw_action(
            cw.SEND_ASSET, (fresh, zero, cw.SPOT_DEX, cw.SPOT_DEX, 0, 100), "probe"
        ),
        "send of another token": cw.raw_action(
            cw.SEND_ASSET, (cw.USDC_SYSTEM, zero, cw.SPOT_DEX, cw.SPOT_DEX, 150, 100), "probe"
        ),
        "limit order": core(cw.header(1) + encode(order, (1, True, 1, 1, False, 3, 7))),
        "depositFor another account": cw.CoreCall(
            to=CORE_DEPOSIT_WALLET,
            data=calldata(
                "depositFor(address,uint256,uint32)",
                ("address", "uint256", "uint32"),
                (fresh, 1, cw.SPOT_DEX),
            ),
            note="probe",
        ),
        "approve another spender": cw.CoreCall(
            to=cw.USDC_CONTRACT,
            data=calldata("approve(address,uint256)", ("address", "uint256"), (fresh, 1)),
            note="probe",
        ),
    }


def allowed_names(fixture, fresh, expiry):
    """The operator's action 9 under each deployment name, simulated through Roles."""
    names = AgentNames()
    allowed = {}
    for name in names.both:
        tx = {
            "from": fixture.operator.address,
            "to": fixture.profile.roles_module,
            "data": role_call(fixture, cw.agent_approval(fresh, names.label(name, expiry))),
        }
        allowed[name] = fixture.node.rpc("eth_call", [tx, "latest"])
    return allowed


def test_action_9_grant_refuses_the_empty_name_and_any_other(custody_fork):
    fixture = custody_fork
    expiry = clock(fixture.node) + 3600 * 1000
    fresh = "0x" + "a5" * 20
    allowed = allowed_names(fixture, fresh, expiry)
    results = {
        name: refused(fixture, call) for name, call in probe_cases(fixture, fresh, expiry).items()
    }
    assert set(results.values()) == {"OrViolation", "FunctionNotAllowed", "ParameterNotAllowed"}
    assert results["moved empty name"] == "OrViolation"
    fixture.report["action_9_probe"] = {
        "allowed": allowed,
        "refused": results,
        "literal_pins": literal_pins_admit_a_moved_empty_name(fixture, fresh, expiry),
    }
    write_report(fixture)


def literal_pins_admit_a_moved_empty_name(fixture, fresh, expiry):
    """On a second Safe, the first proposed pins admit the moved empty name; the payload
    its RawAction carries decodes to the empty-name main agent."""
    node, owner = fixture.node, fixture.owner
    profile, _ = deploy_profile(node.rpc, fixture.chain, owner, fixture.operator, "literal")
    grant = FunctionGrant(
        target=cw.CORE_WRITER,
        signature=cw.SEND_RAW_ACTION,
        condition=matches(either(withdrawal_branch(), literal_pins(AgentNames()))),
        explanation=("probe: blueprint 30's first action 9 pins",),
    )
    partition = SimpleNamespace(profile=profile, grants=(grant,))
    for call in installation_bundle(partition):
        owner_transaction(node.rpc, owner, profile.fund.address, call.to, call.data)
    attack = moved_name(fresh, AgentNames().label("almanak1", expiry))
    receipt, _ = as_safe(fixture, core(attack), profile)
    ((_, emitted),) = raw_actions(receipt, profile.fund.address)
    assert emitted == attack and decode(("address", "string"), emitted[4:]) == (fresh, "")
    return {
        "safe": profile.fund.address,
        "transaction": receipt["transactionHash"],
        "payload": "0x" + attack.hex(),
    }


def test_custody_lifecycle_through_the_generated_grants(custody_fork):
    """Each step is its own function: fork tests run outside CI, so each must stay simple
    enough for the CRAP gate at zero coverage."""
    fixture = custody_fork
    with ModelService(fixture.model) as service:
        info = HyperCoreInfo(model=service.url, clock=lambda: clock(fixture.node))
        run = SimpleNamespace(
            fixture=fixture,
            node=fixture.node,
            safe=fixture.safe,
            backend=Backend(fixture, info),
            steps=fixture.report.setdefault("steps", {}),
        )
        custody = Custody(safe=fixture.safe, agents=Agents(policy=POLICY))
        for step in (
            deposit_and_activate,
            standard_mode,
            first_agent,
            griefed_renewal,
            renewed_agent,
            withdrawal,
            dropped_beside_matching_credit,
            foreign_agent_halts,
        ):
            custody = step(run, custody)
    fixture.report["model"] = fixture.model.doc
    write_report(fixture)


def resolved(run, custody, call):
    custody, _ = action(run.fixture, run.backend, custody, call)
    assert custody.requests[-1].status == "resolved"
    return custody


def deposit_and_activate(run, custody):
    """Approve, deposit and revoke; the deposit activates the account. Then the owner
    sends HYPE to HyperCore for the withdrawal's system transaction."""
    approve, deposit, revoke = cw.deposit_calls(30_000_000)
    as_safe(run.fixture, approve)
    receipt, timestamp = as_safe(run.fixture, deposit)
    as_safe(run.fixture, revoke)
    custody = record_deposit(custody, receipt, timestamp, 30_000_000)
    custody = settle(run.fixture, run.backend, custody)
    assert custody.requests[-1].status == "resolved"
    require_activated(observe(run.backend, UserRole(user=run.safe)), run.safe)
    run.steps["deposit"] = custody.requests[-1].model_dump(mode="json")
    owner_transaction(run.node.rpc, run.fixture.owner, run.safe, cw.HYPE_SYSTEM, "0x", 10**16)
    run.fixture.keeper.advance()
    return custody


def standard_mode(run, custody):
    """Standard mode before any class transfer or agent approval."""
    custody = resolved(run, custody, cw.standard_mode(run.safe))
    require_standard_mode(observe(run.backend, UserAbstraction(user=run.safe)), run.safe)
    return custody


def first_agent(run, custody):
    """A fresh key under the first name, then USDC to perp."""
    custody, call = approve_agent(custody, run.fixture.keys, clock(run.node))
    custody, _ = action(run.fixture, run.backend, custody, call)
    run.first = agent_rules.require_signing(custody.agents, clock(run.node))
    assert (run.first.name, run.first.status) == ("almanak1", "active")
    CustodyRequests.audit(run.backend, custody)
    run.steps["first_agent"] = run.first.model_dump(mode="json")
    return resolved(run, custody, cw.class_transfer(12_000_000, True))


def griefed_renewal(run, custody):
    """An outsider makes the renewal's fresh address a user before HyperCore applies its
    approval (T14): it is dropped, replaces nothing, and the first agent keeps signing."""
    warp(run.node, 3600)
    custody, call = approve_agent(custody, run.fixture.keys, clock(run.node))
    run.griefed = custody.agents.outstanding.address
    run.fixture.model.grief(run.griefed)
    custody, _ = action(run.fixture, run.backend, custody, call)
    assert custody.agents.outstanding.status == "requested"
    warp(run.node, 121)
    custody = settle(run.fixture, run.backend, custody)
    dropped = custody.agents.approvals[-1]
    assert (dropped.address, dropped.status) == (run.griefed, "dropped")
    signing = agent_rules.require_signing(custody.agents, clock(run.node))
    assert signing.address == run.first.address
    with pytest.raises(ValueError, match="never approved twice"):
        agent_rules.prepare(custody.agents, run.griefed, clock(run.node))
    run.steps["griefed_renewal"] = dropped.model_dump(mode="json")
    return custody


def renewed_agent(run, custody):
    """The retry: another fresh key under the same other name, which then signs."""
    custody, call = approve_agent(custody, run.fixture.keys, clock(run.node))
    custody, _ = action(run.fixture, run.backend, custody, call)
    second = agent_rules.require_signing(custody.agents, clock(run.node))
    assert (second.name, second.status) == ("almanak2", "active")
    assert second.address not in (run.first.address, run.griefed)
    CustodyRequests.audit(run.backend, custody)
    run.steps["renewed_agent"] = second.model_dump(mode="json")
    return custody


def withdrawal(run, custody):
    """Back to spot and out to the Safe on HyperEVM."""
    custody = resolved(run, custody, cw.class_transfer(12_000_000, False))
    before = usdc_balance(run.node, run.safe)
    custody, _ = action(run.fixture, run.backend, custody, cw.withdrawal(20_000_000))
    custody = settle(run.fixture, run.backend, custody)
    assert custody.requests[-1].status == "resolved"
    assert usdc_balance(run.node, run.safe) - before == 20_000_000
    run.steps["withdrawal"] = custody.requests[-1].model_dump(mode="json")
    return custody


def dropped_beside_matching_credit(run, custody):
    """The class transfer to spot is dropped while an external depositFor credits the
    same amount to spot in the same block, so the spot balance moves as if it applied."""
    custody = resolved(run, custody, cw.class_transfer(5_000_000, True))
    spot_before = spot_usdc(run.backend, run.safe)
    run.fixture.model.drop(cw.USD_CLASS_TRANSFER, run.safe)
    call = cw.class_transfer(5_000_000, False)
    receipt, timestamp = dropped_beside_deposit(run.fixture, call, 5_000_000)
    custody = record_action(custody, call, receipt, timestamp)
    custody = settle(run.fixture, run.backend, custody)
    assert spot_usdc(run.backend, run.safe) - spot_before == 5
    assert custody.requests[-1].status == "unresolved"
    warp(run.node, 121)
    with pytest.raises(DataError) as halted:
        settle(run.fixture, run.backend, custody)
    assert halted.value.kind == "custody_unresolved"
    run.steps["dropped_beside_matching_credit"] = halted.value.details
    return custody


def foreign_agent_halts(run, custody):
    """An agent under the deployment's names whose key it does not hold halts it: the
    Safe owners replace the first agent with a key of their own, spaced after the last
    approval as HyperCore requires (T8)."""
    warp(run.node, 61)
    foreign = "0x" + keccak(text="foreign agent").hex()[-40:]
    label = AgentNames().label("almanak1", clock(run.node) + 3600 * 1000)
    data = cw.agent_approval(foreign, label).data
    owner_transaction(run.node.rpc, run.fixture.owner, run.safe, cw.CORE_WRITER, data)
    run.fixture.keeper.advance()
    with pytest.raises(DataError) as halted:
        CustodyRequests.audit(run.backend, custody)
    assert halted.value.details["agents"] == [foreign]
    run.steps["foreign_agent"] = halted.value.details["agents"]
    return custody


def usdc_balance(node, holder):
    data = calldata("balanceOf(address)", ("address",), (holder,))
    return int(node.rpc("eth_call", [{"to": cw.USDC_CONTRACT, "data": data}, "latest"]), 16)


def spot_usdc(backend, safe):
    balances = observe(backend, SpotClearinghouseState(user=safe)).result["balances"]
    return next(float(b["total"]) for b in balances if b["coin"] == "USDC")


def signed(node, signer, to, data):
    nonce = int(node.rpc("eth_getTransactionCount", [signer.address, "pending"]), 16)
    transaction = {
        "chainId": 31337,
        "nonce": nonce,
        "to": to,
        "data": data,
        "value": 0,
        "gas": 1_000_000,
        "gasPrice": int(node.rpc("eth_gasPrice"), 16),
    }
    return "0x" + signer.sign_transaction(transaction).raw_transaction.hex()


def dropped_beside_deposit(fixture, call, amount):
    """The Safe's call and an external funder's depositFor of `amount` to the Safe, which
    is outside the Safe's authority, mined in one block."""
    node, funder = fixture.node, fixture.funder
    approve = calldata(
        "approve(address,uint256)", ("address", "uint256"), (CORE_DEPOSIT_WALLET, amount)
    )
    node.rpc.send(funder, to_checksum_address(cw.USDC_CONTRACT), approve)
    deposit = calldata(
        "depositFor(address,uint256,uint32)",
        ("address", "uint256", "uint32"),
        (fixture.safe, amount, cw.SPOT_DEX),
    )
    roles = to_checksum_address(fixture.profile.roles_module)
    raws = [
        signed(node, fixture.operator, roles, role_call(fixture, call)),
        signed(node, funder, to_checksum_address(CORE_DEPOSIT_WALLET), deposit),
    ]
    node.rpc("evm_setAutomine", [False])
    try:
        hashes = [node.rpc("eth_sendRawTransaction", [raw]) for raw in raws]
        node.rpc("evm_mine")
    finally:
        node.rpc("evm_setAutomine", [True])
    receipts = [node.rpc.wait_receipt(digest) for digest in hashes]
    assert receipts[0]["blockNumber"] == receipts[1]["blockNumber"]
    block = node.rpc("eth_getBlockByNumber", [receipts[0]["blockNumber"], False])
    return receipts[0], int(block["timestamp"], 16)


def write_report(fixture):
    path = ROOT / "artifacts/hyperliquid-custody-fork/report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(fixture.report, indent=1, default=str) + "\n")
