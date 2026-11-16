"""Hyperliquid phase 2 offline: custody grants, D9 agents, custody requests and the model.

The grants are checked here by a small evaluator of the Roles 2.1 operators they use;
the fork test (`tests/fork/test_hypercore_custody.py`) installs them in Roles itself.
"""

import json
import os
import stat
import threading

import httpx
import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.connectors.hyperliquid import agents as rules
from almanak.connectors.hyperliquid import corewriter as cw
from almanak.connectors.hyperliquid.agents import (
    AgentNames,
    AgentPolicy,
    Agents,
    Submission,
)
from almanak.connectors.hyperliquid.assets import CORE_DEPOSIT_WALLET
from almanak.connectors.hyperliquid.custody import (
    Custody,
    CustodyRequests,
    approve_agent,
    record_action,
    record_deposit,
    require_activated,
    require_standard_mode,
    with_resolution,
)
from almanak.connectors.hyperliquid.custody_evidence import (
    CDW_TRANSFER,
    RAW_ACTION,
    TRANSFER,
    Reads,
    system_transfer,
    topic,
)
from almanak.connectors.hyperliquid.custody_grants import custody_grants
from almanak.connectors.hyperliquid.custody_requests import (
    ClassTransfer,
    CreditLog,
    Deposit,
    Evidence,
    StandardMode,
    SystemTransfer,
    Withdrawal,
    attributed,
)
from almanak.connectors.hyperliquid.fork_model import (
    RECEIVED,
    HyperCoreModel,
    Keeper,
    parsed_label,
    received,
    text,
)
from almanak.connectors.hyperliquid.fork_service import ModelService, query_from
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_agent_keys import LocalAgentKeys
from almanak.gateway.hypercore_info import (
    ClearinghouseState,
    ExtraAgents,
    HyperCoreInfo,
    InfoRequest,
    InfoResult,
    Meta,
    SpotClearinghouseState,
    UserAbstraction,
    UserNonFundingLedgerUpdates,
    UserRole,
)
from almanak.gateway.roles_conditions import (
    Condition,
    nested_call,
    pinned,
    require_compatible,
    type_tree,
)
from almanak.gateway.safe_fixture import calldata

SAFE = "0x" + "5a" * 20
AGENT = "0x" + "a1" * 20
OTHER = "0x" + "a2" * 20
T0 = 1_793_777_833_000
NAMES = AgentNames()


# A Roles 2.1 evaluator for the operators the custody grants use.


def word(data: bytes, offset: int) -> bytes:
    return data[offset : offset + 32].ljust(32, b"\x00")


def dynamic(body: bytes, head: int) -> bytes:
    start = int.from_bytes(word(body, head), "big")
    length = int.from_bytes(word(body, start), "big")
    # Roles plucks a dynamic value with its padding to a whole word.
    return body[start + 32 : start + 32 + (length + 31) // 32 * 32]


def allows(node: Condition, value: bytes) -> bool:
    if node.operator == 1:
        return all(allows(child, value) for child in node.children)
    if node.operator == 2:
        return any(allows(child, value) for child in node.children)
    if node.operator == 5:
        body = value[4:] if node.parameter == 5 else value
        return all(
            allows(
                child,
                dynamic(body, 32 * index) if type_tree(child)[0] != 1 else word(body, 32 * index),
            )
            for index, child in enumerate(node.children)
        )
    compared = bytes.fromhex(node.value[2:])
    if node.operator == 16:
        return value == compared
    if node.operator == 17:
        return int.from_bytes(value, "big") > int.from_bytes(compared, "big")
    if node.operator == 21:
        shift = int.from_bytes(compared[:2], "big")
        if shift >= len(value):
            return False
        mask, expected = compared[2:17], compared[17:32]
        window = value[shift : shift + 15].ljust(15, b"\x00")
        return bytes(a & b for a, b in zip(window, mask, strict=True)) == expected
    raise AssertionError(f"operator {node.operator} is not evaluated here")


def grant_for(signature):
    return next(g for g in custody_grants(SAFE, NAMES) if g.signature == signature)


def permitted(call: cw.CoreCall) -> bool:
    grant = next(
        (
            g
            for g in custody_grants(SAFE, NAMES)
            if g.target == call.to and g.selector == bytes.fromhex(call.data[2:10])
        ),
        None,
    )
    return grant is not None and allows(grant.condition, bytes.fromhex(call.data[2:]))


def raw(payload: bytes) -> cw.CoreCall:
    return cw.CoreCall(
        to=cw.CORE_WRITER, data=calldata(cw.SEND_RAW_ACTION, ("bytes",), (payload,)), note="test"
    )


def label(name=NAMES.first, valid_until=T0 + 3600_000):
    return f"{name} valid_until {valid_until}"


def test_generated_grants_install_in_one_roles_type_tree():
    grants = custody_grants(SAFE, NAMES)
    assert [(g.target, g.signature) for g in grants] == [
        (cw.USDC_CONTRACT, "approve(address,uint256)"),
        (CORE_DEPOSIT_WALLET, "deposit(uint256,uint32)"),
        (cw.CORE_WRITER, "sendRawAction(bytes)"),
    ]
    for grant in grants:
        require_compatible(grant.condition, grant.signature)
        assert len(grant.conditions) < 256
    # Action 13 is the one decoded branch; every other branch matches bytes alone.
    branches = grants[2].condition.children[0].children
    assert [type_tree(b)[0] for b in branches] == [5, 2, 2, 2]


def test_nested_call_keeps_its_selector_bitmask():
    selector = keccak(text="transfer(address,uint256)")[:4]
    assert nested_call("transfer(address,uint256)").children[1] == pinned(0, selector)[0]
    assert len(pinned(0, bytes(31))) == 3
    for expected, mask in ((b"", None), (b"\x01", b"\x01\x01"), (b"\x03", b"\x01")):
        with pytest.raises(ValueError):
            pinned(0, expected, mask)


@pytest.mark.parametrize(
    "call",
    [
        cw.deposit_calls(5_000_000)[0],
        cw.deposit_calls(5_000_000)[1],
        cw.deposit_calls(5_000_000)[2],
        cw.class_transfer(12_000_000, True),
        cw.class_transfer(1, False),
        cw.withdrawal(13_986_680),
        cw.standard_mode(SAFE),
        cw.agent_approval(AGENT, label()),
        cw.agent_approval(OTHER, label(NAMES.second, 9_999_999_999_999)),
    ],
    ids=lambda call: call.note,
)
def test_grants_admit_the_custody_calls(call):
    assert permitted(call)


def payload_with(index, value):
    changed = bytearray(cw.payload(cw.ADD_API_WALLET, (AGENT, label())))
    changed[index] = value
    return bytes(changed)


@pytest.mark.parametrize(
    "call",
    [
        cw.agent_approval(AGENT, ""),
        cw.agent_approval(AGENT, NAMES.first),
        cw.agent_approval(AGENT, label("almanak3")),
        cw.agent_approval(AGENT, label("almanak10")),
        cw.agent_approval(AGENT, label("ALMANAK1")),
        cw.agent_approval(AGENT, f"almanak1 valid_untix {T0}"),
        cw.agent_approval(AGENT, "almanak1 valid_until abcdefghijklm"),
        cw.agent_approval(AGENT, "almanak1 valid_until 179247438700:"),
        cw.agent_approval(AGENT, "almanak1 valid_until 17924743870 0"),
        cw.agent_approval(AGENT, label(valid_until=10**14)),
        raw(payload_with(4, 1)),
        raw(payload_with(4 + 32 + 31, 0x60)),
        raw(payload_with(4 + 64 + 31, 0)),
        raw(bytes([2]) + cw.payload(cw.ADD_API_WALLET, (AGENT, label()))[1:]),
        cw.standard_mode(OTHER),
        cw.raw_action(cw.SET_ABSTRACTION, (SAFE, 2), "unified"),
        cw.raw_action(
            cw.SEND_ASSET, (OTHER, "0x" + "00" * 20, cw.SPOT_DEX, cw.SPOT_DEX, 0, 1), "x"
        ),
        cw.raw_action(
            cw.SEND_ASSET, (cw.USDC_SYSTEM, "0x" + "00" * 20, cw.SPOT_DEX, cw.SPOT_DEX, 150, 1), "x"
        ),
        cw.raw_action(
            cw.SEND_ASSET, (cw.USDC_SYSTEM, "0x" + "00" * 20, cw.SPOT_DEX, cw.SPOT_DEX, 0, 0), "x"
        ),
        cw.raw_action(cw.SEND_ASSET, (cw.USDC_SYSTEM, OTHER, cw.SPOT_DEX, cw.SPOT_DEX, 0, 1), "x"),
        cw.raw_action(cw.SEND_ASSET, (cw.USDC_SYSTEM, "0x" + "00" * 20, 0, cw.SPOT_DEX, 0, 1), "x"),
        raw(cw.header(1) + encode(("uint32", "bool"), (1, True))),
        raw(cw.header(6) + encode(("address", "uint64", "uint64"), (OTHER, 0, 1))),
        cw.CoreCall(
            to=cw.USDC_CONTRACT,
            data=calldata("approve(address,uint256)", ("address", "uint256"), (OTHER, 1)),
            note="other spender",
        ),
        cw.CoreCall(
            to=CORE_DEPOSIT_WALLET,
            data=calldata("deposit(uint256,uint32)", ("uint256", "uint32"), (0, cw.SPOT_DEX)),
            note="zero deposit",
        ),
        cw.CoreCall(
            to=CORE_DEPOSIT_WALLET,
            data=calldata("deposit(uint256,uint32)", ("uint256", "uint32"), (1, 1)),
            note="other dex",
        ),
        cw.CoreCall(
            to=CORE_DEPOSIT_WALLET,
            data=calldata(
                "depositFor(address,uint256,uint32)",
                ("address", "uint256", "uint32"),
                (OTHER, 1, cw.SPOT_DEX),
            ),
            note="depositFor",
        ),
    ],
)
def test_grants_refuse_everything_else(call):
    assert not permitted(call)


def moved_name(agent, text_label):
    """An action 9 payload whose name offset skips a decoy of an admitted label and points
    at an empty name: the decoy fills the bytes a fixed-offset pin inspects."""
    decoy = text_label.encode()
    body = (
        encode(("address",), (agent,))
        + (0xA0).to_bytes(32, "big")
        + len(decoy).to_bytes(32, "big")
        + decoy.ljust(64, b"\x00")
        + bytes(32)
    )
    return cw.header(cw.ADD_API_WALLET) + body


def literal_pins(names):
    """The pins blueprint 30 first proposed: header and padding, the length word's low 15
    bytes and the name's first 15 bytes; the name's offset word is left free."""
    from almanak.gateway.roles_conditions import either, every

    length = NAMES.label_length.to_bytes(15, "big")
    return every(
        *pinned(0, cw.header(cw.ADD_API_WALLET) + bytes(12)),
        *pinned(85, length),
        either(*(every(*pinned(100, names.prefix(n)[:15])) for n in names.both)),
    )


@pytest.mark.parametrize("position", [0, 6, 12])
def test_each_expiry_byte_admits_exactly_the_ten_digits(position):
    admitted = []
    for value in range(256):
        changed = bytearray(cw.payload(cw.ADD_API_WALLET, (AGENT, label())))
        changed[4 + 96 + len(NAMES.prefix(NAMES.first)) + position] = value
        if permitted(raw(bytes(changed))):
            admitted.append(value)
    assert bytes(admitted) == b"0123456789"


def test_the_name_offset_is_pinned_so_a_moved_empty_name_is_refused():
    attack = moved_name(AGENT, label())
    # A standard ABI decoder reads the moved name: the empty-name main agent.
    assert decode(("address", "string"), attack[4:]) == (AGENT, "")
    padded = attack + bytes(-len(attack) % 32)
    assert allows(literal_pins(NAMES), padded)
    assert not permitted(raw(attack))


def test_corewriter_encodes_the_canary_units_and_refuses_empty_amounts():
    action, fields = cw.decoded(bytes.fromhex(cw.withdrawal(13_986_680).data[138:]))
    assert (action, fields[5]) == (cw.SEND_ASSET, 1_398_668_000)
    assert cw.decoded(b"\x02\x00\x00\x07") is None
    assert cw.decoded(cw.header(1) + bytes(64)) is None
    assert cw.decoded(cw.header(7) + b"\x01") is None
    assert str(cw.usdc_text(13_986_680)) == "13.986680"
    for build in (
        lambda: cw.deposit_calls(0),
        lambda: cw.class_transfer(0, True),
        lambda: cw.withdrawal(0),
    ):
        with pytest.raises(ValueError):
            build()


# D9 agents.


def extra(agents_listed, received_at, user=SAFE):
    return InfoResult(
        request=InfoRequest(info=ExtraAgents(user=user)),
        received_at=received_at,
        result=[{"name": n, "address": a, "validUntil": v} for n, a, v in agents_listed],
    )


def submit(agents, number=10, time=T0):
    tx = "0x" + f"{number:064x}"
    approval = agents.outstanding
    queued = approval.address, NAMES.label(approval.name, approval.valid_until) if approval else ""
    return rules.submitted(agents, Submission(transaction=tx, block=number, time=time), *queued)


def test_agent_names_and_policy_are_validated():
    assert NAMES.label_length == len("almanak1 valid_until 1794382753834")
    with pytest.raises(ValueError):
        AgentNames(first="a", second="a")
    with pytest.raises(ValueError):
        AgentNames(first="a", second="bb")
    with pytest.raises(ValueError):
        NAMES.label("other", T0)
    with pytest.raises(ValueError):
        NAMES.label(NAMES.first, 1)
    with pytest.raises(ValueError):
        AgentPolicy(lifetime=10, renewal_lead=10)
    assert NAMES.owns("almanak1") and NAMES.owns("almanak2 valid_until x")
    assert not NAMES.owns("almanak10") and not NAMES.owns("")


def test_first_approval_then_renewal_under_the_other_name():
    policy = AgentPolicy(lifetime=3600_000, renewal_lead=600_000, window=120_000)
    agents = Agents(policy=policy)
    assert rules.due(agents, T0)
    agents, text_label = rules.prepare(agents, AGENT, T0)
    assert text_label == f"almanak1 valid_until {T0 + 3600_000}"
    assert not rules.due(agents, T0)
    with pytest.raises(ValueError, match="no agent approval is due"):
        rules.prepare(agents, OTHER, T0)
    with pytest.raises(ValueError, match="not the prepared approval"):
        rules.submitted(agents, submission(), OTHER, text_label)
    agents = submit(agents)
    with pytest.raises(ValueError, match="no prepared"):
        rules.submitted(agents, submission(), AGENT, text_label)
    # Listed with another expiry, it is not this approval; before the window it waits.
    agents = rules.resolve(agents, extra([("almanak1", AGENT, T0)], T0 + 1000), T0 + 1000)
    assert agents.outstanding.status == "requested"
    agents = rules.resolve(
        agents, extra([("almanak1", AGENT, T0 + 3600_000)], T0 + 1000), T0 + 1000
    )
    first = rules.require_signing(agents, T0 + 1000)
    assert (first.address, first.status) == (AGENT, "active")
    assert rules.next_name(agents, T0) == "almanak2"
    # Renewal falls due within the lead time of the expiry.
    renewal_time = T0 + 3600_000 - 600_000
    assert not rules.due(agents, renewal_time - 1) and rules.due(agents, renewal_time)
    agents, text_label = rules.prepare(agents, OTHER, renewal_time)
    assert text_label.startswith("almanak2 ")
    agents = submit(agents, 20, renewal_time)
    # Until extraAgents lists the new key, the current agent keeps signing.
    assert rules.require_signing(agents, renewal_time + 5000).address == AGENT
    listed = [("almanak1", AGENT, T0 + 3600_000), ("almanak2", OTHER, renewal_time + 3600_000)]
    agents = rules.resolve(agents, extra(listed, renewal_time + 5000), renewal_time + 5000)
    assert rules.require_signing(agents, renewal_time + 5000).address == OTHER
    # The renewal after that replaces the first agent under its own name.
    third = "0x" + "a3" * 20
    later = renewal_time + 3000_000
    agents, _ = rules.prepare(agents, third, later)
    requested = submit(agents, 30, later)
    listed = [("almanak2", OTHER, renewal_time + 3600_000), ("almanak1", third, later + 3600_000)]
    agents = rules.resolve(requested, extra(listed, later + 1000), later + 1000)
    assert [a.status for a in agents.approvals] == ["replaced", "active", "active"]
    # A custody outcome carries only the resolved approval: the document takes it in alike.
    custody = Custody(safe=SAFE, agents=requested)
    assert with_resolution(custody, approval=agents.approvals[-1]).agents == agents
    with pytest.raises(ValueError, match="only the outstanding agent approval resolves"):
        rules.settled(agents, agents.approvals[-1])


def test_approvals_are_spaced_after_the_last_one_queued():
    policy = AgentPolicy(lifetime=3600_000, renewal_lead=600_000, window=20_000, spacing=60_000)
    agents, _ = rules.prepare(Agents(policy=policy), AGENT, T0)
    agents = submit(agents)
    agents = rules.resolve(agents, extra([], T0 + 20_000), T0 + 20_000)
    # Dropped, possibly by HyperCore's spacing rather than a griefer: the retry waits.
    assert agents.approvals[0].status == "dropped"
    assert not rules.due(agents, T0 + 59_999) and rules.due(agents, T0 + 60_000)
    with pytest.raises(ValueError, match="no agent approval is due"):
        rules.prepare(agents, OTHER, T0 + 59_999)
    agents, _ = rules.prepare(agents, OTHER, T0 + 60_000)
    assert agents.outstanding.address == OTHER


def test_a_dropped_approval_is_retried_with_a_fresh_key_and_never_reapproved():
    agents = Agents(policy=AgentPolicy(lifetime=3600_000, renewal_lead=600_000, window=120_000))
    agents, _ = rules.prepare(agents, AGENT, T0)
    agents = submit(agents)
    # An early answer, or one taken before the window, never drops it.
    agents = rules.resolve(agents, extra([], T0 + 119_999), T0 + 200_000)
    agents = rules.resolve(agents, extra([], T0 + 200_000), T0 + 119_999)
    assert agents.outstanding.status == "requested"
    agents = rules.resolve(agents, extra([], T0 + 120_000), T0 + 120_000)
    assert agents.approvals[0].status == "dropped" and agents.outstanding is None
    with pytest.raises(DataError, match="no unexpired"):
        rules.require_signing(agents, T0 + 120_000)
    assert rules.due(agents, T0 + 120_000)
    with pytest.raises(ValueError, match="never approved twice"):
        rules.prepare(agents, AGENT, T0 + 120_000)
    agents, text_label = rules.prepare(agents, OTHER, T0 + 120_000)
    assert text_label.startswith("almanak1 ")
    with pytest.raises(ValueError, match="at most once"):
        Agents(approvals=(agents.approvals[0], agents.approvals[0]))
    with pytest.raises(ValueError, match="extraAgents"):
        rules.resolve(
            agents, InfoResult(request=InfoRequest(info=Meta()), received_at=T0, result={}), T0
        )
    assert rules.resolve(agents, extra([], T0), T0) == agents


def test_an_expired_agent_with_an_unresolved_renewal_halts():
    agents = Agents(policy=AgentPolicy(lifetime=3600_000, renewal_lead=600_000, window=120_000))
    agents, _ = rules.prepare(agents, AGENT, T0)
    agents = rules.resolve(submit(agents), extra([("almanak1", AGENT, T0 + 3600_000)], T0), T0)
    agents, _ = rules.prepare(agents, OTHER, T0 + 3000_000)
    agents = submit(agents, 20, T0 + 3000_000)
    assert rules.require_signing(agents, T0 + 3599_999).address == AGENT
    with pytest.raises(DataError) as halted:
        rules.require_signing(agents, T0 + 3600_000)
    assert [a["status"] for a in halted.value.details["approvals"]] == ["active", "requested"]


def test_verification_halts_on_any_agent_under_our_names_the_service_does_not_hold():
    agents = Agents()
    listed = [("almanak1", AGENT, None), ("theirs", OTHER, None)]
    rules.verify(agents, extra(listed, T0), frozenset({AGENT}))
    for listed in ([("almanak2", OTHER, T0)], [("almanak1 valid_untix 1", OTHER, None)]):
        with pytest.raises(DataError) as halted:
            rules.verify(agents, extra(listed, T0), frozenset({AGENT}))
        assert halted.value.details["agents"] == [OTHER]
    with pytest.raises(ValueError):
        rules.verify(
            agents,
            InfoResult(request=InfoRequest(info=Meta()), received_at=T0, result={}),
            frozenset(),
        )


# Custody requests.


def submission(block=100, time=T0):
    return Submission(transaction="0x" + f"{block:064x}", block=block, time=time)


def ledger(entries, received_at=T0 + 1000, start=T0, end=T0 + 999):
    query = UserNonFundingLedgerUpdates(user=SAFE, start_time=start, end_time=end)
    return InfoResult(request=InfoRequest(info=query), received_at=received_at, result=entries)


def entry(delta, time=T0 + 61):
    return {"delta": delta, "hash": "0x" + "7a" * 32, "time": time}


def spot_transfer(amount="20.0", user=cw.USDC_SYSTEM, destination=SAFE):
    return {
        "amount": amount,
        "destination": destination,
        "fee": "0.0",
        "feeToken": "",
        "nativeTokenFee": "0.0",
        "nonce": 1,
        "token": "USDC",
        "type": "spotTransfer",
        "usdcValue": amount,
        "user": user,
    }


def deposit_request():
    credit = CreditLog(
        transaction="0x" + "c1" * 32, log_index=3, block=100, recipient=SAFE, amount=20_000_000
    )
    return Deposit(key="d", safe=SAFE, submission=submission(), amount=20_000_000, credit=credit)


def test_a_deposit_resolves_only_by_its_own_unique_credit():
    request = deposit_request()
    activation = {"amount": "1.0", "token": "USDC", "type": "accountActivationGas"}
    own = entry(spot_transfer())
    evidence = Evidence(ledger=ledger([entry(activation), own]), credits=(request.credit,))
    resolved = attributed(request, evidence, T0 + 1000, 120_000)
    assert (resolved.status, resolved.evidence[0]["hash"]) == ("resolved", own["hash"])
    assert attributed(resolved, Evidence(), T0, 120_000) is resolved
    # An external depositFor of the same amount in the same second writes the same entry.
    rival = request.credit.model_copy(update={"transaction": "0x" + "e1" * 32})
    ambiguous = Evidence(
        ledger=ledger([own, entry(spot_transfer())]), credits=(request.credit, rival)
    )
    for evidence in (
        ambiguous,
        Evidence(ledger=ledger([own]), credits=(request.credit, rival)),
        Evidence(ledger=ledger([entry(spot_transfer("19.0"))])),
        Evidence(ledger=ledger([entry(spot_transfer(user=OTHER))])),
        Evidence(ledger=ledger([own] * 500)),
        Evidence(),
    ):
        assert attributed(request, evidence, T0 + 1000, 120_000).status == "unresolved"
    with pytest.raises(DataError) as halted:
        attributed(request, Evidence(ledger=ledger([own] * 2, T0 + 120_000)), T0 + 120_000, 120_000)
    assert halted.value.kind == "custody_unresolved"
    assert halted.value.details["request"]["key"] == "d"


def test_a_dropped_class_transfer_beside_a_matching_credit_stays_unresolved_then_halts():
    payload = "0x" + cw.payload(cw.USD_CLASS_TRANSFER, (5_000_000, False)).hex()
    request = ClassTransfer(
        key="c",
        safe=SAFE,
        submission=submission(),
        amount=5_000_000,
        to_perp=False,
        payload=payload,
    )
    # An unrelated credit of the same amount, in the same second: the balance moves as if
    # the transfer applied, but no accountClassTransfer entry is the Safe's.
    credit = entry(spot_transfer("5.0"))
    evidence = Evidence(ledger=ledger([credit]), actions=(payload,))
    assert attributed(request, evidence, T0 + 1000, 120_000).status == "unresolved"
    late = Evidence(ledger=ledger([credit], T0 + 120_000), actions=(payload,))
    # Halting needs both the deployment clock and the observation past the window.
    assert attributed(request, late, T0 + 119_999, 120_000).status == "unresolved"
    assert attributed(request, evidence, T0 + 120_000, 120_000).status == "unresolved"
    with pytest.raises(DataError, match="no identified effect"):
        attributed(request, late, T0 + 120_000, 120_000)
    applied = entry({"toPerp": False, "type": "accountClassTransfer", "usdc": "5.0"})
    found = Evidence(ledger=ledger([credit, applied]), actions=(payload,))
    assert attributed(request, found, T0 + 1000, 120_000).status == "resolved"
    twice = Evidence(ledger=ledger([applied, applied]), actions=(payload, payload))
    assert attributed(request, twice, T0 + 1000, 120_000).status == "unresolved"


def send_entry(amount="13.98668"):
    return {
        "amount": amount,
        "destination": cw.USDC_SYSTEM,
        "destinationDex": "spot",
        "fee": "0.0",
        "feeToken": "",
        "nativeTokenFee": "0.00002",
        "nonce": 5453675,
        "sourceDex": "spot",
        "token": "USDC",
        "type": "send",
        "usdcValue": amount,
        "user": SAFE,
    }


def test_a_withdrawal_needs_its_send_entry_and_the_system_transfer_after_its_block():
    payload = (
        "0x"
        + cw.payload(
            cw.SEND_ASSET,
            (cw.USDC_SYSTEM, "0x" + "00" * 20, cw.SPOT_DEX, cw.SPOT_DEX, 0, 1_398_668_000),
        ).hex()
    )
    request = Withdrawal(
        key="w", safe=SAFE, submission=submission(), amount=13_986_680, payload=payload
    )
    paid = SystemTransfer(
        transaction="0x" + "05" * 32, block=101, recipient=SAFE, amount=13_986_680
    )
    send = entry(send_entry())
    resolved = attributed(
        request, Evidence(ledger=ledger([send]), actions=(payload,), system=(paid,)), T0, 120_000
    )
    assert resolved.status == "resolved" and resolved.evidence[1]["block"] == 101
    for evidence in (
        Evidence(ledger=ledger([send]), actions=(payload,)),
        Evidence(
            ledger=ledger([send]),
            actions=(payload,),
            system=(paid.model_copy(update={"block": 100}),),
        ),
        # A payout in any later block is another withdrawal's.
        Evidence(
            ledger=ledger([send]),
            actions=(payload,),
            system=(paid.model_copy(update={"block": 102}),),
        ),
        # Two payouts in the next block cannot be told apart.
        Evidence(
            ledger=ledger([send]),
            actions=(payload,),
            system=(paid, paid.model_copy(update={"transaction": "0x" + "06" * 32})),
        ),
        Evidence(ledger=ledger([entry(send_entry("1.0"))]), actions=(payload,), system=(paid,)),
    ):
        assert attributed(request, evidence, T0, 120_000).status == "unresolved"
    # The scan of later blocks must itself reach past the window before a halt.
    early = Evidence(ledger=ledger([send], T0 + 200_000), actions=(payload,), head_time=T0 + 1000)
    assert attributed(request, early, T0 + 200_000, 120_000).status == "unresolved"
    with pytest.raises(DataError):
        attributed(
            request, early.model_copy(update={"head_time": T0 + 120_000}), T0 + 200_000, 120_000
        )


def test_standard_mode_resolves_from_the_mode_read_after_its_block():
    request = StandardMode(key="s", safe=SAFE, submission=submission(), payload="0x")

    def mode(value, received_at):
        query = UserAbstraction(user=SAFE)
        return InfoResult(request=InfoRequest(info=query), received_at=received_at, result=value)

    assert (
        attributed(request, Evidence(abstraction=mode("disabled", T0)), T0, 1).status == "resolved"
    )
    assert (
        attributed(request, Evidence(abstraction=mode("disabled", T0 - 1)), T0, 10).status
        == "unresolved"
    )
    assert (
        attributed(request, Evidence(abstraction=mode("default", T0)), T0, 10).status
        == "unresolved"
    )
    with pytest.raises(DataError):
        attributed(request, Evidence(abstraction=mode("default", T0 + 10)), T0 + 10, 10)


# The synthesized model.


def log(address, topics, data, index=0):
    return {"address": address, "topics": topics, "data": data, "logIndex": hex(index)}


def credit_log(recipient, amount, index=0):
    return log(
        CORE_DEPOSIT_WALLET, [TRANSFER, topic(recipient), topic(cw.USDC_SYSTEM)], hex(amount), index
    )


def action_log(user, call, index=0):
    data = "0x" + call.data[10:]
    return log(cw.CORE_WRITER, [RAW_ACTION, topic(user)], data, index)


def hype_log(user, amount, indexed=True):
    if indexed:
        return log(cw.HYPE_SYSTEM, [RECEIVED, topic(user)], "0x" + amount.to_bytes(32, "big").hex())
    return log(
        cw.HYPE_SYSTEM, [RECEIVED], "0x" + encode(("address", "uint256"), (user, amount)).hex()
    )


def model_at(tmp_path):
    model = HyperCoreModel(tmp_path / "model.json", 10)
    model.apply(11, T0 // 1000, [credit_log(SAFE, 20_000_000), hype_log(SAFE, 10**16)])
    return model


def ask(model, query):
    return model.answer(query_from(InfoRequest(info=query).model_dump()["info"]))


def test_model_activates_on_the_first_usdc_credit_and_orders_actions_after_it(tmp_path):
    model = HyperCoreModel(tmp_path / "model.json", 10)
    # An account activated in a block cannot act in it.
    model.apply(
        11, T0 // 1000, [credit_log(SAFE, 20_000_000), action_log(SAFE, cw.standard_mode(SAFE), 1)]
    )
    assert ask(model, UserAbstraction(user=SAFE)) == "default"
    assert ask(model, UserRole(user=SAFE)) == {"role": "user"}
    deltas = [e["delta"]["type"] for e in model.doc["ledger"][SAFE]]
    assert deltas == ["accountActivationGas", "spotTransfer"]
    assert model.doc["ledger"][SAFE][1]["delta"]["fee"] == "1.0"
    with pytest.raises(ValueError):
        model.apply(11, T0 // 1000, [])
    # A credit below the activation fee, or a first HYPE credit, activates nothing.
    model.apply(12, T0 // 1000 + 1, [credit_log(OTHER, 10), hype_log(AGENT, 10**16, indexed=False)])
    assert ask(model, UserRole(user=OTHER)) == {"role": "missing"}
    assert HyperCoreModel(tmp_path / "model.json", 0).doc["block"] == 12
    stale = json.loads((tmp_path / "model.json").read_text()) | {"register": "other"}
    (tmp_path / "model.json").write_text(json.dumps(stale))
    with pytest.raises(ValueError):
        HyperCoreModel(tmp_path / "model.json", 0)


def test_model_applies_custody_actions_and_owes_the_withdrawal(tmp_path):
    model = model_at(tmp_path)
    assert received(hype_log(SAFE, 5)) == (SAFE, 5)
    owed = model.apply(
        12,
        T0 // 1000 + 2,
        [
            action_log(SAFE, cw.standard_mode(SAFE)),
            action_log(SAFE, cw.class_transfer(12_000_000, True), 1),
            action_log(SAFE, cw.class_transfer(2_000_000, False), 2),
            action_log(SAFE, cw.withdrawal(9_000_000), 3),
            action_log(SAFE, cw.class_transfer(10**9, True), 4),
            action_log(SAFE, cw.withdrawal(10**9), 5),
            action_log(SAFE, cw.standard_mode(OTHER), 6),
            action_log(OTHER, cw.class_transfer(1, True), 7),
            log(
                cw.CORE_WRITER,
                [RAW_ACTION, topic(SAFE)],
                "0x" + encode(("bytes",), (b"\x01",)).hex(),
                8,
            ),
            log(cw.CORE_WRITER, ["0x" + "00" * 32], "0x", 9),
        ],
    )
    assert owed == [{"id": "12:0", "user": SAFE, "amount": 9_000_000}]
    assert ask(model, UserAbstraction(user=SAFE)) == "disabled"
    # 19 USDC after the activation fee: 12 to perp, 2 back, 9 out; 0.00002 HYPE paid.
    spot = ask(model, SpotClearinghouseState(user=SAFE))["balances"]
    assert [(b["coin"], b["total"]) for b in spot] == [("HYPE", "0.00998")]
    perp = ask(model, ClearinghouseState(user=SAFE))
    assert perp["withdrawable"] == "10.0"
    send = model.doc["ledger"][SAFE][-1]["delta"]
    assert (send["type"], send["amount"], send["nativeTokenFee"]) == ("send", "9.0", "0.00002")


def approvals(model, block, time, calls):
    """Each approval in a block of its own, 31 seconds after the previous one; returns
    the next block and its time."""
    for call in calls:
        block, time = block + 1, time + 31
        model.apply(block, time, [action_log(SAFE, call)])
    return block, time


def test_model_agents_follow_the_canary(tmp_path):
    model = model_at(tmp_path)
    expiry = T0 + 3600_000
    model.grief(OTHER)
    block, time = approvals(
        model,
        11,
        T0 // 1000,
        [
            cw.agent_approval(AGENT, label(valid_until=expiry)),
            # A griefed fresh address is a user before its approval applies (T14).
            cw.agent_approval(OTHER, label(NAMES.second, expiry)),
            # Beyond 180 days ahead.
            cw.agent_approval("0x" + "a4" * 20, label(valid_until=10**13 * 9)),
        ],
    )
    assert ask(model, UserRole(user=AGENT)) == {"role": "agent", "data": {"user": SAFE}}
    assert ask(model, UserRole(user=OTHER)) == {"role": "user"}
    assert ask(model, ExtraAgents(user=SAFE)) == [
        {"name": "almanak1", "address": AGENT, "validUntil": expiry}
    ]
    assert model.doc["injected"] == [{"kind": "grief", "address": OTHER, "block": 12}]
    # Re-approval of an existing agent is dropped; a fresh one under its name replaces it;
    # an empty-name main agent is never listed; a fourth named agent is dropped.
    fresh = ["0x" + f"{n:040x}" for n in range(1, 6)]
    approvals(
        model,
        block,
        time,
        [
            cw.agent_approval(AGENT, label(NAMES.second, expiry)),
            cw.agent_approval(fresh[0], label(valid_until=expiry)),
            cw.agent_approval(fresh[1], ""),
            cw.agent_approval(fresh[2], "b"),
            cw.agent_approval(fresh[3], "c"),
            cw.agent_approval(fresh[4], "d"),
        ],
    )
    names = [(a["name"], a["address"]) for a in ask(model, ExtraAgents(user=SAFE))]
    assert names == [("almanak1", fresh[0]), ("b", fresh[2]), ("c", fresh[3])]
    assert ask(model, UserRole(user=AGENT)) == {"role": "missing"}
    assert ask(model, UserRole(user=fresh[1]))["role"] == "agent"
    # Expired agents are no longer listed or recognised.
    model.apply(30, (expiry // 1000) + 1, [])
    assert ask(model, ExtraAgents(user=SAFE)) == [
        {"name": "b", "address": fresh[2], "validUntil": None},
        {"name": "c", "address": fresh[3], "validUntil": None},
    ]
    assert parsed_label("x valid_until 12a") == ("x valid_until 12a", None)
    assert text(0, 6) == "0.0" and text(13_986_680, 6) == "13.98668"


def test_model_drops_an_approval_within_seconds_of_the_previous_one(tmp_path):
    """The canary wind-down (T8): in one transaction only the first action 9 applied, one
    two seconds after another was dropped, and approvals 35 seconds apart applied."""
    model = model_at(tmp_path)
    fresh = ["0x" + f"{n:040x}" for n in range(1, 6)]
    t = T0 // 1000
    model.apply(
        12,
        t + 1,
        [
            action_log(SAFE, cw.agent_approval(fresh[0], label(valid_until=T0 + 3600_000))),
            action_log(SAFE, cw.agent_approval(fresh[1], "b"), 1),
        ],
    )
    model.apply(13, t + 3, [action_log(SAFE, cw.agent_approval(fresh[2], "c"))])
    # Measured from the previous action 9, dropped ones included.
    model.apply(14, t + 32, [action_log(SAFE, cw.agent_approval(fresh[3], "d"))])
    model.apply(15, t + 67, [action_log(SAFE, cw.agent_approval(fresh[4], "e"))])
    listed = [a["address"] for a in ask(model, ExtraAgents(user=SAFE))]
    assert listed == [fresh[0], fresh[4]]
    # Another account's approvals are spaced apart from this one's.
    model.apply(16, t + 67, [credit_log(OTHER, 2_000_000)])
    model.apply(17, t + 68, [action_log(OTHER, cw.agent_approval("0x" + "a9" * 20, "x"))])
    assert ask(model, UserRole(user="0x" + "a9" * 20))["role"] == "agent"


def test_model_drops_a_faulted_action_and_retains_the_fault(tmp_path):
    model = model_at(tmp_path)
    model.drop(cw.SET_ABSTRACTION, SAFE)
    model.apply(12, T0 // 1000 + 1, [action_log(SAFE, cw.standard_mode(SAFE))])
    assert ask(model, UserAbstraction(user=SAFE)) == "default"
    assert model.doc["injected"][0]["kind"] == "drop" and model.doc["faults"] == []


def test_model_service_answers_through_the_closed_shapes(tmp_path):
    model = model_at(tmp_path)
    with ModelService(model) as service:
        info = HyperCoreInfo(model=service.url, clock=model.now)
        for query in (
            UserRole(user=SAFE),
            UserAbstraction(user=SAFE),
            ExtraAgents(user=SAFE),
            SpotClearinghouseState(user=SAFE),
            ClearinghouseState(user=SAFE),
            UserNonFundingLedgerUpdates(user=SAFE, start_time=T0, end_time=T0 + 999),
        ):
            result = info.fetch(InfoRequest(info=query))
            assert result.received_at == model.now()
        with pytest.raises(DataError, match="HTTP 422"):
            info.fetch(InfoRequest(info=Meta()))
        assert httpx.post(service.url + "/other", json={}).status_code == 404
        assert httpx.post(service.url + "/info", content=b"{").status_code == 422
        info.close()
    with pytest.raises(ValueError):
        HyperCoreInfo(model="https://api.hyperliquid.xyz")


def test_overlapping_key_generation_keeps_every_returned_key(tmp_path):
    path = tmp_path / "agent-keys.json"
    start = threading.Barrier(8)
    returned: list[str] = []

    def generate():
        keys = LocalAgentKeys(path, "deployment")
        start.wait()
        returned.extend(keys.generate() for _ in range(10))

    workers = [threading.Thread(target=generate) for _ in range(8)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    assert len(set(returned)) == 80
    assert LocalAgentKeys(path, "deployment").held() == set(returned)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["agent-keys.json", "agent-keys.json.lock"]


def test_a_failed_key_write_leaves_no_temporary_file(tmp_path, monkeypatch):
    keys = LocalAgentKeys(tmp_path / "agent-keys.json", "deployment")

    def refuse(*_):
        raise OSError("disk full")

    monkeypatch.setattr("almanak.gateway.hypercore_agent_keys.os.replace", refuse)
    with pytest.raises(OSError):
        keys.generate()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["agent-keys.json.lock"]


def test_local_agent_keys_hold_keys_and_return_only_addresses(tmp_path):
    keys = LocalAgentKeys(tmp_path / "keys" / "agent-keys.json", "deployment")
    assert keys.held() == frozenset()
    first, second = keys.generate(), keys.generate()
    assert keys.held() == {first, second} and first != second
    assert stat.S_IMODE(os.stat(keys.path).st_mode) == 0o600
    assert LocalAgentKeys(keys.path, "other").held() == frozenset()
    os.chmod(keys.path, 0o644)
    with pytest.raises(PermissionError):
        keys.held()
    with pytest.raises(ValueError):
        LocalAgentKeys(keys.path, "")


# An offline custody lifecycle: the model applies a fake chain's blocks.


class Chain:
    """Blocks with timestamps, transactions and logs, answering the reads custody makes."""

    def __init__(self, model):
        self.model = model
        self.blocks = [{"timestamp": T0 // 1000 - 1, "transactions": [], "logs": []}] * 11
        self.receipts = {}

    def mine(self, timestamp, transactions=(), logs=(), status="0x1"):
        number = len(self.blocks)
        txs = [{"hash": "0x" + f"{number:032x}{i:032x}", **t} for i, t in enumerate(transactions)]
        stamped = [
            {
                **entry,
                "blockNumber": hex(number),
                "transactionHash": txs[0]["hash"] if txs else "0x" + "00" * 32,
                "logIndex": hex(i),
            }
            for i, entry in enumerate(logs)
        ]
        self.blocks.append({"timestamp": timestamp, "transactions": txs, "logs": stamped})
        for tx in txs:
            self.receipts[tx["hash"]] = {
                "status": status,
                "logs": stamped if status == "0x1" else [],
            }
        self.model.apply(number, timestamp, stamped if status == "0x1" else [])
        return {
            "transactionHash": txs[0]["hash"] if txs else None,
            "blockNumber": hex(number),
            "logs": stamped,
        }

    def __call__(self, method, params=()):
        if method == "eth_blockNumber":
            return hex(len(self.blocks) - 1)
        if method == "eth_getBlockByNumber":
            number = int(params[0], 16)
            if number >= len(self.blocks):
                return None
            block = self.blocks[number]
            return {"timestamp": hex(block["timestamp"]), "transactions": block["transactions"]}
        if method == "eth_getTransactionByHash":
            mined = (t for block in self.blocks for t in block["transactions"])
            return next((t for t in mined if t["hash"] == params[0]), None)
        if method == "eth_getTransactionReceipt":
            return self.receipts.get(params[0])
        assert method == "eth_getLogs"
        query = params[0]
        found = []
        for number in range(int(query["fromBlock"], 16), int(query["toBlock"], 16) + 1):
            for entry in self.blocks[number]["logs"]:
                if entry["address"] == query["address"] and all(
                    t is None or t == entry["topics"][i] for i, t in enumerate(query["topics"])
                ):
                    found.append(entry)
        return found


def payment(amount, recipient=SAFE):
    return calldata("transfer(address,uint256)", ("address", "uint256"), (recipient, amount))


def usdc_credit(recipient, amount):
    topics = [TRANSFER, topic(CORE_DEPOSIT_WALLET), topic(recipient)]
    return log(cw.USDC_CONTRACT, topics, hex(amount))


class Backend:
    def __init__(self, chain, info, keys):
        self.chain = chain
        self.reads = Reads(chain, info)
        self.keys = keys

    def now(self):
        return self.chain.blocks[-1]["timestamp"] * 1000


def operator_tx(call):
    return {"from": "0x" + "0e" * 20, "to": call.to, "input": call.data}


def test_offline_custody_lifecycle_through_the_binding(tmp_path):
    model = HyperCoreModel(tmp_path / "model.json", 10)
    chain = Chain(model)
    keys = LocalAgentKeys(tmp_path / "agent-keys.json", "offline")
    with ModelService(model) as service:
        info = HyperCoreInfo(model=service.url, clock=lambda: chain.blocks[-1]["timestamp"] * 1000)
        backend = Backend(chain, info, keys)
        custody = Custody(safe=SAFE)
        assert CustodyRequests.reconcile(backend, custody) is None
        t = T0 // 1000
        deposit = cw.deposit_calls(20_000_000)[1]
        receipt = chain.mine(t, [operator_tx(deposit)], [credit_log(SAFE, 20_000_000)])
        custody = record_deposit(custody, receipt, t, 20_000_000)
        with pytest.raises(DataError):
            record_deposit(custody, receipt, t, 1)
        assert CustodyRequests.unresolved(custody)
        custody = CustodyRequests.reconcile(backend, custody)
        assert not CustodyRequests.unresolved(custody)
        role = info.fetch(InfoRequest(info=UserRole(user=SAFE)))
        require_activated(role, SAFE)
        chain.mine(t + 1, [operator_tx(deposit)], [hype_log(SAFE, 10**16)])

        def act(custody, call, timestamp):
            receipt = chain.mine(timestamp, [operator_tx(call)], [action_log(SAFE, call)])
            custody = record_action(custody, call, receipt, timestamp)
            return CustodyRequests.reconcile(backend, custody)

        with pytest.raises(DataError, match="not in standard mode"):
            require_standard_mode(info.fetch(InfoRequest(info=UserAbstraction(user=SAFE))), SAFE)
        custody = act(custody, cw.standard_mode(SAFE), t + 2)
        require_standard_mode(info.fetch(InfoRequest(info=UserAbstraction(user=SAFE))), SAFE)
        custody, call = approve_agent(custody, keys, backend.now())
        custody = act(custody, call, t + 3)
        agent = rules.require_signing(custody.agents, backend.now())
        CustodyRequests.audit(backend, custody)
        custody = act(custody, cw.class_transfer(5_000_000, True), t + 4)
        custody = act(custody, cw.class_transfer(5_000_000, False), t + 5)
        withdrawal = cw.withdrawal(10_000_000)
        receipt = chain.mine(t + 6, [operator_tx(withdrawal)], [action_log(SAFE, withdrawal)])
        custody = record_action(custody, withdrawal, receipt, t + 6)
        custody = CustodyRequests.reconcile(backend, custody)
        assert custody.requests[-1].status == "unresolved"
        # The model's keeper pays it in the next block, from the system address.
        (owed,) = model.doc["deliveries"]
        pay = {"from": cw.USDC_SYSTEM, "to": CORE_DEPOSIT_WALLET, "input": payment(owed["amount"])}
        chain.mine(t + 7, [pay], [usdc_credit(SAFE, owed["amount"])])
        custody = CustodyRequests.reconcile(backend, custody)
        assert [r.status for r in custody.requests] == ["resolved"] * 5
        assert custody.requests[-1].evidence[1]["block"] == len(chain.blocks) - 1
        assert agent.name == "almanak1" and keys.held() == {agent.address}
        with pytest.raises(ValueError):
            require_activated(info.fetch(InfoRequest(info=UserRole(user=OTHER))), SAFE)
        with pytest.raises(ValueError):
            require_standard_mode(info.fetch(InfoRequest(info=UserAbstraction(user=OTHER))), SAFE)
        with pytest.raises(DataError, match="not activated"):
            require_activated(info.fetch(InfoRequest(info=UserRole(user=OTHER))), OTHER)
        info.close()
    assert CustodyRequests.cancellation_plan(None, custody, "a") is None
    assert not CustodyRequests.is_cancellation(custody, None)


def test_record_action_refuses_a_receipt_without_its_raw_action():
    custody = Custody(safe=SAFE)
    call = cw.class_transfer(1, True)
    receipt = {"transactionHash": "0x" + "01" * 32, "blockNumber": "0x1", "logs": []}
    with pytest.raises(DataError):
        record_action(custody, call, receipt, T0 // 1000)
    other = action_log(OTHER, call)
    with pytest.raises(DataError):
        record_action(custody, call, receipt | {"logs": [other]}, T0 // 1000)


def test_a_paid_withdrawal_without_its_send_entry_still_halts_after_the_window(tmp_path):
    """A system transfer found in the next block does not freeze the observation time:
    with no `send` entry the request halts once the head passes its window."""
    model = HyperCoreModel(tmp_path / "model.json", 10)
    chain = Chain(model)
    t = T0 // 1000
    funding = cw.deposit_calls(20_000_000)[1]
    chain.mine(t, [operator_tx(funding)], [credit_log(SAFE, 20_000_000), hype_log(SAFE, 10**16)])
    model.drop(cw.SEND_ASSET, SAFE)
    call = cw.withdrawal(5_000_000)
    receipt = chain.mine(t + 1, [operator_tx(call)], [action_log(SAFE, call)])
    custody = record_action(Custody(safe=SAFE), call, receipt, t + 1)
    pay = {"from": cw.USDC_SYSTEM, "to": CORE_DEPOSIT_WALLET, "input": payment(5_000_000)}
    chain.mine(t + 2, [pay], [usdc_credit(SAFE, 5_000_000)])
    with ModelService(model) as service:
        info = HyperCoreInfo(model=service.url, clock=lambda: chain.blocks[-1]["timestamp"] * 1000)
        backend = Backend(chain, info, None)
        custody = CustodyRequests.reconcile(backend, custody)
        assert custody.requests[0].status == "unresolved"
        chain.mine(t + 121)
        with pytest.raises(DataError) as halted:
            CustodyRequests.reconcile(backend, custody)
        assert halted.value.details["evidence"]["system"][0]["block"] == 13
        assert halted.value.details["evidence"]["head_time"] == (t + 121) * 1000
        info.close()


def withdrawal_chain(tmp_path, amount):
    """A model and chain where the Safe holds spot USDC and HYPE for the fee."""
    model = HyperCoreModel(tmp_path / "model.json", 10)
    chain = Chain(model)
    funding = cw.deposit_calls(amount)[1]
    credits = [credit_log(SAFE, 4 * amount), hype_log(SAFE, 10**16)]
    chain.mine(T0 // 1000, [operator_tx(funding)], credits)
    return model, chain


def withdraw(chain, custody, amount, timestamp):
    call = cw.withdrawal(amount)
    receipt = chain.mine(timestamp, [operator_tx(call)], [action_log(SAFE, call)])
    return record_action(custody, call, receipt, timestamp)


def test_a_reverted_payout_is_not_redeemed_by_a_later_withdrawals_payout(tmp_path):
    """The review case: a withdrawal whose next-block payout reverted stays unresolved
    although a separate withdrawal of the same amount is paid two blocks later."""
    amount, t = 5_000_000, T0 // 1000
    model, chain = withdrawal_chain(tmp_path, amount)
    pay = {"from": cw.USDC_SYSTEM, "to": CORE_DEPOSIT_WALLET, "input": payment(amount)}
    custody = withdraw(chain, Custody(safe=SAFE), amount, t + 1)
    chain.mine(t + 2, [pay], status="0x0")
    later = withdraw(chain, Custody(safe=SAFE), amount, t + 3)
    chain.mine(t + 4, [pay], [usdc_credit(SAFE, amount)])
    with ModelService(model) as service:
        info = HyperCoreInfo(model=service.url, clock=lambda: chain.blocks[-1]["timestamp"] * 1000)
        backend = Backend(chain, info, None)
        assert CustodyRequests.reconcile(backend, later).requests[0].status == "resolved"
        assert CustodyRequests.reconcile(backend, custody).requests[0].status == "unresolved"
        chain.mine(t + 121)
        with pytest.raises(DataError) as halted:
            CustodyRequests.reconcile(backend, custody)
        assert halted.value.details["evidence"]["system"] == []
        info.close()


def test_system_transfers_are_read_only_from_the_system_address():
    paid = calldata("transfer(address,uint256)", ("address", "uint256"), (SAFE, 7))
    transaction = {
        "hash": "0x" + "01" * 32,
        "from": cw.USDC_SYSTEM,
        "to": CORE_DEPOSIT_WALLET,
        "input": paid,
    }
    assert system_transfer(transaction, 5) == SystemTransfer(
        transaction=transaction["hash"], block=5, recipient=SAFE, amount=7
    )
    assert paid.startswith(CDW_TRANSFER)
    for changed in ({"from": OTHER}, {"to": OTHER}, {"input": "0x"}):
        assert system_transfer(transaction | changed, 5) is None


class Node:
    """A fork node for the keeper, with the system address's nonces, a mempool when
    automining is off, and a receipt wait that fails while a payment is unmined."""

    def __init__(self, blocks=2):
        self.blocks = [{"timestamp": 1, "transactions": []} for _ in range(blocks + 1)]
        self.receipts = {}
        self.pool = []
        self.calls = []
        self.automine = True

    def count(self, tag):
        mined = sum(tx["from"] == cw.USDC_SYSTEM for b in self.blocks for tx in b["transactions"])
        return mined + (len(self.pool) if tag == "pending" else 0)

    def __call__(self, method, params=()):
        self.calls.append(method)
        if method == "eth_blockNumber":
            return hex(len(self.blocks) - 1)
        if method == "eth_getBlockByNumber":
            block = self.blocks[int(params[0], 16)]
            return {"timestamp": hex(block["timestamp"]), "transactions": block["transactions"]}
        if method == "eth_getTransactionCount":
            return hex(self.count(params[1]))
        if method == "eth_sendTransaction":
            return self.send(params[0])
        if method == "eth_getTransactionReceipt":
            return self.receipts.get(params[0])
        if method in ("eth_getBalance", "eth_gasPrice"):
            return "0x5"
        return [] if method == "eth_getLogs" else None

    def send(self, tx):
        nonce = int(tx["nonce"], 16)
        if nonce != self.count("pending"):
            raise RuntimeError(f"nonce {nonce} is already used or pending")
        digest = "0x" + f"{len(self.blocks):032x}{nonce:032x}"
        keys = ("from", "to", "nonce")
        self.pool.append({"hash": digest, "input": tx["data"], **{k: tx[k] for k in keys}})
        if self.automine:
            self.mine()
        return digest

    def mine(self):
        self.blocks.append({"timestamp": 2, "transactions": self.pool})
        for tx in self.pool:
            self.receipts[tx["hash"]] = {"status": "0x1", "blockNumber": hex(len(self.blocks) - 1)}
        self.pool = []

    def wait_receipt(self, digest):
        if digest not in self.receipts:
            raise TimeoutError("the keeper stopped before its payment was mined")
        return self.receipts[digest]


def owed_delivery(path, block=2, amount=7):
    model = HyperCoreModel(path, block)
    model.doc["deliveries"].append({"id": f"{block}:0", "user": SAFE, "amount": amount})
    model.save()
    return model


def test_keeper_tails_blocks_and_pays_from_the_system_address(tmp_path):
    model = HyperCoreModel(tmp_path / "model.json", 0)
    node = Node()
    keeper = Keeper(node, model)
    model.doc["deliveries"].append({"id": "1:0", "user": SAFE, "amount": 1})
    rows = keeper.settle(None)
    assert [r.get("block") for r in rows] == [1, 2, 3]
    assert rows[-1]["advanced_seconds"] == 0 and model.doc["deliveries"] == []
    assert model.doc["paid"][0]["nonce"] == 0
    assert node.calls[-2:] == ["anvil_stopImpersonatingAccount", "anvil_setBalance"]
    assert keeper.align(None, 7) == 7


def test_a_restarted_keeper_waits_for_a_pending_payment_instead_of_paying_twice(tmp_path):
    """The review case: the payment is accepted but unmined when the keeper stops, and it
    is mined only after the keeper resumes."""
    path = tmp_path / "model.json"
    node = Node()
    node.automine = False
    with pytest.raises(TimeoutError):
        Keeper(node, owed_delivery(path)).advance()
    restarted = HyperCoreModel(path, 0)
    assert Keeper(node, restarted).advance() == [{"pending": restarted.doc["deliveries"][0]}]
    node.mine()
    rows = Keeper(node, HyperCoreModel(path, 0)).advance()
    assert rows[-1]["block"] == 3 and node.calls.count("eth_sendTransaction") == 1
    assert HyperCoreModel(path, 0).doc["deliveries"] == []


def test_a_restarted_keeper_records_a_mined_payment_and_resends_only_an_unused_nonce(tmp_path):
    path = tmp_path / "model.json"
    node = Node()
    model = owed_delivery(path)
    # Mined, but the keeper stopped before recording it.
    model.sending("2:0", 2, 0)
    node.send(
        {"from": cw.USDC_SYSTEM, "to": CORE_DEPOSIT_WALLET, "data": payment(7), "nonce": "0x0"}
    )
    rows = Keeper(node, HyperCoreModel(path, 0)).advance()
    assert rows[-1]["transaction"] == node.blocks[3]["transactions"][0]["hash"]
    assert node.calls.count("eth_sendTransaction") == 0
    # Noted as sending at a nonce nothing used: sent again, at that nonce.
    model = HyperCoreModel(path, 0)
    model.doc["deliveries"].append({"id": "3:0", "user": SAFE, "amount": 8})
    model.sending("3:0", 3, 1)
    Keeper(node, model).advance()
    assert node.calls.count("eth_sendTransaction") == 1 and model.doc["deliveries"] == []
    # Another transaction at a delivery's nonce paid nothing: the model stops.
    model.doc["deliveries"].append({"id": "4:0", "user": SAFE, "amount": 9})
    model.sending("4:0", 4, 2)
    node.send(
        {"from": cw.USDC_SYSTEM, "to": CORE_DEPOSIT_WALLET, "data": payment(1), "nonce": "0x2"}
    )
    with pytest.raises(RuntimeError, match="paid no delivery"):
        Keeper(node, model).advance()


class Forks:
    def __init__(self, rpc):
        self.node = rpc

    def rpc(self, chain):
        return self.node


def test_fork_keeper_opens_the_model_at_the_fork_head(tmp_path):
    keeper = CustodyRequests.fork_keeper(Forks(Node()), tmp_path / "keepers")
    assert keeper.model.doc["block"] == 2
    assert keeper.model.path == tmp_path / "keepers" / "hypercore-model.json"
