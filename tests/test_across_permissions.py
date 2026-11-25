"""The Roles grants of a bridge, held to the calldata the connector's own planner signs.

`admits` evaluates a grant's condition tree over calldata the way Roles 2.1 does for
the operators these grants use: a static word is compared as the word, and a dynamic
value by its length word and padded bytes. It is a model for these tests, not the
contract: no grant of a bridge was ever installed in a Roles module.
"""

from types import SimpleNamespace

import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.caip import AssetType
from almanak.connectors.across.authority import declared
from almanak.connectors.across.operations import (
    DEPOSIT,
    DEPOSIT_TYPES,
    ApproveOperation,
    DepositOperation,
    RevokeOperation,
    deposit_arguments,
    plan_call,
)
from almanak.connectors.across.operations import word as padded
from almanak.connectors.across.permissions import AcrossPermissions, derive
from almanak.gateway import permissions
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import Account, AccountBinding, SafeRoles, StrategyAccounts
from tests.across_support import (
    ACCOUNTS,
    AMOUNT,
    ARBITRUM,
    BASE,
    BASE_USDC,
    BLOCK,
    PIN,
    RECIPIENT,
    SENDER,
    USDC,
    allocation,
    parameters,
    quote,
    route,
)

AND, OR, MATCHES, EQUAL, GREATER = 1, 2, 5, 16, 17
STATIC, DYNAMIC = 1, 2
OTHER = "0x" + "99" * 20
ROLES, ROLE = "0x" + "55" * 20, "0x" + "66" * 32
ARGUMENTS = (
    "depositor",
    "recipient",
    "input_token",
    "output_token",
    "input_amount",
    "output_amount",
    "destination_chain",
    "exclusive_relayer",
    "quote_timestamp",
    "fill_deadline",
    "exclusivity",
    "message",
)


def safe(name, chain, address):
    fund = Account(chain=chain, address=address)
    profile = SafeRoles(
        kind="safe_roles",
        fund=fund,
        operator=Account(chain=chain, address="0x" + "44" * 20),
        roles_module=ROLES,
        role_key=ROLE,
    )
    return AccountBinding(name=name, profile=profile)


SAFE_SOURCE = StrategyAccounts(bindings=(safe("arb", ARBITRUM, SENDER), ACCOUNTS.bindings[1]))
SAFE_BOTH = StrategyAccounts(
    bindings=(safe("arb", ARBITRUM, SENDER), safe("base", BASE, RECIPIENT))
)
CONFIG = parameters(accounts=SAFE_SOURCE)
TARGET = CONFIG.target("move")
PROFILE = CONFIG.profile("arb")


def grants(config=CONFIG, account="arb"):
    return {g.signature: g for g in derive(config.profile(account), declared(config))}


def deposit(target=TARGET, **terms):
    return DepositOperation(
        target=target,
        amount=CONFIG.bridge("move", AMOUNT).amount,
        quote=quote(**terms),
        quote_time_buffer=3600,
    )


def encoded(values) -> str:
    return "0x" + (keccak(text=DEPOSIT)[:4] + encode(DEPOSIT_TYPES, values)).hex()


def changed(**changes) -> str:
    """The planner's deposit calldata with named arguments replaced."""
    values = dict(zip(ARGUMENTS, deposit_arguments(deposit()), strict=True))
    return encoded(tuple({**values, **changes}.values()))


def argument(data: bytes, index: int, kind: int) -> bytes:
    """What Roles compares for one top-level argument: the head word of a static one,
    the length word and padded bytes of a dynamic one."""
    head = data[4 + 32 * index : 36 + 32 * index]
    if kind == STATIC:
        return head
    start = 4 + int.from_bytes(head, "big")
    length = int.from_bytes(data[start : start + 32], "big")
    return data[start : start + 32 + (length + 31) // 32 * 32]


def holds(condition, payload: bytes) -> bool:
    value = bytes.fromhex(condition.value[2:])
    if condition.operator == AND:
        return all(holds(child, payload) for child in condition.children)
    if condition.operator == OR:
        return any(holds(child, payload) for child in condition.children)
    if condition.operator == GREATER:
        return int.from_bytes(payload, "big") > int.from_bytes(value, "big")
    assert condition.operator == EQUAL
    # The deployed Packer drops the head offset of a dynamic comparison value.
    return payload == (value[32:] if condition.parameter == DYNAMIC else value)


def kind(condition) -> int:
    return kind(condition.children[0]) if condition.operator in (AND, OR) else condition.parameter


def admits(grant, call) -> bool:
    """Whether the grant's target, selector and every argument condition admit the call."""
    data = bytes.fromhex(call.data[2:])
    if (call.to, data[:4], call.value) != (grant.target, grant.selector, 0):
        return False
    root = grant.condition
    assert root.operator == MATCHES and root.parameter == 5
    return all(
        holds(child, argument(data, index, kind(child)))
        for index, child in enumerate(root.children)
    )


def call(data: str, to=PIN.address):
    return SimpleNamespace(to=to, data=data, value=0)


def test_a_safe_source_takes_one_approval_and_one_deposit_grant():
    found = grants()
    assert set(found) == {"approve(address,uint256)", DEPOSIT}
    assert found[DEPOSIT].target == TARGET.route.spoke_pool == PIN.address
    assert found[DEPOSIT].selector == keccak(text=DEPOSIT)[:4]
    assert found["approve(address,uint256)"].target == USDC.reference
    # Every grant compiles to the flat conditions an owner installs.
    assert all(grant.conditions for grant in found.values())
    # No Roles allowance and no custom condition: scope only.
    assert {c.operator for g in found.values() for c in g.conditions} <= {AND, OR, 5, 16, 17}


def test_the_deposit_grant_pins_each_argument_by_position():
    conditions = dict(zip(ARGUMENTS, grants()[DEPOSIT].condition.children, strict=True))
    pinned = {
        "depositor": padded(SENDER),
        "recipient": padded(RECIPIENT),
        "input_token": padded(USDC.reference),
        "output_token": padded(BASE_USDC.reference),
        "destination_chain": (8453).to_bytes(32, "big"),
    }
    for name, expected in pinned.items():
        condition = conditions[name]
        assert (condition.parameter, condition.operator) == (STATIC, EQUAL), name
        assert bytes.fromhex(condition.value[2:]) == expected, name
    # Amounts and the two times: any nonzero word, never a bound.
    for name in ("input_amount", "output_amount", "quote_timestamp", "fill_deadline"):
        condition = conditions[name]
        assert (condition.operator, int(condition.value, 16)) == (GREATER, 0), name
    # The venue's relayer and exclusivity: any word, zero included.
    for name in ("exclusive_relayer", "exclusivity"):
        assert conditions[name].operator == OR, name
        assert {child.operator for child in conditions[name].children} == {EQUAL, GREATER}
    message = conditions["message"]
    assert (message.parameter, message.operator) == (DYNAMIC, EQUAL)
    assert decode(("bytes",), bytes.fromhex(message.value[2:])) == (b"",)


def test_a_deposit_the_planner_builds_for_a_safe_satisfies_its_grant():
    grant = grants()[DEPOSIT]
    planned = plan_call(deposit())
    assert planned.fund == PROFILE.fund and planned.data == changed()
    assert admits(grant, planned)
    # The venue's choices per quote stay free: no relayer, another one, other times.
    assert admits(grant, plan_call(deposit(exclusiveRelayer="0x" + "00" * 20)))
    assert admits(grant, call(changed(exclusive_relayer=padded(OTHER), exclusivity=0)))
    assert admits(grant, call(changed(quote_timestamp=1, fill_deadline=2**32 - 1)))
    # Any nonzero amounts: Roles relates neither to the other.
    assert admits(grant, call(changed(input_amount=2**255, output_amount=1)))


@pytest.mark.parametrize(
    "refused",
    [
        {"depositor": padded(OTHER)},
        {"recipient": padded(OTHER)},
        {"recipient": padded(SENDER)},
        {"input_token": padded(OTHER)},
        {"output_token": padded(OTHER)},
        {"destination_chain": 1},
        {"message": b"\x01"},
        {"message": bytes(32)},
        {"input_amount": 0},
        {"output_amount": 0},
        {"quote_timestamp": 0},
        {"fill_deadline": 0},
    ],
    ids=lambda refused: "-".join(refused),
)
def test_a_deposit_outside_the_route_does_not_satisfy_the_grant(refused):
    assert not admits(grants()[DEPOSIT], call(changed(**refused)))


def test_a_deposit_to_another_contract_or_with_value_is_outside_the_grant():
    grant = grants()[DEPOSIT]
    assert not admits(grant, call(changed(), to=OTHER))
    assert not admits(grant, SimpleNamespace(to=PIN.address, data=changed(), value=1))
    # Another function of the SpokePool with the same arguments is another selector.
    unsafe = "0x" + keccak(text=DEPOSIT.replace("deposit", "unsafeDeposit"))[:4].hex()
    assert not admits(grant, call(unsafe + changed()[10:]))


def test_the_approval_grant_pins_the_spoke_pool_and_admits_the_revocation():
    grant = grants()["approve(address,uint256)"]
    spender, amount = grant.condition.children
    assert decode(("address",), bytes.fromhex(spender.value[2:])) == (PIN.address,)
    assert {child.operator for child in amount.children} == {EQUAL, GREATER}
    approve = plan_call(ApproveOperation(target=TARGET, amount=CONFIG.bridge("move", 7).amount))
    revoke = plan_call(RevokeOperation(target=TARGET, final=True))
    assert admits(grant, approve) and admits(grant, revoke)
    other = "0x" + (grant.selector + encode(("address", "uint256"), (OTHER, 0))).hex()
    assert not admits(grant, call(other, to=USDC.reference))
    assert not admits(grant, call(revoke.data, to=BASE_USDC.reference))


def test_only_the_sending_safe_takes_grants_and_each_route_its_own():
    both = parameters(accounts=SAFE_BOTH)
    assert set(grants(both, "arb")) == {"approve(address,uint256)", DEPOSIT}
    # A Safe that only receives calls nothing and takes no grant.
    assert derive(both.profile("base"), declared(both)) == ()
    assert grants(both)[DEPOSIT] == grants()[DEPOSIT]
    # A grant follows its route: another recipient is another condition.
    elsewhere = StrategyAccounts(
        bindings=(SAFE_SOURCE.bindings[0], safe("base", BASE, OTHER)),
    )
    moved = grants(parameters(accounts=elsewhere))[DEPOSIT]
    assert moved != grants()[DEPOSIT] and not admits(moved, plan_call(deposit()))


def test_two_routes_of_one_safe_keep_their_arguments_correlated():
    usdt = AssetType("eip155:42161/erc20:0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9")
    base_usdt = AssetType("eip155:8453/erc20:0xfde4c96c8593536e31f229ea8f37b2ada2699bb2")
    second = allocation(name="other", route=route(asset=usdt, destination_asset=base_usdt))
    config = parameters(allocation(), second, accounts=SAFE_SOURCE)
    grant = grants(config)[DEPOSIT]
    assert grant.condition.operator == OR and len(grant.condition.children) == 2

    def any_branch(data):
        return any(
            admits(grant.model_copy(update={"condition": branch}), call(data))
            for branch in grant.condition.children
        )

    assert any_branch(changed())
    # One route's input token with the other route's output token fits neither branch.
    assert not any_branch(changed(output_token=padded(base_usdt.reference)))
    assert any_branch(
        changed(input_token=padded(usdt.reference), output_token=padded(base_usdt.reference))
    )


def test_the_identities_are_the_sent_assets_and_never_the_spoke_pool():
    seen = []

    def identity(address):
        seen.append(address)
        return ContractIdentity(address=address, code_hash="0x" + "ab" * 32)

    reader = SimpleNamespace(identity=identity)
    found = AcrossPermissions.identities(CONFIG, "arb", reader)
    assert [contract.address for contract in found] == [USDC.reference] == seen
    assert AcrossPermissions.identities(CONFIG, "base", reader) == ()
    text = " ".join(AcrossPermissions.runtime_checks)
    assert "gateway checks" in text and "Roles cannot relate" in text


class Storage:
    """Roles storage with nothing installed and no member."""

    def __init__(self, reader, block, reads=None):
        self.reader, self.block = reader, block

    def storage(self, address, slot):
        return 0

    def identity(self, address):
        return ContractIdentity(address=address, code_hash="0x" + "ab" * 32)


def generated(monkeypatch, config):
    reader = SimpleNamespace(pin_block=lambda tag: BLOCK)
    backend = SimpleNamespace(reader=reader, guard=lambda: None)
    monkeypatch.setattr(permissions, "backend_for", lambda selected, chain: selected)
    monkeypatch.setattr(permissions, "AuthorityReader", Storage)
    monkeypatch.setattr(permissions, "verify_wiring", lambda *a, **k: None)
    strategy = SimpleNamespace(sha256="0" * 64)
    prepared = SimpleNamespace(require_configuration=lambda *a: None, capabilities=declared(config))
    return permissions.generate(
        config, strategy, backend, connector=AcrossPermissions, prepared=prepared
    )


def selectors(partition) -> list[str]:
    return [transaction.data[:10] for transaction in partition.transactions]


def selector(signature: str) -> str:
    return "0x" + keccak(text=signature)[:4].hex()


def test_a_safe_beside_an_eoa_generates_one_partition_and_its_owner_bundle(monkeypatch):
    manifest = generated(monkeypatch, CONFIG)
    (partition,) = manifest.partitions
    assert (partition.account, partition.profile) == ("arb", PROFILE)
    assert partition.grants == tuple(derive(PROFILE, declared(CONFIG)))
    assert [contract.address for contract in partition.contracts] == [USDC.reference]
    scope_target = selector("scopeTarget(bytes32,address)")
    scope_function = selector(
        "scopeFunction(bytes32,address,bytes4,(uint8,uint8,uint8,bytes)[],uint8)"
    )
    assign = selector("assignRoles(address,bytes32[],bool[])")
    assert selectors(partition) == [scope_target] * 2 + [scope_function] * 2 + [assign]
    assert {transaction.to for transaction in partition.transactions} == {ROLES}
    # Execution options 0 on every function: no native value and no delegate call.
    # The Safe executes this calldata, so each field is read back from it, not from the
    # in-memory grant the manifest check compares.
    role = bytes.fromhex(PROFILE.role_key[2:])
    for transaction, grant in zip(partition.transactions[2:4], partition.grants, strict=True):
        key, target, chosen, conditions, options = decode(
            ("bytes32", "address", "bytes4", "(uint8,uint8,uint8,bytes)[]", "uint8"),
            bytes.fromhex(transaction.data[10:]),
        )
        assert (key, target.lower(), options) == (role, grant.target, 0)
        assert chosen == grant.selector == keccak(text=grant.signature)[:4]
        assert list(conditions) == [tuple(c.abi()) for c in grant.conditions]
    operator, keys, enabled = decode(
        ("address", "bytes32[]", "bool[]"), bytes.fromhex(partition.transactions[4].data[10:])
    )
    assert operator.lower() == PROFILE.operator.address
    assert (list(keys), list(enabled)) == ([role], [True])
    permissions.validate_manifest(CONFIG, manifest, connector=AcrossPermissions)


def test_a_receiving_safe_has_a_partition_with_no_grant(monkeypatch):
    config = parameters(accounts=SAFE_BOTH)
    manifest = generated(monkeypatch, config)
    assert [partition.account for partition in manifest.partitions] == ["arb", "base"]
    receiving = manifest.partitions[1]
    assert (receiving.grants, receiving.contracts) == ((), ())
    # Its owner only assigns the operator its role; nothing is scoped.
    assert selectors(receiving) == [selector("assignRoles(address,bytes32[],bool[])")]
    permissions.validate_manifest(config, manifest, connector=AcrossPermissions)


def test_a_manifest_must_hold_exactly_the_safe_partitions(monkeypatch):
    manifest = generated(monkeypatch, CONFIG)
    assert permissions.safe_bindings(parameters()) == ()
    empty = manifest.model_copy(update={"partitions": ()})
    with pytest.raises(ValueError, match="each Safe authorization partition"):
        permissions.validate_manifest(CONFIG, empty, connector=AcrossPermissions)
    partition = manifest.partitions[0]
    extra = partition.grants[0].model_copy(update={"target": OTHER})
    widened = partition.model_copy(update={"grants": (*partition.grants, extra)})
    with pytest.raises(ValueError, match="differ from trusted connector derivation"):
        permissions.validate_manifest(
            CONFIG,
            manifest.model_copy(update={"partitions": (widened,)}),
            connector=AcrossPermissions,
        )
    simulated = SimpleNamespace(
        accounts=SimpleNamespace(bindings=(SimpleNamespace(profile=object()),))
    )
    with pytest.raises(ValueError, match="explicit supported account adapter"):
        permissions.safe_bindings(simulated)


def test_one_safe_address_on_both_chains_takes_grants_on_its_source_chain_only():
    """A Safe is normally deployed at one address on every chain. The destination
    chain's Safe has the source Safe's address and still takes no grant: a grant
    follows the fund's chain and address together."""
    accounts = StrategyAccounts(
        bindings=(safe("arb", ARBITRUM, SENDER), safe("base", BASE, SENDER))
    )
    config = parameters(accounts=accounts)
    source, destination = (binding.profile for binding in accounts.bindings)
    assert source.fund.address == destination.fund.address and source.fund != destination.fund
    capabilities = declared(config)
    assert derive(destination, capabilities) == ()
    found = {g.signature: g for g in derive(source, capabilities)}
    assert set(found) == {"approve(address,uint256)", DEPOSIT}
    depositor, recipient = found[DEPOSIT].condition.children[:2]
    assert depositor.value == recipient.value == "0x" + padded(SENDER).hex()


class Wired(Storage):
    """Roles storage of a Safe that only receives: no scoped target, and the operator
    in the role or not, as `member` and `modules` say."""

    member, enabled = 1, None

    def storage(self, address, slot):
        profile = SAFE_BOTH.bindings[1].profile
        held = (address, slot) == (profile.roles_module, permissions.membership_slot(profile))
        return self.member if held else 0

    def modules(self, address):
        operator = SAFE_BOTH.bindings[1].profile.operator.address
        return (operator,) if self.enabled is None else self.enabled


def receiving(monkeypatch, wiring=None, **storage):
    """Generate the receiving Safe's partition over `Wired` storage; `wiring` stands
    for the Safe and Roles wiring check and records what it was asked to hold."""
    config = parameters(
        accounts=StrategyAccounts(bindings=(ACCOUNTS.bindings[0], SAFE_BOTH.bindings[1]))
    )
    asked = []

    def checked(reader, profile, *, operator_enabled):
        asked.append(operator_enabled)
        if wiring is not None:
            raise wiring

    backend = SimpleNamespace(
        reader=SimpleNamespace(pin_block=lambda tag: BLOCK, rpc_count=0), guard=lambda: None
    )
    monkeypatch.setattr(permissions, "backend_for", lambda selected, chain: selected)
    monkeypatch.setattr(permissions, "AuthorityReader", type("Reads", (Wired,), storage))
    monkeypatch.setattr(permissions, "verify_wiring", checked)
    prepared = SimpleNamespace(require_configuration=lambda *a: None, capabilities=declared(config))
    manifest = permissions.generate(
        config,
        SimpleNamespace(sha256="0" * 64),
        backend,
        connector=AcrossPermissions,
        prepared=prepared,
    )
    return config, manifest, backend, asked


def test_a_wired_receiving_safe_is_installed_and_verifies(monkeypatch):
    config, manifest, backend, asked = receiving(monkeypatch)
    (partition,) = manifest.partitions
    # The EOA that sends has no partition; the Safe that receives has one with no grant.
    assert (partition.account, partition.grants, partition.transactions) == ("base", (), ())
    assert asked == [True]
    stated = permissions.statement(manifest)
    assert stated["pending"] is False and stated["partitions"][0]["status"] == "installed"
    monkeypatch.setattr(Wired, "retain_plan", lambda self: None, raising=False)
    verified = permissions.verify(config, manifest, backend, connector=AcrossPermissions)
    assert verified["partitions"] == [{"account": "base"}] and asked == [True, True]


@pytest.mark.parametrize(
    "storage",
    [{"member": 0}, {"enabled": ()}, {"enabled": (OTHER,)}],
    ids=["no-role", "operator-not-enabled", "another-module"],
)
def test_a_receiving_safe_without_its_operator_exports_the_pending_assignment(monkeypatch, storage):
    config, manifest, backend, asked = receiving(monkeypatch, **storage)
    (partition,) = manifest.partitions
    assert selectors(partition) == [selector("assignRoles(address,bytes32[],bool[])")]
    # The wiring is then held with no operator enabled, so another module refuses there.
    assert asked == [False]
    stated = permissions.statement(manifest)
    assert stated["pending"] is True and stated["partitions"][0]["status"] == "pending"


def test_a_receiving_safe_with_other_wiring_or_no_membership_fails_closed(monkeypatch):
    from almanak.gateway.ethereum import DataError

    wrong = DataError("authority", "Safe must enable only its dedicated Roles module")
    with pytest.raises(DataError, match="dedicated Roles module"):
        receiving(monkeypatch, wiring=wrong)
    # Installed when generated, the role gone when verified: verification refuses.
    config, manifest, backend, _ = receiving(monkeypatch)
    monkeypatch.setattr(Wired, "retain_plan", lambda self: None, raising=False)
    monkeypatch.setattr(permissions, "AuthorityReader", type("Gone", (Wired,), {"member": 0}))
    with pytest.raises(DataError, match="role membership is absent"):
        permissions.verify(config, manifest, backend, connector=AcrossPermissions)
