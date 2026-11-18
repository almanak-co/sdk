"""Safe/Roles authority read from storage alone: installed state and assignment.

Grants carry scope only (blueprint 27), so no allowance word is installed or read.
"""

import json
from types import SimpleNamespace

import pytest
from eth_utils import keccak

from almanak.caip import ChainId
from almanak.gateway import permissions
from almanak.gateway.ethereum import DataError
from almanak.gateway.roles_conditions import FunctionGrant
from almanak.gateway.roles_storage import mapping_slot
from almanak.models import Block, Chain, SafeRoles

CHAIN = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
SAFE = "0x" + "5a" * 20
ROLES = "0x" + "4a" * 20
OPERATOR = "0x" + "0b" * 20
TARGET = "0x" + "7a" * 20
ROLE = b"AlmanakAgentRole".ljust(32, b"\0")
PROFILE = SafeRoles.model_validate_json(
    json.dumps(
        {
            "kind": "safe_roles",
            "fund": {"chain": CHAIN.model_dump(mode="json"), "address": SAFE},
            "operator": {"chain": CHAIN.model_dump(mode="json"), "address": OPERATOR},
            "roles_module": ROLES,
            "role_key": "0x" + ROLE.hex(),
        }
    )
)
GRANT = FunctionGrant(target=TARGET, signature="withdraw()", condition=None, explanation=("x",))
BLOCK = Block(chain=CHAIN, number=10, hash="0x" + "11" * 32, timestamp=10)

ROLE_SLOT = mapping_slot("bytes32", ROLE, 4)
CLEARANCE = mapping_slot("address", TARGET, ROLE_SLOT + 1)
SCOPE = mapping_slot(
    "bytes32", bytes.fromhex(TARGET[2:]) + GRANT.selector + bytes(8), ROLE_SLOT + 2
)
MEMBER = mapping_slot("address", OPERATOR, ROLE_SLOT)
# A leftover allowance from an earlier quota-bearing installation: nothing reads it.
ALLOWANCE = mapping_slot("bytes32", bytes.fromhex("99" * 32), 5)
ASSIGN_ROLES = "0x" + keccak(text="assignRoles(address,bytes32[],bool[])")[:4].hex()


LEFTOVER = {ALLOWANCE: 1_000 << 128, ALLOWANCE + 1: 400 << 64}

# A complete installation: scoped target, the selector-only header and membership.
INSTALLED = {CLEARANCE: 2, SCOPE: 1 << 216, MEMBER: 1}


class Reader:
    """Roles storage at the pinned block; unknown slots read as zero."""

    def __init__(self, slots=(), modules=()):
        self.slots, self.roles_modules = dict(slots), tuple(modules)

    def storage(self, address, slot):
        assert address == ROLES
        return self.slots.get(slot, 0)

    def modules(self, address):
        assert address == ROLES
        return self.roles_modules


def partition():
    return SimpleNamespace(account="main", profile=PROFILE, grants=(GRANT,), contracts=())


@pytest.fixture
def generate(monkeypatch):
    """Run `generate` for one Safe account over a storage `Reader`; wiring checks are recorded."""
    wiring = []
    monkeypatch.setattr(
        permissions,
        "backend_for",
        lambda _backend, _chain: SimpleNamespace(
            guard=lambda: None, reader=SimpleNamespace(pin_block=lambda _: BLOCK)
        ),
    )
    monkeypatch.setattr(
        permissions,
        "verify_wiring",
        lambda _reader, _profile, *, operator_enabled: wiring.append(operator_enabled),
    )
    monkeypatch.setattr(permissions, "configuration_hash", lambda _config: "0" * 64)
    connector = SimpleNamespace(
        codec=SimpleNamespace(evaluate=lambda _strategy, _config: "capabilities"),
        derive=lambda _profile, _capabilities: (GRANT,),
        identities=lambda _config, _name, _reader: (),
        manifest_type=lambda **fields: SimpleNamespace(**fields),
        runtime_checks=(),
    )
    config = SimpleNamespace(
        accounts=SimpleNamespace(bindings=(SimpleNamespace(name="main", profile=PROFILE),))
    )

    def run(reader):
        monkeypatch.setattr(permissions, "AuthorityReader", lambda _raw, _block: reader)
        manifest = permissions.generate(
            config, SimpleNamespace(sha256="0" * 64), None, connector=connector
        )
        (generated,) = manifest.partitions
        return generated, wiring

    return run


def assigns_roles(generated):
    return any(call.data.startswith(ASSIGN_ROLES) for call in generated.transactions)


def test_an_operator_assigned_at_safe_creation_is_not_assigned_again(generate):
    generated, wiring = generate(Reader({MEMBER: 1}, modules=(OPERATOR,)))
    assert generated.transactions and not assigns_roles(generated)
    # The wiring check expects the operator as the Roles module's only module.
    assert wiring == [True]


@pytest.mark.parametrize(
    "reader",
    [
        pytest.param(Reader(), id="unassigned"),
        pytest.param(Reader({MEMBER: 1}), id="member-without-module"),
    ],
)
def test_an_unassigned_operator_is_assigned_by_the_bundle(generate, reader):
    generated, wiring = generate(reader)
    assert assigns_roles(generated)
    assert wiring == [False]


def test_a_complete_installation_is_verified_with_no_bundle(generate):
    generated, wiring = generate(Reader(INSTALLED, modules=(OPERATOR,)))
    assert generated.transactions == ()
    # The full partition verification checks the wiring once, with the operator enabled.
    assert wiring == [True]


@pytest.mark.parametrize(
    "slots",
    [
        pytest.param({CLEARANCE: 2}, id="target-without-function-scope"),
        pytest.param({**INSTALLED, MEMBER: 0}, id="grants-without-membership"),
    ],
)
def test_a_partial_installation_is_refused(generate, slots):
    with pytest.raises(DataError):
        generate(Reader(slots, modules=(OPERATOR,)))


@pytest.fixture
def verify_partition(monkeypatch):
    """`verify_partition` over a storage `Reader`, with the wiring check covered elsewhere."""
    monkeypatch.setattr(permissions, "verify_wiring", lambda *_args, **_kwargs: None)
    return lambda reader: permissions.verify_partition(reader, partition())


def test_verification_reports_no_quotas(verify_partition):
    assert verify_partition(Reader(INSTALLED, modules=(OPERATOR,))) == {"account": "main"}


def test_an_allowance_left_by_an_earlier_installation_is_ignored(generate, verify_partition):
    assert verify_partition(Reader({**INSTALLED, **LEFTOVER}, modules=(OPERATOR,)))
    # Leftover allowance words alone do not count as a started installation.
    generated, _ = generate(Reader(LEFTOVER))
    assert generated.transactions


def test_the_bundle_installs_no_allowance(generate):
    generated, _ = generate(Reader())
    set_allowance = (
        "0x" + keccak(text="setAllowance(bytes32,uint128,uint128,uint128,uint64,uint64)")[:4].hex()
    )
    assert not any(call.data.startswith(set_allowance) for call in generated.transactions)


def test_a_default_role_for_the_operator_is_refused(verify_partition):
    default = mapping_slot("address", OPERATOR, 7)
    with pytest.raises(DataError, match="default role"):
        verify_partition(Reader({**INSTALLED, default: 1}, modules=(OPERATOR,)))
