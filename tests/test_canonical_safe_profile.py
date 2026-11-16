"""The canonical Safe 1.4.1 profile: SafeL2, its compatibility handler and a pre-assigned role."""

import json
from types import SimpleNamespace

import pytest
from eth_utils import keccak

from almanak.gateway import roles_storage
from almanak.gateway.ethereum import DataError
from almanak.gateway.permissions import installation_bundle
from almanak.gateway.safe_fixture import (
    ROLES_SINGLETON,
    SAFE_L2_SINGLETON,
    SAFE_SINGLETON,
)

SAFE = "0x543c384765c940836f9be600bb6b89f881962f43"
ROLES = "0x1b51ef07080c0fa399a4a952516c9245f82e8308"
OPERATOR = "0x4afc7b839eea6a396a508aa29e5a4c26280d5bea"
ROLE = b"AlmanakAgentRole".ljust(32, b"\0")


def profile():
    return SimpleNamespace(
        fund=SimpleNamespace(address=SAFE),
        operator=SimpleNamespace(address=OPERATOR),
        roles_module=ROLES,
        role_key="0x" + ROLE.hex(),
    )


def test_a_pre_assigned_operator_is_not_assigned_again():
    selector = "0x" + keccak(text="assignRoles(address,bytes32[],bool[])")[:4].hex()
    partition = SimpleNamespace(profile=profile(), grants=())
    assigned = installation_bundle(partition, assign_operator=False)
    fresh = installation_bundle(partition)
    assert not any(call.data.startswith(selector) for call in assigned)
    assert fresh[-1].data.startswith(selector)


class Reader:
    """Pinned-authority reads for one Safe; unknown reads fail the test."""

    def __init__(self, singleton, handler=0, codes=None):
        roles_artifact = json.loads(
            (roles_storage.ARTIFACTS / "roles-2.1.0-bytecode.json").read_text()
        )
        proxy = "363d3d373d3d3d363d73" + ROLES_SINGLETON[2:] + "5af43d82803e903d91602b57fd5bf3"
        self.codes = {
            SAFE: b"safe-proxy",
            SAFE_SINGLETON: b"safe-l1",
            SAFE_L2_SINGLETON: b"safe-l2",
            ROLES_SINGLETON: bytes.fromhex(roles_artifact["bytecode"].removeprefix("0x")),
            ROLES: bytes.fromhex(proxy),
            roles_storage.COMPATIBILITY_FALLBACK_HANDLER: b"compat-handler",
            **(codes or {}),
        }
        self.singleton, self.handler = singleton, handler

    def code(self, address, tag=None):
        return self.codes[address]

    def call(self, address, signature, *, inputs=(), args=(), outputs=("address",)):
        return {
            (SAFE, "masterCopy()"): (self.singleton,),
            (SAFE, "VERSION()"): ("1.4.1",),
        }[(address, signature)]

    def storage(self, address, slot):
        assert address == SAFE
        slots = {roles_storage.GUARD_SLOT: 0, roles_storage.FALLBACK_HANDLER_SLOT: self.handler}
        return slots[slot]


@pytest.fixture
def pinned(monkeypatch):
    digest = lambda code: "0x" + keccak(code).hex()  # noqa: E731
    monkeypatch.setattr(roles_storage, "SAFE_PROXY_HASH", digest(b"safe-proxy"))
    monkeypatch.setattr(
        roles_storage,
        "SAFE_SINGLETONS",
        {SAFE_SINGLETON: digest(b"safe-l1"), SAFE_L2_SINGLETON: digest(b"safe-l2")},
    )
    monkeypatch.setattr(
        roles_storage, "COMPATIBILITY_FALLBACK_HANDLER_HASH", digest(b"compat-handler")
    )


@pytest.mark.parametrize("singleton", [SAFE_SINGLETON, SAFE_L2_SINGLETON])
def test_both_canonical_safe_singletons_are_admitted(pinned, singleton):
    roles_storage.verify_implementations(Reader(singleton), profile())


def test_other_singletons_and_altered_singleton_code_are_refused(pinned):
    with pytest.raises(DataError, match="outside the supported profile"):
        roles_storage.verify_implementations(Reader("0x" + "22" * 20), profile())
    altered = Reader(SAFE_L2_SINGLETON, codes={SAFE_L2_SINGLETON: b"patched"})
    with pytest.raises(DataError, match="differs from its verified artifact"):
        roles_storage.verify_implementations(altered, profile())


def test_only_no_handler_or_the_canonical_compatibility_handler_is_admitted(pinned):
    handler = int(roles_storage.COMPATIBILITY_FALLBACK_HANDLER, 16)
    roles_storage.verify_fallback_handler(Reader(SAFE_L2_SINGLETON), SAFE)
    roles_storage.verify_fallback_handler(Reader(SAFE_L2_SINGLETON, handler=handler), SAFE)
    other = Reader(SAFE_L2_SINGLETON, handler=int("33" * 20, 16))
    with pytest.raises(DataError, match="canonical 1.4.1 handler"):
        roles_storage.verify_fallback_handler(other, SAFE)
    altered = Reader(
        SAFE_L2_SINGLETON,
        handler=handler,
        codes={roles_storage.COMPATIBILITY_FALLBACK_HANDLER: b"patched"},
    )
    with pytest.raises(DataError, match="canonical 1.4.1 handler"):
        roles_storage.verify_fallback_handler(altered, SAFE)
