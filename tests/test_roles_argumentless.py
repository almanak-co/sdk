"""Selector-only grants cannot erase parameter or execution-option constraints."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import decode
from eth_utils import keccak

from almanak.gateway.ethereum import DataError
from almanak.gateway.permissions import function_installation
from almanak.gateway.roles_conditions import FunctionGrant, combine, equal, matches
from almanak.gateway.roles_storage import mapping_slot, verify_grant_storage
from almanak.models import SafeRoles


def fixture():
    raw = json.loads(
        gzip.decompress(
            (
                Path(__file__).with_name("fixtures") / "safe-roles-2.1.0-authority.json.gz"
            ).read_bytes()
        )
    )
    profile = SafeRoles.model_validate_json(json.dumps(raw["manifest"]["partitions"][0]["profile"]))
    grant = FunctionGrant(
        target="0x" + "ab" * 20,
        signature="claimFees()",
        condition=None,
        explanation=("Claim this pool's fund fees.",),
    )
    return profile, grant


@pytest.mark.parametrize(
    "signature",
    ["approve(address,uint256)", "claimFees(uint256)", "claimFees( )", "claimFees()extra"],
)
def test_parameterized_or_noncanonical_signatures_require_conditions(signature):
    _, grant = fixture()
    with pytest.raises(ValueError, match="zero-argument"):
        FunctionGrant(**{**grant.model_dump(), "signature": signature})


def test_argumentless_owner_bundle_retains_exact_target_selector_and_no_options():
    profile, grant = fixture()
    installed = function_installation(profile, grant)
    raw = bytes.fromhex(installed.data[2:])
    assert raw[:4] == keccak(text="allowFunction(bytes32,address,bytes4,uint8)")[:4]
    assert decode(("bytes32", "address", "bytes4", "uint8"), raw[4:]) == (
        bytes.fromhex(profile.role_key[2:]),
        grant.target,
        grant.selector,
        0,
    )
    assert installed.to == profile.roles_module and installed.value == "0"
    combined = combine([grant, grant])
    assert len(combined) == 1 and combined[0].condition is None
    assert (combined[0].target, combined[0].selector) == (grant.target, grant.selector)
    assert grant.conditions == ()
    scoped = grant.model_copy(update={"condition": matches(equal("uint256", 1))})
    with pytest.raises(ValueError, match="cannot combine"):
        combine([grant, scoped])


@pytest.mark.parametrize("change", [0, 1, 1 << 224, 1 << 225, 1 << 240])
def test_storage_verification_requires_exact_zero_argument_header(change):
    profile, grant = fixture()
    role_slot = mapping_slot("bytes32", bytes.fromhex(profile.role_key[2:]), 4)
    target_slot = mapping_slot("address", grant.target, role_slot + 1)

    def reader(header):
        return SimpleNamespace(storage=lambda _address, slot: 2 if slot == target_slot else header)

    verify_grant_storage(reader(1 << 216), profile, grant)
    header = 0 if change == 0 else (1 << 216) | change
    with pytest.raises(DataError, match="altered execution authority"):
        verify_grant_storage(reader(header), profile, grant)
