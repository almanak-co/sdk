"""Tests for the canonical ERC-20 ABI surface and allowance cache."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
from eth_abi import encode as abi_encode
from eth_utils import function_signature_to_4byte_selector, keccak

from almanak.connectors._base import erc20_abi as boundary_abi
from almanak.connectors._strategy_base.erc20_abi import (
    ERC20_ALLOWANCE_SELECTOR,
    ERC20_APPROVE_SELECTOR,
    ERC20_BALANCE_OF_SELECTOR,
    ERC20_TRANSFER_TOPIC,
    MAX_UINT256,
    AllowanceCache,
    encode_allowance,
    encode_approve,
    encode_balance_of,
    pad_address,
    pad_uint256,
)

OWNER = "0x1111111111111111111111111111111111111111"
TOKEN = "0x2222222222222222222222222222222222222222"
SPENDER = "0x3333333333333333333333333333333333333333"


def _selector(signature: str) -> str:
    return "0x" + function_signature_to_4byte_selector(signature).hex()


def test_constants_are_derived_from_canonical_signatures() -> None:
    assert ERC20_APPROVE_SELECTOR == _selector("approve(address,uint256)")
    assert ERC20_ALLOWANCE_SELECTOR == _selector("allowance(address,address)")
    assert ERC20_BALANCE_OF_SELECTOR == _selector("balanceOf(address)")
    assert ERC20_TRANSFER_TOPIC == "0x" + keccak(text="Transfer(address,address,uint256)").hex()


def test_strategy_surface_reexports_dependency_free_boundary_primitives() -> None:
    assert boundary_abi.encode_approve is encode_approve
    assert boundary_abi.encode_allowance is encode_allowance
    assert boundary_abi.encode_balance_of is encode_balance_of


@pytest.mark.parametrize("amount", [0, 1, 10**18, MAX_UINT256])
def test_encode_approve_is_byte_equal_to_standard_abi_encoder(amount: int) -> None:
    expected = function_signature_to_4byte_selector("approve(address,uint256)") + abi_encode(
        ["address", "uint256"], [SPENDER, amount]
    )
    assert bytes.fromhex(encode_approve(SPENDER, amount)[2:]) == expected


def test_read_encoders_are_byte_equal_to_standard_abi_encoder() -> None:
    expected_allowance = function_signature_to_4byte_selector("allowance(address,address)") + abi_encode(
        ["address", "address"], [OWNER, SPENDER]
    )
    expected_balance = function_signature_to_4byte_selector("balanceOf(address)") + abi_encode(["address"], [OWNER])
    assert bytes.fromhex(encode_allowance(OWNER, SPENDER)[2:]) == expected_allowance
    assert bytes.fromhex(encode_balance_of(OWNER)[2:]) == expected_balance


def test_padding_is_fixed_width_and_case_normalized() -> None:
    assert pad_address(SPENDER.upper()) == "0" * 24 + SPENDER[2:]
    assert pad_address(SPENDER[2:]) == "0" * 24 + SPENDER[2:]
    assert pad_uint256(MAX_UINT256) == "f" * 64


@pytest.mark.parametrize("address", ["0xabc", "0x" + "g" * 40, "0x" + "1" * 41, ""])
def test_pad_address_rejects_malformed_addresses(address: str) -> None:
    with pytest.raises(ValueError, match="address must be 20 bytes"):
        pad_address(address)


@pytest.mark.parametrize("value", [-1, MAX_UINT256 + 1])
def test_pad_uint256_rejects_out_of_range_values(value: int) -> None:
    with pytest.raises(ValueError, match="uint256 value"):
        pad_uint256(value)


@pytest.mark.parametrize("value", [True, "1", None])
def test_pad_uint256_rejects_non_integer_values(value: object) -> None:
    with pytest.raises(TypeError, match="must be an int"):
        pad_uint256(value)  # type: ignore[arg-type]


def test_allowance_cache_separates_confirmed_and_planned_values() -> None:
    cache = AllowanceCache(OWNER.upper())
    cache.record_confirmed(TOKEN, SPENDER, 100)
    cache.record_planned(TOKEN.upper(), SPENDER.upper(), 250)

    assert cache.get(TOKEN, SPENDER) == 250
    assert cache.get_planned(TOKEN, SPENDER) == 250
    assert cache.is_sufficient(TOKEN, SPENDER, 200)

    cache.clear_planned()
    assert cache.get(TOKEN, SPENDER) == 100
    assert cache.get_planned(TOKEN, SPENDER) is None
    assert not cache.is_sufficient(TOKEN, SPENDER, 200)


def test_confirmed_update_replaces_planned_value_and_pair_invalidation_is_scoped() -> None:
    other_spender = "0x4444444444444444444444444444444444444444"
    cache = AllowanceCache(OWNER)
    cache.record_planned(TOKEN, SPENDER, MAX_UINT256)
    cache.record_confirmed(TOKEN, SPENDER, 7)
    cache.record_confirmed(TOKEN, other_spender, 9)

    assert cache.get(TOKEN, SPENDER) == 7
    cache.invalidate(TOKEN, SPENDER)
    assert cache.get(TOKEN, SPENDER) is None
    assert cache.get(TOKEN, other_spender) == 9


def test_no_erc20_literals_are_reintroduced_in_internal_foundations_or_permission_hints() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    paths = list((repo_root / "almanak/connectors/_strategy_base").rglob("*.py"))
    paths.extend((repo_root / "almanak/connectors").rglob("*permission_hints.py"))
    forbidden = {
        ERC20_APPROVE_SELECTOR,
        ERC20_ALLOWANCE_SELECTOR,
        ERC20_BALANCE_OF_SELECTOR,
        ERC20_TRANSFER_TOPIC,
    }
    violations: list[str] = []
    for path in paths:
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and node.value in forbidden:
                violations.append(f"{path.relative_to(repo_root)}:{node.lineno}")

    assert violations == []
