"""Tests for deterministic Anvil account helpers."""

import pytest

from almanak.framework.anvil.accounts import (
    ANVIL_DEFAULT_ADDRESS,
    ANVIL_DEFAULT_PRIVATE_KEY,
    anvil_default_address,
    anvil_default_private_key,
    synthetic_evm_address,
)


def test_default_account_constants_match_helpers() -> None:
    assert ANVIL_DEFAULT_PRIVATE_KEY == anvil_default_private_key(0)
    assert ANVIL_DEFAULT_ADDRESS == anvil_default_address(0)


def test_second_default_account_is_available() -> None:
    assert anvil_default_private_key(1).startswith("0x")
    assert anvil_default_address(1).startswith("0x")


def test_negative_default_account_index_is_rejected() -> None:
    with pytest.raises(ValueError, match="index -1"):
        anvil_default_private_key(-1)


def test_out_of_range_default_account_index_is_rejected() -> None:
    with pytest.raises(ValueError, match="index 2"):
        anvil_default_private_key(2)


def test_synthetic_evm_address_bounds() -> None:
    assert synthetic_evm_address(1) == "0x0000000000000000000000000000000000000001"
    with pytest.raises(ValueError, match="positive"):
        synthetic_evm_address(0)
    with pytest.raises(ValueError, match="160 bits"):
        synthetic_evm_address(1 << 160)
