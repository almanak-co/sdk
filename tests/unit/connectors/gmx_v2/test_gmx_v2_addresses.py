"""Tests for GMX V2 contract address completeness.

Verifies that all chains have valid contract addresses configured,
especially the event_emitter which is required for receipt parsing.
"""

import re

import pytest

from almanak.framework.connectors.gmx_v2.adapter import GMX_V2_ADDRESSES


ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

REQUIRED_KEYS = [
    "exchange_router",
    "order_handler",
    "data_store",
    "reader",
    "event_emitter",
    "order_vault",
    "router",
]


class TestGMXV2Addresses:
    """Tests that all chain address configs are complete."""

    @pytest.mark.parametrize("chain", list(GMX_V2_ADDRESSES.keys()))
    def test_no_zero_addresses(self, chain: str) -> None:
        """No chain should have zero-address placeholders for required keys."""
        addresses = GMX_V2_ADDRESSES[chain]
        for key in REQUIRED_KEYS:
            assert key in addresses, f"{chain} missing {key}"
            assert addresses[key] != ZERO_ADDRESS, (
                f"{chain}.{key} is zero address placeholder"
            )

    @pytest.mark.parametrize("chain", list(GMX_V2_ADDRESSES.keys()))
    def test_addresses_are_valid_hex(self, chain: str) -> None:
        """All addresses should be valid 42-char hex strings."""
        for key, addr in GMX_V2_ADDRESSES[chain].items():
            assert re.match(r"^0x[a-fA-F0-9]{40}$", addr), f"{chain}.{key} is not a valid hex address: {addr}"

    def test_avalanche_event_emitter_set(self) -> None:
        """Avalanche event_emitter should be the real deployed address."""
        assert GMX_V2_ADDRESSES["avalanche"]["event_emitter"] == "0xDb17B211c34240B014ab6d61d4A31FA0C0e20c26"
