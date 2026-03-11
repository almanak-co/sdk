"""Tests for Morpho Blue receipt parser event topic hashes.

Verifies that all EVENT_TOPICS keccak256 hashes match their canonical
Solidity event signatures. Regression test for VIB-357.
"""

import pytest
from web3 import Web3

from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

# Canonical Morpho Blue event signatures from IMorpho.sol
# https://github.com/morpho-org/morpho-blue/blob/main/src/interfaces/IMorpho.sol
MORPHO_EVENT_SIGNATURES: dict[str, str] = {
    "Supply": "Supply(bytes32,address,address,uint256,uint256)",
    "Withdraw": "Withdraw(bytes32,address,address,address,uint256,uint256)",
    "Borrow": "Borrow(bytes32,address,address,address,uint256,uint256)",
    "Repay": "Repay(bytes32,address,address,uint256,uint256)",
    "SupplyCollateral": "SupplyCollateral(bytes32,address,address,uint256)",
    "WithdrawCollateral": "WithdrawCollateral(bytes32,address,address,address,uint256)",
    "Liquidate": "Liquidate(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)",
    "FlashLoan": "FlashLoan(address,address,uint256)",
    "CreateMarket": "CreateMarket(bytes32,(address,address,address,address,uint256))",
    "SetAuthorization": "SetAuthorization(address,address,bool)",
    "AccrueInterest": "AccrueInterest(bytes32,uint256,uint256,uint256)",
}

# ERC-20 standard events (well-known, shared across all tokens)
ERC20_EVENT_SIGNATURES: dict[str, str] = {
    "Transfer": "Transfer(address,address,uint256)",
    "Approval": "Approval(address,address,uint256)",
}


def _keccak256(sig: str) -> str:
    return "0x" + Web3.keccak(text=sig).hex()


class TestMorphoBlueEventTopics:
    """Verify all EVENT_TOPICS hashes match their Solidity event signatures."""

    @pytest.mark.parametrize("event_name,signature", list(MORPHO_EVENT_SIGNATURES.items()))
    def test_morpho_event_hash(self, event_name: str, signature: str):
        expected = _keccak256(signature)
        actual = EVENT_TOPICS[event_name]
        assert actual == expected, (
            f"{event_name} topic hash mismatch:\n"
            f"  expected: {expected} (from '{signature}')\n"
            f"  actual:   {actual}"
        )

    @pytest.mark.parametrize("event_name,signature", list(ERC20_EVENT_SIGNATURES.items()))
    def test_erc20_event_hash(self, event_name: str, signature: str):
        expected = _keccak256(signature)
        actual = EVENT_TOPICS[event_name]
        assert actual == expected, (
            f"{event_name} topic hash mismatch:\n"
            f"  expected: {expected} (from '{signature}')\n"
            f"  actual:   {actual}"
        )

    def test_all_events_covered(self):
        """Ensure every event in EVENT_TOPICS has a signature test."""
        all_sigs = {**MORPHO_EVENT_SIGNATURES, **ERC20_EVENT_SIGNATURES}
        for event_name in EVENT_TOPICS:
            assert event_name in all_sigs, f"Event '{event_name}' has no signature test"
