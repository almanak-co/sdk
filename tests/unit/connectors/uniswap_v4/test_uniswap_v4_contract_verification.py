"""Tests for Uniswap V4 contract address verification and registry integration.

Verifies that V4 contract addresses in contracts.py match canonical CREATE2
deployments, that the ContractRegistry has V4 entries, and that the Protocol
enum includes UNISWAP_V4.
"""

from __future__ import annotations

import pytest

from almanak.core.contracts import UNISWAP_V4
from almanak.core.enums import Protocol
from almanak.framework.connectors.contract_registry import get_default_registry
from almanak.framework.connectors.uniswap_v4.sdk import (
    MODIFY_LIQUIDITIES_SELECTOR,
    MODIFY_LIQUIDITIES_WITHOUT_UNLOCK_SELECTOR,
    POOL_MANAGER_ADDRESSES,
    SWAP_SELECTOR,
    UNIVERSAL_ROUTER_EXECUTE_SELECTOR,
    V4_SWAP_EXACT_IN,
    V4_SWAP_EXACT_IN_SINGLE,
    V4_SWAP_EXACT_OUT,
    V4_SWAP_EXACT_OUT_SINGLE,
)


# =============================================================================
# Canonical address verification
# =============================================================================

# These are the canonical Uniswap V4 CREATE2 deployment addresses.
# All chains use the same addresses due to CREATE2 deterministic deployment.
CANONICAL_V4_ADDRESSES = {
    "pool_manager": "0x000000000004444c5dc75cB358380D2e3dE08A90",
    "position_manager": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",
    "universal_router": "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af",
    "quoter": "0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203",
    "state_view": "0x7ffA62d1F57a97A4A4A35c6dDF1f9e36bCBBbE8a",
}

# Chains that should have V4 contracts
EXPECTED_V4_CHAINS = {"ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche", "bsc"}


class TestCanonicalAddresses:
    """Verify V4 contract addresses match canonical CREATE2 deployments."""

    def test_all_expected_chains_present(self):
        """All expected chains should have V4 contract entries."""
        for chain in EXPECTED_V4_CHAINS:
            assert chain in UNISWAP_V4, f"Chain '{chain}' missing from UNISWAP_V4"

    @pytest.mark.parametrize("contract_key", list(CANONICAL_V4_ADDRESSES.keys()))
    def test_canonical_addresses_match(self, contract_key: str):
        """Each canonical address should be the same on all chains (CREATE2)."""
        expected = CANONICAL_V4_ADDRESSES[contract_key].lower()
        for chain, addrs in UNISWAP_V4.items():
            actual = addrs.get(contract_key, "").lower()
            assert actual == expected, (
                f"Chain '{chain}' has wrong {contract_key}: {actual} != {expected}"
            )

    def test_pool_manager_is_canonical(self):
        """PoolManager should be the well-known V4 address on all chains."""
        for chain, addr in POOL_MANAGER_ADDRESSES.items():
            assert addr.lower() == "0x000000000004444c5dc75cb358380d2e3de08a90", (
                f"PoolManager on {chain} is not canonical: {addr}"
            )

    def test_all_chains_have_all_contracts(self):
        """Every V4 chain entry should have all expected contract keys."""
        required_keys = {"pool_manager", "position_manager", "universal_router", "quoter", "state_view"}
        for chain, addrs in UNISWAP_V4.items():
            missing = required_keys - set(addrs.keys())
            assert not missing, f"Chain '{chain}' missing V4 contract keys: {missing}"


# =============================================================================
# Protocol enum
# =============================================================================


class TestProtocolEnum:
    def test_uniswap_v4_in_protocol_enum(self):
        """UNISWAP_V4 should be a valid Protocol enum value."""
        assert Protocol.UNISWAP_V4 == Protocol("UNISWAP_V4")
        assert Protocol.UNISWAP_V4.value == "UNISWAP_V4"


# =============================================================================
# ContractRegistry integration
# =============================================================================


class TestContractRegistryV4:
    def test_pool_manager_registered(self):
        """V4 PoolManager should be in the default registry."""
        registry = get_default_registry()
        info = registry.lookup("ethereum", CANONICAL_V4_ADDRESSES["pool_manager"])
        assert info is not None, "V4 PoolManager not in registry"
        assert info.protocol == "uniswap_v4"
        assert "SWAP" in info.supported_actions

    def test_position_manager_registered(self):
        """V4 PositionManager should be in the default registry."""
        registry = get_default_registry()
        info = registry.lookup("ethereum", CANONICAL_V4_ADDRESSES["position_manager"])
        assert info is not None, "V4 PositionManager not in registry"
        assert info.protocol == "uniswap_v4"
        assert "LP_OPEN" in info.supported_actions
        assert "LP_CLOSE" in info.supported_actions

    def test_v4_registered_on_all_chains(self):
        """V4 PoolManager should be registered on all supported chains."""
        registry = get_default_registry()
        for chain in EXPECTED_V4_CHAINS:
            info = registry.lookup(chain, CANONICAL_V4_ADDRESSES["pool_manager"])
            assert info is not None, f"V4 PoolManager not registered on {chain}"

    def test_v4_swap_action_supported(self):
        """SWAP action should be supported for V4 PoolManager."""
        registry = get_default_registry()
        assert registry.is_action_supported(
            "arbitrum", CANONICAL_V4_ADDRESSES["pool_manager"], "SWAP"
        )

    def test_v4_lp_action_supported(self):
        """LP actions should be supported for V4 PositionManager."""
        registry = get_default_registry()
        assert registry.is_action_supported(
            "arbitrum", CANONICAL_V4_ADDRESSES["position_manager"], "LP_OPEN"
        )
        assert registry.is_action_supported(
            "arbitrum", CANONICAL_V4_ADDRESSES["position_manager"], "LP_CLOSE"
        )

    def test_uniswap_v4_in_supported_protocols(self):
        """uniswap_v4 should appear in the set of supported protocols."""
        registry = get_default_registry()
        assert "uniswap_v4" in registry.get_supported_protocols()


# =============================================================================
# Function selector constants
# =============================================================================


class TestV4Selectors:
    def test_swap_selector_format(self):
        """SWAP_SELECTOR should be a valid 4-byte selector."""
        assert SWAP_SELECTOR.startswith("0x")
        assert len(SWAP_SELECTOR) == 10  # "0x" + 8 hex chars

    def test_modify_liquidities_selector_keccak(self):
        """modifyLiquidities selector must match keccak256 of canonical signature."""
        from eth_utils import keccak

        expected = "0x" + keccak(text="modifyLiquidities(bytes,uint256)")[:4].hex()
        assert MODIFY_LIQUIDITIES_SELECTOR == expected, (
            f"MODIFY_LIQUIDITIES_SELECTOR {MODIFY_LIQUIDITIES_SELECTOR} != keccak {expected}"
        )

    def test_modify_liquidities_without_unlock_selector_keccak(self):
        """modifyLiquiditiesWithoutUnlock selector must match keccak256."""
        from eth_utils import keccak

        expected = "0x" + keccak(text="modifyLiquiditiesWithoutUnlock(bytes,bytes[])")[:4].hex()
        assert MODIFY_LIQUIDITIES_WITHOUT_UNLOCK_SELECTOR == expected, (
            f"MODIFY_LIQUIDITIES_WITHOUT_UNLOCK_SELECTOR {MODIFY_LIQUIDITIES_WITHOUT_UNLOCK_SELECTOR} != keccak {expected}"
        )

    def test_universal_router_execute_selector_keccak(self):
        """UniversalRouter execute selector must match keccak256."""
        from eth_utils import keccak

        expected = "0x" + keccak(text="execute(bytes,bytes[],uint256)")[:4].hex()
        assert UNIVERSAL_ROUTER_EXECUTE_SELECTOR == expected, (
            f"UNIVERSAL_ROUTER_EXECUTE_SELECTOR {UNIVERSAL_ROUTER_EXECUTE_SELECTOR} != keccak {expected}"
        )

    def test_v4_command_bytes(self):
        """V4 UniversalRouter command byte should be V4_SWAP (0x10) via two-layer encoding."""
        # All swap variants alias to V4_SWAP (0x10) per UniversalRouter Dispatcher.sol
        # The specific swap type is encoded in the inner V4 actions layer.
        assert V4_SWAP_EXACT_IN_SINGLE == 0x10
        assert V4_SWAP_EXACT_IN == 0x10
        assert V4_SWAP_EXACT_OUT_SINGLE == 0x10
        assert V4_SWAP_EXACT_OUT == 0x10

    def test_selectors_are_distinct(self):
        """All selectors should be unique."""
        selectors = {
            SWAP_SELECTOR,
            MODIFY_LIQUIDITIES_SELECTOR,
            MODIFY_LIQUIDITIES_WITHOUT_UNLOCK_SELECTOR,
            UNIVERSAL_ROUTER_EXECUTE_SELECTOR,
        }
        assert len(selectors) == 4, "Duplicate selector found"


# =============================================================================
# Compiler V4 routing (quarantine removed — V4 now routes via UniversalRouter)
# =============================================================================


class TestCompilerV4Unblocked:
    def test_v4_swap_compiles_via_universal_router(self):
        """V4 swaps compile successfully via the canonical UniversalRouter."""
        from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig

        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        from almanak.framework.intents.vocabulary import SwapIntent

        intent = SwapIntent(
            from_token="WETH",
            to_token="USDC",
            amount=1,
            protocol="uniswap_v4",
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol_version"] == "v4"
        assert result.action_bundle.metadata["router"] == "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af"
