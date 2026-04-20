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

# Official Uniswap V4 addresses per chain (from https://docs.uniswap.org/contracts/v4/deployments).
# Addresses are DIFFERENT per chain — do NOT assume CREATE2 uniformity.
CANONICAL_V4_ADDRESSES_PER_CHAIN = {
    "ethereum": {
        "pool_manager": "0x000000000004444c5dc75cB358380D2e3dE08A90",
        "position_manager": "0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e",
        "universal_router": "0x66a9893cc07d91d95644aedd05d03f95e1dba8af",
        "quoter": "0x52f0e24d1c21c8a0cb1e5a5dd6198556bd9e1203",
        "state_view": "0x7ffe42c4a5deea5b0fec41c94c136cf115597227",
    },
    "base": {
        "pool_manager": "0x498581ff718922c3f8e6a244956af099b2652b2b",
        "position_manager": "0x7c5f5a4bbd8fd63184577525326123b519429bdc",
        "universal_router": "0x6ff5693b99212da76ad316178a184ab56d299b43",
        "quoter": "0x0d5e0f971ed27fbff6c2837bf31316121532048d",
        "state_view": "0xa3c0c9b65bad0b08107aa264b0f3db444b867a71",
    },
    "arbitrum": {
        "pool_manager": "0x360e68faccca8ca495c1b759fd9eee466db9fb32",
        "position_manager": "0xd88f38f930b7952f2db2432cb002e7abbf3dd869",
        "universal_router": "0xa51afafe0263b40edaef0df8781ea9aa03e381a3",
        "quoter": "0x3972c00f7ed4885e145823eb7c655375d275a1c5",
        "state_view": "0x76fd297e2d437cd7f76d50f01afe6160f86e9990",
    },
    "optimism": {
        "pool_manager": "0x9a13f98cb987694c9f086b1f5eb990eea8264ec3",
        "position_manager": "0x3c3ea4b57a46241e54610e5f022e5c45859a1017",
        "universal_router": "0x851116d9223fabed8e56c0e6b8ad0c31d98b3507",
        "quoter": "0x1f3131a13296fb91c90870043742c3cdbff1a8d7",
        "state_view": "0xc18a3169788f4f75a170290584eca6395c75ecdb",
    },
    "polygon": {
        "pool_manager": "0x67366782805870060151383f4bbff9dab53e5cd6",
        "position_manager": "0x1ec2ebf4f37e7363fdfe3551602425af0b3ceef9",
        "universal_router": "0x1095692a6237d83c6a72f3f5efedb9a670c49223",
        "quoter": "0xb3d5c3dfc3a7aebff71895a7191796bffc2c81b9",
        "state_view": "0x5ea1bd7974c8a611cbab0bdcafcb1d9cc9b3ba5a",
    },
    "avalanche": {
        "pool_manager": "0x06380c0e0912312b5150364b9dc4542ba0dbbc85",
        "position_manager": "0xb74b1f14d2754acfcbbe1a221023a5cf50ab8acd",
        "universal_router": "0x94b75331ae8d42c1b61065089b7d48fe14aa73b7",
        "quoter": "0xbe40675bb704506a3c2ccfb762dcfd1e979845c2",
        "state_view": "0xc3c9e198c735a4b97e3e683f391ccbdd60b69286",
    },
    "bsc": {
        "pool_manager": "0x28e2ea090877bf75740558f6bfb36a5ffee9e9df",
        "position_manager": "0x7a4a5c919ae2541aed11041a1aeee68f1287f95b",
        "universal_router": "0x1906c1d672b88cd1b9ac7593301ca990f94eae07",
        "quoter": "0x9f75dd27d6664c475b90e105573e550ff69437b0",
        "state_view": "0xd13dd3d6e93f276fafc9db9e6bb47c1180aee0c4",
    },
}

# Chains that should have V4 contracts
EXPECTED_V4_CHAINS = {"ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche", "bsc"}


class TestCanonicalAddresses:
    """Verify V4 contract addresses match canonical CREATE2 deployments."""

    def test_all_expected_chains_present(self):
        """All expected chains should have V4 contract entries."""
        for chain in EXPECTED_V4_CHAINS:
            assert chain in UNISWAP_V4, f"Chain '{chain}' missing from UNISWAP_V4"

    @pytest.mark.parametrize("contract_key", ["pool_manager", "position_manager", "universal_router", "quoter", "state_view"])
    def test_canonical_addresses_match(self, contract_key: str):
        """Each chain's address should match the official per-chain deployment."""
        for chain, addrs in UNISWAP_V4.items():
            expected = CANONICAL_V4_ADDRESSES_PER_CHAIN[chain][contract_key].lower()
            actual = addrs.get(contract_key, "").lower()
            assert actual == expected, (
                f"Chain '{chain}' has wrong {contract_key}: {actual} != {expected}"
            )

    def test_pool_manager_is_canonical(self):
        """PoolManager should match the official per-chain address."""
        for chain, addr in POOL_MANAGER_ADDRESSES.items():
            expected = CANONICAL_V4_ADDRESSES_PER_CHAIN[chain]["pool_manager"].lower()
            assert addr.lower() == expected, (
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
        info = registry.lookup("ethereum", CANONICAL_V4_ADDRESSES_PER_CHAIN["ethereum"]["pool_manager"])
        assert info is not None, "V4 PoolManager not in registry"
        assert info.protocol == "uniswap_v4"
        assert "SWAP" in info.supported_actions

    def test_position_manager_registered(self):
        """V4 PositionManager should be in the default registry."""
        registry = get_default_registry()
        info = registry.lookup("ethereum", CANONICAL_V4_ADDRESSES_PER_CHAIN["ethereum"]["position_manager"])
        assert info is not None, "V4 PositionManager not in registry"
        assert info.protocol == "uniswap_v4"
        assert "LP_OPEN" in info.supported_actions
        assert "LP_CLOSE" in info.supported_actions

    def test_v4_registered_on_all_chains(self):
        """V4 PoolManager should be registered on all supported chains."""
        registry = get_default_registry()
        for chain in EXPECTED_V4_CHAINS:
            pm_addr = CANONICAL_V4_ADDRESSES_PER_CHAIN[chain]["pool_manager"]
            info = registry.lookup(chain, pm_addr)
            assert info is not None, f"V4 PoolManager not registered on {chain}"

    def test_v4_swap_action_supported(self):
        """SWAP action should be supported for V4 PoolManager."""
        registry = get_default_registry()
        pm_addr = CANONICAL_V4_ADDRESSES_PER_CHAIN["arbitrum"]["pool_manager"]
        assert registry.is_action_supported("arbitrum", pm_addr, "SWAP")

    def test_v4_lp_action_supported(self):
        """LP actions should be supported for V4 PositionManager."""
        registry = get_default_registry()
        pos_addr = CANONICAL_V4_ADDRESSES_PER_CHAIN["arbitrum"]["position_manager"]
        assert registry.is_action_supported("arbitrum", pos_addr, "LP_OPEN")
        assert registry.is_action_supported("arbitrum", pos_addr, "LP_CLOSE")

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
        assert result.action_bundle.metadata["router"].lower() == CANONICAL_V4_ADDRESSES_PER_CHAIN["arbitrum"]["universal_router"].lower()
