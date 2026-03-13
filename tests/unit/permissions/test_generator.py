"""Tests for the permission manifest generator."""

import pytest

from almanak.framework.execution.signer.safe.constants import (
    MULTISEND_ADDRESSES,
    MULTISEND_SELECTOR,
    SafeOperation,
)
from almanak.framework.intents.compiler import ERC20_APPROVE_SELECTOR
from almanak.framework.permissions.generator import generate_manifest
from almanak.framework.permissions.models import (
    ContractPermission,
    FunctionPermission,
    PermissionManifest,
)


class TestGenerateManifest:
    """Test the main generate_manifest orchestrator."""

    def test_basic_swap_manifest(self):
        """Generate manifest for a simple swap strategy."""
        manifest = generate_manifest(
            strategy_name="test_swap",
            chain="arbitrum",
            supported_protocols=["uniswap_v3"],
            intent_types=["SWAP"],
        )
        assert isinstance(manifest, PermissionManifest)
        assert manifest.version == "1.0"
        assert manifest.chain == "arbitrum"
        assert manifest.strategy == "test_swap"
        assert len(manifest.permissions) > 0

    def test_manifest_includes_multisend(self):
        """MultiSend should always be included."""
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["uniswap_v3"],
            intent_types=["SWAP"],
        )
        multisend_addr = MULTISEND_ADDRESSES["arbitrum"].lower()
        multisend_perms = [p for p in manifest.permissions if p.target == multisend_addr]
        assert len(multisend_perms) == 1
        assert multisend_perms[0].operation == SafeOperation.DELEGATE_CALL
        selectors = {s.selector for s in multisend_perms[0].function_selectors}
        assert MULTISEND_SELECTOR in selectors

    def test_manifest_excludes_enso_when_not_used(self):
        """Enso delegates should not appear when enso is not in protocols."""
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["uniswap_v3"],
            intent_types=["SWAP"],
        )
        enso_perms = [p for p in manifest.permissions if "enso" in p.label.lower()]
        assert len(enso_perms) == 0

    def test_manifest_includes_enso_delegates(self):
        """Enso delegates should appear when enso is in protocols."""
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["enso"],
            intent_types=["SWAP"],
        )
        enso_perms = [p for p in manifest.permissions if "enso" in p.label.lower()]
        assert len(enso_perms) >= 1
        for perm in enso_perms:
            assert perm.operation == SafeOperation.DELEGATE_CALL

    def test_deterministic_output(self):
        """Two runs should produce the same permissions (order, content)."""
        kwargs = dict(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["uniswap_v3"],
            intent_types=["SWAP"],
        )
        m1 = generate_manifest(**kwargs)
        m2 = generate_manifest(**kwargs)

        # Compare permissions (ignoring generated_at timestamp)
        assert len(m1.permissions) == len(m2.permissions)
        for p1, p2 in zip(m1.permissions, m2.permissions):
            assert p1.target == p2.target
            assert p1.operation == p2.operation
            assert p1.send_allowed == p2.send_allowed
            assert [s.selector for s in p1.function_selectors] == [s.selector for s in p2.function_selectors]

    def test_sorted_by_target(self):
        """Permissions should be sorted by target address."""
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["uniswap_v3", "aave_v3"],
            intent_types=["SWAP", "SUPPLY"],
        )
        targets = [p.target for p in manifest.permissions]
        assert targets == sorted(targets)


class TestTokenExtraction:
    """Test token permission extraction from config."""

    def test_extracts_tokens_from_config(self):
        """Config with token fields should produce approve permissions."""
        config = {
            "base_token": "WETH",
            "quote_token": "USDC",
        }
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["uniswap_v3"],
            intent_types=["SWAP"],
            config=config,
        )
        # Find approve-only permissions (ERC-20 tokens from config)
        approve_perms = [
            p for p in manifest.permissions
            if p.label.startswith("ERC-20:")
        ]
        assert len(approve_perms) >= 1

    def test_extracts_anvil_funding_tokens(self):
        """Tokens in anvil_funding should produce approve permissions."""
        config = {
            "anvil_funding": {
                "USDC": 10000,
                "WETH": 5,
            },
        }
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["uniswap_v3"],
            intent_types=["SWAP"],
            config=config,
        )
        # Check that the approve selector appears on the known token addresses.
        # Note: labels may vary because discovery and config permissions get
        # merged by target address (first label wins).
        approve_targets = [
            p.target for p in manifest.permissions
            if any(s.selector == ERC20_APPROVE_SELECTOR for s in p.function_selectors)
        ]
        # WETH and USDC addresses on arbitrum should both have approve
        assert len(approve_targets) >= 2

    def test_no_config_no_token_permissions(self):
        """No config should produce no extra token permissions."""
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["uniswap_v3"],
            intent_types=["SWAP"],
            config=None,
        )
        # Token approve perms from config should be absent
        # (the compiler may still produce approve txs for the synthetic intent)
        config_approve_perms = [
            p for p in manifest.permissions
            if p.label.startswith("ERC-20:")
        ]
        assert len(config_approve_perms) == 0


class TestManifestSerialization:
    """Test to_dict serialization."""

    def test_to_dict_structure(self):
        """Manifest should serialize to expected JSON structure."""
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["uniswap_v3"],
            intent_types=["SWAP"],
        )
        d = manifest.to_dict()
        assert d["version"] == "1.0"
        assert d["chain"] == "arbitrum"
        assert d["strategy"] == "test"
        assert "generated_at" in d
        assert isinstance(d["warnings"], list)
        assert isinstance(d["permissions"], list)
        if d["permissions"]:
            perm = d["permissions"][0]
            assert "target" in perm
            assert "label" in perm
            assert "operation" in perm
            assert "send_allowed" in perm
            assert "function_selectors" in perm


class TestWarnings:
    """Test warning generation."""

    def test_empty_protocols_no_crash(self):
        """Empty protocols list should produce manifest with just infrastructure."""
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=[],
            intent_types=["SWAP"],
        )
        # Should still have MultiSend
        assert len(manifest.permissions) >= 1


class TestZodiacTargetConversion:
    """Test to_zodiac_targets() conversion."""

    def _make_manifest(
        self, permissions: list[ContractPermission], chain: str = "arbitrum"
    ) -> PermissionManifest:
        return PermissionManifest(
            version="1.0",
            chain=chain,
            strategy="test",
            generated_at="2025-01-01T00:00:00+00:00",
            permissions=permissions,
        )

    def test_basic_call_with_selectors(self):
        """CALL + selectors -> clearance=2, executionOptions=0, functions present."""
        manifest = self._make_manifest([
            ContractPermission(
                target="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                label="ERC-20: USDC",
                operation=0,
                send_allowed=False,
                function_selectors=[
                    FunctionPermission(selector="0x095ea7b3", label="approve(address,uint256)"),
                ],
            ),
        ])
        targets = manifest.to_zodiac_targets()
        assert len(targets) == 1
        t = targets[0]
        assert t["address"] == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        assert t["clearance"] == 2
        assert t["executionOptions"] == 0
        assert len(t["functions"]) == 1
        assert t["functions"][0]["selector"] == "0x095ea7b3"
        assert t["functions"][0]["wildcarded"] is True

    def test_delegatecall_no_selectors_target_clearance(self):
        """DELEGATECALL + no selectors -> clearance=1 (Target), executionOptions=2."""
        manifest = self._make_manifest([
            ContractPermission(
                target="0x7663fd40081dccd47805c00e613b6beac3b87f08",
                label="Enso Delegate",
                operation=1,
                send_allowed=False,
                function_selectors=[],
            ),
        ])
        targets = manifest.to_zodiac_targets()
        assert len(targets) == 1
        t = targets[0]
        assert t["clearance"] == 1  # Target-level
        assert t["executionOptions"] == 2  # DelegateCall
        assert "functions" not in t  # No functions key for Target clearance

    def test_send_allowed_execution_options(self):
        """send_allowed=True with CALL -> executionOptions=1 (Send)."""
        manifest = self._make_manifest([
            ContractPermission(
                target="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                label="test",
                operation=0,
                send_allowed=True,
                function_selectors=[
                    FunctionPermission(selector="0xaabbccdd", label="test()"),
                ],
            ),
        ])
        targets = manifest.to_zodiac_targets()
        assert targets[0]["executionOptions"] == 1  # Send

    def test_delegatecall_with_send_execution_options(self):
        """DELEGATECALL + send_allowed -> executionOptions=3 (Both)."""
        manifest = self._make_manifest([
            ContractPermission(
                target="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                label="test",
                operation=1,
                send_allowed=True,
                function_selectors=[
                    FunctionPermission(selector="0xaabbccdd", label="test()"),
                ],
            ),
        ])
        targets = manifest.to_zodiac_targets()
        assert targets[0]["executionOptions"] == 3  # Both

    def test_addresses_are_checksummed(self):
        """All addresses in zodiac targets should be EIP-55 checksummed."""
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["uniswap_v3"],
            intent_types=["SWAP"],
        )
        targets = manifest.to_zodiac_targets()
        assert len(targets) > 0
        from web3 import Web3
        for t in targets:
            assert t["address"] == Web3.to_checksum_address(t["address"])

    def test_multiple_selectors(self):
        """Multiple selectors produce multiple function entries."""
        manifest = self._make_manifest([
            ContractPermission(
                target="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                label="Router",
                operation=0,
                send_allowed=False,
                function_selectors=[
                    FunctionPermission(selector="0x095ea7b3", label="approve(address,uint256)"),
                    FunctionPermission(selector="0x8d80ff0a", label="multiSend(bytes)"),
                ],
            ),
        ])
        targets = manifest.to_zodiac_targets()
        assert len(targets[0]["functions"]) == 2

    def test_empty_manifest(self):
        """Empty permissions produce empty targets."""
        manifest = self._make_manifest([])
        assert manifest.to_zodiac_targets() == []

    def test_full_manifest_roundtrip(self):
        """Generate a manifest and convert to zodiac targets end-to-end."""
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["uniswap_v3"],
            intent_types=["SWAP"],
        )
        targets = manifest.to_zodiac_targets()
        assert len(targets) > 0
        # Every target must have required fields
        for t in targets:
            assert "address" in t
            assert "clearance" in t
            assert t["clearance"] in (1, 2)
            assert "executionOptions" in t
            assert t["executionOptions"] in (0, 1, 2, 3)
            if t["clearance"] == 2:
                assert "functions" in t
                for fn in t["functions"]:
                    assert "selector" in fn
                    assert fn["wildcarded"] is True

    def test_enso_delegates_get_target_clearance(self):
        """Enso delegates (no selectors) should get clearance=1."""
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["enso"],
            intent_types=["SWAP"],
        )
        targets = manifest.to_zodiac_targets()
        enso_targets = [
            t for t in targets
            if t["address"].lower() in {
                "0x7663fd40081dccd47805c00e613b6beac3b87f08",
                "0xa2f4f9c6ec598ca8c633024f8851c79ca5f43e48",
            }
        ]
        assert len(enso_targets) > 0, "Expected at least one Enso delegate target"
        for t in enso_targets:
            assert t["clearance"] == 1  # Target-level
            assert t["executionOptions"] == 2  # DelegateCall
            assert "functions" not in t

    def test_non_evm_chain_returns_empty(self):
        """Non-EVM chains (Solana) should return empty zodiac targets."""
        manifest = self._make_manifest(
            permissions=[
                ContractPermission(
                    target="epjfwdd5aufqssqem2qn1xzybapc8g4weggkzwytdt1v",
                    label="USDC (Solana)",
                    operation=0,
                    send_allowed=False,
                    function_selectors=[
                        FunctionPermission(selector="0x095ea7b3", label="approve(address,uint256)"),
                    ],
                ),
            ],
            chain="solana",
        )
        assert manifest.to_zodiac_targets() == []
