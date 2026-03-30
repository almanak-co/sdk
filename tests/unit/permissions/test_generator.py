"""Tests for the permission manifest generator."""

from decimal import Decimal

import pytest

from almanak.framework.connectors.enso.adapter import ENSO_FUNCTION_SELECTORS
from almanak.framework.connectors.enso.client import CHAIN_MAPPING, ROUTER_ADDRESSES
from almanak.framework.execution.signer.safe.constants import (
    MULTISEND_ADDRESSES,
    MULTISEND_SELECTOR,
    SafeOperation,
)
from almanak.framework.intents.compiler import ERC20_APPROVE_SELECTOR
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.permissions.generator import (
    _overrides_teardown,
    discover_teardown_protocols,
    generate_manifest,
)
from almanak.framework.permissions.models import (
    ContractPermission,
    FunctionPermission,
    PermissionManifest,
)
from almanak.framework.teardown.models import TeardownMode


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
        """Enso Router should not appear when enso is not in protocols."""
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["uniswap_v3"],
            intent_types=["SWAP"],
        )
        enso_perms = [p for p in manifest.permissions if "enso" in p.label.lower()]
        assert len(enso_perms) == 0

    def test_manifest_includes_enso_router(self):
        """Enso Router should appear with scoped CALL when enso is in protocols."""
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["enso"],
            intent_types=["SWAP"],
        )
        chain_id = CHAIN_MAPPING["arbitrum"]
        router_addr = ROUTER_ADDRESSES[chain_id].lower()
        enso_perms = [p for p in manifest.permissions if p.target == router_addr]
        assert len(enso_perms) == 1
        perm = enso_perms[0]
        assert perm.operation == SafeOperation.CALL
        assert perm.label == "Enso Router"
        # Should have exactly the 4 Enso function selectors
        selectors = {s.selector for s in perm.function_selectors}
        assert selectors == set(ENSO_FUNCTION_SELECTORS.values())

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
                target="0x38869bf66a61cf6bdb996a6ae40d5853fd43b526",
                label="MultiSend (example)",
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

    def test_enso_router_gets_function_clearance(self):
        """Enso Router should get clearance=2 (Function) with CALL+Send and 4 selectors."""
        manifest = generate_manifest(
            strategy_name="test",
            chain="arbitrum",
            supported_protocols=["enso"],
            intent_types=["SWAP"],
        )
        targets = manifest.to_zodiac_targets()
        chain_id = CHAIN_MAPPING["arbitrum"]
        router_addr = ROUTER_ADDRESSES[chain_id]
        from web3 import Web3
        checksummed = Web3.to_checksum_address(router_addr)
        enso_targets = [t for t in targets if t["address"] == checksummed]
        assert len(enso_targets) == 1, "Expected exactly one Enso Router target"
        t = enso_targets[0]
        assert t["clearance"] == 2  # Function-level
        assert t["executionOptions"] == 1  # CALL + Send (native-token swaps carry value)
        assert len(t["functions"]) == len(ENSO_FUNCTION_SELECTORS)
        target_selectors = {f["selector"] for f in t["functions"]}
        assert target_selectors == set(ENSO_FUNCTION_SELECTORS.values())

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


# ---------------------------------------------------------------------------
# Teardown protocol discovery
# ---------------------------------------------------------------------------


class _FakeBaseStrategy:
    """Simulates IntentStrategy base class with default generate_teardown_intents."""

    def generate_teardown_intents(self, mode, market=None):
        return []


class _StrategyWithEnsoTeardown(_FakeBaseStrategy):
    """Strategy that uses enso only in teardown."""

    def generate_teardown_intents(self, mode, market=None):
        return [
            SwapIntent(
                from_token="ALMANAK",
                to_token="USDC",
                amount=Decimal("100"),
                max_slippage=Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005"),
                protocol="enso",
            ),
        ]


class _StrategyWithMultiProtocolTeardown(_FakeBaseStrategy):
    """Strategy using different protocols for different teardown modes."""

    def generate_teardown_intents(self, mode, market=None):
        if mode == TeardownMode.SOFT:
            return [
                SwapIntent(
                    from_token="WETH",
                    to_token="USDC",
                    amount=Decimal("1"),
                    max_slippage=Decimal("0.005"),
                    protocol="uniswap_v3",
                ),
            ]
        return [
            SwapIntent(
                from_token="WETH",
                to_token="USDC",
                amount=Decimal("1"),
                max_slippage=Decimal("0.03"),
                protocol="enso",
            ),
        ]


class _StrategyNoTeardownOverride(_FakeBaseStrategy):
    """Strategy that does not override generate_teardown_intents."""

    pass


class _TeardownMixin:
    """Shared mixin that provides teardown logic (simulates inherited teardown)."""

    def generate_teardown_intents(self, mode, market=None):
        return [
            SwapIntent(
                from_token="WETH",
                to_token="USDC",
                amount=Decimal("1"),
                max_slippage=Decimal("0.01"),
                protocol="enso",
            ),
        ]


class _StrategyWithInheritedTeardown(_TeardownMixin, _FakeBaseStrategy):
    """Strategy that inherits teardown from a mixin (not defined directly on class)."""

    pass


class _StrategyOldSignature(_FakeBaseStrategy):
    """Strategy using old-style teardown signature (no market param)."""

    def generate_teardown_intents(self, mode):
        return [
            SwapIntent(
                from_token="ALMANAK",
                to_token="USDC",
                amount=Decimal("100"),
                max_slippage=Decimal("0.03"),
                protocol="enso",
            ),
        ]


class _StrategyBrokenTeardown(_FakeBaseStrategy):
    """Strategy whose teardown raises."""

    def generate_teardown_intents(self, mode, market=None):
        raise RuntimeError("needs live market data")


class TestDiscoverTeardownProtocols:
    """Test discover_teardown_protocols helper."""

    def test_discovers_enso_from_teardown(self):
        """Enso protocol should be discovered when only used in teardown."""
        protocols, warnings = discover_teardown_protocols(_StrategyWithEnsoTeardown, "base")
        assert "enso" in protocols
        assert not warnings

    def test_discovers_multiple_protocols(self):
        """Both SOFT and HARD mode protocols should be discovered."""
        protocols, warnings = discover_teardown_protocols(_StrategyWithMultiProtocolTeardown, "arbitrum")
        assert "uniswap_v3" in protocols
        assert "enso" in protocols

    def test_no_override_returns_empty(self):
        """Strategy inheriting a no-op teardown should return empty set with advisory warning."""
        protocols, warnings = discover_teardown_protocols(_StrategyNoTeardownOverride, "arbitrum")
        assert protocols == set()
        # Advisory warning emitted because introspection succeeded but found no protocols
        assert any("no protocols" in w for w in warnings)

    def test_broken_teardown_returns_warnings(self):
        """Teardown that raises should return warnings, not crash."""
        protocols, warnings = discover_teardown_protocols(_StrategyBrokenTeardown, "base")
        assert len(warnings) > 0
        assert "needs live market data" in warnings[0]

    def test_class_without_teardown_method(self):
        """Class with no generate_teardown_intents at all should return empty."""

        class PlainClass:
            pass

        protocols, warnings = discover_teardown_protocols(PlainClass, "arbitrum")
        assert protocols == set()
        assert not warnings

    def test_discovers_inherited_teardown(self):
        """Teardown inherited from a mixin should be discovered."""
        protocols, warnings = discover_teardown_protocols(_StrategyWithInheritedTeardown, "base")
        assert "enso" in protocols

    def test_discovers_old_signature_teardown(self):
        """Old-style teardown (no market param) should be discovered via fallback."""
        protocols, warnings = discover_teardown_protocols(_StrategyOldSignature, "base")
        assert "enso" in protocols
        assert not warnings


class TestOverridesTeardown:
    """Test _overrides_teardown helper."""

    def test_detects_override(self):
        assert _overrides_teardown(_StrategyWithEnsoTeardown) is True

    def test_inherits_from_non_framework_base(self):
        """Teardown inherited from a non-framework base is still detected."""
        # _FakeBaseStrategy is a test class (not almanak.framework.*),
        # so _StrategyNoTeardownOverride inherits a "real" teardown via MRO.
        assert _overrides_teardown(_StrategyNoTeardownOverride) is True

    def test_detects_inherited_from_mixin(self):
        """Teardown from a mixin should be detected as an override."""
        assert _overrides_teardown(_StrategyWithInheritedTeardown) is True

    def test_framework_base_not_detected(self):
        """A class whose teardown comes only from a framework base returns False."""
        # Simulate by checking a class whose module is in almanak.framework
        from almanak.framework.strategies.stateless_strategy import StatelessStrategy

        assert _overrides_teardown(StatelessStrategy) is False

    def test_plain_class(self):

        class NoMethod:
            pass

        assert _overrides_teardown(NoMethod) is False
