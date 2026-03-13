"""Protocol compatibility tests for the permission manifest generator.

Auto-discovers all valid (protocol, intent_type, chain) triples from
the compiler's deployment registries and the synthetic intent factory's
protocol category sets. For each triple, verifies that:

1. build_synthetic_intents() returns at least one intent
2. The intent compiles without crashing
3. discover_permissions() produces real contract addresses and selectors

If someone adds a protocol to a category set but the compiler can't
handle it, or a permission_hints.py has a broken import, these tests
catch it automatically.

Known gaps are documented in _KNOWN_GAPS - when a gap is fixed, the
test will start asserting permissions are produced, catching the fix.
"""

import pytest

from almanak.framework.intents.compiler import (
    LENDING_POOL_ADDRESSES,
    LP_POSITION_MANAGERS,
    PROTOCOL_ROUTERS,
)
from almanak.framework.permissions.constants import METAMORPHO_VAULTS
from almanak.framework.permissions.discovery import discover_permissions
from almanak.framework.permissions.synthetic_intents import (
    _FLASH_LOAN_PROVIDERS,
    _LENDING_PROTOCOLS,
    _LP_PROTOCOLS,
    _PERP_PROTOCOLS,
    _SWAP_PROTOCOLS,
    build_synthetic_intents,
)

# ---------------------------------------------------------------------------
# Known gaps: (protocol, intent_type, chain) triples where compilation
# succeeds at the synthetic-intent level but the compiler can't produce
# transactions without external state (API keys, RPC, missing fee tiers).
#
# These are tested for "no crash" but not for "produces permissions".
# When a gap is fixed upstream, remove it here - the test will then
# assert that permissions are actually produced.
# ---------------------------------------------------------------------------
_KNOWN_GAPS: set[tuple[str, str, str]] = set()


def _is_known_gap(protocol: str, intent_type: str, chain: str) -> bool:
    return (protocol, intent_type, chain) in _KNOWN_GAPS


# ---------------------------------------------------------------------------
# Parameter collection from deployment registries
# ---------------------------------------------------------------------------

_CHAIN_ALIASES = {"bnb", "bsc"}  # bnb/bsc are identical; keep only canonical entries


def _collect_swap_params() -> list[tuple[str, str]]:
    """Collect (protocol, chain) pairs for SWAP intents.

    Router-checked protocols use PROTOCOL_ROUTERS.
    Router-exempt protocols (enso, curve, pendle) use arbitrum as default.
    """
    router_exempt = {"enso", "curve", "pendle"}
    params = []
    for protocol in sorted(_SWAP_PROTOCOLS):
        if protocol in router_exempt:
            params.append((protocol, "arbitrum"))
        else:
            for chain, routers in sorted(PROTOCOL_ROUTERS.items()):
                if protocol in routers and chain not in _CHAIN_ALIASES:
                    params.append((protocol, chain))
    return params


def _collect_lp_open_params() -> list[tuple[str, str]]:
    """Collect (protocol, chain) pairs for LP_OPEN intents."""
    params = []
    for chain, managers in sorted(LP_POSITION_MANAGERS.items()):
        if chain in _CHAIN_ALIASES:
            continue
        for protocol in sorted(managers):
            if protocol in _LP_PROTOCOLS:
                params.append((protocol, chain))
    return params


def _collect_lp_close_params() -> list[tuple[str, str]]:
    """Collect (protocol, chain) pairs for LP_CLOSE intents (same as LP_OPEN)."""
    return _collect_lp_open_params()


def _collect_lending_params() -> list[tuple[str, str]]:
    """Collect (protocol, chain) pairs for lending intents.

    Aave V3 uses LENDING_POOL_ADDRESSES.
    morpho_blue and compound_v3 bypass the registry check; use ethereum.
    """
    registry_exempt = {"morpho_blue", "compound_v3"}
    params = []
    for protocol in sorted(_LENDING_PROTOCOLS):
        if protocol in registry_exempt:
            params.append((protocol, "ethereum"))
        else:
            for chain, pools in sorted(LENDING_POOL_ADDRESSES.items()):
                if protocol in pools and chain not in _CHAIN_ALIASES:
                    params.append((protocol, chain))
    return params


def _collect_perp_params() -> list[tuple[str, str]]:
    """Collect (protocol, chain) pairs for perp intents."""
    return [(p, "arbitrum") for p in sorted(_PERP_PROTOCOLS)]


def _collect_flash_loan_params() -> list[tuple[str, str]]:
    """Collect (protocol, chain) pairs for flash loan intents."""
    return [(p, "arbitrum") for p in sorted(_FLASH_LOAN_PROVIDERS)]


def _collect_vault_params() -> list[tuple[str, str]]:
    """Collect (protocol, chain) pairs for vault intents."""
    return [("metamorpho", chain) for chain in sorted(METAMORPHO_VAULTS)]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _id(param: tuple[str, str]) -> str:
    return f"{param[0]}-{param[1]}"


def _assert_permissions_or_known_gap(
    protocol: str, intent_type: str, chain: str,
    permissions: list, warnings: list,
):
    """Assert permissions are produced, unless this is a known gap.

    Known gaps: verify no crash (compilation errors), but don't require
    permissions. Non-gaps: require at least one permission.
    """
    compile_errors = [w for w in warnings if "Compilation error" in w]
    assert not compile_errors, f"Compilation errors: {compile_errors}"

    if _is_known_gap(protocol, intent_type, chain):
        return  # no crash is sufficient

    assert len(permissions) >= 1, (
        f"No permissions discovered for {intent_type} {protocol} on {chain}. "
        f"If this is expected, add ({protocol!r}, {intent_type!r}, {chain!r}) to _KNOWN_GAPS."
    )
    for perm in permissions:
        assert perm.target.startswith("0x"), f"Invalid target address: {perm.target}"
        assert len(perm.function_selectors) >= 1, f"No selectors for {perm.target}"


# ---------------------------------------------------------------------------
# SWAP
# ---------------------------------------------------------------------------

class TestSwapCompatibility:
    """Every protocol in _SWAP_PROTOCOLS with a known router compiles a SWAP."""

    @pytest.mark.parametrize("protocol,chain", _collect_swap_params(), ids=[_id(p) for p in _collect_swap_params()])
    def test_swap_synthetic_intent(self, protocol: str, chain: str):
        intents = build_synthetic_intents(protocol, "SWAP", chain)
        assert len(intents) >= 1, f"No synthetic SWAP intent for {protocol} on {chain}"

    @pytest.mark.parametrize("protocol,chain", _collect_swap_params(), ids=[_id(p) for p in _collect_swap_params()])
    def test_swap_discovers_permissions(self, protocol: str, chain: str):
        permissions, warnings = discover_permissions(chain, [protocol], ["SWAP"])
        _assert_permissions_or_known_gap(protocol, "SWAP", chain, permissions, warnings)


# ---------------------------------------------------------------------------
# LP_OPEN
# ---------------------------------------------------------------------------

class TestLPOpenCompatibility:
    """Every protocol in _LP_PROTOCOLS with a known position manager compiles LP_OPEN."""

    @pytest.mark.parametrize("protocol,chain", _collect_lp_open_params(), ids=[_id(p) for p in _collect_lp_open_params()])
    def test_lp_open_synthetic_intent(self, protocol: str, chain: str):
        intents = build_synthetic_intents(protocol, "LP_OPEN", chain)
        assert len(intents) >= 1, f"No synthetic LP_OPEN intent for {protocol} on {chain}"

    @pytest.mark.parametrize("protocol,chain", _collect_lp_open_params(), ids=[_id(p) for p in _collect_lp_open_params()])
    def test_lp_open_discovers_permissions(self, protocol: str, chain: str):
        permissions, warnings = discover_permissions(chain, [protocol], ["LP_OPEN"])
        _assert_permissions_or_known_gap(protocol, "LP_OPEN", chain, permissions, warnings)


# ---------------------------------------------------------------------------
# LP_CLOSE
# ---------------------------------------------------------------------------

class TestLPCloseCompatibility:
    """Every protocol in _LP_PROTOCOLS with a known position manager compiles LP_CLOSE.

    LP_CLOSE often requires RPC to look up the position, so compilation may
    warn rather than succeed. We verify no crashes; permissions are optional
    for known-gap combos.
    """

    @pytest.mark.parametrize("protocol,chain", _collect_lp_close_params(), ids=[_id(p) for p in _collect_lp_close_params()])
    def test_lp_close_synthetic_intent(self, protocol: str, chain: str):
        intents = build_synthetic_intents(protocol, "LP_CLOSE", chain)
        assert len(intents) >= 1, f"No synthetic LP_CLOSE intent for {protocol} on {chain}"

    @pytest.mark.parametrize("protocol,chain", _collect_lp_close_params(), ids=[_id(p) for p in _collect_lp_close_params()])
    def test_lp_close_no_crash(self, protocol: str, chain: str):
        """LP_CLOSE may not produce permissions (needs RPC), but must not crash."""
        _permissions, warnings = discover_permissions(chain, [protocol], ["LP_CLOSE"])
        compile_errors = [w for w in warnings if "Compilation error" in w]
        assert not compile_errors, f"Compilation errors: {compile_errors}"


# ---------------------------------------------------------------------------
# LENDING (SUPPLY, WITHDRAW, BORROW, REPAY)
# ---------------------------------------------------------------------------

class TestLendingCompatibility:
    """Every protocol in _LENDING_PROTOCOLS compiles all lending intent types."""

    _lending_intent_types = ("SUPPLY", "WITHDRAW", "BORROW", "REPAY")

    @pytest.mark.parametrize("protocol,chain", _collect_lending_params(), ids=[_id(p) for p in _collect_lending_params()])
    def test_lending_synthetic_intents(self, protocol: str, chain: str):
        for intent_type in self._lending_intent_types:
            intents = build_synthetic_intents(protocol, intent_type, chain)
            assert len(intents) >= 1, f"No synthetic {intent_type} intent for {protocol} on {chain}"

    @pytest.mark.parametrize("protocol,chain", _collect_lending_params(), ids=[_id(p) for p in _collect_lending_params()])
    def test_lending_discovers_permissions(self, protocol: str, chain: str):
        permissions, warnings = discover_permissions(chain, [protocol], self._lending_intent_types)
        _assert_permissions_or_known_gap(protocol, "SUPPLY", chain, permissions, warnings)


# ---------------------------------------------------------------------------
# PERP (PERP_OPEN, PERP_CLOSE)
# ---------------------------------------------------------------------------

class TestPerpCompatibility:
    """Every protocol in _PERP_PROTOCOLS compiles perp intents."""

    @pytest.mark.parametrize("protocol,chain", _collect_perp_params(), ids=[_id(p) for p in _collect_perp_params()])
    def test_perp_synthetic_intents(self, protocol: str, chain: str):
        for intent_type in ["PERP_OPEN", "PERP_CLOSE"]:
            intents = build_synthetic_intents(protocol, intent_type, chain)
            assert len(intents) >= 1, f"No synthetic {intent_type} intent for {protocol} on {chain}"

    @pytest.mark.parametrize("protocol,chain", _collect_perp_params(), ids=[_id(p) for p in _collect_perp_params()])
    def test_perp_discovers_permissions(self, protocol: str, chain: str):
        permissions, warnings = discover_permissions(chain, [protocol], ["PERP_OPEN", "PERP_CLOSE"])
        _assert_permissions_or_known_gap(protocol, "PERP_OPEN", chain, permissions, warnings)


# ---------------------------------------------------------------------------
# FLASH_LOAN
# ---------------------------------------------------------------------------

class TestFlashLoanCompatibility:
    """Every provider in _FLASH_LOAN_PROVIDERS compiles flash loan intents."""

    @pytest.mark.parametrize("protocol,chain", _collect_flash_loan_params(), ids=[_id(p) for p in _collect_flash_loan_params()])
    def test_flash_loan_synthetic_intent(self, protocol: str, chain: str):
        intents = build_synthetic_intents(protocol, "FLASH_LOAN", chain)
        assert len(intents) >= 1, f"No synthetic FLASH_LOAN intent for {protocol} on {chain}"

    @pytest.mark.parametrize("protocol,chain", _collect_flash_loan_params(), ids=[_id(p) for p in _collect_flash_loan_params()])
    def test_flash_loan_discovers_permissions(self, protocol: str, chain: str):
        permissions, warnings = discover_permissions(chain, [protocol], ["FLASH_LOAN"])
        _assert_permissions_or_known_gap(protocol, "FLASH_LOAN", chain, permissions, warnings)


# ---------------------------------------------------------------------------
# VAULT (VAULT_DEPOSIT, VAULT_REDEEM)
# ---------------------------------------------------------------------------

class TestVaultCompatibility:
    """MetaMorpho vault intents compile on supported chains."""

    @pytest.mark.parametrize("protocol,chain", _collect_vault_params(), ids=[_id(p) for p in _collect_vault_params()])
    def test_vault_synthetic_intents(self, protocol: str, chain: str):
        for intent_type in ["VAULT_DEPOSIT", "VAULT_REDEEM"]:
            intents = build_synthetic_intents(protocol, intent_type, chain)
            assert len(intents) >= 1, f"No synthetic {intent_type} intent for {protocol} on {chain}"

    @pytest.mark.parametrize("protocol,chain", _collect_vault_params(), ids=[_id(p) for p in _collect_vault_params()])
    def test_vault_discovers_permissions(self, protocol: str, chain: str):
        permissions, warnings = discover_permissions(chain, [protocol], ["VAULT_DEPOSIT", "VAULT_REDEEM"])
        _assert_permissions_or_known_gap(protocol, "VAULT_DEPOSIT", chain, permissions, warnings)


# ---------------------------------------------------------------------------
# Cross-cutting: negative cases
# ---------------------------------------------------------------------------

class TestNegativeCases:
    """Protocols that don't support certain intent types return empty."""

    def test_swap_protocol_returns_empty_for_lending(self):
        intents = build_synthetic_intents("uniswap_v3", "SUPPLY", "arbitrum")
        assert intents == []

    def test_lending_protocol_returns_empty_for_swap(self):
        intents = build_synthetic_intents("aave_v3", "SWAP", "arbitrum")
        assert intents == []

    def test_unknown_protocol_returns_empty(self):
        intents = build_synthetic_intents("nonexistent_protocol", "SWAP", "arbitrum")
        assert intents == []

    def test_discover_permissions_unknown_protocol_no_crash(self):
        permissions, warnings = discover_permissions("arbitrum", ["nonexistent_protocol"], ["SWAP"])
        assert permissions == []


# ---------------------------------------------------------------------------
# Coverage summary
# ---------------------------------------------------------------------------

def test_coverage_summary():
    """Verify sufficient coverage of the protocol/chain space."""
    swap = _collect_swap_params()
    lp = _collect_lp_open_params()
    lending = _collect_lending_params()
    perp = _collect_perp_params()
    flash = _collect_flash_loan_params()
    vault = _collect_vault_params()

    total = len(swap) + len(lp) * 2 + len(lending) + len(perp) + len(flash) + len(vault)
    protocols = set()
    chains = set()
    for params in [swap, lp, lending, perp, flash, vault]:
        for p, c in params:
            protocols.add(p)
            chains.add(c)

    assert len(protocols) >= 8, f"Expected at least 8 protocols, got {len(protocols)}: {protocols}"
    assert len(chains) >= 4, f"Expected at least 4 chains, got {len(chains)}: {chains}"
    assert total >= 30, f"Expected at least 30 test triples, got {total}"
