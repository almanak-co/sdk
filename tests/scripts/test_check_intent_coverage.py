"""Tests for ``scripts/ci/check_intent_coverage.py`` — VIB-4340.

Covers the multi-kwarg attribution model (``protocol=``,
``preferred_bridge=``, ``provider=``) and the drift-detection lockstep
against the production intent class definitions.

Pair-wise attribution semantics are documented in
``_collect_protocol_intent_pairs``. Each test pins one branch of that
function so a regression surfaces here before it surfaces as a
silently-dropped coverage cell or a phantom credit.
"""

from __future__ import annotations

import ast
import importlib.util
import sys
import typing
from pathlib import Path

import pytest


def _load_module():
    """Load the gate script as a top-level module via importlib."""
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "ci" / "check_intent_coverage.py"
    spec = importlib.util.spec_from_file_location("check_intent_coverage", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_intent_coverage"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def gate():
    return _load_module()


# The gate's registry-derived BRIDGE-connector set in production includes
# at least these three; in synthetic tests we hardcode the set so we
# don't have to import the registry.
_BRIDGE_CONNECTORS = frozenset({"across", "stargate", "lifi"})


def _scan(
    gate,
    code: str,
    *,
    bridge_connectors: frozenset[str] = _BRIDGE_CONNECTORS,
) -> set[tuple[str, str]]:
    """Parse *code*, run _collect_protocol_intent_pairs, return pairs."""
    tree = ast.parse(code)
    return gate._collect_protocol_intent_pairs(
        tree,
        path=Path("<synthetic>"),
        bridge_connectors=bridge_connectors,
    )


# ────────────────────────────────────────────────────────────────────────────
# Existing ``protocol=`` semantics (regression guard)
# ────────────────────────────────────────────────────────────────────────────


def test_protocol_kwarg_emits_pair(gate):
    pairs = _scan(gate, 'SwapIntent(protocol="uniswap_v3", amount=1)')
    assert pairs == {("uniswap_v3", "SWAP")}


def test_protocol_kwarg_lowercases(gate):
    pairs = _scan(gate, 'SwapIntent(protocol="Uniswap_V3")')
    assert pairs == {("uniswap_v3", "SWAP")}


def test_protocol_no_kwarg_emits_nothing(gate):
    pairs = _scan(gate, "SwapIntent(amount=1)")
    assert pairs == set()


def test_protocol_non_literal_silently_skipped(gate):
    pairs = _scan(gate, 'p = "uniswap_v3"\nSwapIntent(protocol=p)')
    assert pairs == set()


# ────────────────────────────────────────────────────────────────────────────
# Validator-bypass constructors (model_construct / for_permission_discovery)
# still count for coverage — intent-test fixtures use them to build a bundled
# borrow the BorrowIntent guard would reject at normal construction.
# ────────────────────────────────────────────────────────────────────────────


def test_model_construct_emits_pair(gate):
    pairs = _scan(gate, 'BorrowIntent.model_construct(protocol="aave_v3", collateral_amount=1)')
    assert pairs == {("aave_v3", "BORROW")}


def test_for_permission_discovery_emits_pair(gate):
    pairs = _scan(gate, 'BorrowIntent.for_permission_discovery(protocol="morpho_blue")')
    assert pairs == {("morpho_blue", "BORROW")}


def test_unknown_method_on_intent_class_skipped(gate):
    # A non-constructor method call on an intent class must NOT be credited.
    pairs = _scan(gate, 'BorrowIntent.serialize(protocol="aave_v3")')
    assert pairs == set()


def test_protocol_unknown_value_passes_through(gate):
    # Legacy behavior: `protocol=` is NOT validated against the registry
    # at scan time. Phantom credit gets filtered out by `gaps = required
    # - covered - excused` in main(). Preserved to avoid breaking existing
    # tests during VIB-4340.
    pairs = _scan(gate, 'SwapIntent(protocol="nonexistent")')
    assert pairs == {("nonexistent", "SWAP")}


# ────────────────────────────────────────────────────────────────────────────
# BridgeIntent.preferred_bridge attribution (new in VIB-4340)
# ────────────────────────────────────────────────────────────────────────────


def test_bridge_known_value_emits_pair(gate):
    pairs = _scan(gate, 'BridgeIntent(preferred_bridge="Across", to_chain="x")')
    assert pairs == {("across", "BRIDGE")}


def test_bridge_lowercase_value_emits_pair(gate):
    pairs = _scan(gate, 'BridgeIntent(preferred_bridge="stargate", to_chain="x")')
    assert pairs == {("stargate", "BRIDGE")}


def test_bridge_no_kwarg_emits_nothing(gate):
    pairs = _scan(gate, 'BridgeIntent(to_chain="x", from_chain="y")')
    assert pairs == set()


def test_bridge_non_literal_silently_skipped(gate):
    pairs = _scan(gate, 'v = "Across"\nBridgeIntent(preferred_bridge=v, to_chain="x")')
    assert pairs == set()


def test_bridge_unknown_value_fails_loud(gate):
    with pytest.raises(gate.CoverageError) as excinfo:
        _scan(gate, 'BridgeIntent(preferred_bridge="bogus", to_chain="x")')
    assert "bogus" in str(excinfo.value)
    assert "BridgeIntent" in str(excinfo.value)
    assert "preferred_bridge" in str(excinfo.value)


# ────────────────────────────────────────────────────────────────────────────
# FlashLoanIntent.provider attribution (new in VIB-4340)
# ────────────────────────────────────────────────────────────────────────────


def test_flashloan_aave_normalizes_to_aave_v3(gate):
    pairs = _scan(gate, 'FlashLoanIntent(provider="aave", token="USDC")')
    assert pairs == {("aave_v3", "FLASH_LOAN")}


def test_flashloan_morpho_normalizes_to_morpho_blue(gate):
    pairs = _scan(gate, 'FlashLoanIntent(provider="morpho", token="USDC")')
    assert pairs == {("morpho_blue", "FLASH_LOAN")}


def test_flashloan_balancer_identity_mapping(gate):
    pairs = _scan(gate, 'FlashLoanIntent(provider="balancer", token="USDC")')
    assert pairs == {("balancer", "FLASH_LOAN")}


def test_flashloan_auto_silently_skipped(gate):
    # ``provider="auto"`` is runtime routing — no specific connector to credit.
    pairs = _scan(gate, 'FlashLoanIntent(provider="auto", token="USDC")')
    assert pairs == set()


def test_flashloan_unknown_value_fails_loud(gate):
    with pytest.raises(gate.CoverageError) as excinfo:
        _scan(gate, 'FlashLoanIntent(provider="curve", token="USDC")')
    msg = str(excinfo.value)
    assert "curve" in msg
    assert "FlashLoanIntent" in msg
    assert "provider" in msg
    assert "PROVIDER_TO_CONNECTOR" in msg


def test_flashloan_non_literal_silently_skipped(gate):
    pairs = _scan(gate, 'p = "aave"\nFlashLoanIntent(provider=p, token="USDC")')
    assert pairs == set()


def test_flashloan_positional_arg_ignored(gate):
    # The gate only inspects keyword args; positional provider is not credited.
    pairs = _scan(gate, 'FlashLoanIntent("aave", "USDC")')
    assert pairs == set()


# ────────────────────────────────────────────────────────────────────────────
# Defensive: both `protocol=` and alternate kwarg present
# ────────────────────────────────────────────────────────────────────────────


def test_both_protocol_and_alternate_fails_loud(gate):
    with pytest.raises(gate.CoverageError) as excinfo:
        _scan(
            gate,
            'FlashLoanIntent(protocol="aave_v3", provider="aave", token="USDC")',
        )
    msg = str(excinfo.value)
    assert "protocol=" in msg
    assert "provider=" in msg
    assert "pick one" in msg


def test_bridge_both_protocol_and_preferred_bridge_fails_loud(gate):
    with pytest.raises(gate.CoverageError) as excinfo:
        _scan(
            gate,
            'BridgeIntent(protocol="across", preferred_bridge="Across", to_chain="x")',
        )
    msg = str(excinfo.value)
    assert "protocol=" in msg
    assert "preferred_bridge=" in msg


# ────────────────────────────────────────────────────────────────────────────
# Pair-wise attribution: mixed-protocol files don't credit off-diagonals
# ────────────────────────────────────────────────────────────────────────────


def test_mixed_intents_no_off_diagonal_credit(gate):
    # A file that constructs both BridgeIntent and SwapIntent should
    # credit (across, BRIDGE) and (uniswap_v3, SWAP) — NOT the
    # off-diagonal (across, SWAP) or (uniswap_v3, BRIDGE).
    pairs = _scan(
        gate,
        'BridgeIntent(preferred_bridge="Across", to_chain="x")\n'
        'SwapIntent(protocol="uniswap_v3", amount=1)',
    )
    assert pairs == {("across", "BRIDGE"), ("uniswap_v3", "SWAP")}


# ────────────────────────────────────────────────────────────────────────────
# Lockstep — drift between gate tables and production vocab
# ────────────────────────────────────────────────────────────────────────────


def test_connector_kwarg_lockstep(gate):
    """The gate's CONNECTOR_KWARG_BY_INTENT_CLASS keys must match the
    field names actually present on the production Pydantic intent
    classes. Catches a future field rename (e.g. ``preferred_bridge``
    → ``bridge_provider``) before it silently drops coverage.
    """
    from almanak.framework.intents.advanced_intents import FlashLoanIntent
    from almanak.framework.intents.bridge import BridgeIntent

    assert "preferred_bridge" in BridgeIntent.model_fields, (
        "BridgeIntent.preferred_bridge field renamed/removed; update "
        "CONNECTOR_KWARG_BY_INTENT_CLASS in check_intent_coverage.py"
    )
    assert "provider" in FlashLoanIntent.model_fields, (
        "FlashLoanIntent.provider field renamed/removed; update "
        "CONNECTOR_KWARG_BY_INTENT_CLASS in check_intent_coverage.py"
    )


def test_provider_literal_lockstep(gate):
    """The gate's PROVIDER_TO_CONNECTOR keys must exactly match
    FlashLoanIntent.provider's Literal values. Drift surfaces a new
    provider literal added to the vocab without a matching gate-table
    entry, OR a stale gate-table entry for a retired provider.
    """
    from almanak.framework.intents.advanced_intents import FlashLoanIntent

    provider_annotation = FlashLoanIntent.model_fields["provider"].annotation
    literal_values = set(typing.get_args(provider_annotation))
    table_keys = set(gate.PROVIDER_TO_CONNECTOR.keys())

    assert literal_values == table_keys, (
        f"FlashLoanIntent.provider Literal values {sorted(literal_values)!r} "
        f"don't match PROVIDER_TO_CONNECTOR keys {sorted(table_keys)!r}. "
        f"Update PROVIDER_TO_CONNECTOR in check_intent_coverage.py."
    )
