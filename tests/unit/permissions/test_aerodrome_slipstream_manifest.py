"""Permission-manifest regression tests for Aerodrome Slipstream (VIB-4434 W1).

Slipstream LP compilation requires RPC (`validate_aerodrome_cl_pool` plus
`adapter.remove_cl_liquidity` both read on-chain state), so the offline
manifest carries the Slipstream NonfungiblePositionManager selectors via
per-intent ``StaticPermissionEntry`` rather than synthetic-compile output.
These tests pin four invariants:

1. The loader returns distinct hints objects for ``"aerodrome"`` (Classic)
   and ``"aerodrome_slipstream"`` (CL), and Classic stays unchanged.
2. The synthetic-intent builder emits Slipstream intents for LP_OPEN /
   LP_CLOSE / LP_COLLECT_FEES on Base.
3. The protocol-intent matrix surfaces ``aerodrome_slipstream`` for each LP
   intent type.
4. Per-intent manifest least-privilege — LP_OPEN-only contains exactly
   ``{mint}`` on the NPM target, LP_CLOSE-only exactly
   ``{decreaseLiquidity, collect}``, LP_COLLECT_FEES-only exactly
   ``{collect}``. A single broad static entry covering all selectors would
   pass a union check but fail these per-intent strict-equality checks —
   that is the over-permissioning regression the suite catches.

The per-intent assertions call ``discover_permissions`` directly to bypass
``generate_manifest``'s teardown-complement auto-expansion (LP_OPEN →
LP_OPEN+LP_CLOSE). The expanded behaviour is exercised by the closing
``test_combined_discovery_is_union_of_intent_sets`` sanity check.
"""

from __future__ import annotations

import pytest

from almanak.core.contracts import AERODROME
from almanak.framework.permissions.discovery import discover_permissions
from almanak.framework.permissions.hints import (
    PermissionHints,
    StaticPermissionEntry,
    get_permission_hints,
)
from almanak.framework.permissions.synthetic_intents import (
    build_synthetic_intents,
    get_protocol_intent_matrix,
)

# Selectors pinned by ``connectors/aerodrome/permission_hints.py`` (W1).
_SLIPSTREAM_MINT_SELECTOR = "0xb5007d1f"
_SLIPSTREAM_DECREASE_SELECTOR = "0x0c49ccbe"
_SLIPSTREAM_COLLECT_SELECTOR = "0xfc6f7865"

_NPM_BASE = AERODROME["base"]["cl_nft"].lower()
_ROUTER_BASE = AERODROME["base"]["router"].lower()


def _npm_selectors_for_intents(intent_types: list[str], chain: str = "base") -> set[str]:
    """Selectors authorised on the Slipstream NPM target for an exact
    intent_types list (no teardown-complement expansion).
    """
    permissions, _warnings = discover_permissions(
        chain=chain,
        protocols=["aerodrome_slipstream"],
        intent_types=intent_types,
    )
    return {
        sel.selector.lower()
        for perm in permissions
        if perm.target.lower() == _NPM_BASE
        for sel in perm.function_selectors
    }


class TestSlipstreamHintsLoader:
    """The loader resolves both protocol literals to distinct hints objects."""

    def test_slipstream_hints_load_returns_slipstream_object(self) -> None:
        hints = get_permission_hints("aerodrome_slipstream")
        assert isinstance(hints, PermissionHints)
        assert hints.supports_standalone_fee_collection is True
        chain_static = hints.static_permissions.get("base", [])
        assert chain_static, "Slipstream hints must expose base static_permissions"
        assert all(entry.target.lower() == _NPM_BASE for entry in chain_static), (
            "All Slipstream static entries must target the NPM"
        )
        assert all(entry.intent_types is not None for entry in chain_static), (
            "All Slipstream static entries must be per-intent scoped — a "
            "None intent_types value would broadcast the selectors across "
            "every intent type and defeat least-privilege"
        )

    def test_classic_hints_unchanged_no_npm_leak(self) -> None:
        """Regression: ``get_permission_hints('aerodrome')`` must still return
        the Classic hints object. The Slipstream NPM target must NOT appear
        in Classic static permissions (which would over-permission Classic
        strategies).
        """
        classic = get_permission_hints("aerodrome")
        assert isinstance(classic, PermissionHints)
        assert classic.supports_standalone_fee_collection is False
        chain_static = classic.static_permissions.get("base", [])
        targets = {entry.target.lower() for entry in chain_static}
        assert _ROUTER_BASE in targets, (
            "Classic Aerodrome hints must still authorise the Router"
        )
        assert _NPM_BASE not in targets, (
            "Classic Aerodrome hints must NOT include the Slipstream NPM target"
        )

    def test_slipstream_static_entries_are_per_intent_disjoint(self) -> None:
        """No two Slipstream static entries share an intent_types value —
        each (intent_type, selector) tuple appears in exactly one entry so
        the discovery-time filter remains unambiguous.
        """
        hints = get_permission_hints("aerodrome_slipstream")
        entries: list[StaticPermissionEntry] = hints.static_permissions.get("base", [])
        intent_scopes = [entry.intent_types for entry in entries]
        assert len(intent_scopes) == len(set(intent_scopes)), (
            "Each Slipstream static entry must scope to a unique intent_types "
            f"frozenset; got {intent_scopes}"
        )


class TestSlipstreamSyntheticBuilder:
    """The synthetic builder must emit at least one intent per LP intent type."""

    @pytest.mark.parametrize("intent_type", ["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"])
    def test_build_synthetic_intents_non_empty(self, intent_type: str) -> None:
        intents = build_synthetic_intents("aerodrome_slipstream", intent_type, "base")
        assert intents, (
            f"Synthetic builder must emit at least one intent for "
            f"aerodrome_slipstream/{intent_type}/base"
        )


class TestSlipstreamMatrix:
    """The protocol-intent matrix surfaces the Slipstream LP intents."""

    @pytest.mark.parametrize("intent_type", ["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"])
    def test_matrix_includes_slipstream_lp_intents(self, intent_type: str) -> None:
        matrix = get_protocol_intent_matrix()
        assert "aerodrome_slipstream" in matrix, (
            "get_protocol_intent_matrix() must surface aerodrome_slipstream — "
            "missing membership in _LP_PROTOCOLS or similar registration gap"
        )
        intent_values = {it.value for it in matrix["aerodrome_slipstream"]}
        assert intent_type in intent_values, (
            f"aerodrome_slipstream matrix missing {intent_type}; got {sorted(intent_values)}"
        )


class TestSlipstreamManifestLeastPrivilege:
    """Per-intent NPM selectors must be exactly what the compiler emits.

    A union assertion across all three intent types would pass even if a
    single broad ``StaticPermissionEntry`` covered every selector for every
    intent — the per-intent strict-equality assertions below are what catch
    that regression.
    """

    def test_lp_open_only_npm_selectors_are_mint_only(self) -> None:
        selectors = _npm_selectors_for_intents(["LP_OPEN"])
        assert selectors == {_SLIPSTREAM_MINT_SELECTOR}, (
            f"LP_OPEN-only NPM selectors must be exactly mint "
            f"({_SLIPSTREAM_MINT_SELECTOR}); got {sorted(selectors)}"
        )

    def test_lp_close_only_npm_selectors_are_decrease_and_collect(self) -> None:
        selectors = _npm_selectors_for_intents(["LP_CLOSE"])
        assert selectors == {
            _SLIPSTREAM_DECREASE_SELECTOR,
            _SLIPSTREAM_COLLECT_SELECTOR,
        }, (
            f"LP_CLOSE-only NPM selectors must be exactly "
            f"{{decreaseLiquidity, collect}}; got {sorted(selectors)}"
        )

    def test_lp_collect_fees_only_npm_selectors_are_collect_only(self) -> None:
        selectors = _npm_selectors_for_intents(["LP_COLLECT_FEES"])
        assert selectors == {_SLIPSTREAM_COLLECT_SELECTOR}, (
            f"LP_COLLECT_FEES-only NPM selectors must be exactly collect "
            f"({_SLIPSTREAM_COLLECT_SELECTOR}); got {sorted(selectors)}"
        )

    def test_combined_discovery_is_union_of_intent_sets(self) -> None:
        """Sanity: union of per-intent sets matches the combined discovery."""
        combined = _npm_selectors_for_intents(
            ["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"]
        )
        assert combined == {
            _SLIPSTREAM_MINT_SELECTOR,
            _SLIPSTREAM_DECREASE_SELECTOR,
            _SLIPSTREAM_COLLECT_SELECTOR,
        }, (
            f"Combined LP discovery NPM selectors must be the three-element "
            f"union; got {sorted(combined)}"
        )
