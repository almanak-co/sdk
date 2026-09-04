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

``TestSlipstreamSwapManifest`` (VIB-5990) additionally pins the SWAP manifest:
the slipstream slug's swaps compile through the shared
``compile_swap_aerodrome`` path onto the CL SwapRouter, discovered
synthetically via the connector-owned ``build_discovery_vectors`` override.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from almanak.connectors._strategy_base.pool_validation_base import (
    PoolValidationReason,
    PoolValidationResult,
)
from almanak.connectors.aerodrome.addresses import AERODROME, slipstream_lp_deployments
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
# CL SwapRouter exactInputSingle — adapter-owned constant (VIB-5990).
_CL_EXACT_INPUT_SINGLE_SELECTOR = "0xa026383e"
# Classic (Solidly) router swapExactTokensForTokens — the route the shared
# compiler's auto ladder falls back to when no CL pool exists (VIB-5990).
_CLASSIC_SWAP_SELECTOR = "0xcac88ea9"
_ERC20_APPROVE_SELECTOR = "0x095ea7b3"

_DEPLOYMENTS_BASE = slipstream_lp_deployments("base")
_BY_GENERATION = {deployment.generation: deployment for deployment in _DEPLOYMENTS_BASE}
_NPM_CURRENT = _BY_GENERATION["current"].position_manager.lower()
_NPM_LEGACY = _BY_GENERATION["legacy"].position_manager.lower()
_NPM_TARGETS = {deployment.position_manager.lower() for deployment in _DEPLOYMENTS_BASE}
_ROUTER_BASE = AERODROME["base"]["router"].lower()
# One CL SwapRouter per reviewed generation. The pool decides which one a live
# swap executes on, so the offline manifest must authorise every one of them.
_CL_ROUTERS_BASE = {deployment.swap_router.lower() for deployment in _DEPLOYMENTS_BASE if deployment.swap_router}


def _npm_selectors_for_intents(
    intent_types: list[str],
    chain: str = "base",
    *,
    target: str = _NPM_CURRENT,
) -> set[str]:
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
        if perm.target.lower() == target
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
        assert {entry.target.lower() for entry in chain_static} == _NPM_TARGETS, (
            "Slipstream static entries must target exactly the reviewed current and legacy NPMs"
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
        assert targets.isdisjoint(_NPM_TARGETS), (
            "Classic Aerodrome hints must NOT include any Slipstream NPM target"
        )

    def test_slipstream_static_entries_are_per_intent_disjoint(self) -> None:
        """No two Slipstream static entries share an intent_types value —
        each (intent_type, selector) tuple appears in exactly one entry so
        the discovery-time filter remains unambiguous.
        """
        hints = get_permission_hints("aerodrome_slipstream")
        entries: list[StaticPermissionEntry] = hints.static_permissions.get("base", [])
        target_scopes = [(entry.target.lower(), entry.intent_types) for entry in entries]
        assert len(target_scopes) == len(set(target_scopes)), (
            "Each Slipstream static entry must have a unique target/intent scope; "
            f"got {target_scopes}"
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
        # The pool a strategy names decides which generation mints, so every
        # reviewed NPM is authorised for mint and nothing else.
        for target in _NPM_TARGETS:
            selectors = _npm_selectors_for_intents(["LP_OPEN"], target=target)
            assert selectors == {_SLIPSTREAM_MINT_SELECTOR}, (
                f"LP_OPEN-only selectors on {target} must be exactly mint "
                f"({_SLIPSTREAM_MINT_SELECTOR}); got {sorted(selectors)}"
            )

    def test_lp_close_only_npm_selectors_are_decrease_and_collect(self) -> None:
        for target in _NPM_TARGETS:
            selectors = _npm_selectors_for_intents(["LP_CLOSE"], target=target)
            assert selectors == {
                _SLIPSTREAM_DECREASE_SELECTOR,
                _SLIPSTREAM_COLLECT_SELECTOR,
            }, (
                f"LP_CLOSE-only selectors on {target} must be exactly "
                f"{{decreaseLiquidity, collect}}; got {sorted(selectors)}"
            )

    def test_lp_collect_fees_only_npm_selectors_are_collect_only(self) -> None:
        for target in _NPM_TARGETS:
            selectors = _npm_selectors_for_intents(["LP_COLLECT_FEES"], target=target)
            assert selectors == {_SLIPSTREAM_COLLECT_SELECTOR}, (
                f"LP_COLLECT_FEES-only selectors on {target} must be exactly collect "
                f"({_SLIPSTREAM_COLLECT_SELECTOR}); got {sorted(selectors)}"
            )

    def test_combined_discovery_is_union_of_intent_sets(self) -> None:
        """Sanity: union of per-intent sets matches the combined discovery."""
        for target in _NPM_TARGETS:
            combined = _npm_selectors_for_intents(
                ["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"],
                target=target,
            )
            assert combined == {
                _SLIPSTREAM_MINT_SELECTOR,
                _SLIPSTREAM_DECREASE_SELECTOR,
                _SLIPSTREAM_COLLECT_SELECTOR,
            }, (
                f"Combined discovery on {target} must include mint/decrease/collect; "
                f"got {sorted(combined)}"
            )


def _discover(intent_types: list[str], chain: str = "base"):
    permissions, _warnings = discover_permissions(
        chain=chain,
        protocols=["aerodrome_slipstream"],
        intent_types=intent_types,
    )
    return permissions


def _selectors_by_target(permissions) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for perm in permissions:
        result.setdefault(perm.target.lower(), set()).update(
            sel.selector.lower() for sel in perm.function_selectors
        )
    return result


class TestSlipstreamSwapManifest:
    """SWAP manifest coverage for the ``aerodrome_slipstream`` slug (VIB-5990).

    ``AerodromeCompiler`` dispatches SWAP for BOTH protocol slugs through the
    shared ``compile_swap_aerodrome`` path (CL SwapRouter ``exactInputSingle``),
    but slipstream was absent from the SWAP synthetic-discovery set, so
    ``discover_permissions(..., protocols=["aerodrome_slipstream"],
    intent_types=["SWAP"])`` returned an EMPTY manifest and every Safe-path
    slipstream swap reverted unauthorized at ``execTransactionWithRole``.
    These tests pin the fixed manifest: CL SwapRouter target + swap selector,
    the ERC-20 approve for the router, and least-privilege isolation between
    the SWAP and LP manifests.
    """

    @pytest.fixture(autouse=True)
    def _offline_probes(self):
        """Hold both pool probes unverifiable so the default cells model offline discovery."""
        unverifiable = PoolValidationResult(exists=None, reason=PoolValidationReason.RPC_UNAVAILABLE, warning="offline")
        with (
            patch(
                "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
                return_value=unverifiable,
            ),
            patch(
                "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_pool",
                return_value=unverifiable,
            ),
        ):
            yield

    def test_swap_manifest_authorises_every_router_even_when_a_probe_names_one_generation(self) -> None:
        """A reachable RPC must not narrow the manifest to the generation the synthetic pair happens to live on.

        Which generation a strategy's real pools live on is not knowable from
        the synthetic pair, so discovery emits every reviewed router; the
        Safe-path swap through the other generation's router would otherwise
        revert unauthorized.
        """
        legacy = next(d for d in slipstream_lp_deployments("base") if d.generation == "legacy")
        confirmed = PoolValidationResult(
            exists=True,
            reason=PoolValidationReason.CONFIRMED,
            pool_address="0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59",
            factory=legacy.factory,
        )
        with patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
            return_value=confirmed,
        ):
            by_target = _selectors_by_target(_discover(["SWAP"]))
        for router in _CL_ROUTERS_BASE:
            assert by_target.get(router) == {_CL_EXACT_INPUT_SINGLE_SELECTOR}, (router, sorted(by_target))

    def test_swap_manifest_contains_cl_router_swap_selector(self) -> None:
        # Both reviewed generations, named explicitly: a pool owned by either
        # factory executes on that generation's router.
        assert _CL_ROUTERS_BASE == {
            _BY_GENERATION["current"].swap_router.lower(),
            _BY_GENERATION["legacy"].swap_router.lower(),
        }
        by_target = _selectors_by_target(_discover(["SWAP"]))
        for router in _CL_ROUTERS_BASE:
            assert router in by_target, (
                "SWAP manifest must authorise every reviewed Slipstream CL SwapRouter "
                f"({router}); got targets {sorted(by_target)} — a manifest missing a "
                "router reverts every Safe-path slipstream swap on that generation's pools"
            )
            assert by_target[router] == {_CL_EXACT_INPUT_SINGLE_SELECTOR}, (
                "CL SwapRouter selectors for SWAP-only discovery must be exactly "
                f"exactInputSingle ({_CL_EXACT_INPUT_SINGLE_SELECTOR}); got "
                f"{sorted(by_target[router])}"
            )

    def test_swap_manifest_contains_erc20_approve(self) -> None:
        by_target = _selectors_by_target(_discover(["SWAP"]))
        approve_targets = {
            target
            for target, selectors in by_target.items()
            if _ERC20_APPROVE_SELECTOR in selectors
        }
        assert approve_targets, (
            "SWAP manifest must authorise an ERC-20 approve "
            f"({_ERC20_APPROVE_SELECTOR}) for the CL SwapRouter spender; got "
            f"{ {t: sorted(s) for t, s in by_target.items()} }"
        )
        assert approve_targets.isdisjoint(_CL_ROUTERS_BASE), (
            "approve() must be authorised on the token contract, never on a "
            "router target itself"
        )

    def test_swap_manifest_authorises_classic_fallback_router(self) -> None:
        """The Classic router route must be authorised too (VIB-5990).

        ``compile_swap_aerodrome`` routes on POOL EXISTENCE, not on the
        protocol slug: the auto ladder probes CL across the candidate tick
        spacings and, finding none, falls back to the Classic router
        (``swapExactTokensForTokens``). So a slipstream-slug swap on a pair
        with no CL pool emits Classic-router calldata. If the manifest only
        authorises the CL route, that swap reverts unauthorized at
        ``execTransactionWithRole`` — the same failure this ticket fixed,
        merely narrowed to long-tail pairs. Offline discovery cannot observe
        the fallback branch (``_aerodrome_is_offline`` short-circuits to
        CL@100), so ``build_discovery_vectors`` emits BOTH route shapes and
        the manifest is their union.
        """
        by_target = _selectors_by_target(_discover(["SWAP"]))
        assert _ROUTER_BASE in by_target, (
            "SWAP manifest must authorise the Classic router target "
            f"{_ROUTER_BASE} — the compiler's auto-fallback route; got "
            f"{sorted(by_target)}"
        )
        assert _CLASSIC_SWAP_SELECTOR in by_target[_ROUTER_BASE], (
            "Classic router must be authorised for "
            f"{_CLASSIC_SWAP_SELECTOR} (swapExactTokensForTokens); got "
            f"{sorted(by_target[_ROUTER_BASE])}"
        )

    def test_swap_synthetic_builder_emits_both_route_shapes(self) -> None:
        """Both discovery vectors are load-bearing — pin the count and shape.

        Guards against a future 'simplification' that drops the
        ``classic=True`` vector: without it the Classic-router permission
        silently disappears from the manifest and the fallback swap reverts.
        """
        intents = build_synthetic_intents("aerodrome_slipstream", "SWAP", "base")
        assert len(intents) == 2, (
            "Expected exactly two SWAP discovery vectors (CL auto + explicit "
            f"Classic); got {len(intents)}"
        )
        classic_flags = sorted(
            bool((getattr(i, "swap_params", None) or {}).get("classic"))
            for i in intents
        )
        assert classic_flags == [False, True], (
            "One vector must leave routing to the auto ladder (CL) and the "
            "other must pin ``classic=True``; got swap_params="
            f"{[getattr(i, 'swap_params', None) for i in intents]}"
        )

    def test_swap_manifest_excludes_npm_target(self) -> None:
        """Least privilege: a SWAP-only manifest must not authorise the LP NPM."""
        by_target = _selectors_by_target(_discover(["SWAP"]))
        assert _NPM_TARGETS.isdisjoint(by_target), (
            "SWAP-only manifest must NOT include any Slipstream NPM target "
            "(LP over-permissioning)"
        )

    def test_lp_manifest_excludes_cl_router_target(self) -> None:
        """Least privilege: LP-only manifests must not authorise the SwapRouter."""
        by_target = _selectors_by_target(
            _discover(["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"])
        )
        assert _CL_ROUTERS_BASE.isdisjoint(by_target), (
            "LP-only manifest must NOT include any CL SwapRouter target "
            "(swap over-permissioning)"
        )

    def test_swap_synthetic_builder_non_empty(self) -> None:
        intents = build_synthetic_intents("aerodrome_slipstream", "SWAP", "base")
        assert intents, (
            "Synthetic builder must emit at least one intent for "
            "aerodrome_slipstream/SWAP/base — the connector-owned "
            "build_discovery_vectors override (VIB-5990) is not firing"
        )

    def test_swap_synthetic_builder_empty_on_optimism(self) -> None:
        """Velodrome on Optimism is Classic-only (no reviewed Slipstream
        generation), so the slipstream slug must emit no SWAP synthetic there."""
        assert build_synthetic_intents("aerodrome_slipstream", "SWAP", "optimism") == []

    def test_matrix_includes_slipstream_swap(self) -> None:
        matrix = get_protocol_intent_matrix()
        intent_values = {it.value for it in matrix.get("aerodrome_slipstream", frozenset())}
        assert "SWAP" in intent_values, (
            f"aerodrome_slipstream matrix must include SWAP; got {sorted(intent_values)}"
        )
