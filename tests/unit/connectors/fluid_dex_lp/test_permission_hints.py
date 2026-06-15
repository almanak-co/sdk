"""Unit tests for Fluid DEX LP (SmartLending) synthetic permission discovery.

``fluid_dex_lp`` joins the synthetic Zodiac discovery matrix (VIB-5125) via
STATIC permissions rather than a compilation-based ``build_discovery_vectors``:
its compile path is RPC-bound (the 51013 deposit pre-flight + the live
close-balance read), so the manifest selectors are pinned (the TraderJoe V2
precedent). These tests pin that static surface — every non-native wrapper's
``deposit`` (LP_OPEN) / ``withdraw`` (LP_CLOSE) selector, the per-leg ERC-20
``approve`` (LP_OPEN, spender=wrapper), the native-leg exclusion, and the
least-privilege intent-type scoping — and assert the discovery driver emits the
exact ``(target, selector)`` triples for fluid_dex_lp.
"""

from __future__ import annotations

from almanak.connectors.fluid.addresses import FLUID_DEX_LP_NATIVE_SENTINEL, FLUID_SMARTLENDING_MARKETS
from almanak.connectors.fluid_dex_lp.permission_hints import (
    _DEPOSIT_SELECTOR,
    _ERC20_APPROVE_SELECTOR,
    _WITHDRAW_SELECTOR,
    PERMISSION_HINTS,
)
from almanak.framework.permissions.discovery import discover_permissions
from almanak.framework.permissions.synthetic_intents import _lp_protocols, get_protocol_intent_matrix

_CHAIN = "arbitrum"


def _arbitrum_wrappers() -> dict[str, dict]:
    return FLUID_SMARTLENDING_MARKETS[_CHAIN]


def _non_native_wrappers() -> dict[str, dict]:
    return {
        addr: e
        for addr, e in _arbitrum_wrappers().items()
        if not (
            bool(e.get("native_token1"))
            or str(e.get("token0", "")).lower() == FLUID_DEX_LP_NATIVE_SENTINEL.lower()
        )
    }


class TestSyntheticMembership:
    def test_declares_lp_open_and_close(self):
        assert PERMISSION_HINTS.synthetic_discovery_intents == frozenset({"LP_OPEN", "LP_CLOSE"})

    def test_member_of_lp_protocols(self):
        assert "fluid_dex_lp" in _lp_protocols()

    def test_matrix_has_lp_pair_only(self):
        matrix = get_protocol_intent_matrix()
        types = {it.value for it in matrix.get("fluid_dex_lp", frozenset())}
        assert types == {"LP_OPEN", "LP_CLOSE"}

    def test_no_collect_fees(self):
        # Fungible LP: fees auto-compound; no standalone fee collection.
        assert PERMISSION_HINTS.supports_standalone_fee_collection is False


class TestStaticPermissionSurface:
    def test_arbitrum_present(self):
        assert _CHAIN in PERMISSION_HINTS.static_permissions

    def test_no_native_leg_wrappers_in_surface(self):
        targets = {e.target for e in PERMISSION_HINTS.static_permissions[_CHAIN]}
        # fSL5 (FLUID / native-ETH) is refused at compile (VIB-5121) — it must
        # not appear as a wrapper target in the discovery surface.
        for addr, entry in _arbitrum_wrappers().items():
            is_native = bool(entry.get("native_token1")) or (
                str(entry.get("token0", "")).lower() == FLUID_DEX_LP_NATIVE_SENTINEL.lower()
            )
            if is_native:
                assert addr.lower() not in targets, f"native-leg wrapper {addr} must be excluded"

    def test_native_sentinel_never_an_approve_target(self):
        targets = {e.target for e in PERMISSION_HINTS.static_permissions[_CHAIN]}
        assert FLUID_DEX_LP_NATIVE_SENTINEL.lower() not in targets

    def test_deposit_scoped_to_lp_open(self):
        for entry in PERMISSION_HINTS.static_permissions[_CHAIN]:
            if _DEPOSIT_SELECTOR in entry.selectors:
                assert entry.intent_types == frozenset({"LP_OPEN"})

    def test_withdraw_scoped_to_lp_close(self):
        for entry in PERMISSION_HINTS.static_permissions[_CHAIN]:
            if _WITHDRAW_SELECTOR in entry.selectors:
                assert entry.intent_types == frozenset({"LP_CLOSE"})

    def test_approve_scoped_to_lp_open(self):
        for entry in PERMISSION_HINTS.static_permissions[_CHAIN]:
            if _ERC20_APPROVE_SELECTOR in entry.selectors:
                assert entry.intent_types == frozenset({"LP_OPEN"})

    def test_every_non_native_wrapper_has_deposit_and_withdraw(self):
        deposit_targets = {
            e.target for e in PERMISSION_HINTS.static_permissions[_CHAIN] if _DEPOSIT_SELECTOR in e.selectors
        }
        withdraw_targets = {
            e.target for e in PERMISSION_HINTS.static_permissions[_CHAIN] if _WITHDRAW_SELECTOR in e.selectors
        }
        for addr in _non_native_wrappers():
            assert addr.lower() in deposit_targets
            assert addr.lower() in withdraw_targets


class TestDiscoveryOutput:
    """End-to-end through the offline discovery driver (no RPC, deterministic)."""

    def test_lp_open_emits_approve_and_deposit(self):
        perms, warnings = discover_permissions(
            chain=_CHAIN, protocols=["fluid_dex_lp"], intent_types=["LP_OPEN"], rpc_url=None
        )
        assert not warnings
        by_target = {p.target: {f.selector for f in p.function_selectors} for p in perms}
        for addr, entry in _non_native_wrappers().items():
            assert _DEPOSIT_SELECTOR in by_target[addr.lower()]
            # No withdraw on an LP_OPEN-only manifest (least privilege).
            assert _WITHDRAW_SELECTOR not in by_target[addr.lower()]
            for leg in ("token0", "token1"):
                leg_addr = str(entry.get(leg, "")).lower()
                if leg_addr and leg_addr != FLUID_DEX_LP_NATIVE_SENTINEL.lower():
                    assert _ERC20_APPROVE_SELECTOR in by_target[leg_addr]

    def test_lp_close_emits_withdraw_only(self):
        perms, warnings = discover_permissions(
            chain=_CHAIN, protocols=["fluid_dex_lp"], intent_types=["LP_CLOSE"], rpc_url=None
        )
        assert not warnings
        by_target = {p.target: {f.selector for f in p.function_selectors} for p in perms}
        for addr in _non_native_wrappers():
            assert _WITHDRAW_SELECTOR in by_target[addr.lower()]
            assert _DEPOSIT_SELECTOR not in by_target[addr.lower()]
        # No approve targets on a close-only manifest.
        assert _ERC20_APPROVE_SELECTOR not in {sel for sels in by_target.values() for sel in sels}

    def test_no_native_wrapper_in_discovery_output(self):
        perms, _ = discover_permissions(
            chain=_CHAIN, protocols=["fluid_dex_lp"], intent_types=["LP_OPEN", "LP_CLOSE"], rpc_url=None
        )
        targets = {p.target for p in perms}
        for addr, entry in _arbitrum_wrappers().items():
            is_native = bool(entry.get("native_token1")) or (
                str(entry.get("token0", "")).lower() == FLUID_DEX_LP_NATIVE_SENTINEL.lower()
            )
            if is_native:
                assert addr.lower() not in targets


class TestNativeRefusalCoupling:
    """Discovery exclusion and the compiler's native refusal share ONE predicate.

    Closes the prose-only-coupling concern: the native-leg test lives once in
    ``fluid.addresses.is_native_leg`` and is consumed by BOTH the discovery
    surface and ``FluidDexLpCompiler._refuse_native``. These tests enforce the
    agreement structurally so the surface cannot silently drift from the
    compiler (e.g. when VIB-5121 changes the native definition).
    """

    def test_discovery_uses_the_shared_predicate(self):
        # The permission-hints module imports the canonical helper, not a copy.
        from almanak.connectors.fluid.addresses import is_native_leg as canonical
        from almanak.connectors.fluid_dex_lp import permission_hints as ph

        assert ph.is_native_leg is canonical

    def test_compiler_refusal_matches_discovery_exclusion(self):
        # For every configured wrapper on every chain, "refused at compile"
        # (is_native_leg True) iff "excluded from the discovery surface".
        from almanak.connectors.fluid.addresses import is_native_leg

        for chain, wrappers in FLUID_SMARTLENDING_MARKETS.items():
            surface_targets = {e.target for e in PERMISSION_HINTS.static_permissions.get(chain, [])}
            for addr, entry in wrappers.items():
                if is_native_leg(entry):
                    assert addr.lower() not in surface_targets, (
                        f"{chain}:{addr} is native (compiler refuses) but appears in the discovery surface"
                    )
                else:
                    assert addr.lower() in surface_targets, (
                        f"{chain}:{addr} is non-native (compiler executes) but is missing from the surface"
                    )
