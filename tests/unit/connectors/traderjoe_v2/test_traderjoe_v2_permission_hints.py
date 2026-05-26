"""Tests for TraderJoe V2 permission hints.

Covers static permission generation for LBRouter + per-pair LBPair
selectors (approveForAll for LP_CLOSE, collectFees for LP_COLLECT_FEES).
"""

from almanak.connectors.traderjoe_v2.permission_hints import (
    PERMISSION_HINTS,
    _build_static_permissions,
)
from almanak.framework.permissions.hints import PermissionHints, StaticPermissionEntry

# Selectors mirrored from permission_hints._build_static_permissions for
# direct assertions (kept private in the module).
_ADD_LIQUIDITY = "0xa3c7271a"
_REMOVE_LIQUIDITY = "0xc22159b6"
_SWAP_EXACT_TOKENS = "0x2a443fae"
_APPROVE_FOR_ALL = "0xe584b654"
_COLLECT_FEES = "0x225b20b9"


class TestPermissionHintsModule:
    """Top-level PermissionHints object exposed by the connector."""

    def test_permission_hints_is_instance(self) -> None:
        assert isinstance(PERMISSION_HINTS, PermissionHints)

    def test_supports_standalone_fee_collection_true(self) -> None:
        assert PERMISSION_HINTS.supports_standalone_fee_collection is True

    def test_selector_labels_present(self) -> None:
        labels = PERMISSION_HINTS.selector_labels
        # All five selectors must carry a human-readable label.
        for selector in (
            _ADD_LIQUIDITY,
            _REMOVE_LIQUIDITY,
            _SWAP_EXACT_TOKENS,
            _APPROVE_FOR_ALL,
            _COLLECT_FEES,
        ):
            assert selector in labels
            assert labels[selector]  # non-empty

    def test_static_permissions_keyed_by_chain(self) -> None:
        assert isinstance(PERMISSION_HINTS.static_permissions, dict)


class TestBuildStaticPermissions:
    """Tests for the per-chain registry generation."""

    def test_returns_dict(self) -> None:
        result = _build_static_permissions()
        assert isinstance(result, dict)

    def test_contains_known_chains(self) -> None:
        # avalanche has a registered LBPair, so it MUST appear.
        result = _build_static_permissions()
        assert "avalanche" in result

    def test_avalanche_router_entry(self) -> None:
        """The first entry on avalanche must be the LBRouter with LP selectors only."""
        result = _build_static_permissions()
        avax = result["avalanche"]
        # Router entry is the first item per build order
        router_entry = avax[0]
        assert isinstance(router_entry, StaticPermissionEntry)
        assert router_entry.label == "TraderJoe V2 LBRouter"
        # Only the two LP selectors live on the router; SWAP selector is intentionally
        # absent so SWAP-only manifests stay least-privilege.
        assert _ADD_LIQUIDITY in router_entry.selectors
        assert _REMOVE_LIQUIDITY in router_entry.selectors
        assert _SWAP_EXACT_TOKENS not in router_entry.selectors
        assert _APPROVE_FOR_ALL not in router_entry.selectors

    def test_avalanche_lbpair_entries_present(self) -> None:
        """Per-pair entries must surface approveForAll AND collectFees independently."""
        result = _build_static_permissions()
        avax = result["avalanche"]
        # Skip router; remaining are per-pair entries (approveForAll + collectFees per pair).
        pair_entries = avax[1:]
        # At least one registered pair on avalanche → at least 2 entries (one per selector).
        assert len(pair_entries) >= 2
        approve_entries = [e for e in pair_entries if _APPROVE_FOR_ALL in e.selectors]
        collect_entries = [e for e in pair_entries if _COLLECT_FEES in e.selectors]
        assert approve_entries, "approveForAll entry missing"
        assert collect_entries, "collectFees entry missing"

    def test_lbpair_target_lowercased(self) -> None:
        """Permissions must be registered against lowercase addresses."""
        result = _build_static_permissions()
        for entries in result.values():
            for entry in entries:
                assert entry.target == entry.target.lower(), (
                    f"target {entry.target} not lowercased"
                )

    def test_approve_for_all_scoped_to_lp_close(self) -> None:
        result = _build_static_permissions()
        avax = result["avalanche"]
        approve_entries = [e for e in avax if _APPROVE_FOR_ALL in e.selectors]
        for entry in approve_entries:
            # Scoped strictly to LP_CLOSE — keeps SWAP/LP_OPEN manifests at least-privilege
            assert entry.intent_types == frozenset({"LP_CLOSE"})

    def test_collect_fees_scoped_to_lp_collect_fees(self) -> None:
        result = _build_static_permissions()
        avax = result["avalanche"]
        collect_entries = [e for e in avax if _COLLECT_FEES in e.selectors]
        for entry in collect_entries:
            assert entry.intent_types == frozenset({"LP_COLLECT_FEES"})

    def test_pair_label_includes_token_pair_and_bin_step(self) -> None:
        """Labels must be self-describing: tokenX/tokenY/binStep format."""
        result = _build_static_permissions()
        avax = result["avalanche"]
        pair_entries = [e for e in avax if e.label != "TraderJoe V2 LBRouter"]
        # We registered WAVAX/USDC/20 in TRADERJOE_V2_LBPAIRS.
        assert any("WAVAX" in e.label and "USDC" in e.label and "20" in e.label for e in pair_entries)


class TestPermissionHintsConsistency:
    """End-to-end consistency: PERMISSION_HINTS reflects _build_static_permissions output."""

    def test_static_permissions_match_builder(self) -> None:
        builder_output = _build_static_permissions()
        # The module-level PERMISSION_HINTS uses the builder verbatim.
        for chain, entries in builder_output.items():
            assert chain in PERMISSION_HINTS.static_permissions
            module_entries = PERMISSION_HINTS.static_permissions[chain]
            assert len(module_entries) == len(entries)
