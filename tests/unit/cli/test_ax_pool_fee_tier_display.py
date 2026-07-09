"""Tests for ``ax pool`` fee-tier display logic (Plan 027 Step 3).

The ``ax pool`` command computes a title suffix that depends on protocol-family
membership:
- Protocols in ``TICK_SPACING_FEE_DISPLAY`` (aerodrome_slipstream): ``tick_spacing=<N>``
- All other protocols (e.g. uniswap_v3): ``<N/10000:.2f>%``

These tests pin the registry-based dispatch that replaced the old
``if protocol == "aerodrome_slipstream":`` literal.  They directly exercise the
dispatch logic that ``ax pool`` executes after collecting pool state, without
needing to invoke the full Click command tree.
"""

from __future__ import annotations

from almanak.framework.cli.ax import _pool_title_suffix as _compute_title_suffix


class TestPoolFeeTierDisplayTitle:
    """Fee-tier title suffix uses registry-derived family membership."""

    def test_aerodrome_slipstream_uses_tick_spacing_suffix(self) -> None:
        """aerodrome_slipstream is in TICK_SPACING_FEE_DISPLAY -> tick_spacing=<N>."""
        suffix = _compute_title_suffix("aerodrome_slipstream", fee_tier=100)
        assert suffix == "tick_spacing=100"

    def test_aerodrome_slipstream_different_tick_spacing(self) -> None:
        """tick_spacing value is rendered verbatim (200 tick spacing)."""
        suffix = _compute_title_suffix("aerodrome_slipstream", fee_tier=200)
        assert suffix == "tick_spacing=200"

    def test_aerodrome_slipstream_alias_normalized_before_membership(self) -> None:
        """Hyphen/space/case CLI aliases normalize to the registry key before the
        family membership check."""
        for alias in ("aerodrome-slipstream", "Aerodrome Slipstream", "AERODROME_SLIPSTREAM"):
            assert _compute_title_suffix(alias, fee_tier=100) == "tick_spacing=100"

    def test_aerodrome_manifest_alias_shares_family_membership(self) -> None:
        """``aerodrome`` (the pool-reader manifest's canonical key) is
        widened through the manifest key set to its sibling key
        ``aerodrome_slipstream``, which carries TICK_SPACING_FEE_DISPLAY."""
        assert _compute_title_suffix("aerodrome", fee_tier=100) == "tick_spacing=100"
        assert _compute_title_suffix("Aerodrome", fee_tier=200) == "tick_spacing=200"

    def test_uniswap_v3_uses_percentage_suffix(self) -> None:
        """uniswap_v3 is NOT in TICK_SPACING_FEE_DISPLAY -> percent format."""
        suffix = _compute_title_suffix("uniswap_v3", fee_tier=3000)
        assert suffix == "0.30%"

    def test_uniswap_v3_five_bps(self) -> None:
        """Standard 0.05% (500 bps) uniswap_v3 pool."""
        suffix = _compute_title_suffix("uniswap_v3", fee_tier=500)
        assert suffix == "0.05%"

    def test_velodrome_slipstream_uses_percentage_suffix(self) -> None:
        """velodrome_slipstream is NOT in TICK_SPACING_FEE_DISPLAY (strict parity
        with old ``if protocol == 'aerodrome_slipstream':`` literal guard)."""
        suffix = _compute_title_suffix("velodrome_slipstream", fee_tier=200)
        # velodrome_slipstream deliberately excluded from the family -- old literal
        # only covered aerodrome_slipstream. Exact assertion catches format/math
        # regressions, not just family membership.
        assert suffix == "0.02%"

    def test_unknown_protocol_uses_percentage_suffix(self) -> None:
        """Any protocol not in the family falls back to percentage rendering."""
        suffix = _compute_title_suffix("some_unknown_dex", fee_tier=3000)
        assert suffix == "0.30%"
