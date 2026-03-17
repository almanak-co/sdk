"""Unit tests for protocol alias resolution.

Tests normalize_protocol() and display_protocol() from the protocol_aliases module,
and verifies that IntentCompiler._resolve_protocol() correctly normalizes aliases
during compilation.
"""

from decimal import Decimal

import pytest

from almanak import IntentCompiler, IntentCompilerConfig, SwapIntent
from almanak.framework.connectors.protocol_aliases import (
    PROTOCOL_ALIASES,
    PROTOCOL_DISPLAY_NAMES,
    display_protocol,
    normalize_protocol,
)


class TestNormalizeProtocol:
    """Tests for normalize_protocol() function."""

    def test_agni_resolves_to_uniswap_v3_on_mantle(self):
        """Agni alias should resolve to uniswap_v3 on Mantle."""
        assert normalize_protocol("mantle", "agni") == "uniswap_v3"

    def test_agni_finance_resolves_to_uniswap_v3_on_mantle(self):
        """agni_finance alias should also resolve to uniswap_v3 on Mantle."""
        assert normalize_protocol("mantle", "agni_finance") == "uniswap_v3"

    def test_alias_is_chain_scoped(self):
        """Agni alias should NOT resolve on non-Mantle chains."""
        assert normalize_protocol("arbitrum", "agni") == "agni"
        assert normalize_protocol("ethereum", "agni") == "agni"
        assert normalize_protocol("base", "agni") == "agni"

    def test_canonical_protocol_passes_through(self):
        """Already-canonical protocol names should pass through unchanged."""
        assert normalize_protocol("mantle", "uniswap_v3") == "uniswap_v3"
        assert normalize_protocol("arbitrum", "uniswap_v3") == "uniswap_v3"
        assert normalize_protocol("base", "aerodrome") == "aerodrome"

    def test_unknown_protocol_passes_through(self):
        """Unknown protocols should pass through as-is (lowercased)."""
        assert normalize_protocol("mantle", "unknown_protocol") == "unknown_protocol"

    def test_case_insensitive(self):
        """Alias lookup should be case-insensitive."""
        assert normalize_protocol("Mantle", "AGNI") == "uniswap_v3"
        assert normalize_protocol("MANTLE", "Agni") == "uniswap_v3"
        assert normalize_protocol("mantle", "Agni_Finance") == "uniswap_v3"

    def test_chain_enum_string(self):
        """Should accept Chain enum-style strings."""
        assert normalize_protocol("Chain.MANTLE", "agni") == "agni"  # str(Chain.MANTLE) != "mantle"
        # But lowercased chain names work
        assert normalize_protocol("mantle", "agni") == "uniswap_v3"

    def test_idempotent(self):
        """Calling normalize on an already-normalized value should be a no-op."""
        result = normalize_protocol("mantle", "agni")
        assert result == "uniswap_v3"
        result2 = normalize_protocol("mantle", result)
        assert result2 == "uniswap_v3"


class TestDisplayProtocol:
    """Tests for display_protocol() function."""

    def test_display_name_for_uniswap_v3_on_mantle(self):
        """uniswap_v3 on Mantle should display as 'Agni Finance'."""
        assert display_protocol("mantle", "uniswap_v3") == "Agni Finance"

    def test_display_name_via_alias(self):
        """Passing an alias should also resolve to display name."""
        assert display_protocol("mantle", "agni") == "Agni Finance"

    def test_fallback_to_canonical_key(self):
        """No display name registered -> falls back to canonical key."""
        assert display_protocol("arbitrum", "uniswap_v3") == "uniswap_v3"
        assert display_protocol("base", "aerodrome") == "aerodrome"

    def test_unknown_protocol_fallback(self):
        """Unknown protocols should return the lowercased key."""
        assert display_protocol("mantle", "unknown_proto") == "unknown_proto"


class TestRegistryConsistency:
    """Verify internal consistency of the alias and display name registries."""

    def test_every_alias_has_a_canonical_target(self):
        """Every alias should map to a non-empty canonical key."""
        for (chain, alias), canonical in PROTOCOL_ALIASES.items():
            assert canonical, f"Alias ({chain}, {alias}) maps to empty canonical key"
            assert isinstance(canonical, str)

    def test_display_names_use_canonical_keys(self):
        """Display name keys should use canonical protocol keys, not aliases."""
        alias_keys = set(PROTOCOL_ALIASES.keys())
        for (chain, protocol), _display_name in PROTOCOL_DISPLAY_NAMES.items():
            assert (chain, protocol) not in alias_keys, (
                f"Display name ({chain}, {protocol}) uses an alias instead of canonical key"
            )
            assert normalize_protocol(chain, protocol) == protocol, (
                f"Display name ({chain}, {protocol}) should be canonical (not an alias)"
            )


class TestCompilerResolveProtocol:
    """Test IntentCompiler._resolve_protocol() integration."""

    @pytest.fixture()
    def mantle_compiler(self):
        return IntentCompiler(
            chain="mantle",
            price_oracle={"MNT": Decimal("0.80"), "USDT": Decimal("1")},
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

    def test_resolve_agni_to_uniswap_v3(self, mantle_compiler):
        """Compiler should resolve 'agni' to 'uniswap_v3' on mantle."""
        assert mantle_compiler._resolve_protocol("agni") == "uniswap_v3"

    def test_resolve_none_returns_default(self, mantle_compiler):
        """None protocol should return the compiler's default_protocol."""
        result = mantle_compiler._resolve_protocol(None)
        assert result == mantle_compiler.default_protocol

    def test_resolve_canonical_unchanged(self, mantle_compiler):
        """Canonical protocol name should pass through unchanged."""
        assert mantle_compiler._resolve_protocol("uniswap_v3") == "uniswap_v3"

    def test_default_protocol_normalized_in_init(self):
        """When default_protocol='agni' on mantle, __init__ should normalize to uniswap_v3."""
        compiler = IntentCompiler(
            chain="mantle",
            default_protocol="agni",
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        assert compiler.default_protocol == "uniswap_v3"


class TestWrappedNativeMNT:
    """Verify WMNT -> MNT price alias resolution."""

    def test_wmnt_resolves_to_mnt_price(self):
        """WMNT should use MNT price when only MNT is in the oracle."""
        compiler = IntentCompiler(
            chain="mantle",
            price_oracle={"MNT": Decimal("0.80"), "USDT": Decimal("1")},
            config=IntentCompilerConfig(allow_placeholder_prices=False),
        )
        price = compiler._require_token_price("WMNT")
        assert price == Decimal("0.80")

    def test_mnt_and_wmnt_in_placeholder_prices(self):
        """Placeholder prices should include MNT and WMNT."""
        compiler = IntentCompiler(
            chain="mantle",
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        prices = compiler._get_placeholder_prices()
        assert "MNT" in prices
        assert "WMNT" in prices
        assert prices["MNT"] == prices["WMNT"]
