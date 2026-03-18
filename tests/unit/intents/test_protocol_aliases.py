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
    UNISWAP_V3_FORKS,
    display_protocol,
    is_uniswap_v3_fork,
    normalize_protocol,
)


class TestNormalizeProtocol:
    """Tests for normalize_protocol() function."""

    def test_agni_resolves_to_agni_finance_on_mantle(self):
        """Agni alias should resolve to agni_finance on Mantle."""
        assert normalize_protocol("mantle", "agni") == "agni_finance"

    def test_agni_finance_is_canonical_on_mantle(self):
        """agni_finance should be the canonical key on Mantle."""
        assert normalize_protocol("mantle", "agni_finance") == "agni_finance"

    def test_uniswap_v3_resolves_to_agni_finance_on_mantle(self):
        """uniswap_v3 on Mantle should resolve to agni_finance (the local V3 fork)."""
        assert normalize_protocol("mantle", "uniswap_v3") == "agni_finance"

    def test_uniswap_v3_canonical_on_other_chains(self):
        """uniswap_v3 should remain canonical on non-Mantle chains."""
        assert normalize_protocol("arbitrum", "uniswap_v3") == "uniswap_v3"
        assert normalize_protocol("ethereum", "uniswap_v3") == "uniswap_v3"
        assert normalize_protocol("base", "uniswap_v3") == "uniswap_v3"

    def test_alias_is_chain_scoped(self):
        """Agni alias should NOT resolve on non-Mantle chains."""
        assert normalize_protocol("arbitrum", "agni") == "agni"
        assert normalize_protocol("ethereum", "agni") == "agni"
        assert normalize_protocol("base", "agni") == "agni"

    def test_canonical_protocol_passes_through(self):
        """Already-canonical protocol names should pass through unchanged."""
        assert normalize_protocol("base", "aerodrome") == "aerodrome"

    def test_unknown_protocol_passes_through(self):
        """Unknown protocols should pass through as-is (lowercased)."""
        assert normalize_protocol("mantle", "unknown_protocol") == "unknown_protocol"

    def test_case_insensitive(self):
        """Alias lookup should be case-insensitive."""
        assert normalize_protocol("Mantle", "AGNI") == "agni_finance"
        assert normalize_protocol("MANTLE", "Agni") == "agni_finance"
        assert normalize_protocol("mantle", "Agni_Finance") == "agni_finance"
        assert normalize_protocol("Mantle", "UNISWAP_V3") == "agni_finance"

    def test_chain_enum_string(self):
        """Should accept Chain enum-style strings."""
        assert normalize_protocol("Chain.MANTLE", "agni") == "agni"  # str(Chain.MANTLE) != "mantle"
        # But lowercased chain names work
        assert normalize_protocol("mantle", "agni") == "agni_finance"

    def test_hyphen_to_underscore_normalization(self):
        """Hyphens in protocol names should be normalized to underscores (VIB-1463)."""
        assert normalize_protocol("base", "uniswap-v4") == "uniswap_v4"
        assert normalize_protocol("arbitrum", "uniswap-v3") == "uniswap_v3"
        assert normalize_protocol("base", "pancakeswap-v3") == "pancakeswap_v3"
        assert normalize_protocol("avalanche", "trader-joe-v2") == "traderjoe_v2"

    def test_idempotent(self):
        """Calling normalize on an already-normalized value should be a no-op."""
        result = normalize_protocol("mantle", "agni")
        assert result == "agni_finance"
        result2 = normalize_protocol("mantle", result)
        assert result2 == "agni_finance"


class TestDisplayProtocol:
    """Tests for display_protocol() function."""

    def test_display_name_for_agni_finance_on_mantle(self):
        """agni_finance on Mantle should display as 'Agni Finance'."""
        assert display_protocol("mantle", "agni_finance") == "Agni Finance"

    def test_display_name_via_uniswap_v3_alias_on_mantle(self):
        """uniswap_v3 on Mantle should display as 'Agni Finance'."""
        assert display_protocol("mantle", "uniswap_v3") == "Agni Finance"

    def test_display_name_via_agni_alias(self):
        """Passing an alias should also resolve to display name."""
        assert display_protocol("mantle", "agni") == "Agni Finance"

    def test_fallback_to_canonical_key(self):
        """No display name registered -> falls back to canonical key."""
        assert display_protocol("arbitrum", "uniswap_v3") == "uniswap_v3"
        assert display_protocol("base", "aerodrome") == "aerodrome"

    def test_unknown_protocol_fallback(self):
        """Unknown protocols should return the lowercased key."""
        assert display_protocol("mantle", "unknown_proto") == "unknown_proto"


class TestIsUniswapV3Fork:
    """Tests for is_uniswap_v3_fork() function."""

    def test_known_v3_forks(self):
        assert is_uniswap_v3_fork("uniswap_v3")
        assert is_uniswap_v3_fork("sushiswap_v3")
        assert is_uniswap_v3_fork("pancakeswap_v3")
        assert is_uniswap_v3_fork("agni_finance")

    def test_non_v3_protocols(self):
        assert not is_uniswap_v3_fork("aerodrome")
        assert not is_uniswap_v3_fork("curve")
        assert not is_uniswap_v3_fork("enso")

    def test_case_insensitive(self):
        assert is_uniswap_v3_fork("AGNI_FINANCE")
        assert is_uniswap_v3_fork("Uniswap_V3")


class TestRegistryConsistency:
    """Verify internal consistency of the alias and display name registries."""

    def test_every_alias_has_a_canonical_target(self):
        """Every alias should map to a non-empty canonical key."""
        for (chain, alias), canonical in PROTOCOL_ALIASES.items():
            assert canonical, f"Alias ({chain}, {alias}) maps to empty canonical key"
            assert isinstance(canonical, str)

    def test_display_names_use_canonical_keys(self):
        """Display name keys should use canonical protocol keys, not aliases."""
        for (chain, protocol), _display_name in PROTOCOL_DISPLAY_NAMES.items():
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

    def test_resolve_agni_to_agni_finance(self, mantle_compiler):
        """Compiler should resolve 'agni' to 'agni_finance' on mantle."""
        assert mantle_compiler._resolve_protocol("agni") == "agni_finance"

    def test_resolve_uniswap_v3_to_agni_finance(self, mantle_compiler):
        """Compiler should resolve 'uniswap_v3' to 'agni_finance' on mantle."""
        assert mantle_compiler._resolve_protocol("uniswap_v3") == "agni_finance"

    def test_resolve_none_returns_default(self, mantle_compiler):
        """None protocol should return the compiler's default_protocol."""
        result = mantle_compiler._resolve_protocol(None)
        assert result == mantle_compiler.default_protocol

    def test_resolve_canonical_unchanged(self, mantle_compiler):
        """Canonical protocol name should pass through unchanged."""
        assert mantle_compiler._resolve_protocol("agni_finance") == "agni_finance"

    def test_default_protocol_normalized_in_init(self):
        """When default_protocol='agni' on mantle, __init__ should normalize to agni_finance."""
        compiler = IntentCompiler(
            chain="mantle",
            default_protocol="agni",
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        assert compiler.default_protocol == "agni_finance"

    def test_default_protocol_uniswap_v3_normalized_on_mantle(self):
        """When default_protocol='uniswap_v3' on mantle, __init__ should normalize to agni_finance."""
        compiler = IntentCompiler(
            chain="mantle",
            default_protocol="uniswap_v3",
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        assert compiler.default_protocol == "agni_finance"


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
