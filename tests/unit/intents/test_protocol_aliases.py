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

    def test_velodrome_resolves_to_aerodrome_on_optimism(self):
        """Velodrome alias should resolve to aerodrome on Optimism."""
        assert normalize_protocol("optimism", "velodrome") == "aerodrome"

    def test_velodrome_alias_is_chain_scoped(self):
        """Velodrome alias should NOT resolve on non-Optimism chains."""
        assert normalize_protocol("base", "velodrome") == "velodrome"
        assert normalize_protocol("arbitrum", "velodrome") == "velodrome"

    def test_aerodrome_canonical_on_optimism(self):
        """aerodrome should remain canonical on Optimism (not re-aliased)."""
        assert normalize_protocol("optimism", "aerodrome") == "aerodrome"

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

    def test_display_name_for_velodrome_on_optimism(self):
        """aerodrome on Optimism should display as 'Velodrome V2'."""
        assert display_protocol("optimism", "aerodrome") == "Velodrome V2"

    def test_display_name_via_velodrome_alias(self):
        """velodrome alias on Optimism should display as 'Velodrome V2'."""
        assert display_protocol("optimism", "velodrome") == "Velodrome V2"

    def test_fallback_to_canonical_key(self):
        """No display name registered -> falls back to canonical key."""
        assert display_protocol("arbitrum", "uniswap_v3") == "uniswap_v3"

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


class TestVelodromeCompilerIntegration:
    """Test Velodrome/Aerodrome compiler routing on Optimism."""

    @pytest.fixture()
    def optimism_compiler(self):
        return IntentCompiler(
            chain="optimism",
            price_oracle={"ETH": Decimal("3000"), "USDC": Decimal("1"), "WETH": Decimal("3000")},
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

    @pytest.fixture()
    def base_compiler(self):
        return IntentCompiler(
            chain="base",
            price_oracle={"ETH": Decimal("3000"), "USDC": Decimal("1"), "WETH": Decimal("3000")},
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

    def test_resolve_velodrome_to_aerodrome_on_optimism(self, optimism_compiler):
        """Compiler should resolve 'velodrome' to 'aerodrome' on Optimism."""
        assert optimism_compiler._resolve_protocol("velodrome") == "aerodrome"

    def test_aerodrome_swap_compiles_on_optimism(self, optimism_compiler):
        """Aerodrome swap should compile on Optimism (not blocked by Base-only check)."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            protocol="aerodrome",
        )
        result = optimism_compiler.compile(intent)
        assert result.status.value == "SUCCESS", f"Expected SUCCESS but got {result.status}: {result.error}"

    def test_velodrome_swap_routes_to_aerodrome_path(self, optimism_compiler):
        """protocol='velodrome' should route through Aerodrome swap path on Optimism."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            protocol="velodrome",
        )
        result = optimism_compiler.compile(intent)
        assert result.status.value == "SUCCESS", f"Expected SUCCESS but got {result.status}: {result.error}"

    def test_aerodrome_swap_unsupported_chain_fails(self):
        """Aerodrome swap should fail on chains without AERODROME addresses."""
        compiler = IntentCompiler(
            chain="ethereum",
            price_oracle={"ETH": Decimal("3000"), "USDC": Decimal("1"), "WETH": Decimal("3000")},
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            protocol="aerodrome",
        )
        result = compiler.compile(intent)
        assert result.status.value == "FAILED"
        assert "not supported on ethereum" in (result.error or "").lower()

    def test_base_aerodrome_defaults_to_cl(self, base_compiler):
        """On Base, Aerodrome swap should default to CL routing (has cl_router)."""
        from almanak.core.contracts import AERODROME as AERODROME_ADDRESSES

        chain_addrs = AERODROME_ADDRESSES["base"]
        assert "cl_router" in chain_addrs
        assert "cl_factory" in chain_addrs

    def test_optimism_velodrome_defaults_to_classic(self):
        """On Optimism, Velodrome should default to classic routing (no cl_router)."""
        from almanak.core.contracts import AERODROME as AERODROME_ADDRESSES

        chain_addrs = AERODROME_ADDRESSES["optimism"]
        assert "cl_router" not in chain_addrs
        assert "cl_factory" not in chain_addrs

    def test_optimism_compiles_with_classic_default(self):
        """Optimism compilation should succeed using classic (non-CL) routing."""
        compiler = IntentCompiler(
            chain="optimism",
            price_oracle={"ETH": Decimal("3000"), "USDC": Decimal("1"), "WETH": Decimal("3000")},
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            protocol="aerodrome",
        )
        result = compiler.compile(intent)
        assert result.status.value == "SUCCESS", f"Expected SUCCESS but got {result.status}: {result.error}"


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
