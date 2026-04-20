"""Tests for deprecation warnings on legacy token APIs.

Verifies that:
- TokenRegistry.__init__() emits DeprecationWarning
- get_default_registry() emits DeprecationWarning
- TOKEN_ADDRESSES has been removed from compiler.py (tested in test_compiler_token_resolver.py)
- Warnings include migration instructions pointing to get_token_resolver()
"""

import warnings

from almanak.framework.data.tokens.registry import TokenRegistry


class TestTokenRegistryDeprecation:
    """Test deprecation warnings for TokenRegistry instantiation."""

    def setup_method(self) -> None:
        """Reset the deprecation warning flag before each test."""
        TokenRegistry._warned = False

    def teardown_method(self) -> None:
        """Reset the deprecation warning flag after each test."""
        TokenRegistry._warned = False

    def test_token_registry_init_emits_warning(self) -> None:
        """Verify TokenRegistry() emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _ = TokenRegistry()

            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "TokenRegistry is deprecated" in str(w[0].message)
            assert "get_token_resolver()" in str(w[0].message)

    def test_token_registry_warning_includes_migration_instructions(self) -> None:
        """Verify warning message contains migration path."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _ = TokenRegistry()

            msg = str(w[0].message)
            assert "get_token_resolver()" in msg
            assert "almanak.framework.data.tokens" in msg

    def test_token_registry_warning_only_once(self) -> None:
        """Verify deprecation warning is only emitted once per session."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _ = TokenRegistry()
            _ = TokenRegistry()
            _ = TokenRegistry()

            # Only one warning despite multiple instantiations
            deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(deprecation_warnings) == 1

    def test_token_registry_still_functional(self) -> None:
        """Verify TokenRegistry still works after deprecation warning."""
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            registry = TokenRegistry()

            # Should still be functional
            assert len(registry) == 0
            assert not registry.has("USDC")
            assert "USDC" not in registry


class TestGetDefaultRegistryDeprecation:
    """Test deprecation warnings for get_default_registry()."""

    def test_get_default_registry_emits_warning(self) -> None:
        """Verify get_default_registry() emits DeprecationWarning."""
        # Reset TokenRegistry._warned so we only see the get_default_registry warning
        TokenRegistry._warned = True  # Suppress the TokenRegistry warning

        from almanak.framework.data.tokens import get_default_registry

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            registry = get_default_registry()

            # Should get the get_default_registry deprecation warning
            deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(deprecation_warnings) >= 1
            assert any("get_default_registry() is deprecated" in str(x.message) for x in deprecation_warnings)

            # Registry should still be functional
            assert registry is not None
            assert len(registry) > 0

    def test_get_default_registry_warning_includes_migration(self) -> None:
        """Verify get_default_registry() warning contains migration path."""
        TokenRegistry._warned = True  # Suppress the TokenRegistry warning

        from almanak.framework.data.tokens import get_default_registry

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _ = get_default_registry()

            deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            msg = str(deprecation_warnings[0].message)
            assert "get_token_resolver()" in msg
            assert "almanak.framework.data.tokens" in msg
