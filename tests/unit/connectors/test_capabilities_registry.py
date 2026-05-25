"""Tests for ``CapabilitiesRegistry`` per-protocol lazy-load isolation.

Pins the contract that ``CapabilitiesRegistry.get(protocol)`` imports ONLY
the connector module that owns ``protocol`` -- a broken sibling connector
must not poison unrelated capability lookups.

These tests do NOT pop ``sys.modules`` entries or call ``reset_cache``: doing
either would re-evaluate connector capability modules and orphan the
value-dict references held by ``vocabulary.PROTOCOL_CAPABILITIES`` (cached in
``vocabulary.globals()`` on first access) and by other test modules that
imported the symbol at collection time. Tests that need an entry to be
absent from the registry pop it locally with `_aggregated.pop(...)` and
restore in ``finally`` so the global identity contract stays intact.
"""

from __future__ import annotations

import importlib
from typing import Any
from unittest.mock import patch

from almanak.framework.connectors.capabilities_registry import (
    CapabilitiesRegistry,
    get_protocol_capabilities,
)


def _pop_cached(protocol: str) -> dict[str, Any] | None:
    """Remove ``protocol`` from the registry's aggregated cache, returning it."""
    return CapabilitiesRegistry._aggregated.pop(protocol, None)


def _restore_cached(protocol: str, value: dict[str, Any] | None) -> None:
    if value is not None:
        CapabilitiesRegistry._aggregated[protocol] = value


class TestPerProtocolLazyLoad:
    """``get()`` must import only the requested protocol's module."""

    def test_get_imports_only_requested_module(self) -> None:
        """`get('aave_v3')` must not import unrelated connector modules.

        Counts calls to ``importlib.import_module`` filtered to
        ``*.capabilities`` paths. ``import_module`` is invoked even when
        ``sys.modules`` already has the entry cached, so the spy reflects
        what the registry asked for regardless of process-level import state.
        """
        cached = _pop_cached("aave_v3")
        try:
            with patch.object(
                importlib, "import_module", wraps=importlib.import_module
            ) as spy:
                caps = CapabilitiesRegistry.get("aave_v3")

            assert caps is not None
            assert "operations" in caps
            imported_capability_modules = sorted(
                {
                    call.args[0]
                    for call in spy.call_args_list
                    if call.args and call.args[0].endswith(".capabilities")
                }
            )
            assert imported_capability_modules == [
                "almanak.framework.connectors.aave_v3.capabilities"
            ], (
                "Expected only aave_v3.capabilities to be imported, "
                f"got {imported_capability_modules}"
            )
        finally:
            _restore_cached("aave_v3", cached)

    def test_broken_sibling_module_does_not_block_unrelated_lookup(self) -> None:
        """A broken sibling connector must not poison unrelated capability lookups."""
        cached = _pop_cached("aave_v3")
        original_import_module = importlib.import_module

        def selective_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "almanak.framework.connectors.polymarket.capabilities":
                raise ImportError("simulated broken sibling connector")
            return original_import_module(name, *args, **kwargs)

        try:
            with patch.object(importlib, "import_module", side_effect=selective_import):
                caps = CapabilitiesRegistry.get("aave_v3")

            assert caps is not None
            assert "operations" in caps
        finally:
            _restore_cached("aave_v3", cached)

    def test_alias_resolves_to_canonical_module(self) -> None:
        """`morpho` is an alias for `morpho_blue` and resolves via shared identity.

        The morpho_blue capability module deliberately aliases both keys to a
        single dict instance so that a mutation through either name is visible
        through the other (see ``connectors/morpho_blue/capabilities.py``).
        Asserting ``is`` rather than value-equality pins that contract -- a
        future refactor that splits the keys into separate-but-equal dicts
        would silently break the long-standing mutate-through-alias invariant.
        """
        morpho_caps = CapabilitiesRegistry.get("morpho")
        morpho_blue_caps = CapabilitiesRegistry.get("morpho_blue")
        assert morpho_caps is not None
        assert morpho_blue_caps is not None
        assert morpho_caps is morpho_blue_caps
        assert morpho_caps.get("requires_market_id") is True
        assert morpho_blue_caps.get("requires_market_id") is True

    def test_unknown_protocol_returns_none(self) -> None:
        assert CapabilitiesRegistry.get("does-not-exist") is None
        # get_protocol_capabilities returns {} (not None) for downstream `.get(...)` ergonomics.
        assert get_protocol_capabilities("does-not-exist") == {}

    def test_get_caches_per_protocol(self) -> None:
        """Repeated `get` for the same protocol invokes ``import_module`` exactly once."""
        cached = _pop_cached("aave_v3")
        try:
            with patch.object(
                importlib, "import_module", wraps=importlib.import_module
            ) as spy:
                first = CapabilitiesRegistry.get("aave_v3")
                second = CapabilitiesRegistry.get("aave_v3")

            assert first is second  # identity stable
            aave_imports = [
                call
                for call in spy.call_args_list
                if call.args
                and call.args[0] == "almanak.framework.connectors.aave_v3.capabilities"
            ]
            assert len(aave_imports) == 1
        finally:
            _restore_cached("aave_v3", cached)


class TestAllCapabilities:
    """``all_capabilities`` is the legitimate bulk-load entry point."""

    def test_all_capabilities_includes_every_registered_protocol(self) -> None:
        caps = CapabilitiesRegistry.all_capabilities()
        for protocol in CapabilitiesRegistry.supported_protocols():
            assert protocol in caps, f"missing aggregated entry for {protocol}"

    def test_all_capabilities_identity_is_stable(self) -> None:
        first = CapabilitiesRegistry.all_capabilities()
        second = CapabilitiesRegistry.all_capabilities()
        assert first is second

    def test_all_capabilities_picks_up_prior_get_results(self) -> None:
        """A prior `get(X)` populates the aggregated view -- no duplicate load."""
        single = CapabilitiesRegistry.get("aave_v3")
        aggregated = CapabilitiesRegistry.all_capabilities()
        assert aggregated["aave_v3"] is single


class TestMutationContract:
    """Value-dicts must be the connector module's own dicts so the long-standing
    monkey-patch test pattern in ``test_vocabulary.py`` keeps working."""

    def test_value_dict_identity_matches_connector_module(self) -> None:
        from almanak.framework.connectors.aave_v3 import capabilities as aave_caps_module

        caps = CapabilitiesRegistry.get("aave_v3")
        assert caps is aave_caps_module.PROTOCOL_CAPABILITIES["aave_v3"]


class TestResetCacheIdentityContract:
    """``reset_cache`` must NOT orphan the aggregated dict reference.

    ``vocabulary.PROTOCOL_CAPABILITIES`` and the package-level re-export in
    ``framework/intents/__init__.py`` cache the aggregated dict in their
    own ``globals()`` on first access (PEP 562 ``__getattr__``). If
    ``reset_cache`` rebinds ``_aggregated`` to a fresh dict, those cached
    references become stale and tests like
    ``test_vocabulary.py::test_rate_mode_not_in_valid_modes`` -- which
    mutate ``PROTOCOL_CAPABILITIES['aave_v3'][...]`` and expect the next
    validator call to see the mutation -- silently break.
    """

    def test_reset_cache_clears_in_place(self) -> None:
        before = CapabilitiesRegistry.all_capabilities()
        CapabilitiesRegistry.reset_cache()
        after = CapabilitiesRegistry.all_capabilities()
        # Same dict instance; re-populated by the second call.
        assert before is after
        assert "aave_v3" in after
