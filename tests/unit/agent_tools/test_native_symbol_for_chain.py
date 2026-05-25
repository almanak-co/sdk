"""Unit tests for ``_native_symbol_for_chain`` (executor.py).

The helper is a thin wrapper around ``ChainRegistry.try_resolve`` that the
``_execute_get_portfolio`` call site uses to look up native gas-token
symbols. Coverage was previously implicit (only exercised through the gRPC
dispatch path); CodeRabbit (PR #2418 round 3) flagged that the new helper
and its call-site warning branch should be unit-covered directly.

VIB-4801: matches the test discipline established by
``tests/unit/runner/test_resolve_gas_context.py`` for the sibling
ChainRegistry cutover (lean SimpleNamespace stubs, real registry for the
happy path, monkeypatch for the unknown-chain branch).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from almanak.framework.agent_tools import executor as executor_module
from almanak.framework.agent_tools.executor import _native_symbol_for_chain


class TestNativeSymbolForChainHappyPath:
    """Real ChainRegistry entries — pin the canonical native symbols."""

    def test_ethereum_returns_eth(self) -> None:
        assert _native_symbol_for_chain("ethereum") == "ETH"

    def test_arbitrum_returns_eth(self) -> None:
        # L2s rolled up onto Ethereum share the parent native symbol.
        assert _native_symbol_for_chain("arbitrum") == "ETH"

    def test_polygon_returns_matic(self) -> None:
        # Ticker stays observable to the portfolio call site; if the
        # registry migrates to "POL", update this test in the same PR.
        assert _native_symbol_for_chain("polygon") == "MATIC"

    def test_avalanche_returns_avax(self) -> None:
        assert _native_symbol_for_chain("avalanche") == "AVAX"

    def test_resolution_is_case_insensitive(self) -> None:
        # ChainRegistry.try_resolve does ``key.lower().strip()`` itself;
        # the helper relies on that behaviour rather than re-lowercasing.
        assert _native_symbol_for_chain("ETHEREUM") == "ETH"
        assert _native_symbol_for_chain("  Arbitrum  ") == "ETH"


class TestNativeSymbolForChainUnknown:
    """Unknown chain → ``None`` (the call site converts this into a warning)."""

    def test_unknown_chain_returns_none(self) -> None:
        # Any string that does not resolve in the real registry → None.
        # No monkeypatch needed; ``try_resolve`` already returns None for
        # unknown names without raising.
        assert _native_symbol_for_chain("not-a-real-chain") is None

    def test_empty_string_returns_none(self) -> None:
        assert _native_symbol_for_chain("") is None


class TestNativeSymbolForChainMockedRegistry:
    """Verify the helper's wrapper contract independently of the live
    registry: it must read ``descriptor.native.symbol`` and propagate
    ``None`` unchanged. Mocking ``try_resolve`` lets us pin behaviour
    even after future registry additions.
    """

    def test_propagates_symbol_from_descriptor_native(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Build a minimal duck-typed descriptor — the helper only touches
        # ``descriptor.native.symbol``.
        fake_descriptor = SimpleNamespace(native=SimpleNamespace(symbol="FAKE"))
        # Patch the local ``ChainRegistry`` symbol imported lazily inside
        # the helper. The helper does ``from almanak.core.chains import
        # ChainRegistry`` at call time, so monkeypatch the source.
        from almanak.core import chains as chains_pkg

        monkeypatch.setattr(
            chains_pkg.ChainRegistry, "try_resolve", lambda _name: fake_descriptor
        )
        assert _native_symbol_for_chain("any-chain") == "FAKE"

    def test_propagates_none_when_registry_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from almanak.core import chains as chains_pkg

        monkeypatch.setattr(
            chains_pkg.ChainRegistry, "try_resolve", lambda _name: None
        )
        assert _native_symbol_for_chain("any-chain") is None


def test_helper_is_exported_for_call_site() -> None:
    """Static guard: the helper must remain importable from the executor
    module under its original name. ``_execute_get_portfolio`` calls
    ``_native_symbol_for_chain(chain_key)`` directly; renaming or removing
    the helper without updating the call site would be a silent break.
    """
    assert hasattr(executor_module, "_native_symbol_for_chain")
    assert callable(executor_module._native_symbol_for_chain)
