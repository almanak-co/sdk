"""Unit tests for the strategy-side receipt-parser connector registry.

Covers the structural invariants of
``almanak/connectors/_strategy_base/receipt_parser_registry.py``:

* ``register`` accepts instances, rejects classes, collides loudly.
* ``classes_by_key()`` builds a *lazy* mapping: enumerating keys must
  not import any parser module.
* A connector whose ``receipt_parser_class()`` raises ``ImportError``
  must NOT break unrelated lookups (this is the regression CodeRabbit
  + Codex flagged on PR 2457 — the old eager resolution made one bad
  module abort enrichment for every protocol).
* Collisions across different connectors raise at map-build time;
  multiple keys on the same connector resolving to the same class is
  fine (canonical alias pattern).
* Empty / invalid ``receipt_parser_keys()`` return values raise.
"""

from __future__ import annotations

from typing import ClassVar

import pytest

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    LazyParserClassMap,
    ReceiptParserCapability,
    ReceiptParserConnector,
    ReceiptParserConnectorRegistry,
    ReceiptParserRegistryError,
)

# ---------------------------------------------------------------------------
# Test parser classes — defined at module scope so we can assert identity.
# ---------------------------------------------------------------------------


class _StubParserA:
    def parse_receipt(self, receipt):  # noqa: D401, ANN001
        return None


class _StubParserB:
    def parse_receipt(self, receipt):  # noqa: D401, ANN001
        return None


# ---------------------------------------------------------------------------
# Test connectors
# ---------------------------------------------------------------------------


class _GoodConnectorA(ReceiptParserConnector, ReceiptParserCapability):
    """Single key, single class."""

    protocol: ClassVar[ProtocolName] = ProtocolName("test_a")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"test_a"})

    def receipt_parser_class(self, key: str) -> type:
        return _StubParserA


class _GoodConnectorB(ReceiptParserConnector, ReceiptParserCapability):
    """Two keys (canonical + alias) resolving to the same class."""

    protocol: ClassVar[ProtocolName] = ProtocolName("test_b")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"test_b", "test_b_alias"})

    def receipt_parser_class(self, key: str) -> type:
        return _StubParserB


class _ImportFailingConnector(ReceiptParserConnector, ReceiptParserCapability):
    """Simulates a connector whose parser module is unimportable.

    The pre-fix code eagerly resolved every key during the first
    ``classes_by_key()`` call, so this connector being registered
    poisoned every unrelated lookup. After the fix, looking up a
    different connector's key must succeed.
    """

    protocol: ClassVar[ProtocolName] = ProtocolName("test_broken")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"test_broken"})

    def receipt_parser_class(self, key: str) -> type:
        raise ImportError("simulated heavy-dep import failure")


class _EmptyKeysConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("test_empty")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset()  # empty — must raise at resolution

    def receipt_parser_class(self, key: str) -> type:  # pragma: no cover
        return _StubParserA


class _NonFrozenKeysConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("test_nonfrozen")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def receipt_parser_keys(self):  # type: ignore[override]
        return {"test_nonfrozen"}  # set, not frozenset — must raise

    def receipt_parser_class(self, key: str) -> type:  # pragma: no cover
        return _StubParserA


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRegister:
    def test_accepts_instance(self) -> None:
        registry = ReceiptParserConnectorRegistry()
        registry.register(_GoodConnectorA())
        assert registry.get(ProtocolName("test_a")) is not None

    def test_rejects_class(self) -> None:
        registry = ReceiptParserConnectorRegistry()
        with pytest.raises(ReceiptParserRegistryError, match="instance"):
            registry.register(_GoodConnectorA)  # type: ignore[arg-type]

    def test_collision_on_protocol_name_raises(self) -> None:
        registry = ReceiptParserConnectorRegistry()
        registry.register(_GoodConnectorA())
        with pytest.raises(ReceiptParserRegistryError, match="already registered"):
            registry.register(_GoodConnectorA())


class TestClassesByKeyLazyResolution:
    """The critical invariant: parser modules load on demand, not en masse.

    This is the regression Gemini + Codex flagged on PR 2457.
    """

    def test_returns_a_mapping(self) -> None:
        registry = ReceiptParserConnectorRegistry()
        registry.register(_GoodConnectorA())
        mapping = registry.classes_by_key()
        assert isinstance(mapping, LazyParserClassMap)

    def test_iter_does_not_resolve_classes(self) -> None:
        """Walking keys must NOT call ``receipt_parser_class`` on any
        connector. The framework's ``list_protocols`` /
        ``is_registered`` paths only need key membership — eagerly
        resolving classes here would defeat the lazy design."""
        registry = ReceiptParserConnectorRegistry()
        registry.register(_GoodConnectorA())
        registry.register(_ImportFailingConnector())  # would raise if resolved

        mapping = registry.classes_by_key()

        # Membership checks: safe.
        assert "test_a" in mapping
        assert "test_broken" in mapping
        # Key iteration: safe.
        assert set(mapping.keys()) == {"test_a", "test_broken"}
        # Length: safe.
        assert len(mapping) == 2

    def test_unrelated_lookup_unaffected_by_broken_connector(self) -> None:
        """The fix: an unimportable connector parser must NOT break
        lookups for other protocols. Previously, ``classes_by_key()``
        eagerly called ``receipt_parser_class(key)`` for every
        registered connector, so a single ``ImportError`` poisoned
        every unrelated ``get_parser("...")`` call.
        """
        registry = ReceiptParserConnectorRegistry()
        registry.register(_GoodConnectorA())
        registry.register(_ImportFailingConnector())

        mapping = registry.classes_by_key()

        # The healthy connector resolves.
        assert mapping["test_a"] is _StubParserA

        # The broken connector raises ImportError ONLY when its key is
        # actually requested.
        with pytest.raises(ImportError, match="simulated"):
            _ = mapping["test_broken"]

        # And after the failed lookup, the healthy connector still works.
        assert mapping["test_a"] is _StubParserA

    def test_lookup_caches_resolved_class(self) -> None:
        registry = ReceiptParserConnectorRegistry()
        registry.register(_GoodConnectorA())
        mapping = registry.classes_by_key()
        first = mapping["test_a"]
        second = mapping["test_a"]
        assert first is second

    def test_missing_key_raises_keyerror(self) -> None:
        registry = ReceiptParserConnectorRegistry()
        registry.register(_GoodConnectorA())
        mapping = registry.classes_by_key()
        with pytest.raises(KeyError):
            _ = mapping["never_registered"]

    def test_alias_keys_resolve_to_same_class(self) -> None:
        registry = ReceiptParserConnectorRegistry()
        registry.register(_GoodConnectorB())
        mapping = registry.classes_by_key()
        assert mapping["test_b"] is _StubParserB
        assert mapping["test_b_alias"] is _StubParserB

    def test_register_invalidates_cached_map(self) -> None:
        """Registering a new connector after ``classes_by_key()`` was
        first called must invalidate the cache so the next call reflects
        the new connector."""
        registry = ReceiptParserConnectorRegistry()
        registry.register(_GoodConnectorA())
        first = registry.classes_by_key()
        assert "test_b" not in first

        registry.register(_GoodConnectorB())
        second = registry.classes_by_key()
        assert "test_b" in second
        # The two mapping objects should be different identities (cache
        # was invalidated).
        assert first is not second


class TestKeyValidation:
    def test_empty_frozenset_raises(self) -> None:
        registry = ReceiptParserConnectorRegistry()
        registry.register(_EmptyKeysConnector())
        with pytest.raises(ReceiptParserRegistryError, match="non-empty frozenset"):
            registry.classes_by_key()

    def test_non_frozenset_raises(self) -> None:
        registry = ReceiptParserConnectorRegistry()
        registry.register(_NonFrozenKeysConnector())
        with pytest.raises(ReceiptParserRegistryError, match="non-empty frozenset"):
            registry.classes_by_key()


class TestKeyCollisionAcrossConnectors:
    def test_two_connectors_publishing_same_key_raise(self) -> None:
        """Two different connectors publishing the same key is a hard
        error — the registry can't decide which parser class wins."""

        class _Conflict(ReceiptParserConnector, ReceiptParserCapability):
            protocol: ClassVar[ProtocolName] = ProtocolName("test_conflict")
            kind: ClassVar[ProtocolKind] = ProtocolKind.LP

            def receipt_parser_keys(self) -> frozenset[str]:
                return frozenset({"test_a"})  # collides with _GoodConnectorA

            def receipt_parser_class(self, key: str) -> type:  # pragma: no cover
                return _StubParserA

        registry = ReceiptParserConnectorRegistry()
        registry.register(_GoodConnectorA())
        registry.register(_Conflict())

        with pytest.raises(ReceiptParserRegistryError, match="claimed by both"):
            registry.classes_by_key()


class TestWithCapability:
    def test_returns_only_capability_implementors(self) -> None:
        registry = ReceiptParserConnectorRegistry()
        registry.register(_GoodConnectorA())
        registry.register(_GoodConnectorB())
        connectors = registry.with_capability(ReceiptParserCapability)
        assert len(connectors) == 2
        assert all(isinstance(c, ReceiptParserCapability) for c in connectors)
