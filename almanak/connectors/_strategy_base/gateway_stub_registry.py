"""Strategy-side registry for connector-published gRPC client stubs (VIB-4989).

The framework's ``GatewayClient`` calls :meth:`GatewayStubRegistry.build_stubs`
once at connect time to construct every connector-owned gRPC client stub from the
gateway channel, then exposes them by name. This replaces the hardcoded
``polymarket_pb2_grpc`` import + ``.polymarket`` property the framework client
previously carried (VIB-4989).

Each connector that ships its own gRPC service publishes a module-level
:data:`GATEWAY_STUB_SPEC` (a
:class:`~almanak.connectors._strategy_base.gateway_stub_base.GatewayStubSpec`) in
its ``gateway_stub`` module. The manifest-derived dispatch maps the connector folder name to
that module + attribute. Adding a gRPC-shipping connector is one folder + one row
— no framework edit.

Broken-connector isolation: each spec is imported lazily and per-connector inside
its own ``try``/``except`` — a broken or missing sibling connector is skipped with
a warning and cannot block stub-building for healthy connectors (mirrors
:class:`AccountingTreatmentRegistry`). The isolation holds at *both* phases: a spec
that fails to import/load is skipped in ``_iter_specs``, and a ``stub_factory`` that
raises when invoked is isolated in ``build_stubs`` (its stub is simply absent). The
one error that is *not* swallowed is a ``service_name`` collision (two connectors
claiming the same stub name — a programming error the registry cannot resolve).

The ``polymarket`` connector currently declares a stub spec
(``polymarket/gateway_stub.py``); further connectors opt in the same way.

Gateway-boundary note: strategy-side, no network egress. The connector
``gateway_stub`` modules import generated proto (pure codegen).
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Iterator
from typing import Any, ClassVar

from almanak.connectors._strategy_base.gateway_stub_base import GatewayStubSpec

logger = logging.getLogger(__name__)

__all__ = ["GatewayStubRegistry"]


class GatewayStubRegistry:
    """Connector folder name → published gRPC-client-stub-spec registry.

    The ``polymarket`` connector declares ``gateway_stub=ImportRef(...)`` on its
    manifest; further gRPC-shipping connectors opt in with the same manifest
    declaration + a published ``GATEWAY_STUB_SPEC`` — no registry edit.
    """

    # Manifest-derived ``connector -> (module path, attribute)`` spec map, built
    # lazily on first use. ``None`` means "not built yet".
    _spec_loader_map: ClassVar[dict[str, tuple[str, str]] | None] = None

    _spec_cache: ClassVar[dict[str, GatewayStubSpec]] = {}

    @classmethod
    def _spec_loaders(cls) -> dict[str, tuple[str, str]]:
        """Return the manifest-derived ``connector -> (module, attribute)`` map."""
        if cls._spec_loader_map is None:
            # Deferred import: avoids a module-level cycle through the
            # connector descriptor.
            from almanak.connectors._connector import CONNECTOR_REGISTRY

            cls._spec_loader_map = {
                connector_manifest.name: (
                    connector_manifest.gateway_stub.module,
                    connector_manifest.gateway_stub.attribute,
                )
                for connector_manifest in CONNECTOR_REGISTRY.with_gateway_stub()
                if connector_manifest.gateway_stub is not None
            }
        return cls._spec_loader_map

    @classmethod
    def _load_spec(cls, connector: str) -> GatewayStubSpec:
        """Import one connector's ``gateway_stub`` and return its published spec.

        Raises on a missing loader entry, a failed import, or a wrong-type
        attribute — :meth:`_iter_specs` catches these to isolate one broken
        connector from the rest.
        """
        cached = cls._spec_cache.get(connector)
        if cached is not None:
            return cached
        module_path, attribute = cls._spec_loaders()[connector]
        module = importlib.import_module(module_path)
        spec = getattr(module, attribute, None)
        if not isinstance(spec, GatewayStubSpec):
            raise TypeError(
                f"Registry maps {connector!r} to {module_path}.{attribute}, but that "
                f"attribute is {type(spec).__name__}, not a GatewayStubSpec."
            )
        cls._spec_cache[connector] = spec
        return spec

    @classmethod
    def _iter_specs(cls) -> Iterator[tuple[str, GatewayStubSpec]]:
        """Yield ``(connector, spec)`` for each loader, isolating broken siblings.

        A connector whose module fails to import or whose attribute is the wrong
        type is skipped with a warning (its stub is simply absent); healthy
        connectors are unaffected.
        """
        for connector in cls._spec_loaders():
            try:
                spec = cls._load_spec(connector)
            except Exception:  # noqa: BLE001 — isolate one broken connector
                logger.warning(
                    "Skipping gateway-stub spec for connector %r: its module failed to "
                    "import or published an invalid GATEWAY_STUB_SPEC. Its gRPC stub is "
                    "absent; unrelated connectors are unaffected.",
                    connector,
                    exc_info=True,
                )
                continue
            yield connector, spec

    @classmethod
    def build_stubs(cls, channel: Any) -> dict[str, Any]:
        """Build every connector gRPC client stub from ``channel``.

        Called once per ``GatewayClient.connect()``. Returns ``{service_name: stub}``.
        A ``service_name`` claimed by two connectors is a hard ``ValueError`` (the
        registry cannot silently pick a side). A broken connector is skipped (its
        stub absent), never blocking healthy connectors' stubs — whether it fails at
        import/load (``_iter_specs``) or its ``stub_factory`` raises at call time.
        """
        stubs: dict[str, Any] = {}
        owner: dict[str, str] = {}
        for connector, spec in cls._iter_specs():
            name = spec.service_name
            existing = owner.get(name)
            if existing is not None and existing != connector:
                raise ValueError(f"gateway stub service_name {name!r} claimed by both {existing!r} and {connector!r}")
            owner[name] = connector
            # Isolate a broken connector's factory at *runtime* too: ``_iter_specs``
            # only guards import/load. A ``stub_factory(channel)`` that raises (e.g. a
            # proto/runtime mismatch surfacing at call time) must not crash
            # ``GatewayClient.connect()`` for healthy connectors. The service_name
            # collision above stays a hard error (a config bug the registry cannot
            # resolve); a raising factory is an isolatable connector fault — the stub
            # is simply absent (``connector_stub`` returns ``None``).
            try:
                stubs[name] = spec.stub_factory(channel)
            except Exception:  # noqa: BLE001 — isolate one broken connector's factory
                logger.warning(
                    "Failed to build gateway stub for connector %r (service %r): its "
                    "stub_factory raised. Its gRPC stub will be absent; unrelated "
                    "connectors are unaffected.",
                    connector,
                    name,
                    exc_info=True,
                )
        return stubs

    @classmethod
    def stub_names(cls) -> tuple[str, ...]:
        """Return every connector stub ``service_name`` (broken connectors skipped)."""
        return tuple(sorted(spec.service_name for _, spec in cls._iter_specs()))

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: drop the resolved-spec cache so the next call re-imports."""
        cls._spec_cache.clear()
        cls._spec_loader_map = None
