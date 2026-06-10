"""Strategy-side dispatch registry for connector-owned prediction-market execution (VIB-4989).

Companion to :class:`~almanak.connectors._strategy_base.prediction_read_registry.PredictionReadRegistry`.
Owns the protocol-identifier → owning-connector ``clob_handler`` mapping and lazily
imports *only* the connector that owns a requested protocol, so the runner never
hardcodes a venue name (or a chain) to wire CLOB execution.

Each prediction connector that exposes CLOB order execution publishes a
module-level :data:`PREDICTION_EXECUTE_SPEC` (a
:class:`~almanak.connectors._strategy_base.prediction_execute_base.PredictionExecuteSpec`)
in its ``clob_handler`` module. The manifest-derived dispatch maps the protocol identifier to
that module + attribute. Adding a prediction venue is one folder plus one
``prediction_execute=ImportRef(...)`` manifest declaration — no framework edit.

The table is **empty** until a connector opts in (the Polymarket row lands with
``polymarket/clob_handler.py`` in a later commit of VIB-4989).

Gateway-boundary note: strategy-side, no network egress. The built handler routes
all order submission through the gateway (it signs nothing locally).
"""

from __future__ import annotations

import importlib
import logging
from typing import TYPE_CHECKING, Any, ClassVar

from almanak.connectors._strategy_base.prediction_execute_base import PredictionExecuteSpec

if TYPE_CHECKING:
    from almanak.framework.execution.handler_registry import ExecutionHandler

logger = logging.getLogger(__name__)

__all__ = ["PredictionExecuteRegistry"]


class PredictionExecuteRegistry:
    """Protocol-identifier → connector prediction-execute-spec dispatch registry.

    Adding a prediction venue is one folder (the connector's ``clob_handler``
    module publishing ``PREDICTION_EXECUTE_SPEC`` and its ``CONNECTOR`` manifest
    declaring ``prediction_execute=ImportRef(...)``) — no framework or registry
    edit.
    """

    # Manifest-derived ``protocol -> (module path, attribute)`` spec map, built
    # lazily on first use. ``None`` means "not built yet".
    _spec_loader_map: ClassVar[dict[str, tuple[str, str]] | None] = None

    _spec_cache: ClassVar[dict[str, PredictionExecuteSpec]] = {}

    @classmethod
    def _spec_loaders(cls) -> dict[str, tuple[str, str]]:
        """Return the manifest-derived ``protocol -> (module, attribute)`` map."""
        if cls._spec_loader_map is None:
            # Deferred import: avoids a module-level cycle through the
            # connector descriptor.
            from almanak.connectors._connector import CONNECTOR_REGISTRY

            cls._spec_loader_map = {
                connector_manifest.name: (
                    connector_manifest.prediction_execute.module,
                    connector_manifest.prediction_execute.attribute,
                )
                for connector_manifest in CONNECTOR_REGISTRY.with_prediction_execute()
                if connector_manifest.prediction_execute is not None
            }
        return cls._spec_loader_map

    @classmethod
    def _normalize(cls, protocol: str | None) -> str:
        # Total by design (see PredictionReadRegistry._normalize).
        if not isinstance(protocol, str):
            return ""
        return protocol.lower().replace("-", "_")

    @classmethod
    def has(cls, protocol: str) -> bool:
        """Return True when ``protocol`` has a connector-owned CLOB execution handler."""
        return cls._normalize(protocol) in cls._spec_loaders()

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Return every protocol identifier with a connector-owned CLOB handler."""
        return tuple(sorted(cls._spec_loaders()))

    @classmethod
    def canonical(cls, protocol: str | None) -> str | None:
        """Return the canonical key for ``protocol`` if it has a CLOB handler, else ``None``."""
        if not isinstance(protocol, str) or not protocol:
            return None
        key = cls._normalize(protocol)
        return key if key in cls._spec_loaders() else None

    @classmethod
    def _load_spec(cls, protocol: str) -> PredictionExecuteSpec | None:
        """Resolve and cache one protocol's prediction-execute spec.

        Imports ONLY the owning connector module — a broken sibling cannot block
        this lookup. Returns ``None`` for an unknown protocol; raises on a broken
        import / wrong-type attribute (public methods wrap this to fail closed).
        """
        cached = cls._spec_cache.get(protocol)
        if cached is not None:
            return cached
        entry = cls._spec_loaders().get(protocol)
        if entry is None:
            return None
        module_path, attribute = entry
        module = importlib.import_module(module_path)
        spec = getattr(module, attribute, None)
        if not isinstance(spec, PredictionExecuteSpec):
            raise TypeError(
                f"Registry maps {protocol!r} to {module_path}.{attribute}, "
                f"but that attribute is {type(spec).__name__}, not a PredictionExecuteSpec."
            )
        cls._spec_cache[protocol] = spec
        return spec

    @classmethod
    def supports_chain(cls, protocol: str, chain: str) -> bool:
        """Return True when ``protocol``'s CLOB execution supports ``chain`` (fails closed)."""
        try:
            spec = cls._load_spec(cls._normalize(protocol))
        except Exception:  # noqa: BLE001 — isolate one broken connector
            logger.warning(
                "prediction-execute spec for %r failed to load; supports_chain→False", protocol, exc_info=True
            )
            return False
        if spec is None:
            return False
        norm_chain = chain.lower() if isinstance(chain, str) else ""
        return norm_chain in spec.chains

    @classmethod
    def protocols_for_chain(cls, chain: str) -> tuple[str, ...]:
        """Return the protocols whose CLOB execution supports ``chain``.

        Replaces the runner's ``if strategy.chain.lower() == "polygon"`` gate: the
        runner asks the registry which prediction protocols a chain supports
        instead of naming a chain. A connector whose spec fails to load is skipped
        (its protocol is simply absent) — never raises into the runner.
        """
        norm_chain = chain.lower() if isinstance(chain, str) else ""
        out: list[str] = []
        for protocol in cls._spec_loaders():
            try:
                spec = cls._load_spec(protocol)
            except Exception:  # noqa: BLE001 — isolate one broken connector
                logger.warning(
                    "prediction-execute spec for %r failed to load; omitting from chain scan", protocol, exc_info=True
                )
                continue
            if spec is not None and norm_chain in spec.chains:
                out.append(protocol)
        return tuple(sorted(out))

    @classmethod
    def build_handler(cls, protocol: str, *, gateway_client: Any, wallet: str | None = None) -> ExecutionHandler | None:
        """Build the connector's CLOB execution handler for ``protocol``, or ``None``.

        Returns ``None`` (fail closed) when the protocol has no connector-owned
        CLOB handler OR when the owning connector is broken — matching today's
        ``clob_handler = None`` behaviour when no venue claims the chain, never
        crashing the runner on a malformed connector.
        """
        try:
            spec = cls._load_spec(cls._normalize(protocol))
            if spec is None:
                return None
            return spec.build_handler(gateway_client=gateway_client, wallet=wallet)
        except Exception:  # noqa: BLE001 — isolate one broken connector, fail closed
            logger.warning(
                "Failed to build CLOB execution handler for protocol %r; returning None "
                "(runner runs without a CLOB handler).",
                protocol,
                exc_info=True,
            )
            return None

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: drop the resolved-spec cache so the next call re-imports."""
        cls._spec_cache.clear()
        cls._spec_loader_map = None
