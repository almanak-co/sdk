"""Strategy-side dispatch registry for connector-owned prediction-market reads (VIB-4989).

Sibling of :class:`~almanak.connectors._strategy_base.perps_read_registry.PerpsReadRegistry`.
Owns the single protocol-identifier → owning-connector ``prediction_read`` mapping
and lazily imports *only* the connector that owns a requested protocol, so a
broken sibling connector cannot poison an unrelated lookup, and the framework
prediction surface never hardcodes a venue name.

Each prediction connector that exposes a CLOB market-data read publishes a
module-level :data:`PREDICTION_READ_SPEC` (a
:class:`~almanak.connectors._strategy_base.prediction_read_base.PredictionReadSpec`)
in its ``prediction_read`` module. ``_SPEC_LOADERS`` maps the protocol identifier
to that module + attribute. Adding a prediction venue is one folder (its
``prediction_read`` module publishing ``PREDICTION_READ_SPEC``) plus one
``_SPEC_LOADERS`` row — no framework edit.

The table is **empty** until a connector opts in (the Polymarket row lands with
``polymarket/prediction_read.py`` in a later commit of VIB-4989).

Gateway-boundary note: this module is strategy-side and performs no network
egress. The owning connector ``prediction_read`` modules it imports are pure data
+ pure factories; the gateway-routed gRPC round-trip lives behind the gateway
client the built provider wraps.
"""

from __future__ import annotations

import importlib
import logging
from typing import TYPE_CHECKING, Any, ClassVar

from almanak.connectors._strategy_base.prediction_read_base import PredictionReadSpec

if TYPE_CHECKING:
    from almanak.connectors._strategy_base.prediction_read_base import PredictionProvider

logger = logging.getLogger(__name__)

__all__ = ["PredictionReadRegistry"]


class PredictionReadRegistry:
    """Protocol-identifier → connector prediction-read-spec dispatch registry.

    Adding a prediction venue is one folder (the connector's ``prediction_read``
    module publishing ``PREDICTION_READ_SPEC``) plus one row in
    :data:`_SPEC_LOADERS` — no framework edit. The table is empty until a
    connector opts in.
    """

    # Protocol identifier -> (module path, attribute) naming the connector's
    # published ``PredictionReadSpec``.
    _SPEC_LOADERS: ClassVar[dict[str, tuple[str, str]]] = {
        "polymarket": ("almanak.connectors.polymarket.prediction_read", "PREDICTION_READ_SPEC"),
    }

    # Optional protocol aliases mapping onto a canonical key in ``_SPEC_LOADERS``.
    _ALIASES: ClassVar[dict[str, str]] = {}

    _spec_cache: ClassVar[dict[str, PredictionReadSpec]] = {}

    @classmethod
    def _normalize(cls, protocol: str | None) -> str:
        # Total by design: ``None`` / non-``str`` input (loosely typed strategy
        # metadata reaching a public method) normalises to the empty string rather
        # than raising ``AttributeError`` on ``.lower()`` — every public entry
        # point then fails closed (no spec for "" ⇒ ``None`` / ``False`` / empty).
        if not isinstance(protocol, str):
            return ""
        key = protocol.lower().replace("-", "_")
        return cls._ALIASES.get(key, key)

    @classmethod
    def has(cls, protocol: str) -> bool:
        """Return True when ``protocol`` has a connector-owned prediction read."""
        return cls._normalize(protocol) in cls._SPEC_LOADERS

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Return every protocol identifier with a connector-owned prediction read.

        Framework discovery (the ``cli/run.py`` opt-in gate) iterates this instead
        of hardcoding a venue list, so adding a connector extends discovery with no
        framework edit.
        """
        return tuple(sorted(cls._SPEC_LOADERS))

    @classmethod
    def canonical(cls, protocol: str | None) -> str | None:
        """Return the canonical key for ``protocol`` if it has a prediction read.

        Total by design: ``None`` / non-``str`` input returns ``None`` rather than
        raising, so callers can use it in a ``canonical(p) or fallback`` chain.
        """
        if not isinstance(protocol, str) or not protocol:
            return None
        key = cls._normalize(protocol)
        return key if key in cls._SPEC_LOADERS else None

    @classmethod
    def _load_spec(cls, protocol: str) -> PredictionReadSpec | None:
        """Resolve and cache one protocol's prediction-read spec.

        Imports ONLY the connector module that owns ``protocol`` (per
        ``_SPEC_LOADERS``) — a broken sibling connector cannot block this lookup.
        Returns ``None`` when the protocol is unknown; raises on a broken import or
        wrong-type attribute (the public ``build_provider`` wraps this to fail
        closed).
        """
        cached = cls._spec_cache.get(protocol)
        if cached is not None:
            return cached
        entry = cls._SPEC_LOADERS.get(protocol)
        if entry is None:
            return None
        module_path, attribute = entry
        module = importlib.import_module(module_path)
        spec = getattr(module, attribute, None)
        if not isinstance(spec, PredictionReadSpec):
            raise TypeError(
                f"Registry maps {protocol!r} to {module_path}.{attribute}, "
                f"but that attribute is {type(spec).__name__}, not a PredictionReadSpec."
            )
        cls._spec_cache[protocol] = spec
        return spec

    @classmethod
    def supports_chain(cls, protocol: str, chain: str) -> bool:
        """Return True when ``protocol``'s prediction read supports ``chain``.

        Fails closed (``False``) when the protocol is unknown or its connector is
        broken — never raises into the framework.
        """
        try:
            spec = cls._load_spec(cls._normalize(protocol))
        except Exception:  # noqa: BLE001 — isolate one broken connector
            logger.warning("prediction-read spec for %r failed to load; supports_chain→False", protocol, exc_info=True)
            return False
        if spec is None:
            return False
        norm_chain = chain.lower() if isinstance(chain, str) else ""
        return norm_chain in spec.chains

    @classmethod
    def build_provider(
        cls, protocol: str, *, gateway_client: Any, wallet: str | None = None
    ) -> PredictionProvider | None:
        """Build the connector's prediction provider for ``protocol``, or ``None``.

        Returns ``None`` (fail closed) when the protocol has no connector-owned
        prediction read OR when the owning connector is broken — the framework
        then runs without a prediction provider (today's behaviour when no venue
        claims the chain), never crashing the runner on a malformed connector.
        """
        try:
            spec = cls._load_spec(cls._normalize(protocol))
            if spec is None:
                return None
            return spec.build_provider(gateway_client=gateway_client, wallet=wallet)
        except Exception:  # noqa: BLE001 — isolate one broken connector, fail closed
            logger.warning(
                "Failed to build prediction provider for protocol %r; returning None "
                "(framework runs without a prediction provider).",
                protocol,
                exc_info=True,
            )
            return None

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: drop the resolved-spec cache so the next call re-imports.

        Production code should never call this — it exists for narrow test setups
        that intentionally re-trigger a connector import or swap ``_SPEC_LOADERS``.
        """
        cls._spec_cache.clear()
