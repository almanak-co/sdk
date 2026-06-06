"""Lazy registry for connector-owned intent compilers."""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._connector import CONNECTOR_REGISTRY, ImportRef
from almanak.connectors._strategy_base.base.compiler import BaseProtocolCompiler


class CompilerRegistry:
    """Protocol-name to connector compiler registry.

    Connector manifests publish compiler ``ImportRef`` values. This strategy-side
    registry composes those refs into the protocol dispatch table consumed by
    ``IntentCompiler`` without naming concrete connector modules here.
    """

    _cache: ClassVar[dict[str, BaseProtocolCompiler]] = {}

    @classmethod
    def get(cls, protocol: str) -> BaseProtocolCompiler | None:
        """Return a compiler instance for ``protocol`` when one is registered."""
        key = cls._normalize_protocol(protocol)
        if key in cls._cache:
            return cls._cache[key]
        compiler_cls = cls._load_class(key)
        if compiler_cls is None:
            return None
        compiler = compiler_cls()
        if not isinstance(compiler, BaseProtocolCompiler):
            raise TypeError(f"{compiler_cls.__module__}.{compiler_cls.__qualname__} is not a BaseProtocolCompiler")
        cls._cache[key] = compiler
        return compiler

    @classmethod
    def has(cls, protocol: str) -> bool:
        """Return True when ``protocol`` has a connector compiler."""
        return cls._normalize_protocol(protocol) in cls._compiler_loaders()

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Return all protocol names with connector-owned compilers."""
        return tuple(sorted(cls._compiler_loaders()))

    @classmethod
    def _load_class(cls, key: str) -> type[BaseProtocolCompiler] | None:
        """Import a connector compiler class without instantiating it."""
        normalized_key = cls._normalize_protocol(key)
        loader = cls._compiler_loaders().get(normalized_key)
        if loader is None:
            return None
        compiler_cls = loader.load()
        if not isinstance(compiler_cls, type) or not issubclass(compiler_cls, BaseProtocolCompiler):
            raise TypeError(f"{loader.module}.{loader.attribute} is not a BaseProtocolCompiler class")
        return compiler_cls

    @classmethod
    def protocols_for_intent(cls, intent_type: Any) -> tuple[str, ...]:
        """Return loader-key protocol names whose connector declares ``intent_type``.

        Backs error-message hints in framework code ("Supported: ...") so
        per-intent lists don't have to be hand-maintained in
        ``intents/compiler.py``.
        """
        out: list[str] = []
        for key in cls.supported_protocols():
            compiler_cls = cls._load_class(key)
            if compiler_cls is None:
                continue
            if intent_type in compiler_cls.intents:
                out.append(key)
        return tuple(out)

    @classmethod
    def default_protocol(cls, dispatch_key: str) -> str | None:
        """Return the configured fallback protocol for a dispatch key, or None."""
        return cls._default_protocols().get(cls._normalize_default_key(dispatch_key))

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: clear instantiated compiler instances."""
        cls._cache.clear()

    @classmethod
    def _compiler_loaders(cls) -> dict[str, ImportRef]:
        """Return protocol -> connector-published compiler import ref."""
        loaders: dict[str, ImportRef] = {}
        owners: dict[str, str] = {}
        for connector in CONNECTOR_REGISTRY.with_compiler():
            if connector.compiler is None:
                continue
            for protocol in connector.compiler_keys:
                key = cls._normalize_protocol(protocol)
                owner = owners.get(key)
                if owner is not None:
                    raise ValueError(f"compiler protocol {key!r} is claimed by both {owner!r} and {connector.name!r}")
                owners[key] = connector.name
                loaders[key] = connector.compiler
        return loaders

    @classmethod
    def _default_protocols(cls) -> dict[str, str]:
        """Return dispatch-default key -> connector protocol mapping."""
        defaults: dict[str, str] = {}
        owners: dict[str, str] = {}
        loaders = cls._compiler_loaders()
        for connector in CONNECTOR_REGISTRY.with_compiler():
            if not connector.compiler_default_keys:
                continue
            protocol = cls._normalize_protocol(connector.name)
            if protocol not in loaders:
                raise ValueError(
                    f"compiler default keys for connector {connector.name!r} point to {protocol!r}, "
                    "but that protocol is not published by the connector compiler"
                )
            for dispatch_key in connector.compiler_default_keys:
                key = cls._normalize_default_key(dispatch_key)
                owner = owners.get(key)
                if owner is not None:
                    raise ValueError(
                        f"compiler default key {key!r} is claimed by both {owner!r} and {connector.name!r}"
                    )
                owners[key] = connector.name
                defaults[key] = protocol
        return defaults

    @staticmethod
    def _normalize_protocol(protocol: str) -> str:
        """Normalize protocol identifiers for compiler dispatch."""
        return protocol.strip().lower().replace("-", "_")

    @staticmethod
    def _normalize_default_key(dispatch_key: str) -> str:
        """Normalize non-protocol compiler dispatch-default keys."""
        return dispatch_key.strip().upper()


def get_compiler(protocol: str) -> BaseProtocolCompiler | None:
    """Module-level convenience wrapper."""
    return CompilerRegistry.get(protocol)


def supported_protocols() -> tuple[str, ...]:
    """Module-level convenience wrapper."""
    return CompilerRegistry.supported_protocols()


__all__ = ["CompilerRegistry", "get_compiler", "supported_protocols"]
