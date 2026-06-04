"""Strategy-side connector hooks for runner-specific enrichment.

This registry is intentionally narrow: it is for runner concerns that used
to import concrete connector modules directly. The connector manifest owns
the provider import reference, the boot file registers providers, and the
framework runner consumes only this protocol-clean registry.
"""

from __future__ import annotations

import inspect
import logging
import re
from typing import Any, ClassVar, Protocol, TypeVar, runtime_checkable

from almanak.connectors._base.types import ProtocolKind, ProtocolName

__all__ = [
    "STRATEGY_RUNNER_HOOK_REGISTRY",
    "RunnerHookConnector",
    "RunnerHookRegistry",
    "RunnerHookRegistryError",
    "RunnerLPReceiptTopicCapability",
    "RunnerPoolKeyLookupCapability",
    "RunnerResultEnrichmentCapability",
]


class RunnerHookRegistryError(Exception):
    """Registry contract violation."""


logger = logging.getLogger(__name__)
_TOPIC_RE = re.compile(r"^0x[0-9a-f]{64}$")


@runtime_checkable
class RunnerLPReceiptTopicCapability(Protocol):
    """Connector publishes LP receipt topics used to choose bundle receipts."""

    def lp_receipt_topics(self) -> frozenset[str]: ...


@runtime_checkable
class RunnerResultEnrichmentCapability(Protocol):
    """Connector performs best-effort post-receipt result enrichment."""

    def enrich_result(self, result: Any, *, gateway_client: Any, chain: str) -> None: ...


@runtime_checkable
class RunnerPoolKeyLookupCapability(Protocol):
    """Connector builds pool-key lookup callbacks for receipt parsing."""

    def build_pool_key_lookup(self, gateway_client: Any) -> Any | None: ...


class RunnerHookConnector:
    """Base class for strategy-runner hook connector instances."""

    protocol: ClassVar[ProtocolName]
    kind: ClassVar[ProtocolKind]


T = TypeVar("T")


class RunnerHookRegistry:
    """In-process registry of strategy-runner hook connectors."""

    def __init__(self) -> None:
        self._connectors: dict[ProtocolName, RunnerHookConnector] = {}
        self._lp_receipt_topics_cache: frozenset[str] | None = None

    def register(self, connector: RunnerHookConnector) -> None:
        """Register a connector instance. Collision on protocol raises."""
        if not isinstance(connector, RunnerHookConnector):
            raise RunnerHookRegistryError(
                "register() expects a RunnerHookConnector instance, got "
                f"{type(connector).__qualname__!s} ({connector!r}); did you "
                "forget to instantiate the class?"
            )
        if not (
            isinstance(connector, RunnerLPReceiptTopicCapability)
            or isinstance(connector, RunnerResultEnrichmentCapability)
            or isinstance(connector, RunnerPoolKeyLookupCapability)
        ):
            raise RunnerHookRegistryError(
                "register() expects a connector implementing at least one runner hook capability; "
                f"{type(connector).__qualname__!s} is missing the required methods."
            )
        self._validate_capability_signatures(connector)
        if isinstance(connector, RunnerLPReceiptTopicCapability):
            self._validate_lp_receipt_topics(connector, connector.lp_receipt_topics())
        proto = connector.protocol
        existing = self._connectors.get(proto)
        if existing is not None:
            raise RunnerHookRegistryError(
                f"protocol {proto!r} already registered by "
                f"{type(existing).__qualname__}; refusing to overwrite with "
                f"{type(connector).__qualname__}"
            )
        self._connectors[proto] = connector
        self._lp_receipt_topics_cache = None

    def get(self, protocol: ProtocolName) -> RunnerHookConnector | None:
        """Return the connector registered under ``protocol`` (or ``None``)."""
        return self._connectors.get(protocol)

    def all(self) -> tuple[RunnerHookConnector, ...]:
        """Return every registered connector in registration order."""
        return tuple(self._connectors.values())

    def with_capability(self, capability: type[T]) -> tuple[T, ...]:
        """Return every registered connector implementing ``capability``."""
        return tuple(c for c in self._connectors.values() if isinstance(c, capability))

    def lp_receipt_topics(self) -> frozenset[str]:
        """Return every normalized LP receipt-selection topic."""
        if self._lp_receipt_topics_cache is not None:
            return self._lp_receipt_topics_cache

        topics: set[str] = set()
        for connector in self._connectors.values():
            if not isinstance(connector, RunnerLPReceiptTopicCapability):
                continue
            topics.update(self._normalized_topic(topic) for topic in connector.lp_receipt_topics())
        self._lp_receipt_topics_cache = frozenset(topics)
        return self._lp_receipt_topics_cache

    def enrich_result(self, result: Any, *, gateway_client: Any, chain: str) -> None:
        """Run every registered post-receipt enrichment hook."""
        for connector in self._connectors.values():
            if not isinstance(connector, RunnerResultEnrichmentCapability):
                continue
            try:
                connector.enrich_result(result, gateway_client=gateway_client, chain=chain)
            except Exception:
                logger.debug(
                    "Runner hook connector %s enrichment failed; continuing",
                    type(connector).__qualname__,
                    exc_info=True,
                )

    def build_pool_key_lookup(self, gateway_client: Any) -> Any | None:
        """Build the first connector-provided pool-key lookup callback."""
        for connector in self._connectors.values():
            if not isinstance(connector, RunnerPoolKeyLookupCapability):
                continue
            lookup = connector.build_pool_key_lookup(gateway_client)
            if lookup is not None:
                return lookup
        return None

    def clear(self) -> None:
        """Test helper: clear registrations."""
        self._connectors.clear()
        self._lp_receipt_topics_cache = None

    @classmethod
    def _validate_capability_signatures(cls, connector: RunnerHookConnector) -> None:
        if isinstance(connector, RunnerLPReceiptTopicCapability):
            cls._validate_method_signature(connector, "lp_receipt_topics", positional_count=0)
        if isinstance(connector, RunnerResultEnrichmentCapability):
            cls._validate_method_signature(
                connector,
                "enrich_result",
                positional_count=1,
                keyword_names=("gateway_client", "chain"),
            )
        if isinstance(connector, RunnerPoolKeyLookupCapability):
            cls._validate_method_signature(connector, "build_pool_key_lookup", positional_count=1)

    @classmethod
    def _validate_method_signature(
        cls,
        connector: RunnerHookConnector,
        method_name: str,
        *,
        positional_count: int,
        keyword_names: tuple[str, ...] = (),
    ) -> None:
        method = getattr(connector, method_name)
        signature = inspect.signature(method)
        params = tuple(signature.parameters.values())
        has_var_positional = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params)
        has_var_keyword = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params)
        positional = tuple(
            p for p in params if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        )
        required_positional = tuple(p for p in positional if p.default is inspect.Parameter.empty)
        if len(required_positional) > positional_count:
            raise RunnerHookRegistryError(
                f"{type(connector).__qualname__}.{method_name}() must not require more than "
                f"{positional_count} positional argument(s)"
            )
        if not has_var_positional and len(positional) < positional_count:
            raise RunnerHookRegistryError(
                f"{type(connector).__qualname__}.{method_name}() must accept {positional_count} positional argument(s)"
            )
        for candidate in params:
            if (
                candidate.kind == inspect.Parameter.KEYWORD_ONLY
                and candidate.default is inspect.Parameter.empty
                and candidate.name not in keyword_names
                and not has_var_keyword
            ):
                raise RunnerHookRegistryError(
                    f"{type(connector).__qualname__}.{method_name}() must not require unsupported "
                    f"keyword argument {candidate.name!r}"
                )
        for keyword_name in keyword_names:
            keyword_param = signature.parameters.get(keyword_name)
            if keyword_param is None:
                if has_var_keyword:
                    continue
                raise RunnerHookRegistryError(
                    f"{type(connector).__qualname__}.{method_name}() must accept keyword argument {keyword_name!r}"
                )
            if keyword_param.kind == inspect.Parameter.POSITIONAL_ONLY:
                raise RunnerHookRegistryError(
                    f"{type(connector).__qualname__}.{method_name}() must accept keyword argument {keyword_name!r}"
                )

    @classmethod
    def _validate_lp_receipt_topics(
        cls,
        connector: RunnerLPReceiptTopicCapability,
        topics: frozenset[str],
    ) -> None:
        if not isinstance(topics, frozenset) or not topics:
            raise RunnerHookRegistryError(
                f"{type(connector).__qualname__}.lp_receipt_topics() must return a non-empty frozenset, got {topics!r}"
            )
        for topic in topics:
            cls._normalized_topic(topic)

    @staticmethod
    def _normalized_topic(topic: str) -> str:
        if not isinstance(topic, str) or not topic.strip():
            raise RunnerHookRegistryError(f"LP receipt topic must be a non-empty string, got {topic!r}")
        normalized = topic.strip().lower()
        if not normalized.startswith("0x"):
            normalized = f"0x{normalized}"
        if _TOPIC_RE.fullmatch(normalized) is None:
            raise RunnerHookRegistryError(f"LP receipt topic must be a 32-byte hex topic, got {topic!r}")
        return normalized


STRATEGY_RUNNER_HOOK_REGISTRY: RunnerHookRegistry = RunnerHookRegistry()
