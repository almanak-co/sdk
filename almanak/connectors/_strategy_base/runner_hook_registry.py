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
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, ClassVar, Protocol, TypeVar, runtime_checkable

from almanak.connectors._base.types import ProtocolKind, ProtocolName

__all__ = [
    "STRATEGY_RUNNER_HOOK_REGISTRY",
    "AsyncSettlementPolicy",
    "AsyncSettlementStatus",
    "AsyncSettlementVerdict",
    "FillReconciliationVerdict",
    "RunnerAsyncSettlementCapability",
    "RunnerFillReconciliationCapability",
    "RunnerHookConnector",
    "RunnerHookRegistry",
    "RunnerHookRegistryError",
    "RunnerCurvePoolMetaLookupCapability",
    "RunnerLPReceiptTopicCapability",
    "RunnerPoolKeyLookupCapability",
    "RunnerResultEnrichmentCapability",
    "RunnerV4PositionStateCapability",
]


class RunnerHookRegistryError(Exception):
    """Registry contract violation."""


logger = logging.getLogger(__name__)
_TOPIC_RE = re.compile(r"^0x[0-9a-f]{64}$")


class AsyncSettlementStatus(StrEnum):
    """Protocol-neutral outcome of observing asynchronous order settlement."""

    SETTLED = "SETTLED"
    PENDING = "PENDING"
    PENDING_SETTLEMENT_TIMEOUT = "PENDING_SETTLEMENT_TIMEOUT"
    INFRASTRUCTURE_UNSUPPORTED = "INFRASTRUCTURE_UNSUPPORTED"
    OBSERVATION_FAILED = "OBSERVATION_FAILED"
    TERMINAL_FAILED = "TERMINAL_FAILED"


@dataclass(frozen=True)
class AsyncSettlementPolicy:
    """Connector-owned timing and managed-fork support declaration."""

    timeout_seconds: int
    poll_interval_seconds: int
    supports_local_order_execution: bool
    supports_cancellation: bool

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("AsyncSettlementPolicy.timeout_seconds must be positive")
        if self.poll_interval_seconds <= 0:
            raise ValueError("AsyncSettlementPolicy.poll_interval_seconds must be positive")
        if not isinstance(self.supports_local_order_execution, bool):
            raise TypeError("AsyncSettlementPolicy.supports_local_order_execution must be a bool")
        if not isinstance(self.supports_cancellation, bool):
            raise TypeError("AsyncSettlementPolicy.supports_cancellation must be a bool")


@dataclass(frozen=True)
class AsyncSettlementVerdict:
    """One measured connector verdict for a group of submitted async orders."""

    status: AsyncSettlementStatus
    terminal: bool
    orders: tuple[dict[str, Any], ...] = ()
    reason: str | None = None
    # Connector-private evidence carried between polls by the barrier. It is
    # deliberately excluded from equality/repr/serialization: callers consume
    # only the protocol-neutral verdict while the owning connector can retain a
    # measured pre-settlement baseline without process-global mutable state.
    observation_state: Any = field(default=None, repr=False, compare=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "terminal": self.terminal,
            "orders": [dict(order) for order in self.orders],
            "reason": self.reason,
        }


@runtime_checkable
class RunnerAsyncSettlementCapability(Protocol):
    """Connector owns async-order observation and local-test progression.

    The framework consumes only this protocol. It never branches on a concrete
    venue name or cancellation delay.
    """

    def async_settlement_policy(self) -> AsyncSettlementPolicy: ...

    def observe_async_orders(
        self,
        *,
        gateway_client: Any,
        chain: str,
        wallet_address: str,
        orders: tuple[Any, ...],
        intent: Any,
        observation_state: Any,
    ) -> AsyncSettlementVerdict: ...

    def prepare_pending_orders_for_teardown(
        self,
        *,
        gateway_client: Any,
        chain: str,
        wallet_address: str,
        residuals: tuple[Any, ...],
        network: str,
    ) -> bool: ...


@runtime_checkable
class RunnerLPReceiptTopicCapability(Protocol):
    """Connector publishes LP receipt topics used to choose bundle receipts."""

    def lp_receipt_topics(self) -> frozenset[str]: ...


@runtime_checkable
class RunnerResultEnrichmentCapability(Protocol):
    """Connector performs best-effort post-receipt result enrichment.

    ``wallet_address`` (VIB-5595) is the acting wallet for the executed intent.
    It is required by async-settlement perp venues (Hyperliquid) whose fill
    economics live off-EVM and are keyed by account — the enrichment reads
    ``userFills`` / ``userFunding`` for this wallet through the gateway. Hooks
    that do not need it (LP slot0 fallbacks) simply ignore the argument.
    """

    def enrich_result(self, result: Any, *, gateway_client: Any, chain: str, wallet_address: str = "") -> None: ...


@dataclass(frozen=True)
class FillReconciliationVerdict:
    """A connector-produced fill verdict for a pending async-settlement order (VIB-5614).

    The framework stays vocabulary-free: it never inspects ``status`` (an opaque
    ``FillStatus``-style value the strategy's ``reconcile_fill`` understands). It
    only reads ``terminal`` to decide whether to drop the cached pending handle
    (a confirmed fill / reject is terminal; UNMEASURED / RESTING is not, so the
    handle is retained and re-pumped next tick).

    Attributes:
        status: The opaque fill-status value passed straight to
            ``strategy.reconcile_fill(intent_type, status)``. A ``StrEnum`` member
            (which IS a str) so the framework can hold it without importing the
            connector's enum.
        terminal: True iff the verdict resolved the submission (fill or reject) —
            the runner clears the pending handle. False (UNMEASURED / RESTING)
            keeps it pending. Empty ≠ Zero: an unmeasured read is NOT terminal.
    """

    status: Any
    terminal: bool


@runtime_checkable
class RunnerFillReconciliationCapability(Protocol):
    """Connector reconciles an async-settlement (submission ≠ fill) perp order (VIB-5614).

    CoreWriter-style venues (Hyperliquid) settle orders off-EVM: a submit receipt
    proves submission, not fill. A strategy submits ``PERP_OPEN`` → PENDING →
    HOLDs until an observed fill promotes it (``strategy.reconcile_fill``). This
    capability is the runner-agnostic seam that feeds that observation:

    * ``extract_pending_fill_handle(result)`` — distil a just-executed result into
      a small **serializable** correlation handle (carrying the owning
      ``protocol`` + ``intent_type`` + the venue key, e.g. cloid) the runner
      caches, or ``None`` when the result is not a pending open this connector
      reconciles (a close, a non-venue result). Runs at execute time when the
      result (and its cloid) is still in scope.
    * ``resolve_fill_status(gateway_client, wallet_address, handle)`` — read the
      venue fill signal for a cached handle and return a
      :class:`FillReconciliationVerdict`, or ``None`` when the handle is not this
      connector's. Best-effort + fail-closed: a failed / not-yet-settled read is a
      NON-terminal UNMEASURED verdict (strategy stays PENDING), never a fabricated
      fill or reject. All egress stays gateway-side (boundary rule).

    **Invariant — one pending open per deployment.** The runner caches a SINGLE
    handle per ``deployment_id`` (``StrategyRunner._pending_fill_handles``); a new
    pending open OVERWRITES any prior un-reconciled one. The reference strategies
    satisfy this — a single ``PHASE_PENDING_FILL`` / ``_fill_confirmed`` gate holds
    the loop, so only one open is ever in flight. A connector/strategy that submits
    multiple concurrent async opens before either settles would strand the earlier
    handles in PENDING (teardown still covers the on-chain risk, but the phantom
    bookkeeping never resolves). Before reusing this seam that way, generalize the
    cache to key by cloid (a set/dict of handles pumped independently).
    """

    def extract_pending_fill_handle(self, result: Any) -> Any | None: ...

    def resolve_fill_status(
        self, *, gateway_client: Any, wallet_address: str, handle: Any
    ) -> FillReconciliationVerdict | None: ...


@runtime_checkable
class RunnerPoolKeyLookupCapability(Protocol):
    """Connector builds pool-key lookup callbacks for receipt parsing."""

    def build_pool_key_lookup(self, gateway_client: Any) -> Any | None: ...


@runtime_checkable
class RunnerCurvePoolMetaLookupCapability(Protocol):
    """Connector builds a sync uncurated-pool metadata lookup for receipt parsing (VIB-5628).

    The returned callback maps ``(pool_address, chain) -> CurvePoolMetadata | None``,
    binding the connector-owned dynamic pool resolver to the runner's gateway
    client so the Curve receipt parser can label an uncurated pool's LP legs on a
    static-registry miss. Keeps the framework runner free of any concrete Curve
    module import while the on-chain read stays gateway-routed.
    """

    def build_curve_pool_meta_lookup(self, gateway_client: Any) -> Any | None: ...


@runtime_checkable
class RunnerV4PositionStateCapability(Protocol):
    """Connector builds a live V4 LP on-chain position-state reader (VIB-5024).

    The returned callback maps ``(chain, token_id) → V4PositionState | None``,
    resolving the connector-owned PositionManager / StateView addresses and
    routing the read through the gateway ``QueryV4PositionState`` RPC. Keeps the
    framework valuer free of any hard-coded V4 protocol addresses while the
    on-chain read stays boundary-compliant.
    """

    def build_v4_position_state_reader(self, gateway_client: Any) -> Any | None: ...


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
            or isinstance(connector, RunnerFillReconciliationCapability)
            or isinstance(connector, RunnerPoolKeyLookupCapability)
            or isinstance(connector, RunnerCurvePoolMetaLookupCapability)
            or isinstance(connector, RunnerV4PositionStateCapability)
            or isinstance(connector, RunnerAsyncSettlementCapability)
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

    def enrich_result(
        self,
        result: Any,
        *,
        gateway_client: Any,
        chain: str,
        wallet_address: str = "",
    ) -> None:
        """Run every registered post-receipt enrichment hook.

        ``wallet_address`` (VIB-5595) is threaded to hooks that need the acting
        account (async-settlement perp fill reads); LP hooks ignore it.
        """
        for connector in self._connectors.values():
            if not isinstance(connector, RunnerResultEnrichmentCapability):
                continue
            try:
                connector.enrich_result(
                    result,
                    gateway_client=gateway_client,
                    chain=chain,
                    wallet_address=wallet_address,
                )
            except Exception:
                logger.debug(
                    "Runner hook connector %s enrichment failed; continuing",
                    type(connector).__qualname__,
                    exc_info=True,
                )

    def extract_pending_fill_handle(self, result: Any) -> Any | None:
        """Return the first connector's pending-fill handle for ``result`` (VIB-5614).

        Offered to every fill-reconciliation connector; the first to recognise the
        result (return non-``None``) wins. The handle is protocol-tagged so the
        pump can route :meth:`resolve_fill_status` back to the same connector.
        Fail-open: a raising connector is skipped, not fatal.
        """
        for connector in self._connectors.values():
            if not isinstance(connector, RunnerFillReconciliationCapability):
                continue
            try:
                handle = connector.extract_pending_fill_handle(result)
            except Exception:
                logger.debug(
                    "extract_pending_fill_handle failed for %s; continuing",
                    type(connector).__qualname__,
                    exc_info=True,
                )
                continue
            if handle is not None:
                return handle
        return None

    def resolve_fill_status(
        self,
        *,
        protocol: ProtocolName,
        gateway_client: Any,
        wallet_address: str,
        handle: Any,
    ) -> FillReconciliationVerdict | None:
        """Resolve the fill verdict for ``handle`` via the ``protocol`` connector (VIB-5614).

        Routes to the connector registered under ``protocol`` (the handle carries
        its owning protocol tag). Returns ``None`` when no such connector / it does
        not implement the capability / the read produced nothing. Fail-closed: a
        raising read yields ``None`` (the pump keeps the position PENDING), never a
        fabricated verdict.
        """
        connector = self._connectors.get(protocol)
        if not isinstance(connector, RunnerFillReconciliationCapability):
            return None
        try:
            return connector.resolve_fill_status(
                gateway_client=gateway_client,
                wallet_address=wallet_address,
                handle=handle,
            )
        except Exception:
            logger.debug(
                "resolve_fill_status failed for %s; treating as unmeasured",
                type(connector).__qualname__,
                exc_info=True,
            )
            return None

    def build_pool_key_lookup(self, gateway_client: Any) -> Any | None:
        """Build the first connector-provided pool-key lookup callback."""
        for connector in self._connectors.values():
            if not isinstance(connector, RunnerPoolKeyLookupCapability):
                continue
            lookup = connector.build_pool_key_lookup(gateway_client)
            if lookup is not None:
                return lookup
        return None

    def build_curve_pool_meta_lookup(self, gateway_client: Any) -> Any | None:
        """Build the first connector-provided Curve uncurated-pool metadata lookup (VIB-5628).

        Returns ``None`` when no connector declares the capability (parity with
        :meth:`build_pool_key_lookup`), so the Curve receipt parser degrades to
        the legacy static-registry-only path.
        """
        for connector in self._connectors.values():
            if not isinstance(connector, RunnerCurvePoolMetaLookupCapability):
                continue
            lookup = connector.build_curve_pool_meta_lookup(gateway_client)
            if lookup is not None:
                return lookup
        return None

    def build_v4_position_state_reader(self, gateway_client: Any) -> Any | None:
        """Build the first connector-provided live V4 position-state reader (VIB-5024)."""
        for connector in self._connectors.values():
            if not isinstance(connector, RunnerV4PositionStateCapability):
                continue
            reader = connector.build_v4_position_state_reader(gateway_client)
            if reader is not None:
                return reader
        return None

    def async_settlement_policy(self, protocol: ProtocolName) -> AsyncSettlementPolicy | None:
        """Return the connector-owned async settlement policy, if declared."""
        connector = self._connectors.get(protocol)
        if not isinstance(connector, RunnerAsyncSettlementCapability):
            return None
        return connector.async_settlement_policy()

    def observe_async_orders(
        self,
        *,
        protocol: ProtocolName,
        gateway_client: Any,
        chain: str,
        wallet_address: str,
        orders: tuple[Any, ...],
        intent: Any,
        observation_state: Any = None,
    ) -> AsyncSettlementVerdict | None:
        """Observe submitted orders through their owning connector."""
        connector = self._connectors.get(protocol)
        if not isinstance(connector, RunnerAsyncSettlementCapability):
            return None
        try:
            return connector.observe_async_orders(
                gateway_client=gateway_client,
                chain=chain,
                wallet_address=wallet_address,
                orders=orders,
                intent=intent,
                observation_state=observation_state,
            )
        except Exception as exc:
            logger.warning(
                "Async settlement observation raised for protocol %s: %s",
                protocol,
                exc,
                exc_info=True,
            )
            return AsyncSettlementVerdict(
                status=AsyncSettlementStatus.OBSERVATION_FAILED,
                terminal=False,
                reason=f"{type(exc).__name__}: {exc}",
                observation_state=observation_state,
            )

    def prepare_pending_orders_for_teardown(
        self,
        *,
        protocol: ProtocolName,
        gateway_client: Any,
        chain: str,
        wallet_address: str,
        residuals: tuple[Any, ...],
        network: str,
    ) -> bool:
        """Ask a connector to progress a managed-test pending-order window."""
        connector = self._connectors.get(protocol)
        if not isinstance(connector, RunnerAsyncSettlementCapability):
            return False
        try:
            return connector.prepare_pending_orders_for_teardown(
                gateway_client=gateway_client,
                chain=chain,
                wallet_address=wallet_address,
                residuals=residuals,
                network=network,
            )
        except Exception:
            logger.warning(
                "Async pending-order teardown preparation raised for protocol %s",
                protocol,
                exc_info=True,
            )
            return False

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
                keyword_names=("gateway_client", "chain", "wallet_address"),
            )
        if isinstance(connector, RunnerFillReconciliationCapability):
            cls._validate_method_signature(connector, "extract_pending_fill_handle", positional_count=1)
            cls._validate_method_signature(
                connector,
                "resolve_fill_status",
                positional_count=0,
                keyword_names=("gateway_client", "wallet_address", "handle"),
            )
        if isinstance(connector, RunnerPoolKeyLookupCapability):
            cls._validate_method_signature(connector, "build_pool_key_lookup", positional_count=1)
        if isinstance(connector, RunnerCurvePoolMetaLookupCapability):
            cls._validate_method_signature(connector, "build_curve_pool_meta_lookup", positional_count=1)
        if isinstance(connector, RunnerV4PositionStateCapability):
            cls._validate_method_signature(connector, "build_v4_position_state_reader", positional_count=1)
        if isinstance(connector, RunnerAsyncSettlementCapability):
            cls._validate_method_signature(connector, "async_settlement_policy", positional_count=0)
            cls._validate_method_signature(
                connector,
                "observe_async_orders",
                positional_count=0,
                keyword_names=("gateway_client", "chain", "wallet_address", "orders", "intent", "observation_state"),
            )
            cls._validate_method_signature(
                connector,
                "prepare_pending_orders_for_teardown",
                positional_count=0,
                keyword_names=("gateway_client", "chain", "wallet_address", "residuals", "network"),
            )

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
