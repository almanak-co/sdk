"""Tests for the strategy-side runner-hook registry."""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.runner_hook_registry import (
    AsyncSettlementPolicy,
    AsyncSettlementStatus,
    AsyncSettlementVerdict,
    RunnerAsyncSettlementCapability,
    RunnerHookConnector,
    RunnerHookRegistry,
    RunnerHookRegistryError,
    RunnerLPReceiptTopicCapability,
    RunnerPoolKeyLookupCapability,
    RunnerResultEnrichmentCapability,
)

TOPIC_A = "0x" + "a" * 64
TOPIC_B = "0x" + "b" * 64


class _TopicConnector(RunnerHookConnector, RunnerLPReceiptTopicCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("topic")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def lp_receipt_topics(self) -> frozenset[str]:
        return frozenset({TOPIC_A[2:].upper(), TOPIC_B})


class _CountingTopicConnector(_TopicConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("counting_topic")

    def __init__(self) -> None:
        self.calls = 0

    def lp_receipt_topics(self) -> frozenset[str]:
        self.calls += 1
        return super().lp_receipt_topics()


class _EnrichmentConnector(RunnerHookConnector, RunnerResultEnrichmentCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("enrichment")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def __init__(self) -> None:
        self.calls: list[tuple[Any, Any, str]] = []

    def enrich_result(self, result: Any, *, gateway_client: Any, chain: str, wallet_address: str = "") -> None:
        self.calls.append((result, gateway_client, chain))


class _FailingEnrichmentConnector(RunnerHookConnector, RunnerResultEnrichmentCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("failing_enrichment")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def enrich_result(self, result: Any, *, gateway_client: Any, chain: str, wallet_address: str = "") -> None:
        raise RuntimeError("boom")


class _SecondEnrichmentConnector(_EnrichmentConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("second_enrichment")


class _BadEnrichmentSignatureConnector(RunnerHookConnector, RunnerResultEnrichmentCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("bad_enrichment_signature")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def enrich_result(self) -> None:
        return None


class _PoolKeyLookupConnector(RunnerHookConnector, RunnerPoolKeyLookupCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("pool_key")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def __init__(self, lookup: Any | None) -> None:
        self.lookup = lookup
        self.calls: list[Any] = []

    def build_pool_key_lookup(self, gateway_client: Any) -> Any | None:
        self.calls.append(gateway_client)
        return self.lookup


class _SecondPoolKeyLookupConnector(_PoolKeyLookupConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("pool_key_found")


class _NoCapabilityConnector(RunnerHookConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("none")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP


class _AsyncSettlementConnector(RunnerHookConnector, RunnerAsyncSettlementCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("async_settlement")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def __init__(self) -> None:
        self.observed: list[dict[str, Any]] = []
        self.executed: list[dict[str, Any]] = []
        self.prepared: list[dict[str, Any]] = []

    def async_settlement_policy(self) -> AsyncSettlementPolicy:
        return AsyncSettlementPolicy(
            timeout_seconds=12,
            poll_interval_seconds=3,
            supports_local_order_execution=False,
            supports_cancellation=True,
        )

    def observe_async_orders(
        self,
        *,
        gateway_client: Any,
        chain: str,
        wallet_address: str,
        orders: tuple[Any, ...],
        intent: Any,
        observation_state: Any = None,
    ) -> AsyncSettlementVerdict:
        self.observed.append(
            {
                "gateway_client": gateway_client,
                "chain": chain,
                "wallet_address": wallet_address,
                "orders": orders,
                "intent": intent,
                "observation_state": observation_state,
            }
        )
        return AsyncSettlementVerdict(status=AsyncSettlementStatus.SETTLED, terminal=True)

    def execute_pending_orders_for_test(
        self,
        *,
        gateway_client: Any,
        chain: str,
        wallet_address: str,
        orders: tuple[Any, ...],
        intent: Any,
        network: str,
    ) -> AsyncSettlementVerdict:
        self.executed.append(
            {
                "gateway_client": gateway_client,
                "chain": chain,
                "wallet_address": wallet_address,
                "orders": orders,
                "intent": intent,
                "network": network,
            }
        )
        return AsyncSettlementVerdict(status=AsyncSettlementStatus.SETTLED, terminal=True)

    def prepare_pending_orders_for_teardown(
        self,
        *,
        gateway_client: Any,
        chain: str,
        wallet_address: str,
        residuals: tuple[Any, ...],
        network: str,
    ) -> bool:
        self.prepared.append(
            {
                "gateway_client": gateway_client,
                "chain": chain,
                "wallet_address": wallet_address,
                "residuals": residuals,
                "network": network,
            }
        )
        return True


class _EmptyTopicConnector(RunnerHookConnector, RunnerLPReceiptTopicCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("empty")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def lp_receipt_topics(self) -> frozenset[str]:
        return frozenset()


class _MalformedTopicConnector(RunnerHookConnector, RunnerLPReceiptTopicCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("malformed")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def __init__(self, topic: str) -> None:
        self.topic = topic

    def lp_receipt_topics(self) -> frozenset[str]:
        return frozenset({self.topic})


def test_register_rejects_classes() -> None:
    """Registry stores connector instances, not classes."""
    registry = RunnerHookRegistry()

    with pytest.raises(RunnerHookRegistryError, match="did you forget to instantiate"):
        registry.register(_TopicConnector)  # type: ignore[arg-type]


def test_register_rejects_connector_without_capability() -> None:
    """A runner-hook connector must implement at least one hook capability."""
    registry = RunnerHookRegistry()

    with pytest.raises(RunnerHookRegistryError, match="at least one runner hook capability"):
        registry.register(_NoCapabilityConnector())


def test_register_rejects_duplicate_protocols() -> None:
    """Protocol collisions are hard errors."""
    registry = RunnerHookRegistry()
    registry.register(_TopicConnector())

    with pytest.raises(RunnerHookRegistryError, match="already registered"):
        registry.register(_TopicConnector())


def test_lp_receipt_topics_are_normalized() -> None:
    """LP receipt topics are lowercased and prefixed with 0x."""
    registry = RunnerHookRegistry()
    registry.register(_TopicConnector())

    assert registry.lp_receipt_topics() == frozenset({TOPIC_A, TOPIC_B})


def test_lp_receipt_topics_reject_empty_sets() -> None:
    """Topic-capable connectors must publish at least one topic."""
    registry = RunnerHookRegistry()

    with pytest.raises(RunnerHookRegistryError, match="non-empty frozenset"):
        registry.register(_EmptyTopicConnector())


@pytest.mark.parametrize("topic", ["0xabc", "0x" + "g" * 64])
def test_lp_receipt_topics_reject_malformed_topics(topic: str) -> None:
    """Topic-capable connectors must publish canonical 32-byte EVM topics."""
    registry = RunnerHookRegistry()

    with pytest.raises(RunnerHookRegistryError, match="32-byte hex topic"):
        registry.register(_MalformedTopicConnector(topic))


def test_lp_receipt_topics_are_cached_after_first_read() -> None:
    """LP receipt topic normalization is cached after the first registry read."""
    registry = RunnerHookRegistry()
    connector = _CountingTopicConnector()
    registry.register(connector)

    assert connector.calls == 1
    assert registry.lp_receipt_topics() == frozenset({TOPIC_A, TOPIC_B})
    assert registry.lp_receipt_topics() == frozenset({TOPIC_A, TOPIC_B})
    assert connector.calls == 2


def test_enrich_result_dispatches_to_capable_connectors() -> None:
    """Result enrichment is dispatched to registered enrichment connectors."""
    registry = RunnerHookRegistry()
    connector = _EnrichmentConnector()
    result = object()
    gateway_client = object()
    registry.register(connector)

    registry.enrich_result(result, gateway_client=gateway_client, chain="arbitrum")

    assert connector.calls == [(result, gateway_client, "arbitrum")]


def test_enrich_result_continues_after_connector_failure() -> None:
    """A failing enrichment hook must not prevent later hooks from running."""
    registry = RunnerHookRegistry()
    connector = _SecondEnrichmentConnector()
    result = object()
    gateway_client = object()
    registry.register(_FailingEnrichmentConnector())
    registry.register(connector)

    registry.enrich_result(result, gateway_client=gateway_client, chain="arbitrum")

    assert connector.calls == [(result, gateway_client, "arbitrum")]


def test_register_rejects_bad_capability_signature() -> None:
    """Runtime-checkable protocols only prove names, so registration checks signatures."""
    registry = RunnerHookRegistry()

    with pytest.raises(RunnerHookRegistryError, match="enrich_result"):
        registry.register(_BadEnrichmentSignatureConnector())


def test_build_pool_key_lookup_returns_first_callback() -> None:
    """Pool-key lookup builders return the first available callback."""
    registry = RunnerHookRegistry()
    skipped = _PoolKeyLookupConnector(None)
    found = _SecondPoolKeyLookupConnector(lambda pool_id, chain: (pool_id, chain))
    gateway_client = object()
    registry.register(skipped)
    registry.register(found)

    assert registry.build_pool_key_lookup(gateway_client) is found.lookup
    assert skipped.calls == [gateway_client]
    assert found.calls == [gateway_client]


def test_async_settlement_capability_dispatches_without_protocol_branching() -> None:
    registry = RunnerHookRegistry()
    connector = _AsyncSettlementConnector()
    registry.register(connector)
    gateway_client = object()
    order = object()
    intent = object()

    policy = registry.async_settlement_policy(connector.protocol)
    verdict = registry.observe_async_orders(
        protocol=connector.protocol,
        gateway_client=gateway_client,
        chain="arbitrum",
        wallet_address="0xabc",
        orders=(order,),
        intent=intent,
    )
    executed = registry.execute_pending_orders_for_test(
        protocol=connector.protocol,
        gateway_client=gateway_client,
        chain="arbitrum",
        wallet_address="0xabc",
        orders=(order,),
        intent=intent,
        network="anvil",
    )
    prepared = registry.prepare_pending_orders_for_teardown(
        protocol=connector.protocol,
        gateway_client=gateway_client,
        chain="arbitrum",
        wallet_address="0xabc",
        residuals=(order,),
        network="anvil",
    )

    assert policy == AsyncSettlementPolicy(12, 3, False, True)
    assert verdict == AsyncSettlementVerdict(status=AsyncSettlementStatus.SETTLED, terminal=True)
    assert executed == AsyncSettlementVerdict(status=AsyncSettlementStatus.SETTLED, terminal=True)
    assert prepared is True
    assert connector.observed[0]["orders"] == (order,)
    assert connector.executed[0]["network"] == "anvil"
    assert connector.prepared[0]["network"] == "anvil"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("supports_local_order_execution", "no"),
        ("supports_cancellation", 1),
    ],
)
def test_async_settlement_policy_rejects_non_boolean_capabilities(field: str, value: Any) -> None:
    kwargs = {
        "timeout_seconds": 12,
        "poll_interval_seconds": 3,
        "supports_local_order_execution": False,
        "supports_cancellation": True,
    }
    kwargs[field] = value

    with pytest.raises(TypeError, match=field):
        AsyncSettlementPolicy(**kwargs)
