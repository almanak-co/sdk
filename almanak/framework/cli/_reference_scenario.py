"""Explicit reference observations for the local managed-Anvil scenario harness."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from google.protobuf.json_format import ParseDict, ParseError

from almanak.framework.deployment.mode import is_hosted
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReferenceScenarioEvent:
    scenario_at: datetime
    responses: tuple[bytes, ...]


def _keys(raw: Any, required: set[str], optional: set[str] | None = None) -> None:
    if not isinstance(raw, dict):
        raise ValueError("reference scenario requires objects")
    missing = required - raw.keys()
    unknown = raw.keys() - required - (optional or set())
    if missing or unknown:
        raise ValueError(f"reference scenario fields missing={sorted(missing)}, unknown={sorted(unknown)}")


def _event_time(raw: Any) -> datetime:
    if not isinstance(raw, str):
        raise ValueError("scenario_at must be an explicit timezone-aware ISO timestamp")
    value = datetime.fromisoformat(raw)
    if value.tzinfo is None:
        raise ValueError("scenario_at must include timezone")
    return value.astimezone(UTC)


def _parse_wire_response(raw: Any) -> gateway_pb2.ReferencePriceResponse:
    descriptor = gateway_pb2.ReferencePriceResponse.DESCRIPTOR
    _keys(raw, {f.name for f in descriptor.fields} - {"composition"}, {"composition"})
    for key in ("observed_at", "market_status_as_of"):
        if type(raw[key]) is not int or raw[key] < 0:
            raise ValueError(f"{key} must be an explicit nonnegative epoch second")
    if type(raw["stale"]) is not bool:
        raise ValueError("stale must be an explicit boolean")
    confidence = Decimal(str(raw["confidence"]))
    if isinstance(raw["confidence"], bool) or not confidence.is_finite() or not 0 <= confidence <= 1:
        raise ValueError("reference confidence must be finite and within 0..1")
    try:
        response = ParseDict(raw, gateway_pb2.ReferencePriceResponse(), ignore_unknown_fields=False)
    except ParseError as exc:
        raise ValueError(f"invalid reference wire observation: {exc}") from exc
    response.chain = response.chain.strip().lower()
    if not response.instrument or not response.chain or not response.quote:
        raise ValueError("reference identity must be complete")
    if response.instrument != response.instrument.upper() or response.quote != response.quote.upper():
        raise ValueError("reference instrument and quote must use canonical uppercase")
    if response.availability not in (
        gateway_pb2.REFERENCE_PRICE_AVAILABILITY_AVAILABLE,
        gateway_pb2.REFERENCE_PRICE_AVAILABILITY_UNMEASURED,
        gateway_pb2.REFERENCE_PRICE_AVAILABILITY_ERRORED,
    ):
        raise ValueError("reference availability must be explicitly supported")
    if response.market_status not in (
        gateway_pb2.REFERENCE_MARKET_STATUS_OPEN,
        gateway_pb2.REFERENCE_MARKET_STATUS_CLOSED,
        gateway_pb2.REFERENCE_MARKET_STATUS_UNKNOWN,
    ):
        raise ValueError("reference market status must be explicit")
    if response.basis not in (
        gateway_pb2.REFERENCE_PRICE_BASIS_UNDERLYING_SHARE,
        gateway_pb2.REFERENCE_PRICE_BASIS_RAW_TOKEN,
    ):
        raise ValueError("reference basis must be explicit")
    return response


def _validate_composition_shape(raw: Any, response: gateway_pb2.ReferencePriceResponse) -> None:
    if response.HasField("composition"):
        composition = raw["composition"]
        _keys(composition, {f.name for f in gateway_pb2.ReferencePriceComposition.DESCRIPTOR.fields})
        for key in (
            "underlying_observed_at",
            "multiplier_block_number",
            "multiplier_block_timestamp",
            "multiplier_read_at",
            "multiplier_effective_at",
            "composed_at",
        ):
            if type(composition[key]) is not int or composition[key] < 0:
                raise ValueError(f"composition.{key} must be an explicit nonnegative integer")


def _parse_response(raw: Any) -> bytes:
    from almanak.framework.market.reference import decode_reference_composition
    from almanak.integrations.bstocks.catalog import reference_profile

    response = _parse_wire_response(raw)
    profile = reference_profile(response.chain, response.instrument, response.token_address)
    _validate_composition_shape(raw, response)
    if response.availability == gateway_pb2.REFERENCE_PRICE_AVAILABILITY_AVAILABLE:
        price = Decimal(response.price)
        if not price.is_finite() or price <= 0:
            raise ValueError("available reference price must be positive and finite")
        if not response.source or response.observed_at <= 0 or not response.market_status_source:
            raise ValueError("available reference observation provenance must be complete")
        if response.market_status_as_of <= 0:
            raise ValueError("available reference session timestamp must be measured")
        decode_reference_composition(
            response, profile, instrument=response.instrument, chain=response.chain, quote=response.quote
        )
    elif response.price or not response.reason or not response.stale:
        raise ValueError("unavailable reference must have empty price, explicit reason and stale=true")
    response.source = f"synthetic:{response.source}"
    response.market_status_source = f"synthetic:{response.market_status_source}"
    if response.HasField("composition"):
        response.composition.underlying_source = f"synthetic:{response.composition.underlying_source}"
    return response.SerializeToString()


def parse_reference_events(raw: Any) -> tuple[ReferenceScenarioEvent, ...]:
    """Require complete observations; never supply omitted clocks or provenance."""
    if not isinstance(raw, list) or not raw:
        raise ValueError("reference_events must be a nonempty list")
    events: list[ReferenceScenarioEvent] = []
    for event in raw:
        _keys(event, {"scenario_at", "references"})
        at = _event_time(event["scenario_at"])
        if events and at <= events[-1].scenario_at:
            raise ValueError("scenario_at must strictly increase between events")
        if not isinstance(event["references"], list):
            raise ValueError("references must be a list")
        responses = tuple(_parse_response(response) for response in event["references"])
        keys = [_identity(gateway_pb2.ReferencePriceResponse.FromString(value)) for value in responses]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate reference observation in event")
        events.append(ReferenceScenarioEvent(at, responses))
    return tuple(events)


def _identity(value: Any) -> tuple[str, str, str, str]:
    return (
        value.instrument.strip().upper(),
        value.chain.strip().lower(),
        value.quote.strip().upper(),
        value.token_address.lower(),
    )


def require_reference_test_runtime(*, network: str | None, managed: bool) -> None:
    if is_hosted() or network != "anvil" or not managed:
        raise ValueError(
            "reference scenarios require local managed Anvil with explicit --network anvil; "
            "hosted/mainnet/external gateways are forbidden"
        )


class _ReferenceMarket:
    def __init__(self, delegate: Any, responses: tuple[bytes, ...], guard: Any):
        self._delegate = delegate
        self._guard = guard
        self._responses = {_identity(gateway_pb2.ReferencePriceResponse.FromString(raw)): raw for raw in responses}
        self.unconsumed = set(self._responses)

    def GetReferencePrice(self, request: Any, **kwargs: Any) -> Any:  # noqa: N802 - protobuf API
        self._guard()
        identity = _identity(request)
        value = self._responses.get(identity)
        if value is None:
            return self._delegate.GetReferencePrice(request, **kwargs)
        self.unconsumed.discard(identity)
        return gateway_pb2.ReferencePriceResponse.FromString(value)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


class _ReferenceClient:
    def __init__(self, delegate: Any, responses: tuple[bytes, ...], guard: Any):
        self._delegate = delegate
        self.market = _ReferenceMarket(delegate.market, responses, guard)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


class ReferenceScenarioHook:
    """Consume one explicit event per runner snapshot; exhaustion fails closed."""

    def __init__(self, events: tuple[ReferenceScenarioEvent, ...], *, network: str, managed: bool, client: Any):
        require_reference_test_runtime(network=network, managed=managed)
        self._network = network
        self._managed = managed
        self._client = client
        self._events = events
        self._next = 0
        self._active_market: _ReferenceMarket | None = None
        self.digest = hashlib.sha256(
            b"".join(event.scenario_at.isoformat().encode() + b"".join(event.responses) for event in events)
        ).hexdigest()
        chains = {
            gateway_pb2.ReferencePriceResponse.FromString(raw).chain for event in events for raw in event.responses
        }
        if len(chains) != 1:
            raise ValueError("reference scenario requires observations on exactly one chain")
        self._chain = next(iter(chains))
        for chain in chains:
            result = client.rpc.Call(
                gateway_pb2.RpcRequest(chain=chain, method="web3_clientVersion", params="[]"), timeout=10
            )
            if not result.success or not str(json.loads(result.result)).lower().startswith("anvil"):
                raise ValueError("reference scenario gateway is not connected to Anvil")

    def _guard(self) -> None:
        require_reference_test_runtime(network=self._network, managed=self._managed)

    def assert_consumed(self) -> None:
        """Reject unused observations in the active frame, not future frames."""
        if self._active_market is not None and self._active_market.unconsumed:
            missing = {
                "synthetic": True,
                "scenario_sha256": self.digest,
                "event": self._next,
                "unconsumed": sorted(self._active_market.unconsumed),
            }
            logger.error("SYNTHETIC_REFERENCE_UNCONSUMED %s", json.dumps(missing, sort_keys=True))
            raise ValueError("reference scenario has unconsumed observations in the active frame")

    def wrap_cleanup(self, cleanup: Callable[[], Awaitable[None]]) -> Callable[[], Coroutine[Any, Any, None]]:
        """Validate after risk reduction and cleanup, preserving cleanup failures."""

        async def cleanup_and_validate() -> None:
            await cleanup()
            self.assert_consumed()

        return cleanup_and_validate

    def __call__(self, market: Any) -> None:
        from almanak.framework.market.snapshot import MarketSnapshot

        self._guard()
        if not isinstance(market, MarketSnapshot) or market.chain != self._chain:
            raise ValueError("reference scenario requires a matching single-chain MarketSnapshot")
        self.assert_consumed()
        if self._next >= len(self._events):
            raise ValueError("reference scenario exhausted; no observation or clock is automatically refreshed")
        event = self._events[self._next]
        self._next += 1
        client = _ReferenceClient(self._client, event.responses, self._guard)
        self._active_market = client.market
        market._gateway_client = client
        market._timestamp = event.scenario_at
        controls = {
            "synthetic": True,
            "scenario_sha256": self.digest,
            "event": self._next,
            "scenario_at": event.scenario_at.isoformat(),
        }
        cast(Any, market)._synthetic_reference_scenario = controls
        logger.warning("SYNTHETIC_REFERENCE_SCENARIO %s", json.dumps(controls, sort_keys=True))
