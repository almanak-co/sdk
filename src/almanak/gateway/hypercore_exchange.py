"""Posting signed HyperCore actions to Hyperliquid's `/exchange` (blueprint 30, section 5.2).

The gateway posts only bytes the runtime durably recorded as one admitted attempt, and
returns either the venue's answer in a closed shape or an explicit unknown outcome. A
lost answer never authorizes a fresh request for the same attempt: a resend posts the
same bytes, and HyperCore's nonce set refuses a second application of them.

The answers are closed from the mainnet canary's captures: `ok` with `default` for
`updateLeverage` and `noop`, `ok` with per-order statuses for `order` (`filled` with its
oid and cloid, `resting`, or an `error` string), and `err` with a message for an action
refused whole, such as one past its `expiresAfter` ("Action already expired"). Phase 4's
answers are closed from the API documentation alone, never captured: `ok` with per-cancel
statuses (`success` or an `error` string) for `cancel`, and `default` for
`updateIsolatedMargin`. The documentation shows no answer for `scheduleCancel`; it is
taken to answer `default`, and an answer outside every shape is an unknown outcome,
never a guess.
"""

from typing import Annotated, Literal

import httpx
from pydantic import Field, TypeAdapter, ValidationError

from almanak.gateway.auth import loopback_endpoint
from almanak.gateway.hypercore_shapes import Cloid, DecimalText
from almanak.models import Model, UInt

API = "https://api.hyperliquid.xyz/exchange"
EXPIRED = "Action already expired"


class Filled(Model):
    totalSz: DecimalText
    avgPx: DecimalText
    oid: UInt
    cloid: Cloid | None = None


class Resting(Model):
    oid: UInt
    cloid: Cloid | None = None


class FilledStatus(Model):
    filled: Filled


class RestingStatus(Model):
    resting: Resting


class ErrorStatus(Model):
    error: Annotated[str, Field(max_length=512)]


class OrderData(Model):
    statuses: tuple[FilledStatus | RestingStatus | ErrorStatus, ...]


class OrderResponse(Model):
    type: Literal["order"]
    data: OrderData


class CancelData(Model):
    statuses: tuple[Literal["success"] | ErrorStatus, ...]


class CancelResponse(Model):
    type: Literal["cancel"]
    data: CancelData


class DefaultResponse(Model):
    type: Literal["default"]


class Accepted(Model):
    status: Literal["ok"]
    response: Annotated[
        OrderResponse | CancelResponse | DefaultResponse, Field(discriminator="type")
    ]


class Refused(Model):
    status: Literal["err"]
    response: Annotated[str, Field(max_length=512)]


Answer = Annotated[Accepted | Refused, Field(discriminator="status")]
ANSWER: TypeAdapter = TypeAdapter(Answer)


class UnknownOutcome(Exception):
    """The request may have reached HyperCore; only the attempt's evidence can tell."""


class HyperCoreExchange:
    """Posts recorded `/exchange` bodies to the mainnet API, or in a fork workspace only to
    the synthesized model's loopback service."""

    def __init__(self, *, model: str | None = None, timeout: float = 10.0):
        self.api = API if model is None else loopback_endpoint(model) + "/exchange"
        self.client = httpx.Client(timeout=timeout, trust_env=False)

    def post(self, body: bytes) -> Accepted | Refused:
        try:
            response = self.client.post(
                self.api, content=body, headers={"Content-Type": "application/json"}
            )
        except httpx.HTTPError:
            raise UnknownOutcome("the exchange request failed in flight") from None
        if response.status_code != 200:
            raise UnknownOutcome(f"the exchange answered HTTP {response.status_code}")
        try:
            return ANSWER.validate_json(response.content)
        except ValidationError:
            raise UnknownOutcome("the exchange answer differs from its shape") from None

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        self.close()
