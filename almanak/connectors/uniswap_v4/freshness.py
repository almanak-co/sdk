"""Time and canonicality checks over gateway-observed V4 quote headers."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from almanak.framework.execution.interfaces import ConnectorValidationError
from almanak.framework.venues.provider import GatewayBlockIdentity


@dataclass(frozen=True, slots=True)
class QuoteFreshnessObservation:
    """Immutable evidence suitable for recording a refused execution attempt."""

    quote: GatewayBlockIdentity
    head: GatewayBlockIdentity
    expected_quote_hash: str
    observed_at: int
    max_age_seconds: int
    max_clock_skew_seconds: int
    managed_fork: bool = False


class QuoteFreshnessError(ConnectorValidationError):
    """A pre-submission refusal carrying the observations that caused it."""

    def __init__(self, reason: str, observation: QuoteFreshnessObservation) -> None:
        self.reason = reason
        self.observation = observation
        super().__init__(
            f"V4 quote freshness refused ({reason}): {json.dumps(asdict(observation), sort_keys=True)}",
            code=reason,
            evidence=asdict(observation),
        )


def validate_quote_freshness(observation: QuoteFreshnessObservation) -> None:
    """Check elapsed seconds independently of block production frequency.

    Wall time also bounds age so a stalled or lagging RPC cannot make an old
    quote appear current merely by returning two equally old headers.
    """
    for field in ("observed_at", "max_age_seconds", "max_clock_skew_seconds"):
        value = getattr(observation, field)
        if type(value) is not int or value <= 0:
            raise ValueError(f"V4 freshness {field} must be a positive integer")
    if type(observation.managed_fork) is not bool:
        raise ValueError("V4 freshness managed_fork must be a boolean")
    quote, head = observation.quote, observation.head
    age_clock = head.timestamp if observation.managed_fork else max(head.timestamp, observation.observed_at)
    reason = None
    if quote.block_hash != observation.expected_quote_hash:
        reason = "quote_reorganized"
    elif head.number < quote.number:
        reason = "head_behind_quote"
    elif head.number == quote.number and head != quote:
        reason = "inconsistent_head"
    elif head.timestamp < quote.timestamp:
        reason = "timestamp_inversion"
    elif not observation.managed_fork and (
        abs(observation.observed_at - head.timestamp) > observation.max_clock_skew_seconds
    ):
        reason = "head_clock_skew"
    elif age_clock - quote.timestamp > observation.max_age_seconds:
        reason = "quote_stale"
    if reason is not None:
        raise QuoteFreshnessError(reason, observation)
