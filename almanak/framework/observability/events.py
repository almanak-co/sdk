"""Structured forensic events for strategy observability.

ForensicEvent captures what happened at each phase of the strategy lifecycle
with enough context for post-mortem debugging without full event-sourcing.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)


class StrategyPhase(StrEnum):
    """Strategy lifecycle phases for forensic event correlation.

    These are higher-level than ExecutionPhase (which covers tx-level steps
    like SIGNING, SUBMISSION). StrategyPhase covers the decide->execute cycle.
    """

    DECIDE = "DECIDE"
    COMPILE = "COMPILE"
    VALIDATE = "VALIDATE"
    EXECUTE = "EXECUTE"
    ENRICH = "ENRICH"


@dataclass
class ForensicEvent:
    """Structured forensic event for debugging and observability.

    Schema: {cycle_id, timestamp, phase, event_type, payload}
    Stored as JSON lines via ObserveService.RecordTimelineEvent.
    """

    cycle_id: str
    timestamp: datetime
    phase: StrategyPhase
    event_type: str
    deployment_id: str
    payload: dict[str, Any] = field(default_factory=dict)

    def to_json_line(self) -> str:
        """Serialize to a single JSON line for file-based storage."""
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        d["phase"] = self.phase.value
        return json.dumps(d, default=str)

    def to_details_dict(self) -> dict[str, Any]:
        """Convert to a dict suitable for timeline event details_json."""
        # Payload first so reserved fields cannot be overwritten
        result = dict(self.payload)
        result["cycle_id"] = self.cycle_id
        result["phase"] = self.phase.value
        return result

    @classmethod
    def create(
        cls,
        *,
        cycle_id: str,
        phase: StrategyPhase,
        event_type: str,
        deployment_id: str,
        **payload: Any,
    ) -> ForensicEvent:
        """Create a forensic event with current timestamp."""
        return cls(
            cycle_id=cycle_id,
            timestamp=datetime.now(UTC),
            phase=phase,
            event_type=event_type,
            deployment_id=deployment_id,
            payload=payload,
        )
