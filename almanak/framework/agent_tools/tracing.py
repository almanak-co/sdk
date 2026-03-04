"""Structured decision tracing for agent tool executions.

Every tool call through the ToolExecutor is automatically traced, capturing
the full lifecycle: arguments -> policy check -> execution result -> timing.
This is not optional and not prompt-dependent -- the framework guarantees
a complete audit trail.

Trace entries are written to a configurable sink (in-memory, file, or custom
callback). Entries within the same agent loop iteration share a correlation ID
for grouping related calls.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Fields whose values must never appear in trace output.
SENSITIVE_FIELDS = frozenset(
    {
        "private_key",
        "seed",
        "mnemonic",
        "password",
        "api_key",
        "secret",
        "secret_key",
        "access_token",
        "refresh_token",
    }
)

_REDACTED = "***REDACTED***"


# ---------------------------------------------------------------------------
# Trace entry
# ---------------------------------------------------------------------------


@dataclass
class TraceEntry:
    """A single tool call trace entry.

    Captures the complete lifecycle of one tool execution:
    input arguments, policy decision, execution outcome, timing, and
    any state changes that resulted.
    """

    trace_id: str
    correlation_id: str
    timestamp: datetime
    tool_name: str
    args: dict
    policy_result: dict | None
    execution_result: dict | None
    error: str | None
    duration_ms: float
    state_delta: dict | None = None


# ---------------------------------------------------------------------------
# Trace sinks
# ---------------------------------------------------------------------------


class TraceSink:
    """Base class for trace output destinations."""

    def write(self, entry: TraceEntry) -> None:
        """Write a single trace entry."""

    def flush(self) -> None:
        """Flush any buffered output."""


class InMemoryTraceSink(TraceSink):
    """Stores traces in memory. Default sink for development and testing.

    Args:
        max_entries: Maximum entries to retain. Oldest are evicted when
            the limit is reached. Defaults to 10 000.
    """

    def __init__(self, max_entries: int = 10_000) -> None:
        self.entries: list[TraceEntry] = []
        self._max_entries = max_entries

    def write(self, entry: TraceEntry) -> None:
        self.entries.append(entry)
        if len(self.entries) > self._max_entries:
            self.entries = self.entries[-self._max_entries :]

    def flush(self) -> None:
        pass  # nothing to flush


class FileTraceSink(TraceSink):
    """Writes one JSON line per trace entry to a file.

    Uses buffered append-mode writes. Call ``flush()`` to ensure all
    entries are persisted (e.g., before process exit).

    Supports context manager protocol for automatic cleanup::

        with FileTraceSink("trace.jsonl") as sink:
            tracer = DecisionTracer(sink=sink)
            ...
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._file = open(self._path, "a", encoding="utf-8")  # noqa: SIM115

    def write(self, entry: TraceEntry) -> None:
        line = json.dumps(_entry_to_serializable(entry), default=str)
        self._file.write(line + "\n")

    def flush(self) -> None:
        if self._file and not self._file.closed:
            self._file.flush()

    def close(self) -> None:
        """Close the underlying file handle."""
        if self._file and not self._file.closed:
            self._file.close()

    def __enter__(self) -> FileTraceSink:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __del__(self) -> None:
        # Safety net: close if GC collects before explicit close.
        try:
            self.close()
        except Exception:  # noqa: BLE001
            pass


class CallbackTraceSink(TraceSink):
    """Calls a user-provided function for each trace entry.

    Useful for piping traces into custom observability systems
    (e.g., structured logging, metrics, external dashboards).
    """

    def __init__(self, callback: Callable[[TraceEntry], Any]) -> None:
        self._callback = callback

    def write(self, entry: TraceEntry) -> None:
        self._callback(entry)

    def flush(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Decision tracer
# ---------------------------------------------------------------------------


class DecisionTracer:
    """Automatic structured tracing for all agent tool executions.

    Records the full decision lifecycle at the ToolExecutor level.
    Not optional, not prompt-dependent -- every tool call is traced.

    Args:
        sink: Where to write trace entries. Defaults to ``InMemoryTraceSink``.
        correlation_id: Initial correlation ID. A random UUID is generated
            if not provided.
        max_entries: Maximum in-memory entries retained. Oldest entries are
            evicted when the limit is reached. Defaults to 10 000.
    """

    _DEFAULT_MAX_ENTRIES = 10_000

    def __init__(
        self,
        sink: TraceSink | None = None,
        correlation_id: str | None = None,
        max_entries: int | None = None,
    ) -> None:
        effective_max = max_entries or self._DEFAULT_MAX_ENTRIES
        self._sink = sink or InMemoryTraceSink(max_entries=effective_max)
        self._correlation_id = correlation_id or str(uuid.uuid4())
        self._max_entries = effective_max
        self._entries: list[TraceEntry] = []

    # -- Public API ---------------------------------------------------------

    @property
    def correlation_id(self) -> str:
        """Current correlation ID."""
        return self._correlation_id

    @property
    def sink(self) -> TraceSink:
        """The active trace sink."""
        return self._sink

    def new_correlation(self) -> str:
        """Start a new correlation group (e.g., new agent loop iteration).

        Returns the new correlation ID.
        """
        self._correlation_id = str(uuid.uuid4())
        return self._correlation_id

    def trace_tool_call(
        self,
        tool_name: str,
        args: dict,
        policy_result: dict | None,
        execution_result: dict | None,
        error: str | None,
        duration_ms: float,
        state_delta: dict | None = None,
    ) -> TraceEntry:
        """Record a complete tool call trace.

        This is called automatically by ``ToolExecutor.execute()`` in its
        ``finally`` block, so every tool call (success, failure, or policy
        denial) is captured.

        Returns the created ``TraceEntry``.
        """
        entry = TraceEntry(
            trace_id=str(uuid.uuid4()),
            correlation_id=self._correlation_id,
            timestamp=datetime.now(UTC),
            tool_name=tool_name,
            args=args,
            policy_result=policy_result,
            execution_result=execution_result,
            error=error,
            duration_ms=duration_ms,
            state_delta=state_delta,
        )
        self._entries.append(entry)
        # Evict oldest entries when the cap is reached to prevent unbounded growth.
        if len(self._entries) > self._max_entries:
            self._entries = self._entries[-self._max_entries :]
        try:
            self._sink.write(entry)
        except Exception:  # noqa: BLE001
            logger.debug("Failed to write trace entry to sink (non-fatal)")
        return entry

    def get_entries(self, correlation_id: str | None = None) -> list[TraceEntry]:
        """Get trace entries, optionally filtered by correlation ID."""
        if correlation_id:
            return [e for e in self._entries if e.correlation_id == correlation_id]
        return list(self._entries)

    def get_summary(self) -> dict:
        """Get a summary of all traced decisions.

        Returns a dict with aggregate statistics useful for dashboards
        and post-mortem analysis.
        """
        if not self._entries:
            return {
                "total_calls": 0,
                "successful": 0,
                "failed": 0,
                "policy_denied": 0,
                "unique_tools": 0,
                "total_duration_ms": 0.0,
                "avg_duration_ms": 0.0,
                "correlation_groups": 0,
            }

        successful = sum(
            1
            for e in self._entries
            if e.error is None and e.execution_result is not None and e.execution_result.get("status") != "error"
        )
        failed = sum(
            1
            for e in self._entries
            if e.error is not None or (e.execution_result is not None and e.execution_result.get("status") == "error")
        )
        policy_denied = sum(
            1 for e in self._entries if e.policy_result is not None and not e.policy_result.get("allowed", True)
        )
        unique_tools = len({e.tool_name for e in self._entries})
        total_duration = sum(e.duration_ms for e in self._entries)
        correlation_groups = len({e.correlation_id for e in self._entries})

        return {
            "total_calls": len(self._entries),
            "successful": successful,
            "failed": failed,
            "policy_denied": policy_denied,
            "unique_tools": unique_tools,
            "total_duration_ms": total_duration,
            "avg_duration_ms": total_duration / len(self._entries),
            "correlation_groups": correlation_groups,
        }


# ---------------------------------------------------------------------------
# Argument sanitization
# ---------------------------------------------------------------------------


def _to_snake_case(name: str) -> str:
    """Convert camelCase/PascalCase to snake_case for key matching."""
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name).lower()


def sanitize_args(args: dict) -> dict:
    """Redact potentially sensitive fields before tracing.

    Recursively walks the dict (including dicts nested inside lists)
    and replaces values whose keys match ``SENSITIVE_FIELDS`` with a
    redaction marker. Keys are normalized to snake_case before matching,
    so ``privateKey``, ``private_key``, and ``PRIVATE_KEY`` are all redacted.
    """
    sanitized: dict = {}
    for k, v in args.items():
        key_normalized = _to_snake_case(k) if isinstance(k, str) else ""
        if key_normalized in SENSITIVE_FIELDS:
            sanitized[k] = _REDACTED
        elif isinstance(v, dict):
            sanitized[k] = sanitize_args(v)
        elif isinstance(v, list):
            sanitized[k] = [sanitize_args(item) if isinstance(item, dict) else item for item in v]
        else:
            sanitized[k] = v
    return sanitized


# ---------------------------------------------------------------------------
# Serialization helper
# ---------------------------------------------------------------------------


def _entry_to_serializable(entry: TraceEntry) -> dict:
    """Convert a TraceEntry to a JSON-serializable dict."""
    d = asdict(entry)
    # datetime -> ISO string
    if isinstance(d.get("timestamp"), datetime):
        d["timestamp"] = d["timestamp"].isoformat()
    # Handle Decimal values anywhere in the dict
    return _convert_decimals(d)


def _convert_decimals(obj: Any) -> Any:
    """Recursively convert Decimal values to strings for JSON serialization."""
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_decimals(item) for item in obj]
    return obj
