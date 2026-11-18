"""Diagnostics and memoized checks that live for exactly one gateway request.

Nothing here is authority. A memoized check is reused only inside the request that ran it,
so every privileged request still performs it once; timings only reach the operator log.
"""

import time
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

# Seconds after a gateway request starts past which no provider read retry may wait. The
# client times a request out after 60 s, and one more provider attempt can take its 15 s
# timeout, so a request that meets a brownout answers with a transient refusal in time
# for the coordinator to hold instead of losing the answer.
READ_DEADLINE_SECONDS = 40.0


@dataclass
class RequestScope:
    rpc: Counter = field(default_factory=Counter)
    # Monotonic time after which provider read retries stop for this request.
    read_deadline: float = field(default_factory=lambda: time.monotonic() + READ_DEADLINE_SECONDS)
    # Accumulated wall seconds by activity, such as provider reads or the journal lock.
    seconds: dict[str, float] = field(default_factory=dict)
    memo: dict = field(default_factory=dict)
    # HTTP exchanges with the provider; a JSON-RPC batch is one however many calls it holds.
    round_trips: int = 0

    def log_fields(self) -> dict:
        fields: dict[str, Any] = {
            "rpc_calls": sum(self.rpc.values()),
            "rpc_round_trips": self.round_trips,
        }
        fields.update({f"{k}_ms": round(v * 1000, 1) for k, v in sorted(self.seconds.items())})
        if self.rpc:
            fields["rpc"] = dict(sorted(self.rpc.items()))
        return fields


_scope: ContextVar[RequestScope | None] = ContextVar("gateway_request_scope", default=None)


@contextmanager
def request_scope() -> Iterator[RequestScope]:
    scope = RequestScope()
    token = _scope.set(scope)
    try:
        yield scope
    finally:
        _scope.reset(token)


def read_deadline() -> float | None:
    """The current gateway request's read deadline, or None outside a request."""
    scope = _scope.get()
    return None if scope is None else scope.read_deadline


def count_rpc(method: str, units: int = 1) -> None:
    scope = _scope.get()
    if scope is not None:
        scope.rpc[method] += units


def count_round_trip() -> None:
    scope = _scope.get()
    if scope is not None:
        scope.round_trips += 1


@contextmanager
def timed(activity: str) -> Iterator[None]:
    started = time.monotonic()
    try:
        yield
    finally:
        scope = _scope.get()
        if scope is not None:
            elapsed = time.monotonic() - started
            scope.seconds[activity] = scope.seconds.get(activity, 0.0) + elapsed


def once(key: Any, check: Callable[[], Any]) -> Any:
    """Run `check` once per request for `key`; failures are never retained."""
    scope = _scope.get()
    if scope is None:
        return check()
    if key not in scope.memo:
        scope.memo[key] = check()
    return scope.memo[key]
