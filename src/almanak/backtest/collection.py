"""Bounded provider work, independent of chain and connector semantics."""

import threading
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from typing import Annotated, Any

from pydantic import Field

from almanak.models import Model


class CollectionLimits(Model):
    max_in_flight: Annotated[int, Field(ge=1, le=64)] = 4
    batch_size: Annotated[int, Field(ge=1, le=32)] = 32
    requests_per_second: Annotated[float, Field(gt=0, allow_inf_nan=False)] | None = None
    max_retries: Annotated[int, Field(ge=0, le=8)] = 3


# Provider requests answered without a throttle before one more may overlap again.
RECOVERY_REQUESTS = 64


class ProviderGate:
    def __init__(self, limits: CollectionLimits):
        self.limits = limits
        # Requests that may overlap. A provider's throttle halves it; it grows back one
        # at a time, so a burst that was too much is not repeated at once.
        self.allowed = limits.max_in_flight
        self.calm = 0
        self.active = 0
        self.state = threading.Condition()
        self.next_start = 0.0

    def same_budget(self, limits):
        return (self.limits.max_in_flight, self.limits.requests_per_second) == (
            limits.max_in_flight,
            limits.requests_per_second,
        )

    @contextmanager
    def admit(self):
        with self.state:
            while self.active >= self.allowed:
                self.state.wait()
            self.active += 1
        answered = False
        try:
            self.pace()
            yield
            answered = True
        finally:
            with self.state:
                self.active -= 1
                if answered:
                    self.recover()
                self.state.notify_all()

    def recover(self):
        self.calm += 1
        if self.calm >= RECOVERY_REQUESTS and self.allowed < self.limits.max_in_flight:
            self.allowed, self.calm = self.allowed + 1, 0

    def pace(self):
        rate = self.limits.requests_per_second
        with self.state:
            now = time.monotonic()
            wait = max(0.0, self.next_start - now)
            if rate is not None:
                self.next_start = max(now, self.next_start) + 1 / rate
        if wait:
            time.sleep(wait)

    def hold(self, seconds: float, *, throttled: bool = False):
        """Delay every later admission by `seconds`; a throttle also halves the overlap."""
        with self.state:
            self.next_start = max(self.next_start, time.monotonic() + seconds)
            if throttled:
                self.allowed, self.calm = max(1, self.allowed // 2), 0


def bounded_map(function, items, workers):
    """Keep at most `workers` outstanding tasks; yield in declared input order."""
    items = iter(items)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending: deque[Future[Any]] = deque()
        for _ in range(workers):
            item = next(items, None)
            if item is not None:
                pending.append(executor.submit(function, item))
        while pending:
            yield pending.popleft().result()
            item = next(items, None)
            if item is not None:
                pending.append(executor.submit(function, item))
