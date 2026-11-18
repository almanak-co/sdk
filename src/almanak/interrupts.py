"""Defer CLI interrupts so library validation cannot reinterpret KeyboardInterrupt."""

import signal
from collections.abc import Callable, Iterator
from contextlib import contextmanager


@contextmanager
def deferred_sigint() -> Iterator[Callable[[], None]]:
    requested = False

    def record(signum, frame):
        nonlocal requested
        requested = True

    def check():
        if requested:
            raise KeyboardInterrupt

    previous = signal.signal(signal.SIGINT, record)
    try:
        yield check
        check()
    finally:
        signal.signal(signal.SIGINT, previous)
