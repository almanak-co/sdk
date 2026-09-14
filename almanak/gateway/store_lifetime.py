"""Admission and quiescence for one gateway's operational store."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import wraps
from threading import Condition
from typing import Concatenate, Protocol


class StoreLifetime:
    """Keep independent operations concurrent while making close terminal."""

    def __init__(self) -> None:
        self._condition = Condition()
        self._active = 0
        self._closing = False
        self._closed = False
        self._close_error: BaseException | None = None

    @contextmanager
    def operation(self) -> Iterator[None]:
        with self._condition:
            if self._closing or self._closed:
                raise RuntimeError("Gateway store is closed")
            self._active += 1
        try:
            yield
        finally:
            with self._condition:
                self._active -= 1
                if self._closing:
                    self._condition.notify_all()

    def close(self, release: Callable[[], None]) -> None:
        with self._condition:
            while self._closing and not self._closed:
                self._condition.wait()
            if self._closed:
                if self._close_error is not None:
                    raise RuntimeError("Gateway store close previously failed") from self._close_error
                return
            self._closing = True
            self._condition.notify_all()
            while self._active:
                self._condition.wait()
        try:
            release()
        except BaseException as exc:
            self._close_error = exc
            raise
        finally:
            with self._condition:
                self._closed = True
                self._condition.notify_all()


class _OwnedStore(Protocol):
    _lifetime: StoreLifetime


def store_operation[Store: _OwnedStore, **Params, Result](
    method: Callable[Concatenate[Store, Params], Result],
) -> Callable[Concatenate[Store, Params], Result]:
    @wraps(method)
    def guarded(self: Store, /, *args: Params.args, **kwargs: Params.kwargs) -> Result:
        with self._lifetime.operation():
            return method(self, *args, **kwargs)

    return guarded
