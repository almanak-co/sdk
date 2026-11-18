"""Optional provider caps shared by all transports in one gateway process."""

import os
import threading
from collections import deque
from time import monotonic
from typing import Literal

Category = Literal["rpc", "coingecko", "morpho", "hyperliquid"]
VARIABLES: dict[Category, tuple[str, float]] = {
    "rpc": ("ALMANAK_GATEWAY_RPC_REQUESTS_PER_SECOND", 1.0),
    "coingecko": ("ALMANAK_GATEWAY_COINGECKO_REQUESTS_PER_MINUTE", 60.0),
    "morpho": ("ALMANAK_GATEWAY_MORPHO_REQUESTS_PER_MINUTE", 60.0),
    # Hyperliquid limits its API by request weight, so this cap counts weight too.
    "hyperliquid": ("ALMANAK_GATEWAY_HYPERLIQUID_WEIGHT_PER_MINUTE", 60.0),
}


def configured_limit(category: Category) -> tuple[int | None, float]:
    variable, period = VARIABLES[category]
    value = os.environ.get(variable)
    if value is None:
        return None, period
    if not value.isascii() or not value.isdecimal():
        raise ValueError(f"{variable} must be a non-negative integer")
    return int(value), period


class Window:
    def __init__(self, maximum: int, period: float):
        self.maximum = maximum
        self.period = period
        # Each admission as [time, units still counted]; a list, so a settled
        # reservation can return what it did not use.
        self.events: deque[list] = deque()
        self.used = 0
        self.lock = threading.Lock()

    def admit(self, units: int) -> list | None:
        with self.lock:
            now = monotonic()
            while self.events and self.events[0][0] <= now - self.period:
                expired = self.events.popleft()
                self.used -= expired[1]
                expired[1] = 0
            if self.used + units > self.maximum:
                return None
            event = [now, units]
            self.events.append(event)
            self.used += units
            return event

    def release(self, event: list, units: int) -> None:
        # An expired admission no longer counts, so it has nothing to return.
        with self.lock:
            returned = min(units, event[1])
            event[1] -= returned
            self.used -= returned


_windows: dict[Category, Window] = {}
_windows_lock = threading.Lock()


def shared_window(category: Category, maximum: int | None, period: float) -> Window | None:
    # A new reader or provider alias must not create another allowance.
    with _windows_lock:
        window = _windows.get(category)
        if window is not None:
            if window.maximum != maximum:
                raise ValueError("restart the gateway to change its provider rate caps")
            return window
        if maximum is None:
            return None
        window = Window(maximum, period)
        _windows[category] = window
        return window


class ProviderLimit:
    def __init__(self, category: Category):
        self.category = category
        self.maximum, self.period = configured_limit(category)
        self._window = shared_window(category, self.maximum, self.period)

    def admit(self, units: int = 1) -> None:
        self.reserve(units)

    def reserve(self, units: int) -> "Reservation":
        """Admit the most a request can cost; settling returns what it did not use."""
        from almanak.gateway.ethereum import DataError

        if type(units) is not int or units < 1:
            raise ValueError("provider request units must be a positive integer")
        event = None
        if self._window is not None:
            event = self._window.admit(units)
            if event is None:
                raise DataError("rate_limit", f"{self.category}: gateway rate cap exhausted")
        return Reservation(self._window, event, units)


class Reservation:
    def __init__(self, window: Window | None, event: list | None, units: int):
        self.window = window
        self.event = event
        self.units = units

    def settle(self, used: int) -> None:
        # A request never costs more than it reserved; an unsettled reservation keeps
        # its whole charge, as a failed request does.
        if self.window is not None and self.event is not None and used < self.units:
            self.window.release(self.event, self.units - used)
            self.units = used
