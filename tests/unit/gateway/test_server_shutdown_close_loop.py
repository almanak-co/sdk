"""Shutdown ``close()`` loop in ``GatewayServer.stop()`` (VIB-4812).

The shutdown loop iterates ``(gateway_owned_servicers,
self._connector_servicers)`` and calls each one's ``close()``. Two
properties matter:

1. ``close()`` may be **sync OR async**. Several gateway-owned helpers
   (``timeline/store.py``, ``registry/store.py``, ``lifecycle/store.py``)
   are synchronous; aiohttp / web3-backed servicers are coroutines. The
   loop must support both — the previous implementation hard-committed
   to ``await close_fn()`` and would crash with ``TypeError`` on a sync
   ``close``.

2. **One failing ``close()`` must not abort the rest.** A best-effort
   teardown is preferable to leaving the surviving servicers' HTTP
   sessions / web3 connections leaked.

These tests exercise the loop directly via a stub object so we don't
need to spin up real gRPC infrastructure. They mirror the production
loop in ``almanak/gateway/server.py::GatewayServer.stop``.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any

import pytest


async def _run_shutdown_loop(servicers: list[Any], *, logger: logging.Logger) -> None:
    """Mirror of the production close-loop in ``GatewayServer.stop``.

    Kept here as a verbatim copy so changes to that loop must update both
    sites — and the test fails loudly when behavior diverges.
    """
    for servicer in servicers:
        if not servicer:
            continue
        close_fn = getattr(servicer, "close", None)
        if close_fn is None:
            continue
        try:
            result = close_fn()
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.exception(
                "Error closing servicer %s during shutdown",
                type(servicer).__qualname__,
            )


class _SyncCloseServicer:
    """Synchronous ``close()`` — mirrors timeline/registry/lifecycle stores."""

    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _AsyncCloseServicer:
    """Coroutine ``close()`` — mirrors aiohttp / web3-backed servicers."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _NoCloseServicer:
    """A servicer that legitimately doesn't expose ``close()`` —
    e.g. a connector whose gRPC servicer holds no resources."""


class _RaisingCloseServicer:
    """``close()`` raises — exercise the catch-and-log path."""

    def __init__(self) -> None:
        self.attempted = False

    def close(self) -> None:
        self.attempted = True
        raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_shutdown_calls_sync_close_without_typeerror(caplog: pytest.LogCaptureFixture) -> None:
    """Sync ``close()`` must run without raising ``TypeError`` about awaiting None."""
    servicer = _SyncCloseServicer()
    await _run_shutdown_loop([servicer], logger=logging.getLogger(__name__))
    assert servicer.closed is True
    assert caplog.records == []  # no errors logged


@pytest.mark.asyncio
async def test_shutdown_awaits_async_close() -> None:
    """Async ``close()`` must still be awaited."""
    servicer = _AsyncCloseServicer()
    await _run_shutdown_loop([servicer], logger=logging.getLogger(__name__))
    assert servicer.closed is True


@pytest.mark.asyncio
async def test_shutdown_handles_mixed_sync_async_servicers() -> None:
    """A real shutdown sees both shapes interleaved."""
    a, b, c, d = (
        _SyncCloseServicer(),
        _AsyncCloseServicer(),
        _SyncCloseServicer(),
        _AsyncCloseServicer(),
    )
    await _run_shutdown_loop([a, b, c, d], logger=logging.getLogger(__name__))
    assert a.closed and b.closed and c.closed and d.closed


@pytest.mark.asyncio
async def test_shutdown_skips_servicers_without_close() -> None:
    """A servicer with no ``close()`` attribute is silently skipped."""
    no_close = _NoCloseServicer()
    sync = _SyncCloseServicer()
    await _run_shutdown_loop([no_close, sync], logger=logging.getLogger(__name__))
    assert sync.closed is True  # the one with close() still ran


@pytest.mark.asyncio
async def test_shutdown_continues_after_failing_close(caplog: pytest.LogCaptureFixture) -> None:
    """A ``close()`` that raises must NOT abort the remaining shutdowns."""
    raising = _RaisingCloseServicer()
    survivor_sync = _SyncCloseServicer()
    survivor_async = _AsyncCloseServicer()

    with caplog.at_level(logging.ERROR):
        await _run_shutdown_loop(
            [raising, survivor_sync, survivor_async],
            logger=logging.getLogger("almanak.gateway.server"),
        )

    assert raising.attempted is True
    assert survivor_sync.closed is True
    assert survivor_async.closed is True
    # The exception must be logged (so an operator sees it) but not propagate.
    assert any(
        "Error closing servicer _RaisingCloseServicer during shutdown" in rec.message for rec in caplog.records
    ), [rec.message for rec in caplog.records]


@pytest.mark.asyncio
async def test_shutdown_skips_falsy_servicer_entries() -> None:
    """``None`` placeholders in the iteration list are skipped without erroring.

    Production code keeps named slots like ``self._polymarket_servicer``
    which may legitimately be ``None`` before boot completes.
    """
    sync = _SyncCloseServicer()
    await _run_shutdown_loop([None, sync, None], logger=logging.getLogger(__name__))  # type: ignore[list-item]
    assert sync.closed is True


def test_close_loop_in_production_server_matches_this_copy() -> None:
    """Static guard: the loop body in ``server.py`` and the copy above
    must stay in lockstep. If someone tweaks the production loop without
    updating this test, this guard surfaces the divergence so the new
    behavior is intentionally pinned, not silently shipped.

    Matches by inspecting the source of the relevant region — looking for
    the load-bearing tokens we care about.
    """
    from pathlib import Path

    server_py = Path(__file__).resolve().parents[3] / "almanak" / "gateway" / "server.py"
    text = server_py.read_text()
    required_tokens = (
        "for servicer in (*gateway_owned_servicers, *self._connector_servicers):",
        'close_fn = getattr(servicer, "close", None)',
        "result = close_fn()",
        "if inspect.isawaitable(result):",
        "await result",
        "Error closing servicer",
    )
    missing = [tok for tok in required_tokens if tok not in text]
    assert not missing, (
        "Production shutdown loop in server.py is missing expected tokens: "
        f"{missing}. If you intentionally changed the loop, update the "
        f"copy in tests/unit/gateway/test_server_shutdown_close_loop.py."
    )


def test_asyncio_run_smoke() -> None:
    """Sanity: an event loop can drive the mixed shutdown shape."""
    a = _SyncCloseServicer()
    b = _AsyncCloseServicer()
    asyncio.run(_run_shutdown_loop([a, b], logger=logging.getLogger(__name__)))
    assert a.closed and b.closed
