"""Package-local default event loop for legacy sync tests.

Several test modules here are SYNC tests that drive async stores via
``asyncio.get_event_loop().run_until_complete(...)``. pytest-asyncio (used by
async tests elsewhere in the suite) unsets the worker's default event loop
when its own tests finish, so under pytest-xdist the legacy pattern raises
``RuntimeError: There is no current event loop`` depending entirely on which
tests happened to be scheduled earlier on the same worker — a
scheduling-order flake (observed on PR #2731 when a merge from main
reshuffled the distribution).

Pin a fresh default loop per test and tear it down after. Async
(pytest-asyncio) tests in this package are unaffected — the plugin manages
its own loop and ignores the default.
"""

from __future__ import annotations

import asyncio

import pytest


@pytest.fixture(autouse=True)
def _default_event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield
    asyncio.set_event_loop(None)
    loop.close()
