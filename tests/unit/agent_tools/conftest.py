"""Agent-tools test isolation.

Redirects ``XDG_CACHE_HOME`` to a per-test tmp dir so the persistent bundle
cache never writes to the developer's real ``~/.cache/almanak/bundles/``
during unit tests. Applied automatically to every test in this package.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _isolate_bundle_cache(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    yield
