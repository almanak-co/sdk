"""Smoke test for connector manifest-owned compiler registrations.

Connector manifests publish compiler ``ImportRef`` values that resolve a
per-protocol ``BaseProtocolCompiler`` subclass at runtime. The values are
loaded lazily, so a typo (renamed class, moved module, ``balancer ->
balancer_v2`` rename) fails only at compile-time of an intent for that
protocol - i.e. potentially in production, days after the bad commit.

This smoke test imports every loader target at test time and asserts it
resolves to a subclass of ``BaseProtocolCompiler``. Catches drift early.

Why this test exists: CodeRabbit auto-review on PR #2447 flagged the
runtime-loaded shape as a regression risk. The cost is low and the
catch is high-value.
"""

from __future__ import annotations

import pytest

from almanak.connectors._strategy_base.base.compiler import BaseProtocolCompiler
from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry


@pytest.mark.parametrize("protocol", CompilerRegistry.supported_protocols())
def test_manifest_compiler_loaders_are_importable(protocol: str) -> None:
    """Each loader target imports cleanly and yields a ``BaseProtocolCompiler`` subclass.

    A typo in ``module_path`` (file moved, package renamed) surfaces as
    ``ModuleNotFoundError``. A typo in ``class_name`` (class renamed)
    surfaces as ``AttributeError``. Either failure points the operator
    straight at the broken cell.
    """
    cls = CompilerRegistry._load_class(protocol)

    assert isinstance(cls, type), f"{protocol}: loader did not resolve to a class"
    assert issubclass(cls, BaseProtocolCompiler), (
        f"{protocol}: {cls.__qualname__!r} does not inherit from BaseProtocolCompiler"
    )
