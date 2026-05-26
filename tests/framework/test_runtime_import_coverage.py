"""Regression guard: every runtime module under ``almanak.framework``,
``almanak.gateway``, and ``almanak.connectors`` must import cleanly.

Background: the gateway sidecar's lazy ``__init__.py`` cascade (PR #1969)
defers framework module loading until first access. That's a real
cold-start RSS win (the rc2 OOM-at-512Mi was largely module-load
allocations), but it also means any module that would fail at import
time — bad import cycle, missing optional dep, typo in a top-level
constant — now surfaces only when the specific code path runs in
production rather than at gateway boot.

This test is the complement to ``test_imports_lean.py``: that suite
asserts *forbidden* heavy modules stay out of ``sys.modules`` after
gateway boot. This one asserts *every* runtime module CAN be imported
on demand. Run together they pin both halves of the lazy contract.

Coverage: walks ``almanak.framework``, ``almanak.gateway``, and
``almanak.connectors`` recursively and tries ``importlib.import_module``
on every discovered module. ``almanak.connectors`` is the post-VIB-4835
home for both strategy-side connector code and the gateway-side
``<protocol>/gateway/`` subtree, so it is on the runtime-import surface
the same way ``almanak.framework`` is. Skips:
- ``*.tests.*`` (defensive — connector unit tests have been consolidated
  into ``tests/unit/connectors/`` so no in-tree test packages should exist
  under any connector package; the filter is retained so a stray
  re-introduction is silently tolerated rather than failing CI).
- ``*.__main__`` (CLI entry points that ``sys.exit()`` on import — running
  them as modules is a separate concern).

If a new module fails this test, the failure message names the module and
the import error. The most common causes:
- A circular import introduced by a refactor.
- A top-level ``from X import Y`` referencing a name that no longer exists.
- A new optional-dep import at module top level instead of function scope.

Subprocess execution: pytest itself loads many modules (numpy/pandas via
plugins) and the lazy-imports feature mutates global ``sys.modules`` state.
Run in a fresh subprocess so the import probe sees the same module-load
sequence a freshly-booted gateway / strategy container would.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap


def _walk_and_import_in_subprocess() -> tuple[list[dict[str, str]], int, int]:
    """Walk almanak.framework + almanak.gateway + almanak.connectors and import every module.

    Returns ``(failures, imported_count, skipped_count)``.
    """
    script = textwrap.dedent(
        """
        import contextlib
        import importlib
        import io
        import json
        import pkgutil
        import sys

        failures = []
        imported = 0
        skipped = 0
        # Suppress before importing the root packages — almanak.framework and
        # almanak.gateway register click CLI groups at import time and would
        # otherwise pollute stdout, breaking the JSON payload at the bottom.
        # Also: do not catch BaseException in the per-module import — that
        # would swallow KeyboardInterrupt and make the subprocess unkillable.
        # Catch (Exception, SystemExit) only; SystemExit covers __main__-style
        # CLI scripts that exit on import (those are filtered by the .__main__
        # name check anyway, but we belt-and-brace).
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            import almanak.connectors
            import almanak.framework
            import almanak.gateway

            for pkg in (almanak.framework, almanak.gateway, almanak.connectors):
                for mod_info in pkgutil.walk_packages(pkg.__path__, prefix=pkg.__name__ + "."):
                    name = mod_info.name
                    if ".tests." in name or name.endswith(".__main__") or name.endswith(".tests"):
                        skipped += 1
                        continue
                    imported += 1
                    try:
                        importlib.import_module(name)
                    except (Exception, SystemExit) as e:
                        failures.append({"module": name, "error": f"{type(e).__name__}: {e}"})

        sys.__stdout__.write(json.dumps({"failures": failures, "imported": imported, "skipped": skipped}))
        """
    )
    env = os.environ.copy()
    # Mirror lean-import test: don't auto-discover strategies, keep the
    # subprocess as close to a fresh container as possible.
    env["ALMANAK_STRATEGIES_DIR"] = "/nonexistent_strategies_dir_for_runtime_import_coverage"
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
        env=env,
        timeout=180,
    )
    payload = json.loads(result.stdout)
    return payload["failures"], payload["imported"], payload["skipped"]


def test_every_runtime_module_imports_cleanly() -> None:
    failures, imported, skipped = _walk_and_import_in_subprocess()

    # Sanity: the walk must actually find a meaningful number of modules.
    # If this drops far below 1000 it likely means the package layout
    # changed and walk_packages is no longer reaching subpackages.
    # Threshold bumped post-VIB-4835: ``almanak.connectors`` was added to
    # the walk roots after the strategy-side connector home moved out of
    # ``almanak.framework.connectors``; the resulting count is ~1100.
    assert imported >= 1000, (
        f"runtime-import-coverage walk only reached {imported} modules "
        f"(expected >= 1000); package layout may have changed"
    )

    if failures:
        lines = [
            f"{len(failures)} runtime module(s) failed to import "
            f"({imported} attempted, {skipped} skipped):",
            "",
        ]
        for f in failures:
            lines.append(f"  - {f['module']}: {f['error']}")
        lines += [
            "",
            "Likely causes:",
            "  - A circular import introduced by a refactor",
            "  - A top-level `from X import Y` where Y no longer exists",
            "  - A new optional-dep import at module top level (move to function scope)",
        ]
        raise AssertionError("\n".join(lines))
