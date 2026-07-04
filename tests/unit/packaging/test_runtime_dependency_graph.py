"""The production runtime dependency graph must not contain dev tooling.

Production images (``almanak/services/backtest/Dockerfile.platform-runner``,
``almanak/services/backtest/Dockerfile``, ``deploy/docker/*``) install from
``uv.lock`` via ``uv sync --frozen --no-dev [--extra ...]``: everything
reachable from the base dependencies or a non-dev extra ships to production.

Polymarket's ``py-order-utils`` and ``poly-eip712-structs`` declare ``pytest``
in ``Requires-Dist`` as a runtime dependency (upstream packaging bug), which
put pytest into every production image until the
``[[tool.uv.dependency-metadata]]`` correction in ``pyproject.toml``. Those
correction entries are version-pinned, so bumping either package silently
falls back to the upstream (broken) metadata — this test walks the locked
graph and fails CI if dev tooling re-enters it, instead of letting it ship.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

ROOT_PACKAGE = "almanak"

# The "dev" extra ([project.optional-dependencies].dev) is publishing/docs
# tooling users opt into explicitly; no production image installs it.
EXCLUDED_ROOT_EXTRAS = frozenset({"dev"})

# Dev-only tools that must never be importable in a production container.
DEV_TOOL_DENYLIST = frozenset(
    {
        "pytest",
        "pytest-asyncio",
        "pytest-cov",
        "pytest-split",
        "pytest-timeout",
        "pytest-xdist",
        "coverage",
        "ruff",
        "mypy",
        "radon",
        "xenon",
        "diff-cover",
    }
)


def _load_lock() -> dict:
    with (REPO_ROOT / "uv.lock").open("rb") as f:
        return tomllib.load(f)


def _dependency_edges(package: dict) -> set[str]:
    """All dependency names of a locked package, extras included.

    Extra-gated and marker-gated dependencies are treated as always present:
    production images run on linux and the lock only records extras that are
    actually enabled somewhere in the graph, so over-approximating keeps the
    walk simple without missing anything that could ship.
    """
    edges = {dep["name"] for dep in package.get("dependencies", [])}
    for extra_deps in package.get("optional-dependencies", {}).values():
        edges.update(dep["name"] for dep in extra_deps)
    return edges


def _runtime_reachable_packages(lock: dict) -> set[str]:
    packages = {p["name"]: p for p in lock["package"]}
    root = packages[ROOT_PACKAGE]

    seeds = {dep["name"] for dep in root.get("dependencies", [])}
    for extra, extra_deps in root.get("optional-dependencies", {}).items():
        if extra not in EXCLUDED_ROOT_EXTRAS:
            seeds.update(dep["name"] for dep in extra_deps)

    reachable: set[str] = set()
    frontier = list(seeds)
    while frontier:
        name = frontier.pop()
        if name in reachable:
            continue
        reachable.add(name)
        if name in packages:
            frontier.extend(_dependency_edges(packages[name]) - reachable)
    return reachable


def test_no_dev_tooling_reachable_from_runtime_graph() -> None:
    lock = _load_lock()
    shipped_dev_tools = _runtime_reachable_packages(lock) & DEV_TOOL_DENYLIST
    assert not shipped_dev_tools, (
        f"Dev tooling {sorted(shipped_dev_tools)} is reachable from the runtime "
        "dependency graph and would ship in every production image. If a "
        "transitive dependency declares it as a runtime requirement (see the "
        "Polymarket packages' pytest bug), correct its metadata with a "
        "version-pinned [[tool.uv.dependency-metadata]] entry in "
        "pyproject.toml and re-run `uv lock`."
    )


def test_polymarket_metadata_correction_matches_locked_versions() -> None:
    """The dependency-metadata pins must match the versions uv resolved.

    A pin/lock mismatch means the correction entry is inert and upstream's
    broken metadata is back in effect; the graph test above catches the
    resulting pytest leak, this one points at the stale entry directly.
    """
    lock = _load_lock()
    with (REPO_ROOT / "pyproject.toml").open("rb") as f:
        pyproject = tomllib.load(f)

    corrections = {
        entry["name"]: entry["version"]
        for entry in pyproject.get("tool", {}).get("uv", {}).get("dependency-metadata", [])
    }
    assert corrections, "expected [[tool.uv.dependency-metadata]] corrections in pyproject.toml"

    locked_versions = {p["name"]: p["version"] for p in lock["package"]}
    stale = {
        name: (pinned, locked_versions.get(name))
        for name, pinned in corrections.items()
        if locked_versions.get(name) != pinned
    }
    assert not stale, (
        f"dependency-metadata pins out of sync with uv.lock: {stale}. Update "
        "the [[tool.uv.dependency-metadata]] entry to the new version's real "
        "metadata (minus dev tooling) or remove it if upstream fixed the bug."
    )
