"""Pytest plugin: opt-in skipping of tests that need locally retained evidence.

Retained evidence under docs/internal/reports/evidence/ is ignored by Git. Missing
evidence is a failure by default; hosted CI passes --skip-missing-evidence so the
modules that depend on that tree are reported as skipped instead of failing, and the
count is printed so a skipped case is never mistaken for a pass.
"""

import ast
from pathlib import Path

import pytest

EVIDENCE = "docs/internal/reports/evidence"
OPTION = "--skip-missing-evidence"
REASON = "missing retained evidence"
KEY = pytest.StashKey[set[Path]]()
ACTIVE = pytest.StashKey[bool]()


def references_evidence(source):
    return any(
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and "reports/evidence" in node.value
        for node in ast.walk(source)
    )


def imported_modules(source, path, tests):
    modules = []
    for node in ast.walk(source):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = ".".join(path.relative_to(tests.parent).with_suffix("").parts[: -node.level])
                module = base + ("." + node.module if node.module else "")
            else:
                module = node.module or ""
            modules.append(module)
            modules.extend(module + "." + alias.name for alias in node.names)
    return [m for m in modules if m == "tests" or m.startswith("tests.")]


def module_path(module, tests):
    parts = module.split(".")[1:]
    if not parts:
        return None
    relative = Path(*parts)
    for candidate in (tests / relative.with_suffix(".py"), tests / relative / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def dependent_modules(tests):
    """Test modules that read the evidence tree, directly or through imported helpers."""
    sources = {}
    for path in sorted(tests.rglob("*.py")):
        try:
            sources[path] = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:
            continue
    # This plugin names the tree without reading it, so it never seeds the closure.
    plugin = Path(__file__).resolve()
    dependent = {
        path
        for path, source in sources.items()
        if path.resolve() != plugin and references_evidence(source)
    }
    imports = {
        path: {
            resolved
            for module in imported_modules(source, path, tests)
            if (resolved := module_path(module, tests)) is not None
        }
        for path, source in sources.items()
    }
    changed = True
    while changed:
        changed = False
        for path, needed in imports.items():
            if path not in dependent and needed & dependent:
                dependent.add(path)
                changed = True
    return dependent


def evidence_root(config):
    return config.rootpath / EVIDENCE


def missing_evidence_file(excinfo, root):
    if excinfo is None or not isinstance(excinfo.value, FileNotFoundError):
        return None
    filename = excinfo.value.filename
    if filename is None:
        return None
    try:
        return Path(filename).resolve().relative_to(root.resolve())
    except ValueError:
        return None


def pytest_addoption(parser):
    parser.addoption(
        OPTION,
        action="store_true",
        help="hosted CI only: report tests that need absent retained evidence as skipped",
    )


def pytest_configure(config):
    active = bool(config.getoption(OPTION)) and not evidence_root(config).exists()
    config.stash[ACTIVE] = active
    config.stash[KEY] = dependent_modules(config.rootpath / "tests") if active else set()


def pytest_ignore_collect(collection_path, config):
    if collection_path in config.stash[KEY]:
        return True
    return None


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    report = yield
    # A present evidence tree with one missing capture is a real failure.
    if report.failed and item.config.stash[ACTIVE]:
        missing = missing_evidence_file(call.excinfo, evidence_root(item.config))
        if missing is not None:
            report.outcome = "skipped"
            report.longrepr = (str(item.path), item.location[1] or 0, f"{REASON}: {missing}")
    return report


def pytest_terminal_summary(terminalreporter, config):
    if not config.stash[ACTIVE]:
        return
    modules = config.stash[KEY]
    tests = sorted(p.name for p in modules if p.name.startswith("test_"))
    skips = [
        r
        for r in terminalreporter.stats.get("skipped", ())
        if isinstance(r.longrepr, tuple) and str(r.longrepr[2]).startswith(REASON)
    ]
    terminalreporter.write_line(
        f"evidence-skips: {len(tests)} test modules and {len(modules) - len(tests)} helper "
        f"modules not collected, {len(skips)} tests skipped at run time; retained evidence "
        f"under {EVIDENCE} is absent. Skipped cases are not passes."
    )
