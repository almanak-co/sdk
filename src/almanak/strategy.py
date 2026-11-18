"""A strategy's author sources, captured as part of a durable deployment.

`source` is strategy.py. `modules` holds the sibling modules it may import (today only
`parameters.py`, the shared typed parameter model); they run with the strategy, so they
are part of `sha256`, the source hash permissions bind to. `dashboard` is dashboard.py:
display only, read and run only to build the dashboard, so it stays outside `sha256` and
does not make two definitions of the same strategy unequal.
"""

import ast
import hashlib
import json
import sys
import types
from pathlib import Path

from pydantic import Field

from almanak.models import Model

# Sibling author modules a strategy or dashboard may import, by module name.
AUTHOR_MODULES = ("parameters",)
DASHBOARD_FILE = "dashboard.py"


def defines(source: str, name: str) -> bool:
    """Whether `source` binds `name` at its top level (a def, class or assignment)."""
    for node in ast.parse(source).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if node.name == name:
                return True
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(t, ast.Name) and t.id == name for t in targets):
                return True
    return False


class StrategyDefinition(Model):
    filename: str
    source: str
    modules: dict[str, str] = Field(default={}, exclude_if=lambda value: not value)
    dashboard: str | None = Field(default=None, exclude_if=lambda value: value is None)

    @classmethod
    def read(cls, path: Path) -> "StrategyDefinition":
        path = Path(path)
        source = path.read_text()
        compile(source, str(path), "exec")
        modules, dashboard = siblings(path.parent)
        if dashboard is not None and defines(source, "dashboard"):
            raise ValueError(
                f"both {DASHBOARD_FILE} and {path.name} define dashboard(context); keep it in "
                f"{DASHBOARD_FILE} and remove the hook from {path.name}"
            )
        return cls(filename=path.name, source=source, modules=modules, dashboard=dashboard)

    @property
    def sha256(self) -> str:
        if not self.modules:
            return hashlib.sha256(self.source.encode()).hexdigest()
        identity = {"source": self.source, "modules": dict(sorted(self.modules.items()))}
        return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()

    def __eq__(self, other):
        # The dashboard is display only: editing it does not change the strategy.
        if not isinstance(other, StrategyDefinition):
            return NotImplemented
        return (self.filename, self.source, self.modules) == (
            other.filename,
            other.source,
            other.modules,
        )

    def __hash__(self):
        return hash((self.filename, self.source, tuple(sorted(self.modules.items()))))


def siblings(root: Path) -> tuple[dict[str, str], str | None]:
    modules = {}
    for name in AUTHOR_MODULES:
        file = Path(root) / f"{name}.py"
        if file.is_file():
            text = file.read_text()
            compile(text, str(file), "exec")
            modules[name] = text
    file = Path(root) / DASHBOARD_FILE
    dashboard = None
    if file.is_file():
        dashboard = file.read_text()
        compile(dashboard, str(file), "exec")
    return modules, dashboard


def install_modules(definition: StrategyDefinition) -> dict[str, types.ModuleType]:
    """Load the sibling author modules from their captured sources, inside the sandbox.

    The sandbox cannot read the workspace, so `import parameters` resolves to the module
    built here from the definition, never to a file.
    """
    import linecache

    installed = {}
    for name, text in definition.modules.items():
        if name not in AUTHOR_MODULES:
            raise ValueError(f"unknown author module {name!r}")
        filename = f"{name}.py"
        linecache.cache[filename] = (len(text), None, text.splitlines(keepends=True), filename)
        module = types.ModuleType(name)
        module.__file__ = filename
        sys.modules[name] = module
        exec(compile(text, filename, "exec"), module.__dict__)
        installed[name] = module
    return installed


def dashboard_namespace(definition: StrategyDefinition) -> dict:
    """The namespace a dashboard hook runs in: dashboard.py alone when the workspace has
    one, else strategy.py (whose `dashboard` hook is the fallback)."""
    import linecache

    modules = install_modules(definition)
    filename, source = (
        (DASHBOARD_FILE, definition.dashboard)
        if definition.dashboard is not None
        else (definition.filename, definition.source)
    )
    linecache.cache[filename] = (len(source), None, source.splitlines(keepends=True), filename)
    namespace = {"__name__": "__dashboard__", "__file__": filename, "__author_modules__": modules}
    exec(compile(source, filename, "exec"), namespace)
    return namespace


class DecisionRequest[Context](Model):
    strategy: StrategyDefinition
    context: Context


class ValidationRequest[Config](Model):
    strategy: StrategyDefinition
    config: Config


class OutcomeRequest[Context](Model):
    strategy: StrategyDefinition
    context: Context


class EventRequest[Context](Model):
    strategy: StrategyDefinition
    context: Context


class EconomicDecisionRequest[Context](DecisionRequest[Context]):
    pass


class EconomicOutcomeRequest[Context](OutcomeRequest[Context]):
    pass


class EconomicValidationRequest[Config](ValidationRequest[Config]):
    pass


class DataDependencyRequest[Config](Model):
    strategy: StrategyDefinition
    config: Config


class EconomicDataDependencyRequest[Config](DataDependencyRequest[Config]):
    pass
