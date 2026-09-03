"""CLI command: ``almanak strat check`` — pre-flight validation for a strategy.

This command is a fast, no-network pre-flight that a PM (or human operator)
can run against a strategy directory before attempting to execute it. It
complements ``strat run --dry-run``:

``--dry-run`` compiles intents, which requires providers, networks, and a full
strategy init path. ``check`` is deliberately lighter — it does not connect to
the gateway, does not compile intents, and is safe to run in CI or on a laptop
without any credentials.

Three layers, in order (each layer runs even if earlier layers produced
warnings, so the operator sees the full picture per invocation):

1. Load + validate
   - Load the EFFECTIVE config through the shared validation engine
     (``config_validation.load_effective_config``): parse + schema-validate
     ``config.json`` / ``config.yaml``, then apply the hosted
     ``ALMANAK_STRATEGY_CONFIG`` deep-merge exactly as the runtime does —
     ``check`` validates the config the strategy actually runs with, so
     "check passed, deployment invalid" drift is impossible (VIB-5986).
   - Validate the optional per-strategy ``CONFIG_MODEL`` Pydantic contract
     and scan for present-but-empty market-identity keys (the placeholder
     shape that shipped the 3-day fail-closed HOLD incident).
   - Import ``strategy.py`` and locate the concrete ``IntentStrategy`` class.
   - Instantiate it with the loaded config; the framework calls
     ``validate_config()`` from ``IntentStrategy.__init__`` (a separate hook
     added by a parallel work item). If that hook raises
     ``ConfigValidationError``, we format ``field`` + ``message``.
   - If ``validate_config`` isn't available on the installed SDK yet, we
     fall through to AST-level checks (the AttributeError is swallowed) so
     this CLI still works during the rollout window.

2. AST scan (works even if instantiation fails)
   - Placeholder addresses (``0x_SET_...``, ``REPLACE_ME``, ``0xDEADBEEF``,
     zero address) in strategy source or config values.
   - Empty ``generate_teardown_intents()`` bodies (returns ``[]`` or only
     ``pass``) — an operator close request would silently no-op.
   - Strategies that import ``PositionInfo`` but never override
     ``get_open_positions`` (operators can't see positions during teardown).
   - Stateful templates missing persistence hooks.

3. Template-aware heuristics (best-effort inferences based on scaffold shape)
   - Perps template should surface a ``direction`` config field.
   - Lending template should surface a ``min_health_factor`` config field.
   - LP template should surface a fee-tier / pool config field.

Exit codes:
    0  clean (no findings)
    1  warnings present, but nothing blocking
    2  errors present (at least one must-fix)

Output modes:
    default: human-readable, colorized sections
    --json : a stable JSON object (PM will consume this)
"""

from __future__ import annotations

import ast
import importlib.util
import json
import logging
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

import click

from almanak.framework.anvil.accounts import ANVIL_DEFAULT_ADDRESS

logger = logging.getLogger(__name__)


# =============================================================================
# Finding model
# =============================================================================


class Severity(StrEnum):
    """Finding severity. ``ERROR`` drives exit code 2, ``WARNING`` drives 1."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class Layer(StrEnum):
    """Which check layer produced the finding (for --json consumers)."""

    LOAD = "load"
    AST = "ast"
    TEMPLATE = "template"


@dataclass
class Finding:
    """A single ``strat check`` finding.

    ``field`` and ``line`` are optional so AST-only findings and top-level
    errors can both share this shape. The ``code`` is a short stable slug that
    PM / CI tooling can key off of when filtering.
    """

    severity: Severity
    layer: Layer
    code: str
    message: str
    file: str | None = None
    line: int | None = None
    field: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-friendly dict (enums flattened to their string values)."""
        data = asdict(self)
        data["severity"] = self.severity.value
        data["layer"] = self.layer.value
        return data


@dataclass
class CheckReport:
    """Aggregated report for one ``strat check`` invocation."""

    strategy_dir: str
    findings: list[Finding] = field(default_factory=list)
    strategy_class: str | None = None
    template: str | None = None

    def add(self, finding: Finding) -> None:
        self.findings.append(finding)

    def has_errors(self) -> bool:
        return any(f.severity == Severity.ERROR for f in self.findings)

    def has_warnings(self) -> bool:
        return any(f.severity == Severity.WARNING for f in self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_dir": self.strategy_dir,
            "strategy_class": self.strategy_class,
            "template": self.template,
            "findings": [f.to_dict() for f in self.findings],
            "summary": {
                "errors": sum(1 for f in self.findings if f.severity == Severity.ERROR),
                "warnings": sum(1 for f in self.findings if f.severity == Severity.WARNING),
                "infos": sum(1 for f in self.findings if f.severity == Severity.INFO),
            },
        }


# =============================================================================
# Placeholder detection (shared between AST + config scan)
# =============================================================================


# These patterns catch the most common "didn't fill this in" footguns in
# scaffolded strategies. Keep the list short and high-signal: false positives
# here erode trust in the whole command.
_PLACEHOLDER_ADDRESSES: tuple[str, ...] = (
    "0x0000000000000000000000000000000000000000",
    "0xDEADBEEF",
    "0xdeadbeef",
    "REPLACE_ME",
)

_PLACEHOLDER_PREFIXES: tuple[str, ...] = (
    "0x_SET_",
    "0X_SET_",
)


def _is_placeholder_value(value: str) -> str | None:
    """Return the matched placeholder token if ``value`` looks like a stub.

    We match both exact literals and known prefixes. This is deliberately
    lenient (case-insensitive on the literal list) so copy-paste variations
    are caught.
    """
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped:
        return None

    lowered = stripped.lower()
    for placeholder in _PLACEHOLDER_ADDRESSES:
        if lowered == placeholder.lower():
            return placeholder

    for prefix in _PLACEHOLDER_PREFIXES:
        if stripped.startswith(prefix) or lowered.startswith(prefix.lower()):
            return prefix

    return None


# =============================================================================
# Layer 1: Load + validate (instantiate strategy, trigger validate_config)
# =============================================================================


def _load_effective_config(strategy_dir: Path, report: CheckReport) -> tuple[dict[str, Any] | None, Path | None]:
    """Load the effective config via the shared engine, folding findings into ``report``.

    Delegates to ``config_validation.load_effective_config`` — the same
    parse + hosted-override merge the runtime uses — and maps the engine's
    harness-neutral findings into this CLI's ``Finding`` shape.
    """
    from .config_validation import load_effective_config

    config, config_path, engine_findings = load_effective_config(strategy_dir)
    for ef in engine_findings:
        report.add(
            Finding(
                severity=Severity.ERROR if ef.severity == "error" else Severity.WARNING,
                layer=Layer.LOAD,
                code=ef.code,
                message=ef.message,
                file=str(config_path) if config_path else None,
                field=ef.field,
            )
        )
    return config, config_path


def _apply_engine_config_findings(
    strategy_class: type | None,
    config: dict[str, Any] | None,
    config_path: Path | None,
    report: CheckReport,
) -> None:
    """CONFIG_MODEL contract + market-identity placeholder scan (shared engine)."""
    from .config_validation import config_model_findings, market_identity_findings

    engine_findings = list(market_identity_findings(config))
    if strategy_class is not None:
        engine_findings.extend(config_model_findings(strategy_class, config))

    for ef in engine_findings:
        report.add(
            Finding(
                severity=Severity.ERROR if ef.severity == "error" else Severity.WARNING,
                layer=Layer.LOAD,
                code=ef.code,
                message=ef.message,
                file=str(config_path) if config_path else None,
                field=ef.field,
            )
        )


def _find_strategy_file(strategy_dir: Path) -> Path | None:
    """Locate the strategy entry point in ``strategy_dir``.

    Convention matches ``strat run`` / ``strat teardown``: the file is named
    ``strategy.py`` at the directory root.
    """
    candidate = strategy_dir / "strategy.py"
    return candidate if candidate.exists() else None


def _try_load_strategy_class(strategy_file: Path) -> tuple[type | None, str | None]:
    """Import ``strategy_file`` and return the concrete ``IntentStrategy`` subclass.

    We mirror ``teardown.load_strategy_from_file`` so behaviour is consistent
    across commands (add the strategy dir to ``sys.path`` so local imports
    resolve, pick the most-derived local class when multiple are present).
    """
    try:
        from ..strategies.intent_strategy import IntentStrategy
    except Exception as exc:  # pragma: no cover - SDK import failure
        return None, f"Failed to import IntentStrategy base: {exc}"

    strategy_dir = str(strategy_file.parent)
    inserted = False
    if strategy_dir not in sys.path:
        sys.path.insert(0, strategy_dir)
        inserted = True

    # Use a unique module name so repeated invocations in the same process
    # (e.g. inside the test suite) don't collide. We intentionally do not
    # cache the module — ``check`` is a one-shot CLI.
    module_name = f"_strat_check_{abs(hash(str(strategy_file.resolve())))}"

    try:
        spec = importlib.util.spec_from_file_location(module_name, strategy_file)
        if spec is None or spec.loader is None:
            return None, f"Could not create module spec for {strategy_file}"
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception as exc:
            return None, f"Error importing strategy: {exc}"

        candidates: list[type] = []
        for attr_name in dir(module):
            obj = getattr(module, attr_name)
            if (
                isinstance(obj, type)
                and obj is not IntentStrategy
                and issubclass(obj, IntentStrategy)
                and not getattr(obj, "__abstractmethods__", frozenset())
            ):
                candidates.append(obj)

        if not candidates:
            return None, "No concrete IntentStrategy subclass found in strategy.py"

        # Prefer classes defined locally in this module so we don't accidentally
        # pick up an imported base like StatelessStrategy.
        local = [c for c in candidates if c.__module__ == module.__name__]
        if local:
            candidates = local
        # Prefer the most-derived class (greatest MRO depth). This keeps the
        # loader deterministic when a strategy file defines a helper base
        # class in addition to the concrete strategy, AND it gives
        # ``_ast_scan_strategy_file`` a single anchor class to align on.
        candidates.sort(key=lambda c: len(c.__mro__), reverse=True)
        return candidates[0], None
    finally:
        if inserted:
            try:
                sys.path.remove(strategy_dir)
            except ValueError:
                pass


def _instantiate_strategy(
    strategy_class: type,
    config: dict[str, Any] | None,
) -> tuple[Any, list[Finding]]:
    """Instantiate the strategy.

    This triggers T1's ``validate_config`` hook in ``IntentStrategy.__init__``.
    We catch ``ConfigValidationError`` and convert it to a structured finding,
    and we also handle any other instantiation error without crashing the CLI
    so the AST layer can still run.

    Returns ``(instance_or_None, findings)``.
    """
    findings: list[Finding] = []

    # Resolve chain/wallet in a way that doesn't require live gateway/RPC.
    # Priority: config["chain"] -> decorator default_chain -> "arbitrum".
    chain: str | None = None
    if isinstance(config, dict):
        cfg_chain = config.get("chain")
        if isinstance(cfg_chain, str) and cfg_chain.strip():
            chain = cfg_chain.strip().lower()
    if not chain:
        metadata = getattr(strategy_class, "STRATEGY_METADATA", None)
        if metadata is not None:
            default_chain = getattr(metadata, "default_chain", "") or ""
            if default_chain:
                chain = default_chain
            else:
                supported = getattr(metadata, "supported_chains", []) or []
                if supported:
                    chain = supported[0]
    if not chain:
        chain = "arbitrum"

    # Use the well-known Anvil account so addresses are syntactically valid
    # without requiring a real key. No execution happens during ``check``.
    wallet = ANVIL_DEFAULT_ADDRESS

    import inspect as _inspect

    from ._strategy_config import coerce_strategy_config

    try:
        # Coerce through the SAME path the runner uses (dataclass resolution,
        # Decimal conversion, DictConfigWrapper fallback) so validate_config()
        # observes the exact config object it would see at boot — a check that
        # validated a different config type than the runtime was the drift
        # class VIB-5986 closes. ``enforce_config_model=False`` because the
        # CONFIG_MODEL contract already ran as its own findings pass; letting
        # coercion re-raise the same violations would duplicate them. Same
        # reasoning for enforce_market_identity=False: scan_market_identity_findings()
        # already ran as its own findings pass below.
        config_instance = coerce_strategy_config(
            strategy_class,
            dict(config) if isinstance(config, dict) else {},
            echo=False,
            enforce_config_model=False,
            enforce_market_identity=False,
        )

        base_kwargs: dict[str, Any] = {
            "config": config_instance,
            "chain": chain,
            "wallet_address": wallet,
        }

        try:
            # Introspect the constructor signature directly from the class — this
            # avoids the mypy ``unsound __init__`` complaint on instance access
            # and still gives us the full parameter list including *args/**kwargs.
            sig = _inspect.signature(strategy_class)
            params = sig.parameters
            has_var_keyword = any(p.kind == _inspect.Parameter.VAR_KEYWORD for p in params.values())
            if not has_var_keyword:
                base_kwargs = {k: v for k, v in base_kwargs.items() if k in params}
        except (TypeError, ValueError):
            # Fall back to the base kwargs — any TypeError below is surfaced as a finding.
            pass

        instance = strategy_class(**base_kwargs)
        return instance, findings
    except AttributeError as exc:
        # T1's validate_config hook may not yet exist on this SDK version.
        # The contract says: swallow AttributeError from a missing hook and
        # fall through. Only swallow the specific case — re-raise other
        # AttributeErrors so real bugs surface.
        message = str(exc)
        if "validate_config" in message:
            logger.debug("validate_config hook not yet available: %s", message)
            # Re-try without validate_config by patching a no-op on the class
            # ONLY if the error came from inside __init__. Easier: just log
            # and continue — the instantiation failed, but AST layer will run.
            findings.append(
                Finding(
                    severity=Severity.INFO,
                    layer=Layer.LOAD,
                    code="validate_config_unavailable",
                    message=(
                        "IntentStrategy.validate_config hook not yet present on this SDK "
                        "version; skipping config-level validation."
                    ),
                )
            )
            return None, findings
        findings.append(
            Finding(
                severity=Severity.ERROR,
                layer=Layer.LOAD,
                code="instantiation_failed",
                message=f"Strategy instantiation raised AttributeError: {exc}",
            )
        )
        return None, findings
    except Exception as exc:
        # ConfigValidationError is the happy-path for "invalid config".
        finding = _format_config_validation_error(exc)
        if finding is not None:
            findings.append(finding)
            return None, findings
        findings.append(
            Finding(
                severity=Severity.ERROR,
                layer=Layer.LOAD,
                code="instantiation_failed",
                message=f"Strategy instantiation failed: {type(exc).__name__}: {exc}",
            )
        )
        return None, findings


def _format_config_validation_error(exc: Exception) -> Finding | None:
    """If ``exc`` is T1's ``ConfigValidationError``, format it as a finding.

    We detect by class name so we don't create a hard import dependency on a
    contract that might not yet be in ``main``. If the class later moves, add
    its module here — the detection is purely nominal.
    """
    name = type(exc).__name__
    if name != "ConfigValidationError":
        return None

    field_name = getattr(exc, "field", None)
    message = getattr(exc, "message", None) or str(exc)
    return Finding(
        severity=Severity.ERROR,
        layer=Layer.LOAD,
        code="config_validation_failed",
        message=message,
        field=field_name if isinstance(field_name, str) else None,
    )


# =============================================================================
# Layer 2: AST scan (works even when instantiation fails)
# =============================================================================


_STRATEGY_BASE_NAMES: tuple[str, ...] = (
    "IntentStrategy",
    "StatelessStrategy",
    "Strategy",
    "StrategyBase",
)

_INVALID_SLIPPAGE_ESTIMATE_FIELDS: frozenset[str] = frozenset(
    {
        "slippage",
        "slippage_pct",
        "price_impact",
        "price_impact_pct",
    }
)

_SLIPPAGE_ESTIMATE_RECEIVER_NAMES: frozenset[str] = frozenset(
    {
        "market",
        "market_snapshot",
        "snap",
        "snapshot",
    }
)


def _is_estimate_slippage_call(node: ast.AST) -> bool:
    """Return whether ``node`` calls the SDK market slippage API.

    Method name alone is insufficient: strategies can define unrelated risk
    models with their own ``estimate_slippage`` result contracts.
    """
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return False
    if node.func.attr != "estimate_slippage":
        return False
    receiver = _binding_key(node.func.value)
    return receiver is not None and receiver[-1] in _SLIPPAGE_ESTIMATE_RECEIVER_NAMES


def _binding_key(node: ast.AST) -> tuple[str, ...] | None:
    """Return a stable key for a simple name or attribute chain."""
    if isinstance(node, ast.Name):
        return (node.id,)
    if isinstance(node, ast.Attribute):
        parent = _binding_key(node.value)
        if parent is not None:
            return (*parent, node.attr)
    return None


def _assignment_binding_keys(node: ast.AST) -> set[tuple[str, ...]]:
    """Return trackable bindings written by an assignment target."""
    key = _binding_key(node)
    if key is not None:
        return {key}
    if isinstance(node, ast.Starred):
        return _assignment_binding_keys(node.value)
    if isinstance(node, ast.Tuple | ast.List):
        return {key for element in node.elts for key in _assignment_binding_keys(element)}
    return set()


def _pattern_binding_names(pattern: ast.pattern) -> set[str]:
    """Return names introduced by one structural-pattern match."""
    names: set[str] = set()
    for node in ast.walk(pattern):
        if isinstance(node, ast.MatchAs | ast.MatchStar) and node.name is not None:
            names.add(node.name)
        elif isinstance(node, ast.MatchMapping) and node.rest is not None:
            names.add(node.rest)
    return names


def _is_irrefutable_pattern(pattern: ast.pattern) -> bool:
    """Return whether a pattern matches every subject when unguarded."""
    if isinstance(pattern, ast.MatchAs):
        return pattern.pattern is None or _is_irrefutable_pattern(pattern.pattern)
    if isinstance(pattern, ast.MatchOr):
        return any(_is_irrefutable_pattern(option) for option in pattern.patterns)
    return False


def _method_receiver_name(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
    """Return a method's receiver name, excluding static methods."""
    if any(isinstance(decorator, ast.Name) and decorator.id == "staticmethod" for decorator in node.decorator_list):
        return None
    positional = (*node.args.posonlyargs, *node.args.args)
    return positional[0].arg if positional else None


def _method_instance_slippage_suffixes(
    member: ast.FunctionDef | ast.AsyncFunctionDef,
    inherited_suffixes: set[tuple[str, ...]],
) -> set[tuple[str, ...]]:
    """Return receiver attributes that can hold estimates when a method exits."""
    receiver = _method_receiver_name(member)
    if receiver is None:
        return set()

    flow = _StrategyASTVisitor.for_instance_flow(receiver, inherited_suffixes)
    flow._visit_statements(member.body)
    assert flow._recorded_slippage_exit_states is not None
    exit_states = list(flow._recorded_slippage_exit_states)
    if flow._flow_reachable:
        exit_states.append(flow._current_slippage_bindings())
    possible_at_exit = set().union(*exit_states)
    return {
        key[1:]
        for key in possible_at_exit
        if len(key) > 1 and key[0] == receiver and key in flow._written_slippage_binding_keys
    }


def _collect_class_instance_slippage_suffixes(node: ast.ClassDef) -> set[tuple[str, ...]]:
    """Collect cross-method receiver provenance to a fixed point.

    A method can copy an estimate cached by another method. Each method is
    seeded only with suffixes exported by *other* methods, which permits those
    aliases without feeding a method's own later assignments back into its
    entry state and losing statement-order precision.
    """
    methods = [member for member in node.body if isinstance(member, ast.FunctionDef | ast.AsyncFunctionDef)]
    method_suffixes: list[set[tuple[str, ...]]] = [set() for _ in methods]

    while True:
        next_suffixes: list[set[tuple[str, ...]]] = []
        for index, member in enumerate(methods):
            inherited = set().union(
                *(suffixes for other_index, suffixes in enumerate(method_suffixes) if other_index != index)
            )
            discovered = _method_instance_slippage_suffixes(member, inherited)
            next_suffixes.append(method_suffixes[index] | discovered)
        if next_suffixes == method_suffixes:
            return set().union(*method_suffixes)
        method_suffixes = next_suffixes


class _FunctionLocalBindingCollector(ast.NodeVisitor):
    """Collect function-local root names without entering nested scopes."""

    def __init__(self) -> None:
        self.names: set[str] = set()
        self.nonlocal_names: set[str] = set()

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Store | ast.Del):
            self.names.add(node.id)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.names.add(node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.names.add(node.name)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.names.add(node.name)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        return

    def visit_ListComp(self, node: ast.ListComp) -> None:
        return

    def visit_SetComp(self, node: ast.SetComp) -> None:
        return

    def visit_DictComp(self, node: ast.DictComp) -> None:
        return

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        return

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.names.add(alias.asname or alias.name.split(".", maxsplit=1)[0])

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            if alias.name != "*":
                self.names.add(alias.asname or alias.name)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.name is not None:
            self.names.add(node.name)
        self.generic_visit(node)

    def visit_Match(self, node: ast.Match) -> None:
        for case in node.cases:
            self.names.update(_pattern_binding_names(case.pattern))
        self.generic_visit(node)

    def visit_Global(self, node: ast.Global) -> None:
        self.nonlocal_names.update(node.names)

    def visit_Nonlocal(self, node: ast.Nonlocal) -> None:
        self.nonlocal_names.update(node.names)


def _function_local_binding_names(node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda) -> set[str]:
    """Return roots that shadow captured bindings inside a function body."""
    arguments = node.args
    names = {
        argument.arg
        for argument in (
            *arguments.posonlyargs,
            *arguments.args,
            *arguments.kwonlyargs,
        )
    }
    if arguments.vararg is not None:
        names.add(arguments.vararg.arg)
    if arguments.kwarg is not None:
        names.add(arguments.kwarg.arg)

    collector = _FunctionLocalBindingCollector()
    if isinstance(node, ast.Lambda):
        collector.visit(node.body)
    else:
        for statement in node.body:
            collector.visit(statement)
    return (names | collector.names) - collector.nonlocal_names


def _default_ast_facts() -> dict[str, Any]:
    """Return the fact dict populated when the AST walk can't run.

    Kept as a helper so read-failure / syntax-error paths share one source of
    truth with the ``_StrategyASTVisitor`` initial state.
    """
    return {
        "imports_position_info": False,
        "overrides_get_open_positions": False,
        "overrides_generate_teardown_intents": False,
        "teardown_body_empty": False,
        "has_on_intent_executed": False,
        "class_name": None,
    }


class _StrategyASTVisitor(ast.NodeVisitor):
    """Single-pass AST visitor folding every Layer-2 heuristic into one walk.

    We keep the heuristics that run against the full module in ``visit_*``
    methods (``visit_Constant`` for placeholders, ``visit_Import`` /
    ``visit_ImportFrom`` for ``PositionInfo`` tracking, ``visit_ClassDef`` for
    strategy-class resolution) so each concern reads like a small, testable
    method instead of a branch in a ~200-line procedure.

    Ordering note: ``ast.NodeVisitor.generic_visit`` is depth-first, whereas
    the previous implementation used ``ast.walk`` (breadth-first). For the
    findings we emit this is equivalent — placeholder nodes are always at
    the same depth as their enclosing statement, so ``lineno`` order is
    preserved. The characterization tests pin that ordering explicitly.

    Nesting note: because DFS visits a nested ``ClassDef`` before any later
    top-level sibling, fallback-class resolution explicitly restricts itself
    to top-level classes (``_class_depth == 0``). A nested strategy class
    inside an unrelated wrapper would otherwise outrank the real top-level
    class and corrupt downstream ``missing_*`` findings — the pre-refactor
    BFS walk was implicitly safe from this.
    """

    def __init__(self, strategy_file: Path, report: CheckReport, target_class_name: str | None) -> None:
        self.strategy_file = strategy_file
        self.report = report
        self.target_class_name = target_class_name
        self.facts: dict[str, Any] = _default_ast_facts()
        self.strategy_class_node: ast.ClassDef | None = None
        self.fallback_class_node: ast.ClassDef | None = None
        # Bindings are statement-ordered and lexical. A module-wide set keyed
        # only by variable name would leak aliases between unrelated methods
        # and keep them alive after reassignment.
        self._slippage_result_scopes: list[set[tuple[str, ...]]] = [set()]
        self._slippage_scope_kinds: list[str] = ["module"]
        self._class_instance_slippage_suffixes: list[set[tuple[str, ...]]] = []
        self._recorded_slippage_exit_states: list[set[tuple[str, ...]]] | None = None
        self._written_slippage_binding_keys: set[tuple[str, ...]] = set()
        self._flow_nested_scope_depth = 0
        self._flow_reachable = True
        # Depth of currently-open ``ClassDef`` nodes. Fallback resolution only
        # fires when this is 0 (i.e. the class being visited is top-level).
        self._class_depth = 0

    @classmethod
    def for_instance_flow(
        cls,
        receiver: str,
        inherited_suffixes: set[tuple[str, ...]],
    ) -> _StrategyASTVisitor:
        """Create the isolated visitor used for class-instance provenance."""
        flow = cls(
            strategy_file=Path("<class-instance-slippage-flow>"),
            report=CheckReport(strategy_dir=""),
            target_class_name=None,
        )
        flow._recorded_slippage_exit_states = []
        flow._slippage_scope_kinds[-1] = "function"
        flow._replace_current_slippage_bindings({(receiver, *suffix) for suffix in inherited_suffixes})
        return flow

    # -- Public entry point ------------------------------------------------

    def run(self, tree: ast.Module) -> dict[str, Any]:
        """Walk ``tree`` and apply class-level heuristics, returning ``facts``."""
        self.visit(tree)
        if self.strategy_class_node is None:
            self.strategy_class_node = self.fallback_class_node
        if self.strategy_class_node is not None:
            self._analyze_strategy_class(self.strategy_class_node)
        return self.facts

    # -- Node visitors -----------------------------------------------------

    def visit_Constant(self, node: ast.Constant) -> None:
        """Flag placeholder string literals wherever they appear."""
        if isinstance(node.value, str):
            hit = _is_placeholder_value(node.value)
            if hit:
                finding = Finding(
                    severity=Severity.ERROR,
                    layer=Layer.AST,
                    code="placeholder_address",
                    message=(
                        f"Placeholder address literal {node.value!r} found in source "
                        f"(matched on: {hit}). Replace with the real value before running."
                    ),
                    file=str(self.strategy_file),
                    line=node.lineno,
                )
                if finding not in self.report.findings:
                    self.report.add(finding)
        # Constant nodes have no meaningful children for our purposes, but
        # we still call ``generic_visit`` to stay consistent with the API.
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """``from x import PositionInfo`` -> mark fact."""
        for alias in node.names:
            if alias.name == "PositionInfo":
                self.facts["imports_position_info"] = True
            if alias.name != "*":
                self._kill_slippage_root(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        """Dotted ``import ...PositionInfo`` also counts."""
        for alias in node.names:
            if alias.name.endswith("PositionInfo"):
                self.facts["imports_position_info"] = True
            self._kill_slippage_root(alias.asname or alias.name.split(".", maxsplit=1)[0])
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        """Update tracked aliases after evaluating an assignment value."""
        self.visit(node.value)
        is_slippage_result = self._is_slippage_result_expression(node.value)
        for target in node.targets:
            self._set_slippage_bindings(target, is_slippage_result=is_slippage_result)
            self.visit(target)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        """Track or kill an annotated assignment binding."""
        self.visit(node.annotation)
        if node.value is not None:
            self.visit(node.value)
        self._set_slippage_bindings(
            node.target,
            is_slippage_result=node.value is not None and self._is_slippage_result_expression(node.value),
        )
        self.visit(node.target)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        """Track aliases introduced by assignment expressions."""
        self.visit(node.value)
        self._set_slippage_bindings(
            node.target,
            is_slippage_result=self._is_slippage_result_expression(node.value),
        )
        self.visit(node.target)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        """An augmented assignment always invalidates a tracked result."""
        self.visit(node.target)
        self.visit(node.value)
        self._set_slippage_bindings(node.target, is_slippage_result=False)

    def visit_Delete(self, node: ast.Delete) -> None:
        """Deleting a name or attribute invalidates its tracked binding."""
        for target in node.targets:
            self.visit(target)
            self._set_slippage_bindings(target, is_slippage_result=False)

    def visit_Return(self, node: ast.Return) -> None:
        """Visit the return value and optionally record a method-exit state."""
        if node.value is not None:
            self.visit(node.value)
        if self._recorded_slippage_exit_states is not None and self._flow_nested_scope_depth == 0:
            self._recorded_slippage_exit_states.append(self._current_slippage_bindings())
        self._flow_reachable = False

    def visit_Raise(self, node: ast.Raise) -> None:
        """Visit raise expressions and retain state from exceptional exits."""
        if node.exc is not None:
            self.visit(node.exc)
        if node.cause is not None:
            self.visit(node.cause)
        if self._recorded_slippage_exit_states is not None and self._flow_nested_scope_depth == 0:
            self._recorded_slippage_exit_states.append(self._current_slippage_bindings())
        self._flow_reachable = False

    def visit_If(self, node: ast.If) -> None:
        """Merge aliases that may survive either conditional branch."""
        self.visit(node.test)
        initial = self._current_slippage_bindings()
        body_state = self._visit_statement_branch(node.body, initial)
        else_state = self._visit_statement_branch(node.orelse, initial) if node.orelse else initial
        self._merge_flow_states([body_state, else_state])

    def visit_IfExp(self, node: ast.IfExp) -> None:
        """Merge assignment-expression effects from either expression arm."""
        self.visit(node.test)
        initial = self._current_slippage_bindings()
        body_state = self._visit_expression_branch(node.body, initial)
        else_state = self._visit_expression_branch(node.orelse, initial)
        self._merge_flow_states([body_state, else_state])

    def visit_BoolOp(self, node: ast.BoolOp) -> None:
        """Preserve paths where boolean evaluation short-circuits."""
        if not node.values:
            return
        self.visit(node.values[0])
        possible = self._current_slippage_bindings()
        for value in node.values[1:]:
            evaluated = self._visit_expression_branch(value, possible)
            possible |= evaluated or set()
            self._replace_current_slippage_bindings(possible)

    def visit_For(self, node: ast.For) -> None:
        """Treat loop targets as rebindings and merge zero/one-plus iterations."""
        self._visit_loop(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:
        """Apply the same conservative flow rules to async loops."""
        self._visit_loop(node)

    def visit_While(self, node: ast.While) -> None:
        """Merge zero-iteration and one-or-more-iteration loop states."""
        self.visit(node.test)
        initial = self._current_slippage_bindings()
        body_state = self._visit_statement_branch(node.body, initial)
        possible_completion = initial | (body_state or set())
        else_state = (
            self._visit_statement_branch(node.orelse, possible_completion) if node.orelse else possible_completion
        )
        self._merge_flow_states([initial, body_state, else_state])

    def visit_With(self, node: ast.With) -> None:
        """Track context-manager target rebindings in execution order."""
        self._visit_with(node)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
        """Track async context-manager target rebindings too."""
        self._visit_with(node)

    def visit_Try(self, node: ast.Try) -> None:
        """Conservatively merge normal and handled-exception flows."""
        self._visit_try(node)

    def visit_TryStar(self, node: ast.TryStar) -> None:
        """Apply the same alias merge to exception-group handlers."""
        self._visit_try(node)

    def visit_Match(self, node: ast.Match) -> None:
        """Merge aliases from every structural-pattern branch."""
        self.visit(node.subject)
        initial = self._current_slippage_bindings()
        exhaustive = any(case.guard is None and _is_irrefutable_pattern(case.pattern) for case in node.cases)
        states: list[set[tuple[str, ...]] | None] = [] if exhaustive else [initial]
        for case in node.cases:
            self._replace_current_slippage_bindings(initial)
            self._flow_reachable = True
            self.visit(case.pattern)
            for name in _pattern_binding_names(case.pattern):
                self._kill_slippage_root(name)
            if case.guard is not None:
                self.visit(case.guard)
            self._visit_statements(case.body)
            states.append(self._current_slippage_bindings() if self._flow_reachable else None)
        self._merge_flow_states(states)

    def visit_ListComp(self, node: ast.ListComp) -> None:
        self._visit_comprehension(node.generators, [node.elt])

    def visit_SetComp(self, node: ast.SetComp) -> None:
        self._visit_comprehension(node.generators, [node.elt])

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        self._visit_comprehension(node.generators, [node.elt])

    def visit_DictComp(self, node: ast.DictComp) -> None:
        self._visit_comprehension(node.generators, [node.key, node.value])

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Keep local aliases from leaking into other functions or methods."""
        self._visit_lexical_scope(node)
        self._kill_slippage_root(node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Keep async-function aliases lexically scoped too."""
        self._visit_lexical_scope(node)
        self._kill_slippage_root(node.name)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        """Keep lambda bindings isolated from their containing scope."""
        self._visit_lexical_scope(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        """Flag invalid direct fields on a known slippage result."""
        if node.attr in _INVALID_SLIPPAGE_ESTIMATE_FIELDS and self._is_slippage_result_expression(node.value):
            self._report_invalid_slippage_field(node.attr, node.lineno)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        """Flag invalid ``getattr(result, field, ...)`` compatibility probes."""
        if (
            isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and self._is_slippage_result_expression(node.args[0])
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
            and node.args[1].value in _INVALID_SLIPPAGE_ESTIMATE_FIELDS
        ):
            self._report_invalid_slippage_field(node.args[1].value, node.lineno)
        self.generic_visit(node)

    def _is_slippage_result_expression(self, node: ast.AST) -> bool:
        """Recognize known aliases and inline ``estimate_slippage`` results."""
        if _is_estimate_slippage_call(node):
            return True
        key = _binding_key(node)
        if key is not None and key in self._slippage_result_scopes[-1]:
            return True
        return (
            isinstance(node, ast.Attribute) and node.attr == "value" and self._is_slippage_result_expression(node.value)
        )

    def _set_slippage_bindings(self, target: ast.AST, *, is_slippage_result: bool) -> None:
        """Set or kill simple bindings written in the current lexical scope."""
        bindings = self._slippage_result_scopes[-1]
        target_keys = _assignment_binding_keys(target)
        if self._recorded_slippage_exit_states is not None and self._flow_nested_scope_depth == 0:
            self._written_slippage_binding_keys.update(target_keys)
        for key in target_keys:
            bindings.difference_update({binding for binding in bindings if binding[: len(key)] == key})
            if is_slippage_result:
                bindings.add(key)

    def _kill_slippage_root(self, name: str) -> None:
        """Invalidate a root name and every attribute binding below it."""
        bindings = self._slippage_result_scopes[-1]
        bindings.difference_update({binding for binding in bindings if binding and binding[0] == name})

    def _current_slippage_bindings(self) -> set[tuple[str, ...]]:
        return set(self._slippage_result_scopes[-1])

    def _replace_current_slippage_bindings(self, bindings: set[tuple[str, ...]]) -> None:
        self._slippage_result_scopes[-1] = set(bindings)

    def _capture_source_bindings(self) -> set[tuple[str, ...]]:
        """Return bindings visible to a nested code object.

        Function and comprehension bodies do not close over a class namespace,
        so skip class scopes while looking for the nearest capture source.
        """
        for bindings, kind in zip(
            reversed(self._slippage_result_scopes),
            reversed(self._slippage_scope_kinds),
            strict=True,
        ):
            if kind != "class":
                return set(bindings)
        return set()

    def _visit_lexical_scope(self, node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda) -> None:
        """Visit outer expressions, then a body with captured non-local aliases."""
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            for decorator in node.decorator_list:
                self.visit(decorator)
            for type_parameter in getattr(node, "type_params", []):
                self.visit(type_parameter)
        for argument in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs):
            if argument.annotation is not None:
                self.visit(argument.annotation)
        if node.args.vararg is not None and node.args.vararg.annotation is not None:
            self.visit(node.args.vararg.annotation)
        if node.args.kwarg is not None and node.args.kwarg.annotation is not None:
            self.visit(node.args.kwarg.annotation)
        for default in (*node.args.defaults, *(value for value in node.args.kw_defaults if value is not None)):
            self.visit(default)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.returns is not None:
            self.visit(node.returns)

        local_names = _function_local_binding_names(node)
        captured = {binding for binding in self._capture_source_bindings() if binding and binding[0] not in local_names}
        if (
            isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
            and self._slippage_scope_kinds[-1] == "class"
            and self._class_instance_slippage_suffixes
        ):
            receiver = _method_receiver_name(node)
            if receiver is not None:
                captured.update((receiver, *suffix) for suffix in self._class_instance_slippage_suffixes[-1])
        self._slippage_result_scopes.append(captured)
        self._slippage_scope_kinds.append("function")
        self._flow_nested_scope_depth += 1
        parent_reachable = self._flow_reachable
        self._flow_reachable = True
        try:
            if isinstance(node, ast.Lambda):
                self.visit(node.body)
            else:
                self._visit_statements(node.body)
        finally:
            self._flow_nested_scope_depth -= 1
            self._slippage_result_scopes.pop()
            self._slippage_scope_kinds.pop()
            self._flow_reachable = parent_reachable

    def _visit_statements(self, statements: list[ast.stmt]) -> None:
        for statement in statements:
            if not self._flow_reachable:
                break
            self.visit(statement)

    def _visit_statement_branch(
        self,
        statements: list[ast.stmt],
        initial: set[tuple[str, ...]],
    ) -> set[tuple[str, ...]] | None:
        self._replace_current_slippage_bindings(initial)
        self._flow_reachable = True
        self._visit_statements(statements)
        return self._current_slippage_bindings() if self._flow_reachable else None

    def _visit_expression_branch(
        self,
        expression: ast.expr,
        initial: set[tuple[str, ...]],
    ) -> set[tuple[str, ...]] | None:
        self._replace_current_slippage_bindings(initial)
        self._flow_reachable = True
        self.visit(expression)
        return self._current_slippage_bindings() if self._flow_reachable else None

    def _merge_flow_states(self, states: Sequence[set[tuple[str, ...]] | None]) -> None:
        """Merge fallthrough states and mark all-terminated flows unreachable."""
        fallthrough = [state for state in states if state is not None]
        self._flow_reachable = bool(fallthrough)
        if fallthrough:
            self._replace_current_slippage_bindings(set().union(*fallthrough))

    def _visit_loop(self, node: ast.For | ast.AsyncFor) -> None:
        self.visit(node.iter)
        initial = self._current_slippage_bindings()
        possible_completion = set(initial)
        loop_entry = set(initial)
        while True:
            self._replace_current_slippage_bindings(loop_entry)
            self._flow_reachable = True
            self._set_slippage_bindings(node.target, is_slippage_result=False)
            self.visit(node.target)
            self._visit_statements(node.body)
            body_state = self._current_slippage_bindings() if self._flow_reachable else None
            expanded = possible_completion | (body_state or set())
            if expanded == possible_completion:
                break
            possible_completion = expanded
            loop_entry = expanded
        else_state = (
            self._visit_statement_branch(node.orelse, possible_completion) if node.orelse else possible_completion
        )
        self._merge_flow_states([initial, possible_completion, else_state])

    def _visit_with(self, node: ast.With | ast.AsyncWith) -> None:
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars is not None:
                self._set_slippage_bindings(item.optional_vars, is_slippage_result=False)
                self.visit(item.optional_vars)
        self._visit_statements(node.body)

    def _visit_try(self, node: ast.Try | ast.TryStar) -> None:
        initial = self._current_slippage_bindings()
        recorded_exit_start = (
            len(self._recorded_slippage_exit_states)
            if self._recorded_slippage_exit_states is not None and self._flow_nested_scope_depth == 0
            else None
        )
        body_state = self._visit_statement_branch(node.body, initial)
        handler_initial = initial | (body_state or set())
        handler_states: list[set[tuple[str, ...]] | None] = []
        for handler in node.handlers:
            self._replace_current_slippage_bindings(handler_initial)
            self._flow_reachable = True
            if handler.type is not None:
                self.visit(handler.type)
            if handler.name is not None:
                self._kill_slippage_root(handler.name)
            self._visit_statements(handler.body)
            if handler.name is not None and self._flow_reachable:
                self._kill_slippage_root(handler.name)
            handler_states.append(self._current_slippage_bindings() if self._flow_reachable else None)
        else_state = (
            self._visit_statement_branch(node.orelse, body_state)
            if node.orelse and body_state is not None
            else body_state
        )
        fallthrough = [state for state in [body_state, else_state, *handler_states] if state is not None]
        if node.finalbody:
            pending_exit_states: list[set[tuple[str, ...]]] = []
            if recorded_exit_start is not None and self._recorded_slippage_exit_states is not None:
                pending_exit_states = self._recorded_slippage_exit_states[recorded_exit_start:]
                del self._recorded_slippage_exit_states[recorded_exit_start:]

            final_state: set[tuple[str, ...]] | None = None
            if fallthrough:
                possible = initial | set().union(*fallthrough)
                final_state = self._visit_statement_branch(node.finalbody, possible)
            elif not pending_exit_states:
                # The normal scanner does not record terminal states, but it
                # still needs to inspect a finally-only path for bad fields.
                self._visit_statement_branch(node.finalbody, initial)

            if self._recorded_slippage_exit_states is not None:
                for exit_state in pending_exit_states:
                    transformed_exit = self._visit_statement_branch(node.finalbody, exit_state)
                    if transformed_exit is not None:
                        # The original return/raise resumes after ``finally``.
                        self._recorded_slippage_exit_states.append(transformed_exit)
            self._merge_flow_states([final_state])
        else:
            self._merge_flow_states(fallthrough)

    def _visit_comprehension(self, generators: list[ast.comprehension], values: list[ast.expr]) -> None:
        if not generators:
            for value in values:
                self.visit(value)
            return

        # Python evaluates the first iterable outside the comprehension's
        # implicit function scope. The targets and all remaining expressions
        # are evaluated inside it.
        self.visit(generators[0].iter)
        outer_state = self._current_slippage_bindings()
        outer_reachable = self._flow_reachable
        captured = self._capture_source_bindings()
        self._slippage_result_scopes.append(captured)
        self._slippage_scope_kinds.append("comprehension")
        try:
            first = generators[0]
            self._set_slippage_bindings(first.target, is_slippage_result=False)
            self.visit(first.target)
            for condition in first.ifs:
                self.visit(condition)
            for generator in generators[1:]:
                self.visit(generator.iter)
                self._set_slippage_bindings(generator.target, is_slippage_result=False)
                self.visit(generator.target)
                for condition in generator.ifs:
                    self.visit(condition)
            for value in values:
                self.visit(value)
        finally:
            self._slippage_result_scopes.pop()
            self._slippage_scope_kinds.pop()
            self._replace_current_slippage_bindings(outer_state)
            self._flow_reachable = outer_reachable

    def _report_invalid_slippage_field(self, field_name: str, line: int) -> None:
        """Emit the actionable ALM-3329 contract finding."""
        finding = Finding(
            severity=Severity.ERROR,
            layer=Layer.AST,
            code="invalid_slippage_estimate_field",
            message=(
                f"SlippageEstimate has no {field_name!r} field. Use "
                "effective_slippage_bps / price_impact_bps directly, or "
                "within_limits(); do not hide SDK contract mismatches behind getattr fallbacks."
            ),
            file=str(self.strategy_file),
            line=line,
            field=field_name,
        )
        if finding not in self.report.findings:
            self.report.add(finding)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Resolve the concrete strategy class by name or by base.

        Target-name matches win regardless of nesting (the loader may pick a
        nested class via ``__qualname__``). Fallback resolution is restricted
        to top-level classes to match the pre-refactor BFS behaviour.
        """
        if (
            self.target_class_name is not None
            and self.strategy_class_node is None
            and node.name == self.target_class_name
        ):
            self.strategy_class_node = node
        elif self._class_depth == 0 and self.fallback_class_node is None and self._has_strategy_base(node):
            self.fallback_class_node = node

        # Bases, keywords, decorators, and type parameters are evaluated in
        # the enclosing scope, before the class namespace exists.
        for decorator in node.decorator_list:
            self.visit(decorator)
        for base in node.bases:
            self.visit(base)
        for keyword in node.keywords:
            self.visit(keyword)
        for type_parameter in getattr(node, "type_params", []):
            self.visit(type_parameter)

        # Placeholder detection is source-wide rather than reachability-based.
        # Pre-scan the class body so a class-level raise cannot hide literals
        # in later statements; normal traversal remains flow-sensitive.
        for statement in node.body:
            for descendant in ast.walk(statement):
                if isinstance(descendant, ast.Constant):
                    self.visit_Constant(descendant)

        self._class_depth += 1
        self._slippage_result_scopes.append(self._current_slippage_bindings())
        self._slippage_scope_kinds.append("class")
        self._class_instance_slippage_suffixes.append(_collect_class_instance_slippage_suffixes(node))
        try:
            self._visit_statements(node.body)
        finally:
            self._class_instance_slippage_suffixes.pop()
            self._slippage_result_scopes.pop()
            self._slippage_scope_kinds.pop()
            self._class_depth -= 1
        self._kill_slippage_root(node.name)

    # -- Class resolution helpers -----------------------------------------

    @staticmethod
    def _base_name(base: ast.expr) -> str | None:
        """Return the identifier used as a class-base, if any.

        Handles plain ``Name`` bases, ``pkg.Base`` attribute bases, and
        ``Generic[T]``-style subscripted bases whose value is a ``Name``.
        """
        if isinstance(base, ast.Name):
            return base.id
        if isinstance(base, ast.Attribute):
            return base.attr
        if isinstance(base, ast.Subscript) and isinstance(base.value, ast.Name):
            return base.value.id
        return None

    def _has_strategy_base(self, node: ast.ClassDef) -> bool:
        """True if any of the class's bases names a known strategy base."""
        return any(self._base_name(b) in _STRATEGY_BASE_NAMES for b in node.bases)

    # -- Strategy-class analysis (runs once the target class is resolved) --

    def _analyze_strategy_class(self, class_node: ast.ClassDef) -> None:
        """Populate method-override facts and emit missing/empty findings."""
        self.facts["class_name"] = class_node.name
        inherits_stateless = self._inherits_stateless(class_node)
        self.facts["inherits_stateless"] = inherits_stateless

        method_defs = self._collect_methods(class_node)
        self._check_teardown(method_defs, inherits_stateless)
        self._check_get_open_positions(method_defs, inherits_stateless)
        self.facts["has_on_intent_executed"] = "on_intent_executed" in method_defs

    @staticmethod
    def _inherits_stateless(class_node: ast.ClassDef) -> bool:
        """``class X(StatelessStrategy)`` or ``class X(pkg.StatelessStrategy)``."""
        for base in class_node.bases:
            if isinstance(base, ast.Name) and base.id == "StatelessStrategy":
                return True
            if isinstance(base, ast.Attribute) and base.attr == "StatelessStrategy":
                return True
        return False

    @staticmethod
    def _collect_methods(
        class_node: ast.ClassDef,
    ) -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
        """Return ``{name: FunctionDef}`` for direct methods on the class."""
        return {item.name: item for item in class_node.body if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef)}

    def _check_teardown(
        self,
        method_defs: dict[str, ast.FunctionDef | ast.AsyncFunctionDef],
        inherits_stateless: bool,
    ) -> None:
        """Teardown override: absent -> warning; trivial body -> empty warning."""
        if "generate_teardown_intents" in method_defs:
            self.facts["overrides_generate_teardown_intents"] = True
            method = method_defs["generate_teardown_intents"]
            if _is_trivial_teardown_body(method):
                self.facts["teardown_body_empty"] = True
                self.report.add(
                    Finding(
                        severity=Severity.WARNING,
                        layer=Layer.AST,
                        code="empty_teardown_intents",
                        message=(
                            "generate_teardown_intents() returns an empty list or only 'pass'. "
                            "Operator close-requests will silently no-op."
                        ),
                        file=str(self.strategy_file),
                        line=method.lineno,
                    )
                )
        elif not inherits_stateless:
            # StatelessStrategy subclasses inherit a valid default implementation.
            self.report.add(
                Finding(
                    severity=Severity.WARNING,
                    layer=Layer.AST,
                    code="missing_teardown_intents",
                    message=(
                        "generate_teardown_intents() is not overridden. Operators cannot "
                        "safely close positions for this strategy."
                    ),
                    file=str(self.strategy_file),
                )
            )

    def _check_get_open_positions(
        self,
        method_defs: dict[str, ast.FunctionDef | ast.AsyncFunctionDef],
        inherits_stateless: bool,
    ) -> None:
        """``get_open_positions`` missing while ``PositionInfo`` is imported."""
        if "get_open_positions" in method_defs:
            self.facts["overrides_get_open_positions"] = True
            return
        if self.facts["imports_position_info"] and not inherits_stateless:
            # StatelessStrategy provides a valid empty get_open_positions();
            # don't nag even if the subclass happens to import PositionInfo
            # for typing.
            self.report.add(
                Finding(
                    severity=Severity.WARNING,
                    layer=Layer.AST,
                    code="missing_get_open_positions",
                    message=(
                        "Strategy imports PositionInfo but does not override "
                        "get_open_positions(). Teardown preview will return an "
                        "empty position list."
                    ),
                    file=str(self.strategy_file),
                )
            )


def _ast_scan_strategy_file(
    strategy_file: Path,
    report: CheckReport,
    target_class_name: str | None = None,
) -> tuple[ast.Module | None, dict[str, Any]]:
    """Walk the strategy's AST and emit findings.

    Returns ``(tree, facts)`` where ``facts`` is a small bag of booleans /
    names used downstream (template-aware heuristics).

    If ``target_class_name`` is provided (e.g. because the loader already
    picked a concrete class) we lock onto that exact class in the tree so
    the two passes can't disagree about which class is the "strategy".

    Implementation: this function is intentionally thin — it only handles
    the error paths (unreadable / unparseable files) and delegates the
    actual walk to :class:`_StrategyASTVisitor`. Keep the body short so the
    cyclomatic complexity budget stays well within the Phase-7 CC ≤ 12 bar.
    """
    try:
        # Force UTF-8 so we don't get locale-dependent decoding surprises
        # on Windows or non-UTF-8 CI runners.
        source = strategy_file.read_text(encoding="utf-8")
    except Exception as exc:
        report.add(
            Finding(
                severity=Severity.ERROR,
                layer=Layer.AST,
                code="read_failed",
                message=f"Cannot read {strategy_file}: {exc}",
                file=str(strategy_file),
            )
        )
        return None, _default_ast_facts()

    try:
        tree = ast.parse(source, filename=str(strategy_file))
    except SyntaxError as exc:
        report.add(
            Finding(
                severity=Severity.ERROR,
                layer=Layer.AST,
                code="syntax_error",
                message=f"Syntax error: {exc.msg}",
                file=str(strategy_file),
                line=exc.lineno,
            )
        )
        return None, _default_ast_facts()

    visitor = _StrategyASTVisitor(strategy_file, report, target_class_name)
    facts = visitor.run(tree)
    return tree, facts


def _is_trivial_teardown_body(func: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """True if the function body is effectively empty.

    "Empty" here means:
    - only ``pass``
    - only a docstring
    - returns an empty list / empty tuple / ``None`` unconditionally
    - any combination of the above
    """
    meaningful_statements: list[ast.stmt] = []
    for stmt in func.body:
        # Strip docstrings (Constant string at top of block).
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, str):
            continue
        if isinstance(stmt, ast.Pass):
            continue
        meaningful_statements.append(stmt)

    if not meaningful_statements:
        return True

    if len(meaningful_statements) == 1 and isinstance(meaningful_statements[0], ast.Return):
        value = meaningful_statements[0].value
        if value is None:
            return True
        if isinstance(value, ast.List) and not value.elts:
            return True
        if isinstance(value, ast.Tuple) and not value.elts:
            return True
        if isinstance(value, ast.Constant) and value.value is None:
            return True

    return False


# =============================================================================
# Layer 3: Template-aware heuristics
# =============================================================================


# These keyword->detector mappings stay small on purpose: they're advisory
# warnings. Adding too many here makes ``check`` feel nagging.
_TEMPLATE_HINTS: dict[str, list[tuple[str, str]]] = {
    # name -> list of (config_key, human-readable hint)
    "perps": [
        ("direction", "Perps strategies should surface a 'direction' config field (long/short)."),
    ],
    "lending": [
        (
            "min_health_factor",
            "Lending strategies should surface a 'min_health_factor' config field to guard liquidation.",
        ),
    ],
    "lp": [
        ("fee_tier", "LP strategies should surface a fee-tier config field (e.g. 'fee_tier' or 'pool')."),
    ],
}


def _detect_template(strategy_class: type | None, config: dict[str, Any] | None, facts: dict[str, Any]) -> str | None:
    """Best-effort detection of the template family.

    Returns one of the keys in ``_TEMPLATE_HINTS`` or ``None`` if we can't
    classify the strategy confidently. We use multiple signals so the check
    still fires for hand-written strategies that didn't come through
    ``strat new``.
    """
    signals: list[str] = []

    if strategy_class is not None:
        meta = getattr(strategy_class, "STRATEGY_METADATA", None)
        if meta is not None:
            tags = getattr(meta, "tags", []) or []
            signals.extend(str(t).lower() for t in tags)
            intent_types = getattr(meta, "intent_types", []) or []
            if intent_types:
                declared = {str(getattr(intent_type, "value", intent_type)).lower() for intent_type in intent_types}
                if any(value.startswith("perp_") for value in declared):
                    return "perps"
                if declared & {"supply", "withdraw", "borrow", "repay", "deleverage"}:
                    return "lending"
                if declared & {"lp_open", "lp_close", "lp_collect_fees"}:
                    return "lp"
                # Explicit intent metadata is authoritative. In particular, a
                # SWAP/HOLD strategy with an exact-pool config must not become
                # an LP strategy merely because its config contains ``pool``.
                return None
            protocols = getattr(meta, "supported_protocols", []) or []
            signals.extend(str(p).lower() for p in protocols)

        class_name = strategy_class.__name__.lower()
        signals.append(class_name)

    if isinstance(config, dict):
        signals.extend(str(k).lower() for k in config.keys())
        # Some config values are themselves telling (e.g. protocol="aave_v3").
        for value in config.values():
            if isinstance(value, str):
                signals.append(value.lower())

    joined = " ".join(signals)

    # Order matters — check "perp" before "lp" (pendle_yt_yield type configs
    # might otherwise miss), and prefer specific over generic matches.
    if any(keyword in joined for keyword in ("perp", "perp_market", "perps")):
        return "perps"
    if any(
        keyword in joined
        for keyword in (
            "lend",
            "borrow",
            "aave",
            "morpho",
            "compound",
            "spark",
            "health_factor",
            "collateral",
        )
    ):
        return "lending"
    if any(keyword in joined for keyword in ("lp_open", "lp_close", "liquidity", "_lp", "lp_", "pool", "fee_tier")):
        return "lp"

    return None


def _apply_template_heuristics(
    template: str | None, config: dict[str, Any] | None, report: CheckReport, strategy_file: Path
) -> None:
    """Emit template-specific advisory warnings when keys are missing."""
    if template is None:
        return

    hints = _TEMPLATE_HINTS.get(template)
    if not hints:
        return

    config_keys: set[str] = set()
    if isinstance(config, dict):
        config_keys = {str(k) for k in config.keys()}

    for required_key, message in hints:
        if required_key in config_keys:
            continue
        # Secondary tolerance: for LP, accept 'pool' OR 'fee_tier' OR
        # 'pool_address' since strategies disagree on naming.
        if template == "lp" and ({"pool", "pool_address", "fee_tier"} & config_keys):
            continue
        report.add(
            Finding(
                severity=Severity.WARNING,
                layer=Layer.TEMPLATE,
                code=f"template_{template}_missing_{required_key}",
                message=message,
                file=str(strategy_file),
                field=required_key,
            )
        )


# =============================================================================
# Config-level placeholder scan (separate from AST so config-only dirs still work)
# =============================================================================


def _scan_config_placeholders(config: dict[str, Any] | None, config_path: Path | None, report: CheckReport) -> None:
    """Walk ``config`` (recursively) and flag placeholder values.

    We visit nested dicts/lists because ``token_funding``, ``copy_trading``,
    and similar fields commonly embed addresses.
    """
    if not isinstance(config, dict) or config_path is None:
        return

    def _walk(obj: Any, path: str) -> None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                _walk(value, f"{path}.{key}" if path else str(key))
        elif isinstance(obj, list):
            for idx, value in enumerate(obj):
                _walk(value, f"{path}[{idx}]")
        else:
            hit = _is_placeholder_value(obj) if isinstance(obj, str) else None
            if hit:
                report.add(
                    Finding(
                        severity=Severity.ERROR,
                        layer=Layer.AST,
                        code="placeholder_address",
                        message=(
                            f"Placeholder value {obj!r} in config (matched on: {hit}). "
                            "Replace with the real on-chain value before running."
                        ),
                        file=str(config_path),
                        field=path or None,
                    )
                )

    _walk(config, "")


# =============================================================================
# Orchestrator
# =============================================================================


def run_checks(strategy_dir: Path) -> CheckReport:
    """Execute all three check layers and return the aggregated report."""
    report = CheckReport(strategy_dir=str(strategy_dir))

    if not strategy_dir.exists():
        report.add(
            Finding(
                severity=Severity.ERROR,
                layer=Layer.LOAD,
                code="dir_missing",
                message=f"Strategy directory does not exist: {strategy_dir}",
            )
        )
        return report

    if not strategy_dir.is_dir():
        report.add(
            Finding(
                severity=Severity.ERROR,
                layer=Layer.LOAD,
                code="not_a_directory",
                message=f"Path is not a directory: {strategy_dir}",
            )
        )
        return report

    strategy_file = _find_strategy_file(strategy_dir)
    if strategy_file is None:
        report.add(
            Finding(
                severity=Severity.ERROR,
                layer=Layer.LOAD,
                code="missing_strategy_py",
                message=f"No strategy.py found in {strategy_dir}",
            )
        )
        # Nothing more we can do.
        return report

    # Layer 1a: load the EFFECTIVE config (file parse + hosted env override)
    # through the shared validation engine. Parse/override failures land as
    # findings; the AST scan below is still valuable either way.
    config, config_path = _load_effective_config(strategy_dir, report)

    # Layer 1b: try to load the class first so we can anchor the AST pass
    # to the exact concrete class name. Import errors are captured as
    # findings so the caller can still see AST results.
    strategy_class, load_err = _try_load_strategy_class(strategy_file)
    if load_err is not None:
        report.add(
            Finding(
                severity=Severity.ERROR,
                layer=Layer.LOAD,
                code="import_error",
                message=load_err,
                file=str(strategy_file),
            )
        )

    target_class_name = strategy_class.__name__ if strategy_class is not None else None

    # Layer 2 (safe even if the module can't be imported).
    _, ast_facts = _ast_scan_strategy_file(strategy_file, report, target_class_name=target_class_name)

    # Config-level placeholder scan (cheap, always run).
    _scan_config_placeholders(config, config_path, report)

    # Shared-engine passes: CONFIG_MODEL contract + market-identity
    # placeholder scan (both run against the EFFECTIVE config).
    _apply_engine_config_findings(strategy_class, config, config_path, report)

    if strategy_class is not None:
        report.strategy_class = f"{strategy_class.__module__}.{strategy_class.__name__}"
        _, load_findings = _instantiate_strategy(strategy_class, config)
        for f in load_findings:
            report.add(f)

    # Layer 3: template heuristics.
    template = _detect_template(strategy_class, config, ast_facts)
    if template is not None:
        report.template = template
        _apply_template_heuristics(template, config, report, strategy_file)

    return report


# =============================================================================
# Output
# =============================================================================


_SEVERITY_STYLES: dict[Severity, dict[str, Any]] = {
    Severity.ERROR: {"fg": "red", "bold": True},
    Severity.WARNING: {"fg": "yellow"},
    Severity.INFO: {"fg": "cyan"},
}


def _format_human(report: CheckReport) -> str:
    """Pretty-print the report for a human operator."""
    lines: list[str] = []
    lines.append(click.style("Strategy check", bold=True))
    lines.append(f"  dir:   {report.strategy_dir}")
    if report.strategy_class:
        lines.append(f"  class: {report.strategy_class}")
    if report.template:
        lines.append(f"  template: {report.template}")
    lines.append("")

    if not report.findings:
        lines.append(click.style("OK — no findings.", fg="green", bold=True))
        return "\n".join(lines)

    # Group by severity (errors first) for easy scanning.
    for sev in (Severity.ERROR, Severity.WARNING, Severity.INFO):
        items = [f for f in report.findings if f.severity == sev]
        if not items:
            continue
        header = {Severity.ERROR: "Errors", Severity.WARNING: "Warnings", Severity.INFO: "Info"}[sev]
        lines.append(click.style(f"{header} ({len(items)}):", **_SEVERITY_STYLES[sev]))
        for f in items:
            loc_parts: list[str] = []
            if f.file:
                loc = f.file
                if f.line is not None:
                    loc = f"{loc}:{f.line}"
                loc_parts.append(loc)
            if f.field:
                loc_parts.append(f"field={f.field}")
            loc_str = f" [{' | '.join(loc_parts)}]" if loc_parts else ""
            lines.append(f"  - [{f.code}]{loc_str} {f.message}")
        lines.append("")

    summary = (
        f"Summary: {sum(1 for f in report.findings if f.severity == Severity.ERROR)} errors, "
        f"{sum(1 for f in report.findings if f.severity == Severity.WARNING)} warnings, "
        f"{sum(1 for f in report.findings if f.severity == Severity.INFO)} info"
    )
    lines.append(summary)
    return "\n".join(lines)


def _format_json(report: CheckReport) -> str:
    """Serialize the report for machine consumption (PM ingests this)."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


# =============================================================================
# Click command
# =============================================================================


@click.command("check")
@click.option(
    "--working-dir",
    "-d",
    type=click.Path(exists=False),
    default=".",
    help="Strategy directory (the one containing strategy.py and config.json). Defaults to cwd.",
)
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    default=False,
    help="Emit findings as JSON (for PM / CI consumption).",
)
def check(working_dir: str, json_output: bool) -> None:
    """Pre-flight validation for a strategy.

    Runs three layers of checks over a strategy directory:

    \b
    1. Load + validate: imports strategy.py, instantiates the class,
       catches ConfigValidationError from validate_config().
    2. AST scan: placeholder addresses, empty teardown bodies, missing
       get_open_positions() overrides.
    3. Template heuristics: warns when scaffold-like strategies are
       missing expected config fields (direction / min_health_factor / fee_tier).

    \b
    Exit codes:
      0  clean
      1  warnings
      2  errors
    """
    strategy_dir = Path(working_dir).resolve()
    report = run_checks(strategy_dir)

    if json_output:
        click.echo(_format_json(report))
    else:
        click.echo(_format_human(report))

    if report.has_errors():
        sys.exit(2)
    if report.has_warnings():
        sys.exit(1)
    sys.exit(0)


__all__ = [
    "CheckReport",
    "Finding",
    "Layer",
    "Severity",
    "check",
    "run_checks",
]
