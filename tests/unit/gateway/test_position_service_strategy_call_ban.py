"""Strategy-code call ban for PositionService client (T24 / VIB-4210, ADR §6).

Reconciliation is a CONTROL-PLANE operation. Strategy code MUST NOT import
or call the ``PositionService`` client — the gateway-boundary rule plus the
ADR §6 ban together mean:

- ``almanak/framework/strategies/`` MUST NOT import PositionService symbols.
- ``almanak/framework/`` outside ``cli/`` MUST NOT import PositionService symbols.
- ``strategies/`` MUST NOT import PositionService symbols.
- ``almanak/demo_strategies/`` MUST NOT import PositionService symbols.

The CLI surface ``almanak/framework/cli/`` IS explicitly allowed (operator
CLI is the control-plane entry point).

This guard uses AST scanning so it can't be defeated by aliasing — any
``import position_service`` or ``from ... import PositionServiceStub`` shows
up in the AST regardless of how it's spelled at the import site.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

# Symbols whose import is banned from strategy code. Covers both module-path
# and symbol-name imports.
_BANNED_MODULE_SUFFIXES = (
    "position_service",
    "PositionService",
)
_BANNED_SYMBOLS = (
    "PositionServiceStub",
    "PositionServiceServicer",
    "ReconcileRequest",
    "ReconcileResponse",
    "MatchedPosition",
    "PhantomMissingPosition",
    "StrandedRow",
    "RebuiltRow",
)

# Directories that MUST NOT import PositionService symbols.
_FORBIDDEN_ROOTS = (
    REPO_ROOT / "almanak" / "framework" / "strategies",
    REPO_ROOT / "strategies",
    REPO_ROOT / "almanak" / "demo_strategies",
)

# Directories under almanak/framework/ that are control-plane / not strategy
# container — these are EXEMPT from the ban. Operator CLI lives here.
_FRAMEWORK_CONTROL_PLANE_EXEMPTIONS = (
    REPO_ROOT / "almanak" / "framework" / "cli",
)

# Files that legitimately reference the position_service module (e.g. the
# gateway-side service implementation itself, the client property docstring,
# the framework gateway_client that wires the stub).
_EXPECTED_IMPORTERS = (
    REPO_ROOT / "almanak" / "framework" / "gateway_client.py",
    # Gateway-side files are NOT scanned (they ARE the egress layer per
    # CLAUDE.md gateway-boundary rule).
)


def _walk_py_files(root: pathlib.Path):
    if not root.exists():
        return
    for path in root.rglob("*.py"):
        # Skip __pycache__ and similar
        if "__pycache__" in path.parts:
            continue
        yield path


def _imports_position_service(path: pathlib.Path) -> tuple[bool, str | None]:
    """Return (banned_import_found, offending_line) for a Python source file.

    Detects three classes of bypass:

    1. ``import almanak.gateway.proto.gateway_pb2_grpc`` then
       ``gateway_pb2_grpc.PositionServiceStub(...)`` — alias-via-module.
    2. ``from almanak.gateway.proto.gateway_pb2_grpc import PositionServiceStub``
       — direct symbol import.
    3. ``import almanak.gateway.services.position_service`` — direct module
       import.

    CodeRabbit MAJOR (PR #2240): a previous version only checked import
    statements, so an aliased ``import ...gateway_pb2_grpc as g`` plus
    ``g.PositionServiceStub(...)`` slipped past. We now collect aliases
    that point at the proto-grpc module and treat any banned attribute
    access on those aliases as a violation.
    """
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return False, None
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return False, None

    # First pass: enumerate every imported alias whose name (or asname)
    # resolves to the proto-grpc module, AND fail-fast on direct banned
    # imports. We record (alias_local_name → module_path) so a downstream
    # attribute access can be flagged.
    pb2_grpc_aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                alias_local = alias.asname or alias.name.split(".")[-1]
                # Track gateway_pb2_grpc as an alias source for the second
                # pass — bare `import almanak.gateway.proto.gateway_pb2_grpc`
                # is allowed (the test ban is on calling PositionService*
                # symbols, not on touching gateway_pb2_grpc, which holds
                # every other servicer stub).
                if alias.name.endswith("gateway_pb2_grpc"):
                    pb2_grpc_aliases.add(alias_local)
                if alias.name.endswith(_BANNED_MODULE_SUFFIXES):
                    return True, f"import {alias.name}"
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module.endswith(_BANNED_MODULE_SUFFIXES):
                return True, f"from {module} import ..."
            for alias in node.names:
                if alias.name in _BANNED_SYMBOLS:
                    return True, f"from {module} import {alias.name}"
            # `from almanak.gateway.proto import gateway_pb2_grpc` — the
            # imported symbol IS the proto-grpc module; track its local
            # alias for the second-pass attribute check.
            for alias in node.names:
                if alias.name == "gateway_pb2_grpc":
                    pb2_grpc_aliases.add(alias.asname or alias.name)

    # Second pass: any attribute access on a proto-grpc-aliased name that
    # touches a banned symbol (e.g. ``g.PositionServiceStub``) is a bypass
    # of the import-only check above.
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id in pb2_grpc_aliases
            and node.attr in _BANNED_SYMBOLS
        ):
            return True, f"{node.value.id}.{node.attr}"
    return False, None


def test_strategies_dir_does_not_import_position_service():
    """`strategies/` MUST NOT import PositionService.

    Strategy code is the strategy container. Control-plane RPCs (reconcile)
    are operator-only — they can write registry rows without writing a
    corresponding ledger row, which is a privilege a strategy must NEVER
    have (would corrupt the audit trail).
    """
    offenders: list[tuple[pathlib.Path, str]] = []
    root = REPO_ROOT / "strategies"
    for path in _walk_py_files(root):
        banned, detail = _imports_position_service(path)
        if banned:
            offenders.append((path, detail or "<unknown>"))
    assert not offenders, (
        f"PositionService imported from strategies/ — control-plane ban violated. "
        f"Offenders: {[(str(p.relative_to(REPO_ROOT)), d) for p, d in offenders]}"
    )


def test_framework_strategies_dir_does_not_import_position_service():
    """`almanak/framework/strategies/` MUST NOT import PositionService."""
    offenders: list[tuple[pathlib.Path, str]] = []
    root = REPO_ROOT / "almanak" / "framework" / "strategies"
    for path in _walk_py_files(root):
        banned, detail = _imports_position_service(path)
        if banned:
            offenders.append((path, detail or "<unknown>"))
    assert not offenders, (
        f"PositionService imported from almanak/framework/strategies/. "
        f"Offenders: {[(str(p.relative_to(REPO_ROOT)), d) for p, d in offenders]}"
    )


def test_demo_strategies_dir_does_not_import_position_service():
    """`almanak/demo_strategies/` MUST NOT import PositionService."""
    offenders: list[tuple[pathlib.Path, str]] = []
    root = REPO_ROOT / "almanak" / "demo_strategies"
    for path in _walk_py_files(root):
        banned, detail = _imports_position_service(path)
        if banned:
            offenders.append((path, detail or "<unknown>"))
    assert not offenders, (
        f"PositionService imported from almanak/demo_strategies/. "
        f"Offenders: {[(str(p.relative_to(REPO_ROOT)), d) for p, d in offenders]}"
    )


def test_framework_outside_cli_does_not_import_position_service():
    """`almanak/framework/` outside `cli/` MUST NOT import PositionService.

    Carve-outs:
    - `almanak/framework/cli/` — operator CLI; explicitly allowed.
    - `almanak/framework/gateway_client.py` — wires the stub property (the
      property itself raises a runtime warning if imported from strategy
      paths). The single line `self._position_stub: gateway_pb2_grpc.PositionServiceStub | None = None`
      AND the `def position(self)` property; both belong on the client surface.
    """
    offenders: list[tuple[pathlib.Path, str]] = []
    root = REPO_ROOT / "almanak" / "framework"
    for path in _walk_py_files(root):
        # Skip CLI exemption
        if any(exempt in path.parents or path == exempt for exempt in _FRAMEWORK_CONTROL_PLANE_EXEMPTIONS):
            continue
        # Skip gateway_client.py — the wiring belongs there
        if path in _EXPECTED_IMPORTERS:
            continue
        banned, detail = _imports_position_service(path)
        if banned:
            offenders.append((path, detail or "<unknown>"))
    assert not offenders, (
        f"PositionService imported from almanak/framework/ outside cli/. "
        f"Offenders: {[(str(p.relative_to(REPO_ROOT)), d) for p, d in offenders]}"
    )


def test_cli_exemption_is_explicit():
    """The CLI exemption is intentional + scoped — verifies the allowlist
    is correctly anchored at `almanak/framework/cli/`.
    """
    expected_exemption = REPO_ROOT / "almanak" / "framework" / "cli"
    assert expected_exemption in _FRAMEWORK_CONTROL_PLANE_EXEMPTIONS
    assert expected_exemption.exists(), (
        "CLI directory must exist for the exemption to be meaningful"
    )


def test_gateway_client_exception_is_explicit():
    """`gateway_client.py` IS the wiring site; record it as an explicit
    expected importer rather than a quiet allowlist.
    """
    assert REPO_ROOT / "almanak" / "framework" / "gateway_client.py" in _EXPECTED_IMPORTERS


def test_scanner_detects_alias_based_pb2_grpc_bypass(tmp_path: pathlib.Path):
    """Regression test for the CodeRabbit MAJOR finding (PR #2240).

    Scenario: a future contributor adds, in strategy code,

        import almanak.gateway.proto.gateway_pb2_grpc as g
        g.PositionServiceStub(channel).Reconcile(...)

    The previous import-only AST scan let this through because no banned
    symbol appeared on either ``import`` line. The scanner now detects
    banned-symbol attribute access on any tracked proto-grpc alias.
    """
    sneaky = tmp_path / "sneaky.py"
    sneaky.write_text(
        "import almanak.gateway.proto.gateway_pb2_grpc as g\n"
        "\n"
        "def call_reconcile(channel):\n"
        "    return g.PositionServiceStub(channel)\n"
    )
    banned, detail = _imports_position_service(sneaky)
    assert banned, "Alias-via-attribute access to PositionServiceStub must be flagged"
    assert "PositionServiceStub" in (detail or "")


def test_scanner_detects_from_import_module_alias_bypass(tmp_path: pathlib.Path):
    """The other alias-bypass shape: ``from almanak.gateway.proto import gateway_pb2_grpc``
    then attribute access.
    """
    sneaky = tmp_path / "sneaky_from.py"
    sneaky.write_text(
        "from almanak.gateway.proto import gateway_pb2_grpc as gp\n"
        "\n"
        "def call_reconcile(channel):\n"
        "    return gp.PositionServiceStub(channel)\n"
    )
    banned, detail = _imports_position_service(sneaky)
    assert banned, "from-import alias attribute access must be flagged"
    assert "PositionServiceStub" in (detail or "")


def test_scanner_does_not_flag_unrelated_pb2_grpc_usage(tmp_path: pathlib.Path):
    """Negative test: aliased ``gateway_pb2_grpc`` access to OTHER stubs
    (e.g. ``RpcServiceStub``) must NOT be flagged — the ban is scoped to
    PositionService symbols.
    """
    benign = tmp_path / "benign.py"
    benign.write_text(
        "from almanak.gateway.proto import gateway_pb2_grpc as gp\n"
        "\n"
        "def call_rpc(channel):\n"
        "    return gp.RpcServiceStub(channel)\n"
    )
    banned, _ = _imports_position_service(benign)
    assert not banned, "Unrelated gateway_pb2_grpc usage must not be flagged"


# crap-allowlist: not applicable; this is a static guard file (T24/VIB-4210).
# Removing or weakening these tests requires explicit human review per ADR §6.
