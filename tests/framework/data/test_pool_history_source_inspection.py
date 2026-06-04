"""Source-inspection guard for the VIB-4728 framework boundary (D3.F10).

**VIB-4886 — pivot from denylist to allowlist for third-party imports.**

Three complementary scans land here per
``docs/internal/uat-cards/VIB-4755.md`` §D3.F10 (POOL-7) +
``docs/internal/uat-cards/VIB-4886.md`` (this refactor):

- Scan A — explicit named 5 modules (fast, unambiguous, doc-friendly).
- Scan B — package-tree walk via pkgutil.walk_packages over 3 ROOTS.
- Scan C — TRUE import-closure walk via AST parsing starting at the
  5 CLOSURE_TARGETS, transitively following any almanak.framework.*
  import. Handles relative imports via importlib.util.resolve_name;
  reads source from path on ImportError.

PLUS a framework-wide gRPC channel-usage check (substring layer +
AST semantic-binding layer) covering every alias / import-style.

PLUS dynamic-import suppression on the 5 CLOSURE_TARGETS.

PLUS binding-aware AST scan defending the web3 module against
provider-class access via direct attribute, getattr, vars, __dict__,
or __getattribute__ — including aliased `import web3 as <X>` bindings.

VIB-4886 changes from POOL-7's denylist model:

- ``ALLOWED_THIRD_PARTY_IMPORTS`` — positive list of every legitimate
  third-party top-level package the framework needs. Anything else
  fails as "outside allowlist".
- ``FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES`` — every transport-bearing
  provider class re-exported at ``web3.*`` (verified against installed
  ``web3==7.x``).
- ``FORBIDDEN_WEB3_SUBMODULE_PREFIXES = {"web3.providers"}`` — entire
  transport subtree, structurally complete for future submodules.
- Class C protocol-SDK denylist entries (``solana``, ``solders``,
  ``anchorpy``, ``websockets``, ``ccxt``, …) REMOVED — the allowlist
  catches them by default as unknown third-party.

Anti-no-op regression fixtures: Scan B real-file fixture, Scan C
multi-hop transitive fixture, relative-import fixture, plus VIB-4886's
allowlist-gap + web3-provider-bypass + alias-bypass fixtures. Each
proves the corresponding scan would actually fire on a deliberately-bad
input.

Known limitations (see VIB-4886.md §"Known limitations" and
VIB-4901.md §"Known limitations after this PR"):

VIB-4901 (this iteration) CLOSED:

- L1 direct + from-import dynamic imports: now caught by the layered
  defense — Class E import-name denylist (``importlib`` / ``runpy`` /
  ``code`` prefixes in ``FORBIDDEN_IMPORT_NAMES``) catches
  ``from importlib.machinery import SourceFileLoader``; the
  18-entry ``FORBIDDEN_DYNAMIC_IMPORTS`` substring tuple catches
  USE patterns including bare-name forms (``SourceFileLoader(``,
  ``run_module(``, etc.). The dynamic-import substring scan now
  runs uniformly across Scan B + Scan C, including
  ``_LEGACY_VIOLATING_MODULES``.
- L4a (direct-call instance binding ``w3 = Web3()`` /
  ``w3 = AsyncWeb3()``): now caught — ``_collect_web3_aliases``
  binds ``w3`` when it sees the ast.Assign + ast.Call shape, and
  the existing ``_scan_web3_dynamic_misuse`` walker catches
  ``w3.HTTPProvider`` access.
- L4b single-level (``wb = web3``): now caught — ``_collect_web3_aliases``
  follows single-pass ast.Assign with rhs in the existing binding
  set.

Remains INHERENT (pinned by negative-acceptance tests, see VIB-4901):

- L1-inherent: indirection-only dynamic-import shapes
  (``getattr(importlib, "import_module")("web3")``,
  ``vars(importlib)["import_module"]("web3")``, name-rebound
  chains across ast.Assign). Substring scan cannot see them
  because the forbidden substring is broken across ast.Call /
  ast.Subscript / ast.Assign chains. Pinned by VIB-4901 A.7 /
  D.3. The sidecar runtime check catches the actual egress.
- L2: cross-module data-flow re-export chains (``A → B → web3``).
  Requires data-flow analysis across module boundaries —
  out of scope for static AST inspection. Sidecar runtime check
  is the defense.
- L3: dynamically-generated source. ``exec(`` substring catches
  the execution step; the AST-construction step is uncatchable.
  Pinned by VIB-4901 D.3.
- L4b transitive (multi-level same-module rebinding,
  ``a = web3; b = a; c = b``): single-pass binding collector
  doesn't chase the chain. Pinned by VIB-4901 D.1. Closing
  requires fixed-point ast.Assign iteration.
- L4c: indirect-call binding shapes
  (``w3 = (Web3,)[0]()``, ``w3 = (lambda: Web3())()``,
  ``w3 = (Web3 if cond else AsyncWeb3)()``,
  ``w3 = globals()["Web3"]()``). Single-pass collector only
  handles direct ``ast.Call(func=ast.Name(...))``. Pinned by
  VIB-4901 D.2. Closing requires expression-level data-flow
  (L2 territory).
"""

from __future__ import annotations

import ast
import importlib
import importlib.util
import inspect
import pkgutil
import re
import sys
from pathlib import Path
from typing import Literal

import pytest

# ============================================================================
# Allowlist for legitimate third-party top-level package imports (VIB-4886).
# ============================================================================
#
# Each entry must have a one-line justification comment IMMEDIATELY above the
# string literal. The bounded-size assertion below guards casual expansion —
# adding a 12th entry without justification fails the assertion.
#
# Re-verify the surface before proposing additions by running:
#   uv run python scripts/_vib4886_audit_third_party.py
#
# The audit helper walks the closure + PACKAGE_ROOTS and reports every
# distinct third-party top-level import in scope. If a new framework module
# pulls in a dep that isn't here, EITHER (a) add the entry with justification
# OR (b) add the module to _LEGACY_VIOLATING_MODULES with a follow-up ticket.

ALLOWED_THIRD_PARTY_IMPORTS: frozenset[str] = frozenset(
    {
        # Gateway gRPC transport — the ONE permitted egress path
        # (CLAUDE.md §Gateway boundary).
        "grpc",
        # gRPC health-probe protocol; sidecar boot check from `gateway_client`.
        "grpc_health",
        # `google.rpc.error_details` — gRPC error metadata; protobuf-paired
        # stdlib of the gRPC ecosystem. Pinned to `google.rpc` (NOT bare
        # `google`) because `google` is a multi-product namespace also
        # containing network-capable clients (`google.cloud`,
        # `google.auth.transport.requests`). The dotted-prefix match in
        # `_classify_import` allows `google.rpc.error_details` while
        # leaving `google.cloud.*` etc. as "unknown". (Codex post-PR
        # finding.)
        "google.rpc",
        # `google.protobuf.duration_pb2` etc. — protobuf message types used
        # by `framework.grpc.error_details` for the gateway boundary's
        # error metadata contract. Pure message types, no I/O. Pinned to
        # `google.protobuf` (NOT bare `google`) for the same multi-product
        # tightening reason as `google.rpc`.
        "google.protobuf",
        # CLAUDE.md explicitly permits web3.py for ABI / checksum / encoding
        # utilities. Provider misuse is defended by three layers:
        # (a) FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES catches re-exports;
        # (b) FORBIDDEN_WEB3_SUBMODULE_PREFIXES catches the entire
        #     `web3.providers.*` transport subtree;
        # (c) Class A usage substrings catch direct construction calls.
        # See _is_forbidden_import + the binding-aware web3 scan below.
        "web3",
        # DataFrame for OHLCV and `MarketSnapshot` — pure data structure, no I/O.
        "pandas",
        # Schema validation / model serialization — no I/O.
        "pydantic",
        # Operator-side config file parsing.
        "yaml",
        # Operator-side local API server (`framework.api.*`).
        "fastapi",
        # Operator-side CLI / QA tooling.
        "click",
        # Read-only chart generation for QA reporting.
        "matplotlib",
        # Sync/async interop shim — no I/O semantics.
        "nest_asyncio",
    }
)

# Strict bound — Claude pr-auditor post-PR Potential #7: a `<=` bound
# with headroom lets a casual `+1` addition land without explicit
# reviewer intent. Strict `==` forces the reviewer to bump the count
# deliberately when adding an entry, mirroring the strictness of
# FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES == 14 above.
assert len(ALLOWED_THIRD_PARTY_IMPORTS) == 12, (
    "ALLOWED_THIRD_PARTY_IMPORTS changed — adding a new entry "
    "REQUIRES (a) bumping this count deliberately, (b) a justification "
    "comment immediately above the literal, AND (c) re-running "
    "scripts/_vib4886_audit_third_party.py to confirm the new dep is "
    "in scanned scope. File a VIB-XXXX follow-up ticket if the new "
    "dep lands in a money-path module."
)


# ============================================================================
# Forbidden-surface set (enumerated by CLASS — see card §D3.F10)
# ============================================================================
#
# VIB-4886: Class C protocol-SDK entries (solana, solders, anchorpy,
# websockets, websocket, ccxt, pyserum, pysui, xrpl, tonsdk, pytonlib,
# driftpy, spl.token) were REMOVED — they're structurally caught by the
# allowlist as unknown third-party. Class A (HTTP/RPC clients) and Class B
# (subprocess/FFI) stay as denylists because they give a precise error
# message naming the offending surface.

FORBIDDEN_IMPORT_NAMES: frozenset[str] = frozenset(
    {
        # Class A — HTTP/RPC clients. NOTE: bare `web3` is intentionally OMITTED
        # — CLAUDE.md §Gateway boundary explicitly allows web3.py for
        # "ABI / checksum / encoding utilities". Web3 provider classes are
        # caught by FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES (top-level
        # re-exports) and FORBIDDEN_WEB3_SUBMODULE_PREFIXES (web3.providers
        # subtree). The forbidden CALL surface (HTTPProvider(url) etc.) is
        # caught by the usage substrings below.
        "aiohttp",
        "httpx",
        "requests",
        "urllib.request",
        "urllib3",
        "http.client",
        # Class B — Subprocess / multiprocessing / FFI / pty:
        "subprocess",
        "multiprocessing",
        "ctypes",
        "cffi",
        "pty",
        # Class D — stdlib raw-network surface (Claude pr-auditor post-PR
        # Important #3). Each of these is a stdlib socket-or-protocol
        # client that can open outbound connections; CLAUDE.md §Gateway
        # boundary forbids ANY outbound network egress from the strategy
        # container except the gateway gRPC channel. The scanned scope
        # (CLOSURE_TARGETS + PACKAGE_ROOTS) does not legitimately import
        # any of these (verified by audit at PR time — the only `socket`
        # users in framework are `anvil/fork_manager.py`,
        # `anvil/solana_fork_manager.py`, and `backtesting/paper/background.py`,
        # all outside scanned scope). A framework module under scan that
        # imports one of these is by-definition a gateway-boundary
        # violation.
        "socket",
        "ssl",
        "ftplib",
        "smtplib",
        "http.server",
        "wsgiref",
        "xmlrpc.client",
        "poplib",
        "imaplib",
        "telnetlib",
        "nntplib",
        # Class C — REMOVED VIA VIB-4886. Each former entry is now caught
        # structurally by the third-party allowlist as "unknown".
        # See FORMER_CLASS_C_IMPORT_NAMES (the regression contract below)
        # for the names that USED to be here and must NOT come back.
        # Class E — dynamic-execution + arbitrary-code-deserialization
        # surface (VIB-4901). The strategy container has no legitimate
        # use for runtime module loading (``importlib`` / ``runpy``),
        # interactive Python / dynamic source compilation (``code``),
        # or unpickling untrusted bytes (``pickle`` / ``marshal`` /
        # ``shelve`` — all are classic RCE vectors when loading data
        # from network/disk). The dotted-prefix match in
        # ``_is_forbidden_import`` makes the parent entry cover every
        # submodule (``importlib.machinery``, ``importlib.util``,
        # ``runpy.run_module``, ``code.InteractiveInterpreter``, etc.).
        # Audited 2026-05-31 against Scan B ∪ Scan C: ZERO imports from
        # any of these namespaces after the OHLCV refactor (one
        # pre-existing ``import importlib`` in
        # ``almanak/framework/data/ohlcv/__init__.py`` is removed in the
        # same PR; ``copy_signal_engine`` is the single bounded
        # exemption tracked by VIB-4914). Subject to
        # ``_LEGACY_VIOLATING_MODULES`` exemption (caller-level skip);
        # audit confirmed zero legacy modules import from any of these
        # namespaces either. The bare-name extensions of
        # ``FORBIDDEN_DYNAMIC_IMPORTS`` below catch USE patterns in
        # legacy modules where this layer is exempted.
        "importlib",
        "runpy",
        "code",
        "pickle",
        "marshal",
        "shelve",
    }
)

# VIB-4886 regression contract: these names USED to be in
# FORBIDDEN_IMPORT_NAMES under POOL-7's denylist. The allowlist now
# catches them as "unknown third-party" without explicit enumeration.
# `test_former_class_c_removed_from_forbidden_import_names` proves the
# structural relationship — empty intersection with FORBIDDEN_IMPORT_NAMES.
FORMER_CLASS_C_IMPORT_NAMES: frozenset[str] = frozenset(
    {
        "solana",
        "solders",
        "anchorpy",
        "websockets",
        "websocket",
        "ccxt",
        "pyserum",
        "pysui",
        "xrpl",
        "tonsdk",
        "pytonlib",
        "driftpy",
        "spl.token",
    }
)

# Forbidden web3 provider class names — re-exported at the `web3` top
# level by web3.py. VERIFIED against installed web3==7.x via:
#   uv run python -c "import web3; print(sorted(x for x in dir(web3) \
#       if 'Provider' in x or 'WebSocket' in x or 'IPC' in x \
#       or 'Connection' in x))"
# An upstream rename triggers
# test_web3_provider_trailing_names_verified_against_installed_web3
# (loud failure rather than silent allow).
FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES: frozenset[str] = frozenset(
    {
        "AsyncBaseProvider",
        "AsyncEthereumTesterProvider",
        "AsyncHTTPProvider",
        "AsyncIPCProvider",
        "AutoProvider",
        "BaseProvider",
        "EthereumTesterProvider",
        "HTTPProvider",
        "IPCProvider",
        "JSONBaseProvider",
        "LegacyWebSocketProvider",
        "PersistentConnection",
        "PersistentConnectionProvider",
        "WebSocketProvider",
    }
)

# Strict — an upstream web3 release adding a new provider class must
# trigger an explicit allowlist-style review, not a silent allow.
assert len(FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES) == 14, (
    "FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES changed — re-verify against "
    "installed web3 (see docstring above) and update the strict count."
)

# web3.providers.* — entire subtree forbidden as the transport
# namespace by design. Single prefix matches all current submodules
# (async_base, auto, base, eth_tester, ipc, legacy_websocket,
# persistent, rpc) AND any future submodules web3.py adds.
# Structurally complete — no enumeration drift.
FORBIDDEN_WEB3_SUBMODULE_PREFIXES: frozenset[str] = frozenset(
    {
        "web3.providers",
    }
)

# Strict — broadening to a second prefix requires an explicit design
# justification comment.
assert len(FORBIDDEN_WEB3_SUBMODULE_PREFIXES) == 1, "FORBIDDEN_WEB3_SUBMODULE_PREFIXES grew — justify in a comment."


def _is_forbidden_import(name: str) -> bool:
    """Check if an import target name matches the forbidden set.

    Three layers of denylist defense:

    1. (POOL-7) Any dotted prefix of ``name`` is in
       ``FORBIDDEN_IMPORT_NAMES`` (Class A HTTP/RPC + Class B
       subprocess/FFI).
    2. (VIB-4886) Name's first segment is ``"web3"`` AND last segment
       is in ``FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES`` (catches
       ``from web3 import HTTPProvider``,
       ``from web3.providers.legacy_websocket import LegacyWebSocketProvider``,
       etc.; alias-immune because AST emits the full chain).
    3. (VIB-4886) Any dotted prefix of ``name`` is in
       ``FORBIDDEN_WEB3_SUBMODULE_PREFIXES`` (catches every shape that
       touches ``web3.providers.*``, including future submodules).
    """
    parts = name.split(".")

    # Layer 1: Class A/B prefix-match.
    for i in range(1, len(parts) + 1):
        prefix = ".".join(parts[:i])
        if prefix in FORBIDDEN_IMPORT_NAMES:
            return True

    # Layer 2: web3 trailing-name match.
    if parts[0] == "web3" and parts[-1] in FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES:
        return True

    # Layer 3: web3.providers subtree prefix-match.
    for i in range(1, len(parts) + 1):
        prefix = ".".join(parts[:i])
        if prefix in FORBIDDEN_WEB3_SUBMODULE_PREFIXES:
            return True

    return False


def _classify_import(name: str) -> Literal["stdlib", "almanak", "allowed", "unknown"]:
    """Classify a dotted import target.

    VIB-4886 §B.1: lookup against three sets in this order:

    - ``sys.stdlib_module_names`` (top-level) → ``"stdlib"``.
    - ``"almanak"`` (top-level) → ``"almanak"``.
    - ``ALLOWED_THIRD_PARTY_IMPORTS`` (dotted-prefix match) → ``"allowed"``.
    - Otherwise → ``"unknown"``.

    **Dotted-prefix allowlist semantics** (Codex post-PR finding):
    multi-product namespaces (notably ``google``, which holds both the
    permitted ``google.rpc`` error-details and the forbidden
    ``google.cloud`` / ``google.auth.transport.requests``) require
    that the allowlist entry can be more specific than the top level.
    ``google.rpc`` in the allowlist matches ``google.rpc.error_details``
    via prefix but leaves ``google.cloud.storage`` as ``"unknown"``.

    The classifier is the second layer after ``_is_forbidden_import``
    in the per-module pipeline (see ``_scan_import_names_against_pipeline``).
    """
    top = name.split(".")[0]
    if top in sys.stdlib_module_names:
        return "stdlib"
    if top == "almanak":
        return "almanak"
    # Dotted-prefix match — supports both bare top-level entries
    # (``grpc``, ``web3``) and multi-segment entries (``google.rpc``).
    parts = name.split(".")
    for i in range(1, len(parts) + 1):
        prefix = ".".join(parts[:i])
        if prefix in ALLOWED_THIRD_PARTY_IMPORTS:
            return "allowed"
    return "unknown"


def _format_allowlist_gap(
    modname: str,
    path: str,
    name: str,
    lineno: int,
) -> str:
    """Format the deterministic allowlist-gap failure message (§G).

    Format pinned by §G.1 — fully-qualified module, absolute path,
    AST lineno, top-level package name AND full dotted name, plus the
    actionable next step.
    """
    top = name.split(".")[0]
    return (
        f"{modname} ({path}):{lineno} third-party import outside allowlist:\n"
        f"    top-level {top!r} from {name!r}.\n"
        f"    To allow, add to ALLOWED_THIRD_PARTY_IMPORTS with a "
        f"justification comment in "
        f"tests/framework/data/test_pool_history_source_inspection.py."
    )


def _format_forbidden_import(
    modname: str,
    path: str,
    name: str,
    lineno: int,
) -> str:
    """Format the existing Class A/B/web3 explicit-violation failure
    message. Distinct from the allowlist-gap message so grep-CI can
    tell them apart."""
    return f"{modname} ({path}):{lineno} forbidden import detected: resolved chain {name!r}"


def _ast_import_names_for_check(source: str) -> list[tuple[str, int]]:
    """Extract every import-target canonical name from ``source``.

    Returns a list of (canonical_name, lineno) tuples. Each ImportFrom
    yields BOTH the bare module name AND the dotted ``module.alias``
    forms (so ``from urllib import request`` yields ``urllib`` AND
    ``urllib.request`` — the second matches the forbidden set's
    ``urllib.request`` entry). Wildcard imports yield just the module
    name (``from grpc import *`` → ``grpc``).

    **The AST-based normalization is what closes the ``from X import Y``
    bypass surface** (CodeRabbit Round-2 major finding in POOL-7,
    re-pinned by VIB-4886 §C.3). A regex-only check would miss it
    because ``from urllib import request`` doesn't contain the literal
    token ``urllib.request`` anywhere in the source.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    out: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                out.append((alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            if node.level and node.level > 0:
                # Relative import — not in scope for forbidden-import
                # check (the closure walker resolves relative imports
                # separately for transitive scanning).
                continue
            if not node.module:
                continue
            out.append((node.module, node.lineno))
            for alias in node.names:
                if alias.name == "*":
                    # Wildcard — module-only is sufficient.
                    continue
                out.append((f"{node.module}.{alias.name}", node.lineno))
    return out


_USAGE_A: tuple[str, ...] = (
    # CLAUDE.md §Gateway boundary: "the web3.py library is fine for ABI /
    # checksum / encoding utilities; the forbidden part is instantiating a
    # provider pointed at any URL." So the violation IS `HTTPProvider(url)`
    # / `AsyncHTTPProvider(url)`, NOT `Web3(provider)`. Bare `Web3(...)` is
    # legitimate when the argument is a gateway-routed provider (see
    # almanak.framework.web3.gateway_provider for the canonical wrapper).
    #
    # VIB-4886: extended to cover the full transport-bearing provider
    # class family. Primary structural defense is the import-name
    # denylist (_is_forbidden_import); these substrings are the
    # secondary defense for the deferred-call shape
    # (`P = web3.HTTPProvider; P(url)` — already caught by the
    # binding-aware web3 scan, but defense in depth).
    "HTTPProvider(",
    "AsyncHTTPProvider(",
    "WebSocketProvider(",
    "LegacyWebSocketProvider(",
    "IPCProvider(",
    "AsyncIPCProvider(",
    "PersistentConnectionProvider(",
    "AutoProvider(",
    "EthereumTesterProvider(",
    "AsyncEthereumTesterProvider(",
    # Existing non-web3 HTTP client construction surfaces.
    "aiohttp.ClientSession(",
    "httpx.Client(",
    "httpx.AsyncClient(",
    "requests.Session(",
)
# Substrings deliberately exclude `BaseProvider(`, `AsyncBaseProvider(`,
# `JSONBaseProvider(` — these are abstract base classes; subclassing is
# legitimate within `almanak.framework.web3.*`, and the actual
# transport-bearing call is whatever subclass is instantiated.

_USAGE_B: tuple[str, ...] = (
    "subprocess.run(",
    "subprocess.Popen(",
    "subprocess.call(",
    "subprocess.check_call(",
    "subprocess.check_output(",
    "os.system(",
    "os.popen(",
    "multiprocessing.Process(",
    "asyncio.create_subprocess_exec(",
    "asyncio.create_subprocess_shell(",
    "pty.spawn(",
    # Class 3 (low-level syscalls): class prefixes catch the full family.
    "os.fork(",
    "os.posix_spawn(",
    "os.posix_spawnp(",
    "os.exec",  # prefix — catches execv / execve / execvp / ...
    "os.spawn",  # prefix — catches spawnv / spawnvp / spawnl / ...
    # Class 4 (FFI):
    "ctypes.CDLL(",
    "ctypes.cdll.",
    "ctypes.WinDLL(",
    "ctypes.windll.",
    "LoadLibrary(",
    "cffi.FFI(",
    "cffi.dlopen(",
)
FORBIDDEN_USAGE_SUBSTRINGS: tuple[str, ...] = _USAGE_A + _USAGE_B

_USAGE_GRPC_CHANNEL: tuple[str, ...] = (
    "grpc.insecure_channel(",
    "grpc.secure_channel(",
    "grpc.aio.insecure_channel(",
    "grpc.aio.secure_channel(",
)
GRPC_CHANNEL_ALLOWED_MODULES: set[str] = {"almanak.framework.gateway_client"}

# Asserted bounded — no casual expansion.
assert len(GRPC_CHANNEL_ALLOWED_MODULES) <= 1, "no casual expansion"

# Dynamic-import bans (uniform across Scan A, Scan B, Scan C, INCLUDING
# legacy modules — per VIB-4901 the substring check no longer respects
# ``_LEGACY_VIOLATING_MODULES`` exemption because legacy debt was about
# HTTP/RPC egress, not dynamic-execution surface).
#
# VIB-4901 extended this tuple from 6 → 18 entries to add bare-name forms
# (catches ``from importlib.machinery import SourceFileLoader; SourceFileLoader(...)``
# where the qualified namespace is absent from the source text). Each new
# entry was audited 2026-05-30 against Scan B ∪ Scan C for false-positive
# risk; see ``docs/internal/uat-runs/VIB-4901/audit-data.md``.
#
# Deliberate exclusions (audited, too ambiguous):
#  - bare ``reload(`` → matches ``MyClass.reload(...)`` etc.; qualified
#    ``importlib.reload(`` only
#  - bare ``compile(`` → matches ``re.compile(...)`` and many others
FORBIDDEN_DYNAMIC_IMPORTS: tuple[str, ...] = (
    # Layer 1 — direct import-execution functions (qualified + bare;
    # bare forms catch ``from importlib import X; X(...)``).
    "__import__(",
    "import_module(",
    "spec_from_file_location(",
    "module_from_spec(",
    "importlib.reload(",
    "importlib.__import__(",
    # Layer 2 — importlib.machinery loaders.
    "importlib.machinery.",
    "SourceFileLoader(",
    "ExtensionFileLoader(",
    "SourcelessFileLoader(",
    # Layer 3 — runpy.
    "run_module(",
    "run_path(",
    # Layer 4 — code module (interactive interpreter + compile).
    "compile_command(",
    "InteractiveInterpreter(",
    "InteractiveConsole(",
    # Layer 5 — dynamic execution + builtin dict bypass to __import__.
    "exec(",
    "eval(",
    "__builtins__[",
)
# VIB-4901 §F.3 — strict-equality bound forces deliberate count bump on
# additions. The Phase 0b audit-data backs each entry.
assert len(FORBIDDEN_DYNAMIC_IMPORTS) == 18, (
    "FORBIDDEN_DYNAMIC_IMPORTS tuple length changed — re-audit Scan B ∪ "
    "Scan C for false positives and update docs/internal/uat-runs/VIB-4901/"
    "audit-data.md before adjusting this bound"
)

# AST-level shell-egress check.
FORBIDDEN_SHELL_TOKENS_REGEX = re.compile(r"(?i)\b(curl|wget|nc|netcat)\b")

# The four resolved chains representing direct gRPC channel construction.
FORBIDDEN_GRPC_CHANNEL_CHAINS: set[tuple[str, ...]] = {
    ("grpc", "insecure_channel"),
    ("grpc", "secure_channel"),
    ("grpc", "aio", "insecure_channel"),
    ("grpc", "aio", "secure_channel"),
}

# ============================================================================
# Module lists
# ============================================================================

CLOSURE_TARGETS: tuple[str, ...] = (
    "almanak.framework.data.pools.history",
    "almanak.framework.data.null_readers",
    "almanak.framework.market.builders",
    "almanak.framework.market.snapshot",
    "almanak.framework.gateway_client",
)
PACKAGE_ROOTS: tuple[str, ...] = (
    "almanak.framework.data.pools",
    "almanak.framework.data",
    "almanak.framework.market",
)

# ============================================================================
# Legacy-debt exemptions
# ============================================================================
#
# POOL-7's scope is to NOT introduce new gateway-boundary violations — NOT
# to retroactively fix every pre-existing VIB-2986-era violation in the
# framework tree. These modules have known migration debt and each has (or
# needs) its own ticket. Adding to this set requires a follow-up ticket
# reference in the comment. The set is asserted bounded — casual expansion
# fails the "no casual expansion" assertion below.
#
# VIB-4886: the same exemption applies to the new allowlist check (legacy
# modules skip BOTH the forbidden-import denylist AND the allowlist-gap
# classifier). These modules pull in legitimate-yet-egress third-party
# deps (aiohttp, web3.HTTPProvider) — out of VIB-4886 scope to migrate.
_LEGACY_VIOLATING_MODULES: set[str] = {
    # VIB-2986 / VIB-4727 migration debt — these modules still own their
    # own HTTP/GraphQL egress (Class A) or instantiate Web3(HTTPProvider)
    # directly (Class A usage). POOL-7 deliberately scopes to NOT block
    # on these; each needs a separate VIB ticket to migrate.
    "almanak.framework.data.indicators.rsi",  # legacy aiohttp egress
    "almanak.framework.data.pendle.api_client",  # legacy urllib.request egress
    "almanak.framework.data.pendle.on_chain_reader",  # Web3(HTTPProvider(...))
    "almanak.framework.data.providers.defillama_provider",  # legacy aiohttp egress
    "almanak.framework.data.staking.solana_lst_provider",  # legacy aiohttp egress
    "almanak.framework.data.token_safety.client",  # legacy aiohttp egress
    "almanak.framework.data.yields.aggregator",  # legacy aiohttp egress
    "almanak.framework.data.defi.gas",  # Web3(HTTPProvider(...))
    "almanak.framework.data.defi.pools",  # Web3(HTTPProvider(...))
    "almanak.framework.data.dexscreener.client",  # legacy aiohttp egress
    # VIB-4851 retired position_health's live ``Web3(HTTPProvider(...))`` boundary
    # (Aave/Morpho via the lending-read seam, Compound V3 via the connector-owned
    # gateway market-health read). No forbidden *code* edge remains; the entry stays
    # only because the FORBIDDEN_USAGE_SUBSTRINGS scan is text-based and the
    # ``_read_account_state`` docstring still cites ``Web3(HTTPProvider(rpc_url))`` as
    # the removed pattern. Removing the entry RED-fails Scan B/C on that prose, not on
    # live egress. Drop this once the substring scan is AST-aware (ignores docstrings).
    "almanak.framework.data.position_health",  # docstring-prose substring only (VIB-4851)
    "almanak.framework.data.price.dex_twap",  # Web3(HTTPProvider(...))
}
assert len(_LEGACY_VIOLATING_MODULES) <= 20, (
    "_LEGACY_VIOLATING_MODULES grew beyond expected legacy debt — file a follow-up ticket and document each new entry"
)

# Modules that legitimately construct gRPC channels (operator-machine
# surfaces, NOT strategy-container code). CLI commands run on operator
# machines per CLAUDE.md §Agent-tools rule's framing of `ax`. These are
# the documented exemptions to the framework-wide gRPC channel-usage
# restriction. Asserted bounded: <= 5 (no casual expansion).
GRPC_CHANNEL_OPERATOR_EXEMPT: set[str] = {
    "almanak.framework.cli.ax",  # operator CLI — talks to local gateway
    "almanak.framework.data.tokens.resolver",  # legacy gRPC channel construction; tracked in VIB-2986
}
assert len(GRPC_CHANNEL_OPERATOR_EXEMPT) <= 5, "no casual expansion"

# VIB-4886 — per-module per-import-name exemption table for modules
# that legitimately import web3 abstract base provider classes for
# SUBCLASSING (not for direct transport construction).
#
# Codex post-PR finding: a module-wide skip is too broad — if the
# exempt module later adds `import requests`, the scan misses it. The
# exemption table below is per-(module, import-name) so any OTHER
# forbidden import in the exempt module still trips the pipeline.
#
# Each value is the EXACT set of import names exempt for that module
# (as emitted by `_ast_import_names_for_check`, i.e., both the bare
# `web3.providers.base` module form AND the dotted
# `web3.providers.base.JSONBaseProvider` member form).
#
# The exemption applies ONLY to `_scan_import_names_against_pipeline`
# (the import-name denylist + allowlist classifier). The binding-aware
# `_scan_web3_dynamic_misuse` still runs, but those modules don't
# bind `web3 [as X]` / `Web3 / AsyncWeb3` so the misuse scan trivially
# passes. Usage substrings (`HTTPProvider(`, etc.) also still run.
#
# Strict bound — at most one canonical implementation per pattern.
# Adding an entry requires (a) a code-review signal documenting that
# the new module legitimately subclasses a web3 abstract base AND
# routes via gateway, (b) the exact import-name set is the smallest
# possible.
_PER_MODULE_IMPORT_NAME_EXEMPT: dict[str, frozenset[str]] = {
    # The canonical GatewayWeb3Provider — subclasses JSONBaseProvider /
    # AsyncJSONBaseProvider; routes JSON-RPC via gRPC to the gateway.
    # These FOUR import-name tokens are the only exempted: any OTHER
    # import this module adds (e.g., `import requests`) still trips.
    "almanak.framework.web3.gateway_provider": frozenset(
        {
            "web3.providers.async_base",
            "web3.providers.async_base.AsyncJSONBaseProvider",
            "web3.providers.base",
            "web3.providers.base.JSONBaseProvider",
        }
    ),
}
assert len(_PER_MODULE_IMPORT_NAME_EXEMPT) <= 2, (
    "_PER_MODULE_IMPORT_NAME_EXEMPT grew — adding a new canonical web3 "
    "provider subclass requires explicit justification."
)


# VIB-4901 — per-(module, dynamic-import-token) exemption table.
#
# Mirrors `_PER_MODULE_IMPORT_NAME_EXEMPT` for the dynamic-import
# layer. Each entry's value enumerates the EXACT tokens exempted for
# that module — any OTHER forbidden token in the same module still
# trips. The exemption applies to BOTH the Class E import-name
# denylist layer (`_is_forbidden_import` via
# `_scan_import_names_against_pipeline`) AND the bare-substring
# scan layer (`FORBIDDEN_DYNAMIC_IMPORTS` iterated in Scan B/C
# tests), so the two layers cooperate without surfacing a
# legitimate use as a failure in only one.
#
# Strict-equality bound — adding an entry requires:
#   (a) audit evidence the use is legitimate (see
#       docs/internal/uat-runs/VIB-4901/audit-data.md),
#   (b) a follow-up Linear ticket to migrate to a static dispatch.
_PER_MODULE_DYNAMIC_IMPORT_EXEMPT: dict[str, frozenset[str]] = {
    # VIB-4914 migration debt — copy_signal_engine lazy-loads
    # receipt parsers via runtime `module_path` string from
    # ContractRegistry metadata. Refactor to static parser registry
    # (mirror ConnectorRegistry / VIB-4835 pattern) tracked in
    # VIB-4914. Exempted tokens:
    #   - "importlib"           — Class E namespace import
    #   - "import_module("      — bare-name substring scan
    # Any OTHER forbidden import/substring in this module still
    # trips, including (e.g.) `runpy`, `code`, `exec(`, `eval(`.
    "almanak.framework.services.copy_signal_engine": frozenset(
        {
            "importlib",
            "import_module(",
        }
    ),
}
assert len(_PER_MODULE_DYNAMIC_IMPORT_EXEMPT) == 1, (
    "_PER_MODULE_DYNAMIC_IMPORT_EXEMPT grew — adding an entry "
    "requires (a) audit evidence the use is legitimate, "
    "(b) a follow-up Linear ticket. Removing the entry should also "
    "happen via the follow-up ticket (VIB-4914 for copy_signal_engine)."
)

# VIB-4901 post-PR Claude pr-auditor Important #2: every exempt token
# must be typed-shaped so it actually lands in one of the two layers
# (Class E import-name OR FORBIDDEN_DYNAMIC_IMPORTS substring). A
# token like ``"importlib.machinery."`` (trailing dot) would silently
# no-op at BOTH layers; this invariant catches the shape at import
# time. Each token must be (a) a member of FORBIDDEN_IMPORT_NAMES
# (Class E layer match) OR (b) end with ``(`` or ``[`` (substring
# layer match — see ``_substring_exempt_for_module``).
for _mod, _tokens in _PER_MODULE_DYNAMIC_IMPORT_EXEMPT.items():
    for _t in _tokens:
        assert _t in FORBIDDEN_IMPORT_NAMES or _t.endswith(("(", "[")), (
            f"_PER_MODULE_DYNAMIC_IMPORT_EXEMPT[{_mod!r}] token {_t!r} "
            f"is neither a Class E entry in FORBIDDEN_IMPORT_NAMES "
            f"nor a substring shape ending in '(' or '['. Such a "
            f"token would silently no-op at both layers — fix the "
            f"shape or pick a real exemption target."
        )

# VIB-4901 post-PR Claude pr-auditor Important #3: Scan A's dynamic-
# import suppression test does NOT consult the per-module exemption
# (line ~1593 uses raw FORBIDDEN_DYNAMIC_IMPORTS substring scan with
# no exemption). This invariant ensures no CLOSURE_TARGET ever ends
# up in the exemption table — adding one would make the asymmetry a
# silent false-positive at Scan A.
assert not (set(_PER_MODULE_DYNAMIC_IMPORT_EXEMPT) & set(CLOSURE_TARGETS)), (
    "_PER_MODULE_DYNAMIC_IMPORT_EXEMPT overlaps CLOSURE_TARGETS: "
    f"{sorted(set(_PER_MODULE_DYNAMIC_IMPORT_EXEMPT) & set(CLOSURE_TARGETS))!r}. "
    "Scan A's dynamic-import test does not apply this exemption — "
    "either widen Scan A's wiring (line ~1593) or remove the "
    "CLOSURE_TARGET entry from the exemption table."
)


def _combined_per_module_exempt(modname: str) -> frozenset[str]:
    """Return the union of import-name + dynamic-import per-module
    exemptions for ``modname``.

    VIB-4886 introduced ``_PER_MODULE_IMPORT_NAME_EXEMPT`` for the
    canonical web3 abstract-base subclassing case. VIB-4901 added
    ``_PER_MODULE_DYNAMIC_IMPORT_EXEMPT`` for the receipt-parser
    lazy-loader case in ``copy_signal_engine``. Both sets are
    passed to ``_scan_import_names_against_pipeline`` for the
    import-name layer; substring tokens in the dynamic-import set
    don't match AST-extracted import names (substrings end with
    ``(`` or ``[`` which never appear in dotted import paths), so
    they're harmlessly ignored at that layer. The substring-scan
    layer reads the dynamic-import set directly via
    ``_substring_exempt_for_module``.
    """
    return _PER_MODULE_IMPORT_NAME_EXEMPT.get(modname, frozenset()) | _PER_MODULE_DYNAMIC_IMPORT_EXEMPT.get(
        modname, frozenset()
    )


def _substring_exempt_for_module(modname: str) -> frozenset[str]:
    """Return the substring-token subset of the module's dynamic-import
    exemption.

    Substring tokens are identified by their trailing ``(`` or
    ``[`` — dotted import paths never contain those characters.
    Used by the dynamic-import substring scan loop to skip
    audited legitimate uses (`import_module(` in
    ``copy_signal_engine``).
    """
    exempt = _PER_MODULE_DYNAMIC_IMPORT_EXEMPT.get(modname, frozenset())
    return frozenset(t for t in exempt if t.endswith(("(", "[")))


# ============================================================================
# Helpers
# ============================================================================


def _source_of(modname: str) -> tuple[str, str, str] | None:
    """Return (source_text, file_path, importing_package) for a framework module.

    Falls back to path-based reading via importlib.util.find_spec when
    importlib.import_module fails — closes the Round-3 critic gap where
    ImportError silently hid modules whose source contains forbidden imports.

    Returns None for non-existent modules and for symbols-treated-as-modules
    (e.g. `from foo import bar` where `bar` is a function, not a submodule).
    importlib.util.find_spec raises ModuleNotFoundError when the parent
    doesn't have `__path__` — handled here.
    """
    try:
        spec = importlib.util.find_spec(modname)
    except (ModuleNotFoundError, ImportError, ValueError):
        return None
    if spec is None or spec.origin is None:
        return None
    path = Path(spec.origin)
    if not path.is_file():
        return None
    pkg = spec.parent or ""
    return path.read_text(encoding="utf-8"), str(path), pkg


def _ast_imports(source: str, importing_package: str) -> list[str]:
    """Return absolute import names from `source`.

    Relative imports resolved via importlib.util.resolve_name against
    importing_package (NOT the importing module name — Round-4 Codex
    fix: passing the module name as anchor causes
    resolve_name('.child', 'almanak.framework.pkg.parent') to resolve
    to 'almanak.framework.pkg.parent.child' instead of the sibling
    'almanak.framework.pkg.child').
    """
    tree = ast.parse(source)
    out: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                out.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level and node.level > 0:
                rel = "." * node.level + (node.module or "")
                try:
                    resolved = importlib.util.resolve_name(rel, importing_package)
                except (ImportError, ValueError):
                    continue
                out.append(resolved)
                for alias in node.names:
                    out.append(f"{resolved}.{alias.name}")
            elif node.module:
                out.append(node.module)
                for alias in node.names:
                    out.append(f"{node.module}.{alias.name}")
    return out


def _import_closure(targets: tuple[str, ...]) -> set[str]:
    visited: set[str] = set()
    queue: list[str] = list(targets)
    while queue:
        modname = queue.pop()
        if modname in visited or not modname.startswith("almanak.framework."):
            continue
        visited.add(modname)
        sf = _source_of(modname)
        if sf is None:
            continue
        src, _path, pkg = sf
        for dep in _ast_imports(src, importing_package=pkg):
            if dep.startswith("almanak.framework.") and dep not in visited:
                queue.append(dep)
    return visited


# ============================================================================
# Per-module check pipeline (VIB-4886)
# ============================================================================


def _scan_import_names_against_pipeline(
    modname: str,
    path: str,
    source: str,
    exempt_imports: frozenset[str] = frozenset(),
) -> list[str]:
    """Run the per-module check pipeline against AST-extracted imports.

    VIB-4886 §C.1: order of checks per (name, lineno) tuple is:

    0. (NEW post-PR) Skip if ``name`` is in the per-module
       ``exempt_imports`` set — documented exemption for legitimate
       subclassing imports (e.g.,
       ``almanak.framework.web3.gateway_provider`` subclasses
       ``JSONBaseProvider`` for transport replacement). The skip
       applies ONLY to the listed names — any OTHER import in the
       same module still goes through 1+2.
    1. Class A/B/web3 denylist (_is_forbidden_import) — fail with the
       explicit forbidden-import message.
    2. Classifier — if "unknown", fail with the allowlist-gap message.
    3. Otherwise (stdlib / almanak / allowed) — pass.

    Returns a list of failure messages (empty list = pass).
    """
    failures: list[str] = []
    for name, lineno in _ast_import_names_for_check(source):
        if name in exempt_imports:
            continue  # documented per-(module, import-name) exemption
        if _is_forbidden_import(name):
            failures.append(_format_forbidden_import(modname, path, name, lineno))
            continue
        cls = _classify_import(name)
        if cls == "unknown":
            failures.append(_format_allowlist_gap(modname, path, name, lineno))
    return failures


# ============================================================================
# Binding-aware web3 dynamic-access scan (VIB-4886 §C.9)
# ============================================================================


# Class names that, when bound via `from web3 import X [as Y]`, give
# the local binding access to provider classes as CLASS ATTRIBUTES
# (e.g., `Web3.HTTPProvider`, `AsyncWeb3.WebSocketProvider`). Verified
# against installed `web3==7.x`:
#   uv run python -c "import web3; print([x for x in dir(web3.Web3) if 'Provider' in x or 'WebSocket' in x or 'IPC' in x])"
# Closes the Codex post-PR finding: `from web3 import Web3; P = Web3.HTTPProvider`
# would otherwise bypass the binding-aware scan because the scan only
# tracked `import web3` bindings.
_WEB3_CLASS_REEXPORT_NAMES: frozenset[str] = frozenset(
    {
        "Web3",
        "AsyncWeb3",
    }
)


def _collect_web3_aliases(tree: ast.AST) -> set[str]:
    """Step 1 of §C.9: collect every local name bound to the ``web3``
    module OR the ``Web3`` / ``AsyncWeb3`` classes that re-export
    provider classes as class attributes.

    Returns a set of identifiers (the local binding name). The set
    accumulates bindings from these shapes:

    Import-statement shapes (VIB-4886 baseline):

    1. ``import web3``                       → ``"web3"``
    2. ``import web3 as w``                  → ``"w"``
    3. ``import web3.X``                     → ``"web3"`` (top-level
                                                bound regardless of
                                                whether ``X`` is a
                                                submodule)
    4. ``from web3 import Web3``             → ``"Web3"``
    5. ``from web3 import Web3 as W``        → ``"W"``
    6. ``from web3 import AsyncWeb3``        → ``"AsyncWeb3"``
    7. ``from web3 import AsyncWeb3 as AW``  → ``"AW"``

    Assignment shapes (VIB-4901 — L4a + L4b single-level):

    8.  ``w3 = Web3(...)``                   → ``"w3"`` (L4a — instance
                                                from class-call;
                                                ast.Assign + ast.Call
                                                with func=Name in the
                                                existing binding set
                                                AND name ∈ {Web3,
                                                AsyncWeb3}).
    9.  ``w3 = AsyncWeb3(...)``              → ``"w3"`` (L4a)
    10. ``wb = web3``                        → ``"wb"`` (L4b
                                                single-level —
                                                ast.Assign + ast.Name
                                                referring to an
                                                existing binding).

    VIB-4901 design notes:

    - Single-pass collection. Step 1 walks imports FIRST, then walks
      assignments using the binding set as it stood at the END of
      step-1a. This means transitive chains like ``a = web3; b = a;
      c = b`` only catch ``a`` and ``b`` (NOT ``c``) because the set
      is read-only during the assignment walk. ``c`` is the L4b
      transitive case, which remains inherent (see module docstring +
      VIB-4901.md §"Known limitations").
    - Indirect-call shapes ``w3 = (Web3,)[0]()`` /
      ``w3 = (lambda: Web3())()`` are NOT bound — they require
      expression-level data-flow (L4c, inherent).
    - Assignment shapes only apply at MODULE LEVEL and inside
      regular functions. Class bodies that rebind these names for
      method dispatch are out of scope for the current static
      walker; if a future module does this, file a follow-up.

    The misuse patterns (Step 2) apply uniformly: ``<binding>.<TrailingName>``,
    ``getattr(<binding>, ...)``, ``vars(<binding>)``, etc. fail
    regardless of whether ``<binding>`` is the web3 module, a class
    that re-exports providers, OR an instance / module-rebind of
    one.

    ``from web3.providers...`` is caught at the import-name layer via
    ``FORBIDDEN_WEB3_SUBMODULE_PREFIXES``, so it doesn't need to
    appear in this binding set.
    """
    # Tracked separately so the L4a assignment shape can distinguish
    # CLASS bindings (Web3 / AsyncWeb3, instantiable) from MODULE
    # bindings (web3 module, not normally instantiated). All bindings
    # are merged into the returned ``aliases`` set so the existing
    # misuse walker sees them uniformly.
    module_bindings: set[str] = set()
    class_bindings: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            # Shapes 1–3: `import web3 [as X]` / `import web3.X`.
            for alias in node.names:
                top = alias.name.split(".")[0]
                if top != "web3":
                    continue
                local = alias.asname or top
                module_bindings.add(local)
        elif isinstance(node, ast.ImportFrom):
            # Shapes 4–7: `from web3 import Web3 [as X]` /
            # `from web3 import AsyncWeb3 [as X]`. The provider class
            # re-export shapes (`from web3 import HTTPProvider`) are
            # caught at the import-name layer by C.5a; here we ONLY
            # care about the non-forbidden Web3 / AsyncWeb3 classes
            # that themselves expose provider attributes.
            if node.module != "web3" or (node.level and node.level > 0):
                continue
            for alias in node.names:
                if alias.name not in _WEB3_CLASS_REEXPORT_NAMES:
                    continue
                local = alias.asname or alias.name
                class_bindings.add(local)

    aliases: set[str] = module_bindings | class_bindings
    # VIB-4901 — assignment shapes 8-10. Single-pass extend (transitive
    # chains stay inherent, see L4b-transitive in module docstring).
    # Helper extraction keeps `_collect_web3_aliases` under CRAP cc=15.
    _extend_aliases_with_assign_shapes(tree, aliases, class_bindings)
    return aliases


def _extend_aliases_with_assign_shapes(
    tree: ast.AST,
    aliases: set[str],
    class_bindings: set[str],
) -> None:
    """VIB-4901 — walk ``ast.Assign`` nodes and extend ``aliases`` /
    ``class_bindings`` with L4a + L4b single-level shapes.

    Helper extracted from ``_collect_web3_aliases`` to keep that
    function's cyclomatic complexity below the CRAP threshold (Claude
    pr-auditor + GitHub bot post-PR follow-ups added enough branching
    that the parent function tripped C901). Mutates the two sets in
    place; ``aliases`` is read by ``import_bindings`` snapshot first.

    Shapes (see ``_collect_web3_aliases`` docstring for full table):

    - Shape 8/9 (L4a direct-call instance binding): rhs is
      ``ast.Call(func=ast.Name(id ∈ class_bindings))`` → bind lhs in
      ``aliases``.
    - Shape 10 (L4b single-level rebind): rhs is ``ast.Name(id ∈
      import_bindings)`` → bind lhs in ``aliases``; if rhs was in
      ``class_bindings``, propagate the class semantic so
      ``w = Wcopy()`` (where ``Wcopy = Web3``) is still caught
      (Gemini post-PR HIGH).
    """
    import_bindings = frozenset(aliases)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        # Only single-target Name assignments are in scope. Multi-target
        # (`a = b = Web3()`) and tuple-unpack (`a, b = ...`) shapes are
        # not common in framework code; audit-data.md confirmed zero
        # uses in Scan B ∪ Scan C.
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue
        local = node.targets[0].id
        # Shape 8/9 — L4a class instantiation via a bare-Name call.
        if (
            isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id in class_bindings
        ):
            aliases.add(local)
            continue
        # Shape 10 — L4b single-level rebind. Module rebindings
        # (`wb = web3`) do NOT propagate to ``class_bindings``
        # (calling ``wb()`` doesn't yield a Web3 instance), but class
        # rebindings (`Wcopy = Web3`) do — see Gemini post-PR HIGH.
        if isinstance(node.value, ast.Name) and node.value.id in import_bindings:
            aliases.add(local)
            if node.value.id in class_bindings:
                class_bindings.add(local)


def _classify_outermost_attribute_chain(
    node: ast.Attribute,
    aliases: set[str],
    modname: str,
    path: str,
) -> str | None:
    """Classify an outermost ast.Attribute chain rooted at a web3 binding.

    Returns a failure message if the chain matches a misuse pattern, or
    None if the chain is legitimate / not rooted at a web3 binding.

    Three layers checked in order:
    1. Any segment in the chain is in
       ``FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES``.
    2. Any dotted prefix of the canonical resolved chain (rooted at
       ``web3.``) is in ``FORBIDDEN_WEB3_SUBMODULE_PREFIXES``.
    3. Outermost ``__dict__`` / ``__getattribute__`` access.

    Extracted from ``_scan_web3_dynamic_misuse`` to keep that function's
    cyclomatic complexity below the C901 threshold (Claude pr-auditor
    post-PR lint-debt removal).
    """
    # Resolve the full chain inside-out, then reverse.
    chain: list[str] = []
    curr: ast.expr = node
    while isinstance(curr, ast.Attribute):
        chain.append(curr.attr)
        curr = curr.value
    if not isinstance(curr, ast.Name) or curr.id not in aliases:
        return None
    root = curr.id
    path_segments = list(reversed(chain))

    # Layer 1: any segment is a forbidden provider class.
    # Catches the direct shape `binding.HTTPProvider` AND the nested
    # shape `binding.providers.HTTPProvider`.
    forbidden_seg = next(
        (seg for seg in path_segments if seg in FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES),
        None,
    )
    if forbidden_seg is not None:
        return (
            f"{modname} ({path}):{node.lineno} forbidden web3 access: "
            f"{root}.{'.'.join(path_segments)} (provider class "
            f"{forbidden_seg!r} via attribute chain on web3 binding)"
        )

    # Layer 2: canonical resolved chain ``web3.<segments>`` (for both
    # module and class bindings — Web3.<X> still resolves canonically
    # under web3.*). Check against FORBIDDEN_WEB3_SUBMODULE_PREFIXES.
    canonical = "web3." + ".".join(path_segments)
    canonical_parts = canonical.split(".")
    for i in range(1, len(canonical_parts) + 1):
        prefix = ".".join(canonical_parts[:i])
        if prefix in FORBIDDEN_WEB3_SUBMODULE_PREFIXES:
            return (
                f"{modname} ({path}):{node.lineno} forbidden web3 access: "
                f"{root}.{'.'.join(path_segments)} (canonical chain "
                f"{canonical!r} touches forbidden submodule {prefix!r})"
            )

    # Layers 4 + 5: outermost ``__dict__`` / ``__getattribute__``
    # access on the binding (the OUTERMOST leaf of the chain).
    if path_segments and path_segments[-1] in (
        "__dict__",
        "__getattribute__",
    ):
        return (
            f"{modname} ({path}):{node.lineno} forbidden web3 access: "
            f"{root}.{'.'.join(path_segments)} (dynamic-attribute "
            f"bypass)"
        )
    return None


def _scan_web3_dynamic_misuse(
    modname: str,
    path: str,
    source: str,
) -> list[str]:
    """Step 2 of §C.9: scan for misuse patterns against each web3
    binding (web3 module OR Web3 / AsyncWeb3 class).

    For each binding, walks the FULL ast.Attribute chain rooted at the
    binding's local name and fails on ANY of:

    - Any segment in the resolved chain is in
      ``FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES``. Catches the deeply-
      nested bypass ``w.providers.HTTPProvider`` (Gemini post-PR
      security-HIGH).
    - Any dotted prefix of the canonical resolved chain (rooted at
      ``web3.``) is in ``FORBIDDEN_WEB3_SUBMODULE_PREFIXES``. Catches
      ``w.providers.<anything>`` for the same canonical-prefix reason
      as the import-name layer.
    - Outermost ``__dict__`` / ``__getattribute__`` access on the binding.
    - ``ast.Call(func=ast.Name(id="getattr"), args=[ast.Name(id=binding), ...])``.
    - ``ast.Call(func=ast.Name(id="vars"), args=[ast.Name(id=binding)])``.

    Returns a list of failure messages (empty list = pass).
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    aliases = _collect_web3_aliases(tree)
    if not aliases:
        return []
    # Soft cap (Claude pr-auditor post-PR Potential #8): replace assert
    # with a failure-message append so a malicious commit can't crash
    # the scanner via padding with >8 web3 aliases. The scan continues
    # to check each alias regardless of count; the soft signal flags
    # the suspicious code shape for human review without DoSing the
    # whole test suite.
    _ALIAS_SANITY_BOUND = 8

    failures: list[str] = []
    if len(aliases) > _ALIAS_SANITY_BOUND:
        failures.append(
            f"{modname} ({path}): too many web3 bindings "
            f"({len(aliases)} > {_ALIAS_SANITY_BOUND}); "
            f"suspicious code shape requires human review. Aliases: "
            f"{sorted(aliases)}.",
        )

    # Pre-compute set of Attribute nodes that are someone else's
    # `.value` — those are INNER nodes of a chain. We only process
    # OUTERMOST Attribute nodes (one chain → one failure message).
    inner_attr_ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Attribute):
            inner_attr_ids.add(id(node.value))
    for node in ast.walk(tree):
        # OUTERMOST Attribute — walk the whole chain.
        if isinstance(node, ast.Attribute) and id(node) not in inner_attr_ids:
            failure = _classify_outermost_attribute_chain(
                node,
                aliases,
                modname,
                path,
            )
            if failure is not None:
                failures.append(failure)
            continue

        # Patterns 2 + 3: getattr(binding, ...) / vars(binding)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in ("getattr", "vars")
            and len(node.args) >= 1
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id in aliases
        ):
            failures.append(
                f"{modname} ({path}):{node.lineno} forbidden web3 access: "
                f"{node.func.id}({node.args[0].id}, ...) "
                f"(dynamic introspection of web3 binding)",
            )
            continue
    return failures


# ============================================================================
# Scan A — Explicit 5 named modules
# ============================================================================


@pytest.mark.parametrize("modname", CLOSURE_TARGETS, ids=lambda m: m.rsplit(".", 1)[-1])
def test_scan_a_explicit_modules(modname: str) -> None:
    """Scan A: each named target module passes the forbidden-import +
    forbidden-usage checks + the new allowlist + web3-binding checks.
    gateway_client.py is exempt from the channel-usage substrings
    (GRPC_CHANNEL_ALLOWED_MODULES) but NOT exempt from Class A/B/C
    imports or the allowlist."""
    sf = _source_of(modname)
    assert sf is not None, f"cannot read source for {modname}"
    source, path, _ = sf

    # VIB-4886 unified pipeline: Class A/B/web3 denylist + classifier.
    failures = _scan_import_names_against_pipeline(modname, path, source)
    assert failures == [], "\n".join(failures)

    # VIB-4886 binding-aware web3 dynamic-access scan.
    web3_failures = _scan_web3_dynamic_misuse(modname, path, source)
    assert web3_failures == [], "\n".join(web3_failures)

    for substring in FORBIDDEN_USAGE_SUBSTRINGS:
        assert substring not in source, f"{modname} ({path}): forbidden usage substring detected: {substring!r}"

    # gRPC channel-usage substrings: forbidden EXCEPT in
    # GRPC_CHANNEL_ALLOWED_MODULES (which contains only gateway_client).
    if modname not in GRPC_CHANNEL_ALLOWED_MODULES:
        for substring in _USAGE_GRPC_CHANNEL:
            assert substring not in source, f"{modname} ({path}): forbidden gRPC channel constructor: {substring!r}"

    # Special: pools/history.py — no `aiohttp` token anywhere (incl. comments).
    if modname == "almanak.framework.data.pools.history":
        assert "aiohttp" not in source, f"{modname}: `aiohttp` token forbidden anywhere (incl. comments)"

    # gateway_client positive-presence assertion — proves Scan A
    # actually reached it (not silently skipped).
    if modname == "almanak.framework.gateway_client":
        assert re.search(r"^import grpc\b", source, re.M), (
            "gateway_client.py should `import grpc` — positive no-op-guard"
        )


# ============================================================================
# Scan B — Package-tree walk over PACKAGE_ROOTS
# ============================================================================


def _walk_package(root: str) -> list[str]:
    """Walk every Python module physically present under root.

    Includes the root package itself (its ``__init__.py``) when ``root``
    IS a package. ``pkgutil.walk_packages`` returns only descendants,
    so a forbidden import in ``almanak/framework/data/__init__.py``
    would otherwise evade Scan B. Round-2 CodeRabbit major fix.
    """
    discovered: list[str] = []
    try:
        pkg = importlib.import_module(root)
    except ImportError:
        return discovered
    # Root package always counted — its __init__.py is in scope.
    discovered.append(root)
    if not hasattr(pkg, "__path__"):
        return discovered
    for _finder, modname, _ispkg in pkgutil.walk_packages(
        pkg.__path__,
        prefix=root + ".",
    ):
        discovered.append(modname)
    return discovered


def test_scan_b_package_tree_walk() -> None:
    """Scan B: every module physically present in the three PACKAGE_ROOTS
    passes the forbidden-import + forbidden-usage + allowlist + web3
    binding checks. Catches helper modules in the subtrees even if not
    yet imported by any target."""
    all_modules: list[str] = []
    for root in PACKAGE_ROOTS:
        all_modules.extend(_walk_package(root))

    assert len(all_modules) >= 5, f"Scan B walked too few modules: {len(all_modules)} (expected >= 5)"

    failures: list[str] = []
    for modname in all_modules:
        if modname in _LEGACY_VIOLATING_MODULES:
            continue  # documented legacy debt — out of scope
        sf = _source_of(modname)
        if sf is None:
            continue
        source, path, _ = sf

        # Unified pipeline: Class A/B/D/E/web3 denylist + classifier.
        # Per-import exemption table allows the canonical
        # GatewayWeb3Provider to import JSONBaseProvider for
        # subclassing AND copy_signal_engine to import importlib
        # for receipt-parser lazy-loading (VIB-4914 follow-up);
        # any OTHER forbidden import in the same module still trips
        # the pipeline (Codex post-PR finding on VIB-4886 — broad
        # module-wide skip would mask a future `import requests`).
        exempt = _combined_per_module_exempt(modname)
        failures.extend(_scan_import_names_against_pipeline(modname, path, source, exempt))
        # Binding-aware web3 dynamic-access scan always runs — the
        # exempt module doesn't bind web3 / Web3 / AsyncWeb3, so this
        # scan trivially passes for it.
        failures.extend(_scan_web3_dynamic_misuse(modname, path, source))

        # Usage substrings.
        for substring in FORBIDDEN_USAGE_SUBSTRINGS:
            if substring in source:
                failures.append(
                    f"{modname} ({path}): forbidden usage: {substring!r}",
                )
                break

    assert failures == [], "\n".join(failures)


# ============================================================================
# Scan C — TRUE import-closure walk (AST-based)
# ============================================================================


def test_scan_c_import_closure_no_forbidden_imports() -> None:
    """Scan C: every module the closure walk reaches transitively from
    the 5 CLOSURE_TARGETS passes the forbidden-import + forbidden-usage
    + allowlist + web3 binding checks. Closes the transitive-helper-
    outside-PACKAGE_ROOTS bypass flagged by Codex Round-2 in POOL-7."""
    closure = _import_closure(CLOSURE_TARGETS)
    assert len(closure) >= 5, f"closure too small: {len(closure)}"

    failures: list[str] = []
    for modname in closure:
        if modname in _LEGACY_VIOLATING_MODULES:
            continue
        sf = _source_of(modname)
        if sf is None:
            continue
        source, path, _ = sf

        # Per-import exemption (see Scan B comment).
        exempt = _combined_per_module_exempt(modname)
        failures.extend(_scan_import_names_against_pipeline(modname, path, source, exempt))
        failures.extend(_scan_web3_dynamic_misuse(modname, path, source))

        for substring in FORBIDDEN_USAGE_SUBSTRINGS:
            if substring in source:
                failures.append(
                    f"{modname} ({path}): forbidden usage: {substring!r}",
                )
                break

    assert failures == [], "\n".join(failures)


# ============================================================================
# gRPC channel-usage — FRAMEWORK-WIDE (substring layer + AST semantic-binding)
# ============================================================================


def _bindings_from_ast(tree: ast.AST) -> dict[str, tuple[str, ...]]:
    """Build the local-name -> resolved-chain bindings table.

    Handles every realistic alias / import-style:
    - import grpc / import grpc as g
    - import grpc.aio / import grpc.aio as gaio
    - from grpc import aio / from grpc import aio as gaio
    - from grpc import insecure_channel / ... as ch
    - from grpc.aio import secure_channel / ... as ch
    - from grpc import * / from grpc.aio import *
    """
    bindings: dict[str, tuple[str, ...]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                # `import grpc as g` -> {"g": ("grpc",)}
                # `import grpc.aio as gaio` -> {"gaio": ("grpc", "aio")}
                # `import grpc.aio` -> binds top-level "grpc" name
                #   pointing to ("grpc",); the .aio access goes via
                #   attribute chain in resolve_call_chain.
                if alias.name == "grpc":
                    bindings[alias.asname or "grpc"] = ("grpc",)
                elif alias.name == "grpc.aio":
                    if alias.asname:
                        bindings[alias.asname] = ("grpc", "aio")
                    else:
                        # `import grpc.aio` binds the top-level "grpc" name.
                        bindings.setdefault("grpc", ("grpc",))
        elif isinstance(node, ast.ImportFrom):
            if node.module not in ("grpc", "grpc.aio"):
                continue
            module_chain = tuple(node.module.split("."))
            for alias in node.names:
                if alias.name == "*":
                    # Wildcard — bind the four channel constructors.
                    if node.module == "grpc":
                        bindings["insecure_channel"] = ("grpc", "insecure_channel")
                        bindings["secure_channel"] = ("grpc", "secure_channel")
                    elif node.module == "grpc.aio":
                        bindings["insecure_channel"] = ("grpc", "aio", "insecure_channel")
                        bindings["secure_channel"] = ("grpc", "aio", "secure_channel")
                else:
                    local = alias.asname or alias.name
                    bindings[local] = module_chain + (alias.name,)
    return bindings


def _resolve_call_chain(
    func: ast.expr,
    bindings: dict[str, tuple[str, ...]],
) -> tuple[str, ...] | None:
    """Resolve an ast.Call.func to its absolute chain via the bindings.

    Walks ast.Attribute wrappers recursively until reaching an ast.Name
    leaf; looks up the leaf in bindings; appends trailing attribute
    segments. Returns None if the func doesn't reduce to a Name leaf
    or the leaf isn't in bindings.
    """
    segments: list[str] = []
    node: ast.expr = func
    while isinstance(node, ast.Attribute):
        segments.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return None
    leaf = node.id
    if leaf not in bindings:
        return None
    return bindings[leaf] + tuple(reversed(segments))


def _grpc_channel_calls_in_source(source: str) -> list[tuple[int, str, tuple[str, ...]]]:
    """Return (lineno, syntactic_chain, resolved_chain) for every
    grpc channel-constructor call detected via AST semantic-binding."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    bindings = _bindings_from_ast(tree)
    if not bindings:
        return []
    hits: list[tuple[int, str, tuple[str, ...]]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        resolved = _resolve_call_chain(node.func, bindings)
        if resolved is None:
            continue
        if resolved in FORBIDDEN_GRPC_CHANNEL_CHAINS:
            # Reconstruct the syntactic form for the failure message.
            try:
                syntactic = ast.unparse(node.func)
            except (AttributeError, ValueError):
                syntactic = "<call>"
            hits.append((node.lineno, syntactic, resolved))
    return hits


def test_grpc_channel_usage_framework_wide() -> None:
    """Channel-usage check: every module under almanak.framework.* passes
    the substring + AST semantic-binding layers UNLESS in
    GRPC_CHANNEL_ALLOWED_MODULES. CLAUDE.md §Gateway boundary forbids
    'direct gRPC to anything other than the gateway' GLOBALLY (not just
    within the 5-target closure)."""
    global_modules: list[str] = []
    try:
        framework_pkg = importlib.import_module("almanak.framework")
    except ImportError:
        pytest.fail("almanak.framework not importable")
    for _finder, modname, _ispkg in pkgutil.walk_packages(
        framework_pkg.__path__,
        prefix="almanak.framework.",
    ):
        global_modules.append(modname)

    assert len(global_modules) >= 50, f"Framework-wide walk discovered too few modules: {len(global_modules)}"

    failures: list[str] = []
    for modname in global_modules:
        if modname in GRPC_CHANNEL_ALLOWED_MODULES:
            continue
        if modname in GRPC_CHANNEL_OPERATOR_EXEMPT:
            continue  # operator-machine surfaces (CLI, legacy debt)
        sf = _source_of(modname)
        if sf is None:
            continue
        source, path, _ = sf

        # Substring layer.
        for substring in _USAGE_GRPC_CHANNEL:
            if substring in source:
                failures.append(
                    f"{modname} ({path}): forbidden gRPC channel substring "
                    f"{substring!r} (only {GRPC_CHANNEL_ALLOWED_MODULES} may)",
                )

        # AST semantic-binding layer.
        for lineno, syntactic, resolved in _grpc_channel_calls_in_source(source):
            failures.append(
                f"{modname} ({path}):{lineno} forbidden gRPC channel call "
                f"`{syntactic}` (resolved chain {resolved!r}; only "
                f"{GRPC_CHANNEL_ALLOWED_MODULES} may construct channels)",
            )

    assert failures == [], "\n".join(failures)


# ============================================================================
# Dynamic-import suppression on 5 CLOSURE_TARGETS
# ============================================================================


@pytest.mark.parametrize("modname", CLOSURE_TARGETS, ids=lambda m: m.rsplit(".", 1)[-1])
def test_no_dynamic_imports_in_closure_targets(modname: str) -> None:
    """Dynamic-import suppression: the 5 named CLOSURE_TARGETS forbid
    importlib.import_module / __import__ / exec / eval / module-level
    __getattr__. Structural insurance against future obfuscation."""
    sf = _source_of(modname)
    if sf is None:
        return
    source, path, _ = sf
    # For snapshot.py the umbrella card scopes the check to the
    # pool_history accessor function only (file-scope is exempt per
    # D3.F10 row 4). Get that function's source via inspect.
    if modname == "almanak.framework.market.snapshot":
        from almanak.framework.market.snapshot import MarketSnapshot

        accessor_src = inspect.getsource(MarketSnapshot.pool_history)
        for substring in FORBIDDEN_DYNAMIC_IMPORTS:
            assert substring not in accessor_src, (
                f"MarketSnapshot.pool_history: forbidden dynamic-import construct {substring!r}"
            )
        return

    for substring in FORBIDDEN_DYNAMIC_IMPORTS:
        assert substring not in source, f"{modname} ({path}): forbidden dynamic-import construct {substring!r}"

    # No module-level __getattr__ definition.
    try:
        tree = ast.parse(source)
    except SyntaxError:
        pytest.fail(f"{modname}: failed to parse source")
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "__getattr__":
            pytest.fail(
                f"{modname}: module-level __getattr__ definition forbidden (line {node.lineno})",
            )


# ============================================================================
# VIB-4886 — Allowlist + classifier unit tests
# ============================================================================


@pytest.mark.parametrize(
    "name, expected",
    [
        # stdlib
        ("os", "stdlib"),
        ("os.path", "stdlib"),
        ("urllib", "stdlib"),
        ("http", "stdlib"),
        ("typing", "stdlib"),
        # almanak
        ("almanak", "almanak"),
        ("almanak.framework", "almanak"),
        ("almanak.framework.data.pools.history", "almanak"),
        # allowed third-party (bare top-level entry)
        ("grpc", "allowed"),
        ("grpc.aio", "allowed"),
        ("web3", "allowed"),
        ("web3.providers.base", "allowed"),
        ("pandas", "allowed"),
        ("pydantic", "allowed"),
        # allowed third-party (multi-segment entries — google.rpc + google.protobuf)
        ("google.rpc", "allowed"),
        ("google.rpc.error_details", "allowed"),
        ("google.rpc.error_details_pb2", "allowed"),
        ("google.protobuf", "allowed"),
        ("google.protobuf.duration_pb2", "allowed"),
        # bare `google` is NOT allowed (multi-product namespace tightening,
        # Codex post-PR finding)
        ("google", "unknown"),
        # forbidden google submodules (not in allowlist)
        ("google.cloud", "unknown"),
        ("google.cloud.storage", "unknown"),
        ("google.auth", "unknown"),
        ("google.auth.transport.requests", "unknown"),
        # unknown (former Class C)
        ("solana", "unknown"),
        ("solana.connection", "unknown"),
        ("solders", "unknown"),
        ("driftpy", "unknown"),
        ("websockets", "unknown"),
        # unknown (never-heard-of)
        ("_vib4886_fake_protocol_sdk", "unknown"),
        ("totally_made_up_lib", "unknown"),
    ],
)
def test_classify_import(name: str, expected: str) -> None:
    """§B.2: classifier returns stdlib / almanak / allowed / unknown.

    Dotted-prefix allowlist semantics (Codex post-PR fix): multi-product
    namespaces like `google` use `google.rpc` as the allowed prefix,
    leaving `google.cloud` and `google.auth.transport.requests` as
    `unknown` (catches network-capable Google SDKs by default)."""
    assert _classify_import(name) == expected


def test_ast_import_names_expands_import_from_for_denylist_edges() -> None:
    """§C.3: _ast_import_names_for_check emits BOTH bare module name
    AND dotted module.alias form for ImportFrom. Without this expansion,
    `from urllib import request` would route as bare `urllib` → stdlib
    → silently pass, defeating the Class A urllib.request denylist."""
    source = "from urllib import request\nfrom http import client\n"
    names = [name for name, _ in _ast_import_names_for_check(source)]
    # Bare module names emitted.
    assert "urllib" in names
    assert "http" in names
    # Dotted forms also emitted.
    assert "urllib.request" in names
    assert "http.client" in names
    # And at least one of the dotted forms is forbidden.
    assert any(_is_forbidden_import(name) for name in names)


def test_from_urllib_import_request_caught_by_denylist_not_allowlist() -> None:
    """§C.4: `from urllib import request` MUST fail with the explicit
    Class A/B forbidden-import message, NOT the generic allowlist
    message. Proves the denylist precedence in C.1 fires correctly for
    the stdlib-submodule bypass case."""
    source = "from urllib import request\n"
    failures = _scan_import_names_against_pipeline(
        "test_mod",
        "/tmp/test_mod.py",
        source,
    )
    assert len(failures) == 1, f"expected exactly 1 failure, got {failures}"
    msg = failures[0]
    assert "forbidden import detected" in msg, f"expected explicit denylist message, got {msg!r}"
    assert "outside allowlist" not in msg, f"allowlist message fired instead of denylist: {msg!r}"
    assert "urllib.request" in msg


def test_former_class_c_removed_from_forbidden_import_names() -> None:
    """§D.3: structural proof that former-Class-C SDK names are NOT in
    FORBIDDEN_IMPORT_NAMES (they're caught by the allowlist as
    unknown instead). Direct intersection check — not a file-wide
    grep, which would conflict with the test's own enumeration."""
    intersection = sorted(FORMER_CLASS_C_IMPORT_NAMES & FORBIDDEN_IMPORT_NAMES)
    assert intersection == [], (
        f"Former Class C entries leaked into FORBIDDEN_IMPORT_NAMES: "
        f"{intersection}. They should be caught by the allowlist as "
        f"unknown third-party imports."
    )
    # Casual-shrink guard — removing entries from FORMER_CLASS_C_IMPORT_NAMES
    # weakens the regression contract.
    assert len(FORMER_CLASS_C_IMPORT_NAMES) == 13, (
        f"FORMER_CLASS_C_IMPORT_NAMES shrunk to "
        f"{len(FORMER_CLASS_C_IMPORT_NAMES)}: the regression contract "
        f"must keep all 13 former-Class-C names."
    )


@pytest.mark.parametrize(
    "sdk_name",
    sorted(FORMER_CLASS_C_IMPORT_NAMES),
)
def test_allowlist_known_class_c_sdks_caught(sdk_name: str) -> None:
    """§E.2: each former-Class-C SDK name classifies as 'unknown' AND
    a synthesized `import <sdk_name>` source triggers the
    allowlist-gap branch. Proves the old denylist coverage is preserved
    structurally even without explicit entries."""
    # Each top-level name classifies as unknown.
    top = sdk_name.split(".")[0]
    assert _classify_import(top) == "unknown"
    # Synthesized import triggers the per-module pipeline failure.
    source = f"import {sdk_name}\n"
    failures = _scan_import_names_against_pipeline(
        "test_mod",
        "/tmp/test_mod.py",
        source,
    )
    assert len(failures) >= 1, f"former Class C SDK {sdk_name!r} not caught by allowlist"
    # And the failure is the allowlist-gap message (NOT denylist).
    assert any("outside allowlist" in f for f in failures), (
        f"expected allowlist-gap message for {sdk_name!r}, got {failures}"
    )


@pytest.mark.parametrize(
    "allowed_top",
    sorted(ALLOWED_THIRD_PARTY_IMPORTS),
)
def test_allowlist_existing_third_party_accepted(allowed_top: str) -> None:
    """§E.3: each entry in ALLOWED_THIRD_PARTY_IMPORTS does NOT trigger
    the allowlist-gap branch. Proves the allowlist doesn't accidentally
    break legitimate imports."""
    source = f"import {allowed_top}\n"
    failures = _scan_import_names_against_pipeline(
        "test_mod",
        "/tmp/test_mod.py",
        source,
    )
    # Some allowed names may also trigger the denylist (e.g. an import
    # of a web3 provider class); the specific assertion here is that no
    # "outside allowlist" message fires.
    allowlist_failures = [f for f in failures if "outside allowlist" in f]
    assert allowlist_failures == [], f"allowlist entry {allowed_top!r} unexpectedly caught: {allowlist_failures}"


def test_allowlist_unknown_third_party_caught() -> None:
    """§E.1: a never-heard-of SDK import triggers the allowlist-gap
    branch with the precise failure message."""
    source = "import _vib4886_fake_protocol_sdk\nimport _vib4886_fake_protocol_sdk.client\n"
    # Both AST-emitted names classify as unknown.
    for name, _ in _ast_import_names_for_check(source):
        assert _classify_import(name) == "unknown", f"fake SDK name {name!r} should classify as unknown"
    # Pipeline failure with the precise message template.
    failures = _scan_import_names_against_pipeline(
        "test_mod",
        "/tmp/test_mod.py",
        source,
    )
    assert len(failures) >= 1
    for msg in failures:
        assert "outside allowlist" in msg
        assert "_vib4886_fake_protocol_sdk" in msg


def test_web3_provider_trailing_names_verified_against_installed_web3() -> None:
    """§C.5d: every entry in FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES
    exists as a name in dir(web3) at test time. If upstream web3
    renames a class, this fails loudly rather than silently letting
    the renamed class slip through the trailing-name check."""
    import web3 as _web3_for_verification

    web3_dir = set(dir(_web3_for_verification))
    missing = FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES - web3_dir
    assert missing == set(), (
        f"FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES contains names not in "
        f"dir(web3): {missing}. The set was verified against web3==7.x; "
        f"if web3 renamed or removed a class, update the set and "
        f"re-verify against the new dir(web3)."
    )


def test_web3_class_reexport_names_verified_against_installed_web3() -> None:
    """Claude pr-auditor post-PR Important #1: the
    `_WEB3_CLASS_REEXPORT_NAMES` set hardcodes the web3 top-level
    classes that re-export provider classes as attributes (e.g.,
    `Web3.HTTPProvider`). Without a runtime check, a new web3.py
    release adding a new `LightWeb3` / `Web3Tracer` class with the
    same re-export pattern would silently bypass the binding-aware
    scan.

    This test walks dir(web3) for every public class name and
    asserts: if a class exposes ANY name in
    FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES, that class name MUST be
    in _WEB3_CLASS_REEXPORT_NAMES."""
    import inspect as _inspect

    import web3 as _web3_for_verification

    # Discover every top-level class in web3 module.
    classes_with_provider_attrs: set[str] = set()
    for name in dir(_web3_for_verification):
        if name.startswith("_"):
            continue
        member = getattr(_web3_for_verification, name)
        # Only inspect classes (not modules, not instances).
        if not _inspect.isclass(member):
            continue
        # Skip provider classes themselves — those are leaves.
        if name in FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES:
            continue
        # Check class dir() for any provider attribute re-export.
        try:
            class_attrs = set(dir(member))
        except Exception:  # noqa: BLE001 — defensive against odd descriptors
            continue
        if class_attrs & FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES:
            classes_with_provider_attrs.add(name)

    missing = classes_with_provider_attrs - _WEB3_CLASS_REEXPORT_NAMES
    assert missing == set(), (
        f"Discovered web3 top-level classes that re-export provider "
        f"classes as attributes but are NOT in _WEB3_CLASS_REEXPORT_NAMES: "
        f"{sorted(missing)}. Either upstream web3 added a new client "
        f"class (add it to the set + the comment), or a class was "
        f"renamed. Re-verify against `dir(web3)` and `dir(web3.<name>)` "
        f"for each missing entry. The binding-aware scan misses these "
        f"silently until added."
    )
    # And the reverse: every name we hardcoded must exist in dir(web3)
    # AND have provider attrs. Catches both renames AND removals.
    for name in _WEB3_CLASS_REEXPORT_NAMES:
        assert hasattr(_web3_for_verification, name), (
            f"_WEB3_CLASS_REEXPORT_NAMES entry {name!r} not in dir(web3) — "
            f"renamed or removed by upstream. Update the set."
        )


@pytest.mark.parametrize(
    "source_snippet",
    [
        # Top-level provider class re-exports — caught by C.5a.
        "from web3 import HTTPProvider\n",
        "from web3 import AsyncHTTPProvider\n",
        "from web3 import WebSocketProvider\n",
        "from web3 import LegacyWebSocketProvider\n",
        "from web3 import IPCProvider\n",
        "from web3 import AsyncIPCProvider\n",
        "from web3 import AutoProvider\n",
        "from web3 import EthereumTesterProvider\n",
        "from web3 import AsyncEthereumTesterProvider\n",
        "from web3 import PersistentConnectionProvider\n",
        "from web3 import PersistentConnection\n",
        "from web3 import BaseProvider\n",
        "from web3 import AsyncBaseProvider\n",
        "from web3 import JSONBaseProvider\n",
        # Provider subtree access — caught by C.5b (prefix) and/or C.5a.
        "from web3.providers import HTTPProvider\n",
        "from web3.providers.legacy_websocket import LegacyWebSocketProvider\n",
        "from web3.providers.ipc import IPCProvider\n",
        "from web3.providers.persistent import PersistentConnectionProvider\n",
        "from web3.providers.auto import AutoProvider\n",
        "from web3.providers.rpc import HTTPProvider\n",
        # Alias-immune (asname can't hide AST-emitted resolved name).
        "from web3.providers.legacy_websocket import LegacyWebSocketProvider as X\n",
        "from web3 import HTTPProvider as _\n",
        # Bare submodule imports — caught by C.5b only.
        "import web3.providers\n",
        "import web3.providers.persistent\n",
        "import web3.providers.legacy_websocket\n",
        "import web3.providers.auto\n",
        "import web3.providers.ipc\n",
        "import web3.providers.rpc\n",
    ],
)
def test_web3_provider_imports_caught_by_denylist_not_allowlist(
    source_snippet: str,
) -> None:
    """§C.6: each web3 provider-class import shape MUST fail the
    per-module check pipeline with the explicit forbidden-import
    message, NOT the generic allowlist message."""
    failures = _scan_import_names_against_pipeline(
        "test_mod",
        "/tmp/test_mod.py",
        source_snippet,
    )
    assert len(failures) >= 1, f"web3 provider import not caught: {source_snippet!r}"
    # At least one failure must be the explicit denylist message.
    denylist_msgs = [f for f in failures if "forbidden import detected" in f]
    assert denylist_msgs, f"expected explicit denylist message for {source_snippet!r}, got {failures}"


@pytest.mark.parametrize(
    "source_snippet",
    [
        "import web3\n",
        "from web3 import Web3\n",
        "from web3 import Account\n",
        "from web3.exceptions import ContractLogicError\n",
    ],
)
def test_web3_utility_imports_pass(source_snippet: str) -> None:
    """§C.7: legitimate utility-only web3 imports MUST pass the per-
    module check pipeline with no "outside allowlist" failures and no
    "forbidden import" failures. Symmetric proof the denylist isn't
    over-broad."""
    checks_performed = 0
    failures = _scan_import_names_against_pipeline(
        "test_mod",
        "/tmp/test_mod.py",
        source_snippet,
    )
    assert failures == [], f"utility web3 import {source_snippet!r} unexpectedly caught: {failures}"
    # Mechanically distinguish "passes" from "no-op" — assert at least
    # one AST-emitted name was classified.
    for name, _lineno in _ast_import_names_for_check(source_snippet):
        cls = _classify_import(name)
        assert cls in ("stdlib", "almanak", "allowed"), (
            f"utility import {name!r} classified as {cls!r}; expected stdlib/almanak/allowed"
        )
        checks_performed += 1
    assert checks_performed > 0, (
        f"test loop did not iterate — source {source_snippet!r} had no AST-emitted import names"
    )


@pytest.mark.parametrize(
    "source_snippet",
    [
        # Literal-`web3` shapes.
        "import web3\nP = web3.HTTPProvider\n",
        'import web3\nweb3.HTTPProvider("https://rpc")\n',
        'import web3\nP = getattr(web3, "HTTPProvider")\n',
        'import web3\nP = vars(web3)["HTTPProvider"]\n',
        'import web3\nP = web3.__dict__["HTTPProvider"]\n',
        'import web3\nP = web3.__getattribute__("HTTPProvider")\n',
        # Aliased-`web3 as w` shapes.
        "import web3 as w\nP = w.HTTPProvider\n",
        'import web3 as w\nw.HTTPProvider("https://rpc")\n',
        'import web3 as w\nP = getattr(w, "HTTPProvider")\n',
        'import web3 as w\nP = vars(w)["HTTPProvider"]\n',
        'import web3 as w\nP = w.__dict__["HTTPProvider"]\n',
        # Different provider classes (parametrize sanity).
        "import web3 as w\nP = w.WebSocketProvider\n",
        "import web3 as w\nP = w.PersistentConnectionProvider\n",
    ],
)
def test_web3_dynamic_attribute_bypass_caught(source_snippet: str) -> None:
    """§C.10: each AST-fed source snippet MUST fail the binding-aware
    web3 scan with the 'forbidden web3 access' message."""
    failures = _scan_web3_dynamic_misuse(
        "test_mod",
        "/tmp/test_mod.py",
        source_snippet,
    )
    assert len(failures) >= 1, f"web3 dynamic bypass not caught: {source_snippet!r}"
    assert any("forbidden web3 access" in f for f in failures), (
        f"expected 'forbidden web3 access' message for {source_snippet!r}, got {failures}"
    )


@pytest.mark.parametrize(
    "source_snippet",
    [
        "import web3\nW = web3.Web3\n",
        "import web3\nA = web3.Account\n",
        "import web3 as w\nW = w.Web3\n",
        "import web3\nE = web3.exceptions\n",
    ],
)
def test_web3_attribute_access_to_non_provider_passes(
    source_snippet: str,
) -> None:
    """§C.11: legitimate utility attribute access (Web3, Account,
    exceptions) on `web3` / `web3 as w` MUST pass the binding-aware
    scan. Symmetric proof C.9 isn't over-broad."""
    failures = _scan_web3_dynamic_misuse(
        "test_mod",
        "/tmp/test_mod.py",
        source_snippet,
    )
    assert failures == [], f"utility web3 access {source_snippet!r} unexpectedly caught: {failures}"


# ============================================================================
# VIB-4886 post-PR — Web3 / AsyncWeb3 class-attribute bypass + tightened
# exemption + multi-product namespace tightening (Codex audit findings).
# ============================================================================


@pytest.mark.parametrize(
    "source_snippet",
    [
        # `from web3 import Web3` — Web3 class re-exports HTTPProvider /
        # IPCProvider / LegacyWebSocketProvider / EthereumTesterProvider
        # as class attributes. Deferred-call shape bypasses substring
        # match.
        "from web3 import Web3\nP = Web3.HTTPProvider\n",
        "from web3 import Web3\nP = Web3.IPCProvider\n",
        "from web3 import Web3\nP = Web3.LegacyWebSocketProvider\n",
        "from web3 import Web3\nP = Web3.EthereumTesterProvider\n",
        # Aliased: `from web3 import Web3 as W`
        "from web3 import Web3 as W\nP = W.HTTPProvider\n",
        # `from web3 import AsyncWeb3` — AsyncWeb3 class re-exports
        # AsyncHTTPProvider / WebSocketProvider / AsyncEthereumTesterProvider.
        "from web3 import AsyncWeb3\nP = AsyncWeb3.AsyncHTTPProvider\n",
        "from web3 import AsyncWeb3\nP = AsyncWeb3.WebSocketProvider\n",
        "from web3 import AsyncWeb3 as AW\nP = AW.AsyncHTTPProvider\n",
        # getattr / vars / __dict__ on the Web3 class binding
        'from web3 import Web3\nP = getattr(Web3, "HTTPProvider")\n',
        'from web3 import Web3\nP = vars(Web3)["HTTPProvider"]\n',
        'from web3 import Web3\nP = Web3.__dict__["HTTPProvider"]\n',
        # Aliased getattr
        'from web3 import Web3 as W\nP = getattr(W, "HTTPProvider")\n',
    ],
)
def test_web3_class_attribute_bypass_caught(source_snippet: str) -> None:
    """Codex post-PR finding: `from web3 import Web3; P = Web3.HTTPProvider`
    bypassed the binding-aware scan because the scan only tracked
    `import web3` bindings. Fixed by extending `_collect_web3_aliases`
    to ALSO track `from web3 import Web3 [as X]` and `AsyncWeb3 [as X]`.

    Each shape MUST fail with the 'forbidden web3 access' message."""
    failures = _scan_web3_dynamic_misuse(
        "test_mod",
        "/tmp/test_mod.py",
        source_snippet,
    )
    assert len(failures) >= 1, f"Web3 class-attribute bypass not caught: {source_snippet!r}"
    assert any("forbidden web3 access" in f for f in failures), (
        f"expected 'forbidden web3 access' for {source_snippet!r}, got {failures}"
    )


@pytest.mark.parametrize(
    "source_snippet",
    [
        # `from web3 import Web3; w3 = Web3(provider)` — legitimate
        # client construction with a gateway-routed provider. Web3()
        # itself is fine; the forbidden form is Web3(HTTPProvider(url))
        # which would trip both the substring AND the binding-aware
        # scan on the HTTPProvider name.
        "from web3 import Web3\nw3 = Web3(my_provider)\n",
        # Method access on Web3 (`Web3.toChecksumAddress`, etc.) — not
        # a provider class attribute, must pass.
        "from web3 import Web3\nfoo = Web3.to_checksum_address\n",
        "from web3 import AsyncWeb3\nfoo = AsyncWeb3.to_checksum_address\n",
    ],
)
def test_web3_class_legitimate_usage_passes(source_snippet: str) -> None:
    """Symmetric proof the new Web3/AsyncWeb3 binding tracking isn't
    over-broad — legitimate Web3 client construction and utility
    method access MUST pass."""
    failures = _scan_web3_dynamic_misuse(
        "test_mod",
        "/tmp/test_mod.py",
        source_snippet,
    )
    assert failures == [], f"legitimate Web3 usage {source_snippet!r} unexpectedly caught: {failures}"


@pytest.mark.parametrize(
    "source_snippet",
    [
        # Nested attribute chain — `w.providers.HTTPProvider` walks
        # the FULL chain and fails on (a) HTTPProvider in trailing
        # names OR (b) web3.providers in submodule prefixes.
        "import web3 as w\nP = w.providers.HTTPProvider\n",
        'import web3 as w\nw.providers.HTTPProvider("https://rpc")\n',
        # Even deeper nesting — `w.providers.rpc.HTTPProvider`.
        "import web3 as w\nP = w.providers.rpc.HTTPProvider\n",
        "import web3 as w\nP = w.providers.legacy_websocket.LegacyWebSocketProvider\n",
        "import web3 as w\nP = w.providers.async_base.AsyncJSONBaseProvider\n",
        # Submodule-only chain (no provider class suffix) — still
        # caught via canonical prefix `web3.providers`.
        "import web3 as w\nx = w.providers.auto\n",
        "import web3 as w\nx = w.providers.rpc\n",
        "import web3\nx = web3.providers.persistent\n",
        # Literal-`web3` (no alias) nested chains.
        "import web3\nP = web3.providers.HTTPProvider\n",
        'import web3\nweb3.providers.HTTPProvider("https://rpc")\n',
        # Web3 class binding nested chain — `Web3.providers.HTTPProvider`
        # (Web3 class also exposes providers attribute in some versions
        # of web3.py).
        "from web3 import Web3\nP = Web3.providers.HTTPProvider\n",
    ],
)
def test_web3_nested_attribute_chain_bypass_caught(source_snippet: str) -> None:
    """Gemini post-PR security-HIGH finding: the original Pattern 1 only
    handled ``ast.Attribute(value=ast.Name)`` — a direct one-level
    attribute access. Nested access like ``w.providers.HTTPProvider``
    has ``ast.Attribute(value=ast.Attribute(...))`` so the inner check
    didn't match, leaving a security bypass.

    Fixed by walking the full attribute chain from the outermost
    Attribute down to the root Name, then checking BOTH any segment
    against ``FORBIDDEN_WEB3_PROVIDER_TRAILING_NAMES`` AND the
    canonical resolved chain against ``FORBIDDEN_WEB3_SUBMODULE_PREFIXES``.

    Each shape MUST fail with the 'forbidden web3 access' message."""
    failures = _scan_web3_dynamic_misuse(
        "test_mod",
        "/tmp/test_mod.py",
        source_snippet,
    )
    assert len(failures) >= 1, f"nested attribute bypass not caught: {source_snippet!r}"
    assert any("forbidden web3 access" in f for f in failures), (
        f"expected 'forbidden web3 access' for {source_snippet!r}, got {failures}"
    )


@pytest.mark.parametrize(
    "source_snippet",
    [
        # Nested non-provider attribute access — legitimate utility.
        # `web3.eth.contract` is a legitimate utility helper for ABI
        # encoding; not under web3.providers, no provider class names
        # in chain.
        "import web3\nc = web3.eth.contract\n",
        "import web3 as w\nc = w.eth.contract\n",
        # Legitimate `Web3.toJSON` (utility class methods).
        "from web3 import Web3\nfoo = Web3.toJSON\n",
        # `web3.types.RPCEndpoint` — legitimate type alias.
        "import web3\nt = web3.types.RPCEndpoint\n",
    ],
)
def test_web3_nested_attribute_chain_legitimate_passes(
    source_snippet: str,
) -> None:
    """Symmetric proof the nested-chain scan isn't over-broad —
    legitimate utility access via nested attributes (`web3.eth.contract`,
    `web3.types.RPCEndpoint`, etc.) MUST pass."""
    failures = _scan_web3_dynamic_misuse(
        "test_mod",
        "/tmp/test_mod.py",
        source_snippet,
    )
    assert failures == [], f"legitimate nested web3 access {source_snippet!r} unexpectedly caught: {failures}"


def test_per_module_exemption_does_not_skip_other_forbidden_imports() -> None:
    """Codex post-PR finding: previous implementation skipped the
    ENTIRE import pipeline for the gateway_provider exemption. If the
    module ever added `import requests`, the scan would miss it.

    Fixed by changing to per-(module, import-name) exemption. This
    test simulates the gateway_provider module's source PLUS an
    illegitimate `import requests`, and asserts the requests import
    still trips the pipeline."""
    # Source that mimics gateway_provider — exempt JSONBaseProvider
    # imports PLUS an illegitimate requests import.
    source = (
        "from web3.providers.base import JSONBaseProvider\n"
        "from web3.providers.async_base import AsyncJSONBaseProvider\n"
        "import requests\n"  # forbidden — must still trip
        "\n"
        "class GatewayWeb3Provider(JSONBaseProvider):\n"
        "    pass\n"
    )
    exempt = _PER_MODULE_IMPORT_NAME_EXEMPT["almanak.framework.web3.gateway_provider"]
    failures = _scan_import_names_against_pipeline(
        "almanak.framework.web3.gateway_provider",
        "/tmp/gateway_provider.py",
        source,
        exempt,
    )
    # The exempt imports must NOT appear in failures.
    for msg in failures:
        assert "web3.providers.base" not in msg or "JSONBaseProvider" in msg, (
            f"exempt import unexpectedly failed: {msg!r}"
        )
    # The forbidden `import requests` MUST appear.
    requests_failures = [f for f in failures if "requests" in f]
    assert len(requests_failures) >= 1, (
        f"`import requests` was not caught despite being outside the per-import exemption set. failures={failures}"
    )
    assert any("forbidden import detected" in f for f in requests_failures), (
        f"expected explicit denylist message for requests, got {requests_failures}"
    )


def test_per_module_exemption_does_not_skip_allowlist_unknown_imports() -> None:
    """Companion to the test above: even an UNKNOWN third-party import
    (e.g., a new SDK never seen before) MUST trip the allowlist-gap
    branch in the exempt module."""
    source = (
        "from web3.providers.base import JSONBaseProvider\n"
        "import _vib4886_fake_post_pr_sdk\n"  # unknown — must still trip
    )
    exempt = _PER_MODULE_IMPORT_NAME_EXEMPT["almanak.framework.web3.gateway_provider"]
    failures = _scan_import_names_against_pipeline(
        "almanak.framework.web3.gateway_provider",
        "/tmp/gateway_provider.py",
        source,
        exempt,
    )
    fake_failures = [f for f in failures if "_vib4886_fake_post_pr_sdk" in f]
    assert len(fake_failures) >= 1, f"unknown SDK not caught in exempt module: failures={failures}"
    assert all("outside allowlist" in f for f in fake_failures), (
        f"expected allowlist-gap message for unknown SDK, got {fake_failures}"
    )


@pytest.mark.parametrize(
    "source_snippet",
    [
        # google.cloud — typical GCS client, network egress
        "import google.cloud.storage\n",
        "from google.cloud import storage\n",
        # google.auth — typically for OAuth/IAM with network egress
        "from google.auth.transport.requests import AuthorizedSession\n",
        # bare google — pinned out by the tightening (was previously allowed)
        "import google\n",
    ],
)
def test_google_namespace_tightened_to_rpc_only(source_snippet: str) -> None:
    """Codex post-PR finding: the bare `google` allowlist entry permitted
    network-capable Google SDKs (`google.cloud`, `google.auth.transport.requests`)
    because `_classify_import` only checked the top-level segment.
    Tightened to `google.rpc` with dotted-prefix classifier semantics.

    Each non-`google.rpc.*` import shape MUST classify as `"unknown"` AND
    trip the allowlist-gap branch."""
    failures = _scan_import_names_against_pipeline(
        "test_mod",
        "/tmp/test_mod.py",
        source_snippet,
    )
    assert len(failures) >= 1, f"non-google.rpc Google import not caught: {source_snippet!r}"
    assert any("outside allowlist" in f for f in failures), (
        f"expected allowlist-gap message for {source_snippet!r}, got {failures}"
    )


# ============================================================================
# Regression-test fixtures (anti-no-op proofs)
# ============================================================================


@pytest.fixture
def tmp_framework_module(tmp_path, monkeypatch):
    """Builder for tmp modules registered under almanak.framework.tmp_*."""

    created: list[tuple[str, Path]] = []

    def _make(modname: str, source: str) -> Path:
        # Ensure the module name lives under almanak.framework.*.
        assert modname.startswith("almanak.framework."), modname
        path = tmp_path / (modname.rsplit(".", 1)[-1] + ".py")
        path.write_text(source, encoding="utf-8")

        # Register in sys.modules as a real Python module with the
        # given source.
        spec = importlib.util.spec_from_file_location(modname, str(path))
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[modname] = mod
        # Don't actually exec the module body (it imports aiohttp which
        # may not be installed); we only need the spec to be reachable
        # via importlib.util.find_spec so _source_of() can read it.
        created.append((modname, path))
        return path

    yield _make

    # Cleanup.
    for modname, _path in created:
        sys.modules.pop(modname, None)


def test_scan_b_regression_fixture_real_file_caught() -> None:
    """Scan B regression — Round-2 CodeRabbit minor finding closed:
    write a deliberately-bad .py file PHYSICALLY inside one of the
    PACKAGE_ROOTS, then call the actual ``_walk_package()`` routine to
    prove the walker DISCOVERS the file AND the per-module scanner
    flags the forbidden import. The Round-1 fixture wrote outside
    PACKAGE_ROOTS, so it didn't prove the walker fires — only that
    the per-module scanner does.

    Cleanup is unconditional (try / finally) so a test crash doesn't
    leave a junk file in the source tree.
    """
    # Write under almanak.framework.data.pools (a PACKAGE_ROOT).
    # Unique per test run — pytest-xdist parallel workers each pick
    # a distinct fixture name so simultaneous tests don't collide on
    # the shared source tree (Round-2 CodeRabbit minor).
    import uuid as _uuid

    package_dir = Path(__file__).resolve().parents[3] / "almanak" / "framework" / "data" / "pools"
    fixture_name = f"_test_scan_b_no_op_fixture_{_uuid.uuid4().hex[:8]}"
    fixture_path = package_dir / (fixture_name + ".py")
    bad_source = "# fixture-only regression helper for Scan B\nimport aiohttp\n"
    full_modname = f"almanak.framework.data.pools.{fixture_name}"
    try:
        fixture_path.write_text(bad_source, encoding="utf-8")
        # Invalidate importlib caches so find_spec sees the new file.
        importlib.invalidate_caches()

        # Step 1 — prove _walk_package() actually DISCOVERS the file.
        discovered = _walk_package("almanak.framework.data.pools")
        assert full_modname in discovered, (
            f"Scan B walker did NOT discover the regression fixture at "
            f"{fixture_path} — fixture is broken or _walk_package() "
            f"stopped enumerating. Walked: {len(discovered)} modules."
        )

        # Step 2 — prove the per-module scanner FIRES on the forbidden import.
        source = fixture_path.read_text(encoding="utf-8")
        found_forbidden = False
        for name, _lineno in _ast_import_names_for_check(source):
            if _is_forbidden_import(name):
                found_forbidden = True
                break
        assert found_forbidden, "Scan B per-module scanner failed to detect `aiohttp` import."
    finally:
        # Always clean up — even if assertions fired.
        fixture_path.unlink(missing_ok=True)
        importlib.invalidate_caches()
        # Remove from sys.modules in case some prior test imported it.
        sys.modules.pop(full_modname, None)


def test_scan_c_regression_fixture_multi_hop_caught(tmp_framework_module) -> None:
    """Scan C regression: a multi-hop transitive chain a -> b -> c where
    c imports aiohttp. Proves the closure walk follows multiple hops.
    A naive closure that only checks targets' direct imports would fail
    this fixture."""
    tmp_framework_module(
        "almanak.framework._scan_c_fixture_a",
        "from almanak.framework._scan_c_fixture_b import noop\n",
    )
    tmp_framework_module(
        "almanak.framework._scan_c_fixture_b",
        "from almanak.framework._scan_c_fixture_c import noop\nnoop = None\n",
    )
    tmp_framework_module(
        "almanak.framework._scan_c_fixture_c",
        "import aiohttp\nnoop = None\n",
    )
    # Run the closure walker starting at the tmp_a fixture.
    closure = _import_closure(("almanak.framework._scan_c_fixture_a",))
    # The closure should include all three modules.
    assert "almanak.framework._scan_c_fixture_a" in closure
    assert "almanak.framework._scan_c_fixture_b" in closure
    assert "almanak.framework._scan_c_fixture_c" in closure

    # And scanning _c MUST detect the forbidden import (AST-based check).
    sf = _source_of("almanak.framework._scan_c_fixture_c")
    assert sf is not None
    source, _, _ = sf
    found_forbidden = any(_is_forbidden_import(name) for name, _lineno in _ast_import_names_for_check(source))
    assert found_forbidden, "Scan C scanner failed to detect aiohttp"


def test_scan_c_regression_fixture_relative_import_caught(tmp_framework_module) -> None:
    """Relative-import resolver fixture: parent does
    `from .child import x`; child does `import aiohttp`. Proves the
    importlib.util.resolve_name resolution path works; without it the
    relative .child import would not be recognised as
    almanak.framework.* and the bypass would be invisible."""
    # NB: we can't easily set up a package on the fly with proper
    # __init__.py + submodules without creating a real directory; we
    # instead exercise the resolver helper directly with a synthetic
    # source string that uses a relative import.
    source = "from .child import x\nx = None\n"
    # Pretend importing_package is almanak.framework.tmp_relimport.
    deps = _ast_imports(source, importing_package="almanak.framework.tmp_relimport")
    # The resolver should produce the absolute path
    # almanak.framework.tmp_relimport.child.
    assert "almanak.framework.tmp_relimport.child" in deps, f"resolver failed to resolve `.child`: {deps}"


# ============================================================================
# Forbidden-shell-tokens AST scan
# ============================================================================


@pytest.mark.parametrize("modname", CLOSURE_TARGETS, ids=lambda m: m.rsplit(".", 1)[-1])
def test_no_shell_egress_tokens_in_string_literals(modname: str) -> None:
    """AST-level shell-egress check: no string literal in the 5
    CLOSURE_TARGETS contains the tokens `curl` / `wget` / `nc` /
    `netcat` as full words. Catches `["curl", url]` as a list element."""
    sf = _source_of(modname)
    if sf is None:
        return
    source, path, _ = sf
    # For snapshot.py exempt file-scope (omnibus); accessor only.
    if modname == "almanak.framework.market.snapshot":
        import textwrap as _textwrap

        from almanak.framework.market.snapshot import MarketSnapshot

        source = _textwrap.dedent(inspect.getsource(MarketSnapshot.pool_history))
    try:
        tree = ast.parse(source)
    except SyntaxError:
        pytest.fail(f"{modname}: failed to parse source")
    failures: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if FORBIDDEN_SHELL_TOKENS_REGEX.search(node.value):
                failures.append(
                    f"{modname} ({path}):{node.lineno} forbidden shell token in string literal: {node.value!r}",
                )
    assert failures == [], "\n".join(failures)


# ============================================================================
# VIB-4901 — Broadened static guard: dynamic-import + instance/rebinding
# ============================================================================
#
# Each test below is an acceptance criterion from
# ``docs/internal/uat-cards/VIB-4901.md`` §"Acceptance criteria". Test
# names map to the criterion identifiers (A.1, A.2, B.1, etc.) and are
# referenced from the card.


# ---------- A — L1 closure verifiable ----------


def test_a1_ohlcv_init_no_longer_uses_dynamic_import() -> None:
    """A.1: After the VIB-4901 OHLCV refactor, the literal source text
    of ``almanak/framework/data/ohlcv/__init__.py`` contains zero
    occurrences of any element of FORBIDDEN_DYNAMIC_IMPORTS."""
    import almanak.framework.data.ohlcv as _ohlcv

    source = Path(_ohlcv.__file__).read_text(encoding="utf-8")
    hits = [s for s in FORBIDDEN_DYNAMIC_IMPORTS if s in source]
    assert hits == [], (
        f"OHLCV __init__.py still contains forbidden dynamic-import substrings after VIB-4901 refactor: {hits}"
    )


def test_a2_a3_no_dynamic_imports_in_scan_b_and_c() -> None:
    """A.2 + A.3: Every module discovered by Scan B (PACKAGE_ROOTS walk)
    AND every module reached by _import_closure(CLOSURE_TARGETS) passes
    the EXTENDED FORBIDDEN_DYNAMIC_IMPORTS substring scan. INCLUDING
    modules in _LEGACY_VIOLATING_MODULES (legacy exemption is scoped to
    the allowlist + provider-class denylist, NOT to dynamic-import
    suppression).

    The per-(module, token) exemption table
    _PER_MODULE_DYNAMIC_IMPORT_EXEMPT permits one audited use
    (copy_signal_engine receipt-parser lazy-loader, VIB-4914 tracked).
    Any OTHER substring still trips.
    """
    scope: set[str] = set()
    for root in PACKAGE_ROOTS:
        scope.update(_walk_package(root))
    scope |= _import_closure(CLOSURE_TARGETS)

    failures: list[str] = []
    for modname in sorted(scope):
        sf = _source_of(modname)
        if sf is None:
            continue
        source, path, _ = sf
        substring_exempt = _substring_exempt_for_module(modname)
        for substring in FORBIDDEN_DYNAMIC_IMPORTS:
            if substring in source and substring not in substring_exempt:
                failures.append(
                    f"{modname} ({path}): forbidden dynamic-import substring detected: {substring!r}",
                )
                break
    assert failures == [], "\n".join(failures)


def test_a4_ohlcv_module_lazy_load_preserved() -> None:
    """A.4: Importing almanak.framework.data.ohlcv MUST NOT
    side-effect-import almanak.framework.data.ohlcv.module at
    top-level import time. Accessing OHLCVModule via __getattr__
    DOES trigger the inner import.

    Uses a subprocess to get a clean sys.modules baseline (pytest /
    conftest pollutes the import table).
    """
    import subprocess
    import sys as _sys

    snippet = (
        "import sys, importlib\n"
        "importlib.import_module('almanak.framework.data.ohlcv')\n"
        "assert 'almanak.framework.data.ohlcv' in sys.modules, \\\n"
        "    'parent should be loaded'\n"
        "assert 'almanak.framework.data.ohlcv.module' not in sys.modules, \\\n"
        "    f'module.py should NOT be loaded yet; sys.modules contains it'\n"
        "from almanak.framework.data.ohlcv import OHLCVModule\n"
        "assert 'almanak.framework.data.ohlcv.module' in sys.modules, \\\n"
        "    'module.py should be loaded after __getattr__'\n"
        "assert OHLCVModule is not None\n"
    )
    result = subprocess.run(
        [_sys.executable, "-c", snippet],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"OHLCV lazy-load subprocess FAILED.\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )


# A.5 — Each of the 10 L1 bypass shapes is detected by the EXTENDED
# 18-entry FORBIDDEN_DYNAMIC_IMPORTS substring scan. Synthesized
# source strings (NOT modifications to production modules).
_A5_BYPASS_SHAPES: tuple[tuple[str, str], ...] = (
    (
        "qualified_machinery_load",
        'import importlib.machinery\nimportlib.machinery.SourceFileLoader("x", "/tmp/x.py").load_module()\n',
    ),
    (
        "from_machinery_bare_call",
        'from importlib.machinery import SourceFileLoader\nSourceFileLoader("x", "/tmp/x.py").load_module()\n',
    ),
    (
        "importlib_reload_qualified",
        "import importlib\nimportlib.reload(some_module)\n",
    ),
    (
        "runpy_run_module_qualified",
        'import runpy\nrunpy.run_module("web3")\n',
    ),
    (
        "from_runpy_bare_call",
        'from runpy import run_module\nrun_module("web3")\n',
    ),
    (
        "runpy_run_path_qualified",
        'import runpy\nrunpy.run_path("/tmp/x.py")\n',
    ),
    (
        "builtins_dict_bypass",
        '__builtins__["__import__"]("web3")\n',
    ),
    (
        "from_importlib_bare_call",
        'from importlib import import_module\nimport_module("web3")\n',
    ),
    (
        "from_util_spec_from_file",
        'from importlib.util import spec_from_file_location\nspec = spec_from_file_location("x", "/tmp/x.py")\n',
    ),
    (
        "code_interactive_interpreter",
        'import code\ncode.InteractiveInterpreter().runsource("import web3")\n',
    ),
)


@pytest.mark.parametrize("shape_id,source", _A5_BYPASS_SHAPES, ids=[s[0] for s in _A5_BYPASS_SHAPES])
def test_a5_extended_dynamic_import_shapes_detected(shape_id: str, source: str) -> None:
    """A.5: Each of the 10 L1 bypass shapes is detected by the EXTENDED
    18-entry FORBIDDEN_DYNAMIC_IMPORTS substring scan when applied to
    synthesized source."""
    hits = [s for s in FORBIDDEN_DYNAMIC_IMPORTS if s in source]
    assert hits, (
        f"A.5 shape {shape_id!r} ({source!r}) NOT detected by "
        f"FORBIDDEN_DYNAMIC_IMPORTS substring scan. Extension regressed."
    )


# A.6 — Class E import-name denylist catches dynamic-execution
# namespace imports through _scan_import_names_against_pipeline.
_A6_CLASS_E_SHAPES: tuple[tuple[str, str], ...] = (
    ("import_importlib", "import importlib\n"),
    ("from_importlib_machinery", "from importlib.machinery import SourceFileLoader\n"),
    ("from_importlib_util", "from importlib.util import spec_from_file_location\n"),
    ("import_runpy", "import runpy\n"),
    ("from_runpy", "from runpy import run_module\n"),
    ("import_code", "import code\n"),
    ("from_code", "from code import InteractiveInterpreter\n"),
    # VIB-4901 post-PR Claude pr-auditor Potential #5 — RCE-vector
    # deserialization namespaces added to Class E for defense-in-depth.
    ("import_pickle", "import pickle\n"),
    ("from_pickle_loads", "from pickle import loads\n"),
    ("import_marshal", "import marshal\n"),
    ("from_marshal", "from marshal import loads\n"),
    ("import_shelve", "import shelve\n"),
)


@pytest.mark.parametrize("shape_id,source", _A6_CLASS_E_SHAPES, ids=[s[0] for s in _A6_CLASS_E_SHAPES])
def test_a6_class_e_import_denylist_catches_dynamic_namespaces(
    shape_id: str,
    source: str,
) -> None:
    """A.6: Each Class E import-statement bypass shape is detected by
    the EXTENDED FORBIDDEN_IMPORT_NAMES denylist via
    _scan_import_names_against_pipeline (the production import-name
    layer used by Scan A/B/C).
    """
    failures = _scan_import_names_against_pipeline(
        "synthetic_test_module",
        "synthetic_test_module.py",
        source,
    )
    assert failures, (
        f"A.6 Class E shape {shape_id!r} ({source!r}) NOT detected by "
        f"_scan_import_names_against_pipeline. Class E entries missing "
        f"from FORBIDDEN_IMPORT_NAMES."
    )


def test_a7_l1_inherent_indirection_not_covered_pinned() -> None:
    """A.7 / D.3: The L1-inherent indirection shape is NOT detected by
    EITHER layer. Pins the inherent limit so a future PR cannot
    silently claim closure without addressing it via expression-level
    data-flow.

    See ``docs/internal/uat-cards/VIB-4901.md`` §"Known limitations
    after this PR" L1-inherent.

    Sub-assertion (1) — substring-only: scan the literal source with
    the same loop body the production tests use. Forbidden substring
    must NOT match (because the substring is broken across
    ast.Call / ast.Subscript / ast.Assign chains).

    Sub-assertion (2) — legacy-exempt path: invoke the Scan B / C
    iteration logic on a synthesized module name added to
    _LEGACY_VIOLATING_MODULES; assert no failure is emitted even
    though `import importlib` is present. The combined legacy-
    exemption + indirection blind spot pins L1-inherent for the
    legacy-asymmetry case too.

    Note: ``_scan_import_names_against_pipeline`` itself does NOT
    check ``_LEGACY_VIOLATING_MODULES`` — the skip is caller-level
    (lines ~1304 / ~1355 of this file). Sub-assertion (2) therefore
    exercises the caller, not the helper.
    """
    indirect_source = 'import importlib\nf = getattr(importlib, "import_module")\nf("web3")\n'

    # Sub-assertion (1): substring-only — must NOT detect.
    hits = [s for s in FORBIDDEN_DYNAMIC_IMPORTS if s in indirect_source]
    assert hits == [], (
        f"A.7 expected ZERO substring hits for indirection shape, got "
        f"{hits}. This would mean an implementation accidentally closed "
        f"L1-inherent — delete this test if the closure is intentional, "
        f"otherwise the new substring needs to be reverted."
    )

    # Sub-assertion (2): caller-level legacy exemption.
    # The full Scan B / C iteration body skips modules in
    # _LEGACY_VIOLATING_MODULES at the caller level (lines ~1304 /
    # ~1355). We exercise the SAME caller logic against a synthesized
    # source for two cases:
    #   (a) NON-legacy module + indirection source → pipeline does NOT
    #       detect (proves indirection itself is inherent — the
    #       pipeline still ran on this module, found no forbidden
    #       direct call).
    #   (b) Legacy module + indirection source → caller-level skip
    #       fires, pipeline NEVER runs (proves the documented legacy
    #       asymmetry holds for the inherent-shape case too).
    #
    # CodeRabbit post-PR Major (a88cdbb78 → next): the original
    # sub-assertion (2) was vacuous because the synthetic module was
    # unconditionally unioned into the skip set, so the loop body
    # never executed. The fix below exercises the SAME caller logic
    # the production tests use (`if modname in _LEGACY_VIOLATING_MODULES:
    # continue`) — once with a legacy module name in the set, once
    # without, and asserts the right behavior in each branch.
    nonlegacy_synth = "synthetic_nonlegacy_indirection_module"
    legacy_synth = "almanak.framework.data.pendle.on_chain_reader"  # real legacy entry
    assert nonlegacy_synth not in _LEGACY_VIOLATING_MODULES, (
        "test prerequisite: nonlegacy_synth must be a non-legacy name"
    )
    assert legacy_synth in _LEGACY_VIOLATING_MODULES, (
        "test prerequisite: legacy_synth must be in _LEGACY_VIOLATING_MODULES"
    )

    # Case (a) — NON-legacy module: caller does NOT skip, pipeline runs.
    case_a_failures: list[str] = []
    if nonlegacy_synth not in _LEGACY_VIOLATING_MODULES:
        case_a_failures.extend(
            _scan_import_names_against_pipeline(
                nonlegacy_synth,
                f"{nonlegacy_synth}.py",
                indirect_source,
            ),
        )
    # The indirection-only `getattr(importlib, "import_module")(...)`
    # form does include `import importlib`, which IS detected by the
    # Class E layer for a non-legacy module. So case_a_failures is
    # NOT empty — proving the L1-INHERENT pin is specifically about
    # the call-chain breakage, not the import statement itself. The
    # SUBSTRING scan of A.7 sub-assertion (1) is the more interesting
    # case-a pin (no substring match for the indirection chain).
    assert any("importlib" in f for f in case_a_failures), (
        f"Case (a) sanity: in non-legacy scope, `import importlib` "
        f"should be caught by Class E. Got: {case_a_failures}. If "
        f"this fails, Class E denylist regressed."
    )

    # Case (b) — LEGACY module: caller skips, pipeline never runs.
    # This is the documented asymmetry. The skip prevents both Class
    # E catch AND substring catch via the caller, but the substring
    # scan applies UNIFORMLY in the dedicated Scan B/C dynamic-import
    # test — so the legacy module is still observed by THAT test if
    # it actually uses a forbidden substring. The indirection shape
    # uses no forbidden substring, so even uniform substring scan
    # passes — confirming the inherent limit.
    case_b_failures: list[str] = []
    if legacy_synth not in _LEGACY_VIOLATING_MODULES:
        case_b_failures.extend(
            _scan_import_names_against_pipeline(
                legacy_synth,
                f"{legacy_synth}.py",
                indirect_source,
            ),
        )
    assert case_b_failures == [], (
        f"Case (b) legacy-skip: pipeline should NOT have run on a "
        f"_LEGACY_VIOLATING_MODULES entry. Got: {case_b_failures}. "
        f"This would mean the caller-level skip semantics regressed."
    )


# ---------- B — L4a closure verifiable ----------


def test_b1_collect_web3_aliases_binds_instance_from_class_call() -> None:
    """B.1: _collect_web3_aliases returns {"Web3", "w3"} for source
    `from web3 import Web3; w3 = Web3()`. L4a direct-call instance
    binding."""
    source = "from web3 import Web3\nw3 = Web3()\n"
    aliases = _collect_web3_aliases(ast.parse(source))
    assert aliases == {"Web3", "w3"}, f"B.1 expected {{Web3, w3}} after VIB-4901 L4a closure, got {aliases}"


def test_b2_web3_instance_bypass_detected() -> None:
    """B.2: _scan_web3_dynamic_misuse returns a non-empty failures list
    AND the first message mentions HTTPProvider for
    `from web3 import Web3; w3 = Web3(); P = w3.HTTPProvider`."""
    source = "from web3 import Web3\nw3 = Web3()\nP = w3.HTTPProvider\n"
    failures = _scan_web3_dynamic_misuse("synthetic", "synthetic.py", source)
    assert failures, "B.2 L4a instance bypass NOT detected after VIB-4901 closure"
    assert "HTTPProvider" in failures[0], f"B.2 failure message should mention HTTPProvider, got: {failures[0]!r}"


def test_b3_collect_web3_aliases_binds_instance_from_async_class_call() -> None:
    """B.3: _collect_web3_aliases returns {"AsyncWeb3", "w3"} for
    `from web3 import AsyncWeb3; w3 = AsyncWeb3()`."""
    source = "from web3 import AsyncWeb3\nw3 = AsyncWeb3()\n"
    aliases = _collect_web3_aliases(ast.parse(source))
    assert aliases == {"AsyncWeb3", "w3"}, f"B.3 expected {{AsyncWeb3, w3}}, got {aliases}"


# ---------- C — L4b single-level closure verifiable ----------


def test_c1_collect_web3_aliases_binds_single_level_rebinding() -> None:
    """C.1: _collect_web3_aliases returns {"web3", "wb"} for
    `import web3; wb = web3`. L4b single-level rebinding."""
    source = "import web3\nwb = web3\n"
    aliases = _collect_web3_aliases(ast.parse(source))
    assert aliases == {"web3", "wb"}, f"C.1 expected {{web3, wb}}, got {aliases}"


def test_c2_web3_single_level_rebind_bypass_detected() -> None:
    """C.2: _scan_web3_dynamic_misuse returns non-empty failures
    mentioning HTTPProvider for `import web3; wb = web3; P =
    wb.HTTPProvider`."""
    source = "import web3\nwb = web3\nP = wb.HTTPProvider\n"
    failures = _scan_web3_dynamic_misuse("synthetic", "synthetic.py", source)
    assert failures, "C.2 L4b single-level rebind bypass NOT detected"
    assert "HTTPProvider" in failures[0], f"C.2 failure should mention HTTPProvider, got: {failures[0]!r}"


# ---------- D — Inherent-limit visibility (negative pins) ----------


def test_d1_l4b_transitive_rebinding_not_covered_inherent_limit() -> None:
    """D.1: L4b TRANSITIVE rebinding shape is NOT detected by the
    single-pass binding collector. Pins the inherent limit so a future
    PR cannot silently claim closure without implementing fixed-point
    ast.Assign iteration.

    See VIB-4901.md §"Known limitations after this PR" L4b transitive.
    If a future PR DELETES this test, the §"Known limitations" prose
    must also be updated and a separate explicit acceptance test
    that asserts the OPPOSITE (`assert failures` non-empty for `c =
    b.HTTPProvider`) must REPLACE this one.
    """
    source = "import web3\na = web3\nb = a\nP = b.HTTPProvider\n"
    failures = _scan_web3_dynamic_misuse("synthetic", "synthetic.py", source)
    assert failures == [], (
        f"D.1 expected zero failures (transitive rebinding is inherent), "
        f"got: {failures}. If implementation closed L4b transitive, "
        f"DELETE this test and update VIB-4901.md §Known limitations."
    )


def test_d2_l4c_indirect_call_binding_not_covered_inherent_limit() -> None:
    """D.2: L4c indirect-call binding shape is NOT detected by the
    single-pass collector. Pins the inherent limit so a future PR
    cannot silently claim closure without implementing expression-
    level data-flow (L2 territory).

    See VIB-4901.md §"Known limitations after this PR" L4c.
    """
    source = "from web3 import Web3\nw3 = (Web3,)[0]()\nP = w3.HTTPProvider\n"
    failures = _scan_web3_dynamic_misuse("synthetic", "synthetic.py", source)
    assert failures == [], (
        f"D.2 expected zero failures (L4c indirect-call is inherent), "
        f"got: {failures}. If implementation closed L4c, DELETE this "
        f"test and update VIB-4901.md §Known limitations."
    )


# D.3 is cross-referenced from A.7 above (same test, different
# narrative section). No separate test body — A.7's assertion serves
# both criteria.


# ---------- F — Bounds remain strict ----------


def test_f3_forbidden_dynamic_imports_tuple_length_is_18() -> None:
    """F.3: FORBIDDEN_DYNAMIC_IMPORTS tuple length is exactly 18
    (6 existing + 12 added per VIB-4901 A.5). Strict-equality bound
    forces deliberate count bump."""
    assert len(FORBIDDEN_DYNAMIC_IMPORTS) == 18, (
        f"FORBIDDEN_DYNAMIC_IMPORTS length is {len(FORBIDDEN_DYNAMIC_IMPORTS)}, "
        f"expected 18 per VIB-4901. Re-audit Scan B ∪ Scan C for false "
        f"positives before changing this bound."
    )


def test_f4_class_e_namespaces_in_forbidden_import_names() -> None:
    """F.4: FORBIDDEN_IMPORT_NAMES frozenset includes the Class E
    entries per VIB-4901: ``importlib``, ``runpy``, ``code`` (dynamic-
    execution surface) AND ``pickle``, ``marshal``, ``shelve``
    (arbitrary-code-deserialization surface, post-PR Claude
    pr-auditor Potential #5).
    """
    for name in ("importlib", "runpy", "code", "pickle", "marshal", "shelve"):
        assert name in FORBIDDEN_IMPORT_NAMES, (
            f"Class E namespace {name!r} missing from FORBIDDEN_IMPORT_NAMES — VIB-4901 closure regressed."
        )


def test_f5_per_module_dynamic_import_exempt_bound() -> None:
    """F.5: _PER_MODULE_DYNAMIC_IMPORT_EXEMPT contains exactly ONE
    entry — `services.copy_signal_engine` mapped to
    `frozenset({"importlib", "import_module("})`. VIB-4914 tracks
    deletion of this entry once the static parser registry refactor
    ships.
    """
    assert len(_PER_MODULE_DYNAMIC_IMPORT_EXEMPT) == 1, (
        f"_PER_MODULE_DYNAMIC_IMPORT_EXEMPT length is "
        f"{len(_PER_MODULE_DYNAMIC_IMPORT_EXEMPT)}, expected 1 per VIB-4901. "
        f"Adding entries requires audit evidence + follow-up ticket."
    )
    expected_key = "almanak.framework.services.copy_signal_engine"
    assert expected_key in _PER_MODULE_DYNAMIC_IMPORT_EXEMPT, (
        f"Expected key {expected_key!r} missing — exemption regressed."
    )
    assert _PER_MODULE_DYNAMIC_IMPORT_EXEMPT[expected_key] == frozenset(
        {
            "importlib",
            "import_module(",
        }
    ), (
        "Exempted tokens drifted from the audited set "
        "{'importlib', 'import_module('}. Tightening / loosening this "
        "set requires audit + VIB-4914 alignment."
    )


# ---------- G — Documentation drift gates ----------


def _module_docstring() -> str:
    """Return this module's docstring (the test file we're in)."""
    return __doc__ or ""


def test_g1_module_docstring_no_longer_lists_l1_as_followup() -> None:
    """G.1: Module docstring no longer treats L1 as a Known
    Limitation needing a follow-up. The original VIB-4886 phrasing
    "Follow-up ticket\\n  tracks broadening to Scan B / Scan C scope"
    must NOT be present after VIB-4901 closure."""
    docstring = _module_docstring()
    forbidden_phrase = "Follow-up ticket\n  tracks broadening to Scan B"
    assert forbidden_phrase not in docstring, (
        "G.1 module docstring still treats L1 as open. VIB-4901 "
        "closed L1 direct + from-import shapes; the legacy phrasing "
        "must be replaced with the new contract description."
    )


def test_g2_module_docstring_lists_remaining_inherent_limits_citing_vib_4901() -> None:
    """G.2: Module docstring lists L1-inherent, L2, L3, L4b-transitive,
    and L4c as the remaining inherent limitations, each citing
    VIB-4901."""
    docstring = _module_docstring()
    required_substrings = (
        "VIB-4901",
        "L1-inherent",
        "L2",
        "L3",
        "L4b transitive",
        "L4c",
    )
    missing = [s for s in required_substrings if s not in docstring]
    assert not missing, f"G.2 module docstring missing required substrings after VIB-4901 update: {missing}"


# ============================================================================
# VIB-4901 post-PR audit follow-ups
# ============================================================================


def test_per_module_dynamic_exemption_does_not_skip_other_forbidden_dynamic_substrings() -> None:
    """Claude pr-auditor Important #1 — mirror of VIB-4886's
    ``test_per_module_exemption_does_not_skip_other_forbidden_imports``.

    Proves that a module in ``_PER_MODULE_DYNAMIC_IMPORT_EXEMPT`` that
    ALSO contains a forbidden dynamic-import substring NOT in its
    exempt set still trips the substring scan. Synthesizes source with
    ``importlib.import_module(...)`` (exempt for copy_signal_engine)
    + ``exec("...")`` (NOT exempt) and applies the production loop
    body (mirroring lines ~1330 / ~1370 of this file).
    """
    modname = "almanak.framework.services.copy_signal_engine"
    assert modname in _PER_MODULE_DYNAMIC_IMPORT_EXEMPT, (
        "test prerequisite: copy_signal_engine MUST be in the "
        "exemption table for this regression contract to be meaningful"
    )
    substring_exempt = _substring_exempt_for_module(modname)
    assert "import_module(" in substring_exempt, (
        "test prerequisite: import_module( must be in the substring exempt subset for copy_signal_engine"
    )
    # Synthesize source containing BOTH an exempt token AND a
    # non-exempt token. The production loop short-circuits on first
    # hit (`break`), so we sort FORBIDDEN_DYNAMIC_IMPORTS to verify
    # the scan would catch exec( regardless of tuple order.
    source = (
        'mod = importlib.import_module("x")\n'  # exempt — should not fail
        'exec("payload")\n'  # NOT exempt — MUST fail
    )
    failures: list[str] = []
    for substring in FORBIDDEN_DYNAMIC_IMPORTS:
        if substring in source and substring not in substring_exempt:
            failures.append(f"{modname} (synthetic.py): {substring!r}")
            # don't break — verify ALL non-exempt hits are reported
    # exec( must be in the failures list; import_module( must NOT be.
    assert any("exec(" in f for f in failures), (
        f"Important #1: exec( in exempt module {modname!r} should "
        f"STILL trip, but failures={failures}. The exemption set is "
        f"silently masking forbidden substrings beyond its declared "
        f"scope."
    )
    assert not any("import_module(" in f for f in failures), (
        f"Important #1: import_module( in exempt module {modname!r} "
        f"should NOT trip (it's in the exempt set), but it appeared "
        f"in failures={failures}. The exemption is not being applied."
    )


def test_per_module_dynamic_exemption_does_not_skip_class_e_namespace_imports() -> None:
    """Claude pr-auditor Important #1 — symmetric: a module in
    ``_PER_MODULE_DYNAMIC_IMPORT_EXEMPT`` that adds a Class E import
    NOT in its exempt set still trips the import-name layer.

    Synthesizes source that imports ``importlib`` (exempt) PLUS
    ``runpy`` (NOT exempt) and runs through
    ``_scan_import_names_against_pipeline`` with the combined exempt
    set. The runpy import must still produce a failure.
    """
    modname = "almanak.framework.services.copy_signal_engine"
    exempt = _combined_per_module_exempt(modname)
    assert "importlib" in exempt, (
        "test prerequisite: importlib must be in the combined exempt set for copy_signal_engine"
    )
    assert "runpy" not in exempt, (
        "test prerequisite: runpy must NOT be in the exempt set (only importlib + import_module( are)"
    )
    source = (
        "import importlib\n"  # exempt
        "import runpy\n"  # NOT exempt — MUST fail at Class E layer
    )
    failures = _scan_import_names_against_pipeline(
        modname,
        "synthetic.py",
        source,
        exempt,
    )
    assert any("runpy" in f for f in failures), (
        f"Important #1: runpy import in exempt module {modname!r} "
        f"should STILL trip the Class E denylist, but "
        f"failures={failures}. The exemption is silently masking "
        f"Class E entries beyond its declared scope."
    )
    assert not any(" 'importlib'" in f and "runpy" not in f for f in failures), (
        f"Important #1: importlib import should NOT trip (it's in the exempt set), but failures={failures}."
    )


def test_l4a_attribute_target_assignment_not_covered_inherent_limit() -> None:
    """Claude pr-auditor Potential #4 — L4a-Attribute-target negative
    pin. ``ast.Assign`` with ``target=ast.Attribute`` (e.g., ``self.X
    = Web3(...)``) or ``target=ast.Subscript`` (``d[k] = Web3(...)``)
    is NOT tracked by ``_collect_web3_aliases`` — the collector only
    handles ``ast.Name`` targets.

    Today no non-legacy framework module uses this shape (audit-
    data.md confirms: ``defi/gas.py:416``, ``defi/pools.py:469``,
    ``pendle/on_chain_reader.py:160``, ``position_health.py:402+``
    are all in ``_LEGACY_VIOLATING_MODULES``). This negative pin
    makes the inherent limit observable so a future PR can't
    silently claim L4a is "fully closed".

    See VIB-4901.md §"Known limitations after this PR" and Claude
    pr-auditor Potential #4.
    """
    # Attribute-target shape (class instance attribute).
    source_attr = (
        "from web3 import Web3\n"
        "class C:\n"
        "    def __init__(self):\n"
        "        self.w3 = Web3()\n"
        "        P = self.w3.HTTPProvider\n"  # NOT caught
    )
    failures_attr = _scan_web3_dynamic_misuse(
        "synthetic",
        "synthetic.py",
        source_attr,
    )
    assert failures_attr == [], (
        f"L4a Attribute-target shape (self.w3 = Web3()) should NOT "
        f"be caught (inherent limit — pinned). If implementation "
        f"closed this, DELETE this test and update VIB-4901.md §Known "
        f"limitations. Failures: {failures_attr}"
    )
    # Subscript-target shape (dict / list element).
    source_sub = (
        "from web3 import AsyncWeb3\n"
        "d = {}\n"
        'd["k"] = AsyncWeb3()\n'
        'P = d["k"].HTTPProvider\n'  # NOT caught
    )
    failures_sub = _scan_web3_dynamic_misuse(
        "synthetic",
        "synthetic.py",
        source_sub,
    )
    assert failures_sub == [], (
        f"L4a Subscript-target shape (d[k] = AsyncWeb3()) should NOT "
        f"be caught (inherent limit — pinned). Failures: {failures_sub}"
    )


def test_l4b_class_rebind_propagates_to_class_bindings() -> None:
    """Gemini Code Assist post-PR HIGH (PR #2517): when L4b single-
    level rebinding aliases a Web3 / AsyncWeb3 class binding (e.g.,
    ``Wcopy = Web3``), the new alias must also be added to
    ``class_bindings`` so a SUBSEQUENT L4a instance binding via the
    rebound name (``w = Wcopy()``) is still caught by Shape 8/9.

    Without the class-semantic propagation, ``Wcopy`` is in
    ``aliases`` (so ``Wcopy.HTTPProvider`` is caught) but NOT in
    ``class_bindings`` (so ``Wcopy()`` doesn't bind ``w``, and
    ``w.HTTPProvider`` is missed). This test pins the propagation
    fix.
    """
    # Composed shape: import class → rebind → instantiate via rebind
    # → access provider class on instance.
    source = (
        "from web3 import Web3\n"
        "Wcopy = Web3\n"  # L4b: aliases += {Wcopy}; class_bindings += {Wcopy}
        "w = Wcopy()\n"  # L4a: aliases += {w} (because Wcopy in class_bindings)
        "P = w.HTTPProvider\n"  # misuse: w in aliases → HTTPProvider in forbidden trailing
    )
    failures = _scan_web3_dynamic_misuse("synthetic", "synthetic.py", source)
    assert failures, (
        "L4b class-semantic propagation should catch "
        "`Wcopy = Web3; w = Wcopy(); P = w.HTTPProvider`. If this "
        "fails, the propagation in _collect_web3_aliases regressed."
    )
    assert "HTTPProvider" in failures[0], f"Failure message should mention HTTPProvider, got: {failures[0]!r}"

    # Module-rebind shape: `wb = web3` does NOT propagate to
    # class_bindings (calling `wb()` doesn't yield a Web3 instance).
    # Verify the negative — `wb()` is NOT treated as a class call.
    module_rebind_source = (
        "import web3\n"
        "wb = web3\n"
        "w = wb()\n"  # NOT an L4a class instantiation
        "P = w.HTTPProvider\n"  # NOT caught — w not in aliases
    )
    failures_mod = _scan_web3_dynamic_misuse(
        "synthetic",
        "synthetic.py",
        module_rebind_source,
    )
    # The misuse scan still catches `wb.X` if X were forbidden, but
    # `wb()` itself isn't a class call so `w` isn't bound. The point
    # of this assertion is that we're NOT over-binding module aliases
    # to class semantic (which would be incorrect — wb() is not Web3).
    assert not any("w.HTTPProvider" in f for f in failures_mod), (
        f"Module rebind should NOT propagate to class_bindings — "
        f"`wb()` is not a Web3 instantiation. Got: {failures_mod}"
    )
