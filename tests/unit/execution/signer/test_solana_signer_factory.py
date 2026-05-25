"""Tests for SVM signer surface exposed by SvmFamily.signer_factory (VIB-4804).

This file pins three things:

1. ``SvmFamily.signer_factory(...)`` returns a module namespace mirroring
   the shape used by :meth:`EvmFamily.signer_factory` — see
   ``blueprints/05-connectors.md`` and the protocol contract on
   :class:`ChainFamilyAdapter`.
2. The Solana signer surface exposed via the family adapter is the same
   :class:`SolanaSigner` consumed by the gateway-side
   :class:`SolanaExecutionPlanner`. Adding a second SVM signer somewhere
   else would silently break the gateway boundary.
3. The Jupiter and Kamino connector adapters DO NOT import the SVM signer
   directly. They emit unsigned base64-encoded ``VersionedTransaction``s;
   the gateway-side planner is the only component that holds the
   keypair. This is the static enforcement of the "key material does not
   leave the gateway" rule for SVM connectors.

If a future SVM connector needs to do anything other than emit unsigned
transactions, the right path is to widen the gateway's planner — not to
import :class:`SolanaSigner` from connector code. The static assertions
in :class:`TestSvmConnectorGatewayBoundary` will block that drift at
PR-review time.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import types
from pathlib import Path

import pytest

from almanak.framework.chain_family import EvmFamily, SvmFamily


# ---------------------------------------------------------------------------
# 1. SvmFamily.signer_factory shape
# ---------------------------------------------------------------------------


class TestSvmFamilySignerFactoryShape:
    """The SVM signer factory must mirror the EVM signer factory contract."""

    def test_returns_module(self) -> None:
        ns = SvmFamily().signer_factory(descriptor=None)
        assert isinstance(ns, types.ModuleType)

    def test_exposes_solana_signer(self) -> None:
        ns = SvmFamily().signer_factory(descriptor=None)
        # The concrete signer class lives in the returned namespace and
        # is the SolanaSigner that the gateway-side planner constructs.
        from almanak.framework.execution.solana import SolanaSigner

        assert getattr(ns, "SolanaSigner") is SolanaSigner

    def test_exposes_solana_signer_error(self) -> None:
        ns = SvmFamily().signer_factory(descriptor=None)
        from almanak.framework.execution.solana import SolanaSignerError

        assert getattr(ns, "SolanaSignerError") is SolanaSignerError

    def test_exposes_planner(self) -> None:
        """The gateway-side planner is the sole consumer of the keypair.
        Exposing it through the family namespace keeps the boundary single-
        sourced — every component that needs to talk to SVM signing
        reaches through ``family.signer_factory``.
        """
        ns = SvmFamily().signer_factory(descriptor=None)
        from almanak.framework.execution.solana import SolanaExecutionPlanner

        assert getattr(ns, "SolanaExecutionPlanner") is SolanaExecutionPlanner


class TestFamilySignerFactoryParity:
    """EVM and SVM families return the same KIND of value — a module
    namespace whose ``__name__`` resolves to a real package, and which
    exposes at least one concrete signer class.

    This parity is what lets a generic caller (gateway ``ExecutionService``,
    future ``SignerSelector``) work without an ``isinstance(family,
    EvmFamily)`` ladder.
    """

    def test_both_return_modules(self) -> None:
        evm_ns = EvmFamily().signer_factory(descriptor=None)
        svm_ns = SvmFamily().signer_factory(descriptor=None)
        assert isinstance(evm_ns, types.ModuleType)
        assert isinstance(svm_ns, types.ModuleType)

    def test_both_expose_signer_class(self) -> None:
        from almanak.framework.execution.signer import LocalKeySigner
        from almanak.framework.execution.solana import SolanaSigner

        evm_ns = EvmFamily().signer_factory(descriptor=None)
        svm_ns = SvmFamily().signer_factory(descriptor=None)
        assert getattr(evm_ns, "LocalKeySigner") is LocalKeySigner
        assert getattr(svm_ns, "SolanaSigner") is SolanaSigner

    def test_modules_have_canonical_dotted_names(self) -> None:
        """Pin the dotted module names so a refactor that "splits" the
        signer modules into smaller pieces breaks this test loudly,
        forcing a deliberate revisit of the gateway-side ``import`` path.
        """
        evm_ns = EvmFamily().signer_factory(descriptor=None)
        svm_ns = SvmFamily().signer_factory(descriptor=None)
        assert evm_ns.__name__ == "almanak.framework.execution.signer"
        assert svm_ns.__name__ == "almanak.framework.execution.solana"


# ---------------------------------------------------------------------------
# 2. SvmFamily.signer_factory is callable with no real descriptor
# ---------------------------------------------------------------------------


class TestSvmFamilySignerFactoryDescriptor:
    """The ``descriptor`` argument is reserved for future use (per-chain
    signer config). Today both implementations ignore it; the test pins
    that contract so a refactor that starts reading the descriptor at
    call time must update the test together — preventing a silent change
    in the public protocol.
    """

    def test_descriptor_argument_is_optional_today(self) -> None:
        SvmFamily().signer_factory(descriptor=None)
        EvmFamily().signer_factory(descriptor=None)

    def test_descriptor_argument_is_ignored_today(self) -> None:
        a = SvmFamily().signer_factory(descriptor=None)
        b = SvmFamily().signer_factory(descriptor=object())
        assert a is b


# ---------------------------------------------------------------------------
# 3. Gateway boundary — Jupiter and Kamino must not import the SVM signer
# ---------------------------------------------------------------------------


_REPO_ROOT = Path(__file__).resolve().parents[4]


def _import_lines(connector_pkg: str) -> list[tuple[str, int]]:
    """Return (module, line) for every ``import`` / ``from ... import ...``
    statement in every Python file under ``connector_pkg``.

    Walks the AST rather than text-grepping so commented-out imports and
    string mentions don't trigger false positives. Relative imports are
    preserved with their leading-dot prefix (``from ...execution.solana
    import SolanaSigner`` records as ``...execution.solana.SolanaSigner``)
    so the forbidden-suffix matcher can catch them too.
    """
    pkg = importlib.import_module(connector_pkg)
    pkg_path = Path(inspect.getfile(pkg)).parent
    hits: list[tuple[str, int]] = []
    for py in pkg_path.rglob("*.py"):
        try:
            tree = ast.parse(py.read_text())
        except SyntaxError:  # pragma: no cover — defensive
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    hits.append((alias.name, node.lineno))
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                rel_prefix = "." * getattr(node, "level", 0)
                for alias in node.names:
                    hits.append((f"{rel_prefix}{mod}.{alias.name}", node.lineno))
    return hits


# Dot-bounded path fragments that connector code must NOT contain. Each
# entry matches when it appears as a *segment-aligned* substring of a
# recorded import dotted-name — i.e. preceded by start-of-string or a dot
# AND followed by end-of-string or a dot. That boundary keeps
# ``my_signer`` from matching ``signer`` and ``solders_keypair`` from
# matching ``solders.keypair``.
#
# We list the short forms only. Fully-qualified prefixes are redundant
# under segment-aligned matching and were bypassable by relative-import
# forms (Gemini + CodeRabbit review on #2425) and by top-level re-export
# (``from almanak.framework.execution.solana import SolanaSigner``, which
# never contains the literal ``.signer`` segment).
_FORBIDDEN_IMPORT_FRAGMENTS = (
    # Anything from the signer submodule itself, absolute or relative.
    "execution.solana.signer",
    # The class re-exported at top of ``almanak.framework.execution.solana``
    # (``from almanak.framework.execution.solana import SolanaSigner`` —
    # the dotted name produced by :func:`_import_lines` is
    # ``almanak.framework.execution.solana.SolanaSigner``).
    "execution.solana.SolanaSigner",
    "execution.solana.SolanaSignerError",
    # Stray ``solders.keypair`` imports from connector code. The legitimate
    # solders imports under connector code are for type construction
    # (Pubkey, Instruction, MessageV0) — Keypair belongs in the gateway-
    # held signer only.
    "solders.keypair",
)


def _dotted_contains_fragment(dotted: str, fragment: str) -> bool:
    """True when ``fragment`` appears in ``dotted`` as a segment-aligned
    sub-path — boundaries on both sides must be either string ends or
    dots.

    Examples (with ``fragment = "execution.solana.signer"``):
      * ``"almanak.framework.execution.solana.signer.SolanaSigner"`` — True
      * ``"...execution.solana.signer.SolanaSigner"`` — True
      * ``"execution.solana.signer"`` — True
      * ``"my_execution.solana.signer"`` — False (left boundary fails)
    """
    if dotted == fragment:
        return True
    needle_prefix = fragment + "."
    needle_suffix = "." + fragment
    needle_middle = "." + fragment + "."
    return (
        dotted.startswith(needle_prefix)
        or dotted.endswith(needle_suffix)
        or needle_middle in dotted
    )


def _import_violates_boundary(dotted: str) -> str | None:
    """Return the matching forbidden fragment, or ``None``."""
    for fragment in _FORBIDDEN_IMPORT_FRAGMENTS:
        if _dotted_contains_fragment(dotted, fragment):
            return fragment
    return None


class TestSvmConnectorGatewayBoundary:
    """Static enforcement of the "key material does not leave the gateway"
    rule for SVM connectors.

    Per ``CLAUDE.md``: "Strategies have no secrets. Connector and framework
    code may NOT load or handle the Solana keypair directly. The keypair
    lives in the gateway; the signer makes calls through the gateway."

    Jupiter and Kamino satisfy this today by emitting unsigned base64
    ``VersionedTransaction`` blobs through ``ActionBundle.transactions``.
    The gateway-side ``SolanaExecutionPlanner`` is the only component that
    constructs a ``SolanaSigner``. These tests block a regression where a
    well-meaning refactor pulls signer construction back into connector
    code.
    """

    @pytest.mark.parametrize(
        "connector_pkg",
        [
            "almanak.framework.connectors.jupiter",
            "almanak.framework.connectors.kamino",
        ],
    )
    def test_connector_does_not_import_svm_signer(self, connector_pkg: str) -> None:
        offenders = []
        for dotted, lineno in _import_lines(connector_pkg):
            matched = _import_violates_boundary(dotted)
            if matched is not None:
                offenders.append((dotted, lineno, matched))
        assert offenders == [], (
            f"{connector_pkg} must not import the SVM signer or solders.keypair "
            f"directly — the keypair lives in the gateway. Move the offending "
            f"imports to the gateway-side planner. Offenders: {offenders}"
        )

    @pytest.mark.parametrize(
        "connector_pkg",
        [
            "almanak.framework.connectors.jupiter",
            "almanak.framework.connectors.kamino",
        ],
    )
    def test_connector_does_not_call_solana_signer_constructors(
        self, connector_pkg: str
    ) -> None:
        """Belt-and-braces against indirect re-exports.

        The forbidden-import check above already pins direct imports of the
        signer module + top-level re-exports + ``solders.keypair`` (with
        dot-boundary suffix matching covering absolute and relative import
        forms). This second pass catches two narrow leftover cases:

        1. ``import almanak.framework.execution.solana`` followed by
           ``almanak.framework.execution.solana.SolanaSigner(...)`` — the
           import binds the parent package, so the suffix matcher won't
           flag the import line, but the constructor call still appears
           textually as ``SolanaSigner(``.
        2. ``from almanak.framework.execution.solana import SolanaSigner
           as X`` followed by ``X(...)`` — the import IS caught above
           (the alias name preserves ``.SolanaSigner``), but if a future
           refactor reorders the suffix list, this scan stays as a second
           line of defense.

        Limitation: a deliberate alias rebinding through a third module
        (``from foo import SolanaSigner_renamed``, then calls under that
        new name) would slip past this text scan. That is an explicit
        adversarial pattern, not a likely accidental refactor — the
        primary defense remains the import-level suffix matcher, with
        this text scan as belt-and-braces.
        """
        pkg = importlib.import_module(connector_pkg)
        pkg_path = Path(inspect.getfile(pkg)).parent
        offenders: list[tuple[str, str]] = []
        forbidden_calls = (
            "SolanaSigner(",
            "SolanaSigner.from_",
            "Keypair(",
            "Keypair.from_",
        )
        for py in pkg_path.rglob("*.py"):
            text = py.read_text()
            for marker in forbidden_calls:
                if marker in text:
                    offenders.append((str(py.relative_to(_REPO_ROOT)), marker))
        assert offenders == [], (
            f"{connector_pkg} must not construct SolanaSigner or Keypair "
            f"directly. The gateway-side SolanaExecutionPlanner owns the "
            f"keypair. Offenders: {offenders}"
        )


class TestForbiddenImportMatcher:
    """Pin the dot-boundary suffix semantics used by the boundary test.

    A regression in this matcher would silently weaken
    :class:`TestSvmConnectorGatewayBoundary`. The cases below cover the
    review feedback on PR #2425 (Gemini + CodeRabbit on relative-import
    bypass) and the genuine adjacent-substring false-positive risk.
    """

    @pytest.mark.parametrize(
        "dotted",
        [
            # Absolute, fully qualified.
            "almanak.framework.execution.solana.signer.SolanaSigner",
            "almanak.framework.execution.solana.signer.SolanaSignerError",
            # Top-level re-export (``from almanak.framework.execution.solana
            # import SolanaSigner``). The previous fully-qualified-only
            # forbidden string missed this.
            "almanak.framework.execution.solana.SolanaSigner",
            "almanak.framework.execution.solana.SolanaSignerError",
            # Relative imports, with leading-dot prefix preserved by
            # :func:`_import_lines`.
            "...execution.solana.signer.SolanaSigner",
            "....execution.solana.signer.SolanaSigner",
            # ``solders.keypair`` (absolute and via attribute).
            "solders.keypair",
            "solders.keypair.Keypair",
        ],
    )
    def test_caught(self, dotted: str) -> None:
        assert _import_violates_boundary(dotted) is not None, dotted

    @pytest.mark.parametrize(
        "dotted",
        [
            # Adjacent-substring false-positive guard — these must NOT
            # match because the dot boundary is required.
            "almanak.framework.connectors.kamino.my_signer",
            "my_solana_signer",
            "package.solders_keypair",
            # Legitimate solders imports under connector code stay clean.
            "solders.pubkey.Pubkey",
            "solders.instruction.Instruction",
            "solders.message.MessageV0",
            "solders.transaction.VersionedTransaction",
            # Unrelated almanak modules.
            "almanak.framework.execution.signer.LocalKeySigner",
            "almanak.framework.connectors.jupiter.adapter",
        ],
    )
    def test_not_caught(self, dotted: str) -> None:
        assert _import_violates_boundary(dotted) is None, dotted


# ---------------------------------------------------------------------------
# 4. SvmFamily.signer_factory is wired (regression for the VIB-4803 deferral)
# ---------------------------------------------------------------------------


class TestSvmSignerFactoryNoLongerDeferred:
    """Regression: in VIB-4803 the SVM signer factory raised
    ``NotImplementedError("VIB-4804")``. After VIB-4804 lands, the same
    call must succeed. This test fails loudly if a future refactor
    accidentally restores the NotImplementedError stub.
    """

    def test_does_not_raise_not_implemented_error(self) -> None:
        SvmFamily().signer_factory(descriptor=None)  # must not raise

    def test_returns_non_none(self) -> None:
        assert SvmFamily().signer_factory(descriptor=None) is not None
