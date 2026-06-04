"""Guard test: every connector with a receipt_parser.py must be in the registry.

This test catches the recurring pattern where a new protocol connector adds a
receipt_parser.py but forgets to register it. Without a registry entry, the
ResultEnricher silently skips enrichment and strategy authors get None where
they expected parsed data.

Discovered in Kitchen Loop iter 167: silo_v2, joelend, euler_v2 all had parser
implementations but no registry entries. All 3 external reviewers flagged this.

VIB-2750: turns a recurring enrichment-skip bug into a P0 CI failure.

VIB-4854 (W2): the registry source-of-truth moved from
``ReceiptParserRegistry._BUILTIN_LOADERS`` (a central protocol → module-path
dict) onto each connector's ``receipt_parser_provider.py`` module. The
strategy-side ``STRATEGY_RECEIPT_PARSER_REGISTRY`` aggregates them. This test
now checks that every connector with a ``receipt_parser.py`` file ALSO has a
sibling ``receipt_parser_provider.py`` registered into the strategy registry —
the structural invariant the old ``_BUILTIN_LOADERS`` check was a proxy for.
"""

from pathlib import Path

import pytest

from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
)
from almanak.connectors._strategy_receipt_registry import (
    STRATEGY_RECEIPT_PARSER_REGISTRY,
)

# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

CONNECTORS_DIR = Path(__file__).resolve().parents[3] / "almanak" / "connectors"

# The base/ directory contains shared infrastructure (BaseReceiptParser),
# not a protocol-specific parser.
EXCLUDED_DIRS = {"base", "__pycache__"}


def _discover_connector_parsers() -> list[tuple[str, ...]]:
    """Find all connector paths that contain a receipt_parser.py.

    CodeRabbit audit (VIB-3226, 2026-04-21): upgraded to a recursive walk
    so nested connector families (e.g. ``bridges/across/``, ``bridges/stargate/``)
    are also validated by the forward completeness check. Returns each
    connector as a tuple of path parts relative to ``CONNECTORS_DIR`` — callers
    turn it into both a module suffix and a human-readable id.
    """
    parsers: list[tuple[str, ...]] = []
    for parser_file in sorted(CONNECTORS_DIR.rglob("receipt_parser.py")):
        rel_parts = parser_file.relative_to(CONNECTORS_DIR).parts[:-1]
        if not rel_parts:
            continue
        if any(part in EXCLUDED_DIRS for part in rel_parts):
            continue
        parsers.append(tuple(rel_parts))
    return parsers


CONNECTOR_PARSERS = _discover_connector_parsers()


def _registered_provider_modules() -> set[str]:
    """Return the set of provider module paths registered into the registry.

    A connector's ``receipt_parser_provider.py`` is "registered" iff it
    contributed a connector instance to
    ``STRATEGY_RECEIPT_PARSER_REGISTRY``. We key on the provider's
    ``__module__`` (e.g. ``"almanak.connectors.uniswap_v3.receipt_parser_provider"``)
    because that's the structural invariant: every ``receipt_parser.py``
    file under a connector folder MUST have a sibling
    ``receipt_parser_provider.py`` instantiated and registered.

    Resolving via the provider's module — not via the resolved parser
    class's ``__module__`` — correctly handles shims like
    ``pancakeswap_perps``, whose provider returns the canonical
    ``AsterPerpsReceiptParser`` class (so the class's ``__module__``
    is ``aster_perps.receipt_parser``, not ``pancakeswap_perps``…). The
    pancakeswap_perps provider file still exists and is still registered;
    the shim is the legitimate way to keep the legacy key alive.
    """
    return {
        type(connector).__module__
        for connector in STRATEGY_RECEIPT_PARSER_REGISTRY.all()
        if isinstance(connector, ReceiptParserCapability)
    }


_REGISTERED_PROVIDER_MODULES = _registered_provider_modules()


# ---------------------------------------------------------------------------
# Forward check: every parser file must have a sibling provider registered
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("connector_parts", CONNECTOR_PARSERS, ids=lambda c: "/".join(c))
def test_connector_parser_registered(connector_parts: tuple[str, ...]):
    """Every connectors/{...}/receipt_parser.py must have a sibling provider.

    The sibling ``receipt_parser_provider.py`` must be registered into
    ``STRATEGY_RECEIPT_PARSER_REGISTRY`` so the ``ResultEnricher`` can
    find the parser at runtime.

    Handles both flat layouts (``connectors/aerodrome/receipt_parser.py``) and
    nested families (``connectors/<family>/<connector>/receipt_parser.py``).
    """
    connector_id = "/".join(connector_parts)
    suffix = ".".join(connector_parts)
    expected_provider_module = f"almanak.connectors.{suffix}.receipt_parser_provider"
    assert expected_provider_module in _REGISTERED_PROVIDER_MODULES, (
        f"Connector '{connector_id}' has a receipt_parser.py but no "
        f"matching sibling receipt_parser_provider.py is registered into "
        f"STRATEGY_RECEIPT_PARSER_REGISTRY (looked for module "
        f"'{expected_provider_module}'). Either add the sibling provider:\n"
        f"  almanak/connectors/{connector_id}/receipt_parser_provider.py\n"
        f"and publish it from almanak/connectors/{connector_id}/connector.py "
        f"as CONNECTOR.receipt_parser_connector. If the parser module is "
        f"intentionally unused, delete it."
    )


# ---------------------------------------------------------------------------
# Reverse check: every registered provider must point to an existing parser
# ---------------------------------------------------------------------------


def test_no_stale_registry_entries():
    """Every registered provider must have a real ``receipt_parser.py`` sibling.

    Catches stale entries left behind when connectors are removed or renamed.

    Accepts two path shapes so nested connector families (if any remain) can
    register their receipt parsers:

      1. ``almanak.connectors.<connector>.receipt_parser_provider`` (4 parts)
      2. ``almanak.connectors.<family>.<connector>.receipt_parser_provider``
         (5 parts, one level of nesting)
    """
    stale = []
    expected_prefix = ("almanak", "connectors")
    for connector in STRATEGY_RECEIPT_PARSER_REGISTRY.all():
        if not isinstance(connector, ReceiptParserCapability):
            continue
        provider_module = type(connector).__module__
        cls_name = type(connector).__name__
        parts = provider_module.split(".")
        if (
            len(parts) not in (4, 5)
            or tuple(parts[:2]) != expected_prefix
            or parts[-1] != "receipt_parser_provider"
        ):
            stale.append(
                f"  {connector.protocol} -> invalid module path format: "
                f"{provider_module}::{cls_name}"
            )
            continue

        connector_dir = CONNECTORS_DIR.joinpath(*parts[2:-1])
        parser_file = connector_dir / "receipt_parser.py"
        if not parser_file.is_file():
            stale.append(
                f"  {connector.protocol} -> {provider_module}::{cls_name} "
                f"(no sibling receipt_parser.py)"
            )

    assert not stale, (
        "Stale entries in STRATEGY_RECEIPT_PARSER_REGISTRY "
        "(parser modules no longer exist):\n" + "\n".join(stale)
    )


# ---------------------------------------------------------------------------
# Sanity: we actually discovered parsers
# ---------------------------------------------------------------------------


def test_discovery_finds_parsers():
    """Sanity check: discovery should find a reasonable number of parsers.

    If this fails, the test infrastructure is broken (wrong path, etc.).
    """
    connector_ids = {"/".join(parts) for parts in CONNECTOR_PARSERS}
    assert "uniswap_v3" in connector_ids, (
        "Discovery failed to find 'uniswap_v3' connector. "
        f"Check CONNECTORS_DIR: {CONNECTORS_DIR}"
    )
    assert len(CONNECTOR_PARSERS) >= 20, (
        f"Only found {len(CONNECTOR_PARSERS)} connector receipt parsers, "
        f"expected at least 20. Check CONNECTORS_DIR: {CONNECTORS_DIR}"
    )
