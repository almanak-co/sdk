"""Guard test: every connector with a receipt_parser.py must be in the registry.

This test catches the recurring pattern where a new protocol connector adds a
receipt_parser.py but forgets to register it in ReceiptParserRegistry._BUILTIN_LOADERS.
Without a registry entry, the ResultEnricher silently skips enrichment and strategy
authors get None where they expected parsed data.

Discovered in Kitchen Loop iter 167: silo_v2, joelend, euler_v2 all had parser
implementations but no registry entries. All 3 external reviewers flagged this.

VIB-2750: turns a recurring enrichment-skip bug into a P0 CI failure.
"""

from pathlib import Path

import pytest

from almanak.framework.execution.receipt_registry import ReceiptParserRegistry

# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

CONNECTORS_DIR = Path(__file__).resolve().parents[3] / "almanak" / "framework" / "connectors"

# The base/ directory contains shared infrastructure (BaseReceiptParser),
# not a protocol-specific parser.
EXCLUDED_DIRS = {"base", "__pycache__"}


def _discover_connector_parsers() -> list[str]:
    """Find all connector directories that contain a receipt_parser.py."""
    parsers = []
    for child in sorted(CONNECTORS_DIR.iterdir()):
        if child.is_dir() and child.name not in EXCLUDED_DIRS:
            if (child / "receipt_parser.py").exists():
                parsers.append(child.name)
    return parsers


CONNECTOR_PARSERS = _discover_connector_parsers()

# Build the set of module paths referenced by the registry.
# Each _BUILTIN_LOADERS value is (module_path, class_name).
_REGISTERED_MODULE_PATHS = {
    module_path for module_path, _class_name in ReceiptParserRegistry._BUILTIN_LOADERS.values()
}


# ---------------------------------------------------------------------------
# Forward check: every parser file must have a registry entry
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("connector_name", CONNECTOR_PARSERS, ids=lambda c: c)
def test_connector_parser_registered(connector_name: str):
    """Every connectors/{name}/receipt_parser.py must have a _BUILTIN_LOADERS entry.

    The registry entry's module_path must point to the connector's receipt_parser module.
    This ensures the ResultEnricher can find the parser at runtime.
    """
    expected_module = f"almanak.framework.connectors.{connector_name}.receipt_parser"
    assert expected_module in _REGISTERED_MODULE_PATHS, (
        f"Connector '{connector_name}' has a receipt_parser.py but no entry in "
        f"ReceiptParserRegistry._BUILTIN_LOADERS points to "
        f"'{expected_module}'. Add an entry like:\n"
        f'    "{connector_name}": (\n'
        f'        "{expected_module}",\n'
        f'        "<ParserClassName>",\n'
        f"    ),"
    )


# ---------------------------------------------------------------------------
# Reverse check: every registry entry must point to an existing module
# ---------------------------------------------------------------------------


def test_no_stale_registry_entries():
    """Every _BUILTIN_LOADERS entry must point to a connector that still exists.

    Catches stale entries left behind when connectors are removed or renamed.
    """
    stale = []
    expected_prefix = ("almanak", "framework", "connectors")
    for protocol, (module_path, class_name) in ReceiptParserRegistry._BUILTIN_LOADERS.items():
        # Validate module path format:
        # "almanak.framework.connectors.<connector>.receipt_parser"
        parts = module_path.split(".")
        if len(parts) != 5 or tuple(parts[:3]) != expected_prefix or parts[4] != "receipt_parser":
            stale.append(
                f"  {protocol} -> invalid module path format: {module_path}::{class_name}"
            )
            continue

        connector_dir = CONNECTORS_DIR / parts[3]
        parser_file = connector_dir / "receipt_parser.py"
        if not parser_file.is_file():
            stale.append(f"  {protocol} -> {module_path}::{class_name}")

    assert not stale, (
        "Stale entries in ReceiptParserRegistry._BUILTIN_LOADERS "
        "(module files no longer exist):\n" + "\n".join(stale)
    )


# ---------------------------------------------------------------------------
# Sanity: we actually discovered parsers
# ---------------------------------------------------------------------------


def test_discovery_finds_parsers():
    """Sanity check: discovery should find a reasonable number of parsers.

    If this fails, the test infrastructure is broken (wrong path, etc.).
    """
    assert "uniswap_v3" in CONNECTOR_PARSERS, (
        "Discovery failed to find 'uniswap_v3' connector. "
        f"Check CONNECTORS_DIR: {CONNECTORS_DIR}"
    )
    assert len(CONNECTOR_PARSERS) >= 20, (
        f"Only found {len(CONNECTOR_PARSERS)} connector receipt parsers, "
        f"expected at least 20. Check CONNECTORS_DIR: {CONNECTORS_DIR}"
    )
