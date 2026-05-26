"""Verify every connector declares Zodiac permission hints.

Zodiac permissions are generated per connector by running the real
``IntentCompiler`` against synthetic intents; connectors contribute
metadata via ``permission_hints.py``. When a connector ships without
that file the generator silently falls back to empty defaults, which
means manifests can omit targets/selectors the connector actually emits
at runtime — causing production reverts under a Zodiac Roles Modifier.

This meta-test is a coverage gate: every connector directory under
``almanak/connectors/`` must define a ``permission_hints.py``
that exports ``PERMISSION_HINTS: PermissionHints``. An empty
``PermissionHints()`` is valid — presence of the file is the signal
that the connector author has considered permission discovery.

See ``.claude/skills/sdk-integrator/SKILL.md`` Phase 6 for patterns.

Exemption: a connector can opt out by placing a ``.permissions_exempt``
sentinel file in its directory (typically for incubating connectors
not yet ready to ship hints). The sentinel may contain a justification
note; presence alone is the opt-out signal.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from almanak.framework.permissions.hints import PermissionHints

CONNECTORS_ROOT = Path(__file__).resolve().parents[3] / "almanak" / "connectors"

# Directories under connectors/ that are NOT protocol connectors — shared
# infrastructure (base types, routing selectors, registries). They have no
# adapter.py and don't participate in Zodiac permission discovery.
# ``base`` / ``vaults`` foundation moved to ``_strategy_base/`` (VIB-4835)
# and is filtered out by the leading-underscore rule in
# ``_discover_connector_dirs``. ``flash_loan`` is a protocol-agnostic
# selector and stays under its own name. ``beefy`` / ``yearn`` are
# gateway-side stubs with no strategy adapter yet.
_NON_CONNECTOR_DIRS: frozenset[str] = frozenset({"flash_loan", "beefy", "yearn"})

_EXEMPT_SENTINEL = ".permissions_exempt"


def _discover_connector_dirs() -> list[Path]:
    """Return sorted connector directories under CONNECTORS_ROOT.

    Excludes shared infra (see ``_NON_CONNECTOR_DIRS``), cache/private dirs
    (leading ``_``), and files (e.g. ``bridge_base.py``).
    """
    return sorted(
        item
        for item in CONNECTORS_ROOT.iterdir()
        if item.is_dir() and not item.name.startswith("_") and item.name not in _NON_CONNECTOR_DIRS
    )


@pytest.mark.parametrize(
    "connector_dir",
    _discover_connector_dirs(),
    ids=lambda p: p.name,
)
def test_connector_declares_permission_hints(connector_dir: Path) -> None:
    """Every connector must ship ``permission_hints.py`` exporting ``PERMISSION_HINTS``."""
    if (connector_dir / _EXEMPT_SENTINEL).exists():
        pytest.skip(
            f"Connector '{connector_dir.name}' opts out of permission coverage "
            f"via {_EXEMPT_SENTINEL} sentinel. Remove the sentinel and add "
            f"permission_hints.py before enabling this connector in a Zodiac "
            f"deployment."
        )

    hints_file = connector_dir / "permission_hints.py"
    assert hints_file.exists(), (
        f"Connector '{connector_dir.name}' is missing permission_hints.py. "
        f"Every connector must declare Zodiac permission hints. Minimal valid "
        f"body:\n\n"
        f"    from almanak.framework.permissions.hints import PermissionHints\n"
        f"    PERMISSION_HINTS = PermissionHints()\n\n"
        f"See .claude/skills/sdk-integrator/SKILL.md Phase 6 for when to use "
        f"non-empty hints (market IDs, fee-tier overrides, static permissions)."
    )

    module_path = f"almanak.connectors.{connector_dir.name}.permission_hints"
    module = importlib.import_module(module_path)

    assert hasattr(module, "PERMISSION_HINTS"), (
        f"{module_path} must export PERMISSION_HINTS (a PermissionHints instance)."
    )

    hints = module.PERMISSION_HINTS
    assert isinstance(hints, PermissionHints), (
        f"{module_path}.PERMISSION_HINTS must be a PermissionHints instance, got {type(hints).__name__}."
    )
