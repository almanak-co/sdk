"""LLM-facing tool-surface golden snapshot (VIB-4860 / W8, plan §5).

The agent-tool surface the LLM sees is the *rendered tool list* produced
by :class:`~almanak.framework.agent_tools.catalog.ToolCatalog` — consumed
identically by the OpenAI, MCP, and LangChain adapters. A regression here
means "the agent forgets how to call a protocol", which is the headline
risk of W8 (per the ticket).

W8 adopts **Alternative C**: it does NOT move tool definitions out of the
central catalog. The read tools delegate only their on-chain
address/selector knowledge to per-connector providers via
``STRATEGY_AGENT_READ_REGISTRY``; the Lagoon vault tools resolve their SDK
through ``STRATEGY_VAULT_TOOL_REGISTRY``. Neither touches the
``ToolDefinition`` name / description / request schema. Therefore the
expected delta of every pure-dispatch W8 step is **ZERO**.

This test pins ``to_openai_tools()`` + ``to_mcp_tools()`` to a committed
golden. It fails loudly if any W8 change alters a tool name, description,
default, or JSON-Schema field — forcing a conscious golden update with
reviewer sign-off.

Regenerating the golden (only when the surface *intentionally* changes)::

    REGEN_TOOL_SURFACE_GOLDEN=1 uv run pytest \\
        tests/unit/agent_tools/test_tool_surface_snapshot.py \\
        --import-mode=importlib

and review the diff before committing.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from almanak.framework.agent_tools.catalog import get_default_catalog

_SNAPSHOT_PATH = Path(__file__).parent / "_snapshots" / "tool_surface.json"


def _render_surface() -> dict:
    """Render the LLM-facing tool surface deterministically.

    ``json.dumps(..., sort_keys=True)`` on the round-trip gives a
    canonical, order-stable representation so the golden diff is a pure
    semantic diff (catalog list order does not leak into the snapshot).
    """
    catalog = get_default_catalog()
    surface = {
        "openai_tools": catalog.to_openai_tools(),
        "mcp_tools": catalog.to_mcp_tools(),
    }
    # Round-trip through sort_keys to canonicalise nested dict ordering.
    return json.loads(json.dumps(surface, sort_keys=True))


def test_tool_surface_matches_golden() -> None:
    """The rendered OpenAI + MCP tool surface is byte-identical to the golden.

    Opt-in regeneration via ``REGEN_TOOL_SURFACE_GOLDEN=1`` writes the
    current surface and re-reads it so the assertion still runs (a typo
    in the regen path can't silently pass).
    """
    surface = _render_surface()

    if os.environ.get("REGEN_TOOL_SURFACE_GOLDEN"):
        _SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        _SNAPSHOT_PATH.write_text(json.dumps(surface, indent=2, sort_keys=True) + "\n")

    assert _SNAPSHOT_PATH.exists(), (
        f"Golden snapshot missing at {_SNAPSHOT_PATH}. Run with REGEN_TOOL_SURFACE_GOLDEN=1 to create it."
    )
    golden = json.loads(_SNAPSHOT_PATH.read_text())

    # Names are the most user-visible contract — assert them first with a
    # precise diff so a missing/renamed tool is obvious in the failure.
    surface_names = sorted(t["function"]["name"] for t in surface["openai_tools"])
    golden_names = sorted(t["function"]["name"] for t in golden["openai_tools"])
    assert surface_names == golden_names, (
        "Agent-tool NAME set changed vs golden. Added: "
        f"{sorted(set(surface_names) - set(golden_names))}; removed: "
        f"{sorted(set(golden_names) - set(surface_names))}. "
        "If intentional, regenerate with REGEN_TOOL_SURFACE_GOLDEN=1."
    )

    # MCP name set must match too (same catalog, different render shape).
    surface_mcp_names = sorted(t["name"] for t in surface["mcp_tools"])
    golden_mcp_names = sorted(t["name"] for t in golden["mcp_tools"])
    assert surface_mcp_names == golden_mcp_names

    # Full surface byte-equivalence: descriptions + JSON-Schema parameters.
    assert surface == golden, (
        "Rendered tool surface (descriptions / schemas / defaults) differs "
        "from the golden. W8 (Alternative C) must NOT change the LLM-facing "
        "surface — expected delta is ZERO. If this change is intentional, "
        "regenerate with REGEN_TOOL_SURFACE_GOLDEN=1 and get reviewer sign-off."
    )


def test_tool_surface_count_is_pinned() -> None:
    """Sanity pin: 38 tools both ways (mirrors test_catalog.py count pins)."""
    surface = _render_surface()
    assert len(surface["openai_tools"]) == 38
    assert len(surface["mcp_tools"]) == 38
