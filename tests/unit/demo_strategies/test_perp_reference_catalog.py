from __future__ import annotations

import ast
from pathlib import Path

_DEMO_ROOT = Path(__file__).resolve().parents[3] / "almanak" / "demo_strategies"


def test_perp_reference_catalog_distinguishes_authoring_from_diagnostics():
    guide = (_DEMO_ROOT / "REFERENCE.md").read_text(encoding="utf-8")

    assert "gmx_v2_directional_perp" in guide
    assert "hyperliquid_trailing_perp" in guide
    assert "Diagnostic perpetual demos" in guide
    assert "gmx_perp_lifecycle" in guide


def test_every_packaged_perp_demo_uses_the_normalized_probe_boundary():
    for name in (
        "gmx_v2_directional_perp",
        "hyperliquid_trailing_perp",
        "gmx_perp_lifecycle",
    ):
        source = (_DEMO_ROOT / name / "strategy.py").read_text(encoding="utf-8")
        assert "probe_perp_position" in source, f"{name} bypasses the normalized perp probe"
        direct_reads = [
            node
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "perp_positions"
        ]
        assert not direct_reads, f"{name} reads raw perp rows instead of the normalized probe"
