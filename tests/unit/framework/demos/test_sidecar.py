"""Tests for ``almanak.framework.demos.sidecar``."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from almanak.framework.demos.sidecar import SidecarRegistry


@pytest.fixture
def repo_layout(tmp_path: Path) -> Path:
    """Build a temporary repo-like layout: ``<root>/.github/sidecar-demos.yml``."""
    (tmp_path / ".github").mkdir()
    return tmp_path


def _write_registry(repo: Path, body: str) -> Path:
    path = repo / ".github" / "sidecar-demos.yml"
    path.write_text(textwrap.dedent(body).strip())
    return path


class TestSidecarLoad:
    def test_load_basic(self, repo_layout: Path):
        path = _write_registry(
            repo_layout,
            """
            connectors:
              uniswap_v3:
                demo_dir: almanak/demo_strategies/uniswap_lp
                chain: arbitrum
                force_action: open
                max_iterations: 1
              aerodrome:
                demo_dir: almanak/demo_strategies/aerodrome_lp
                chain: base
                force_action: open
                max_iterations: 1
            """,
        )
        registry = SidecarRegistry.load(path)
        assert set(registry.connectors) == {"uniswap_v3", "aerodrome"}
        uni = registry.connectors["uniswap_v3"]
        assert uni.chain == "arbitrum"
        assert uni.demo_name == "uniswap_lp"
        assert uni.max_iterations == 1

    def test_explicit_connector_gives_one_connector_several_cells(self, repo_layout: Path):
        path = _write_registry(
            repo_layout,
            """
            connectors:
              uniswap_v4:
                demo_dir: strategies/internal/demo_catalog/uniswap_v4_swap
                chain: base
                force_action: ""
                max_iterations: 1
              uniswap_v4_lp:
                connector: uniswap_v4
                demo_dir: almanak/demo_strategies/uniswap_v4_hooks
                chain: base
                force_action: open
                max_iterations: 1
            """,
        )
        registry = SidecarRegistry.load(path)
        assert set(registry.connectors) == {"uniswap_v4", "uniswap_v4_lp"}
        assert registry.covered_connectors() == {"uniswap_v4"}
        lp = registry.connectors["uniswap_v4_lp"]
        assert (lp.key, lp.connector, lp.demo_name) == ("uniswap_v4_lp", "uniswap_v4", "uniswap_v4_hooks")
        assert [e.key for e in registry.for_connector("uniswap_v4")] == ["uniswap_v4", "uniswap_v4_lp"]

    def test_cell_key_outside_the_shared_charset_raises(self, repo_layout: Path):
        """The shell picker's awk only sees `[a-z0-9_]+` keys; a key it cannot read must not load."""
        path = _write_registry(
            repo_layout,
            """
            connectors:
              uniswap-v4-lp:
                connector: uniswap_v4
                demo_dir: almanak/demo_strategies/uniswap_v4_hooks
                chain: base
            """,
        )
        with pytest.raises(ValueError, match=r"must match \[a-z0-9_\]\+"):
            SidecarRegistry.load(path)

    def test_non_string_cell_key_raises_valueerror(self, repo_layout: Path):
        """`123:` loads as int; callers catch ValueError, so a TypeError would escape as a traceback."""
        path = _write_registry(
            repo_layout,
            """
            connectors:
              123:
                demo_dir: almanak/demo_strategies/uniswap_v4_hooks
                chain: base
            """,
        )
        with pytest.raises(ValueError, match=r"must match \[a-z0-9_\]\+"):
            SidecarRegistry.load(path)

    def test_invalid_connector_name_raises(self, repo_layout: Path):
        path = _write_registry(
            repo_layout,
            """
            connectors:
              uniswap_v4_lp:
                connector: "uniswap v4"
                demo_dir: almanak/demo_strategies/uniswap_v4_hooks
                chain: base
            """,
        )
        with pytest.raises(ValueError, match="invalid connector name"):
            SidecarRegistry.load(path)

    def test_missing_demo_dir_raises(self, repo_layout: Path):
        path = _write_registry(
            repo_layout,
            """
            connectors:
              uniswap_v3:
                chain: arbitrum
                force_action: open
                max_iterations: 1
            """,
        )
        with pytest.raises(ValueError, match="missing demo_dir"):
            SidecarRegistry.load(path)

    def test_missing_chain_raises(self, repo_layout: Path):
        path = _write_registry(
            repo_layout,
            """
            connectors:
              uniswap_v3:
                demo_dir: almanak/demo_strategies/uniswap_lp
                force_action: open
                max_iterations: 1
            """,
        )
        with pytest.raises(ValueError, match="missing chain"):
            SidecarRegistry.load(path)

    def test_non_int_max_iterations_raises(self, repo_layout: Path):
        path = _write_registry(
            repo_layout,
            """
            connectors:
              uniswap_v3:
                demo_dir: almanak/demo_strategies/uniswap_lp
                chain: arbitrum
                force_action: open
                max_iterations: many
            """,
        )
        with pytest.raises(ValueError, match="non-integer max_iterations"):
            SidecarRegistry.load(path)

    def test_empty_file_yields_empty_registry(self, repo_layout: Path):
        path = _write_registry(repo_layout, "")
        registry = SidecarRegistry.load(path)
        assert registry.connectors == {}

    def test_top_level_must_be_mapping(self, repo_layout: Path):
        path = _write_registry(
            repo_layout,
            """
            connectors:
              - one
              - two
            """,
        )
        with pytest.raises(ValueError, match="must be a mapping"):
            SidecarRegistry.load(path)

    def test_for_demo_lookup(self, repo_layout: Path):
        # Write the demo dir so resolve() works.
        demo_dir = repo_layout / "almanak" / "demo_strategies" / "uniswap_lp"
        demo_dir.mkdir(parents=True)
        path = _write_registry(
            repo_layout,
            """
            connectors:
              uniswap_v3:
                demo_dir: almanak/demo_strategies/uniswap_lp
                chain: arbitrum
                force_action: open
                max_iterations: 1
            """,
        )
        registry = SidecarRegistry.load(path)
        entry = registry.for_demo(demo_dir)
        assert entry is not None
        assert entry.connector == "uniswap_v3"

    def test_load_default_finds_repo_file(self):
        """Sanity check: ``load_default()`` finds the actual repo file."""
        registry = SidecarRegistry.load_default()
        # The default registry has at least one connector entry.
        assert len(registry.connectors) >= 1
