"""Unit tests for `almanak info matrix` CLI command.

Tests validate the support matrix CLI functionality:
- _build_matrix returns correct structure
- _render_table produces readable ASCII output
- Filters (--category, --chain, --protocol) work correctly
- --json flag produces valid JSON
- No-match filter path prints error message
"""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from almanak.framework.cli.support_matrix import (
    _build_matrix,
    _render_table,
    support_matrix,
)


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create Click test runner."""
    return CliRunner()


@pytest.fixture
def matrix_data() -> dict:
    """Build a real matrix from SDK data structures."""
    return _build_matrix()


# =============================================================================
# _build_matrix Tests
# =============================================================================


class TestBuildMatrix:
    """Tests for _build_matrix() function."""

    def test_returns_expected_keys(self, matrix_data: dict) -> None:
        assert "chains" in matrix_data
        assert "protocols" in matrix_data

    def test_chains_is_list_of_strings(self, matrix_data: dict) -> None:
        chains = matrix_data["chains"]
        assert isinstance(chains, list)
        assert len(chains) > 0
        for c in chains:
            assert isinstance(c, str)

    def test_protocols_have_required_fields(self, matrix_data: dict) -> None:
        protocols = matrix_data["protocols"]
        assert isinstance(protocols, list)
        assert len(protocols) > 0
        for p in protocols:
            assert "name" in p
            assert "category" in p
            assert "chains" in p
            assert isinstance(p["chains"], list)

    def test_known_protocols_present(self, matrix_data: dict) -> None:
        """Core protocols should always appear in the matrix."""
        names = {p["name"] for p in matrix_data["protocols"]}
        # At minimum, these should exist
        assert "uniswap_v3" in names
        assert "aave_v3" in names

    def test_known_chains_present(self, matrix_data: dict) -> None:
        """Core chains should always appear."""
        chains = set(matrix_data["chains"])
        assert "ethereum" in chains
        assert "arbitrum" in chains

    def test_chain_order_ethereum_first(self, matrix_data: dict) -> None:
        """Ethereum should be the first chain in the ordered list."""
        chains = matrix_data["chains"]
        assert chains[0] == "ethereum"

    def test_protocol_chains_subset_of_all_chains(self, matrix_data: dict) -> None:
        """Every protocol's chain list should be a subset of the global chains list."""
        all_chains = set(matrix_data["chains"])
        for proto in matrix_data["protocols"]:
            for chain in proto["chains"]:
                assert chain in all_chains, f"{proto['name']} has chain {chain} not in global list"


# =============================================================================
# _render_table Tests
# =============================================================================


class TestRenderTable:
    """Tests for _render_table() function."""

    def test_returns_string(self, matrix_data: dict) -> None:
        result = _render_table(matrix_data)
        assert isinstance(result, str)

    def test_contains_header(self, matrix_data: dict) -> None:
        result = _render_table(matrix_data)
        assert "Protocol" in result
        assert "Category" in result

    def test_contains_chain_names(self, matrix_data: dict) -> None:
        result = _render_table(matrix_data)
        for chain in matrix_data["chains"]:
            assert chain in result

    def test_contains_summary_line(self, matrix_data: dict) -> None:
        result = _render_table(matrix_data)
        assert "Chains:" in result
        assert "Protocols:" in result
        assert "Supported pairs:" in result

    def test_contains_separator(self, matrix_data: dict) -> None:
        result = _render_table(matrix_data)
        lines = result.split("\n")
        # Second line should be a separator
        assert lines[1].startswith("-")


# =============================================================================
# CLI Command Tests
# =============================================================================


class TestSupportMatrixCLI:
    """Tests for the `matrix` click command."""

    def test_default_table_output(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(support_matrix)
        assert result.exit_code == 0
        assert "Protocol" in result.output
        assert "Category" in result.output

    def test_json_output(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(support_matrix, ["--json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "chains" in data
        assert "protocols" in data
        assert isinstance(data["protocols"], list)

    def test_filter_by_category(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(support_matrix, ["--json", "-c", "lending"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        for p in data["protocols"]:
            assert p["category"] == "lending"

    def test_filter_by_chain(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(support_matrix, ["--json", "--chain", "arbitrum"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["chains"] == ["arbitrum"]
        for p in data["protocols"]:
            assert p["chains"] == ["arbitrum"]

    def test_filter_by_protocol(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(support_matrix, ["--json", "-p", "aave"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        for p in data["protocols"]:
            assert "aave" in p["name"].lower()

    def test_no_match_filter(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(support_matrix, ["-c", "nonexistent_category"])
        # Should print error to stderr, exit 0 (click.echo(err=True) doesn't set exit code)
        assert "No protocols match" in result.output or "No protocols match" in (result.stderr or "")

    def test_combined_filters(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(support_matrix, ["--json", "-c", "swap", "--chain", "ethereum"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        for p in data["protocols"]:
            assert p["category"] == "swap"
            assert "ethereum" in p["chains"]


# =============================================================================
# Curve Matrix Entry Tests
# =============================================================================


class TestCurveMatrixEntry:
    """Tests for Curve appearing in the support matrix (swap + LP)."""

    def test_curve_in_swap_category(self, matrix_data: dict) -> None:
        """Curve should appear as a swap protocol."""
        swap_protos = [p for p in matrix_data["protocols"] if p["name"] == "curve" and p["category"] == "swap"]
        assert len(swap_protos) == 1, "Curve should appear exactly once in swap category"
        assert len(swap_protos[0]["chains"]) > 0, "Curve swap should support at least one chain"

    def test_curve_in_lp_category(self, matrix_data: dict) -> None:
        """Curve should appear as an LP protocol."""
        lp_protos = [p for p in matrix_data["protocols"] if p["name"] == "curve" and p["category"] == "lp"]
        assert len(lp_protos) == 1, "Curve should appear exactly once in lp category"
        assert len(lp_protos[0]["chains"]) > 0, "Curve LP should support at least one chain"

    def test_curve_chains_match_addresses(self, matrix_data: dict) -> None:
        """Curve chains in matrix should match CURVE_ADDRESSES keys."""
        from almanak.framework.connectors.curve.adapter import CURVE_ADDRESSES

        expected_chains = set(CURVE_ADDRESSES.keys())
        swap_protos = [p for p in matrix_data["protocols"] if p["name"] == "curve" and p["category"] == "swap"]
        actual_chains = set(swap_protos[0]["chains"])
        assert actual_chains == expected_chains, f"Expected {expected_chains}, got {actual_chains}"

    def test_curve_filter_by_protocol(self, cli_runner: CliRunner) -> None:
        """Filtering by curve protocol should return swap and lp entries."""
        result = cli_runner.invoke(support_matrix, ["--json", "-p", "curve"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        categories = {p["category"] for p in data["protocols"]}
        assert "swap" in categories, "Curve should have swap category"
        assert "lp" in categories, "Curve should have lp category"


# =============================================================================
# Compound V3 Matrix Entry Tests
# =============================================================================


class TestCompoundV3MatrixEntry:
    """Tests for Compound V3 chains matching adapter's COMET_ADDRESSES."""

    def test_compound_v3_chains_match_comet_addresses(self, matrix_data: dict) -> None:
        """Compound V3 chains in matrix must match COMPOUND_V3_COMET_ADDRESSES keys."""
        from almanak.framework.connectors.compound_v3 import COMPOUND_V3_COMET_ADDRESSES

        expected_chains = set(COMPOUND_V3_COMET_ADDRESSES.keys())
        compound_protos = [p for p in matrix_data["protocols"] if p["name"] == "compound_v3"]
        assert len(compound_protos) == 1, "compound_v3 should appear in matrix"
        actual_chains = set(compound_protos[0]["chains"])
        assert actual_chains == expected_chains, (
            f"Matrix has {actual_chains} but adapter has {expected_chains}"
        )
