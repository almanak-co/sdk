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
from datetime import datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from almanak.framework.cli.support_matrix import (
    SCHEMA_VERSION,
    SUPPORTED_CATEGORIES,
    WITHHELD_CATEGORIES,
    _build_matrix,
    _git_dir,
    _intent_category_map,
    _published_matrix_names,
    _render_table,
    _source_commit,
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

    def test_bridge_protocols_present(self, matrix_data: dict) -> None:
        """Bridge protocols (across, stargate) must appear and be categorised as bridge."""
        names = {p["name"] for p in matrix_data["protocols"]}
        assert "across" in names, "across missing from support matrix"
        assert "stargate" in names, "stargate missing from support matrix"
        bridge_names = {p["name"] for p in matrix_data["protocols"] if p["category"] == "bridge"}
        assert "across" in bridge_names, "across not in bridge category"
        assert "stargate" in bridge_names, "stargate not in bridge category"

    def test_curvance_present(self, matrix_data: dict) -> None:
        """Curvance lending protocol must appear in the matrix."""
        names = {p["name"] for p in matrix_data["protocols"]}
        assert "curvance" in names, "curvance missing from support matrix"

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
        from almanak.connectors.curve.adapter import CURVE_ADDRESSES

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
        from almanak.connectors.compound_v3 import COMPOUND_V3_COMET_ADDRESSES

        expected_chains = set(COMPOUND_V3_COMET_ADDRESSES.keys())
        compound_protos = [p for p in matrix_data["protocols"] if p["name"] == "compound_v3"]
        assert len(compound_protos) == 1, "compound_v3 should appear in matrix"
        actual_chains = set(compound_protos[0]["chains"])
        assert actual_chains == expected_chains, (
            f"Matrix has {actual_chains} but adapter has {expected_chains}"
        )


# =============================================================================
# Previously-missing connector coverage
# =============================================================================


class TestPreviouslyMissingConnectors:
    """Guards against regressions where real connectors drop out of the matrix."""

    @pytest.mark.parametrize(
        ("name", "category", "expected_chains"),
        [
            ("silo_v2", "lending", {"avalanche"}),
            # joelend removed — Joe Lend wound down by governance; VIB-3960
            # jupiter_lend removed — folded into compiler_solana.py but
            # unexercised (no demo / no intent test); deregistered from
            # ConnectorRegistry and from support_matrix.py
            ("aster_perps", "perps", {"bsc"}),
            ("pancakeswap_perps", "perps", {"bsc"}),
            # hyperliquid PERP execution now shipped on HyperEVM via CoreWriter
            # (PERP_OPEN/PERP_CLOSE); the runner data layer is tracked separately
            # (VIB-5576) but the connector is registered and matrix-visible.
            ("hyperliquid", "perps", {"hyperevm"}),
            ("gimo", "yield", {"zerog"}),
            # polymarket / prediction temporarily withheld from the matrix
            # pending further testing — see TestPredictionDisabledPendingTesting.
            ("fluid", "swap", {"arbitrum", "base", "ethereum", "polygon"}),
        ],
    )
    def test_connector_present(
        self, matrix_data: dict, name: str, category: str, expected_chains: set[str]
    ) -> None:
        entries = [p for p in matrix_data["protocols"] if p["name"] == name and p["category"] == category]
        assert len(entries) == 1, f"{name} ({category}) should appear exactly once"
        assert set(entries[0]["chains"]) == expected_chains

    @pytest.mark.parametrize(
        ("name", "category"),
        [
            # Deregistered from ConnectorRegistry and removed from
            # support_matrix.py collectors. Re-emitting any of these would
            # advertise an unexercised / not-production-ready connector in
            # `almanak strat matrix`.
            ("jupiter_lend", "lending"),  # folded compiler unexercised
            ("joelend", "lending"),  # VIB-3960 — protocol wound down
        ],
    )
    def test_deregistered_connector_absent(
        self, matrix_data: dict, name: str, category: str
    ) -> None:
        entries = [p for p in matrix_data["protocols"] if p["name"] == name and p["category"] == category]
        assert entries == [], f"{name} ({category}) is deregistered; must not appear in matrix"

    def test_euler_v2_chains_match_adapter(self, matrix_data: dict) -> None:
        from almanak.connectors.euler_v2.adapter import CHAIN_ADDRESSES

        entries = [p for p in matrix_data["protocols"] if p["name"] == "euler_v2" and p["category"] == "lending"]
        assert len(entries) == 1
        assert set(entries[0]["chains"]) == set(CHAIN_ADDRESSES.keys())

    def test_traderjoe_v2_swap_entry(self, matrix_data: dict) -> None:
        """TraderJoe V2 has a dedicated swap compilation path (VIB-1928); matrix must expose it."""
        from almanak.connectors.traderjoe_v2.addresses import TRADERJOE_V2

        entries = [p for p in matrix_data["protocols"] if p["name"] == "traderjoe_v2" and p["category"] == "swap"]
        assert len(entries) == 1
        assert set(entries[0]["chains"]) == set(TRADERJOE_V2.keys())

    def test_uniswap_v4_swap_entry(self, matrix_data: dict) -> None:
        """Uniswap V4 swaps through the Universal Router are supported on every V4 chain."""
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

        entries = [p for p in matrix_data["protocols"] if p["name"] == "uniswap_v4" and p["category"] == "swap"]
        assert len(entries) == 1
        assert set(entries[0]["chains"]) == set(UNISWAP_V4.keys())


# =============================================================================
# Prediction-market / Flash-loan disabled-pending-testing Tests
# =============================================================================


class TestPredictionDisabledPendingTesting:
    """Guards that the `prediction` and `flash_loan` categories are withheld
    from `almanak info matrix` while those capabilities undergo further
    testing.

    The connectors stay registered and the intents still compile/execute —
    they are simply not advertised as supported until validation completes.
    Removing `ACTION_PREDICTION` / `ACTION_FLASH_LOAN` from
    `SUPPORTED_CATEGORIES` is what gates rendering (see support_matrix.py).
    Re-enable by adding the constants back and restoring the original
    presence assertions (previously TestPredictionCategory / VIB-3139).
    """

    def test_prediction_category_absent(self, matrix_data: dict) -> None:
        categories = {p["category"] for p in matrix_data["protocols"]}
        assert "prediction" not in categories

    def test_flash_loan_category_absent(self, matrix_data: dict) -> None:
        categories = {p["category"] for p in matrix_data["protocols"]}
        assert "flash_loan" not in categories

    def test_polymarket_absent_from_matrix(self, matrix_data: dict) -> None:
        """Polymarket must not surface while prediction is withheld."""
        entries = [p for p in matrix_data["protocols"] if p["name"] == "polymarket"]
        assert entries == [], "polymarket must not appear while prediction is disabled"

    def test_balancer_absent_from_matrix(self, matrix_data: dict) -> None:
        """Balancer (flash-loan-only venue) must not surface while flash loans
        are withheld."""
        entries = [p for p in matrix_data["protocols"] if p["name"] == "balancer"]
        assert entries == [], "balancer must not appear while flash loans are disabled"

    def test_filter_by_prediction_category_empty(self, cli_runner: CliRunner) -> None:
        """`almanak info matrix -c prediction` should now match nothing."""
        result = cli_runner.invoke(support_matrix, ["-c", "prediction"])
        assert "No protocols match" in result.output or "No protocols match" in (result.stderr or "")

    def test_filter_by_flash_loan_category_empty(self, cli_runner: CliRunner) -> None:
        """`almanak info matrix -c flash_loan` should now match nothing."""
        result = cli_runner.invoke(support_matrix, ["-c", "flash_loan"])
        assert "No protocols match" in result.output or "No protocols match" in (result.stderr or "")

    def test_disabled_categories_absent_from_supported(self) -> None:
        """Neither category should be registered in SUPPORTED_CATEGORIES while
        withheld; the underlying constants remain defined for easy re-enable."""
        from almanak.framework.cli.support_matrix import (
            ACTION_FLASH_LOAN,
            ACTION_PREDICTION,
            SUPPORTED_CATEGORIES,
        )

        assert ACTION_PREDICTION == "prediction"
        assert ACTION_FLASH_LOAN == "flash_loan"
        assert ACTION_PREDICTION not in SUPPORTED_CATEGORIES
        assert ACTION_FLASH_LOAN not in SUPPORTED_CATEGORIES

    def test_category_help_text_excludes_disabled(self) -> None:
        """CLI --category help text must not advertise the withheld categories."""
        opt = next(p for p in support_matrix.params if p.name == "category")
        assert "prediction" not in (opt.help or "")
        assert "flash_loan" not in (opt.help or "")


# =============================================================================
# Registry-driven matrix discovery (VIB-4856 / W4)
# =============================================================================


class TestDynamicCapabilityDiscovery:
    """Locks the W4 invariant: a connector that publishes
    ``MatrixEntry`` rows automatically appears in the matrix without any
    edit to ``support_matrix.py``.

    The test registers a synthetic ``ConnectorManifest`` into the live
    ``ConnectorRegistry`` (using a unique connector name that no real
    connector uses), rebuilds the matrix, and asserts the synthetic
    rows surface in the rendered output. Teardown unregisters the
    connector so the test is isolated from siblings in the same suite.

    Matrix metadata is published via :class:`MatrixEntry` (strategy-side)
    rather than ``SupportedActionsCapability`` (gateway-side): the matrix
    CLI is a strategy-container module and the strategy-side import
    boundary (``tests/static/test_strategy_import_boundary.py``) forbids
    reading anything under ``almanak.connectors._base.gateway_*``. See
    the ``support_matrix`` module docstring for the architectural call.
    """

    def test_matrix_entries_picks_up_new_connector(self) -> None:
        """A ``ConnectorManifest`` with a single ``matrix_entries`` row
        produces exactly one matrix row, verbatim from the declaration.
        Mirrors the simple-case dispatch every connector goes through.
        """
        from almanak.connectors._strategy_base.registry import (
            ConnectorManifest,
            ConnectorRegistry,
            MatrixEntry,
        )
        from almanak.framework.intents.vocabulary import IntentType

        manifest = ConnectorManifest(
            name="vib_4856_mock_swap",
            intents=(IntentType.SWAP,),
            chains=("ethereum",),
            matrix_entries=(
                MatrixEntry(
                    matrix_name="vib_4856_mock_swap",
                    category="swap",
                    chains=frozenset({"ethereum", "arbitrum"}),
                ),
            ),
        )
        ConnectorRegistry.register(manifest)
        try:
            data = _build_matrix()
            mock_rows = [p for p in data["protocols"] if p["name"] == "vib_4856_mock_swap"]
            assert len(mock_rows) == 1, f"mock connector should produce one matrix row, got {mock_rows!r}"
            assert mock_rows[0]["category"] == "swap"
            assert set(mock_rows[0]["chains"]) == {"ethereum", "arbitrum"}
        finally:
            ConnectorRegistry._entries.pop("vib_4856_mock_swap", None)

    def test_matrix_entries_multi_row_connector(self) -> None:
        """One ``ConnectorManifest`` can publish multiple ``MatrixEntry``
        rows under different ``matrix_name``\\ s — the mechanism Aerodrome's
        slipstream alias uses in production.
        """
        from almanak.connectors._strategy_base.registry import (
            ConnectorManifest,
            ConnectorRegistry,
            MatrixEntry,
        )
        from almanak.framework.intents.vocabulary import IntentType

        manifest = ConnectorManifest(
            name="vib_4856_mock_multi",
            intents=(IntentType.LP_OPEN,),
            chains=("ethereum",),
            matrix_entries=(
                MatrixEntry(
                    matrix_name="vib_4856_mock_multi",
                    category="lp",
                    chains=frozenset({"ethereum"}),
                ),
                MatrixEntry(
                    matrix_name="vib_4856_mock_multi_alias",
                    category="lp",
                    chains=frozenset({"ethereum", "base"}),
                ),
            ),
        )
        ConnectorRegistry.register(manifest)
        try:
            data = _build_matrix()
            names = {p["name"] for p in data["protocols"]}
            assert "vib_4856_mock_multi" in names
            assert "vib_4856_mock_multi_alias" in names

            alias_rows = [p for p in data["protocols"] if p["name"] == "vib_4856_mock_multi_alias"]
            assert set(alias_rows[0]["chains"]) == {"base", "ethereum"}
        finally:
            ConnectorRegistry._entries.pop("vib_4856_mock_multi", None)

    def test_manifest_matrix_entries_picks_up_new_connector(self) -> None:
        """Strategy-side path: a ``ConnectorManifest`` with explicit
        ``matrix_entries`` produces matrix rows verbatim — used by
        connectors without a gateway-side provider (e.g. LiFi).
        """
        from almanak.connectors._strategy_base.registry import (
            ConnectorManifest,
            ConnectorRegistry,
            MatrixEntry,
        )
        from almanak.framework.intents.vocabulary import IntentType

        manifest = ConnectorManifest(
            name="vib_4856_mock_strategy",
            intents=(IntentType.SWAP,),
            chains=("ethereum",),
            matrix_entries=(
                MatrixEntry(
                    matrix_name="vib_4856_mock_strategy",
                    category="aggregator",
                    chains=frozenset({"ethereum", "polygon"}),
                ),
            ),
        )
        ConnectorRegistry.register(manifest)
        try:
            data = _build_matrix()
            rows = [
                p
                for p in data["protocols"]
                if p["name"] == "vib_4856_mock_strategy"
            ]
            assert len(rows) == 1
            assert rows[0]["category"] == "aggregator"
            assert set(rows[0]["chains"]) == {"ethereum", "polygon"}
        finally:
            ConnectorRegistry._entries.pop("vib_4856_mock_strategy", None)

    def test_manifest_empty_matrix_entries_suppresses_derivation(self) -> None:
        """A connector that declares ``matrix_entries=()`` (e.g. when the
        gateway side is authoritative) does NOT produce a derived row.
        Guards against the regression where the strategy-side intent
        derivation double-counts a connector that the gateway already
        published.
        """
        from almanak.connectors._strategy_base.registry import (
            ConnectorManifest,
            ConnectorRegistry,
        )
        from almanak.framework.intents.vocabulary import IntentType

        manifest = ConnectorManifest(
            name="vib_4856_mock_suppressed",
            intents=(IntentType.SUPPLY, IntentType.BORROW),
            chains=("ethereum",),
            matrix_entries=(),
        )
        ConnectorRegistry.register(manifest)
        try:
            data = _build_matrix()
            rows = [
                p
                for p in data["protocols"]
                if p["name"] == "vib_4856_mock_suppressed"
            ]
            assert rows == [], (
                "matrix_entries=() must suppress strategy-side derivation"
            )
        finally:
            ConnectorRegistry._entries.pop("vib_4856_mock_suppressed", None)


# =============================================================================
# Per-(intent, chain) exclusions (VIB-6111)
# =============================================================================


class TestIntentChainExclusionDerivation:
    """A category's chain set is the UNION of its intents' narrowed chain sets.

    The consequence that matters in production: a category survives on a chain
    as long as ONE of its verbs does (Aave V3 keeps its ``lending`` row on
    mantle because SUPPLY / WITHDRAW / REPAY still work), and a category
    disappears from a chain only when EVERY verb in it is excluded there.
    """

    @staticmethod
    def _derive(intents, chains, exclusions):
        from almanak.connectors._strategy_base.registry import ConnectorManifest
        from almanak.framework.cli.support_matrix import _derive_entries_from_intents

        manifest = ConnectorManifest(
            name="vib_6111_probe",
            intents=intents,
            chains=chains,
            intent_chain_exclusions=exclusions,
        )
        return {
            (name, category): set(chain_set)
            for name, category, chain_set in _derive_entries_from_intents(
                manifest.name, manifest.intents, manifest.chains, manifest.chains_for_intent
            )
        }

    def test_partially_excluded_category_keeps_the_chain(self) -> None:
        from almanak.connectors._strategy_base.registry import IntentChainExclusion
        from almanak.framework.intents.vocabulary import IntentType

        derived = self._derive(
            (IntentType.SUPPLY, IntentType.BORROW),
            ("ethereum", "mantle"),
            (
                IntentChainExclusion(
                    intent=IntentType.BORROW,
                    chains=frozenset({"mantle"}),
                    reason="ltv zeroed on every reserve",
                    ticket="VIB-6111",
                ),
            ),
        )

        assert derived[("vib_6111_probe", "lending")] == {"ethereum", "mantle"}

    def test_fully_excluded_category_drops_the_chain(self) -> None:
        from almanak.connectors._strategy_base.registry import IntentChainExclusion
        from almanak.framework.intents.vocabulary import IntentType

        # SWAP is the ONLY verb in the swap category, so excluding it on mantle
        # removes mantle from the swap row entirely — while the lending row,
        # driven by a different verb, keeps mantle.
        derived = self._derive(
            (IntentType.SWAP, IntentType.SUPPLY),
            ("ethereum", "mantle"),
            (
                IntentChainExclusion(
                    intent=IntentType.SWAP,
                    chains=frozenset({"mantle"}),
                    reason="no router deployed",
                    ticket="VIB-6111",
                ),
            ),
        )

        assert derived[("vib_6111_probe", "swap")] == {"ethereum"}
        assert derived[("vib_6111_probe", "lending")] == {"ethereum", "mantle"}

    def test_no_exclusions_is_unchanged_behaviour(self) -> None:
        from almanak.framework.intents.vocabulary import IntentType

        derived = self._derive((IntentType.SWAP,), ("ethereum", "mantle"), None)
        assert derived == {("vib_6111_probe", "swap"): {"ethereum", "mantle"}}

    def test_narrowing_callable_is_required_not_a_silent_fallback(self) -> None:
        """Omitting the narrowing read must RAISE, never widen silently.

        It used to default to the raw ``chains``, so a caller that forgot the
        argument quietly got pre-VIB-6111 semantics — a silent widening in the
        one function whose contract is truthful narrowing. Wrong answers must be
        loud here.
        """
        import pytest

        from almanak.framework.cli.support_matrix import _derive_entries_from_intents
        from almanak.framework.intents.vocabulary import IntentType

        with pytest.raises(TypeError):
            _derive_entries_from_intents(
                "vib_6111_probe", (IntentType.SWAP,), ("ethereum", "mantle")
            )


class TestExclusionsSurviveTheFullMatrixBuild:
    """VIB-6111 regression: Phase B must not re-widen a narrowed row.

    The per-function tests above exercise ``_derive_entries_from_intents`` in
    ISOLATION, which is exactly why the original defect passed them: the
    narrowing was correct there and then undone one call later.
    ``_build_matrix()`` runs Phase A (registry) AND Phase B (compiler routing
    tables), and Phase B unions router-table chains into any key it does not
    consider authoritative. A connector that appears in ``PROTOCOL_ROUTERS``
    therefore got its excluded chain added straight back.

    This test must run the FULL build, and must use a protocol name that really
    is in the routing tables — otherwise it re-tests Phase A and proves nothing.
    """

    def test_excluded_chain_is_not_re_added_by_compiler_tables(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from almanak.framework.intents import compiler_constants
        from almanak.connectors._strategy_base.registry import (
            ConnectorManifest,
            ConnectorRegistry,
            IntentChainExclusion,
        )
        from almanak.framework.intents.vocabulary import IntentType

        proto_name = "vib_6111_router_probe"
        routed_chain = "ethereum"
        other_chain = "arbitrum"

        # Put the synthetic protocol in the routing table so Phase B genuinely
        # tries to widen it. A real connector name can't be used here:
        # ``_build_matrix`` re-imports every connector, which would collide with
        # the injected manifest.
        patched_routers = {
            chain: dict(protos) if isinstance(protos, dict) else list(protos)
            for chain, protos in compiler_constants.PROTOCOL_ROUTERS.items()
        }
        bucket = patched_routers.setdefault(routed_chain, {})
        if isinstance(bucket, dict):
            bucket[proto_name] = "0x" + "11" * 20
        else:
            bucket.append(proto_name)
        monkeypatch.setattr(compiler_constants, "PROTOCOL_ROUTERS", patched_routers)

        manifest = ConnectorManifest(
            name=proto_name,
            intents=(IntentType.SWAP, IntentType.SUPPLY),
            chains=(routed_chain, other_chain),
            intent_chain_exclusions=(
                IntentChainExclusion(
                    intent=IntentType.SWAP,
                    chains=frozenset({routed_chain}),
                    reason="probe: connector declares SWAP unsupported here",
                    ticket="VIB-6111",
                ),
            ),
        )
        ConnectorRegistry.register(manifest)
        try:
            data = _build_matrix()
            swap_rows = [
                p for p in data["protocols"] if p["name"] == proto_name and p["category"] == "swap"
            ]
            assert swap_rows, "the connector should still publish a swap row for its other chain"
            chains = set(swap_rows[0]["chains"])
            assert other_chain in chains, "the non-excluded chain must survive"
            assert routed_chain not in chains, (
                f"{routed_chain!r} was excluded for SWAP but reappeared in the rendered matrix — "
                "Phase B (compiler routing tables) re-widened a narrowed row"
            )
        finally:
            ConnectorRegistry._entries.pop(proto_name, None)

    def test_exclusion_does_not_freeze_unrelated_categories(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An exclusion on a LENDING verb must not shrink the SWAP row.

        The first fix froze every derived row of any exclusion-declaring
        connector, which subtracted compiler-table-only chains from categories
        the exclusion never mentioned — under-advertising real support, and
        invisible to the excluded-chain assertion above.
        """
        from almanak.framework.intents import compiler_constants
        from almanak.connectors._strategy_base.registry import (
            ConnectorManifest,
            ConnectorRegistry,
            IntentChainExclusion,
        )
        from almanak.framework.intents.vocabulary import IntentType

        proto_name = "vib_6111_category_probe"
        # Router table covers a chain the manifest does NOT declare; the
        # historical union is what puts it on the swap row.
        patched = {
            chain: dict(protos) if isinstance(protos, dict) else list(protos)
            for chain, protos in compiler_constants.PROTOCOL_ROUTERS.items()
        }
        bucket = patched.setdefault("base", {})
        if isinstance(bucket, dict):
            bucket[proto_name] = "0x" + "22" * 20
        else:
            bucket.append(proto_name)
        monkeypatch.setattr(compiler_constants, "PROTOCOL_ROUTERS", patched)

        manifest = ConnectorManifest(
            name=proto_name,
            intents=(IntentType.SWAP, IntentType.SUPPLY, IntentType.BORROW),
            chains=("ethereum", "arbitrum"),
            intent_chain_exclusions=(
                IntentChainExclusion(
                    intent=IntentType.BORROW,
                    chains=frozenset({"arbitrum"}),
                    reason="probe: lending-only exclusion",
                    ticket="VIB-6111",
                ),
            ),
        )
        ConnectorRegistry.register(manifest)
        try:
            data = _build_matrix()
            swap = [p for p in data["protocols"] if p["name"] == proto_name and p["category"] == "swap"]
            assert swap, "swap row must exist"
            assert "base" in set(swap[0]["chains"]), (
                "a BORROW exclusion must not remove the compiler-table chain 'base' "
                "from the unrelated swap row"
            )
            lending = [
                p for p in data["protocols"] if p["name"] == proto_name and p["category"] == "lending"
            ]
            assert lending, "lending row must exist (SUPPLY still works on both chains)"
            assert set(lending[0]["chains"]) == {"ethereum", "arbitrum"}, (
                "SUPPLY survives on both chains, so the lending row keeps both"
            )
        finally:
            ConnectorRegistry._entries.pop(proto_name, None)


    def test_unmentioned_compiler_chains_survive_the_narrowing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only the EXCLUDED chains are subtracted — not the whole row.

        Freezing a narrowed key stops Phase B re-adding the excluded chain, but
        it also drops compiler-table chains the exclusion never mentioned, which
        under-advertises real support. The denylist is per-chain for that reason.
        """
        from almanak.framework.intents import compiler_constants
        from almanak.connectors._strategy_base.registry import (
            ConnectorManifest,
            ConnectorRegistry,
            IntentChainExclusion,
        )
        from almanak.framework.intents.vocabulary import IntentType

        proto_name = "vib_6111_denylist_probe"
        patched = {
            chain: dict(protos) if isinstance(protos, dict) else list(protos)
            for chain, protos in compiler_constants.PROTOCOL_ROUTERS.items()
        }
        # Router table lists the protocol on the EXCLUDED chain and on a chain
        # the manifest never declares at all.
        for chain in ("ethereum", "polygon"):
            bucket = patched.setdefault(chain, {})
            if isinstance(bucket, dict):
                bucket[proto_name] = "0x" + "33" * 20
            else:
                bucket.append(proto_name)
        monkeypatch.setattr(compiler_constants, "PROTOCOL_ROUTERS", patched)

        manifest = ConnectorManifest(
            name=proto_name,
            # TWO intents: excluding the only verb on a declared chain is
            # rejected by the dual invariant ("the connector claims a chain it
            # supports nothing on"), so SUPPLY keeps ethereum alive while SWAP
            # is disclaimed there.
            intents=(IntentType.SWAP, IntentType.SUPPLY),
            chains=("ethereum", "arbitrum"),
            intent_chain_exclusions=(
                IntentChainExclusion(
                    intent=IntentType.SWAP,
                    chains=frozenset({"ethereum"}),
                    reason="probe: SWAP disclaimed here",
                    ticket="VIB-6111",
                ),
            ),
        )
        ConnectorRegistry.register(manifest)
        try:
            data = _build_matrix()
            row = [
                p for p in data["protocols"] if p["name"] == proto_name and p["category"] == "swap"
            ]
            assert row, "swap row must exist"
            chains = set(row[0]["chains"])
            assert "ethereum" not in chains, "the EXCLUDED chain must not be re-advertised"
            assert "arbitrum" in chains, "the declared, non-excluded chain must survive"
            assert "polygon" in chains, (
                "a compiler-table chain the exclusion never mentioned must still union in — "
                "freezing the whole row would silently under-advertise it"
            )
        finally:
            ConnectorRegistry._entries.pop(proto_name, None)


# =============================================================================
# Schema v2 — per-intent chain coverage (Edge/Platform execution contract)
# =============================================================================


def _rows_named(data: dict, name: str) -> list[dict]:
    return [p for p in data["protocols"] if p["name"] == name]


def _row(data: dict, name: str, category: str) -> dict:
    rows = [p for p in _rows_named(data, name) if p["category"] == category]
    assert rows, f"expected a {name}/{category} row"
    return rows[0]


class TestSchemaV2BackwardCompatibility:
    """v2 adds fields; it must not move or reshape a single v1 field.

    Edge and the Platform's ``supported_protocol`` generator both consume the
    v1 shape today, and the docs table renders from the same builder. A v2 that
    quietly renamed or re-ordered anything would break them on upgrade.
    """

    def test_v1_fields_still_present_and_typed(self, matrix_data: dict) -> None:
        for proto in matrix_data["protocols"]:
            assert isinstance(proto["name"], str)
            assert isinstance(proto["category"], str)
            assert isinstance(proto["chains"], list)
            assert proto["chains"] == sorted(proto["chains"])

    def test_v2_fields_present_on_every_row(self, matrix_data: dict) -> None:
        for proto in matrix_data["protocols"]:
            assert isinstance(proto["chainsByIntent"], dict), proto["name"]
            assert isinstance(proto["intentsKnown"], bool), proto["name"]

    def test_build_matrix_is_deterministic(self) -> None:
        """No timestamp leaks into the builder — only the CLI JSON envelope.

        ``generatedAt`` in ``_build_matrix`` would make the table renderer, the
        agent tool and every test non-reproducible.
        """
        assert _build_matrix() == _build_matrix()


class TestFluidNoLongerOverAdvertisesLending:
    """The widening this schema was designed around (ALM-3034 / ALM-3044).

    Fluid ships SWAP on four chains but fToken lending on arbitrum + base only
    (VIB-5030). That narrowing used to live in a hand-written ``matrix_entries``
    lending row, which scoped the RENDERED row while
    ``chains_for_intent(SUPPLY)`` — the accessor every consumer is told to ask —
    still answered all four chains. Publishing per-intent coverage straight from
    the accessor would have shipped that split as a live over-advertisement, so
    the truth moved to ``intent_chain_exclusions``.

    If someone reverts fluid to the override, this test fails loudly rather than
    Edge quietly routing a SUPPLY to a chain with no fToken market.
    """

    def test_supply_is_arbitrum_and_base_only(self, matrix_data: dict) -> None:
        by_intent = _row(matrix_data, "fluid", "lending")["chainsByIntent"]
        assert by_intent["SUPPLY"] == ["arbitrum", "base"]
        assert by_intent["WITHDRAW"] == ["arbitrum", "base"]

    def test_swap_keeps_all_four_chains(self, matrix_data: dict) -> None:
        by_intent = _row(matrix_data, "fluid", "swap")["chainsByIntent"]
        assert by_intent["SWAP"] == ["arbitrum", "base", "ethereum", "polygon"]

    def test_rendered_rows_are_unchanged_by_the_reconciliation(self, matrix_data: dict) -> None:
        """Moving the narrowing to exclusions must be rendering-neutral."""
        assert _row(matrix_data, "fluid", "swap")["chains"] == [
            "arbitrum",
            "base",
            "ethereum",
            "polygon",
        ]
        assert _row(matrix_data, "fluid", "lending")["chains"] == ["arbitrum", "base"]

    def test_fluid_declares_no_borrow(self, matrix_data: dict) -> None:
        """ALM-3034: ``category: lending`` reads as borrow-capable; it isn't."""
        assert "BORROW" not in _row(matrix_data, "fluid", "lending")["chainsByIntent"]


class TestCategoryCollapseIsUndone:
    """The four things ``yield`` means, and the perps-only case (ALM-3029)."""

    @pytest.mark.parametrize(
        ("protocol", "expected_intents"),
        [
            ("lido", {"STAKE", "UNSTAKE"}),
            ("morpho_vault", {"VAULT_DEPOSIT", "VAULT_REDEEM"}),
            ("pendle", {"SWAP", "LP_OPEN", "LP_CLOSE", "WITHDRAW"}),
        ],
    )
    def test_yield_protocols_publish_their_real_verbs(
        self, matrix_data: dict, protocol: str, expected_intents: set[str]
    ) -> None:
        row = _row(matrix_data, protocol, "yield")
        assert set(row["chainsByIntent"]) == expected_intents

    def test_pendle_renders_as_yield_but_publishes_swap(self, matrix_data: dict) -> None:
        """VIB-6174: the ``yield`` override is deliberate (VIB-5300 drift guard).

        Both facts are true at once under v2 — the rendering category stays
        ``yield`` while the intent map carries the SWAP that Edge needs.
        """
        row = _row(matrix_data, "pendle", "yield")
        assert row["category"] == "yield"
        assert row["chainsByIntent"]["SWAP"] == ["arbitrum", "ethereum"]

    def test_gmx_is_perps_only(self, matrix_data: dict) -> None:
        row = _row(matrix_data, "gmx_v2", "perps")
        assert set(row["chainsByIntent"]) == {"PERP_OPEN", "PERP_CLOSE"}

    def test_lending_protocols_separate_supply_from_borrow(self, matrix_data: dict) -> None:
        row = _row(matrix_data, "morpho_blue", "lending")
        assert {"SUPPLY", "BORROW", "REPAY", "WITHDRAW"} <= set(row["chainsByIntent"])


class TestWithheldCategoriesStayWithheld:
    """A withheld category must not leak back through the intent map.

    ``flash_loan`` and ``prediction`` are withheld as a PRODUCT decision (an EOA
    cannot receive a flash loan). Publishing them per-intent would re-advertise
    through v2 exactly what the category filter suppresses.
    """

    def test_flash_loan_not_published_for_a_connector_that_declares_it(
        self, matrix_data: dict
    ) -> None:
        row = _row(matrix_data, "morpho_blue", "lending")
        assert "FLASH_LOAN" not in row["chainsByIntent"]

    def test_no_row_publishes_a_withheld_intent(self, matrix_data: dict) -> None:
        withheld = {
            intent.name
            for intent, category in _intent_category_map().items()
            if category in WITHHELD_CATEGORIES
        }
        assert withheld, "expected at least FLASH_LOAN / PREDICTION_* to be withheld"
        for proto in matrix_data["protocols"]:
            assert not (withheld & set(proto["chainsByIntent"])), proto["name"]

    def test_every_category_is_supported_or_withheld_exactly_once(self) -> None:
        """No third state where a row is hidden but its intents are published."""
        assert not set(SUPPORTED_CATEGORIES) & set(WITHHELD_CATEGORIES)
        for category in _intent_category_map().values():
            assert category in SUPPORTED_CATEGORIES or category in WITHHELD_CATEGORIES, category


class TestChainsByIntentNeverOutrunsTheDeclaration:
    """The invariant that makes v2 safe to gate execution on."""

    def test_coverage_is_a_subset_of_declared_chains(self, matrix_data: dict) -> None:
        from almanak.connectors._strategy_base.registry import (
            ConnectorRegistry,
            _import_all_connectors,
        )

        _import_all_connectors()
        manifests = {m.name: m for m in ConnectorRegistry.all()}
        for proto in matrix_data["protocols"]:
            manifest = manifests.get(proto["name"])
            if manifest is None or manifest.chains is None:
                continue
            declared = set(manifest.chains)
            for intent, chains in proto["chainsByIntent"].items():
                assert set(chains) <= declared, f"{proto['name']}/{intent} outran strategy_chains"

    def test_compiler_table_chains_are_not_claimed_as_intent_coverage(self, matrix_data: dict) -> None:
        """A routable-but-unverified chain stays out of the execution answer.

        ``enso`` renders more chains (compiler routing tables + its declared
        ``matrix_entries``) than its ``strategy_chains`` declares. Those extras may
        appear in the rendering row; they must never read as executable coverage.

        The example chain is **derived**, not hardcoded: this test originally
        asserted on ``sepolia``, and when VIB-6231 removed that unresolvable chain
        the precondition failed even though the property under test still held.
        """
        row = _row(matrix_data, "enso", "aggregator")
        rendered = set(row["chains"])
        covered = set(row["chainsByIntent"]["SWAP"])
        extras = rendered - covered
        assert extras, (
            "precondition: enso must render at least one chain its intents do not cover, or this test proves nothing"
        )
        # The real property: nothing rendered-but-undeclared leaks into coverage.
        assert not (extras & covered)

    def test_intent_chain_exclusions_propagate(self, matrix_data: dict) -> None:
        """VIB-6111: aave_v3 BORROW is excluded on mantle, SUPPLY is not."""
        by_intent = _row(matrix_data, "aave_v3", "lending")["chainsByIntent"]
        assert "mantle" in by_intent["SUPPLY"]
        assert "mantle" not in by_intent["BORROW"]

    def test_a_connectors_rows_all_publish_the_same_map(self, matrix_data: dict) -> None:
        """Coverage is connector-scoped, so reading any one row is sufficient.

        Intersecting with each row's chains would make the same intent report
        different coverage depending on which row a consumer happened to read.
        """
        by_name: dict[str, list[dict]] = {}
        for proto in matrix_data["protocols"]:
            by_name.setdefault(proto["name"], []).append(proto)
        for name, rows in by_name.items():
            first = rows[0]["chainsByIntent"]
            for other in rows[1:]:
                assert other["chainsByIntent"] == first, name


class TestIntentsKnownDistinguishesUnknownFromUnsupported:
    """Rows no manifest describes must read as unknown, not unsupported.

    A consumer that fails closed on "operation absent" would otherwise demote
    every compiler-table-only DEX and alias row — real, routable venues — the
    moment it started keying on per-intent coverage.
    """

    @pytest.mark.parametrize(
        "protocol", ["velodrome", "agni_finance", "aerodrome_slipstream"]
    )
    def test_manifest_less_rows_are_marked_unknown(
        self, matrix_data: dict, protocol: str
    ) -> None:
        rows = _rows_named(matrix_data, protocol)
        assert rows, f"{protocol} must still render"
        for row in rows:
            assert row["intentsKnown"] is False
            assert row["chainsByIntent"] == {}
            assert row["chains"], "the row still advertises chains — it is routable"

    def test_manifest_backed_rows_are_marked_known(self, matrix_data: dict) -> None:
        for protocol in ("fluid", "pendle", "gmx_v2", "aave_v3"):
            for row in _rows_named(matrix_data, protocol):
                assert row["intentsKnown"] is True

    def test_known_rows_carry_coverage(self, matrix_data: dict) -> None:
        """``intentsKnown`` must not be true-but-empty for a rendered row."""
        for proto in matrix_data["protocols"]:
            if proto["intentsKnown"]:
                assert proto["chainsByIntent"], proto["name"]

    def test_no_two_connectors_claim_the_same_matrix_name(self) -> None:
        """Coverage is merged by matrix_name — a collision would blend two
        connectors' intents into one row without anyone noticing."""
        from almanak.connectors._strategy_base.registry import (
            ConnectorRegistry,
            _import_all_connectors,
        )

        _import_all_connectors()
        seen: dict[str, str] = {}
        for manifest in ConnectorRegistry.all():
            for name in _published_matrix_names(manifest):
                assert name not in seen, f"{name} claimed by {seen.get(name)} and {manifest.name}"
                seen[name] = manifest.name


class TestSchemaV2JsonEnvelope:
    """Provenance — a vendored copy has drifted from its SDK before."""

    def test_envelope_fields(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(support_matrix, ["--json"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["schemaVersion"] == SCHEMA_VERSION == 2
        assert payload["sdkVersion"]
        assert "sourceCommit" in payload
        assert datetime.fromisoformat(payload["generatedAt"]).tzinfo is not None

    def test_chain_filter_keeps_the_map_consistent(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(support_matrix, ["--json", "--chain", "arbitrum"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        for proto in payload["protocols"]:
            assert proto["chains"] == ["arbitrum"]
            for intent, chains in proto["chainsByIntent"].items():
                assert chains == ["arbitrum"], f"{proto['name']}/{intent} leaked another chain"

    def test_chain_filter_drops_intents_without_coverage(self, cli_runner: CliRunner) -> None:
        """A row surviving on a routable-only chain must show no coverage.

        ``uniswap_v3`` renders on ``linea`` via the compiler routing tables but
        declares no strategy-side support there, so the filtered row keeps its
        chain and reports an empty map — unknown, not supported.
        """
        result = cli_runner.invoke(support_matrix, ["--json", "--chain", "linea"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        rows = [p for p in payload["protocols"] if p["name"] == "uniswap_v3"]
        assert rows, "precondition: uniswap_v3 still renders on linea"
        for row in rows:
            assert row["chainsByIntent"] == {}


class TestSourceCommitProvenance:
    """Provenance resolution — plain file I/O over ``.git``, no subprocess.

    Covers every branch because the failure mode is silent: a helper that
    returns ``None`` where a commit exists degrades provenance to "unknown"
    without any error, and a vendored artifact that has drifted then looks
    identical to one that hasn't.
    """

    def _repo(self, tmp_path: Path, head: str) -> Path:
        git = tmp_path / ".git"
        git.mkdir()
        (git / "HEAD").write_text(head, encoding="utf-8")
        return git

    def test_detached_head_is_the_sha_itself(self, tmp_path: Path) -> None:
        """The common CI shape — actions/checkout leaves HEAD detached."""
        self._repo(tmp_path, "a" * 40)
        assert _source_commit(tmp_path / "mod.py") == "a" * 40

    def test_symbolic_head_resolves_via_loose_ref(self, tmp_path: Path) -> None:
        git = self._repo(tmp_path, "ref: refs/heads/main\n")
        loose = git / "refs" / "heads" / "main"
        loose.parent.mkdir(parents=True)
        loose.write_text("b" * 40 + "\n", encoding="utf-8")
        assert _source_commit(tmp_path / "mod.py") == "b" * 40

    def test_symbolic_head_falls_back_to_packed_refs(self, tmp_path: Path) -> None:
        """A fresh clone packs refs and writes no loose ref."""
        git = self._repo(tmp_path, "ref: refs/heads/main\n")
        (git / "packed-refs").write_text(
            "# pack-refs with: peeled fully-peeled sorted\n"
            f"{'c' * 40} refs/heads/main\n"
            f"{'d' * 40} refs/tags/v1\n"
            f"^{'e' * 40}\n",
            encoding="utf-8",
        )
        assert _source_commit(tmp_path / "mod.py") == "c" * 40

    def test_linked_worktree_resolves_through_commondir(self, tmp_path: Path) -> None:
        """A worktree holds its own HEAD but shares refs with the main checkout.

        This repo runs a lot of work in agent worktrees; without the commondir
        hop every one of them would silently report no provenance.
        """
        main_git = tmp_path / "main" / ".git"
        (main_git / "refs" / "heads").mkdir(parents=True)
        (main_git / "refs" / "heads" / "feat").write_text("f" * 40, encoding="utf-8")

        wt_git = tmp_path / "wt" / ".git-dir"
        wt_git.mkdir(parents=True)
        (wt_git / "HEAD").write_text("ref: refs/heads/feat\n", encoding="utf-8")
        (wt_git / "commondir").write_text(str(main_git), encoding="utf-8")

        checkout = tmp_path / "wt"
        (checkout / ".git").write_text(f"gitdir: {wt_git}\n", encoding="utf-8")
        assert _source_commit(checkout / "mod.py") == "f" * 40

    def test_unresolvable_ref_is_none_not_a_guess(self, tmp_path: Path) -> None:
        self._repo(tmp_path, "ref: refs/heads/missing\n")
        assert _source_commit(tmp_path / "mod.py") is None

    def test_no_git_dir_is_none(self, tmp_path: Path) -> None:
        """An installed wheel has no .git; sdkVersion identifies it instead."""
        assert _git_dir(tmp_path / "mod.py") is None
        assert _source_commit(tmp_path / "mod.py") is None

    def test_malformed_git_file_is_none(self, tmp_path: Path) -> None:
        (tmp_path / ".git").write_text("not a gitdir pointer\n", encoding="utf-8")
        assert _git_dir(tmp_path / "mod.py") is None

    def test_git_file_pointing_nowhere_is_none(self, tmp_path: Path) -> None:
        (tmp_path / ".git").write_text(f"gitdir: {tmp_path / 'absent'}\n", encoding="utf-8")
        assert _git_dir(tmp_path / "mod.py") is None

    def test_relative_gitdir_pointer_resolves(self, tmp_path: Path) -> None:
        real = tmp_path / "real-git"
        real.mkdir()
        (real / "HEAD").write_text("9" * 40, encoding="utf-8")
        (tmp_path / ".git").write_text("gitdir: ./real-git\n", encoding="utf-8")
        assert _source_commit(tmp_path / "mod.py") == "9" * 40

    def test_unreadable_head_is_none_not_an_exception(self, tmp_path: Path) -> None:
        """``.git`` present but HEAD missing — degrade, never raise.

        `almanak info matrix --json` must not fail because provenance is
        unavailable; the matrix is the payload, provenance is the annotation.
        """
        (tmp_path / ".git").mkdir()
        assert _source_commit(tmp_path / "mod.py") is None

    def test_empty_loose_ref_is_none(self, tmp_path: Path) -> None:
        git = self._repo(tmp_path, "ref: refs/heads/main\n")
        loose = git / "refs" / "heads" / "main"
        loose.parent.mkdir(parents=True)
        loose.write_text("  \n", encoding="utf-8")
        assert _source_commit(tmp_path / "mod.py") is None


class TestProvenanceRejectsMalformedCommits:
    """False provenance is worse than none.

    Without validation, any non-empty text in `.git/HEAD`, a loose ref, or
    `packed-refs` is published verbatim as `sourceCommit` — a truncated write,
    a stray editor newline, or a `ref:` line written into a ref file by mistake
    would all identify a build that does not exist. A consumer diffing a
    vendored copy against the SDK it claims to describe would then compare
    against a value naming no build at all.
    """

    def _repo(self, tmp_path: Path, head: str) -> Path:
        git = tmp_path / ".git"
        git.mkdir()
        (git / "HEAD").write_text(head, encoding="utf-8")
        return git

    @pytest.mark.parametrize(
        "head",
        [
            "not-a-sha",
            "z" * 40,          # right length, not hex
            "a" * 39,          # one short
            "a" * 41,          # one long
            "ref: refs/heads/main extra junk",
        ],
    )
    def test_malformed_detached_head_is_none(self, tmp_path: Path, head: str) -> None:
        self._repo(tmp_path, head)
        assert _source_commit(tmp_path / "mod.py") is None

    def test_malformed_loose_ref_is_none(self, tmp_path: Path) -> None:
        git = self._repo(tmp_path, "ref: refs/heads/main\n")
        loose = git / "refs" / "heads" / "main"
        loose.parent.mkdir(parents=True)
        loose.write_text("deadbeef-not-a-sha\n", encoding="utf-8")
        assert _source_commit(tmp_path / "mod.py") is None

    def test_malformed_packed_ref_is_none(self, tmp_path: Path) -> None:
        git = self._repo(tmp_path, "ref: refs/heads/main\n")
        (git / "packed-refs").write_text("garbage refs/heads/main\n", encoding="utf-8")
        assert _source_commit(tmp_path / "mod.py") is None

    def test_sha256_object_id_is_accepted(self, tmp_path: Path) -> None:
        """Validation must not break a repo using the newer hash."""
        self._repo(tmp_path, "b" * 64)
        assert _source_commit(tmp_path / "mod.py") == "b" * 64

    def test_cli_publishes_null_for_a_malformed_head(self, tmp_path: Path) -> None:
        """End-to-end: the artifact must say 'unidentified', not publish junk."""
        import almanak.framework.cli.support_matrix as sm

        self._repo(tmp_path, "not-a-sha")
        original = sm._git_dir
        sm._git_dir = lambda *a, **k: tmp_path / ".git"
        try:
            result = CliRunner().invoke(support_matrix, ["--json"])
            assert result.exit_code == 0
            payload = json.loads(result.output)
            assert "sourceCommit" in payload
            assert payload["sourceCommit"] is None
            assert payload["protocols"]
        finally:
            sm._git_dir = original


class TestWorktreeDirtyProvenance:
    """`sourceCommit` alone over-claims on a dirty checkout.

    A matrix generated with uncommitted connector or registry edits reflects
    those edits but still reports HEAD, so a vendored artifact would name a
    commit that cannot reproduce its contents — the exact drift the provenance
    envelope exists to expose.
    """

    def _git(self, *args: str, cwd: Path) -> None:
        import subprocess

        subprocess.run(
            ["git", *args], cwd=str(cwd), capture_output=True, check=True, timeout=30
        )

    def _repo(self, tmp_path: Path) -> Path:
        tmp_path.mkdir(parents=True, exist_ok=True)
        self._git("init", "-q", cwd=tmp_path)
        self._git("config", "user.email", "t@example.com", cwd=tmp_path)
        self._git("config", "user.name", "t", cwd=tmp_path)
        (tmp_path / "f.txt").write_text("a\n", encoding="utf-8")
        self._git("add", "f.txt", cwd=tmp_path)
        self._git("commit", "-q", "-m", "init", cwd=tmp_path)
        return tmp_path

    def test_clean_checkout_is_not_dirty(self, tmp_path: Path, monkeypatch) -> None:
        import almanak.framework.cli.support_matrix as sm

        repo = self._repo(tmp_path)
        monkeypatch.setattr(sm, "_checkout_root", lambda *a, **k: repo)
        assert sm._worktree_dirty() is False

    def test_linked_worktree_is_measured_not_skipped(self, tmp_path: Path) -> None:
        """A linked worktree must report real dirtiness, not None.

        `.git` is a FILE there and the git dir resolves to
        `<main>/.git/worktrees/<name>`, so deriving the working tree from the
        git dir lands INSIDE `.git` and git status reports nothing usable. This
        repo runs much of its work in worktrees, so that blind spot would hide
        dirtiness exactly where it matters most.
        """
        import almanak.framework.cli.support_matrix as sm

        main = self._repo(tmp_path / "main")
        linked = tmp_path / "linked"
        self._git("worktree", "add", "-q", str(linked), "HEAD", cwd=main)
        assert (linked / ".git").is_file(), "precondition: linked worktree uses a .git FILE"

        root = sm._checkout_root(linked / "pkg" / "mod.py")
        assert root == linked, root

        import unittest.mock as _mock

        with _mock.patch.object(sm, "_checkout_root", lambda *a, **k: linked):
            assert sm._worktree_dirty() is False
            (linked / "f.txt").write_text("changed\n", encoding="utf-8")
            assert sm._worktree_dirty() is True

    def test_uncommitted_change_is_dirty(self, tmp_path: Path, monkeypatch) -> None:
        import almanak.framework.cli.support_matrix as sm

        repo = self._repo(tmp_path)
        (repo / "f.txt").write_text("changed\n", encoding="utf-8")
        monkeypatch.setattr(sm, "_checkout_root", lambda *a, **k: repo)
        assert sm._worktree_dirty() is True

    def test_untracked_file_is_dirty(self, tmp_path: Path, monkeypatch) -> None:
        """An untracked connector file changes the matrix as surely as an edit."""
        import almanak.framework.cli.support_matrix as sm

        repo = self._repo(tmp_path)
        (repo / "new.txt").write_text("x\n", encoding="utf-8")
        monkeypatch.setattr(sm, "_checkout_root", lambda *a, **k: repo)
        assert sm._worktree_dirty() is True

    def test_no_git_dir_is_none_not_false(self, tmp_path: Path, monkeypatch) -> None:
        """Undeterminable must not read as 'clean' — that would be a guess."""
        import almanak.framework.cli.support_matrix as sm

        monkeypatch.setattr(sm, "_checkout_root", lambda *a, **k: None)
        assert sm._worktree_dirty() is None

    def test_git_failure_is_none_not_false(self, monkeypatch) -> None:
        import subprocess

        import almanak.framework.cli.support_matrix as sm

        def boom(*a, **k):
            raise OSError("git not found")

        monkeypatch.setattr(subprocess, "run", boom)
        assert sm._worktree_dirty() is None

    def test_cli_publishes_the_flag(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(support_matrix, ["--json"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert "sourceDirty" in payload
        assert payload["sourceDirty"] in (True, False, None)


class TestProvenanceSurvivesMalformedEncoding:
    """`UnicodeDecodeError` is not an `OSError` subclass.

    Catching `OSError` alone let a `HEAD`, loose-ref or `packed-refs` file with
    malformed encoding crash `almanak info matrix --json` outright — the exact
    opposite of the "provenance degrades, never fails" contract, and a hard
    failure of the whole catalogue over an unreadable annotation.
    """

    def _repo(self, tmp_path: Path, head: bytes) -> Path:
        git = tmp_path / ".git"
        git.mkdir()
        (git / "HEAD").write_bytes(head)
        return git

    def test_undecodable_head_is_none_not_a_crash(self, tmp_path: Path) -> None:
        self._repo(tmp_path, b"\xff\xfe\x00invalid")
        assert _source_commit(tmp_path / "mod.py") is None

    def test_undecodable_loose_ref_is_none_not_a_crash(self, tmp_path: Path) -> None:
        git = self._repo(tmp_path, b"ref: refs/heads/main\n")
        loose = git / "refs" / "heads" / "main"
        loose.parent.mkdir(parents=True)
        loose.write_bytes(b"\xff\xfe\x00")
        assert _source_commit(tmp_path / "mod.py") is None

    def test_undecodable_packed_refs_is_none_not_a_crash(self, tmp_path: Path) -> None:
        git = self._repo(tmp_path, b"ref: refs/heads/main\n")
        (git / "packed-refs").write_bytes(b"\xff\xfe\x00 refs/heads/main\n")
        assert _source_commit(tmp_path / "mod.py") is None

    def test_undecodable_git_pointer_file_is_none_not_a_crash(self, tmp_path: Path) -> None:
        """`_git_dir` is reached by `_worktree_dirty` outside any try."""
        (tmp_path / ".git").write_bytes(b"\xff\xfe\x00")
        assert _git_dir(tmp_path / "mod.py") is None

    def test_cli_still_emits_a_matrix_when_provenance_is_undecodable(
        self, tmp_path: Path, cli_runner: CliRunner
    ) -> None:
        import almanak.framework.cli.support_matrix as sm

        self._repo(tmp_path, b"\xff\xfe\x00invalid")
        original = sm._git_dir
        sm._git_dir = lambda *a, **k: tmp_path / ".git"
        try:
            result = cli_runner.invoke(support_matrix, ["--json"])
            assert result.exit_code == 0, result.output[:400]
            payload = json.loads(result.output)
            assert payload["sourceCommit"] is None
            assert payload["protocols"], "the catalogue must survive bad provenance"
        finally:
            sm._git_dir = original


class TestProvenanceRefusesAnUnrelatedRepository:
    """The installed-SDK layout: `almanak` lives inside the USER's repo.

    `<user-repo>/.venv/lib/pythonX/site-packages/almanak/framework/cli/...` — so
    an unanchored upward walk finds the USER'S `.git` and provenance reports a
    confidently-wrong 40-hex SHA describing an unrelated project, with
    `sourceDirty` reporting their working tree and `git status` executing inside
    their repository.

    That is worse than reporting nothing: a drift check would compare two
    unrelated repositories and report match or mismatch with equal
    meaninglessness, while `_valid_sha` would happily pass the value along.
    """

    def _user_repo_with_vendored_sdk(self, tmp_path: Path) -> Path:
        import subprocess

        repo = tmp_path / "userrepo"
        pkg = repo / ".venv" / "lib" / "python3.12" / "site-packages" / "almanak" / "framework" / "cli"
        pkg.mkdir(parents=True)
        real = Path(__import__("almanak.framework.cli.support_matrix", fromlist=["x"]).__file__)
        (pkg / "support_matrix.py").write_text(real.read_text(encoding="utf-8"), encoding="utf-8")
        for args in (
            ["init", "-q"],
            ["config", "user.email", "t@example.com"],
            ["config", "user.name", "t"],
        ):
            subprocess.run(["git", *args], cwd=str(repo), capture_output=True, check=True, timeout=30)
        (repo / "strategy.py").write_text("x = 1\n", encoding="utf-8")
        subprocess.run(["git", "add", "-A"], cwd=str(repo), capture_output=True, check=True, timeout=30)
        subprocess.run(
            ["git", "commit", "-q", "-m", "user work"],
            cwd=str(repo), capture_output=True, check=True, timeout=30,
        )
        return pkg / "support_matrix.py"

    def _load(self, module_file: Path):
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "almanak.framework.cli.support_matrix", module_file
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_does_not_report_the_users_repo_as_provenance(self, tmp_path: Path) -> None:
        mod = self._load(self._user_repo_with_vendored_sdk(tmp_path))
        assert mod._git_dir() is None
        assert mod._checkout_root() is None
        assert mod._source_commit() is None, "must not publish the user's commit"
        assert mod._worktree_dirty() is None, "must not report the user's working tree"

    def test_real_sdk_checkout_still_resolves(self) -> None:
        """The anchor must not break the case provenance exists for.

        Skipped when the suite runs against an installed package rather than a
        checkout (e.g. inside the nightly image), where there is legitimately no
        provenance to resolve.
        """
        import subprocess

        import almanak.framework.cli.support_matrix as sm

        if sm._checkout_root() is None:
            pytest.skip("not running from an SDK checkout; provenance is N/A")
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, timeout=30
        ).stdout.strip()
        assert sm._source_commit() == head


class TestEveryIntentIsClassified:
    """The partition that matters is over INTENTS, not categories.

    An intent absent from the category map yielded `None`, which is not in
    WITHHELD_CATEGORIES, so it was published into `chainsByIntent` having never
    passed the product/withheld decision — hyperliquid published
    `PERP_WITHDRAW` that way. The old guard iterated the map's VALUES, so it
    structurally could not see an intent that was never added to the map, and
    adding an IntentType failed no test.
    """

    def test_every_intent_type_is_classified_exactly_once(self) -> None:
        from almanak.framework.cli.support_matrix import UNCATEGORISED_INTENTS
        from almanak.framework.intents.vocabulary import IntentType

        categorised = {i.name for i in _intent_category_map()}
        overlap = categorised & UNCATEGORISED_INTENTS
        assert not overlap, f"classified twice: {sorted(overlap)}"
        stale = UNCATEGORISED_INTENTS - {i.name for i in IntentType}
        assert not stale, (
            f"UNCATEGORISED_INTENTS names verbs that no longer exist: {sorted(stale)}. "
            "A renamed or removed IntentType leaves a dead entry that silently "
            "widens the set."
        )
        missing = {i.name for i in IntentType} - categorised - UNCATEGORISED_INTENTS
        assert not missing, (
            f"unclassified IntentType members: {sorted(missing)}. Add each to the "
            "category map (to render a row) or to UNCATEGORISED_INTENTS (to publish "
            "coverage with no row). Leaving a verb unclassified publishes it to "
            "consumers without a product decision."
        )

    def test_an_unclassified_verb_fails_closed(self, monkeypatch) -> None:
        """Removing a verb from both sets must RAISE, not silently publish."""
        import almanak.framework.cli.support_matrix as sm

        monkeypatch.setattr(
            sm, "UNCATEGORISED_INTENTS", sm.UNCATEGORISED_INTENTS - {"PERP_WITHDRAW"}
        )
        with pytest.raises(RuntimeError, match="not classified for the support matrix"):
            sm._build_matrix()

    def test_uncategorised_verbs_are_still_published(self, matrix_data: dict) -> None:
        """They are real capabilities; the vocabulary just cannot draw a row."""
        row = [p for p in matrix_data["protocols"] if p["name"] == "hyperliquid"][0]
        assert "PERP_WITHDRAW" in row["chainsByIntent"]


class TestChainFilterAcceptsAliases:
    def test_registered_alias_resolves(self, cli_runner: CliRunner) -> None:
        """`bnb` is a registered alias for `bsc`, which is fully supported.

        Rows are spelled canonically, so comparing a raw lowercased string
        reported "no support" for a supported chain.
        """
        result = cli_runner.invoke(support_matrix, ["--json", "--chain", "bnb"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["chains"] == ["bsc"]
        assert payload["protocols"], "bsc has supported protocols"

    def test_json_emits_a_valid_envelope_when_nothing_matches(
        self, cli_runner: CliRunner
    ) -> None:
        """Zero bytes on stdout made `jq` fail and could not be distinguished
        from a broken command."""
        result = cli_runner.invoke(support_matrix, ["--json", "--chain", "nosuchchain"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["protocols"] == []
        assert payload["schemaVersion"] == SCHEMA_VERSION

    def test_table_path_still_explains_itself(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(support_matrix, ["--chain", "nosuchchain"])
        assert result.exit_code == 0
        assert "No protocols match" in result.output


class TestOffChainVenuesAreNotMarkedKnown:
    """An off-chain venue must still RENDER, but must not claim coverage.

    The first version of this test iterated real manifests and asserted only
    inside `if proto["name"] in off_chain`. Kraken — the only `chains is None`
    manifest — declares no `matrix_entries` and therefore emits no row, so the
    body never executed and the test asserted NOTHING while reading as coverage.
    It would not have caught the regression it was written to prevent (a guard
    that suppressed the row entirely). It now injects the shape under test.
    """

    def _off_chain_manifest(self):
        from almanak.connectors._strategy_base.registry import (
            ConnectorManifest,
            MatrixEntry,
        )
        from almanak.framework.intents.vocabulary import IntentType

        return ConnectorManifest(
            name="uat_offvenue",
            intents=(IntentType.SWAP,),
            chains=None,  # off-chain venue, e.g. a CEX
            matrix_entries=(
                MatrixEntry(
                    matrix_name="uat_offvenue",
                    category="swap",
                    chains=frozenset({"ethereum"}),
                ),
            ),
        )

    def test_off_chain_row_is_published_but_not_marked_known(self) -> None:
        from almanak.connectors._strategy_base.registry import (
            ConnectorRegistry,
            _import_all_connectors,
        )

        _import_all_connectors()
        ConnectorRegistry._entries["uat_offvenue"] = self._off_chain_manifest()
        try:
            rows = [p for p in _build_matrix()["protocols"] if p["name"] == "uat_offvenue"]
            assert rows, (
                "the declared row must still render — suppressing it entirely makes "
                "check_protocol_support report 'not integrated in the SDK', a worse "
                "demotion than the empty map this guard replaced"
            )
            row = rows[0]
            assert row["intentsKnown"] is False, "off-chain coverage is not expressible"
            assert row["chainsByIntent"] == {}
            assert row["chains"] == ["ethereum"], "the declared chains still render"
        finally:
            ConnectorRegistry._entries.pop("uat_offvenue", None)

    def test_real_off_chain_manifests_never_claim_coverage(self) -> None:
        """Whatever rows the real off-chain venues emit, none may claim known."""
        from almanak.connectors._strategy_base.registry import (
            ConnectorRegistry,
            _import_all_connectors,
        )

        _import_all_connectors()
        off_chain = {m.name for m in ConnectorRegistry.all() if m.chains is None}
        assert off_chain, "precondition: at least one off-chain venue (e.g. kraken)"
        for proto in _build_matrix()["protocols"]:
            if proto["name"] in off_chain:
                assert proto["intentsKnown"] is False, proto["name"]


class TestRowsDoNotAdvertiseACategoryTheyCannotServe:
    """A Phase-B row must not contradict the connector's own declaration.

    VIB-6231. ``camelot`` declares ``strategy_intents=("SWAP",)`` and its
    compiler answers ``CamelotCompiler does not support intent type
    IntentType.LP_OPEN``. But its address in ``LP_POSITION_MANAGERS`` minted an
    ``lp`` row, so the matrix advertised an LP venue that cannot do LP.
    ``chainsByIntent`` was already honest (``{"SWAP": [...]}`` on both rows) --
    only a v1 consumer reading ``category`` was misled.
    """

    def test_camelot_publishes_no_lp_row(self) -> None:
        from almanak.framework.cli.support_matrix import _build_matrix

        rows = [(p["name"], p["category"]) for p in _build_matrix()["protocols"]]
        assert ("camelot", "lp") not in rows
        # Guard the assertion: camelot must still publish its real swap row, or
        # this would pass by camelot having vanished entirely.
        assert ("camelot", "swap") in rows

    def test_no_row_advertises_an_unservable_category(self) -> None:
        """The general invariant, not just the one row that motivated it."""
        from almanak.framework.cli.support_matrix import (
            _build_matrix,
            _category_is_served_by_coverage,
        )

        offenders = [
            (p["name"], p["category"], sorted(p["chainsByIntent"]))
            for p in _build_matrix()["protocols"]
            if p["intentsKnown"]
            and p["chainsByIntent"]
            and not _category_is_served_by_coverage(p["category"], p["chainsByIntent"])
        ]
        # Declared ``matrix_entries`` rows are deliberate rendering overrides
        # (enso renders SWAP as ``aggregator``), so they are expected here and
        # are excluded from the suppression rule by design.
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        declared = {
            (entry.matrix_name, entry.category)
            for connector in CONNECTOR_REGISTRY.all()
            for entry in connector.strategy_matrix_entries or ()
        }
        undeclared = [o for o in offenders if (o[0], o[1]) not in declared]
        assert undeclared == [], f"rows advertising a category their intents cannot serve: {undeclared}"

    def test_declared_rendering_overrides_survive(self) -> None:
        """Negative control: the suppression must not eat declared rows.

        ``enso`` declares ``category="aggregator"`` while its only intent is
        SWAP (category ``swap``). An unscoped version of this rule deleted the
        enso and lifi aggregator rows outright.
        """
        from almanak.framework.cli.support_matrix import _build_matrix

        rows = [(p["name"], p["category"]) for p in _build_matrix()["protocols"]]
        assert ("enso", "aggregator") in rows
        assert ("lifi", "aggregator") in rows

    def test_routing_only_rows_are_untouched(self) -> None:
        """Rows no manifest describes keep publishing -- absent != unsupported.

        ``intentsKnown=False`` rows have no coverage to contradict, so the
        suppression must never reach them.
        """
        from almanak.framework.cli.support_matrix import _build_matrix

        unknown = [p for p in _build_matrix()["protocols"] if not p["intentsKnown"]]
        assert unknown, "expected some routing-table-only rows; the guard would be vacuous without them"
        assert all(p["chains"] for p in unknown)
