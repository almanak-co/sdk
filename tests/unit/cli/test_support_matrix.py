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
