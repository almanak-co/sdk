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

from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.chains.polygon import DESCRIPTOR as POLYGON
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
        assert actual_chains == expected_chains, f"Matrix has {actual_chains} but adapter has {expected_chains}"


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
    def test_connector_present(self, matrix_data: dict, name: str, category: str, expected_chains: set[str]) -> None:
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
    def test_deregistered_connector_absent(self, matrix_data: dict, name: str, category: str) -> None:
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

    Tests inject a synthetic connector descriptor into the descriptor query,
    rebuild the matrix, and assert its rows surface without central edits.

    Matrix metadata is published via descriptor-owned
    :class:`StrategyMatrixEntry` rather than a gateway-side capability: the matrix
    CLI is a strategy-container module and the strategy-side import
    boundary (``tests/static/test_strategy_import_boundary.py``) forbids
    reading anything under ``almanak.connectors._base.gateway_*``. See
    the ``support_matrix`` module docstring for the architectural call.
    """

    def test_matrix_entries_picks_up_new_connector(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A descriptor with one classification row produces one matrix row.

        Its chains derive from the row's listed intent.
        Mirrors the simple-case dispatch every connector goes through.
        """
        from almanak.connectors._base.types import ProtocolKind
        from almanak.connectors._connector import (
            CONNECTOR_REGISTRY,
            Connector,
            StrategyMatrixEntry,
            SupportedChainsSpec,
        )

        connector = Connector(
            name="vib_4856_mock_swap",
            kind=ProtocolKind.SWAP,
            strategy_intents=("SWAP",),
            supported_chains=SupportedChainsSpec(chains=(ETHEREUM, ARBITRUM)),
            strategy_matrix_entries=(
                StrategyMatrixEntry(
                    matrix_name="vib_4856_mock_swap",
                    category="swap",
                    intents=("SWAP",),
                ),
            ),
        )
        monkeypatch.setattr(CONNECTOR_REGISTRY, "with_strategy_support", lambda: (connector,))
        data = _build_matrix()
        mock_rows = [p for p in data["protocols"] if p["name"] == "vib_4856_mock_swap"]
        assert len(mock_rows) == 1, f"mock connector should produce one matrix row, got {mock_rows!r}"
        assert mock_rows[0]["category"] == "swap"
        assert set(mock_rows[0]["chains"]) == {"ethereum", "arbitrum"}

    def test_matrix_entries_multi_row_connector(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """One connector can publish multiple classification rows
        rows under different ``matrix_name``\\ s — the mechanism Aerodrome's
        slipstream alias uses in production.
        """
        from almanak.connectors._base.types import ProtocolKind
        from almanak.connectors._connector import (
            CONNECTOR_REGISTRY,
            Connector,
            StrategyMatrixEntry,
            SupportedChainsSpec,
        )

        connector = Connector(
            name="vib_4856_mock_multi",
            kind=ProtocolKind.LP,
            strategy_intents=("LP_OPEN", "LP_CLOSE"),
            supported_chains=SupportedChainsSpec(chains=(ETHEREUM, BASE)),
            strategy_matrix_entries=(
                StrategyMatrixEntry(
                    matrix_name="vib_4856_mock_multi",
                    category="lp",
                    intents=("LP_OPEN",),
                ),
                StrategyMatrixEntry(
                    matrix_name="vib_4856_mock_multi_alias",
                    category="lp",
                    intents=("LP_CLOSE",),
                ),
            ),
        )
        monkeypatch.setattr(CONNECTOR_REGISTRY, "with_strategy_support", lambda: (connector,))
        data = _build_matrix()
        names = {p["name"] for p in data["protocols"]}
        assert "vib_4856_mock_multi" in names
        assert "vib_4856_mock_multi_alias" in names
        alias_rows = [p for p in data["protocols"] if p["name"] == "vib_4856_mock_multi_alias"]
        assert set(alias_rows[0]["chains"]) == {"base", "ethereum"}

    def test_descriptor_matrix_entries_picks_up_new_connector(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A descriptor classification row groups exact intent-chain support.

        This is used by
        connectors without a gateway-side provider (e.g. LiFi).
        """
        from almanak.connectors._base.types import ProtocolKind
        from almanak.connectors._connector import (
            CONNECTOR_REGISTRY,
            Connector,
            StrategyMatrixEntry,
            SupportedChainsSpec,
        )

        connector = Connector(
            name="vib_4856_mock_strategy",
            kind=ProtocolKind.SWAP,
            strategy_intents=("SWAP",),
            supported_chains=SupportedChainsSpec(chains=(ETHEREUM, POLYGON)),
            strategy_matrix_entries=(
                StrategyMatrixEntry(
                    matrix_name="vib_4856_mock_strategy",
                    category="aggregator",
                    intents=("SWAP",),
                ),
            ),
        )
        monkeypatch.setattr(CONNECTOR_REGISTRY, "with_strategy_support", lambda: (connector,))
        data = _build_matrix()
        rows = [p for p in data["protocols"] if p["name"] == "vib_4856_mock_strategy"]
        assert len(rows) == 1
        assert rows[0]["category"] == "aggregator"
        assert set(rows[0]["chains"]) == {"ethereum", "polygon"}

    def test_descriptor_empty_matrix_entries_suppresses_derivation(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A connector that declares ``strategy_matrix_entries=()``
        gateway side is authoritative) does NOT produce a derived row.
        Guards intentional matrix suppression.
        """
        from almanak.connectors._base.types import ProtocolKind
        from almanak.connectors._connector import (
            CONNECTOR_REGISTRY,
            Connector,
            SupportedChainsSpec,
        )

        connector = Connector(
            name="vib_4856_mock_suppressed",
            kind=ProtocolKind.LENDING,
            strategy_intents=("SUPPLY", "BORROW"),
            supported_chains=SupportedChainsSpec(chains=(ETHEREUM,)),
            strategy_matrix_entries=(),
        )
        monkeypatch.setattr(CONNECTOR_REGISTRY, "with_strategy_support", lambda: (connector,))
        data = _build_matrix()
        rows = [p for p in data["protocols"] if p["name"] == "vib_4856_mock_suppressed"]
        assert rows == [], "strategy_matrix_entries=() must suppress derivation"

    def test_compiler_fallback_cannot_cross_widen_intent_overrides(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A resolvable connector remains bounded by exact intent support."""
        from almanak.connectors._base.types import ProtocolKind
        from almanak.connectors._connector import CONNECTOR_REGISTRY, Connector, SupportedChainsSpec
        from almanak.framework.cli.support_matrix import _collect_from_compiler_tables
        from almanak.framework.intents import compiler_constants

        connector = Connector(
            name="fallback_exact",
            kind=ProtocolKind.LP,
            strategy_intents=("SWAP", "LP_OPEN"),
            supported_chains=SupportedChainsSpec(
                chains=(BASE,),
                intent_overrides={"SWAP": (ETHEREUM,)},
            ),
        )
        monkeypatch.setattr(
            CONNECTOR_REGISTRY,
            "get",
            lambda name: connector if name == "fallback_exact" else None,
        )
        monkeypatch.setattr(
            compiler_constants,
            "PROTOCOL_ROUTERS",
            {"base": {"fallback_exact"}, "ethereum": {"fallback_exact"}},
        )
        monkeypatch.setattr(
            compiler_constants,
            "LP_POSITION_MANAGERS",
            {
                "base": {"fallback_exact": "0x1"},
                "ethereum": {"fallback_exact": "0x2"},
            },
        )
        monkeypatch.setattr(compiler_constants, "BALANCER_VAULT_ADDRESSES", {})

        entries: dict[tuple[str, str], set[str]] = {}
        _collect_from_compiler_tables(entries, authoritative=set())

        assert entries[("fallback_exact", "swap")] == {"ethereum"}
        assert entries[("fallback_exact", "lp")] == {"base"}



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
    the truth moved to ``SupportedChainsSpec.intent_overrides``.

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
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        for proto in matrix_data["protocols"]:
            connector = CONNECTOR_REGISTRY.get(proto["name"])
            if connector is None or connector.supported_chains is None:
                continue
            declared = set(connector.supported_chains_for_protocol(proto["name"]))
            for intent, chains in proto["chainsByIntent"].items():
                assert set(chains) <= declared, f"{proto['name']}/{intent} outran supported_chains"

    def test_descriptor_row_cannot_be_widened_by_compiler_tables(self, matrix_data: dict) -> None:
        """A connector-backed row is bounded by its unified declaration."""
        row = _row(matrix_data, "enso", "aggregator")
        assert set(row["chains"]) == set(row["chainsByIntent"]["SWAP"])

    def test_intent_override_propagates(self, matrix_data: dict) -> None:
        """VIB-6111: Aave BORROW omits Mantle while SUPPLY keeps it."""
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

    @pytest.mark.parametrize("protocol", ["velodrome"])
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
        for protocol in ("fluid", "pendle", "gmx_v2", "aave_v3", "agni_finance", "aerodrome_slipstream"):
            for row in _rows_named(matrix_data, protocol):
                assert row["intentsKnown"] is True

    def test_agni_alias_coverage_is_mantle_only(self, matrix_data: dict) -> None:
        row = _row(matrix_data, "agni_finance", "swap")
        assert row["chainsByIntent"]["SWAP"] == ["mantle"]

    def test_known_rows_carry_coverage(self, matrix_data: dict) -> None:
        """``intentsKnown`` must not be true-but-empty for a rendered row."""
        for proto in matrix_data["protocols"]:
            if proto["intentsKnown"]:
                assert proto["chainsByIntent"], proto["name"]

    def test_no_two_connectors_claim_the_same_matrix_name(self) -> None:
        """Coverage is merged by matrix_name — a collision would blend two
        connectors' intents into one row without anyone noticing."""
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        seen: dict[str, str] = {}
        for manifest in CONNECTOR_REGISTRY.with_strategy_support():
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
        """A chain filter retains only intents with exact descriptor coverage.

        Fluid swaps on Ethereum but its lending intents do not. Compiler tables
        cannot reintroduce the lending row or widen those intent cells.
        """
        result = cli_runner.invoke(support_matrix, ["--json", "--chain", "ethereum"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        rows = [p for p in payload["protocols"] if p["name"] == "fluid"]
        assert len(rows) == 1
        assert rows[0]["category"] == "swap"
        assert rows[0]["chainsByIntent"] == {"SWAP": ["ethereum"]}


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

        checkout_root = sm._checkout_root()
        if checkout_root is None:
            pytest.skip("not running from an SDK checkout; provenance is N/A")
        # Anchor on the resolved checkout, not the process cwd: sibling tests in
        # this directory ``monkeypatch.chdir`` into tmp_path, and under
        # random ordering this ran ``git rev-parse`` inside one of those temp
        # git repos and compared its HEAD against the SDK's.
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(checkout_root),
            capture_output=True,
            text=True,
            timeout=30,
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


class TestOffChainVenuesAreNotPublishedAsOnChainSupport:
    """An explicit off-chain declaration contributes no on-chain matrix row."""

    def test_real_off_chain_connectors_emit_no_rows(self) -> None:
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        off_chain = {
            connector.name
            for connector in CONNECTOR_REGISTRY.with_strategy_support()
            if connector.supported_chains is not None and connector.supported_chains.is_offchain
        }
        assert off_chain, "precondition: at least one off-chain venue (e.g. kraken)"
        published = {row["name"] for row in _build_matrix()["protocols"]}
        assert not (off_chain & published)


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
