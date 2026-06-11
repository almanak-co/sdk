"""Registry/manifest integration tests for the Fluid connector (Phase 1).

Per-module behaviour is covered in ``tests/unit/connectors/fluid/``; this
file pins what the rest of the framework sees: the manifest surface and
the registries fluid must appear in.
"""

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors.fluid.connector import CONNECTOR
from almanak.connectors.fluid.receipt_parser import FluidReceiptParser


class TestFluidManifest:
    def test_swap_and_lending_intents(self):
        # LP intents stay removed (Phase 1, VIB-5029): direct pool LP is
        # whitelist-gated on-chain (VIB-5028 §V4); LP returns in Phase 4.
        # SUPPLY/WITHDRAW are the Phase-2 fToken lending surface (VIB-5030).
        assert CONNECTOR.strategy_intents == ("SWAP", "SUPPLY", "WITHDRAW")

    def test_matrix_chains(self):
        entries = {e.category: e.chains for e in CONNECTOR.strategy_matrix_entries}
        assert entries["swap"] == frozenset(("arbitrum", "base", "ethereum", "polygon"))
        # Lending scoped to the Phase-0-validated chains (VIB-5030).
        assert entries["lending"] == frozenset(("arbitrum", "base"))

    def test_fluid_lending_alias(self):
        # The platform spec emits protocol="fluid_lending" — must resolve to
        # this connector both via the manifest alias and the global registry.
        assert "fluid_lending" in CONNECTOR.aliases
        from almanak.connectors._strategy_base.protocol_aliases import normalize_protocol

        assert normalize_protocol("base", "fluid_lending") == "fluid"
        assert normalize_protocol("arbitrum", "fluid_lending") == "fluid"

    def test_kind_is_swap(self):
        assert CONNECTOR.kind is ProtocolKind.SWAP

    def test_lending_chain_sets_in_sync(self):
        # pr-auditor 2026-06-11: the lending chain universe lives in FOUR
        # places — the compiler gate, the manifest matrix row, the valuation
        # market table, and the permission-hints synthetic gate. They are
        # hand-maintained copies today (single-source derivation is a
        # follow-up); this pin prevents silent drift, where e.g. a chain
        # compiles supplies that valuation cannot mark or discovery never
        # authorises.
        from almanak.connectors.fluid.compiler import FluidCompiler
        from almanak.connectors.fluid.lending_read import FLUID_FTOKEN_MARKETS
        from almanak.connectors.fluid.permission_hints import _LENDING_CHAINS

        manifest_lending_chains = next(
            e.chains for e in CONNECTOR.strategy_matrix_entries if e.category == "lending"
        )
        assert FluidCompiler.LENDING_CHAINS == manifest_lending_chains
        assert FluidCompiler.LENDING_CHAINS == frozenset(FLUID_FTOKEN_MARKETS.keys())
        assert FluidCompiler.LENDING_CHAINS == _LENDING_CHAINS

    def test_swap_quote_connector_declared(self):
        ref = CONNECTOR.swap_quote_connector
        assert ref is not None
        assert ref.attribute == "FluidSwapQuoteConnector"

    def test_compiler_declared(self):
        assert CONNECTOR.compiler.attribute == "FluidCompiler"


class TestFluidRegistries:
    def test_receipt_registry_has_fluid(self):
        from almanak.framework.execution.receipt_registry import ReceiptParserRegistry

        registry = ReceiptParserRegistry()
        parser = registry.get("fluid")
        assert parser is not None
        assert isinstance(parser, FluidReceiptParser)

    def test_fluid_not_in_lp_position_managers(self):
        # Phase 1 removed the LP_POSITION_MANAGER role with the LP intents —
        # fluid is routerless/SWAP-only and maps to no framework role table.
        from almanak.framework.intents.compiler import LP_POSITION_MANAGERS

        for chain_managers in LP_POSITION_MANAGERS.values():
            assert "fluid" not in chain_managers

    def test_synthetic_membership_swap_only(self):
        from almanak.framework.permissions.synthetic_intents import (
            _lp_protocols,
            _swap_protocols,
        )

        assert "fluid" in _swap_protocols()
        assert "fluid" not in _lp_protocols()
