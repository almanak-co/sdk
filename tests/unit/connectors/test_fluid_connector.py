"""Registry/manifest integration tests for the Fluid connector (Phase 1).

Per-module behaviour is covered in ``tests/unit/connectors/fluid/``; this
file pins what the rest of the framework sees: the manifest surface and
the registries fluid must appear in.
"""

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors.fluid.connector import CONNECTOR
from almanak.connectors.fluid.receipt_parser import FluidReceiptParser


class TestFluidManifest:
    def test_swap_only_intents(self):
        # LP intents were removed in Phase 1 (VIB-5029): direct pool LP is
        # whitelist-gated on-chain (VIB-5028 §V4); LP returns in Phase 4.
        assert CONNECTOR.strategy_intents == ("SWAP",)

    def test_four_chains(self):
        assert CONNECTOR.strategy_chains == ("arbitrum", "base", "ethereum", "polygon")

    def test_kind_is_swap(self):
        assert CONNECTOR.kind is ProtocolKind.SWAP

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
