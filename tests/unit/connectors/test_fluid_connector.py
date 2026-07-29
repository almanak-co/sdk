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
        """Advertised chain scope per category — now DERIVED, not hand-written.

        Matrix schema v2: the hand-written ``strategy_matrix_entries`` override
        scoped only the RENDERED row, while ``chains_for_intent(SUPPLY)`` — the
        accessor consumers are told to ask — still answered all four chains.
        Publishing per-intent coverage would have shipped that split as a live
        over-advertisement, so the narrowing moved to
        ``strategy_intent_chain_exclusions`` and the rows derive from it.

        Asserted on the rendered matrix rather than the declaration, so this
        keeps pinning the same observable fact the override used to pin.
        """
        from almanak.framework.cli.support_matrix import _build_matrix

        assert CONNECTOR.strategy_matrix_entries is None
        rows = {
            p["category"]: p["chains"] for p in _build_matrix()["protocols"] if p["name"] == "fluid"
        }
        assert rows["swap"] == ["arbitrum", "base", "ethereum", "polygon"]
        # Lending scoped to the Phase-0-validated chains (VIB-5030).
        assert rows["lending"] == ["arbitrum", "base"]

    def test_lending_exclusions_pin_the_narrowing(self):
        """The exclusions are now the single source of the lending narrowing."""
        excluded = {
            (x.intent, tuple(sorted(x.chains))) for x in CONNECTOR.strategy_intent_chain_exclusions
        }
        assert excluded == {
            ("SUPPLY", ("ethereum", "polygon")),
            ("WITHDRAW", ("ethereum", "polygon")),
        }
        # SUPPLY and WITHDRAW must be excluded TOGETHER: advertising a withdraw
        # on a chain that cannot open a position implies a redeemable position
        # that cannot exist.
        assert {x.intent for x in CONNECTOR.strategy_intent_chain_exclusions} == {
            "SUPPLY",
            "WITHDRAW",
        }

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
        # places — the compiler gate, the manifest's advertised lending scope,
        # the valuation market table, and the permission-hints synthetic gate.
        # They are hand-maintained copies today (single-source derivation is a
        # follow-up); this pin prevents silent drift, where e.g. a chain
        # compiles supplies that valuation cannot mark or discovery never
        # authorises.
        #
        # The manifest copy used to be the hand-written ``matrix_entries``
        # lending row; under matrix schema v2 it is ``chains_for_intent(SUPPLY)``
        # — the accessor consumers actually read, and the one that was silently
        # DISAGREEING with the other three before the exclusions landed. Reading
        # the row would now pass while the published contract over-advertised.
        from almanak.connectors._strategy_base.registry import (
            ConnectorRegistry,
            _import_all_connectors,
        )
        from almanak.connectors.fluid.compiler import FluidCompiler
        from almanak.connectors.fluid.lending_read import FLUID_FTOKEN_MARKETS
        from almanak.connectors.fluid.permission_hints import _LENDING_CHAINS
        from almanak.framework.intents.vocabulary import IntentType

        # Connectors register lazily on first attribute access.
        _import_all_connectors()
        manifest = ConnectorRegistry.get("fluid")
        assert manifest is not None, "fluid must be registered"
        manifest_lending_chains = frozenset(manifest.chains_for_intent(IntentType.SUPPLY))
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
