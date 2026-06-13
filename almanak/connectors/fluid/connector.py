"""Fluid connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    LendingReadDecl,
    MetadataAmountEncoding,
    StrategyMatrixEntry,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="fluid",
    external_ids={"defillama": "fluid-dex"},
    # SWAP (Phase 1, VIB-5029, 4 chains) + fToken lending SUPPLY/WITHDRAW
    # (Phase 2, VIB-5030, arbitrum+base). Fluid's LP surface is
    # whitelist-gated on-chain (Phase-0 finding, VIB-5028 §V4) and ships
    # later via SmartLending / smart vaults (VIB-5032); vault borrow is
    # VIB-5031. ``kind`` stays SWAP (primary surface); the strategy matrix
    # rows below scope lending to its validated chains.
    kind=ProtocolKind.SWAP,
    # The platform spec emits ``protocol: "fluid_lending"`` for fToken
    # supply strategies — same connector, alias resolved at compile ingress.
    aliases=("fluid_lending",),
    address_tables=(
        AddressTableSpec(
            protocol="fluid",
            module="almanak.connectors.fluid.addresses",
            attribute="FLUID",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.fluid.gateway.provider",
        attribute="FluidGatewayConnector",
        order=4,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.fluid.receipt_parser_provider",
        attribute="FluidReceiptParserConnector",
    ),
    swap_quote_connector=ImportRef(
        module="almanak.connectors.fluid.swap_quote_provider",
        attribute="FluidSwapQuoteConnector",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.fluid.contract_roles",
        attribute="CONTRACT_ROLES",
        order=8,
    ),
    compiler=ImportRef(
        module="almanak.connectors.fluid.compiler",
        attribute="FluidCompiler",
    ),
    # fToken aggregate account-state read (VIB-5030): market-scoped on the
    # per-underlying fToken; powers lending pre/post-state capture
    # (confidence=HIGH) and valuation. Compound V3 / Silo V2 shape.
    lending_read=LendingReadDecl(
        account_state=ImportRef(
            module="almanak.connectors.fluid.lending_read",
            attribute="ACCOUNT_STATE_READ_SPEC",
        ),
        market_table=ImportRef(
            module="almanak.connectors.fluid.lending_read",
            attribute="FLUID_FTOKEN_MARKETS",
        ),
        # Lending-scoped alias (aave precedent: "aave" -> aave_v3): the
        # platform spec emits ``protocol: "fluid_lending"`` and the raw
        # string travels on the intent into the accounting layer, whose
        # ``_GENERIC_PRE_STATE_PROTOCOLS`` gate and position-key derivation
        # canonicalize via ``LendingReadRegistry.normalize_protocol``.
        # Without this alias the gate degrades fluid rows to ESTIMATED and
        # the keys diverge (``:fluid_lending:`` vs ``:fluid:``).
        aliases=("fluid_lending",),
    ),
    # Fluid's lending compiler ships metadata amounts wei-encoded
    # (``supply_amount`` / ``withdraw_amount`` = ERC-4626 asset base units);
    # the orchestrator's pre-flight balance check and description formatter
    # both derive the wei/human classification from this declaration
    # (VIB-3747 / VIB-4851 C1). Without it the amounts would be classified
    # human and mis-scaled by 10**decimals.
    metadata_amount_encoding=MetadataAmountEncoding(lending="wei"),
    strategy_intents=("SWAP", "SUPPLY", "WITHDRAW"),
    strategy_chains=("arbitrum", "base", "ethereum", "polygon"),
    strategy_matrix_entries=(
        StrategyMatrixEntry(
            matrix_name="fluid",
            category="swap",
            chains=frozenset(("arbitrum", "base", "ethereum", "polygon")),
        ),
        StrategyMatrixEntry(
            matrix_name="fluid",
            category="lending",
            chains=frozenset(("arbitrum", "base")),
        ),
    ),
)

__all__ = ["CONNECTOR"]
