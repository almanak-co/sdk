"""Fluid connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    LendingReadDecl,
    MetadataAmountEncoding,
    StrategyIntentChainExclusion,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="fluid",
    external_ids={"defillama": "fluid-dex"},
    # SWAP (Phase 1, VIB-5029, 4 chains) + fToken lending SUPPLY/WITHDRAW
    # (Phase 2, VIB-5030, arbitrum+base). Fluid's LP surface is
    # whitelist-gated on-chain (Phase-0 finding, VIB-5028 §V4) and ships
    # later via SmartLending / smart vaults (VIB-5032); vault borrow is
    # VIB-5031. ``kind`` stays SWAP (primary surface); the intent/chain
    # exclusions below scope lending to its validated chains.
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
        module="almanak.connectors._fluid_core.gateway.provider",
        attribute="FluidGatewayConnector",
        order=4,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.fluid.receipt_parser_provider",
        attribute="FluidReceiptParserConnector",
    ),
    swap_quote_connector=ImportRef(
        module="almanak.connectors._fluid_core.swap_quote_provider",
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
        # VIB-5493: fTokens are supply-only and carry NO market_id — one fToken
        # per underlying token per chain, so the position identity IS the token.
        # The teardown lending guard splits its position key per token for
        # token-keyed protocols, so two distinct Fluid supplies (e.g. USDC + USDT)
        # are two positions instead of collapsing to one ``(fluid, chain, "")``
        # key (which silently dropped the second withdraw). The vault CDP surface
        # (``fluid_vault``) stays account/vault-keyed — it REQUIRES a market_id.
        token_keyed=True,
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
    # Lending is arbitrum + base only; SWAP is all four chains. This narrowing
    # used to live in ``strategy_matrix_entries`` (a hand-written ``lending``
    # row pinned to arbitrum/base), which scoped the RENDERED row but left
    # ``chains_for_intent(SUPPLY)`` answering all four chains — the accessor
    # every consumer is told to ask. Publishing per-intent chain coverage
    # (matrix schema v2) turned that split into a live over-advertisement, so
    # the truth moved to the typed per-cell mechanism and the matrix rows are
    # now DERIVED from it: SWAP -> swap on 4 chains, SUPPLY/WITHDRAW ->
    # lending on arbitrum+base, exactly the two rows the override produced.
    # Fluid appears in no compiler routing table, so dropping the override
    # cannot let Phase B widen these rows.
    strategy_intent_chain_exclusions=(
        StrategyIntentChainExclusion(
            intent="SUPPLY",
            chains=frozenset({"ethereum", "polygon"}),
            reason=(
                "fToken lending (VIB-5030) shipped on arbitrum + base only. "
                "FLUID_FTOKEN_MARKETS carries no ethereum or polygon market, so a "
                "SUPPLY cannot compile there. Those two chains carry the Fluid DEX "
                "SWAP surface only (VIB-5029), which stays advertised."
            ),
            ticket="VIB-5030",
        ),
        StrategyIntentChainExclusion(
            intent="WITHDRAW",
            chains=frozenset({"ethereum", "polygon"}),
            reason=(
                "Dual of the SUPPLY exclusion — WITHDRAW redeems an fToken position "
                "that cannot exist on ethereum or polygon (no market in "
                "FLUID_FTOKEN_MARKETS). Excluded together so the lending lifecycle "
                "is advertised as a whole or not at all: advertising WITHDRAW alone "
                "would imply a redeemable position on a chain that cannot open one."
            ),
            ticket="VIB-5030",
        ),
    ),
)

__all__ = ["CONNECTOR"]
