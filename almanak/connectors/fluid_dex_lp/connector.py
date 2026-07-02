"""Fluid DEX LP (SmartLending) connector manifest — ``fluid_dex_lp``."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    PositionReadDecl,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.position_read_base import FUNGIBLE_LP

CONNECTOR = Connector(
    # DEX LP surface (Phase 4, VIB-5032): a third thin manifest over the fluid
    # package. Fluid SmartLending wrappers are fungible ERC-20-share, two-token
    # DEX-LP positions (no NFT, no tick range) — direct pool LP is whitelist-
    # gated (Phase-0 §V4), so the wrapper is the whitelisted supplier. Distinct
    # protocol key keeps LP accounting keys (``lp:fluid_dex_lp:...``) separate
    # from the fToken lending (``fluid``) and vault borrow (``fluid_vault``)
    # surfaces. All implementation modules live in ``almanak.connectors.fluid``.
    name="fluid_dex_lp",
    kind=ProtocolKind.LP,
    aliases=(),
    # Fungible ERC-20 LP token: ``LPCloseIntent.position_id`` is the wrapper
    # address, never an NFT discriminator (curve precedent, VIB-4968). Drives
    # the fungible-LP accounting/position-key path (no tokenId/tick segments).
    fungible_lp=True,
    address_tables=(
        AddressTableSpec(
            protocol="fluid_dex_lp",
            module="almanak.connectors._fluid_core.addresses",
            attribute="FLUID_DEX_LP",
        ),
    ),
    compiler=ImportRef(
        module="almanak.connectors._fluid_core.dex_lp_compiler",
        attribute="FluidDexLpCompiler",
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.fluid_dex_lp.receipt_parser_provider",
        attribute="FluidDexLpReceiptParserConnector",
    ),
    # On-chain LP repricing is connector-valued (share balance → per-share
    # token0/token1 claim via the SmartLending resolver). Declaring the
    # fungible_lp kind + builder routes the valuer's FungibleLpPositionReader
    # through PositionReadRegistry instead of its old _BOOTSTRAP map (VIB-5126).
    position_read=PositionReadDecl(
        kind=FUNGIBLE_LP,
        builder=ImportRef(
            module="almanak.connectors._fluid_core.dex_lp_valuation",
            attribute="read_fungible_lp_position",
        ),
    ),
    strategy_intents=("LP_OPEN", "LP_CLOSE"),
    # v1 scope — arbitrum only (the sole chain whose SmartLending wrappers were
    # round-tripped on-chain). base/ethereum/polygon need per-chain resolver
    # verification before being added.
    strategy_chains=("arbitrum",),
)

__all__ = ["CONNECTOR"]
