"""Camelot connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="camelot",
    kind=ProtocolKind.SWAP,
    address_tables=(
        AddressTableSpec(
            protocol="camelot",
            module="almanak.connectors.camelot.addresses",
            attribute="CAMELOT",
        ),
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.camelot.contract_roles",
        attribute="CONTRACT_ROLES",
        order=7,
    ),
    swap_classification=ImportRef(
        module="almanak.connectors.camelot.swap_classification",
        attribute="SWAP_CLASSIFICATION",
        order=4,
    ),
    compiler=ImportRef(
        module="almanak.connectors.camelot.compiler",
        attribute="CamelotCompiler",
    ),
    strategy_intents=("SWAP",),
    strategy_chains=("arbitrum",),
)

__all__ = ["CONNECTOR"]
