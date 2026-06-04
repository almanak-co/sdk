"""Camelot connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="camelot",
    kind=ProtocolKind.SWAP,
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
)

__all__ = ["CONNECTOR"]
