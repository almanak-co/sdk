"""Spark connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="spark",
    kind=ProtocolKind.LENDING,
    address_tables=(
        AddressTableSpec(
            protocol="spark",
            module="almanak.connectors.spark.addresses",
            attribute="SPARK",
        ),
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.spark.receipt_parser_provider",
        attribute="SparkReceiptParserConnector",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.spark.contract_roles",
        attribute="CONTRACT_ROLES",
        order=10,
    ),
    compiler=ImportRef(
        module="almanak.connectors.spark.compiler",
        attribute="SparkCompiler",
    ),
    strategy_intents=("SUPPLY", "BORROW", "REPAY", "WITHDRAW"),
    strategy_chains=("ethereum",),
)

__all__ = ["CONNECTOR"]
