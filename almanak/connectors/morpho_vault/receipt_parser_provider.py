"""Strategy-side receipt-parser connector for MetaMorpho vaults (VIB-4854 / W2).

The MetaMorpho vault parser lives under ``morpho_vault/`` (sibling of
``morpho_blue``); its registry key is ``metamorpho``, matching the
ERC-4626 vault concept rather than the connector folder name.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class MetaMorphoReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("metamorpho")
    kind: ClassVar[ProtocolKind] = ProtocolKind.VAULT

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"metamorpho"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.morpho_vault.receipt_parser import (
            MetaMorphoReceiptParser,
        )

        return MetaMorphoReceiptParser


__all__ = ["MetaMorphoReceiptParserConnector"]
