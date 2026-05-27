"""Strategy-side receipt-parser connector for Aerodrome (VIB-4854 / W2).

Two distinct parser classes ride this connector:

* ``aerodrome`` — classic Solidly-style stable/volatile pools
  (``AerodromeReceiptParser``).
* ``aerodrome_slipstream`` — Uniswap-V3-style concentrated liquidity
  on Base (``AerodromeSlipstreamReceiptParser``).

The capability's ``receipt_parser_class(key)`` routes by key.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
    ReceiptParserRegistryError,
)


class AerodromeReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("aerodrome")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"aerodrome", "aerodrome_slipstream"})

    def receipt_parser_class(self, key: str) -> type:
        # Lazy import so this provider can be registered without
        # touching the (large) parser module.
        from almanak.connectors.aerodrome.receipt_parser import (
            AerodromeReceiptParser,
            AerodromeSlipstreamReceiptParser,
        )

        if key == "aerodrome":
            return AerodromeReceiptParser
        if key == "aerodrome_slipstream":
            return AerodromeSlipstreamReceiptParser
        raise ReceiptParserRegistryError(f"AerodromeReceiptParserConnector does not own key {key!r}")


__all__ = ["AerodromeReceiptParserConnector"]
