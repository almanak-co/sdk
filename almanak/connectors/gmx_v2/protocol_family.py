"""GMX V2 protocol-family membership.

TD-02 (VIB-5460): declares this connector's contribution to the ``GMX_V2_PERP``
family (the framework's ``GMX_COMPATIBLE_PROTOCOLS`` set — perp connectors
sharing the GMX V2 ``(market, collateralToken, isLong)`` position model). The
migration backfill / runner registry dispatch key their perp
``position_registry`` cutover branch on this registry-derived union, so there is
no ``gmx_v2`` string literal in framework code. No framework module imports this
connector directly.

Strategy-side only — gateway code does not consult this module.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.protocol_family_registry import (
    ProtocolFamily,
    ProtocolFamilySpec,
)

PROTOCOL_FAMILY = ProtocolFamilySpec(families={ProtocolFamily.GMX_V2_PERP: frozenset({"gmx_v2"})})


__all__ = ["PROTOCOL_FAMILY"]
