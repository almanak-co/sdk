"""Aave V3 protocol-family membership.

VIB-4928 (PR-3b): declares this connector's contribution to the ``AAVE_V3``
family (the framework's ``AAVE_COMPATIBLE_PROTOCOLS`` set — lending connectors
sharing the Aave V3 ``supply`` / ``borrow`` / ``repay`` / ``withdraw`` Pool
ABI). Spark rides the Aave V3 ABI but is intentionally NOT declared here —
preserving the exact ``AAVE_COMPATIBLE_PROTOCOLS`` coverage the compiler's
compatibility-membership test has always had (adding Spark to the V3 family is
a separate decision). No framework module imports this connector directly.

Strategy-side only — gateway code does not consult this module.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.protocol_family_registry import (
    ProtocolFamily,
    ProtocolFamilySpec,
)

PROTOCOL_FAMILY = ProtocolFamilySpec(families={ProtocolFamily.AAVE_V3: frozenset({"aave_v3"})})


__all__ = ["PROTOCOL_FAMILY"]
