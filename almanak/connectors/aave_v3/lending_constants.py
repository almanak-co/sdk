"""Aave V3 lending-classification constants.

VIB-4872 (W6-followup): the framework branch ``protocol in
AAVE_COMPATIBLE_PROTOCOLS`` used to live as a hard-coded set in
``almanak/framework/intents/compiler_constants.py``. Lending connectors
that share the Aave V3 ``supply`` / ``borrow`` / ``repay`` / ``withdraw``
ABI now publish their membership here.

The shape is intentionally trivial — a frozenset of protocol names — to
match the legacy lookup site (a single ``in`` membership test). A future
W6-shaped capability could carry richer per-protocol lending metadata,
but the byte-equivalent move is to keep the set shape.
"""

from __future__ import annotations

# Canonical Aave V3 (this connector). Spark intentionally does NOT
# appear in the legacy ``AAVE_COMPATIBLE_PROTOCOLS`` set even though
# spark's adapter rides on the Aave V3 ABI — preserve that exact
# coverage to keep byte-equivalence at the compiler's compatibility-
# membership test. Adding Spark to the V3 family is a separate decision.
AAVE_V3_FAMILY_PROTOCOLS: frozenset[str] = frozenset({"aave_v3"})


__all__ = ["AAVE_V3_FAMILY_PROTOCOLS"]
