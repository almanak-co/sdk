"""Radiant V2 lending-classification constants.

VIB-4872 (W6-followup): Radiant V2 is an Aave V2 fork — its lending
ABI uses the V2 ``deposit()`` selector (vs Aave V3's ``supply()``) but
the rest of the surface matches. The framework's compile-time
``protocol in AAVE_V2_FORKS`` branch keys on this set.

The shape is intentionally trivial — a frozenset of protocol names — to
match the legacy lookup site (single ``in`` membership test). The
compiled ``AAVE_V2_FORKS`` derived view in
``almanak/framework/intents/compiler_constants.py`` is the union of
every V2-fork connector's contribution.
"""

from __future__ import annotations

AAVE_V2_FORK_PROTOCOLS: frozenset[str] = frozenset({"radiant_v2"})


__all__ = ["AAVE_V2_FORK_PROTOCOLS"]
