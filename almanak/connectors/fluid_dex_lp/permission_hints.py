"""Fluid SmartLending DEX LP permission hints (VIB-5032).

``fluid_dex_lp`` exposes only ``LP_OPEN`` / ``LP_CLOSE`` (fungible ERC-20-share
wrappers). Like Curve's fungible LP, it is deliberately NOT part of the
synthetic Zodiac discovery matrix yet — its intent tests carry
``@pytest.mark.no_zodiac`` and on-chain proof runs as an EOA. Wiring
``fluid_dex_lp`` into synthetic discovery (a connector-owned
``build_discovery_vectors`` over the SmartLending wrapper universe) is a
follow-up (VIB-5125).

Until then this connector declares empty hints (the minimal valid body): no
synthetic-discovery intents, so the framework adds no synthetic LP vectors for
``fluid_dex_lp`` and the Safe/Roles permission surface is not auto-expanded for
an unproven discovery path.
"""

from __future__ import annotations

from almanak.framework.permissions.hints import PermissionHints

PERMISSION_HINTS = PermissionHints()
