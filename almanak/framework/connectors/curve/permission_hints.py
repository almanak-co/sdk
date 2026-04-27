"""Curve Finance permission hints for permission discovery.

Curve does NOT use the generic ``synthetic_swap_pair`` mechanism. Its pools
are pair-specific (StableSwap, CryptoSwap, Tricrypto), so a single pair only
resolves to one curated pool per chain — leaving every other registered pool
unauthorised on the Safe (#1903). The synthetic discovery path for curve is
handled directly in
``almanak.framework.permissions.synthetic_intents._build_curve_swap_intents``,
which iterates ``CURVE_POOLS[chain]`` and emits one synthetic ``SwapIntent``
per registered pool. The manifest then authorises the entire curated curve
pool surface for every supported chain.
"""

from almanak.framework.permissions.hints import PermissionHints

PERMISSION_HINTS = PermissionHints()
