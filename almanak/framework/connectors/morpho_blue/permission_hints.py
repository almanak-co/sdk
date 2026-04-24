"""Morpho Blue permission hints for permission discovery.

Morpho Blue is market-ID driven and routes SUPPLY / WITHDRAW on flags
(``use_as_collateral`` / ``is_collateral``) that decide between the loan-token
path (``supply`` / ``withdraw``) and the collateral path (``supplyCollateral``
/ ``withdrawCollateral``). A single synthetic intent only exercises one flag
value, so naive compilation-based discovery emits just one selector per
operation — the manifest would miss the opposite path.

Rather than paper over this with ``static_permissions`` (which would merge
unconditionally and over-authorise ``borrow`` / ``repay`` on the Safe for a
SUPPLY-only strategy — see codex review 3135601928), the synthetic intent
generator is taught to emit BOTH flag variants for morpho_blue SUPPLY and
WITHDRAW. See ``almanak/framework/permissions/synthetic_intents.py`` —
``_build_supply_intents`` / ``_build_withdraw_intents`` — for the dispatch.
The compiler then naturally emits ``supply`` + ``supplyCollateral`` for SUPPLY
and ``withdraw`` + ``withdrawCollateral`` for WITHDRAW, and the manifest
carries only the selectors matching the requested intent types.

``selector_labels`` remains so human-readable labels still render if a
selector appears on the manifest.

See ``almanak.core.contracts.MORPHO_BLUE`` for the per-chain singleton
addresses — ethereum / base use the vanity ``0xBBBB...FFCb``, arbitrum and
polygon use chain-specific deployments.
"""

from almanak.framework.permissions.hints import PermissionHints

# Well-known Morpho Blue market ID (WETH/USDC on Ethereum).
# Used as a synthetic market_id for permission discovery - the actual
# market_id value doesn't affect which selectors are discovered.
# Per-chain overrides come from ``MORPHO_MARKETS`` in the adapter via the
# synthetic intent builder (see ``_build_supply_intents``); this default
# is retained for ethereum where the registry key matches.
_SYNTHETIC_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"

# Morpho Blue singleton selectors (4-byte). Sourced from
# ``almanak/framework/connectors/morpho_blue/adapter.py``; any change there
# must be mirrored here so manifests render human-readable labels.
_MORPHO_SELECTOR_LABELS: dict[str, str] = {
    "0xa99aad89": "supply((address,address,address,address,uint256),uint256,uint256,address,bytes)",
    "0x238d6579": "supplyCollateral((address,address,address,address,uint256),uint256,address,bytes)",
    "0x5c2bea49": "withdraw((address,address,address,address,uint256),uint256,uint256,address,address)",
    "0x8720316d": "withdrawCollateral((address,address,address,address,uint256),uint256,address,address)",
    "0x50d8cd4b": "borrow((address,address,address,address,uint256),uint256,uint256,address,address)",
    "0x20b76e81": "repay((address,address,address,address,uint256),uint256,uint256,address,bytes)",
}


PERMISSION_HINTS = PermissionHints(
    synthetic_market_id=_SYNTHETIC_MARKET_ID,
    selector_labels=dict(_MORPHO_SELECTOR_LABELS),
)
