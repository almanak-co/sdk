"""Protocol Alias and Display Name Registry.

Maps user-facing protocol names (aliases) to canonical internal protocol keys,
and canonical keys to human-readable display names for logging/UI.

Many chains have Uniswap V3 forks that use identical bytecode but different
branding (e.g., Agni Finance on Mantle). Internally the SDK routes these
through the same connector code using the canonical key (e.g., "uniswap_v3").
This module provides the translation layer so strategy authors can write
``protocol="agni"`` and see "Agni Finance" in logs.

Design principles:
    - Aliases are always (chain, alias) -> canonical (chain-scoped, never global)
    - Display names are (chain, canonical) -> human-readable brand
    - normalize_protocol() is called at ingress boundaries (compiler, config, registry)
    - display_protocol() is called at egress boundaries (logs, UI, operator cards)
    - Unknown aliases pass through as-is (no silent swallowing of typos)

Example:
    >>> from almanak.framework.connectors.protocol_aliases import normalize_protocol, display_protocol
    >>> normalize_protocol("mantle", "agni")
    'uniswap_v3'
    >>> display_protocol("mantle", "uniswap_v3")
    'Agni Finance'
    >>> normalize_protocol("arbitrum", "uniswap_v3")
    'uniswap_v3'
"""

# =============================================================================
# Alias Registry: (chain, user_input) -> canonical_protocol
# =============================================================================

PROTOCOL_ALIASES: dict[tuple[str, str], str] = {
    # Mantle — Agni Finance is the primary Uniswap V3 fork
    ("mantle", "agni"): "uniswap_v3",
    ("mantle", "agni_finance"): "uniswap_v3",
}

# ---------------------------------------------------------------------------
# Global aliases: applied on ALL chains after hyphen->underscore normalization.
# These handle cases where hyphen normalization produces a key that differs
# from the canonical registry key (e.g., "trader-joe-v2" -> "trader_joe_v2"
# but canonical is "traderjoe_v2").
# ---------------------------------------------------------------------------
_GLOBAL_ALIASES: dict[str, str] = {
    "trader_joe_v2": "traderjoe_v2",
}

# =============================================================================
# Display Name Registry: (chain, canonical_protocol) -> human_readable_name
# =============================================================================

PROTOCOL_DISPLAY_NAMES: dict[tuple[str, str], str] = {
    ("mantle", "uniswap_v3"): "Agni Finance",
}


# =============================================================================
# Public API
# =============================================================================


def normalize_protocol(chain: str, protocol: str) -> str:
    """Resolve a protocol alias to its canonical internal key.

    If no alias exists, returns the protocol as-is (lowercased).
    This is safe to call on already-canonical values — it's a no-op.

    Args:
        chain: Chain name (e.g., "mantle", "arbitrum"). Accepts Chain enum or string.
        protocol: User-supplied protocol name (e.g., "agni", "uniswap_v3").

    Returns:
        Canonical protocol key (e.g., "uniswap_v3").
    """
    chain_lower = str(chain).lower()
    # Normalize hyphens to underscores: "uniswap-v4" -> "uniswap_v4"
    # SDK protocol keys use underscores, but users/configs often use hyphens.
    protocol_lower = protocol.lower().replace("-", "_")
    # Chain-scoped alias first, then global alias fallback
    resolved = PROTOCOL_ALIASES.get((chain_lower, protocol_lower), protocol_lower)
    return _GLOBAL_ALIASES.get(resolved, resolved)


def display_protocol(chain: str, protocol: str) -> str:
    """Get human-readable display name for a protocol on a given chain.

    Falls back to the canonical protocol key if no display name is registered.

    Args:
        chain: Chain name (e.g., "mantle", "arbitrum"). Accepts Chain enum or string.
        protocol: Protocol name (alias or canonical).

    Returns:
        Human-readable display name (e.g., "Agni Finance") or canonical key.
    """
    chain_lower = str(chain).lower()
    canonical = normalize_protocol(chain_lower, protocol)
    return PROTOCOL_DISPLAY_NAMES.get((chain_lower, canonical), canonical)
