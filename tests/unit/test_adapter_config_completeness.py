"""CI test: Validate adapter config dicts have no chain gaps.

Cross-references contracts.py (authoritative protocol-chain registry) against
the compiler's PROTOCOL_ROUTERS, LP_POSITION_MANAGERS, SWAP_QUOTER_ADDRESSES,
and LENDING_POOL_ADDRESSES dicts. Fails if a protocol is deployed on a chain
(per contracts.py) but missing from the corresponding compiler config.

Catches the recurring pattern where new chain support is added to contracts.py
but the compiler config lags behind, causing silent failures at runtime.

VIB-613: Systematic config gap sweep.
"""

from __future__ import annotations

import pytest

from almanak.core.contracts import (
    AAVE_V3,
    AERODROME,
    PANCAKESWAP_V3,
    SUSHISWAP_V3,
    TRADERJOE_V2,
    UNISWAP_V3,
)
from almanak.framework.intents.compiler import (
    LENDING_POOL_ADDRESSES,
    LP_POSITION_MANAGERS,
    PROTOCOL_ROUTERS,
    SWAP_QUOTER_ADDRESSES,
)

# ============================================================================
# DEX swap router completeness
# ============================================================================

# Mapping: contracts.py dict -> protocol key in PROTOCOL_ROUTERS
# Only protocols that support swaps should be listed here.
_SWAP_PROTOCOLS = {
    "uniswap_v3": UNISWAP_V3,
    "sushiswap_v3": SUSHISWAP_V3,
    "pancakeswap_v3": PANCAKESWAP_V3,
    "aerodrome": AERODROME,
    "traderjoe_v2": TRADERJOE_V2,
}

# Chains that exist in contracts.py but are intentionally excluded from
# the compiler because they use a fork/alias or don't have swap support yet.
# Format: (protocol, chain) tuples
_SWAP_ROUTER_KNOWN_GAPS: set[tuple[str, str]] = {
    # Uniswap V3 forks on these chains use the same router under "uniswap_v3" key
    # but contracts.py lists them separately. Linea/Blast/Monad are newer chains
    # with less testing coverage.
    ("uniswap_v3", "linea"),
    ("uniswap_v3", "blast"),
    ("uniswap_v3", "monad"),
    # Aerodrome on Optimism uses Velodrome alias in PROTOCOL_ROUTERS
    ("aerodrome", "optimism"),
    # TraderJoe V2 uses LBRouter2 (bin-based AMM) incompatible with DefaultSwapAdapter (VIB-1406).
    # Swaps are blocked at compiler level; LP operations still work via LP_POSITION_MANAGERS.
    ("traderjoe_v2", "avalanche"),
    ("traderjoe_v2", "arbitrum"),
    ("traderjoe_v2", "bsc"),
    ("traderjoe_v2", "ethereum"),
}


def _collect_swap_gaps() -> list[str]:
    """Find protocols missing from PROTOCOL_ROUTERS for chains in contracts.py."""
    gaps = []
    for protocol_key, contracts_dict in _SWAP_PROTOCOLS.items():
        for chain in contracts_dict:
            if (protocol_key, chain) in _SWAP_ROUTER_KNOWN_GAPS:
                continue
            chain_routers = PROTOCOL_ROUTERS.get(chain, {})
            if protocol_key not in chain_routers:
                gaps.append(f"PROTOCOL_ROUTERS['{chain}'] missing '{protocol_key}'")
    return gaps


def test_swap_router_completeness():
    """Every DEX in contracts.py must have a PROTOCOL_ROUTERS entry for each chain."""
    gaps = _collect_swap_gaps()
    assert not gaps, (
        f"Adapter config gaps found ({len(gaps)}):\n"
        + "\n".join(f"  - {g}" for g in gaps)
        + "\n\nFix: add missing entries to PROTOCOL_ROUTERS in compiler.py, "
        "or add to _SWAP_ROUTER_KNOWN_GAPS if intentionally excluded."
    )


# ============================================================================
# LP position manager completeness
# ============================================================================

# Protocols that support LP operations — must have LP_POSITION_MANAGERS entries
_LP_PROTOCOLS = {
    "uniswap_v3": UNISWAP_V3,
    "sushiswap_v3": SUSHISWAP_V3,
    "pancakeswap_v3": PANCAKESWAP_V3,
    "aerodrome": AERODROME,
    "traderjoe_v2": TRADERJOE_V2,
}

_LP_MANAGER_KNOWN_GAPS: set[tuple[str, str]] = {
    # Same as swap router gaps — newer/fork chains
    ("uniswap_v3", "linea"),
    ("uniswap_v3", "blast"),
    ("uniswap_v3", "monad"),
    # Aerodrome on Optimism uses Velodrome alias
    ("aerodrome", "optimism"),
}


def _collect_lp_gaps() -> list[str]:
    """Find protocols missing from LP_POSITION_MANAGERS for chains in contracts.py."""
    gaps = []
    for protocol_key, contracts_dict in _LP_PROTOCOLS.items():
        for chain in contracts_dict:
            if (protocol_key, chain) in _LP_MANAGER_KNOWN_GAPS:
                continue
            chain_managers = LP_POSITION_MANAGERS.get(chain, {})
            if protocol_key not in chain_managers:
                gaps.append(f"LP_POSITION_MANAGERS['{chain}'] missing '{protocol_key}'")
    return gaps


def test_lp_position_manager_completeness():
    """Every LP-capable protocol in contracts.py must have LP_POSITION_MANAGERS entry."""
    gaps = _collect_lp_gaps()
    assert not gaps, (
        f"LP position manager gaps found ({len(gaps)}):\n"
        + "\n".join(f"  - {g}" for g in gaps)
        + "\n\nFix: add missing entries to LP_POSITION_MANAGERS in compiler.py, "
        "or add to _LP_MANAGER_KNOWN_GAPS if intentionally excluded."
    )


# ============================================================================
# Swap quoter completeness
# ============================================================================

# Protocols that use on-chain quoters (V3-style AMMs)
_QUOTER_PROTOCOLS = {
    "uniswap_v3": UNISWAP_V3,
    "sushiswap_v3": SUSHISWAP_V3,
    "pancakeswap_v3": PANCAKESWAP_V3,
}

_QUOTER_KNOWN_GAPS: set[tuple[str, str]] = {
    ("uniswap_v3", "linea"),
    ("uniswap_v3", "blast"),
    ("uniswap_v3", "monad"),
    # SushiSwap V3 quoter not yet configured for all chains
    ("sushiswap_v3", "optimism"),
}


def _collect_quoter_gaps() -> list[str]:
    """Find protocols missing from SWAP_QUOTER_ADDRESSES for chains in contracts.py."""
    gaps = []
    for protocol_key, contracts_dict in _QUOTER_PROTOCOLS.items():
        for chain in contracts_dict:
            if (protocol_key, chain) in _QUOTER_KNOWN_GAPS:
                continue
            # Only check chains that also have a swap router (no point having quoter without router)
            chain_routers = PROTOCOL_ROUTERS.get(chain, {})
            if protocol_key not in chain_routers:
                continue
            chain_quoters = SWAP_QUOTER_ADDRESSES.get(chain, {})
            if protocol_key not in chain_quoters:
                gaps.append(f"SWAP_QUOTER_ADDRESSES['{chain}'] missing '{protocol_key}'")
    return gaps


def test_swap_quoter_completeness():
    """V3-style AMMs with swap routers should also have quoter addresses."""
    gaps = _collect_quoter_gaps()
    assert not gaps, (
        f"Swap quoter gaps found ({len(gaps)}):\n"
        + "\n".join(f"  - {g}" for g in gaps)
        + "\n\nFix: add missing entries to SWAP_QUOTER_ADDRESSES in compiler.py, "
        "or add to _QUOTER_KNOWN_GAPS if intentionally excluded."
    )


# ============================================================================
# Lending pool completeness
# ============================================================================

_LENDING_PROTOCOLS = {
    "aave_v3": AAVE_V3,
}


def _collect_lending_gaps() -> list[str]:
    """Find protocols missing from LENDING_POOL_ADDRESSES for chains in contracts.py."""
    gaps = []
    for protocol_key, contracts_dict in _LENDING_PROTOCOLS.items():
        for chain in contracts_dict:
            chain_pools = LENDING_POOL_ADDRESSES.get(chain, {})
            if protocol_key not in chain_pools:
                gaps.append(f"LENDING_POOL_ADDRESSES['{chain}'] missing '{protocol_key}'")
    return gaps


def test_lending_pool_completeness():
    """Every lending protocol in contracts.py must have LENDING_POOL_ADDRESSES entry."""
    gaps = _collect_lending_gaps()
    assert not gaps, (
        f"Lending pool gaps found ({len(gaps)}):\n"
        + "\n".join(f"  - {g}" for g in gaps)
        + "\n\nFix: add missing entries to LENDING_POOL_ADDRESSES in compiler.py, "
        "or add to a known gaps set if intentionally excluded."
    )


# ============================================================================
# Internal consistency: PROTOCOL_ROUTERS ↔ LP_POSITION_MANAGERS
# ============================================================================

# Protocols that support both swap and LP — if a chain has a swap router,
# it should also have an LP position manager
_SWAP_AND_LP_PROTOCOLS = set(_SWAP_PROTOCOLS.keys()) & set(_LP_PROTOCOLS.keys())


def test_router_lp_manager_consistency():
    """If a protocol has a swap router on a chain, it should also have an LP manager."""
    gaps = []
    for chain, routers in PROTOCOL_ROUTERS.items():
        chain_managers = LP_POSITION_MANAGERS.get(chain, {})
        for protocol in routers:
            if protocol not in _SWAP_AND_LP_PROTOCOLS:
                continue
            if protocol not in chain_managers:
                gaps.append(
                    f"PROTOCOL_ROUTERS['{chain}']['{protocol}'] exists but "
                    f"LP_POSITION_MANAGERS['{chain}']['{protocol}'] is missing"
                )
    assert not gaps, (
        f"Router/LP manager inconsistencies ({len(gaps)}):\n"
        + "\n".join(f"  - {g}" for g in gaps)
        + "\n\nFix: add LP_POSITION_MANAGERS entries for protocols that support both swap and LP."
    )


# ============================================================================
# Address format validation
# ============================================================================


def _is_valid_evm_address(addr: object) -> bool:
    """Check if a value is a valid EVM address (0x + 40 hex chars)."""
    return (
        isinstance(addr, str)
        and len(addr) == 42
        and addr.startswith("0x")
        and all(c in "0123456789abcdefABCDEF" for c in addr[2:])
    )


def _collect_invalid_addresses() -> list[str]:
    """Find invalid EVM addresses across all config dicts."""
    invalid = []
    config_dicts = {
        "PROTOCOL_ROUTERS": PROTOCOL_ROUTERS,
        "LP_POSITION_MANAGERS": LP_POSITION_MANAGERS,
        "SWAP_QUOTER_ADDRESSES": SWAP_QUOTER_ADDRESSES,
        "LENDING_POOL_ADDRESSES": LENDING_POOL_ADDRESSES,
    }
    for dict_name, config in config_dicts.items():
        for chain, protocols in config.items():
            for protocol, addr in protocols.items():
                if not _is_valid_evm_address(addr):
                    invalid.append(f"{dict_name}['{chain}']['{protocol}'] = '{addr}' (invalid format)")
    return invalid


def test_all_addresses_valid_format():
    """Every address in compiler config dicts must be a valid EVM address (0x + 40 hex chars)."""
    invalid = _collect_invalid_addresses()
    assert not invalid, (
        f"Invalid EVM addresses found ({len(invalid)}):\n"
        + "\n".join(f"  - {i}" for i in invalid)
        + "\n\nValid EVM addresses must be exactly 42 characters: '0x' + 40 hex digits."
    )


# ============================================================================
# Summary report (always prints, useful for CI logs)
# ============================================================================


@pytest.fixture(autouse=True, scope="module")
def _print_coverage_summary():
    """Print a summary of config coverage after all tests run."""
    yield
    # Count coverage
    all_swap_entries = sum(len(v) for v in PROTOCOL_ROUTERS.values())
    all_lp_entries = sum(len(v) for v in LP_POSITION_MANAGERS.values())
    all_quoter_entries = sum(len(v) for v in SWAP_QUOTER_ADDRESSES.values())
    all_lending_entries = sum(len(v) for v in LENDING_POOL_ADDRESSES.values())
    print(
        f"\n[Config Completeness] "
        f"Routers={all_swap_entries}, LP={all_lp_entries}, "
        f"Quoters={all_quoter_entries}, Lending={all_lending_entries}"
    )
