"""Fluid contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the Fluid entries previously held in
``almanak.framework.intents.compiler_constants.LP_POSITION_MANAGERS``
(VIB-4872 / epic VIB-4851).

Fluid deploys deterministically — the factory and resolver addresses are
identical on every supported chain (verified on-chain per chain at
Phase 0 / Phase 1, VIB-5028 / VIB-5029). Per-pool addresses are resolved
dynamically at runtime via the DexReservesResolver, not stored here.

The contract-kind vocabulary is connector-private — callers outside
this folder should consume the registry, not guess key names.
"""

from __future__ import annotations

from typing import Any

_FLUID_CHAIN_ENTRY: dict[str, str] = {
    # Fluid DexFactory — pools are resolved dynamically against this.
    "dex_factory": "0x91716C4EDA1Fb55e84Bf8b4c7085f84285c19085",
    # DexReservesResolver — pool enumeration + estimateSwapIn quotes.
    "dex_reserves_resolver": "0x05Bd8269A20C472b148246De20E6852091BF16Ff",
}

FLUID: dict[str, dict[str, str]] = {
    "arbitrum": dict(_FLUID_CHAIN_ENTRY),
    "base": dict(_FLUID_CHAIN_ENTRY),
    "ethereum": dict(_FLUID_CHAIN_ENTRY),
    "polygon": dict(_FLUID_CHAIN_ENTRY),
}

# =============================================================================
# Vault NFT-CDP surface (Phase 3, VIB-5031) — protocol key ``fluid_vault``
# =============================================================================

# Both contracts deploy deterministically to the same address on arbitrum
# AND base — re-verified on-chain per chain on 2026-06-12 forks
# (docs/internal/qa/fluid-vault-verification-2026-06-12.md, D3/D4: resolver
# code size 23,764 bytes on both; ``VaultResolver.FACTORY()`` reads back the
# factory on both).
_FLUID_VAULT_CHAIN_ENTRY: dict[str, str] = {
    # VaultResolver — getVaultEntireData / positionByNftId / positionsByUser.
    "vault_resolver": "0xA5C3E16523eeeDDcC34706b0E6bE88b4c6EA95cC",
    # VaultFactory — the ERC-721 home of every vault position NFT (mints
    # Transfer(0x0 -> wallet) + NewPositionMinted). Receipt-side nftId
    # capture is gated on THIS emitter (ADR §5).
    "vault_factory": "0x324c5Dc1fC42c7a4D43d92df1eBA58a54d13Bf2d",
}

#: Address table registered for the ``fluid_vault`` manifest (Checkpoint-1
#: scope: arbitrum + base only — no cross-product over-claim).
FLUID_VAULT: dict[str, dict[str, str]] = {
    "arbitrum": dict(_FLUID_VAULT_CHAIN_ENTRY),
    "base": dict(_FLUID_VAULT_CHAIN_ENTRY),
}

#: Fluid's native-token sentinel as it appears in VaultEntireData token slots
#: (type-1 vaults pair raw native collateral — no WETH wrapping).
FLUID_VAULT_NATIVE_SENTINEL = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

#: Pinned type-1 vault universe, keyed by chain -> lowercased vault address
#: (= the ``market_id`` canonical form). Every entry was verified on-chain
#: at 2026-06-12 fork blocks (verification report D3/D4): address via
#: ``getVaultAddress(id)`` cross-checked with a successful
#: ``getVaultEntireData`` decode; ``vault_type`` 10000 = VAULT_T1_TYPE;
#: token pair from ``constantVariables.supplyToken/borrowToken``.
#:
#: This doubles as the lending-read market table: ``collateral_token`` /
#: ``loan_token`` are the symbols the framework reader prices + injects
#: (``valuation_role_keys``), exactly like Morpho's ``MORPHO_MARKETS``.
#: The compiler refuses any ``market_id`` outside this table (fail closed —
#: an unpinned vault would compile positions valuation cannot mark).
_FLUID_VAULT_MARKETS_SRC: dict[str, dict[str, dict[str, Any]]] = {
    "arbitrum": {
        # Vault id 1: native-ETH collateral -> USDC debt.
        "0xeAbBfca72F8a8bf14C4ac59e69ECB2eB69F0811C": {
            "vault_id": 1,
            "vault_type": 10000,
            "collateral_token": "ETH",
            "collateral_address": FLUID_VAULT_NATIVE_SENTINEL,
            "loan_token": "USDC",
            "loan_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "native_collateral": True,
            "native_debt": False,
        },
    },
    "base": {
        # Vault id 47: sUSDai (18 dec) collateral -> USDC debt.
        "0x01F0D07fdE184614216e76782c6b7dF663F5375e": {
            "vault_id": 47,
            "vault_type": 10000,
            "collateral_token": "sUSDai",
            "collateral_address": "0x0B2b2B2076d95dda7817e785989fE353fe955ef9",
            "loan_token": "USDC",
            "loan_address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "native_collateral": False,
            "native_debt": False,
        },
    },
}

#: Runtime view with LOWERCASED vault keys — the canonical lookup form every
#: consumer uses (compiler/lending-read lowercase market_id before lookup).
#: Source literals above stay EIP-55 checksummed for the repo-wide address
#: checksum guard (tests/unit/core/test_eip55_checksum.py).
FLUID_VAULT_MARKETS: dict[str, dict[str, dict[str, Any]]] = {
    chain: {vault.lower(): entry for vault, entry in rows.items()} for chain, rows in _FLUID_VAULT_MARKETS_SRC.items()
}

# =============================================================================
# DEX LP surface (Phase 4, VIB-5032) — protocol key ``fluid_dex_lp``
# =============================================================================

# Fluid SmartLending: fungible ERC-20-share wrappers over Fluid DEX pools.
# Direct pool LP is whitelist-gated (``DexT1__UserSupplyInNotOn`` 51013, Phase-0
# §V4) — the wrapper IS the whitelisted supplier, so an EOA/Safe can LP through
# it. The SmartLendingResolver enumerates wrappers + returns per-wrapper
# (reserves, totalSupply, token0/token1/dex) for resolver-side NAV.
_FLUID_DEX_LP_CHAIN_ENTRY: dict[str, str] = {
    # SmartLendingResolver — getAllSmartLendingAddresses + getSmartLendingEntireData.
    # Verified on Arbitrum (docs/internal/qa/fluid-smartlending-validation-2026-06-12.md).
    "smart_lending_resolver": "0x3E69A3Af4305b65598b228d3da70786Bd9cfeB0e",
}

#: Address table for the ``fluid_dex_lp`` manifest. v1 scope: arbitrum only —
#: the only chain whose SmartLending wrappers were round-tripped on-chain.
#: Other chains require per-chain resolver verification before being added.
FLUID_DEX_LP: dict[str, dict[str, str]] = {
    "arbitrum": dict(_FLUID_DEX_LP_CHAIN_ENTRY),
}

#: Fluid's native-token sentinel as it appears in a SmartLending wrapper's
#: TOKEN0()/TOKEN1() slots (e.g. fSL5 FLUID/native-ETH — no WETH wrapping).
FLUID_DEX_LP_NATIVE_SENTINEL = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

#: Pinned SmartLending wrapper universe, keyed by chain -> lowercased wrapper
#: address (the LP ``pool`` / ``position_id`` canonical form). token0/token1 +
#: decimals/symbols verified on Arbitrum forks (validation report P0.1-P0.2).
#: ``deposit_enabled`` records the supply-on state observed at the probe block;
#: it is documentation only — the compiler ALWAYS re-checks deposit-enabled
#: live (the 51013 pre-flight), so a wrapper flipping on/off is handled at
#: compile time, not by this static flag. The compiler refuses any wrapper
#: outside this table (fail closed — an unpinned wrapper cannot be valued).
_FLUID_SMARTLENDING_MARKETS_SRC: dict[str, dict[str, dict[str, Any]]] = {
    "arbitrum": {
        # fSL5: FLUID / native-ETH (the native-leg fixture).
        "0x82C53239c4CFC89A8E55A691422af24c18A944b1": {
            "symbol": "fSL5",
            "dex": "0x2886a01a0645390872a9eb99dAe1283664b0c524",
            "token0": "0x61E030A56D33e8260FdD81f03B162A79Fe3449Cd",
            "token0_symbol": "FLUID",
            "token0_decimals": 18,
            "token1": FLUID_DEX_LP_NATIVE_SENTINEL,
            "token1_symbol": "ETH",
            "token1_decimals": 18,
            "native_token1": True,
            "deposit_enabled": True,
        },
        # fSL9: sUSDai / USDC (the round-trip fixture).
        "0x1F0bFd9862ae58208d26db0d80797974434EC013": {
            "symbol": "fSL9",
            "dex": "0x86f874212335Af27C41cDb855C2255543d1499cE",
            "token0": "0x0B2b2B2076d95dda7817e785989fE353fe955ef9",
            "token0_symbol": "sUSDai",
            "token0_decimals": 18,
            "token1": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "token1_symbol": "USDC",
            "token1_decimals": 6,
            "native_token1": False,
            "deposit_enabled": True,
        },
        # fSL12: RLP / USDC (the deposit-DISABLED negative fixture — kept in the
        # table so the compiler's live 51013 pre-flight is exercised end to end).
        "0xdC1dF9E55f3B7EBD4F19001b294d1e537320BC2E": {
            "symbol": "fSL12",
            "dex": "0x836951EB21F3Df98273517B7249dCEFF270d34bf",
            "token0": "0x35E5dB674D8e93a03d814FA0ADa70731efe8a4b9",
            "token0_symbol": "RLP",
            "token0_decimals": 18,
            "token1": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "token1_symbol": "USDC",
            "token1_decimals": 6,
            "native_token1": False,
            "deposit_enabled": False,
        },
    },
}

#: Runtime view with LOWERCASED wrapper keys — the canonical lookup form. Source
#: literals stay EIP-55 checksummed for the repo-wide checksum guard.
FLUID_SMARTLENDING_MARKETS: dict[str, dict[str, dict[str, Any]]] = {
    chain: {wrapper.lower(): entry for wrapper, entry in rows.items()}
    for chain, rows in _FLUID_SMARTLENDING_MARKETS_SRC.items()
}


def is_native_leg(entry: dict[str, Any]) -> bool:
    """True when either leg of a SmartLending wrapper is the native-ETH sentinel.

    Single source of truth for the native-leg test shared by the compiler
    (``FluidDexLpCompiler._refuse_native`` — refuses native wrappers at COMPILE
    because a native leg rides as ``msg.value`` with no ERC-20 ``Transfer`` log
    and would mis-account as a measured zero, VIB-5121) and synthetic
    permission discovery (``fluid_dex_lp`` static permissions — excludes native
    wrappers from the discovery surface so it never authorises a flow the
    compiler refuses). Both consumers MUST agree on this definition, so it lives
    here next to ``FLUID_SMARTLENDING_MARKETS`` rather than being re-derived in
    each module. A wrapper is native-leg when ``native_token1`` is set OR
    ``token0`` equals ``FLUID_DEX_LP_NATIVE_SENTINEL`` (case-insensitive).
    """
    native_t1 = bool(entry.get("native_token1"))
    # ``or ""`` (not the get-default) so an explicit ``None`` token0 compares as
    # "" rather than the string "None" — robust against a malformed config row.
    native_t0 = str(entry.get("token0") or "").lower() == FLUID_DEX_LP_NATIVE_SENTINEL.lower()
    return native_t0 or native_t1


__all__ = [
    "FLUID",
    "FLUID_DEX_LP",
    "FLUID_DEX_LP_NATIVE_SENTINEL",
    "FLUID_SMARTLENDING_MARKETS",
    "FLUID_VAULT",
    "FLUID_VAULT_MARKETS",
    "FLUID_VAULT_NATIVE_SENTINEL",
    "is_native_leg",
]
