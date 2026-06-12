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

__all__ = ["FLUID", "FLUID_VAULT", "FLUID_VAULT_MARKETS", "FLUID_VAULT_NATIVE_SENTINEL"]
