"""Meteora DLMM program constants.

Program IDs, PDA seeds, instruction discriminators, and bin math
constants for the Meteora DLMM program on Solana mainnet.

Reference: https://github.com/nicholasgasior/meteora-dlmm-db (Anchor IDL)
"""

import hashlib

# =========================================================================
# Program IDs
# =========================================================================

# Meteora DLMM program (mainnet)
DLMM_PROGRAM_ID = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"

# SPL Token program
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

# Associated Token Account program
ASSOCIATED_TOKEN_PROGRAM_ID = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

# System program
SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"

# Rent sysvar
RENT_SYSVAR_ID = "SysvarRent111111111111111111111111111111111"

# Event authority PDA seed
EVENT_AUTHORITY_SEED = b"__event_authority"

# =========================================================================
# PDA Seeds
# =========================================================================

POSITION_SEED = b"position"
BIN_ARRAY_SEED = b"bin_array"
ORACLE_SEED = b"oracle"

# =========================================================================
# Bin constants
# =========================================================================

# Number of bins per bin array account
BIN_ARRAY_SIZE = 70

# Bin ID zero-offset (center bin_id maps to price 1.0)
BIN_ID_OFFSET = 8388608

# =========================================================================
# Instruction Discriminators (Anchor: sha256("global:<method_name>")[:8])
# =========================================================================


def _anchor_discriminator(method_name: str) -> bytes:
    """Compute Anchor instruction discriminator."""
    return hashlib.sha256(f"global:{method_name}".encode()).digest()[:8]


INITIALIZE_POSITION_DISCRIMINATOR = _anchor_discriminator("initialize_position")
ADD_LIQUIDITY_BY_STRATEGY_DISCRIMINATOR = _anchor_discriminator("add_liquidity_by_strategy")
REMOVE_LIQUIDITY_BY_RANGE_DISCRIMINATOR = _anchor_discriminator("remove_liquidity_by_range")
CLOSE_POSITION_DISCRIMINATOR = _anchor_discriminator("close_position")

# =========================================================================
# Strategy Types (for addLiquidityByStrategy)
# =========================================================================

STRATEGY_TYPE_SPOT_BALANCED = 6
STRATEGY_TYPE_CURVE_BALANCED = 7
STRATEGY_TYPE_BID_ASK_BALANCED = 8

# =========================================================================
# API
# =========================================================================

METEORA_API_BASE_URL = "https://dlmm-api.meteora.ag"

# =========================================================================
# Common bin_step values and their approximate fee tiers (bps)
# bin_step is in basis points of price change per bin
# =========================================================================

BIN_STEP_FEE_MAP = {
    1: 1,  # 0.01% per bin
    2: 2,  # 0.02% per bin
    5: 5,  # 0.05% per bin
    10: 10,  # 0.10% per bin
    15: 15,  # 0.15% per bin
    20: 20,  # 0.20% per bin
    25: 25,  # 0.25% per bin
    50: 50,  # 0.50% per bin
    80: 80,  # 0.80% per bin
    100: 100,  # 1.00% per bin
}
