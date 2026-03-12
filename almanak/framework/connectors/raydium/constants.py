"""Raydium CLMM program constants.

Program IDs, PDA seeds, instruction discriminators, and account layout
constants for the Raydium CLMM program on Solana mainnet.

Reference: https://github.com/raydium-io/raydium-clmm
"""

# =========================================================================
# Program IDs
# =========================================================================

# Raydium CLMM program (mainnet)
CLMM_PROGRAM_ID = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"

# SPL Token program
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

# SPL Token-2022 program
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

# Associated Token Account program
ASSOCIATED_TOKEN_PROGRAM_ID = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

# System program
SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"

# Rent sysvar
RENT_SYSVAR_ID = "SysvarRent111111111111111111111111111111111"

# Metaplex Token Metadata program
METADATA_PROGRAM_ID = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"

# =========================================================================
# PDA Seeds
# =========================================================================

POOL_SEED = b"pool"
POSITION_SEED = b"position"
TICK_ARRAY_SEED = b"tick_array"
OBSERVATION_SEED = b"observation"

# =========================================================================
# Instruction Discriminators (first 8 bytes)
# =========================================================================

# openPositionV2 — supports both SPL Token and Token-2022
OPEN_POSITION_V2_DISCRIMINATOR = bytes.fromhex("4db84ad67056f1c7")

# increaseLiquidityV2
INCREASE_LIQUIDITY_V2_DISCRIMINATOR = bytes.fromhex("851d59df45eeb00a")

# decreaseLiquidityV2
DECREASE_LIQUIDITY_V2_DISCRIMINATOR = bytes.fromhex("3a7fbc3e4f52c460")

# closePosition
CLOSE_POSITION_DISCRIMINATOR = bytes.fromhex("7b86510031446262")

# =========================================================================
# Tick math constants
# =========================================================================

# Minimum and maximum tick indices
MIN_TICK = -443636
MAX_TICK = 443636

# Q64.64 fixed-point constants
Q64 = 1 << 64

# Min/max sqrt prices (Q64.64)
MIN_SQRT_PRICE_X64 = 4295048016
MAX_SQRT_PRICE_X64 = 79226673521066979257578248091

# =========================================================================
# Raydium API
# =========================================================================

RAYDIUM_API_BASE_URL = "https://api-v3.raydium.io"

# Common CLMM tick spacings and their fee tiers
TICK_SPACINGS = {
    1: 100,  # 0.01% fee
    10: 500,  # 0.05% fee
    60: 3000,  # 0.30% fee
    120: 10000,  # 1.00% fee
}
