"""Orca Whirlpools program constants.

Program IDs, PDA seeds, instruction discriminators, and tick math
constants for the Orca Whirlpools program on Solana mainnet.

Reference: https://github.com/orca-so/whirlpools
"""

import hashlib
import os

# =========================================================================
# Program IDs
# =========================================================================

# Orca Whirlpools program (mainnet)
WHIRLPOOL_PROGRAM_ID = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"

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

POSITION_SEED = b"position"
TICK_ARRAY_SEED = b"tick_array"

# =========================================================================
# Anchor Instruction Discriminators
# sha256("global:<method_name>")[:8]
# =========================================================================


def _anchor_discriminator(method_name: str) -> bytes:
    """Compute Anchor instruction discriminator."""
    return hashlib.sha256(f"global:{method_name}".encode()).digest()[:8]


OPEN_POSITION_DISCRIMINATOR = _anchor_discriminator("open_position")
OPEN_POSITION_WITH_METADATA_DISCRIMINATOR = _anchor_discriminator("open_position_with_metadata")
INCREASE_LIQUIDITY_DISCRIMINATOR = _anchor_discriminator("increase_liquidity")
DECREASE_LIQUIDITY_DISCRIMINATOR = _anchor_discriminator("decrease_liquidity")
CLOSE_POSITION_DISCRIMINATOR = _anchor_discriminator("close_position")

# =========================================================================
# Tick math constants (same Q64.64 model as Raydium CLMM)
# =========================================================================

# Orca uses the same min/max tick as Raydium CLMM
MIN_TICK = -443636
MAX_TICK = 443636

# Ticks in a tick array
TICK_ARRAY_SIZE = 88

# =========================================================================
# Orca API
# =========================================================================

ORCA_API_BASE_URL = os.environ.get("ORCA_API_BASE_URL") or "https://api.orca.so/v2/solana"

# Common Whirlpool tick spacings and their fee tiers
TICK_SPACINGS = {
    1: 100,  # 0.01% fee
    8: 400,  # 0.04% fee
    16: 800,  # 0.08% fee
    64: 3000,  # 0.30% fee
    128: 6400,  # 0.64% fee
    256: 10000,  # 1.00% fee
}
