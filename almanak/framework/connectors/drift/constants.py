"""Drift Protocol program constants.

Program IDs, PDA seeds, instruction discriminators, market indexes,
and precision constants for Drift perpetual futures on Solana mainnet.

Reference: https://github.com/drift-labs/protocol-v2
"""

# =========================================================================
# Program IDs
# =========================================================================

# Drift V2 program (mainnet)
DRIFT_PROGRAM_ID = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"

# SPL Token program
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

# Associated Token Account program
ASSOCIATED_TOKEN_PROGRAM_ID = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

# System program
SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"

# Rent sysvar
RENT_SYSVAR_ID = "SysvarRent111111111111111111111111111111111"

# =========================================================================
# PDA Seeds
# =========================================================================

STATE_SEED = b"drift_state"
USER_SEED = b"user"
USER_STATS_SEED = b"user_stats"
PERP_MARKET_SEED = b"perp_market"
SPOT_MARKET_SEED = b"spot_market"

# =========================================================================
# Instruction Discriminators (Anchor: sha256("global:<method>")[:8])
# =========================================================================

PLACE_PERP_ORDER_DISCRIMINATOR = bytes.fromhex("45a15dca787e4cb9")
INITIALIZE_USER_DISCRIMINATOR = bytes.fromhex("6f11b9fa3c7a26fe")
INITIALIZE_USER_STATS_DISCRIMINATOR = bytes.fromhex("fef34862fb82a8d5")
DEPOSIT_DISCRIMINATOR = bytes.fromhex("f223c68952e1f2b6")
CANCEL_ORDER_DISCRIMINATOR = bytes.fromhex("5f81edf00831df84")

# =========================================================================
# Perp Market Indexes (index → symbol)
# Top markets by volume/TVL on Drift
# =========================================================================

PERP_MARKETS: dict[int, str] = {
    0: "SOL-PERP",
    1: "BTC-PERP",
    2: "ETH-PERP",
    3: "APT-PERP",
    4: "MATIC-PERP",
    5: "1MBONK-PERP",
    6: "ARB-PERP",
    7: "DOGE-PERP",
    8: "BNB-PERP",
    9: "SUI-PERP",
    10: "PEPE-PERP",
    11: "OP-PERP",
    12: "RNDR-PERP",
    13: "XRP-PERP",
    14: "HNT-PERP",
    15: "INJ-PERP",
    16: "LINK-PERP",
    17: "RLB-PERP",
    18: "PYTH-PERP",
    19: "TIA-PERP",
    20: "JTO-PERP",
    21: "SEI-PERP",
    22: "AVAX-PERP",
    23: "WIF-PERP",
    24: "JUP-PERP",
    25: "DYM-PERP",
    26: "TAO-PERP",
    27: "W-PERP",
    28: "KMNO-PERP",
    29: "TNSR-PERP",
    30: "DRIFT-PERP",
}

# Reverse mapping: symbol → index
PERP_MARKET_SYMBOL_TO_INDEX: dict[str, int] = {v: k for k, v in PERP_MARKETS.items()}

# =========================================================================
# Spot Market Indexes
# =========================================================================

SPOT_MARKET_USDC_INDEX = 0
SPOT_MARKET_SOL_INDEX = 1

# USDC mint on Solana mainnet
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# =========================================================================
# Precision Constants
# =========================================================================

BASE_PRECISION = 1_000_000_000  # 1e9 for perp base asset amounts
PRICE_PRECISION = 1_000_000  # 1e6 for oracle prices
QUOTE_PRECISION = 1_000_000  # 1e6 for USDC/quote amounts
FUNDING_RATE_PRECISION = 1_000_000_000  # 1e9 for funding rates
MARGIN_PRECISION = 10_000  # 1e4 for margin ratios

# =========================================================================
# Order Type Enums (Borsh encoding values)
# =========================================================================

# OrderType
ORDER_TYPE_MARKET = 0
ORDER_TYPE_LIMIT = 1
ORDER_TYPE_TRIGGER_MARKET = 2
ORDER_TYPE_TRIGGER_LIMIT = 3
ORDER_TYPE_ORACLE = 4

# MarketType
MARKET_TYPE_PERP = 0
MARKET_TYPE_SPOT = 1

# PositionDirection
DIRECTION_LONG = 0
DIRECTION_SHORT = 1

# PostOnlyParam
POST_ONLY_NONE = 0
POST_ONLY_MUST_POST_ONLY = 1
POST_ONLY_TRY_POST_ONLY = 2
POST_ONLY_SLIDE = 3

# TriggerCondition
TRIGGER_CONDITION_ABOVE = 0
TRIGGER_CONDITION_BELOW = 1

# =========================================================================
# User Account Layout Offsets
# =========================================================================

# Drift User account layout (Anchor account discriminator = 8 bytes)
# Verified against Drift protocol-v2 program (mainnet deploy as of 2026-03-02).
# If Drift upgrades the program, these offsets MUST be re-verified.
# Sanity check: after parsing, verify authority == expected wallet address.
USER_ACCOUNT_DISCRIMINATOR_SIZE = 8
# Authority pubkey offset (after discriminator)
USER_AUTHORITY_OFFSET = 8
# Sub-account ID offset
USER_SUB_ACCOUNT_ID_OFFSET = 40
# Perp positions array offset (after fixed header fields)
USER_PERP_POSITIONS_OFFSET = 264
# Each perp position is 80 bytes
PERP_POSITION_SIZE = 80
# Number of perp position slots
MAX_PERP_POSITIONS = 8
# Spot positions start after perp positions
USER_SPOT_POSITIONS_OFFSET = USER_PERP_POSITIONS_OFFSET + (PERP_POSITION_SIZE * MAX_PERP_POSITIONS)
# Each spot position is 48 bytes
SPOT_POSITION_SIZE = 48
# Number of spot position slots
MAX_SPOT_POSITIONS = 8

# =========================================================================
# Perp Market Account Layout Offsets
# =========================================================================

# Market account oracle offsets (verified against Drift protocol-v2, 2026-03-02).
# PerpMarket: oracle pubkey at offset 48 (after 8-byte discriminator + header fields)
PERP_MARKET_ORACLE_OFFSET = 48
# SpotMarket: oracle pubkey at offset 48 (same position in SpotMarket layout)
# NOTE: Verified against Drift IDL. If spot market layout diverges from perp,
# this must be updated separately.
SPOT_MARKET_ORACLE_OFFSET = 48

# =========================================================================
# Data API
# =========================================================================

DRIFT_DATA_API_BASE_URL = "https://data.api.drift.trade"
