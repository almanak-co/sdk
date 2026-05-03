"""Drift Protocol program constants.

Program IDs, PDA seeds, instruction discriminators, market indexes,
and precision constants for Drift perpetual futures on Solana mainnet.

Reference: https://github.com/drift-labs/protocol-v2
"""

import os

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
# VIB-3817: each constant below MUST equal sha256("global:<name>")[:8]; the
# DRIFT_INSTRUCTION_NAMES map below records the canonical Anchor name for
# every discriminator so anchor_discriminator() can re-derive and verify
# them at SDK construction. If Drift renames an instruction, the constant
# AND the entry in DRIFT_INSTRUCTION_NAMES both need updating.

PLACE_PERP_ORDER_DISCRIMINATOR = bytes.fromhex("45a15dca787e4cb9")
INITIALIZE_USER_DISCRIMINATOR = bytes.fromhex("6f11b9fa3c7a26fe")
INITIALIZE_USER_STATS_DISCRIMINATOR = bytes.fromhex("fef34862fb82a8d5")
DEPOSIT_DISCRIMINATOR = bytes.fromhex("f223c68952e1f2b6")
CANCEL_ORDER_DISCRIMINATOR = bytes.fromhex("5f81edf00831df84")

# VIB-3817 self-check map — see DriftDiscriminatorMismatchError. Pairs every
# vendored discriminator with its source Anchor instruction name so the SDK
# can re-derive and assert at construction time.
DRIFT_INSTRUCTION_NAMES: dict[str, bytes] = {
    "place_perp_order": PLACE_PERP_ORDER_DISCRIMINATOR,
    "initialize_user": INITIALIZE_USER_DISCRIMINATOR,
    "initialize_user_stats": INITIALIZE_USER_STATS_DISCRIMINATOR,
    "deposit": DEPOSIT_DISCRIMINATOR,
    "cancel_order": CANCEL_ORDER_DISCRIMINATOR,
}

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

# -------------------------------------------------------------------------
# Spot Market Registry (index -> symbol)
# -------------------------------------------------------------------------
#
# Drift is a cross-margin protocol: the assets users can deposit as
# collateral are exactly the assets that appear in Drift's on-chain spot
# markets. Any Drift spot-market asset can back a position in ANY Drift
# perp market (the margin engine nets across all registered spot positions).
# As a result there is no per-perp-market collateral rule — only a single
# global allow-list of supported collateral mints.
#
# Source: Drift protocol-v2 SDK, ``sdk/src/constants/spotMarkets.ts``
# (https://github.com/drift-labs/protocol-v2). Indexes below are the stable,
# long-lived mainnet-beta spot markets as of DRIFT_LAYOUT_VERSION. New spot
# markets are onboarded periodically by Drift DAO governance; when a new
# market is added on-chain it MUST be appended here (and verified against
# the SDK) so compile-time collateral validation stays in sync.
#
# Symbols use Drift's canonical casing (e.g. ``mSOL``, ``wBTC``, ``USDC``).
# Case-insensitive comparison is the responsibility of the validator, not
# the table. Consumers that need the uppercase form for lookup should call
# ``symbol.upper()`` at the call site.
SPOT_MARKETS: dict[int, str] = {
    0: "USDC",
    1: "SOL",
    2: "mSOL",
    3: "wBTC",
    4: "wETH",
    5: "USDT",
    6: "jitoSOL",
    7: "PYTH",
    8: "bSOL",
    9: "JTO",
    10: "WIF",
    11: "JUP",
    12: "RENDER",
    13: "W",
    14: "TNSR",
    15: "DRIFT",
    16: "INF",
    17: "dSOL",
    18: "USDY",
    19: "JLP",
    20: "POPCAT",
    21: "CLOUD",
    22: "PYUSD",
    23: "USDe",
    24: "sUSDe",
}


# Reverse mapping: symbol (canonical casing) -> spot market index.
#
# Fail fast on duplicate / case-collision symbols. If two distinct spot-market
# indexes ever map to the same symbol (or to symbols that differ only in
# case, e.g. ``"SOL"`` vs ``"sol"``), the reverse map would silently clobber
# one entry — which would then propagate to ``ALLOWED_COLLATERAL_MINTS`` and
# subtly break collateral validation. Raising at import time forces the
# ambiguity to be resolved in the source table before it can ship.
def _build_spot_market_symbol_to_index() -> dict[str, int]:
    seen_canonical: dict[str, int] = {}
    seen_upper: dict[str, int] = {}
    collisions_canonical: list[tuple[str, int, int]] = []
    collisions_upper: list[tuple[str, int, int]] = []
    for idx, symbol in SPOT_MARKETS.items():
        if symbol in seen_canonical:
            collisions_canonical.append((symbol, seen_canonical[symbol], idx))
        else:
            seen_canonical[symbol] = idx
        upper = symbol.upper()
        if upper in seen_upper:
            collisions_upper.append((upper, seen_upper[upper], idx))
        else:
            seen_upper[upper] = idx
    problems: list[str] = []
    if collisions_canonical:
        problems.extend(f"symbol {sym!r} at indexes {a} and {b}" for sym, a, b in collisions_canonical)
    if collisions_upper:
        # Only report case-collisions that aren't already flagged as exact
        # duplicates above (to keep the error message tight).
        exact = {sym.upper() for sym, _a, _b in collisions_canonical}
        case_only = [c for c in collisions_upper if c[0] not in exact]
        problems.extend(f"case-collision on {sym!r} at indexes {a} and {b}" for sym, a, b in case_only)
    if problems:
        raise ValueError(f"Duplicate spot market symbols detected in SPOT_MARKETS: {'; '.join(problems)}")
    return {v: k for k, v in SPOT_MARKETS.items()}


SPOT_MARKET_SYMBOL_TO_INDEX: dict[str, int] = _build_spot_market_symbol_to_index()

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
# Account Layout Offsets
# =========================================================================
#
# Version: Verified against Drift protocol-v2 IDL
# Program ID: dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH
# Verified date: 2026-03-02
#
# IMPORTANT: If Drift upgrades the program, ALL offsets below MUST be
# re-verified against the new IDL. Steps:
#   1. Fetch the latest IDL: `anchor idl fetch dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH`
#   2. Compute byte offsets from the Anchor User/PerpMarket/SpotMarket structs
#   3. Sanity check: after parsing, verify authority == expected wallet address
#   4. Update DRIFT_LAYOUT_VERSION below to the new program version
#
DRIFT_LAYOUT_VERSION = "2026-03-02"

# --- User Account Layout ---
# Anchor account discriminator = 8 bytes
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

# --- Market Account Layout ---
# PerpMarket: oracle pubkey at offset 48 (after 8-byte discriminator + header fields)
PERP_MARKET_ORACLE_OFFSET = 48
# SpotMarket: oracle pubkey at offset 48 (same position in SpotMarket layout)
# NOTE: If spot market layout diverges from perp in a future upgrade,
# this must be updated separately.
SPOT_MARKET_ORACLE_OFFSET = 48

# =========================================================================
# Data API
# =========================================================================

DRIFT_DATA_API_BASE_URL = os.environ.get("DRIFT_DATA_API_BASE_URL") or "https://data.api.drift.trade"
