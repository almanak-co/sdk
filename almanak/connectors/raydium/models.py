"""Raydium CLMM data models.

Dataclasses for pool state, position state, and transaction responses.
All models include factory methods for construction from API responses
and on-chain account data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RaydiumPool:
    """Raydium CLMM pool information.

    Can be constructed from the Raydium API response or on-chain data.

    Attributes:
        address: Pool state account address (Base58).
        mint_a: Token A mint address.
        mint_b: Token B mint address.
        symbol_a: Token A symbol (e.g., "SOL").
        symbol_b: Token B symbol (e.g., "USDC").
        decimals_a: Token A decimals.
        decimals_b: Token B decimals.
        tick_spacing: Tick spacing for this pool.
        current_price: Current price of token A in terms of token B.
        tvl: Total value locked in USD.
        vault_a: Token A vault address.
        vault_b: Token B vault address.
        amm_config: AMM config account address.
        fee_rate: Fee rate in basis points (e.g., 3000 = 0.30%).
        observation_address: Observation account address.
        program_id: CLMM program ID.
    """

    address: str
    mint_a: str
    mint_b: str
    symbol_a: str = ""
    symbol_b: str = ""
    decimals_a: int = 9
    decimals_b: int = 6
    tick_spacing: int = 60
    current_price: float = 0.0
    tvl: float = 0.0
    vault_a: str = ""
    vault_b: str = ""
    amm_config: str = ""
    fee_rate: int = 3000
    observation_address: str = ""
    program_id: str = ""
    raw_response: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> RaydiumPool:
        """Create from Raydium API /pools/info/list response item."""
        mint_a = data.get("mintA", {})
        mint_b = data.get("mintB", {})
        config = data.get("config", {})

        return cls(
            address=data.get("id", ""),
            mint_a=mint_a.get("address", ""),
            mint_b=mint_b.get("address", ""),
            symbol_a=mint_a.get("symbol", ""),
            symbol_b=mint_b.get("symbol", ""),
            decimals_a=mint_a.get("decimals", 9),
            decimals_b=mint_b.get("decimals", 6),
            tick_spacing=config.get("tickSpacing", 60),
            current_price=float(data.get("price", 0)),
            tvl=float(data.get("tvl", 0)),
            vault_a=data.get("mintVaultA", data.get("vault", {}).get("A", "")),
            vault_b=data.get("mintVaultB", data.get("vault", {}).get("B", "")),
            amm_config=config.get("id", ""),
            fee_rate=int(config.get("tradeFeeRate", 3000)),
            observation_address=data.get("observationId", ""),
            program_id=data.get("programId", ""),
            raw_response=data,
        )


@dataclass
class RaydiumPosition:
    """Raydium CLMM position (owned by the user).

    Attributes:
        nft_mint: Position NFT mint address.
        pool_address: Pool state account address.
        tick_lower: Lower tick boundary.
        tick_upper: Upper tick boundary.
        liquidity: Current liquidity in the position.
        token_fees_owed_a: Accumulated fees for token A.
        token_fees_owed_b: Accumulated fees for token B.
        personal_position_address: PersonalPositionState PDA address.
    """

    nft_mint: str
    pool_address: str
    tick_lower: int
    tick_upper: int
    liquidity: int = 0
    token_fees_owed_a: int = 0
    token_fees_owed_b: int = 0
    personal_position_address: str = ""


@dataclass
class RaydiumTransactionBundle:
    """Bundle of serialized transactions for a Raydium operation.

    Attributes:
        transactions: List of base64-encoded VersionedTransactions.
        action: Action type ("open_position", "increase_liquidity", etc.).
        position_nft_mint: NFT mint address (for open_position).
        metadata: Additional metadata.
    """

    transactions: list[str]
    action: str
    position_nft_mint: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
