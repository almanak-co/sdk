"""Orca Whirlpools data models.

Dataclasses for pool state, position state, and transaction responses.
All models include factory methods for construction from API responses.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class OrcaPool:
    """Orca Whirlpool pool information.

    Constructed from the Orca API response or on-chain data.

    Attributes:
        address: Whirlpool account address (Base58).
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
        fee_rate: Fee rate in basis points.
        tick_current_index: Current tick index.
        sqrt_price: Current sqrt price as string (u128).
        oracle_address: Oracle account address.
    """

    address: str
    mint_a: str
    mint_b: str
    symbol_a: str = ""
    symbol_b: str = ""
    decimals_a: int = 9
    decimals_b: int = 6
    tick_spacing: int = 64
    current_price: float = 0.0
    tvl: float = 0.0
    vault_a: str = ""
    vault_b: str = ""
    fee_rate: int = 3000
    tick_current_index: int = 0
    sqrt_price: str = "0"
    oracle_address: str = ""
    raw_response: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> OrcaPool:
        """Create from Orca API /pools/{address} response."""
        token_a = data.get("tokenA", {})
        token_b = data.get("tokenB", {})

        return cls(
            address=data.get("address", ""),
            mint_a=token_a.get("mint", ""),
            mint_b=token_b.get("mint", ""),
            symbol_a=token_a.get("symbol", ""),
            symbol_b=token_b.get("symbol", ""),
            decimals_a=token_a.get("decimals", 9),
            decimals_b=token_b.get("decimals", 6),
            tick_spacing=data.get("tickSpacing", 64),
            current_price=float(data.get("price", 0)),
            tvl=float(data.get("tvl", 0)),
            vault_a=token_a.get("vault", ""),
            vault_b=token_b.get("vault", ""),
            fee_rate=int(data.get("feeRate", 3000)),
            tick_current_index=data.get("tickCurrentIndex", 0),
            sqrt_price=str(data.get("sqrtPrice", "0")),
            oracle_address=data.get("oracle", ""),
            raw_response=data,
        )


@dataclass
class OrcaPosition:
    """Orca Whirlpool position (owned by the user).

    Attributes:
        nft_mint: Position NFT mint address.
        pool_address: Whirlpool account address.
        tick_lower: Lower tick boundary.
        tick_upper: Upper tick boundary.
        liquidity: Current liquidity in the position.
        position_address: Position PDA address.
    """

    nft_mint: str
    pool_address: str
    tick_lower: int
    tick_upper: int
    liquidity: int = 0
    position_address: str = ""


@dataclass
class OrcaTransactionBundle:
    """Bundle of serialized transactions for an Orca operation.

    Attributes:
        transactions: List of base64-encoded VersionedTransactions.
        action: Action type ("open_position", "close_position", etc.).
        position_nft_mint: NFT mint address (for open_position).
        metadata: Additional metadata.
    """

    transactions: list[str]
    action: str
    position_nft_mint: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
