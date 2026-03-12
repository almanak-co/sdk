"""Meteora DLMM data models.

Dataclasses for pool state, position state, and bin information.
Models include factory methods for construction from API responses
and on-chain account data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MeteoraBin:
    """A single bin in a DLMM pool.

    Attributes:
        bin_id: Bin identifier.
        amount_x: Amount of token X in smallest units.
        amount_y: Amount of token Y in smallest units.
        price: Price at this bin (token Y per token X).
    """

    bin_id: int
    amount_x: int = 0
    amount_y: int = 0
    price: float = 0.0


@dataclass
class MeteoraPool:
    """Meteora DLMM pool information.

    Can be constructed from the DLMM API response.

    Attributes:
        address: Pool (lb_pair) account address (Base58).
        mint_x: Token X mint address.
        mint_y: Token Y mint address.
        symbol_x: Token X symbol (e.g., "SOL").
        symbol_y: Token Y symbol (e.g., "USDC").
        decimals_x: Token X decimals.
        decimals_y: Token Y decimals.
        bin_step: Bin step in basis points.
        active_bin_id: Currently active bin ID.
        current_price: Current price of token X in terms of token Y.
        tvl: Total value locked in USD.
        reserve_x: Token X reserve in pool.
        reserve_y: Token Y reserve in pool.
        fee_bps: Base fee in basis points.
        vault_x: Token X vault address.
        vault_y: Token Y vault address.
        oracle_address: Oracle PDA address.
        raw_response: Full API response dict.
    """

    address: str
    mint_x: str
    mint_y: str
    symbol_x: str = ""
    symbol_y: str = ""
    decimals_x: int = 9
    decimals_y: int = 6
    bin_step: int = 10
    active_bin_id: int = 0
    current_price: float = 0.0
    tvl: float = 0.0
    reserve_x: str = "0"
    reserve_y: str = "0"
    fee_bps: int = 0
    vault_x: str = ""
    vault_y: str = ""
    oracle_address: str = ""
    raw_response: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> MeteoraPool:
        """Create from Meteora DLMM API response.

        Works with both /pair/{address} and /pair/all_with_pagination items.
        """
        # Extract mint info
        mint_x = data.get("mint_x", data.get("mintX", ""))
        mint_y = data.get("mint_y", data.get("mintY", ""))

        return cls(
            address=data.get("address", data.get("pair_address", "")),
            mint_x=mint_x,
            mint_y=mint_y,
            symbol_x=data.get("name", "").split("-")[0].strip() if data.get("name") else "",
            symbol_y=data.get("name", "").split("-")[1].strip()
            if data.get("name") and "-" in data.get("name", "")
            else "",
            decimals_x=int(data.get("mint_x_decimals", data.get("decimals_x", 9))),
            decimals_y=int(data.get("mint_y_decimals", data.get("decimals_y", 6))),
            bin_step=int(data.get("bin_step", 10)),
            active_bin_id=int(data.get("active_id", data.get("activeId", 0))),
            current_price=float(data.get("current_price", 0)),
            tvl=float(data.get("liquidity", data.get("tvl", 0))),
            reserve_x=str(data.get("reserve_x", data.get("reserve_x_amount", "0"))),
            reserve_y=str(data.get("reserve_y", data.get("reserve_y_amount", "0"))),
            fee_bps=int(data.get("base_fee_percentage", data.get("fee_bps", 0))),
            vault_x=data.get("reserve_x_address", data.get("vault_x", "")),
            vault_y=data.get("reserve_y_address", data.get("vault_y", "")),
            oracle_address=data.get("oracle", ""),
            raw_response=data,
        )


@dataclass
class MeteoraPosition:
    """Meteora DLMM position state (on-chain).

    Unlike Raydium CLMM (NFT-based), Meteora positions are
    non-transferable program accounts identified by their address.

    Attributes:
        position_address: Position account address (Base58).
        lb_pair: Pool address.
        owner: Owner wallet address.
        lower_bin_id: Lower bin ID of the position range.
        upper_bin_id: Upper bin ID of the position range.
        bins: List of bins with amounts.
        total_x: Total token X in position.
        total_y: Total token Y in position.
    """

    position_address: str
    lb_pair: str
    owner: str = ""
    lower_bin_id: int = 0
    upper_bin_id: int = 0
    bins: list[MeteoraBin] = field(default_factory=list)
    total_x: int = 0
    total_y: int = 0
