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

        Works with both the current datapi.meteora.ag /pools endpoint
        (which nests token info under token_x/token_y objects) and the
        legacy flat-field format.
        """
        # New API nests token info under token_x/token_y objects
        token_x_obj = data.get("token_x", {}) if isinstance(data.get("token_x"), dict) else {}
        token_y_obj = data.get("token_y", {}) if isinstance(data.get("token_y"), dict) else {}

        # Extract mint addresses: new API (token_x.address) or legacy (mint_x/mintX)
        mint_x = token_x_obj.get("address", "") or data.get("mint_x", data.get("mintX", ""))
        mint_y = token_y_obj.get("address", "") or data.get("mint_y", data.get("mintY", ""))

        # Extract decimals: new API (token_x.decimals) or legacy fields
        # Use explicit None checks because API may return null for these keys
        _dx = token_x_obj.get("decimals")
        decimals_x = _dx if _dx is not None else (data.get("mint_x_decimals") or data.get("decimals_x") or 9)
        _dy = token_y_obj.get("decimals")
        decimals_y = _dy if _dy is not None else (data.get("mint_y_decimals") or data.get("decimals_y") or 6)

        # Extract symbols: new API (token_x.symbol) or parse from name
        symbol_x = token_x_obj.get("symbol", "")
        symbol_y = token_y_obj.get("symbol", "")
        if not symbol_x and data.get("name"):
            symbol_x = data["name"].split("-")[0].strip()
        if not symbol_y and data.get("name") and "-" in data.get("name", ""):
            symbol_y = data["name"].split("-")[1].strip()

        # Extract bin_step: new API nests under pool_config
        pool_config = data.get("pool_config", {}) if isinstance(data.get("pool_config"), dict) else {}
        bin_step = int(pool_config.get("bin_step") or data.get("bin_step") or 10)

        # Extract fee: new API uses pool_config.base_fee_pct (percentage, not bps)
        base_fee_pct = pool_config.get("base_fee_pct") or 0
        fee_val = data.get("base_fee_percentage") or data.get("fee_bps") or 0
        fee_bps = int(float(fee_val))
        if not fee_bps and base_fee_pct:
            fee_bps = round(float(base_fee_pct) * 100)  # pct -> bps

        return cls(
            address=data.get("address", data.get("pair_address", "")),
            mint_x=mint_x,
            mint_y=mint_y,
            symbol_x=symbol_x,
            symbol_y=symbol_y,
            decimals_x=int(decimals_x),
            decimals_y=int(decimals_y),
            bin_step=bin_step,
            active_bin_id=int(data.get("active_id", data.get("activeId", 0))),
            current_price=float(data.get("current_price", 0)),
            tvl=float(data.get("liquidity", data.get("tvl", 0))),
            reserve_x=str(data.get("reserve_x") or data.get("reserve_x_amount") or data.get("token_x_amount") or "0"),
            reserve_y=str(data.get("reserve_y") or data.get("reserve_y_amount") or data.get("token_y_amount") or "0"),
            fee_bps=fee_bps,
            vault_x=data.get("reserve_x_address") or data.get("vault_x") or "",
            vault_y=data.get("reserve_y_address") or data.get("vault_y") or "",
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
