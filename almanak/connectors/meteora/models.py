"""Meteora DLMM data models.

Dataclasses for pool state, position state, and bin information.
Models include factory methods for construction from API responses
and on-chain account data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _nested_dict(data: dict[str, Any], key: str) -> dict[str, Any]:
    """Return data[key] when it is a dict, else an empty dict."""
    value = data.get(key)
    return value if isinstance(value, dict) else {}


def _first_truthy(*values: Any, default: Any) -> Any:
    """First truthy value, else default — replaces literal-tailed `or` chains."""
    for value in values:
        if value:
            return value
    return default


def _extract_mint(token_obj: dict[str, Any], data: dict[str, Any], snake_key: str, camel_key: str) -> str:
    """Mint address: new API (token_x.address) or legacy (mint_x/mintX).

    The legacy tail is a key-presence fallback (not a truthy chain), so a
    present-but-falsy legacy value passes through unchanged.
    """
    return token_obj.get("address", "") or data.get(snake_key, data.get(camel_key, ""))


def _extract_decimals(
    token_obj: dict[str, Any],
    data: dict[str, Any],
    primary_key: str,
    fallback_key: str,
    default: int,
) -> int:
    """Decimals: nested value wins unless None (0 is a measured value)."""
    value = token_obj.get("decimals")
    if value is not None:
        return int(value)
    return int(_first_truthy(data.get(primary_key), data.get(fallback_key), default=default))


def _extract_symbols(token_x_obj: dict[str, Any], token_y_obj: dict[str, Any], name: Any) -> tuple[str, str]:
    """Symbols: new API (token_x.symbol) or parsed from a "X-Y" pool name."""
    symbol_x = token_x_obj.get("symbol", "")
    symbol_y = token_y_obj.get("symbol", "")
    if not symbol_x and name:
        symbol_x = name.split("-")[0].strip()
    if not symbol_y and name and "-" in name:
        symbol_y = name.split("-")[1].strip()
    return symbol_x, symbol_y


def _extract_fee_bps(pool_config: dict[str, Any], data: dict[str, Any]) -> int:
    """Fee in bps: flat base_fee_percentage/fee_bps beats pool_config.base_fee_pct.

    The pool_config value is a percentage, converted pct -> bps only when the
    flat fields coerce to zero.
    """
    base_fee_pct = pool_config.get("base_fee_pct") or 0
    fee_val = _first_truthy(data.get("base_fee_percentage"), data.get("fee_bps"), default=0)
    fee_bps = int(float(fee_val))
    if not fee_bps and base_fee_pct:
        fee_bps = round(float(base_fee_pct) * 100)  # pct -> bps
    return fee_bps


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
        # New API nests token info under token_x/token_y/pool_config objects
        token_x_obj = _nested_dict(data, "token_x")
        token_y_obj = _nested_dict(data, "token_y")
        pool_config = _nested_dict(data, "pool_config")
        symbol_x, symbol_y = _extract_symbols(token_x_obj, token_y_obj, data.get("name"))

        return cls(
            address=data.get("address", data.get("pair_address", "")),
            mint_x=_extract_mint(token_x_obj, data, "mint_x", "mintX"),
            mint_y=_extract_mint(token_y_obj, data, "mint_y", "mintY"),
            symbol_x=symbol_x,
            symbol_y=symbol_y,
            decimals_x=_extract_decimals(token_x_obj, data, "mint_x_decimals", "decimals_x", 9),
            decimals_y=_extract_decimals(token_y_obj, data, "mint_y_decimals", "decimals_y", 6),
            bin_step=int(_first_truthy(pool_config.get("bin_step"), data.get("bin_step"), default=10)),
            active_bin_id=int(data.get("active_id", data.get("activeId", 0))),
            current_price=float(data.get("current_price", 0)),
            tvl=float(data.get("liquidity", data.get("tvl", 0))),
            reserve_x=str(
                _first_truthy(
                    data.get("reserve_x"),
                    data.get("reserve_x_amount"),
                    data.get("token_x_amount"),
                    default="0",
                )
            ),
            reserve_y=str(
                _first_truthy(
                    data.get("reserve_y"),
                    data.get("reserve_y_amount"),
                    data.get("token_y_amount"),
                    default="0",
                )
            ),
            fee_bps=_extract_fee_bps(pool_config, data),
            vault_x=_first_truthy(data.get("reserve_x_address"), data.get("vault_x"), default=""),
            vault_y=_first_truthy(data.get("reserve_y_address"), data.get("vault_y"), default=""),
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
