"""Connector-local fungible-LP valuation for Fluid SmartLending (VIB-5032).

Registers a builder with the framework :class:`FungibleLpPositionReader` so the
portfolio valuer can mark an open ``fluid_dex_lp`` position from live on-chain
state: share balance → per-share (token0, token1) claim via the SmartLending
resolver (gateway-routed). Two-legged; the framework prices each leg.
"""

from __future__ import annotations

import logging

from almanak.connectors.fluid.addresses import FLUID_DEX_LP, FLUID_SMARTLENDING_MARKETS
from almanak.framework.valuation.fungible_lp_position_reader import (
    FungibleLpPosition,
    register_fungible_lp_reader,
)

logger = logging.getLogger(__name__)


def read_fungible_lp_position(
    gateway_client: object,
    chain: str,
    wrapper: str,
    wallet_address: str,
) -> FungibleLpPosition | None:
    """Read a Fluid SmartLending LP position (share balance → token0/token1)."""
    chain_lower = (chain or "").lower()
    entry = FLUID_SMARTLENDING_MARKETS.get(chain_lower, {}).get((wrapper or "").lower())
    resolver_addrs = FLUID_DEX_LP.get(chain_lower)
    if entry is None or resolver_addrs is None:
        return None

    from almanak.connectors.fluid.smart_lending_sdk import FluidSmartLendingSDK

    sdk = FluidSmartLendingSDK(
        chain=chain_lower,
        resolver_address=resolver_addrs["smart_lending_resolver"],
        gateway_client=gateway_client,  # type: ignore[arg-type]
    )
    # Token addresses let the valuer price each leg BY ADDRESS (engages the
    # oracle's CoinGecko/DexScreener by-address paths — a bare symbol does not).
    token0_address = str(entry["token0"])
    token1_address = str(entry["token1"])
    shares = sdk.get_share_balance(wrapper, wallet_address)
    if shares <= 0:
        return FungibleLpPosition(
            wrapper=wrapper,
            token0_symbol=str(entry["token0_symbol"]),
            token1_symbol=str(entry["token1_symbol"]),
            token0_decimals=int(entry["token0_decimals"]),
            token1_decimals=int(entry["token1_decimals"]),
            amount0_wei=0,
            amount1_wei=0,
            shares_wei=0,
            token0_address=token0_address,
            token1_address=token1_address,
        )
    amount0, amount1 = sdk.position_token_amounts(wrapper, shares)
    return FungibleLpPosition(
        wrapper=wrapper,
        token0_symbol=str(entry["token0_symbol"]),
        token1_symbol=str(entry["token1_symbol"]),
        token0_decimals=int(entry["token0_decimals"]),
        token1_decimals=int(entry["token1_decimals"]),
        amount0_wei=amount0,
        amount1_wei=amount1,
        shares_wei=shares,
        token0_address=token0_address,
        token1_address=token1_address,
    )


register_fungible_lp_reader("fluid_dex_lp", read_fungible_lp_position)

__all__ = ["read_fungible_lp_position"]
