"""Silo V2 teardown on-chain closure verifier (VIB-5795).

TD-14 post-condition for Silo V2 lending positions (Avalanche). Positions carry
only the underlying symbol (``details["asset"]``); read targets come from the
connector's own ``_TOKEN_TO_SILO_MAP`` catalogue (symbol → every silo of that
asset across markets). ALL matching silos are read — WAVAX appears in several
markets, and a first-match-only read would miss a residual (or live debt) in a
second market → false CHAIN_VERIFIED.

Reads (per ``position_type``):
  * SUPPLY — Σ ``convertToAssets(balanceOf(wallet))`` per silo. The Silo
    contract IS the ERC-20 borrowable-collateral share token, and its own
    ``convertToAssets`` handles the non-standard virtual-offset rounding (the
    recorded field case: 1,000 leftover shares ≈ 1 wei of USDC → closed).
    Known limitation: Protected-type deposits (``collateralType=0``) mint a
    sibling share token this read cannot see — currently unreachable via
    intents (the compiler never plumbs ``collateral_type``; every adapter
    public method defaults to borrowable collateral). If Protected deposits
    are ever plumbed, this hook needs the ShareProtectedCollateralToken read.
  * BORROW — Σ ``maxRepay(wallet)`` per silo: full outstanding debt in
    underlying units, not liquidity-capped. The in-repo silo strategy is
    supply-only today, but the connector vocabulary is borrow-capable, so the
    debt leg is implemented rather than left as a false-green hole.
"""

from __future__ import annotations

from typing import Any

from almanak.connectors._strategy_base.lending_post_condition import (
    combine_leg_reads,
    read_erc4626_owned_assets,
    read_uint_address_call,
    verify_lending_closure,
)
from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult
from almanak.connectors.silo_v2.adapter import _TOKEN_TO_SILO_MAP, SILO_V2_FUNCTION_SELECTORS

_MAX_REPAY_SELECTOR = SILO_V2_FUNCTION_SELECTORS["max_repay"]

# The silo catalogue is Avalanche-only; any other chain is uncatalogued by
# construction (→ no targets → unmeasured).
_SILO_CHAIN = "avalanche"


def _matching_silo_addresses(chain: str, asset: str) -> list[str]:
    """Every catalogued silo on ``chain`` holding ``asset`` (all markets)."""
    if chain != _SILO_CHAIN:
        return []
    entries = _TOKEN_TO_SILO_MAP.get(asset.upper()) or []
    return [silo_address for _market_name, silo_address, _idx in entries]


def _supply_residual(
    gateway_client: Any, chain: str, asset: str, wallet_address: str, block: int | str | None
) -> int | None:
    targets = _matching_silo_addresses(chain, asset)
    if not targets:
        return None
    return combine_leg_reads(
        [read_erc4626_owned_assets(gateway_client, chain, silo, wallet_address, block) for silo in targets]
    )


def _debt_residual(
    gateway_client: Any, chain: str, asset: str, wallet_address: str, block: int | str | None
) -> int | None:
    targets = _matching_silo_addresses(chain, asset)
    if not targets:
        return None
    return combine_leg_reads(
        [
            read_uint_address_call(gateway_client, chain, silo, _MAX_REPAY_SELECTOR, wallet_address, block)
            for silo in targets
        ]
    )


def silo_v2_teardown_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,  # noqa: ARG001 — protocol signature; gateway boundary: never consumed
    block: int | str | None = None,
) -> ClosureCheckResult:
    """Verify a Silo V2 position is flat on-chain (supply value / debt ≤ dust)."""
    return verify_lending_closure(
        position,
        wallet_address,
        gateway_client,
        block,
        read_supply=_supply_residual,
        read_debt=_debt_residual,
    )


__all__ = ["silo_v2_teardown_post_condition"]
