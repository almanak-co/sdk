"""Euler V2 teardown on-chain closure verifier (VIB-5795).

TD-14 post-condition for Euler V2 lending positions. Positions carry only the
underlying symbol (``details["asset"]``), so read targets are resolved from the
connector's own ``EULER_V2_VAULTS_BY_CHAIN`` catalogue — the same table the
intent path compiles against, so the verify read cannot drift from the vaults
the strategy actually used.

Read targets are ALL catalogued vaults whose underlying matches the asset, not
just the ``preferred`` one: a residual parked in a sibling vault (e.g. after a
catalogue preference change mid-position) must be caught, and a couple of extra
pinned eth_calls are cheap on a once-per-teardown path.

Reads (per ``position_type``):
  * SUPPLY — Σ ``convertToAssets(balanceOf(wallet))`` over matching vaults.
    Euler EVaults are genuine ERC-4626. Deliberately NOT ``maxWithdraw`` (the
    account-state spec's read): ``maxWithdraw`` is capped at vault cash, so a
    fully-utilised vault reads 0 for a live position → false CHAIN_VERIFIED.
  * BORROW — Σ ``debtOf(wallet)`` over matching vaults (debt lives on the
    borrowed asset's controller vault, in underlying units; exact, not
    liquidity-capped).

Sub-accounts: the in-repo Euler compiler only ever operates the main account
(no EVC sub-account addressing), so a wallet-address read is complete. If
sub-accounts are ever plumbed, this hook must read per sub-account or return
unmeasured.
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
from almanak.connectors.euler_v2.adapter import DEBT_OF_SELECTOR, EULER_V2_VAULTS_BY_CHAIN


def _matching_vault_addresses(chain: str, asset: str) -> list[str]:
    """Every catalogued vault on ``chain`` whose underlying symbol is ``asset``."""
    vaults = EULER_V2_VAULTS_BY_CHAIN.get(chain) or {}
    wanted = asset.lower()
    return [
        str(entry.get("vault_address") or "")
        for entry in vaults.values()
        if str(entry.get("underlying_symbol") or "").lower() == wanted
    ]


def _supply_residual(
    gateway_client: Any, chain: str, asset: str, wallet_address: str, block: int | str | None
) -> int | None:
    targets = _matching_vault_addresses(chain, asset)
    if not targets:
        return None
    return combine_leg_reads(
        [read_erc4626_owned_assets(gateway_client, chain, vault, wallet_address, block) for vault in targets]
    )


def _debt_residual(
    gateway_client: Any, chain: str, asset: str, wallet_address: str, block: int | str | None
) -> int | None:
    targets = _matching_vault_addresses(chain, asset)
    if not targets:
        return None
    return combine_leg_reads(
        [
            read_uint_address_call(gateway_client, chain, vault, DEBT_OF_SELECTOR, wallet_address, block)
            for vault in targets
        ]
    )


def euler_v2_teardown_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,  # noqa: ARG001 — protocol signature; gateway boundary: never consumed
    block: int | str | None = None,
) -> ClosureCheckResult:
    """Verify an Euler V2 position is flat on-chain (supply value / debt ≤ dust)."""
    return verify_lending_closure(
        position,
        wallet_address,
        gateway_client,
        block,
        read_supply=_supply_residual,
        read_debt=_debt_residual,
    )


__all__ = ["euler_v2_teardown_post_condition"]
