"""Shared lending teardown on-chain closure template (VIB-5795).

Before this module, no ``ProtocolKind.LENDING`` connector had a TD-14 teardown
post-condition: after the closing intents executed there was no independent
on-chain assertion that the account was actually flat, so every lending teardown
was structurally pinned at ``UNVERIFIED`` (the euler_v2 / silo_v2 field runs
behind VIB-5795 — "0 of 1 position(s) had an on-chain post-condition"). The
TD-15 reconciliation lane cannot stand in: it only demotes a verdict on a
measured residual, never promotes to ``CHAIN_VERIFIED``, and the lending
strategies' synthetic position ids carry no resolvable market key anyway.

Why a shared TEMPLATE + per-connector manifest hooks, NOT a LENDING-kind default
--------------------------------------------------------------------------------
The VAULT and fungible-LP defaults work because ONE read covers the whole kind
(``convertToAssets(balanceOf)`` / LP-token ``balanceOf``). Lending has no single
closure read — ERC-4626 vault (Euler), paired silos (Silo), Compound-V2 snapshot
(Benqi), aToken/debtToken (Aave) are mutually incompatible shapes. A kind-keyed
default would have to dispatch on protocol family inside the framework (the
protocol-literal coupling the ratchet tests forbid) or degrade everything to
``unmeasured``. So each lending connector declares its own hook via the manifest
``teardown_post_condition`` ImportRef (the designed extension point — gmx_v2 /
uniswap_v4 precedent), and this template centralises the parts that must be
correct exactly once: guard rails, per-``position_type`` dispatch, the dust
floor, and Empty ≠ Zero trichotomy.

The debt-leg trap (why dispatch is per position_type)
-----------------------------------------------------
A levered lending strategy snapshots the account as SEPARATE ``PositionInfo``
rows: a SUPPLY (collateral) position AND a BORROW (debt) position. A
supply-only shares read on the BORROW row would report "no shares → closed" and
green-light a teardown that left live debt — a false ``CHAIN_VERIFIED`` on a
position that can be liquidated. This template therefore routes SUPPLY rows to
the connector's supply-residual reader and BORROW rows to its debt-residual
reader, and treats ANY other ``position_type`` as ``unmeasured`` (never a
silent closed=True skip: lending slugs never legitimately emit other types, and
a silent skip is a false-green vector).

Positions carry symbols, not addresses
--------------------------------------
Lending strategies emit synthetic position ids (``"euler_v2-collateral-WETH-
ethereum"``) and ``details={"asset": "<SYMBOL>", ...}`` — no vault / market /
qiToken address. Connector hooks therefore resolve the on-chain read targets
from their OWN adapter catalogues keyed by (chain, asset) — the same
single-sourced tables the intent path compiles against, so the verify read can
never drift from the addresses the strategy actually traded. An uncatalogued
(chain, asset) resolves to no targets → ``unmeasured`` (honest UNVERIFIED),
never a guess.

Closure rule — underlying-asset wei dust floor
----------------------------------------------
Same choice and rationale as VIB-5573's ``_VAULT_ASSET_DUST_WEI``: a clean
"withdraw all" round-trip leaves ~1 wei of underlying regardless of decimals
(the recorded silo_v2 field case: 1,000 leftover shares ≈ $0.000001 → ~1 wei of
USDC — MUST verify closed), so a decimal-blind wei floor is oracle-free and
cannot mask a material strand (the VIB-5573 incident residual was 3.2M wei —
5+ orders above). The same floor applies to the debt leg: repay-in-full zeroes
the borrow balance exactly on all covered protocols, but a 1–2 wei
interest-tick residue must not FAILED-latch (VIB-5572) a clean teardown; 10 wei
of debt is economically nil and cannot compound into anything material.

Gateway boundary: every on-chain read goes through the supplied
``gateway_client`` (``query_erc20_balance`` / ``eth_call``). ``rpc_url`` is
accepted by hooks to satisfy the ``TeardownPostCondition`` protocol but
intentionally NOT consumed. NEVER raises.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Any

from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult
from almanak.connectors._strategy_base.vault_post_condition import (
    _decode_uint256,
    _encode_convert_to_assets,
    _is_evm_address,
    _read_with_retry,
)

logger = logging.getLogger(__name__)

# Underlying-asset wei dust floor for lending closure (supply AND debt legs).
# See module docstring for the full rationale; deliberately the same value as
# VIB-5573's _VAULT_ASSET_DUST_WEI (the residue mechanism — round-trip rounding
# of a full withdraw/repay — is identical, and the floor is decimal-blind).
_LENDING_ASSET_DUST_WEI = 10

# A leg-residual reader: (gateway_client, chain, asset, wallet_address, block)
# -> residual in underlying-asset wei, or None when the value could not be
# measured (uncatalogued (chain, asset), read fault after retry). Empty ≠ Zero:
# None is "don't know", never zero.
LegResidualReader = Callable[[Any, str, str, str, int | str | None], int | None]


def combine_leg_reads(values: Sequence[int | None]) -> int | None:
    """Combine per-target residual reads for one leg into a single verdict value.

    A leg can span several catalogued targets (e.g. every Euler vault whose
    underlying matches the position's asset — reading only the "preferred"
    vault would miss a residual parked in a sibling vault → false closed).

    Fault-dominance lattice (Empty ≠ Zero):
      * no targets → ``None`` (nothing measurable — caller marks unmeasured);
      * all targets measured → their sum (0 is a MEASURED zero);
      * some target faulted → ``None`` (can't prove closed while a target is
        unreadable) — UNLESS the measured partial sum already exceeds the dust
        floor, in which case the measured residual is decisive evidence of
        non-closure and is returned (a read fault elsewhere must not downgrade
        a measured strand from FAILED to UNVERIFIED).
    """
    if not values:
        return None
    measured = [v for v in values if v is not None]
    total = sum(measured)
    if len(measured) < len(values):
        return total if total > _LENDING_ASSET_DUST_WEI else None
    return total


def read_erc4626_owned_assets(
    gateway_client: Any,
    chain: str,
    vault_address: str,
    wallet_address: str,
    block: int | str | None,
) -> int | None:
    """Underlying-asset value of the wallet's ERC-4626 shares in one vault.

    ``balanceOf(owner)`` (the vault contract IS the ERC-20 share token) chained
    into ``convertToAssets(shares)`` — total value still owned, NOT
    ``maxWithdraw`` (which is capped at vault liquidity: a fully-utilised vault
    reads 0 for a live position → false closed). ``None`` on any read fault
    (never a fabricated value).
    """
    if not _is_evm_address(vault_address):
        return None
    shares = _read_with_retry(
        lambda: gateway_client.query_erc20_balance(
            chain=chain,
            token_address=vault_address,
            wallet_address=wallet_address,
            block=block,
        )
    )
    try:
        shares = int(shares) if shares is not None else None
    except (TypeError, ValueError):
        shares = None
    if shares is None:
        return None
    if shares == 0:
        return 0
    raw = _read_with_retry(
        lambda: gateway_client.eth_call(
            chain=chain,
            to=vault_address,
            data=_encode_convert_to_assets(shares),
            block=block,
        )
    )
    return _decode_uint256(raw)


def read_uint_address_call(
    gateway_client: Any,
    chain: str,
    to: str,
    selector: str,
    wallet_address: str,
    block: int | str | None,
) -> int | None:
    """Single-``uint256`` view read of ``selector(address wallet)`` on ``to``.

    Generic building block for debt reads (Euler ``debtOf``, Silo ``maxRepay``).
    ``None`` on any fault; strict single-word decode (a revert-shaped payload is
    a fault, never a value).
    """
    if not _is_evm_address(to) or not _is_evm_address(wallet_address):
        return None
    data = selector + f"{int(wallet_address, 16):064x}"
    raw = _read_with_retry(lambda: gateway_client.eth_call(chain=chain, to=to, data=data, block=block))
    return _decode_uint256(raw)


def verify_lending_closure(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None,
    block: int | str | None,
    *,
    read_supply: LegResidualReader,
    read_debt: LegResidualReader,
) -> ClosureCheckResult:
    """Shared TD-14 closure check for a lending ``PositionInfo``.

    Dispatches on ``position.position_type``: SUPPLY → ``read_supply``,
    BORROW → ``read_debt``, anything else → unmeasured. A reader returning
    ``None`` is unmeasured (→ UNVERIFIED at the seam), a value ≤
    ``_LENDING_ASSET_DUST_WEI`` is closed, and a larger value is a MEASURED
    residual (→ FAILED at the seam). Never raises.
    """
    protocol = (getattr(position, "protocol", "") or "").lower() or "lending"
    position_id = str(getattr(position, "position_id", "") or "")

    chain = getattr(position, "chain", None) or ""
    if not chain:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=f"{protocol} post-condition needs position.chain; none found — cannot verify (unmeasured)",
        )
    if gateway_client is None:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"{protocol} post-condition requires a gateway_client to read on-chain "
                "residual; none supplied — cannot verify (unmeasured)"
            ),
        )

    raw_details = getattr(position, "details", None)
    # A truthy non-mapping (list / str) must degrade to unmeasured via the
    # missing-asset guard below, never raise off ``.get`` — the hook contract
    # is "never raises" (CodeRabbit, PR #3336).
    details = raw_details if isinstance(raw_details, dict) else {}
    asset = details.get("asset")
    if not isinstance(asset, str) or not asset.strip():
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"{protocol} post-condition needs details['asset'] (underlying symbol) to "
                f"resolve the market; found {asset!r} — cannot verify (unmeasured)"
            ),
        )
    asset = asset.strip()

    # PositionType is a StrEnum, so plain string comparison is exact; the
    # string form also keeps this connector-layer module free of framework
    # imports (framework → connector is the only allowed direction).
    position_type = str(getattr(position, "position_type", "") or "").upper()
    if position_type == "SUPPLY":
        leg, reader = "supply", read_supply
    elif position_type == "BORROW":
        leg, reader = "debt", read_debt
    else:
        # NOT a silent closed=True skip — lending slugs never legitimately
        # emit other position types, and a silent skip is a false-green vector.
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"{protocol} post-condition has no closure read for "
                f"position_type={position_type!r} (chain={chain}, asset={asset}) — "
                "cannot verify (unmeasured)"
            ),
        )

    try:
        residual = reader(gateway_client, chain, asset, wallet_address, block)
    except Exception as exc:  # noqa: BLE001 — a hook must never raise; a raise is a read fault
        logger.debug("%s %s-leg residual reader raised: %s", protocol, leg, exc)
        residual = None
    if residual is None:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"{protocol} {leg}-leg residual for asset {asset} on {chain} could not be "
                "measured (uncatalogued market or gateway/RPC fault after retry); "
                "cannot confirm closure — unmeasured"
            ),
        )
    if residual <= _LENDING_ASSET_DUST_WEI:
        return ClosureCheckResult(closed=True, protocol=protocol, position_id=position_id)
    return ClosureCheckResult(
        closed=False,
        protocol=protocol,
        position_id=position_id,
        residual={"asset": asset, "leg": leg, "residual_wei": residual},
    )


__all__ = [
    "combine_leg_reads",
    "read_erc4626_owned_assets",
    "read_uint_address_call",
    "verify_lending_closure",
]
