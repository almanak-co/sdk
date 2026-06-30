"""Shared EIP-1559 live-submit fee builder (VIB-5419).

Two byte-identical copies of the live gas-price math used to live in
``orchestrator.get_gas_price`` and ``chain_executor.get_gas_params``. Both
floored the priority fee (miner tip) to 1 gwei **only when the RPC raised**,
never when the node returned a legitimate ``0`` — common on Ethereum L1. The
result on L1 was ``maxPriorityFeePerGas≈0`` with ``maxFee = 2·base_fee + 0``,
which stalls or drops the moment the base fee rises (it stalled a real
euler_v2 first deposit on mainnet).

This module is the single source of truth for the floor + max-fee math, so a
node returning ``0`` (or raising) is floored to the **per-chain descriptor's
live floor** (:attr:`GasProfile.min_priority_fee_gwei`) rather than a magic
1-gwei constant. Both call sites delegate here, killing the duplication.

Design:
    * The tip floor is owned by the chain descriptor (VIB-4801 SSOT) — L1 has
      a real floor (~2 gwei); L2s declare ``None`` / ``0`` because their
      near-zero base fees let even a zero tip land, so the floor is a no-op
      and behaviour is preserved.
    * RPC suggestion ``None`` (the node raised) is treated identically to a
      returned ``0``: there is no usable suggestion, so the floor applies.
    * The EIP-1559 invariant ``priority ≤ max_fee`` is enforced here.
    * The post-build gas-price *cap* is intentionally NOT applied here — it
      differs per call site (chain_executor caps with a WARNING; the
      orchestrator path does not cap in this method). Callers apply their own
      cap after this helper; ``chain_executor`` re-applies the
      ``priority ≤ max_fee`` invariant after capping (VIB-1605).
"""

from __future__ import annotations

from decimal import Decimal

from almanak.core.chains import ChainRegistry

_WEI_PER_GWEI = 10**9


def priority_fee_floor_wei(chain: str) -> int:
    """Return the per-chain live priority-fee (tip) floor in wei.

    Sourced from :attr:`GasProfile.min_priority_fee_gwei` on the chain
    descriptor. Returns ``0`` when the chain is unknown or declares no floor
    (the L2 case), which makes the floor a behaviour-preserving no-op.

    Args:
        chain: Chain name / alias (e.g. ``"ethereum"``, ``"base"``).

    Returns:
        Floor in wei (``>= 0``).
    """
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None or descriptor.gas is None:
        return 0
    floor_gwei = descriptor.gas.min_priority_fee_gwei
    if not floor_gwei:  # None or 0.0
        return 0
    # Decimal(str(...)) avoids float drift on sub-gwei descriptor values.
    return int(Decimal(str(floor_gwei)) * _WEI_PER_GWEI)


def build_eip1559_fees(
    *,
    base_fee_wei: int,
    rpc_priority_fee_wei: int | None,
    chain: str,
) -> dict[str, int]:
    """Build EIP-1559 fee fields, flooring the tip to the per-chain descriptor.

    The miner tip is ``max(rpc_suggestion, descriptor_floor)``; a ``None``
    suggestion (the RPC raised) is treated as "no suggestion" and floored.
    ``max_fee = 2·base_fee + tip`` (the standard cushion that survives a single
    base-fee doubling), and the ``priority ≤ max_fee`` invariant is enforced.

    The post-build gas-price cap is the caller's responsibility (see module
    docstring) — this helper returns uncapped fees.

    Args:
        base_fee_wei: Latest block base fee per gas, in wei.
        rpc_priority_fee_wei: The node's ``eth_maxPriorityFeePerGas``
            suggestion in wei, or ``None`` if the call raised / is unsupported.
        chain: Chain name / alias used to resolve the descriptor floor.

    Returns:
        ``{"max_fee_per_gas", "max_priority_fee_per_gas", "base_fee_per_gas"}``
        in wei.
    """
    base_fee_int = int(base_fee_wei) if base_fee_wei else 0
    suggestion = int(rpc_priority_fee_wei) if rpc_priority_fee_wei else 0
    priority_fee = max(suggestion, priority_fee_floor_wei(chain))
    max_fee = base_fee_int * 2 + priority_fee
    # EIP-1559 requires priority <= max_fee. With no cap applied here this is
    # always already true, but keep it explicit so callers that cap afterwards
    # inherit a well-formed starting point.
    priority_fee = min(priority_fee, max_fee)
    return {
        "max_fee_per_gas": max_fee,
        "max_priority_fee_per_gas": priority_fee,
        "base_fee_per_gas": base_fee_int,
    }
