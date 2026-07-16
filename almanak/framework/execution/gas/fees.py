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

VIB-5673 — the floor must be *relative*, not a frozen constant:
    The original floors were absolute gwei calibrated against pre-2024 L1
    (base fees 20-50 gwei, where 2 gwei was a ~5% tip). Post-blob L1 sits at
    ~0.16 gwei, which left the 2 gwei floor at **12.5x the base fee** — 86% of
    ``max_fee``. Because ``max(suggestion, floor)`` discards the node's own
    (correct, landable) ~0.05 gwei estimate, and because **the tip is always
    paid** in EIP-1559, every L1 tx overpaid ~10x. The inflated ``max_fee``
    additionally inflated the node's ``balance >= gas_limit * maxFeePerGas +
    value`` admission check, rejecting well-funded wallets with ``-32003``.

    The floor is now ``max(absolute_component, base_fee * MULTIPLIER)``, so it
    scales with congestion and costs ~nothing when the chain is quiet.

Design:
    * The tip floor is owned by the chain descriptor (VIB-4801 SSOT).
      ``min_priority_fee_gwei`` is the floor's **absolute component**;
      ``None`` / ``0`` means the chain declares *no floor policy at all* —
      correct for L2s, whose near-zero base fees let even a zero tip land.
      A chain that declares no policy gets **no relative term either**, so
      L2 behaviour is exactly preserved (a returned ``0`` stays ``0``).
    * Two distinct kinds of floor share this one field, and the distinction
      matters (VIB-5673):
        - **Hard, protocol-enforced minimums** — polygon's ~30 gwei is
          enforced by PoS validators; a tx below it is *dropped*. This is a
          protocol fact, not a heuristic, and must never be undercut. A
          purely relative floor would compute 0.05 * 283.95 = ~14 gwei on
          polygon and silently break every tx — which is why the relative
          term is a ``max`` against the absolute component, never a
          replacement for it.
        - **Soft anti-stall heuristics** — ethereum / avalanche. These are
          the ones that were miscalibrated; they are now small absolute
          components that the relative term lifts under congestion.
    * RPC suggestion ``None`` (the node raised) is treated identically to a
      returned ``0``: there is no usable suggestion, so the floor applies.
      A *usable* (> 0) suggestion is the node's own estimate and is trusted;
      the floor only ever raises it, and post-VIB-5673 the floor is small
      enough that a healthy suggestion wins (preserving VIB-5419's
      "tip > 0 or the tx stalls" invariant without overriding the node).
    * The EIP-1559 invariant ``priority ≤ max_fee`` is enforced here.
    * ``max_fee = 2·base_fee + tip`` — the cushion is **deliberately
      unchanged** (VIB-5673). It is the anti-spike protection that lets a tx
      survive a single base-fee doubling; lowering it recovers only ~7% of
      the required balance, leaves the overpay entirely untouched, and
      strands txs mid-flight when the base fee rises.
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

# VIB-5673: the tip floor's *relative* component, as a fraction of the live
# base fee. A tip worth 5% of the base fee is a meaningful priority signal at
# any base fee, which is precisely what an absolute gwei constant is not: the
# old 2 gwei L1 floor was ~5% of a 2024 base fee but 12.5x a post-blob one.
# Applied only to chains that declare an absolute component (see
# ``priority_fee_floor_wei``), and only ever as a ``max`` against it — so it
# can raise a soft heuristic under congestion but can never undercut a
# protocol-enforced minimum like polygon's.
#
# Declared as ``Decimal`` (not ``float``) so the exact decimal value is the
# literal in the source, rather than being recovered from a float via
# ``Decimal(str(...))`` on every call. This is a money path: the multiplier
# should never round-trip through binary floating point.
PRIORITY_FEE_BASE_FEE_MULTIPLIER = Decimal("0.05")


def priority_fee_floor_wei(chain: str, *, base_fee_wei: int = 0) -> int:
    """Return the effective live priority-fee (tip) floor in wei.

    The floor is ``max(absolute_component, base_fee * MULTIPLIER)`` where the
    absolute component is :attr:`GasProfile.min_priority_fee_gwei` on the chain
    descriptor. Returns ``0`` when the chain is unknown or declares no floor
    (the L2 case) — including no relative term, so the floor stays a
    behaviour-preserving no-op there.

    The relative term (VIB-5673) makes the floor track congestion instead of a
    frozen gwei constant, and is a ``max`` rather than a replacement so that
    protocol-enforced minimums (polygon's validator-gated ~30 gwei) can never
    be undercut.

    Args:
        chain: Chain name / alias (e.g. ``"ethereum"``, ``"base"``).
        base_fee_wei: Latest block base fee per gas, in wei. Defaults to ``0``
            (no relative term → the absolute component alone).

    Returns:
        Floor in wei (``>= 0``).
    """
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None or descriptor.gas is None:
        return 0
    floor_gwei = descriptor.gas.min_priority_fee_gwei
    if not floor_gwei:  # None or 0.0 → chain declares no floor policy at all
        return 0
    # Decimal(str(...)) avoids float drift on sub-gwei descriptor values.
    # (``floor_gwei`` is a float on the descriptor, so it must still be
    # recovered via str(); the multiplier is already an exact Decimal.)
    absolute_wei = int(Decimal(str(floor_gwei)) * _WEI_PER_GWEI)
    base_fee_int = int(base_fee_wei) if base_fee_wei else 0
    relative_wei = int(PRIORITY_FEE_BASE_FEE_MULTIPLIER * base_fee_int)
    return max(absolute_wei, relative_wei)


def build_eip1559_fees(
    *,
    base_fee_wei: int,
    rpc_priority_fee_wei: int | None,
    chain: str,
) -> dict[str, int]:
    """Build EIP-1559 fee fields, flooring the tip to the per-chain descriptor.

    The miner tip is ``max(rpc_suggestion, effective_floor)`` where the
    effective floor is congestion-relative (see :func:`priority_fee_floor_wei`
    and VIB-5673); a ``None`` suggestion (the RPC raised) is treated as "no
    suggestion" and floored. ``max_fee = 2·base_fee + tip`` (the standard
    cushion that survives a single base-fee doubling — deliberately unchanged
    by VIB-5673), and the ``priority ≤ max_fee`` invariant is enforced.

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
    priority_fee = max(suggestion, priority_fee_floor_wei(chain, base_fee_wei=base_fee_int))
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
