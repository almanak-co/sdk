"""Generic fungible-ERC-20-LP teardown on-chain closure verifier (VIB-5795 / VIB-5896).

Before this hook, fungible-LP venues (Curve StableSwap/CryptoSwap, Fluid DEX
smart-lending shares — ``ProtocolKind.LP`` connectors with ``fungible_lp=True``)
had NO post-teardown on-chain authority: the TD-14 registry had no hook under
their slugs, both verify lanes correctly skipped them, and every teardown was
structurally pinned at ``UNVERIFIED`` ("closed-by-execution, not
chain-confirmed") — a false-negative for a genuinely clean close, and worse, a
residual position was invisible. Observed on the ``20260718-0026-noneth-fringe``
quant-test: a Curve 3pool LP_CLOSE with ``3Crv balanceOf == 0`` on-fork still
reported UNVERIFIED (the sealed auditor had to re-derive closure by hand).

This is a **framework default keyed on the fungible-LP capability** (registered
in ``almanak.framework.teardown.post_conditions``), the fungible-LP analogue of
the ERC-4626 vault default (VIB-5573) and the Uniswap-V3-NPM default — NOT a
per-connector hook — so every current and future fungible-LP connector is
covered by one implementation. A connector-published manifest
``teardown_post_condition`` always wins over this default.

Closure rule
------------
A fungible LP position is a plain ERC-20 LP-token balance in the wallet (no
NFT, no share↔asset conversion): ``remove_liquidity`` burns the exact balance,
so a clean close leaves **exactly 0**. Closure therefore requires
``balanceOf(wallet) <= _LP_TOKEN_DUST_WEI`` — a tiny wei floor kept only as
insurance against venue-specific rounding residue (LP tokens are 18-decimal in
practice, so the floor is ~1e-17 tokens; a material residual is tens of orders
of magnitude above it).

Coverage & honesty (Empty ≠ Zero)
---------------------------------
The LP-token address is taken from ``position.details`` (``lp_token`` /
``lp_token_address``) with an address-shaped ``position_id`` as fallback (the
Curve demo stores the LP token address there; a Curve LP_CLOSE intent's
``position_id``-as-burn-amount is filtered out by the address-shape gate). A
position that carries no resolvable LP-token address returns
``unmeasured=True`` → the seam keeps it at ``UNVERIFIED`` (honest "this default
cannot verify that position"), NEVER ``FAILED`` and NEVER a false
``CHAIN_VERIFIED``. A ``None`` balance read after retry is a read fault →
``unmeasured`` — never a fabricated residual.

Gateway boundary: the on-chain read goes through the supplied
``gateway_client.query_erc20_balance``. ``rpc_url`` is accepted to satisfy the
``TeardownPostCondition`` protocol but intentionally NOT consumed — framework
code crosses the gateway boundary only. NEVER raises.
"""

from __future__ import annotations

import logging
from typing import Any

from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult
from almanak.connectors._strategy_base.vault_post_condition import (
    _is_evm_address,
    _read_with_retry,
)

logger = logging.getLogger(__name__)

# Wei-level dust floor for LP-token residue. A fungible-LP close burns the
# exact balance (no ERC-4626 round-trip rounding), so the expected residual is
# exactly 0; the floor only guards hypothetical venue-specific wei residue.
# 18-decimal LP tokens make 10 wei ~1e-17 tokens — far below any material
# strand, so this can never mask a real residual.
_LP_TOKEN_DUST_WEI = 10

# Detail keys that may carry the LP-token contract address, in priority order.
# Deliberately narrow: ambiguous keys like ``address`` / ``pool_address`` may
# hold the POOL contract (which for legacy Curve pools is NOT the ERC-20 LP
# token), and a wrong-but-valid token address could measure an unrelated wallet
# balance as a "residual" → false FAILED. Unresolvable → unmeasured (honest).
_LP_TOKEN_DETAIL_KEYS = ("lp_token", "lp_token_address")


def fungible_lp_teardown_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,
    block: int | str | None = None,
) -> ClosureCheckResult:
    """Verify a fungible-ERC-20-LP position holds zero LP-token balance on-chain.

    Reads ``balanceOf(wallet)`` on the LP-token contract via the gateway;
    closed iff the balance is within the wei dust floor.

    Returns:
        ``closed=True`` when the LP-token balance is ``<= _LP_TOKEN_DUST_WEI``;
        ``closed=False`` + ``residual`` when a positive balance is MEASURED;
        ``unmeasured=True`` when the read could not be completed (missing
        client/chain, no resolvable LP-token address, gateway/RPC fault after
        retry) — never a fabricated residual (Empty ≠ Zero).
    """
    protocol = (getattr(position, "protocol", "") or "").lower() or "fungible_lp"
    position_id = str(getattr(position, "position_id", "") or "")

    chain = getattr(position, "chain", None) or ""
    if not chain:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error="Fungible-LP post-condition needs position.chain; none found — cannot verify (unmeasured)",
        )

    if gateway_client is None:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                "Fungible-LP post-condition requires a gateway_client to read the "
                "LP-token balanceOf; none supplied — cannot verify (unmeasured)"
            ),
        )

    details = getattr(position, "details", None) or {}
    lp_token = ""
    for key in _LP_TOKEN_DETAIL_KEYS:
        candidate = str(details.get(key) or "")
        if _is_evm_address(candidate):
            lp_token = candidate
            break
    # Fallback: an address-shaped position_id (the Curve convention — the demo
    # stores the LP token address there). A burn-amount-overloaded position_id
    # (plain integer string) fails the address-shape gate and is never used.
    if not lp_token and _is_evm_address(position_id):
        lp_token = position_id
    if not lp_token:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                "Fungible-LP post-condition needs the LP-token address "
                "(details['lp_token'|'lp_token_address'] or an address-shaped position_id); "
                f"none resolvable (position_id={position_id!r}) — cannot verify (unmeasured)"
            ),
        )

    balance = _read_with_retry(
        lambda: gateway_client.query_erc20_balance(
            chain=chain,
            token_address=lp_token,
            wallet_address=wallet_address,
            block=block,
        )
    )
    # A None read (or a non-int the gateway shouldn't but might return) is a
    # read fault, never a fabricated residual → unmeasured.
    try:
        balance = int(balance) if balance is not None else None
    except (TypeError, ValueError):
        balance = None
    if balance is None:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"LP-token balanceOf({lp_token}) read returned None/non-numeric after retry "
                "(gateway/RPC fault); cannot confirm closure — unmeasured"
            ),
        )

    if balance <= _LP_TOKEN_DUST_WEI:
        return ClosureCheckResult(closed=True, protocol=protocol, position_id=position_id)

    logger.warning(
        "Fungible-LP post-condition MEASURED residual LP-token balance: protocol=%s "
        "lp_token=%s wallet=%s balance=%d (position NOT closed on-chain)",
        protocol,
        lp_token,
        wallet_address,
        balance,
    )
    return ClosureCheckResult(
        closed=False,
        protocol=protocol,
        position_id=position_id,
        residual={"lp_token": lp_token, "balance": str(balance)},
        error=f"Residual LP-token balance {balance} wei on {lp_token} exceeds dust floor {_LP_TOKEN_DUST_WEI}",
    )


__all__ = ["fungible_lp_teardown_post_condition"]
