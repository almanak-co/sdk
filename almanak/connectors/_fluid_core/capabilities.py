"""Fluid vault protocol capabilities for intent validation (VIB-5031, ADR §2.1).

Scoped to the ``fluid_vault`` key ONLY. The Phase-2 fToken surface
(``fluid`` / alias ``fluid_lending``) deliberately has NO capabilities
entry: its compiler REJECTS any truthy ``market_id`` (key-fork prevention,
PR #2723), and one key cannot demand and forbid ``market_id``
simultaneously — which is exactly why vault lending is a second protocol
key rather than a classification on ``fluid`` (ADR r2 audit blocker Q0).

``requires_market_id=True`` makes intent construction
(``lending_intents._validate_protocol_params``) reject ``fluid_vault``
intents without a ``market_id`` (the vault address) — the Morpho Blue
isolated-market precedent.
"""

from __future__ import annotations

from typing import Any

_FLUID_VAULT: dict[str, Any] = {
    "supports_interest_rate_mode": False,
    "supports_collateral_toggle": False,  # vault collateral is always collateral
    "requires_market_id": True,  # the (lowercased) vault address
    "operations": ["supply", "withdraw", "borrow", "repay"],
    # Fluid vaults open atomically: a single ``operate()`` mints the NFT-CDP and
    # supplies + borrows in one on-chain call, so a bundled
    # ``Intent.borrow(collateral_amount>0)`` is the protocol's NATIVE action --
    # not the accounting-incorrect anti-pattern the bundled-collateral guard
    # rejects for SEPARABLE protocols (Aave/Compound/Morpho/Benqi/...). Opting in
    # here exempts ``fluid_vault`` from that guard; the supply/borrow accounting
    # split for the atomic operate() is owned by the Fluid receipt-parser /
    # accounting path, not by intent-level decomposition. See
    # ``lending_intents.BorrowIntent`` and ``docs/internal/archive/reports/bundled-collateral-borrow-migration.md``.
    "supports_bundled_collateral_borrow": True,
}

PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "fluid_vault": _FLUID_VAULT,
}

__all__ = ["PROTOCOL_CAPABILITIES"]
