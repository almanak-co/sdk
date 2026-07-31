"""Canonical intent-type identity shared across framework and connectors.

This module is deliberately a dependency-free leaf. Connector manifests are
loaded during lightweight discovery and must not import the full framework
intent vocabulary merely to name an intent.
"""

from __future__ import annotations

from enum import Enum

__all__ = ["IntentType"]


class IntentType(Enum):
    """Types of intents that strategies and execution lanes can express."""

    SWAP = "SWAP"
    LP_OPEN = "LP_OPEN"
    LP_CLOSE = "LP_CLOSE"
    BORROW = "BORROW"
    REPAY = "REPAY"
    SUPPLY = "SUPPLY"
    WITHDRAW = "WITHDRAW"
    PERP_OPEN = "PERP_OPEN"
    PERP_CLOSE = "PERP_CLOSE"
    # Cancel a pending (unfilled) perp order, recovering its committed collateral.
    # Not a position open/close — a refund of committed-but-unspent collateral
    # (the recovery half of VIB-5116; see PerpCancelIntent). NO_ACCOUNTING category.
    PERP_CANCEL_ORDER = "PERP_CANCEL_ORDER"
    # Withdraw free margin off a perp venue's off-chain account back to L1 (a cash
    # movement, not a trade — no position, no PnL). On Hyperliquid this is a
    # CoreWriter spotSend HyperCore→HyperEVM bridge (VIB-5617). NO_ACCOUNTING category.
    PERP_WITHDRAW = "PERP_WITHDRAW"
    BRIDGE = "BRIDGE"
    ENSURE_BALANCE = "ENSURE_BALANCE"
    FLASH_LOAN = "FLASH_LOAN"
    STAKE = "STAKE"
    UNSTAKE = "UNSTAKE"
    HOLD = "HOLD"
    # Prediction market intents
    PREDICTION_BUY = "PREDICTION_BUY"
    PREDICTION_SELL = "PREDICTION_SELL"
    PREDICTION_REDEEM = "PREDICTION_REDEEM"
    # Vault intents (MetaMorpho ERC-4626)
    VAULT_DEPOSIT = "VAULT_DEPOSIT"
    VAULT_REDEEM = "VAULT_REDEEM"
    VAULT_REALLOCATE = "VAULT_REALLOCATE"  # Phase 2
    VAULT_MANAGE = "VAULT_MANAGE"  # Phase 4
    # LP fee collection (without removing liquidity)
    LP_COLLECT_FEES = "LP_COLLECT_FEES"
    # Native token wrap/unwrap (ETH<->WETH, MATIC<->WMATIC, etc.)
    WRAP_NATIVE = "WRAP_NATIVE"
    UNWRAP_NATIVE = "UNWRAP_NATIVE"
    # Emergency deleverage — structurally a repay but carries risk-event context
    # (trigger_reason, observed_hf, target_hf) so dashboards and accounting can
    # distinguish forced unwinds from routine repays.
    DELEVERAGE = "DELEVERAGE"
    # ──────────────────────────────────────────────────────────────────────
    # P0 PLACEHOLDERS (VIB-4165 / VIB-4160 T5) — locked design item #5.
    #
    # These five enum values exist WITHOUT real connectors so future code paths
    # (LLM tool calls, strategy templates, the agent_tools PolicyEngine) cannot
    # silently smuggle CDP / liquidation / stablecoin-mint operations through
    # generic BORROW / REPAY / SUPPLY and pollute lending accounting before the
    # real connector ships in P1. The compiler MUST raise NotImplementedError on
    # each — see ``_raise_if_placeholder_intent`` in
    # ``almanak/framework/intents/compiler.py`` and
    # ``tests/unit/intents/test_placeholder_compilers.py`` (Hard Ratification
    # Condition #5).
    LIQUIDATE = "LIQUIDATE"
    OPEN_CDP = "OPEN_CDP"
    MINT_STABLE = "MINT_STABLE"
    REPAY_STABLE = "REPAY_STABLE"
    CLOSE_CDP = "CLOSE_CDP"

    @classmethod
    def try_parse(cls, value: object) -> IntentType | None:
        """Resolve a runtime string or return an existing enum member.

        Manifest declarations must pass enum members directly. This permissive
        resolver exists only for string-facing query/config boundaries.
        """
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            return None
        normalized = value.strip().upper()
        if not normalized:
            return None
        try:
            return cls(normalized)
        except ValueError:
            return None
