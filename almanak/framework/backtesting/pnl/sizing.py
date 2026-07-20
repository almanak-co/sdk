"""Single-owner resolution of ``amount="all"`` sizing for the backtest engine.

Live execution resolves ``"all"`` from wallet/protocol balances at compile
time. The backtest resolves it here, once, from the simulated portfolio —
the engine ingress and every adapter delegate to this module instead of
carrying their own interpretation (the perp/generic split-brain read the
same intent as $1,000 and $50).

Wallet-balance-sized intent types (SWAP, SUPPLY, VAULT_DEPOSIT) resolve to
the portfolio's held units of the spent token; PERP_OPEN collateral "all"
resolves to the spendable collateral balance (phase-5 replay — safe once
sizing has one owner). Everything else stays fail-closed with a typed
rejection code.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.intent_extraction import (
    _CHAINED_SIZING_ATTRIBUTES,
    UNSUPPORTED_ALL_SIZING_REASON,
    WALLET_BALANCE_ALL_INTENT_TYPES,
    intent_has_unresolved_all_sizing,
)

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.data_provider import MarketState
    from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

__all__ = ["RejectionCode", "ResolvedAllSizing", "SizingRejection", "apply_resolved_sizing", "resolve_all_sizing"]


class RejectionCode(StrEnum):
    """Machine-readable rejection reasons, carried in fill/trade metadata."""

    UNSUPPORTED_ALL_SIZING = "UNSUPPORTED_ALL_SIZING"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    UNPRICEABLE = "UNPRICEABLE"


@dataclass(frozen=True, slots=True)
class SizingRejection:
    code: RejectionCode
    detail: str


@dataclass(frozen=True, slots=True)
class ResolvedAllSizing:
    """A concrete resolution of an ``"all"`` sentinel: token units + USD."""

    token: Any
    units: Decimal
    amount_usd: Decimal


#: The token attribute each wallet-sized intent type spends from.
#: WRAP_NATIVE is absent by design: it spends the chain's NATIVE token, which
#: no intent attribute names (``intent.token`` is the wrapped token RECEIVED)
#: — ``_spend_token`` derives it from the chain registry instead.
_SPEND_TOKEN_ATTRIBUTES: dict[IntentType, tuple[str, ...]] = {
    IntentType.SWAP: ("from_token", "token_in", "token"),
    IntentType.SUPPLY: ("token", "asset"),
    IntentType.VAULT_DEPOSIT: ("token", "deposit_token", "asset"),
    IntentType.PERP_OPEN: ("collateral_token",),
    IntentType.UNWRAP_NATIVE: ("token",),
}


def resolve_all_sizing(
    intent: Any,
    intent_type: IntentType,
    portfolio: SimulatedPortfolio,
    market_state: MarketState | None,
) -> ResolvedAllSizing | SizingRejection | None:
    """Resolve an unresolved ``"all"`` sentinel, or explain why it cannot be.

    Returns None when the intent carries no unresolved sentinel (nothing to
    do). Wallet-balance-sized types resolve to the portfolio's spendable
    units of the spent token; other types reject with a typed code. Callers
    must treat a :class:`SizingRejection` as terminal for the fill.
    """
    if not intent_has_unresolved_all_sizing(intent, intent_type):
        return None

    if intent_type not in WALLET_BALANCE_ALL_INTENT_TYPES and intent_type is not IntentType.PERP_OPEN:
        return SizingRejection(code=RejectionCode.UNSUPPORTED_ALL_SIZING, detail=UNSUPPORTED_ALL_SIZING_REASON)

    token = _spend_token(intent, intent_type, chain=getattr(portfolio, "chain", None))
    if token is None:
        return SizingRejection(
            code=RejectionCode.UNSUPPORTED_ALL_SIZING,
            detail=f'{intent_type.value} intent carries amount="all" but no spend token to size from',
        )

    if intent_type is IntentType.PERP_OPEN and not portfolio.is_cash_equivalent(token):
        # The simulated portfolio funds perp margin from cash-like balances,
        # so sizing "all" from a held non-cash token would debit a DIFFERENT
        # balance than the one measured (sized-from-WETH, debited-from-cash).
        return SizingRejection(
            code=RejectionCode.UNSUPPORTED_ALL_SIZING,
            detail=(
                f'perp collateral="all" requires a cash-equivalent collateral token '
                f"(simulated perps fund margin from cash); swap {token} to cash first"
            ),
        )

    units = _spendable_units(portfolio, token)
    if units <= Decimal("0"):
        return SizingRejection(
            code=RejectionCode.INSUFFICIENT_BALANCE,
            detail=f'cannot size amount="all": no spendable {token} balance held',
        )

    if portfolio.is_cash_equivalent(token):
        # Cash-equivalent balances are USD-denominated inside the sim (the
        # sweep holds them at $1); re-pricing those dollars at a depegged
        # market price would resize the spendable quantity itself.
        return ResolvedAllSizing(token=token, units=units, amount_usd=units)

    price = _token_price(token, market_state)
    if price is None:
        return SizingRejection(
            code=RejectionCode.UNPRICEABLE,
            detail=f'cannot size amount="all" for {token}: no market price at this tick',
        )

    return ResolvedAllSizing(token=token, units=units, amount_usd=units * price)


def _spend_token(intent: Any, intent_type: IntentType, chain: Any = None) -> Any | None:
    if intent_type is IntentType.WRAP_NATIVE:
        # A wrap spends the chain's native token; ``intent.token`` names the
        # WRAPPED token received, so the spend side comes from the registry's
        # native↔wrapped mapping (the same map the wrap flow converts on).
        from almanak.framework.backtesting.pnl._engine_helpers import resolve_native_wrap_pair

        pair = resolve_native_wrap_pair(str(getattr(intent, "chain", None) or chain or ""))
        return pair[0] if pair is not None else None
    for attribute in _SPEND_TOKEN_ATTRIBUTES.get(intent_type, ()):
        token = getattr(intent, attribute, None)
        if token:
            return token
    return None


def _spendable_units(portfolio: SimulatedPortfolio, token: Any) -> Decimal:
    units = portfolio.get_token_balance(token)
    if units > Decimal("0"):
        return units
    # Cash-equivalent stables live in cash_usd at $1; "all" of one spends
    # the full cash-like balance, mirroring how their outflows debit.
    if portfolio.is_cash_equivalent(token):
        return portfolio.cash_like_available()
    return Decimal("0")


def _token_price(token: Any, market_state: MarketState | None) -> Decimal | None:
    if market_state is None:
        return None
    try:
        price = market_state.get_price(token)
    except KeyError:
        return None
    if price is None:
        return None
    return price if price > Decimal("0") else None


def apply_resolved_sizing(intent: Any, resolution: ResolvedAllSizing) -> Any:
    """Return a copy of ``intent`` with its "all" sentinel replaced by units.

    Downstream lanes (adapters and the generic lane) then see a concrete
    token-unit amount and never re-interpret the sentinel.
    """
    import copy

    for attribute in _CHAINED_SIZING_ATTRIBUTES:
        if str(getattr(intent, attribute, None) or "").lower() != "all":
            continue
        model_copy = getattr(intent, "model_copy", None)
        if callable(model_copy):
            return model_copy(update={attribute: resolution.units})
        clone = copy.copy(intent)
        setattr(clone, attribute, resolution.units)
        return clone
    return intent
