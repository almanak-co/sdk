"""Token-funding based startup portfolio for historical PnL backtests."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.framework.backtesting.pnl.data_provider import (
    MarketState,
    TokenRef,
    normalize_token_key,
    token_ref_display,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.models.token_funding import AmountType, TokenFunding, parse_token_funding


class TokenFundingInitializationError(ValueError):
    """Raised when a PnL backtest cannot seed its wallet from token_funding."""


@dataclass(frozen=True)
class FundingSeed:
    """Resolved token_funding entry used to seed the simulated wallet."""

    entry: TokenFunding
    token: TokenRef
    amount_tokens: Decimal
    price_usd: Decimal
    value_usd: Decimal


def active_token_funding_entries(
    raw_funding: Any,
    *,
    chain: str,
) -> list[TokenFunding]:
    """Parse and return token_funding entries for the active chain."""
    _reject_negative_active_funding(raw_funding, chain=chain)
    funding = parse_token_funding(raw_funding, strategy_chain=chain)
    if not funding:
        raise TokenFundingInitializationError(
            "Historical PnL backtests require strategy config token_funding for the active chain."
        )

    normalized_chain = chain.lower()
    active = [(entry if entry.chain else entry.model_copy(update={"chain": normalized_chain})) for entry in funding]
    active = [entry for entry in active if (entry.chain or normalized_chain).lower() == normalized_chain]
    if not active:
        raise TokenFundingInitializationError(
            f"Historical PnL backtests require token_funding entries for active chain '{normalized_chain}'."
        )
    return active


def _reject_negative_active_funding(raw_funding: Any, *, chain: str) -> None:
    """Fail PnL startup before the permissive shared token_funding parser skips negatives."""
    if not isinstance(raw_funding, list):
        return

    normalized_chain = chain.lower()
    for index, entry in enumerate(raw_funding):
        if not isinstance(entry, Mapping):
            continue
        entry_chain = entry.get("chain")
        if entry_chain is not None and str(entry_chain).lower() != normalized_chain:
            continue
        raw_amount = entry.get("amount")
        if raw_amount is None:
            continue
        try:
            amount = Decimal(str(raw_amount))
        except (InvalidOperation, ValueError):
            continue
        if amount < Decimal("0"):
            symbol = entry.get("symbol") or entry.get("address") or f"entry {index}"
            raise TokenFundingInitializationError(f"token_funding amount cannot be negative for {symbol}: {raw_amount}")


def funded_token_refs(
    raw_funding: Any,
    *,
    chain: str,
) -> list[TokenRef]:
    """Return address-native token refs funded on the active chain."""
    return [_entry_token_ref(entry, chain) for entry in active_token_funding_entries(raw_funding, chain=chain)]


def build_initial_portfolio_from_token_funding(
    *,
    raw_funding: Any,
    chain: str,
    market_state: MarketState,
) -> SimulatedPortfolio:
    """Build a startup portfolio from token_funding at the first market tick.

    ``amount_type="token"`` seeds exact token units. ``amount_type="usd"``
    converts the declared USD value into token units using the first available
    historical price. ``amount_type="percentage"`` has no wallet state to
    reference at startup and is rejected.
    """
    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"), cash_usd=Decimal("0"), chain=chain)
    seed_portfolio_from_token_funding(portfolio, raw_funding=raw_funding, chain=chain, market_state=market_state)
    return portfolio


def seed_portfolio_from_token_funding(
    portfolio: SimulatedPortfolio,
    *,
    raw_funding: Any,
    chain: str,
    market_state: MarketState,
) -> Decimal:
    """Mutate ``portfolio`` with active-chain token_funding and return USD value."""
    seeds = resolve_funding_seeds(raw_funding=raw_funding, chain=chain, market_state=market_state)
    for seed in seeds:
        if seed.amount_tokens <= Decimal("0"):
            continue
        portfolio.tokens[seed.token] = portfolio.tokens.get(seed.token, Decimal("0")) + seed.amount_tokens
        portfolio._cost_basis[seed.token] = seed.price_usd

    initial_value = sum((seed.value_usd for seed in seeds), Decimal("0"))
    portfolio.initial_capital_usd = initial_value
    return initial_value


def resolve_funding_seeds(
    *,
    raw_funding: Any,
    chain: str,
    market_state: MarketState,
) -> list[FundingSeed]:
    """Resolve active-chain token_funding entries into explicit token units."""
    seeds: list[FundingSeed] = []
    for entry in active_token_funding_entries(raw_funding, chain=chain):
        if entry.amount_type == AmountType.PERCENTAGE:
            raise TokenFundingInitializationError(
                f"token_funding percentage amount is not valid for PnL startup: {entry.symbol}"
            )

        token = _entry_token_ref(entry, chain)
        raw_amount = Decimal(entry.amount)
        if raw_amount < Decimal("0"):
            raise TokenFundingInitializationError(
                f"token_funding amount cannot be negative for {entry.symbol}: {entry.amount}"
            )
        if raw_amount == Decimal("0"):
            seeds.append(
                FundingSeed(
                    entry=entry,
                    token=token,
                    amount_tokens=Decimal("0"),
                    price_usd=Decimal("0"),
                    value_usd=Decimal("0"),
                )
            )
            continue

        price = _funding_price(entry, token, market_state)

        if entry.amount_type == AmountType.TOKEN:
            token_amount = raw_amount
            value_usd = raw_amount * price
        elif entry.amount_type == AmountType.USD:
            token_amount = raw_amount / price if raw_amount > Decimal("0") else Decimal("0")
            value_usd = raw_amount
        else:
            raise TokenFundingInitializationError(
                f"Unsupported token_funding amount_type for {entry.symbol}: {entry.amount_type}"
            )

        seeds.append(
            FundingSeed(
                entry=entry,
                token=token,
                amount_tokens=token_amount,
                price_usd=price,
                value_usd=value_usd,
            )
        )
    return seeds


def _entry_token_ref(entry: TokenFunding, chain: str) -> TokenRef:
    entry_chain = (entry.chain or chain).lower()
    return normalize_token_key(entry_chain, entry.address)


def _funding_price(entry: TokenFunding, token: TokenRef, market_state: MarketState) -> Decimal:
    try:
        price = market_state.get_price(token)
    except KeyError as exc:
        raise TokenFundingInitializationError(
            f"Missing first-tick price for funded token {entry.symbol} ({token_ref_display(token)})."
        ) from exc
    if price is None or price <= Decimal("0"):
        raise TokenFundingInitializationError(
            f"First-tick price for funded token {entry.symbol} ({token_ref_display(token)}) must be positive."
        )
    return price


__all__ = [
    "FundingSeed",
    "TokenFundingInitializationError",
    "active_token_funding_entries",
    "build_initial_portfolio_from_token_funding",
    "funded_token_refs",
    "resolve_funding_seeds",
    "seed_portfolio_from_token_funding",
]
