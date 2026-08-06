"""Token-funding based startup portfolio for historical PnL backtests."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.framework.backtesting.pnl.data_provider import (
    MarketState,
    TokenRef,
    normalize_token_key,
    token_ref_display,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.models.token_funding import AmountType, TokenFunding, parse_token_funding

logger = logging.getLogger(__name__)

_PLATFORM_EVM_NATIVE_ALIAS = "0x0000000000000000000000000000000000000000"


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
    return canonical_token_funding_entries(raw_funding, chain=chain, require_active=True)


def canonical_token_funding_entries(
    raw_funding: Any,
    *,
    chain: str,
    require_active: bool = False,
) -> list[TokenFunding]:
    """Return active-chain funding entries on the canonical token identity plane.

    The platform serializes an EVM native gas asset as the zero address, while
    the SDK's price, balance, and portfolio planes use the ERC-7528 sentinel.
    This is the single backtesting ingestion boundary for that compatibility
    alias: every downstream consumer receives the registered chain's canonical
    native symbol and sentinel address. The alias is refused for unknown and
    non-EVM chains rather than being treated as an unpriceable token.

    Unless ``require_active`` is set, an absent or invalid basket returns an
    empty list so token-map builders can use this boundary before a backtest
    config is required. A present zero-address alias that cannot be
    canonicalized, or a basket whose entries collapse to one canonical token
    identity, always raises.
    """
    _reject_negative_active_funding(raw_funding, chain=chain)
    funding = parse_token_funding(raw_funding, strategy_chain=chain)
    if not funding:
        if require_active:
            raise TokenFundingInitializationError(
                "Historical PnL backtests require strategy config token_funding for the active chain."
            )
        return []

    normalized_chain = chain.lower()
    defaulted = [(entry if entry.chain else entry.model_copy(update={"chain": normalized_chain})) for entry in funding]
    active = [entry for entry in defaulted if (entry.chain or normalized_chain).lower() == normalized_chain]
    # Declared capital filtered off the run must be loud, not a silent shrink.
    for entry in defaulted:
        if entry not in active:
            logger.warning(
                "token_funding entry DROPPED (chain mismatch): %s on chain '%s' is not on the active chain '%s' "
                "— this capital is NOT funded in the backtest",
                entry.symbol or entry.address,
                entry.chain,
                normalized_chain,
            )
    if not active and require_active:
        raise TokenFundingInitializationError(
            f"Historical PnL backtests require token_funding entries for active chain '{normalized_chain}'."
        )

    canonical = [_canonicalize_funding_entry(entry, default_chain=normalized_chain) for entry in active]
    _reject_duplicate_funding_identities(canonical, default_chain=normalized_chain)
    return canonical


def _canonicalize_funding_entry(entry: TokenFunding, *, default_chain: str) -> TokenFunding:
    """Canonicalize the platform's EVM-native alias, leaving ERC-20s unchanged."""
    if entry.address.lower() != _PLATFORM_EVM_NATIVE_ALIAS:
        return entry

    entry_chain = entry.chain or default_chain
    descriptor = ChainRegistry.try_resolve(entry_chain)
    if descriptor is None:
        raise TokenFundingInitializationError(
            "token_funding zero-address native alias requires a registered EVM chain; "
            f"chain '{entry_chain}' is unknown."
        )
    if descriptor.family is not ChainFamily.EVM:
        raise TokenFundingInitializationError(
            "token_funding zero-address native alias is only valid on registered EVM chains; "
            f"chain '{descriptor.name}' uses family '{descriptor.family.value}'."
        )

    from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL

    return entry.model_copy(
        update={
            "address": NATIVE_SENTINEL,
            "chain": descriptor.name,
            "symbol": descriptor.native.symbol.upper(),
        }
    )


def _reject_duplicate_funding_identities(entries: list[TokenFunding], *, default_chain: str) -> None:
    """Refuse baskets that canonicalize more than one entry to the same asset."""
    seen: dict[TokenRef, TokenFunding] = {}
    for entry in entries:
        identity = normalize_token_key(entry.chain or default_chain, entry.address)
        previous = seen.get(identity)
        if previous is not None:
            raise TokenFundingInitializationError(
                "token_funding contains duplicate canonical token identity "
                f"{token_ref_display(identity)} ({previous.symbol} and {entry.symbol}); "
                "declare each funded asset exactly once."
            )
        seen[identity] = entry


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
    "canonical_token_funding_entries",
    "funded_token_refs",
    "resolve_funding_seeds",
    "seed_portfolio_from_token_funding",
]
