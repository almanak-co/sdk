"""Read-side identity joins between persisted swap lots and wallet observations."""

from decimal import Decimal
from functools import cache

from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.identity import canonicalize_token_identity


def _canonical_identity(token: str, chain: str) -> tuple[str, str] | None:
    try:
        return canonicalize_token_identity(token, chain)
    except TokenResolutionError:
        return None


def align_swap_inventory_inputs(
    lot_keys: list[str],
    balances: dict[str, Decimal],
    prices: dict[str, Decimal],
    chain: str,
    numeraire: str | None,
    base_token: str | None,
) -> tuple[dict[str, Decimal], dict[str, Decimal], str | None, str | None]:
    """Match aliases only through unique chain-bound token identities.

    These copies belong only to inventory classification, never wallet NAV.
    Multiple lot or wallet keys for one asset require reconciliation rather
    than allocating the same wallet holding twice. Unresolved legacy strings
    can still match exactly; they cannot establish an address alias.
    """
    exact_lots = {token.casefold() for token in lot_keys}

    @cache
    def identity(token: str) -> tuple[str, str] | None:
        return _canonical_identity(token, chain)

    def index(tokens: list[str]) -> dict[tuple[str, str], str]:
        result: dict[tuple[str, str], str] = {}
        for token in tokens:
            key = identity(token)
            if key is None:
                continue
            previous = result.get(key)
            if previous is not None and previous != token:
                # Existing case-only matches remain the classifier's quantity aggregation.
                if previous.casefold() != token.casefold() or token.casefold() not in exact_lots:
                    raise ValueError("Multiple inventory keys identify the same chain asset")
            result[key] = token
        return result

    wallet_index = index(list(balances))
    lot_index = index(lot_keys)
    aligned_balances = dict(balances)
    aligned_prices = dict(prices)
    exact_balances = {token.casefold() for token in balances}
    for lot in lot_keys:
        if lot.casefold() in exact_balances:
            continue
        key = identity(lot)
        wallet_key = wallet_index.get(key) if key is not None else None
        if wallet_key is None:
            continue
        aligned_balances[lot] = balances[wallet_key]
        # Use the price of the observed wallet asset, not a separate alias quote.
        for price_key in list(aligned_prices):
            if price_key.casefold() == lot.casefold():
                aligned_prices.pop(price_key)
        if wallet_key in prices:
            aligned_prices[lot] = prices[wallet_key]

    def align_hint(token: str | None) -> str | None:
        if token is None:
            return None
        key = identity(token)
        return lot_index.get(key, token) if key is not None else token

    return aligned_balances, aligned_prices, align_hint(numeraire), align_hint(base_token)
