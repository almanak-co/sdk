"""Spot token valuation: balance * price.

Pure deterministic math. No I/O, no gateway calls.
"""

from decimal import Decimal

from almanak.framework.accounting.measured import MeasuredMoney
from almanak.framework.portfolio.models import TokenBalance


def value_tokens(
    balances: dict[str, Decimal],
    prices: dict[str, Decimal],
    addresses: dict[str, str] | None = None,
) -> list[TokenBalance]:
    """Value wallet token balances using spot prices.

    Args:
        balances: Token symbol -> human-readable balance (e.g. {"ETH": Decimal("1.5")})
        prices: Token symbol -> USD price (e.g. {"ETH": Decimal("3500")})
        addresses: Optional token symbol -> contract address mapping

    Returns:
        List of TokenBalance with value_usd and price_usd populated.
        Tokens with zero/negative balance or missing/non-positive price are excluded.
    """
    addresses = addresses or {}
    result: list[TokenBalance] = []

    for symbol, balance in balances.items():
        if balance <= 0:
            continue

        price = prices.get(symbol)
        if price is None or price <= 0:
            continue

        result.append(
            TokenBalance(
                symbol=symbol,
                balance=balance,
                value_usd=balance * price,
                price_usd=price,
                address=addresses.get(symbol, ""),
            )
        )

    return result


def total_value(token_balances: list[TokenBalance]) -> Decimal:
    """Sum USD value across all token balances.

    Seeded with ``MeasuredMoney.measured(Decimal("0"))`` and folded with
    MeasuredMoney addition (VIB-5216 / Empty≠Zero): a measured-zero seed never
    masquerades as unmeasured, and the finite-Decimal contract is enforced at the
    boundary. ``value_tokens`` already drops unpriced tokens, so every balance
    here is measured — the result is byte-identical to the prior raw-Decimal sum.
    """
    total = MeasuredMoney.measured(Decimal("0"))
    for tb in token_balances:
        total = total + MeasuredMoney.measured(tb.value_usd)
    return total.value_or(Decimal("0"))
