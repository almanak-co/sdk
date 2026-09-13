"""Validate per-transaction execution-gas liability in native units and USD."""

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from fractions import Fraction

from almanak.framework.execution.interfaces import UnsignedTransaction


def _finite_nonnegative(value: object, name: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"{name} must be finite and nonnegative") from None
    if not number.is_finite() or number < 0:
        raise ValueError(f"{name} must be finite and nonnegative")
    return number


def gas_cap_violations(
    transactions: list[UnsignedTransaction],
    *,
    max_gas_price_gwei: int,
    max_gas_cost_native: float,
    max_gas_cost_usd: float,
    native_token_price_usd: float,
    native_token_price_timestamp: datetime | None = None,
    native_token_price_max_age_seconds: float = 60.0,
) -> list[str]:
    """Compare finalized gas limits and fee caps; zero disables a limit.

    The liability excludes transaction value and supplementary chain fees.
    USD callers must supply a fresh authoritative quote; this pure validator
    checks its numeric validity, not its provenance.
    """
    try:
        gwei = _finite_nonnegative(max_gas_price_gwei, "max_gas_price_gwei")
        native = _finite_nonnegative(max_gas_cost_native, "max_gas_cost_native")
        usd = _finite_nonnegative(max_gas_cost_usd, "max_gas_cost_usd")
        price = _finite_nonnegative(native_token_price_usd, "native_token_price_usd") if usd else Decimal(0)
        if usd and not price:
            raise ValueError("USD gas cap requires a finite positive native token price")
        if usd:
            _validate_quote_age(native_token_price_timestamp, native_token_price_max_age_seconds)
    except ValueError as exc:
        return [str(exc)]
    if not (gwei or native or usd):
        return []
    violations: list[str] = []
    for i, tx in enumerate(transactions):
        fee = tx.max_fee_per_gas if tx.max_fee_per_gas is not None else tx.gas_price
        if not isinstance(fee, int) or isinstance(fee, bool) or fee < 0:
            violations.append(f"Transaction {i}: gas cap requires a nonnegative integer fee per gas")
            continue
        if gwei and fee > Fraction(gwei) * 10**9:
            violations.append(f"Transaction {i}: gas price {fee / 10**9:.1f} gwei exceeds limit {gwei} gwei")
        if native or usd:
            violations.extend(_cost_violations(i, tx.gas_limit, fee, native, usd, price))
    return violations


def _cost_violations(index: int, gas_limit: int, fee: int, native: Decimal, usd: Decimal, price: Decimal) -> list[str]:
    if not isinstance(gas_limit, int) or isinstance(gas_limit, bool) or gas_limit <= 0:
        return [f"Transaction {index}: gas cost cap requires a positive integer gas limit"]
    cost_wei = gas_limit * fee
    exact_cost = Fraction(cost_wei, 10**18)
    cost = Decimal(cost_wei) / Decimal(10**18)
    violations = []
    if native and exact_cost > native:
        violations.append(f"Transaction {index}: estimated gas cost {cost} native exceeds limit {native} native")
    if usd and exact_cost * Fraction(price) > usd:
        violations.append(f"Transaction {index}: estimated gas cost ${cost * price} USD exceeds limit ${usd} USD")
    return violations


def _validate_quote_age(timestamp: datetime | None, max_age: float) -> None:
    limit = _finite_nonnegative(max_age, "native_token_price_max_age_seconds")
    if not isinstance(timestamp, datetime) or timestamp.tzinfo is None:
        raise ValueError("USD gas cap requires a timezone-aware native price timestamp")
    age = Decimal(str((datetime.now(UTC) - timestamp).total_seconds()))
    if age < 0 or age > limit:
        raise ValueError("USD gas cap requires a fresh native token price at signing")
