"""Typed money and identity primitives for the PnL backtest engine.

Two structural rules: one economic token has exactly one identity, and USD
notionals never mix with token units without an explicit price.

Key Components:
    - TokenIdentity: canonical (chain, address, symbol) identity; hash/eq
      ignore the display symbol so one asset cannot occupy two keys
    - UsdAmount / TokenUnits: value kinds with no cross-kind arithmetic;
      conversion only through a PriceQuote, raising PriceUnavailableError
      when absent
    - PriceQuote: positive, provenance-stamped USD price (a zeroed price is
      a data defect and must be expressed as absence)
    - as_decimal: strict Decimal ingress (floats rejected)

Examples:
    weth = TokenIdentity(chain="base", address="0x4200000000000000000000000000000000000006", symbol="WETH")
    held = TokenUnits(token=weth, units=Decimal("0.5"))
    quote = PriceQuote(token=weth, usd_per_unit=Decimal("2500"), source="coingecko")
    held.to_usd(quote)                      # UsdAmount(Decimal("1250"))
    held.to_usd(None)                       # raises PriceUnavailableError
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation

from almanak.framework.market.errors import PriceUnavailableError

__all__ = [
    "PriceQuote",
    "TokenIdentity",
    "TokenUnits",
    "UsdAmount",
    "ValueKindError",
    "as_decimal",
]


class ValueKindError(TypeError):
    """Raised when USD notionals and token units are combined without a price."""


def as_decimal(value: Decimal | int | str | float, *, what: str) -> Decimal:
    """Return ``value`` as a Decimal, rejecting floats and other lossy inputs.

    Floats are refused outright rather than coerced: a float that has already
    lost precision cannot be repaired by ``Decimal(float)``, and backtest
    accounting is Decimal-exact end to end.
    """
    if isinstance(value, bool):  # bool is an int subclass; never money
        raise ValueKindError(f"{what} must be a Decimal, got bool")
    if isinstance(value, float):
        raise ValueKindError(f"{what} must be a Decimal (got float); floats are rejected — construct from str")
    try:
        # Decimal(Decimal) round-trips exactly; int/str construct exactly.
        result = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueKindError(f"{what} must be a Decimal (got {type(value).__name__})") from exc
    if not result.is_finite():
        raise ValueKindError(f"{what} must be finite, got {result}")
    return result


@dataclass(frozen=True, slots=True)
class TokenIdentity:
    """The single canonical identity of a token within a backtest run.

    Equality and hashing use ``(chain, address)`` when the address is known and
    ``(chain, symbol)`` otherwise — the display symbol never participates when
    an address exists, so a symbol-shaped reference and an address-shaped
    reference to the same asset are the SAME key. ``address=None`` is reserved
    for tokens the run could not resolve; they still get exactly one identity.
    """

    chain: str
    address: str | None
    symbol: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "chain", self.chain.lower().strip())
        object.__setattr__(self, "symbol", self.symbol.upper().strip())
        if self.address is not None:
            from almanak.framework.backtesting.pnl.data_provider import is_address_like

            address = self.address.lower().strip()
            if not is_address_like(address):
                raise ValueError(f"TokenIdentity address must be a 0x…40-hex string or None, got {self.address!r}")
            object.__setattr__(self, "address", address)
        if not self.chain:
            raise ValueError("TokenIdentity chain must be non-empty")
        if not self.symbol:
            raise ValueError("TokenIdentity symbol must be non-empty")

    @property
    def key(self) -> tuple[str, str]:
        """The balance-dict key: ``(chain, address)`` or ``(chain, symbol)``."""
        return (self.chain, self.address if self.address is not None else self.symbol)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TokenIdentity):
            return NotImplemented
        return self.key == other.key

    def __hash__(self) -> int:
        return hash(self.key)

    def display(self) -> str:
        return f"{self.symbol}({self.chain}:{self.address or 'unresolved'})"


@dataclass(frozen=True, slots=True)
class PriceQuote:
    """A positive USD-per-unit price observation for one token.

    A quote is evidence, so it carries provenance. Zero and negative prices are
    rejected at construction: a zeroed price is a data defect masquerading as a
    measurement (Empty != Zero), and callers must express it as *absence* —
    which conversion surfaces as PriceUnavailableError, never as $0 value.
    """

    token: TokenIdentity
    usd_per_unit: Decimal
    source: str
    timestamp: datetime | None = field(default=None)

    def __post_init__(self) -> None:
        object.__setattr__(self, "usd_per_unit", as_decimal(self.usd_per_unit, what="PriceQuote.usd_per_unit"))
        if self.usd_per_unit <= 0:
            raise ValueError(
                f"PriceQuote for {self.token.display()} must be positive, got {self.usd_per_unit}; "
                "a zero/negative price is a data defect — represent it as an absent quote"
            )
        if not self.source:
            raise ValueError("PriceQuote.source must be non-empty")


@dataclass(frozen=True, slots=True)
class UsdAmount:
    """A USD notional. Adds/subtracts only with UsdAmount; scales by Decimal/int."""

    value: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", as_decimal(self.value, what="UsdAmount.value"))

    def to_units(self, quote: PriceQuote | None) -> TokenUnits:
        """Convert to units of ``quote.token``; absent quote raises, never guesses."""
        if quote is None:
            raise PriceUnavailableError(
                "<usd amount>",
                "no price quote — refusing to size token units from USD (a raw-USD-as-units fallback mints value)",
            )
        return TokenUnits(token=quote.token, units=self.value / quote.usd_per_unit)

    def __add__(self, other: object) -> UsdAmount:
        if not isinstance(other, UsdAmount):
            return NotImplemented
        return UsdAmount(self.value + other.value)

    def __sub__(self, other: object) -> UsdAmount:
        if not isinstance(other, UsdAmount):
            return NotImplemented
        return UsdAmount(self.value - other.value)

    def __mul__(self, scalar: object) -> UsdAmount:
        if isinstance(scalar, bool) or not isinstance(scalar, Decimal | int):
            return NotImplemented
        return UsdAmount(self.value * scalar)

    __rmul__ = __mul__

    def __truediv__(self, scalar: object) -> UsdAmount:
        if isinstance(scalar, bool) or not isinstance(scalar, Decimal | int):
            return NotImplemented
        return UsdAmount(self.value / scalar)

    def __neg__(self) -> UsdAmount:
        return UsdAmount(-self.value)

    def __abs__(self) -> UsdAmount:
        return UsdAmount(abs(self.value))

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, UsdAmount):
            return NotImplemented
        return self.value < other.value

    def __le__(self, other: object) -> bool:
        if not isinstance(other, UsdAmount):
            return NotImplemented
        return self.value <= other.value

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, UsdAmount):
            return NotImplemented
        return self.value > other.value

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, UsdAmount):
            return NotImplemented
        return self.value >= other.value

    def __bool__(self) -> bool:
        return self.value != 0


@dataclass(frozen=True, slots=True)
class TokenUnits:
    """A quantity of one specific token. Same-token arithmetic only."""

    token: TokenIdentity
    units: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "units", as_decimal(self.units, what="TokenUnits.units"))

    def to_usd(self, quote: PriceQuote | None) -> UsdAmount:
        """Convert to USD via ``quote``; absent or mismatched quote raises."""
        if quote is None:
            raise PriceUnavailableError(
                self.token.display(),
                "no price quote — refusing to value token units in USD (a $1-per-unit fallback mints value)",
            )
        if quote.token != self.token:
            raise PriceUnavailableError(
                self.token.display(),
                f"quote is for {quote.token.display()} — cross-token pricing is never implicit",
            )
        return UsdAmount(self.units * quote.usd_per_unit)

    def _require_same_token(self, other: TokenUnits, op: str) -> None:
        if other.token != self.token:
            raise ValueKindError(
                f"cannot {op} {other.token.display()} units with {self.token.display()} units; convert via to_usd first"
            )

    def __add__(self, other: object) -> TokenUnits:
        if not isinstance(other, TokenUnits):
            return NotImplemented
        self._require_same_token(other, "add")
        return TokenUnits(token=self.token, units=self.units + other.units)

    def __sub__(self, other: object) -> TokenUnits:
        if not isinstance(other, TokenUnits):
            return NotImplemented
        self._require_same_token(other, "subtract")
        return TokenUnits(token=self.token, units=self.units - other.units)

    def __mul__(self, scalar: object) -> TokenUnits:
        if isinstance(scalar, bool) or not isinstance(scalar, Decimal | int):
            return NotImplemented
        return TokenUnits(token=self.token, units=self.units * scalar)

    __rmul__ = __mul__

    def __truediv__(self, scalar: object) -> TokenUnits:
        if isinstance(scalar, bool) or not isinstance(scalar, Decimal | int):
            return NotImplemented
        return TokenUnits(token=self.token, units=self.units / scalar)

    def __neg__(self) -> TokenUnits:
        return TokenUnits(token=self.token, units=-self.units)

    def __abs__(self) -> TokenUnits:
        return TokenUnits(token=self.token, units=abs(self.units))

    def __bool__(self) -> bool:
        return self.units != 0
