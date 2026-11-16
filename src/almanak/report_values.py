"""Display exact retained amounts with their identity and declared precision."""

import html
from decimal import Decimal, localcontext
from fractions import Fraction


def quantity(value, metadata):
    asset = value.get("asset")
    if not isinstance(asset, str) or not ({"amount", "numerator"} & value.keys()):
        return None
    raw = value.get("amount", value.get("numerator"))
    if raw is None:
        return asset, "Not measured"
    denominator = int(value.get("denominator", 1))
    if denominator == 0:
        return asset, "Not measured"
    precision = metadata.get(asset, {}).get("decimals")
    unit = "base units" if precision is None else metadata[asset].get("symbol") or "tokens"
    denominator *= 10 ** (precision or 0)
    number = Fraction(int(raw), denominator)
    if number.denominator == 1:
        text = str(number.numerator)
    elif int(value.get("denominator", 1)) == 1:
        text = exact_decimal(int(raw), precision or 0)
    else:
        with localcontext() as context:
            context.prec = 12
            text = "approximately " + format(
                Decimal(number.numerator) / Decimal(number.denominator), "g"
            )
    suffix = " (incomplete measurement)" if value.get("complete") is False else ""
    return asset, f"{text} {unit}{suffix}"


def exact_decimal(amount: int, decimals: int, trim: bool = True) -> str:
    """`amount` base units in whole units, every digit kept; `trim` drops trailing zeros."""
    sign = "-" if amount < 0 else ""
    if decimals <= 0:
        return sign + str(abs(amount) * 10**-decimals)
    digits = str(abs(amount)).zfill(decimals + 1)
    text = digits[:-decimals] + "." + digits[-decimals:]
    return sign + (text.rstrip("0").rstrip(".") if trim else text)


def value_html(value, metadata=None):
    metadata = metadata or {}
    if value is None:
        return '<span class="missing">Not measured</span>'
    if isinstance(value, dict):
        amount = quantity(value, metadata)
        if amount is not None:
            asset, displayed = amount
            return (
                '<span class="quantity">' + html.escape(displayed) + "</span>"
                '<span class="asset">' + html.escape(asset) + "</span>"
            )
        return (
            "<dl>"
            + "".join(
                f"<dt>{html.escape(str(key).replace('_', ' '))}</dt>"
                f"<dd>{value_html(item, metadata)}</dd>"
                for key, item in value.items()
            )
            + "</dl>"
        )
    if isinstance(value, list):
        return (
            "<ol>" + "".join(f"<li>{value_html(item, metadata)}</li>" for item in value) + "</ol>"
        )
    return html.escape(str(value))
