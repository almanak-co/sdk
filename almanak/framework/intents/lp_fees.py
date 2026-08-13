"""Typed LP fee declarations shared by intent, compiler, and backtest paths.

``fee_tier_units`` is the canonical V3-style representation: hundredths of a
basis point (``500 == 0.05%``). ``fee_rate`` is a separate economic fraction
for protocols whose fee is not a factory discriminator. Keeping the two names
distinct prevents raw factory units from entering fee arithmetic directly.

Migration contract: the deprecated ``protocol_params["fee_tier"]`` alias also
means raw factory units, so ``500`` is accepted but a fractional declaration
such as ``0.0005`` is rejected. Dynamic-fee protocols must migrate fractional
values to the explicitly named ``fee_rate`` field.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

_FEE_UNIT_SCALE = Decimal("1000000")


@dataclass(frozen=True, slots=True)
class LPFeeDeclaration:
    """Validated caller fee constraint in exactly one representation."""

    fee_tier_units: int | None = None
    fee_rate: Decimal | None = None
    source: str | None = None

    @property
    def economic_rate(self) -> Decimal | None:
        if self.fee_tier_units is not None:
            return Decimal(self.fee_tier_units) / _FEE_UNIT_SCALE
        return self.fee_rate


def fee_rate_from_units(fee_tier_units: int) -> Decimal:
    """Convert validated V3 factory units to an economic fraction."""
    return Decimal(_validate_fee_tier_units(fee_tier_units, "fee_tier_units")) / _FEE_UNIT_SCALE


def pool_fee_tier_units(pool: Any) -> int | None:
    """Return the integral third ``TOKEN0/TOKEN1/TIER`` segment.

    This parser deliberately preserves out-of-domain integral values such as
    ``0`` or ``1000000`` so venue-specific consumers can reject them with a
    typed error. It only decides whether the segment is an integer declaration;
    :func:`fee_rate_from_units` owns the V3 range check.
    """
    if not isinstance(pool, str):
        return None
    parts = [part.strip() for part in pool.split("/")]
    if len(parts) != 3 or not parts[2]:
        return None
    try:
        return _parse_integral_fee_units(parts[2], "pool fee tier", coerce=True)
    except ValueError:
        return None


def _parse_integral_fee_units(value: Any, field_name: str, *, coerce: bool = False) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer in raw factory units, not bool")
    if not coerce and not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer in raw factory units")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer in raw factory units") from exc
    if not parsed.is_finite() or parsed != parsed.to_integral_value():
        raise ValueError(
            f"{field_name} must be an integer in raw factory units "
            "(for example 500 for 0.05%); fractional rates belong in fee_rate"
        )
    return int(parsed)


def _validate_fee_tier_units(value: Any, field_name: str, *, coerce: bool = False) -> int:
    units = _parse_integral_fee_units(value, field_name, coerce=coerce)
    if not 0 < units < 1_000_000:
        raise ValueError(f"{field_name} must be between 1 and 999999 raw factory units")
    return units


def _validate_fee_rate(value: Any, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a finite decimal fraction, not bool")
    try:
        rate = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite decimal fraction") from exc
    if not rate.is_finite() or not Decimal("0") < rate <= Decimal("1"):
        raise ValueError(f"{field_name} must be finite and in the interval (0, 1]")
    return rate


def parse_lp_fee_declaration(
    *,
    fee_tier_units: Any = None,
    fee_rate: Any = None,
    protocol_params: Mapping[str, Any] | None = None,
) -> LPFeeDeclaration:
    """Validate canonical fields plus the legacy ``protocol_params`` alias.

    The legacy ``fee_tier``/``feeTier`` key follows the live V3 contract and
    therefore means raw factory units. It never means an economic fraction.
    """
    params = protocol_params or {}
    legacy_values = [
        _validate_fee_tier_units(params[key], f"protocol_params['{key}']", coerce=True)
        for key in ("fee_tier", "feeTier")
        if key in params
    ]
    if len(legacy_values) == 2 and legacy_values[0] != legacy_values[1]:
        raise ValueError("protocol_params fee_tier and feeTier declarations conflict")

    canonical_units = _validate_fee_tier_units(fee_tier_units, "fee_tier_units") if fee_tier_units is not None else None
    legacy_units = legacy_values[0] if legacy_values else None
    if canonical_units is not None and legacy_units is not None and canonical_units != legacy_units:
        raise ValueError(
            f"fee_tier_units={canonical_units} conflicts with legacy protocol_params fee_tier={legacy_units}"
        )
    units = canonical_units if canonical_units is not None else legacy_units
    rate = _validate_fee_rate(fee_rate, "fee_rate") if fee_rate is not None else None
    if units is not None and rate is not None:
        raise ValueError("fee_tier_units and fee_rate are mutually exclusive fee representations")

    source = None
    if canonical_units is not None:
        source = "fee_tier_units"
    elif legacy_units is not None:
        source = "legacy_protocol_params"
    elif rate is not None:
        source = "fee_rate"
    return LPFeeDeclaration(fee_tier_units=units, fee_rate=rate, source=source)


def lp_fee_declaration_from_intent(intent: Any) -> LPFeeDeclaration:
    """Read and validate an intent, including unvalidated ``model_construct`` values.

    ``protocol_params['fee_tier']`` is a deprecated raw-unit alias. Migrate
    values such as ``500`` to ``fee_tier_units=500``. Fractional legacy values
    such as ``0.0005`` are rejected; dynamic-fee protocols must use
    ``fee_rate=Decimal('0.0005')`` instead.
    """
    params = getattr(intent, "protocol_params", None)
    if params is not None and not isinstance(params, Mapping):
        raise ValueError("protocol_params must be a mapping")
    return parse_lp_fee_declaration(
        fee_tier_units=getattr(intent, "fee_tier_units", None),
        fee_rate=getattr(intent, "fee_rate", None),
        protocol_params=params,
    )


__all__ = [
    "LPFeeDeclaration",
    "fee_rate_from_units",
    "lp_fee_declaration_from_intent",
    "parse_lp_fee_declaration",
    "pool_fee_tier_units",
]
