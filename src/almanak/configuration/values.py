"""Exact author quantities and time values; no provider access or rounding."""

import re
from datetime import datetime

from almanak.caip import AssetType
from almanak.configuration.yaml import ConfigurationError


def seconds(value):
    if type(value) is int and value >= 0:
        return value
    match = re.fullmatch(r"(0|[1-9][0-9]*)(s|m|h|d)", value) if isinstance(value, str) else None
    if match is None:
        raise ConfigurationError("duration requires whole seconds or a value such as 5m")
    return int(match[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[match[2]]


DURATION_FIELDS = (
    "sample_seconds",
    "rate_sample_seconds",
    "max_age_seconds",
    "arrival_latency_seconds",
)


def duration_fields(values, names=DURATION_FIELDS):
    result = dict(values)
    for name in names:
        if name in result:
            result[name] = seconds(result[name])
    return result


def instant(value):
    if not isinstance(value, str):
        raise ConfigurationError("time requires an ISO datetime with an explicit timezone")
    try:
        date = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ConfigurationError("invalid ISO datetime") from exc
    if date.utcoffset() is None or date.microsecond:
        raise ConfigurationError("time requires a timezone and whole-second precision")
    return int(date.timestamp())


def quantity(value, decimals):
    if isinstance(value, dict) and set(value) == {"base_units"}:
        raw = value["base_units"]
        if type(raw) is int and raw >= 0:
            return raw
        raise ConfigurationError("base_units must be a nonnegative integer")
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]+(\.[0-9]+)?", value):
        raise ConfigurationError("quantity requires a quoted decimal or {base_units: INTEGER}")
    whole, _, fraction = value.partition(".")
    if len(fraction) > decimals:
        raise ConfigurationError(f"quantity exceeds the asset's {decimals} decimal places")
    return int(whole) * 10**decimals + int(fraction.ljust(decimals, "0") or "0")


def configured_quantity(value, decimals, field):
    try:
        return quantity(value, decimals)
    except ConfigurationError as error:
        raise ConfigurationError(str(error), field=field) from error


def asset_catalog(values):
    result = {}
    for identity, metadata in values.items():
        asset = AssetType(identity)
        if not isinstance(metadata, dict) or set(metadata) - {"decimals", "symbol"}:
            raise ConfigurationError("asset metadata permits decimals and optional symbol only")
        precision = metadata.get("decimals")
        if type(precision) is not int or not 0 <= precision <= 255:
            raise ConfigurationError("asset decimals must be an integer from 0 to 255")
        if str(asset) in result:
            raise ConfigurationError(
                "duplicate canonical asset identity", field=f"assets.{identity}"
            )
        result[str(asset)] = metadata
    return result


def amount(value, assets):
    if not isinstance(value, dict) or set(value) != {"asset", "quantity"}:
        raise ConfigurationError("asset amount requires asset and quantity")
    assets = {str(key): metadata for key, metadata in assets.items()}
    identity = str(AssetType(value["asset"]))
    if identity not in assets:
        raise ConfigurationError(f"asset metadata is missing for {identity}")
    return {"asset": identity, "amount": quantity(value["quantity"], precision(assets[identity]))}


def precision(metadata):
    return metadata["decimals"] if isinstance(metadata, dict) else metadata.decimals
