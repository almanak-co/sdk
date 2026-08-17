"""Typed price-provenance view over ledger oracle inputs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

ConfidenceLiteral = Literal["HIGH", "ESTIMATED", "STALE", "UNAVAILABLE"]


@dataclass(frozen=True)
class PriceSnapshot:
    """Parsed view over ``transaction_ledger.price_inputs_json``.

    Shape: ``{symbol_or_address: {"price_usd": str, "oracle_source": str,
    "observed_at": iso8601, "confidence": HIGH|ESTIMATED|STALE|UNAVAILABLE,
    "raw_confidence": float|null, "stale": bool|null}}``.
    """

    raw: dict[str, dict[str, Any]]

    def _entry(self, symbol_or_address: str) -> dict[str, Any] | None:
        entry = self.raw.get(symbol_or_address)
        if entry is not None:
            return entry
        lookup = symbol_or_address.lower()
        return next((value for key, value in self.raw.items() if key.lower() == lookup), None)

    def usd(self, symbol_or_address: str) -> Decimal | None:
        """Return USD price, or None when missing/unparseable."""
        entry = self._entry(symbol_or_address)
        if not entry:
            return None
        try:
            price = Decimal(str(entry.get("price_usd")))
        except (InvalidOperation, TypeError):
            return None
        return price if price.is_finite() else None

    def confidence(self, symbol_or_address: str) -> ConfidenceLiteral:
        """Return a valid coarse confidence, failing unknown values closed."""
        entry = self._entry(symbol_or_address)
        if not entry:
            return "UNAVAILABLE"
        confidence = entry.get("confidence")
        if confidence in ("HIGH", "ESTIMATED", "STALE", "UNAVAILABLE"):
            return confidence
        return "UNAVAILABLE"

    def oracle_source(self, symbol_or_address: str) -> str | None:
        entry = self._entry(symbol_or_address)
        return None if entry is None else entry.get("oracle_source")

    def observed_at(self, symbol_or_address: str) -> datetime | None:
        """Return the provider observation time, never the accounting write time."""
        entry = self._entry(symbol_or_address)
        if entry is None:
            return None
        value = entry.get("observed_at") or entry.get("fetched_at")
        if not isinstance(value, str) or not value:
            return None
        try:
            observed_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=UTC)
        return observed_at.astimezone(UTC)

    def raw_confidence(self, symbol_or_address: str) -> float | None:
        entry = self._entry(symbol_or_address)
        if entry is None:
            return None
        value = entry.get("raw_confidence")
        if value is None:
            return None
        try:
            confidence = float(value)
        except (TypeError, ValueError):
            return None
        return confidence if 0.0 <= confidence <= 1.0 else None

    def stale(self, symbol_or_address: str) -> bool | None:
        entry = self._entry(symbol_or_address)
        if entry is None:
            return None
        value = entry.get("stale")
        return value if isinstance(value, bool) else None

    def is_empty(self) -> bool:
        return not self.raw

    @classmethod
    def from_json(cls, value: str) -> PriceSnapshot:
        if not value:
            return cls(raw={})
        try:
            decoded = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return cls(raw={})
        if not isinstance(decoded, dict):
            return cls(raw={})
        cleaned: dict[str, dict[str, Any]] = {key: entry for key, entry in decoded.items() if isinstance(entry, dict)}
        return cls(raw=cleaned)
