"""Scalar compiler prices paired with their original ledger observations."""

from collections.abc import Mapping
from copy import deepcopy
from decimal import Decimal
from typing import Any

from almanak.framework.market.models import PriceData


class ObservedPrices(dict[str, Decimal]):
    """Keep the dict contract while invalidating provenance on scalar writes."""

    def __init__(self, values: Mapping[str, Decimal] | None = None) -> None:
        super().__init__()
        self._observations: dict[str, PriceData] = {}
        if values is not None:
            self.update(values)

    def record(self, symbol: str, observation: PriceData) -> None:
        self[symbol] = observation.price
        self._observations[symbol] = deepcopy(observation)

    def __setitem__(self, symbol: str, price: Decimal) -> None:
        self._observations.pop(symbol, None)
        super().__setitem__(symbol, price)

    def __delitem__(self, symbol: str) -> None:
        super().__delitem__(symbol)
        self._observations.pop(symbol, None)

    def update(self, values=(), **kwargs) -> None:
        observations = deepcopy(values._observations) if isinstance(values, ObservedPrices) else {}
        for symbol, price in dict(values, **kwargs).items():
            self[symbol] = price
            if isinstance(values, ObservedPrices) and symbol not in kwargs:
                observation = observations.get(symbol)
                if observation is not None and observation.price == price:
                    self._observations[symbol] = deepcopy(observation)

    def clear(self) -> None:
        super().clear()
        self._observations.clear()

    def pop(self, symbol, *default):
        value = super().pop(symbol, *default)
        self._observations.pop(symbol, None)
        return value

    def popitem(self):
        symbol, value = super().popitem()
        self._observations.pop(symbol, None)
        return symbol, value

    def setdefault(self, symbol, default=None):
        if symbol not in self:
            self[symbol] = default
        return self[symbol]

    def copy(self):
        return ObservedPrices(self)

    def __or__(self, other):
        result = self.copy()
        result.update(other)
        return result

    def __ror__(self, other):
        result = ObservedPrices(other)
        result.update(self)
        return result

    def __ior__(self, other):
        self.update(other)
        return self

    def ledger_inputs(self) -> dict[str, Any]:
        return {
            symbol: observation.to_oracle_entry()
            if (observation := self._observations.get(symbol)) is not None and observation.price == price
            else price
            for symbol, price in self.items()
        }


def assert_lp_price_provenance(payload: dict[str, Any], prices: ObservedPrices) -> None:
    """Require the persisted LP confidence to retain each priced deposit's evidence."""
    import json

    from almanak.framework.accounting.price_snapshot import PriceSnapshot
    from almanak.framework.market.price_store import lookup_price

    assert isinstance(prices, ObservedPrices), "LP proof requires observed price inputs"
    records = prices.ledger_inputs()
    snapshot = PriceSnapshot.from_json(json.dumps(records, default=str))
    rank = {"HIGH": 0, "ESTIMATED": 1, "STALE": 2, "UNAVAILABLE": 3}
    selected = []
    for index in (0, 1):
        amount = Decimal(payload[f"amount{index}"])
        assert amount.is_finite() and amount >= 0
        if amount == 0:
            continue
        found = lookup_price(records, token=payload[f"token{index}"], chain=payload.get("chain"), quote="USD")
        assert found is not None, "LP proof requires a price for each nonzero deposit"
        key = str(found.key)
        confidence = snapshot.confidence(key)
        assert confidence != "UNAVAILABLE", f"LP proof lacks confidence evidence for {key}"
        assert snapshot.oracle_source(key) not in (None, "", "unknown")
        assert snapshot.observed_at(key) is not None
        selected.append(confidence)
    assert selected, "LP proof requires a nonzero priced deposit"
    expected = max(selected, key=rank.__getitem__)
    assert payload["confidence"] == expected, f"LP confidence {payload['confidence']} != observed {expected}"
