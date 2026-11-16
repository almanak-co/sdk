"""Connector-owned interpretation of immutable prepared timelines."""

from decimal import Decimal

from almanak.backtest.config import financial_policy
from almanak.backtest.dataset import InputRecord, Timeline
from almanak.backtest.economic_data import (
    MODEL,
    AccountingSource,
    OraclePrice,
    PriceRecord,
    RateCurve,
    RateRecord,
    RiskRecord,
    VendorRiskPrice,
    rate_key,
    require_range,
    reserve_key,
)
from almanak.backtest.economic_math import MarketView
from almanak.backtest.prepared import load_dataset
from almanak.backtest.pricing import (
    CoinGeckoPrice,
    PoolPriceRecord,
    ProtocolPrice,
    VendorPriceRecord,
    VendorReference,
    price_binding,
)
from almanak.backtest.strategy_prices import is_series


class EconomicBook:
    def __init__(self, directory, reserves, start, end, *, expected_hash=None, data=None):
        self.manifest, self.reader = load_dataset(directory, expected_hash)
        self.data = data
        self.vendor_prices = {}
        self.oracle_prices = {}
        self.rates = {}
        self.risks = {}
        self.prices = {}
        self.sources = {}
        try:
            if self.manifest.preparation.model != MODEL:
                raise ValueError("prepared dataset belongs to another economic model")
            if data is not None and self.manifest.preparation.policy_digest != financial_policy(
                data
            ):
                raise ValueError("prepared dataset differs from requested financial data policy")
            for reserve in dict.fromkeys(reserves):
                self.add_reserve(reserve, start, end)
            if data is not None:
                for binding in data.prices:
                    self.binding_timeline(binding, start, end)
        except BaseException:
            self.reader.close()
            raise

    def timeline(self, key, record_type, start, end):
        requirement = self.manifest.preparation.requirement(key)
        if requirement.start > start or requirement.end < end:
            raise ValueError("prepared data does not cover initialization and cleanup")
        expected = {
            "risk": ("changes", "aave_v4_risk_configuration"),
            "price": ("changes", "USD_per_asset_e8"),
            "interest": ("sampled", "annual_fraction_ray"),
        }
        if (requirement.semantics, requirement.unit) != expected[key.product]:
            raise ValueError("prepared product has unsupported units or sampling semantics")
        if requirement.visibility != "end_of_block" or requirement.payload_revision != MODEL:
            raise ValueError("prepared data has unsupported visibility or payload semantics")
        return self.reader.timeline(requirement, record_type)

    def add_reserve(self, reserve, start, end):
        require_range(reserve, start, end)
        risk = self.timeline(reserve_key(reserve, "risk"), RiskRecord, start, end)
        prices = self.risk_prices(reserve, start, end)
        for row in risk.rows:
            if row.payload.reserve.source != reserve or row.payload.reserve.block != row.reference:
                raise ValueError("prepared risk differs from its reserve or block evidence")
        for row in prices.rows:
            if row.payload.source != reserve:
                raise ValueError("prepared price differs from its reserve or chain evidence")
        for row in (*risk.rows, *(p for p in prices.rows if isinstance(p.payload, OraclePrice))):
            if row.source_time != row.reference.timestamp:
                raise ValueError("prepared input time differs from its historical block")
        source = AccountingSource(
            chain=reserve.chain,
            hub=reserve.hub,
            asset=reserve.asset,
            hub_asset_id=risk.at(start).payload.reserve.hub_asset_id,
        )
        if any(row.payload.reserve.hub_asset_id != source.hub_asset_id for row in risk.rows):
            raise ValueError("reserve Hub accounting identity changed during prepared coverage")
        if source not in self.rates:
            self.rates[source] = RateCurve(self.timeline(rate_key(source), RateRecord, start, end))
        self.sources[reserve] = source
        self.risks[reserve], self.prices[reserve] = risk, prices

    def binding_timeline(self, binding, start, end):
        if isinstance(binding.source, ProtocolPrice):
            if binding.source.key not in {reserve_key(r, "price") for r in self.sources}:
                raise ValueError("oracle price dependency is outside declared Aave markets")
            if binding.key not in self.oracle_prices:
                self.oracle_prices[binding.key] = self.timeline(
                    binding.key, PriceRecord, start, end
                )
            return self.oracle_prices[binding.key]
        if binding.key not in self.vendor_prices:
            requirement = self.manifest.preparation.requirement(binding.key)
            if is_series(binding):
                self.vendor_prices[binding.key] = self.series(binding, requirement, start, end)
                return self.vendor_prices[binding.key]
            validate_vendor_requirement(requirement, binding, start, end)
            timeline = self.reader.timeline(requirement, VendorPriceRecord)
            for row in timeline.rows:
                validate_vendor_row(row, binding)
            self.vendor_prices[binding.key] = timeline
        return self.vendor_prices[binding.key]

    def series(self, binding, requirement, start, end):
        from almanak.backtest.pool_prices import (
            validate_pool_requirement,
            validate_pool_row,
            verify_rows,
        )

        validate_pool_requirement(requirement, binding, start, end)
        timeline = self.reader.timeline(requirement, PoolPriceRecord)
        for row in timeline.rows:
            validate_pool_row(row, binding)
        verify_rows(binding, timeline.rows, self.reader.evidence)
        return timeline

    def risk_prices(self, reserve, start, end):
        if self.data is None or self.data.risk_prices == "spoke_oracle_events":
            return self.timeline(reserve_key(reserve, "price"), PriceRecord, start, end)
        binding = price_binding(
            self.data.prices, "risk", reserve.chain, reserve.asset, reserve_key(reserve, "price")
        )
        if isinstance(binding.source, ProtocolPrice):
            if binding.key != reserve_key(reserve, "price"):
                raise ValueError("risk oracle differs from reserve oracle")
            return self.timeline(binding.key, PriceRecord, start, end)
        timeline = self.binding_timeline(binding, start, end)
        record_type = InputRecord[VendorRiskPrice, VendorReference]
        rows = tuple(
            record_type(
                **row.model_dump(exclude={"payload", "reference", "key"}),
                key=row.key,
                reference=row.reference,
                payload=VendorRiskPrice(
                    source=reserve,
                    vendor=binding.source,
                    price=usd_e8(row.payload.value),
                ),
            )
            for row in timeline.rows
        )
        return Timeline(rows, timeline.requirement)

    def observations(self, purpose, time):
        if self.data is None:
            return ()
        return tuple(
            self.observation(binding, time)
            for binding in self.data.prices
            if binding.purpose == purpose
        )

    def observation(self, binding, time):
        from almanak.backtest.dependencies import PriceObservation

        row = self.binding_timeline(binding, time, time).at(time)
        value = (
            row.payload.value
            if isinstance(binding.source, CoinGeckoPrice)
            else Decimal(row.payload.price) / Decimal(10**8)
        )
        return PriceObservation(
            binding=binding,
            value=value,
            source_time=row.source_time,
            available_at=row.available_at,
            evidence=row.evidence,
        )

    def strategy_prices(self, time):
        """Each declared strategy price as available at `time`, in declaration order.

        A pool series arrives as its window; a single price (a coin price or a protocol
        oracle) as one observation, as Aave V4 strategies have always read it.
        """
        from almanak.price_series import series_at

        if self.data is None:
            return ()
        result: list = []
        for binding in self.data.prices:
            if binding.purpose != "strategy":
                continue
            if is_series(binding):
                timeline = self.binding_timeline(binding, time, time)
                result.append(series_at(binding, timeline.rows, time))
            else:
                result.append(self.observation(binding, time))
        return tuple(result)

    def at(self, reserve, time):
        supply, borrow = self.rates[self.sources[reserve]].at(time)
        return MarketView(
            supply_index=supply,
            borrow_index=borrow,
            risk=self.risks[reserve].at(time).payload,
            price=self.prices[reserve].at(time).payload,
        )

    def evidence(self, reserve, time):
        rows = (
            self.rates[self.sources[reserve]].records.at(time),
            self.risks[reserve].at(time),
            self.prices[reserve].at(time),
        )
        return tuple(r.source_time for r in rows), tuple(
            sorted({e for r in rows for e in r.evidence})
        )

    def next(self, time):
        timelines = (
            *(curve.records for curve in self.rates.values()),
            *self.risks.values(),
            *self.prices.values(),
            *self.vendor_prices.values(),
            *self.oracle_prices.values(),
        )
        changes = [
            next_time
            for timeline in timelines
            for next_time in (timeline.next(time), timeline.expiry(time))
            if next_time is not None
        ]
        return min(changes) if changes else None

    def close(self):
        self.reader.close()


def usd_e8(value):
    numerator, denominator = value.as_integer_ratio()
    return numerator * 10**8 // denominator


def validate_vendor_requirement(requirement, binding, start, end):
    if requirement.start > start or requirement.end < end:
        raise ValueError("vendor dataset does not cover requested window")
    expected = (
        "USD_per_asset",
        "retrospective",
        "vendor-retrospective-v1",
        binding.source.seconds,
        binding.source.max_age_seconds,
    )
    actual = (
        requirement.unit,
        requirement.visibility,
        requirement.payload_revision,
        requirement.sample_seconds,
        requirement.max_age_seconds,
    )
    if actual != expected:
        raise ValueError("vendor dataset differs from requested semantics")


def validate_vendor_row(row, binding):
    if row.payload.asset != binding.asset or row.payload.source != binding.source:
        raise ValueError("vendor price differs from its explicit asset/source mapping")
    if binding.source.coin_id is None:
        discovery = row.reference.discovery
        if discovery is None or discovery.asset != binding.asset:
            raise ValueError("vendor reference lacks discovery for the requested asset")
        if row.reference.coin_id != discovery.coin_id:
            raise ValueError("vendor reference differs from discovered coin identity")
    elif row.reference.coin_id != binding.source.coin_id:
        raise ValueError("vendor reference differs from configured coin identity")
    if row.available_at < row.source_time + binding.source.availability_lag_seconds:
        raise ValueError("vendor price precedes its configured availability")
    if row.source_time != (row.reference.timestamp_ms + 999) // 1000:
        raise ValueError("vendor timestamp differs from retained reference")
