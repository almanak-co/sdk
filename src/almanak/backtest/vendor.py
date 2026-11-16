"""Scoped bulk vendor acquisition in the historical gateway only."""

import hashlib
import json
import threading
import time
from decimal import Decimal
from itertools import pairwise
from typing import Literal

import httpx
from dotenv import dotenv_values
from pydantic import model_validator

from almanak.backtest.collection import ProviderGate
from almanak.backtest.dataset import DataRequirement, RawEvidence
from almanak.backtest.history_batch import TransientError, retry_delay, wait_to_retry
from almanak.backtest.pricing import (
    AssetPrice,
    CoinGeckoPrice,
    VendorPriceRecord,
    VendorReference,
)
from almanak.backtest.types import Name, UnixTime
from almanak.backtest.vendor_discovery import (
    CoinId,
    CoinMapping,
    Discovery,
    LookupRequest,
    coin_for,
    platform_for,
)
from almanak.caip import AssetType
from almanak.gateway.ethereum import DataError
from almanak.gateway.rate_limits import ProviderLimit
from almanak.models import Model


class VendorRequest(Model):
    provider: Name
    coin_id: CoinId | None
    asset: AssetType | None = None
    interval: Literal["hourly", "5m", "daily"]
    quote: Literal["USD"] = "USD"
    start: UnixTime
    end: UnixTime

    @model_validator(mode="after")
    def identity(self):
        if (self.coin_id is None) == (self.asset is None):
            raise ValueError("vendor request requires either a coin ID or an asset for discovery")
        if self.asset is not None:
            from almanak.backtest.vendor_discovery import require_contract

            require_contract(self.asset)
        return self

    @property
    def key(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()


class VendorEvidence(Model):
    request: VendorRequest
    endpoint: str
    response: str
    retrieved_at: float
    discovery: Discovery | None = None


def requests_for(binding, start, end):
    source = binding.source
    if not isinstance(source, CoinGeckoPrice):
        return ()
    first = max(0, start - source.max_age_seconds - source.seconds)
    chunk = (10 if source.interval == "5m" else 90) * 86400
    return tuple(
        VendorRequest(
            provider=source.provider,
            coin_id=source.coin_id,
            asset=binding.asset if source.coin_id is None else None,
            interval=source.interval,
            start=UnixTime(left),
            end=UnixTime(min(end, left + chunk)),
        )
        for left in range(first, end, chunk)
    )


def requirement_for(binding, start, end):
    source = binding.source
    if not isinstance(source, CoinGeckoPrice):
        raise ValueError("vendor preparation requires an explicit vendor source")
    return DataRequirement(
        key=binding.key,
        start=start,
        end=end,
        semantics="sampled",
        sample_seconds=source.seconds,
        max_age_seconds=source.max_age_seconds,
        unit="USD_per_asset",
        visibility="retrospective",
        payload_revision="vendor-retrospective-v1",
    )


class VendorCollector:
    def __init__(self, capture, scopes, vendors, env_file):
        self.provider_limit = ProviderLimit("coingecko")
        self.capture = capture
        self.scopes = {r.key for r in scopes}
        self.vendors = {v.name: v.provider for v in vendors}
        self.values = dotenv_values(env_file, interpolate=False) if env_file else {}
        self.gates: dict[str, ProviderGate] = {}
        self.by_name = {}
        for name, provider in self.vendors.items():
            identity = provider.model_dump_json(exclude={"collection"})
            gate = self.gates.setdefault(identity, ProviderGate(provider.collection))
            if not gate.same_budget(provider.collection):
                raise ValueError("shared vendor requires identical concurrency and rate limits")
            self.by_name[name] = gate
        self.discovery_lock = threading.Lock()
        self.client = httpx.Client(timeout=60)
        with capture.connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS vendor_observations (key TEXT PRIMARY KEY, body TEXT)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS vendor_discovery (key TEXT PRIMARY KEY, body TEXT)"
            )

    def read(self, request):
        if request.key not in self.scopes or request.provider not in self.vendors:
            raise ValueError("vendor request is outside declared preparation requirements")
        with self.capture.connect() as db:
            row = db.execute(
                "SELECT body FROM vendor_observations WHERE key=?", (request.key,)
            ).fetchone()
        if row:
            self.capture.metric("vendor_cache_hits")
            evidence = VendorEvidence.model_validate_json(row[0])
            if evidence.request != request:
                raise ValueError("vendor capture identity mismatch")
            return evidence
        provider = self.vendors[request.provider]
        discovery = self.resolve(request) if request.coin_id is None else None
        for attempt in range(provider.collection.max_retries + 1):
            try:
                with self.by_name[request.provider].admit():
                    evidence = self.fetch(request, provider, discovery)
                with self.capture.connect() as db:
                    db.execute(
                        "INSERT INTO vendor_observations VALUES (?, ?)",
                        (request.key, evidence.model_dump_json()),
                    )
                return evidence
            except TransientError as error:
                if attempt == provider.collection.max_retries:
                    raise
                wait_to_retry(error, attempt)
        raise AssertionError("unreachable retry state")

    def fetch(self, request, provider, discovery=None):
        key = self.values.get(provider.api_key_env)
        if not key:
            raise DataError(
                "missing_credential",
                f"missing vendor credential {provider.api_key_env}; supply --env-file",
            )
        host = "pro-api" if provider.api == "pro" else "api"
        coin_id = discovery.coin_id if discovery is not None else request.coin_id
        endpoint = f"https://{host}.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
        self.capture.metric("vendor_http_requests")
        identity = self.capture.begin_attempt(request)
        started = time.monotonic()
        try:
            self.provider_limit.admit()
            response = self.client.get(
                endpoint,
                params={
                    "vs_currency": "usd",
                    "from": request.start,
                    "to": request.end,
                    "interval": request.interval,
                    "precision": "full",
                },
                headers={f"x-cg-{provider.api}-api-key": key},
            )
            validate_response(response)
        except httpx.HTTPError:
            self.capture.finish_attempt(identity, time.monotonic() - started, "transport")
            raise TransientError("transport", "vendor transport failed") from None
        except DataError as error:
            self.capture.finish_attempt(identity, time.monotonic() - started, error.kind)
            raise
        self.capture.finish_attempt(identity, time.monotonic() - started, "success")
        return VendorEvidence(
            request=request,
            endpoint=endpoint,
            response=response.text,
            retrieved_at=time.time(),
            discovery=discovery,
        )

    def resolve(self, request):
        if request.asset is None:
            raise ValueError("CoinGecko discovery requires an explicit asset")
        identity = f"{request.provider}:{request.asset}"
        # Different historical chunks must share one durable mapping, even in parallel.
        with self.discovery_lock:
            with self.capture.connect() as db:
                row = db.execute(
                    "SELECT body FROM vendor_discovery WHERE key=?", (identity,)
                ).fetchone()
            if row:
                result = Discovery.model_validate_json(row[0])
                if result.asset != request.asset:
                    raise ValueError("cached CoinGecko discovery belongs to another asset")
                return result
            result = self.discover(request)
            with self.capture.connect() as db:
                db.execute(
                    "INSERT INTO vendor_discovery VALUES (?, ?)",
                    (identity, result.model_dump_json()),
                )
            return result

    def discover(self, request):
        from almanak.backtest.vendor_discovery import require_contract

        require_contract(request.asset)
        platforms = self.lookup(request, "asset_platforms")
        platform = platform_for(request.asset, platforms)
        coin = self.lookup(request, f"coins/{platform}/contract/{request.asset.reference}")
        return Discovery(
            asset=request.asset,
            platform=platform,
            coin_id=coin_for(request.asset, platform, coin),
            platforms_response=platforms,
            coin_response=coin,
            retrieved_at=time.time(),
        )

    def lookup(self, request, path):
        provider = self.vendors[request.provider]
        for attempt in range(provider.collection.max_retries + 1):
            try:
                with self.by_name[request.provider].admit():
                    return self.fetch_lookup(request, provider, path)
            except TransientError as error:
                if attempt == provider.collection.max_retries:
                    raise
                wait_to_retry(error, attempt)
        raise AssertionError("unreachable discovery retry state")

    def fetch_lookup(self, request, provider, path):
        key = self.values.get(provider.api_key_env)
        if not key:
            raise DataError(
                "missing_credential",
                f"missing vendor credential {provider.api_key_env}; supply --env-file",
            )
        host = "pro-api" if provider.api == "pro" else "api"
        self.capture.metric("vendor_discovery_http_requests")
        lookup = LookupRequest(provider=request.provider, asset=request.asset, path=path)
        identity = self.capture.begin_attempt(lookup)
        started = time.monotonic()
        try:
            self.provider_limit.admit()
            response = self.client.get(
                f"https://{host}.coingecko.com/api/v3/{path}",
                headers={f"x-cg-{provider.api}-api-key": key},
            )
            validate_status(response)
        except httpx.HTTPError:
            self.capture.finish_attempt(identity, time.monotonic() - started, "transport")
            raise TransientError("transport", "vendor discovery transport failed") from None
        except DataError as error:
            self.capture.finish_attempt(identity, time.monotonic() - started, error.kind)
            raise
        self.capture.finish_attempt(identity, time.monotonic() - started, "success")
        return response.text

    def close(self):
        self.client.close()


def validate_status(response):
    if response.status_code == 429 or response.status_code >= 500:
        raise TransientError("http", f"vendor HTTP {response.status_code}", retry_delay(response))
    if response.status_code != 200:
        raise DataError("http", f"vendor HTTP {response.status_code}; verify coverage and API plan")


def validate_response(response):
    validate_status(response)
    try:
        if not isinstance(response.json()["prices"], list):
            raise ValueError()
    except (ValueError, KeyError, TypeError):
        raise DataError("malformed", "vendor response lacks a price series") from None


def price_records(binding, requirement, evidence):
    source = binding.source
    if not isinstance(source, CoinGeckoPrice):
        raise ValueError("price normalization requires a vendor source")
    mappings = {item.discovery.model_dump_json() for item in evidence if item.discovery is not None}
    if len(mappings) > 1:
        raise ValueError("vendor history contains conflicting discovery mappings")
    points: dict[int, tuple[Decimal, str, VendorEvidence]] = {}
    for item in evidence:
        request = item.request
        if (request.provider, request.coin_id, request.interval, request.quote) != (
            source.provider,
            source.coin_id,
            source.interval,
            source.quote,
        ):
            raise ValueError("vendor evidence differs from requested source")
        validate_discovery(binding, item)
        raw = RawEvidence(body=item.model_dump_json())
        for stamp, value in json.loads(item.response, parse_float=Decimal)["prices"]:
            if not isinstance(stamp, int) or isinstance(stamp, bool) or stamp < 0:
                raise ValueError("vendor timestamp must be integer milliseconds")
            if isinstance(value, bool):
                raise ValueError("vendor price must be numeric, not boolean")
            point = (Decimal(value), raw.digest, item)
            if stamp in points and points[stamp][0] != point[0]:
                raise ValueError("vendor returned conflicting price history")
            points.setdefault(stamp, point)
    return normalize_points(binding, requirement, points)


def validate_discovery(binding, item):
    if binding.source.coin_id is not None:
        if item.discovery is not None:
            raise ValueError("explicit coin identity cannot contain discovered mapping")
        return
    if item.request.asset != binding.asset or item.discovery is None:
        raise ValueError("vendor history lacks discovery for the requested asset")
    if item.discovery.asset != binding.asset:
        raise ValueError("vendor discovery belongs to another asset")
    expected = f"/coins/{item.discovery.coin_id}/market_chart/range"
    if not item.endpoint.endswith(expected):
        raise ValueError("vendor history endpoint differs from discovered coin")


def normalize_points(binding, requirement, points):
    source = binding.source
    rows = []
    for stamp, (value, digest, item) in sorted(points.items()):
        observed = (stamp + 999) // 1000
        available = observed + source.availability_lag_seconds
        if available > requirement.end:
            continue
        row = VendorPriceRecord(
            key=binding.key,
            unit=requirement.unit,
            source_time=UnixTime(observed),
            available_at=UnixTime(max(requirement.start, available)),
            retrieved_at=item.retrieved_at,
            evidence=(digest,),
            reference=VendorReference(
                coin_id=item.discovery.coin_id if item.discovery else source.coin_id,
                discovery=(
                    CoinMapping(
                        asset=item.discovery.asset,
                        platform=item.discovery.platform,
                        coin_id=item.discovery.coin_id,
                    )
                    if item.discovery
                    else None
                ),
                endpoint=item.endpoint,
                timestamp_ms=stamp,
            ),
            payload=AssetPrice(asset=binding.asset, source=source, quote=source.quote, value=value),
        )
        if available <= requirement.start:
            rows = [row]
        else:
            rows.append(row)
    if not rows or rows[0].available_at != requirement.start:
        raise ValueError("vendor history lacks an available initialization price")
    boundaries = [*(r.available_at for r in rows[1:]), requirement.end]
    for row, next_time in zip(rows, boundaries, strict=True):
        if next_time - row.source_time > source.max_age_seconds:
            raise ValueError("vendor history is missing data or has incompatible resolution")
    validate_resolution(rows, source.seconds)
    return tuple(rows)


def validate_resolution(rows, seconds):
    for before, after in pairwise(rows):
        elapsed = after.source_time - before.source_time
        if not seconds // 2 <= elapsed <= seconds * 3 // 2:
            raise ValueError("vendor returned incompatible sampling resolution")
