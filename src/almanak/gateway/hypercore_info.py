"""HyperCore reads through Hyperliquid's `/info` API.

Hyperliquid's API is an accepted source (20 October 2026) for HyperCore
observations, reconciliation and the fork model's seed: its figures are the
venue's own state, not a vendor's. The whitelist is a closed set of query
shapes whose request bodies are built here, so a caller cannot ask the API
anything else, and every answer must match the closed shape that recorded
mainnet responses fix (`hypercore_shapes.py`). An answer that does not match is
refused, so a change to the API stops reads instead of being misread.

`/info` has no as-of query. An answer is HyperCore's state when the API served
it, so it is retained with its request and the time this process received it,
never tied to a HyperEVM block. Only the first perpetual dex is admitted; HIP-3
dexes are not. The API needs no key.
"""

import json
import math
import time
from typing import Annotated, Literal

import httpx
from pydantic import Field, JsonValue, ValidationError, model_validator

from almanak.gateway.auth import loopback_endpoint
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_shapes import RESPONSES, Cloid, Millis
from almanak.gateway.rate_limits import ProviderLimit
from almanak.models import Address, Model

API = "https://api.hyperliquid.xyz/info"

# A first-dex perpetual as `meta` names it. Spot pairs (`@107`, `PURR/USDC`) and HIP-3
# markets (`xyz:XYZ100`) are not admitted.
Coin = Annotated[str, Field(pattern=r"^[A-Za-z0-9]{1,32}$")]
Oid = Annotated[int, Field(strict=True, ge=0, le=2**64 - 1)]


class Meta(Model):
    type: Literal["meta"] = "meta"


class MetaAndAssetCtxs(Model):
    type: Literal["metaAndAssetCtxs"] = "metaAndAssetCtxs"


class SpotMeta(Model):
    type: Literal["spotMeta"] = "spotMeta"


class L2Book(Model):
    # Full precision only: the API's level aggregation (nSigFigs, mantissa) is not admitted.
    type: Literal["l2Book"] = "l2Book"
    coin: Coin


class ClearinghouseState(Model):
    type: Literal["clearinghouseState"] = "clearinghouseState"
    user: Address


class SpotClearinghouseState(Model):
    type: Literal["spotClearinghouseState"] = "spotClearinghouseState"
    user: Address


class ActiveAssetData(Model):
    # An account's leverage and mode for one market, also while it holds no position.
    type: Literal["activeAssetData"] = "activeAssetData"
    user: Address
    coin: Coin


class OpenOrders(Model):
    # Every open order of the account on the first dex, spot included, whoever placed it.
    type: Literal["openOrders"] = "openOrders"
    user: Address


class ExtraAgents(Model):
    # The account's approved agents with their expiries: the evidence of a renewal,
    # which userRole cannot tell from the approval it replaces.
    type: Literal["extraAgents"] = "extraAgents"
    user: Address


class UserRole(Model):
    type: Literal["userRole"] = "userRole"
    user: Address


class UserAbstraction(Model):
    # The account abstraction mode decides whether the perp state is meaningful at all:
    # under a unified account or portfolio margin the spot state holds every balance.
    type: Literal["userAbstraction"] = "userAbstraction"
    user: Address


class UserRateLimit(Model):
    type: Literal["userRateLimit"] = "userRateLimit"
    user: Address


class OrderStatus(Model):
    type: Literal["orderStatus"] = "orderStatus"
    user: Address
    oid: Oid | Cloid


class TimeRange(Model):
    # Both ends are inclusive milliseconds. The end is required, so a page names its
    # own range instead of meaning "until whenever the API served it".
    user: Address
    start_time: Millis
    end_time: Millis

    @model_validator(mode="after")
    def ordered(self):
        if self.end_time < self.start_time:
            raise ValueError("a time range must not end before it starts")
        return self


class UserFillsByTime(TimeRange):
    # Fills are read unaggregated: one fill per trade, so each keeps its own tid.
    type: Literal["userFillsByTime"] = "userFillsByTime"


class UserFunding(TimeRange):
    type: Literal["userFunding"] = "userFunding"


class UserNonFundingLedgerUpdates(TimeRange):
    type: Literal["userNonFundingLedgerUpdates"] = "userNonFundingLedgerUpdates"


# Candle intervals a backtest may read, in seconds. The API also serves 3d, 1w and 1M.
CANDLE_SECONDS = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "8h": 28800,
    "12h": 43200,
    "1d": 86400,
}
CandleInterval = Literal["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d"]
# The most candles one request may cover, so the weight reserved before sending always
# covers the answer: the API itself would return up to 5000.
MAX_CANDLES = 500


class CandleRange(Model):
    # Both ends are inclusive candle open times in milliseconds.
    coin: Coin
    interval: CandleInterval
    start_time: Millis
    end_time: Millis

    @model_validator(mode="after")
    def ordered(self):
        if self.end_time < self.start_time:
            raise ValueError("a candle range must not end before it starts")
        step = CANDLE_SECONDS[self.interval] * 1000
        if (self.end_time - self.start_time) // step + 1 > MAX_CANDLES:
            raise ValueError(f"a candle range covers at most {MAX_CANDLES} candles")
        return self


class CandleSnapshot(Model):
    # Trade candles of one first-dex market. The API keeps only the latest 5000 of an
    # interval; backtest preparation reads them under D4 (blueprint 30, section 12).
    type: Literal["candleSnapshot"] = "candleSnapshot"
    req: CandleRange


class FundingHistory(Model):
    # The market's hourly funding rates, both ends inclusive; read for backtests (D4).
    type: Literal["fundingHistory"] = "fundingHistory"
    coin: Coin
    start_time: Millis
    end_time: Millis

    @model_validator(mode="after")
    def ordered(self):
        if self.end_time < self.start_time:
            raise ValueError("a time range must not end before it starts")
        return self


InfoQuery = Annotated[
    Meta
    | MetaAndAssetCtxs
    | SpotMeta
    | L2Book
    | ClearinghouseState
    | SpotClearinghouseState
    | ActiveAssetData
    | OpenOrders
    | ExtraAgents
    | UserRole
    | UserAbstraction
    | UserRateLimit
    | OrderStatus
    | UserFillsByTime
    | UserFunding
    | UserNonFundingLedgerUpdates
    | CandleSnapshot
    | FundingHistory,
    Field(discriminator="type"),
]


class InfoRequest(Model):
    info: InfoQuery


class InfoResult(Model):
    request: InfoRequest
    # Milliseconds since the epoch when the answer was received. `/info` answers at read
    # time, so this, not a block, places the observation.
    received_at: Millis
    # The answer as received, after it matched its recorded shape.
    result: JsonValue


# The API's documented request weights against its limit of 1200 per minute per IP;
# every other admitted query weighs 20.
WEIGHTS = {
    "l2Book": 2,
    "clearinghouseState": 2,
    "spotClearinghouseState": 2,
    "orderStatus": 2,
    "userRole": 60,
}
# These also weigh one more per 20 items returned. A page holds at most this many
# items (2000 fills, 500 funding payments, 2000 ledger entries, as recorded), so the
# most a page can cost is reserved before sending and the unused part returned once
# the answer is known. The API documents no per-item weight for the ledger; charging
# its long pages as if it did can only overcount, which keeps the cap under the
# per-IP limit either way.
PER_ITEM_PAGES = {
    "userFillsByTime": 2000,
    "userFunding": 500,
    "userNonFundingLedgerUpdates": 2000,
    "fundingHistory": 500,
    "candleSnapshot": MAX_CANDLES,
}
ITEMS_PER_WEIGHT = 20
# Candles weigh one more per 60 returned.
CANDLES_PER_WEIGHT = 60
WIRE_NAMES = {"start_time": "startTime", "end_time": "endTime"}


def weight(query) -> int:
    return WEIGHTS.get(query.type, 20)


def reserved_weight(query) -> int:
    return answered_weight(query, PER_ITEM_PAGES.get(query.type, 0))


def answered_weight(query, items: int) -> int:
    # Rounded up, so a partial batch is charged rather than missed.
    per = CANDLES_PER_WEIGHT if query.type == "candleSnapshot" else ITEMS_PER_WEIGHT
    extra = math.ceil(items / per) if query.type in PER_ITEM_PAGES else 0
    return weight(query) + extra


def body(query) -> dict:
    """The exact API request body of one validated query."""
    return wire(query.model_dump())


def wire(fields: dict) -> dict:
    return {
        WIRE_NAMES.get(name, name): wire(value) if isinstance(value, dict) else value
        for name, value in fields.items()
    }


def matched(query, answer: object) -> JsonValue:
    """The answer, refused unless it matches the closed shape recorded for its query."""
    try:
        text = json.dumps(answer)
        # JSON mode keeps strict semantics: JSON arrays validate as typed tuples.
        RESPONSES[query.type].validate_json(text)
    except ValidationError as error:
        where = shape_error(error)
        raise DataError(
            "malformed", f"hyperliquid_info: {query.type} answer differs from its shape{where}"
        ) from None
    echoed(query, json.loads(text))
    return json.loads(text)


def echoed(query, answer) -> None:
    """Refuse an answer that names another market, account, order or time than asked.

    The gateway wraps an answer in the request it was sent for, so an answer for
    another identifier would otherwise reach the caller under the asked one.
    """
    mismatch = None
    if query.type == "l2Book" and answer["coin"] != query.coin:
        mismatch = "market"
    elif query.type == "activeAssetData" and (answer["user"], answer["coin"]) != (
        query.user,
        query.coin,
    ):
        mismatch = "account or market"
    elif query.type == "orderStatus" and answer["status"] == "order":
        order = answer["order"]["order"]
        echo = order["oid"] if isinstance(query.oid, int) else order["cloid"]
        mismatch = None if echo == query.oid else "order"
    elif isinstance(query, TimeRange) and any(
        not query.start_time <= entry["time"] <= query.end_time for entry in answer
    ):
        mismatch = "time range"
    elif isinstance(query, CandleSnapshot):
        mismatch = candles_mismatch(query.req, answer)
    elif isinstance(query, FundingHistory):
        if any(entry["coin"] != query.coin for entry in answer):
            mismatch = "market"
        elif any(not query.start_time <= entry["time"] <= query.end_time for entry in answer):
            mismatch = "time range"
    if mismatch is not None:
        raise DataError(
            "source_mismatch", f"hyperliquid_info: {query.type} answer is for another {mismatch}"
        )


def candles_mismatch(asked: CandleRange, answer) -> str | None:
    if any((entry["s"], entry["i"]) != (asked.coin, asked.interval) for entry in answer):
        return "market or interval"
    if any(not asked.start_time <= entry["t"] <= asked.end_time for entry in answer):
        return "time range"
    return None


def shape_error(error: ValidationError) -> str:
    location = ".".join(str(part) for part in error.errors()[0]["loc"])
    return f" at {location}" if location else ""


def typed(result: InfoResult):
    """The answer of a retained result as its typed shape."""
    return RESPONSES[result.request.info.type].validate_json(json.dumps(result.result))


def document(query, response: httpx.Response) -> JsonValue:
    """The API's answer, or the category of its refusal."""
    if response.status_code == 429:
        raise DataError("rate_limit", "hyperliquid_info: API rate limit")
    if response.status_code != 200:
        raise DataError(
            "unavailable", f"hyperliquid_info: {query.type}: HTTP {response.status_code}"
        )
    try:
        answer = response.json()
    except ValueError:
        raise DataError("malformed", f"hyperliquid_info: {query.type} answer is not JSON") from None
    return matched(query, answer)


def received_now() -> int:
    return time.time_ns() // 1_000_000


class HyperCoreInfo:
    """Answers whitelisted `/info` queries; the read gateway holds one of these.

    It reads the mainnet API, or in a fork workspace only the synthesized HyperCore model
    (`model`, a loopback endpoint) on the fork's clock, whose answers pass the same shapes.
    """

    def __init__(self, *, model: str | None = None, clock=received_now):
        self.limit = ProviderLimit("hyperliquid")
        self.client = httpx.Client(timeout=30)
        self.api = API if model is None else loopback_endpoint(model) + "/info"
        self.clock = clock

    def fetch(self, request: InfoRequest) -> InfoResult:
        query = request.info
        reservation = self.limit.reserve(reserved_weight(query))
        try:
            response = self.client.post(
                self.api, json=body(query), headers={"Content-Type": "application/json"}
            )
        except httpx.HTTPError:
            raise DataError(
                "unavailable", f"hyperliquid_info: {query.type}: request failed"
            ) from None
        answer = document(query, response)
        if isinstance(answer, list):
            reservation.settle(answered_weight(query, len(answer)))
        return InfoResult(request=request, received_at=self.clock(), result=answer)

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        self.close()


class GatewayInfo:
    """Sends `/info` queries to a read gateway. The answer is checked again here."""

    def __init__(self, endpoint_url: str, token: str):
        self.endpoint = loopback_endpoint(endpoint_url)
        self.token = token
        self.client = httpx.Client(timeout=httpx.Timeout(15.0, read=60.0))

    def fetch(self, request: InfoRequest) -> InfoResult:
        try:
            response = self.client.post(
                self.endpoint + "/info",
                content=request.model_dump_json(),
                headers={
                    "Authorization": "Bearer " + self.token,
                    "Content-Type": "application/json",
                },
            )
            payload = response.json()
        except (httpx.HTTPError, ValueError):
            raise DataError(
                "gateway_unavailable", "hyperliquid_info: read gateway request failed"
            ) from None
        if not isinstance(payload, dict):
            raise DataError("malformed", "hyperliquid_info: invalid gateway response")
        if response.status_code != 200:
            kind = payload.get("error", "gateway_error")
            message = payload.get("message", "hyperliquid_info: gateway refused the query")
            raise DataError(str(kind), str(message))
        return answered(request, payload.get("result"))

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        self.close()


def answered(request: InfoRequest, result: object) -> InfoResult:
    """The gateway's answer, refused unless it names the query sent and matches its shape."""
    try:
        observed = InfoResult.model_validate_json(json.dumps(result))
    except (TypeError, ValueError):
        raise DataError("malformed", "hyperliquid_info: invalid gateway response") from None
    if observed.request != request:
        raise DataError("source_mismatch", "hyperliquid_info: gateway answered a different query")
    matched(request.info, observed.result)
    return observed
