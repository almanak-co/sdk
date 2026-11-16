"""HyperCore `/info` history in the historical gateway only (D4, blueprint 30, section 12).

The gateway admits `candleSnapshot` and `fundingHistory` for the markets, interval and
window a preparation declared, and nothing else. It reads the mainnet API anonymously
through the closed whitelist, so each answer has matched its recorded shape, and keeps
every answer in the capture: a resumed preparation reads the same answers again without
the network. The API keeps only the latest 5000 candles of an interval, which is why the
answers, not the API, are the evidence a dataset retains.
"""

import hashlib
import json
import time
from typing import Literal

from pydantic import model_validator

from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import (
    CandleInterval,
    CandleSnapshot,
    Coin,
    FundingHistory,
    HyperCoreInfo,
    InfoRequest,
    InfoResult,
    Millis,
    matched,
)
from almanak.models import Model

RETRIES = 3
TRANSIENT = ("rate_limit", "unavailable")


class HistoryScope(Model):
    """One declared read: a market's candles of one interval, or its funding, in a window
    of milliseconds."""

    product: Literal["candles", "funding"]
    coin: Coin
    interval: CandleInterval | None = None
    start_ms: Millis
    end_ms: Millis

    @model_validator(mode="after")
    def shape(self):
        if (self.product == "candles") != (self.interval is not None):
            raise ValueError("only a candle scope names an interval")
        if self.end_ms < self.start_ms:
            raise ValueError("a history scope must not end before it starts")
        return self

    def admits(self, query) -> bool:
        """Whether `query` lies inside this scope's market, interval and window."""
        if isinstance(query, CandleSnapshot):
            asked = query.req
            return (self.product, self.coin, self.interval) == (
                "candles",
                asked.coin,
                asked.interval,
            ) and (self.start_ms <= asked.start_time <= asked.end_time <= self.end_ms)
        if isinstance(query, FundingHistory):
            return (self.product, self.coin) == ("funding", query.coin) and (
                self.start_ms <= query.start_time <= query.end_time <= self.end_ms
            )
        return False


def request_key(request: InfoRequest) -> str:
    return hashlib.sha256(request.model_dump_json().encode()).hexdigest()


class HyperCoreCollector:
    def __init__(self, capture, scopes: tuple[HistoryScope, ...], client=None, pause=time.sleep):
        self.capture = capture
        self.scopes = scopes
        self.client = client
        self.pause = pause
        with capture.connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS hypercore_observations "
                "(key TEXT PRIMARY KEY, body TEXT NOT NULL)"
            )

    def read(self, request: InfoRequest) -> InfoResult:
        """The retained answer to `request`, read from the venue once and kept in the capture."""
        if not any(scope.admits(request.info) for scope in self.scopes):
            raise ValueError("HyperCore history request is outside the declared preparation")
        key = request_key(request)
        with self.capture.connect() as db:
            row = db.execute(
                "SELECT body FROM hypercore_observations WHERE key=?", (key,)
            ).fetchone()
        if row:
            self.capture.metric("hypercore_cache_hits")
            retained = InfoResult.model_validate_json(row[0])
            if retained.request != request:
                raise ValueError("HyperCore capture identity mismatch")
            return retained
        result = self.fetch(request)
        with self.capture.connect() as db:
            db.execute(
                "INSERT INTO hypercore_observations VALUES (?, ?)", (key, result.model_dump_json())
            )
        return result

    def fetch(self, request: InfoRequest) -> InfoResult:
        """The venue's answer, retrying rate limits and unavailability, never other refusals."""
        if self.client is None:
            self.client = HyperCoreInfo()
        for attempt in range(RETRIES + 1):
            self.capture.metric("hypercore_http_requests")
            try:
                return self.client.fetch(request)
            except DataError as error:
                if error.kind not in TRANSIENT or attempt == RETRIES:
                    raise
                self.pause(2**attempt)
        raise AssertionError("unreachable retry state")

    def close(self):
        if self.client is not None:
            self.client.close()


class HistoryClient:
    """A preparation's side of the hop: every answer is checked again here."""

    def __init__(self, reader):
        self.reader = reader

    def fetch(self, request: InfoRequest) -> InfoResult:
        result = InfoResult.model_validate_json(
            json.dumps(self.reader.send("hypercore_read", request))
        )
        if result.request != request:
            raise DataError("source_mismatch", "gateway answered another HyperCore request")
        matched(request.info, result.result)
        return result
