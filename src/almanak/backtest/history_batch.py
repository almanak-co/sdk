"""Bounded archive batches retain the identity and evidence of each logical read."""

import json
import sys
import threading
import time
from contextlib import ExitStack
from typing import Annotated

import httpx
from pydantic import Field, RootModel, model_validator

from almanak.backtest.collection import CollectionLimits, ProviderGate, bounded_map
from almanak.backtest.history import (
    MAX_BATCH_READS,
    HistoricalGateway,
    ReadEvidence,
    ReadRequest,
)
from almanak.gateway.ethereum import RATE_LIMITED, DataError, throttled, transient
from almanak.models import Chain, Model

# Provider rate limits one archive batch waits out before it fails. With the doubling
# wait that is about 40 seconds, many times a provider's one-second rate window.
THROTTLE_RETRIES = 8


class ReadBatch(Model):
    requests: Annotated[tuple[ReadRequest, ...], Field(min_length=1, max_length=MAX_BATCH_READS)]

    @model_validator(mode="after")
    def distinct(self):
        if len({r.key for r in self.requests}) != len(self.requests):
            raise ValueError("historical batch contains duplicate requests")
        return self


class HistoricalRequest(RootModel[ReadRequest | ReadBatch]):
    pass


class BatchedHistoricalGateway(HistoricalGateway):
    def __init__(self, *args, limits=None, provider_keys=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.limits = limits or {}
        self.gates: dict[object, ProviderGate] = {}
        self.provider_gates = {}
        self.request_locks = tuple(threading.RLock() for _ in range(257))
        for chain in self.readers:
            key = (provider_keys or {}).get(chain, chain)
            limit = self.limits.get(chain, CollectionLimits())
            gate = self.gates.setdefault(key, ProviderGate(limit))
            if not gate.same_budget(limit):
                raise ValueError("shared provider requires identical concurrency and rate limits")
            self.provider_gates[chain] = gate

    def dispatch(self, method, request):
        request = request.root if isinstance(request, HistoricalRequest) else request
        if isinstance(request, ReadRequest):
            if method != "read":
                raise ValueError("unsupported historical read operation")
            requests: tuple[ReadRequest, ...] = (request,)
        elif method == "read_many" and isinstance(request, ReadBatch):
            requests = request.requests
        else:
            raise ValueError("unsupported historical batch operation")
        # Fixed stripes bound lock memory; sorted acquisition prevents batch deadlocks.
        stripes = sorted({int(r.key, 16) % len(self.request_locks) for r in requests})
        with ExitStack() as stack:
            for stripe in stripes:
                stack.enter_context(self.request_locks[stripe])
            result = self.batch(ReadBatch(requests=requests))
        return result[0] if isinstance(request, ReadRequest) else result

    def batch(self, batch):
        retained = {}
        missing: dict[Chain, list[ReadRequest]] = {}
        for request in batch.requests:
            self.validate_request(request)
            evidence = self.capture.get(request)
            if evidence is None:
                missing.setdefault(request.chain, []).append(request)
            else:
                self.validate_response(request, evidence.result)
                self.capture.metric("cache_hits")
                retained[request.key] = evidence
        if any(chain not in self.readers for chain in missing):
            raise DataError("missing_capture", "offline batch lacks a required observation")
        for chain, requests in missing.items():
            retained.update(self.fetch_chain(chain, requests))
        return tuple(retained[r.key] for r in batch.requests)

    def fetch_chain(self, chain, requests):
        """Read one chain's missing requests in provider batches that may overlap."""
        limits = self.limits.get(chain, CollectionLimits())
        size = limits.batch_size
        maximum = self.readers[chain].provider_limit.maximum
        if maximum is not None:
            size = min(size, max(1, maximum))
        groups = [requests[offset : offset + size] for offset in range(0, len(requests), size)]
        # The provider gate bounds what is in flight across every request this gateway
        # serves; the pool only lets one large request use that whole allowance. An
        # operator's rate cap refuses what exceeds it instead of pacing it, so the
        # batches of a capped provider follow one another as they always did.
        workers = limits.max_in_flight if maximum is None else 1
        if len(groups) == 1:
            return self.fetch_group(chain, groups[0], limits.max_retries)
        retained = {}
        for part in bounded_map(
            lambda group: self.fetch_group(chain, group, limits.max_retries), groups, workers
        ):
            retained.update(part)
        return retained

    def fetch_group(self, chain, requests, retries):
        """One provider batch, re-sent after a transient failure without its answered reads."""
        retained = {}

        def send():
            pending = [r for r in requests if r.key not in retained]
            retained.update(self.fetch_batch(self.readers[chain], pending))

        def settled():
            retained.update(retained_results(self.capture, requests))
            return all(r.key in retained for r in requests)

        self.at_gate(chain, retries, send, settled)
        return retained

    def fetch(self, reader, request):
        """A read outside the batches, such as a price pool's resolution, sent alone.

        It passes the provider's gate and waits out a transient failure like a batch.
        """
        read = super().fetch

        def send():
            try:
                return read(reader, request)
            except DataError as error:
                if not transient(request.method, error):
                    raise
                wait = getattr(error, "retry_after", None) or 0.0
                raise TransientError(
                    error.kind, str(error), wait, throttled=throttled(error)
                ) from None

        limits = self.limits.get(request.chain, CollectionLimits())
        return self.at_gate(request.chain, limits.max_retries, send)

    def at_gate(self, chain, retries, send, settled=lambda: False):
        """`send()` admitted by the chain's provider gate, again after a transient failure.

        A provider's rate limit is back-pressure, not a fault: it is waited out up to
        `THROTTLE_RETRIES` times, apart from the `retries` other transient failures get.
        `settled` says whether a failed attempt already retained everything it owed.
        """
        gate = self.provider_gates[chain]
        failures = throttles = 0
        while True:
            try:
                with gate.admit():
                    return send()
            except TransientError as error:
                throttles, failures = throttles + error.throttled, failures + (not error.throttled)
                if failures > retries or throttles > max(retries, THROTTLE_RETRIES):
                    raise
                # Every request of this provider waits, not only this one, and a
                # throttle also narrows how many may overlap from here on.
                wait = retry_wait(error, failures + throttles - 1)
                gate.hold(wait, throttled=error.throttled)
                if settled():
                    return None

    def fetch_batch(self, reader, requests):
        self.capture.metric("http_requests")
        identities = begin_batch(self.capture, requests)
        print(
            json.dumps({"event": "historical_provider_batch", "reads": len(requests)}),
            file=sys.stderr,
            flush=True,
        )
        started = time.monotonic()
        try:
            single = self.limits.get(requests[0].chain, CollectionLimits()).batch_size == 1
            results = transport_batch(reader, requests, single=single)
            for request, response in zip(requests, results, strict=True):
                if "error" not in response and response.get("result") is not None:
                    self.validate_response(request, response["result"])
        except ValueError:
            finish_failure(self.capture, identities, time.monotonic() - started, "source_mismatch")
            raise
        except DataError as error:
            finish_failure(self.capture, identities, time.monotonic() - started, error.kind)
            raise
        retained, failed = retain_batch(
            self.capture, requests, identities, results, time.monotonic() - started
        )
        if failed:
            rejected = [r for r in results if "error" in r or r.get("result") is None]
            codes = {r.get("error", {}).get("code") for r in rejected}
            if codes <= RATE_LIMITED:
                raise TransientError(
                    "rpc", "historical provider rate limited reads", throttled=True
                )
            if all(code in (-32005, -32016, -32603) for code in codes):
                raise TransientError("rpc", "historical provider temporarily rejected reads")
            if all(code is not None and code not in (-32601, -32602) for code in codes):
                # Rejections outside the permanent protocol set are usually
                # provider capacity blips; bound retries ride them out while
                # explicit permanent codes and absent results still fail fast.
                raise TransientError(
                    "rpc", f"historical provider rejected reads (codes {sorted(codes)})"
                )
            raise DataError("rpc", "historical batch contains rejected or unavailable reads")
        return retained


def begin_batch(capture, requests):
    started = time.time()
    with capture.connect() as db:
        return tuple(
            db.execute(
                "INSERT INTO provider_activity (request, started_at) VALUES (?, ?)",
                (request.key, started),
            ).lastrowid
            for request in requests
        )


def finish_failure(capture, identities, elapsed, status):
    with capture.connect() as db:
        db.executemany(
            "UPDATE provider_activity SET elapsed=?, status=? WHERE id=? AND status IS NULL",
            ((elapsed, status, identity) for identity in identities),
        )


def transport_batch(reader, requests, *, single=False):
    body = [
        dict(jsonrpc="2.0", id=i, method=r.method, params=list(r.params))
        for i, r in enumerate(requests)
    ]
    reader.admit_provider_calls(len(requests))
    try:
        response = reader._client.post(reader._endpoint, json=body[0] if single else body)
    except (httpx.HTTPError, httpx.InvalidURL):
        raise TransientError("transport", "historical batch transport failed") from None
    if response.status_code == 429 or response.status_code >= 500:
        raise TransientError(
            "http",
            f"historical batch HTTP {response.status_code}",
            retry_delay(response),
            throttled=response.status_code == 429,
        )
    if response.status_code != 200:
        raise DataError("http", f"historical batch HTTP {response.status_code}")
    try:
        payload = response.json()
    except ValueError:
        # Providers answer an overloaded moment with an HTML or truncated 200 page.
        raise TransientError("malformed", "historical batch response is not JSON") from None
    return validate_response([payload] if single else payload, len(requests))


def validate_response(payload, size):
    if not isinstance(payload, list) or len(payload) != size:
        raise DataError("malformed", "historical batch has an invalid response count")
    indexed = {}
    for row in payload:
        if not isinstance(row, dict) or row.get("jsonrpc") != "2.0":
            raise DataError("malformed", "historical batch has an invalid response envelope")
        identity = row.get("id")
        if type(identity) is not int or not 0 <= identity < size or identity in indexed:
            raise DataError("malformed", "historical batch response identity differs")
        indexed[identity] = row
    return tuple(indexed[i] for i in range(size))


def retain_batch(capture, requests, identities, results, elapsed):
    # Each row measures a logical read's latency. Summing these durations includes
    # overlapping time for batched reads; it is not the run's wall-clock duration.
    failed = False
    with capture.connect() as db:
        for request, identity, result in zip(requests, identities, results, strict=True):
            available = "error" not in result and result.get("result") is not None
            status = "success" if available else "rpc"
            db.execute(
                "UPDATE provider_activity SET elapsed=?, status=? WHERE id=?",
                (elapsed, status, identity),
            )
            if available:
                retain_response(db, request, result["result"])
            else:
                failed = True
    return retained_results(capture, requests), failed


def retain_response(db, request, result):
    evidence = ReadEvidence(request=request, result=result, retrieved_at=time.time())
    db.execute(
        "INSERT OR IGNORE INTO observations VALUES (?, ?)",
        (request.key, evidence.model_dump_json()),
    )
    row = db.execute("SELECT body FROM observations WHERE key=?", (request.key,)).fetchone()
    if ReadEvidence.model_validate_json(row[0]).result != result:
        raise DataError("changed_history", "batch returned conflicting historical evidence")


def retained_results(capture, requests):
    result = {}
    for request in requests:
        evidence = capture.get(request)
        if evidence is not None:
            result[request.key] = evidence
    return result


class TransientError(DataError):
    """A failure worth another attempt; `throttled` marks the provider's own rate limit."""

    def __init__(self, kind, message, retry_after=0.0, *, throttled=False):
        super().__init__(kind, message)
        self.retry_after = retry_after
        self.throttled = throttled


def retry_delay(response):
    from email.utils import parsedate_to_datetime

    value = response.headers.get("Retry-After", "0")
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return 0.0


def retry_wait(error, attempt):
    if error.retry_after > 60:
        raise DataError("rate_limit", "provider retry delay exceeds 60-second collection budget")
    return max(error.retry_after, min(0.25 * 2**attempt, 8))


def wait_to_retry(error, attempt):
    time.sleep(retry_wait(error, attempt))
