"""Durable historical RPC inputs and a scoped, read-only gateway transport."""

import hashlib
import json
import sqlite3
import sys
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

import httpx
from pydantic import JsonValue, ValidationError

from almanak.backtest.history_logs import validate_logs
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import Address, Chain, Hash32, Model

# Logical reads one gateway batch may carry. The gateway splits them into provider
# batches; the bound stays well inside the reader's cache of inactive responses.
MAX_BATCH_READS = 1024
# A batch answered with reads it did not ask for, or with inconsistent history, is a
# broken source rather than an unavailable optimization, so it is never ignored.
PREFETCH_INTEGRITY_ERRORS = frozenset(
    {"source_mismatch", "malformed", "corrupt_capture", "changed_history", "chain_mismatch"}
)


class LogScope(Model):
    address: Address
    topics: tuple[Hash32, Hash32]


class ReadRequest(Model):
    chain: Chain
    method: Literal[
        "eth_chainId",
        "eth_getBlockByNumber",
        "eth_call",
        "eth_getCode",
        "eth_getStorageAt",
        "eth_getBalance",
        "eth_getLogs",
        "eth_getTransactionByHash",
        "eth_getTransactionReceipt",
    ]
    params: tuple[JsonValue, ...]

    @property
    def key(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()


class ReadEvidence(Model):
    request: ReadRequest
    result: JsonValue
    retrieved_at: float


class Capture:
    def __init__(self, path: Path):
        self.path = path
        self.metrics: dict[str, int] = {}
        self._lock = threading.RLock()
        self._db = sqlite3.connect(path, timeout=30, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        with self.connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS collection_metrics "
                "(name TEXT PRIMARY KEY, value INTEGER)"
            )
            db.execute("CREATE TABLE IF NOT EXISTS observations (key TEXT PRIMARY KEY, body TEXT)")
            db.execute(
                "CREATE TABLE IF NOT EXISTS provider_activity (id INTEGER PRIMARY KEY, "
                "request TEXT NOT NULL, started_at REAL NOT NULL, elapsed REAL, status TEXT)"
            )

    @contextmanager
    def connect(self):
        with self._lock, self._db:
            yield self._db

    def metric(self, name, amount=1):
        with self._lock:
            self.metrics[name] = self.metrics.get(name, 0) + amount

    def close(self):
        with self.connect() as db:
            for name, value in self.metrics.items():
                db.execute(
                    "INSERT INTO collection_metrics VALUES (?, ?) "
                    "ON CONFLICT(name) DO UPDATE SET value=value+excluded.value",
                    (name, value),
                )
            self.metrics.clear()
        with self._lock:
            self._db.close()

    def get(self, request: ReadRequest) -> ReadEvidence | None:
        with self.connect() as db:
            row = db.execute("SELECT body FROM observations WHERE key=?", (request.key,)).fetchone()
        if row is None:
            return None
        evidence = ReadEvidence.model_validate_json(row[0])
        if evidence.request != request:
            raise DataError("corrupt_capture", "captured request does not match its identity")
        return evidence

    def put(self, request: ReadRequest, result) -> ReadEvidence:
        evidence = ReadEvidence(request=request, result=result, retrieved_at=time.time())
        with self.connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO observations VALUES (?, ?)",
                (request.key, evidence.model_dump_json()),
            )
        retained = self.get(request)
        if retained is None or retained.result != result:
            raise DataError("changed_history", "provider returned conflicting historical evidence")
        return retained

    def captured_logs(self, chain):
        with self.connect() as db:
            rows = db.execute(
                "SELECT body FROM observations WHERE json_extract(body, '$.request.method')=? ",
                ("eth_getLogs",),
            ).fetchall()
        values = (ReadEvidence.model_validate_json(row[0]) for row in rows)
        return tuple(value for value in values if value.request.chain == chain)

    def count(self) -> int:
        with self.connect() as db:
            return db.execute("SELECT COUNT(*) FROM observations").fetchone()[0]

    def begin_attempt(self, request: ReadRequest):
        with self.connect() as db:
            return db.execute(
                "INSERT INTO provider_activity (request, started_at) VALUES (?, ?)",
                (request.key, time.time()),
            ).lastrowid

    def finish_attempt(self, identity, elapsed: float, status: str):
        with self.connect() as db:
            db.execute(
                "UPDATE provider_activity SET elapsed=?, status=? WHERE id=? AND status IS NULL",
                (elapsed, status, identity),
            )

    def activity(self) -> dict:
        with self.connect() as db:
            total, distinct, elapsed, pending = db.execute(
                "SELECT COUNT(*), COUNT(DISTINCT request), COALESCE(SUM(elapsed), 0), "
                "COALESCE(SUM(status IS NULL), 0) FROM provider_activity"
            ).fetchone()
        with self.connect() as db:
            metrics = dict(db.execute("SELECT name, value FROM collection_metrics"))
        for name, value in self.metrics.items():
            metrics[name] = metrics.get(name, 0) + value
        return {
            "collection": metrics,
            "attempts": total,
            "distinct_requests": distinct,
            "retries": total - distinct,
            "elapsed_seconds": elapsed,
            "duration_semantics": "sum_of_overlapping_logical_request_latencies_not_wall_time",
            "unknown_outcomes": pending,
        }


class HistoricalGateway:
    def __init__(
        self,
        capture: Capture,
        readers: dict[Chain, EthereumReader],
        contracts,
        log_scopes=None,
        caller_scopes=None,
        transaction_scopes=None,
    ):
        self.capture = capture
        self.readers = readers
        self.contracts: dict[Chain, tuple[Address, ...]] = contracts
        self.log_scopes = log_scopes or {}
        self.caller_scopes = caller_scopes or {}
        self.transaction_scopes = transaction_scopes or {}

    def dispatch(self, method: str, request: ReadRequest):
        if method != "read" or request.chain not in self.contracts:
            raise ValueError("historical request is outside this gateway binding")
        self.validate_request(request)
        evidence = self.capture.get(request)
        if evidence is None:
            if request.chain not in self.readers:
                raise DataError("missing_capture", "offline run is missing a required observation")
            reader = self.readers[request.chain]
            print(
                json.dumps(
                    {
                        "event": "historical_provider_request",
                        "method": request.method,
                        "request": request.key,
                    }
                ),
                file=sys.stderr,
                flush=True,
            )
            result = self.fetch(reader, request)
            self.validate_response(request, result)
            evidence = self.capture.put(request, result)
        self.validate_response(request, evidence.result)
        return evidence

    def validate_request(self, request):
        if request.chain not in self.contracts:
            raise ValueError("historical request is outside this gateway binding")
        validate_read(
            request,
            self.contracts[request.chain],
            self.log_scopes.get(request.chain, ()),
            implementation_lookup=self.capture.get,
            caller_scopes=self.caller_scopes.get(request.chain, ()),
            transaction_lookup=self.transaction_source,
        )

    def transaction_source(self, request):
        from almanak.backtest.history_authority import source_log

        return source_log(
            request,
            self.capture.captured_logs(request.chain),
            self.transaction_scopes.get(request.chain, ()),
            self.capture.get,
        )

    def validate_response(self, request, result):
        from almanak.backtest.history_authority import validate_transaction_result

        if request.method in ("eth_getTransactionByHash", "eth_getTransactionReceipt"):
            validate_transaction_result(request, result, self.transaction_source(request))

    def fetch(self, reader, request):
        identity = self.capture.begin_attempt(request)
        started = time.monotonic()
        try:
            result = reader._rpc(request.method, list(request.params))
        except DataError as error:
            self.capture.finish_attempt(identity, time.monotonic() - started, error.kind)
            raise
        self.capture.finish_attempt(identity, time.monotonic() - started, "success")
        return result

    def close(self):
        self.capture.close()
        for reader in self.readers.values():
            reader.__exit__()


def validate_read(
    request: ReadRequest,
    contracts,
    log_scopes=(),
    *,
    implementation_lookup=None,
    caller_scopes=(),
    transaction_lookup=None,
):
    params = request.params
    if request.method == "eth_chainId":
        if params:
            raise ValueError("chain identity takes no parameters")
        return
    if request.method == "eth_getBlockByNumber":
        validate_header_request(params)
        return
    if request.method == "eth_getLogs":
        if any(isinstance(s, LogScope) for s in log_scopes):
            validate_logs_request(params, tuple(s for s in log_scopes if isinstance(s, LogScope)))
        else:
            validate_logs(params, log_scopes)
        return
    if request.method in ("eth_getTransactionByHash", "eth_getTransactionReceipt"):
        if transaction_lookup is None:
            raise ValueError("historical transaction access requires captured source authority")
        transaction_lookup(request)
        return
    validate_contract_request(request, contracts, implementation_lookup, caller_scopes)


def validate_logs_request(params, scopes):
    if len(params) != 1 or not isinstance(params[0], dict):
        raise ValueError("historical logs require one scoped filter")
    query = params[0]
    if set(query) != {"address", "topics", "fromBlock", "toBlock"}:
        raise ValueError("historical logs require a fixed event/pool and numbered interval")
    if not any(query["address"] == s.address and query["topics"] == list(s.topics) for s in scopes):
        raise ValueError("historical log event or pool is outside the declared scope")
    first, last = query["fromBlock"], query["toBlock"]
    if not isinstance(first, str) or not isinstance(last, str):
        raise ValueError("historical log bounds must be hexadecimal block numbers")
    low, high = int(first, 16), int(last, 16)
    if low < 0 or (first, last) != (hex(low), hex(high)) or not 0 <= high - low < 2000:
        raise ValueError("historical log range must be canonical and at most 2000 blocks")


def validate_header_request(params):
    if len(params) != 2 or params[1] is not False:
        raise ValueError("historical reads require headers without transactions")
    selector = params[0]
    if selector == "finalized":
        return
    if not isinstance(selector, str) or not selector.startswith("0x"):
        raise ValueError("historical header requires a numbered or finalized anchor")
    if hex(int(selector, 16)) != selector:
        raise ValueError("header number must be canonical hexadecimal")


def validate_contract_request(request, contracts, implementation_lookup=None, caller_scopes=()):
    params = request.params
    expected = 3 if request.method == "eth_getStorageAt" else 2
    if len(params) != expected:
        raise ValueError("invalid historical request parameters")
    block = params[-1]
    if not isinstance(block, dict) or set(block) != {"blockHash", "requireCanonical"}:
        raise ValueError("historical contract reads require a canonical hash anchor")
    if block["requireCanonical"] is not True:
        raise ValueError("canonical history is required")
    target = params[0]
    if request.method == "eth_call":
        if isinstance(target, dict) and "from" in target:
            from almanak.backtest.history_authority import validate_caller

            validate_caller(target, caller_scopes)
        elif not isinstance(target, dict) or set(target) != {"to", "data"}:
            raise ValueError("historical calls permit only target and calldata")
        target = target["to"]
    if target not in contracts and not implementation_code(
        request, contracts, implementation_lookup
    ):
        raise ValueError("historical contract is outside the configured markets")


# `implementation()` on an EIP-1967 beacon.
BEACON_IMPLEMENTATION = "0x5c60da1b"


def implementation_code(request, contracts, lookup):
    """Whether a read follows a configured proxy to its code, by pointers already captured.

    Only a same-block pointer captured from a configured contract permits the extra read:
    an implementation's code, a beacon's code, the beacon's exact `implementation()`
    call, and the code that call names. It grants no other calls or storage access.
    """
    if lookup is None or request.method not in ("eth_getCode", "eth_call"):
        return False
    block = request.params[-1]
    if request.method == "eth_call":
        target = request.params[0]
        beacon = target.get("to")
        return (
            target.get("data") == BEACON_IMPLEMENTATION
            and isinstance(beacon, str)
            and beacon.lower() in beacons(request, contracts, lookup)
        )
    address = request.params[0]
    if any(pointer_matches(word, address) for word in proxy_pointers(request, contracts, lookup)):
        return True
    for beacon in beacons(request, contracts, lookup):
        if isinstance(address, str) and beacon == address.lower():
            return True
        call = ReadRequest(
            chain=request.chain,
            method="eth_call",
            params=({"to": beacon, "data": BEACON_IMPLEMENTATION}, block),
        )
        evidence = lookup(call)
        if evidence is not None and pointer_matches(evidence.result, address):
            return True
    return False


def proxy_pointers(request, contracts, lookup, slots=None):
    from almanak.gateway.roles_storage import (
        IMPLEMENTATION_SLOT,
        MATIC_IMPLEMENTATION_SLOT,
        USDC_IMPLEMENTATION_SLOT,
    )

    for target in contracts:
        for slot in slots or (
            IMPLEMENTATION_SLOT,
            USDC_IMPLEMENTATION_SLOT,
            MATIC_IMPLEMENTATION_SLOT,
        ):
            pointer = ReadRequest(
                chain=request.chain,
                method="eth_getStorageAt",
                params=(target, hex(slot), request.params[-1]),
            )
            evidence = lookup(pointer)
            if evidence is not None:
                yield evidence.result


def beacons(request, contracts, lookup):
    from almanak.gateway.roles_storage import BEACON_SLOT

    found = set()
    for word in proxy_pointers(request, contracts, lookup, (BEACON_SLOT,)):
        if isinstance(word, str) and len(word) == 66 and word.lower().startswith("0x" + "0" * 24):
            address = "0x" + word[-40:].lower()
            if int(address, 16):
                found.add(address)
    return found


def pointer_matches(value, target):
    if not isinstance(value, str) or not isinstance(target, str):
        return False
    # Addresses compare case-insensitively: a request may carry a checksummed address.
    return value.lower() == "0x" + "0" * 24 + target.removeprefix("0x").lower()


class HistoricalReader(EthereumReader):
    def __init__(self, chain: Chain, endpoint: str, token: str):
        super().__init__(chain, endpoint)
        # Outbound provider calls have deadlines; local reads may wait for throttling/retries.
        self._client.timeout = httpx.Timeout(15.0, read=None)
        self.token = token
        self.used: set[str] = set()
        self.cache: OrderedDict[str, ReadEvidence] = OrderedDict()

    def _rpc(self, method, params, *, allow_none=False):
        request = ReadRequest(chain=self.chain, method=method, params=tuple(params))
        if request.key in self.cache:
            self.used.add(request.key)
            self.cache.move_to_end(request.key)
            self.trim_cache()
            return self.cache[request.key].result
        evidence = ReadEvidence.model_validate_json(json.dumps(self.send("read", request)))
        if evidence.request != request:
            raise DataError("source_mismatch", "gateway returned a different historical request")
        self.used.add(request.key)
        self.cache[request.key] = evidence
        self.trim_cache()
        return evidence.result

    def send(self, operation, request):
        try:
            response = self._client.post(
                self._endpoint + "/" + operation,
                content=request.model_dump_json(),
                headers={"Authorization": "Bearer " + self.token},
            )
        except httpx.HTTPError:
            raise DataError("gateway_unavailable", "historical gateway connection failed") from None
        if response.status_code != 200:
            # The local gateway sanitizes provider failures before returning them.
            # Retain the category and request identity so missing capture is actionable.
            failure = response.json()
            raise DataError(
                failure["error"],
                f"{failure['message']} (request {getattr(request, 'key', 'batch')})",
            )
        return response.json()["result"]

    def prefetch(self, requests):
        from almanak.backtest.history_batch import ReadBatch

        if any(r.chain != self.chain for r in requests):
            raise DataError("chain_mismatch", "prefetch belongs to a different historical chain")
        missing = tuple(r for r in requests if r.key not in self.cache)
        for start in range(0, len(missing), MAX_BATCH_READS):
            group = missing[start : start + MAX_BATCH_READS]
            rows = self.send("read_many", ReadBatch(requests=group))
            if not isinstance(rows, list) or len(rows) != len(group):
                raise DataError("source_mismatch", "gateway batch has an invalid response count")
            for request, row in zip(group, rows, strict=True):
                evidence = ReadEvidence.model_validate_json(json.dumps(row))
                if evidence.request != request:
                    raise DataError("source_mismatch", "gateway batch returned a different request")
                self.cache[request.key] = evidence
        self.trim_cache()

    def trim_cache(self):
        # Evidence being assembled is pinned until its consumer clears `used`.
        # Everything else remains durable in the gateway and may be fetched again.
        if len(self.cache) <= 4096 + len(self.used):
            return
        for key in tuple(self.cache):
            if key not in self.used:
                del self.cache[key]
            if len(self.cache) <= 4096 + len(self.used):
                break


def warm(reader, requests) -> bool:
    """Read `requests` ahead in batches; False when the reader cannot, and reads go alone.

    Reading ahead is only an optimization. A reader without batches, a refused batch and
    a capture that lacks some of the reads all leave each later read to be made on its
    own, where it succeeds or fails as itself. A batch answered inconsistently is a
    source failure and is raised.
    """
    prefetch = getattr(reader, "prefetch", None)
    if prefetch is None:
        return False
    try:
        prefetch(tuple(requests))
    except DataError as error:
        if error.kind in PREFETCH_INTEGRITY_ERRORS:
            raise
        return False
    except ValidationError:
        # A batch row that is not valid evidence is a malformed answer, not a refusal.
        raise
    except ValueError:
        # An unbatched gateway refuses the batch itself.
        return False
    return True
