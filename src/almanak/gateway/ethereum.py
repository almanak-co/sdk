from collections import OrderedDict, deque
from collections.abc import Callable, Iterable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar
from itertools import count
from time import monotonic, sleep
from typing import Any, Self

import httpx
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.caip import AssetType, ChainId
from almanak.chains import evm_network
from almanak.gateway.evm import chain_number, erc20_address, native_asset
from almanak.gateway.rate_limits import ProviderLimit
from almanak.gateway.request_scope import count_round_trip, count_rpc, read_deadline, timed
from almanak.gateway.reverts import is_revert, revert_data
from almanak.models import Asset, Block, Chain, address


class DataError(Exception):
    # Whether a contract reverted the call; a revert keeps the generic `rpc` kind.
    reverted = False
    details: dict | None = None

    def __init__(
        self,
        kind: str,
        message: str,
        *,
        code: int | None = None,
        transient: bool = False,
        details: dict | None = None,
    ):
        self.kind = kind
        # Structured facts behind the refusal, such as each allocation's remaining
        # obligations at teardown; JSON-serializable, never credentials.
        self.details = details
        # The JSON-RPC error code of an `rpc` refusal, or 429 for a provider's HTTP throttle.
        self.code = code
        # A provider read failure a later attempt may pass (see `transient`). The
        # coordinator holds the tick on it instead of stopping.
        self.transient = transient
        super().__init__(message)


class ProviderBusy(DataError):
    """HTTP 429 from the provider; `retry_after` is its requested wait in seconds, if any."""

    def __init__(self, label: str, retry_after: float | None, reason: str = "HTTP 429"):
        super().__init__("http", f"{label}: {reason}", code=429)
        self.retry_after = retry_after


class ProviderFault(DataError):
    """HTTP 5xx: the provider failed to serve a request it did not refuse."""


class UnreadableAnswer(ProviderFault):
    """HTTP 200 whose body is not JSON, such as an overloaded provider's HTML page.

    The provider failed to serve the request, as with a 5xx. JSON that answers wrongly
    stays `malformed` and is not retried.
    """


class BlockNotServed(DataError):
    """A node error saying it does not know the requested block or header.

    It keeps the `rpc` kind and code. A load-balanced backend that has not yet seen a
    block another backend served answers a read pinned to it this way.
    """


class MembersBusy(ProviderBusy):
    """A batch answered with HTTP 200 whose rate-limited members the provider did not serve.

    `results` holds every member in request order, with None at each index in `pending`.
    Only the pending members are sent again; the answered ones are kept.
    """

    def __init__(self, results: list[Any], pending: tuple[int, ...]):
        super().__init__("batch", None, "provider rate limit on batch members (JSON-RPC 429)")
        self.results, self.pending = results, pending


def retry_after(response: httpx.Response) -> float | None:
    try:
        return max(0.0, float(response.headers.get("Retry-After", "")))
    except ValueError:
        return None


# Batched walks and concurrent reads can meet a provider's burst limit. Idempotent reads
# are re-sent a bounded number of times after a throttle, each attempt admitted and
# counted again; a longer requested wait fails the read instead.
BUSY_RETRIES = 6
BUSY_BACKOFF = 0.5
BUSY_WAIT_LIMIT = 30.0
# Execution, receipt and authority reads share one gateway request and its caller's 60 s
# timeout, so each re-sends a transient failure within this shorter budget. It outlasts a
# load-balanced provider serving a just-mined block a few seconds late.
EXECUTION_RETRY_BUDGET = 8.0
# Longer than the execution budget, so a first attempt that times out has spent it; see
# `resend_bounds` for the one re-send it still gets.
PROVIDER_TIMEOUT = 15.0


def request_limit(limit: float) -> float:
    """`limit`, shortened so no retry wait ends past the gateway request's read deadline."""
    deadline = read_deadline()
    return limit if deadline is None else min(limit, deadline - monotonic())


def wait_out(busy: DataError, attempt: int, limit: float = BUSY_WAIT_LIMIT) -> None:
    """Sleep before re-sending a throttled read, or raise `busy` once the bounds are spent.

    `limit` is the longest wait allowed now.
    """
    wait = getattr(busy, "retry_after", None) or min(BUSY_BACKOFF * 2**attempt, 8.0)
    if attempt == BUSY_RETRIES or wait > limit:
        raise busy
    sleep(wait)


def resend_timeout(failure: DataError) -> float:
    """The provider timeout for re-sending after `failure`, ending by the read deadline.

    Raises `failure` when the gateway request's read deadline has already passed.
    """
    timeout = request_limit(PROVIDER_TIMEOUT)
    if timeout <= 0:
        raise failure
    return timeout


def resend_bounds(attempt: int, left: float, budget: float) -> tuple[float, float]:
    """The longest wait before the next re-send and the longest that re-send may take.

    `left` is what remains of the read's retry budget. A first attempt that leaves less
    than the first backoff, as one that timed out does, is still re-sent once: after that
    backoff, with at most `budget` as its provider timeout. One read therefore ends within
    `PROVIDER_TIMEOUT + BUSY_BACKOFF + budget` seconds or, when the budget was not spent,
    within `budget + PROVIDER_TIMEOUT`.
    """
    if attempt == 0 and left < BUSY_BACKOFF:
        return BUSY_BACKOFF, budget
    return left, PROVIDER_TIMEOUT


def still_pending(results: list[Any], pending: list[int], busy: ProviderBusy) -> list[int]:
    """Keep what a throttled batch answered; the members still owed are re-sent alone.

    `pending` maps the positions of the batch just sent to indexes of the original one.
    A whole-batch HTTP 429 answered nothing, so every member is still owed.
    """
    if not isinstance(busy, MembersBusy):
        return pending
    owed = set(busy.pending)
    for position, value in enumerate(busy.results):
        if position not in owed:
            results[pending[position]] = value
    return [pending[position] for position in busy.pending]


def envelope(payload: Any, request_id: Any) -> bool:
    return (
        isinstance(payload, dict)
        and payload.get("jsonrpc") == "2.0"
        and type(payload.get("id")) is int
        and payload["id"] == request_id
    )


# A tolerant batch member the provider did not answer with a result.
UNREAD = object()

# Member errors that mean the provider throttled a call rather than refused it. Alchemy
# answers a batch with HTTP 200 and code 429 on each member past its compute-unit rate;
# -32005 is the EIP-1474 "limit exceeded" code other providers use for the same.
RATE_LIMITED = frozenset((429, -32005))
# Reads without side effects, the only calls a transient failure re-sends. Submission keeps
# its own recovery, so a throttled send is never re-sent here; fork control calls are absent.
READS = frozenset(
    {
        "eth_chainId",
        "net_version",
        "eth_blockNumber",
        "eth_getBlockByNumber",
        "eth_getBlockByHash",
        "eth_call",
        "eth_estimateGas",
        "eth_getCode",
        "eth_getStorageAt",
        "eth_getBalance",
        "eth_getProof",
        "eth_getLogs",
        "eth_getTransactionCount",
        "eth_getTransactionByHash",
        "eth_getTransactionReceipt",
        "eth_gasPrice",
        "eth_feeHistory",
        "debug_traceTransaction",
    }
)
# Creation code returning (block.timestamp, block.number, blockhash(block.number - 1)) as the
# provider's EVM saw them: one call proves a pinned read runs at the pinned block.
CONTEXT_PROBE = "0x4260005243602052600143034060405260606000f3"
# Recent blocks a reader keeps headers and context proofs for. A gateway pins a fresh head
# on every tick, so both are bounded; an evicted block is fetched and proved again on use.
HEADER_CACHE = 4096


def remember(cache: OrderedDict, key: str, value: Any) -> None:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > HEADER_CACHE:
        cache.popitem(last=False)


def hash_selector(value: Any) -> bool:
    """An EIP-1898 block-hash parameter. A log filter's `blockHash` carries filter fields too."""
    return (
        isinstance(value, dict)
        and "blockHash" in value
        and set(value) <= {"blockHash", "requireCanonical"}
    )


# A load-balanced provider can answer a block read from a backend that has not yet seen
# a block another backend served moments ago, so an empty block read may pass later.
BLOCK_READS = frozenset({"eth_getBlockByNumber", "eth_getBlockByHash"})


def throttled(error: DataError) -> bool:
    """A provider's push-back, which a later attempt may pass, rather than a refusal."""
    return error.kind in ("rpc", "http") and error.code in RATE_LIMITED


def transient(method: str, error: DataError) -> bool:
    """Whether a later attempt of this idempotent read may pass where this one failed.

    A throttle, a transport failure, an HTTP 5xx, a body that is not JSON or an empty
    block read is transient. A refusal, a revert or a malformed answer is not, and no
    submission ever is.
    """
    if method not in READS:
        return False
    if error.kind == "unavailable":
        return method in BLOCK_READS
    return (
        throttled(error)
        or error.kind == "transport"
        or isinstance(error, (ProviderFault, BlockNotServed))
    )


# Generic node error codes some providers answer an unknown block with; the message tells
# that apart from other failures. Only the code reaches messages and logs.
BLOCK_ERROR_CODES = frozenset((-32000, -32001))
BLOCK_NOT_SERVED = ("header not found", "unknown block", "block not found")


def block_not_served(payload: dict) -> bool:
    error = payload.get("error")
    message = error.get("message") if isinstance(error, dict) else None
    return (
        member_code(payload) in BLOCK_ERROR_CODES
        and isinstance(message, str)
        and any(text in message.lower() for text in BLOCK_NOT_SERVED)
    )


def member_code(payload: dict) -> int | None:
    error = payload.get("error")
    code = error.get("code") if isinstance(error, dict) else None
    return code if type(code) is int else None


def refusal_code(response: httpx.Response) -> int | None:
    """The JSON-RPC error code of a non-200 answer, such as Alchemy's HTTP 400 refusals."""
    try:
        payload = response.json()
    except ValueError:
        return None
    return member_code(payload) if isinstance(payload, dict) else None


class Reverted(DataError):
    """A call the node executed and the EVM reverted; `data` is the bounded revert output.

    It keeps the generic `rpc` kind so reads that tolerate a reverting view call are
    unchanged. Execution simulation reclassifies it with the decoded reason.
    """

    reverted = True

    def __init__(self, message: str, data: bytes | None, code: int | None = None):
        super().__init__("rpc", message, code=code)
        self.data = data


# Each reason pairs a fixed phrase with the lowercased markers that name it.
Reasons = tuple[tuple[str, tuple[str, ...]], ...]
# Provider error text is never echoed: it can carry endpoints, keys or request data.
# A message that names one of these known failures is reported by the fixed phrase
# alone, never by any part of the provider's own text.
KNOWN_REASONS: Reasons = (
    ("EVM error OpcodeNotFound", ("opcodenotfound", "opcode not found", "invalid opcode")),
    ("header not found", ("header not found",)),
    # A node that pruned the state of an old block. The block only gets older, so this is
    # a refusal and never transient.
    ("the endpoint no longer serves state at that block", ("missing trie node",)),
)
# Calls that submit a transaction rather than read; a refusal names the submission.
SUBMISSIONS = frozenset({"eth_sendRawTransaction", "eth_sendTransaction"})
# Standard transaction-pool refusals: the go-ethereum wording that geth, reth and anvil
# share, plus erigon's own discard reasons and Nethermind's result names. The first match
# wins, so a marker that another reason's text contains follows that reason ("transaction
# underpriced" follows "replacement transaction underpriced").
SUBMISSION_REASONS: Reasons = (
    ("nonce too low", ("nonce too low", "oldnonce")),
    (
        "nonce too high",
        (
            "nonce too high",
            "nonce gap",
            "noncegap",
            "nonce too far in future",
            "noncetoofarinfuture",
        ),
    ),
    ("nonce has max value", ("nonce has max value",)),
    (
        "already known",
        (
            "already known",
            "known transaction",
            "already imported",
            "alreadyknown",
            "existing txn with same hash",
        ),
    ),
    ("replacement transaction underpriced", ("replacement transaction underpriced",)),
    (
        "transaction underpriced",
        ("underpriced", "fee too low", "feetoolow", "below minimum required priority fee"),
    ),
    ("intrinsic gas too low", ("intrinsic gas too low", "intrinsicgas")),
    ("insufficient funds", ("insufficient funds", "insufficientfunds")),
    ("max fee per gas less than block base fee", ("less than block base fee",)),
    (
        "max priority fee per gas higher than max fee per gas",
        ("max priority fee per gas higher than max fee per gas",),
    ),
    ("fee per gas higher than 2^256-1", ("higher than 2^256-1",)),
    ("exceeds block gas limit", ("exceeds block gas limit", "gaslimitexceeded")),
    ("gas limit reached", ("gas limit reached",)),
    ("tx fee exceeds the configured cap", ("exceeds the configured cap",)),
    ("txpool is full", ("txpool is full", "transaction pool is full", "sub-pool is full")),
    ("invalid sender", ("invalid sender",)),
    ("invalid chain id", ("invalid chain id", "chain id mismatch", "incompatible eip-155")),
    ("only replay-protected transactions allowed", ("only replay-protected",)),
)


def reasons_for(method: str) -> Reasons:
    return SUBMISSION_REASONS + KNOWN_REASONS if method in SUBMISSIONS else KNOWN_REASONS


def known_reason(error: Any, reasons: Reasons = KNOWN_REASONS) -> str | None:
    """The fixed phrase for a recognised node failure, or None."""
    message = error.get("message") if isinstance(error, dict) else None
    if not isinstance(message, str):
        return None
    lowered = message.lower()
    for phrase, markers in reasons:
        if any(marker in lowered for marker in markers):
            return phrase
    return None


def rejection(
    method: str, code: int | None, reason: str | None = None, *, status: int | None = None
) -> str:
    """`<method>: RPC rejected the requested read (JSON-RPC <code>: <reason>)`.

    A submission is reported as `RPC refused the transaction submission`, or as not
    accepted when the code is a throttle. `status` names a non-200 HTTP answer.
    """
    codes = [] if status is None else [f"HTTP {status}"]
    if code is not None:
        codes.append(f"JSON-RPC {code}")
    parts = [", ".join(codes)] if codes else []
    if reason is not None:
        parts.append(reason)
    detail = f" ({': '.join(parts)})" if parts else ""
    if method not in SUBMISSIONS:
        return f"{method}: RPC rejected the requested read{detail}"
    verb = "did not accept" if code in RATE_LIMITED else "refused"
    return f"{method}: RPC {verb} the transaction submission{detail}"


def http_refusal(label: str, response: httpx.Response, code: int | None) -> str:
    """`<label>: HTTP <status>`; a submission's names it with any known fixed phrase."""
    if label not in SUBMISSIONS:
        return f"{label}: HTTP {response.status_code}"
    try:
        payload = response.json()
    except ValueError:
        payload = None
    error = payload.get("error") if isinstance(payload, dict) else None
    reason = known_reason(error, reasons_for(label))
    return rejection(label, code, reason, status=response.status_code)


def member_result(method: str, payload: dict, *, allow_none: bool = False) -> Any:
    if "error" in payload:
        # The numeric code tells a throttled read from a refused one. A revert keeps its
        # data and never a reason; another refusal may add a fixed known phrase.
        code = member_code(payload)
        if is_revert(payload["error"]):
            raise Reverted(rejection(method, code), revert_data(payload["error"]), code)
        message = rejection(method, code, known_reason(payload["error"], reasons_for(method)))
        raise DataError("rpc", message, code=code)
    if "result" not in payload or (payload["result"] is None and not allow_none):
        raise DataError("unavailable", f"{method}: requested data is unavailable")
    return payload["result"]


class EthereumReader:
    """Trusted read transport. No signing, submission or source fallback.

    Batched reads are re-sent after a provider's throttle, within the bounds above. A
    single read fails on a transient failure (see `transient`) unless `retry_budget` is
    positive: `BUSY_WAIT_LIMIT` for observation readers and `EXECUTION_RETRY_BUDGET` for
    execution readers. Those re-send it within the same bounds, and no wait ends more
    than `retry_budget` seconds after the first attempt started. The one exception is a
    first attempt that spent the budget itself, as a timeout does with the execution
    budget: it is re-sent once (see `resend_bounds`). A re-send's provider timeout
    ends by the gateway request's read deadline, so a transient read still fails below
    its callers' 60 s timeouts. A failure that outlasts the bounds is
    marked `transient`. A transaction submission is never re-sent.

    An `observation` reader on a live chain pins `finalized` and `latest` the network's
    `observation_depth` behind the tag's block, where its providers serve pinned state.
    """

    def __init__(
        self,
        chain: Chain,
        endpoint: str,
        *,
        client: httpx.Client | None = None,
        local_transport: bool = False,
        retry_budget: float = 0.0,
        observation: bool = False,
    ):
        chain_number(chain.chain_id)
        self.chain = chain
        self.transport_chain = (
            ChainId("eip155:31337") if chain.environment.startswith("anvil:") else chain.chain_id
        )
        self.provider_limit = ProviderLimit("rpc")
        self._local_transport = local_transport
        self._retry_budget = retry_budget
        self._endpoint = endpoint
        self._client = client if client is not None else httpx.Client(timeout=PROVIDER_TIMEOUT)
        self._owns_client = client is None
        self._ids = count(1)
        self._rpc_count: ContextVar[int] = ContextVar("ethereum_rpc_count", default=0)
        self._verified = False
        self._pinned_context: ContextVar[Block | None] = ContextVar("ethereum_pinned", default=None)
        # Forks serve their own blocks; only remote providers need number pins or proof.
        try:
            network = evm_network(chain.chain_id)
        except ValueError:
            network = None
        remote = not local_transport and chain.environment == "mainnet"
        self._pin_by_number = remote and network is not None and network.pins_reads_by_number
        self._verify_context = remote and network is not None and network.verifies_read_context
        # Through an observation gateway too: its own reader proves each block it is sent.
        self._tag_depth = (
            network.observation_depth
            if observation and network is not None and chain.environment == "mainnet"
            else 0
        )
        # Headers by hash (number, timestamp, parent hash) and the blocks whose context
        # the provider already proved; both hold canonical facts, so contexts may share them.
        self._headers: OrderedDict[str, tuple[int, int, str]] = OrderedDict()
        self._proved: OrderedDict[str, None] = OrderedDict()

    @property
    def _pinned(self) -> Block | None:
        # Permission inspection and runtime requests can share this transport.
        # Their independently pinned observations must not overwrite one another.
        return self._pinned_context.get()

    @contextmanager
    def retained_pin(self) -> Iterator[None]:
        """Restore the caller's pinned block after a nested check pins its own.

        A nested check such as Safe authority verification pins a fresh head. The
        caller's later reads stay at the block it pinned; each read still requires
        that block to be canonical.
        """
        token = self._pinned_context.set(self._pinned)
        try:
            yield
        finally:
            self._pinned_context.reset(token)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        if self._owns_client:
            self._client.close()

    @property
    def rpc_count(self) -> int:
        # Concurrent gateway polling must not enter another request's measurements.
        return self._rpc_count.get()

    @property
    def _retries(self) -> bool:
        # A local fork serves its own blocks: its failures mean the node is gone, so they
        # fail at once and are never marked transient.
        return self._retry_budget > 0 and not self._local_transport

    def _rpc(self, method: str, params: list[Any], *, allow_none: bool = False) -> Any:
        return self._send(method, self._pin(method, params), allow_none=allow_none)

    def _pin(self, method: str, params: list[Any]) -> list[Any]:
        """Apply the network's pinning to every EIP-1898 block-hash parameter of a read.

        A number pin replaces the hash only for a known canonical header, and every block is
        proved before its first read, so a provider that runs a read elsewhere is refused.
        """
        if not (self._pin_by_number or self._verify_context):
            return params
        pinned = []
        for value in params:
            if hash_selector(value):
                number = self._prove(value["blockHash"])
                value = hex(number) if self._pin_by_number else value
            pinned.append(value)
        return pinned

    def _header(self, block_hash: str) -> tuple[int, int, str]:
        header = self._headers.get(block_hash)
        if header is not None:
            return header
        data = self._send("eth_getBlockByHash", [block_hash, False], allow_none=True)
        if data is None:
            raise DataError("block_mismatch", f"provider does not know block {block_hash}")
        return self._remember(data, block_hash)

    def _remember(self, data: Any, block_hash: str | None = None) -> tuple[int, int, str]:
        try:
            header = (int(data["number"], 16), int(data["timestamp"], 16), data["parentHash"])
            if block_hash is not None and data["hash"] != block_hash:
                raise ValueError("header hash differs")
            remember(self._headers, data["hash"], header)
        except (KeyError, TypeError, ValueError):
            raise DataError("malformed", "block header is incomplete or invalid") from None
        return header

    def _prove(self, block_hash: str) -> int:
        number, timestamp, parent = self._header(block_hash)
        if not self._verify_context or block_hash in self._proved:
            return number
        selector = (
            hex(number)
            if self._pin_by_number
            else {"blockHash": block_hash, "requireCanonical": True}
        )
        raw = self._send("eth_call", [{"data": CONTEXT_PROBE}, selector])
        try:
            words = bytes.fromhex(raw[2:]) if isinstance(raw, str) and raw.startswith("0x") else b""
            if len(words) != 96:
                raise ValueError("probe result is not three words")
            seen = (
                int.from_bytes(words[32:64]),
                int.from_bytes(words[:32]),
                "0x" + words[64:96].hex(),
            )
        except ValueError:
            raise DataError("malformed", "eth_call: invalid block context probe result") from None
        if seen != (number, timestamp, parent):
            raise DataError(
                "provider_context",
                f"RPC ran a read pinned to block {number} (timestamp {timestamp}) at block "
                f"{seen[0]} (timestamp {seen[1]}); it does not serve the pinned block's state",
                details={"pinned": [number, timestamp, parent], "executed": list(seen)},
            )
        remember(self._proved, block_hash, None)
        return number

    def _send(self, method: str, params: list[Any], *, allow_none: bool = False) -> Any:
        deadline = monotonic() + self._retry_budget
        timeout = None
        for attempt in range(BUSY_RETRIES + 1):
            try:
                return self._rpc_once(method, params, allow_none, timeout)
            except DataError as error:
                if not self._retries or not transient(method, error):
                    raise
                error.transient = True
                wait, longest = resend_bounds(attempt, deadline - monotonic(), self._retry_budget)
                wait_out(error, attempt, request_limit(wait))
                timeout = min(resend_timeout(error), longest)
        raise AssertionError("unreachable")

    def _rpc_once(
        self, method: str, params: list[Any], allow_none: bool, timeout: float | None = None
    ) -> Any:
        self.admit_provider_calls()
        request_id = next(self._ids)
        self._rpc_count.set(self.rpc_count + 1)
        count_rpc(method)
        count_round_trip()
        with timed("rpc"):
            payload = self._post(
                method,
                {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
                timeout,
            )
        if not envelope(payload, request_id):
            raise DataError("malformed", f"{method}: invalid response envelope")
        if block_not_served(payload):
            code = member_code(payload)
            raise BlockNotServed(
                "rpc", f"{method}: provider has not served the block (JSON-RPC {code})", code=code
            )
        return member_result(method, payload, allow_none=allow_none)

    def _post(self, label: str, body: Any, timeout: float | None = None) -> Any:
        # A first attempt keeps the client's timeout; only a re-send is bounded by the
        # gateway request's read deadline, which every request shares, execution included.
        try:
            response = self._client.post(
                self._endpoint,
                json=body,
                timeout=httpx.USE_CLIENT_DEFAULT if timeout is None else timeout,
            )
        except (httpx.HTTPError, httpx.InvalidURL):
            raise DataError("transport", f"{label}: transport failed") from None
        if response.status_code == 429:
            raise ProviderBusy(label, retry_after(response))
        if response.status_code >= 500:
            raise ProviderFault(
                "http", f"{label}: HTTP {response.status_code}", code=refusal_code(response)
            )
        if response.status_code != 200:
            code = refusal_code(response)
            raise DataError("http", http_refusal(label, response, code), code=code)
        try:
            return response.json()
        except ValueError:
            raise UnreadableAnswer("malformed", f"{label}: response is not JSON") from None

    def _rpc_batches(
        self,
        batches: Iterable[Sequence[tuple[str, list[Any]]]],
        in_flight: int,
        *,
        tolerant: bool = False,
    ) -> Iterator[list[Any]]:
        """Yield each JSON-RPC batch's results in order, with at most `in_flight` outstanding.

        Every member is one logical RPC operation. Admission and counting happen here, in
        the caller's context, before a batch leaves, so the rate cap and per-request
        measurements see every member although worker threads send the requests. A
        `tolerant` batch yields `UNREAD` for a member the provider refused or left empty.
        """
        with ThreadPoolExecutor(in_flight) as pool:
            window = [in_flight]

            def send(calls, timeout=None):
                self.admit_provider_calls(len(calls))
                self._rpc_count.set(self.rpc_count + len(calls))
                for method, _ in calls:
                    count_rpc(method)
                count_round_trip()
                body = [
                    {
                        "jsonrpc": "2.0",
                        "id": next(self._ids),
                        "method": m,
                        "params": self._pin(m, p),
                    }
                    for m, p in calls
                ]
                return pool.submit(self._send_batch, body, tolerant, timeout)

            def settle(calls, future):
                results: list[Any] = [None] * len(calls)
                pending = list(range(len(calls)))
                for attempt in range(BUSY_RETRIES + 1):
                    try:
                        with timed("rpc"):
                            answered = future.result()
                    except ProviderBusy as busy:
                        # Batches always re-send a throttle; only an exhausted batch of
                        # allowlisted reads from a remote provider holds the coordinator.
                        busy.transient = not self._local_transport and all(
                            method in READS for method, _ in calls
                        )
                        wait_out(busy, attempt, request_limit(BUSY_WAIT_LIMIT))
                        pending = still_pending(results, pending, busy)
                        # After the provider pushes back, stop overlapping batches.
                        window[0] = 1
                        future = send([calls[index] for index in pending], resend_timeout(busy))
                        continue
                    for index, value in zip(pending, answered, strict=True):
                        results[index] = value
                    return results
                raise AssertionError("unreachable")

            pending: deque = deque()
            for calls in batches:
                pending.append((calls, send(calls)))
                while len(pending) >= window[0]:
                    yield settle(*pending.popleft())
            while pending:
                yield settle(*pending.popleft())

    def _send_batch(
        self, body: list[dict], tolerant: bool = False, timeout: float | None = None
    ) -> list[Any]:
        payload = self._post("batch", body, timeout)
        if isinstance(payload, dict) and "error" in payload:
            # A provider without batch support answers with one error object. Fail
            # instead of reading the history some other way.
            raise DataError("rpc", "batch: RPC rejected the JSON-RPC batch")
        if not isinstance(payload, list) or len(payload) != len(body):
            raise DataError("malformed", "batch: invalid response count")
        rows = {}
        for row in payload:
            identity = row.get("id") if isinstance(row, dict) else None
            if not envelope(row, identity) or identity in rows:
                raise DataError("malformed", "batch: invalid response envelope")
            rows[identity] = row
        # Providers may reorder members; each result belongs to the request with its id.
        if set(rows) != {request["id"] for request in body}:
            raise DataError("malformed", "batch: response identity differs")
        results: list[Any] = []
        pending = []
        for index, request in enumerate(body):
            row = rows[request["id"]]
            if member_code(row) in RATE_LIMITED:
                results.append(None)
                pending.append(index)
                continue
            try:
                results.append(member_result(request["method"], row))
            except DataError:
                if not tolerant:
                    raise
                results.append(UNREAD)
        if pending:
            raise MembersBusy(results, tuple(pending))
        return results

    def batch(self, calls: Sequence[tuple[str, list[Any]]]) -> list[Any]:
        """Send `calls` as one JSON-RPC batch; the results are in call order."""
        # JSON-RPC 2.0 refuses an empty batch, so nothing is sent for no calls.
        if not calls:
            return []
        return next(self._rpc_batches([calls], 1))

    def admit_provider_calls(self, units: int = 1) -> None:
        # Anvil premium reads are charged once at the fork proxy upstream hop.
        # Only trusted provisioning can select local transport, never a chain label.
        if not self._local_transport:
            self.provider_limit.admit(units)

    def verify_chain(self) -> None:
        if self._verified:
            return
        try:
            reported = int(self._rpc("eth_chainId", []), 16)
        except (ValueError, TypeError):
            raise DataError("malformed", "eth_chainId: invalid chain identifier") from None
        if reported != chain_number(self.transport_chain):
            raise DataError("chain_mismatch", "RPC chain does not match the requested chain")
        self._verified = True

    def pin_block(self, selector: int | str) -> Block:
        self.verify_chain()
        if type(selector) is int and selector >= 0:
            tag = hex(selector)
        elif selector in ("finalized", "latest"):
            tag = selector
        else:
            raise ValueError("block must be a non-negative integer, 'finalized' or 'latest'")
        data = self._rpc("eth_getBlockByNumber", [tag, False])
        if type(selector) is not int and self._tag_depth:
            selector = max(0, self._header_number(data) - self._tag_depth)
            data = self._rpc("eth_getBlockByNumber", [hex(selector), False])
        try:
            block = Block(
                chain=self.chain,
                number=int(data["number"], 16),
                hash=data["hash"],
                timestamp=int(data["timestamp"], 16),
            )
        except (KeyError, TypeError, ValueError):
            raise DataError("malformed", "block header is incomplete or invalid") from None
        if type(selector) is int and block.number != selector:
            raise DataError("malformed", "RPC returned a different block number")
        if self._pin_by_number or self._verify_context:
            self._remember(data)
        self._pinned_context.set(block)
        return block

    @staticmethod
    def _header_number(data: Any) -> int:
        try:
            return int(data["number"], 16)
        except (KeyError, TypeError, ValueError):
            raise DataError("malformed", "block header is incomplete or invalid") from None

    def call(
        self,
        to: str,
        signature: str,
        block: Block,
        *,
        inputs: Sequence[str] = (),
        args: Sequence[Any] = (),
        outputs: Sequence[str] = ("uint256",),
        rpc: Callable[[str, list[Any]], Any] | None = None,
    ) -> tuple[Any, ...]:
        self.verify_chain()
        if block.chain != self.chain:
            raise DataError("chain_mismatch", "block belongs to another chain/environment")
        if block != self._pinned:
            pinned = (
                f"{self._pinned.number} ({self._pinned.hash})" if self._pinned is not None else None
            )
            raise DataError(
                "block_mismatch",
                f"pin this block through the reader before using it: {signature} on {to} "
                f"at block {block.number} ({block.hash}), reader pinned block {pinned}",
            )
        target = address(to)
        data = keccak(text=signature)[:4] + encode(inputs, args)
        raw = (rpc or self._rpc)(
            "eth_call",
            [
                {"to": target, "data": "0x" + data.hex()},
                {"blockHash": block.hash, "requireCanonical": True},
            ],
        )
        try:
            if not isinstance(raw, str) or not raw.startswith("0x"):
                raise ValueError("invalid bytes")
            encoded = bytes.fromhex(raw[2:])
            decoded = decode(outputs, encoded)
            if encode(outputs, decoded) != encoded:
                raise ValueError("noncanonical ABI result")
        except Exception:
            # Distinct from envelope "malformed": the provider answered, but the
            # contract's return data does not decode as the requested ABI.
            raise DataError("abi", f"{signature}: invalid ABI response") from None
        return decoded

    def asset(self, token: AssetType, block: Block) -> Asset:
        if not isinstance(token, AssetType):
            raise ValueError("asset metadata requires a CAIP-19 asset type")
        if block.chain != self.chain or block != self._pinned:
            raise DataError("block_mismatch", "asset metadata requires this reader's pinned block")
        if token.namespace == "slip44" and token == native_asset(self.chain.chain_id):
            decimals = 18
        else:
            (decimals,) = self.call(
                erc20_address(token, self.chain.chain_id), "decimals()", block, outputs=("uint8",)
            )
        return Asset(chain=self.chain, identifier=token, decimals=decimals)
