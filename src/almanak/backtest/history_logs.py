"""Bounded EVM log capabilities and canonical native-order validation."""

from typing import Annotated

from pydantic import Field, TypeAdapter, model_validator

from almanak.models import Address, Block, Hash32, Model, UInt, address

MAX_LOG_BLOCKS = 2_000
# Log windows read ahead in one batch: each adds its filter and two boundary headers.
LOG_WINDOWS_AHEAD = 256
HASH = TypeAdapter(Hash32)


class LogScope(Model):
    address: Address
    topics: Annotated[tuple[Hash32, ...], Field(min_length=1)]
    first_block: UInt
    last_block: UInt

    @model_validator(mode="after")
    def ordered(self):
        if self.last_block < self.first_block or len(set(self.topics)) != len(self.topics):
            raise ValueError("log scope requires an ordered range and distinct event topics")
        return self


def block_number(raw) -> int:
    if not isinstance(raw, str) or not raw.startswith("0x"):
        raise ValueError("log range requires explicit canonical block numbers")
    number = int(raw, 16)
    if number < 0 or hex(number) != raw:
        raise ValueError("log range requires explicit canonical block numbers")
    return number


def log_filter(params):
    if len(params) != 1 or not isinstance(params[0], dict):
        raise ValueError("historical log reads require one filter")
    value = params[0]
    if set(value) != {"fromBlock", "toBlock", "address", "topics"}:
        raise ValueError("historical logs require address, topics and a bounded block range")
    first, last = block_number(value["fromBlock"]), block_number(value["toBlock"])
    if not 0 <= last - first < MAX_LOG_BLOCKS:
        raise ValueError("historical log range exceeds its maximum block count")
    target = address(value["address"])
    topics = value["topics"]
    if not isinstance(topics, list) or not 1 <= len(topics) <= 4:
        raise ValueError("historical logs require an explicit event signature filter")
    selected = topics[0] if isinstance(topics[0], list) else [topics[0]]
    if not selected:
        raise ValueError("historical logs require at least one event signature")
    signatures = tuple(HASH.validate_python(topic) for topic in selected)
    for topic in topics[1:]:
        if topic is not None:
            HASH.validate_python(topic)
    return target, first, last, signatures


def validate_logs(params, scopes: tuple[LogScope, ...]):
    target, first, last, signatures = log_filter(params)
    for scope in scopes:
        if (
            target == scope.address
            and scope.first_block <= first <= last <= scope.last_block
            and set(signatures) <= set(scope.topics)
        ):
            return
    raise ValueError("historical logs are outside the bound address, event topics or range")


class HistoricalLog(Model):
    block: Block
    transaction_hash: Hash32
    transaction_index: UInt
    log_index: UInt
    address: Address
    topics: Annotated[tuple[Hash32, ...], Field(min_length=1, max_length=4)]
    data: Annotated[str, Field(pattern=r"^0x(?:[0-9a-f]{2})*$")]

    @property
    def order(self) -> tuple[int, int, int]:
        return self.block.number, self.transaction_index, self.log_index


def log_params(scope: LogScope, first: int, last: int) -> list:
    return [
        {
            "fromBlock": hex(first),
            "toBlock": hex(last),
            "address": scope.address,
            "topics": [list(scope.topics)],
        }
    ]


def read_log_windows(history, scope: LogScope, first: int, last: int):
    """Yield the logs of each `MAX_LOG_BLOCKS` window from `first` through `last`, in order.

    Windows are read ahead in batches. Every window is still read and validated by
    `read_logs`, so an unavailable batch only leaves each window to read alone.
    """
    lows = range(first, last + 1, MAX_LOG_BLOCKS)
    for offset in range(0, len(lows), LOG_WINDOWS_AHEAD):
        windows = [
            (low, min(last, low + MAX_LOG_BLOCKS - 1))
            for low in lows[offset : offset + LOG_WINDOWS_AHEAD]
        ]
        warm_logs(history, scope, windows)
        for low, high in windows:
            yield read_logs(history, scope, low, high)


def warm_logs(history, scope: LogScope, windows) -> None:
    from almanak.backtest.history import ReadRequest, warm

    reader = history.reader
    if not hasattr(reader, "prefetch"):
        return
    filters = tuple(
        ReadRequest(chain=reader.chain, method="eth_getLogs", params=tuple(log_params(scope, *w)))
        for w in windows
    )
    bounds = sorted({number for window in windows for number in window} - history.headers.keys())
    if not warm(reader, filters) or not history.retain_headers(bounds):
        return
    # Each log is checked against the header of its block, so those are read ahead too.
    # A malformed answer is left for `read_logs` to refuse.
    numbers = set()
    for request in filters:
        evidence = reader.cache.get(request.key)
        rows = evidence.result if evidence is not None else None
        for row in rows if isinstance(rows, list) else ():
            raw = row.get("blockNumber") if isinstance(row, dict) else None
            if isinstance(raw, str) and raw.startswith("0x"):
                try:
                    numbers.add(int(raw, 16))
                except ValueError:
                    pass
    history.retain_headers(sorted(numbers - history.headers.keys()))


def read_logs(history, scope: LogScope, first: int, last: int) -> tuple[HistoricalLog, ...]:
    params = log_params(scope, first, last)
    validate_logs(params, (scope,))
    if last > history.upper.number:
        raise ValueError("log range exceeds finalized historical coverage")
    # Retain both boundary headers even when a range contains no matching events.
    history.header(first)
    history.header(last)
    raw = history.reader._rpc("eth_getLogs", params)
    if not isinstance(raw, list):
        raise ValueError("historical logs must be an array")
    logs = tuple(decode_log(history, scope, first, last, item) for item in raw)
    orders = tuple(log.order for log in logs)
    identities = tuple((log.block.number, log.log_index) for log in logs)
    if orders != tuple(sorted(orders)) or identities != tuple(sorted(set(identities))):
        raise ValueError("historical logs contain duplicates or invalid native event order")
    return logs


def decode_log(history, scope, first, last, item) -> HistoricalLog:
    if not isinstance(item, dict) or item.get("removed") is not False:
        raise ValueError("historical logs must be canonical, non-removed events")
    required = {
        "blockNumber",
        "blockHash",
        "transactionHash",
        "transactionIndex",
        "logIndex",
        "address",
        "topics",
        "data",
    }
    if not required <= item.keys() or not isinstance(item["topics"], list):
        raise ValueError("historical log is missing required event evidence")
    number = block_number(item["blockNumber"])
    if not first <= number <= last:
        raise ValueError("historical log is outside its requested block range")
    block = history.header(number)
    if item["blockHash"] != block.hash:
        raise ValueError("historical log block hash differs from the finalized header")
    record = HistoricalLog(
        block=block,
        transaction_hash=item["transactionHash"],
        transaction_index=block_number(item["transactionIndex"]),
        log_index=block_number(item["logIndex"]),
        address=item["address"],
        topics=tuple(item["topics"]),
        data=item["data"],
    )
    if record.address != scope.address or record.topics[0] not in scope.topics:
        raise ValueError("historical log differs from its requested source or event signature")
    return record
