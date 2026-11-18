"""Read-only caller grants and transaction provenance from captured source logs."""

from typing import Annotated

from pydantic import Field, TypeAdapter, model_validator

from almanak.models import Address, Hash32, Model, UInt

HASH = TypeAdapter(Hash32)


class CallerScope(Model):
    sender: Address
    target: Address
    selectors: Annotated[
        tuple[Annotated[str, Field(pattern=r"^0x[0-9a-f]{8}$")], ...], Field(min_length=1)
    ]


class TransactionSourceScope(Model):
    address: Address
    topics: Annotated[tuple[Hash32, ...], Field(min_length=1, max_length=4)]
    first_block: UInt
    last_block: UInt

    @model_validator(mode="after")
    def ordered(self):
        if self.last_block < self.first_block:
            raise ValueError("transaction source range must be ordered")
        return self


def validate_source_query(params, scopes):
    from almanak.backtest.history_logs import log_filter

    target, first, last, _ = log_filter(params)
    for scope in scopes:
        if target == scope.address and scope.first_block <= first <= last <= scope.last_block:
            if exact_topic_prefix(params[0]["topics"], scope.topics):
                return
    raise ValueError("transaction source query is outside its exact event and range grant")


def exact_topic_prefix(query, expected):
    return len(query) >= len(expected) and all(
        actual == topic or actual == [topic] for actual, topic in zip(query, expected, strict=False)
    )


def validate_caller(call, scopes):
    if not isinstance(call, dict) or set(call) != {"from", "to", "data"}:
        raise ValueError("historical caller grants permit only sender, target and calldata")
    data = call["data"]
    if not isinstance(data, str) or len(data) < 10:
        raise ValueError("historical caller grant requires an explicit function selector")
    if not any(
        call["from"] == scope.sender and call["to"] == scope.target and data[:10] in scope.selectors
        for scope in scopes
    ):
        raise ValueError("historical caller, target or selector is outside its exact grant")


def transaction_hash(params):
    if len(params) != 1:
        raise ValueError("historical transaction reads require exactly one evidenced hash")
    return HASH.validate_python(params[0])


def source_log(request, logs, scopes, lookup):
    from almanak.backtest.history import ReadRequest
    from almanak.backtest.history_logs import block_number

    wanted = transaction_hash(request.params)
    for evidence in logs:
        source = evidence.request
        if source.chain != request.chain or source.method != "eth_getLogs":
            continue
        try:
            validate_source_query(source.params, scopes)
        except ValueError:
            continue
        if not isinstance(evidence.result, list):
            continue
        for log in evidence.result:
            if not isinstance(log, dict) or log.get("transactionHash") != wanted:
                continue
            if log.get("removed") is not False:
                raise ValueError("transaction source log must be canonical and non-removed")
            validate_source_event(source.params[0], log)
            number = block_number(log.get("blockNumber"))
            HASH.validate_python(log.get("blockHash"))
            header = lookup(
                ReadRequest(
                    chain=request.chain, method="eth_getBlockByNumber", params=(hex(number), False)
                )
            )
            if (
                header is None
                or not isinstance(header.result, dict)
                or header.result.get("hash") != log["blockHash"]
                or header.result.get("number") != log["blockNumber"]
            ):
                raise ValueError("transaction source log lacks its matching captured header")
            return log
    raise ValueError("historical transaction hash lacks a captured authorized source log")


def validate_transaction_result(request, result, log):
    field = "hash" if request.method == "eth_getTransactionByHash" else "transactionHash"
    if not isinstance(result, dict) or result.get(field) != request.params[0]:
        raise ValueError("historical transaction response differs from its authorized hash")
    for name in ("blockHash", "blockNumber", "transactionIndex"):
        if name not in log or result.get(name) != log[name]:
            raise ValueError(
                "historical transaction response differs from its source event identity"
            )
    if request.method == "eth_getTransactionReceipt":
        values = result.get("logs")
        if not isinstance(values, list) or log not in values:
            raise ValueError("historical receipt omits or changes its authorized source event")


def validate_source_event(query, log):
    from almanak.backtest.history_logs import block_number

    number = block_number(log.get("blockNumber"))
    if log.get("address") != query["address"] or not block_number(
        query["fromBlock"]
    ) <= number <= block_number(query["toBlock"]):
        raise ValueError("transaction source event differs from its authorized address or range")
    actual = log.get("topics")
    if not isinstance(actual, list) or len(actual) < len(query["topics"]):
        raise ValueError("transaction source event lacks its authorized topics")
    for expected, topic in zip(query["topics"], actual, strict=False):
        allowed = expected if isinstance(expected, list) else [expected]
        if expected is not None and topic not in allowed:
            raise ValueError("transaction source event differs from its authorized topics")
    block_number(log.get("transactionIndex"))
    block_number(log.get("logIndex"))
