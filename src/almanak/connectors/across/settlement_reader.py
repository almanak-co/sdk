"""Canonical reads of fills and refunds: bounded log pages, decoded and matched strictly."""

from eth_abi import decode
from eth_abi.exceptions import DecodingError
from eth_utils import keccak

from almanak.connectors.across.operations import word
from almanak.connectors.across.receipts import TRANSFER_TOPIC, topic_address, topic_word, uint_data
from almanak.gateway.ethereum import DataError
from almanak.models import Block

# Blocks one page reads at most. A provider bounds a log query's range; this stays inside.
MAXIMUM_EVENT_BLOCK_SPAN = 2_000
FILLED_RELAY = (
    "FilledRelay(bytes32,bytes32,uint256,uint256,uint256,uint256,uint256,uint32,uint32,"
    "bytes32,bytes32,bytes32,bytes32,bytes32,(bytes32,bytes32,uint256,uint8))"
)
FILLED_RELAY_TOPIC = "0x" + keccak(text=FILLED_RELAY).hex()
# Non-indexed fields of FilledRelay, in event order.
FILL_TYPES = (
    "bytes32",
    "bytes32",
    "uint256",
    "uint256",
    "uint256",
    "uint32",
    "uint32",
    "bytes32",
    "bytes32",
    "bytes32",
    "bytes32",
    "(bytes32,bytes32,uint256,uint8)",
)
REFUND_ROOT = (
    "ExecutedRelayerRefundRoot(uint256,uint256,uint256[],uint32,uint32,address,address[],"
    "bool,address)"
)
REFUND_ROOT_TOPIC = "0x" + keccak(text=REFUND_ROOT).hex()
# Non-indexed fields of ExecutedRelayerRefundRoot, in event order.
REFUND_TYPES = ("uint256", "uint256[]", "address", "address[]", "bool", "address")
FILLED = 2
EMPTY = bytes(32)


def integrity(message: str) -> DataError:
    return DataError("integrity", message)


def uint_topic(value: int) -> str:
    return "0x" + value.to_bytes(32).hex()


def address_topic(address: str) -> str:
    return "0x" + word(address).hex()


def canonical(reader, block: Block) -> Block:
    if reader.pin_block(block.number) != block:
        raise DataError("reorganization", "Across scan boundaries are no longer canonical")
    return block


def block_at_or_before(reader, timestamp: int, head: Block) -> Block:
    """The last block whose timestamp is not after `timestamp`, by bisection below `head`."""
    low, high = 0, head.number
    while low < high:
        middle = (low + high + 1) // 2
        if reader.pin_block(middle).timestamp <= timestamp:
            low = middle
        else:
            high = middle - 1
    return reader.pin_block(low)


def anchored(reader, block: Block, timestamp: int) -> bool:
    """Whether `block` is the last one not after `timestamp`: the page start's own proof."""
    return block.timestamp <= timestamp < reader.pin_block(block.number + 1).timestamp


def position(row) -> tuple[int, int]:
    return int(row["blockNumber"], 16), int(row["logIndex"], 16)


def logs(reader, address: str, topics: list, start: Block, end: Block) -> tuple[dict, ...]:
    """Every log the query matches in the page, in chain order, checked against the query."""
    query = {
        "address": address,
        "fromBlock": hex(start.number),
        "toBlock": hex(end.number),
        "topics": topics,
    }
    rows = reader._rpc("eth_getLogs", [query])
    for row in rows:
        if (
            row["address"].lower() != address
            or row["topics"][: len(topics)] != topics
            or not start.number <= position(row)[0] <= end.number
            or row.get("removed")
        ):
            raise integrity("Across scan returned a log outside its query")
    ordered = tuple(sorted(rows, key=position))
    if len({position(row) for row in ordered}) != len(ordered):
        raise integrity("Across scan returned duplicate log identities")
    return ordered


def fill_logs(reader, pool: str, origin: int, deposit_id: int, start: Block, end: Block):
    topics = [FILLED_RELAY_TOPIC, uint_topic(origin), uint_topic(deposit_id)]
    return logs(reader, pool, topics, start, end)


def decoded_fill(row) -> dict:
    """A FilledRelay log as the relay it filled and what the fill itself changed."""
    try:
        data = decode(FILL_TYPES, bytes.fromhex(row["data"][2:]))
    except (DecodingError, ValueError):
        raise integrity("Across fill event data is malformed") from None
    if len(row["topics"]) != 4:
        raise integrity("Across fill event has another topic count")
    input_token, output_token, sent, offered, _repayment, fill_deadline = data[:6]
    exclusivity, exclusive_relayer, depositor, recipient, message_hash, execution = data[6:]
    updated_recipient, updated_message_hash, updated_output, fill_type = execution
    if message_hash != EMPTY or updated_message_hash != EMPTY:
        # A relay with a message is another relay than any this connector deposits.
        return {"relay": None}
    origin, deposit_id = (int.from_bytes(topic_word(row, index)) for index in (1, 2))
    return {
        "relay": (
            depositor,
            recipient,
            exclusive_relayer,
            input_token,
            output_token,
            sent,
            offered,
            origin,
            deposit_id,
            fill_deadline,
            exclusivity,
            b"",
        ),
        "updated_recipient": updated_recipient,
        "updated_output": updated_output,
        "fill_type": fill_type,
    }


def identity(row) -> tuple:
    return position(row), row["address"].lower(), tuple(row["topics"]), row["data"]


def receipt_of(reader, row) -> dict:
    """The canonical successful receipt of a log's transaction."""
    receipt = reader._rpc("eth_getTransactionReceipt", [row["transactionHash"]], allow_none=True)
    if (
        receipt is None
        or receipt["blockHash"] != row["blockHash"]
        or receipt["status"] != "0x1"
        or identity(row) not in [identity(other) for other in receipt["logs"]]
    ):
        raise DataError("reorganization", "Across settlement log has no canonical receipt")
    return receipt


def paid_to(receipt, token: str, payee: str, after: int, sender: str | None) -> int | None:
    """Base units of the first transfer of `token` to `payee` after log index `after`.

    The SpokePool emits the fill, then pays. A slow fill is paid from the pool's own
    balance, so `sender` is the pool. A fast fill is pulled from whoever called the
    pool, which the event does not name (its relayer field is the repayment address),
    so `sender` is None and any payer is taken.
    """
    for row in sorted(receipt["logs"], key=position):
        if (
            row["address"].lower() == token
            and len(row["topics"]) == 3
            and row["topics"][0] == TRANSFER_TOPIC
            and topic_address(row, 2) == payee
            and position(row)[1] > after
        ):
            if sender is not None and topic_address(row, 1) != sender:
                raise DataError(
                    "accounting",
                    f"Across slow fill {receipt['transactionHash']} was not paid by the SpokePool",
                )
            return uint_data(row)
    return None


def fill_status(reader, pool: str, relay_hash: str, block: Block) -> int:
    # The reader answers calls only at the block it pinned last.
    return reader.call(
        pool,
        "fillStatuses(bytes32)",
        canonical(reader, block),
        inputs=("bytes32",),
        args=(bytes.fromhex(relay_hash[2:]),),
        outputs=("uint256",),
    )[0]


def refund_logs(reader, token: str, pool: str, fund: str, start: Block, end: Block):
    """Transfers of the sent asset from the SpokePool to the fund: what a refund pays."""
    topics = [TRANSFER_TOPIC, address_topic(pool), address_topic(fund)]
    return logs(reader, token, topics, start, end)


def refund_entries(receipt, pool: str, token: str, fund: str) -> list[int]:
    """Amounts the transaction's executed refund leaves name for the fund in `token`."""
    entries = []
    for row in receipt["logs"]:
        if row["address"].lower() != pool or row["topics"][0] != REFUND_ROOT_TOPIC:
            continue
        try:
            _, amounts, leaf_token, payees, _, _ = decode(
                REFUND_TYPES, bytes.fromhex(row["data"][2:])
            )
        except (DecodingError, ValueError):
            raise integrity("Across refund event data is malformed") from None
        if leaf_token.lower() != token or len(amounts) != len(payees):
            continue
        entries += [
            a for a, payee in zip(amounts, payees, strict=True) if payee.lower() == fund and a
        ]
    return entries


def refund_payments(receipt, token: str, pool: str, fund: str) -> list[int]:
    return [
        uint_data(row)
        for row in receipt["logs"]
        if row["address"].lower() == token
        and len(row["topics"]) == 3
        and row["topics"][0] == TRANSFER_TOPIC
        and (topic_address(row, 1), topic_address(row, 2)) == (pool, fund)
    ]


def deferred_refund(reader, pool: str, token: str, fund: str, block: Block) -> int:
    """What the SpokePool holds for the fund because a refund transfer to it failed."""
    return reader.call(
        pool,
        "getRelayerRefund(address,address)",
        canonical(reader, block),
        inputs=("address", "address"),
        args=(token, fund),
        outputs=("uint256",),
    )[0]
