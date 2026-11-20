"""Proof that a signed attempt can never be mined: a final transaction took its nonce.

An account's transactions are mined in nonce order, one per nonce. Once another
transaction of the submitter with the attempt's nonce is in a final block, no chain
that keeps that block can include the attempt. The proof is positive: it names the
transaction that took the nonce and the block that holds it. A missing receipt alone
proves nothing, because a provider that lags or has pruned its transaction index also
answers none.

Two facts make the block final here, and both are required. The block is at or below
the endpoint's `finalized` block, and it is older than the head by the network's own
`finality_margin_seconds`. The tag alone is only as good as the endpoint: some serve
their head under it, and a client cannot tell that from real fast finality. The margin
is the chain's documented time to finality and does not depend on the tag. A network
with no stated margin releases nothing.

The submitter's transaction count only locates the block. It never falls along one
chain, so the first block whose state has it above the nonce is found by bisection
between the block the attempt was planned at and the finalized block. Whatever the
counts say, the release rests on the block that is then read whole. A provider that
does not serve those states refuses the read, and the attempt stays recorded.
"""

import re

from almanak.chains import evm_network
from almanak.gateway.ethereum import DataError
from almanak.models import Block
from almanak.operator_deployment import ConsumedNonce

HASH = re.compile(r"0x[0-9a-f]{64}")


def refused(message: str) -> DataError:
    return DataError("nonce_conflict", message)


def prove(backend, pending) -> ConsumedNonce:
    """Name the final transaction that took the attempt's nonce, or refuse.

    Refused while the nonce is only occupied: by a transaction the provider still
    holds, or by one in a block that is not final yet, which a reorganization could
    drop and leave the signed bytes valid again.
    """
    from almanak.gateway.execution import submission_slot

    slot = submission_slot(pending)
    if slot is None or getattr(pending, "family", None) is not None:
        raise refused("only a single signed transaction is released by its consumed nonce")
    backend.guard()
    if backend.receipt(pending) is not None:
        raise refused("a mined transaction is resolved only by its recorded outcome")
    submitter, nonce = slot
    reader = backend.reader
    margin = finality_margin(reader.chain)
    with reader.retained_pin():
        head = reader.pin_block("latest")
        finalized = reader.pin_block("finalized")
        number = first_block_past(reader, submitter, nonce, pending.before.block.number, finalized)
        block, consumer = consuming_transaction(reader, number, submitter, nonce)
    require_other(pending, block, consumer)
    require_margin(block, head, margin, nonce)
    return ConsumedNonce(
        operation=pending.operation.kind,
        transaction=pending.tx_hash,
        submitter=submitter,
        nonce=nonce,
        consumed_by=consumer,
        block=block,
    )


def finality_margin(chain) -> int:
    """The seconds a block of this network must be behind the head to count as final."""
    try:
        margin = evm_network(chain.chain_id).finality_margin_seconds
    except ValueError:
        margin = None
    if margin is None:
        raise refused(
            f"{chain.chain_id} states no finality margin, so no signed attempt is released"
            " by its consumed nonce there"
        )
    return margin


def require_other(pending, block: Block, consumer: str) -> None:
    if consumer == pending.tx_hash.lower():
        raise DataError(
            "inconsistent",
            f"the recorded transaction is in block {block.number} but has no receipt",
        )


def require_margin(block: Block, head: Block, margin: int, nonce: int) -> None:
    age = head.timestamp - block.timestamp
    if age < margin:
        raise refused(
            f"the transaction that took nonce {nonce} is in block {block.number}, {age}s"
            f" behind the head; it counts as final {margin}s behind it. Ask again later"
        )


def count_at(reader, submitter: str, number: int) -> int:
    return int(reader._rpc("eth_getTransactionCount", [submitter, hex(number)]), 16)


def first_block_past(reader, submitter: str, nonce: int, low: int, finalized: Block) -> int:
    """The first block whose state has the submitter's count above `nonce`.

    `low` is the block the attempt was planned at, where the count was at most `nonce`.
    """
    high = finalized.number
    if high <= low or count_at(reader, submitter, high) <= nonce:
        raise refused(
            f"nonce {nonce} of submitter {submitter} is not consumed at the finalized block"
            f" {high}: the signed attempt may still be mined, or the transaction that took"
            " its nonce is not final yet; ask again once it is"
        )
    if count_at(reader, submitter, low) > nonce:
        raise DataError(
            "inconsistent",
            f"nonce {nonce} was already used at the block the attempt was planned at",
        )
    while high - low > 1:
        middle = (low + high) // 2
        if count_at(reader, submitter, middle) > nonce:
            high = middle
        else:
            low = middle
    return high


def consuming_transaction(reader, number: int, submitter: str, nonce: int) -> tuple[Block, str]:
    """The block at `number` and its one transaction of the submitter with `nonce`.

    Hashes and the sender are read in lower case, whatever case the provider answers in.
    """
    data = reader._rpc("eth_getBlockByNumber", [hex(number), True])
    try:
        block = Block(
            chain=reader.chain,
            number=int(data["number"], 16),
            hash=data["hash"].lower(),
            timestamp=int(data["timestamp"], 16),
        )
        found = [
            item["hash"].lower()
            for item in data["transactions"]
            if item["from"].lower() == submitter and int(item["nonce"], 16) == nonce
        ]
    except (AttributeError, KeyError, TypeError, ValueError):
        raise DataError("malformed", "block or its transactions are incomplete") from None
    if block.number != number:
        raise DataError("malformed", "RPC returned a different block number")
    if not all(HASH.fullmatch(value) for value in found):
        raise DataError("malformed", "block holds a transaction with a malformed hash")
    if len(found) != 1:
        # The count also rises without a transaction of the account, as when another
        # transaction carries its signed authorization. That is not identified here.
        raise refused(
            f"block {number} holds no single transaction of submitter {submitter} with"
            f" nonce {nonce}, so what consumed the nonce is not identified"
        )
    return block, found[0]
