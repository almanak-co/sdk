"""A chain of evenly spaced blocks with the logs, receipts and views settlement reads."""

from types import SimpleNamespace

from eth_abi import encode

from almanak import external_effects
from almanak.connectors.across import settlement
from almanak.connectors.across.operations import word
from almanak.connectors.across.receipts import TRANSFER_TOPIC
from almanak.connectors.across.settlement_reader import (
    FILL_TYPES,
    FILLED_RELAY_TOPIC,
    REFUND_ROOT_TOPIC,
    REFUND_TYPES,
)
from almanak.gateway import external_effects as gateway_effects
from almanak.gateway.ethereum import DataError
from almanak.models import Block
from almanak.work import merge, project

RELAYER = "0x" + "77" * 20


def topic(value) -> str:
    raw = value.to_bytes(32) if isinstance(value, int) else word(value)
    return "0x" + raw.hex()


class FakeChain:
    """Block `n` is mined at `genesis + n * step`. Nothing is read from a provider."""

    def __init__(self, chain, genesis, step, head):
        self.chain, self.genesis, self.step, self.head = chain, genesis, step, head
        self.logs, self.receipts, self.views, self.pins = [], {}, {}, 0

    def block(self, number) -> Block:
        return Block(
            chain=self.chain,
            number=number,
            hash="0x" + (number + 1).to_bytes(32).hex(),
            timestamp=self.genesis + number * self.step,
        )

    def at(self, timestamp) -> int:
        """The last block not after `timestamp`."""
        return (timestamp - self.genesis) // self.step

    def pin_block(self, selector):
        self.pins += 1
        number = self.head if selector == "latest" else selector
        if number > self.head:
            raise DataError("malformed", "block is beyond the head")
        return self.block(number)

    def _rpc(self, method, params, allow_none=False):
        if method == "eth_getTransactionReceipt":
            return self.receipts.get(params[0])
        query = params[0]
        first, last = int(query["fromBlock"], 16), int(query["toBlock"], 16)
        wanted = query["topics"]
        return [
            row
            for row in self.logs
            if row["address"] == query["address"]
            and first <= int(row["blockNumber"], 16) <= last
            and row["topics"][: len(wanted)] == wanted
        ]

    def call(self, address, signature, block, inputs=(), args=(), outputs=()):
        return (self.views.get(signature, 0),)

    def row(self, address, topics, data, number, index, tx_hash):
        return {
            "address": address,
            "topics": topics,
            "data": "0x" + data.hex(),
            "blockNumber": hex(number),
            "blockHash": self.block(number).hash,
            "logIndex": hex(index),
            "transactionHash": tx_hash,
        }

    def mine(self, number, tx_hash, rows):
        """Record one successful transaction with its logs, in the order given."""
        used = sum(int(row["blockNumber"], 16) == number for row in self.logs)
        logs = [self.row(*row, number, used + index, tx_hash) for index, row in enumerate(rows)]
        self.logs += logs
        self.receipts[tx_hash] = {
            "transactionHash": tx_hash,
            "blockHash": self.block(number).hash,
            "blockNumber": hex(number),
            "status": "0x1",
            "logs": logs,
        }
        self.head = max(self.head, number)
        return logs


def payment(token, sender, payee, amount):
    return token, [TRANSFER_TOPIC, topic(sender), topic(payee)], amount.to_bytes(32)


def fill(pool, relay, paid=None, fill_type=0, recipient=None, message_hash=bytes(32)):
    """A FilledRelay log of `relay`, in the SpokePool's own layout."""
    depositor, to, exclusive, sent_token, token, sent, offered, origin, deposit_id = relay[:9]
    execution = (recipient or to, bytes(32), offered if paid is None else paid, fill_type)
    data = encode(
        FILL_TYPES,
        (sent_token, token, sent, offered, origin, relay[9], relay[10], exclusive)
        + (depositor, to, message_hash, execution),
    )
    return pool, [FILLED_RELAY_TOPIC, topic(origin), topic(deposit_id), topic(RELAYER)], data


def refund_leaf(pool, token, payees, amounts, deferred=False):
    """An ExecutedRelayerRefundRoot log, in the SpokePool's own layout."""
    data = encode(REFUND_TYPES, (0, list(amounts), token, list(payees), deferred, RELAYER))
    return pool, [REFUND_ROOT_TOPIC, topic(42161), topic(9), topic(0)], data


def backend(config, reader):
    fake = SimpleNamespace(config=config, reader=reader)
    fake.audit_history = lambda state: settlement.audit(fake, state)
    return fake


def settle(state, config, reader):
    """One external-effects tick of one chain: propose, commit, verify, merge.

    The proposed page is committed in the chain's projection, re-derived independently
    as the gateway does, and merged back. Returns the state, changed or not.
    """
    fake, local = backend(config, reader), project(state, reader.chain)
    page = settlement.reconcile(fake, local)
    if page is None:
        return state
    committed = external_effects.commit(local, local, page)
    assert gateway_effects.verified_commit(fake, local, committed) == committed
    return merge(state, reader.chain, committed)
