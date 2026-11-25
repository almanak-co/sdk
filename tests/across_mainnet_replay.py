"""Replay of retained mainnet reads through the settlement entry point.

`Recorded` answers exactly the reads a capture retained and refuses any other, so a
replayed page proves which reads the production code makes and what it derives from
real answers. `Recording` is the same interface over a live reader; only the capture
of the fixture uses it, read-only.
"""

import json
from types import SimpleNamespace

from almanak.models import Block


def key(*parts) -> str:
    return json.dumps(parts, sort_keys=True, separators=(",", ":"))


def call_key(to, signature, block, args) -> str:
    plain = ["0x" + a.hex() if isinstance(a, bytes) else a for a in args]
    return key(to, signature, block.number, plain)


class Recorded:
    """The chain as one capture saw it, with `head` as its latest block."""

    def __init__(self, chain, reads: dict):
        self.chain, self.reads, self.asked = chain, reads, []

    def answer(self, kind: str, name: str):
        self.asked.append((kind, name))
        try:
            return self.reads[kind][name]
        except KeyError:
            raise AssertionError(f"a read the capture does not hold: {kind} {name}") from None

    def pin_block(self, selector) -> Block:
        number = self.reads["head"] if selector == "latest" else selector
        return Block(chain=self.chain, **self.answer("blocks", str(number)))

    def _rpc(self, method, params, *, allow_none=False):
        return self.answer("rpc", key(method, params))

    def call(self, to, signature, block, *, inputs=(), args=(), outputs=()):
        assert self.pin_block(block.number) == block
        return tuple(self.answer("calls", call_key(to, signature, block, args)))


class Recording:
    """A live reader that retains every answer in the shape `Recorded` serves."""

    def __init__(self, reader, head: int):
        self.reader, self.chain = reader, reader.chain
        self.reads = {"head": head, "blocks": {}, "rpc": {}, "calls": {}}

    def pin_block(self, selector) -> Block:
        number = self.reads["head"] if selector == "latest" else selector
        block = self.reader.pin_block(number)
        self.reads["blocks"][str(number)] = {
            "number": block.number,
            "hash": block.hash,
            "timestamp": block.timestamp,
        }
        return block

    def _rpc(self, method, params, *, allow_none=False):
        answer = self.reader._rpc(method, params, allow_none=allow_none)
        self.reads["rpc"][key(method, params)] = answer
        return answer

    def call(self, to, signature, block, *, inputs=(), args=(), outputs=()):
        # Through this recorder, so the capture holds the block a replayed call checks.
        assert self.pin_block(block.number) == block
        answer = self.reader.call(to, signature, block, inputs=inputs, args=args, outputs=outputs)
        self.reads["calls"][call_key(to, signature, block, args)] = list(answer)
        return answer


def deployment(config, *transfers, scans=()):
    """What settlement reads of a deployment: its routes, transfers and committed pages."""
    return SimpleNamespace(config=config, transfers=transfers, external_scans=scans)


def backend(config, reader):
    return SimpleNamespace(config=config, reader=reader)
