"""Read the samples ahead in batches; every sample still reads and validates its own block.

A sampled product repeats the same reads at each sample's block. Resolving the blocks of
the samples ahead and asking for those reads in bulk lets the gateway overlap provider
batches, instead of one round trip per read. Nothing here decides what a sample is: the
sampler runs unchanged and finds its reads retained, or makes them itself when it does not.
"""

import json

from almanak.backtest.history import ReadRequest, warm
from almanak.backtest.types import UnixTime
from almanak.gateway.ethereum import DataError
from almanak.models import Block

# Samples whose block anchors are resolved together, one batch per search level.
ANCHORS_AHEAD = 128
# Reads warmed at once. With the anchors of one group it stays inside the reader's cache
# of inactive responses, so a warmed read is still there when its sample asks for it.
READS_AHEAD = 2048


def block_reference(block: Block) -> dict:
    return {"blockHash": block.hash, "requireCanonical": True}


class ReadAhead:
    def __init__(self, history, times):
        self.history = history
        self.times = tuple(times)
        self.positions = {at: index for index, at in enumerate(self.times)}
        self.shapes: dict[str, ReadRequest] = {}
        self.anchored = 0
        # Samples before `warm_end` are warmed with the shapes in `warm_shapes`.
        self.warm_end = 0
        self.warm_shapes: set[str] = set()

    def before(self, at: int) -> None:
        """Call before sampling `at`: reads ahead for a new group, or for new shapes."""
        index = self.positions[at]
        self.anchor(index + 1)
        if not self.shapes:
            return
        if index >= self.warm_end:
            count = min(ANCHORS_AHEAD, max(1, READS_AHEAD // len(self.shapes)))
            self.warm_end = min(len(self.times), index + count)
            self.anchor(self.warm_end)
            self.warm_shapes = set()
        fresh = [key for key in self.shapes if key not in self.warm_shapes]
        if fresh:
            warm(self.history.reader, self.requests(self.times[index : self.warm_end], fresh))
            self.warm_shapes.update(fresh)

    def anchor(self, end: int) -> None:
        if end > self.anchored:
            group = self.times[self.anchored : max(end, self.anchored + ANCHORS_AHEAD)]
            self.history.prefetch_floors(tuple(UnixTime(time) for time in group))
            self.anchored += len(group)

    def requests(self, times, shapes):
        result = []
        for at in times:
            try:
                block = self.history.floor(UnixTime(at))
            except DataError:
                # The sample itself reports a time this history cannot resolve.
                break
            reference = block_reference(block)
            result += [
                self.shapes[key].model_copy(
                    update={"params": (*self.shapes[key].params[:-1], reference)}
                )
                for key in shapes
            ]
        return result

    def learn(self, block: Block, keep=lambda request: True) -> None:
        """Retain the shapes of the block-pinned reads the sample at `block` just used.

        Only request shapes are carried to later blocks, never a value read at this one.
        """
        reader = self.history.reader
        reference = (block_reference(block),)
        used = (reader.cache[key].request for key in sorted(reader.used))
        self.shapes = {
            shape(request): request
            for request in used
            if request.params[-1:] == reference and keep(request)
        }


def shape(request: ReadRequest) -> str:
    """A request's identity without the block it is pinned to."""
    return json.dumps([request.method, request.params[:-1]], sort_keys=True)
