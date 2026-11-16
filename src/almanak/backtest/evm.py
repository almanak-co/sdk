"""Sparse EVM timestamp resolution; no transaction or block execution."""

from almanak.backtest.types import UnixTime
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import Block

# Levels of the timestamp search read ahead in one batch: 2**4 - 1 = 15 headers,
# within one historical batch. The search still reads exactly its own path.
SEARCH_PREFETCH_LEVELS = 4


class EvmHistory:
    def __init__(self, reader: EthereumReader):
        self.reader = reader
        self.headers: dict[int, Block] = {}
        self.upper = reader.pin_block("finalized")
        self.headers[self.upper.number] = self.upper

    def header(self, number: int) -> Block:
        if number not in self.headers:
            self.headers[number] = self.reader.pin_block(number)
        return self.headers[number]

    def floor(self, timestamp: UnixTime) -> Block:
        if timestamp > self.upper.timestamp:
            raise DataError("unavailable_history", "requested time exceeds pinned history")
        # The search path must survive cache loss on resume. Cached headers save
        # reads, but cannot choose different queries that offline reproduction lacks.
        lo, hi = 0, self.upper.number
        ahead = 0
        while lo < hi:
            middle = (lo + hi + 1) // 2
            if ahead == 0 and middle not in self.headers:
                self.prefetch_search(lo, hi)
                ahead = SEARCH_PREFETCH_LEVELS
            ahead = max(0, ahead - 1)
            if self.header(middle).timestamp <= timestamp:
                lo = middle
            else:
                hi = middle - 1
        result = self.header(lo)
        if result.timestamp > timestamp:
            raise DataError("unavailable_history", "requested time precedes chain history")
        if lo < self.upper.number and self.header(lo + 1).timestamp <= timestamp:
            raise DataError("inconsistent_history", "timestamp floor boundary does not reconcile")
        return result

    def prefetch_search(self, lo: int, hi: int):
        """Read the next search levels in one round trip instead of one per level.

        Every header the search can visit in those levels is requested, so the search
        then finds its path cached. Which headers it reads, and so what it retains, is
        unchanged. The batch is only an optimization: a capture made without it lacks
        the extra headers offline, and an unavailable or refused batch leaves the search
        to read alone (offline, one local gateway request per four levels). A batch
        answered inconsistently is a source failure and is raised.
        """
        numbers: list[int] = []
        ranges = [(lo, hi)]
        for _ in range(SEARCH_PREFETCH_LEVELS):
            following = []
            for low, high in ranges:
                if low < high:
                    middle = (low + high + 1) // 2
                    if middle not in self.headers:
                        numbers.append(middle)
                    following += [(low, middle - 1), (middle, high)]
            ranges = following
        if len(numbers) >= 2:
            self.warm_headers(numbers)

    def prefetch_floors(self, timestamps):
        """Read the search paths of many timestamps level by level, one batch per level.

        Each search keeps its own path, and the batches hold only headers on those paths,
        so nothing is read that the searches would not read alone. Timestamps beyond the
        pinned history are left for `floor` to refuse. As with `prefetch_search`, an
        unavailable batch leaves each search to read alone.
        """
        if not hasattr(self.reader, "prefetch"):
            return
        spans = {
            timestamp: (0, self.upper.number)
            for timestamp in timestamps
            if timestamp <= self.upper.timestamp
        }
        while middles := {(lo + hi + 1) // 2 for lo, hi in spans.values() if lo < hi}:
            if not self.retain_headers(sorted(middles - self.headers.keys())):
                return
            for timestamp, (lo, hi) in spans.items():
                if lo < hi:
                    middle = (lo + hi + 1) // 2
                    below = self.headers[middle].timestamp <= timestamp
                    spans[timestamp] = (middle, hi) if below else (lo, middle - 1)
        # A search that never moves its lower bound ends on a block it did not probe.
        self.retain_headers(sorted({lo for lo, _ in spans.values()} - self.headers.keys()))

    def retain_headers(self, numbers) -> bool:
        """Read `numbers` ahead and keep them as headers; False when the batch is unavailable."""
        from almanak.backtest.history import MAX_BATCH_READS

        for offset in range(0, len(numbers), MAX_BATCH_READS):
            group = numbers[offset : offset + MAX_BATCH_READS]
            if not self.warm_headers(group):
                return False
            # The reader keeps a bounded cache, so each batch becomes headers at once.
            for number in group:
                self.header(number)
        return True

    def warm_headers(self, numbers) -> bool:
        from almanak.backtest.history import ReadRequest, warm

        return warm(
            self.reader,
            (
                ReadRequest(
                    chain=self.reader.chain, method="eth_getBlockByNumber", params=(hex(n), False)
                )
                for n in numbers
            ),
        )

    def after(self, timestamp: UnixTime) -> Block:
        floor = self.floor(timestamp)
        if floor.number == self.upper.number:
            raise DataError("unavailable_history", "no eligible block in pinned history")
        return self.header(floor.number + 1)

    def event_floor(self, event_block: Block) -> Block:
        """Resolve end-of-second visibility from a retained canonical event.

        The event, not an ephemeral header cache, determines this search path.
        Chains may have several blocks with the same integer timestamp.
        """
        if event_block.chain != self.reader.chain or event_block.number > self.upper.number:
            raise DataError("source_mismatch", "event anchor is outside finalized chain history")
        current = self.pin(event_block)
        while current.number < self.upper.number:
            following = self.header(current.number + 1)
            if following.timestamp < current.timestamp:
                raise DataError("inconsistent_history", "historical timestamps moved backwards")
            if following.timestamp > event_block.timestamp:
                break
            current = following
        return current

    def pin(self, block: Block) -> Block:
        actual = self.reader.pin_block(block.number)
        if actual != block:
            raise DataError("changed_history", "historical header changed")
        return actual
