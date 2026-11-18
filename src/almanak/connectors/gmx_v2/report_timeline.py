"""When usable keeper publications for a GMX market happen on its origin chain.

A fork test settles a request only by replaying reports a keeper already published
for the market's tokens, after the request and within its expiration. On a quiet
market keepers publish minutes apart, so a request created at an arbitrary fork time
can find none. A fork whose clock runs behind the chain head knows the coming
publications, so a test chooses its times by them: the fork starts where they are
dense, and each later decision lands shortly before one. Nothing is synthesized; the
schedule only picks when the fork acts.
"""

from eth_utils import keccak

from almanak.connectors.gmx_v2 import report_source as source
from almanak.connectors.gmx_v2.events import decode_event
from almanak.connectors.gmx_v2.oracle import report_fields
from almanak.gateway.block_time import first_block_at
from almanak.gateway.ethereum import DataError

# A request is created after its decision's earlier transactions (approvals, and a composed
# strategy's other legs): 38 seconds in a composed fork run on a loaded host. LEAD_MIN leaves
# a margin of over a minute above that. The reports must follow the request and arrive within
# GMX's 300 second expiration of it, so a publication serves a decision LEAD_MIN to LEAD_MAX
# seconds before it.
LEAD_MIN = 120
LEAD_MAX = 280
# The first decision follows admission, whose duration varies; the fork starts where
# publications stay this dense for this long.
ADMISSION_SECONDS = 60
COVERAGE_SECONDS = 900
SEARCH_WINDOWS = 4
PAGE_BLOCKS = 1000


class Timeline:
    """Usable publications for one market, read from its origin chain on demand."""

    def __init__(self, reader, market, adjustments=None):
        self.reader, self.market = reader, market
        self.selections = source.selections_for(market)
        self.tokens = tuple(
            dict.fromkeys(
                a.reference for a in (market.index_asset, market.long_asset, market.short_asset)
            )
        )
        self.adjustments = adjustments or {}
        self.known: dict[str, tuple[int, int] | None] = {}

    def publications(self, start: int, end: int) -> list[tuple[int, int]]:
        """(earliest, latest) report times of usable publications whose reports fall in
        [start, end], in order; only what the chain has already published."""
        head = self.reader.pin_block("latest")
        if head.timestamp < start:
            return []
        block = first_block_at(self.reader, start, head=head)
        found = set()
        while block.number <= head.number:
            last = self.reader.pin_block(min(head.number, block.number + PAGE_BLOCKS - 1))
            for transaction in self.transactions(block.number, last.number):
                times = self.times(transaction)
                if times is not None and start <= times[0] and times[1] <= end:
                    found.add(times)
            if last.timestamp > end + LEAD_MAX or last.number == head.number:
                break
            block = self.reader.pin_block(last.number + 1)
        return sorted(found)

    def transactions(self, start: int, end: int) -> list[str]:
        query = {
            "address": self.market.contracts.event_emitter,
            "topics": [
                None,
                "0x" + keccak(text="OraclePriceUpdate").hex(),
                "0x" + self.market.index_asset.reference[2:].rjust(64, "0"),
            ],
            "fromBlock": hex(start),
            "toBlock": hex(end),
        }
        hashes = []
        for log in self.reader._rpc("eth_getLogs", [query]):
            event = decode_event(log, emitter=self.market.contracts.event_emitter)
            if event is not None and event.sender == self.market.contracts.oracle:
                hashes.append(event.transaction_hash)
        return list(dict.fromkeys(hashes))

    def times(self, transaction_hash: str) -> tuple[int, int] | None:
        if transaction_hash not in self.known:
            transaction = self.reader._rpc("eth_getTransactionByHash", [transaction_hash])
            self.known[transaction_hash] = self.report_times(transaction)
        return self.known[transaction_hash]

    def report_times(self, transaction) -> tuple[int, int] | None:
        try:
            vectors = source.transaction_vectors(transaction, self.selections)
        except DataError:
            return None
        if vectors is None or any(token not in vectors for token in self.tokens):
            return None
        times = [
            report_fields(vectors[token][1])["observed_at"] - self.adjustments.get(token, 0)
            for token in self.tokens
        ]
        return min(times), max(times)

    def align(self, after: int) -> int | None:
        """The earliest time at or after `after` that a publication follows by LEAD_MIN
        to LEAD_MAX seconds; None when the chain has published none yet."""
        horizon = COVERAGE_SECONDS
        while horizon <= COVERAGE_SECONDS * SEARCH_WINDOWS:
            for earliest, latest in self.publications(after + LEAD_MIN, after + horizon):
                start = max(after, latest - LEAD_MAX)
                if earliest >= start + LEAD_MIN:
                    return start
            horizon += COVERAGE_SECONDS
        return None

    def anchor(self, latest: int) -> int | None:
        """A fork start at or before `latest` whose first decision finds a publication.

        Preferably where publications stay dense for COVERAGE_SECONDS, so any admission
        time works; on a market too quiet for that, at the latest start that puts one
        publication midway through the window of a decision ADMISSION_SECONDS later, which
        tolerates admission about (LEAD_MAX - LEAD_MIN) / 2 seconds shorter or longer.
        """
        published: list[tuple[int, int]] = []
        for window in range(SEARCH_WINDOWS):
            low = latest - window * COVERAGE_SECONDS
            published = self.publications(low - COVERAGE_SECONDS, latest + COVERAGE_SECONDS)
            starts = sorted({e - ADMISSION_SECONDS - LEAD_MIN for e, _ in published}, reverse=True)
            for start in starts:
                if low - COVERAGE_SECONDS <= start <= latest and dense(published, start):
                    return start
        before = [
            start
            for earliest, last in published
            if last - earliest <= LEAD_MAX - LEAD_MIN
            and (start := last - ADMISSION_SECONDS - (LEAD_MAX + LEAD_MIN) // 2) <= latest
        ]
        return max(before, default=None)


def dense(published, start) -> bool:
    """Every decision time between admission and the coverage end has a publication."""
    decision = start + ADMISSION_SECONDS
    for earliest, latest in published:
        if earliest < decision + LEAD_MIN:
            continue
        if latest > decision + LEAD_MAX:
            return False
        decision = earliest - LEAD_MIN + 1
        if decision >= start + COVERAGE_SECONDS:
            return True
    return False
