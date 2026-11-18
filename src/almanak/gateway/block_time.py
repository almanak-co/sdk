"""The first canonical block at or after a timestamp, found by reads of pinned blocks."""

from almanak.gateway.ethereum import DataError


def first_block_at(reader, timestamp: int, *, head=None):
    """The earliest block whose timestamp is at least `timestamp`.

    Gallops back from the head until a block precedes `timestamp`, then bisects, so a
    lookup costs a few dozen block reads whatever the chain's block interval.
    """
    head = reader.pin_block("latest") if head is None else head
    if head.timestamp < timestamp:
        raise DataError("unavailable", "no block has reached the requested timestamp yet")
    high, step = head, 1
    low = None
    while low is None:
        number = high.number - step
        if number <= 0:
            low = reader.pin_block(0)
            break
        candidate = reader.pin_block(number)
        if candidate.timestamp < timestamp:
            low = candidate
        else:
            high, step = candidate, step * 2
    if low.timestamp >= timestamp:
        return low
    while high.number - low.number > 1:
        middle = reader.pin_block((low.number + high.number) // 2)
        if middle.timestamp < timestamp:
            low = middle
        else:
            high = middle
    return high
