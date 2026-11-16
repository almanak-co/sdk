"""FastLZ level-1 byte length for the signed-transaction Fjord fee calculation.

Derived from FastLZ by Ariya Hidayat, under the MIT license retained in
third_party/FastLZ-LICENSE.txt. This counts the encoded bytes without allocating
compressed output. Network fees use level 1 even for inputs above 64 KiB.
"""


def bucket(data: bytes, offset: int) -> int:
    word = int.from_bytes(data[offset : offset + 3], "little")
    # FastLZ hashes with a wrapping 32-bit multiplication.
    return ((word * 2654435769) & 0xFFFFFFFF) >> 19


def literals(length: int) -> int:
    groups, tail = divmod(length, 32)
    return groups * 33 + tail + bool(tail)


def candidate(data: bytes, table: list[int], offset: int, limit: int) -> tuple[int, int]:
    while True:
        slot = bucket(data, offset)
        previous = table[slot]
        table[slot] = offset
        matches = (
            offset - previous < 8192 and data[offset : offset + 3] == data[previous : previous + 3]
        )
        if offset >= limit:
            return previous, offset
        offset += 1
        if matches:
            return previous, offset


def extension(data: bytes, previous: int, offset: int) -> int:
    end = len(data) - offset - 7
    for length in range(end):
        if data[previous + 3 + length] != data[offset + 3 + length]:
            # The encoder's extension counter includes its mismatch lookahead.
            return length + 1
    return end


def match_bytes(extension_length: int) -> int:
    groups, tail = divmod(extension_length - 1, 262)
    return groups * 3 + (3 if tail >= 6 else 2)


def compressed_size(data: bytes) -> int:
    table = [0] * 8192
    offset, anchor, size = 2, 0, 0
    limit = max(0, len(data) - 13)
    while offset < limit:
        previous, offset = candidate(data, table, offset, limit)
        if offset >= limit:
            break
        offset -= 1
        size += literals(offset - anchor)
        length = extension(data, previous, offset)
        size += match_bytes(length)
        offset += length
        for _ in range(2):
            table[bucket(data, offset)] = offset
            offset += 1
        anchor = offset
    return size + literals(len(data) - anchor)
