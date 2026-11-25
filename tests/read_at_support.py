"""The `read_at` object an `ax` read states, under a fixed command clock."""

from almanak import ax


def read_at(monkeypatch, selector, block, age: int = 7) -> dict:
    """Fix the command's clock `age` seconds after the block and return what a read
    pinned to it by `selector` must state. Call it before the read."""
    monkeypatch.setattr(ax, "now", lambda: block.timestamp + age)
    return {
        "block": selector,
        "number": block.number,
        "timestamp": block.timestamp,
        "age_seconds": age,
    }
