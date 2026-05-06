"""Test seeding helpers for VIB-4062 MarketSnapshot.

Re-exports ``MarketSnapshotBuilder.seeded(...)`` and the public ``seed_*``
instance methods. Strategy authors writing unit tests should import from
this module to get the one-liner ergonomics PRD §4.6 promises.
"""

from __future__ import annotations

from .builders import MarketSnapshotBuilder
from .snapshot import MarketSnapshot


def seeded(**kwargs) -> MarketSnapshot:
    """Shorthand for ``MarketSnapshotBuilder.seeded(**kwargs)``."""
    return MarketSnapshotBuilder.seeded(**kwargs)


__all__ = ["seeded", "MarketSnapshotBuilder", "MarketSnapshot"]
