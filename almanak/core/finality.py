"""Canonical, distinct finality vocabularies for data and cache domains.

This module is intentionally dependency-free beyond the Python standard
library. Framework providers and gateway history caches both import it, so
the vocabulary lives in the neutral ``almanak.core`` layer and does not depend
on either implementation surface.

The two enums deliberately remain separate even though both contain the wire
value ``"finalized"``:

* :class:`DataFinality` describes the confidence/finality of observed market
  or block data.
* :class:`CacheFinality` describes whether a cache entry is provisional or
  durable and therefore which lifecycle/TTL rules apply.

Both inherit from :class:`enum.StrEnum`, preserving the historical JSON,
protobuf, database, and log values byte-for-byte.  Internal APIs should accept
the enum type for their domain; untrusted or historical strings are converted
once with the matching ``parse_*`` function at a boundary and serialized with
``.value`` on the way out.
"""

from __future__ import annotations

from enum import Enum, StrEnum
from typing import assert_never


class DataFinality(StrEnum):
    """Finality/confidence of an observed market or block-data value."""

    FINALIZED = "finalized"
    SAFE = "safe"
    LATEST = "latest"
    OFF_CHAIN = "off_chain"

    @property
    def is_finalized(self) -> bool:
        """Return whether the observation is anchored to a finalized block.

        The exhaustive match makes a newly-added data-finality member a type
        checking failure until its semantics are consciously classified.
        """
        match self:
            case DataFinality.FINALIZED:
                return True
            case DataFinality.SAFE | DataFinality.LATEST | DataFinality.OFF_CHAIN:
                return False
            case _ as unreachable:
                assert_never(unreachable)


class CacheFinality(StrEnum):
    """Lifecycle state of a versioned or TTL-managed cache entry.

    A cache entry may remain in its current state or be promoted from
    ``PROVISIONAL`` to ``FINALIZED``.  ``FINALIZED`` to ``PROVISIONAL`` is not
    a state transition: a later provisional fetch is a distinct/replacement
    entry and must not demote a durable entry in place.
    """

    PROVISIONAL = "provisional"
    FINALIZED = "finalized"

    def can_transition_to(self, target: CacheFinality) -> bool:
        """Return whether an in-place lifecycle transition is valid."""
        if not isinstance(target, CacheFinality):
            raise TypeError(f"cache finality transitions require CacheFinality values; got {type(target).__name__}")
        match self:
            case CacheFinality.PROVISIONAL:
                return target in (CacheFinality.PROVISIONAL, CacheFinality.FINALIZED)
            case CacheFinality.FINALIZED:
                return target is CacheFinality.FINALIZED
            case _ as unreachable:
                assert_never(unreachable)

    def transition_to(self, target: CacheFinality) -> CacheFinality:
        """Validate and return ``target`` for an in-place transition.

        Raises:
            TypeError: If ``target`` belongs to another domain.
            ValueError: If the transition would demote a finalized entry.
        """
        if not self.can_transition_to(target):
            raise ValueError(f"Invalid cache finality transition: {self.value} -> {target.value}")
        return target


CANONICAL_DATA_FINALITY_VALUES: tuple[str, ...] = tuple(value.value for value in DataFinality)
"""Stable serialized values for data finality."""

CANONICAL_CACHE_FINALITY_VALUES: tuple[str, ...] = tuple(value.value for value in CacheFinality)
"""Stable serialized values for cache-entry finality."""


def parse_data_finality(value: object, *, field_name: str = "finality") -> DataFinality:
    """Strictly parse a historical/config/protobuf data-finality value.

    Exact historical strings are accepted for compatibility.  Whitespace,
    case changes, aliases, and enum members from another domain are rejected;
    none can be silently normalized into a different confidence claim.
    """
    if isinstance(value, DataFinality):
        return value
    if isinstance(value, Enum):
        raise TypeError(f"{field_name} must be DataFinality, not {type(value).__name__}")
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be DataFinality or string; got {type(value).__name__}")
    try:
        return DataFinality(value)
    except ValueError as exc:
        expected = ", ".join(CANONICAL_DATA_FINALITY_VALUES)
        raise ValueError(f"Invalid {field_name} {value!r}. Expected one of: {expected}") from exc


def parse_cache_finality(value: object, *, field_name: str = "finality_status") -> CacheFinality:
    """Strictly parse a historical/config/protobuf cache-finality value.

    Exact historical strings are accepted for compatibility.  Unknown values
    fail at the boundary and enum members from another domain are rejected
    before their overlapping string values can be interpreted.
    """
    if isinstance(value, CacheFinality):
        return value
    if isinstance(value, Enum):
        raise TypeError(f"{field_name} must be CacheFinality, not {type(value).__name__}")
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be CacheFinality or string; got {type(value).__name__}")
    try:
        return CacheFinality(value)
    except ValueError as exc:
        expected = ", ".join(CANONICAL_CACHE_FINALITY_VALUES)
        raise ValueError(f"Invalid {field_name} {value!r}. Expected one of: {expected}") from exc


__all__ = [
    "CANONICAL_CACHE_FINALITY_VALUES",
    "CANONICAL_DATA_FINALITY_VALUES",
    "CacheFinality",
    "DataFinality",
    "parse_cache_finality",
    "parse_data_finality",
]
