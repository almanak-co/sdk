"""Three-variant extract result contract for receipt parsers.

Receipt parsers historically returned `None` for BOTH "no event of this type
in the receipt" (benign) and "I crashed parsing" (accounting-critical).
Callers could not distinguish the two — a parse failure would be silently
treated as "no position opened", resulting in ghost positions.

This module introduces a tagged variant result so parsers can communicate
three distinct outcomes:

- ``ExtractOk(value)``     — successfully extracted data
- ``ExtractMissing()``     — the receipt did not contain the expected event
                              (this is a normal, benign outcome)
- ``ExtractError(error)``  — parsing raised or produced an invalid shape
                              (this is accounting-broken and MUST NOT be
                              treated as "no event")

The ``ResultEnricher`` is the primary consumer and decides policy:
  * live mode:  ExtractError -> raise ``CriticalAccountingError``
  * paper mode: ExtractError -> warn + counter, never silently discard

``CriticalAccountingError`` inherits from ``BaseException`` (not ``Exception``)
so it propagates through generic ``except Exception:`` handlers in callers
(e.g. ``StrategyRunner``). Accounting failures must never be swallowed by a
catch-all — that is precisely the ghost-position failure mode VIB-3159
addresses.

See ``docs/internal/vib-3159-followup.md`` for the migration plan covering
the remaining receipt parsers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class ExtractOk[T]:
    """Successful extraction — the value is present and parsed."""

    value: T


@dataclass(frozen=True)
class ExtractMissing:
    """The receipt does not contain the event this extractor looks for.

    This is a benign, expected outcome — e.g. calling ``extract_swap_amounts``
    on an LP_OPEN receipt. Callers should treat this the same as "field not
    applicable".
    """

    reason: str = ""


@dataclass(frozen=True)
class ExtractError:
    """Parsing failed. This is accounting-broken.

    A parse error means the strategy author's belief about what just happened
    on-chain may diverge from reality. In live mode the enricher converts
    this into a fatal error; in paper/backtest mode it surfaces as a warning
    plus an observability counter.
    """

    error: str
    exception: BaseException | None = field(default=None, compare=False)


# The tagged variant every migrated ``extract_*`` method should return.
# Expressed as a union so callers can exhaustively match.
ExtractResult = ExtractOk[T] | ExtractMissing | ExtractError


class CriticalAccountingError(BaseException):
    """Raised by ``ResultEnricher`` when extraction fails in live mode.

    Inherits from ``BaseException`` (not ``Exception``) on purpose: a parse
    error at this layer means the framework cannot reliably report what
    happened on-chain, and accounting / state machines must not proceed on
    stale beliefs. ``except Exception:`` handlers will not swallow this —
    only an explicit ``except (Exception, CriticalAccountingError):`` or
    ``except BaseException:`` will, which is the intended contract.
    """

    def __init__(
        self,
        message: str,
        *,
        field_name: str | None = None,
        intent_type: str | None = None,
        protocol: str | None = None,
        original: BaseException | None = None,
    ) -> None:
        self.field_name = field_name
        self.intent_type = intent_type
        self.protocol = protocol
        self.original = original
        super().__init__(message)


def wrap_legacy_return(value: Any) -> ExtractResult[Any]:
    """Convert a legacy ``None`` / ``value`` return into the tagged variant.

    Used by ``ResultEnricher`` to keep parsers that have not yet been
    migrated working. We treat ``None`` as ``ExtractMissing`` (the common
    "no event" case) — this preserves today's behavior for un-migrated
    parsers while migrated parsers gain the new three-way signal.

    Exceptions raised by the parser are caught at the call site and become
    ``ExtractError`` there; this helper only handles successful returns.
    """
    if value is None:
        return ExtractMissing(reason="legacy None return")
    return ExtractOk(value=value)


__all__ = [
    "CriticalAccountingError",
    "ExtractError",
    "ExtractMissing",
    "ExtractOk",
    "ExtractResult",
    "wrap_legacy_return",
]
