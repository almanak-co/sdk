"""Reader for ``scripts/ci/demo-quarantine.yml``.

The quarantine file lives in the internal CI tree (not in
``almanak/demo_strategies/``) so ``almanak strat demo`` keeps emitting
clean demo dirs without leaking CI state to users. Hard-fails on expired
``until`` dates — soft enforcement is what got us here.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in [here, *here.parents]:
        if (parent / "scripts" / "ci").is_dir() and (parent / "pyproject.toml").is_file():
            return parent
    return here.parents[3]


class QuarantineExpiredError(RuntimeError):
    """Raised when a quarantine entry's ``until`` date has passed.

    Hard-fails CI so a stale quarantine cannot silently mask a real demo
    bug for months.
    """


@dataclass(frozen=True)
class QuarantineEntry:
    demo: str
    chain: str
    ticket: str
    until: date
    reason: str

    def is_expired(self, today: date | None = None) -> bool:
        return (today or date.today()) > self.until

    def matches(self, demo: str, chain: str | None = None) -> bool:
        if self.demo != demo:
            return False
        if chain is None:
            return True
        return self.chain == chain


@dataclass
class Quarantine:
    """Catalog of quarantined ``(demo, chain)`` pairs."""

    entries: list[QuarantineEntry]
    source_path: Path | None = None

    def is_quarantined(self, demo: str, chain: str | None = None) -> bool:
        return any(e.matches(demo, chain) for e in self.entries)

    def find(self, demo: str, chain: str | None = None) -> QuarantineEntry | None:
        for e in self.entries:
            if e.matches(demo, chain):
                return e
        return None

    def expired(self, today: date | None = None) -> list[QuarantineEntry]:
        return [e for e in self.entries if e.is_expired(today)]

    def assert_not_expired(self, today: date | None = None) -> None:
        stale = self.expired(today)
        if not stale:
            return
        lines = [f"  - {e.demo}/{e.chain} (ticket {e.ticket}, expired {e.until.isoformat()})" for e in stale]
        raise QuarantineExpiredError(
            "Demo quarantine entries are past their `until:` date. "
            "Either fix the underlying bug or extend the deadline:\n" + "\n".join(lines)
        )

    @classmethod
    def load(cls, path: Path) -> Quarantine:
        """Parse ``path``. Returns an empty Quarantine if the file is missing."""
        if not path.is_file():
            return cls(entries=[], source_path=path)

        try:
            import yaml
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("PyYAML is required to parse demo-quarantine.yml") from exc

        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}

        raw_entries = data.get("quarantines") or []
        if not isinstance(raw_entries, list):
            raise ValueError(f"{path}: top-level 'quarantines:' must be a list, got {type(raw_entries).__name__}")

        entries: list[QuarantineEntry] = []
        for idx, raw in enumerate(raw_entries):
            if not isinstance(raw, dict):
                raise ValueError(f"{path}: entry #{idx} must be a mapping")
            demo = str(raw.get("demo", "")).strip()
            chain = str(raw.get("chain", "")).strip()
            ticket = str(raw.get("ticket", "")).strip()
            until_raw = raw.get("until")
            reason = str(raw.get("reason", "")).strip()
            if not demo or not chain:
                raise ValueError(f"{path}: entry #{idx} requires both 'demo' and 'chain' fields")
            if not ticket:
                raise ValueError(
                    f"{path}: entry #{idx} ({demo}/{chain}) requires a Linear 'ticket' "
                    "(quarantines without an owning ticket are forbidden)"
                )
            until = _parse_date(until_raw, source=f"{path}#{idx} ({demo}/{chain})")
            if not reason:
                raise ValueError(f"{path}: entry #{idx} ({demo}/{chain}) requires a 'reason' field")
            entries.append(QuarantineEntry(demo=demo, chain=chain, ticket=ticket, until=until, reason=reason))

        # Reject duplicate (demo, chain) pairs — would silently mask a real
        # quarantine entry behind a stale one.
        seen: set[tuple[str, str]] = set()
        for e in entries:
            key = (e.demo, e.chain)
            if key in seen:
                raise ValueError(f"{path}: duplicate quarantine entry for {e.demo}/{e.chain}")
            seen.add(key)

        return cls(entries=entries, source_path=path)

    @classmethod
    def load_default(cls) -> Quarantine:
        return cls.load(_repo_root() / "scripts" / "ci" / "demo-quarantine.yml")


def _parse_date(raw: object, *, source: str) -> date:
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, str):
        try:
            return date.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(f"{source}: 'until' must be ISO-8601 (YYYY-MM-DD), got {raw!r}") from exc
    raise ValueError(f"{source}: 'until' is required (got {raw!r})")
