"""Bounded local timing evidence. No credentials, network calls or financial state."""

import fcntl
import os
import secrets
from pathlib import Path

from almanak.backtest.progress_types import TimingEntry, TimingHistory
from almanak.models import Model


def atomic_snapshot(path: Path, value: Model):
    temporary = path.with_name(path.name + "." + secrets.token_hex(8) + ".tmp")
    try:
        with temporary.open("x") as output:
            os.chmod(temporary, 0o600)
            output.write(value.model_dump_json() + "\n")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


class History:
    def __init__(self, path: Path):
        self.path = path

    def read(self) -> TimingHistory:
        if not self.path.exists():
            return TimingHistory()
        return TimingHistory.model_validate_json(self.path.read_text())

    def matching(self, key: str) -> tuple[TimingEntry, ...]:
        return tuple(row for row in self.read().entries if row.key == key)

    def record(self, entry: TimingEntry):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.with_name(self.path.name + ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            history = self.read()
            atomic_snapshot(self.path, TimingHistory(entries=(*history.entries[-63:], entry)))
