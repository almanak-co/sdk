"""Tests for ``almanak.framework.demos.quarantine``."""

from __future__ import annotations

import textwrap
from datetime import date, timedelta
from pathlib import Path

import pytest

from almanak.framework.demos.quarantine import (
    Quarantine,
    QuarantineEntry,
    QuarantineExpiredError,
)


def _write(path: Path, body: str) -> Path:
    path.write_text(textwrap.dedent(body).strip())
    return path


class TestQuarantineLoad:
    def test_missing_file_returns_empty(self, tmp_path: Path):
        q = Quarantine.load(tmp_path / "absent.yml")
        assert q.entries == []
        assert not q.is_quarantined("anything")

    def test_basic_load(self, tmp_path: Path):
        _write(
            tmp_path / "q.yml",
            """
            quarantines:
              - demo: joelend_lending_lifecycle
                chain: avalanche
                ticket: VIB-3858
                until: 2026-06-01
                reason: BORROW reverts on AVAX fork
              - demo: uniswap_v4_hooks
                chain: arbitrum
                ticket: VIB-2057
                until: 2026-05-15
                reason: V4 PoolManager not deployed on arbitrum
            """,
        )
        q = Quarantine.load(tmp_path / "q.yml")
        assert len(q.entries) == 2
        assert q.is_quarantined("joelend_lending_lifecycle", "avalanche")
        assert not q.is_quarantined("joelend_lending_lifecycle", "ethereum")
        # demo-only match (no chain) returns True if any chain quarantined.
        assert q.is_quarantined("uniswap_v4_hooks")

    def test_missing_ticket_rejected(self, tmp_path: Path):
        _write(
            tmp_path / "q.yml",
            """
            quarantines:
              - demo: foo
                chain: arbitrum
                until: 2026-12-31
                reason: x
            """,
        )
        with pytest.raises(ValueError, match="requires a Linear 'ticket'"):
            Quarantine.load(tmp_path / "q.yml")

    def test_missing_reason_rejected(self, tmp_path: Path):
        _write(
            tmp_path / "q.yml",
            """
            quarantines:
              - demo: foo
                chain: arbitrum
                ticket: VIB-1
                until: 2026-12-31
            """,
        )
        with pytest.raises(ValueError, match="requires a 'reason'"):
            Quarantine.load(tmp_path / "q.yml")

    def test_invalid_until_format(self, tmp_path: Path):
        _write(
            tmp_path / "q.yml",
            """
            quarantines:
              - demo: foo
                chain: arbitrum
                ticket: VIB-1
                until: tomorrow
                reason: x
            """,
        )
        with pytest.raises(ValueError, match="ISO-8601"):
            Quarantine.load(tmp_path / "q.yml")

    def test_duplicate_pair_rejected(self, tmp_path: Path):
        _write(
            tmp_path / "q.yml",
            """
            quarantines:
              - demo: foo
                chain: arbitrum
                ticket: VIB-1
                until: 2026-12-31
                reason: a
              - demo: foo
                chain: arbitrum
                ticket: VIB-2
                until: 2026-12-31
                reason: b
            """,
        )
        with pytest.raises(ValueError, match="duplicate quarantine"):
            Quarantine.load(tmp_path / "q.yml")


class TestQuarantineExpiry:
    def _entry(self, until: date) -> QuarantineEntry:
        return QuarantineEntry(
            demo="foo",
            chain="arbitrum",
            ticket="VIB-1",
            until=until,
            reason="x",
        )

    def test_expired_detection(self):
        past = self._entry(date.today() - timedelta(days=1))
        future = self._entry(date.today() + timedelta(days=30))
        q = Quarantine(entries=[past, future])
        assert past.is_expired()
        assert not future.is_expired()
        assert q.expired() == [past]

    def test_assert_not_expired_passes_when_clean(self):
        future = self._entry(date.today() + timedelta(days=10))
        Quarantine(entries=[future]).assert_not_expired()

    def test_assert_not_expired_raises_with_diagnostics(self):
        past = self._entry(date.today() - timedelta(days=1))
        with pytest.raises(QuarantineExpiredError) as exc:
            Quarantine(entries=[past]).assert_not_expired()
        msg = str(exc.value)
        assert "VIB-1" in msg
        assert "foo/arbitrum" in msg
