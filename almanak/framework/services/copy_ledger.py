"""Local-first persistent ledger for copy-trading signals, decisions, and executions."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from almanak.framework.services.copy_trading_models import CopyDecision, CopyExecutionRecord, CopySignal

_SCHEMA_VERSION = 1


class CopyLedger:
    """SQLite-backed copy-trading ledger with lightweight migrations."""

    def __init__(self, db_path: str | Path = "./almanak_copy_ledger.db") -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.row_factory = sqlite3.Row
        self._ensure_schema()

    @property
    def db_path(self) -> Path:
        """Return ledger database path."""
        return self._db_path

    def close(self) -> None:
        """Close SQLite connection."""
        self._conn.close()

    def has_seen_signal(self, signal_id: str) -> bool:
        """Check replay protection set."""
        row = self._conn.execute("SELECT 1 FROM copy_seen_signals WHERE signal_id = ?", (signal_id,)).fetchone()
        return row is not None

    def mark_seen_signal(self, signal_id: str, detected_at: int | None = None) -> None:
        """Record signal ID in deterministic seen-set."""
        ts = detected_at or int(time.time())
        self._conn.execute(
            """
            INSERT OR IGNORE INTO copy_seen_signals (signal_id, first_seen_at)
            VALUES (?, ?)
            """,
            (signal_id, ts),
        )
        self._conn.commit()

    def record_signal(self, signal: CopySignal) -> None:
        """Persist normalized signal with full payload lineage."""
        payload_json = self._to_json(signal.action_payload)
        metadata_json = self._to_json(signal.metadata)
        capability_json = self._to_json(signal.capability_flags)
        signal_id = signal.signal_id or signal.event_id

        self._conn.execute(
            """
            INSERT OR REPLACE INTO copy_signals (
                signal_id,
                event_id,
                leader_tx_hash,
                leader_block,
                detected_at,
                age_seconds,
                action_type,
                protocol,
                chain,
                leader_address,
                payload_json,
                metadata_json,
                capability_flags_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal_id,
                signal.event_id,
                signal.leader_tx_hash,
                signal.leader_block,
                signal.detected_at,
                signal.age_seconds,
                signal.action_type,
                signal.protocol,
                signal.chain,
                signal.leader_address,
                payload_json,
                metadata_json,
                capability_json,
                int(time.time()),
            ),
        )
        self.mark_seen_signal(signal_id, signal.detected_at)
        self._conn.commit()

    def record_decision(self, decision: CopyDecision) -> None:
        """Persist policy decision and risk snapshot."""
        signal_id = decision.signal.signal_id or decision.signal.event_id
        self._conn.execute(
            """
            INSERT OR REPLACE INTO copy_decisions (
                decision_id,
                signal_id,
                action,
                skip_reason_code,
                policy_results_json,
                risk_snapshot_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision.decision_id,
                signal_id,
                decision.action,
                decision.skip_reason_code or decision.skip_reason,
                self._to_json(decision.policy_results),
                self._to_json(decision.risk_snapshot),
                int(time.time()),
            ),
        )
        self._conn.commit()

    def record_execution(self, record: CopyExecutionRecord) -> None:
        """Persist execution outcome and quality metrics."""
        self._conn.execute(
            """
            INSERT INTO copy_executions (
                signal_id,
                event_id,
                intent_id,
                intent_ids_json,
                submission_mode,
                status,
                status_code,
                skip_reason,
                tx_hashes_json,
                leader_follower_lag_ms,
                price_deviation_bps,
                timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.signal_id or record.event_id,
                record.event_id,
                record.intent_id,
                self._to_json(record.intent_ids or []),
                str(record.submission_mode) if record.submission_mode is not None else None,
                record.status,
                record.status_code,
                record.skip_reason,
                self._to_json(record.tx_hashes or []),
                record.leader_follower_lag_ms,
                record.price_deviation_bps,
                record.timestamp or int(time.time()),
            ),
        )
        self._conn.commit()

    def get_summary(self, since_ts: int | None = None) -> dict[str, Any]:
        """Get aggregate operational metrics from ledger."""
        where = ""
        params: tuple[Any, ...] = ()
        if since_ts is not None:
            where = "WHERE timestamp >= ?"
            params = (since_ts,)

        status_rows = self._conn.execute(
            f"""
            SELECT status, COUNT(*) AS n
            FROM copy_executions
            {where}
            GROUP BY status
            """,
            params,
        ).fetchall()

        execution_counts = {row["status"]: int(row["n"]) for row in status_rows}

        signal_count_query = "SELECT COUNT(*) AS n FROM copy_signals"
        decision_count_query = "SELECT COUNT(*) AS n FROM copy_decisions"
        if since_ts is not None:
            signal_count_query += " WHERE detected_at >= ?"
            decision_count_query += " WHERE created_at >= ?"

        signal_count = int(self._conn.execute(signal_count_query, params).fetchone()["n"])
        decision_count = int(self._conn.execute(decision_count_query, params).fetchone()["n"])

        lag_row = self._conn.execute(
            f"""
            SELECT AVG(leader_follower_lag_ms) AS avg_lag_ms,
                   MAX(price_deviation_bps) AS max_dev_bps
            FROM copy_executions
            {where}
            """,
            params,
        ).fetchone()

        return {
            "signals": signal_count,
            "decisions": decision_count,
            "executions": execution_counts,
            "avg_leader_follower_lag_ms": float(lag_row["avg_lag_ms"] or 0),
            "max_price_deviation_bps": int(lag_row["max_dev_bps"] or 0),
            "db_path": str(self._db_path),
        }

    def get_execution_rows(self, since_ts: int | None = None) -> list[dict[str, Any]]:
        """Return execution rows, optionally filtered by timestamp."""
        if since_ts is None:
            rows = self._conn.execute(
                "SELECT signal_id, status, status_code, timestamp FROM copy_executions"
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT signal_id, status, status_code, timestamp FROM copy_executions WHERE timestamp >= ?",
                (since_ts,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_recent_decisions(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return most recent decisions for audits and reporting."""
        rows = self._conn.execute(
            """
            SELECT decision_id, signal_id, action, skip_reason_code, policy_results_json, risk_snapshot_json, created_at
            FROM copy_decisions
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [
            {
                "decision_id": row["decision_id"],
                "signal_id": row["signal_id"],
                "action": row["action"],
                "skip_reason_code": row["skip_reason_code"],
                "policy_results": self._parse_json(row["policy_results_json"]),
                "risk_snapshot": self._parse_json(row["risk_snapshot_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def _ensure_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS copy_ledger_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

        version_row = self._conn.execute("SELECT value FROM copy_ledger_meta WHERE key = 'schema_version'").fetchone()
        current_version = int(version_row["value"]) if version_row else 0

        if current_version < 1:
            self._migrate_v1()

    def _migrate_v1(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS copy_signals (
                signal_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                leader_tx_hash TEXT,
                leader_block INTEGER,
                detected_at INTEGER,
                age_seconds INTEGER,
                action_type TEXT NOT NULL,
                protocol TEXT NOT NULL,
                chain TEXT NOT NULL,
                leader_address TEXT NOT NULL,
                payload_json TEXT,
                metadata_json TEXT,
                capability_flags_json TEXT,
                created_at INTEGER NOT NULL
            )
            """
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_copy_signals_detected_at ON copy_signals(detected_at)")

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS copy_seen_signals (
                signal_id TEXT PRIMARY KEY,
                first_seen_at INTEGER NOT NULL
            )
            """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS copy_decisions (
                decision_id TEXT PRIMARY KEY,
                signal_id TEXT NOT NULL,
                action TEXT NOT NULL,
                skip_reason_code TEXT,
                policy_results_json TEXT,
                risk_snapshot_json TEXT,
                created_at INTEGER NOT NULL
            )
            """
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_copy_decisions_signal_id ON copy_decisions(signal_id)")

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS copy_executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT,
                event_id TEXT NOT NULL,
                intent_id TEXT,
                intent_ids_json TEXT,
                submission_mode TEXT,
                status TEXT NOT NULL,
                status_code TEXT,
                skip_reason TEXT,
                tx_hashes_json TEXT,
                leader_follower_lag_ms INTEGER,
                price_deviation_bps INTEGER,
                timestamp INTEGER NOT NULL
            )
            """
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_copy_executions_timestamp ON copy_executions(timestamp)")

        self._conn.execute(
            """
            INSERT INTO copy_ledger_meta(key, value)
            VALUES('schema_version', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (str(_SCHEMA_VERSION),),
        )
        self._conn.commit()

    @staticmethod
    def _to_json(value: Any) -> str:
        if value is None:
            return "{}"
        if is_dataclass(value) and not isinstance(value, type):
            value = asdict(value)
        if isinstance(value, Decimal):
            return json.dumps(str(value))
        return json.dumps(value, default=str, sort_keys=True)

    @staticmethod
    def _parse_json(raw: str | None) -> Any:
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except Exception:
            return {"raw": raw}
