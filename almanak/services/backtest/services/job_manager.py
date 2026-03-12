"""In-memory job tracking for backtest jobs."""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from almanak.services.backtest.models import JobStatus, ProgressInfo


@dataclass
class JobState:
    """Internal state of a backtest job."""

    job_id: str
    status: JobStatus
    progress: ProgressInfo = field(default_factory=ProgressInfo)  # type: ignore[arg-type]
    result: dict[str, Any] | None = None
    error: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None


class JobManager:
    """Thread-safe in-memory backtest job tracker.

    Stores job state in a dict. No persistence — jobs are lost on restart.
    This is intentional for v1 (see PRD: session restart recovery is deferred).

    Completed/failed jobs are evicted when ``max_total`` is exceeded to
    prevent unbounded memory growth in long-running deployments.
    """

    def __init__(self, max_concurrent: int = 4, max_total: int = 1000) -> None:
        self._jobs: dict[str, JobState] = {}
        self._lock = threading.Lock()
        self._max_concurrent = max_concurrent
        self._max_total = max_total

    def create_job(self) -> str:
        """Create a new pending job and return its ID."""
        job_id = f"bt_{uuid.uuid4().hex[:12]}"
        with self._lock:
            active = sum(1 for j in self._jobs.values() if j.status in (JobStatus.PENDING, JobStatus.RUNNING))
            if active >= self._max_concurrent:
                raise RuntimeError(
                    f"Max concurrent jobs ({self._max_concurrent}) reached. Wait for running jobs to complete."
                )
            self._jobs[job_id] = JobState(job_id=job_id, status=JobStatus.PENDING)
            self._evict_completed_unlocked()
        return job_id

    def get_job(self, job_id: str) -> JobState | None:
        """Get job state by ID, or None if not found."""
        with self._lock:
            return self._jobs.get(job_id)

    def mark_running(self, job_id: str) -> None:
        """Transition job to RUNNING."""
        with self._lock:
            if job := self._jobs.get(job_id):
                job.status = JobStatus.RUNNING

    def update_progress(
        self,
        job_id: str,
        percent: float,
        current_step: str = "",
        eta_seconds: int | None = None,
    ) -> None:
        """Update progress for a running job."""
        with self._lock:
            if job := self._jobs.get(job_id):
                job.progress = ProgressInfo(
                    percent=percent,
                    current_step=current_step,
                    eta_seconds=eta_seconds,
                )

    def complete_job(self, job_id: str, result: dict[str, Any]) -> None:
        """Mark job as complete with results."""
        with self._lock:
            if job := self._jobs.get(job_id):
                job.status = JobStatus.COMPLETE
                job.result = result
                job.completed_at = datetime.now(UTC)
                job.progress = ProgressInfo(percent=100.0, current_step="Done", eta_seconds=0)

    def fail_job(self, job_id: str, error: str) -> None:
        """Mark job as failed with error message."""
        with self._lock:
            if job := self._jobs.get(job_id):
                job.status = JobStatus.FAILED
                job.error = error
                job.completed_at = datetime.now(UTC)

    @property
    def active_count(self) -> int:
        """Number of active (pending + running) jobs."""
        with self._lock:
            return sum(1 for j in self._jobs.values() if j.status in (JobStatus.PENDING, JobStatus.RUNNING))

    def _evict_completed_unlocked(self) -> None:
        """Evict oldest completed/failed jobs when total exceeds max_total. Caller must hold _lock."""
        if len(self._jobs) <= self._max_total:
            return
        finished = sorted(
            (j for j in self._jobs.values() if j.status in (JobStatus.COMPLETE, JobStatus.FAILED)),
            key=lambda j: j.created_at,
        )
        to_remove = len(self._jobs) - self._max_total
        for job in finished[:to_remove]:
            del self._jobs[job.job_id]
