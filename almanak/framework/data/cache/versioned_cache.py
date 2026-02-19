"""Versioned historical data cache for deterministic backtest replay.

Stores historical data with dataset versioning, finality tagging, and
sha256 integrity validation.  Backtests can pin to a specific dataset
version so that replays produce identical results.

Storage layout::

    ~/.almanak/data_cache/
        <data_type>/
            <sha256_of_key>.json   # latest version pointer
            <sha256_of_key>_v<N>.json  # versioned snapshots

Only data from finalized blocks is written to the immutable versioned
store.  ``latest`` and ``safe`` block data is tagged *provisional* and
overwritten on next access once finalized.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULT_CACHE_DIR = Path.home() / ".almanak" / "data_cache"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CacheEntry:
    """A single versioned cache entry.

    Attributes:
        data: The cached payload (any JSON-serializable structure).
        dataset_version: Monotonically increasing version number.
        fetched_at: UTC ISO timestamp when the data was fetched from source.
        finality_status: ``"finalized"`` or ``"provisional"``.
        checksum: SHA256 hex digest of the serialized ``data`` payload.
    """

    data: object
    dataset_version: int
    fetched_at: str
    finality_status: str  # "finalized" | "provisional"
    checksum: str

    def __post_init__(self) -> None:
        if self.finality_status not in ("finalized", "provisional"):
            raise ValueError(f"finality_status must be 'finalized' or 'provisional', got '{self.finality_status}'")
        if self.dataset_version < 1:
            raise ValueError(f"dataset_version must be >= 1, got {self.dataset_version}")


@dataclass
class CacheStats:
    """Aggregated cache statistics."""

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    integrity_failures: int = 0

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


# ---------------------------------------------------------------------------
# VersionedDataCache
# ---------------------------------------------------------------------------


@dataclass
class VersionedDataCache:
    """Versioned JSON disk cache with finality awareness.

    Parameters:
        cache_dir: Root directory for cached data.  Defaults to
            ``~/.almanak/data_cache/``.
        data_type: Sub-directory grouping (e.g. ``"pool_history"``,
            ``"lending_rates"``).  Each data type gets its own folder.
    """

    cache_dir: Path = field(default_factory=lambda: _DEFAULT_CACHE_DIR)
    data_type: str = "general"

    def __post_init__(self) -> None:
        self._stats = CacheStats()

    # -- public interface ---------------------------------------------------

    @property
    def stats(self) -> CacheStats:
        return self._stats

    def get(self, key: str, dataset_version: int | None = None) -> CacheEntry | None:
        """Retrieve cached data for *key*.

        Args:
            key: Opaque cache key (hashed for file naming).
            dataset_version: ``None`` returns the latest version.
                An explicit integer pins to that version for deterministic
                replay.

        Returns:
            ``CacheEntry`` or ``None`` if not cached / integrity failure.
        """
        if dataset_version is not None:
            entry = self._read_version(key, dataset_version)
        else:
            entry = self._read_latest(key)

        if entry is None:
            self._stats.misses += 1
            return None

        # Validate checksum
        if not self._verify_checksum(entry):
            logger.warning("Cache integrity failure for key=%s version=%d, evicting", key, entry.dataset_version)
            self._evict(key, entry.dataset_version)
            self._stats.integrity_failures += 1
            self._stats.misses += 1
            return None

        self._stats.hits += 1
        return entry

    def put(
        self,
        key: str,
        data: object,
        finality_status: str = "finalized",
    ) -> CacheEntry:
        """Store data under *key* with automatic versioning.

        * **Finalized** data is written to a new immutable version file when
          the payload differs from the previous version.  If identical, the
          existing version is returned without creating a duplicate.
        * **Provisional** data is written to a ``_provisional.json`` sidecar
          that gets overwritten on next put (either provisional or finalized).

        Args:
            key: Opaque cache key.
            data: Any JSON-serializable payload.
            finality_status: ``"finalized"`` or ``"provisional"``.

        Returns:
            The ``CacheEntry`` that was written (or matched).
        """
        checksum = self._compute_checksum(data)
        now = datetime.now(UTC).isoformat()

        if finality_status == "provisional":
            return self._write_provisional(key, data, checksum, now)

        # Finalized path
        return self._write_finalized(key, data, checksum, now)

    def get_versions(self, key: str) -> list[int]:
        """Return sorted list of available dataset versions for *key*."""
        directory = self._type_dir()
        prefix = self._safe_name(key)
        versions: list[int] = []
        if not directory.exists():
            return versions
        for path in directory.iterdir():
            name = path.stem
            if name.startswith(prefix + "_v"):
                try:
                    v = int(name.split("_v")[-1])
                    versions.append(v)
                except ValueError:
                    continue
        versions.sort()
        return versions

    def evict(self, key: str) -> int:
        """Remove all versions (and provisional) for *key*.

        Returns:
            Number of files removed.
        """
        directory = self._type_dir()
        prefix = self._safe_name(key)
        count = 0
        if not directory.exists():
            return count
        for path in directory.iterdir():
            if path.stem.startswith(prefix):
                path.unlink(missing_ok=True)
                count += 1
                self._stats.evictions += 1
        return count

    # -- internal helpers ---------------------------------------------------

    def _type_dir(self) -> Path:
        return self.cache_dir / self.data_type

    def _safe_name(self, key: str) -> str:
        return hashlib.sha256(key.encode()).hexdigest()[:32]

    def _version_path(self, key: str, version: int) -> Path:
        return self._type_dir() / f"{self._safe_name(key)}_v{version}.json"

    def _provisional_path(self, key: str) -> Path:
        return self._type_dir() / f"{self._safe_name(key)}_provisional.json"

    def _meta_path(self, key: str) -> Path:
        """Pointer file storing latest version number."""
        return self._type_dir() / f"{self._safe_name(key)}_meta.json"

    def _compute_checksum(self, data: object) -> str:
        serialized = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()

    def _verify_checksum(self, entry: CacheEntry) -> bool:
        return self._compute_checksum(entry.data) == entry.checksum

    def _read_latest(self, key: str) -> CacheEntry | None:
        """Read the latest finalized version, falling back to provisional."""
        meta = self._read_meta(key)
        if meta is not None:
            entry = self._read_version(key, meta["latest_version"])
            if entry is not None:
                return entry
        # Fall back to provisional
        return self._read_provisional(key)

    def _read_version(self, key: str, version: int) -> CacheEntry | None:
        path = self._version_path(key, version)
        return self._read_entry(path)

    def _read_provisional(self, key: str) -> CacheEntry | None:
        path = self._provisional_path(key)
        return self._read_entry(path)

    def _read_entry(self, path: Path) -> CacheEntry | None:
        if not path.exists():
            return None
        try:
            raw = json.loads(path.read_text())
            return CacheEntry(
                data=raw["data"],
                dataset_version=raw["dataset_version"],
                fetched_at=raw["fetched_at"],
                finality_status=raw["finality_status"],
                checksum=raw["checksum"],
            )
        except Exception:
            logger.debug("Failed to read cache entry %s", path, exc_info=True)
            return None

    def _read_meta(self, key: str) -> dict | None:
        path = self._meta_path(key)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text())
        except Exception:
            return None

    def _write_meta(self, key: str, latest_version: int) -> None:
        path = self._meta_path(key)
        self._type_dir().mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"latest_version": latest_version}))

    def _write_finalized(self, key: str, data: object, checksum: str, fetched_at: str) -> CacheEntry:
        """Write finalized data, auto-incrementing version if payload changed."""
        meta = self._read_meta(key)
        if meta is not None:
            current_version = meta["latest_version"]
            # Check if data is identical to current version
            existing = self._read_version(key, current_version)
            if existing is not None and existing.checksum == checksum:
                # Data unchanged -- return existing without creating new version
                return existing
            next_version = current_version + 1
        else:
            next_version = 1

        entry = CacheEntry(
            data=data,
            dataset_version=next_version,
            fetched_at=fetched_at,
            finality_status="finalized",
            checksum=checksum,
        )
        self._write_entry(self._version_path(key, next_version), entry)
        self._write_meta(key, next_version)

        # Remove provisional sidecar if it exists (finalized supersedes)
        prov = self._provisional_path(key)
        if prov.exists():
            prov.unlink(missing_ok=True)

        logger.debug(
            "Wrote finalized cache v%d for key=%s (checksum=%s…)",
            next_version,
            key,
            checksum[:12],
        )
        return entry

    def _write_provisional(self, key: str, data: object, checksum: str, fetched_at: str) -> CacheEntry:
        """Write provisional data to a sidecar file (overwritten each time)."""
        # Provisional always uses version 0 to distinguish from finalized
        meta = self._read_meta(key)
        version = (meta["latest_version"] if meta else 0) + 1

        entry = CacheEntry(
            data=data,
            dataset_version=version,
            fetched_at=fetched_at,
            finality_status="provisional",
            checksum=checksum,
        )
        self._write_entry(self._provisional_path(key), entry)
        logger.debug("Wrote provisional cache for key=%s (checksum=%s…)", key, checksum[:12])
        return entry

    def _write_entry(self, path: Path, entry: CacheEntry) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "data": entry.data,
            "dataset_version": entry.dataset_version,
            "fetched_at": entry.fetched_at,
            "finality_status": entry.finality_status,
            "checksum": entry.checksum,
        }
        path.write_text(json.dumps(payload, default=str))

    def _evict(self, key: str, version: int) -> None:
        """Remove a specific version file."""
        path = self._version_path(key, version)
        if path.exists():
            path.unlink(missing_ok=True)
            self._stats.evictions += 1
            logger.debug("Evicted corrupt cache v%d for key=%s", version, key)
        # If this was the latest, update meta to previous version
        meta = self._read_meta(key)
        if meta and meta["latest_version"] == version:
            versions = self.get_versions(key)
            if versions:
                self._write_meta(key, versions[-1])
            else:
                # No versions left, remove meta
                meta_path = self._meta_path(key)
                if meta_path.exists():
                    meta_path.unlink(missing_ok=True)
