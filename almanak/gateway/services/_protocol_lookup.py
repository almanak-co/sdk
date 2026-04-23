"""Shared base class for protocol-scoped token lookup services.

Three concrete services — ``JupiterTokenLookup`` (Solana),
``PendleMarketLookup`` (EVM PT/YT/SY/LP), ``AaveMarketLookup`` (EVM
aTokens/vTokens) — all need the same plumbing:

    disk cache (mtime TTL + atomic write)
      -> load orchestration (cache-first, network-fallback, backoff)
         -> in-memory indices
            -> lookup_by_* methods

The plumbing is identical. Only the URL, fetch method (GET vs POST),
response parsing, and index shape differ per protocol. This module
extracts the plumbing; subclasses implement the protocol-specific
bits via ``_fetch_from_network`` and ``_build_indices``.

NOT migrated to this base: ``JupiterTokenLookup``. It's older, has
its own test coverage, and sits on a well-trodden Solana path — a
risk/benefit mismatch for a drive-by refactor. Adding a fourth
service (e.g. Morpho, Compound v3) should subclass this base.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Default backoff after a fetch failure — same value used by every existing
# lookup. Keep it here so subclasses don't need to redeclare it.
_LOAD_FAILURE_BACKOFF_SECONDS = 300  # 5 minutes


class ProtocolTokenLookup(ABC):
    """Abstract base for protocol-scoped token lookup services.

    Subclasses implement ``_fetch_from_network`` (how to pull raw data
    from the protocol's API) and ``_build_indices`` (how to populate
    in-memory indices from that raw data). Everything else — disk
    cache, lock-guarded lazy load, retry backoff, atomic writes — is
    provided here.

    ``_loaded_summary`` is optional; override to produce a friendlier
    log line on successful load (e.g. ``"1814 tokens across 5 chains"``).
    """

    def __init__(
        self,
        cache_path: Path,
        protocol_name: str,
        cache_ttl_seconds: int = 24 * 60 * 60,
    ) -> None:
        self._cache_path = cache_path
        self._protocol_name = protocol_name
        self._cache_ttl_seconds = cache_ttl_seconds
        self._loaded: bool = False
        self._load_lock = asyncio.Lock()
        # Retry state: allow re-fetch after transient network failures
        self._load_failed: bool = False
        self._retry_after: float = 0.0

    # --- Subclass contract ----------------------------------------------------

    @abstractmethod
    async def _fetch_from_network(self) -> Any | None:
        """Fetch raw data from the protocol's public API.

        Return the payload on success, or ``None`` on any kind of
        failure (network error, HTTP non-200, malformed body). The
        base class interprets ``None`` as "no data", which triggers
        the retry backoff. Do not raise — the base expects a total
        function and will treat exceptions as ``None``.
        """

    @abstractmethod
    def _build_indices(self, data: Any) -> None:
        """Populate in-memory indices from raw fetched data.

        Called with whatever ``_fetch_from_network`` returned (or
        what was read from the disk cache). Must not raise; skip
        malformed entries with a ``logger.debug`` and continue.
        """

    def _loaded_summary(self) -> str:  # noqa: D401 — optional override
        """Return a short human-readable summary for the load INFO log.

        Default: just the protocol name. Subclasses typically override
        to include totals, like ``"1814 tokens across 5 chains"``.
        """
        return self._protocol_name

    def _validate_payload(self, data: Any) -> bool:
        """Return True if ``data`` has the expected top-level shape.

        Called on the raw payload returned from both the disk cache
        and the network fetch, before it reaches ``_build_indices``.
        Default: accept anything non-None. Subclasses override to
        reject obvious format mismatches (``isinstance(data, list)``,
        ``"assets" in data``, etc.); a rejected disk cache triggers
        a network re-fetch, a rejected network payload triggers the
        backoff.
        """
        return data is not None

    # --- Orchestration (provided) --------------------------------------------

    async def _load(self) -> None:
        """Load protocol data from disk cache or network, then build indices.

        Lock-guarded so concurrent callers don't fire N parallel
        network fetches on first use. If a prior fetch failed and
        the backoff window hasn't passed, this is a no-op.
        """
        async with self._load_lock:
            if self._loaded:
                return

            # ``_retry_after`` lives on the monotonic clock (see the
            # failure-path writers below) so it's immune to system-clock
            # jumps that could otherwise either suspend retries for an
            # extended period (clock jumps backwards) or unblock them
            # prematurely (clock jumps forwards).
            if self._load_failed and time.monotonic() < self._retry_after:
                return

            data = self._read_disk_cache()
            if data is None:
                try:
                    data = await self._fetch_from_network()
                except Exception as exc:  # defensive — subclasses should not raise
                    logger.warning("%s fetch raised unexpectedly: %s", self._protocol_name, exc)
                    data = None
                if data is not None and not self._validate_payload(data):
                    logger.warning(
                        "%s network payload has unexpected format",
                        self._protocol_name,
                    )
                    data = None

            if data is not None:
                try:
                    self._build_indices(data)
                except Exception as exc:  # defensive — _build_indices should not raise
                    logger.warning(
                        "%s _build_indices raised unexpectedly: %s",
                        self._protocol_name,
                        exc,
                    )
                    self._load_failed = True
                    self._retry_after = time.monotonic() + _LOAD_FAILURE_BACKOFF_SECONDS
                    return
                self._loaded = True
                self._load_failed = False
                logger.info("%s %s", self._protocol_name, self._loaded_summary())
            else:
                # Transient failure — do NOT permanently mark as loaded.
                # Allow a retry after the backoff so the gateway can
                # recover from transient network issues without restart.
                self._load_failed = True
                self._retry_after = time.monotonic() + _LOAD_FAILURE_BACKOFF_SECONDS
                logger.warning(
                    "%s unavailable; dynamic resolution will be limited. Will retry in %d seconds.",
                    self._protocol_name,
                    _LOAD_FAILURE_BACKOFF_SECONDS,
                )

    # --- Disk cache (provided) -----------------------------------------------

    def _read_disk_cache(self) -> Any | None:
        """Read cached data from disk if still fresh and shape-correct.

        Returns ``None`` if the cache is missing, expired, malformed,
        or rejected by ``_validate_payload``. A ``None`` return always
        falls through to the network fetch path in ``_load``.
        """
        if not self._cache_path.exists():
            return None

        try:
            mtime = self._cache_path.stat().st_mtime
            if time.time() - mtime > self._cache_ttl_seconds:
                logger.debug("%s disk cache expired, will re-fetch", self._protocol_name)
                return None

            with self._cache_path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)

        except Exception as exc:
            logger.warning("Failed to read %s disk cache: %s", self._protocol_name, exc)
            return None

        if not self._validate_payload(data):
            logger.warning(
                "%s disk cache has unexpected format, re-fetching",
                self._protocol_name,
            )
            return None

        logger.debug("%s data loaded from disk cache", self._protocol_name)
        return data

    def _write_disk_cache(self, data: Any) -> None:
        """Write ``data`` to the disk cache atomically.

        Uses ``.tmp`` + ``os.replace`` so a concurrent reader never
        sees a half-written file; on write failure the original
        cache (if any) is untouched.
        """
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self._cache_path.with_suffix(".tmp")
            with tmp_path.open("w", encoding="utf-8") as fh:
                json.dump(data, fh)
            os.replace(tmp_path, self._cache_path)
            logger.debug("%s data cached to disk", self._protocol_name)
        except Exception as exc:
            logger.warning("Failed to write %s disk cache: %s", self._protocol_name, exc)

    # --- Public status -------------------------------------------------------

    @property
    def is_loaded(self) -> bool:
        """Return True once data has been successfully loaded.

        Returns False if a prior load failed (even if ``_loaded`` was
        not set to True), so factory helpers know to retry after the
        backoff window passes.
        """
        return self._loaded and not self._load_failed
