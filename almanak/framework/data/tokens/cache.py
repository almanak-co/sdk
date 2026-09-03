"""Token cache with disk persistence for fast lookups.

This module provides a caching layer for token metadata with both
memory (in-process) and disk (JSON file) persistence. The cache
uses an LRU eviction policy and is thread-safe for concurrent access.

Key Components:
    - TokenCacheManager: Main cache class with memory and disk layers
    - cache_key(): Generate consistent cache keys from chain/address/symbol

Performance Targets:
    - Cache hit lookup: <1ms
    - Disk lookup: <10ms

Example:
    from almanak.framework.data.tokens.cache import TokenCacheManager
    from almanak.framework.data.tokens.models import ResolvedToken

    # Create cache with custom location
    cache = TokenCacheManager(cache_file="~/.almanak/token_cache.json")

    # Store a token
    cache.put(resolved_token)

    # Retrieve by address
    token = cache.get("arbitrum", address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831")

    # Retrieve by symbol
    token = cache.get("arbitrum", symbol="USDC")
"""

import asyncio
import json
import logging
import os
import threading
import time
import uuid
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any

from almanak.framework.data.tokens.models import ResolvedToken, normalize_token_address_for_chain

logger = logging.getLogger(__name__)


def _open_temp(dest: Path) -> tuple[int, Path]:
    """Create a sibling temp file with the process umask applied by the kernel.

    ``0o644`` at ``open()`` means the kernel subtracts the umask, with no
    ``os.umask`` read-back (an unlocked process-global write) and no post-hoc
    ``chmod``. ``O_EXCL`` gives the name-collision safety ``tempfile.mkstemp``
    provided.

    **Two divergences from ``main`` IN FILE MODE**, enumerated below. Both are
    deliberate; an earlier draft claimed parity and was wrong, so they are
    enumerated rather than summarised.

    This list is scoped to MODE and is not the full set of differences from
    ``main``'s ``open("w")``. Also different, and out of scope here: the
    destination is a NEW INODE owned by the writing uid (``main`` wrote through
    the existing file and preserved its uid/gid — this is the same mechanism as
    the "takes ownership" self-heal below, seen from the other side); a symlink
    at the cache path is REPLACED rather than followed (deliberate, CWE-59, see
    below); and an abnormal exit can leave a ``.tmp`` sibling where ``main``
    left none.

    1. *Creation mode is narrower.* ``main`` creates at ``0o666 & ~umask``; this
       creates at ``0o644 & ~umask``. Identical under the standard umask 022
       (both 0644) and under 077 (both 0600); they diverge under a PERMISSIVE
       umask — 000 gives main 0666 vs 0644 here, 002 gives 0664 vs 0644. The
       narrowing drops only the group/world WRITE bit, which in the shared
       ``/tmp`` fallback is what would let another uid rewrite a row with a
       wrong ``decimals`` and silently rescale amounts.
    2. *An existing file's mode is reset.* ``open("w")`` is
       ``O_WRONLY|O_CREAT|O_TRUNC``, so its mode applies ONLY at creation and an
       existing file KEEPS whatever mode it had; the temp-file + ``os.replace``
       path rewrites the destination's mode on every write. Measured under umask
       022: an operator-hardened 0600 cache comes back 0644 after one ``put()``
       here, where ``main`` left it 0600. Documented, not fixed — see VIB-6175.

    On world-readability: it is what lets a foreign-owned cache be READ instead
    of locking this process out, and it is why the 0600 pin was withdrawn. But
    it is a consequence of the umask, NOT a property this function can
    guarantee — under umask 077 the file is created 0600 and a second uid is
    locked out exactly as it was under the withdrawn pin. That is accepted here
    only because it is also what ``main`` does; the durable fix is
    directory-level (VIB-6171).
    """
    for _ in range(_TEMP_NAME_ATTEMPTS):
        candidate = dest.parent / f".{dest.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
        try:
            fd = os.open(candidate, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            continue
        return fd, candidate
    raise OSError(f"could not create a unique temp file next to {dest}")


def _safe_log(level: int, msg: str, *args: object, **kwargs: object) -> None:
    """Log without being able to raise into the caller.

    Every tolerance arm below logs, and logging CAN raise:
    ``logging.Handler.handle`` does not wrap ``emit`` in a try — a handler is
    expected to call ``self.handleError`` itself, and a third-party one that
    does not propagates. ``logging.raiseExceptions = False`` does not help; it
    guards errors INSIDE ``handleError``.

    That is not cosmetic here. The corrupt-row arm logs BEFORE it evicts, so a
    raising handler skips the eviction — reinstating verbatim the "the poisoned
    row never self-healed until someone deleted the cache file by hand" defect
    this module fixes. The repair must not be reachable only when logging works.
    """
    try:
        logger.log(level, msg, *args, **kwargs)  # type: ignore[arg-type]
    except Exception:  # noqa: BLE001 — logging must never break the caller
        pass


# Persisted token metadata is untrusted: decimals control powers-of-ten amount
# scaling, so a stale value can silently change transaction quantities.
# Schema mismatches invalidate the complete cache rather than migrating rows
# whose semantic validity cannot be established from stored data alone.
# The version covers authoritative metadata and cache-key normalization.
# EVM address keys are case-insensitive; Solana base58 mint keys preserve case
# because case changes asset identity. Any change that could let a warm cache
# override current registry facts requires a version bump and cold rebuild.
# Rebuilding from authoritative resolution is safer than retaining metadata
# whose identity or decimal scale may no longer be valid.
DISK_CACHE_SCHEMA_VERSION = 3

# A cooldown keeps transient disk faults retryable without causing read storms.
_DISK_RETRY_COOLDOWN_S = 30.0

# Persistent disk faults remain visible without warning on every lookup;
# the interval bounds hot-path log volume.
_DISK_ERROR_REPORT_INTERVAL_S = 900.0

# Bound unique-name attempts so a hostile namespace cannot spin indefinitely.
_TEMP_NAME_ATTEMPTS = 10

# The fallback can be shared across users, so file mode alone cannot secure
# its directory entry. Read access follows the process umask; writes create a
# sibling inode owned by the writer. Symlink traversal is allowed only for
# reads; writes replace the directory entry without following the target.
# Following links on writes would enable CWE-59 arbitrary-file overwrite;
# same-directory replacement confines writes to the cache namespace.
_SHARED_FALLBACK_DIR = Path("/tmp/.almanak")


def cache_key(chain: str, *, address: str | None = None, symbol: str | None = None) -> str:
    """Generate a consistent cache key from chain and identifier.

    Keys are formatted as 'chain:identifier' where:
    - For addresses: chain plus chain-family-normalized address. EVM addresses
      lowercase; Solana base58 mints preserve case.
    - For symbols: chain:SYMBOL_UPPER (e.g., "arbitrum:USDC")

    Args:
        chain: Chain name (e.g., "arbitrum", "ethereum")
        address: Token contract address (mutually exclusive with symbol)
        symbol: Token symbol (mutually exclusive with address)

    Returns:
        Cache key string

    Raises:
        ValueError: If neither or both address and symbol are provided

    Example:
        key = cache_key("arbitrum", address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
        # Returns: "arbitrum:0xaf88d065e77c8cc2239327c5edb3a432268e5831"

        key = cache_key("arbitrum", symbol="USDC")
        # Returns: "arbitrum:USDC"
    """
    if address is not None and symbol is not None:
        raise ValueError("Cannot specify both address and symbol")
    if address is None and symbol is None:
        raise ValueError("Must specify either address or symbol")

    chain_lower = chain.lower()
    if address is not None:
        normalized = normalize_token_address_for_chain(address, chain_lower)
        return f"{chain_lower}:{normalized}"
    else:
        return f"{chain_lower}:{symbol.upper()}"  # type: ignore[union-attr]


class TokenCacheManager:
    """Token cache with memory and disk persistence layers.

    This cache provides fast lookups for resolved tokens with automatic
    persistence to disk. It uses an LRU (Least Recently Used) eviction
    policy when the cache reaches its size limit.

    Resolution order for lookups:
    1. Memory cache (fastest, O(1))
    2. Disk cache (loads from JSON file, promotes to memory on hit)

    Thread Safety:
    - Uses threading.RLock for synchronous access
    - Provides async-safe wrapper methods using asyncio.Lock

    Attributes:
        cache_file: Path to the disk cache JSON file
        max_size: Maximum number of entries (default 10000)

    Example:
        cache = TokenCacheManager()

        # Store tokens
        cache.put(usdc_token)
        cache.put(weth_token)

        # Retrieve tokens
        token = cache.get("arbitrum", address="0x...")
        if token:
            print(f"Found {token.symbol} with {token.decimals} decimals")

        # Force persistence
        cache.flush()
    """

    DEFAULT_CACHE_FILE = "~/.almanak/token_cache.json"
    DEFAULT_MAX_SIZE = 10000

    def __init__(
        self,
        cache_file: str | Path | None = None,
        max_size: int = DEFAULT_MAX_SIZE,
    ) -> None:
        """Initialize the token cache.

        Args:
            cache_file: Path to disk cache file. Defaults to ~/.almanak/token_cache.json.
                Falls back to /tmp/.almanak/token_cache.json if home dir is not writable.
            max_size: Maximum cache entries (default 10000). Uses LRU eviction when full.
        """
        self._cache_file = self._resolve_cache_file(cache_file)
        self._max_size = max_size

        self._memory: OrderedDict[str, ResolvedToken] = OrderedDict()

        self._lock = threading.RLock()
        self._async_lock: asyncio.Lock | None = None

        self._disk_loaded = False
        self._disk_cache: dict[str, dict[str, Any]] = {}
        self._disk_retry_not_before: float | None = None
        self._disk_read_error_reported_at: float | None = None
        self._disk_write_error_reported_at: float | None = None
        self._symlink_warned = False

        self._stats = {
            "memory_hits": 0,
            "disk_hits": 0,
            "misses": 0,
            "evictions": 0,
        }

    @staticmethod
    def _resolve_cache_file(cache_file: str | Path | None) -> Path:
        """Resolve cache file path, falling back to /tmp if home is not writable."""
        if cache_file is not None:
            return Path(cache_file).expanduser()

        primary = Path(TokenCacheManager.DEFAULT_CACHE_FILE).expanduser()
        try:
            primary.parent.mkdir(parents=True, exist_ok=True)
            return primary
        except OSError:
            fallback = _SHARED_FALLBACK_DIR / "token_cache.json"
            try:
                fallback.parent.mkdir(parents=True, exist_ok=True)
            except OSError:
                pass
            return fallback

    def _ensure_disk_loaded(self) -> None:
        """Load disk cache if not already loaded. Must be called with lock held.

        On version mismatch (e.g., a v1 cache file written before PR #2505's
        BTCB-on-BSC fix), the entire disk cache is dropped and the next
        ``put()`` call writes a fresh v2 file. We can't selectively migrate
        because the static registry's authoritative view has changed; the
        only safe thing is to start over.

        Failure contract (VIB-6168)
        --------------------------
        The cache file is untrusted input, so **no** way of failing to read it
        may escape. This used to catch only ``(json.JSONDecodeError, OSError)``,
        which missed the shapes that are valid JSON but not a cache: a top-level
        list or string makes ``data.get(...)`` raise ``AttributeError``, and a
        non-mapping ``tokens`` value poisons every later lookup.

        An escape here was doubly bad. ``self._disk_loaded`` is only set at the
        end, so an escaping exception left it ``False`` — the next call retried,
        re-read the same bad file and raised again, **for the life of the
        process**. And the exception surfaced from the first step of every
        resolve as an ordinary exception, so a caller that reasonably expects a
        cache lookup to either hit or miss got neither.

        So: catch everything and start empty. ``_disk_loaded`` is then set after
        the try/except — but ONLY on the success and structural-corruption
        paths. The transient-``OSError`` arm returns early and deliberately does
        NOT latch, so it can retry (bounded by a cooldown). A cache we cannot
        read is a cold cache, never an error.
        """
        if self._disk_loaded:
            return
        if self._disk_retry_not_before is not None and time.monotonic() < self._disk_retry_not_before:
            # Preserve the unloaded state so transient read failures remain
            # retryable after the cooldown.
            self._disk_cache = {}
            return

        try:
            # Reads may follow symlinks because directory control already permits
            # equivalent cache poisoning. Writes refuse to follow them because
            # write traversal would permit CWE-59 arbitrary-file overwrite.
            if self._cache_file.exists():
                with self._cache_file.open("r") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    raise ValueError(f"cache file root is {type(data).__name__}, expected object")
                cached_version = data.get("version")
                if cached_version != DISK_CACHE_SCHEMA_VERSION:
                    _safe_log(
                        logging.INFO,
                        "token_cache_schema_mismatch",
                        extra={
                            "cache_file": str(self._cache_file),
                            "cached_version": cached_version,
                            "expected_version": DISK_CACHE_SCHEMA_VERSION,
                            "action": "drop_disk_cache",
                            "rationale": "Static registry view changed; stale entries may serve wrong decimals (see PR #2505).",
                        },
                    )
                    self._disk_cache = {}
                else:
                    tokens = data.get("tokens", {})
                    if not isinstance(tokens, dict):
                        raise ValueError(f"cache 'tokens' is {type(tokens).__name__}, expected object")
                    self._disk_cache = tokens
                    _safe_log(logging.DEBUG, f"Loaded {len(self._disk_cache)} tokens from disk cache")
            else:
                self._disk_cache = {}
        except OSError as e:
            # I/O failures leave the disk view unloaded: serve cold and retry
            # rather than overwrite potentially valid data from an empty view.
            now = time.monotonic()
            # Periodic reports keep persistent failures visible without logging
            # every lookup on the hot path.
            first_report = (
                self._disk_read_error_reported_at is None
                or now - self._disk_read_error_reported_at >= _DISK_ERROR_REPORT_INTERVAL_S
            )
            if first_report:
                _safe_log(
                    logging.WARNING,
                    "Error reading disk cache %s (%s: %s). Serving cold; will retry at most "
                    "every %ss. Repeats are DEBUG until this is re-reported.",
                    self._cache_file,
                    type(e).__name__,
                    e,
                    _DISK_RETRY_COOLDOWN_S,
                    exc_info=True,
                )
                self._disk_read_error_reported_at = now
            else:
                _safe_log(logging.DEBUG, "Disk cache still unreadable (%s: %s)", type(e).__name__, e)
            self._disk_cache = {}
            self._disk_retry_not_before = now + _DISK_RETRY_COOLDOWN_S
            return
        except Exception as e:  # noqa: BLE001 — unreadable CONTENT is a COLD cache, never an error
            # Structural corruption latches an empty view because retries cannot
            # make malformed persisted content valid.
            _safe_log(
                logging.WARNING,
                "Failed to load disk cache from %s (%s: %s). Starting with an empty cache; "
                "the file will be rewritten on the next put().",
                self._cache_file,
                type(e).__name__,
                e,
                exc_info=True,
            )
            self._disk_cache = {}

        # Only successful reads and structurally invalid content authorize a
        # later write; transient I/O returns above with an unloaded view.
        self._disk_loaded = True

    def _write_disk_cache(self) -> None:
        """Write disk cache to file ATOMICALLY. Must be called with lock held.

        Refuses to write when the disk cache was never successfully loaded, and
        writes via a temp file + ``os.replace`` (VIB-6168).

        Both halves are data-loss guards on a file shared by EVERY almanak
        process for a user — the runner, the gateway, the dashboard, each ``ax``
        invocation. ``threading.RLock`` gives no cross-process protection.

        **What this does NOT fix — read before trusting the shared-file claim.**
        ``os.replace`` gives atomic *visibility*, not cross-process
        *serialization*. ``_disk_loaded`` latches for the process lifetime, so a
        long-lived runner reads the file once at boot and every later ``put()``
        rewrites the whole document from that boot-time snapshot — silently
        dropping entries another process wrote in between. Measured: an ``ax``
        invocation's tokens are gone after the runner's next ``put()``.

        That needs no I/O fault and no torn read, only two processes and elapsed
        time, so it is MORE frequent than either defect this function does fix.

        **It does NOT only drop entries — it can RESURRECT a superseded value,
        and an earlier draft of this docstring claimed otherwise.** The claim was
        "no wrong value is ever served (an unreadable or missing entry is a miss,
        never a fabricated decimals)". That is false, and it was the stated
        justification for scoping the fix out. Reproduced:

            A: put(USDC, decimals=18)      # wrong value, written first
            B: put(USDC, decimals=6)       # another process CORRECTS it on disk
            A: put(WETH, ...)              # A rewrites its BOOT-TIME snapshot
            C: get(USDC)                   -> 6 is gone; 18 is served again

        A miss is safe because the resolver re-derives. A stale HIT is not: the
        correction is undone, no lookup fails, and nothing re-derives. On this
        path the value is ``decimals``, which scales amounts by powers of ten.

        Nor is it reliably bounded in TIME — a second draft of this paragraph
        claimed it was, and the repro three lines above is the counter-example:
        B never writes again after step 2, so nothing ever corrects A's view and
        the resurrected 18 survives every subsequent read AND process restart,
        because it is now what the file says. The bound is "until some process
        with a fresher view happens to write", which is not a guarantee.
        "Re-derivable" is likewise wrong for this case and contradicts the line
        two paragraphs up: a stale HIT never triggers re-derivation. It applies
        to DROPPED entries, not resurrected ones.

        So the honest statement is that the blast radius is the cache only, and
        scoped out of this change, which is a corruption/atomicity fix and adds
        no cross-process machinery — but scoped out on "this is a separate,
        larger fix", not on "it is harmless". Tracked in VIB-6169, whose priority
        reflects this rather than the withdrawn claim; the fix is a
        read-merge-write under an OS-level ``flock``, the pattern
        ``local_paths.py`` already owns.

        * **The write guard.** ``put()`` calls ``_ensure_disk_loaded()``; on the
          transient-error arm that leaves ``_disk_cache`` empty. Writing then
          truncates a file that is still perfectly valid on disk, destroying
          every dynamically discovered token in it. Retrying on the next lookup
          narrowed that window but did not close it — the loss happens if a
          ``put()`` lands while the fault is live. Refusing to write from a view
          that never loaded is what actually closes it.
        * **Atomicity.** ``open("w")`` truncates and then streams, so a
          concurrent reader can observe valid-prefix-then-EOF and get a
          ``JSONDecodeError`` — which the loader reasonably classifies as
          *structural* corruption and latches on, and whose next ``put()`` then
          overwrites a file that is fine by now. That path needs only
          concurrency, not an I/O fault, so it is likelier than the transient
          case. A temp file in the same directory plus ``os.replace`` (atomic on
          POSIX and Windows) removes torn reads entirely.
        """
        if not self._disk_loaded:
            _safe_log(
                logging.DEBUG,
                "Skipping disk-cache write: the cache was never successfully loaded, so the "
                "in-memory view is not a safe basis for overwriting %s",
                self._cache_file,
            )
            return

        tmp_path: Path | None = None
        try:
            self._cache_file.parent.mkdir(parents=True, exist_ok=True)
            # Keep the unresolved path so replacement changes only the cache
            # directory entry and never follows a symlink target (CWE-59).
            dest = self._cache_file
            if dest.is_symlink() and not self._symlink_warned:
                self._symlink_warned = True
                _safe_log(
                    logging.WARNING,
                    "Cache path %s is a symlink; it will be REPLACED by a regular file rather "
                    "than written through. Symlinked cache paths are not supported — following "
                    "one would make a cache write an arbitrary-file overwrite.",
                    dest,
                )
            fd, tmp_path = _open_temp(dest)
            # os.fdopen owns the descriptor only after it succeeds.
            try:
                handle = os.fdopen(fd, "w")
            except BaseException:
                os.close(fd)
                raise
            with handle as f:
                json.dump(
                    {
                        "version": DISK_CACHE_SCHEMA_VERSION,
                        "updated_at": datetime.now().isoformat(),
                        "tokens": self._disk_cache,
                    },
                    f,
                    indent=2,
                )
                # Flush and fsync before atomic publication so a crash cannot
                # expose a renamed but truncated cache.
                f.flush()
                os.fsync(f.fileno())

            # The new inode retains the mode produced by os.open and the process
            # umask; avoid process-global umask inspection and post-hoc chmod.
            # Same-directory replacement atomically publishes the complete file.
            os.replace(tmp_path, dest)
            tmp_path = None
            # A successful write starts a new error-reporting episode.
            self._disk_write_error_reported_at = None
        except Exception as e:  # noqa: BLE001 — see below
            # Cache persistence is best-effort; a write failure must not turn
            # successfully resolved metadata into a resolution failure.
            now = time.monotonic()
            if (
                self._disk_write_error_reported_at is None
                or now - self._disk_write_error_reported_at >= _DISK_ERROR_REPORT_INTERVAL_S
            ):
                self._disk_write_error_reported_at = now
                _safe_log(
                    logging.WARNING,
                    "Failed to write disk cache %s (%s: %s). Repeats are DEBUG until re-reported.",
                    self._cache_file,
                    type(e).__name__,
                    e,
                    exc_info=True,
                )
            else:
                _safe_log(logging.DEBUG, "Disk cache write still failing (%s: %s)", type(e).__name__, e)
        finally:
            if tmp_path is not None:
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

    def _evict_if_needed(self) -> None:
        """Evict oldest entries if cache exceeds max size. Must be called with lock held."""
        while len(self._memory) >= self._max_size:
            evicted_key, _ = self._memory.popitem(last=False)
            self._stats["evictions"] += 1
            _safe_log(logging.DEBUG, f"Evicted token from cache: {evicted_key}")

    def cache_key(self, chain: str, *, address: str | None = None, symbol: str | None = None) -> str:
        """Generate cache key. Convenience wrapper around module-level cache_key()."""
        return cache_key(chain, address=address, symbol=symbol)

    def get(self, chain: str, *, address: str | None = None, symbol: str | None = None) -> ResolvedToken | None:
        """Get a token from cache by chain and address or symbol.

        Checks memory cache first, then disk cache. On disk hit,
        promotes the token to memory cache.

        Args:
            chain: Chain name (e.g., "arbitrum", "ethereum")
            address: Token contract address
            symbol: Token symbol

        Returns:
            ResolvedToken if found, None otherwise

        Example:
            # Get by address
            token = cache.get("arbitrum", address="0xaf88...")

            # Get by symbol
            token = cache.get("arbitrum", symbol="USDC")
        """
        key = cache_key(chain, address=address, symbol=symbol)

        with self._lock:
            if key in self._memory:
                self._memory.move_to_end(key)
                self._stats["memory_hits"] += 1
                return self._memory[key]

            self._ensure_disk_loaded()
            if key in self._disk_cache:
                start_time = time.perf_counter()
                try:
                    token = ResolvedToken.from_dict(self._disk_cache[key])
                    self._evict_if_needed()
                    self._memory[key] = token
                    self._stats["disk_hits"] += 1

                    elapsed_ms = (time.perf_counter() - start_time) * 1000
                    if elapsed_ms > 10:
                        _safe_log(logging.DEBUG, f"Disk cache lookup took {elapsed_ms:.2f}ms for {key}")

                    return token
                except Exception as e:  # noqa: BLE001 — see below
                    # Persisted token metadata is untrusted. Any deserialization
                    # failure is a miss and eviction; decimals determine amount
                    # scaling by powers of ten, so poisoned rows must not survive.
                    _safe_log(
                        logging.WARNING,
                        "Failed to deserialize cached token %s (%s: %s) — evicting the row; "
                        "it will be re-resolved from source on the next lookup",
                        key,
                        type(e).__name__,
                        e,
                        exc_info=True,
                    )
                    self._disk_cache.pop(key, None)
                    return None

            self._stats["misses"] += 1
            return None

    def put(self, token: ResolvedToken) -> None:
        """Store a token in memory, and on disk when the disk view is loaded.

        Creates cache entries for both address and symbol lookups.

        The memory write always happens. The disk write is skipped while the
        disk view has not loaded — `_write_disk_cache` will not persist from a
        never-loaded view, which is the truncation this module exists to
        prevent. Measured during a read fault: `in_memory=True, on_disk=False`.

        A token stored in that window is NOT recovered by a later successful
        load. Measured: after the fault clears and a load succeeds, the token is
        still absent from the file — the load REPLACES the disk view from disk
        rather than merging memory into it. Only an explicit `flush()`, or
        storing the token again after recovery, persists it. It is served
        correctly from memory throughout; it is simply not durable.

        Args:
            token: ResolvedToken to cache

        Example:
            cache.put(resolved_usdc_token)
        """
        with self._lock:
            self._ensure_disk_loaded()

            address_key = cache_key(token.chain, address=token.address)
            symbol_key = cache_key(token.chain, symbol=token.symbol)

            token_dict = token.to_dict()

            self._evict_if_needed()
            self._memory[address_key] = token
            self._memory.move_to_end(address_key)

            if symbol_key != address_key:
                self._evict_if_needed()
                self._memory[symbol_key] = token
                self._memory.move_to_end(symbol_key)

            self._disk_cache[address_key] = token_dict
            if symbol_key != address_key:
                self._disk_cache[symbol_key] = token_dict

            self._write_disk_cache()

    def remove(self, chain: str, *, address: str | None = None, symbol: str | None = None) -> bool:
        """Remove a token from memory, and from disk when the disk cache is usable.

        Same qualification as `put`: the memory removal always happens, the disk
        write is skipped while a read fault is recorded, so the entry can
        reappear from the file after a later successful load.

        Args:
            chain: Chain name
            address: Token contract address
            symbol: Token symbol

        Returns:
            True if token was found and removed, False otherwise
        """
        key = cache_key(chain, address=address, symbol=symbol)

        with self._lock:
            self._ensure_disk_loaded()

            removed = False
            if key in self._memory:
                del self._memory[key]
                removed = True

            if key in self._disk_cache:
                del self._disk_cache[key]
                self._write_disk_cache()
                removed = True

            return removed

    def clear(self) -> None:
        """Clear memory always; clear the disk file only when `_disk_loaded`.

        The old one-liner said "clear both memory and disk cache"
        unconditionally, which stopped being true when this branch taught
        `clear()` to refuse an overwrite it cannot justify. While a read fault
        is recorded, the disk file is deliberately left intact and only the
        in-memory view is emptied — so entries reappear on the next successful
        load. Do not read a clean return as "the disk cache is now empty"; the
        same qualification `flush()` carries applies here.

        The condition is `_disk_loaded`, NOT "the file was readable". Those come
        apart on structurally corrupt content: unparseable JSON is caught,
        latched as an empty view, and `_disk_loaded` is set — so `clear()` DOES
        overwrite a file it could not parse. Measured. That is intended (the
        content is known-unusable, unlike an unreadable file whose content is
        unknown), but "only if it can be loaded" would be the wrong summary.
        """
        with self._lock:
            # A destructive clear must first establish a disk view; initial
            # unloaded state cannot authorize overwriting unknown contents.
            self._ensure_disk_loaded()

            self._memory.clear()
            self._disk_cache.clear()
            # Refuse writes from an unloaded view; a transient I/O failure must
            # preserve unknown on-disk contents for a later retry.
            if self._disk_loaded:
                self._write_disk_cache()
            else:
                # Memory is cleared, but preserved disk entries may reappear
                # after a later successful load.
                _safe_log(
                    logging.WARNING,
                    "clear(): a read fault is currently recorded for this cache (observed just now, "
                    "or within the last %.0fs), so the file was left untouched rather than "
                    "overwritten from an unloaded view. In-memory entries are cleared; entries in "
                    "the file will reappear if a later load succeeds.",
                    _DISK_RETRY_COOLDOWN_S,
                )
            self._stats = {
                "memory_hits": 0,
                "disk_hits": 0,
                "misses": 0,
                "evictions": 0,
            }

    def flush(self) -> None:
        """Write memory cache to disk — best-effort, NOT a guaranteed force.

        Useful for ensuring persistence before shutdown.

        "Force" was the old wording and it is no longer true. ``_write_disk_cache``
        refuses when ``_disk_loaded`` is False — the write guard that stops a
        never-loaded view from truncating a perfectly good file. So in a
        never-loaded or in-cooldown state this returns having written NOTHING,
        silently. That is the correct trade (a silent no-op loses cache entries
        that the resolver re-derives; the alternative destroys a user's file),
        but a caller must not read a clean return as "it is on disk".
        """
        with self._lock:
            self._ensure_disk_loaded()
            for key, token in self._memory.items():
                self._disk_cache[key] = token.to_dict()
            self._write_disk_cache()

    def size(self) -> tuple[int, int]:
        """Get the number of entries in memory and disk cache.

        Returns:
            Tuple of (memory_size, disk_size)
        """
        with self._lock:
            self._ensure_disk_loaded()
            return len(self._memory), len(self._disk_cache)

    def stats(self) -> dict[str, int]:
        """Get cache performance statistics.

        Returns:
            Dict with memory_hits, disk_hits, misses, evictions
        """
        with self._lock:
            return dict(self._stats)

    async def _get_async_lock(self) -> asyncio.Lock:
        """Get or create async lock. Lazy initialization for event loop compatibility."""
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    async def get_async(
        self, chain: str, *, address: str | None = None, symbol: str | None = None
    ) -> ResolvedToken | None:
        """Async-safe version of get().

        Args:
            chain: Chain name
            address: Token contract address
            symbol: Token symbol

        Returns:
            ResolvedToken if found, None otherwise
        """
        lock = await self._get_async_lock()
        async with lock:
            # Keep synchronous disk I/O off the event loop.
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda: self.get(chain, address=address, symbol=symbol))

    async def put_async(self, token: ResolvedToken) -> None:
        """Async-safe version of put().

        Args:
            token: ResolvedToken to cache
        """
        lock = await self._get_async_lock()
        async with lock:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: self.put(token))

    async def remove_async(self, chain: str, *, address: str | None = None, symbol: str | None = None) -> bool:
        """Async-safe version of remove().

        Args:
            chain: Chain name
            address: Token contract address
            symbol: Token symbol

        Returns:
            True if token was found and removed, False otherwise
        """
        lock = await self._get_async_lock()
        async with lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda: self.remove(chain, address=address, symbol=symbol))

    def __len__(self) -> int:
        """Return the number of entries in memory cache."""
        with self._lock:
            return len(self._memory)

    def __contains__(self, key: str) -> bool:
        """Check if a key exists in memory cache."""
        with self._lock:
            return key in self._memory


__all__ = [
    "cache_key",
    "TokenCacheManager",
]
