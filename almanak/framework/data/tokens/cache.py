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

from almanak.framework.data.tokens.models import ResolvedToken

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


# Disk-cache schema version. Bump this whenever the static registry's view of
# an already-cached token changes in a way that would silently serve wrong
# values to anyone with a warm cache. v2 was introduced by PR #2505 because v1
# could carry a stale ``bsc:WBTC`` / ``bsc:0x7130d2a1…`` entry recording
# ``decimals=8`` (off-by-10^10 vs the on-chain BTCB contract's 18 decimals).
# On load, a version mismatch drops the entire disk cache and forces a re-fill
# from the corrected static registry. Cheap insurance — cache rebuilds itself
# from registry hits within a single session.
DISK_CACHE_SCHEMA_VERSION = 2

#: Minimum gap between disk-cache read retries after a transient fault.
_DISK_RETRY_COOLDOWN_S = 30.0

#: How often to re-report a persistent disk-cache read fault. Long enough not to
#: spam, short enough that a permanent misconfiguration does not go silent.
_DISK_ERROR_REPORT_INTERVAL_S = 900.0

#: Attempts to find an unused temp-file name before giving up.
_TEMP_NAME_ATTEMPTS = 10

#: Fallback cache directory, used when the user's home is not writable.
#: A SHARED namespace (see ``_write_disk_cache``). The cache is created with the
#: umask-derived mode there as everywhere else — NOT 0600. A restrictive mode was
#: tried here and withdrawn: it removed self-healing and forced a rebuild hatch
#: that destroyed good files. The exposure in this directory is DIRECTORY-level
#: (VIB-6171) and is not defensible with a mode bit.
_SHARED_FALLBACK_DIR = Path("/tmp/.almanak")


def cache_key(chain: str, *, address: str | None = None, symbol: str | None = None) -> str:
    """Generate a consistent cache key from chain and identifier.

    Keys are formatted as 'chain:identifier' where:
    - For addresses: chain:address_lower (e.g., "arbitrum:0xaf88...")
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
        return f"{chain_lower}:{address.lower()}"
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

        # Memory cache using OrderedDict for LRU ordering
        self._memory: OrderedDict[str, ResolvedToken] = OrderedDict()

        # Thread safety
        self._lock = threading.RLock()
        self._async_lock: asyncio.Lock | None = None

        # Disk cache state
        self._disk_loaded = False
        self._disk_cache: dict[str, dict[str, Any]] = {}
        #: Cooldown bookkeeping for transient read faults (see _ensure_disk_loaded).
        self._disk_retry_not_before: float | None = None
        self._disk_read_error_reported_at: float | None = None
        self._disk_write_error_reported_at: float | None = None
        #: One-shot latch for the symlinked-cache-path warning.
        self._symlink_warned = False

        # Performance tracking
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
            # Inside the cooldown after a read fault: serve cold without another
            # failed open(). Still NOT latched, so it recovers on its own.
            self._disk_cache = {}
            return

        try:
            # THE READ DELIBERATELY FOLLOWS SYMLINKS; THE WRITE DELIBERATELY
            # DOES NOT. The asymmetry is intentional — do not "fix" it by
            # adding O_NOFOLLOW here to match ``_write_disk_cache``.
            #
            # The two sides are asymmetric because the CAPABILITIES are:
            #
            #   * Write-follows-symlink is an escape primitive. ``os.replace``
            #     through a resolved path destroys a file OUTSIDE the cache
            #     directory — an arbitrary-overwrite, which is why the write
            #     side refuses (CWE-59, and there is a test proving it).
            #   * Read-follows-symlink grants an attacker nothing new. Planting
            #     the symlink already requires owning the cache directory, and
            #     anyone who owns it can plant a poisoned REGULAR
            #     ``token_cache.json`` for identical effect. There is no
            #     information disclosure either: the parsed content is only
            #     ever written back to the cache path, a bad row is evicted by
            #     ``get()``, and no error path leaks file content — the log
            #     lines emit ``type(x).__name__`` only, ``JSONDecodeError``
            #     carries msg/line/col and ``UnicodeDecodeError`` the offending
            #     byte.
            #
            # So refusing here would buy no security and would cost a retry
            # loop: O_NOFOLLOW raises ELOOP, an OSError, which lands on the
            # TRANSIENT arm and retries a condition that will never clear.
            #
            # The read-path exposure that IS real is a non-regular file at this
            # path (a FIFO blocks ``open`` forever while holding ``self._lock``,
            # wedging every resolve in the process). A symlink is not required
            # to trigger it and O_NOFOLLOW does not prevent it — tracked
            # separately, not defensible with a flag on this line.
            if self._cache_file.exists():
                with self._cache_file.open("r") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    # Valid JSON, not a cache document. Explicit because the
                    # implicit version of this check was ``data.get(...)``
                    # raising ``AttributeError`` out of the whole method.
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
            # TRANSIENT vs STRUCTURAL — these need different recovery policies
            # (VIB-6168).
            #
            # An ``OSError`` (EMFILE, EINTR, a transient permission or mount
            # blip) says nothing about the file's CONTENT. Latching
            # ``_disk_loaded = True`` here would be actively destructive: the
            # process would never retry, and the first later ``put()`` would
            # dump the now-empty in-memory view over a cache file that is still
            # perfectly valid on disk — permanently deleting every dynamically
            # discovered token in it. One transient blip would erase the cache.
            #
            # So: degrade for THIS call, but leave ``_disk_loaded`` False so the
            # next lookup re-reads. The cost of retrying is one failed open per
            # lookup; the cost of latching is silent data loss.
            # Bounded retry. Not every OSError is transient — PermissionError
            # (a mode-000 or foreign-uid cache file, routine in containers),
            # IsADirectoryError and ENOTDIR are all OSError and none will fix
            # themselves. Retrying per call on those costs a failed open() and a
            # traceback-bearing WARNING on EVERY resolve, and token resolution
            # runs per-leg per-iteration (backtests resolve hundreds of
            # thousands of times). So: retry, but at most once per cooldown, and
            # report once — the warn-once shape used by _try_record_metric.
            now = time.monotonic()
            # Re-arm periodically. Reporting ONCE meant a permanent misconfiguration
            # (a root-owned or foreign-uid cache file, a directory in its place)
            # made the cache silently inert for the process lifetime after a single
            # WARNING at boot — the disk layer simply gone, with nothing saying so.
            # A fault that never self-heals should stay visible.
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
            # Structural corruption: bad JSON, wrong root type, malformed
            # ``tokens``. Re-reading cannot help — the bytes will not change on
            # their own — so this latches, which is what keeps a corrupt file
            # from being re-parsed and re-raised on every single lookup.
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

        # Reached on success and on structural corruption, but NOT on the
        # transient-OSError path above, which returns early so it can retry.
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
            # Do NOT follow a symlink at the cache path.
            #
            # This briefly did `dest = self._cache_file.resolve()` so a symlinked
            # cache would be written THROUGH rather than replaced — matching
            # `main`'s `open("w")`. That is an arbitrary-file overwrite: with a
            # world-writable directory (the /tmp fallback) and `token_cache.json`
            # symlinked at a victim-owned file, one put() destroys that file and
            # replaces it with cache JSON, and the link survives so it re-fires
            # on every write. Measured (CWE-59).
            #
            # The refusal stands on the CWE-59 measurement alone and needs no
            # other justification. It was originally argued from "no shared
            # volume exists" — a premise this file has since REPUDIATED (see
            # the "IS /tmp/.almanak A SHARED NAMESPACE?" note below in
            # ``_write_disk_cache``; the answer is yes), and
            # which the 0600 withdrawal removed anyway. Recorded so the stale
            # reasoning is not mistaken for live support.
            #
            # Replacing the link is safe: `os.replace` swaps the directory entry,
            # so nothing outside this directory is ever touched. The cost is that
            # a symlinked cache silently becomes a regular file, so we say so
            # once rather than let it happen invisibly.
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
            # ``os.fdopen`` takes ownership of ``fd`` ONLY on success. If it
            # raises, the descriptor is still open and nothing else will close
            # it — the ``finally`` below unlinks the file but cannot reclaim the
            # fd, so a repeated write fault would leak descriptors until EMFILE.
            # Which is itself one of the faults this module now tolerates, so
            # the leak would manufacture the condition it degrades on.
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
                # Durability before visibility: without this the rename can
                # commit before the bytes do, so a crash leaves an atomically
                # renamed but truncated file.
                f.flush()
                os.fsync(f.fileno())

            # NO mode juggling: the temp file carries the kernel-applied umask
            # and ``os.replace`` puts that on the destination. This is NOT mode
            # parity with ``main`` — see ``_open_temp`` for the two divergences
            # (narrower creation mode under a permissive umask; and an existing
            # file's mode reset on every write, which ``main`` preserved).
            #
            # IS ``/tmp/.almanak`` A SHARED NAMESPACE? Yes. Settled here once,
            # because two commits on this branch assumed opposite answers and
            # that is what let a defect through.
            #
            # ``_resolve_cache_file`` falls back there when ``~/.almanak`` is not
            # creatable, and ``/tmp`` is multi-uid by construction. Every claim
            # in this file is now written against that answer.
            #
            # A file mode cannot make it safe, and trying made it worse. Pinning
            # 0600 to "protect" the shared path removed the property that
            # actually mattered — a foreign-owned cache stayed READABLE at 0644,
            # so the load succeeded and the next ``put()`` took ownership, i.e.
            # it SELF-HEALED. At 0600 it cannot, which forced a rebuild escape
            # hatch, and that hatch then destroyed good files from a stale
            # denial (measured: 6 tokens -> 2). In a group-writable ``/tmp``,
            # 0600 plus a hatch is strictly worse than 0644 with none: uid A
            # writes 0600, B cannot read it and rebuilds over A, A cannot read
            # B's and rebuilds over B — mutual destruction.
            #
            # Qualification, measured: the self-heal needs a WRITABLE parent. In
            # a foreign-owned ``/tmp/.almanak`` at the default 0755, a 0644 cache
            # still LOADS (which is the property that matters — no lockout) but
            # ``put()`` cannot take ownership. The file is left byte-identical
            # and no temp file leaks, so the outcome is safe; it simply stays
            # read-only for us rather than being adopted.
            #
            # The real exposure in that directory is DIRECTORY-level — an
            # attacker who pre-creates ``/tmp/.almanak`` (``mkdir`` uses
            # ``exist_ok=True``) owns the namespace whatever mode the file has.
            # That is VIB-6171 and it is not defensible with a mode bit. The
            # honest posture is main's mode — narrowed only to drop the
            # group/world WRITE bit — plus a tracked directory fix, not a
            # restrictive mode that buys nothing and costs self-healing.
            # NB: "main's mode" here means the umask-derived mode, not the
            # literal 0o666; do not widen the literal back to satisfy this
            # sentence. See ``_open_temp`` for why 0o644 is the literal.

            os.replace(tmp_path, dest)
            tmp_path = None
            # Reset the throttle so the FIRST report of a new fault episode is a
            # WARNING. The read arm gets away without this because a successful
            # load latches ``_disk_loaded`` and it never reads again; the write
            # arm has no such latch and sees multiple episodes, so a disk filling
            # up minutes after an unrelated blip was invisible at default level
            # for up to the interval — and the one line emitted said "still
            # failing", which was the opposite of what happened.
            self._disk_write_error_reported_at = None
        except Exception as e:  # noqa: BLE001 — see below
            # Deliberately broad, and symmetric with ``_ensure_disk_loaded``.
            #
            # This used to be ``except OSError`` while the atomic path tripled
            # the syscall surface (``mkdir``, ``os.open``, ``fdopen``,
            # ``json.dump``, ``fsync``, ``replace``). A non-OSError
            # escaping here does NOT stay here: ``put()`` is called by
            # ``resolve()`` on every static-registry hit, so a failure to
            # PERSIST would surface as a failure to RESOLVE — downgrading an
            # already-measured ``decimals`` to unmeasured on the accounting
            # write path. A resolve that succeeded must never be lost because
            # the cache could not be written.
            # Type, path, traceback, and a throttle — the same shape as the read
            # arm. The broad catch above is justified by "unknown exception
            # shapes can reach here", and those are exactly the ones a bare
            # message cannot diagnose: during review an entire write was silently
            # skipped and this line was the only evidence. Unthrottled it also
            # emitted 2000 identical WARNINGs against a permanent EROFS fault,
            # where the read path emits one.
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
                # Never leave a partial temp file behind next to the real cache.
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

    def _evict_if_needed(self) -> None:
        """Evict oldest entries if cache exceeds max size. Must be called with lock held."""
        while len(self._memory) >= self._max_size:
            # Pop oldest item (first item in OrderedDict)
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
            # Check memory cache first
            if key in self._memory:
                # Move to end for LRU ordering
                self._memory.move_to_end(key)
                self._stats["memory_hits"] += 1
                return self._memory[key]

            # Check disk cache
            self._ensure_disk_loaded()
            if key in self._disk_cache:
                start_time = time.perf_counter()
                try:
                    token = ResolvedToken.from_dict(self._disk_cache[key])
                    # Promote to memory
                    self._evict_if_needed()
                    self._memory[key] = token
                    self._stats["disk_hits"] += 1

                    elapsed_ms = (time.perf_counter() - start_time) * 1000
                    if elapsed_ms > 10:
                        _safe_log(logging.DEBUG, f"Disk cache lookup took {elapsed_ms:.2f}ms for {key}")

                    return token
                except Exception as e:  # noqa: BLE001 — see below
                    # A cache row is UNTRUSTED PERSISTED DATA, so every way it can
                    # fail to deserialize is the same outcome: corrupt row, treat
                    # as a miss, evict so it self-heals.
                    #
                    # This used to catch only ``(KeyError, ValueError)``, which was
                    # narrower than the ways ``from_dict`` actually fails
                    # (VIB-6168):
                    #   * ``TypeError``      — type drift, e.g. ``{"decimals": "6"}``;
                    #     ``__post_init__`` does ``self.decimals < 0`` and str < int
                    #     raises.
                    #   * ``AttributeError`` — the row is not a mapping at all.
                    # Both escaped. That matters far more than it looks: a cache
                    # lookup is the FIRST step of every resolve, so one poisoned
                    # row turned every resolve of that token into an exception
                    # rather than a miss — including on accounting write paths,
                    # where callers are written to tolerate a miss and not an
                    # exception.
                    #
                    # Worse, the escape skipped the eviction below, so the poisoned
                    # row NEVER self-healed: every later resolve of that token
                    # raised identically until someone deleted the cache file by
                    # hand. Evicting on every failure mode is the actual repair;
                    # widening the catch is what lets the eviction be reached.
                    #
                    # Deliberately NOT re-raising: this is the same reasoning as
                    # ``best_effort``'s non-string-token branch — a malformed
                    # persisted payload is a DATA defect, not a call-signature
                    # defect, and the correct response to bad data on a
                    # best-effort read path is to drop it loudly, not to fail the
                    # write. Evidence is kept (WARNING + type + traceback), which
                    # is the property the fail-open originally destroyed.
                    _safe_log(
                        logging.WARNING,
                        "Failed to deserialize cached token %s (%s: %s) — evicting the row; "
                        "it will be re-resolved from source on the next lookup",
                        key,
                        type(e).__name__,
                        e,
                        exc_info=True,
                    )
                    # Remove corrupted entry. ``pop`` not ``del``: the row must be
                    # gone whatever happened above. (Only the disk row can exist
                    # here — ``from_dict`` raises before the memory promotion two
                    # lines up, and a memory hit returned long before this point.)
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

            # Create keys for both address and symbol lookups
            address_key = cache_key(token.chain, address=token.address)
            symbol_key = cache_key(token.chain, symbol=token.symbol)

            # Serialize token
            token_dict = token.to_dict()

            # Store in memory (with LRU eviction)
            self._evict_if_needed()
            self._memory[address_key] = token
            self._memory.move_to_end(address_key)

            if symbol_key != address_key:
                self._evict_if_needed()
                self._memory[symbol_key] = token
                self._memory.move_to_end(symbol_key)

            # Store in disk cache
            self._disk_cache[address_key] = token_dict
            if symbol_key != address_key:
                self._disk_cache[symbol_key] = token_dict

            # Write to disk
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
            # Attempt a load first. Do not delete this call.
            #
            # The guard below decides whether we may overwrite the file, and it
            # can only be as good as the information it reads. Without this call
            # it reads state left over from earlier operations, and `clear()`
            # never learns anything about the file it is about to replace.
            #
            # HISTORICAL NOTE, stated carefully because the earlier version of
            # this comment got it wrong: the "authorises an overwrite of a file
            # it never opened" failure belonged to the WIDER guard this branch
            # used to have (`_disk_loaded or _disk_retry_not_before is None`),
            # where a never-read instance took the `is None` arm. Measured
            # against a `main` control at the time: a mode-000 cache went
            # 1 token -> 0 and 0000 -> 0644, where `main` preserved it. That
            # arm is gone. With today's `if self._disk_loaded:`, deleting this
            # call makes a never-read `clear()` REFUSE instead — the opposite
            # direction, caught by
            # `test_clear_on_a_never_read_instance_with_no_fault_still_writes`.
            #
            # `main` was safe in that historical case only by accident: it wrote
            # via `open("w")` on the destination, which raises PermissionError. The
            # atomic `os.replace` path needs only a writable PARENT DIRECTORY,
            # so the destination's own mode stopped being a barrier.
            #
            # NOTE what this call does and does not establish. It does NOT
            # re-stat a cache that is already loaded — it returns immediately
            # when `_disk_loaded` is True. So after a successful earlier load,
            # `clear()` overwrites on the strength of THAT assessment, not a
            # fresh one; a `chmod` landing in between is a TOCTOU window no
            # pre-check can close, and `clear()` is an explicit destructive
            # request from a caller that did successfully read the file. Whether
            # it should re-measure at write time is a genuine design question,
            # tracked in VIB-6183 — deliberately NOT settled by this comment.
            self._ensure_disk_loaded()

            self._memory.clear()
            self._disk_cache.clear()
            # Only authorise the empty overwrite when the disk view is TRUSTED.
            # Latching unconditionally both bypassed the write guard — truncating
            # the file from a never-loaded view, the exact case the guard exists
            # for — and permanently disarmed transient-fault recovery, since
            # `_disk_loaded=True` means the cache never re-reads after the fault
            # clears. `clear()` legitimately wants to write an empty document;
            # it does not want to do so on top of a file it could not read.
            #
            # `_disk_loaded` alone is the whole condition. Every path out of
            # `_ensure_disk_loaded` that leaves it False has recorded a read
            # fault, so a second `_disk_retry_not_before is None` disjunct would
            # be unreachable — and would silently re-authorise the overwrite if
            # anyone later added an early return that skipped the fault record.
            if self._disk_loaded:
                self._write_disk_cache()
            else:
                # The load above did not succeed: either it just failed on the
                # transient arm, or a fault recorded within the last
                # `_DISK_RETRY_COOLDOWN_S` short-circuited it without opening
                # the file at all. Both are "we have no trusted view", which is
                # what this branch turns on — do NOT phrase either as a
                # just-taken measurement of the file (see `4cc321f1b4`).
                #
                # Also state the consequence, which is the part an operator
                # needs: the entries are gone from THIS process, but the file
                # survives, so once `_ensure_disk_loaded` next succeeds they
                # come back. `clear()` is memory-only in this state. Failing
                # safe (no data loss) is deliberate — the alternative is
                # truncating a file we could not read, which is the destruction
                # this module removed a rebuild hatch for.
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
            # Sync all memory entries to disk
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

    # Async-safe wrapper methods

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
            # Run synchronous get in thread pool to avoid blocking event loop
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
