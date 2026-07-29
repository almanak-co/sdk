"""VIB-6168 — the token disk cache must not corrupt the file it shares.

``DEFAULT_CACHE_FILE`` is shared by EVERY almanak process for a user — the
strategy runner, the gateway, the dashboard, and each ``ax`` invocation — and
``threading.RLock`` gives no cross-process protection. Three measured defects:

1. A ``put()`` from a never-loaded view truncated a still-valid file. Measured:
   6 tokens on disk -> 2 after one ``put()`` with reads failing.
2. ``_write_disk_cache`` was non-atomic, so a concurrent reader saw
   valid-prefix-then-EOF, classified it as structural corruption, latched, and
   its next ``put()`` overwrote a file that was fine by then. Needs only
   concurrency, not an I/O fault.
3. The read retry was unbounded on permanent ``OSError`` shapes
   (``PermissionError``, ``IsADirectoryError``), costing a failed ``open()`` and
   a traceback-bearing WARNING on every resolve.

Plus two correctness fixes in the same file: the row-deserialize catch was
narrower than the ways ``ResolvedToken.from_dict`` actually fails AND the escape
skipped the eviction (so a poisoned row never self-healed), and the loader did
not validate that the parsed document is a mapping.

Anti-vacuity controls are paired with each: a good row still loads, structural
corruption still latches, a legitimate write still lands. Without them
"returns None" / "does not raise" would hold against a no-op implementation.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from almanak.framework.data.tokens.cache import (
    DISK_CACHE_SCHEMA_VERSION,
    TokenCacheManager,
    cache_key,
)
from almanak.framework.data.tokens.models import ResolvedToken

# A real Arbitrum USDC row, used as the base for the poisoned variants below.
_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
_GOOD_ROW = {
    "symbol": "USDC",
    "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    "decimals": 6,
    "chain": "arbitrum",
    "chain_id": 42161,
}


def _write_cache(path: Path, tokens: object, *, version: object = DISK_CACHE_SCHEMA_VERSION) -> None:
    path.write_text(json.dumps({"version": version, "tokens": tokens}))


def _cache_with(tmp_path: Path, row: object) -> tuple[TokenCacheManager, str]:
    key = cache_key("arbitrum", address=_USDC)
    cache_file = tmp_path / "token_cache.json"
    _write_cache(cache_file, {key: row})
    return TokenCacheManager(cache_file=str(cache_file)), key


# ---------------------------------------------------------------------------
# 1. A poisoned disk-cache row
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("row", "label"),
    [
        ({**_GOOD_ROW, "decimals": "6"}, "decimals type drift (str) -> TypeError"),
        ({**_GOOD_ROW, "decimals": None}, "decimals None -> TypeError"),
        (["not", "a", "mapping"], "row is a list -> TypeError/AttributeError"),
        ("a string row", "row is a str -> TypeError"),
        ({**_GOOD_ROW, "chain_id": "42161"}, "chain_id type drift"),
        ({"symbol": "USDC"}, "missing required keys -> KeyError"),
        ({**_GOOD_ROW, "decimals": 999}, "decimals out of range -> ValueError"),
        ({**_GOOD_ROW, "address": ""}, "empty address -> ValueError"),
    ],
)
def test_a_poisoned_cache_row_is_a_miss_not_a_raise(tmp_path: Path, row: object, label: str) -> None:
    """Every malformed row shape reads as a cache miss, never an exception.

    A raise here does not stay here: it surfaces from the first step of a
    resolve, turns every resolve of that token into an exception rather than a miss.
    """
    cache, _key = _cache_with(tmp_path, row)
    assert cache.get("arbitrum", address=_USDC) is None, label


def test_a_poisoned_cache_row_self_heals_instead_of_raising_forever(tmp_path: Path) -> None:
    """The bad row is evicted, so it cannot wedge the process.

    Pre-fix, a ``TypeError`` escaped *before* the eviction line, so the row
    survived and every later lookup raised identically until someone deleted the
    cache file by hand. Eviction-on-every-failure is the actual repair; widening
    the catch is only what lets the eviction be reached.
    """
    cache, key = _cache_with(tmp_path, {**_GOOD_ROW, "decimals": "6"})

    assert cache.get("arbitrum", address=_USDC) is None
    assert key not in cache._disk_cache, "the corrupt row must be evicted on the first failure"
    # Second lookup must behave identically and still not raise.
    assert cache.get("arbitrum", address=_USDC) is None


def test_a_good_row_still_loads(tmp_path: Path) -> None:
    """Anti-vacuity control for the parametrised test above.

    Without this, every ``is None`` assertion would also pass if ``get`` had been
    broken into returning ``None`` unconditionally — the tests would be green and
    measuring nothing.
    """
    cache, _key = _cache_with(tmp_path, _GOOD_ROW)
    token = cache.get("arbitrum", address=_USDC)
    assert token is not None
    assert token.symbol == "USDC"
    assert token.decimals == 6


# ---------------------------------------------------------------------------
# 2. A structurally corrupt cache FILE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("content", "label"),
    [
        ('["not", "a", "cache"]', "top-level list -> AttributeError pre-fix"),
        ('"just a string"', "top-level string"),
        ("42", "top-level number"),
        ("null", "top-level null"),
        ("{ not json at all", "invalid JSON"),
        # The version MUST be the current one. With a mismatched version the
        # loader takes the schema-drop branch and never reaches the
        # ``isinstance(tokens, dict)`` check — the row would still pass, but for
        # an unrelated reason, covering nothing. (Caught by CodeRabbit on #3472;
        # it is exactly the vacuous-assertion class this file exists to lock in,
        # one layer down.)
        (
            json.dumps({"version": DISK_CACHE_SCHEMA_VERSION, "tokens": "not-a-dict"}),
            "tokens is not a mapping",
        ),
        (
            json.dumps({"version": DISK_CACHE_SCHEMA_VERSION, "tokens": ["not", "a", "mapping"]}),
            "tokens is a list",
        ),
    ],
)
def test_a_corrupt_cache_file_degrades_to_a_cold_cache(tmp_path: Path, content: str, label: str) -> None:
    """An unreadable cache file is a cold cache, never an error."""
    cache_file = tmp_path / "token_cache.json"
    cache_file.write_text(content)
    cache = TokenCacheManager(cache_file=str(cache_file))

    assert cache.get("arbitrum", address=_USDC) is None, label


def test_the_tokens_type_check_is_reached_not_short_circuited_by_the_version_branch(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Pin the precondition the parametrised row above silently depends on.

    ``_ensure_disk_loaded`` checks the schema version **before** the ``tokens``
    shape. A fixture written with a stale hard-coded version, or a future edit
    reordering those two checks, makes the "tokens is not a mapping" row pass
    via the schema-drop branch while covering nothing at all.

    So assert the *reason*, not just the outcome: a current-version file with a
    malformed ``tokens`` must report the load failure, not a version drop.
    """
    import logging

    cache_file = tmp_path / "token_cache.json"
    cache_file.write_text(json.dumps({"version": DISK_CACHE_SCHEMA_VERSION, "tokens": "not-a-dict"}))
    cache = TokenCacheManager(cache_file=str(cache_file))

    with caplog.at_level(logging.DEBUG):
        assert cache.get("arbitrum", address=_USDC) is None

    messages = [r.getMessage() for r in caplog.records]
    assert any("Failed to load disk cache" in m for m in messages), (
        "the tokens-shape branch must be what failed; if this row is being handled by the "
        "schema-mismatch branch instead, it is covering nothing"
    )
    assert not any("token_cache_schema_mismatch" in m for m in messages), (
        "the fixture's version must match DISK_CACHE_SCHEMA_VERSION, or the version branch "
        "short-circuits the check under test"
    )


def test_a_corrupt_cache_file_is_not_retried_forever(tmp_path: Path) -> None:
    """``_disk_loaded`` is set even on the failure path.

    Pre-fix it was assigned only after the ``try``, so an escaping exception left
    it ``False`` — the next call re-read the same bad file and raised again, for
    the life of the process. A once-only degrade is the difference between a
    warning at boot and a strategy that cannot resolve a token until it is
    restarted.
    """
    cache_file = tmp_path / "token_cache.json"
    cache_file.write_text('["not", "a", "cache"]')
    cache = TokenCacheManager(cache_file=str(cache_file))

    assert cache.get("arbitrum", address=_USDC) is None
    assert cache._disk_loaded is True, "a failed load must still mark the disk cache as loaded"

    # Corrupt file replaced by a valid one: the already-degraded cache must NOT
    # re-read it (that is the documented once-only contract), and must not raise.
    _write_cache(cache_file, {cache_key("arbitrum", address=_USDC): _GOOD_ROW})
    assert cache.get("arbitrum", address=_USDC) is None


def test_a_writable_cache_still_round_trips_after_a_corrupt_load(tmp_path: Path) -> None:
    """Degrading to a cold cache must leave the cache usable, not inert."""
    cache_file = tmp_path / "token_cache.json"
    cache_file.write_text("{ not json at all")
    cache = TokenCacheManager(cache_file=str(cache_file))

    from almanak.framework.data.tokens.models import ResolvedToken

    token = ResolvedToken(**_GOOD_ROW)
    cache.put(token)
    assert cache.get("arbitrum", address=_USDC) is not None


# ---------------------------------------------------------------------------
# 5. Transient I/O must NOT be treated like structural corruption
# ---------------------------------------------------------------------------


def test_a_transient_read_error_does_not_destroy_the_persisted_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``put()`` issued while reads are failing must NOT truncate a valid file.

    This is the property the previous version of this test *claimed* in its
    docstring and never exercised: it made the read fail once and asserted the
    next ``get()`` succeeded, which does not touch the destructive path at all.
    ``put()`` is the only path that can destroy the file — and it did.

    Mechanism: ``put()`` calls ``_ensure_disk_loaded()``, which on the transient
    arm leaves ``_disk_cache`` empty, and then wrote that empty view over a file
    still holding every dynamically discovered token. Not latching narrowed the
    window; refusing to write from a view that never loaded is what closes it.
    (VIB-6168 — the assertion that passed without covering
    its own stated claim.)
    """
    from almanak.framework.data.tokens.models import ResolvedToken

    cache_file = tmp_path / "token_cache.json"
    seeded = {cache_key("arbitrum", address=_USDC): _GOOD_ROW}
    _write_cache(cache_file, seeded)
    before = json.loads(cache_file.read_text())["tokens"]
    assert len(before) == 1, "fixture precondition: the file holds a real row"

    cache = TokenCacheManager(cache_file=str(cache_file))

    real_open = Path.open

    def _always_fails(self: Path, *a: object, **k: object):  # type: ignore[no-untyped-def]
        if self == cache_file:
            raise OSError(24, "Too many open files")
        return real_open(self, *a, **k)

    monkeypatch.setattr(Path, "open", _always_fails)

    assert cache.get("arbitrum", address=_USDC) is None
    assert cache._disk_loaded is False, "a transient OSError must not latch the cache as loaded"

    # THE destructive path: a put() while the fault is live.
    monkeypatch.undo()  # let the WRITE succeed, so only the guard can stop it
    cache.put(ResolvedToken(**{**_GOOD_ROW, "symbol": "NEW", "address": "0x" + "cd" * 20}))

    after = json.loads(cache_file.read_text())["tokens"]
    assert after == before, (
        "a put() from a never-loaded cache view must not overwrite the persisted file; "
        f"had {len(before)} row(s) before, {len(after)} after"
    )


def test_the_cache_recovers_after_the_retry_cooldown(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The transient arm retries — bounded by a cooldown, not per call.

    Retrying on every call would cost a failed ``open()`` and a traceback-bearing
    WARNING on every resolve for a PERMANENT fault (``PermissionError``,
    ``IsADirectoryError`` — all ``OSError``, none self-healing), and token
    resolution runs per-leg per-iteration.
    """
    import almanak.framework.data.tokens.cache as cache_module

    cache_file = tmp_path / "token_cache.json"
    _write_cache(cache_file, {cache_key("arbitrum", address=_USDC): _GOOD_ROW})
    cache = TokenCacheManager(cache_file=str(cache_file))

    real_open = Path.open
    calls = {"n": 0}

    def _flaky(self: Path, *a: object, **k: object):  # type: ignore[no-untyped-def]
        if self == cache_file and calls["n"] == 0:
            calls["n"] += 1
            raise OSError(24, "Too many open files")
        return real_open(self, *a, **k)

    monkeypatch.setattr(Path, "open", _flaky)

    assert cache.get("arbitrum", address=_USDC) is None
    # Inside the cooldown: served cold WITHOUT another failed open().
    assert cache.get("arbitrum", address=_USDC) is None
    assert calls["n"] == 1, "the cooldown must suppress repeated open() attempts"

    # Past the cooldown: retried, and the row is there all along.
    # Advance RELATIVE to the recorded deadline — ``time.monotonic()`` is uptime,
    # so a fixed literal is very likely to move the clock BACKWARDS.
    assert cache._disk_retry_not_before is not None
    resume_at = cache._disk_retry_not_before + 1.0
    monkeypatch.setattr(cache_module.time, "monotonic", lambda: resume_at)
    token = cache.get("arbitrum", address=_USDC)
    assert token is not None and token.symbol == "USDC", "the cache must recover once the fault clears"


def test_structural_corruption_still_latches(tmp_path: Path) -> None:
    """Control for the test above — the two policies must stay distinguishable.

    Re-reading cannot fix bad bytes, so structural corruption latches. Without
    this, "retry on OSError" could have been widened to "retry on everything",
    reinstating the parse-and-raise-on-every-lookup behaviour.
    """
    cache_file = tmp_path / "token_cache.json"
    cache_file.write_text("{ not json at all")
    cache = TokenCacheManager(cache_file=str(cache_file))

    assert cache.get("arbitrum", address=_USDC) is None
    assert cache._disk_loaded is True, "structural corruption must latch so it is not re-parsed per lookup"


# ---------------------------------------------------------------------------
# The atomic write itself — the half that shipped unasserted
# ---------------------------------------------------------------------------


def _usdc_token():  # type: ignore[no-untyped-def]
    from almanak.framework.data.tokens.models import ResolvedToken

    return ResolvedToken(**_GOOD_ROW)


def test_a_successful_write_leaves_no_temp_file_behind(tmp_path: Path) -> None:
    cache_file = tmp_path / "token_cache.json"
    _write_cache(cache_file, {})
    cache = TokenCacheManager(cache_file=str(cache_file))
    cache.put(_usdc_token())

    leftovers = [p.name for p in tmp_path.iterdir() if ".tmp" in p.name]
    assert leftovers == [], f"atomic write leaked a temp file: {leftovers}"


def test_a_failed_write_leaves_no_temp_file_behind(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The cleanup must survive a failure, which is when it matters."""
    import os

    cache_file = tmp_path / "token_cache.json"
    _write_cache(cache_file, {})
    cache = TokenCacheManager(cache_file=str(cache_file))

    def _boom(*_a: object, **_k: object) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(os, "replace", _boom)
    cache.put(_usdc_token())  # must not raise

    leftovers = [p.name for p in tmp_path.iterdir() if ".tmp" in p.name]
    assert leftovers == [], f"a failed atomic write leaked a temp file: {leftovers}"


def test_a_concurrent_reader_never_observes_a_truncated_file(tmp_path: Path) -> None:
    """The property the atomic write exists for — observed CONCURRENTLY.

    The previous version of this test looped ``put()`` then read the file after
    ``put()`` RETURNED, and was mutation-proven vacuous: it passed against the
    pre-fix truncate-then-stream writer. The torn state exists only *during* the
    write, so a sequential reader can never sample it — the assertion and the
    fixture were both fine and the OBSERVATION WINDOW was wrong.

    That is a distinct way for an anti-vacuity control to fail, beyond an
    unreachable fixture or a value that collides with the mutation's output: a
    test can be unable to *observe* the failure it names. A control must be able
    to see the state it claims to reject.

    Measured discrimination for this version — atomic vs a truncate-then-stream
    writer, same fixture: 0/2733 torn reads vs 2507/2850 (~88%). Not a flake.
    """
    import json as _json
    import threading

    cache_file = tmp_path / "token_cache.json"
    # Large enough that streaming the document is not instantaneous — a tiny
    # cache can be written inside one scheduler slice and hide the window.
    seeded = {cache_key("arbitrum", address=f"0x{i:040x}"): {**_GOOD_ROW, "address": f"0x{i:040x}"} for i in range(600)}
    _write_cache(cache_file, seeded)

    cache = TokenCacheManager(cache_file=str(cache_file), max_size=5000)
    cache.get("arbitrum", address=f"0x{0:040x}")  # warm the load

    torn: list[str] = []
    stop = threading.Event()

    def _reader() -> None:
        while not stop.is_set():
            try:
                _json.loads(cache_file.read_text())
            except (ValueError, OSError) as exc:  # truncated / mid-rename
                torn.append(type(exc).__name__)

    t = threading.Thread(target=_reader, daemon=True)
    t.start()
    try:
        for i in range(120):
            addr = f"0x{(10_000 + i):040x}"
            cache.put(ResolvedToken(**{**_GOOD_ROW, "address": addr}))
    finally:
        stop.set()
        t.join(timeout=5)

    assert torn == [], (
        f"a concurrent reader observed {len(torn)} unparseable states — the write is not atomic: {torn[:5]}"
    )


def test_a_write_failure_does_not_lose_an_already_measured_resolve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cache-write hiccup must not downgrade a measured decimals to unmeasured.

    ``put()`` is called by ``resolve()`` on every static-registry hit, so a
    non-``OSError`` escaping the write path would surface as a failure to
    RESOLVE — turning a measured value into an unmeasured one on the accounting
    write path (Empty != Zero). The catch is deliberately broad and symmetric
    with the read path for exactly this reason.
    """
    import json as _json

    cache_file = tmp_path / "token_cache.json"
    _write_cache(cache_file, {})
    cache = TokenCacheManager(cache_file=str(cache_file))

    def _boom(*_a: object, **_k: object) -> None:
        raise RuntimeError("not an OSError")

    monkeypatch.setattr(_json, "dump", _boom)
    monkeypatch.setattr("almanak.framework.data.tokens.cache.json.dump", _boom)

    cache.put(_usdc_token())  # must not raise
    token = cache.get("arbitrum", address=_USDC)
    assert token is not None and token.decimals == 6, "a failure to PERSIST must not become a failure to RESOLVE"


def test_a_raising_log_handler_does_not_skip_the_row_eviction(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The repair must not be reachable only when logging works.

    The corrupt-row arm logs BEFORE it evicts, so a third-party handler whose
    ``emit`` raises would skip the eviction — reinstating verbatim the "poisoned
    row never self-heals" defect this module fixes.
    """
    import logging

    class _ExplodingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            raise TypeError("logging handler exploded")

    cache, key = _cache_with(tmp_path, {**_GOOD_ROW, "decimals": "6"})
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [_ExplodingHandler()])
    monkeypatch.setattr(root, "level", logging.DEBUG)

    assert cache.get("arbitrum", address=_USDC) is None
    assert key not in cache._disk_cache, "the eviction must happen even when logging raises"


# ---------------------------------------------------------------------------
# Coverage for the fixes that were mutation-proven UNCOVERED
# ---------------------------------------------------------------------------


def test_a_permanent_read_fault_is_re_reported_not_silenced_forever(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A fault that never self-heals must stay visible.

    Reporting ONCE meant a permanent misconfiguration (a root-owned cache file,
    a directory in its place) made the disk layer silently vanish for the
    process lifetime after a single WARNING at boot.

    Mutation-proven uncovered before this test: reverting the re-arm to
    report-once left the suite fully green.
    """
    import logging

    import almanak.framework.data.tokens.cache as cache_module

    cache_dir = tmp_path / "token_cache.json"
    cache_dir.mkdir()  # a DIRECTORY where the cache file should be — never self-heals
    cache = TokenCacheManager(cache_file=str(cache_dir))

    clock = {"t": 1000.0}
    monkeypatch.setattr(cache_module.time, "monotonic", lambda: clock["t"])

    with caplog.at_level(logging.WARNING, logger=cache_module.__name__):
        for _ in range(4):
            cache.get("arbitrum", address=_USDC)
            clock["t"] += cache_module._DISK_ERROR_REPORT_INTERVAL_S + 1

    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) >= 3, (
        f"a permanent read fault must be re-reported, not silenced after one WARNING; got {len(warnings)}"
    )


def test_a_failing_fdopen_does_not_leak_the_descriptor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`os.fdopen` takes ownership of the fd ONLY on success.

    If it raises, the descriptor stays open and nothing else closes it — the
    `finally` unlinks the temp file but cannot reclaim an fd. A repeated write
    fault would then leak descriptors until EMFILE, which is *itself* one of the
    faults this module tolerates: the leak would manufacture the very condition
    it degrades on.
    """
    import os as _os

    cache_file = tmp_path / "token_cache.json"
    _write_cache(cache_file, {})
    cache = TokenCacheManager(cache_file=str(cache_file))

    def _boom(fd, *a, **k):  # type: ignore[no-untyped-def]
        raise OSError(24, "Too many open files")

    monkeypatch.setattr("almanak.framework.data.tokens.cache.os.fdopen", _boom)

    if not _os.path.isdir("/dev/fd"):
        pytest.skip("no /dev/fd on this platform — fd counting is unavailable")
    before = len(_os.listdir("/dev/fd"))
    for _ in range(50):
        cache.put(ResolvedToken(**_GOOD_ROW))  # must not raise
    monkeypatch.undo()

    after = len(_os.listdir("/dev/fd"))
    assert after - before < 10, f"descriptors leaked across 50 failed writes: {before} -> {after}"

    # And no temp files survived either.
    assert [p.name for p in tmp_path.iterdir() if ".tmp" in p.name] == []


def test_a_symlinked_cache_path_is_not_followed(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Following a symlink at the cache path is an arbitrary-file overwrite.

    With a world-writable directory (the /tmp fallback) and `token_cache.json`
    symlinked at a victim-owned file, writing THROUGH the link destroys that
    file and replaces it with cache JSON — and the link survives, so it re-fires
    on every write. Measured; CWE-59.

    `main` followed the link (`open("w")`), so this is a deliberate divergence,
    justified by the CWE-59 measurement alone. It was originally also argued
    from "no shared volume exists" — a premise since REPUDIATED (/tmp/.almanak
    IS a shared namespace) and no longer load-bearing here. Noted so the stale
    reasoning is not mistaken for live support.

    Replacing the link touches nothing outside the cache directory — but it does
    silently change a symlink into a regular file, so it is announced.
    """
    import logging

    victim = tmp_path / "victim_secrets.txt"
    victim.write_text("VICTIM DATA")
    link = tmp_path / "token_cache.json"
    link.symlink_to(victim)

    cache = TokenCacheManager(cache_file=str(link))
    with caplog.at_level(logging.WARNING):
        cache.put(ResolvedToken(**_GOOD_ROW))

    assert victim.read_text() == "VICTIM DATA", "a cache write must never overwrite the symlink TARGET"
    assert json.loads(link.read_text())["tokens"], "the write must land at the cache path itself"
    assert any("is a symlink" in r.getMessage() for r in caplog.records), (
        "replacing a symlink must be announced, not silent"
    )


def test_clear_does_not_overwrite_a_cache_it_could_not_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`clear()` must not authorise an empty overwrite from a never-loaded view.

    It used to set `_disk_loaded = True` unconditionally, which both bypassed
    the write guard (truncating the file from an unloaded view — the exact case
    the guard exists for) and permanently disarmed transient-fault recovery,
    since a latched `_disk_loaded` means the cache never re-reads after the
    fault clears.
    """
    cache_file = tmp_path / "token_cache.json"
    _write_cache(cache_file, {cache_key("arbitrum", address=_USDC): _GOOD_ROW})
    before = json.loads(cache_file.read_text())["tokens"]

    cache = TokenCacheManager(cache_file=str(cache_file))

    real_open = Path.open

    def _always_fails(self: Path, *a: object, **k: object):  # type: ignore[no-untyped-def]
        if self == cache_file:
            raise OSError(24, "Too many open files")
        return real_open(self, *a, **k)

    monkeypatch.setattr(Path, "open", _always_fails)
    cache.get("arbitrum", address=_USDC)  # trip the read fault
    assert cache._disk_loaded is False
    monkeypatch.undo()

    cache.clear()

    after = json.loads(cache_file.read_text())["tokens"]
    assert after == before, "clear() must not truncate a file it was unable to read"


def test_an_exotic_json_failure_is_a_cold_cache_not_a_raise(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Mirror of the WRITE path's broad-catch test — the READ path had none.

    Mutation-proven gap: narrowing `_ensure_disk_loaded`'s catch to
    `(json.JSONDecodeError, ValueError)` left the entire suite green, even
    though a non-`ValueError` is reachable from an untrusted file — a deeply
    nested document makes `json.load` raise `RecursionError`.

    That asymmetry matters because `get()` is the first step of every resolve:
    if the catch is ever narrowed, the exception escapes, `_disk_loaded` stays
    False, and every resolve of every token RAISES instead of missing —
    including on accounting write paths whose callers tolerate a miss and not an
    exception. Verbatim the defect this module exists to fix, and nothing would
    have caught its return. A `# noqa: BLE001` on the line is a standing
    invitation to narrow it, so the guard needs a test, not a comment.
    """
    cache_file = tmp_path / "token_cache.json"
    _write_cache(cache_file, {})
    cache = TokenCacheManager(cache_file=str(cache_file))

    def _exotic(*_a: object, **_k: object) -> None:
        raise RecursionError("maximum recursion depth exceeded while decoding")

    monkeypatch.setattr("almanak.framework.data.tokens.cache.json.load", _exotic)

    assert cache.get("arbitrum", address=_USDC) is None, "a non-ValueError must be a miss, not a raise"
    assert cache._disk_loaded is True, "structural corruption must latch so it is not re-parsed per lookup"


def test_clear_on_a_never_read_instance_with_no_fault_still_writes(tmp_path: Path) -> None:
    """`clear()` on a READABLE file must actually clear it.

    This is the "does its job" half of the pair. Its sibling below pins the
    opposite direction — a file this instance could never LOAD must be left
    alone — and together they forbid both a destructive `clear()` and one that
    silently refuses. Neither test alone is sufficient: a no-op `clear()` would
    satisfy the sibling, and an unconditional overwrite would satisfy this one.

    Note the sibling's scope precisely: it covers a cache this instance never
    successfully loaded. It does NOT claim that any unreadable file is always
    spared — a cache loaded successfully and made unreadable afterwards can
    still be cleared, which is the adjudicated behaviour tracked in VIB-6183.

    Note the instance is never read before `clear()`, so this also covers the
    path where `clear()`'s own `_ensure_disk_loaded()` is what loads the file.

    (An earlier version of this docstring claimed to pin an
    `or self._disk_retry_not_before is None` arm of `clear()`'s guard, and said
    that arm was "the only thing distinguishing the new guard from the old
    unconditional bypass". Both statements are obsolete: once `clear()` calls
    `_ensure_disk_loaded()` first, every path leaving `_disk_loaded` False has
    recorded a read fault, so that disjunct became unreachable and was removed.
    The test kept its value; only its stated rationale was wrong.)
    """
    cache_file = tmp_path / "token_cache.json"
    _write_cache(cache_file, {cache_key("arbitrum", address=_USDC): _GOOD_ROW})

    cache = TokenCacheManager(cache_file=str(cache_file))  # never read
    cache.clear()

    assert json.loads(cache_file.read_text())["tokens"] == {}, (
        "clear() with no observed fault must still be able to write an empty document"
    )


def test_clear_never_overwrites_a_cache_it_could_not_read(tmp_path: Path) -> None:
    """The invariant the whole module is built on, applied to ``clear()``.

    A file we cannot read is a file we cannot assess, so overwriting it is data
    loss — the reason the rebuild hatch was removed. ``put()`` honoured that;
    ``clear()`` did not, and NOTHING IN THIS SUITE COULD TELL. The 39 tests
    passed identically whether ``clear()`` preserved the file or destroyed it,
    so "green" carried no information about the one property with irreversible
    consequences.

    Measured against an ``origin/main`` control before the fix:

        origin/main   clear   mode 0000 -> 0000   tokens 1 -> 1   PRESERVED
        this branch   clear   mode 0000 -> 0644   tokens 1 -> 0   DESTROYED

    ``main`` was safe only incidentally: it wrote with ``open("w")`` on the
    destination, which raises PermissionError on a mode-000 file. The atomic
    ``os.replace`` path needs only a writable PARENT DIRECTORY, so the file's
    own mode stopped being a barrier and the accidental protection vanished
    with the very change that made writes safe against tearing.

    Mutation-proven, measured — note WHICH mutation, because the obvious guess
    is wrong: this test fails when ``clear()``'s guard is removed AND
    ``_disk_loaded`` is forced True (the true pre-fix bypass). Deleting only the
    ``_ensure_disk_loaded()`` call does NOT fail this test — the narrowed guard
    then refuses the write, and the READABLE-file sibling above fails instead.
    The two tests together forbid both a destructive ``clear()`` and a
    ``clear()`` that refuses to do its job; neither covers both directions
    alone.

    Removing the guard WITHOUT forcing the latch also leaves this test green,
    because ``_write_disk_cache`` independently refuses to persist from a
    never-loaded view. ``clear()``'s guard is the SECOND layer, not the only
    one — worth knowing before simplifying either on the belief that the other
    carries it.
    """
    if os.geteuid() == 0:
        pytest.skip("root bypasses mode bits, so the unreadable case cannot be staged")

    cache_file = tmp_path / "token_cache.json"
    _write_cache(cache_file, {cache_key("arbitrum", address=_USDC): _GOOD_ROW})
    before = cache_file.read_bytes()
    os.chmod(cache_file, 0o000)

    cache = TokenCacheManager(cache_file=str(cache_file))  # never read
    cache.clear()

    assert cache._disk_loaded is False, (
        "an unreadable file is the NON-latching arm — latching would disarm the write guard"
    )

    os.chmod(cache_file, 0o644)
    assert cache_file.read_bytes() == before, "clear() overwrote a cache file it could not read"


def test_fsync_runs_before_the_rename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Durability before visibility — the docstring claims it, nothing enforced it.

    Mutation-proven gap: removing `os.fsync` left the suite green, so a future
    tidy-up could drop it silently and the rename could commit before the bytes.
    """
    import os

    order: list[str] = []
    real_fsync, real_replace = os.fsync, os.replace
    monkeypatch.setattr(
        "almanak.framework.data.tokens.cache.os.fsync",
        lambda fd: (order.append("fsync"), real_fsync(fd))[1],
    )
    monkeypatch.setattr(
        "almanak.framework.data.tokens.cache.os.replace",
        lambda a, b: (order.append("replace"), real_replace(a, b))[1],
    )

    cache = TokenCacheManager(cache_file=str(tmp_path / "token_cache.json"))
    cache.put(ResolvedToken(**_GOOD_ROW))

    assert order == ["fsync", "replace"], f"fsync must precede replace, got {order}"


def test_the_creation_mode_literal_is_pinned_under_three_umasks(tmp_path: Path) -> None:
    """Pins the SOURCE literal, not just a derived value.

    Asserting `0o666 & ~umask` under CI's umask 022 gives 0644 — and so does
    `0o644 & ~umask`. Mutation-proven: changing the literal 0o666 -> 0o644
    SURVIVED the whole suite, so the control green-lit both values and would
    equally green-light a widening back. Vacuity class 3: the fixture value
    collides with what the mutation produces.

    Two umasks disambiguate: under 0o000 a 0644 literal yields 0644 while 0o666
    would yield 0666 (world-WRITABLE), which is precisely the case that matters
    in the shared /tmp fallback.
    """
    import os
    import stat

    # Only the umask-000 row DISCRIMINATES: 0o644 & ~000 == 0644 but
    # 0o666 & ~000 == 0666. Under 022 both literals give 0644 and under 077 both
    # give 0600, so those two rows are regression pins for the umask-subtraction
    # behaviour, not killers. Stated so a reader does not mistake three rows for
    # three independent checks of the literal.
    for umask_val, expected in ((0o022, 0o644), (0o000, 0o644), (0o077, 0o600)):
        old = os.umask(umask_val)
        try:
            cache_file = tmp_path / f"cache_{umask_val:03o}.json"
            TokenCacheManager(cache_file=str(cache_file)).put(ResolvedToken(**_GOOD_ROW))
            actual = stat.S_IMODE(cache_file.stat().st_mode)
        finally:
            os.umask(old)
        assert actual == expected, (
            f"umask {umask_val:03o}: expected {expected:o}, got {actual:o} — the creation-mode literal has changed"
        )


def test_the_cache_is_never_created_world_writable(tmp_path: Path) -> None:
    """World-READABLE is required (it is what makes a foreign cache self-heal).

    World-WRITABLE is not, and in the shared /tmp fallback a rewritten row can
    carry a wrong `decimals`, which scales amounts.
    """
    import os
    import stat

    old = os.umask(0o000)
    try:
        cache_file = tmp_path / "permissive.json"
        TokenCacheManager(cache_file=str(cache_file)).put(ResolvedToken(**_GOOD_ROW))
        mode = stat.S_IMODE(cache_file.stat().st_mode)
    finally:
        os.umask(old)
    assert not mode & stat.S_IWOTH, f"cache created world-writable ({mode:o}) under a permissive umask"
    assert mode & stat.S_IROTH, f"cache must stay world-readable for self-heal ({mode:o})"


def test_a_world_readable_cache_loads_and_is_taken_over_by_the_next_put(tmp_path: Path) -> None:
    """Renamed from `..._a_foreign_owned_cache_self_heals_...`, which it did NOT test.

    HONEST SCOPE — read this before treating it as the 0600 argument's evidence.
    The fixture `chmod`s a file THIS PROCESS OWNS. There is no second uid and no
    `os.chown` (which needs root, so a unit test cannot have one). It would
    therefore still pass if production rejected every foreign-owned file
    outright — the exact property the old name claimed. Vacuity class 1: the
    fixture cannot reach the condition under test.

    Found by adversarial review, and it mattered because this was cited as the
    load-bearing test for withdrawing the 0600 mode. It is not.

    NOR IS ANYTHING ELSE, and the previous version of this paragraph claimed
    otherwise by citing `test_an_unreadable_cache_is_left_intact_and_recovers`
    — A TEST THAT DOES NOT EXIST. It never has; the name appeared for the first
    time in the commit that wrote this docstring. Fabricating a citation inside
    the paragraph headed HONEST SCOPE, as the evidence for a security decision,
    is worse than the vacuous test it was apologising for: a reader can run a
    weak test and judge it, but cannot run one that was invented.

    What genuinely exists, and what it actually covers: a read FAULT is
    exercised by `test_a_transient_read_error_does_not_destroy_the_persisted_cache`
    and `test_the_cache_recovers_after_the_retry_cooldown`, both of which
    monkeypatch `Path.open` to raise `OSError`. That is a simulated fault, not
    a permission denial.

    A REAL permission denial is now exercised, by
    `test_clear_never_overwrites_a_cache_it_could_not_read`, which `chmod`s the
    cache file to 0o000 and asserts the bytes survive. (This paragraph
    previously said no test in this suite chmods a file unreadable and that the
    only chmod sets 0644 — true when written, falsified by that test.) It
    covers the `clear()` path specifically, so the "you cannot be locked out"
    half of the 0600 argument still rests on fault injection plus the reasoning
    in `_open_temp` for the `get()`/`put()` paths, rather than on an observed
    denial there.

    What this pins, accurately: a world-readable cache loads, and the next
    `put()` takes ownership rather than requiring an escape hatch. Cross-uid
    behaviour is asserted nowhere in this suite and would need an integration
    test running as two users.
    """
    import os
    import stat

    key = cache_key("arbitrum", address=_USDC)
    cache_file = tmp_path / "token_cache.json"
    _write_cache(cache_file, {key: _GOOD_ROW})
    os.chmod(cache_file, 0o644)  # readable by anyone, as main produced

    cache = TokenCacheManager(cache_file=str(cache_file))
    assert cache.get("arbitrum", address=_USDC) is not None, "a 0644 cache must load"

    # umask PINNED. The mode is `0o644 & ~umask`, so the world-readable
    # assertion below is FALSE under umask 077 (-> 0600) or 027 (-> 0640) — it
    # would hard-fail, not flake, on a developer or CI runner with a hardened
    # umask. It also asserted a property `_open_temp`'s own docstring says the
    # function cannot guarantee. Pin the umask and the assertion is about the
    # literal, which IS this function's to guarantee.
    old_umask = os.umask(0o022)
    try:
        cache.put(ResolvedToken(**{**_GOOD_ROW, "symbol": "WETH", "address": "0x" + "ab" * 20}))
    finally:
        os.umask(old_umask)
    tokens = json.loads(cache_file.read_text())["tokens"]
    assert len(tokens) > 1, "the next put() must take ownership — this is the self-heal"

    # The original row must SURVIVE the takeover. Without this the assertion
    # above is satisfied by a rewrite that discarded the foreign entries, which
    # is destruction, not adoption — and destruction is what the withdrawn
    # rebuild hatch did.
    assert key in tokens, "the pre-existing entry must be merged, not discarded"
    assert stat.S_IMODE(cache_file.stat().st_mode) & stat.S_IROTH, (
        "the takeover must not narrow the file out of world-readability"
    )


def test_a_new_write_fault_episode_reports_at_warning_not_debug(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The write-error throttle must RESET on success — mutation-proven uncovered.

    The read arm got this test after being found uncovered; the write arm kept
    the rationale and no test. Without the reset, a disk filling up minutes
    after an unrelated blip is invisible at default level for a full interval,
    and the one line emitted says "still failing" — the opposite of what
    happened, since the first fault had cleared.
    """
    import logging

    import almanak.framework.data.tokens.cache as cache_module

    cache_file = tmp_path / "token_cache.json"
    _write_cache(cache_file, {})
    cache = TokenCacheManager(cache_file=str(cache_file))
    cache.get("arbitrum", address=_USDC)  # load so the write guard permits

    clock = {"t": 1000.0}
    monkeypatch.setattr(cache_module.time, "monotonic", lambda: clock["t"])

    real_replace = os.replace
    fail = {"on": True}

    def _maybe_fail(a, b):  # type: ignore[no-untyped-def]
        if fail["on"]:
            raise OSError(30, "Read-only file system")
        return real_replace(a, b)

    monkeypatch.setattr(cache_module.os, "replace", _maybe_fail)

    with caplog.at_level(logging.WARNING, logger=cache_module.__name__):
        cache.put(ResolvedToken(**_GOOD_ROW))  # episode 1 -> WARNING
        fail["on"] = False
        clock["t"] += 60
        cache.put(ResolvedToken(**_GOOD_ROW))  # success -> resets throttle
        fail["on"] = True
        clock["t"] += 60  # well inside the interval
        cache.put(ResolvedToken(**_GOOD_ROW))  # episode 2 -> must WARN again

    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) >= 2, (
        f"a NEW write-fault episode must report at WARNING, not be throttled as a repeat; got {len(warnings)}"
    )
