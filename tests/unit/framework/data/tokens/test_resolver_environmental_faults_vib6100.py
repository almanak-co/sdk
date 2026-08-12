"""VIB-6100 — environmental faults must not be mislabelled as caller defects.

Why this file exists
====================

**Read this first: the design these tests were written against was withdrawn.**

``best_effort`` originally sorted resolver failures into three tiers, and tier 2
(``TypeError`` / ``AttributeError`` / ``NameError``) **raised**, on the theory
that those types mean "a defect in the calling code". That is sound only if
nothing *environmental* can reach the seam wearing one of those types — and in a
shared Python process it cannot be made sound. Seven rounds of fault injection
found seven producers that violate it, the last two being the seam's own
evidence-keeping branch (VIB-6167).

So the seam is now **total**: every failure returns ``None`` and it never raises,
which removes the halt these tests were guarding against by construction. What
survives, and what these tests now pin, is the *evidence*: an environmental fault
must not be reported as a caller defect, because the defect observer installed by
``tests/conftest.py`` turns a defect report into a **test failure**. Mislabelling
a flaky disk cache as a signature bug would fail unrelated suites; mislabelling a
signature bug as environmental would let the original VIB-6100 defect back
through.

The producer-side containment below is still required for its own reasons — a
connector calling ``TokenResolver.resolve`` directly has no tolerance arm at all,
so a leaking producer is a live fault for it regardless of what the seam does.
The disk-cache half of that work shipped separately as VIB-6168 / PR #3488.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from almanak.framework.data.tokens import resolver as resolver_module
from almanak.framework.data.tokens.cache import (
    DISK_CACHE_SCHEMA_VERSION,
    TokenCacheManager,
    cache_key,
)

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
    resolve, reaches the accounting seam as a tier-2 *defect*, and fails a
    ledger write for a trade that has already executed on-chain.
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
# 3. Telemetry must never break the thing it observes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("metrics backend unavailable"),
        ValueError("label cardinality exceeded"),
        TypeError("collector signature changed"),
        AttributeError("registry has no such collector"),
        KeyError("duplicate timeseries"),
    ],
)
def test_a_broken_metric_never_raises_into_the_caller(monkeypatch: pytest.MonkeyPatch, exc: Exception) -> None:
    """Any metric fault is swallowed — including the tier-2 types.

    ``TypeError``/``AttributeError`` matter most: those are exactly the types the
    seam treats as a caller defect, so a telemetry fault wearing one of them
    would have been re-raised as "the resolver was called incorrectly" and failed
    the accounting write.
    """
    metrics = pytest.importorskip("almanak.gateway.metrics")

    def _boom(*_a: object, **_k: object) -> None:
        raise exc

    monkeypatch.setattr(metrics, "record_token_resolution_cache_hit", _boom, raising=False)
    # The warn-once set is module state; clear it so the parametrisation is order-independent.
    monkeypatch.setattr(resolver_module, "_METRIC_FAILURES_REPORTED", set())

    resolver_module._try_record_metric("record_token_resolution_cache_hit", "arbitrum", "memory")


def test_a_working_metric_is_actually_called(monkeypatch: pytest.MonkeyPatch) -> None:
    """Anti-vacuity control for the test above.

    Without it, ``_try_record_metric`` could have been reduced to ``pass`` and
    every swallow assertion would still be green.
    """
    metrics = pytest.importorskip("almanak.gateway.metrics")
    seen: list[tuple[object, ...]] = []

    monkeypatch.setattr(
        metrics,
        "record_token_resolution_cache_hit",
        lambda *a, **_k: seen.append(a),
        raising=False,
    )
    monkeypatch.setattr(resolver_module, "_METRIC_FAILURES_REPORTED", set())

    resolver_module._try_record_metric("record_token_resolution_cache_hit", "arbitrum", "memory")
    assert seen == [("arbitrum", "memory")]


def test_a_broken_metric_warns_once_not_once_per_resolve(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Evidence is kept, but a persistently broken backend cannot flood the log."""
    metrics = pytest.importorskip("almanak.gateway.metrics")

    def _boom(*_a: object, **_k: object) -> None:
        raise RuntimeError("down")

    monkeypatch.setattr(metrics, "record_token_resolution_cache_hit", _boom, raising=False)
    monkeypatch.setattr(resolver_module, "_METRIC_FAILURES_REPORTED", set())

    with caplog.at_level("WARNING", logger=resolver_module.__name__):
        for _ in range(5):
            resolver_module._try_record_metric("record_token_resolution_cache_hit", "arbitrum", "memory")

    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1, "a broken metric must warn once, not once per call"
    # The evidence the fail-open used to destroy.
    assert warnings[0].exc_info is not None, "the warning must carry a traceback"


# ---------------------------------------------------------------------------
# 4. The warnings machinery — an advisory notice must not fail a ledger write
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("hook", "label"),
    [
        (None, "showwarning=None -> TypeError"),
        (lambda _a, _b: None, "showwarning wrong arity -> TypeError"),
    ],
)
def test_a_broken_warnings_hook_does_not_fail_the_resolve(
    monkeypatch: pytest.MonkeyPatch, hook: object, label: str
) -> None:
    """A bare-symbol resolve must survive a broken process-global warning hook.

    ``warnings.showwarning`` is process-global and third-party-owned — a test
    harness, a logging shim or a notebook frontend may replace it. A bad
    replacement raises out of ``warn_explicit``, and CPython guards only
    ``file is None`` and ``OSError``, so a *replaced* hook is not covered.

    Every bare-symbol resolve routes through the deprecation notice, and all
    five call sites migrated by VIB-6100 pass symbols — so before this guard the
    resulting ``TypeError`` reached the seam's tier 2, was classified as a caller
    defect and re-raised into an accounting write with the trade already
    on-chain. Reproduced by adversarial review of PR #3472.
    """
    import warnings

    from almanak.framework.data.tokens import deprecation
    from almanak.framework.data.tokens.best_effort import resolve_token_best_effort

    # Warm the singleton with a healthy hook so this isolates the WARNING path
    # (construction is a separate arm, covered below).
    resolver_module.get_token_resolver()

    deprecation._apply_symbol_token_policy.cache_clear()
    monkeypatch.setattr(warnings, "showwarning", hook, raising=False)
    monkeypatch.setattr(warnings, "filters", [("always", None, Warning, "", 0)], raising=False)
    # setattr does not bump the filters version; without this the CPython warning
    # machinery may keep a stale filters snapshot and never call showwarning —
    # which would make this test pass without exercising the broken hook
    # (CodeRabbit #3694).
    if hasattr(warnings, "_filters_mutated"):
        warnings._filters_mutated()

    # Must not raise. The token itself may or may not resolve — irrelevant here.
    resolve_token_best_effort("WETH", "arbitrum", context="broken warning hook")


def test_a_warning_filter_set_to_error_still_escalates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Anti-vacuity control AND a behaviour that must not regress.

    ``simplefilter("error", SymbolTokenResolutionWarning)`` is a deliberate
    user/CI escalation ("treat this deprecation as fatal") and is asserted by
    the existing deprecation suites. The tolerance above must NOT swallow it —
    otherwise the guard would have quietly disabled the deprecation policy's
    own escalation mechanism, which is a worse bug than the one it fixes.
    """
    import warnings

    from almanak.framework.data.tokens.deprecation import SymbolTokenResolutionWarning
    from almanak.framework.data.tokens import deprecation

    deprecation._apply_symbol_token_policy.cache_clear()
    with warnings.catch_warnings():
        warnings.simplefilter("error", SymbolTokenResolutionWarning)
        with pytest.raises(SymbolTokenResolutionWarning):
            deprecation.warn_or_reject_symbol_token_reference("WETH", "arbitrum", api="test")


@pytest.mark.expects_resolver_defect


def test_a_resolver_construction_fault_is_unresolved_not_a_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    """Building the singleton is an ENVIRONMENT fault domain, not a caller defect.

    ``get_token_resolver()`` imports every connector metadata provider (and so
    ``web3``); a fault anywhere in that graph used to propagate raw and untiered
    out of the seam, failing an accounting write. It is now reported as
    unmeasured, loudly.
    """
    from almanak.framework.data.tokens import best_effort

    def _boom() -> object:
        raise RuntimeError("connector metadata import exploded")

    monkeypatch.setattr(resolver_module, "get_token_resolver", _boom)
    assert best_effort.resolve_token_best_effort("USDC", "arbitrum", context="ctor fault") is None
    assert best_effort.resolve_token_decimals_best_effort("USDC", "arbitrum", context="ctor fault") is None


@pytest.mark.expects_resolver_defect


def test_a_construction_fault_of_a_tier_2_type_is_also_tolerated(monkeypatch: pytest.MonkeyPatch) -> None:
    """The construction arm must catch tier-2 TYPES too, or it fixes nothing.

    The observed instance raised ``TypeError`` (a broken warning hook during the
    connector import graph). If the construction arm only caught non-tier-2
    exceptions, that exact case would still propagate.
    """
    from almanak.framework.data.tokens import best_effort

    def _boom() -> object:
        raise TypeError("warnings.showwarning() must be set to a function or method")

    monkeypatch.setattr(resolver_module, "get_token_resolver", _boom)
    assert best_effort.resolve_token_best_effort("USDC", "arbitrum", context="ctor fault") is None


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
    (VIB-6100 review of PR #3472 — the assertion that passed without covering
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
# 6. A malformed token-info RETURN is not a token
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("decimals", "label"),
    [
        (999, "decimals far out of range -> would scale by 10**999"),
        (78, "decimals just past ResolvedToken's 0..77"),
        (-1, "negative decimals"),
        (True, "bool is an int subclass and would read as 1 decimal"),
        ("6", "decimals as a string"),
        (None, "decimals absent"),
    ],
)
@pytest.mark.expects_resolver_defect
def test_an_out_of_contract_decimals_is_unmeasured(
    monkeypatch: pytest.MonkeyPatch, decimals: object, label: str
) -> None:
    """The seam validates the RANGE of what it returns, not just the sign.

    It previously checked only ``decimals < 0``, so ``999`` was accepted and the
    caller scaled a raw amount by ``10**999``, and ``True`` was accepted as one
    decimal. ``ResolvedToken`` permits ``int`` in ``0..77`` and is the shape this
    seam claims to return, so anything else is not a token whatever produced it.
    Reported unmeasured, never a guess. (VIB-6100 review of PR #3472.)
    """
    from types import SimpleNamespace

    from almanak.framework.data.tokens import best_effort

    class _Malformed:
        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False)-> SimpleNamespace:
            return SimpleNamespace(symbol="X", address="0x" + "ab" * 20, decimals=decimals, chain=chain)

    monkeypatch.setattr(resolver_module, "get_token_resolver", _Malformed)
    assert best_effort.resolve_token_decimals_best_effort("X", "arbitrum", context="malformed") is None, label


def test_a_legitimate_decimals_still_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Anti-vacuity control for the range check."""
    from types import SimpleNamespace

    from almanak.framework.data.tokens import best_effort

    class _Fine:
        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False)-> SimpleNamespace:
            return SimpleNamespace(symbol="USDC", address="0x" + "ab" * 20, decimals=6, chain=chain)

    monkeypatch.setattr(resolver_module, "get_token_resolver", _Fine)
    assert best_effort.resolve_token_decimals_best_effort("USDC", "arbitrum", context="fine") == 6


# ---------------------------------------------------------------------------
# 7. The evidence-keeping must not break the tolerance it evidences
# ---------------------------------------------------------------------------


@pytest.mark.expects_resolver_defect
def test_a_broken_logging_handler_does_not_fail_the_resolve(monkeypatch: pytest.MonkeyPatch) -> None:
    """The seam's own guard arms log — and logging can raise.

    ``logging.Handler.handle`` does NOT wrap ``emit`` in a try; a handler is
    expected to call ``self.handleError`` itself, and a third-party one that
    does not (or a formatter that chokes on a record) propagates straight out.
    ``logging.raiseExceptions = False`` does not help — that flag guards errors
    INSIDE ``handleError``, not an ``emit`` that raises.

    So the ERROR-and-tolerate branches became raises on the accounting write
    path: the evidence-keeping broke the tolerance it exists to evidence. Sixth
    instance of the tier-2 class on this PR and the most recursive one.
    (VIB-6100 review of #3472.)
    """
    import logging

    from almanak.framework.data.tokens.best_effort import resolve_token_best_effort

    class _ExplodingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            raise TypeError("logging handler exploded")

    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [_ExplodingHandler()])
    monkeypatch.setattr(root, "level", logging.DEBUG)

    # The non-string-token arm logs at ERROR and must still return None.
    assert resolve_token_best_effort(b"not-a-str", "arbitrum", context="broken logger") is None


@pytest.mark.expects_resolver_defect
def test_logging_still_happens_when_the_handler_works(caplog: pytest.LogCaptureFixture) -> None:
    """Anti-vacuity control: the safe wrapper must not silence real logging.

    Without this, `_safe_log` could have been reduced to `pass` and the test
    above would still be green while every guard arm lost its evidence — which
    is the fail-open-that-destroys-its-own-evidence defect this whole PR exists
    to remove.
    """
    import logging

    from almanak.framework.data.tokens.best_effort import resolve_token_best_effort

    with caplog.at_level(logging.ERROR):
        assert resolve_token_best_effort(b"not-a-str", "arbitrum", context="working logger") is None
    assert any("non-string token" in r.getMessage() for r in caplog.records), (
        "the safe log wrapper must still emit when the handler is healthy"
    )
