"""Pendle chain-truth drift guard (VIB-5300).

Pendle's connector manifest advertises strategy support — both
``strategy_chains`` and the explicit ``strategy_matrix_entries`` row that
renders Pendle as ``category="yield"`` in ``almanak info matrix``. Advertised
chains are part of the public contract: a strategy launched on an advertised
chain must actually compile. Before VIB-5300 the matrix over-advertised seven
chains (arbitrum, ethereum, plasma, sonic, base, mantle, bsc) while the
compiler could only build intents on a subset, so a strategy on
sonic/base/mantle/bsc failed at compile time.

This guard asserts every advertised chain is one Pendle can *actually compile
on*. Crucially it does NOT key on the ``PendleCompiler.chains`` ClassVar:
that ClassVar is advisory for non-staking compilers (it is only enforced
inside ``BaseStakingCompiler.compile`` in
``_strategy_base/base/compiler.py``; Pendle's own ``compile`` never reads it).
Pendle's real compile-truth is:

1. ``_check_pendle_chain_supported`` — the hard chain allowlist the compiler
   evaluates on the SWAP / LP / WITHDRAW paths, and
2. per-chain market data — ``PT_TOKEN_INFO`` / ``YT_TOKEN_INFO`` and the
   ``MARKET_BY_*`` lookups; without an entry for the chain the compiler raises
   "No Pendle market found …" / "not found in PT_TOKEN_INFO …".

A chain compiles only when it clears BOTH gates. (The cross-connector,
non-Pendle generalisation of this problem — there is no single static
compile-truth oracle across connector families — is tracked in VIB-5327.)
"""

from __future__ import annotations

from types import SimpleNamespace

from almanak.connectors.pendle.compiler import _check_pendle_chain_supported
from almanak.connectors.pendle.connector import CONNECTOR
from almanak.connectors.pendle.sdk import (
    MARKET_BY_PT_TOKEN,
    MARKET_BY_YT_TOKEN,
    PT_TOKEN_INFO,
    YT_TOKEN_INFO,
)

# The historical, pre-VIB-5300 over-advertised matrix chain set. Pinned here so
# the guard provably has teeth: the checker MUST reject the four chains that
# carried router addresses but no compilable market data.
_PRE_TRIM_OVERADVERTISED_CHAINS = frozenset({"arbitrum", "ethereum", "plasma", "sonic", "base", "mantle", "bsc"})


def _pendle_compiles_on(chain: str) -> bool:
    """Return True iff Pendle can actually compile an intent on ``chain``.

    Combines the compiler's hard chain allowlist
    (``_check_pendle_chain_supported``) with the presence of the per-chain
    market data the compiler dereferences while building PT/YT legs. Both must
    hold; either gate failing makes a real strategy fail at compile time.
    """
    probe = SimpleNamespace(chain=chain)
    chain_allowed = _check_pendle_chain_supported(probe, "drift-guard-probe", "drift-guard-probe") is None
    has_pt_market = bool(PT_TOKEN_INFO.get(chain)) and bool(MARKET_BY_PT_TOKEN.get(chain))
    has_yt_market = bool(YT_TOKEN_INFO.get(chain)) and bool(MARKET_BY_YT_TOKEN.get(chain))
    return chain_allowed and has_pt_market and has_yt_market


def _advertised_chains() -> frozenset[str]:
    """All chains the live Pendle manifest advertises (strategy + matrix)."""
    chains: set[str] = set(CONNECTOR.strategy_chains or ())
    for entry in CONNECTOR.strategy_matrix_entries or ():
        chains.update(entry.chains)
    return frozenset(chains)


def test_pendle_advertised_chains_are_all_compilable() -> None:
    """Every advertised Pendle chain must be one the compiler can build on.

    Fails-before / passes-after: with the pre-trim 7-chain matrix this fails on
    sonic/base/mantle/bsc; after the VIB-5300 trim to {arbitrum, ethereum} it
    passes.
    """
    advertised = _advertised_chains()
    assert advertised, "Pendle manifest advertises no chains — manifest regression?"

    non_compilable = sorted(c for c in advertised if not _pendle_compiles_on(c))
    assert not non_compilable, (
        "Pendle manifest advertises chains the compiler cannot build on: "
        f"{non_compilable}. Advertised chains must clear both "
        "_check_pendle_chain_supported and per-chain PT/YT market data. Either "
        "trim the advertised set (strategy_chains / strategy_matrix_entries) or "
        "extend compiler coverage (chain allowlist + PT_TOKEN_INFO / "
        "MARKET_BY_PT_TOKEN data) — do not advertise what cannot compile."
    )


def test_guard_fires_on_non_compilable_chains() -> None:
    """Teeth check: the guard must FIRE on chains that cannot compile.

    A guard that accepts everything is a silent no-op that would not have caught
    the VIB-5300 over-advertise bug. We prove it fires two ways, neither of which
    hardcodes the rejected SET — VIB-5324 may add genuine compiler coverage for
    some historical chains and legitimately shrink it (so an exact-set assertion
    would be brittle, per Gemini on PR #2946):

    1. A durable synthetic pin — a chain Pendle has no presence on at all must be
       rejected. VIB-5324 only adds real chains, so this never needs updating.
    2. The historical pre-VIB-5300 over-advertised set must still contain at
       least one chain the guard rejects today, otherwise the fails-before
       scenario has no teeth. This auto-adapts: as real coverage lands the
       rejected subset shrinks, and the assertion only trips once EVERY historical
       chain genuinely compiles — at which point this pin should be revisited.
    """
    # (1) Durable, never-compilable input.
    assert not _pendle_compiles_on("__no_such_chain__"), (
        "chain-truth guard accepted a chain with no Pendle presence — the guard "
        "is a no-op and would not have caught the VIB-5300 over-advertise bug."
    )

    # (2) The guard still rejects >=1 historical over-advertised chain.
    still_rejected = sorted(c for c in _PRE_TRIM_OVERADVERTISED_CHAINS if not _pendle_compiles_on(c))
    assert still_rejected, (
        "chain-truth guard rejects NONE of the historical over-advertised chains "
        f"{sorted(_PRE_TRIM_OVERADVERTISED_CHAINS)} — it has degenerated into a "
        "no-op, OR every historical chain now compiles (if the latter, VIB-5324 "
        "landed full coverage and this pin should be revisited)."
    )

    # ...and it must NOT reject the chains that genuinely compile (no over-reach).
    assert _pendle_compiles_on("arbitrum")
    assert _pendle_compiles_on("ethereum")


def test_pendle_matrix_entry_renders_yield_category() -> None:
    """The explicit matrix entry must remain (intent-derivation can't yield 'yield').

    Pendle's intents (SWAP/LP_OPEN/LP_CLOSE/WITHDRAW) auto-derive to
    swap/lp/lending, never 'yield'; the explicit entry is what renders Pendle
    under the yield category. Guarding it prevents a well-meaning "redundant
    entry" cleanup from silently re-categorising Pendle.
    """
    entries = CONNECTOR.strategy_matrix_entries or ()
    yield_rows = [e for e in entries if e.matrix_name == "pendle" and e.category == "yield"]
    assert len(yield_rows) == 1, (
        "Pendle must declare exactly one explicit yield matrix entry; "
        f"got {[(e.matrix_name, e.category) for e in entries]}"
    )
    assert yield_rows[0].chains == frozenset({"arbitrum", "ethereum"}), (
        f"Pendle yield matrix entry chains drifted: {sorted(yield_rows[0].chains)}"
    )
