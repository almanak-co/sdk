"""Every gateway RPC must have a documented, verified path to a real
consumer, or be explicitly exempted with a stated reason (ALM-3474).

ALM-3474 was exactly this defect class: `GetLendingMarket` existed as a fully
implemented gateway RPC (`MarketService`, VIB-5985) with a real servicer
implementation, but nothing in the CLI, agent-tools, or framework/connector
layer ever called it — a user had a documented capability with literally no
supported way to reach it.

**Two tiers, deliberately not one, after a real design failure while
building this file.** The first version of this test used a single,
generic, flat "does the RPC name appear anywhere in almanak/ outside the
gateway's own proto/servicer code" scan and called that "reachable". It
FALSELY passed for `GetLendingMarket` even before ALM-3474 was fixed, and
for `ListLendingMarkets` (still broken today, see EXEMPT_RPCS) — both are
referenced ONLY by an unused `MarketSnapshot` wrapper method
(`lending_market()` / `lending_markets()`) that itself has zero callers
anywhere. A flat textual scan cannot distinguish "a real consumer calls
this" from "a client-side wrapper exists that nothing calls" — exactly the
gap this file exists to close, so trusting it here would be circular.

A general, fully-automated multi-hop call-graph check was prototyped to fix
this (walk the caller graph transitively until reaching a genuine leaf
consumer) and rejected: on this codebase it either stayed shallow (1 hop —
the same flaw above) or, once extended to multiple hops, combinatorially
exploded on common short method names (`price`, `run`, `execute`, ...),
taking minutes per RPC and still risking false "reachable" verdicts from
unrelated same-named functions. Building a properly qualified-name-aware
call-graph tool is a real, separate engineering project, not a fix that
belongs in this PR.

So:

* **Tier 1 — `_has_any_reference_outside_gateway_definition`**: fast, flat,
  whole-gateway, and HONEST about what it proves — only that the RPC name is
  not completely absent from the codebase outside its own definition. This
  is still a real, useful, cheap signal (it correctly found 6 genuinely
  zero-reference RPCs — see EXEMPT_RPCS), but it is NOT proof of a real
  caller, and no test here claims otherwise.
* **Tier 2 — hand-verified exact call sites**, below, for the specific
  highest-consequence class this ticket is about: market/pool discovery-and-
  verification RPCs, where an unreachable one directly blocks safely
  promoting a discovered candidate into a strategy config. Each assertion
  names the EXACT file (and, where the path is multi-hop, every hop), hand-
  verified rather than inferred — the same "reviewed, exact claim" discipline
  `qa_lab/qa_protocol.py`'s obligation `runner=`/`source=` fields
  already use elsewhere in this codebase for this identical class of
  problem. When one of these RPCs' real call site moves, update the
  assertion to the new exact site — never delete it to make the test pass.

**When Tier 1 fails**: either (a) you added a caller for an exempted RPC —
remove it from EXEMPT_RPCS, this is a happy failure; or (b) you added a new
RPC with no reference anywhere yet — wire a caller, or add it to
EXEMPT_RPCS with a real reason and a tracking ticket.

**When a Tier 2 assertion fails**: the exact call site named no longer calls
that RPC. Either you moved it (update the assertion to the new exact site)
or you genuinely orphaned a promotion/verification capability (fix it before
merging — this is the ALM-3474 defect class by definition).
"""

from __future__ import annotations

import subprocess
from functools import cache
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

# Directories excluded from the Tier 1 "is this RPC referenced anywhere" scan:
# - gateway/proto: generated stub/message code; trivially references every
#   RPC name (it IS the RPC's definition).
# - gateway/services: the gateway's OWN servicer implementations; trivially
#   references its own method name in `async def <RpcName>(...)`.
# - tests: a test double referencing the name is not a production caller —
#   this must not be satisfiable by writing a test for its own sake.
_EXCLUDED_PREFIXES = ("almanak/gateway/proto", "almanak/gateway/services", "tests")

# RPCs with NO reference anywhere in almanak/ outside their own gateway
# definition (Tier 1). Each entry is a genuine, honest "unresolved" — NOT a
# confirmed-safe classification. Do not add a reassuring-sounding reason
# unless it has actually been verified with whoever owns that service; an
# honest "needs triage" is safer than a guessed "this is fine".
EXEMPT_RPCS: dict[str, str] = {
    # LifecycleService "agent state and command management (V2 deployment)".
    # WriteState/ReadCommand/AckCommand ARE called from within almanak/ (the
    # runner's lifecycle-writer path); these two are not. Plausible this pair
    # is the OTHER half of a control-plane whose caller lives in a hosted
    # platform repo outside almanak-sdk, not a stranded SDK capability — but
    # that has not been confirmed. NEEDS TRIAGE by whoever owns LifecycleService.
    "ReadState": "found unreachable 2026-09-02 (ALM-3474 audit) — needs owner triage, ALM-3490",
    "WriteCommand": "found unreachable 2026-09-02 (ALM-3474 audit) — needs owner triage, ALM-3490",
    # SimulationService "transaction simulation via Alchemy/Tenderly" — sounds
    # safety-adjacent (pre-flight bundle simulation), and is: traced by hand
    # to confirm the framework's real execution pipeline's SIMULATE phase
    # (orchestrator.py _phase_simulate) is a no-op unless simulation_enabled,
    # which defaults False on every live network.
    "SimulateBundle": "confirmed unreachable, ticketed ALM-3488 (2026-09-02) — mainnet execution never simulates by default",
    # TokenService: ResolveToken / GetTokenMetadata ARE called from within
    # almanak/; these two "lightweight"/"batch" siblings are not — looks like
    # unused convenience endpoints, but not confirmed dead vs. planned.
    "GetTokenDecimals": "found unreachable 2026-09-02 (ALM-3474 audit) — needs owner triage, ALM-3490",
    "BatchResolveTokens": "found unreachable 2026-09-02 (ALM-3474 audit) — needs owner triage, ALM-3490",
    # RpcService.Call (singular) IS called from 23+ files; the batch sibling
    # has no caller anywhere.
    "BatchCall": "found unreachable 2026-09-02 (ALM-3474 audit) — needs owner triage, ALM-3490",
    # IntegrationService.GetWalletPortfolio IS called; this sibling
    # ("External portfolio APIs (Zerion, future providers)") is not.
    "GetWalletPositions": "found unreachable 2026-09-02 (ALM-3474 audit) — needs owner triage, ALM-3490",
}

# NOT part of EXEMPT_RPCS above — Tier 1 legitimately finds a reference for
# `ListLendingMarkets` (it is NOT zero-reference) and would flag it as a
# "stale exemption" if listed there. The reference is real but dead: hand-
# traced to `MarketSnapshot.lending_markets()`
# (almanak/framework/market/snapshot.py) — a wrapper that itself has ZERO
# callers anywhere in almanak/ (confirmed via `grep -rn '\.lending_markets('`).
# `ax lending-reserves` (the CLI command that LOOKS like the consumer) does
# not call this RPC at all: it reads each connector's own curated catalog
# directly in-process instead — the same defect class as GetLendingMarket's
# original gap, on the discovery/list side rather than the verify side.
# This is exactly the gap Tier 1 alone cannot see — see the module docstring
# and TestListLendingMarketsIsAKnownGap below, which asserts this KNOWN-BROKEN
# state explicitly rather than leaving it as a comment that could silently rot.


def _all_gateway_rpcs() -> dict[str, list[str]]:
    """Map RPC method name -> the service(s) that declare it, via reflection
    over the generated `*Servicer` base classes — robust to proto text
    changes and automatically covers any service added in the future."""
    from almanak.gateway.proto import gateway_pb2_grpc as g

    rpcs: dict[str, list[str]] = {}
    for name in dir(g):
        if not name.endswith("Servicer"):
            continue
        cls = getattr(g, name)
        for method in dir(cls):
            if not method.startswith("_"):
                rpcs.setdefault(method, []).append(name)
    return rpcs


@cache
def _has_any_reference_outside_gateway_definition(rpc_name: str) -> bool:
    """Tier 1: fast, flat, whole-repo textual scan. Proves only "not entirely
    absent" — see the module docstring for why this is NOT proof of a real
    caller, and why Tier 2 exists for the RPCs where that distinction
    actually matters.

    Cached: this module's several tests each scan the same ~150 RPC names
    against a filesystem that does not change within one test run.
    """
    result = subprocess.run(
        ["grep", "-rl", "-w", rpc_name, "almanak/", "--include=*.py"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )
    files = result.stdout.splitlines()
    return any(not f.startswith(_EXCLUDED_PREFIXES) for f in files)


def _file_contains(rel_path: str, needle: str) -> bool:
    path = REPO_ROOT / rel_path
    if not path.is_file():
        return False
    return needle in path.read_text(errors="ignore")


class TestGatewayRpcReferenceExists:
    """Tier 1 — see module docstring. A cheap dead-code detector, not a
    reachability proof."""

    def test_every_rpc_has_some_reference_or_is_explicitly_exempted(self) -> None:
        all_rpcs = _all_gateway_rpcs()
        assert len(all_rpcs) > 100, (
            f"expected 100+ gateway RPCs across all services, found {len(all_rpcs)} — "
            "the reflection probably broke, not the gateway shrinking"
        )

        newly_orphaned = [
            name
            for name in sorted(all_rpcs)
            if name not in EXEMPT_RPCS and not _has_any_reference_outside_gateway_definition(name)
        ]
        assert not newly_orphaned, (
            "The following gateway RPC(s) have NO reference anywhere in almanak/ "
            "outside their own proto/servicer definition — this is the exact "
            "ALM-3474 defect class (a capability with no way to reach it). "
            "Either wire a caller (CLI command, agent tool, or connector/"
            "framework usage) before merging, or add to EXEMPT_RPCS above with "
            f"a real reason and a tracking ticket: {newly_orphaned}"
        )

    def test_exemption_list_has_no_stale_entries(self) -> None:
        """The exemption list must track REAL current gaps, not accumulate
        forever. An RPC that gained a reference must be removed from
        EXEMPT_RPCS — otherwise a future regression (that reference being
        deleted) would silently re-orphan it with no signal, because the
        list already "explains it away". This is the enforcement half of
        that discipline."""
        stale = [name for name in EXEMPT_RPCS if _has_any_reference_outside_gateway_definition(name)]
        assert not stale, (
            f"These RPCs are listed in EXEMPT_RPCS but now have a reference — "
            f"remove them from the exemption list (the gap may be resolved, but "
            f"re-verify with a real caller, not just a new textual mention): {stale}"
        )

    def test_exempted_rpcs_are_still_declared_by_some_service(self) -> None:
        """Catches the inverse drift: an RPC removed from the proto entirely
        should have its exemption entry removed too, not left as dead
        config that silently stops meaning anything."""
        all_rpcs = _all_gateway_rpcs()
        vanished = [name for name in EXEMPT_RPCS if name not in all_rpcs]
        assert not vanished, (
            f"These exempted RPC names no longer exist in any gateway service — "
            f"remove their stale entries from EXEMPT_RPCS: {vanished}"
        )


class TestListLendingMarketsIsAKnownGap:
    """ALM-3489: ListLendingMarkets passes Tier 1 (it has a reference —
    MarketSnapshot.lending_markets()) but that wrapper itself has no caller
    anywhere, so there is no real Tier 2 call site to pin yet. This asserts
    the KNOWN-BROKEN state explicitly rather than leaving it as a comment
    that could silently stop being true without anyone noticing.

    Deliberately does NOT reuse ``_has_any_reference_outside_gateway_definition``
    here: a bare-word scan (or a per-file/per-directory exclusion) both
    produce false positives on this symbol — sibling modules in the same
    package (``models.py``, ``errors.py``) mention ``lending_markets`` in
    docstrings with no leading dot, and excluding by file/directory would
    hide a real caller landing elsewhere in that same package. Searching for
    the call-syntax pattern ``.lending_markets(`` instead of the bare word
    sidesteps both: no docstring here writes it with a leading dot, and no
    directory needs excluding because the wrapper's own ``def
    lending_markets(`` definition line doesn't match the pattern either.
    """

    def test_lending_markets_wrapper_still_has_no_caller(self) -> None:
        result = subprocess.run(
            ["grep", "-rn", r"\.lending_markets(", "almanak/", "--include=*.py"],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            check=False,
        )
        callers = [line for line in result.stdout.splitlines() if not line.startswith("tests/")]
        assert not callers, (
            f"MarketSnapshot.lending_markets() (the only current caller of the "
            f"ListLendingMarkets RPC) now has a caller — ALM-3489 may be resolved. "
            f"Verify it's a REAL caller (not another dead wrapper), then replace this "
            f"test with a Tier 2 assertion in "
            f"TestMarketDiscoveryAndVerificationRpcsHaveAVerifiedRealCaller naming the "
            f"exact new call site, and close ALM-3489. Found: {callers}"
        )


class TestMarketDiscoveryAndVerificationRpcsHaveAVerifiedRealCaller:
    """Tier 2 — hand-verified exact call sites for the ALM-3474 defect class:
    market/pool discovery-and-verification RPCs, where an unreachable one
    directly blocks safely promoting a discovered candidate into a strategy
    config. Each site below was read and confirmed by hand, not inferred by
    a generic heuristic. See the module docstring for why a
    generic mechanism was tried and rejected for this specific claim."""

    def test_get_lending_market_is_called_from_the_ax_cli(self) -> None:
        assert _file_contains("almanak/framework/cli/ax.py", "stub.GetLendingMarket("), (
            "GetLendingMarket (ALM-3474's own RPC) must be called from `ax lending-market` "
            "in almanak/framework/cli/ax.py — if this moved, update this assertion to the "
            "new exact call site; if it's gone, the ALM-3474 fix regressed"
        )

    def test_get_perp_market_is_called_from_the_ax_cli(self) -> None:
        assert _file_contains("almanak/framework/cli/ax.py", "stub.GetPerpMarket("), (
            "GetPerpMarket must be called from `ax perp-market` in almanak/framework/cli/ax.py"
        )

    def test_lookup_v4_pool_key_reaches_a_registered_runner_hook(self) -> None:
        """Multi-hop, but each hop hand-verified rather than auto-traced:
        LookupV4PoolKey -> gateway_pool_key_client.lookup_v4_pool_key() ->
        make_sync_pool_key_lookup() -> UniswapV4RunnerHookConnector
        .build_pool_key_lookup(), which the strategy runner's capability
        registry invokes for every Uniswap V4 strategy at receipt-parse time
        (RunnerPoolKeyLookupCapability). Unlike GetLendingMarket/
        ListLendingMarkets, this one's wrapper genuinely IS called — this
        test pins that chain so a break at ANY hop is caught, not just the
        first one."""
        client_file = "almanak/connectors/uniswap_v4/gateway_pool_key_client.py"
        hooks_file = "almanak/connectors/uniswap_v4/runner_hooks.py"
        assert _file_contains(client_file, "client.market.LookupV4PoolKey"), (
            f"LookupV4PoolKey must be called from {client_file}'s lookup_v4_pool_key()"
        )
        assert _file_contains(client_file, "def make_sync_pool_key_lookup"), (
            f"{client_file} must still define make_sync_pool_key_lookup — the sync bridge "
            "the runner hook below depends on"
        )
        assert _file_contains(hooks_file, "make_sync_pool_key_lookup"), (
            f"{hooks_file}'s UniswapV4RunnerHookConnector.build_pool_key_lookup must still "
            "call make_sync_pool_key_lookup — this is the registered runner-hook capability "
            "that makes LookupV4PoolKey reachable in production (invoked automatically for "
            "every Uniswap V4 strategy at receipt-parse time), not a CLI/agent-tool entrypoint"
        )
