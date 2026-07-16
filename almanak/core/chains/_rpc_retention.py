"""Measured historical-state retention of the default public RPC endpoints.

VIB-5869 / ALM-2695.

Why this file exists
--------------------
``RpcProfile.fork_requires_archive`` decides whether a managed-Anvil fork
refuses to start without an archive-capable RPC. Before this module its
membership was **folklore**: chains were added one at a time, each time
someone burned an afternoon on a `missing trie node` (VIB-646 covered three
chains, VIB-3971 / VIB-3973 added two more). Nobody ever measured the
endpoints, so the set ended up inverted relative to real exposure —
Ethereum, whose public RPC serves the *longest* window, was flagged, while
BSC and Arbitrum, the *shortest*, were not.

This table is the measurement. It is the source of truth for flag
membership, and
:func:`tests.unit.core.test_rpc_retention.TestMeasurementDrivesMembership.test_measured_fork_threatening_chains_are_all_flagged`
fails the build if a descriptor drifts away from it.

The failure mode being guarded
------------------------------
Anvil forks pin at a block and fetch state **lazily**. A pinned block stays
valid only while the upstream RPC still serves state at that height. Once
the chain head advances past the endpoint's retention window, every
*uncached* read fails **permanently** — the fork cannot recover, and no
retry helps. This is why the guard must fail at *start*: by the time the
first cold read fails, the run is already unrecoverable.

Method (reproduce with ``scripts/measure_rpc_state_retention.py``)
-----------------------------------------------------------------
Retention is measured **by age against the live head**, never against a
pinned start block, and never in absolute block numbers:

* Probe ``eth_call`` (Multicall3 ``getCurrentBlockTimestamp``) at
  ``head - K``, **re-reading ``head`` on every probe**. Pinning a start
  block lets the chain advance under the probe: on a 0.25s chain a 40s
  bisect silently adds ~160 blocks of phantom depth, which makes fast
  chains look far worse than they are. An early cut of this measurement
  had exactly that bug.
* After every failure, re-probe ``head - 0``. If depth 0 still answers,
  the deep failure is a real boundary; if it also fails, we were merely
  rate-limited and the result is **inconclusive**, not a boundary. Free
  endpoints rate-limit aggressively, and a rate limit is trivially
  mistaken for a retention wall.
* Report ``retention_blocks``; seconds are *derived* (``blocks ×
  block_time``) and are the operationally meaningful figure, because a
  fork wedges after a wall-clock duration, not after a block count.

Two distinct upstream behaviours both mean "not archive":

* ``missing trie node`` / ``header not found`` — genuine state pruning
  (avalanche, zerog).
* ``HTTP 403`` on historical depth while ``latest`` still answers —
  publicnode's free tier *gates* archive-depth requests. Confirmed
  depth-dependent (not a rate limit) by the depth-0 re-probe above.

Both wedge a fork identically, so both count as non-archive.

Cross-checked against ground truth
----------------------------------
The RPC probe predicts a window; a real ``anvil --fork-url <public rpc>`` then
confirms it by wedging. Predicted vs observed first-failure, 2026-07-16:
arbitrum ~16s / +25s, bsc ~48s / +45s, optimism ~128s / +201s, base ~128s /
+202s. Every failure was durable — repeated reads at +90s and +150s kept
failing, which is the direct refutation of "just retry the test". The probe
runs slightly pessimistic (the fork block already trails the head at start),
so the recorded windows are a lower bound, which is the safe direction for a
gate.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

MEASURED_ON = "2026-07-16"
"""ISO date of the measurement run below. Re-measure when stale: endpoints
change tiers without notice (x-layer already moved — see its entry)."""

SAFE_FORK_WINDOW_SECONDS = 86_400.0
"""How long a managed-Anvil fork must be able to survive before its upstream
endpoint counts as safe: 24h of wall-clock.

Rationale: strategies are long-lived processes, so the bar is "outlives a
run", not "survives startup". Every measured public endpoint is orders of
magnitude on one side of this line or the other — the shortest safe chain
(Monad, ~11.5 days) and the longest unsafe one (Ethereum, ~19 minutes) differ
by ~870x — so the exact threshold is not load-bearing and no chain sits near
enough to flip on measurement noise."""


@dataclass(frozen=True)
class RetentionMeasurement:
    """One measured endpoint.

    Attributes:
        endpoint: The exact URL probed. A measurement is only evidence about
            the endpoint that was probed, never about "the chain".
        archive: ``True`` = served state at the deepest probe (archive).
            ``False`` = a real, re-probe-confirmed retention boundary.
            ``None`` = **unmeasured / inconclusive** — never conflate with
            ``False`` (Empty != Zero; AGENTS.md §Accounting states the same
            discipline for measured values).
        retention_blocks: Deepest depth (in blocks, relative to the live
            head) that still served state. ``None`` when ``archive`` is not
            ``False``.
        window_seconds: ``retention_blocks × block_time``, i.e. how long a
            fork survives in wall-clock terms. ``None`` when the chain
            declares no block time or no boundary was found.
        evidence: The upstream error observed at the boundary — the receipt
            for the ``archive=False`` verdict.
    """

    endpoint: str
    archive: bool | None
    retention_blocks: int | None
    window_seconds: float | None
    evidence: str

    def threatens_fork(self) -> bool:
        """Whether a managed-Anvil fork on this endpoint would wedge in
        practice — the question the ``fork_requires_archive`` gate answers.

        "Not archive" is **not** the same as "unsafe": Monad serves ~11.5
        days of state, which outlives any run, so gating it would be pure
        false-positive friction. The predicate is therefore a *threshold*,
        not the raw ``archive`` bit.
        """
        if self.archive is not False:
            # Archive (safe) or unmeasured (no evidence to gate on — an
            # unmeasured endpoint must not silently become a hard failure).
            return False
        if self.window_seconds is None:
            # Measured pruned but the chain declares no block time, so the
            # window cannot be expressed in wall-clock. Fail safe: a measured
            # boundary with an unknown horizon is treated as threatening.
            return True
        return self.window_seconds < SAFE_FORK_WINDOW_SECONDS


# ── The measurement (2026-07-16) ───────────────────────────────────────────
#
# Ranked by exposure — shortest survival first. Note the shape: every
# publicnode endpoint gates at a broadly similar *block* depth, so the
# wall-clock window is essentially ``depth × block_time``. That is why the
# fast chains are the dangerous ones and why the pre-VIB-5869 flag set
# (ethereum flagged, arbitrum not) was exactly backwards.
PUBLIC_RPC_RETENTION: Mapping[str, RetentionMeasurement] = MappingProxyType(
    {
        "arbitrum": RetentionMeasurement(
            endpoint="https://arbitrum-one-rpc.publicnode.com",
            archive=False,
            retention_blocks=64,
            window_seconds=16.0,  # 64 × 0.25s — worst exposure of any chain
            evidence="anvil fork wedged at t=+25s, durably (also: HTTP 403 at historical depth)",
        ),
        "bsc": RetentionMeasurement(
            endpoint="https://bsc-rpc.publicnode.com",
            archive=False,
            retention_blocks=64,
            window_seconds=48.0,  # 64 × 0.75s — ALM-2695
            evidence="anvil fork wedged at t=+45s, durably (also: HTTP 403 at historical depth)",
        ),
        "avalanche": RetentionMeasurement(
            endpoint="https://avalanche-c-chain-rpc.publicnode.com",
            archive=False,
            retention_blocks=8,
            window_seconds=16.0,  # 8 × 1.9s — coreth keeps a very short window
            evidence="missing trie node (genuine state pruning)",
        ),
        "polygon": RetentionMeasurement(
            endpoint="https://polygon-bor-rpc.publicnode.com",
            archive=False,
            retention_blocks=64,
            window_seconds=134.0,
            evidence="HTTP 403 at historical depth while latest still served",
        ),
        "sonic": RetentionMeasurement(
            endpoint="https://sonic-rpc.publicnode.com",
            archive=False,
            retention_blocks=64,
            window_seconds=None,  # chain declares no block_time_seconds
            evidence="HTTP 403 at historical depth while latest still served",
        ),
        "linea": RetentionMeasurement(
            endpoint="https://linea-rpc.publicnode.com",
            archive=False,
            retention_blocks=64,
            window_seconds=None,  # chain declares no block_time_seconds
            evidence="HTTP 403 at historical depth while latest still served",
        ),
        "base": RetentionMeasurement(
            endpoint="https://base-rpc.publicnode.com",
            archive=False,
            retention_blocks=64,
            window_seconds=128.0,
            evidence="anvil fork wedged at t=+202s, durably (also: HTTP 403 at historical depth)",
        ),
        "optimism": RetentionMeasurement(
            endpoint="https://optimism-rpc.publicnode.com",
            archive=False,
            retention_blocks=64,
            window_seconds=128.0,
            evidence="anvil fork wedged at t=+201s, durably (also: HTTP 403 at historical depth)",
        ),
        "zerog": RetentionMeasurement(
            endpoint="https://rpc.ankr.com/0g_mainnet_evm",
            archive=False,
            retention_blocks=116,
            window_seconds=108.0,
            evidence="missing trie node (genuine state pruning) — matches VIB-3971",
        ),
        "ethereum": RetentionMeasurement(
            endpoint="https://ethereum-rpc.publicnode.com",
            archive=False,
            retention_blocks=97,
            window_seconds=1164.0,  # longest window of any chain, yet the
            # only one anyone thought to flag before VIB-5869
            evidence="HTTP 403 at historical depth while latest still served",
        ),
        # ── Archive-capable: no flag needed ────────────────────────────────
        "mantle": RetentionMeasurement(
            endpoint="https://rpc.mantle.xyz",
            archive=True,
            retention_blocks=None,
            window_seconds=None,
            evidence="served state at head-6,000,000",
        ),
        "plasma": RetentionMeasurement(
            endpoint="https://rpc.plasma.to",
            archive=True,
            retention_blocks=None,
            window_seconds=None,
            evidence="served state at head-6,000,000",
        ),
        "robinhood": RetentionMeasurement(
            endpoint="https://rpc.mainnet.chain.robinhood.com",
            archive=True,
            retention_blocks=None,
            window_seconds=None,
            evidence="served state at head-6,000,000",
        ),
        "monad": RetentionMeasurement(
            endpoint="https://rpc.monad.xyz",
            archive=False,
            retention_blocks=2_491_624,
            window_seconds=998_952.0,  # ~11.5 days — deep enough not to gate
            evidence="'Block requested not found' beyond ~2.49M blocks",
        ),
        # x-layer measured ARCHIVE on 2026-07-16 (served head-6,000,000), which
        # CONTRADICTS the VIB-3971 / VIB-3973 incident that added its flag.
        # Deliberately NOT un-flagged: one green probe is not grounds to drop a
        # guard added after a real production stall (the endpoint may have been
        # re-tiered, or the incident may have been rate-limit-shaped). The
        # subset invariant below permits this conservative override; dropping
        # the flag needs a human and a re-run of the original repro.
        "xlayer": RetentionMeasurement(
            endpoint="https://rpc.xlayer.tech",
            archive=True,
            retention_blocks=None,
            window_seconds=None,
            evidence="served state at head-6,000,000 — CONTRADICTS VIB-3971; flag kept deliberately",
        ),
        # hyperevm's endpoint rejected `eth_getBlockByNumber(latest)` with
        # 'invalid block height' during the run — genuinely unmeasured, so it
        # stays None rather than being guessed either way.
        "hyperevm": RetentionMeasurement(
            endpoint="https://rpc.hyperliquid.xyz/evm",
            archive=None,
            retention_blocks=None,
            window_seconds=None,
            evidence="UNMEASURED: endpoint rejected latest-block read during the probe run",
        ),
    }
)


def measured_non_archive_chains() -> frozenset[str]:
    """Chains whose default public RPC is **measured** to lack archive state.

    Strictly ``archive is False`` — an unmeasured/inconclusive endpoint
    (``archive is None``) is never reported as pruned. Note this includes
    endpoints that are pruned but harmless (Monad); use
    :func:`measured_fork_threatening_chains` for the gate.
    """
    return frozenset(name for name, m in PUBLIC_RPC_RETENTION.items() if m.archive is False)


def measured_fork_threatening_chains() -> frozenset[str]:
    """Chains whose default public RPC is measured to wedge a fork.

    This is the **lower bound** on ``fork_requires_archive`` membership: any
    chain here MUST be flagged (enforced by
    ``tests/unit/core/test_rpc_retention.py``). The flag set may be a strict
    superset — a chain flagged by a human after a production incident stays
    flagged even if a later probe disagrees (see the x-layer entry). The
    invariant is deliberately one-directional: measurement can *tighten* the
    guard, never loosen it.
    """
    return frozenset(name for name, m in PUBLIC_RPC_RETENTION.items() if m.threatens_fork())


__all__ = [
    "MEASURED_ON",
    "PUBLIC_RPC_RETENTION",
    "SAFE_FORK_WINDOW_SECONDS",
    "RetentionMeasurement",
    "measured_fork_threatening_chains",
    "measured_non_archive_chains",
]
