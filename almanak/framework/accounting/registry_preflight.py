"""Pre-execution LP registry-collision preflight (VIB-4614).

Closes incident S2: a second handle-less ``LP_OPEN`` into a pool that already
has an open auto-mode ``position_registry`` row mints a real NFT on-chain and
only fails *afterward* at registry persistence with
:class:`RegistryAutoCollisionError`, leaving an **orphan NFT** that no
accounting/registry row tracks.

This module builds the async callback the runner injects into
:class:`almanak.framework.execution.orchestrator.ExecutionOrchestrator`
(``registry_preflight=``). The orchestrator's ``_phase_registry_preflight``
phase — wedged between build and validate — calls it BEFORE any signing or
submission. The callback owns the StateManager read; the orchestrator never
imports a DB handle (layering boundary).

Design notes:

* **Auto-mode only (Option B in VIB-4614).** The ``ix_registry_auto_mode``
  partial unique index guards ``status = 'open' AND handle IS NULL``. A
  handle-supplied open is excluded from the index and must NOT be blocked —
  the callback returns ``None`` (allow) whenever the bundle metadata carries a
  non-empty ``registry_handle``. Handle-handle collisions remain a post-tx
  surprise (rare, operator-induced); escalate to full coverage only if
  operators report them.

* **Single-source predicate.** The actual lookup is
  :meth:`StateManager.find_open_auto_mode_registry_row`, which is the SAME
  SELECT the post-mint commit-path classifier runs (mirrors the index's
  ``WHERE`` clause). No key formats are inlined here — the
  ``semantic_grouping_key`` is built via :func:`semantic_grouping_key_univ3`
  (V3-shape LP families) or :func:`semantic_grouping_key_univ4` (V4
  singleton-PoolManager families), and the ``accounting_category`` via the
  taxonomy :func:`classify`, exactly as the commit path does.

* **V3 vs V4 dispatch mirrors the commit path (VIB-5582).** The runtime
  registry-commit dispatch (``strategy_runner._maybe_save_ledger_with_registry``)
  routes a protocol to the V4 identity/grouping stream
  (``_build_lp_v4_open_registry_row`` / ``semantic_grouping_key_univ4``,
  keyed on ``chain:pool_id``) iff ``protocol in _UNIV4_LP_PROTOCOLS``, and to
  the V3 stream (``semantic_grouping_key_univ3``, keyed on
  ``chain:pool_address``) otherwise. ``_pool_identity_key`` below applies the
  SAME membership test — imported from the SAME registry-derived
  ``UNIV4_LP_GROUPING_PROTOCOLS`` frozenset (capability-gated, not a
  hardcoded protocol-name literal per the chain/protocol coupling ratchet) —
  so the preflight can never disagree with the commit path about which
  identity shape a bundle uses. A V4 ``LP_OPEN`` compiles its ``pool_id``
  (keccak256 of the ABI-encoded ``PoolKey``) at COMPILE time — it is a pure
  function of already-resolved token addresses/fee/tickSpacing/hooks, no
  chain read needed — and carries it on the bundle metadata
  (``uniswap_v4/adapter.py::compile_lp_open_intent``) precisely so this
  preflight has an anchor to check BEFORE the mint, mirroring how the V3
  compiler carries ``metadata["pool"]``.

* **Fail-open on uncertainty.** The preflight is a *defensive early reject*,
  not the authoritative guard — the commit-path unique-index INSERT is the
  backstop (and the only thing that can close the concurrent-writer race the
  SELECT-then-INSERT preflight cannot). So: a backend without registry storage
  (hosted ``GatewayStateManager``), a non-LP bundle, missing pool/chain
  metadata, or any read error all resolve to "allow". The callback only blocks
  when it positively finds a colliding open row.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.framework.models.reproduction_bundle import ActionBundle
    from almanak.framework.state.state_manager import StateManager

logger = logging.getLogger(__name__)


def _is_auto_mode_lp_open(action_bundle: ActionBundle) -> bool:
    """True only for an LP_OPEN whose ``registry_handle`` is absent/empty.

    A handle-supplied open is excluded from ``ix_registry_auto_mode`` and must
    never be preflight-blocked. ``Empty ≠ Zero`` (CLAUDE.md): a missing key and
    an explicit ``None`` both mean auto-mode; only a non-empty string handle
    opts out.
    """
    if (action_bundle.intent_type or "").upper() != "LP_OPEN":
        return False
    handle = (action_bundle.metadata or {}).get("registry_handle")
    if handle is None:
        return True
    return not str(handle).strip()


def _pool_identity_key(*, chain: str, protocol: str, metadata: dict) -> str | None:
    """Compute the auto-mode ``semantic_grouping_key`` for an LP_OPEN bundle.

    Dispatches V3-shape vs V4-shape LP families with the SAME membership test
    the runtime registry-commit path uses
    (``strategy_runner._maybe_save_ledger_with_registry`` — ``protocol in
    _UNIV4_LP_PROTOCOLS`` routes to the V4 stream BEFORE the V3 gate), so the
    preflight can never pick the wrong grouping-key shape for a given
    protocol. Returns ``None`` when the required anchor is absent or
    malformed — Empty ≠ Zero: never guess a pool identity.
    """
    from almanak.framework.intents.compiler_constants import UNIV4_LP_GROUPING_PROTOCOLS
    from almanak.framework.migration import semantic_grouping_key_univ3, semantic_grouping_key_univ4

    protocol_norm = (protocol or "").strip().lower()
    if protocol_norm in UNIV4_LP_GROUPING_PROTOCOLS:
        pool_id = metadata.get("pool_id")
        if not pool_id:
            return None
        try:
            return semantic_grouping_key_univ4(chain=chain, pool_id=str(pool_id))
        except ValueError as exc:
            logger.debug("Registry preflight: could not build V4 semantic_grouping_key: %s", exc)
            return None

    pool = metadata.get("pool")
    if not pool:
        return None
    try:
        return semantic_grouping_key_univ3(chain=chain, pool_address=pool)
    except ValueError as exc:
        logger.debug("Registry preflight: could not build V3 semantic_grouping_key: %s", exc)
        return None


def build_registry_preflight_check(
    state_manager: StateManager,
    deployment_id: str,
):
    """Return the async ``registry_preflight`` callback for the orchestrator.

    The returned coroutine takes the ActionBundle about to be submitted and
    returns a human-readable rejection reason when a colliding open auto-mode
    registry row exists, or ``None`` to allow.

    Args:
        state_manager: The runner's StateManager (source of the registry read).
        deployment_id: The strategy's stable deployment id — the registry is
            scoped per deployment (1 gateway : 1 strategy).
    """

    async def _check(action_bundle: ActionBundle) -> str | None:
        if not _is_auto_mode_lp_open(action_bundle):
            return None

        metadata = action_bundle.metadata or {}
        chain = metadata.get("chain")
        protocol = metadata.get("protocol") or ""
        # Cannot compute the key without a chain anchor → cannot check → allow.
        # The commit-path classifier remains the backstop. (Empty ≠ Zero: we do
        # not guess a chain.)
        if not chain:
            return None

        # Build the predicate inputs the SAME way the commit path does — no
        # inlined key formats (VIB-4614 acceptance: single-source predicate;
        # VIB-5582 extends it to the V4 pool_id shape via `_pool_identity_key`).
        from almanak.framework.primitives.taxonomy import classify

        semantic_grouping_key = _pool_identity_key(chain=chain, protocol=protocol, metadata=metadata)
        if semantic_grouping_key is None:
            return None

        accounting_category = classify("LP_OPEN", protocol=protocol).value

        try:
            existing = await state_manager.find_open_auto_mode_registry_row(
                deployment_id=deployment_id,
                chain=chain,
                accounting_category=accounting_category,
                semantic_grouping_key=semantic_grouping_key,
            )
        except Exception as exc:
            # Backend without registry storage (CutoverStorageNotSupported on
            # hosted GatewayStateManager) or a transient read error → fail-open.
            # The commit-path unique-index INSERT is the authoritative backstop.
            logger.debug("Registry preflight read unavailable; allowing open: %s", exc)
            return None

        if existing is None:
            return None

        # Collision: an open handle-less row already occupies this semantic
        # group. Blocking here prevents the orphan NFT (incident S2). The
        # message mirrors RegistryAutoCollisionError's actionable shape so the
        # author knows to supply a registry_handle on the second open.
        existing_pih = existing.get("physical_identity_hash", "")
        existing_tx = existing.get("opened_tx", "")
        return (
            f"auto-mode LP_OPEN would collide with an open registry position "
            f"in the same group (accounting_category={accounting_category!r}, "
            f"semantic_grouping_key={semantic_grouping_key!r}). Existing: "
            f"physical_identity_hash={existing_pih!r} opened_tx={existing_tx!r}. "
            f"Supply a unique registry_handle on this LP_OPEN to coexist, or "
            f"close the existing position first. Rejected before minting an NFT "
            f"to avoid an orphan position."
        )

    return _check


__all__ = ["build_registry_preflight_check"]
