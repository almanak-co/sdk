"""LP Position Tracker — framework-default auto-capture of LP position metadata.

VIB-3742 — Framework hardening to prevent silent LP teardown leaks.

Background
----------
TraderJoe V2 LP_CLOSE intents that omit ``protocol_params["bin_ids"]`` cause
the compiler to fall back to a heuristic ±50 bin scan around the *current*
active_id. After price drift the original bins may sit outside that window
and ``removeLiquidity`` closes only a subset — leaving liquidity stranded
on-chain while the framework reports success. (See
``connectors/traderjoe_v2/compiler.py`` and ``docs/internal/blueprints/05-connectors.md``.)

Of 17 TJ V2 LP strategies in this repo, 13 forgot to capture / pass
``bin_ids``. That is a framework UX failure, not a strategy-author failure.

Design
------
``LPPositionTracker`` is a small, opt-out component owned by ``IntentStrategy``
that:

1. Records position metadata from successful LP_OPEN ``ExecutionResult`` objects,
   keyed by ``(protocol, chain, normalised_pool)``. For TraderJoe V2 it records
   ``bin_ids``; for Uniswap V3 / V4 / SushiSwap V3 / PancakeSwap V3 / Slipstream
   it records the NFT ``position_id`` (so the same hook generalises to other
   CL-NFT protocols, even though VIB-3742 only mandates the TJ V2 path).
2. Auto-injects the recorded metadata into LP_CLOSE / LP_COLLECT_FEES intents
   the strategy returns from ``decide()``, when the strategy did not already
   supply it via ``protocol_params``.
3. Round-trips its state through ``IntentStrategy.get_persistent_state`` /
   ``load_persistent_state`` under a reserved framework key so a strategy that
   overrides those methods does not lose tracker state.
4. Is a no-op for protocols it does not recognise. Strategies that already
   track manually (``traderjoe_lp_lifecycle``, ``traderjoe_fee_rotator``,
   etc.) continue to work because manual ``protocol_params`` always wins —
   the tracker only fills missing data, never overwrites.

Hard constraints
----------------
- Pure in-memory data structure. No network calls, no gateway calls.
  All on-chain data needed to populate the tracker arrives via the existing
  ``ExecutionResult`` enrichment pipeline (``ResultEnricher`` + per-protocol
  ``ReceiptParser``). The 1:1 strategy:gateway identity model is unaffected.
- No emojis.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# Reserved key in ``IntentStrategy.get_persistent_state()`` for tracker data.
# The double-underscore prefix signals "framework-owned, do not mutate from
# strategy code." Any future framework-owned persistence keys should follow
# the ``__framework_*__`` convention so user state and framework state never
# collide.
PERSISTENT_STATE_KEY = "__framework_lp_position_tracker__"


# Protocols whose LP_OPEN result carries a ``bin_ids`` array (Liquidity Book
# style — fungible ERC1155-like LP tokens, not an NFT). Adding a new such
# protocol here is the entire integration cost.
_BIN_BASED_PROTOCOLS: frozenset[str] = frozenset(
    {
        "traderjoe_v2",
    }
)

# Protocols whose LP_OPEN result carries an NFT ``position_id``. Listed for
# the future-generalisation hook (item 2 of VIB-3742); not mandatory for the
# acceptance criteria.
_NFT_BASED_PROTOCOLS: frozenset[str] = frozenset(
    {
        "uniswap_v3",
        "uniswap_v4",
        "sushiswap_v3",
        "pancakeswap_v3",
        "aerodrome_slipstream",
        "velodrome_slipstream",
    }
)

# VIB-5346 — Fail-closed allowlist: connectors whose LP_CLOSE ``position_id`` IS
# a fungible LP-token wei amount, so ``amount="all"`` chaining can resolve the
# prior LP_OPEN's minted-LP wei into it. A connector must OPT IN here; anything
# not listed is rejected (NFT token-ids, bin-ids, pool addresses, uncategorised
# — all unsafe). This is the single source of truth for the runner-level
# capability gate that fails closed BEFORE the minted wei is resolved into
# ``position_id`` (see ``StrategyRunner._resolve_chained_amount_for_intent``).
_FUNGIBLE_LP_CHAINING_PROTOCOLS: frozenset[str] = frozenset({"pendle"})


def lp_close_amount_chaining_supported(protocol: str | None) -> bool:
    """True iff the protocol's LP_CLOSE ``position_id`` is a fungible LP-token
    wei amount (so ``amount="all"`` chaining is safe). Fail-closed allowlist:
    any protocol not explicitly listed in ``_FUNGIBLE_LP_CHAINING_PROTOCOLS``
    (including ``None``) returns ``False``."""
    return protocol in _FUNGIBLE_LP_CHAINING_PROTOCOLS


@dataclass(frozen=True)
class _PositionKey:
    """Identity for an open position tracked by the framework.

    Tracker keys positions by ``(protocol, chain, pool)``. ``pool`` is a
    case-insensitive string matched against ``intent.pool`` for both the
    open and close intents — this is the symmetry the framework relies on
    to pair a recorded LP_OPEN with a later LP_CLOSE.
    """

    protocol: str
    chain: str
    pool: str

    @classmethod
    def from_intent(cls, intent: Any, default_chain: str | None = None) -> _PositionKey | None:
        """Build a key from any intent that carries (protocol, chain, pool).

        Returns ``None`` if any required field is missing — the caller treats
        that as "tracker can't help with this intent" and moves on.
        """
        protocol = getattr(intent, "protocol", None)
        if not protocol:
            return None
        chain = getattr(intent, "chain", None) or default_chain
        if not chain:
            return None
        pool = getattr(intent, "pool", None)
        if not pool:
            return None
        return cls(
            protocol=str(protocol).lower(),
            chain=str(chain).lower(),
            pool=str(pool).strip().lower(),
        )


@dataclass
class _TrackedPosition:
    """Metadata captured for a single open position."""

    bin_ids: list[int] = field(default_factory=list)
    position_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.bin_ids:
            out["bin_ids"] = list(self.bin_ids)
        if self.position_id is not None:
            out["position_id"] = self.position_id
        return out

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> _TrackedPosition:
        bin_ids_raw = data.get("bin_ids") or []
        try:
            bin_ids = [int(b) for b in bin_ids_raw]
        except (TypeError, ValueError):
            bin_ids = []
        position_id_raw = data.get("position_id")
        position_id = str(position_id_raw) if position_id_raw is not None else None
        return cls(bin_ids=bin_ids, position_id=position_id)


class LPPositionTracker:
    """Framework-default tracker for LP position metadata.

    Thread-safety: not thread-safe by design. Strategies run single-threaded
    inside the runner (``decide()`` and ``on_intent_executed`` execute on the
    runner's worker thread, never overlapping). If a future runner ever
    parallelises strategy callbacks, this class needs a lock — but introducing
    one preemptively would be premature optimisation.
    """

    def __init__(self) -> None:
        self._positions: dict[_PositionKey, _TrackedPosition] = {}
        # VIB-4198 / T12 — optional registry-lookup callback set by the
        # runner at boot. When present, ``maybe_inject`` for UniV3-family
        # protocols consults the registry FIRST and only falls back to
        # ``self._positions`` if the registry has no row. Closed-loop
        # behavior with the registry-mode atomic write site in the runner.
        # ``None`` (the default) preserves the legacy tracker-only path.
        self._registry_lookup: Any = None

    def attach_registry_lookup(self, lookup: Any) -> None:
        """Install a registry-aware lookup callback (VIB-4198 / T12).

        ``lookup`` is an awaitable / sync callable with signature
        ``(protocol: str, chain: str, pool: str) -> str | None`` returning
        the open NFT ``token_id`` for the (protocol, chain, pool) triple
        when the registry knows about it. Returning ``None`` falls back
        to the in-memory tracker.

        The runner installs this at boot once the cutover guard clears
        the LP cutover; the tracker stays in shadow mode and the registry
        is the live answer surface.
        """
        self._registry_lookup = lookup

    # ---------------------------------------------------------------------
    # Recording (called by the framework after a successful intent execution)
    # ---------------------------------------------------------------------

    def record_intent_execution(
        self,
        intent: Any,
        success: bool,
        result: Any,
        default_chain: str | None = None,
    ) -> None:
        """Inspect an executed intent and capture / clear position metadata.

        Behaviour:
        - LP_OPEN success on a known protocol: capture bin_ids / position_id.
        - LP_CLOSE success on a known protocol: clear the tracked position
          (so a later LP_OPEN on the same pool starts fresh).
        - Anything else: no-op.

        Errors are swallowed and logged at WARNING level. The tracker is a
        belt-and-suspenders helper; a tracker fault must never propagate
        into the runner's success path.
        """
        try:
            self._record_intent_execution(intent, success, result, default_chain)
        except Exception as exc:  # noqa: BLE001 — defensive
            logger.warning(
                "LPPositionTracker.record_intent_execution failed (non-fatal): %s",
                exc,
                exc_info=True,
            )

    def _record_intent_execution(
        self,
        intent: Any,
        success: bool,
        result: Any,
        default_chain: str | None,
    ) -> None:
        if not success or result is None:
            return
        intent_type = self._intent_type(intent)
        if intent_type not in {"LP_OPEN", "LP_CLOSE"}:
            return
        key = _PositionKey.from_intent(intent, default_chain=default_chain)
        if key is None:
            return

        if intent_type == "LP_CLOSE":
            # Forget any tracked metadata so the next LP_OPEN on the same pool
            # starts from a clean slate. This is correct even if the close
            # was partial — the strategy will have to re-open to know which
            # bins are live again.
            self._positions.pop(key, None)
            return

        # LP_OPEN: capture metadata if the protocol supports tracking.
        if key.protocol in _BIN_BASED_PROTOCOLS:
            bin_ids = self._extract_bin_ids(result)
            if bin_ids:
                tracked = self._positions.setdefault(key, _TrackedPosition())
                tracked.bin_ids = list(bin_ids)
                logger.info(
                    "LPPositionTracker captured %d bin_ids for %s on %s pool=%s",
                    len(bin_ids),
                    key.protocol,
                    key.chain,
                    key.pool,
                )
        elif key.protocol in _NFT_BASED_PROTOCOLS:
            position_id = self._extract_position_id(result)
            if position_id is not None:
                tracked = self._positions.setdefault(key, _TrackedPosition())
                tracked.position_id = str(position_id)
                logger.debug(
                    "LPPositionTracker captured position_id=%s for %s on %s pool=%s",
                    position_id,
                    key.protocol,
                    key.chain,
                    key.pool,
                )

    # ---------------------------------------------------------------------
    # Injection (called by the framework before LP_CLOSE / LP_COLLECT_FEES
    # intents are compiled)
    # ---------------------------------------------------------------------

    def maybe_inject(self, intent: Any, default_chain: str | None = None) -> Any:
        """Return ``intent`` with framework-tracked metadata filled in.

        - Only acts on LP_CLOSE and LP_COLLECT_FEES intents.
        - Never overwrites caller-supplied ``protocol_params`` keys.
        - Returns the same instance if no injection is needed.
        - Returns a model-validated copy if injection happens (intents are
          immutable Pydantic models).

        Errors are swallowed: a tracker fault must never block a strategy
        intent. The compiler still runs (and may emit its own warning).
        """
        try:
            return self._maybe_inject(intent, default_chain)
        except Exception as exc:  # noqa: BLE001 — defensive
            logger.warning(
                "LPPositionTracker.maybe_inject failed (non-fatal): %s",
                exc,
                exc_info=True,
            )
            return intent

    # crap-allowlist: VIB-4198 — LPPositionTracker is the shadow path; T29 (VIB-4215) removes it after a stable cycle per blueprint 28 §5; investing test coverage here is wasted effort.
    def _maybe_inject(self, intent: Any, default_chain: str | None) -> Any:
        intent_type = self._intent_type(intent)
        if intent_type not in {"LP_CLOSE", "LP_COLLECT_FEES"}:
            return intent
        key = _PositionKey.from_intent(intent, default_chain=default_chain)
        if key is None:
            return intent

        # VIB-4198 / T12 — registry-first lookup for UniV3-family NFT-based
        # protocols. When the runner installed a registry lookup callback
        # AND the registry knows the open token_id for this (protocol, chain,
        # pool), use that as the authoritative source. The in-memory
        # tracker stays as shadow per blueprint 28 §5.
        registry_position_id: str | None = None
        if key.protocol in _NFT_BASED_PROTOCOLS and self._registry_lookup is not None:
            registry_position_id = self._lookup_registry_position_id(
                protocol=key.protocol, chain=key.chain, pool=key.pool
            )

        tracked = self._positions.get(key)
        if tracked is None and registry_position_id is None:
            return intent

        existing = getattr(intent, "protocol_params", None) or {}
        new_params: dict[str, Any] = dict(existing)
        injected = False

        # bin_ids — only inject when caller did not supply (truthy).
        if key.protocol in _BIN_BASED_PROTOCOLS and tracked is not None and tracked.bin_ids:
            existing_bin_ids = new_params.get("bin_ids")
            if not existing_bin_ids:
                new_params["bin_ids"] = list(tracked.bin_ids)
                injected = True

        # NFT position_id — registry first, tracker fallback. Only inject
        # when caller did not supply (truthy).
        if key.protocol in _NFT_BASED_PROTOCOLS:
            existing_pid = new_params.get("position_id") or new_params.get("token_id")
            if not existing_pid:
                pid = registry_position_id
                if not pid and tracked is not None and tracked.position_id is not None:
                    pid = str(tracked.position_id)
                if pid:
                    new_params["position_id"] = pid
                    injected = True

        if not injected:
            return intent

        # Pydantic v2 immutable model: use model_copy(update=...) if available,
        # otherwise reconstruct via dump+validate. Both produce a new instance
        # without mutating the original.
        try:
            updated = intent.model_copy(update={"protocol_params": new_params})
        except AttributeError:
            try:
                data = intent.model_dump()
                data["protocol_params"] = new_params
                updated = type(intent).model_validate(data)
            except Exception:
                # Last resort: refuse to inject rather than corrupt the intent.
                return intent

        logger.info(
            "LPPositionTracker auto-injected protocol_params on %s for %s pool=%s "
            "(strategy did not supply bin_ids — captured at LP_OPEN time)",
            intent_type,
            key.protocol,
            key.pool,
        )
        return updated

    # ---------------------------------------------------------------------
    # Persistence
    # ---------------------------------------------------------------------

    def to_persistent_dict(self) -> dict[str, Any]:
        """Serialise to a JSON-friendly dict for ``get_persistent_state``."""
        out: dict[str, Any] = {}
        for key, tracked in self._positions.items():
            out[f"{key.protocol}|{key.chain}|{key.pool}"] = tracked.to_dict()
        return out

    def load_persistent_dict(self, data: dict[str, Any] | None) -> None:
        """Restore from ``load_persistent_state`` (best-effort, fail-safe)."""
        if not data:
            return
        for key_str, value in data.items():
            try:
                protocol, chain, pool = key_str.split("|", 2)
            except ValueError:
                logger.warning("LPPositionTracker: dropping malformed key %r", key_str)
                continue
            if not isinstance(value, dict):
                continue
            self._positions[_PositionKey(protocol=protocol, chain=chain, pool=pool)] = _TrackedPosition.from_dict(value)

    # ---------------------------------------------------------------------
    # Inspection helpers (used by tests + verify_closure hook)
    # ---------------------------------------------------------------------

    def known_positions(
        self, protocol: str | None = None, chain: str | None = None
    ) -> dict[_PositionKey, _TrackedPosition]:
        """Return a copy of currently tracked positions, optionally filtered."""
        out: dict[_PositionKey, _TrackedPosition] = {}
        for key, tracked in self._positions.items():
            if protocol is not None and key.protocol != protocol.lower():
                continue
            if chain is not None and key.chain != chain.lower():
                continue
            out[key] = tracked
        return out

    # ---------------------------------------------------------------------
    # Internal extraction helpers — tolerant of the various shapes
    # ``ExecutionResult`` / ``SimulatedExecutionResult`` use.
    # ---------------------------------------------------------------------

    @staticmethod
    def _intent_type(intent: Any) -> str | None:
        """Best-effort extraction of an intent's type string."""
        if intent is None:
            return None
        # Some intents expose .intent_type as Enum, others as str.
        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None and isinstance(intent, dict):
            intent_type = intent.get("intent_type") or intent.get("type")
        if intent_type is None:
            return None
        # Enum -> Enum.value
        value = getattr(intent_type, "value", intent_type)
        if value is None:
            return None
        return str(value).upper()

    def _lookup_registry_position_id(self, *, protocol: str, chain: str, pool: str) -> str | None:
        """VIB-4198 / T12 — registry-first lookup for the open NFT token_id.

        Calls the registry-lookup callback installed by the runner at
        boot (when the cutover guard cleared the LP cutover). Returns
        ``None`` on any error or miss — the caller falls back to the
        in-memory tracker.

        Errors are swallowed and logged at DEBUG level: a registry-lookup
        fault must never block a teardown intent. The tracker shadow path
        is the always-on fallback.
        """
        lookup = self._registry_lookup
        if lookup is None:
            return None
        try:
            value = lookup(protocol=protocol, chain=chain, pool=pool)
        except Exception as exc:  # noqa: BLE001 — defensive
            logger.debug(
                "LPPositionTracker._lookup_registry_position_id failed (non-fatal): %s",
                exc,
                exc_info=True,
            )
            return None
        # The registry lookup may return an awaitable; we cannot await
        # here because maybe_inject is sync. Caller is responsible for
        # installing a sync wrapper. Defensive type-check:
        if value is None:
            return None
        if not isinstance(value, str | int):
            return None
        return str(value)

    @staticmethod
    def _extract_bin_ids(result: Any) -> list[int] | None:
        """Pull bin_ids from either ``result.bin_ids`` or
        ``result.extracted_data['bin_ids']``. Returns a normalised list of
        ints, or None when not available.
        """
        if result is None:
            return None
        # Direct attribute
        bin_ids = getattr(result, "bin_ids", None)
        if not bin_ids:
            extracted = getattr(result, "extracted_data", None) or {}
            if isinstance(extracted, dict):
                bin_ids = extracted.get("bin_ids")
        if not bin_ids:
            return None
        try:
            return [int(b) for b in bin_ids]
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_position_id(result: Any) -> str | None:
        """Pull NFT ``position_id`` from a result object."""
        if result is None:
            return None
        position_id = getattr(result, "position_id", None)
        if position_id is None:
            extracted = getattr(result, "extracted_data", None) or {}
            if isinstance(extracted, dict):
                position_id = extracted.get("position_id")
        if position_id is None:
            return None
        return str(position_id)


__all__ = [
    "LPPositionTracker",
    "PERSISTENT_STATE_KEY",
]
