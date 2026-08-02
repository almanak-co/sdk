"""Hyperliquid teardown on-chain closure verifier (VIB-6387).

Before this hook, Hyperliquid PERP had **no** closure authority in teardown:
``get_teardown_post_condition("hyperliquid")`` returned ``None``, so
``plan_a_reconciliation._reconcile_one`` bailed out of its PERP branch at the
``supports_open_state_reconciliation`` check and returned ``UNVERIFIABLE``. Under
the VIB-6285 / W0.1 ratchet that is fatal: ``hyperliquid`` enters
``protocols_to_prove`` and can never enter ``measured_closed_protocols``, so
``closure_unknown`` forces ``success=False`` on a teardown that closed every
position. Measured on mainnet 2026-08-01 (``deployment:919d5bab4916``):
``positions_total=1, positions_closed=1, positions_failed=0`` and
``status=failed`` — *"UNMEASURED for protocol(s) hyperliquid"*. Token
consolidation is gated on that flag and ``mark_failed`` is terminal, so the
wallet was also left un-consolidated.

The absence was a **wiring gap, not a capability gap**: HyperCore's position
precompile (``0x0800``) is already read in production by ``perps_read.py`` and
``fill_reconciliation.py``. This hook points the same read at the teardown seam,
so the ratchet is satisfied by real evidence rather than waived.

**Position-scoped, not account-scoped** — the one structural difference from the
GMX hook. GMX reads ``getAccountPositions``, an account-level "all positions"
call, and can therefore assert "the wallet is flat". HyperCore has no such
precompile: ``position(wallet, assetIndex)`` answers for exactly one market, and
``perps_read`` covers the book only by looping the *seeded* majors. Reading the
position's own market is both cheaper and stricter than that loop — a position in
an unseeded market is unreadable by the loop but perfectly readable here — and it
avoids GMX's converse hazard, where an unrelated market's residual marks every
enumerated position open.

Closure rule is **exact** and three-valued (VIB-5573, Empty ≠ Zero):

* ``szi == 0`` decoded from a full-length struct is ``closed=True`` — a MEASURED
  flat. Confirmed live: the precompile returns the complete 5-word struct for a
  traded-then-closed asset *and* for one the account never traded, so a measured
  zero is the normal healthy answer, not an edge case.
* ``szi != 0`` is ``closed=False`` (→ FAILED) — the residual perp risk this seam
  exists to catch. Side is deliberately not compared: a position that flipped
  long↔short during teardown is still open risk.
* Everything else is ``unmeasured=True`` (→ UNVERIFIED, honest don't-know), never
  a residual — a gateway blip must not fabricate one and brick a healthy strategy
  through the VIB-5572 entry latch.

**The empty-return trap.** ``sdk.decode_position`` maps a zero-length return to
``Position(szi=0, …)``, documented as "no Core account / no position". Fed
straight into the rule above, a zero-byte blob would decode to a measured flat
and fabricate a closure proof off no chain data at all — precisely the
false-green ``ClosureCheckResult.not_applicable`` was introduced to stop. So the
blob is length-checked BEFORE decoding and a short return is unmeasured.

Gateway boundary: the single read goes through the supplied ``gateway_client``.
No provider is opened here; ``rpc_url`` is accepted for protocol parity and not
consumed. NEVER raises.

**Why ``.sdk`` is imported at function scope.** Teardown post-conditions are
registered eagerly, so ``almanak.gateway.server`` transitively imports this
module. ``.sdk`` is on the forbidden list in
``tests/gateway/test_imports_lean.py`` (``.adapter`` / ``.sdk`` / ``.compiler``
/ ``.receipt_parser`` / ``.client`` / ``.models``) — the gateway sidecar OOM'd
at its 512 Mi limit once before, and every connector ``__init__`` is PEP 562
lazy to keep it from happening again. A module-scope import here breaches that
boundary; ``fill_accounting.read_venue_leverage`` defers the same two names for
the same reason. The durable fix is to move the precompile READ helpers out of
a module whose own docstring says *"This is the WRITE path"* — tracked as
VIB-6395, deliberately not done here because ``compiler`` and ``perps_read``
consume them on a live money path.
"""

from __future__ import annotations

import logging
from typing import Any

from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult

from .addresses import PRECOMPILE_POSITION
from .markets import resolve_market

logger = logging.getLogger(__name__)

_PROTOCOL = "hyperliquid"

# The ``position`` precompile returns ``(int64 szi, uint64 entryNtl,
# int64 isolatedRawUsd, uint32 leverage, bool isIsolated)`` — five ABI words,
# 160 bytes. Anything shorter is not a struct this hook can trust, whatever
# ``decode_position`` would make of it (see the module docstring's empty-return
# trap). Verified live on 2026-08-02: a flat account still returns all 160 bytes.
_POSITION_STRUCT_BYTES = 160

# Order-queue residual kinds surfaced by the generic teardown residual discovery.
# This hook reads POSITIONS; it has no authority over HyperCore's pending-order
# queue, so it must not answer for one. Deliberately NOT ``not_applicable``: an
# unfilled CoreWriter order is squarely inside hyperliquid's domain and still
# holds risk, so the honest answer is doubt, not a waiver. Consistent with NOT
# declaring ``handles_pending_orders`` below.
_ORDER_KINDS = frozenset({"pending_order", "residual_unverified"})


def _result(closed: bool, position_id: str, *, unmeasured: bool = False, **residual: Any) -> ClosureCheckResult:
    return ClosureCheckResult(
        closed=closed,
        protocol=_PROTOCOL,
        position_id=position_id,
        residual={k: v for k, v in residual.items() if k != "error"},
        error=residual.get("error"),
        unmeasured=unmeasured,
    )


def _unverified(position_id: str, error: str) -> ClosureCheckResult:
    """A read fault: closure could not be measured (VIB-5573).

    Lowered to UNVERIFIED by the composition seam — NEVER FAILED, and never a
    residual: Empty ≠ Zero, so an unread market must not masquerade as one that
    was read and found open.
    """
    return _result(False, position_id, unmeasured=True, error=error)


def _not_applicable(position_id: str, reason: str) -> ClosureCheckResult:
    """This hook is structurally out of scope for the position it was handed.

    Contributes neither proof nor doubt (VIB-6285). Used only for a non-PERP
    position type: ``hyperliquid`` slugs are shared with any spot/token row a
    strategy on this venue surfaces, and a perp read has no opinion about those.
    """
    return ClosureCheckResult(
        closed=False,
        protocol=_PROTOCOL,
        position_id=position_id,
        residual={},
        error=reason,
        not_applicable=True,
    )


def _market_symbol(position: Any) -> str:
    """The market this position is on, as a symbol (``"ETH"``, ``"BTC-USD"``).

    Both producers agree on ``details["market"]``: the demo strategy's
    ``get_open_positions`` and the registry enumeration's perp arm
    (``registry_enumeration.py`` copies ``market`` into ``details``). There is
    deliberately NO fallback to parsing ``position_id``: a guessed market is a
    read of the WRONG asset, whose measured zero would certify closure of a
    position nobody looked at.
    """
    details = position.details if isinstance(getattr(position, "details", None), dict) else {}
    return str(details.get("market") or "").strip()


def hyperliquid_teardown_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,  # noqa: ARG001 — protocol parity; framework crosses the gateway only
    block: int | str | None = None,
) -> ClosureCheckResult:
    """Verify a Hyperliquid perp position is flat on HyperCore (``0x0800``)."""
    position_id = str(getattr(position, "position_id", "") or "")

    position_type = str(getattr(position, "position_type", "") or "").upper()
    if not position_type.endswith("PERP"):
        # ``PositionType`` is a StrEnum, so ``str()`` is the bare member value on
        # a real position; ``endswith`` also tolerates a plain "PositionType.PERP"
        # repr from a stand-in. Anything else genuinely is not a perp.
        return _not_applicable(
            position_id,
            f"hyperliquid post-condition verifies PERP positions; got {position_type or '<unset>'}",
        )

    details = position.details if isinstance(getattr(position, "details", None), dict) else {}
    kind = str(details.get("kind") or "").lower()
    if kind in _ORDER_KINDS:
        return _unverified(
            position_id,
            (
                f"hyperliquid closure UNVERIFIED for {position_id}: residual kind '{kind}' is a "
                "pending CoreWriter order, and this hook reads the position precompile only — "
                "it has no authority over HyperCore's order queue"
            ),
        )

    chain = str(getattr(position, "chain", "") or "").strip()
    if not chain:
        return _unverified(position_id, "hyperliquid post-condition needs position.chain; none found")
    if gateway_client is None:
        return _unverified(
            position_id,
            (
                "hyperliquid post-condition requires a gateway_client to read the 0x0800 position "
                "precompile. None supplied — verification cannot proceed."
            ),
        )
    if not str(wallet_address or "").strip():
        return _unverified(position_id, "hyperliquid post-condition needs a wallet address; none found")

    symbol = _market_symbol(position)
    if not symbol:
        return _unverified(
            position_id,
            (
                f"hyperliquid closure UNVERIFIED for {position_id}: position carries no "
                "details['market'], so there is no asset index to read"
            ),
        )
    try:
        market = resolve_market(symbol)
    except Exception as exc:  # noqa: BLE001 — an unresolvable market is unmeasured, never closed
        return _unverified(
            position_id,
            (
                f"hyperliquid closure UNVERIFIED for {position_id}: market '{symbol}' is not in the "
                f"resolvable perp universe ({exc}) — cannot read its position precompile"
            ),
        )

    # Deferred so the eagerly-registered post-condition does not drag ``.sdk``
    # into the gateway sidecar's import graph — see the module docstring and
    # VIB-6395. Inside the try because this function NEVER raises: an
    # ImportError here is a read fault like any other, not a residual.
    try:
        from .sdk import decode_position, encode_position_query

        blob = gateway_client.eth_call(
            chain=chain,
            to=PRECOMPILE_POSITION,
            data="0x" + encode_position_query(wallet_address, market.asset_index).hex(),
            block=block,
        )
    except Exception as exc:  # noqa: BLE001 — the CHECK must never fault the teardown lane
        logger.debug("Hyperliquid 0x0800 read raised for %s", position_id, exc_info=True)
        return _unverified(
            position_id,
            (
                f"hyperliquid closure UNVERIFIED for {position_id}: 0x0800 position read raised "
                f"({type(exc).__name__}: {exc}) — read fault, not a residual"
            ),
        )

    # Coercion is part of READING the blob, so it is guarded like the read. A
    # malformed payload — non-hex digits ("0xzz"), or an object ``bytes()``
    # rejects — raises here, and this function promises it NEVER raises. The
    # outer teardown layers do catch it and stay fail-closed, so this was never
    # a false-green; but it escaped as a traceback instead of the honest
    # UNVERIFIED diagnostic, and a future direct caller would not be so lucky.
    try:
        raw = bytes.fromhex(blob[2:]) if isinstance(blob, str) and blob.startswith("0x") else bytes(blob or b"")
    except Exception as exc:  # noqa: BLE001 — a malformed payload is unmeasured, never closed
        logger.debug("Hyperliquid 0x0800 blob coercion failed for %s", position_id, exc_info=True)
        return _unverified(
            position_id,
            (
                f"hyperliquid closure UNVERIFIED for {position_id}: 0x0800 returned a payload that is "
                f"not coercible to bytes ({type(exc).__name__}: {exc}) — read fault, not a residual"
            ),
        )

    if len(raw) < _POSITION_STRUCT_BYTES:
        # See the module docstring: decode_position would turn this into a
        # measured szi==0 and fabricate a closure proof off no chain data.
        return _unverified(
            position_id,
            (
                f"hyperliquid closure UNVERIFIED for {position_id}: 0x0800 returned {len(raw)} bytes, "
                f"expected {_POSITION_STRUCT_BYTES} — an empty/short return is NOT a measured flat"
            ),
        )
    try:
        decoded = decode_position(raw)
    except Exception as exc:  # noqa: BLE001 — an undecodable blob is unmeasured, never closed
        logger.debug("Hyperliquid 0x0800 decode failed for %s", position_id, exc_info=True)
        return _unverified(
            position_id,
            f"hyperliquid closure UNVERIFIED for {position_id}: 0x0800 return did not decode ({exc})",
        )

    if decoded.szi != 0:
        return _result(
            False,
            position_id,
            market=market.symbol,
            szi=decoded.szi,
            entry_ntl=decoded.entry_ntl,
            is_long=decoded.is_long,
        )
    return _result(True, position_id, market=market.symbol)


# TD-08 Plan-A inverts this same measured read: a decoded ``szi == 0`` on the
# position's own market proves that position flat, and a non-zero ``szi`` proves
# it open. Scoped per market, so — unlike an account-level reader — it cannot
# attribute an unrelated market's residual to this position.
hyperliquid_teardown_post_condition.supports_open_state_reconciliation = True  # type: ignore[attr-defined]
# ``handles_pending_orders`` is deliberately NOT declared: this hook reads the
# position precompile, not the order queue, so Plan-A must keep a pending-order
# residual UNVERIFIABLE rather than deferring it here.

__all__ = ["hyperliquid_teardown_post_condition"]
