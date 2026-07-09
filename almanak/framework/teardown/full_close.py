"""Per-KNOWN-position live "close fully" intent builder (VIB-5465 / TD-07).

Teardown closes positions FULLY, and the size that must be closed is the LIVE
on-chain figure for each KNOWN position — debt + accrued interest for a borrow,
the current supply (incl. accrued interest) for a collateral leg, the live
liquidity of an LP NFT, the live share→asset conversion for a vault — resolved
at EXECUTION time, not frozen at plan-build time.

A strategy that hardcodes an exit size (a snapshotted ``swap_amount_usd`` or a
cached ``_collateral_supplied``) bakes a stale figure that no longer matches the
chain by the time teardown runs: the swap reverts / routes 422 (VIB-5453), or a
repay of the snapshotted principal leaves dust debt so the follow-on
withdraw-all reverts (ALM-2811). The fix is to express each close with the
framework's live-resolution MARKER and let the execution lane + compiler resolve
the concrete wei against the chain at execution:

    BORROW  -> Intent.repay(repay_full=True)        # MAX_UINT256 -> live debt+interest
    SUPPLY  -> Intent.withdraw(withdraw_all=True)    # MAX_UINT256 -> live supply+interest
    VAULT   -> Intent.vault_redeem(shares="all")     # live share->asset
    LP      -> Intent.lp_close(position_id)           # connector reads live liquidity
    PERP    -> Intent.perp_close(size_usd=None)       # full close
    STAKE / -> Intent.swap(amount="all")              # live wallet balance of the
    TOKEN                                              # KNOWN held token (clamped)

This is **Plan A**: resolution is per-KNOWN-position (driven by the
``PositionInfo`` set the strategy already tracks via ``get_open_positions`` /
``resolve_open_positions``), NOT a wallet scan. The markers themselves resolve
live downstream — ``amount_resolver.resolve_amount_all`` at compile for
WITHDRAW/REPAY, the teardown lanes' balance read with memo eviction for SWAP —
so this module only *chooses the right marker per position*, ordered by
``PositionType.priority`` (PERP -> BORROW -> SUPPLY -> VAULT -> LP -> ...).

Honesty over guessing (Empty ≠ Zero): a position whose ``details`` lack a field
required to build a safe close (e.g. a PERP without a known direction) is
SKIPPED with a loud WARNING rather than fabricated, and the caller keeps any
hand-rolled close for that position. A swap whose held token already IS the
target is dropped (nothing to do).

Connector caveat: a few connectors cannot honour the bare ``withdraw_all`` /
``repay_full`` marker (e.g. Benqi's Compound-fork ``redeemUnderlying`` needs the
qiToken balance, not MAX_UINT256). Those keep hand-rolling their close. The
Aave-family / Morpho / MetaMorpho connectors used by the lending and vault demos
honour the markers cleanly.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.teardown.models import PositionInfo, PositionType, TeardownPositionSummary

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import AnyIntent

logger = logging.getLogger("almanak.framework.teardown.full_close")

# Default exit slippage for the SWAP-shaped close (STAKE / TOKEN). Matches the
# teardown manager's manual initial slippage; the escalation ladder loosens it
# under operator approval, so this is only the starting tolerance.
_DEFAULT_SWAP_SLIPPAGE = Decimal("0.02")

# A well-formed bytes32 order key: ``0x`` + exactly 64 hex chars (no underscores).
# Mirrors ``PerpCancelIntent``'s validator so the two fail-closed gates never drift.
_BYTES32_RE = re.compile(r"0x[0-9a-fA-F]{64}")


def _first(details: dict[str, Any], *keys: str) -> Any:
    """Return the first present, truthy value among ``keys`` in ``details``."""
    for key in keys:
        value = details.get(key)
        if value:
            return value
    return None


def _is_bytes32_key(value: Any) -> bool:
    """Whether ``value`` is a well-formed ``bytes32`` order key (0x + exactly 64 hex chars).

    Fail-closed gate for pending-order cancellation: a truncated / malformed key is
    left-padded by the adapter and would target a DIFFERENT order, so a residual
    without a real bytes32 key must be skipped (stays a loud uncovered residual),
    never turned into a mis-targeted cancel. Uses a strict regex (not ``int(value,
    16)``, which accepts digit-separator underscores) so the check matches the
    ``PerpCancelIntent`` validator exactly.
    """
    return isinstance(value, str) and _BYTES32_RE.fullmatch(value) is not None


def _perp_close_or_cancel_intent(
    position: PositionInfo,
    details: dict[str, Any],
    *,
    protocol: str,
    chain: str,
) -> AnyIntent | None:
    """Map a PERP position OR a pending-order residual to its risk-reducing intent.

    A pending (unfilled) order residual (VIB-5116 discovery) is NOT a position: it
    holds committed collateral in the OrderVault but was never opened (no keeper
    executed it). It is CANCELLED — recovering the collateral — NOT perp-closed
    (there is no position to close). The residual the discovery lane DETECTS becomes
    a cancel here (VIB-5568), so completeness passes instead of failing loud; the
    protocol is carried on the residual, so the cancel routes to that venue's compiler.
    Everything else is a live full close.
    """
    if details.get("kind") == "pending_order":
        order_key = _first(details, "order_key") or position.position_id
        # Fail-closed: without a real bytes32 order key we cannot safely cancel (a
        # truncated key would target the wrong order), and a not-yet-cancellable
        # order (GMX ~300s REQUEST_EXPIRATION_TIME gate) must not become a doomed
        # cancel that the slippage-escalation ladder burns to FAILED. Skip -> the
        # residual stays surfaced and the recovery lane defers it LOUD.
        if not _is_bytes32_key(order_key) or not details.get("cancellable"):
            return None
        return Intent.perp_cancel_order(order_key=str(order_key), protocol=protocol, chain=chain)

    market = _first(details, "market") or position.position_id
    collateral = _first(details, "collateral_token", "asset")
    is_long = details.get("is_long")
    # Direction is not derivable — never guess long vs short.
    if not market or not collateral or is_long is None:
        return None
    return Intent.perp_close(
        market=str(market),
        collateral_token=str(collateral),
        is_long=bool(is_long),
        size_usd=None,  # full close
        protocol=protocol,
        chain=chain,
        position_id=_first(details, "venue_position_id"),
    )


def _close_intent_for_position(
    position: PositionInfo,
    *,
    target_token: str,
    max_slippage: Decimal,
) -> AnyIntent | None:
    """Map ONE known position to its live-resolving full-close intent.

    Returns ``None`` (caller logs + skips) when the position type is not
    generically closable or required ``details`` are missing — never guesses.
    """
    ptype = position.position_type
    protocol = position.protocol
    chain = position.chain
    details = position.details or {}

    if ptype == PositionType.BORROW:
        token = _first(details, "asset", "token", "borrow_token", "debt_token")
        if not token:
            return None
        # repay_full=True -> MAX_UINT256 -> the protocol settles the live debt
        # INCLUDING interest accrued since the position was opened (ALM-2811).
        return Intent.repay(
            protocol=protocol,
            token=str(token),
            amount=Decimal("0"),
            repay_full=True,
            market_id=details.get("market_id"),
            chain=chain,
        )

    if ptype == PositionType.SUPPLY:
        token = _first(details, "asset", "token", "collateral_token", "supply_token")
        if not token:
            return None
        # withdraw_all=True -> MAX_UINT256 -> live supply incl. accrued interest.
        return Intent.withdraw(
            protocol=protocol,
            token=str(token),
            amount=Decimal("0"),
            withdraw_all=True,
            market_id=details.get("market_id"),
            chain=chain,
        )

    if ptype == PositionType.VAULT:
        vault = _first(details, "vault_address", "address", "vault") or position.position_id
        if not vault:
            return None
        # shares="all" -> live share->asset conversion at execution.
        return Intent.vault_redeem(
            protocol=protocol,
            vault_address=str(vault),
            shares="all",
            deposit_token=_first(details, "asset", "deposit_token", "underlying"),
            chain=chain,
        )

    if ptype == PositionType.LP:
        # No amount: the connector compiler reads the position's LIVE liquidity
        # at close time; the literal position_id identifies WHICH position.
        return Intent.lp_close(
            position_id=position.position_id,
            pool=details.get("pool"),
            collect_fees=True,
            protocol=protocol,
            chain=chain,
        )

    if ptype == PositionType.PERP:
        return _perp_close_or_cancel_intent(position, details, protocol=protocol, chain=chain)

    if ptype in (PositionType.STAKE, PositionType.TOKEN):
        # ``asset_symbol`` / ``pt_token`` are recognised so a Pendle PT/YT held
        # as a generic TOKEN (whose registry/producer identity is the PT symbol,
        # not a plain ``asset``) still resolves a swap-back token (VIB-5590).
        # ``pt_symbol`` is included for parity with completeness._TOKEN_DETAIL_KEYS
        # (which credits both ``pt_token`` and ``pt_symbol``): a producer that sets
        # only ``pt_symbol`` must still resolve a swap-back token here, else the
        # close is silently skipped and the teardown gets stuck (fail-safe, but
        # avoidable) (VIB-5590 / CodeRabbit).
        token = _first(details, "asset", "asset_symbol", "token", "address", "pt_token", "pt_symbol")
        if not token:
            return None
        if str(token).upper() == target_token.upper():
            # Already the target asset — nothing to swap.
            return None
        # A held protocol-token (e.g. a Pendle PT/YT) is NOT a plain ERC20: a
        # protocol-less swap cannot route it (no direct DEX pair), so its close
        # MUST route through the owning protocol's swap compiler. A position whose
        # producer sets ``details["protocol_routed_close"]`` opts in: the close
        # SWAP is stamped with the position's OWN ``protocol`` value so the
        # compiler resolves the market from the token (VIB-5590). Keyed on a
        # capability FLAG — never a protocol-name literal — so this module stays
        # protocol-agnostic (blueprint 22) and generalises to YT / any future
        # routing-required protocol-token. A plain held token carries no flag and
        # keeps the generic protocol-less swap, so this cannot misroute it.
        swap_protocol = protocol if _first(details, "protocol_routed_close") else None
        # amount="all" -> live wallet balance of the KNOWN held token, resolved
        # at execution and clamped to the strategy's tracked quantity (ALM-2766).
        return Intent.swap(
            from_token=str(token),
            to_token=target_token,
            amount="all",
            max_slippage=max_slippage,
            chain=chain,
            protocol=swap_protocol,
        )

    # PREDICTION / CEX / anything else: not generically closable here.
    return None


def full_close_intents(
    positions: TeardownPositionSummary | Iterable[PositionInfo] | None,
    *,
    target_token: str = "USDC",
    max_slippage: Decimal = _DEFAULT_SWAP_SLIPPAGE,
) -> list[AnyIntent]:
    """Build live-resolving "close fully" intents for a set of KNOWN positions.

    Args:
        positions: A ``TeardownPositionSummary`` or any iterable of
            ``PositionInfo`` (e.g. ``get_open_positions().positions``).
        target_token: Token to swap residual held / staked tokens into.
        max_slippage: Starting slippage for the SWAP-shaped close.

    Returns:
        Close intents ordered by ``PositionType.priority`` (PERP first, TOKEN
        last). Each carries a live-resolution marker (repay_full / withdraw_all /
        shares="all" / amount="all" / literal LP position_id), so the concrete
        on-chain amount is resolved at EXECUTION, never at plan-build. Positions
        that cannot be safely closed generically are skipped with a WARNING.
    """
    if positions is None:
        # Defensive: a strategy whose get_open_positions() returns None has no
        # known positions to close. Empty ≠ Zero — treat as "nothing to do".
        return []
    pos_list = list(positions.positions if isinstance(positions, TeardownPositionSummary) else positions)
    # Risk-ordered close (PERP -> BORROW -> SUPPLY -> VAULT -> LP -> STAKE -> ...).
    ordered = sorted(pos_list, key=lambda p: p.position_type.priority)

    intents: list[AnyIntent] = []
    for position in ordered:
        try:
            intent = _close_intent_for_position(
                position,
                target_token=target_token,
                max_slippage=max_slippage,
            )
        except Exception:  # noqa: BLE001 - one bad position must not abort the unwind
            logger.warning(
                "🛑 full_close: failed to build close intent for %s position %s (%s) — "
                "skipping; strategy must hand-roll this close.",
                position.position_type.value,
                position.position_id,
                position.protocol,
                exc_info=True,
            )
            continue
        if intent is None:
            logger.warning(
                "🛑 full_close: no generic live close for %s position %s (%s) — "
                "skipping; strategy must hand-roll this close (missing details or "
                "non-closable type).",
                position.position_type.value,
                position.position_id,
                position.protocol,
            )
            continue
        intents.append(intent)

    return intents
