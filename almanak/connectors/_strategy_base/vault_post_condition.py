"""Generic ERC-4626 vault teardown on-chain closure verifier (VIB-5573).

Before this hook, the whole **VAULT** primitive had no post-teardown on-chain
authority: no connector under ``ProtocolKind.VAULT`` (``beefy``, ``lagoon``,
``morpho_vault``, ``yearn``) registered a ``teardown_post_condition``, and
``plan_a_reconciliation`` returns ``UNVERIFIABLE`` for VAULT. So a vault teardown
was structurally pinned at ``UNVERIFIED`` — a partial / incomplete redeem was
reported as a (degraded-confidence) success and a residual was invisible. This is
the exact gap that stranded a MetaMorpho position in the 20260630 overnight batch
(``docs/internal/archive/reports/fund-recovery-investigation-20260701.md`` §Position 1).

This is a **framework default keyed on the VAULT kind** (registered in
``almanak.framework.teardown.post_conditions``), the vault analogue of the
Uniswap-V3-NPM default — NOT a per-connector hook — so every current and future
ERC-4626 vault connector is covered by one implementation.

Closure rule — asset-denominated dust floor, which is REQUIRED for vaults
------------------------------------------------------------------------
The SDK vault teardown redeems ``maxRedeem(owner)`` (``shares="all"`` →
``get_max_redeem`` → ``redeem(maxRedeem)``), NOT ``balanceOf(owner)`` (a
``redeem(balanceOf)`` REVERTS — ``maxRedeem`` is a hair below ``balanceOf`` from
ERC-4626 share↔asset round-trip rounding). So a CLEAN full redeem ALWAYS leaves a
few wei of **leftover shares** whose asset value is the round-trip rounding error
— **≤ ~1 wei of the underlying** (proven on-fork: exactly 1 wei). An exact-0 rule
(on shares OR assets) would therefore FALSE-FAIL every clean vault teardown, and
— because a FAILED teardown latches entry (VIB-5572) — brick the strategy. The
correct rule is a tiny asset-denominated dust floor:
``convertToAssets(balanceOf(owner)) <= _VAULT_ASSET_DUST_WEI`` → closed.
``convertToAssets`` floors, so the ≤1-wei rounding leftover is closed while any
material residual (the incident's $3.20 = 3.2M wei) is orders of magnitude above
the floor → not closed. Oracle-free and decimals-independent (the rounding error
is ~1 wei regardless of decimals); see ``_VAULT_ASSET_DUST_WEI`` for the floor
rationale + the low-decimal-high-value caveat.

We read ``convertToAssets(balanceOf)`` — the total value the owner still owns —
rather than ``maxWithdraw`` (assets *currently withdrawable*), because a
transiently-illiquid vault (the MetaMorpho ``panic 0x11`` case) still holds the
owner's value: ``maxWithdraw`` would under-report and could call an illiquid-but-
funded position "closed". Total owned value is the correct closure signal.

Coverage & honesty (Empty ≠ Zero)
---------------------------------
The read uses the ERC-4626 standard interface (``balanceOf`` + ``convertToAssets``
via the gateway). A vault that does not implement ``convertToAssets`` (e.g. a
classic non-4626 Beefy vault) makes the ``eth_call`` revert / return ``None`` →
this hook returns ``unmeasured=True`` → the seam lowers it to ``UNVERIFIED``
(honest "this default cannot verify that vault"), NEVER ``FAILED`` and NEVER a
false ``CHAIN_VERIFIED``. A connector-specific hook can supersede the default for
such a vault (a manifest ``teardown_post_condition`` wins over this default).

Gateway boundary: every on-chain read goes through the supplied ``gateway_client``
(``query_erc20_balance`` / ``eth_call``). ``rpc_url`` is accepted to satisfy the
``TeardownPostCondition`` protocol but intentionally NOT consumed — framework code
crosses the gateway boundary only. NEVER raises.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult

logger = logging.getLogger(__name__)

# convertToAssets(uint256) — ERC-4626 standard. Verified via
# eth_utils.function_signature_to_4byte_selector("convertToAssets(uint256)").
_CONVERT_TO_ASSETS_SELECTOR = "0x07a2d13a"

# Bounded read-retry (VIB-5573 / Q7): a transient gateway/RPC blip during the
# post-teardown verify must not be mistaken for an unverifiable position. Retry
# the cheap read a few times before declaring it unmeasured. Kept tiny — this is
# a rare once-per-teardown verification, not a hot path.
_READ_ATTEMPTS = 3
_READ_BACKOFF_S = 0.3

# Asset-wei dust floor for closure (VIB-5573, calibrated from the E2E real-fork
# proof). The SDK teardown redeems ``redeem(maxRedeem)``, and ``maxRedeem`` is a
# hair below ``balanceOf`` (ERC-4626 share↔asset round-trip rounding); a
# ``redeem(balanceOf)`` would REVERT (`ERC20InsufficientBalance`). So a COMPLETE
# redeem inherently leaves a few wei of leftover shares whose asset value is the
# round-trip rounding error — **≤ ~1 wei of the underlying** (observed exactly 1
# wei on a Base metamorpho fork: 926083973261 leftover shares → convertToAssets
# == 1). Treating that as a residual would FAILED-latch entry (VIB-5572) on EVERY
# clean vault teardown. So closure tolerates ``assets <= _VAULT_ASSET_DUST_WEI``.
# This is decimals-independent (the rounding error is ~1 wei regardless of token
# decimals) and never masks a MATERIAL strand: the incident this whole hook exists
# to catch was $3.20 (3_200_000 wei of 6-decimal USDC) — 5+ orders of magnitude
# above this floor. Caveat: a pathological low-decimal, very-high-value underlying
# (none exist as ERC-4626 vault assets today) would make a few wei worth more than
# dust; the architecturally-ideal fix is a USD-priced floor in the verify seam
# (follow-up), but that adds an oracle dependency this pure on-chain read avoids.
_VAULT_ASSET_DUST_WEI = 10


def _read_with_retry(
    fn: Callable[[], Any], *, attempts: int = _READ_ATTEMPTS, backoff_s: float = _READ_BACKOFF_S
) -> Any:
    """Call ``fn`` up to ``attempts`` times, returning the first non-``None``
    result. Returns ``None`` if every attempt returns ``None`` or raises.

    A ``None`` return from a gateway read means "read fault" (Empty ≠ Zero); we
    retry it. Exceptions are caught (fail-safe) and retried too. The final
    ``None`` is the caller's signal to mark the position ``unmeasured``.
    """
    for attempt in range(attempts):
        try:
            result = fn()
        except Exception as exc:  # noqa: BLE001 — fail-safe; a raise is a read fault, retry it
            logger.debug("vault post-condition read raised (attempt %d/%d): %s", attempt + 1, attempts, exc)
            result = None
        if result is not None:
            return result
        if attempt < attempts - 1 and backoff_s > 0:
            time.sleep(backoff_s)
    return None


def _decode_uint256(raw: str | None) -> int | None:
    """Decode a single ``uint256`` eth_call return. ``None`` on any fault.

    A well-formed ``convertToAssets`` return is exactly one 32-byte ABI word
    (64 hex chars). Require EXACTLY that — a short / revert-shaped / multi-word
    payload (e.g. ``0xdeadbeef``) must NOT be parsed as a residual value; it is a
    read fault → ``None`` → the caller marks the position ``unmeasured`` (never a
    fabricated residual). (VIB-5573, CodeRabbit/Gemini.)
    """
    if not isinstance(raw, str) or not raw:
        return None
    body = raw[2:] if raw.startswith(("0x", "0X")) else raw
    if len(body) != 64:
        return None
    try:
        return int(body, 16)
    except ValueError:
        return None


def _encode_convert_to_assets(shares: int) -> str:
    """ABI-encode ``convertToAssets(uint256 shares)`` calldata (selector + arg).

    Pure hex encoding, not egress. ``shares`` is padded to a 32-byte word.
    """
    return _CONVERT_TO_ASSETS_SELECTOR + f"{shares:064x}"


def erc4626_vault_teardown_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,
    block: int | str | None = None,
) -> ClosureCheckResult:
    """Verify an ERC-4626 vault position holds zero residual asset value on-chain.

    Reads ``balanceOf(owner)`` (vault shares — the vault contract IS the ERC-20)
    then ``convertToAssets(shares)``; closed iff the owner's shares are worth zero
    assets (exact-0 on assets — see module docstring for why not shares).

    Returns:
        ``closed=True`` when assets owed round to 0 (clean close, incl. share-dust);
        ``closed=False`` + ``residual`` when a positive asset residual is MEASURED;
        ``unmeasured=True`` when the read could not be completed (missing client,
        gateway/RPC fault after retry, non-ERC-4626 vault) — never a fabricated
        residual (Empty ≠ Zero).
    """
    protocol = (getattr(position, "protocol", "") or "").lower() or "vault"
    position_id = str(getattr(position, "position_id", "") or "")

    chain = getattr(position, "chain", None) or ""
    if not chain:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error="Vault post-condition needs position.chain; none found — cannot verify (unmeasured)",
        )

    if gateway_client is None:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                "Vault post-condition requires a gateway_client to read on-chain residual "
                "(balanceOf / convertToAssets); none supplied — cannot verify (unmeasured)"
            ),
        )

    details = getattr(position, "details", None) or {}
    vault_address = str(
        details.get("vault_address") or details.get("address") or details.get("vault") or position_id or ""
    )
    # Validate as an EVM address BEFORE any on-chain read: a malformed address
    # (e.g. a Pendle-style amount-as-position_id, or a garbled detail) would
    # otherwise burn the full read-retry budget on doomed gateway calls before
    # degrading. An invalid address is "cannot verify" → unmeasured (VIB-5573,
    # Gemini). (_is_evm_address also gates the position_id fallback.)
    if not _is_evm_address(vault_address):
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                "Vault post-condition needs a valid EVM vault address "
                "(details['vault_address'|'address'|'vault'] or an address-shaped position_id); "
                f"found {vault_address!r} (position_id={position_id!r}) — cannot verify (unmeasured)"
            ),
        )

    # 1) Shares held (the vault contract itself is the ERC-20 share token).
    shares = _read_with_retry(
        lambda: gateway_client.query_erc20_balance(
            chain=chain,
            token_address=vault_address,
            wallet_address=wallet_address,
            block=block,
        )
    )
    # A ``None`` read (or a non-int the gateway shouldn't but might return) is a
    # read fault, never a fabricated residual → unmeasured (VIB-5573, CodeRabbit).
    try:
        shares = int(shares) if shares is not None else None
    except (TypeError, ValueError):
        shares = None
    if shares is None:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                "Vault balanceOf read returned None/non-numeric after retry (gateway/RPC fault); "
                "cannot confirm closure — unmeasured"
            ),
        )
    if shares == 0:
        return ClosureCheckResult(closed=True, protocol=protocol, position_id=position_id)

    # 2) Asset value of those shares. convertToAssets FLOORS, so share dust left
    #    by redeem(maxRedeem) is worth ≤ ~1 wei of assets → closed (see
    #    _VAULT_ASSET_DUST_WEI), while any material residual is orders of magnitude
    #    above the floor → not closed.
    raw = _read_with_retry(
        lambda: gateway_client.eth_call(
            chain=chain,
            to=vault_address,
            data=_encode_convert_to_assets(shares),
            block=block,
        )
    )
    assets = _decode_uint256(raw)
    if assets is None:
        # Either a transient fault survived retry, or the vault is not ERC-4626
        # (no convertToAssets). Empty ≠ Zero: do NOT treat non-zero shares as a
        # fabricated residual — a connector-specific hook can supersede this
        # default for non-4626 vaults. Honest: unmeasured → UNVERIFIED.
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"Vault convertToAssets({shares}) read returned None/undecodable after retry "
                f"(gateway/RPC fault or non-ERC-4626 vault {vault_address}); cannot confirm closure — unmeasured"
            ),
        )

    if assets <= _VAULT_ASSET_DUST_WEI:
        # Share dust worth ≤ the ERC-4626 redeem round-trip rounding floor — a
        # clean full redeem (redeem(maxRedeem) cannot take the last ~1 wei). Closed.
        return ClosureCheckResult(closed=True, protocol=protocol, position_id=position_id)

    return ClosureCheckResult(
        closed=False,
        protocol=protocol,
        position_id=position_id,
        residual={"vault_address": vault_address, "shares": shares, "assets": assets},
    )


def _is_evm_address(value: Any) -> bool:
    """True iff ``value`` is a 0x-prefixed 20-byte hex address string."""
    if not isinstance(value, str) or not value.startswith(("0x", "0X")) or len(value) != 42:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


__all__ = ["erc4626_vault_teardown_post_condition"]
