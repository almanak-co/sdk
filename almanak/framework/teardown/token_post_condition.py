"""Generic ERC-20 TOKEN-holding teardown on-chain closure verifier (VIB-6285).

The third instance of an established pattern, after the ERC-4626 vault default
(VIB-5573) and the fungible-ERC-20-LP default (VIB-5795 / VIB-5896): a framework
default that gives a whole PRIMITIVE an on-chain closure authority it previously
lacked, degrading to ``unmeasured`` rather than ever fabricating a result.

Why this exists
---------------
``PositionType.TOKEN`` rows — the plain token holdings swap-only and staking
strategies surface (``uniswap_rsi``, ``lido_staker``, ``metamorpho_base_yield``,
``mantle_mnt_accumulator``, ``pancakeswap_aave_carry_bsc``) — had NO
post-teardown chain authority:

* Plan-A returns ``UNVERIFIABLE`` for TOKEN ("no per-position Plan-A chain read").
* No TD-14 hook covered them. The NFT-shaped uniswap_v3/v4 LP hooks are reached
  for TOKEN rows only because ``protocol="uniswap_v3"`` is shared between LP NFT
  positions and TOKEN rows, and those hooks are structurally out of scope —
  since VIB-6285 they say so explicitly (``not_applicable=True``) instead of
  returning a bare ``closed=True`` that fabricated a proof off zero chain reads.

With both authorities silent, a TOKEN teardown carried no measured closure
evidence at all, so the VIB-6285 certification ratchet could not certify it.
This hook supplies the missing evidence — as a REAL GATEWAY CHAIN READ.

Deliberately NOT a strategy self-report
---------------------------------------
The Uniswap-V3 hook's comment names ``get_open_positions()`` as the owner of
balance-zero verification for TOKEN positions. That is the right owner for
*enumeration*, but it must NOT be the closure authority: a strategy's self-report
is precisely the evidence class the ratchet exists to reject. The GMX canary
certified two positions off ``position_events`` rows the strategy wrote from its
own phase transitions, for a lifecycle that never happened on chain. Closure
evidence must come from the chain, through the gateway.

Closure rule
------------
A TOKEN position is a plain ERC-20 balance in the wallet, so a clean close leaves
**exactly 0**. Closure requires ``balanceOf(wallet) <= _TOKEN_DUST_WEI`` — a wei
floor kept only as insurance against rounding residue. It is denominated in the
token's smallest unit with no price applied.

**DANGER — that immateriality argument is DECIMALS-DEPENDENT (VIB-6312).** 10 base
units is ~1e-17 of an 18-decimal token and $1e-5 of 6-decimal USDC, but this seam
never resolves decimals, and the floor is applied to every ERC-20 alike. On a
LOW-decimal token the same 10 units is material — $0.10 of 2-decimal GUSD, and ten
whole tokens of a 0-decimal one. There it is a FABRICATED CLOSURE: the hook records
a measured proof, and under the VIB-6285 ratchet that proof can certify the whole
protocol group. Narrow (it needs a low-decimal token AND a residual of 1..10 units,
where a full swap/transfer leaves exactly 0), which is why it ships — but it is a
false-GREEN, unlike the false-RED limitations below, so treat it as the more
dangerous of the two.

Coverage & honesty (Empty ≠ Zero)
---------------------------------
The token address is resolved in priority order: an address-shaped
``details['token_address'|'asset_address'|'token']``; then the ``details['asset']``
SYMBOL through the framework ``TokenResolver`` (the shape real strategies
actually write — ``uniswap_rsi`` and ``lido_staker`` both store a symbol); then an
address-shaped ``position_id``. A position with no resolvable token address
returns ``unmeasured=True`` → the seam keeps it at ``UNVERIFIED`` (honest "this
default cannot verify that position"), NEVER ``FAILED`` and NEVER a false
``CHAIN_VERIFIED``. A ``None`` / non-numeric balance read after retry is a read
fault → ``unmeasured``, never a fabricated residual.

Gateway boundary: the on-chain read goes through the supplied
``gateway_client.query_erc20_balance``. ``rpc_url`` is accepted to satisfy the
``TeardownPostCondition`` protocol but intentionally NOT consumed — framework
code crosses the gateway boundary only. NEVER raises.
"""

from __future__ import annotations

import logging
from typing import Any

from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult
from almanak.connectors._strategy_base.vault_post_condition import _is_evm_address, _read_with_retry

logger = logging.getLogger(__name__)

# Wei-level dust floor, in the token's smallest unit. A token close transfers or
# swaps the exact balance, so the expected residual is exactly 0; the floor only
# guards wei-level rounding residue.
#
# NOT SAFE FOR LOW-DECIMAL TOKENS (VIB-6312, CodeRabbit on PR #3531). "No price is
# applied so this cannot mask a material strand" holds at 18 and 6 decimals and
# FAILS at 2 or 0 — see the DANGER note in the module docstring. Do not raise this
# floor, and do not reuse it for a new primitive, without resolving decimals first.
_TOKEN_DUST_WEI = 10

# KNOWN LIMITATION, shipped deliberately (VIB-6285 W0.1; fix tracked as VIB-6311 — see
# ``docs/internal/plans/vib-6285-w01-known-limitations-followup-20260801.md``).
#
# ``balanceOf(wallet)`` is a WHOLE-ACCOUNT read, so a non-zero result cannot be
# attributed to THIS position — yet it is reported as a measured residual, which
# drives ``all_closed=False`` -> FAILED. Teardown deliberately leaves commingled
# funds behind (``swap_clamp``: "we swap the TRACKED quantity, never qty_idle",
# and ``untracked_token`` is skipped outright), so a wallet holding any funding
# balance of the position's token reports a SUCCESSFUL teardown as failed.
# Reachable on ``demo_strategies/uniswap_rsi`` (funds WETH: 1, trades $3) and
# ``lido_staker``. Loud, never a silent strand — but a false-failure mode.
#
# The same read also cannot judge a VIB-5494 target-token no-op, where the wallet
# is SUPPOSED to end holding the consolidation target.
#
# Fix direction (NOT attribution against tracked inventory — that needs the
# ACCOUNTING StateManager, which this seam does not have and which silently
# yields the unmeasured sentinel): answer "no opinion" for an unattributable
# balance, and gate the ratchet on whether an authority exists at all.

# Detail keys that may carry the ERC-20 contract ADDRESS, in priority order.
# Deliberately narrow: an ambiguous key could hold a pool/market address, and a
# wrong-but-valid address would measure an unrelated wallet balance as a
# "residual" → false FAILED. Unresolvable → unmeasured (honest).
_TOKEN_ADDRESS_DETAIL_KEYS = ("token_address", "asset_address", "token")

# Detail keys that may carry the token SYMBOL, resolved via TokenResolver. This
# is the shape real strategies write (``details={"asset": "WETH"}``).
#
# ``"token"`` appears in BOTH tuples deliberately (Codex P2, PR #3531): it is tried
# as an address first, then as a symbol. It was address-only, so the very common
# ``details={"token": "WETH"}`` shape failed the address pass, was never tried as a
# symbol, and resolved to ``""`` → unmeasured. 12+ TOKEN-typed positions write it,
# including the accounting reference fixture ``strategies/accounting/ta/strategy.py``.
# Harmless before VIB-6285 (TOKEN had no authority at all); with the per-protocol
# rule an unmeasured TOKEN row REFUSES TO CERTIFY a successful unwind, so the gap
# became a block. Neither change causes that alone.
_TOKEN_SYMBOL_DETAIL_KEYS = ("asset", "symbol", "base_token", "token")


def _resolve_token_address(details: dict, position_id: str, chain: str) -> str:
    """Resolve the ERC-20 contract address for a TOKEN position, or ``""``.

    Pure and never raises — an unresolvable address is the caller's ``unmeasured``
    signal, never an exception into the teardown verification lane.
    """
    for key in _TOKEN_ADDRESS_DETAIL_KEYS:
        candidate = str(details.get(key) or "")
        if _is_evm_address(candidate):
            return candidate

    # Symbol → address through the framework resolver. Framework → framework is
    # the allowed import direction (this module is framework, unlike the
    # connector-layer vault / fungible-LP defaults it otherwise mirrors).
    for key in _TOKEN_SYMBOL_DETAIL_KEYS:
        symbol = str(details.get(key) or "").strip()
        if not symbol:
            continue
        if _is_evm_address(symbol):
            return symbol
        try:
            # The SINGLETON, not a fresh ``TokenResolver()`` (CodeRabbit, PR #3531).
            # Both lanes that reach this hook wire the gateway channel into the
            # singleton — ``cli/_run_setup.py:_wire_token_resolver`` and
            # ``cli/teardown_helpers.py`` both do
            # ``get_token_resolver().set_gateway_channel(gateway_client.channel)``.
            # A fresh instance carries no channel, so ``_resolve_by_symbol``
            # never takes the dynamic path and any symbol outside the static
            # registry resolves to nothing → ``unmeasured``. That would have
            # silently un-measured the very closures this authority exists to
            # measure. It also reuses the warm caches instead of rebuilding the
            # index per candidate symbol, per position, in the teardown loop.
            # ``resolve`` already defaults ``skip_gateway=False``.
            from almanak.framework.data.tokens import get_token_resolver

            resolved = get_token_resolver().resolve(symbol, chain)
            address = str(getattr(resolved, "address", "") or "")
            if _is_evm_address(address):
                return address
        except Exception:  # noqa: BLE001 — unresolvable is unmeasured, never a fault
            logger.debug(
                "TOKEN post-condition: could not resolve symbol %r on %s to an address",
                symbol,
                chain,
                exc_info=True,
            )

    if _is_evm_address(position_id):
        return position_id
    return ""


def token_balance_teardown_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,
    block: int | str | None = None,
) -> ClosureCheckResult:
    """Verify a TOKEN position holds zero ERC-20 balance on-chain.

    Reads ``balanceOf(wallet)`` on the token contract via the gateway; closed iff
    the balance is within the wei dust floor.

    Returns:
        ``closed=True`` when the balance is ``<= _TOKEN_DUST_WEI``;
        ``closed=False`` + ``residual`` when a positive balance is MEASURED;
        ``unmeasured=True`` when the read could not be completed (missing
        client/chain, no resolvable token address, gateway/RPC fault after retry)
        — never a fabricated residual and never a fabricated closure.
    """
    protocol = (getattr(position, "protocol", "") or "").lower() or "token"
    position_id = str(getattr(position, "position_id", "") or "")

    chain = str(getattr(position, "chain", None) or "")
    if not chain:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error="TOKEN post-condition needs position.chain; none found — cannot verify (unmeasured)",
        )

    if gateway_client is None:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                "TOKEN post-condition requires a gateway_client to read balanceOf; "
                "none supplied — cannot verify (unmeasured)"
            ),
        )

    # The wallet is an INPUT to the measurement, so it gets the same Empty ≠ Zero
    # treatment as chain / gateway_client / token_address (CodeRabbit, PR #3531).
    # ``_teardown_wallet_address`` returns ``""`` for a strategy that exposes no
    # wallet, so an empty value is reachable — and ``balanceOf`` of a wallet that
    # is not an address is not a measured zero. Without this guard the gateway may
    # coerce it (e.g. to the zero address), return 0, and the caller would record a
    # hook proof and CERTIFY the protocol group off a read that measured nothing.
    # Unresolvable input ⇒ unmeasured, never a fabricated closure.
    wallet = str(wallet_address or "")
    if not _is_evm_address(wallet):
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                "TOKEN post-condition needs an address-shaped wallet_address to read "
                f"balanceOf; got {wallet_address!r} — cannot verify (unmeasured)"
            ),
        )

    details = getattr(position, "details", None)
    if not isinstance(details, dict):
        details = {}
    token_address = _resolve_token_address(details, position_id, chain)
    if not token_address:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                "TOKEN post-condition needs the ERC-20 address "
                "(details['token_address'|'asset_address'|'token'], a resolvable "
                "details['asset'|'symbol'|'base_token'] symbol, or an address-shaped "
                f"position_id); none resolvable (position_id={position_id!r}, "
                f"details keys={sorted(details)}) — cannot verify (unmeasured)"
            ),
        )

    balance = _read_with_retry(
        lambda: gateway_client.query_erc20_balance(
            chain=chain,
            token_address=token_address,
            wallet_address=wallet,
            block=block,
        )
    )
    try:
        balance = int(balance) if balance is not None else None
    except (TypeError, ValueError):
        balance = None
    if balance is None:
        logger.error(
            "TOKEN post-condition balance read returned None/non-numeric after retries: "
            "protocol=%s token=%s wallet=%s "
            "block=%s; closure remains unmeasured",
            protocol,
            token_address,
            wallet,
            block,
        )
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"TOKEN balanceOf({token_address}) read returned None/non-numeric after retry "
                "(gateway/RPC fault); cannot confirm closure — unmeasured"
            ),
        )

    if balance <= _TOKEN_DUST_WEI:
        return ClosureCheckResult(closed=True, protocol=protocol, position_id=position_id)

    logger.warning(
        "TOKEN post-condition MEASURED residual balance: protocol=%s token=%s wallet=%s "
        "balance=%d (position NOT closed on-chain)",
        protocol,
        token_address,
        wallet,
        balance,
    )
    return ClosureCheckResult(
        closed=False,
        protocol=protocol,
        position_id=position_id,
        residual={"token": token_address, "balance": str(balance)},
        error=f"Residual token balance {balance} wei on {token_address} exceeds dust floor {_TOKEN_DUST_WEI}",
    )


__all__ = ["token_balance_teardown_post_condition"]
