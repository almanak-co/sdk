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
Native sentinel, zero-address and chain-native aliases use the gateway native
balance read. A positive native wallet balance can include gas principal and is
therefore unmeasured position attribution; the manager's transaction-bound native
inventory authority supplies closure only with independent receipt/balance proof.
The ERC-20 whole-account rule below is unchanged.

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
The verifier resolves the explicit held-token declaration, including the planner's
``asset_symbol``, ``pt_token`` and ``pt_symbol`` forms, through the runner-bound
``TokenResolver``. An unresolved declaration remains unmeasured; it never falls
through to a different asset such as PT sale proceeds in ``base_token``. Legacy
base-only declarations and address-shaped position IDs remain supported when no
held-token metadata is supplied. A missing/non-numeric balance read after retry
is a read fault, never fabricated closure or a measured residual.

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
from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.framework.data.tokens import NATIVE_SENTINEL

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

# Explicit held-token metadata precedes a legacy base-only declaration. A PT's
# base token describes sale proceeds, not the token whose closure must be proved.
#
# Deliberately narrow: a key generic enough to hold a pool/market identifier
# instead of the held token would measure an unrelated wallet balance as a
# residual. A bare ``"address"`` is exactly that shape — TOKEN producers write
# it for the V4 pool-key currency, which is the zero address on a native pool —
# so it stays out, and every site that writes it also declares one of the keys
# below.
_TOKEN_DETAIL_KEYS = (
    "token_address",
    "asset_address",
    "asset",
    "asset_symbol",
    "symbol",
    "token",
    "pt_token",
    "pt_symbol",
)


def _resolve_token_address(details: dict, position_id: str, chain: str) -> str:
    """Resolve the declared held token without substituting another asset."""
    token = next(
        (
            str(details[key]).strip()
            for key in _TOKEN_DETAIL_KEYS
            if details.get(key) is not None and str(details[key]).strip()
        ),
        "",
    )
    if not token and any(key in details for key in _TOKEN_DETAIL_KEYS):
        return ""
    if not token:
        token = str(details.get("base_token") or "").strip()
        if not token and _is_evm_address(position_id):
            return position_id
    if not token:
        return ""
    if _is_evm_address(token):
        return token
    try:
        # The runner-bound singleton retains its gateway channel and token cache.
        from almanak.framework.data.tokens import get_token_resolver

        resolved = get_token_resolver().resolve(token, chain)
        address = str(getattr(resolved, "address", "") or "")
        return address if _is_evm_address(address) else ""
    except Exception:  # noqa: BLE001 — unresolved identity is never closure proof
        logger.debug("TOKEN post-condition: cannot resolve held token %r on %s", token, chain, exc_info=True)
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
                "TOKEN post-condition needs the ERC-20 address from explicit "
                "held-token metadata (including asset_symbol/pt_token), a legacy "
                "base-token-only declaration, or an address-shaped position_id; "
                f"none resolvable (position_id={position_id!r}, "
                f"details keys={sorted(details)}) — cannot verify (unmeasured)"
            ),
        )

    descriptor = ChainRegistry.try_resolve(chain)
    native_addresses = {NATIVE_SENTINEL.lower(), "0x" + "0" * 40}
    if descriptor is not None:
        native_addresses.update(address.lower() for address in descriptor.native.address_aliases)
    is_native = token_address.lower() in native_addresses
    if is_native and (descriptor is None or descriptor.family is not ChainFamily.EVM):
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error="Native TOKEN balance requires a registered EVM chain; closure is unmeasured",
        )
    if is_native:
        balance = _read_with_retry(
            lambda: gateway_client.query_native_balance(chain=chain, wallet_address=wallet, block=block)
        )
    else:
        balance = _read_with_retry(
            lambda: gateway_client.query_erc20_balance(
                chain=chain,
                token_address=token_address,
                wallet_address=wallet,
                block=block,
            )
        )
    try:
        balance = int(balance) if isinstance(balance, int | str) and not isinstance(balance, bool) else None
        if balance is not None and balance < 0:
            balance = None
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
                f"TOKEN {'native balance' if is_native else 'balanceOf'}({token_address}) read returned None/non-numeric after retry "
                "(gateway/RPC fault); cannot confirm closure — unmeasured"
            ),
        )

    if balance <= _TOKEN_DUST_WEI:
        return ClosureCheckResult(closed=True, protocol=protocol, position_id=position_id)

    if is_native:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"Measured native wallet balance {balance} wei includes possible gas or unrelated inventory; "
                "position-attributed closure requires transaction-bound inventory and gas evidence"
            ),
        )

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
