"""On-chain LP position discovery for teardown recovery.

Bug 2 of the 0G DogFooding report (2026-04-16) exposed a position-lifecycle
gap: when the gateway that opened an LP position is restarted or replaced,
the strategy's in-memory position tracking is lost. ``strategy.get_open_positions()``
then returns an empty list even though the wallet still owns live NFT
positions on-chain. ``strat teardown execute`` therefore reports
"No open positions found" and the operator has to close positions with raw
``web3.py`` calls — the opposite of dogfooding.

This module provides a protocol-agnostic on-chain discovery primitive that
queries the NonfungiblePositionManager (NPM) contracts directly via the
gateway's RpcService. It enumerates every NFT the wallet holds on each
registered NPM and surfaces them as ``PositionInfo`` rows the teardown
manager can consume. No local state is required.

Supports Uniswap V3 and its forks (Aerodrome, PancakeSwap V3, SushiSwap V3,
Agni Finance, JAINE DEX, ...) which all share the canonical NPM interface:
``balanceOf(address)``, ``tokenOfOwnerByIndex(address, uint256)``,
``positions(uint256)``.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.address_registry import AbiFamily, AddressRegistry
from almanak.framework.teardown.models import PositionInfo, PositionType, TeardownPositionSummary
from almanak.gateway.proto import gateway_pb2

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)

# NPM-style protocols (Uniswap V3 and its forks) that expose the canonical
# balanceOf / tokenOfOwnerByIndex / positions(tokenId) ABI this scan walker
# speaks, so a single walker can enumerate positions across all of them. The
# membership is connector knowledge, so it lives on the strategy-side
# ``AddressRegistry`` under :attr:`AbiFamily.V3_NPM` — this framework module
# never names a protocol itself. Each member's per-chain
# NonfungiblePositionManager address is also resolved through the registry
# (W1 / VIB-4853); the address tables live on the connectors. Supporting a new
# V3 fork requires only adding its ``position_manager`` to the connector's
# ``addresses.py`` and listing the slug under ``AbiFamily.V3_NPM`` — no edit
# here.
_NPM_PROTOCOLS: tuple[str, ...] = AddressRegistry.protocols_with_abi(AbiFamily.V3_NPM)

# ERC-721 selectors (canonical Uniswap V3 NPM interface)
_SELECTOR_BALANCE_OF = "0x70a08231"  # balanceOf(address)
_SELECTOR_TOKEN_OF_OWNER_BY_INDEX = "0x2f745c59"  # tokenOfOwnerByIndex(address,uint256)
_SELECTOR_POSITIONS = "0x99fbab88"  # positions(uint256)

# Maximum positions we probe per NPM. A single wallet is very unlikely to
# hold more than this many live positions; capping guards against pathological
# NFT farms and keeps discovery bounded.
_MAX_POSITIONS_PER_NPM = 256

# Bounded retries per per-position RPC call. A transient gateway/RPC failure
# on one position used to silently drop it — the scan just skipped the token.
# That re-created Bug 2 (orphaned positions) one call at a time. Retry first,
# then surface unrecoverable failures so the operator knows discovery was
# incomplete.
_RPC_RETRIES = 2


@dataclass(frozen=True)
class DiscoveredPosition:
    """A single LP position discovered on-chain.

    Attributes:
        token_id: NFT tokenId on the NPM contract.
        npm_address: NonfungiblePositionManager contract address.
        chain: Chain name (e.g. ``"zerog"``).
        protocol: Protocol slug (``"uniswap_v3"`` — extend when non-V3 NPMs
            are registered).
        token0: token0 address from ``positions()`` (lower-case).
        token1: token1 address from ``positions()``.
        fee: Fee tier in hundredths of bips.
        tick_lower: Lower tick of the position range.
        tick_upper: Upper tick of the position range.
        liquidity: Current liquidity (may be 0 for fully withdrawn positions
            that haven't been burned).
    """

    token_id: int
    npm_address: str
    chain: str
    protocol: str
    token0: str
    token1: str
    fee: int
    tick_lower: int
    tick_upper: int
    liquidity: int


def _pad_address(address: str) -> str:
    """Left-pad a 20-byte address to a 32-byte word (hex without 0x)."""
    return address.lower().replace("0x", "").zfill(64)


def _pad_uint256(value: int) -> str:
    """Encode a uint256 as a 64-char hex word."""
    return hex(value)[2:].zfill(64)


def _decode_int24(word_hex: str) -> int:
    """Decode a signed int24 from a 64-char ABI-encoded hex word.

    The ABI sign-extends signed integers to the full 256 bits, so negative
    ticks arrive as ``0xffffff...fc18`` (for ``-1000``), not as a right-
    aligned 24-bit value. Mask the input to the low 24 bits first, then
    interpret it as two's-complement. Earlier versions subtracted ``2**24``
    from the full 256-bit integer, which produced nonsense ranges for every
    Uniswap V3 position with a negative tick (Codex P3).
    """
    low_24 = int(word_hex, 16) & 0xFFFFFF
    if low_24 >= 2**23:  # negative (two's complement on 24 bits)
        low_24 -= 2**24
    return low_24


async def _eth_call(
    client: GatewayClient,
    chain: str,
    to: str,
    data: str,
    network: str = "",
    timeout: float = 15.0,
) -> str | None:
    """Issue an ``eth_call`` via the gateway RpcService.

    Returns the hex result (with ``0x`` prefix) on success, or ``None`` on
    failure. All errors are logged at DEBUG so callers can iterate over
    multiple NPMs without a single per-chain failure masking others.

    ``network`` is passed through to RpcService verbatim. Empty string means
    "use the gateway's configured network", which is what we want in the
    normal case — setting it to a literal like ``"mainnet"`` here would
    override a gateway configured for ``anvil`` and route discovery to the
    live chain instead of the local fork (Codex review, Bug 2B follow-up).
    """
    request = gateway_pb2.RpcRequest(
        chain=chain,
        method="eth_call",
        params=json.dumps([{"to": to, "data": data}, "latest"]),
        id="teardown_discover",
        network=network,
    )
    try:
        response = client.rpc.Call(request, timeout=timeout)
    except Exception as e:  # noqa: BLE001
        logger.debug("eth_call failed for %s on %s: %s", to, chain, e)
        return None
    if not response.success:
        logger.debug("eth_call returned error for %s on %s: %s", to, chain, response.error)
        return None
    try:
        return json.loads(response.result)
    except (ValueError, json.JSONDecodeError):
        logger.debug("eth_call returned unparsable result for %s on %s", to, chain)
        return None


async def _balance_of(client: GatewayClient, chain: str, npm: str, wallet: str, network: str = "") -> int | None:
    """Return how many NFT positions ``wallet`` holds on ``npm``.

    Returns:
        - ``int >= 0`` on a successful RPC response (including legitimately 0).
        - ``None`` when the RPC call could not be completed after retries or
          the response was malformed. Callers MUST treat ``None`` as
          "unknown, not zero" — otherwise a transient gateway failure is
          indistinguishable from "wallet owns 0 positions" and the entire
          NPM is silently skipped, re-introducing the Bug 2 failure mode
          (CodeRabbit critical, PR #1522).

    The bounded-retry wrapper is consumed only on actual RPC failures
    (``_eth_call`` returning ``None``) — an empty wallet's ``balanceOf``
    returns ``"0x0000..."`` on the first call and no retries fire.
    """
    calldata = _SELECTOR_BALANCE_OF + _pad_address(wallet)
    raw, _ = await _call_with_retries(
        f"balanceOf({wallet}) on {npm}/{chain}",
        _eth_call,
        client,
        chain,
        npm,
        calldata,
        network=network,
    )
    if raw is None:
        # Every retry attempt failed at the RPC layer.
        return None
    if raw == "0x":
        # Successful eth_call, but the response body is empty. This can
        # happen when the NPM contract doesn't implement balanceOf on the
        # chain — treat as "0 positions" since there's nothing to discover.
        return 0
    try:
        return int(raw, 16)
    except ValueError:
        # Parseable response but not a valid hex uint256 — treat as
        # unknown so the operator investigates rather than silently skip.
        logger.debug("balanceOf(%s) returned unparsable hex %r", wallet, raw)
        return None


async def _token_of_owner_by_index(
    client: GatewayClient,
    chain: str,
    npm: str,
    wallet: str,
    index: int,
    network: str = "",
) -> int | None:
    """Return the ``index``-th tokenId owned by ``wallet`` on ``npm``."""
    calldata = _SELECTOR_TOKEN_OF_OWNER_BY_INDEX + _pad_address(wallet) + _pad_uint256(index)
    raw = await _eth_call(client, chain, npm, calldata, network=network)
    if not raw or raw == "0x":
        return None
    try:
        return int(raw, 16)
    except ValueError:
        return None


async def _read_position(
    client: GatewayClient,
    chain: str,
    npm: str,
    token_id: int,
    network: str = "",
    protocol: str = "uniswap_v3",
) -> DiscoveredPosition | None:
    """Query ``positions(tokenId)`` and parse the 12-tuple struct.

    Returns None if the position is missing / malformed. Liquidity-zero
    positions are still returned so operators can see them and decide
    whether to burn.
    """
    calldata = _SELECTOR_POSITIONS + _pad_uint256(token_id)
    raw = await _eth_call(client, chain, npm, calldata, network=network)
    if not raw:
        return None
    hex_data = raw.removeprefix("0x")
    if len(hex_data) < 12 * 64:  # need 12 words: nonce, op, t0, t1, fee, tL, tU, L, fgi0, fgi1, ow0, ow1
        return None
    words = [hex_data[i * 64 : (i + 1) * 64] for i in range(12)]
    try:
        return DiscoveredPosition(
            token_id=token_id,
            npm_address=npm,
            chain=chain,
            protocol=protocol,
            token0="0x" + words[2][-40:],
            token1="0x" + words[3][-40:],
            fee=int(words[4], 16),
            tick_lower=_decode_int24(words[5]),
            tick_upper=_decode_int24(words[6]),
            liquidity=int(words[7], 16),
        )
    except ValueError:
        return None


# Connectors record the NonfungiblePositionManager under one of two keys:
# ``position_manager`` (uniswap_v3 / agni_finance / sushiswap_v3) or ``nft``
# (pancakeswap_v3, whose receipt parser and intent compiler already standardise
# on ``nft``). Accepting both keeps a single per-fork NPM source — the
# connector's ``addresses.py`` — instead of forcing a key rename that would
# ripple through every Pancake reader (VIB-4902).
_NPM_ADDRESS_KEYS = ("position_manager", "nft")


def _npms_for_chain(chain: str) -> list[tuple[str, str]]:
    """Return ``[(protocol_slug, npm_address), ...]`` for all registered V3-fork
    NPMs on the given chain. Ordered deterministically by protocol slug so
    discovery output is stable across runs.
    """
    found: list[tuple[str, str]] = []
    for protocol in sorted(_NPM_PROTOCOLS):
        npm = AddressRegistry.resolve_contract_address(protocol, chain, _NPM_ADDRESS_KEYS)
        if npm:
            found.append((protocol, npm))
    return found


def npm_for_protocol(protocol: str, chain: str) -> str | None:
    """Resolve the NonfungiblePositionManager for ONE V3-fork protocol on a chain.

    The protocol-scoped counterpart of :func:`_npms_for_chain`, for reads that
    verify a *single KNOWN position*. NPM token ids are per-contract monotonic
    counters — the SAME uint exists independently on every V3-fork NPM deployed
    to a chain (e.g. ethereum has uniswap_v3, sushiswap_v3 AND pancakeswap_v3
    NPMs) — so a per-known-position read MUST be scoped to the position's own
    protocol NPM. Walking all registered NPMs (the wallet-scan discovery shape,
    where token ids come FROM each NPM via ``tokenOfOwnerByIndex``) matches a
    foreign protocol's identically-numbered, unrelated position and reports a
    burned position as still open (VIB-5631: the false-FAILED teardown verdict).

    Returns ``None`` when ``protocol`` is not an :attr:`AbiFamily.V3_NPM` member
    or has no NPM registered on ``chain`` — callers treat ``None`` as
    "cannot verify here", never as a licence to probe other protocols' NPMs.
    """
    slug = str(protocol or "").lower()
    if slug not in _NPM_PROTOCOLS:
        return None
    return AddressRegistry.resolve_contract_address(slug, chain, _NPM_ADDRESS_KEYS)


async def _call_with_retries(
    label: str,
    fn,
    *args,
    retries: int = _RPC_RETRIES,
    **kwargs,
):
    """Invoke an awaitable helper with bounded retries.

    Returns ``(value, attempts_used)`` on success, or ``(None, -1)`` when
    every attempt failed. ``label`` is embedded in debug logs for
    correlation.
    """
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            value = await fn(*args, **kwargs)
            if value is not None:
                return value, attempt
            # None = "the RPC replied but the result is empty". Retrying a
            # structural empty response is harmless and sometimes helps with
            # a racing block transition.
        except Exception as e:  # noqa: BLE001 — best-effort, we log and retry
            last_exc = e
            logger.debug("%s attempt %d raised %s; retrying", label, attempt + 1, e)
    if last_exc is not None:
        logger.debug("%s failed after %d attempts: %s", label, retries + 1, last_exc)
    return None, -1


class DiscoveryIncomplete(RuntimeError):
    """Raised when discovery could not enumerate every position the NPM reports.

    ``balanceOf`` says the wallet holds N positions, but one or more
    ``tokenOfOwnerByIndex`` / ``positions`` calls failed even after retries.
    Raising (rather than silently returning a partial list) prevents the
    exact failure mode Bug 2 was closing — an operator proceeding with
    teardown while a position remains orphaned on-chain.
    """

    def __init__(self, chain: str, npm: str, missing: list[int]):
        super().__init__(
            f"LP discovery incomplete on {chain} NPM {npm}: "
            f"{len(missing)} position(s) unreadable at indices {missing}. "
            f"Re-run discovery — raw web3 inspection may be needed if failures persist."
        )
        self.chain = chain
        self.npm = npm
        self.missing = missing


async def discover_lp_positions(
    client: GatewayClient,
    chain: str,
    wallet: str,
    include_zero_liquidity: bool = False,
    network: str = "",
    strict: bool = True,
) -> list[DiscoveredPosition]:
    """Discover all LP positions the wallet holds on NPMs registered for ``chain``.

    Walks every V3-fork NPM grouped under ``AbiFamily.V3_NPM`` for the chain
    (Uniswap V3, Agni, PancakeSwap V3, SushiSwap V3, ...), resolving each
    one's NonfungiblePositionManager address through the strategy-side
    ``AddressRegistry``. A wallet that opened positions on multiple V3-fork
    protocols on the same chain will have all of them surfaced. Additional
    protocols (e.g. Aerodrome CL, Uniswap V4) are surfaced automatically once
    they join ``AbiFamily.V3_NPM`` on the registry — they are deliberately
    absent today because their NPMs do not share this canonical ABI.

    Args:
        client: Connected GatewayClient — positions are read through the
            gateway's RpcService, keeping discovery on the same auth/routing
            path as the rest of the SDK.
        chain: Chain slug (e.g. ``"zerog"``, ``"base"``).
        wallet: EVM wallet address.
        include_zero_liquidity: When False (default) zero-liquidity NFTs —
            already withdrawn but not yet burned — are filtered out. Set True
            to surface them so operators can burn the residual NFTs.
        network: Per-request network override. Empty string (default) means
            "use the gateway's configured network", which is almost always
            what you want — the gateway already knows whether it's on
            mainnet or anvil. Only set this when you explicitly need to
            override (e.g. discovering mainnet state from an anvil gateway
            for diagnostics).
        strict: When True (default) raise ``DiscoveryIncomplete`` if any
            position the NPM reports cannot be read after retries. When
            False, log a loud warning and return the partial list — only
            appropriate for diagnostics, never for teardown execution.

    Returns:
        List of DiscoveredPosition ordered by (protocol, NPM address, tokenId).
        Empty list if no NPM is registered for the chain or the wallet holds
        no positions.

    Raises:
        DiscoveryIncomplete: If ``strict`` and any per-position read failed.
    """
    npms = _npms_for_chain(chain)
    if not npms:
        logger.info("No V3-fork NPMs registered for chain=%s; skipping LP discovery", chain)
        return []

    discovered: list[DiscoveredPosition] = []

    for protocol, npm in npms:
        count = await _balance_of(client, chain, npm, wallet, network=network)
        if count is None:
            # balanceOf itself is unreadable. Strict mode must NOT silently
            # skip the NPM — that's the whole point of strict mode. Raise
            # with missing=[] to signal "we don't even know how many
            # positions there are" (CodeRabbit critical, PR #1522).
            if strict:
                raise DiscoveryIncomplete(chain=chain, npm=npm, missing=[])
            logger.warning(
                "balanceOf unreadable on %s/%s NPM — skipping this protocol. "
                "Teardown may leave %s's positions on this NPM orphaned.",
                chain,
                protocol,
                wallet,
            )
            continue
        if count == 0:
            continue
        if count > _MAX_POSITIONS_PER_NPM:
            # Truncating silently in strict mode would bypass the whole point
            # of strict mode (CodeRabbit major, PR #1522). Record the
            # truncated indices as "missing" so the subsequent
            # DiscoveryIncomplete below fires, or surface a loud warning in
            # non-strict mode.
            truncated = list(range(_MAX_POSITIONS_PER_NPM, count))
            logger.warning(
                "Wallet %s owns %d positions on %s/%s NPM — capping discovery at %d. Positions beyond the cap will %s.",
                wallet,
                count,
                chain,
                protocol,
                _MAX_POSITIONS_PER_NPM,
                "raise DiscoveryIncomplete" if strict else "be skipped",
            )
            count = _MAX_POSITIONS_PER_NPM
            if strict:
                raise DiscoveryIncomplete(chain=chain, npm=npm, missing=truncated)

        missing_indices: list[int] = []

        for i in range(count):
            token_id, _ = await _call_with_retries(
                f"tokenOfOwnerByIndex({wallet},{i}) on {protocol}/{chain}",
                _token_of_owner_by_index,
                client,
                chain,
                npm,
                wallet,
                i,
                network=network,
            )
            if token_id is None:
                missing_indices.append(i)
                continue

            position, _ = await _call_with_retries(
                f"positions({token_id}) on {protocol}/{chain}",
                _read_position,
                client,
                chain,
                npm,
                token_id,
                network=network,
                protocol=protocol,
            )
            if position is None:
                missing_indices.append(i)
                continue

            if position.liquidity == 0 and not include_zero_liquidity:
                logger.debug(
                    "Skipping burned/withdrawn NFT #%d on %s/%s (liquidity=0)",
                    token_id,
                    chain,
                    protocol,
                )
                continue
            discovered.append(position)

        if missing_indices:
            if strict:
                raise DiscoveryIncomplete(chain=chain, npm=npm, missing=missing_indices)
            logger.warning(
                "Partial discovery on %s/%s: %d position(s) unreadable at indices %s. "
                "Teardown may leave positions orphaned.",
                chain,
                protocol,
                len(missing_indices),
                missing_indices,
            )

    return discovered


def to_teardown_summary(
    deployment_id: str,
    chain: str,
    positions: list[DiscoveredPosition],
) -> TeardownPositionSummary:
    """Adapt discovered on-chain positions to a TeardownPositionSummary.

    ``value_usd`` is left at zero because converting on-chain ``liquidity`` +
    tick-range into a USD value requires real-time per-pool pricing and
    V3-math that this discovery primitive deliberately doesn't implement —
    it's read-only, pure RPC. Each position's ``details`` carries a
    ``value_usd_unknown=True`` flag so downstream consumers
    (SafetyGuard, CLI preview) can distinguish "discovered but unvalued"
    from a legitimate zero-value position.

    **IMPORTANT for safety calculations**: ``calculate_max_acceptable_loss``
    returns the most permissive 3% cap for ``total_value_usd < $50k``.
    When every position has ``value_usd=0`` the summary's total is $0, so
    SafetyGuard applies the 3% cap to every position, which is *looser*
    than the 0.25% a real $1M LP would get. Callers of the ``--discover``
    flow MUST surface this to the operator (the CLI emits a WARNING
    banner) and should consider requiring ``--force`` or an explicit
    price override before executing (CodeRabbit major, PR #1522).
    """
    infos: list[PositionInfo] = []
    for p in positions:
        infos.append(
            PositionInfo(
                position_type=PositionType.LP,
                position_id=str(p.token_id),
                chain=chain,
                protocol=p.protocol,
                value_usd=Decimal("0"),
                details={
                    "token0": p.token0,
                    "token1": p.token1,
                    "fee": p.fee,
                    "tick_lower": p.tick_lower,
                    "tick_upper": p.tick_upper,
                    "liquidity": str(p.liquidity),
                    "npm_address": p.npm_address,
                    "discovered_on_chain": True,
                    # Sentinel: SafetyGuard / CLI can check this before
                    # applying value-based loss caps. A real zero-value
                    # position would NOT have this flag.
                    "value_usd_unknown": True,
                },
            )
        )
    return TeardownPositionSummary(
        deployment_id=deployment_id,
        timestamp=datetime.now(UTC),
        positions=infos,
    )


__all__ = [
    "DiscoveredPosition",
    "DiscoveryIncomplete",
    "discover_lp_positions",
    "npm_for_protocol",
    "to_teardown_summary",
]
