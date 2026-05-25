"""Synthetic intent factory for permission discovery.

Creates minimal valid intents for each (protocol, intent_type) pair.
These intents are compiled by the real IntentCompiler to discover which
contracts and function selectors each protocol uses -- without making
any RPC calls.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from decimal import Decimal
from typing import Literal, cast

from ..intents.compiler import (
    _CHAIN_NATIVE_SYMBOLS,
    CHAIN_TOKENS,
    DEFAULT_SWAP_FEE_TIER,
    LENDING_POOL_ADDRESSES,
    LP_POSITION_MANAGERS,
    PROTOCOL_ROUTERS,
    SWAP_FEE_TIERS,
)
from ..intents.vocabulary import (
    AnyIntent,
    BorrowIntent,
    CollectFeesIntent,
    FlashLoanIntent,
    IntentType,
    LPCloseIntent,
    LPOpenIntent,
    PerpCloseIntent,
    PerpOpenIntent,
    RepayIntent,
    SupplyIntent,
    SwapIntent,
    VaultDepositIntent,
    VaultRedeemIntent,
    WithdrawIntent,
)
from .constants import VAULT_PROTOCOL_REPRESENTATIVE
from .hints import (
    DiscoveryContext,
    PermissionHints,
    get_discovery_vectors_override,
    get_permission_hints,
)

logger = logging.getLogger(__name__)

# Protocols that support each intent type
_SWAP_PROTOCOLS = {
    "uniswap_v3",
    "pancakeswap_v3",
    "sushiswap_v3",
    "camelot",
    "aerodrome",
    "traderjoe_v2",
    "pendle",
    "curve",
    # Note: Enso is excluded - its Router address is per-chain and added
    # statically by the generator (not via synthetic intent compilation).
    # Note: Camelot (Algebra V3 on Arbitrum) is included for SWAP only —
    # CamelotCompiler ships SWAP-only with fail-closed LP / collect stubs
    # per docs/internal/plans/camelot-compiler-connector-folding-plan.md.
    # Not in _NATIVE_IN_SWAP_PROTOCOLS: no native-in SWAP intent test today.
}
# Protocols whose SwapRouter wraps the chain's native gas token via msg.value
# (no ERC-20 approve, single value-bearing tx). Emitting an additional
# native-input synthetic intent for these flips ``send_allowed=True`` on the
# router target, which Zodiac Roles requires for a value-bearing call to
# pass authorisation. Without this, native-in tests (e.g. native MATIC →
# USDC on polygon) compile fine but fail authz at execTransactionWithRole
# because the manifest target was discovered via a value-less synthetic.
_NATIVE_IN_SWAP_PROTOCOLS = {
    "uniswap_v3",
    "pancakeswap_v3",
    "sushiswap_v3",
}
_LP_PROTOCOLS = {
    "uniswap_v3",
    "pancakeswap_v3",
    "sushiswap_v3",
    "aerodrome",
    "aerodrome_slipstream",
    "traderjoe_v2",
    "pendle",
}
_LENDING_PROTOCOLS = {"aave_v3", "morpho_blue", "spark", "compound_v3", "radiant_v2"}
_PERP_PROTOCOLS = {"gmx_v2", "aster_perps", "pancakeswap_perps"}
_FLASH_LOAN_PROVIDERS = {"aave", "balancer"}


def get_protocol_intent_matrix() -> dict[str, frozenset[IntentType]]:
    """Return ``{protocol: frozenset[IntentType]}`` for every pair the manifest
    generator covers via synthetic intent compilation.

    Authoritative source for any caller that needs to enumerate the full set of
    ``(protocol, intent_type)`` pairs whose generated permissions must be
    exercised on-chain. Single source of truth — keep this function and the
    ``_build_*_intents`` dispatch in ``build_synthetic_intents`` in lockstep.

    Excludes pairs that bypass the synthetic-discovery path:
    - ``BRIDGE`` (bridge connectors are not yet wired through the generator)
    - ``WRAP_NATIVE`` / ``UNWRAP_NATIVE`` (infra, not protocol-specific)
    - ``FLASH_LOAN`` (provider strings ``aave`` / ``balancer`` are not
      connector-directory names, so they need separate handling)
    """
    matrix: dict[str, set[IntentType]] = defaultdict(set)
    for proto in _SWAP_PROTOCOLS:
        matrix[proto].add(IntentType.SWAP)
    for proto in _LP_PROTOCOLS:
        matrix[proto].update({IntentType.LP_OPEN, IntentType.LP_CLOSE})
        if get_permission_hints(proto).supports_standalone_fee_collection:
            matrix[proto].add(IntentType.LP_COLLECT_FEES)
    for proto in _LENDING_PROTOCOLS:
        matrix[proto].update(
            {
                IntentType.SUPPLY,
                IntentType.WITHDRAW,
                IntentType.BORROW,
                IntentType.REPAY,
            }
        )
    for proto in _PERP_PROTOCOLS:
        matrix[proto].update({IntentType.PERP_OPEN, IntentType.PERP_CLOSE})
    for proto, chains in VAULT_PROTOCOL_REPRESENTATIVE.items():
        if chains:
            matrix[proto].update({IntentType.VAULT_DEPOSIT, IntentType.VAULT_REDEEM})
    return {proto: frozenset(types) for proto, types in matrix.items()}


def _get_token_pair(chain: str) -> tuple[str, str]:
    """Get a known token pair for a chain (usdc, weth-equivalent).

    Returns addresses from CHAIN_TOKENS in the compiler, falling back
    to well-known USDC/WETH addresses for arbitrum.
    """
    tokens = CHAIN_TOKENS.get(chain, {})
    usdc = tokens.get("usdc", "0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
    weth = tokens.get("weth") or tokens.get("wavax") or tokens.get("wbnb", "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1")
    return usdc, weth


def _resolve_lp_pair(hints: PermissionHints, chain: str) -> tuple[str, str]:
    """Return the (tokenA, tokenB) pair to seed synthetic LP discovery on
    ``chain`` given the protocol's already-resolved ``hints``.

    Per-protocol override via ``PermissionHints.synthetic_lp_pair`` takes
    precedence over the chain-default pair from ``_get_token_pair``.

    Required for chains where the framework's chain-default pair (e.g. bsc's
    ``(USDC, ETH-bridged)``) does not match the canonical liquid LP pair
    actually used by the protocol on that chain (e.g. sushiswap_v3 on bsc
    uses ``(USDT, WBNB)``). Without an override, the synthetic discovery
    emits approves on the wrong tokens and any test that LP-opens on the
    real pair fails Zodiac authorisation. Surfaced by #1902.

    Takes ``hints`` directly (instead of looking it up by protocol) because
    every caller already resolves ``PermissionHints`` for its own purposes
    in the same function — passing the object in avoids a redundant
    ``importlib`` lookup per LP-helper invocation.
    """
    override = hints.synthetic_lp_pair.get(chain)
    if override:
        return override
    return _get_token_pair(chain)


def build_synthetic_intents(
    protocol: str,
    intent_type: str,
    chain: str,
) -> list[AnyIntent]:
    """Build synthetic intents for a (protocol, intent_type) combination.

    Returns a list because some combinations need multiple intents to
    cover all code paths (e.g., LP_CLOSE produces decrease + collect + burn).

    Args:
        protocol: Protocol name (e.g., "uniswap_v3", "aave_v3")
        intent_type: Intent type string (e.g., "SWAP", "SUPPLY")
        chain: Target chain name

    Returns:
        List of synthetic intents. Empty if the combination is not supported.
    """
    try:
        it = IntentType(intent_type)
    except ValueError:
        logger.debug("Unknown intent type: %s", intent_type)
        return []

    protocol_lower = protocol.lower()
    usdc, weth = _get_token_pair(chain)

    if it == IntentType.SWAP:
        return _build_swap_intents(protocol_lower, chain, usdc, weth)
    elif it == IntentType.LP_OPEN:
        return _build_lp_open_intents(protocol_lower, chain)
    elif it == IntentType.LP_CLOSE:
        return _build_lp_close_intents(protocol_lower, chain)
    elif it == IntentType.LP_COLLECT_FEES:
        return _build_lp_collect_fees_intents(protocol_lower, chain)
    elif it == IntentType.SUPPLY:
        return _build_supply_intents(protocol_lower, chain, usdc, weth)
    elif it == IntentType.WITHDRAW:
        return _build_withdraw_intents(protocol_lower, chain, usdc, weth)
    elif it == IntentType.BORROW:
        return _build_borrow_intents(protocol_lower, chain, usdc, weth)
    elif it == IntentType.REPAY:
        return _build_repay_intents(protocol_lower, chain, usdc, weth)
    elif it == IntentType.PERP_OPEN:
        return _build_perp_open_intents(protocol_lower, chain, usdc)
    elif it == IntentType.PERP_CLOSE:
        return _build_perp_close_intents(protocol_lower, chain, usdc)
    elif it == IntentType.FLASH_LOAN:
        return _build_flash_loan_intents(protocol_lower, chain, usdc)
    elif it == IntentType.VAULT_DEPOSIT:
        return _build_vault_deposit_intents(protocol_lower, chain)
    elif it == IntentType.VAULT_REDEEM:
        return _build_vault_redeem_intents(protocol_lower, chain)
    else:
        logger.debug("Intent type %s not supported for permission discovery", intent_type)
        return []


def _build_swap_intents(protocol: str, chain: str, usdc: str, weth: str) -> list[AnyIntent]:
    if protocol not in _SWAP_PROTOCOLS:
        return []
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "SWAP", chain, ctx)
        if result is not None:
            return result
    # Check that this protocol has a router on this chain.
    # Protocols with dedicated swap compile paths (enso, pendle,
    # traderjoe_v2) are exempt because their router address is not stored in
    # PROTOCOL_ROUTERS -- the compiler resolves it from protocol-specific
    # registries (LP_POSITION_MANAGERS for TJv2's LBRouter, the connector's
    # own module for Enso/Pendle). Their dedicated compile path
    # returns FAILED with "not supported" for unsupported chains, which
    # discover_permissions() treats as a non-fatal skip.
    if protocol not in ("enso", "pendle", "traderjoe_v2"):
        routers = PROTOCOL_ROUTERS.get(chain, {})
        if protocol not in routers:
            return []
    # Some protocols need specific token pairs (e.g., Pendle PT tokens).
    # Use hints override when available.
    hints = get_permission_hints(protocol)
    from_token, to_token = usdc, weth
    if hints.synthetic_swap_pair:
        pair = hints.synthetic_swap_pair.get(chain)
        if pair:
            from_token, to_token = pair
    intents: list[AnyIntent] = [
        SwapIntent(
            from_token=from_token,
            to_token=to_token,
            amount=Decimal("1"),
            protocol=protocol,
            chain=chain,
        )
    ]
    # For V3-style routers that support native-in via msg.value (auto-WETH9
    # wrap on the SwapRouter02 ABI), emit a second native→USDC synthetic so
    # ``tx.value > 0`` flips ``send_allowed=True`` on the router target.
    # Without this, the manifest authorises the selector but rejects every
    # value-bearing call at execTransactionWithRole.
    native_symbols = _CHAIN_NATIVE_SYMBOLS.get(chain, frozenset())
    if protocol in _NATIVE_IN_SWAP_PROTOCOLS and native_symbols:
        # ``frozenset`` iteration order is process-stable but
        # implementation-defined. On chains with multiple aliases (polygon
        # exposes ``{"MATIC", "POL"}``), ``next(iter(...))`` could pick
        # either, making the synthetic's ``from_token`` flap across Python
        # versions / interpreter builds and producing a non-deterministic
        # discovery output. ``sorted(...)[0]`` pins the alias
        # lexicographically — for polygon that's ``"MATIC"`` (the
        # historically tested symbol). The compiler accepts both via
        # ``_CHAIN_NATIVE_SYMBOLS`` so functional behaviour is unchanged.
        native_symbol = sorted(native_symbols)[0]
        intents.append(
            SwapIntent(
                from_token=native_symbol,
                to_token=usdc,
                amount=Decimal("0.01"),
                protocol=protocol,
                chain=chain,
            )
        )
    return intents


def _build_lp_open_intents(protocol: str, chain: str) -> list[AnyIntent]:
    if protocol not in _LP_PROTOCOLS:
        return []
    # Override hook runs BEFORE the LP_POSITION_MANAGERS gate so a connector
    # that owns its discovery via ``build_discovery_vectors`` is never blocked
    # by the legacy hardcoded registry — same ordering as the lending builders
    # (see commit 30b0b0e80 and ``TestOverrideBypassesLendingPoolGate``).
    # Pendle is the canonical case: it uses single-sided LP into a market
    # contract (no NFT position manager), so it's deliberately absent from
    # LP_POSITION_MANAGERS and relies on the override hook firing first.
    # ``ctx.usdc`` / ``ctx.weth`` carry the chain-default token pair so the
    # callee doesn't have to re-import ``_get_token_pair``.
    usdc, weth = _get_token_pair(chain)
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "LP_OPEN", chain, ctx)
        if result is not None:
            return result
    managers = LP_POSITION_MANAGERS.get(chain, {})
    if protocol not in managers:
        return []
    hints = get_permission_hints(protocol)
    # Resolve the LP pair via the per-protocol override registry — the chain
    # default (USDC, WETH-equivalent) is wrong on chains where the framework's
    # ``_get_token_pair`` resolves to a non-liquid pair for the protocol on
    # that chain (e.g. sushiswap_v3 on bsc → (USDT, WBNB), not (USDC, ETH-bsc)).
    token0, token1 = _resolve_lp_pair(hints, chain)
    # Include fee tier in pool string for protocols that use Uniswap V3-style
    # fee tiers (parsed by _parse_pool_info which defaults to 3000).
    # Protocols with their own pool format (TraderJoe V2 bins, Aerodrome
    # volatile/stable) omit the fee tier so their parsers use the correct default.
    if protocol in SWAP_FEE_TIERS or hints.synthetic_fee_tier.get(chain):
        fee_tier = hints.synthetic_fee_tier.get(chain) or DEFAULT_SWAP_FEE_TIER.get(protocol, 3000)
        pool = f"{token0}/{token1}/{fee_tier}"
    else:
        pool = f"{token0}/{token1}"
    return [
        LPOpenIntent(
            pool=pool,
            amount0=Decimal("100"),
            amount1=Decimal("0.05"),
            range_lower=Decimal("1500"),
            range_upper=Decimal("4000"),
            protocol=protocol,
            chain=chain,
        )
    ]


def _build_lp_close_intents(protocol: str, chain: str) -> list[AnyIntent]:
    if protocol not in _LP_PROTOCOLS:
        return []
    # Override hook runs BEFORE the LP_POSITION_MANAGERS gate — see
    # ``_build_lp_open_intents`` for the canonical placement rationale.
    usdc, weth = _get_token_pair(chain)
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "LP_CLOSE", chain, ctx)
        if result is not None:
            return result
    managers = LP_POSITION_MANAGERS.get(chain, {})
    if protocol not in managers:
        return []
    hints = get_permission_hints(protocol)
    token0, token1 = _resolve_lp_pair(hints, chain)
    position_id = hints.synthetic_position_id.format(token0=token0, token1=token1)
    return [
        LPCloseIntent(
            position_id=position_id,
            protocol=protocol,
            chain=chain,
        )
    ]


def _build_lp_collect_fees_intents(protocol: str, chain: str) -> list[AnyIntent]:
    hints = get_permission_hints(protocol)
    if not hints.supports_standalone_fee_collection:
        return []
    managers = LP_POSITION_MANAGERS.get(chain, {})
    if protocol not in managers:
        return []
    token0, token1 = _resolve_lp_pair(hints, chain)
    return [
        CollectFeesIntent(
            pool=f"{token0}/{token1}",
            protocol=protocol,
            chain=chain,
        )
    ]


def _build_supply_intents(protocol: str, chain: str, usdc: str, weth: str) -> list[AnyIntent]:
    if protocol not in _LENDING_PROTOCOLS:
        return []
    # Override hook runs BEFORE the chain/pool gate below so a connector that
    # owns its discovery via ``build_discovery_vectors`` is never blocked by
    # the legacy hardcoded singleton exception list. Mirrors
    # ``_build_swap_intents``.
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "SUPPLY", chain, ctx)
        if result is not None:
            return result
    # Check lending pool exists for this chain
    pools = LENDING_POOL_ADDRESSES.get(chain, {})
    if protocol not in pools and protocol not in ("morpho_blue", "compound_v3"):
        return []
    hints = get_permission_hints(protocol)

    return [
        SupplyIntent(
            protocol=protocol,
            token=usdc,
            amount=Decimal("100"),
            chain=chain,
            market_id=hints.synthetic_market_id,
        )
    ]


def _build_withdraw_intents(protocol: str, chain: str, usdc: str, weth: str) -> list[AnyIntent]:
    if protocol not in _LENDING_PROTOCOLS:
        return []
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "WITHDRAW", chain, ctx)
        if result is not None:
            return result
    pools = LENDING_POOL_ADDRESSES.get(chain, {})
    if protocol not in pools and protocol not in ("morpho_blue", "compound_v3"):
        return []
    hints = get_permission_hints(protocol)

    return [
        WithdrawIntent(
            protocol=protocol,
            token=usdc,
            amount=Decimal("50"),
            chain=chain,
            market_id=hints.synthetic_market_id,
        )
    ]


def _build_borrow_intents(protocol: str, chain: str, usdc: str, weth: str) -> list[AnyIntent]:
    if protocol not in _LENDING_PROTOCOLS:
        return []
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "BORROW", chain, ctx)
        if result is not None:
            return result
    pools = LENDING_POOL_ADDRESSES.get(chain, {})
    if protocol not in pools and protocol not in ("morpho_blue", "compound_v3"):
        return []
    hints = get_permission_hints(protocol)

    return [
        BorrowIntent(
            protocol=protocol,
            collateral_token=weth,
            collateral_amount=Decimal("1"),
            borrow_token=usdc,
            borrow_amount=Decimal("100"),
            chain=chain,
            market_id=hints.synthetic_market_id,
        )
    ]


def _build_repay_intents(protocol: str, chain: str, usdc: str, weth: str) -> list[AnyIntent]:
    if protocol not in _LENDING_PROTOCOLS:
        return []
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "REPAY", chain, ctx)
        if result is not None:
            return result
    pools = LENDING_POOL_ADDRESSES.get(chain, {})
    if protocol not in pools and protocol not in ("morpho_blue", "compound_v3"):
        return []
    hints = get_permission_hints(protocol)

    return [
        RepayIntent(
            protocol=protocol,
            token=usdc,
            amount=Decimal("50"),
            chain=chain,
            market_id=hints.synthetic_market_id,
        )
    ]


def _build_perp_open_intents(protocol: str, chain: str, usdc: str) -> list[AnyIntent]:
    if protocol not in _PERP_PROTOCOLS:
        return []
    intents: list[AnyIntent] = [
        PerpOpenIntent(
            market="ETH/USD",
            collateral_token=usdc,
            collateral_amount=Decimal("100"),
            size_usd=Decimal("500"),
            is_long=True,
            leverage=Decimal("5"),
            protocol=protocol,
            chain=chain,
        )
    ]
    # Aster Diamond (and PancakeSwap Perps which is broker_id=2 on the same
    # Diamond) exposes a separate ``openMarketTradeBNB`` selector (0xb7aeae66)
    # for native-BNB-collateral opens, distinct from ``openMarketTrade``
    # (0x703085c7) for ERC20 collateral. The ERC20 synthetic above only
    # authorises the ERC20 selector; without a native-collateral synthetic the
    # manifest blocks every native-margin open at execTransactionWithRole.
    if protocol in {"aster_perps", "pancakeswap_perps"} and chain == "bsc":
        intents.append(
            PerpOpenIntent(
                market="ETH/USD",
                collateral_token="BNB",
                collateral_amount=Decimal("0.5"),
                size_usd=Decimal("500"),
                is_long=True,
                leverage=Decimal("5"),
                protocol=protocol,
                chain=chain,
            )
        )
    return intents


def _build_perp_close_intents(protocol: str, chain: str, usdc: str) -> list[AnyIntent]:
    if protocol not in _PERP_PROTOCOLS:
        return []
    # Aster Diamond's PERP_CLOSE compile path requires ``position_id`` (a 0x-
    # prefixed bytes32 tradeHash). Without one the synthetic compile fails with
    # "PERP_CLOSE requires intent.position_id" and the manifest never sees the
    # ``closeTrade(bytes32)`` selector (0x5177fd3b). Use a placeholder hash —
    # the compiler validates shape, not on-chain existence.
    placeholder_trade_hash = "0x" + "00" * 32
    if protocol in {"aster_perps", "pancakeswap_perps"} and chain == "bsc":
        return [
            PerpCloseIntent(
                market="ETH/USD",
                collateral_token=usdc,
                is_long=True,
                size_usd=None,  # closeTrade(bytes32) is always full-close
                protocol=protocol,
                chain=chain,
                position_id=placeholder_trade_hash,
            )
        ]
    return [
        PerpCloseIntent(
            market="ETH/USD",
            collateral_token=usdc,
            is_long=True,
            size_usd=Decimal("500"),
            protocol=protocol,
            chain=chain,
        )
    ]


def _build_flash_loan_intents(protocol: str, chain: str, usdc: str) -> list[AnyIntent]:
    if protocol not in _FLASH_LOAN_PROVIDERS:
        return []
    _, weth = _get_token_pair(chain)
    # Flash loans require at least one callback intent
    callback = SwapIntent(
        from_token=usdc,
        to_token=weth,
        amount=Decimal("1"),
        protocol="uniswap_v3",
        chain=chain,
    )
    return [
        FlashLoanIntent(
            provider=cast(Literal["aave", "balancer", "morpho", "auto"], protocol),
            token=usdc,
            amount=Decimal("10000"),
            callback_intents=[callback],
            chain=chain,
        )
    ]


def _build_vault_deposit_intents(protocol: str, chain: str) -> list[AnyIntent]:
    vault_chains = VAULT_PROTOCOL_REPRESENTATIVE.get(protocol.lower())
    if not vault_chains:
        return []
    vault_info = vault_chains.get(chain)
    if not vault_info:
        return []
    return [
        VaultDepositIntent(
            protocol=protocol,
            vault_address=vault_info["vault"],
            amount=Decimal("100"),
            chain=chain,
        )
    ]


def _build_vault_redeem_intents(protocol: str, chain: str) -> list[AnyIntent]:
    vault_chains = VAULT_PROTOCOL_REPRESENTATIVE.get(protocol.lower())
    if not vault_chains:
        return []
    vault_info = vault_chains.get(chain)
    if not vault_info:
        return []
    return [
        VaultRedeemIntent(
            protocol=protocol,
            vault_address=vault_info["vault"],
            shares=Decimal("100"),
            chain=chain,
        )
    ]
