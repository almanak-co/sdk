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
from .hints import get_permission_hints

logger = logging.getLogger(__name__)

# Protocols that support each intent type
_SWAP_PROTOCOLS = {
    "uniswap_v3",
    "pancakeswap_v3",
    "sushiswap_v3",
    "aerodrome",
    "traderjoe_v2",
    "pendle",
    "curve",
    # Note: Enso is excluded - its Router address is per-chain and added
    # statically by the generator (not via synthetic intent compilation).
}
_LP_PROTOCOLS = {
    "uniswap_v3",
    "pancakeswap_v3",
    "sushiswap_v3",
    "aerodrome",
    "traderjoe_v2",
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
        return _build_lp_open_intents(protocol_lower, chain, usdc, weth)
    elif it == IntentType.LP_CLOSE:
        return _build_lp_close_intents(protocol_lower, chain)
    elif it == IntentType.LP_COLLECT_FEES:
        return _build_lp_collect_fees_intents(protocol_lower, chain)
    elif it == IntentType.SUPPLY:
        return _build_supply_intents(protocol_lower, chain, usdc)
    elif it == IntentType.WITHDRAW:
        return _build_withdraw_intents(protocol_lower, chain, usdc)
    elif it == IntentType.BORROW:
        return _build_borrow_intents(protocol_lower, chain, usdc, weth)
    elif it == IntentType.REPAY:
        return _build_repay_intents(protocol_lower, chain, usdc)
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
    # Curve is pool-specific: a single ``synthetic_swap_pair`` only authorises
    # one of the curated pools per chain (e.g. 3pool USDC/USDT on ethereum),
    # leaving every other registered pool — notably tricrypto2 — unauthorised
    # on the Safe (issue #1903). Iterate the curated registry instead so the
    # manifest covers the full surface a strategy author can route through.
    if protocol == "curve":
        return _build_curve_swap_intents(chain)
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
    return [
        SwapIntent(
            from_token=from_token,
            to_token=to_token,
            amount=Decimal("1"),
            protocol=protocol,
            chain=chain,
        )
    ]


def _build_curve_swap_intents(chain: str) -> list[AnyIntent]:
    """Emit one synthetic ``SwapIntent`` per curated curve pool on ``chain``.

    Curve pools are pair-specific (StableSwap, CryptoSwap, Tricrypto), so a
    single token pair only resolves to one pool. The compiler's
    ``compile_swap_curve`` walks ``CURVE_POOLS[chain]`` to match pool by
    coin pair; emitting one intent per registered pool — using the first
    two coin addresses of each — guarantees every pool's address lands on
    the manifest.

    The price-oracle gate in ``compile_swap_curve`` (price_ratio for
    CryptoSwap/Tricrypto pools) does NOT fire during permission discovery
    because ``IntentCompiler`` is created with ``allow_placeholder_prices=True``
    and ``_require_token_price`` returns the placeholder map (USDT=$1,
    WETH=$2000, WBTC=$45000, …) — every pool's coin pair resolves to a
    finite, positive price_ratio.

    For polygon's am3pool which sets ``use_underlying=True``, the compiler
    routes to ``exchange_underlying`` automatically based on the pool's
    pool_type; no special-casing is needed here.
    """
    try:
        from ..connectors.curve.adapter import CURVE_POOLS
    except ImportError:
        logger.debug("Curve adapter not importable; skipping synthetic swap discovery")
        return []

    chain_pools = CURVE_POOLS.get(chain, {})
    if not chain_pools:
        return []

    intents: list[AnyIntent] = []
    for pool_name, pool_data in chain_pools.items():
        coins = pool_data.get("coin_addresses") or []
        if len(coins) < 2:
            logger.warning(
                "Curve pool %s on %s has fewer than 2 coins; skipping synthetic discovery",
                pool_name,
                chain,
            )
            continue
        intents.append(
            SwapIntent(
                from_token=coins[0],
                to_token=coins[1],
                amount=Decimal("1"),
                protocol="curve",
                chain=chain,
            )
        )
    return intents


def _build_lp_open_intents(protocol: str, chain: str, usdc: str, weth: str) -> list[AnyIntent]:
    if protocol not in _LP_PROTOCOLS:
        return []
    managers = LP_POSITION_MANAGERS.get(chain, {})
    if protocol not in managers:
        return []
    # Include fee tier in pool string for protocols that use Uniswap V3-style
    # fee tiers (parsed by _parse_pool_info which defaults to 3000).
    # Protocols with their own pool format (TraderJoe V2 bins, Aerodrome
    # volatile/stable) omit the fee tier so their parsers use the correct default.
    hints = get_permission_hints(protocol)
    if protocol in SWAP_FEE_TIERS or hints.synthetic_fee_tier.get(chain):
        fee_tier = hints.synthetic_fee_tier.get(chain) or DEFAULT_SWAP_FEE_TIER.get(protocol, 3000)
        pool = f"{usdc}/{weth}/{fee_tier}"
    else:
        pool = f"{usdc}/{weth}"
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
    managers = LP_POSITION_MANAGERS.get(chain, {})
    if protocol not in managers:
        return []
    hints = get_permission_hints(protocol)
    usdc, weth = _get_token_pair(chain)
    position_id = hints.synthetic_position_id.format(token0=usdc, token1=weth)
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
    usdc, weth = _get_token_pair(chain)
    return [
        CollectFeesIntent(
            pool=f"{usdc}/{weth}",
            protocol=protocol,
            chain=chain,
        )
    ]


def _morpho_blue_synthetic_market_id(chain: str, fallback: str | None) -> str | None:
    """Return a valid synthetic market_id for morpho_blue on ``chain``.

    Morpho Blue markets are chain-specific: a market_id valid on ethereum
    will not resolve on arbitrum/base/polygon/monad. The adapter ships with
    a per-chain registry in ``MORPHO_MARKETS``; prefer its first entry for
    the requested chain so the compiler can actually build the supply tx.

    Falls back to ``fallback`` (the hint-level default, ethereum-tuned) only
    when the adapter registry has no entry for the chain.
    """
    try:
        from ..connectors.morpho_blue.adapter import MORPHO_MARKETS
    except ImportError:
        return fallback
    chain_markets = MORPHO_MARKETS.get(chain, {})
    if chain_markets:
        return next(iter(chain_markets))
    return fallback


def _morpho_blue_loan_token(chain: str, fallback: str) -> str:
    """Return the loan-token address for morpho_blue's synthetic market.

    The loan-token path (``supply`` with ``use_as_collateral=False``) requires
    ``intent.token`` to match the market's loan token. Using the chain default
    USDC can mismatch the selected market (e.g. polygon's first registered
    market is USDT-quoted), producing a compile failure that drops both flag
    variants from the manifest.
    """
    try:
        from ..connectors.morpho_blue.adapter import MORPHO_MARKETS
    except ImportError:
        return fallback
    chain_markets = MORPHO_MARKETS.get(chain, {})
    if not chain_markets:
        return fallback
    first_market = next(iter(chain_markets.values()))
    return first_market.get("loan_token_address") or fallback


def _morpho_blue_collateral_token(chain: str, fallback: str) -> str:
    """Return the collateral-token address for morpho_blue's synthetic market.

    Mirror of :func:`_morpho_blue_loan_token` for the collateral path
    (``supply`` with ``use_as_collateral=True`` / ``withdrawCollateral``).
    """
    try:
        from ..connectors.morpho_blue.adapter import MORPHO_MARKETS
    except ImportError:
        return fallback
    chain_markets = MORPHO_MARKETS.get(chain, {})
    if not chain_markets:
        return fallback
    first_market = next(iter(chain_markets.values()))
    return first_market.get("collateral_token_address") or fallback


def _build_supply_intents(protocol: str, chain: str, usdc: str) -> list[AnyIntent]:
    if protocol not in _LENDING_PROTOCOLS:
        return []
    # Check lending pool exists for this chain
    pools = LENDING_POOL_ADDRESSES.get(chain, {})
    if protocol not in pools and protocol not in ("morpho_blue", "compound_v3"):
        return []
    hints = get_permission_hints(protocol)

    # Morpho Blue routes SUPPLY on ``use_as_collateral``: True calls
    # ``supplyCollateral``, False calls ``supply``. The manifest needs BOTH
    # selectors (a strategy may supply loan-side or collateral-side depending
    # on intent), so we sweep the flag during discovery. Without this sweep,
    # only one of the two selectors lands on the manifest and the other path
    # reverts on the Safe.  See codex review 3135601928.
    if protocol == "morpho_blue":
        market_id = _morpho_blue_synthetic_market_id(chain, hints.synthetic_market_id)
        loan_token = _morpho_blue_loan_token(chain, usdc)
        collateral_token = _morpho_blue_collateral_token(chain, usdc)
        return [
            SupplyIntent(
                protocol=protocol,
                token=collateral_token,
                amount=Decimal("1"),
                chain=chain,
                market_id=market_id,
                use_as_collateral=True,
            ),
            SupplyIntent(
                protocol=protocol,
                token=loan_token,
                amount=Decimal("100"),
                chain=chain,
                market_id=market_id,
                use_as_collateral=False,
            ),
        ]

    return [
        SupplyIntent(
            protocol=protocol,
            token=usdc,
            amount=Decimal("100"),
            chain=chain,
            market_id=hints.synthetic_market_id,
        )
    ]


def _build_withdraw_intents(protocol: str, chain: str, usdc: str) -> list[AnyIntent]:
    if protocol not in _LENDING_PROTOCOLS:
        return []
    pools = LENDING_POOL_ADDRESSES.get(chain, {})
    if protocol not in pools and protocol not in ("morpho_blue", "compound_v3"):
        return []
    hints = get_permission_hints(protocol)

    # Morpho Blue routes WITHDRAW on ``is_collateral`` (True → withdrawCollateral,
    # False → withdraw). Sweep both flag variants so the manifest covers loan
    # reclamation AND collateral withdrawal. See codex review 3135601928.
    if protocol == "morpho_blue":
        market_id = _morpho_blue_synthetic_market_id(chain, hints.synthetic_market_id)
        loan_token = _morpho_blue_loan_token(chain, usdc)
        collateral_token = _morpho_blue_collateral_token(chain, usdc)
        return [
            WithdrawIntent(
                protocol=protocol,
                token=collateral_token,
                amount=Decimal("1"),
                chain=chain,
                market_id=market_id,
                is_collateral=True,
            ),
            WithdrawIntent(
                protocol=protocol,
                token=loan_token,
                amount=Decimal("50"),
                chain=chain,
                market_id=market_id,
                is_collateral=False,
            ),
        ]

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
    pools = LENDING_POOL_ADDRESSES.get(chain, {})
    if protocol not in pools and protocol not in ("morpho_blue", "compound_v3"):
        return []
    hints = get_permission_hints(protocol)

    # Morpho Blue markets are chain-specific in both their pair AND their id
    # (e.g. polygon's first registered market is WBTC/USDC, arbitrum/base use
    # wstETH/USDC). Resolving each through its helper aligns the synthetic
    # BorrowIntent with the same market the supply/withdraw paths discovered,
    # so the manifest authorises the actual collateral approve + Blue.borrow
    # selectors. Falling back to the chain-default ``weth`` declared the wrong
    # collateral, dropping both selectors from the manifest. See #1904.
    if protocol == "morpho_blue":
        market_id = _morpho_blue_synthetic_market_id(chain, hints.synthetic_market_id)
        loan_token = _morpho_blue_loan_token(chain, usdc)
        collateral_token = _morpho_blue_collateral_token(chain, weth)
        return [
            BorrowIntent(
                protocol=protocol,
                collateral_token=collateral_token,
                collateral_amount=Decimal("1"),
                borrow_token=loan_token,
                borrow_amount=Decimal("100"),
                chain=chain,
                market_id=market_id,
            )
        ]

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


def _build_repay_intents(protocol: str, chain: str, usdc: str) -> list[AnyIntent]:
    if protocol not in _LENDING_PROTOCOLS:
        return []
    pools = LENDING_POOL_ADDRESSES.get(chain, {})
    if protocol not in pools and protocol not in ("morpho_blue", "compound_v3"):
        return []
    hints = get_permission_hints(protocol)

    # Morpho Blue: resolve the loan token + market id from the per-chain registry
    # so the synthetic RepayIntent targets the same market as the borrow path.
    # Without this, polygon (USDT-quoted first market) would synthesise a USDC
    # repay against a WBTC/USDC market, mismatching the discovered borrow.
    if protocol == "morpho_blue":
        market_id = _morpho_blue_synthetic_market_id(chain, hints.synthetic_market_id)
        loan_token = _morpho_blue_loan_token(chain, usdc)
        return [
            RepayIntent(
                protocol=protocol,
                token=loan_token,
                amount=Decimal("50"),
                chain=chain,
                market_id=market_id,
            )
        ]

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
    return [
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


def _build_perp_close_intents(protocol: str, chain: str, usdc: str) -> list[AnyIntent]:
    if protocol not in _PERP_PROTOCOLS:
        return []
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
