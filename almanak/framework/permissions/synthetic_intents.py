"""Synthetic intent factory for permission discovery.

Creates minimal valid intents for each (protocol, intent_type) pair.
These intents are compiled by the real IntentCompiler to discover which
contracts and function selectors each protocol uses -- without making
any RPC calls.
"""

from __future__ import annotations

import logging
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
from .constants import METAMORPHO_VAULTS
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
    # Note: Enso is excluded - it uses DELEGATECALL via the Enso delegate
    # contract, which is added statically by the generator (not via compilation).
}
_LP_PROTOCOLS = {
    "uniswap_v3",
    "pancakeswap_v3",
    "sushiswap_v3",
    "aerodrome",
    "traderjoe_v2",
}
_LENDING_PROTOCOLS = {"aave_v3", "morpho_blue", "spark", "compound_v3"}
_PERP_PROTOCOLS = {"gmx_v2"}
_FLASH_LOAN_PROVIDERS = {"aave", "balancer"}


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
    # Check that this protocol has a router on this chain
    if protocol not in ("enso", "curve", "pendle"):
        routers = PROTOCOL_ROUTERS.get(chain, {})
        if protocol not in routers:
            return []
    # Some protocols need specific token pairs (e.g., Curve stablecoin pools,
    # Pendle PT tokens). Use hints override when available.
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


def _build_supply_intents(protocol: str, chain: str, usdc: str) -> list[AnyIntent]:
    if protocol not in _LENDING_PROTOCOLS:
        return []
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


def _build_withdraw_intents(protocol: str, chain: str, usdc: str) -> list[AnyIntent]:
    if protocol not in _LENDING_PROTOCOLS:
        return []
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


def _build_repay_intents(protocol: str, chain: str, usdc: str) -> list[AnyIntent]:
    if protocol not in _LENDING_PROTOCOLS:
        return []
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
    if protocol != "metamorpho":
        return []
    vault_info = METAMORPHO_VAULTS.get(chain)
    if not vault_info:
        return []
    return [
        VaultDepositIntent(
            protocol="metamorpho",
            vault_address=vault_info["vault"],
            amount=Decimal("100"),
            chain=chain,
        )
    ]


def _build_vault_redeem_intents(protocol: str, chain: str) -> list[AnyIntent]:
    if protocol != "metamorpho":
        return []
    vault_info = METAMORPHO_VAULTS.get(chain)
    if not vault_info:
        return []
    return [
        VaultRedeemIntent(
            protocol="metamorpho",
            vault_address=vault_info["vault"],
            shares=Decimal("100"),
            chain=chain,
        )
    ]
