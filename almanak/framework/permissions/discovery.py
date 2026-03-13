"""Compilation-based permission discovery.

Runs the real IntentCompiler with synthetic intents to discover which
contracts and function selectors each (protocol, intent_type) combination
uses. This ensures zero drift between the compiler and the permission
manifest -- any new selector added to the compiler is automatically
picked up here.
"""

from __future__ import annotations

import logging
from typing import Literal, cast

from ..intents.compiler import (
    AAVE_BORROW_SELECTOR,
    AAVE_FLASH_LOAN_SELECTOR,
    AAVE_FLASH_LOAN_SIMPLE_SELECTOR,
    AAVE_REPAY_SELECTOR,
    AAVE_SET_COLLATERAL_SELECTOR,
    AAVE_SUPPLY_SELECTOR,
    AAVE_WITHDRAW_SELECTOR,
    BALANCER_FLASH_LOAN_SELECTOR,
    DEFAULT_SWAP_FEE_TIER,
    ERC20_ALLOWANCE_SELECTOR,
    ERC20_APPROVE_SELECTOR,
    ERC20_TRANSFER_FROM_SELECTOR,
    ERC20_TRANSFER_SELECTOR,
    NFT_POSITION_BURN_SELECTOR,
    NFT_POSITION_COLLECT_SELECTOR,
    NFT_POSITION_DECREASE_SELECTOR,
    NFT_POSITION_INCREASE_SELECTOR,
    NFT_POSITION_MINT_SELECTOR,
    SWAP_FEE_TIERS,
    IntentCompiler,
    IntentCompilerConfig,
)
from .hints import get_permission_hints
from .models import ContractPermission, FunctionPermission
from .synthetic_intents import build_synthetic_intents

logger = logging.getLogger(__name__)

# Base selector labels from compiler constants.
# Protocol-specific labels are merged at runtime from PermissionHints.
_BASE_SELECTOR_LABELS: dict[str, str] = {
    # ERC-20
    ERC20_APPROVE_SELECTOR: "approve(address,uint256)",
    ERC20_ALLOWANCE_SELECTOR: "allowance(address,address)",
    ERC20_TRANSFER_SELECTOR: "transfer(address,uint256)",
    ERC20_TRANSFER_FROM_SELECTOR: "transferFrom(address,address,uint256)",
    # Uniswap V3 NonfungiblePositionManager
    NFT_POSITION_MINT_SELECTOR: "mint(MintParams)",
    NFT_POSITION_INCREASE_SELECTOR: "increaseLiquidity(IncreaseLiquidityParams)",
    NFT_POSITION_DECREASE_SELECTOR: "decreaseLiquidity(DecreaseLiquidityParams)",
    NFT_POSITION_COLLECT_SELECTOR: "collect(CollectParams)",
    NFT_POSITION_BURN_SELECTOR: "burn(uint256)",
    # Aave V3 Pool
    AAVE_SUPPLY_SELECTOR: "supply(address,uint256,address,uint16)",
    AAVE_BORROW_SELECTOR: "borrow(address,uint256,uint256,uint16,address)",
    AAVE_REPAY_SELECTOR: "repay(address,uint256,uint256,address)",
    AAVE_WITHDRAW_SELECTOR: "withdraw(address,uint256,address)",
    AAVE_SET_COLLATERAL_SELECTOR: "setUserUseReserveAsCollateral(address,bool)",
    AAVE_FLASH_LOAN_SELECTOR: "flashLoan(address,address[],uint256[],uint256[],address,bytes,uint16)",
    AAVE_FLASH_LOAN_SIMPLE_SELECTOR: "flashLoanSimple(address,address,uint256,bytes,uint16)",
    # Balancer Vault
    BALANCER_FLASH_LOAN_SELECTOR: "flashLoan(address,address[],uint256[],bytes)",
    # Uniswap V3 SwapRouter selectors (defined inline in compiler, not as constants)
    "0x04e45aaf": "exactInputSingle(ExactInputSingleParams)",  # SwapRouter02 (7-param, no deadline)
    "0x414bf389": "exactInputSingle(ExactInputSingleParams)",  # SwapRouter V1 (8-param, with deadline)
}


def _build_selector_labels(protocols: list[str]) -> dict[str, str]:
    """Merge base labels with per-protocol labels from PermissionHints."""
    labels = dict(_BASE_SELECTOR_LABELS)
    for protocol in protocols:
        hints = get_permission_hints(protocol)
        labels.update(hints.selector_labels)
    return labels


def discover_permissions(
    chain: str,
    protocols: list[str],
    intent_types: list[str],
) -> tuple[list[ContractPermission], list[str]]:
    """Discover required permissions by compiling synthetic intents.

    For each (protocol, intent_type) combination, creates a synthetic
    intent and runs the real IntentCompiler to find out which contracts
    and function selectors are needed.

    Args:
        chain: Target chain name
        protocols: List of protocol names
        intent_types: List of intent type strings

    Returns:
        Tuple of (permissions_list, warnings_list)
    """
    # Build selector label map once, merging base labels with protocol hints
    selector_labels = _build_selector_labels(protocols)

    # Accumulator: target_address -> {selectors, label, send_allowed}
    targets: dict[str, _TargetAccumulator] = {}
    warnings: list[str] = []

    # Cache compilers by (pool_selection_mode, fee_tier) to avoid re-creating them
    _compilers: dict[tuple[str, int], IntentCompiler] = {}

    def _get_compiler(protocol: str) -> IntentCompiler:
        """Get or create a compiler configured for this protocol.

        Protocols with entries in SWAP_FEE_TIERS use "fixed" mode with
        the correct default tier. Protocols without fee tiers (TraderJoe V2,
        Aerodrome, etc.) use "heuristic" mode which handles missing tiers
        gracefully. Per-chain fee tier overrides from PermissionHints take
        precedence (e.g., Agni Finance on mantle uses tier 500).
        """
        hints = get_permission_hints(protocol)
        chain_fee_override = hints.synthetic_fee_tier.get(chain)

        fee_tiers = SWAP_FEE_TIERS.get(protocol)
        if fee_tiers:
            mode = "fixed"
            fee_tier = chain_fee_override or DEFAULT_SWAP_FEE_TIER.get(protocol, fee_tiers[0])
        else:
            mode = "auto"
            fee_tier = chain_fee_override or 3000

        key = (mode, fee_tier)
        if key not in _compilers:
            _compilers[key] = IntentCompiler(
                chain=chain,
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,
                    swap_pool_selection_mode=cast(Literal["auto", "fixed"], mode),
                    fixed_swap_fee_tier=fee_tier,
                ),
            )
        return _compilers[key]

    for protocol in protocols:
        # Inject static permissions from hints (for protocols that can't compile offline)
        hints = get_permission_hints(protocol)
        chain_static = hints.static_permissions.get(chain, [])
        for entry in chain_static:
            target = entry.target.lower()
            if target not in targets:
                targets[target] = _TargetAccumulator(label=entry.label)
            acc = targets[target]
            for sel in entry.selectors:
                acc.selectors.add(sel)
            if entry.send_allowed:
                acc.send_allowed = True
            # Add selector labels from static entries
            for sel, label in entry.selectors.items():
                selector_labels[sel] = label

        compiler = _get_compiler(protocol)

        for intent_type in intent_types:
            synthetic_intents = build_synthetic_intents(protocol, intent_type, chain)
            if not synthetic_intents:
                continue

            for intent in synthetic_intents:
                try:
                    result = compiler.compile(intent)
                except Exception as exc:
                    warnings.append(f"Compilation error for {protocol}/{intent_type} on {chain}: {exc}")
                    continue

                if result.status.value != "SUCCESS":
                    # Some protocol/chain combos legitimately don't compile
                    # (e.g., protocol not deployed on this chain).
                    # Only warn if there's an unexpected error.
                    if result.error and "not supported" not in result.error.lower():
                        warnings.append(f"Compilation failed for {protocol}/{intent_type} on {chain}: {result.error}")
                    continue

                # Extract permissions from compiled transactions
                for tx in result.transactions:
                    target = tx.to.lower()
                    selector = tx.data[:10] if tx.data and len(tx.data) >= 10 else None
                    sends_eth = tx.value > 0

                    if target not in targets:
                        targets[target] = _TargetAccumulator(
                            label=_derive_label(tx.tx_type, protocol, target),
                        )

                    acc = targets[target]
                    if selector:
                        acc.selectors.add(selector)
                    if sends_eth:
                        acc.send_allowed = True

    # Convert accumulators to ContractPermission objects
    permissions = []
    for address, acc in sorted(targets.items()):
        permissions.append(
            ContractPermission(
                target=address,
                label=acc.label,
                operation=0,  # CALL -- infrastructure overrides to DELEGATECALL later
                send_allowed=acc.send_allowed,
                function_selectors=sorted(
                    [FunctionPermission(selector=s, label=selector_labels.get(s, s)) for s in acc.selectors],
                    key=lambda fp: fp.selector,
                ),
            )
        )

    return permissions, warnings


class _TargetAccumulator:
    """Accumulates permission data for a single target contract."""

    __slots__ = ("label", "selectors", "send_allowed")

    def __init__(self, label: str) -> None:
        self.label = label
        self.selectors: set[str] = set()
        self.send_allowed = False


def _derive_label(tx_type: str, protocol: str, target: str = "") -> str:
    """Derive a human-readable label from transaction metadata.

    Attempts to produce labels like "Uniswap V3 SwapRouter" or
    "Aave V3 Pool" from the compiler's description strings.
    """
    # Use tx_type + protocol for a reasonable label
    protocol_display = protocol.replace("_", " ").title()
    if tx_type == "approve":
        short = f"{target[:6]}...{target[-4:]}" if len(target) >= 10 else target
        return f"ERC-20 ({short})"
    return f"{protocol_display} ({tx_type})"
