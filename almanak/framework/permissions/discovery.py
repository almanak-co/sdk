"""Compilation-based permission discovery.

Runs the real IntentCompiler with synthetic intents to discover which
contracts and function selectors each (protocol, intent_type) combination
uses. This ensures zero drift between the compiler and the permission
manifest -- any new selector added to the compiler is automatically
picked up here.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Literal, cast

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.protocol_aliases import normalize_protocol
from almanak.core.intent_types import IntentType

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
from ..intents.compiler_models import CompilationResult, TransactionData
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


def _supported_intent_types_for(
    *,
    chain: str,
    protocol: str,
    intent_types: list[IntentType],
    warnings: list[str],
) -> list[IntentType] | None:
    """Narrow ``intent_types`` to what this connector actually supports here.

    Returns the admissible intent types, or ``None`` when the protocol should be
    skipped for this chain entirely. Appends a human-readable note to
    ``warnings`` for every cell it drops, so a thinner manifest is always
    traceable to a declaration rather than looking like a discovery failure.

    A protocol with no descriptor, or one without strategy support, is passed
    through unnarrowed: the descriptor is the authority on strategy-authored
    coverage, and when there is no descriptor there is nothing to check
    against. Narrowing those to the empty set would silently empty their
    manifests, which reverts at ``execTransactionWithRole`` rather than failing
    loudly.

    The registry lookup runs on the CANONICAL key. Chain-scoped brands are not
    connector discovery keys — ``agni`` on mantle and ``velodrome`` on optimism
    resolve to ``agni_finance`` / ``aerodrome`` only through
    ``normalize_protocol``. Looking up the raw name returns ``None`` for those,
    which takes the fail-open branch above and skips the whole chain/intent
    gate silently, so a brand-spelled protocol was never narrowed at all.
    """
    normalized_protocol = normalize_protocol(chain, protocol)
    connector = CONNECTOR_REGISTRY.get(normalized_protocol) or CONNECTOR_REGISTRY.get(protocol)
    if connector is None or not connector.has_strategy_support:
        return intent_types

    if not connector.supports(chain=chain, protocol=normalized_protocol):
        warnings.append(f"Skipped unsupported permission discovery chain for {protocol} on {chain}")
        return None

    # Permission generation may request GMX's cancel verb while folding over a
    # different perp connector, or connector-declared standalone fee collection
    # even though that verb is absent from ``strategy_intents``. Keep exact
    # descriptor checks for authored cells, but allow these discovery extensions
    # through to their connector-owned hint gates. Non-GMX cancel then yields no
    # synthetic intent and no misleading unsupported-cell warning (VIB-5569).
    hints = get_permission_hints(protocol)
    supported = [
        intent_type
        for intent_type in intent_types
        if connector.supports(chain=chain, protocol=normalized_protocol, intent=intent_type)
        or intent_type is IntentType.PERP_CANCEL_ORDER
        or (intent_type is IntentType.LP_COLLECT_FEES and hints.supports_standalone_fee_collection)
    ]

    omitted = sorted(intent_type.value for intent_type in set(intent_types) - set(supported))
    if omitted:
        warnings.append(f"Skipped unsupported permission discovery cells for {protocol} on {chain}: {omitted}")
    return supported or None


class _TargetAccumulator:
    """Accumulates permission data for a single target contract."""

    __slots__ = ("label", "selectors", "send_allowed")

    def __init__(self, label: str) -> None:
        self.label = label
        self.selectors: set[str] = set()
        self.send_allowed = False


def _parse_requested_intent_types(
    values: Sequence[IntentType | str],
) -> tuple[list[IntentType], list[str]]:
    """Parse the public string boundary into canonical intent types."""
    requested: list[IntentType] = []
    warnings: list[str] = []
    for value in values:
        intent_type = IntentType.try_parse(value)
        if intent_type is None:
            warnings.append(f"Skipped unknown permission discovery intent type: {value!r}")
            continue
        requested.append(intent_type)
    return requested, warnings


class _CompilerCache:
    """Build and cache protocol-configured permission-discovery compilers."""

    def __init__(self, *, chain: str, rpc_url: str | None) -> None:
        self._chain = chain
        self._rpc_url = rpc_url
        self._compilers: dict[tuple[str, int, bool, bool], IntentCompiler] = {}

    def get(self, protocol: str) -> IntentCompiler:
        """Return a compiler configured for one protocol's discovery hints."""
        hints = get_permission_hints(protocol)
        chain_fee_override = hints.synthetic_fee_tier.get(self._chain)
        fee_tiers = SWAP_FEE_TIERS.get(protocol)
        if fee_tiers:
            mode = "fixed"
            fee_tier = chain_fee_override or DEFAULT_SWAP_FEE_TIER.get(protocol, fee_tiers[0])
        else:
            mode = "auto"
            fee_tier = chain_fee_override or 3000

        uses_rpc = hints.needs_rpc_discovery and self._rpc_url is not None
        compiler_rpc = self._rpc_url if uses_rpc else None
        key = (mode, fee_tier, uses_rpc, hints.offline_discovery)
        if key not in self._compilers:
            self._compilers[key] = IntentCompiler(
                chain=self._chain,
                rpc_url=compiler_rpc,
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,
                    swap_pool_selection_mode=cast(Literal["auto", "fixed"], mode),
                    fixed_swap_fee_tier=fee_tier,
                    permission_discovery=True,
                    offline_discovery=hints.offline_discovery,
                    # Declare production explicitly (ALM-3184). Manifest
                    # discovery must be a pure function of the registry, so it
                    # must never issue the undeclared-path fork probe — and it
                    # compiles no live swap whose guard the answer would relax.
                    managed_fork=False,
                ),
            )
        return self._compilers[key]


def _add_static_permissions(
    *,
    chain: str,
    protocol: str,
    intent_types: list[IntentType],
    targets: dict[str, _TargetAccumulator],
    selector_labels: dict[str, str],
) -> None:
    """Add statically declared permissions applicable to this request."""
    hints = get_permission_hints(protocol)
    for entry in hints.static_permissions.get(chain, []):
        if entry.intent_types is not None and not entry.intent_types.intersection(intent_types):
            continue
        target = entry.target.lower()
        if target not in targets:
            targets[target] = _TargetAccumulator(label=entry.label)
        accumulator = targets[target]
        accumulator.selectors.update(entry.selectors)
        accumulator.send_allowed |= entry.send_allowed
        selector_labels.update(entry.selectors)


def _compilation_failure_warning(
    *,
    result: CompilationResult,
    chain: str,
    protocol: str,
    intent_type: IntentType,
    rpc_url: str | None,
) -> str | None:
    """Return a useful warning for an unexpected compilation failure."""
    if not result.error or "not supported" in result.error.lower():
        return None
    message = f"Compilation failed for {protocol}/{intent_type.value} on {chain}: {result.error}"
    if rpc_url is None and ("pool not found" in result.error.lower() or "rpc" in result.error.lower()):
        message += (
            " — This protocol requires on-chain lookups for full permission"
            " discovery. Set ALCHEMY_API_KEY in your .env or pass --rpc-url."
        )
    return message


def _add_compiled_transaction(
    *,
    transaction: TransactionData,
    protocol: str,
    targets: dict[str, _TargetAccumulator],
) -> None:
    """Accumulate one compiler transaction into the permission target map."""
    target = transaction.to.lower()
    selector = transaction.data[:10] if transaction.data and len(transaction.data) >= 10 else None
    if target not in targets:
        targets[target] = _TargetAccumulator(label=_derive_label(transaction.tx_type, protocol, target))
    accumulator = targets[target]
    if selector:
        accumulator.selectors.add(selector)
    accumulator.send_allowed |= transaction.value > 0


def _compile_synthetic_permissions(
    *,
    chain: str,
    protocol: str,
    intent_types: list[IntentType],
    rpc_url: str | None,
    compiler: IntentCompiler,
    targets: dict[str, _TargetAccumulator],
    warnings: list[str],
) -> None:
    """Compile synthetic intents and accumulate their transaction permissions."""
    for intent_type in intent_types:
        for intent in build_synthetic_intents(protocol, intent_type, chain):
            try:
                result = compiler.compile(intent)
            except Exception as exc:
                warnings.append(f"Compilation error for {protocol}/{intent_type.value} on {chain}: {exc}")
                continue
            if result.status.value != "SUCCESS":
                warning = _compilation_failure_warning(
                    result=result,
                    chain=chain,
                    protocol=protocol,
                    intent_type=intent_type,
                    rpc_url=rpc_url,
                )
                if warning:
                    warnings.append(warning)
                continue
            for transaction in result.transactions:
                _add_compiled_transaction(transaction=transaction, protocol=protocol, targets=targets)


def _build_contract_permissions(
    targets: dict[str, _TargetAccumulator],
    selector_labels: dict[str, str],
) -> list[ContractPermission]:
    """Convert discovery accumulators into stable, sorted permissions."""
    return [
        ContractPermission(
            target=address,
            label=accumulator.label,
            operation=0,
            send_allowed=accumulator.send_allowed,
            function_selectors=sorted(
                [
                    FunctionPermission(selector=selector, label=selector_labels.get(selector, selector))
                    for selector in accumulator.selectors
                ],
                key=lambda permission: permission.selector,
            ),
        )
        for address, accumulator in sorted(targets.items())
    ]


def discover_permissions(
    chain: str,
    protocols: list[str],
    intent_types: Sequence[IntentType | str],
    rpc_url: str | None = None,
) -> tuple[list[ContractPermission], list[str]]:
    """Discover required permissions by compiling synthetic intents.

    For each (protocol, intent_type) combination, creates a synthetic
    intent and runs the real IntentCompiler to find out which contracts
    and function selectors are needed.

    Args:
        chain: Target chain name
        protocols: List of protocol names
        intent_types: Canonical intent types or their serialized string values
        rpc_url: Optional RPC URL for on-chain queries during discovery.
            Required for protocols like Aerodrome where LP_CLOSE needs to
            resolve pool addresses via factory contract calls.

    Returns:
        Tuple of (permissions_list, warnings_list)
    """
    requested_intent_types, warnings = _parse_requested_intent_types(intent_types)
    selector_labels = _build_selector_labels(protocols)
    targets: dict[str, _TargetAccumulator] = {}
    compilers = _CompilerCache(chain=chain, rpc_url=rpc_url)

    for protocol in protocols:
        supported_intent_types = _supported_intent_types_for(
            chain=chain,
            protocol=protocol,
            intent_types=requested_intent_types,
            warnings=warnings,
        )
        if supported_intent_types is None:
            continue

        _add_static_permissions(
            chain=chain,
            protocol=protocol,
            intent_types=supported_intent_types,
            targets=targets,
            selector_labels=selector_labels,
        )
        _compile_synthetic_permissions(
            chain=chain,
            protocol=protocol,
            intent_types=supported_intent_types,
            rpc_url=rpc_url,
            compiler=compilers.get(protocol),
            targets=targets,
            warnings=warnings,
        )

    return _build_contract_permissions(targets, selector_labels), warnings


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
