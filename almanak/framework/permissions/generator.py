"""Permission manifest generator.

Orchestrates compilation-based permission discovery, token approval
inference, and infrastructure permissions to produce a complete
Zodiac Roles permission manifest for a strategy.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..connectors.enso.adapter import ENSO_FUNCTION_SELECTORS
from ..connectors.enso.client import CHAIN_MAPPING, ROUTER_ADDRESSES
from ..execution.signer.safe.constants import (
    MULTISEND_ADDRESSES,
    MULTISEND_SELECTOR,
    SafeOperation,
)
from ..intents.compiler import ERC20_APPROVE_SELECTOR
from .discovery import discover_permissions
from .models import ContractPermission, FunctionPermission, PermissionManifest

logger = logging.getLogger(__name__)

# Config field names that contain token symbols or addresses
_TOKEN_CONFIG_FIELDS = frozenset(
    {
        "base_token",
        "quote_token",
        "collateral_token",
        "borrow_token",
        "from_token",
        "to_token",
        "supply_token",
        "withdraw_token",
        "repay_token",
        "deposit_token",
        "pt_token",
        "reward_token",
        "stake_token",
    }
)

MANIFEST_VERSION = "1.0"


def generate_manifest(
    strategy_name: str,
    chain: str,
    supported_protocols: list[str],
    intent_types: list[str],
    config: dict[str, Any] | None = None,
) -> PermissionManifest:
    """Generate a Zodiac Roles permission manifest for a strategy.

    Combines three permission sources:
    1. Protocol permissions - discovered by compiling synthetic intents
    2. Token approvals - ERC-20 approve for tokens referenced in config
    3. Infrastructure - MultiSend (always), Enso Router (if enso protocol)

    Args:
        strategy_name: Strategy identifier
        chain: Target chain name
        supported_protocols: Protocols the strategy uses
        intent_types: Intent types the strategy uses
        config: Optional strategy config dict (from config.json)

    Returns:
        Complete permission manifest
    """
    all_warnings: list[str] = []

    # 1. Protocol permissions via compilation-based discovery
    protocol_permissions, discovery_warnings = discover_permissions(
        chain=chain,
        protocols=supported_protocols,
        intent_types=intent_types,
    )
    all_warnings.extend(discovery_warnings)

    # 2. Token approval permissions from config
    token_permissions = _extract_token_permissions(chain, config or {})

    # 3. Infrastructure permissions
    infra_permissions = _build_infrastructure_permissions(chain, supported_protocols)

    # Merge all permissions by target address
    merged = _merge_permissions(protocol_permissions + token_permissions + infra_permissions)

    # Sort deterministically by target address
    merged.sort(key=lambda p: p.target)

    return PermissionManifest(
        version=MANIFEST_VERSION,
        chain=chain,
        strategy=strategy_name,
        generated_at=datetime.now(UTC).isoformat(),
        warnings=all_warnings,
        permissions=merged,
    )


def _extract_token_permissions(
    chain: str,
    config: dict[str, Any],
) -> list[ContractPermission]:
    """Extract ERC-20 approve permissions for tokens in config.

    Scans config for known token field names and anvil_funding keys,
    resolves their addresses, and generates approve permissions.
    """
    token_symbols: set[str] = set()

    # Scan known config fields
    for key, value in config.items():
        if key in _TOKEN_CONFIG_FIELDS and isinstance(value, str) and value:
            token_symbols.add(value)

    # Scan anvil_funding keys (these are token symbols)
    anvil_funding = config.get("anvil_funding", {})
    if isinstance(anvil_funding, dict):
        for token_key in anvil_funding:
            if isinstance(token_key, str):
                token_symbols.add(token_key)

    if not token_symbols:
        return []

    # Native ETH sentinel - not an ERC-20, skip approve permissions
    _NATIVE_SENTINEL = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

    # Resolve token addresses
    permissions = []
    try:
        from ..data.tokens import get_token_resolver

        resolver = get_token_resolver()
        for symbol in sorted(token_symbols):
            try:
                resolved = resolver.resolve(symbol, chain)
                if resolved and resolved.address and resolved.address.lower() != _NATIVE_SENTINEL:
                    permissions.append(
                        ContractPermission(
                            target=resolved.address.lower(),
                            label=f"ERC-20: {symbol.upper()}",
                            operation=SafeOperation.CALL,
                            send_allowed=False,
                            function_selectors=[
                                FunctionPermission(
                                    selector=ERC20_APPROVE_SELECTOR,
                                    label="approve(address,uint256)",
                                ),
                            ],
                        )
                    )
            except Exception:
                logger.debug(f"Could not resolve token '{symbol}' on {chain}")
    except Exception:
        logger.debug("Token resolver not available, skipping token permissions")

    return permissions


def _build_infrastructure_permissions(
    chain: str,
    protocols: list[str],
) -> list[ContractPermission]:
    """Build always-needed infrastructure permissions.

    - MultiSend: always included (needed for any multi-action intent)
    - Enso Router: included only when "enso" is in protocols (scoped CALL)
    """
    permissions: list[ContractPermission] = []

    # MultiSend (DELEGATECALL)
    multisend_addr = MULTISEND_ADDRESSES.get(chain.lower())
    if multisend_addr:
        permissions.append(
            ContractPermission(
                target=multisend_addr.lower(),
                label="MultiSend (Safe)",
                operation=SafeOperation.DELEGATE_CALL,
                send_allowed=False,
                function_selectors=[
                    FunctionPermission(
                        selector=MULTISEND_SELECTOR,
                        label="multiSend(bytes)",
                    ),
                ],
            )
        )

    # Enso Router (CALL) - only when enso protocol is used
    # Swaps go through the Router via CALL with specific function selectors.
    # send_allowed=True because native-token swaps (ETH, MNT, etc.) send
    # value with the router call — see adapter.py:346 and adapter.py:632.
    # Delegates (DELEGATECALL) are only for lending operations which are not
    # implemented in the SDK — see connectors/enso/client.py for details.
    if any(p.lower() == "enso" for p in protocols):
        chain_id = CHAIN_MAPPING.get(chain.lower())
        router_addr = ROUTER_ADDRESSES.get(chain_id) if chain_id else None
        if router_addr:
            permissions.append(
                ContractPermission(
                    target=router_addr.lower(),
                    label="Enso Router",
                    operation=SafeOperation.CALL,
                    send_allowed=True,
                    function_selectors=[
                        FunctionPermission(selector=sel, label=name)
                        for name, sel in sorted(ENSO_FUNCTION_SELECTORS.items())
                    ],
                )
            )
        else:
            logger.warning(f"No Enso Router address for chain '{chain}' — skipping Enso permissions")

    return permissions


def _merge_permissions(
    permissions: list[ContractPermission],
) -> list[ContractPermission]:
    """Merge permissions with the same target address.

    When multiple permission entries target the same contract:
    - Selectors are unioned
    - send_allowed uses the most permissive value (True wins)
    - operation uses the highest value (DELEGATECALL wins over CALL)
    - First label wins
    """
    merged: dict[str, ContractPermission] = {}

    for perm in permissions:
        key = perm.target.lower()
        if key not in merged:
            merged[key] = ContractPermission(
                target=key,
                label=perm.label,
                operation=perm.operation,
                send_allowed=perm.send_allowed,
                function_selectors=list(perm.function_selectors),
            )
        else:
            existing = merged[key]
            # Merge selectors (union, deduplicated by selector value)
            existing_selectors = {s.selector for s in existing.function_selectors}
            for sel in perm.function_selectors:
                if sel.selector not in existing_selectors:
                    existing.function_selectors.append(sel)
                    existing_selectors.add(sel.selector)
            # Most permissive send_allowed
            if perm.send_allowed:
                existing.send_allowed = True
            # Highest operation (DELEGATECALL > CALL)
            if perm.operation > existing.operation:
                existing.operation = perm.operation

    # Sort selectors within each permission
    for perm in merged.values():
        perm.function_selectors.sort(key=lambda s: s.selector)

    return list(merged.values())


def load_strategy_config(config_path: Path) -> dict[str, Any]:
    """Load a strategy's config.json file.

    Args:
        config_path: Path to config.json

    Returns:
        Parsed configuration dict, or empty dict if not found
    """
    if not config_path.exists():
        return {}
    try:
        return json.loads(config_path.read_text())
    except Exception as exc:
        logger.warning(f"Failed to read {config_path}: {exc}")
        return {}


def discover_teardown_protocols(
    strategy_class: type,
    chain: str,
) -> tuple[set[str], list[str]]:
    """Discover protocols used by a strategy's teardown intents.

    Attempts to introspect the strategy's ``generate_teardown_intents``
    method to find protocols that are not declared in ``supported_protocols``.
    This ensures permissions are generated for all protocols the strategy
    actually uses, including those only referenced during teardown.

    The function tries two approaches in order:

    1. **Runtime introspection** — create a minimal strategy instance and
       call ``generate_teardown_intents`` for both SOFT and HARD modes,
       extracting the ``protocol`` field from each returned intent.
    2. **Graceful fallback** — if instantiation or invocation fails (e.g.
       the method needs live market data), return an empty set with a
       warning so the caller can alert the user.

    Args:
        strategy_class: The loaded strategy class (not an instance).
        chain: Target chain name (set on the stub instance).

    Returns:
        A tuple of (discovered_protocol_names, warnings).
    """
    warnings: list[str] = []

    # Only introspect if the class actually overrides generate_teardown_intents
    method = getattr(strategy_class, "generate_teardown_intents", None)
    if method is None:
        return set(), warnings

    # Check if it's the base-class default (no-op) — skip introspection
    # to avoid false negatives from strategies that inherit the stub.
    if not _overrides_teardown(strategy_class):
        return set(), warnings

    try:
        from ..teardown.models import TeardownMode
    except Exception:
        warnings.append("Could not import TeardownMode — skipping teardown introspection")
        return set(), warnings

    # Build a lightweight stub instance (bypass __init__)
    try:
        instance: Any = object.__new__(strategy_class)
        # Set minimal attributes that teardown methods commonly access.
        # The base strategy class uses @property backed by _chain/_strategy_id,
        # so set private attributes to avoid "has no setter" errors.
        for attr, val in [
            ("chain", chain),
            ("_chain", chain),
            ("state", {}),
            ("_state", {}),
            ("config", {}),
            ("_config", {}),
            ("strategy_id", "__permissions_introspection__"),
            ("_strategy_id", "__permissions_introspection__"),
        ]:
            try:
                setattr(instance, attr, val)
            except AttributeError:
                pass  # read-only property — private attr fallback handles it
    except Exception as exc:
        warnings.append(f"Could not create strategy stub for teardown introspection: {exc}")
        return set(), warnings

    # Call generate_teardown_intents for both modes and collect protocols.
    # Mirror the runner's backward-compat fallback (strategy_runner.py:3462-3468):
    # try new signature (mode, market=) first, fall back to old (mode) on TypeError.
    protocols: set[str] = set()
    saw_success = False
    for mode in (TeardownMode.SOFT, TeardownMode.HARD):
        try:
            try:
                intents = instance.generate_teardown_intents(mode=mode, market=None)
            except TypeError as exc:
                if "unexpected keyword argument" not in str(exc):
                    raise
                intents = instance.generate_teardown_intents(mode)
            saw_success = True
            if not intents:
                continue
            for intent in intents:
                protocol = getattr(intent, "protocol", None)
                if protocol and isinstance(protocol, str):
                    protocols.add(protocol.lower())
        except Exception as exc:
            warnings.append(
                f"Could not introspect teardown intents (mode={mode.value}): {exc}. "
                "Ensure supported_protocols includes all protocols used in generate_teardown_intents()."
            )

    if saw_success and not protocols:
        warnings.append(
            "Teardown introspection returned no protocols. If generate_teardown_intents() "
            "depends on live positions/state, verify supported_protocols manually."
        )
    return protocols, warnings


def _overrides_teardown(strategy_class: type) -> bool:
    """Check whether ``strategy_class`` has a non-framework ``generate_teardown_intents``.

    Walks the MRO so inherited implementations from shared base classes
    or mixins are detected, not just methods defined on the concrete class.
    Returns False only when the first defining class in the MRO is a
    framework base (IntentStrategy, StatelessStrategy, etc.) whose
    implementation is abstract or returns ``[]``.
    """
    _FRAMEWORK_PREFIXES = (
        "almanak.framework.strategies.",
        "almanak.framework.runner.",
        "almanak.framework.teardown.",
    )
    for cls in strategy_class.__mro__:
        if cls is object:
            continue
        if "generate_teardown_intents" not in cls.__dict__:
            continue
        # First defining class in MRO — framework base or user code?
        module = getattr(cls, "__module__", "") or ""
        return not any(module.startswith(p) for p in _FRAMEWORK_PREFIXES)
    return False
