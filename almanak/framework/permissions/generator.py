"""Permission manifest generator.

Orchestrates compilation-based permission discovery, token approval
inference, and infrastructure permissions to produce a complete
Zodiac Roles permission manifest for a strategy.
"""

from __future__ import annotations

import copy
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
        "token0",
        "token1",
    }
)

MANIFEST_VERSION = "1.0"

# Selectors emitted by ``_build_infrastructure_permissions`` that are NOT
# load-bearing for any specific protocol bundle — they are batching primitives
# (Safe MultiSend) or per-token approvals (ERC-20 ``approve``) that are present
# on every manifest but aren't necessarily hit by every compiled bundle.
#
# Negative-authorisation tests (see
# ``tests/intents/_permission_onchain_harness._auto_derive_load_bearing_selector``)
# revoke a target to prove the manifest is load-bearing. Revoking these
# universal-infra selectors produces false-pass results: the bundle still
# succeeds via the non-infra path, the negative test surfaces as
# "DID NOT RAISE", and Zodiac never actually denies — the entire signal the
# negative test is meant to produce is lost.
#
# Protocol-conditional infra selectors (Enso Router, etc.) are deliberately
# NOT in this set — they ARE load-bearing for their protocol's bundles, and
# revoking them in a negative test for that protocol IS the right behavior.
#
# Anyone adding a new universal infrastructure selector to
# ``_build_infrastructure_permissions`` (a future global delegatecall batcher,
# fee router, unified executor) MUST also add it here, or negative-anchor
# tests will silently false-pass on every chain. The unit test
# ``test_universal_infrastructure_selectors_match_exclusion_set`` in
# ``tests/unit/permissions/test_generator_infrastructure_exclusion.py``
# enforces this contract.
INFRASTRUCTURE_NON_LOAD_BEARING_SELECTORS: frozenset[str] = frozenset(
    {
        MULTISEND_SELECTOR,
        ERC20_APPROVE_SELECTOR,
    }
)

# One-way open→close teardown complements.  If a strategy declares an "open"
# intent type, the corresponding "close" type is needed for teardown.
# Only open→close direction is expanded to respect least-privilege: a strategy
# declaring only WITHDRAW should not auto-gain SUPPLY permissions.
# discover_permissions() already skips unsupported (protocol, intent_type) combos
# so adding complements for irrelevant protocols is a harmless no-op.
_TEARDOWN_COMPLEMENTS: dict[str, str] = {
    "SUPPLY": "WITHDRAW",
    "BORROW": "REPAY",
    "LP_OPEN": "LP_CLOSE",
    "VAULT_DEPOSIT": "VAULT_REDEEM",
    "PERP_OPEN": "PERP_CLOSE",
}


def _expand_intent_types_for_teardown(intent_types: list[str]) -> tuple[list[str], list[str]]:
    """Expand intent types to include teardown complements.

    Strategies commonly declare only the "open" side of an operation
    (e.g. SUPPLY) and put the "close" side (WITHDRAW) only in
    ``generate_teardown_intents()``.  Since teardown introspection is
    fragile (requires runtime state), this function deterministically
    adds the complementary intent types so permissions are always
    generated for both sides.

    Returns:
        Tuple of (expanded_intent_types, sorted list of added types).
    """
    expanded = list(intent_types)
    existing = set(expanded)
    added: list[str] = []
    for it in intent_types:
        complement = _TEARDOWN_COMPLEMENTS.get(it)
        if complement and complement not in existing:
            added.append(complement)
            existing.add(complement)
            expanded.append(complement)
    return expanded, sorted(added)


def generate_manifest(
    strategy_name: str,
    chain: str,
    supported_protocols: list[str],
    intent_types: list[str],
    config: dict[str, Any] | None = None,
    rpc_url: str | None = None,
) -> PermissionManifest:
    """Generate a Zodiac Roles permission manifest for a strategy.

    Combines three permission sources:
    1. Protocol permissions - discovered by compiling synthetic intents
    2. Token approvals - ERC-20 approve for tokens referenced in config
    3. Infrastructure - MultiSend (always), Enso Router (if enso protocol)

    Intent types are automatically expanded to include teardown complements
    (e.g. SUPPLY -> WITHDRAW) so that teardown permissions are always
    generated even when the strategy only declares the "open" side.

    Args:
        strategy_name: Deployment identifier
        chain: Target chain name
        supported_protocols: Protocols the strategy uses
        intent_types: Intent types the strategy uses
        config: Optional strategy config dict (from config.json)
        rpc_url: Optional RPC URL for on-chain queries during discovery.
            Enables protocols like Aerodrome to resolve dynamic contract
            addresses (e.g. LP pool addresses from factory).

    Returns:
        Complete permission manifest
    """
    all_warnings: list[str] = []

    # Expand intent types to include teardown complements
    expanded_types, added_types = _expand_intent_types_for_teardown(intent_types)
    if added_types:
        all_warnings.append(
            f"Auto-added teardown complement intent types: {added_types}. "
            "Consider adding them to intent_types in @almanak_strategy() explicitly."
        )

    # 1. Protocol permissions via compilation-based discovery
    protocol_permissions, discovery_warnings = discover_permissions(
        chain=chain,
        protocols=supported_protocols,
        intent_types=expanded_types,
        rpc_url=rpc_url,
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
    config: dict[str, Any] | None = None,
) -> tuple[set[str], list[str]]:
    """Discover protocols used by a strategy's teardown intents.

    Attempts to introspect the strategy's ``generate_teardown_intents``
    method to find protocols that are not declared in ``supported_protocols``.
    This ensures permissions are generated for all protocols the strategy
    actually uses, including those only referenced during teardown.

    The function tries two approaches in order:

    1. **Full init** — create a stub with config populated and attempt to
       run ``__init__`` so derived attributes (e.g. ``self.max_slippage_pct``)
       are available to ``generate_teardown_intents``.
    2. **Bare stub fallback** — if ``__init__`` fails (e.g. needs a live
       gateway), fall back to a minimal stub with only framework attributes.
    3. **Graceful fallback** — if invocation still fails, return an empty
       set with a warning so the caller can alert the user.

    Args:
        strategy_class: The loaded strategy class (not an instance).
        chain: Target chain name (set on the stub instance).
        config: Strategy config.json values (passed to stub so
            ``get_config()`` returns real values during introspection).

    Returns:
        A tuple of (discovered_protocol_names, warnings).
    """
    warnings: list[str] = []
    config = config or {}

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

    # Build a stub instance, trying full __init__ first for strategies whose
    # teardown methods rely on attributes derived from config (e.g.
    # self.max_slippage_pct, self.base_token).
    instance: Any = None
    used_full_init = False
    try:
        instance = object.__new__(strategy_class)
        # Set minimal framework attributes so __init__ can call get_config().
        _set_stub_attrs(instance, chain, config)
        # Attempt full __init__ — this populates derived attributes that
        # teardown methods commonly access.  Use a deep copy so a partially-
        # failing __init__ cannot mutate the caller's config.
        try:
            instance.__init__(
                config=copy.deepcopy(config),
                chain=chain,
                wallet_address="0x0000000000000000000000000000000000000000",
            )
            used_full_init = True
        except Exception:
            # __init__ may need a gateway, market data, or other runtime deps.
            # Discard the partially-initialized instance and create a fresh
            # stub so teardown discovery operates on clean state.
            logger.debug("Strategy __init__ failed during teardown introspection", exc_info=True)
            instance = object.__new__(strategy_class)
            _set_stub_attrs(instance, chain, config)
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
            init_hint = ""
            if not used_full_init:
                init_hint = (
                    " Strategy __init__ could not run during introspection, so config-derived "
                    "attributes are unavailable."
                )
            warnings.append(
                f"Could not introspect teardown intents (mode={mode.value}): {exc}.{init_hint} "
                "Teardown protocols may still be covered if they appear in supported_protocols "
                "or permission hints. Verify the generated permissions include all teardown contracts."
            )

    if saw_success and not protocols:
        warnings.append(
            "Teardown introspection returned no protocols. If generate_teardown_intents() "
            "depends on live positions/state, verify supported_protocols manually."
        )
    return protocols, warnings


def _set_stub_attrs(instance: Any, chain: str, config: dict[str, Any]) -> None:
    """Set minimal framework attributes on a stub strategy instance.

    The base strategy class uses ``@property`` backed by private attrs
    (``_chain``, ``_config``, etc.), so we set both public and private
    names to cover both property-backed and direct-attribute patterns.
    """
    state: dict[str, Any] = {}
    persistent_state: dict[str, Any] = {}
    for attr, val in [
        ("chain", chain),
        ("_chain", chain),
        ("state", state),
        ("_state", state),
        ("config", config),
        ("_config", config),
        ("persistent_state", persistent_state),
        ("_persistent_state", persistent_state),
        ("deployment_id", "__permissions_introspection__"),
        ("_deployment_id", "__permissions_introspection__"),
        ("wallet_address", "0x0000000000000000000000000000000000000000"),
        ("_wallet_address", "0x0000000000000000000000000000000000000000"),
    ]:
        try:
            setattr(instance, attr, val)
        except AttributeError:
            pass  # read-only property — private attr fallback handles it


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
