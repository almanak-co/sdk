"""Permission manifest generator.

Orchestrates compilation-based permission discovery, token approval
inference, and infrastructure permissions to produce a complete
Zodiac Roles permission manifest for a strategy.
"""

from __future__ import annotations

import copy
import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from almanak.connectors._connector import CONNECTOR_REGISTRY, ConnectorDiscoveryError, ImportRef

from ..execution.signer.safe.constants import (
    MULTISEND_ADDRESSES,
    MULTISEND_SELECTOR,
    SafeOperation,
)
from ..intents.compiler import ERC20_APPROVE_SELECTOR
from .discovery import discover_permissions
from .models import ContractPermission, FunctionPermission, PermissionManifest

logger = logging.getLogger(__name__)

InfrastructurePermissionBuilder = Callable[[str], list[ContractPermission]]


class PermissionGenerationError(RuntimeError):
    """Manifest generation cannot produce a required permission (ALM-3175).

    Raised instead of silently omitting an approve target: a Zodiac manifest
    missing one reverts unauthorized at ``execTransactionWithRole`` at
    runtime — potentially on the teardown (risk-reducing) path. Deploy-time
    failure with a named remedy beats a runtime brick.
    """


# Native-asset sentinel address — not an ERC-20, needs no approve permission.
_NATIVE_SENTINEL = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

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
#
# Each open-side type maps to a TUPLE of teardown-recovery complements. PERP_OPEN
# expands to both PERP_CLOSE (unwind a filled position) and PERP_CANCEL_ORDER
# (recover collateral from a stranded, never-filled pending order — VIB-5569).
# Both are risk-reducing verbs the strategy never authors itself, so the Safe
# permission manifest must authorise them or a hosted Safe-wallet teardown
# reverts at execTransactionWithRole. Cancel is a gmx_v2-only compile path; every
# other perp connector emits no cancel synthetic (the builder gates on the
# connector declaring PERP_CANCEL_ORDER), so their manifests are unaffected.
_TEARDOWN_COMPLEMENTS: dict[str, tuple[str, ...]] = {
    "SUPPLY": ("WITHDRAW",),
    "BORROW": ("REPAY",),
    "LP_OPEN": ("LP_CLOSE",),
    "VAULT_DEPOSIT": ("VAULT_REDEEM",),
    "PERP_OPEN": ("PERP_CLOSE", "PERP_CANCEL_ORDER"),
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
        for complement in _TEARDOWN_COMPLEMENTS.get(it, ()):
            if complement not in existing:
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
    strict: bool = True,
) -> PermissionManifest:
    """Generate a Zodiac Roles permission manifest for a strategy.

    Combines three permission sources:
    1. Protocol permissions - discovered by compiling synthetic intents
    2. Token approvals - ERC-20 approve for tokens referenced in config
    3. Infrastructure - MultiSend (always), connector-declared hooks

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
        strict: Fail-closed policy for config token approvals (ALM-3175).
            ``True`` (default, deploy-grade): a symbol-form config token the
            static registry cannot name raises ``PermissionGenerationError``
            — an omitted approve target reverts unauthorized at
            ``execTransactionWithRole`` at runtime. ``False`` (sweep-grade,
            e.g. multichain coverage scans where one shared symbol-form
            config legitimately has registry gaps on some chains): the
            omission is recorded as a manifest warning instead. Never
            silent either way.

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
    token_permissions, token_warnings = _extract_token_permissions(chain, config or {}, strict=strict)
    all_warnings.extend(token_warnings)

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


def _approve_permission(address: str, label: str) -> ContractPermission:
    """Build the ERC-20 ``approve`` permission for one token address."""
    return ContractPermission(
        target=address.lower(),
        label=label,
        operation=SafeOperation.CALL,
        send_allowed=False,
        function_selectors=[
            FunctionPermission(
                selector=ERC20_APPROVE_SELECTOR,
                label="approve(address,uint256)",
            ),
        ],
    )


def _address_form_entry(
    resolver: Any,
    ref: str,
    chain: str,
    warnings: list[str],
) -> ContractPermission | None:
    """Approve permission for an address-form config reference.

    Emits without any registry dependency — the approve target IS the
    address; resolution only decorates the label (offline, best-effort).
    Returns ``None`` for the native sentinel (skipped with a warning).
    """
    address = ref.lower()
    if address == _NATIVE_SENTINEL:
        warnings.append(f"Config token '{ref}' is the native asset on {chain}: no ERC-20 approve permission emitted")
        return None
    label_symbol: str | None = None
    try:
        resolved = resolver.resolve(ref, chain, log_errors=False, skip_gateway=True)
        label_symbol = getattr(resolved, "symbol", None)
    except Exception:
        label_symbol = None  # label-only lookup; the permission never depends on it
    label = f"ERC-20: {label_symbol.upper()}" if label_symbol else f"ERC-20 ({ref[:6]}...{ref[-4:]})"
    return _approve_permission(address, label)


def _anvil_funding_refs(config: dict[str, Any], chain: str) -> set[str]:
    """Return address/native keys from the section applicable to ``chain``.

    Flat funding belongs to the config's single declared ``chain`` when present.
    A ``chains`` list is also consulted; more than one declared chain is
    ambiguous because the same address need not identify the same asset on
    every chain, so flat funding is ignored in that case.
    This matters for manifest discovery across a strategy's wider
    ``supported_chains`` set: an Ethereum config must not leak its addresses
    or native gas symbol into an Avalanche manifest.
    """
    funding = config.get("anvil_funding", {})
    if not isinstance(funding, dict):
        return set()

    from almanak.core.constants import canonical_chain_name

    active_chain = canonical_chain_name(chain)
    if funding and all(isinstance(value, dict) for value in funding.values()):
        matching_sections = [
            value
            for section_chain, value in funding.items()
            if isinstance(section_chain, str) and canonical_chain_name(section_chain) == active_chain
        ]
        if len(matching_sections) > 1:
            raise PermissionGenerationError(f"anvil_funding defines duplicate alias sections for chain {chain!r}")
        funding = matching_sections[0] if matching_sections else {}
    else:
        configured_chain = config.get("chain")
        if isinstance(configured_chain, str):
            if canonical_chain_name(configured_chain) != active_chain:
                return set()
        else:
            configured_chains = config.get("chains")
            if isinstance(configured_chains, list):
                declared_chains = {canonical_chain_name(value) for value in configured_chains if isinstance(value, str)}
                if len(declared_chains) != 1 or active_chain not in declared_chains:
                    return set()
    if not isinstance(funding, dict):
        return set()
    return {key for key in funding if isinstance(key, str)}


def _extract_token_permissions(
    chain: str,
    config: dict[str, Any],
    *,
    strict: bool = True,
) -> tuple[list[ContractPermission], list[str]]:
    """Extract ERC-20 approve permissions for tokens referenced in config.

    Scans known token config fields and ``anvil_funding`` keys. Fail-closed
    contract (ALM-3175): every referenced token either yields an approve
    permission, is skipped with a manifest warning (native asset), or fails
    generation with ``PermissionGenerationError`` — never a silent drop,
    because a manifest missing an approve target reverts unauthorized at
    ``execTransactionWithRole`` at runtime.

    Address-form references emit directly: the approve target IS the
    address, so no registry entry is required (dynamically-resolved tokens
    included) and resolution only decorates the label. Bare symbols from
    ``anvil_funding`` are rejected; symbol-form references in the strategy's
    other legacy token fields resolve through the static registry only
    (``skip_gateway=True``): the manifest must be deterministic, and a
    market-search-resolved address must never be baked into a Safe grant.

    Returns:
        Tuple of (permissions, warnings).
    """
    token_refs: set[str] = set()

    # Scan known config fields
    for key, value in config.items():
        if key in _TOKEN_CONFIG_FIELDS and isinstance(value, str) and value:
            token_refs.add(value)

    # Scan flat or active per-chain anvil_funding address keys. Native symbols
    # are still accepted as the gas-asset exception and filtered below.
    funding_refs = _anvil_funding_refs(config, chain)
    token_refs.update(funding_refs)

    if not token_refs:
        return [], []

    from almanak.core.chains._helpers import native_symbols_for
    from almanak.framework.data.tokens import get_token_resolver
    from almanak.framework.data.tokens.address_resolution import looks_like_evm_address

    native_symbols = native_symbols_for(chain)
    invalid_funding_symbols = sorted(
        ref
        for ref in funding_refs
        if not looks_like_evm_address(ref.strip()) and ref.strip().upper() not in native_symbols
    )
    if invalid_funding_symbols:
        raise PermissionGenerationError(
            "anvil_funding ERC-20 keys must be exact contract addresses; bare symbols are not token identity: "
            + ", ".join(invalid_funding_symbols)
        )

    # Resolver construction failure propagates: it would drop EVERY token
    # approval at once, the worst case of the fail-open this function bans.
    resolver = get_token_resolver()

    permissions: list[ContractPermission] = []
    warnings: list[str] = []
    unresolved: list[str] = []

    for ref in sorted(token_refs):
        stripped = ref.strip()

        if stripped.upper() in native_symbols:
            warnings.append(
                f"Config token '{ref}' is the native asset on {chain}: no ERC-20 approve permission emitted"
            )
            continue

        if looks_like_evm_address(stripped):
            entry = _address_form_entry(resolver, stripped, chain, warnings)
            if entry is not None:
                permissions.append(entry)
            continue

        # Symbol-form reference from a legacy non-funding config field — the
        # static registry is the only trustworthy source of its address during
        # permission generation.
        try:
            resolved = resolver.resolve(stripped, chain, log_errors=False, skip_gateway=True)
        except Exception:
            unresolved.append(ref)
            continue
        address = (getattr(resolved, "address", None) or "").lower()
        if not address:
            unresolved.append(ref)
            continue
        if address == _NATIVE_SENTINEL:
            warnings.append(
                f"Config token '{ref}' is the native asset on {chain}: no ERC-20 approve permission emitted"
            )
            continue
        permissions.append(_approve_permission(address, f"ERC-20: {stripped.upper()}"))

    if unresolved:
        unresolved_names = ", ".join(sorted(set(unresolved)))
        message = (
            f"Cannot resolve config token(s) {unresolved_names} on {chain} to an address "
            "for the ERC-20 approve permission. An omitted approve target reverts unauthorized at "
            "execTransactionWithRole at runtime. Reference the token by its contract address in config "
            "(legacy non-funding symbols resolve through the static registry only), or register "
            "the token in the static registry."
        )
        if strict:
            raise PermissionGenerationError(f"{message} Generation fails instead of emitting an incomplete manifest.")
        logger.warning("permission_token_unresolved chain=%s tokens=%s", chain, unresolved_names)
        warnings.append(f"OMITTED approve permission(s): {message}")

    return permissions, warnings


def _build_infrastructure_permissions(
    chain: str,
    protocols: list[str],
) -> list[ContractPermission]:
    """Build always-needed infrastructure permissions.

    - MultiSend: always included (needed for any multi-action intent)
    - Connector infrastructure: protocol-specific hooks declared in connector manifests
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

    permissions.extend(_build_connector_infrastructure_permissions(chain, protocols))

    return permissions


def _build_connector_infrastructure_permissions(chain: str, protocols: list[str]) -> list[ContractPermission]:
    """Build infrastructure permissions from connector-declared hooks."""
    permissions: list[ContractPermission] = []
    seen_connectors: set[str] = set()

    for protocol in protocols:
        connector_manifest = CONNECTOR_REGISTRY.get(protocol.lower())
        if connector_manifest is None or connector_manifest.permission_infrastructure is None:
            continue
        if connector_manifest.name in seen_connectors:
            continue
        seen_connectors.add(connector_manifest.name)

        builder = _load_infrastructure_permission_builder(connector_manifest.permission_infrastructure)
        connector_permissions = builder(chain)
        _validate_connector_infrastructure_permissions(
            connector_name=connector_manifest.name,
            import_ref=connector_manifest.permission_infrastructure,
            permissions=connector_permissions,
        )
        permissions.extend(connector_permissions)

    return permissions


def _load_infrastructure_permission_builder(import_ref: ImportRef) -> InfrastructurePermissionBuilder:
    """Load and validate a connector infrastructure-permission builder."""
    builder = import_ref.load()
    if not callable(builder):
        raise ConnectorDiscoveryError(
            f"{import_ref.module}.{import_ref.attribute} must be callable, got {type(builder).__qualname__}"
        )
    return builder


def _validate_connector_infrastructure_permissions(
    *,
    connector_name: str,
    import_ref: ImportRef,
    permissions: list[ContractPermission],
) -> None:
    """Validate a connector infrastructure hook result."""
    if not isinstance(permissions, list):
        raise ConnectorDiscoveryError(
            f"{connector_name} permission infrastructure hook "
            f"{import_ref.module}.{import_ref.attribute} must return list[ContractPermission], "
            f"got {type(permissions).__qualname__}"
        )
    bad_permissions = [permission for permission in permissions if not isinstance(permission, ContractPermission)]
    if bad_permissions:
        raise ConnectorDiscoveryError(
            f"{connector_name} permission infrastructure hook "
            f"{import_ref.module}.{import_ref.attribute} returned non-ContractPermission values: "
            f"{bad_permissions!r}"
        )


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
