"""Teardown residual discovery: connector-owned, gateway-routed on-chain sweep for
off-position committed risk/capital (VIB-5116).

Teardown's position enumeration (``registry_enumeration``) answers "what
positions does this deployment have open?" from the strategy's
``get_open_positions()`` reconciled against the durable ``position_registry``.
Both surfaces are blind to on-chain capital that a venue holds in a form that is
NOT (yet) a position and was never written to the registry — the canonical case
is a **GMX V2 pending (unfilled) order**: the collateral is committed to the
OrderVault, but the order is not a position (no keeper executed it) and no
``position_registry`` row exists. Teardown then reports ``no_positions`` success
while the capital is stranded (VIB-5116).

This module runs each connector-published :class:`TeardownResidualDiscovery` over
the deployment's OWN wallet — **independent of the strategy's
``get_open_positions()`` / ``_loop_state``**, which is the enumeration-blindness
root cause — and converts the discovered residuals into :class:`PositionInfo`
so they are folded into the teardown position set (counted, coverage-checked by
:mod:`completeness`, and on-chain closure-verified by the protocol's teardown
post-condition) exactly like any other position.

**Fund-safety (blueprint 20 §1 Gateway : 1 Strategy).** One gateway serves
exactly one strategy, so the deployment's wallet is the deployment's own — a
wallet-scoped read can never surface a sibling deployment's capital. Discoveries
are scoped to the intersection of the deployment's chains and the connector's
declared ``strategy_chains`` so a Base-only strategy never issues an Arbitrum
GMX read.

**Empty ≠ Zero, fail-closed (the exact VIB-5116 bug).** A discovery that returns
``ok=False`` was UNMEASURED (gateway/RPC error, decode fault, partial data). It
must NEVER be treated as "no residuals" and silently pass — this module surfaces
an unmeasured read as a loud, closure-failing sentinel residual so the teardown
fails loud (its on-chain closure verify then re-reads authoritatively). A
MEASURED empty read (``ok=True``, no residuals) is genuinely nothing committed
and surfaces nothing.

**Gateway boundary.** Every read goes through the strategy's gateway client. No
direct RPC/HTTP is opened here; the framework names no protocol (the coupling
lives in the connector manifest — blueprint 22 / coupling ratchet).
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from almanak.connectors._connector import CONNECTOR_REGISTRY, ConnectorDiscoveryError
from almanak.connectors._strategy_base.teardown_residual_discovery import (
    PendingResidual,
    ResidualDiscoveryResult,
    TeardownResidualDiscovery,
    _register_teardown_residual_discovery,
    get_teardown_residual_discovery,
    has_teardown_residual_discovery,
    registered_residual_discovery_protocols,
)
from almanak.core.constants import canonical_chain_name
from almanak.framework.teardown.models import PositionInfo, PositionType

logger = logging.getLogger(__name__)


# connector-name -> the chains the connector declares it operates on. Built at
# manifest-load time so discovery can be scoped to the intersection of the
# deployment's chains and the connector's chains (a deployment that does not
# touch a connector's chain can hold none of its residuals). ``None`` chains ⇒
# the connector is chain-agnostic / off-chain — never auto-run (an unbounded
# wallet read on every chain is neither safe nor cheap); such a connector must
# scope itself.
_DISCOVERY_CHAINS: dict[str, tuple[str, ...]] = {}

# connector-name -> the strategy_intents the connector declares (e.g. GMX V2:
# PERP_OPEN / PERP_CLOSE). Used to decide whether a deployment actually USES a
# connector when scoping the fail-closed (VIB-5116 C1): a strategy that declares
# neither the connector's protocol nor any of its intents cannot hold that
# connector's residuals, so an unmeasured read of it is a chain-overlap probe
# (loud WARNING), not a real strand (hard fail-closed sentinel).
_DISCOVERY_INTENTS: dict[str, frozenset[str]] = {}


def _register_manifest_teardown_residual_discoveries() -> None:
    """Register connector-owned residual discoveries from manifests (VIB-5116)."""
    for connector_manifest in CONNECTOR_REGISTRY.with_teardown_residual_discovery():
        if connector_manifest.teardown_residual_discovery is None:
            continue
        hook = connector_manifest.teardown_residual_discovery.load()
        if not callable(hook):
            raise ConnectorDiscoveryError(
                f"{connector_manifest.teardown_residual_discovery.module}."
                f"{connector_manifest.teardown_residual_discovery.attribute} must be callable, "
                f"got {type(hook).__qualname__}"
            )
        name = connector_manifest.name.lower()
        _register_teardown_residual_discovery(connector_manifest.name, hook)
        chains = connector_manifest.strategy_chains
        # Canonicalize declared chains (alias → ChainRegistry canonical name,
        # e.g. "bnb" → "bsc") so the deployment-chain intersection below can
        # never silently miss on an alias/canonical vocabulary split — a miss
        # here skips the residual sweep for that (connector, chain) entirely
        # (VIB-5293 defect class).
        _DISCOVERY_CHAINS[name] = tuple(canonical_chain_name(str(c)).lower() for c in chains) if chains else ()
        intents = connector_manifest.strategy_intents
        _DISCOVERY_INTENTS[name] = frozenset(i.upper() for i in intents) if intents else frozenset()


_register_manifest_teardown_residual_discoveries()


def _position_type_from_str(value: str) -> PositionType:
    """Map a residual's ``position_type`` string onto the teardown enum.

    Defaults to :attr:`PositionType.PERP` for an unknown value — a residual is
    off-position committed risk teardown must not silently drop, and PERP is the
    canonical (GMX order) case. The specific type only affects how the
    completeness gate reasons about coverage; the authoritative closure signal is
    the protocol's on-chain post-condition regardless.
    """
    try:
        return PositionType[value.strip().upper()]
    except (KeyError, AttributeError):
        return PositionType.PERP


def _position_info_from_residual(residual: PendingResidual) -> PositionInfo | None:
    """Convert a connector :class:`PendingResidual` into a teardown ``PositionInfo``.

    Returns ``None`` when the residual carries no usable identifier — a residual
    without a stable identity cannot be closed/verified and must not be surfaced.
    """
    identifier = str(residual.identifier or "").strip()
    if not identifier:
        return None
    details: dict[str, Any] = {"source": "teardown_residual_discovery"}
    if isinstance(residual.details, dict):
        details.update(residual.details)
    try:
        value_usd = Decimal(str(residual.value_usd))
    except Exception:  # noqa: BLE001 — a malformed value must not fault the lane; treat as unpriced
        value_usd = Decimal("0")
    return PositionInfo(
        position_type=_position_type_from_str(residual.position_type),
        position_id=identifier,
        chain=str(residual.chain or "").lower(),
        protocol=str(residual.protocol or "").lower(),
        value_usd=value_usd,
        details=details,
    )


def _unmeasured_sentinel(protocol: str, chain: str, error: str | None) -> PositionInfo:
    """A loud, closure-failing residual for an UNMEASURED discovery read.

    Empty != Zero, fail-closed-LOUD (guardrail #2 — the exact VIB-5116 bug): a
    discovery that could not MEASURE the wallet's residual state must fail the
    teardown loud, NEVER silently pass as "nothing committed". This is safe to
    fail-closed here (unlike boot strand-detection VIB-5419, where a gRPC blip
    false-halting a healthy strategy was the bug) precisely because teardown's
    failure semantics are INVERTED: a fail-closed marks the closure
    unverified/accounting-degraded but NEVER blocks the next risk-reducing intent,
    so a false-positive from a transient gateway blip costs a loud re-verify, not a
    stranded position. The sentinel is surfaced as an open position so the
    completeness gate flags it and the protocol's post-condition re-reads
    authoritatively at verify time; ``kind`` marks it so the post-condition treats a
    still-unreadable account as not-closed rather than confirming closure.
    """
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=f"{protocol}-residual-unverified-{chain}",
        chain=str(chain or "").lower(),
        protocol=str(protocol or "").lower(),
        value_usd=Decimal("0"),
        details={
            "source": "teardown_residual_discovery",
            "kind": "residual_unverified",
            "error": error or "residual discovery read was unmeasured (gateway/RPC error)",
        },
    )


def _deployment_chains(strategy: Any) -> list[str]:
    """The canonical lower-cased chains this deployment operates on (best-effort).

    Values are alias-normalized through :func:`canonical_chain_name` (e.g. a
    config-supplied ``"bnb"`` → ``"bsc"``) so the intersection with
    ``_DISCOVERY_CHAINS`` — also canonical — compares one vocabulary.
    """
    chains: list[str] = []
    raw = getattr(strategy, "chains", None)
    if isinstance(raw, list | tuple):
        chains = [canonical_chain_name(str(c)).lower() for c in raw if c]
    if not chains:
        primary = getattr(strategy, "chain", None)
        if primary:
            chains = [canonical_chain_name(str(primary)).lower()]
    return chains


def _declared_protocols_and_intents(strategy: Any) -> tuple[frozenset[str], frozenset[str]] | None:
    """The strategy's STATICALLY declared (protocols, intent_types), or ``None``.

    Reads ``STRATEGY_METADATA`` (``@almanak_strategy`` decorator metadata) — a
    class-level, compile-time declaration, NOT runtime state. Scoping on it does
    NOT reintroduce the ``get_open_positions()`` / ``_loop_state`` enumeration-
    blindness this whole module exists to defeat: a strategy declares the
    connector's protocol slug (and its intents) in metadata whether or not its
    runtime ``_loop_state`` currently tracks a position. ``None`` ⇒ no usable
    declaration (undeterminable) — the caller then fails closed (safe default).
    """
    meta = getattr(getattr(strategy, "__class__", None), "STRATEGY_METADATA", None)
    if meta is None:
        meta = getattr(strategy, "STRATEGY_METADATA", None)
    if meta is None:
        return None
    raw_protos = getattr(meta, "supported_protocols", None)
    raw_intents = getattr(meta, "intent_types", None)
    protos = (
        frozenset(str(p).lower() for p in raw_protos)
        if isinstance(raw_protos, list | tuple | set | frozenset)
        else frozenset()
    )
    intents = (
        frozenset(str(i).upper() for i in raw_intents)
        if isinstance(raw_intents, list | tuple | set | frozenset)
        else frozenset()
    )
    if not protos and not intents:
        return None  # metadata present but declares nothing usable ⇒ undeterminable
    return protos, intents


def _strategy_uses_connector(strategy: Any, protocol: str) -> bool | None:
    """Does this deployment STATICALLY declare use of ``protocol``'s connector?

    ``True`` when the strategy declares the connector's protocol slug or any of
    its ``strategy_intents`` (e.g. GMX V2's PERP_OPEN / PERP_CLOSE); ``False`` when
    it declares neither (the connector was only reached via chain-overlap);
    ``None`` when the declaration is unavailable (undeterminable).
    """
    declared = _declared_protocols_and_intents(strategy)
    if declared is None:
        return None
    protos, intents = declared
    if protocol.lower() in protos:
        return True
    connector_intents = _DISCOVERY_INTENTS.get(protocol.lower(), frozenset())
    if connector_intents and intents and (connector_intents & intents):
        return True
    return False


def _fail_closed_scoped(strategy: Any, protocol: str) -> bool:
    """Whether an UNMEASURED read of ``protocol`` should hard fail-closed.

    Hard fail-closed (sentinel) when the strategy USES the connector (``True``) or
    the declaration is undeterminable (``None`` → safe default). A strategy that
    provably does NOT use the connector (``False``) only got probed via
    chain-overlap, so an unmeasured read is a loud WARNING, not a strand — this
    kills the false-FAIL noise (VIB-5116 C1) without weakening safety for any
    strategy that could actually hold the connector's residuals.
    """
    return _strategy_uses_connector(strategy, protocol) is not False


def _resolve_gateway_client(strategy: Any) -> Any | None:
    """Best-effort connected gateway client for the residual sweep.

    Prefers the strategy's own gateway client (the live runner path always wires
    it); falls back to the compiler's. Returns ``None`` when none is connected —
    the caller then skips the sweep (it cannot read on-chain), and the post-
    condition remains the authoritative closure verify at execution time.

    Reads the compiler via the PRIVATE ``_compiler`` attribute, NOT the public
    ``compiler`` property: on ``IntentStrategy`` the property RAISES
    ``RuntimeError`` when no compiler is configured (the live runner path, where
    the runner owns the compiler and never assigns ``strategy._compiler``), and
    ``getattr(strategy, "compiler", None)`` would NOT swallow that (its default
    only catches ``AttributeError``) — which crashed the whole enumeration before
    the residual could be surfaced (VIB-5116 real-fork regression). Every access
    here is guarded so this can never fault the teardown lane.
    """
    candidates: list[Any] = [
        getattr(strategy, "_gateway_client", None),
        getattr(strategy, "gateway_client", None),
    ]
    compiler = getattr(strategy, "_compiler", None)
    if compiler is not None:
        candidates.append(getattr(compiler, "_gateway_client", None))
        candidates.append(getattr(compiler, "gateway_client", None))
    for client in candidates:
        if client is not None and getattr(client, "is_connected", True):
            return client
    return None


def discover_teardown_residuals(strategy: Any) -> list[PositionInfo]:
    """Discover this deployment's off-position on-chain residuals (VIB-5116).

    Runs every connector-published residual discovery over the deployment's own
    wallet, scoped to the intersection of the deployment's chains and each
    connector's declared chains, and returns the discovered residuals as
    :class:`PositionInfo` for the teardown lane to fold in.

    NEVER raises — discovery must never fault the teardown lane (it is called
    inline in ``resolve_open_positions_with_registry``, whose failure aborts the
    whole enumeration). This top-level guard is load-bearing: the VIB-5116
    real-fork run showed a single unguarded attribute access (the raising
    ``IntentStrategy.compiler`` property) crashing the entire teardown enumeration.
    An empty return here means "no residuals discovered"; the protocol's on-chain
    post-condition remains the authoritative closure verify at execution time.
    """
    try:
        return _discover_teardown_residuals(strategy)
    except Exception:  # noqa: BLE001 — discovery must NEVER fault the teardown enumeration
        logger.exception("Teardown residual discovery raised unexpectedly; skipping the residual sweep")
        return []


def _discover_teardown_residuals(strategy: Any) -> list[PositionInfo]:
    """Implementation of :func:`discover_teardown_residuals` (guarded by the wrapper)."""
    protocols = registered_residual_discovery_protocols()
    if not protocols:
        return []

    deployment_chains = set(_deployment_chains(strategy))

    # Connectors this deployment could hold residuals for = declared-used ∩
    # chain-overlap. Used to scope the fail-closed on the degrade branches below.
    def _used_on_chain() -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        for proto in protocols:
            if _strategy_uses_connector(strategy, proto) is False:
                continue  # provably not used ⇒ never fail-closed on it
            for ch in _DISCOVERY_CHAINS.get(proto, ()):
                if ch in deployment_chains:
                    pairs.append((proto, ch))
        return pairs

    gateway_client = _resolve_gateway_client(strategy)
    if gateway_client is None:
        # No connected client at enumeration time: we cannot read on-chain. This is
        # NOT a silent pass — for a connector the deployment actually USES it is an
        # unverified strand risk (the post-condition would have nothing enumerated
        # to dispatch on), so surface a loud fail-closed sentinel per used
        # (connector, chain). Deployments that use no registered connector just log.
        used = _used_on_chain()
        if not used:
            logger.warning(
                "Teardown residual discovery: no connected gateway client available; no "
                "residual-discovery connector is used by this deployment, so nothing to sweep"
            )
            return []
        logger.error(
            "Teardown residual discovery: no connected gateway client available but this "
            "deployment uses %s — cannot verify absence of residual capital; failing closed loud",
            ", ".join(f"{p}/{c}" for p, c in used),
        )
        return [_unmeasured_sentinel(p, c, "no connected gateway client at enumeration time") for p, c in used]

    if not deployment_chains:
        # Chains unresolvable in a fail-closed module = fail-OPEN if we return []
        # silently (S3). Log loud when discoveries are registered.
        logger.warning(
            "Teardown residual discovery: %d discovery connector(s) registered but the "
            "deployment's chains could not be resolved — residual sweep skipped",
            len(protocols),
        )
        return []

    discovered: list[PositionInfo] = []
    for protocol in protocols:
        hook: TeardownResidualDiscovery | None = get_teardown_residual_discovery(protocol)
        if hook is None:
            continue
        connector_chains = _DISCOVERY_CHAINS.get(protocol, ())
        # A connector with no declared chains is not auto-swept (see module note).
        for chain in (c for c in connector_chains if c in deployment_chains):
            discovered.extend(_sweep_connector_chain(strategy, hook, protocol, chain, gateway_client))
    if discovered:
        logger.warning(
            "Teardown residual discovery surfaced %d residual(s) the strategy did not enumerate: %s",
            len(discovered),
            ", ".join(f"{p.protocol}:{p.position_id}" for p in discovered),
        )
    return discovered


def _sweep_connector_chain(
    strategy: Any,
    hook: TeardownResidualDiscovery,
    protocol: str,
    chain: str,
    gateway_client: Any,
) -> list[PositionInfo]:
    """Sweep one (connector, chain) for residuals, applying the C1 fail-closed scope.

    Returns the residual :class:`PositionInfo`\\ s (real orders on a measured read;
    a loud fail-closed sentinel on an UNMEASURED read of a connector the deployment
    actually uses; nothing plus a loud WARNING when the connector was only probed
    via chain-overlap).
    """
    wallet = _wallet_for_chain(strategy, chain)
    if not wallet:
        # Cannot read without a wallet: fail-closed loud only when the deployment
        # uses this connector (a real unverified strand); else just warn.
        if _fail_closed_scoped(strategy, protocol):
            logger.error(
                "Teardown residual discovery: no wallet resolved for %s on %s (used by this "
                "deployment) — failing closed loud",
                protocol,
                chain,
            )
            return [_unmeasured_sentinel(protocol, chain, "no wallet address resolved")]
        logger.warning(
            "Teardown residual discovery: no wallet resolved for %s on %s — skipping "
            "(connector not used by this deployment)",
            protocol,
            chain,
        )
        return []

    result = _run_discovery(hook, protocol, chain, wallet, gateway_client)
    if not result.ok:
        # UNMEASURED read (gateway/RPC error, decode fault, partial data). Scope the
        # fail-closed to connectors the deployment actually uses (VIB-5116 C1): a
        # strategy that USES this connector (or whose declaration is undeterminable)
        # has a real strand risk → fail-closed LOUD sentinel so the teardown never
        # silently reports "nothing committed" off a read it could not make
        # (guardrail #2). A strategy that provably does NOT use it was only probed
        # via chain-overlap → a transient blip is NOT a strand → loud WARNING, no
        # sentinel (kills the VIB-5419-class false-FAIL noise without weakening
        # safety for any strategy that could actually hold the residual).
        if _fail_closed_scoped(strategy, protocol):
            logger.error(
                "Teardown residual discovery: %s read UNMEASURED on %s (%s), and this deployment "
                "uses it — surfacing a loud closure-failing residual so the teardown is not "
                "trusted as complete",
                protocol,
                chain,
                result.error,
            )
            return [_unmeasured_sentinel(protocol, chain, result.error)]
        logger.warning(
            "Teardown residual discovery: %s read UNMEASURED on %s (%s), but this deployment does "
            "not use it (chain-overlap probe only) — not fail-closing an unrelated teardown; "
            "verify on-chain if this recurs",
            protocol,
            chain,
            result.error,
        )
        return []

    return [info for residual in result.residuals if (info := _position_info_from_residual(residual)) is not None]


def _wallet_for_chain(strategy: Any, chain: str) -> str:
    """The deployment's wallet on ``chain`` (per-chain aware), or ``""``."""
    getter = getattr(strategy, "get_wallet_for_chain", None)
    if callable(getter):
        try:
            wallet = getter(chain)
            if wallet:
                return str(wallet)
        except Exception:  # noqa: BLE001 — fall back to the default wallet
            logger.debug("get_wallet_for_chain failed for %s", chain, exc_info=True)
    return str(getattr(strategy, "wallet_address", "") or getattr(strategy, "_wallet_address", "") or "")


def _run_discovery(
    hook: TeardownResidualDiscovery,
    protocol: str,
    chain: str,
    wallet: str,
    gateway_client: Any,
) -> ResidualDiscoveryResult:
    """Invoke one discovery hook, converting any raised exception to ``ok=False``.

    The hook contract is never-raise, but a defensive boundary here guarantees a
    misbehaving connector hook degrades to a loud fail-closed result rather than
    faulting the teardown enumeration.
    """
    try:
        result = hook(wallet_address=wallet, chain=chain, gateway_client=gateway_client)
    except Exception as exc:  # noqa: BLE001 — a hook crash must not fault enumeration
        logger.exception("Teardown residual discovery hook for %s raised", protocol)
        return ResidualDiscoveryResult(ok=False, error=f"discovery hook raised: {exc}")
    if not isinstance(result, ResidualDiscoveryResult):
        return ResidualDiscoveryResult(
            ok=False, error=f"discovery hook returned {type(result).__qualname__}, expected ResidualDiscoveryResult"
        )
    return result


__all__ = [
    "discover_teardown_residuals",
    "get_teardown_residual_discovery",
    "has_teardown_residual_discovery",
]
