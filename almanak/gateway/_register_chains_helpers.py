"""Phase helpers for ``_RegisterChainsServicer.RegisterChains`` (Phase 8.3c).

``RegisterChains`` is the single entry point the strategy container uses to ask
the gateway to pre-initialize execution orchestrators and compilers for a set of
chains. The RPC weaves together five concerns:

1. Derive a fallback wallet address from Safe settings or private-key EOA when
   the request does not supply one.
2. Resolve per-chain wallets from an optional ``wallet_registry`` plugin, with
   a guard that rejects Solana family chains (not yet supported at the
   execution layer).
3. Validate each requested chain name against the EVM allowlist and map it to
   an effective wallet (registry entry first, legacy wallet fallback).
4. Merge in non-requested registry chains so cross-chain intents can resolve
   destination wallets without an explicit registration call.
5. Pre-warm orchestrator + compiler for every valid chain and surface per-chain
   errors in the response.

These concerns used to live inline in a single 124-line, CC-35 function that
was impossible to unit test without spinning up the full servicer. The helpers
below decompose the flow into small, deterministic, unit-testable pieces. The
servicer method now wires them together and is responsible only for
protobuf-response construction and the one side effect on the execution
servicer (compiler cache invalidation).

The error-response shape exposed to clients is unchanged - strings, ordering,
and success=False semantics are all preserved byte-for-byte and pinned by
``tests/gateway/test_register_chains_characterization.py``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing-only imports
    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.services.execution_service import ExecutionServiceServicer

logger = logging.getLogger(__name__)


def derive_default_wallet(settings: GatewaySettings, requested_wallet: str) -> str:
    """Return the wallet to use when the caller did not supply one.

    Precedence mirrors the pre-refactor inline code:

    1. If the request provided ``wallet_address``, echo it straight back.
    2. Else if ``safe_mode`` is 'direct'/'zodiac' AND ``safe_address`` is set,
       use the Safe address.
    3. Else if ``private_key`` is configured, derive the EOA address from it
       (tolerant of a missing ``0x`` prefix).
    4. Otherwise return the empty string (the caller decides whether to error).

    Args:
        settings: Active gateway settings snapshot.
        requested_wallet: ``request.wallet_address`` from the RPC. Empty string
            means "not supplied".

    Returns:
        A wallet address string, possibly empty if none could be derived.
    """
    if requested_wallet:
        return requested_wallet

    safe_mode_enabled = settings.safe_mode in ("direct", "zodiac")
    if settings.safe_address and safe_mode_enabled:
        return settings.safe_address

    if settings.private_key:
        # Local import: eth_account is heavy and only needed on the EOA path.
        from eth_account import Account

        key = settings.private_key
        if not key.startswith("0x"):
            key = "0x" + key
        try:
            return Account.from_key(key).address
        except Exception as e:
            # A malformed gateway-side private key must not escape as a gRPC
            # INTERNAL - callers (``RegisterChains``) need the same recoverable
            # empty-wallet sentinel they already handle for the no-key case.
            # ``GatewayServer._resolve_wallet_address`` follows the same
            # pattern. Log at warning without a stack trace so nothing
            # sensitive leaks into the gateway security boundary.
            logger.warning("Invalid gateway private key during wallet derivation: %s", e)
            return ""

    return ""


def _is_solana_resolved(resolved: Any) -> bool:
    """Return True when a wallet-registry entry belongs to the Solana family.

    The wallet registry plugin's ``ResolvedWallet.family`` is a ``StrEnum``
    (``WalletFamily.SOLANA == "solana"``) we cannot import without taking
    a hard dependency on the optional plugin. The string compare here
    consults the *wallet's* stated family rather than the chain's family —
    the two are equivalent for any well-formed wallet registry entry, but
    the wallet-side check is the defensive one (skip a Solana-tagged
    wallet even if the registry mis-routed it onto an EVM chain). This
    is intentionally NOT the W3 ``ChainFamily.SOLANA`` migration target;
    see the test ``test_registry_skips_solana_via_family_attribute``.
    """
    return hasattr(resolved, "family") and str(resolved.family) == "solana"


def resolve_requested_chain_wallets(wallet_registry: Any | None, chains: list[str]) -> dict[str, str]:
    """Build a {chain -> wallet_address} map for the requested chains.

    Runs the first pass of per-chain resolution against the registry plugin:
    for each raw chain name, normalize via ``validate_chain`` and ask the
    registry for the matching wallet. Entries that come back as Solana family
    are silently skipped (they must be rejected by the downstream guard, not
    smuggled into the EVM pipeline). Any other registry exception is logged at
    debug and the chain falls through to the legacy-wallet path.

    Args:
        wallet_registry: Loaded wallet registry plugin, or ``None``.
        chains: Raw chain names from the request.

    Returns:
        Empty dict if ``wallet_registry`` is None, otherwise the resolved
        subset. Keys are normalized chain names (lowercase, alias-resolved).
    """
    if wallet_registry is None:
        return {}

    from almanak.gateway.validation import validate_chain

    chain_wallets: dict[str, str] = {}
    for raw_chain in chains:
        try:
            validated = validate_chain(raw_chain)
            resolved = wallet_registry.resolve(validated)
            if _is_solana_resolved(resolved):
                continue
            chain_wallets[validated] = resolved.account_address
        except Exception as e:
            logger.debug(f"Wallet registry: no entry for {raw_chain}: {e}")
    return chain_wallets


def find_solana_chain_in_wallets(chains: list[str], chain_wallets: dict[str, str]) -> str | None:
    """Return the first Solana chain that leaked into ``chain_wallets``.

    The wallet registry does not yet support Solana at the execution-servicer
    layer. If a Solana chain made it into ``chain_wallets`` (e.g. because the
    registry returned an EVM-family entry by mistake or validate_chain is
    patched in tests), the RPC must surface an explicit error rather than
    silently attempting to pre-warm a Solana orchestrator.

    Args:
        chains: Raw chain names from the request, checked in request order so
            the error message is stable across refactors.
        chain_wallets: Map produced by :func:`resolve_requested_chain_wallets`.

    Returns:
        The offending original chain name, or ``None`` if clean.
    """
    from almanak.gateway.validation import is_solana_chain

    for chain in chains:
        if is_solana_chain(chain) and chain.lower() in chain_wallets:
            return chain
    return None


def validate_and_map_chains(
    chains: list[str],
    chain_wallets: dict[str, str],
    legacy_wallet: str,
) -> tuple[dict[str, str], list[str]]:
    """Validate requested chains and pair each with an effective wallet.

    For every raw chain name:

    - Run ``validate_chain`` (EVM allowlist). Failures are collected into
      ``errors`` as ``"<raw_chain>: <reason>"`` and the chain is skipped.
    - Look up the wallet in ``chain_wallets`` first; fall back to
      ``legacy_wallet``. If neither yields a non-empty wallet, record
      ``"<chain>: No wallet address available"`` and skip.

    Args:
        chains: Raw chain names from the request.
        chain_wallets: Per-chain wallet overrides from the registry.
        legacy_wallet: Wallet to use when the registry has no entry.

    Returns:
        A ``(chain_wallet_map, errors)`` tuple. ``chain_wallet_map`` has
        normalized chain names as keys.
    """
    from almanak.gateway.validation import validate_chain

    chain_wallet_map: dict[str, str] = {}
    errors: list[str] = []

    for raw_chain in chains:
        try:
            chain = validate_chain(raw_chain)
        except Exception as e:
            errors.append(f"{raw_chain}: {e}")
            continue

        effective_wallet = chain_wallets.get(chain, legacy_wallet or "")
        if not effective_wallet:
            errors.append(f"{chain}: No wallet address available")
            continue

        chain_wallet_map[chain] = effective_wallet

    return chain_wallet_map, errors


def merge_all_registry_chains(wallet_registry: Any | None, chain_wallet_map: dict[str, str]) -> dict[str, str]:
    """Return ``chain_wallet_map`` merged with every non-Solana registry chain.

    Cross-chain intents (bridges especially) need destination-chain wallets
    even when the strategy did not explicitly register those chains. This
    helper walks every chain known to the registry and adds any missing entry
    whose resolved wallet is not Solana. Any exception from ``resolve`` is
    swallowed: a transient registry hiccup for an unrelated chain must not
    kill the RPC.

    Args:
        wallet_registry: Loaded registry plugin, or ``None`` (returns a copy of
            ``chain_wallet_map`` as-is).
        chain_wallet_map: Map produced by :func:`validate_and_map_chains`.

    Returns:
        A new dict (``chain_wallet_map`` is not mutated) containing every
        known non-Solana chain the gateway can route to.
    """
    full: dict[str, str] = dict(chain_wallet_map)
    if wallet_registry is None:
        return full

    for reg_chain in wallet_registry.all_chains():
        if reg_chain in full:
            continue
        try:
            resolved = wallet_registry.resolve(reg_chain)
            if _is_solana_resolved(resolved):
                continue
            full[reg_chain] = resolved.account_address
        except Exception as e:
            # Registry hiccups for unrelated chains must not abort the RPC,
            # but we still want a diagnostic breadcrumb when an extra chain
            # silently drops out of ``chain_wallets`` during cross-chain
            # intent routing.
            logger.debug(
                "Wallet registry: skipping extra chain %s during merge: %s",
                reg_chain,
                e,
            )
    return full


async def prewarm_chains(
    execution: ExecutionServiceServicer,
    chain_wallet_map: dict[str, str],
) -> tuple[list[str], list[str]]:
    """Pre-warm execution orchestrator + compiler for every mapped chain.

    Iterates ``chain_wallet_map`` in insertion order, awaits
    ``_get_orchestrator`` and calls ``_get_compiler`` for each entry. Success
    goes into ``initialized``; any exception is caught and surfaced as
    ``"<chain>: <reason>"`` in ``errors`` (the error message shape is pinned
    by characterization tests). We still proceed to the next chain - partial
    failure is an expected outcome and must yield a response with both
    populated.

    Args:
        execution: Execution servicer exposing ``_get_orchestrator``
            (coroutine) and ``_get_compiler`` (sync).
        chain_wallet_map: Output of :func:`validate_and_map_chains`.

    Returns:
        ``(initialized_chains, errors)``.
    """
    initialized: list[str] = []
    errors: list[str] = []
    for chain, effective_wallet in chain_wallet_map.items():
        try:
            await execution._get_orchestrator(chain, effective_wallet)
            execution._get_compiler(chain, effective_wallet)
            initialized.append(chain)
            logger.info(f"Pre-warmed orchestrator and compiler for {chain} (wallet={effective_wallet[:10]}...)")
        except Exception as e:
            errors.append(f"{chain}: {e}")
            logger.error(f"Failed to pre-warm {chain}: {e}")
    return initialized, errors


async def reinitialize_market_service(market_servicer: Any | None, initialized_chains: list[str]) -> None:
    """Upgrade MarketService from CoinGecko-only to the full 4-source stack.

    When the gateway starts without ``--chains`` (e.g. deployed mode), the
    MarketService falls back to a CoinGecko-only aggregator. Once we know at
    least one chain, reinit it with the first initialized chain so Chainlink,
    Binance, and DexScreener come online too. The reinit call may fail
    (upstream outage, config issue) - that is logged and swallowed, keeping
    the strategy-facing pre-warm path unaffected.

    No-op when there is no market servicer or no initialized chains.
    """
    if market_servicer is None:
        return
    if not initialized_chains:
        return
    try:
        await market_servicer.reinitialize(initialized_chains[0])
    except Exception as e:
        logger.warning("MarketService reinit failed for chain %s: %s", initialized_chains[0], e)
