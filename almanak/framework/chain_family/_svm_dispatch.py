"""SVM (Solana) intent-compile dispatch.

VIB-4803. Owns the protocol-level routing for SWAP / LP_OPEN / LP_CLOSE
intents on Solana chains. The actual per-protocol compilation lives in
per-protocol connector compilers registered through
:data:`CompilerRegistry`. This module is the routing boundary between
:class:`SvmFamily.compile_intent` and those compilers.

Post-#2416: Solana LP compile bodies were moved into per-connector
compilers (``connectors/meteora/compiler.py`` etc.) and dispatched through
:data:`CompilerRegistry`. The connector compilers each enforce their own
``chain in {"solana"}`` check and emit the canonical "<Protocol> is only
supported on Solana" error when called from a non-Solana chain. This
module therefore does NOT replicate those chain checks; it only:

  * normalises the Solana default LP protocol (``raydium_clmm`` when
    ``intent.protocol is None``),
  * routes via :func:`get_connector_compiler` for LP intents,
  * routes SWAP through :data:`CompilerRegistry` to the Jupiter connector
    compiler.

Why this lives next to :class:`SvmFamily` and not inside ``compiler.py``:

    The whole point of VIB-4803 is to make adding a hypothetical ``MoveFamily``
    "a single new class + adapter, no edits elsewhere" (ticket acceptance). The
    dispatch table for "which adapter compiles a SWAP on family X" should
    therefore live with the family adapter, not buried in ``IntentCompiler``.
"""

from __future__ import annotations

from collections.abc import Mapping
from functools import cache
from types import MappingProxyType
from typing import TYPE_CHECKING, NamedTuple

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.compiler_registry import get_compiler as get_connector_compiler
from almanak.connectors._strategy_base.protocol_aliases import normalize_protocol
from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.core.intent_types import IntentType
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus

if TYPE_CHECKING:
    from almanak.framework.intents.compiler import IntentCompiler
    from almanak.framework.intents.vocabulary import (
        LPCloseIntent,
        LPOpenIntent,
        SwapIntent,
    )


_ALLOWED_SOLANA_SWAP_PROTOCOLS = {"jupiter"}
_SOLANA_DEFAULT_LP_PROTOCOL = "raydium_clmm"
_LP_INTENT_TYPES = frozenset({IntentType.LP_OPEN, IntentType.LP_CLOSE})


def _fold_protocol(protocol: str) -> str:
    """Case/hyphen fold shared by the table build and every lookup.

    One function so a key can never be built with a different fold than the
    lookup uses.
    """
    return protocol.strip().lower().replace("-", "_")


class _SolanaLpRouting(NamedTuple):
    """Derived Solana LP routing: accepted spellings, and the names we publish."""

    compiler_key_by_spelling: Mapping[str, str]
    published_names: tuple[str, ...]


@cache
def _solana_lp_routing() -> _SolanaLpRouting:
    """Derive Solana LP protocol routing from the connector manifests.

    VIB-6231 / blueprint 22 W3. This used to be a hand-maintained set holding
    only the connectors' *compiler* keys (``orca_whirlpools`` /
    ``meteora_dlmm`` / ``raydium_clmm``). But ``almanak info matrix`` publishes
    each connector's manifest ``name`` -- ``orca`` / ``meteora`` / ``raydium``
    -- so every protocol name in the published catalogue was rejected here at
    compile time. A documented name that fails to compile is the worst class of
    catalogue error: the user follows the catalogue and gets a rejection.

    Deriving the table from the manifest makes "what the catalogue advertises"
    and "what dispatch accepts" the same fact rather than two copies that drift
    (blueprint 05 §Connector Registration: the manifest is the single source of
    truth for the (connector, intent, chain) universe).

    Both spellings route to the connector's compiler key, because the compiler
    registry is keyed on ``compiler_protocols``.
    """
    spellings: dict[str, str] = {}
    published: list[str] = []
    for connector in CONNECTOR_REGISTRY.all():
        if not _LP_INTENT_TYPES.intersection(connector.strategy_intents or ()):
            continue
        # ``family_of`` answers ``None`` for a name it cannot resolve, so a typo in a
        # declared chain drops that connector from this table and its protocol then
        # fails with a misleading "not supported". That is a real hazard -- but it is
        # a MANIFEST TYPO, i.e. a lint-class defect, and the fix belongs at build
        # time. An earlier revision of this function resolved strictly and raised
        # here; it was wrong twice over. This loop runs for every connector declaring
        # LP intents -- 12 of them, 9 EVM-only -- and ``SvmFamily.compile_intent``
        # calls ``solana_lp_spellings()`` on every LP_OPEN/LP_CLOSE on a non-Solana
        # chain, so a one-character typo in (say) Curve's ``supported_chains`` failed
        # every Uniswap V3 LP compile on Arbitrum. Narrowing to Solana connectors
        # first cannot help either: a typo'd ``"solanaa"`` is exactly the case the
        # narrowing would exclude, so strictness there is unreachable by
        # construction.
        #
        # The invariant is therefore asserted where it can fail loudly and harm
        # nothing: ``scripts/ci/check_connector_registry.py`` (CI-wired today) plus
        # ``check_chain_truth_agreement.py``'s unregistered-chain check. Note those
        # gates are defense in depth; ``SupportedChainsSpec`` also canonicalises
        # and rejects unknown chains during descriptor construction.
        if not any(ChainRegistry.family_of(chain) is ChainFamily.SOLANA for chain in connector.all_supported_chains):
            continue
        compiler_keys = sorted(connector.compiler_keys or ())
        if not compiler_keys:
            continue
        if len(compiler_keys) > 1:
            # Fail loud rather than pick one: a silently-partial table is the
            # defect this derivation exists to remove.
            raise RuntimeError(
                f"Connector {connector.name!r} declares Solana LP intents and "
                f"{len(compiler_keys)} compiler keys ({', '.join(compiler_keys)}); "
                "Solana LP dispatch cannot decide which one its published name "
                "routes to. Give the connector a single compiler key, or teach "
                "_solana_lp_routing() an explicit rule."
            )
        compiler_key = compiler_keys[0]
        published.append(connector.name)
        for spelling in (connector.name, compiler_key, *connector.aliases):
            # Same fold as ``resolve_solana_lp_protocol`` -- keys built with a bare
            # ``.lower()`` while the lookup also stripped and mapped ``-``->``_``
            # meant a hyphenated spelling missed the table.
            key = _fold_protocol(spelling)
            claimed = spellings.get(key)
            if claimed is not None and claimed != compiler_key:
                raise RuntimeError(
                    f"Solana LP protocol spelling {key!r} is claimed by both {claimed!r} and {compiler_key!r}."
                )
            spellings[key] = compiler_key
    return _SolanaLpRouting(MappingProxyType(spellings), tuple(sorted(published)))


# ``_connector_descriptor`` requires consumers that memoize registry-derived
# decisions to register their cache-clear, so a test-hook registry reset cannot
# leave them stale. Without this a test that clears the registry to prove this
# table tracks the manifest passes against a frozen table -- a false green for
# exactly the manifest-to-dispatch drift this module exists to remove.
CONNECTOR_REGISTRY.on_clear(_solana_lp_routing.cache_clear)


def solana_lp_spellings() -> frozenset[str]:
    """Every accepted spelling of a Solana-only LP protocol, lowercased.

    Used by :class:`SvmFamily` to recognise a Solana-only protocol declared on
    an EVM chain, so the connector compiler's own chain check can emit the
    canonical "<Protocol> is only supported on Solana" error.
    """
    return frozenset(_solana_lp_routing().compiler_key_by_spelling)


def resolve_solana_lp_protocol(protocol: str) -> str | None:
    """Return the connector compiler key for ``protocol``, or ``None``.

    Accepts the published manifest name, the compiler key, and any declared
    manifest alias. Folds case and hyphens the same way
    :func:`normalize_protocol` does, so callers that only need to identify a
    Solana LP connector do not have to name a chain to normalise against.
    """
    return _solana_lp_routing().compiler_key_by_spelling.get(_fold_protocol(protocol))


def dispatch_swap(compiler: IntentCompiler, intent: SwapIntent) -> CompilationResult:
    """Route a SWAP intent on Solana to the correct adapter.

    Only entered when the compiler chain is Solana (caller-gated in
    :class:`SvmFamily.compile_intent`).
    """
    protocol = intent.protocol
    # ``protocol is None`` falls through to the Jupiter default; only an
    # explicitly-set, non-jupiter protocol is rejected.
    if protocol and protocol.lower() not in _ALLOWED_SOLANA_SWAP_PROTOCOLS:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            intent_id=intent.intent_id,
            error=f"Protocol '{protocol}' is not supported for SWAP on Solana. Supported: jupiter",
        )
    connector_compiler = get_connector_compiler("jupiter")
    if connector_compiler is None:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            intent_id=intent.intent_id,
            error="Connector compiler for protocol 'jupiter' is not registered.",
        )
    return connector_compiler.compile(compiler._build_compiler_context("jupiter", connector_compiler), intent)


def _dispatch_lp_via_connector(
    compiler: IntentCompiler,
    intent: LPOpenIntent | LPCloseIntent,
    is_solana_chain: bool,
    *,
    intent_label: str,
) -> CompilationResult:
    """Shared LP_OPEN / LP_CLOSE dispatch helper.

    Mirrors ``IntentCompiler._resolve_lp_protocol`` + connector-registry
    dispatch from the post-#2416 framework path, but kept in the SVM family
    boundary so the family adapter owns the (chain, protocol) decision matrix.

    * On Solana chains, normalise ``protocol=None`` to the default
      (``raydium_clmm``), reject non-Solana LP protocols with the canonical
      ``"Protocol 'X' is not supported for {intent_label} on Solana"`` error,
      then dispatch to the connector compiler.
    * On non-Solana chains (entered only when ``intent.protocol`` is
      Solana-only), dispatch to the connector compiler - the connector
      enforces ``chain in {"solana"}`` itself and emits the explicit
      "<Protocol> is only supported on Solana" error.
    """
    protocol = intent.protocol

    if protocol is None:
        # Only reachable on a Solana chain: ``SvmFamily`` routes a
        # protocol-less LP intent to the EVM path on every other chain.
        assert is_solana_chain
        resolved: str | None = _SOLANA_DEFAULT_LP_PROTOCOL
    else:
        canonical = normalize_protocol(compiler.chain, protocol)
        resolved = resolve_solana_lp_protocol(canonical)
        if resolved is None:
            if is_solana_chain:
                # Advertise the published manifest names, which is what
                # ``almanak info matrix`` shows the user.
                supported = ", ".join(_solana_lp_routing().published_names)
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=(
                        f"Protocol '{protocol}' is not supported for {intent_label} on Solana. Supported: {supported}"
                    ),
                )
            # Non-Solana chain, and not a protocol the Solana table owns: fall
            # through on the canonical spelling so the connector compiler's own
            # chain check produces the canonical
            # "<Protocol> is only supported on Solana" error.
            resolved = canonical

    connector_compiler = get_connector_compiler(resolved)
    if connector_compiler is None:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            intent_id=intent.intent_id,
            error=f"Connector compiler for protocol '{resolved}' is not registered.",
        )
    return connector_compiler.compile(compiler._build_compiler_context(resolved, connector_compiler), intent)


def dispatch_lp_open(
    compiler: IntentCompiler,
    intent: LPOpenIntent,
    is_solana_chain: bool,
) -> CompilationResult:
    """Route an LP_OPEN intent to the per-connector compiler."""
    return _dispatch_lp_via_connector(compiler, intent, is_solana_chain, intent_label="LP_OPEN")


def dispatch_lp_close(
    compiler: IntentCompiler,
    intent: LPCloseIntent,
    is_solana_chain: bool,
) -> CompilationResult:
    """Route an LP_CLOSE intent to the per-connector compiler (symmetric to :func:`dispatch_lp_open`)."""
    return _dispatch_lp_via_connector(compiler, intent, is_solana_chain, intent_label="LP_CLOSE")
