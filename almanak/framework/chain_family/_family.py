"""ChainFamilyAdapter Protocol + EvmFamily / SvmFamily implementations.

VIB-4803. See package docstring for the design rationale.

Public surface:

* :class:`ChainFamilyAdapter` — behavior Protocol (``name``, ``signer_factory``,
  ``address_checksum``, ``compile_intent``, ``parse_receipt``).
* :class:`EvmFamily`, :class:`SvmFamily` — concrete adapters.
* :func:`family_for(chain_name)` — primary lookup used by call sites that already
  hold a chain name string (``IntentCompiler.chain``, connector ``ctx.chain``).
* :func:`family_for_chain_enum(chain_enum)` — lookup by :class:`Chain` enum.
* :func:`family_for_kind(kind)` — lookup by :class:`ChainFamily` enum kind.

Receipt parsing pre-dispatch hook
---------------------------------

The ticket calls for ``family.parse_receipt`` to be wired as a *pre-dispatch hook*
that normalizes raw receipts to an envelope shape the protocol registry already
understands. The receipt registry is currently protocol-keyed, not family-keyed,
and a clean cutover requires touching every receipt parser entry-point. That
is intentionally scoped OUT of this PR — see the PR body for the follow-up
ticket. The :meth:`parse_receipt` method is defined on the protocol with an
identity implementation today so that future work can swap in a real
normalization step without changing call sites.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from almanak.core.chains import ChainRegistry
from almanak.core.enums import Chain, ChainFamily

if TYPE_CHECKING:
    from almanak.framework.intents.compiler import IntentCompiler
    from almanak.framework.intents.compiler_models import CompilationResult
    from almanak.framework.intents.vocabulary import AnyIntent


# ---------------------------------------------------------------------------
# Compile context
# ---------------------------------------------------------------------------


class CompileContext(Protocol):
    """Minimal shape :meth:`ChainFamilyAdapter.compile_intent` consumes.

    Today the ``IntentCompiler`` instance itself is the context — SVM dispatch
    still needs the compiler to build connector compiler contexts. We accept
    that as the contract for VIB-4803 to keep the diff focused on the dispatch
    refactor. A typed ``CompileContext`` dataclass — independent of
    ``IntentCompiler`` — is a worthwhile follow-up, but is not required to flip
    the dispatch from ``_is_solana_chain()`` to ``family.compile_intent(...)``.
    """

    chain: str
    wallet_address: str


# ---------------------------------------------------------------------------
# ChainFamilyAdapter Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class ChainFamilyAdapter(Protocol):
    """Behavior protocol owned by a chain family (EVM, SVM, ...).

    The protocol is intentionally narrow: it carries only the state-machine
    boundaries where the EVM and SVM substrates *actually* diverge (signing,
    address format, intent compilation, receipt envelope shape). Anything that
    is the same on both substrates lives in the shared code path.
    """

    name: str
    """Short kind name (``"evm"``, ``"svm"``). Used in error messages, metrics,
    and the exhaustive-case check in :func:`family_for_kind`."""

    kind: ChainFamily
    """The :class:`ChainFamily` enum member this adapter handles. The reverse
    lookup from a registered :class:`ChainDescriptor` to its adapter happens
    via this field."""

    def signer_factory(self, descriptor: Any) -> Any:
        """Return the family's signer namespace.

        Each implementation hands back the module that owns the signer
        hierarchy for its substrate:

        * :class:`EvmFamily` returns ``almanak.framework.execution.signer``
          (with :class:`LocalKeySigner`, :func:`create_safe_signer`,
          :class:`DirectSafeSigner`, :class:`ZodiacSigner`, ...).
        * :class:`SvmFamily` returns ``almanak.framework.execution.solana``
          (with :class:`SolanaSigner`, :class:`SolanaSignerError`,
          :class:`SolanaExecutionPlanner`, ...).

        The ``descriptor`` argument is currently unused — both substrates
        instantiate their concrete signer from a private key held by the
        gateway, not from per-chain descriptor state. Reserved for a later
        cutover where the descriptor carries chain-specific signer config
        (e.g. wrapped-native, signing program ID).

        Gateway boundary
        ----------------

        Returning a module *namespace* rather than a constructed signer is
        deliberate: the caller (gateway ``ExecutionService``) is the only
        component that holds key material and is the only component allowed
        to call ``LocalKeySigner(...)`` / ``SolanaSigner.from_base58(...)``.
        Framework and strategy code may consume the returned namespace for
        type imports (``SignedTransaction``, ``SolanaSignerError``) without
        ever invoking the constructors.
        """
        ...

    def address_checksum(self, addr: str) -> str:
        """Return the family's canonical normalization of ``addr``.

        EVM: EIP-55 checksum (delegates to :func:`eth_utils.to_checksum_address`).
        SVM: base58 mints are case-sensitive and are returned unchanged.
        """
        ...

    def compile_intent(self, compiler: IntentCompiler, intent: AnyIntent) -> CompilationResult | None:
        """Family-specific compilation hook.

        Returns a :class:`CompilationResult` when the family OWNS the dispatch
        for ``intent`` (and the caller must not fall through to other branches),
        or ``None`` when the shared EVM dispatch path should continue.

        This shape lets ``IntentCompiler._dispatch_*`` keep its existing
        "Solana takes the wheel, otherwise fall through to EVM" semantics
        while collapsing every ``self._is_solana_chain()`` branch into a single
        polymorphic call.
        """
        ...

    def parse_receipt(self, raw: Any) -> Any:
        """Family-specific receipt envelope normalization.

        VIB-4803 ships an identity pass-through. The real normalization layer
        is scoped to a follow-up ticket (see module docstring). Keeping the
        method on the protocol now means call sites don't need to be touched
        again when the real implementation lands.
        """
        ...

    def default_swap_protocol(self) -> str | None:
        """Family-default protocol used in IntentCompiler init logging.

        Returns ``None`` for families that have no opinion (EVM uses the
        compiler's ``default_protocol`` constructor argument). Override on
        families with a single dominant on-chain swap venue (e.g. SVM
        returns ``"jupiter"``).

        This is logging-only — runtime dispatch still goes through
        :meth:`compile_intent`. The seam exists so framework code stays
        free of ``isinstance(self._family, SvmFamily)`` checks.
        """
        ...


# ---------------------------------------------------------------------------
# EvmFamily
# ---------------------------------------------------------------------------


class EvmFamily:
    """Adapter for EVM chains (Ethereum, Arbitrum, Base, ...)."""

    name: str = "evm"
    kind: ChainFamily = ChainFamily.EVM

    def signer_factory(self, descriptor: Any) -> Any:
        """Return the EVM signer module/namespace.

        The intent compiler does not call the signer directly; the
        :class:`SignerFactory` work is owned by ``framework.execution.signer``.
        Returning the module gives callers access to ``LocalKeySigner``,
        ``DirectSafeSigner``, ``ZodiacSigner``, ``create_safe_signer``, etc.,
        through a single seam — VIB-4804 will narrow this to a concrete
        ``SignerFactory`` protocol once the SVM signer is in place.
        """
        from almanak.framework.execution import signer as evm_signer

        return evm_signer

    def address_checksum(self, addr: str) -> str:
        """Return the EIP-55 checksum form of ``addr``."""
        from eth_utils import to_checksum_address

        return to_checksum_address(addr)

    def compile_intent(self, compiler: IntentCompiler, intent: AnyIntent) -> CompilationResult | None:
        """EVM compilation hook.

        Returns ``None`` so that the caller falls through to its existing
        EVM-side dispatch (the shared connector registry path). EVM is the
        default substrate in :class:`IntentCompiler`, so the family's job
        here is to refuse to short-circuit — not to duplicate the dispatch
        table.
        """
        return None

    def parse_receipt(self, raw: Any) -> Any:
        """Identity pass-through (see module docstring)."""
        return raw

    def default_swap_protocol(self) -> str | None:  # noqa: D401
        """EVM has no single dominant venue; defer to the compiler's default_protocol."""
        return None


# ---------------------------------------------------------------------------
# SvmFamily
# ---------------------------------------------------------------------------


class SvmFamily:
    """Adapter for Solana / SVM chains."""

    name: str = "svm"
    kind: ChainFamily = ChainFamily.SOLANA

    def signer_factory(self, descriptor: Any) -> Any:
        """Return the SVM signer namespace.

        Mirrors :meth:`EvmFamily.signer_factory` — hands back the module
        that owns the Solana signing hierarchy
        (:class:`SolanaSigner`, :class:`SolanaSignerError`,
        :class:`SolanaExecutionPlanner`).

        Gateway boundary
        ----------------

        The returned namespace is the *only* SVM signing seam in the
        codebase. The keypair lives in the gateway's
        :attr:`Settings.solana_private_key` and is consumed by
        :meth:`gateway.services.execution_service.ExecutionService._get_solana_planner`,
        which constructs the :class:`SolanaExecutionPlanner` (which in
        turn constructs the :class:`SolanaSigner`). Strategy code and
        framework connector code (Jupiter, Kamino, Raydium, Orca,
        Meteora) never touch the keypair — they emit unsigned
        base64-encoded ``VersionedTransaction`` blobs through
        :class:`ActionBundle.transactions` and the gateway-side planner
        is the one component that opens them, signs them, and submits
        them.

        Speculative-refactor scope note (VIB-4804)
        ------------------------------------------

        The parent ticket explicitly flagged this stage as deferrable
        until a real new SVM consumer appears. The user overrode that
        deferral. To honour the "don't speculatively expand" constraint:
        we do NOT introduce a new ``SignerFactory``-style class, a new
        descriptor argument, or per-chain signer config. We return the
        module namespace — the same shape :class:`EvmFamily` uses — and
        rely on the existing :class:`SolanaSigner` (which already
        handles legacy + versioned base64 transactions, multi-signer
        bundles for Raydium NFT-mint flows, and arbitrary-message
        signing). When a future SVM connector needs something the
        current ``SolanaSigner`` cannot do — Address Lookup Table
        resolution at sign time, partial-sign for multi-party flows,
        signer rotation — that connector's PR will widen the namespace.
        """
        from almanak.framework.execution import solana as svm_signer

        return svm_signer

    def address_checksum(self, addr: str) -> str:
        """Solana mints / pubkeys are base58 and case-sensitive.

        We return ``addr`` unchanged — re-encoding would either drop
        information (if normalized to a different alphabet) or no-op (if the
        same alphabet). Validation belongs in the SVM connector adapters,
        not the family-level pre-dispatch hook.
        """
        return addr

    def compile_intent(self, compiler: IntentCompiler, intent: AnyIntent) -> CompilationResult | None:
        """Solana-specific compilation dispatch.

        Returns a :class:`CompilationResult` when SVM owns the intent (every
        SWAP / LP / lending intent on a Solana chain, plus the explicit
        ``meteora_dlmm`` / ``orca_whirlpools`` / ``raydium_clmm`` protocol
        routes on any chain).

        Returns ``None`` only on the *cross-chain SVM-protocol-on-EVM-chain*
        edge case where ``intent.protocol`` declares a Solana-only protocol
        but the compiler's chain is EVM — those land at the FAILED branch in
        the SVM dispatch helpers, which themselves return a
        :class:`CompilationResult` (not ``None``), so in practice this method
        always returns a result for the cases it owns.
        """
        from almanak.framework.intents.vocabulary import (
            IntentType,
            LPCloseIntent,
            LPOpenIntent,
            SwapIntent,
        )

        from . import _svm_dispatch

        is_solana_chain = _chain_name_is_solana(compiler.chain)
        raw_protocol = getattr(intent, "protocol", None)
        # Case-insensitive membership check so an LP intent with ``protocol``
        # spelled "Meteora_DLMM" (or any variant) still trips the cross-chain
        # Solana-only branch. ``_svm_dispatch`` performs its own
        # canonicalisation via ``normalize_protocol`` before the connector
        # registry lookup, so we don't pass a lowercased value downstream.
        protocol_key = raw_protocol.lower() if isinstance(raw_protocol, str) else None

        if intent.intent_type is IntentType.SWAP:
            if not is_solana_chain:
                return None
            assert isinstance(intent, SwapIntent)
            return _svm_dispatch.dispatch_swap(compiler, intent)

        if intent.intent_type is IntentType.LP_OPEN:
            if not (is_solana_chain or protocol_key in _svm_dispatch._ALLOWED_SOLANA_LP_PROTOCOLS):
                return None
            assert isinstance(intent, LPOpenIntent)
            return _svm_dispatch.dispatch_lp_open(compiler, intent, is_solana_chain)

        if intent.intent_type is IntentType.LP_CLOSE:
            if not (is_solana_chain or protocol_key in _svm_dispatch._ALLOWED_SOLANA_LP_PROTOCOLS):
                return None
            assert isinstance(intent, LPCloseIntent)
            return _svm_dispatch.dispatch_lp_close(compiler, intent, is_solana_chain)

        # Other intent types (lending, perp, ...) are owned by connector
        # compilers (kamino, jupiter_lend, drift, ...) and dispatched via the
        # connector registry. SvmFamily does not intercept them.
        return None

    def parse_receipt(self, raw: Any) -> Any:
        """Identity pass-through (see module docstring)."""
        return raw

    def default_swap_protocol(self) -> str | None:
        """Solana's dominant on-chain swap aggregator is Jupiter."""
        return "jupiter"


# ---------------------------------------------------------------------------
# Registry: ChainFamily kind -> adapter
# ---------------------------------------------------------------------------


_FAMILY_ADAPTERS: dict[ChainFamily, ChainFamilyAdapter] = {
    ChainFamily.EVM: EvmFamily(),
    ChainFamily.SOLANA: SvmFamily(),
}

# Cached tuple consumed by :func:`all_families` on the hot dispatch path
# (``IntentCompiler._family_compile_intent`` calls this per intent). The
# adapter set is module-static after import, so we build the tuple once at
# module load rather than allocating a fresh one per call.
_ALL_FAMILIES: tuple[ChainFamilyAdapter, ...] = tuple(_FAMILY_ADAPTERS[kind] for kind in ChainFamily)


def family_for_kind(kind: ChainFamily) -> ChainFamilyAdapter:
    """Return the adapter for a :class:`ChainFamily` enum kind.

    Raises :class:`KeyError` for an unmapped kind — a programming error
    (every enum member must have a registered adapter).
    """
    try:
        return _FAMILY_ADAPTERS[kind]
    except KeyError as exc:
        raise KeyError(
            f"No ChainFamilyAdapter registered for ChainFamily.{kind.name}. "
            f"Register one in almanak/framework/chain_family/_family.py."
        ) from exc


def all_families() -> tuple[ChainFamilyAdapter, ...]:
    """Return every registered family adapter.

    Used by :class:`IntentCompiler._dispatch_*` so dispatch is family-driven:
    the compiler asks every registered family whether it owns the intent
    before falling through to the connector registry. Iteration order is
    deterministic — :class:`ChainFamily` enum order — so adding a new
    family does not perturb existing dispatch precedence. Returns the
    module-cached tuple (see :data:`_ALL_FAMILIES`) to avoid per-call
    allocation on the hot path.
    """
    return _ALL_FAMILIES


def family_for_chain_enum(chain_enum: Chain) -> ChainFamilyAdapter:
    """Return the adapter for a :class:`Chain` enum member.

    Resolves via :class:`ChainRegistry` so the family is read off the
    descriptor (the authoritative VIB-4801 source of truth). The legacy
    ``get_chain_family()`` map is kept around for byte-identity tests but
    is not the lookup path here — a descriptor edit that doesn't sync
    ``CHAIN_FAMILY_MAP`` would silently return the wrong adapter if we
    consulted the map.
    """
    descriptor = ChainRegistry.get(chain_enum)
    return family_for_kind(descriptor.family)


def family_for(chain_name: str) -> ChainFamilyAdapter:
    """Return the adapter for a chain name string (canonical or alias).

    Falls back to :class:`EvmFamily` when the chain name is unknown — this
    matches the legacy ``_is_solana_chain`` contract of "treat unknown as
    not-Solana so EVM dispatch handles it". Unknown chains will fail later in
    the EVM dispatch path (correctly, with a real protocol error).
    """
    descriptor = ChainRegistry.try_resolve(chain_name or "")
    if descriptor is None:
        return _FAMILY_ADAPTERS[ChainFamily.EVM]
    return family_for_kind(descriptor.family)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _chain_name_is_solana(chain_name: str) -> bool:
    """``True`` iff the chain resolves to the SOLANA family.

    Replaces the scattered ``chain.lower() == "solana"`` and
    ``_is_solana_chain(compiler)`` checks. Use :func:`family_for` for new
    code that needs polymorphic dispatch; this helper exists for the handful
    of remaining string-compare sites that are not yet on the family-adapter
    seam (typed-state predicates, etc.).
    """
    descriptor = ChainRegistry.try_resolve(chain_name or "")
    if descriptor is None:
        return False
    return descriptor.family is ChainFamily.SOLANA
