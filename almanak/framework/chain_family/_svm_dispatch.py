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

from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.compiler_registry import get_compiler as get_connector_compiler
from almanak.connectors._strategy_base.protocol_aliases import normalize_protocol
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus

if TYPE_CHECKING:
    from almanak.framework.intents.compiler import IntentCompiler
    from almanak.framework.intents.vocabulary import (
        LPCloseIntent,
        LPOpenIntent,
        SwapIntent,
    )


_ALLOWED_SOLANA_SWAP_PROTOCOLS = {"jupiter"}
_ALLOWED_SOLANA_LP_PROTOCOLS = {"raydium_clmm", "meteora_dlmm", "orca_whirlpools"}
_SOLANA_DEFAULT_LP_PROTOCOL = "raydium_clmm"


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

    if is_solana_chain:
        if protocol is None:
            resolved = _SOLANA_DEFAULT_LP_PROTOCOL
        else:
            resolved = normalize_protocol(compiler.chain, protocol)
        if resolved not in _ALLOWED_SOLANA_LP_PROTOCOLS:
            supported = ", ".join(sorted(_ALLOWED_SOLANA_LP_PROTOCOLS))
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(f"Protocol '{protocol}' is not supported for {intent_label} on Solana. Supported: {supported}"),
            )
    else:
        # Non-Solana chain with a Solana-only protocol declared. The connector
        # compiler's own chain check produces the canonical
        # "<Protocol> is only supported on Solana" error.
        assert protocol is not None
        resolved = normalize_protocol(compiler.chain, protocol)

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
