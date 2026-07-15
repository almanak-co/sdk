"""Per-chain stablecoin resolution (VIB-5727).

Answers two questions that are deliberately kept SEPARATE, because conflating
them is the bug this module exists to fix:

* **Existence** — "does symbol X resolve on chain Y?" → :func:`token_resolves_on_chain`.
  Owned by :class:`~almanak.framework.data.tokens.resolver.TokenResolver` and its
  static catalogue. **Never** infer this from ``ChainDescriptor.tokens``: that
  mapping is ``None`` on several chains (berachain, solana, blast, plasma,
  zerog) where USDC nonetheless resolves perfectly well, because the resolver
  reads an independent registry. Inferring existence from ``tokens`` silently
  regresses those chains.
* **Pick** — "which dollar do we mean on chain Y?" → :func:`resolve_chain_stable`.
  A chain-level judgement that ``ChainDescriptor.canonical_stable`` declares as
  data where a registry-ordering heuristic would get it wrong.

Why a declared field rather than a smarter ordering: on Robinhood (4663) both
USDG and USDe are registered stablecoins, so *any* ordering over registry data
is free to pick either — and the framework's generic picker
(``permissions/synthetic_intents.py:_candidate_stable_symbols``) picks USDe.
That is not just arbitrary, it is unroutable: USDe has zero-liquidity pools on
4663 (VIB-5729) while WETH/USDG is the only real V3 pool. The deciding fact —
which dollar has liquidity — is not present in any registry field, so it must be
declared, not derived.

Ordering contract for :func:`chain_stable_symbols`:

1. ``descriptor.canonical_stable`` — the declared per-chain override.
2. ``descriptor.default_display_tokens`` ∩ stablecoins — the chain's own
   display preference, already curated per chain.
3. Registry order over ``DEFAULT_TOKENS`` — last resort, deterministic.

.. note::
   ``permissions/synthetic_intents.py:_candidate_stable_symbols`` implements
   steps 2-3 independently and does **not** consult ``canonical_stable``, which
   is why Robinhood needs a hand-pinned override in
   ``uniswap_v3/permission_hints.py``. Folding that picker into this module is
   tracked separately: it flips the Zodiac Roles manifest, where a wrong pin
   reverts every call at ``execTransactionWithRole``, so it needs its own
   manifest-regression review rather than riding along with a teardown fix.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

__all__ = [
    "chain_stable_symbols",
    "resolve_chain_stable",
    "token_resolves_on_chain",
]


def _descriptor(chain: str):
    """Best-effort ChainDescriptor for *chain*; ``None`` on any miss."""
    try:
        from almanak.core.chains import ChainRegistry

        return ChainRegistry.try_resolve(chain)
    except Exception:  # noqa: BLE001 — an unknown chain is a miss, not an error
        logger.debug("ChainRegistry could not resolve chain=%r", chain, exc_info=True)
        return None


def token_resolves_on_chain(symbol: str, chain: str) -> bool:
    """True when *symbol* resolves to an address on *chain*.

    The authoritative existence check — this is what decides whether a
    consolidation target is usable. Uses ``skip_gateway=True`` so the answer is
    a pure local registry lookup: no egress (the gateway-boundary rule applies
    to this layer), no ~30s gateway deadline on a symbol that will never
    resolve (the VIB-5746 busy-timeout), and no chance of a dynamic
    DexScreener/CoinGecko lookup silently returning a same-ticker impostor.
    """
    if not symbol or not chain:
        return False
    try:
        from almanak.framework.data.tokens import get_token_resolver

        get_token_resolver().resolve(symbol, chain, log_errors=False, skip_gateway=True)
        return True
    except Exception:  # noqa: BLE001 — any miss/unavailability means "cannot use it"
        return False


def chain_stable_symbols(chain: str) -> tuple[str, ...]:
    """Candidate stablecoin symbols for *chain*, best-first.

    Deterministic and duplicate-free. Membership here means "registered as a
    stablecoin on this chain" — NOT that it resolves; callers that need a
    usable symbol should use :func:`resolve_chain_stable`.
    """
    try:
        from almanak.framework.data.tokens.defaults import DEFAULT_TOKENS
    except Exception:  # noqa: BLE001 — no catalogue means no candidates
        logger.debug("DEFAULT_TOKENS unavailable for chain=%r", chain, exc_info=True)
        return ()

    descriptor = _descriptor(chain)
    token_chain = descriptor.name if descriptor is not None else chain

    stable_symbols = {
        token.symbol.upper() for token in DEFAULT_TOKENS if token.is_stablecoin and token.has_address_on(token_chain)
    }

    symbols: list[str] = []
    seen: set[str] = set()

    def add(symbol: str | None, *, require_stable: bool = True) -> None:
        if not symbol:
            return
        key = symbol.upper()
        if require_stable and key not in stable_symbols:
            return
        if key in seen:
            return
        seen.add(key)
        symbols.append(symbol)

    if descriptor is not None:
        # 1. The declared per-chain override. `require_stable=False`: the field
        #    is the chain's own declaration of its dollar and outranks the
        #    registry's `is_stablecoin` flag, which is not reliable per-chain
        #    (USDe is flagged True on robinhood but False on base/arbitrum).
        #    A declared symbol that does not resolve is caught by the caller
        #    (`resolve_chain_stable`) and by the descriptor unit test.
        add(descriptor.canonical_stable, require_stable=False)

        # 2. The chain's curated display order, filtered to stables.
        for symbol in descriptor.default_display_tokens or ():
            add(symbol)

    # 3. Registry order — deterministic last resort.
    for token in DEFAULT_TOKENS:
        if token.is_stablecoin and token.has_address_on(token_chain):
            add(token.symbol)

    return tuple(symbols)


def resolve_chain_stable(chain: str) -> str | None:
    """The best stablecoin symbol that actually resolves on *chain*.

    ``None`` when the chain has no resolvable stablecoin at all (e.g. blast) —
    callers decide the fallback (wrapped native, or degrade). Never guesses.
    """
    for symbol in chain_stable_symbols(chain):
        if token_resolves_on_chain(symbol, chain):
            return symbol
    return None
