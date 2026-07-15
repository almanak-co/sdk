"""``ChainDescriptor.canonical_stable`` invariants (VIB-5727).

``core`` cannot import the ``framework`` token layer to validate the declared
symbol at construction time (that would be a backward import), so the "it must
actually resolve" invariant is enforced here instead. A descriptor declaring a
symbol the resolver cannot find would silently degrade teardown consolidation
to the next fallback — the failure this field exists to prevent.
"""

import pytest

from almanak.core.chains import ChainRegistry


def _declared() -> list[tuple[str, str]]:
    """(chain, canonical_stable) for every chain that declares one."""
    out = []
    for name in sorted(ChainRegistry.names()):
        descriptor = ChainRegistry.get(name)
        if descriptor.canonical_stable:
            out.append((name, descriptor.canonical_stable))
    return out


class TestCanonicalStableDeclarations:
    def test_robinhood_declares_usdg(self):
        """The chain the field exists for.

        Both USDG and USDe are registered stablecoins on 4663, so a registry
        ordering is free to pick either — and the generic picker picks USDe,
        which has zero-liquidity pools there (VIB-5729). Only USDG routes.
        """
        assert ChainRegistry.get("robinhood").canonical_stable == "USDG"

    def test_declared_symbols_resolve_on_their_chain(self):
        """A declared symbol MUST resolve — this is the check core/ cannot do."""
        from almanak.framework.data.tokens.chain_stable import token_resolves_on_chain

        declared = _declared()
        assert declared, "expected at least one chain to declare canonical_stable"
        for chain, symbol in declared:
            assert token_resolves_on_chain(symbol, chain), (
                f"{chain} declares canonical_stable={symbol!r} but it does not resolve there"
            )

    def test_declared_symbol_is_ranked_first(self):
        """The declaration must actually win over registry ordering.

        Without this, the field could be declared and silently ignored — the
        exact class of bug that made robinhood pick USDe.
        """
        from almanak.framework.data.tokens.chain_stable import chain_stable_symbols

        for chain, symbol in _declared():
            candidates = chain_stable_symbols(chain)
            assert candidates, f"{chain} declares {symbol!r} but has no candidates"
            assert candidates[0].upper() == symbol.upper(), (
                f"{chain} declares canonical_stable={symbol!r} but the picker ranks "
                f"{candidates[0]!r} first"
            )

    def test_undeclared_chains_are_none_not_empty_string(self):
        """Empty ≠ Zero: absence is None, never a falsy placeholder symbol."""
        for name in sorted(ChainRegistry.names()):
            value = ChainRegistry.get(name).canonical_stable
            assert value is None or (isinstance(value, str) and value.strip()), (
                f"{name}: canonical_stable must be None or a non-blank symbol, got {value!r}"
            )

    @pytest.mark.parametrize("chain", ["ethereum", "arbitrum", "base", "optimism", "polygon"])
    def test_liquid_usdc_chains_do_not_declare(self, chain):
        """Sparse by design.

        Chains with a liquid Circle-USDC need no override — USDC already wins on
        its own. Declaring one there would be noise that changes nothing, and
        would invite ranking USDC vs USDT vs DAI, a judgement this field
        deliberately does not make.
        """
        assert ChainRegistry.get(chain).canonical_stable is None
