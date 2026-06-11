"""Cross-source invariant: registry native symbols must resolve in the token registry.

The swap adapters' native gate (``_is_native_token``) is derived per-chain from
``ChainDescriptor.native`` via ``native_symbols_for`` (VIB-4851 A1). When the
gate fires, the adapter routes the input down the wrap-via-msg.value path:
``resolve_for_swap(symbol, chain)`` must resolve the symbol (a native-sentinel
entry in the static token registry) and then resolve the chain's
``WRAPPED_NATIVE`` address. If either lookup is missing, the native path raises
TokenNotFoundError at the wrap step — the exact failure mode the historical
comment on ``UniswapV3Adapter._is_native_token`` warned about for "0G" vs A0GI.

These tests pin the invariant for every EVM chain in the registry, so adding a
chain descriptor without the matching token-registry entries fails loudly here
instead of at swap time.
"""

from __future__ import annotations

import pytest

from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import native_symbols_for
from almanak.core.enums import ChainFamily
from almanak.framework.data.tokens.defaults import WRAPPED_NATIVE
from almanak.framework.data.tokens.resolver import TokenResolver

EVM_CHAINS = sorted(d.name for d in ChainRegistry.all() if d.family is ChainFamily.EVM)


@pytest.mark.parametrize("chain", EVM_CHAINS)
def test_native_symbols_resolve_to_native_sentinel(chain: str) -> None:
    """Every advertised native symbol resolves statically and is flagged native."""
    resolver = TokenResolver.get_instance()
    symbols = native_symbols_for(chain)
    assert symbols, f"{chain}: registry advertises no native symbols"
    for symbol in symbols:
        resolved = resolver.resolve(symbol, chain, skip_gateway=True, log_errors=False)
        assert resolved.is_native, f"{symbol} on {chain} resolved to a non-native address: {resolved.address}"


@pytest.mark.parametrize("chain", EVM_CHAINS)
def test_native_symbols_survive_the_wrap_step(chain: str) -> None:
    """``resolve_for_swap`` lands on the wrapped ERC-20, never back on native."""
    resolver = TokenResolver.get_instance()
    assert WRAPPED_NATIVE.get(chain), f"{chain}: no wrapped_native_address in chains.json"
    for symbol in native_symbols_for(chain):
        wrapped = resolver.resolve_for_swap(symbol, chain)
        assert not wrapped.is_native, f"{symbol} on {chain}: wrap step returned a native token"
        assert wrapped.address.lower() == WRAPPED_NATIVE[chain].lower(), (
            f"{symbol} on {chain}: wrap step resolved {wrapped.address}, "
            f"expected WRAPPED_NATIVE {WRAPPED_NATIVE[chain]}"
        )
