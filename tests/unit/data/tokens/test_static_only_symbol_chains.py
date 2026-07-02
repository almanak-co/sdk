"""Tests for STATIC-ONLY symbol resolution on HyperEVM (VIB-5576).

On HyperEVM the chain's real ERC-20s (USDC / USDT0 / WHYPE / HYPE) are
statically registered, and every OTHER bare symbol a strategy hands the
resolver ("ETH", "BTC", …) is a HyperCore *perp index* — not a balance-able
ERC-20. ERC-20 symbol discovery for such a symbol can never succeed, so it must
fail INSTANTLY (no ~15s gateway ``ResolveToken`` dynamic-symbol lookup) so a
doomed ``GetBalance("ETH")`` returns in <100ms instead of burning ~15s twice per
iteration.

The gate is scoped to ``chain in STATIC_ONLY_SYMBOL_CHAINS`` and must NOT change
resolution behaviour for any other chain (arbitrum/base/ethereum keep dynamic
symbol search), nor break address resolution on HyperEVM.
"""

from unittest.mock import MagicMock, patch

import pytest


class TestStaticOnlySymbolChains:
    """Symbol resolution on STATIC-ONLY chains must never hit the gateway."""

    def _make_resolver_with_gateway(self):
        """Create a fresh TokenResolver wired to a mock gateway channel."""
        from almanak.framework.data.tokens.resolver import TokenResolver

        TokenResolver.reset_instance()
        return TokenResolver(gateway_channel=MagicMock())

    def teardown_method(self, method):
        from almanak.framework.data.tokens.resolver import TokenResolver

        TokenResolver.reset_instance()

    def test_hyperevm_is_registered_static_only(self):
        """hyperevm is the (currently only) static-only-symbol chain."""
        from almanak.framework.data.tokens.resolver import STATIC_ONLY_SYMBOL_CHAINS

        assert "hyperevm" in STATIC_ONLY_SYMBOL_CHAINS
        # Guard against accidentally sweeping in other chains.
        assert "arbitrum" not in STATIC_ONLY_SYMBOL_CHAINS
        assert "ethereum" not in STATIC_ONLY_SYMBOL_CHAINS
        assert "base" not in STATIC_ONLY_SYMBOL_CHAINS

    def test_unknown_symbol_on_hyperevm_fails_fast_no_gateway_call(self):
        """An unknown symbol ("ETH", a perp index) on hyperevm raises
        TokenNotFoundError WITHOUT invoking the gateway dynamic-symbol RPC.
        """
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        resolver = self._make_resolver_with_gateway()

        with patch.object(resolver, "_resolve_symbol_via_gateway") as mock_symbol_gw:
            with patch.object(resolver, "_resolve_via_gateway") as mock_addr_gw:
                with pytest.raises(TokenNotFoundError) as exc_info:
                    resolver.resolve("ETH", "hyperevm")

        # The doomed 15s dynamic-symbol lookup must NOT have been attempted,
        # and neither must the address on-chain lookup (input is a symbol).
        mock_symbol_gw.assert_not_called()
        mock_addr_gw.assert_not_called()
        # The error must describe it as a symbol miss, not an address miss.
        assert "Symbol 'ETH' not found" in exc_info.value.reason

    def test_unknown_symbol_on_arbitrum_still_hits_gateway(self):
        """The SAME unknown symbol on arbitrum still attempts dynamic gateway
        resolution — the gate is hyperevm-scoped and must not leak.
        """
        resolver = self._make_resolver_with_gateway()

        # Return None (gateway says not found) so resolve() still raises, but we
        # only care that the dynamic path WAS invoked.
        with patch.object(resolver, "_resolve_symbol_via_gateway", return_value=None) as mock_symbol_gw:
            from almanak.framework.data.tokens.exceptions import TokenNotFoundError

            with pytest.raises(TokenNotFoundError):
                resolver.resolve("EXOTIC_TOKEN_XYZ", "arbitrum")

        mock_symbol_gw.assert_called_once()

    def test_known_static_symbol_on_hyperevm_resolves_instantly(self):
        """USDC (statically registered on hyperevm) resolves from the static
        registry without touching the gateway.
        """
        resolver = self._make_resolver_with_gateway()

        with patch.object(resolver, "_resolve_symbol_via_gateway") as mock_symbol_gw:
            token = resolver.resolve("USDC", "hyperevm")

        assert token.symbol == "USDC"
        assert token.decimals == 6
        assert token.source == "static"
        mock_symbol_gw.assert_not_called()

    def test_address_resolution_on_hyperevm_still_uses_gateway(self):
        """Resolution BY ADDRESS on hyperevm is NOT gated — an address is a real
        ERC-20 contract, so on-chain discovery via the gateway is meaningful and
        must still fire for an unknown address.
        """
        resolver = self._make_resolver_with_gateway()

        unknown_address = "0x1234567890abcdef1234567890abcdef12345678"

        with patch.object(resolver, "_resolve_via_gateway", return_value=None) as mock_addr_gw:
            with patch.object(resolver, "_resolve_symbol_via_gateway") as mock_symbol_gw:
                from almanak.framework.data.tokens.exceptions import TokenNotFoundError

                with pytest.raises(TokenNotFoundError):
                    resolver.resolve(unknown_address, "hyperevm")

        # Address discovery path fired; symbol path did not.
        mock_addr_gw.assert_called_once()
        mock_symbol_gw.assert_not_called()
