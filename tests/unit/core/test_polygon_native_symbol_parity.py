"""Contract test: the Polygon MATIC <-> POL native-symbol bridge.

Polygon renamed its native gas token MATIC -> POL (Sept 2024, 1:1). The codebase
deliberately keeps TWO canonical views and bridges them with aliases, because
each is correct for its own domain:

* **Gas / price / funding canonical = ``MATIC``.** ``ChainRegistry`` and
  ``gas_pricing._CHAIN_NATIVE_TOKEN`` pin Polygon native to ``MATIC`` (matches
  the Chainlink ``MATIC/USD`` feed key, the gateway ``CHAIN_NATIVE_SYMBOL``
  derived from the registry, and the ``anvil_funding`` key every shipped
  Polygon demo config uses). The gas/price coverage test pins this side.
* **Token-identity canonical = ``POL``.** The token resolver canonicalizes the
  native sentinel address to ``POL`` (the current official ticker) for the
  address -> symbol reverse lookup, while keeping ``MATIC`` forward-resolvable.

These two canonicals MUST stay bridged: any path that recognizes one symbol
must recognize the other, or native price / gas / funding silently breaks on
Polygon. This test pins that bridge so a future "make it consistent" change
cannot flip one side and break the money path. If you genuinely want to migrate
everything to POL, that is a deliberate cross-cutting change (registry + gas
invariant + every price-provider feed key + every Polygon config's funding key)
— not a drive-by edit, and this test is where you would start.

See: VIB-3136 (compiler native<->wrapped expansion), the Polygon investigation
on branch ``polygon-may27``.
"""

from __future__ import annotations

NATIVE_SENTINEL = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"


class TestPolygonNativeSymbolParity:
    """MATIC and POL must remain interchangeable on every Polygon native path."""

    def test_gas_price_canonical_is_matic(self) -> None:
        """The gas/price/funding canonical stays MATIC across registry + gas_pricing.

        These two are pinned together (the gateway's per-chain native symbol is
        derived from ChainRegistry, and gas_pricing's coverage test requires it
        to match). Changing one without the other breaks gas_usd on Polygon.
        """
        from almanak.core.chains import ChainRegistry
        from almanak.framework.accounting.gas_pricing import native_token_for_chain

        descriptor = ChainRegistry.try_resolve("polygon")
        assert descriptor is not None
        assert descriptor.native.symbol == "MATIC"
        assert native_token_for_chain("polygon") == "MATIC"

    def test_both_symbols_count_as_polygon_native_for_balance(self) -> None:
        """GetBalance must treat MATIC and POL as the Polygon native coin."""
        from almanak.gateway.services.market_service import NATIVE_SYMBOLS_BY_CHAIN

        polygon_natives = NATIVE_SYMBOLS_BY_CHAIN["polygon"]
        assert "MATIC" in polygon_natives
        assert "POL" in polygon_natives

    def test_both_symbols_route_to_wrapped_for_pricing(self) -> None:
        """Native-price fallback must route both MATIC and POL through WMATIC."""
        from almanak.gateway.services.market_service import NATIVE_PRICE_ALIASES

        assert NATIVE_PRICE_ALIASES["MATIC"] == "WMATIC"
        assert NATIVE_PRICE_ALIASES["POL"] == "WMATIC"

    def test_both_symbols_are_fundable_native_tokens(self) -> None:
        """Anvil funding must accept both MATIC and POL as native-gas symbols.

        Every shipped Polygon demo config funds native gas under the ``MATIC``
        key; POL must also be honored so a POL-keyed config does not silently
        fall back to the default native amount.
        """
        from almanak.gateway.managed import ManagedGateway

        assert "MATIC" in ManagedGateway.NATIVE_TOKEN_SYMBOLS
        assert "POL" in ManagedGateway.NATIVE_TOKEN_SYMBOLS

    def test_both_symbols_forward_resolve_to_native_sentinel(self) -> None:
        """The token resolver must forward-resolve BOTH symbols on Polygon.

        The resolver canonicalizes the sentinel address to POL for the reverse
        (address -> symbol) lookup, but MATIC must remain forward-resolvable so
        gas_pricing's MATIC lookups never miss.
        """
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        for symbol in ("MATIC", "POL"):
            token = resolver.resolve(symbol, chain="polygon")
            address = token.get("address") if isinstance(token, dict) else getattr(token, "address", token)
            assert str(address).lower() == NATIVE_SENTINEL, (
                f"resolve({symbol!r}, 'polygon') -> {address!r}; expected the native sentinel. "
                "The MATIC<->POL bridge is broken."
            )
