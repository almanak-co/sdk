"""Unit tests for the registry-derived CHAIN_PROTOCOL_MAP in yield_poker.py.

YieldPoker previously maintained a hardcoded CHAIN_PROTOCOL_MAP; plan 021
moves poke functions into connector packages and derives the map from the
connector registry. These tests pin:

  (a) the derived CHAIN_PROTOCOL_MAP equals the historical literal content;
  (b) a connector without yield_poke contributes nothing;
  (c) YieldPoker.register() still allows manual additions.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from almanak.connectors._strategy_base.yield_poke_base import PokeResult


class TestDerivedChainProtocolMap:
    """The CHAIN_PROTOCOL_MAP derived from the connector registry matches history."""

    def test_chain_protocol_map_equals_historical_literal(self) -> None:
        """Derived map must contain exactly the three entries from the old literal."""
        from almanak.framework.backtesting.paper.yield_poker import CHAIN_PROTOCOL_MAP

        actual = sorted((chain, protocol) for chain, lst in CHAIN_PROTOCOL_MAP.items() for protocol, _ in lst)
        expected = [
            ("arbitrum", "aave_v3"),
            ("arbitrum", "compound_v3"),
            ("ethereum", "morpho_blue"),
        ]
        assert actual == expected

    def test_chain_protocol_map_all_entries_are_callable(self) -> None:
        """Every registered poke function must be callable."""
        from almanak.framework.backtesting.paper.yield_poker import CHAIN_PROTOCOL_MAP

        for chain, entries in CHAIN_PROTOCOL_MAP.items():
            for protocol, poke_fn in entries:
                assert callable(poke_fn), f"{protocol} on {chain} poke_fn is not callable"

    def test_connector_without_yield_poke_contributes_nothing(self) -> None:
        """A connector that declares no yield_poke must not appear in the map."""
        from almanak.connectors._connector import CONNECTOR_REGISTRY
        from almanak.framework.backtesting.paper.yield_poker import CHAIN_PROTOCOL_MAP

        registered_protocols = {protocol for lst in CHAIN_PROTOCOL_MAP.values() for protocol, _ in lst}

        # Every connector with no yield_poke decl must not be in the registered set.
        for connector in CONNECTOR_REGISTRY.all():
            if connector.yield_poke is None:
                assert connector.name not in registered_protocols, (
                    f"Connector {connector.name!r} has no yield_poke but appears in CHAIN_PROTOCOL_MAP"
                )

    def test_every_registered_protocol_has_a_yield_poke_decl(self) -> None:
        """Every protocol that appears in the map must have a connector yield_poke decl."""
        from almanak.connectors._connector import CONNECTOR_REGISTRY
        from almanak.framework.backtesting.paper.yield_poker import CHAIN_PROTOCOL_MAP

        connectors_with_poke = {c.name for c in CONNECTOR_REGISTRY.all() if c.yield_poke is not None}
        registered_protocols = {protocol for lst in CHAIN_PROTOCOL_MAP.values() for protocol, _ in lst}

        for protocol in registered_protocols:
            assert protocol in connectors_with_poke, (
                f"Protocol {protocol!r} appears in CHAIN_PROTOCOL_MAP but has no yield_poke decl"
            )


class TestYieldPokerManualRegister:
    """YieldPoker.register() still allows manual additions beyond the manifest set."""

    def test_register_adds_extra_protocol(self) -> None:
        """Manually registered protocols appear in poke_all() results."""
        from almanak.framework.backtesting.paper.yield_poker import YieldPoker

        poker = YieldPoker()
        extra_fn: AsyncMock = AsyncMock(return_value=PokeResult(protocol="test_proto", success=True))
        poker.register("arbitrum", "test_proto", extra_fn)

        assert "test_proto" in poker._poke_hooks.get("arbitrum", {})

    def test_register_on_new_chain(self) -> None:
        """Registering a poke for a previously unknown chain is allowed."""
        from almanak.framework.backtesting.paper.yield_poker import YieldPoker

        poker = YieldPoker()
        fn: AsyncMock = AsyncMock(return_value=PokeResult(protocol="some_proto", success=True))
        poker.register("avalanche", "some_proto", fn)

        assert "avalanche" in poker._poke_hooks
        assert "some_proto" in poker._poke_hooks["avalanche"]

    def test_poke_all_includes_manually_registered(self) -> None:
        """poke_all() executes manually registered poke functions."""
        from almanak.framework.backtesting.paper.yield_poker import YieldPoker

        poker = YieldPoker()
        result = PokeResult(protocol="test_proto", success=True)
        fn: AsyncMock = AsyncMock(return_value=result)
        poker.register("arbitrum", "test_proto", fn)

        results = asyncio.run(poker.poke_all("arbitrum", "http://localhost:8545", "0xwallet"))

        assert any(r.protocol == "test_proto" for r in results)

    def test_poke_all_unknown_chain_returns_empty(self) -> None:
        """poke_all() on a chain with no registered hooks returns an empty list."""
        from almanak.framework.backtesting.paper.yield_poker import YieldPoker

        poker = YieldPoker()
        results = asyncio.run(poker.poke_all("solana", "http://localhost:8545", "0xwallet"))

        assert results == []


class TestYieldPokerDefaultInit:
    """YieldPoker() zero-arg construction populates exactly the expected chains/protocols."""

    def test_default_init_has_arbitrum_aave_v3(self) -> None:
        from almanak.framework.backtesting.paper.yield_poker import YieldPoker

        poker = YieldPoker()
        assert "aave_v3" in poker._poke_hooks.get("arbitrum", {})

    def test_default_init_has_arbitrum_compound_v3(self) -> None:
        from almanak.framework.backtesting.paper.yield_poker import YieldPoker

        poker = YieldPoker()
        assert "compound_v3" in poker._poke_hooks.get("arbitrum", {})

    def test_default_init_has_ethereum_morpho_blue(self) -> None:
        from almanak.framework.backtesting.paper.yield_poker import YieldPoker

        poker = YieldPoker()
        assert "morpho_blue" in poker._poke_hooks.get("ethereum", {})

    def test_default_init_chain_count(self) -> None:
        """Default init registers exactly 2 chains (arbitrum + ethereum)."""
        from almanak.framework.backtesting.paper.yield_poker import YieldPoker

        poker = YieldPoker()
        assert set(poker._poke_hooks.keys()) == {"arbitrum", "ethereum"}
