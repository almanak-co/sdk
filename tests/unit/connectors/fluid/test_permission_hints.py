"""Unit tests for Fluid's synthetic permission-discovery vectors.

Pins every branch of ``build_discovery_vectors``: per-chain SWAP vectors
(routerless — per-pool targets), the lending SUPPLY/WITHDRAW vectors on the
lending chains, the empty-list (NOT ``None``) return on non-lending chains
(so the framework default never emits a doomed lending synthetic), and the
``None`` passthroughs that defer to the framework default.
"""

from decimal import Decimal

from almanak.connectors.fluid.permission_hints import (
    _LENDING_CHAINS,
    _SWAP_VECTORS_BY_CHAIN,
    PERMISSION_HINTS,
    build_discovery_vectors,
)
from almanak.framework.intents.vocabulary import SupplyIntent, SwapIntent, WithdrawIntent
from almanak.framework.permissions.hints import DiscoveryContext

CTX = DiscoveryContext(usdc="USDC", weth="WETH")


class TestSwapVectors:
    def test_every_declared_chain_emits_its_pairs(self):
        for chain, vectors in _SWAP_VECTORS_BY_CHAIN.items():
            intents = build_discovery_vectors("fluid", "SWAP", chain, CTX)
            assert intents is not None
            assert len(intents) == len(vectors)
            for intent, (from_token, to_token, amount) in zip(intents, vectors, strict=True):
                assert isinstance(intent, SwapIntent)
                assert intent.from_token == from_token
                assert intent.to_token == to_token
                assert intent.amount == amount
                assert intent.protocol == "fluid"
                assert intent.chain == chain

    def test_unknown_chain_defers_to_framework_default(self):
        assert build_discovery_vectors("fluid", "SWAP", "avalanche", CTX) is None


class TestLendingVectors:
    def test_supply_vector_on_lending_chains(self):
        for chain in sorted(_LENDING_CHAINS):
            intents = build_discovery_vectors("fluid", "SUPPLY", chain, CTX)
            assert intents is not None and len(intents) == 1
            (supply,) = intents
            assert isinstance(supply, SupplyIntent)
            assert supply.token == "USDC"
            assert supply.amount == Decimal("100")
            assert supply.chain == chain

    def test_withdraw_vectors_cover_both_selectors(self):
        for chain in sorted(_LENDING_CHAINS):
            intents = build_discovery_vectors("fluid", "WITHDRAW", chain, CTX)
            assert intents is not None and len(intents) == 2
            exact, full_exit = intents
            assert isinstance(exact, WithdrawIntent) and not exact.withdraw_all
            assert isinstance(full_exit, WithdrawIntent) and full_exit.withdraw_all

    def test_non_lending_chain_returns_empty_not_none(self):
        # [] (own the dispatch: emit nothing) vs None (framework default,
        # which gates on lending-pool tables fluid is not in and could emit
        # a doomed synthetic).
        assert build_discovery_vectors("fluid", "SUPPLY", "ethereum", CTX) == []
        assert build_discovery_vectors("fluid", "WITHDRAW", "polygon", CTX) == []


class TestPassthrough:
    def test_unknown_intent_type_defers(self):
        assert build_discovery_vectors("fluid", "LP_OPEN", "arbitrum", CTX) is None
        assert build_discovery_vectors("fluid", "BORROW", "arbitrum", CTX) is None


class TestHintsShape:
    def test_declared_intents_and_selector_labels(self):
        assert PERMISSION_HINTS.synthetic_discovery_intents == frozenset({"SWAP", "SUPPLY", "WITHDRAW"})
        assert PERMISSION_HINTS.needs_rpc_discovery is True
        # The lending selectors the manifest labels must cover deposit,
        # withdraw, and the full-exit redeem path.
        for selector in ("0x6e553f65", "0xb460af94", "0xba087652"):
            assert selector in PERMISSION_HINTS.selector_labels
