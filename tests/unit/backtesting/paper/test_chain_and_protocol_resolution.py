"""Unit tests for VIB-4861 registry-backed chain/protocol resolution.

Covers the three behaviours the W9 cleanup introduced or changed in
``almanak/framework/backtesting/paper/engine.py``:

1. ``_chain_name_for_id`` — EIP-155 id -> canonical chain name via
   ``ChainRegistry.by_id``, with unknown ids returning ``None`` (the former
   ``dict.get`` contract) rather than raising.
2. ``_PRICE_SOURCE_CHAINS`` — the Chainlink/DEX-TWAP price-source allowlist,
   now a single registry-validated frozenset. The membership semantics must
   match the former per-provider identity-map dicts byte-for-byte, including
   the chains that must stay disabled (bsc/linea/sonic have Chainlink feeds but
   were never in the engine allowlist).
3. ``_get_intent_protocol`` — the ``PaperTrade.protocol`` telemetry tag, read
   from the intent's own protocol-bearing attribute (lowercased), falling back
   to ``"default"`` after the dead class-name heuristic was removed.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from almanak.framework.backtesting.paper.engine import (
    _PRICE_SOURCE_CHAINS,
    PaperTrader,
    _chain_name_for_id,
)

# =============================================================================
# _chain_name_for_id
# =============================================================================


class TestChainNameForId:
    """``_chain_name_for_id`` resolves ids through ChainRegistry, None on unknown."""

    @pytest.mark.parametrize(
        ("chain_id", "expected"),
        [
            (1, "ethereum"),
            (42161, "arbitrum"),
            (10, "optimism"),
            (8453, "base"),
            (43114, "avalanche"),
            (137, "polygon"),
            (56, "bsc"),
            (146, "sonic"),
            (9745, "plasma"),
            (81457, "blast"),
            (5000, "mantle"),
            (80094, "berachain"),
        ],
    )
    def test_known_ids_resolve_to_canonical_name(self, chain_id: int, expected: str) -> None:
        # These are exactly the 12 ids the former ``_CHAIN_ID_TO_NAME`` dict
        # carried; the registry-backed helper must reproduce each one.
        assert _chain_name_for_id(chain_id) == expected

    @pytest.mark.parametrize("unknown_id", [0, -1, 999999, 424242])
    def test_unknown_id_returns_none(self, unknown_id: int) -> None:
        # Critical: the two callers (get_token_decimals /
        # get_token_decimals_with_fallback) rely on None to fall through to the
        # local TOKEN_DECIMALS registry. The helper must NOT raise on an
        # unregistered id (``ChainRegistry.by_id`` would otherwise raise).
        assert _chain_name_for_id(unknown_id) is None


# =============================================================================
# _PRICE_SOURCE_CHAINS
# =============================================================================


class TestPriceSourceChains:
    """The price-source allowlist preserves the engine's historical 6-chain subset."""

    def test_exact_membership(self) -> None:
        assert _PRICE_SOURCE_CHAINS == frozenset({"ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche"})

    @pytest.mark.parametrize("chain", ["ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche"])
    def test_supported_chains_enabled(self, chain: str) -> None:
        assert chain in _PRICE_SOURCE_CHAINS

    @pytest.mark.parametrize(
        "chain", ["bsc", "linea", "sonic", "plasma", "blast", "mantle", "berachain", "not_a_chain"]
    )
    def test_unsupported_chains_disabled(self, chain: str) -> None:
        # bsc/linea/sonic are in ChainlinkDataProvider._SUPPORTED_CHAINS but were
        # NOT in the engine's historical allowlist — gating on the provider set
        # would have enabled them and changed paper-mode price-source selection
        # (and PnL). They must stay out.
        assert chain not in _PRICE_SOURCE_CHAINS


# =============================================================================
# _get_intent_protocol
# =============================================================================


class _TraderStub(PaperTrader):
    """Bare ``PaperTrader`` instance for exercising ``_get_intent_protocol``.

    ``__init__`` is bypassed because the method under test only reads attributes
    off the passed-in intent and touches no instance state.
    """

    def __init__(self) -> None:  # noqa: D107 - intentional no-op
        pass


@pytest.fixture
def trader() -> _TraderStub:
    return _TraderStub()


class TestGetIntentProtocol:
    """Protocol tag is read from the intent's attribute, lowercased, else default."""

    def test_reads_protocol_attribute(self, trader: _TraderStub) -> None:
        intent = SimpleNamespace(protocol="enso")
        assert trader._get_intent_protocol(intent) == "enso"

    def test_lowercases_value(self, trader: _TraderStub) -> None:
        intent = SimpleNamespace(protocol="Uniswap_V3")
        assert trader._get_intent_protocol(intent) == "uniswap_v3"

    def test_attribute_priority_order(self, trader: _TraderStub) -> None:
        # ``protocol`` wins over the later fallback attributes.
        intent = SimpleNamespace(protocol="curve", protocol_name="aave_v3", connector="gmx_v2")
        assert trader._get_intent_protocol(intent) == "curve"

    def test_falls_back_to_protocol_name(self, trader: _TraderStub) -> None:
        intent = SimpleNamespace(protocol=None, protocol_name="morpho_blue")
        assert trader._get_intent_protocol(intent) == "morpho_blue"

    def test_falls_back_to_connector_then_adapter(self, trader: _TraderStub) -> None:
        assert trader._get_intent_protocol(SimpleNamespace(connector="aerodrome")) == "aerodrome"
        assert trader._get_intent_protocol(SimpleNamespace(adapter="pendle")) == "pendle"

    def test_no_protocol_attributes_returns_default(self, trader: _TraderStub) -> None:
        # A generic intent (no protocol-bearing attribute) — e.g. the
        # ``SwapIntent``/``LPIntent`` class names that the removed class-name
        # heuristic never actually matched — falls through to "default".
        assert trader._get_intent_protocol(SimpleNamespace(amount=1)) == "default"

    @pytest.mark.parametrize("falsy", [None, "", 0])
    def test_empty_or_nonstring_values_fall_through(self, trader: _TraderStub, falsy: object) -> None:
        # Empty / falsy / non-string protocol values are ignored and resolution
        # falls through to "default" (no attribute carries a usable string).
        intent = SimpleNamespace(protocol=falsy, protocol_name=falsy, connector=falsy, adapter=falsy)
        assert trader._get_intent_protocol(intent) == "default"

    def test_class_name_no_longer_infers_protocol(self, trader: _TraderStub) -> None:
        # Regression guard for the dead-heuristic removal: a class whose NAME
        # contains a protocol token but which carries no protocol attribute must
        # resolve to "default" (the old heuristic would have returned
        # "uniswap_v3"). Confirms the byte-identical claim for real repo intents,
        # which are all generically named and never hit the old branch.
        class UniswapV3SwapIntent:
            pass

        assert trader._get_intent_protocol(UniswapV3SwapIntent()) == "default"
