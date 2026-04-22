"""Tests for position lifecycle events (Phase 2, VIB-2774/2775).

Validates:
- PositionEvent creation from LP/perp intents
- SWAP/SUPPLY intents produce no position events
- SQLite persistence and querying
- Position history (chronological lifecycle)
"""

import asyncio
from datetime import UTC, datetime

import pytest

from almanak.framework.observability.position_events import (
    INTENT_TO_EVENT_TYPE,
    PositionEvent,
    build_position_event_from_intent,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

# --- Mock intent/result helpers ---


class MockIntent:
    def __init__(self, intent_type: str, protocol: str = "uniswap_v3", position_id: str = ""):
        self.intent_type = type("IT", (), {"value": intent_type})()
        self.protocol = protocol
        self.position_id = position_id


class MockTxResult:
    def __init__(self, tx_hash: str = "0xabc"):
        self.tx_hash = tx_hash
        self.gas_used = 200000
        self.success = True


class MockResult:
    def __init__(self, position_id: str = "", tx_hash: str = "0xabc"):
        self.position_id = position_id
        self.transaction_results = [MockTxResult(tx_hash)]
        self.gas_cost_usd = "2.50"
        self.extracted_data = {}


class TestBuildPositionEvent:
    """Test building position events from intents."""

    def test_lp_open_produces_open_event(self):
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="12345")
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
            chain="arbitrum",
        )
        assert event is not None
        assert event.event_type == "OPEN"
        assert event.position_type == "LP"
        assert event.position_id == "12345"
        assert event.deployment_id == "strat:abc"
        assert event.chain == "arbitrum"

    def test_lp_close_produces_close_event(self):
        intent = MockIntent("LP_CLOSE", position_id="12345")
        result = MockResult()
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
        )
        assert event is not None
        assert event.event_type == "CLOSE"
        assert event.position_type == "LP"

    def test_perp_open_produces_open_event(self):
        intent = MockIntent("PERP_OPEN", protocol="gmx_v2")
        result = MockResult(position_id="perp-001")
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
        )
        assert event is not None
        assert event.event_type == "OPEN"
        assert event.position_type == "PERP"
        assert event.protocol == "gmx_v2"

    def test_swap_produces_no_event(self):
        intent = MockIntent("SWAP")
        result = MockResult()
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
        )
        assert event is None

    def test_supply_produces_no_event(self):
        intent = MockIntent("SUPPLY")
        result = MockResult()
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
        )
        assert event is None

    def test_borrow_produces_no_event(self):
        intent = MockIntent("BORROW")
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=MockResult(),
        )
        assert event is None

    def test_no_event_when_position_id_empty(self):
        """LP_OPEN with no position_id resolved returns None (guard)."""
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="")  # No position_id resolved
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
        )
        assert event is None

    def test_tx_hash_and_gas_captured(self):
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="12345", tx_hash="0xdeadbeef")
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
        )
        assert event is not None
        assert event.tx_hash == "0xdeadbeef"
        assert event.gas_usd == "2.50"


class TestIntentToEventMapping:
    """Verify the intent->event mapping covers LP and perps only."""

    def test_lp_intents_mapped(self):
        assert "LP_OPEN" in INTENT_TO_EVENT_TYPE
        assert "LP_CLOSE" in INTENT_TO_EVENT_TYPE
        assert "LP_COLLECT_FEES" in INTENT_TO_EVENT_TYPE

    def test_perp_intents_mapped(self):
        assert "PERP_OPEN" in INTENT_TO_EVENT_TYPE
        assert "PERP_CLOSE" in INTENT_TO_EVENT_TYPE

    def test_fungible_intents_not_mapped(self):
        for intent_type in ("SWAP", "SUPPLY", "WITHDRAW", "BORROW", "REPAY", "STAKE", "UNSTAKE", "HOLD"):
            assert intent_type not in INTENT_TO_EVENT_TYPE


# --- SQLite persistence tests ---


@pytest.fixture
def store(tmp_path):
    db_path = str(tmp_path / "test.db")
    config = SQLiteConfig(db_path=db_path)
    s = SQLiteStore(config)
    asyncio.get_event_loop().run_until_complete(s.initialize())
    yield s
    asyncio.get_event_loop().run_until_complete(s.close())


class TestPositionEventPersistence:
    """Test save and query of position events in SQLite."""

    def test_save_and_retrieve(self, store):
        event = PositionEvent(
            deployment_id="strat:abc",
            position_id="12345",
            position_type="LP",
            event_type="OPEN",
            protocol="uniswap_v3",
            chain="arbitrum",
            tick_lower=-1000,
            tick_upper=1000,
        )
        ok = asyncio.get_event_loop().run_until_complete(store.save_position_event(event))
        assert ok

        events = asyncio.get_event_loop().run_until_complete(
            store.get_position_events("strat:abc")
        )
        assert len(events) == 1
        assert events[0]["position_id"] == "12345"
        assert events[0]["event_type"] == "OPEN"
        assert events[0]["tick_lower"] == -1000

    def test_filter_by_position_id(self, store):
        for pid in ("100", "200"):
            event = PositionEvent(
                deployment_id="strat:abc",
                position_id=pid,
                position_type="LP",
                event_type="OPEN",
            )
            asyncio.get_event_loop().run_until_complete(store.save_position_event(event))

        events = asyncio.get_event_loop().run_until_complete(
            store.get_position_events("strat:abc", position_id="100")
        )
        assert len(events) == 1
        assert events[0]["position_id"] == "100"

    def test_filter_by_event_type(self, store):
        for etype in ("OPEN", "SNAPSHOT", "CLOSE"):
            event = PositionEvent(
                deployment_id="strat:abc",
                position_id="100",
                position_type="LP",
                event_type=etype,
            )
            asyncio.get_event_loop().run_until_complete(store.save_position_event(event))

        events = asyncio.get_event_loop().run_until_complete(
            store.get_position_events("strat:abc", event_type="SNAPSHOT")
        )
        assert len(events) == 1

    def test_position_history_chronological(self, store):
        for i, etype in enumerate(["OPEN", "SNAPSHOT", "CLOSE"]):
            event = PositionEvent(
                deployment_id="strat:abc",
                position_id="100",
                position_type="LP",
                event_type=etype,
                timestamp=datetime(2026, 1, 1 + i, tzinfo=UTC),
            )
            asyncio.get_event_loop().run_until_complete(store.save_position_event(event))

        history = asyncio.get_event_loop().run_until_complete(
            store.get_position_history("strat:abc", "100")
        )
        assert len(history) == 3
        assert history[0]["event_type"] == "OPEN"
        assert history[1]["event_type"] == "SNAPSHOT"
        assert history[2]["event_type"] == "CLOSE"


# --- Phase 5h-chars: characterization tests for build_position_event_from_intent ---
#
# These tests pin the CURRENT behavior of build_position_event_from_intent so
# that the Phase 5i extraction (helpers: _seed_event / _apply_lp_open /
# _apply_lp_close / _apply_swap_fallback / _apply_perp / _apply_protocol_fees)
# is a provable behavior-preserving refactor.  They exercise the phase map
# from blueprints/plan: α (dispatch) → β (seed) → γ (lp_open) → δ (lp_close)
# → ε (swap fallback) → ζ (perp) → η (protocol fees) → θ (final guard).
#
# Two tests document KNOWN LATENT BUGS (#1709, #1710) — they assert the
# buggy-but-current behavior.  When those bugs are fixed, the tests flip.


class _Attrs:
    """Duck-typed payload: exposes only the attributes set on it.

    Using a plain attr holder (not a frozen dataclass) lets us selectively
    omit attributes to exercise every `getattr(..., default)` branch in the
    production code without inventing subclasses for each case.
    """

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class MockLPIntent(MockIntent):
    """LP intent with optional token0/token1 and from_token/to_token fields."""

    def __init__(
        self,
        intent_type: str = "LP_OPEN",
        protocol: str = "uniswap_v3",
        position_id: str = "",
        *,
        token0: str | None = None,
        token1: str | None = None,
        from_token: str | None = None,
        to_token: str | None = None,
    ):
        super().__init__(intent_type, protocol, position_id)
        if token0 is not None:
            self.token0 = token0
        if token1 is not None:
            self.token1 = token1
        if from_token is not None:
            self.from_token = from_token
        if to_token is not None:
            self.to_token = to_token


class TestLPOpenEnrichment:
    """Phase γ — lp_open_data enrichment.

    Covers: position_id/liquidity/ticks, amount0/amount1 population (VIB-3205
    audit fix), and token0/token1 pair population with intent.token* preferred
    over intent.{from,to}_token.
    """

    def test_lp_open_populates_amounts_and_ticks_and_liquidity(self):
        intent = MockLPIntent(
            "LP_OPEN", token0="WETH", token1="USDC"
        )
        result = MockResult(position_id="seed-id")
        result.extracted_data = {
            "lp_open_data": _Attrs(
                position_id=987654,
                liquidity=123456789,
                tick_lower=-60,
                tick_upper=60,
                amount0=10,
                amount1=20_000,
            )
        }
        event = build_position_event_from_intent(
            deployment_id="strat:abc", intent=intent, result=result
        )
        assert event is not None
        # lp_open overrides seeded position_id
        assert event.position_id == "987654"
        assert event.liquidity == "123456789"
        assert event.tick_lower == -60
        assert event.tick_upper == 60
        assert event.amount0 == "10"
        assert event.amount1 == "20000"
        assert event.token0 == "WETH"
        assert event.token1 == "USDC"

    def test_lp_open_token_pair_prefers_intent_token0_over_from_token(self):
        """When both intent.token0 and intent.from_token are set, token0 wins."""
        intent = MockLPIntent(
            "LP_OPEN",
            token0="WETH",
            token1="USDC",
            from_token="DAI",
            to_token="USDT",
        )
        result = MockResult(position_id="any")
        result.extracted_data = {"lp_open_data": _Attrs(position_id=1)}
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.token0 == "WETH"
        assert event.token1 == "USDC"

    def test_lp_open_token_pair_falls_back_to_from_and_to_token(self):
        """When intent lacks token0/token1, falls back to from_token/to_token."""
        intent = MockLPIntent(
            "LP_OPEN", from_token="DAI", to_token="USDT"
        )
        result = MockResult(position_id="any")
        result.extracted_data = {"lp_open_data": _Attrs(position_id=1)}
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.token0 == "DAI"
        assert event.token1 == "USDT"

    def test_lp_open_missing_amount0_amount1_leaves_empty_strings(self):
        """When lp_open_data has no amount0/amount1, event amounts stay ''."""
        intent = MockLPIntent("LP_OPEN", token0="WETH", token1="USDC")
        result = MockResult(position_id="any")
        result.extracted_data = {
            "lp_open_data": _Attrs(position_id=42, liquidity=100, tick_lower=-10, tick_upper=10)
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.amount0 == ""
        assert event.amount1 == ""


class TestLPCloseEnrichment:
    """Phase δ — lp_close_data enrichment (fee-attribute coalescing)."""

    def test_lp_close_reads_amount0_amount1_received_attribute_names(self):
        """Prod reads `amount0_received`/`amount1_received` (not `_collected`)."""
        intent = MockIntent("LP_CLOSE", position_id="pos-1")
        result = MockResult()
        result.extracted_data = {
            "lp_close_data": _Attrs(
                amount0_received=500, amount1_received=600
            )
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.amount0 == "500"
        assert event.amount1 == "600"

    def test_lp_close_fees_token0_preferred_over_fee0(self):
        """Phase δ tries fees_token0 first, falls through to fee0."""
        intent = MockIntent("LP_CLOSE", position_id="pos-2")
        result = MockResult()
        result.extracted_data = {
            "lp_close_data": _Attrs(
                amount0_received=1, amount1_received=2,
                fees_token0=77, fee0=999,  # fees_token0 wins
                fees_token1=88, fee1=999,
            )
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.fees_token0 == "77"
        assert event.fees_token1 == "88"

    def test_lp_close_fee0_fallback_when_fees_token0_absent(self):
        """When fees_token0 is absent, fee0 is used."""
        intent = MockIntent("LP_CLOSE", position_id="pos-3")
        result = MockResult()
        result.extracted_data = {
            "lp_close_data": _Attrs(
                amount0_received=1, amount1_received=2,
                fee0=42, fee1=43,
            )
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.fees_token0 == "42"
        assert event.fees_token1 == "43"

    def test_lp_close_with_none_fees_leaves_empty_string(self):
        """When both fee attrs are None, the event keeps empty-string default."""
        intent = MockIntent("LP_CLOSE", position_id="pos-4")
        result = MockResult()
        result.extracted_data = {
            "lp_close_data": _Attrs(
                amount0_received=1, amount1_received=2,
                fees_token0=None, fee0=None,
                fees_token1=None, fee1=None,
            )
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.fees_token0 == ""
        assert event.fees_token1 == ""

    def test_lp_close_missing_amount_attrs_coalesce_to_empty_string(self):
        """`getattr(lp_close, 'amount0_received', '') or ''` → ''."""
        intent = MockIntent("LP_CLOSE", position_id="pos-5")
        result = MockResult()
        result.extracted_data = {"lp_close_data": _Attrs()}  # no attrs at all
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.amount0 == ""
        assert event.amount1 == ""


class TestSwapFallback:
    """Phase ε — swap_amounts ONLY fills empty slots (critical invariant)."""

    def test_swap_does_not_overwrite_lp_open_pair_tokens(self):
        """CRITICAL: single-asset LP provisioning with a co-occurring swap leg
        must not clobber the LP pair identities. (Lines 261-266 of prod.)"""
        intent = MockLPIntent("LP_OPEN", token0="WETH", token1="USDC")
        result = MockResult(position_id="any")
        result.extracted_data = {
            "lp_open_data": _Attrs(
                position_id=1, amount0=10, amount1=20_000
            ),
            "swap_amounts": _Attrs(
                token_in="DAI",
                token_out="USDT",
                amount_in_decimal=111,
                amount_out_decimal=222,
            ),
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        # LP pair identity preserved — NOT replaced by swap leg
        assert event.token0 == "WETH"
        assert event.token1 == "USDC"
        assert event.amount0 == "10"
        assert event.amount1 == "20000"

    def test_swap_fills_empty_tokens_when_lp_open_absent(self):
        """With no lp_open, swap legs populate token0/token1 and amounts."""
        intent = MockIntent("LP_OPEN")  # no token0/token1 on intent
        result = MockResult(position_id="pos-9")
        result.extracted_data = {
            "swap_amounts": _Attrs(
                token_in="USDC",
                token_out="WETH",
                amount_in_decimal=1_000,
                amount_out_decimal="0.4",
            )
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.token0 == "USDC"
        assert event.token1 == "WETH"
        assert event.amount0 == "1000"
        assert event.amount1 == "0.4"

    def test_swap_fills_only_one_side_when_other_is_set(self):
        """Mixed: token0 set by lp_open, token1 empty → swap fills token1 only."""
        intent = MockLPIntent("LP_OPEN", token0="WETH")  # token1 missing
        result = MockResult(position_id="any")
        result.extracted_data = {
            "lp_open_data": _Attrs(position_id=1),
            "swap_amounts": _Attrs(
                token_in="DAI", token_out="USDT",
                amount_in_decimal=1, amount_out_decimal=2,
            ),
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.token0 == "WETH"        # lp_open wins
        assert event.token1 == "USDT"        # swap fills empty slot


class TestPerpEnrichment:
    """Phase ζ — perp_data enrichment (+ documents #1709)."""

    def test_perp_populates_all_fields(self):
        intent = MockIntent("PERP_OPEN", protocol="gmx_v2", position_id="seed")
        result = MockResult(position_id="seed")
        result.extracted_data = {
            "perp_data": _Attrs(
                leverage=5,
                entry_price="3000.5",
                mark_price="3001.0",
                unrealized_pnl="12.34",
                is_long=True,
            )
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.leverage == "5"
        assert event.entry_price == "3000.5"
        assert event.mark_price == "3001.0"
        assert event.unrealized_pnl == "12.34"
        assert event.is_long is True

    def test_perp_is_long_false_preserved(self):
        intent = MockIntent("PERP_OPEN", protocol="gmx_v2", position_id="s")
        result = MockResult(position_id="s")
        result.extracted_data = {"perp_data": _Attrs(is_long=False)}
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.is_long is False

    def test_perp_position_id_override_LATENT_BUG_1709(self):
        """DOCUMENTS #1709: perp.position_id unconditionally OVERWRITES the
        earlier seed (from result.position_id / intent.position_id).

        When #1709 is fixed — either by asserting equality or documenting
        precedence — this test will flip. Until then the BUG behavior is
        pinned here so the Phase 5i refactor is provably behavior-preserving.
        """
        intent = MockIntent("PERP_OPEN", protocol="gmx_v2", position_id="from-intent")
        result = MockResult(position_id="from-result")  # seeded
        result.extracted_data = {
            "perp_data": _Attrs(position_id="from-perp-extractor")
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        # Bug: perp extractor clobbers result.position_id silently
        assert event.position_id == "from-perp-extractor"

    def test_perp_without_position_id_attr_keeps_seeded(self):
        """If perp_data has no position_id (or falsy), seed is preserved."""
        intent = MockIntent("PERP_OPEN", protocol="gmx_v2")
        result = MockResult(position_id="result-pid")
        result.extracted_data = {
            "perp_data": _Attrs(leverage=3)  # no position_id attr
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.position_id == "result-pid"

    def test_perp_with_falsy_position_id_keeps_seeded(self):
        """Empty-string position_id on perp is NOT used; seed wins."""
        intent = MockIntent("PERP_OPEN", protocol="gmx_v2")
        result = MockResult(position_id="result-pid")
        result.extracted_data = {"perp_data": _Attrs(position_id="")}
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.position_id == "result-pid"


class TestProtocolFees:
    """Phase η — protocol_fees VIB-3205 semantics (None vs 0 distinction)."""

    def test_protocol_fees_none_yields_empty_string(self):
        """No protocol_fees key → empty string ('unknown' in VIB-3205 semantics)."""
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="p1")
        result.extracted_data = {"lp_open_data": _Attrs(position_id=1)}
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.protocol_fees_usd == ""

    def test_protocol_fees_zero_yields_string_zero_measured_zero(self):
        """total_usd=0 → '0' string, distinct from '' (measured zero vs unknown)."""
        from decimal import Decimal
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="p2")
        result.extracted_data = {
            "lp_open_data": _Attrs(position_id=1),
            "protocol_fees": _Attrs(total_usd=Decimal("0")),
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.protocol_fees_usd == "0"

    def test_protocol_fees_positive_stringified(self):
        from decimal import Decimal
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="p3")
        result.extracted_data = {
            "lp_open_data": _Attrs(position_id=1),
            "protocol_fees": _Attrs(total_usd=Decimal("1.23")),
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.protocol_fees_usd == "1.23"

    def test_protocol_fees_missing_total_usd_attr_stays_empty(self):
        """protocol_fees object without a total_usd attribute → '' unchanged."""
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="p4")
        result.extracted_data = {
            "lp_open_data": _Attrs(position_id=1),
            "protocol_fees": _Attrs(other_field=1),  # no total_usd
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.protocol_fees_usd == ""

    def test_protocol_fees_total_usd_is_none_leaves_empty(self):
        """total_usd=None → stays ''; the inner None-check skips the assignment."""
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="p5")
        result.extracted_data = {
            "lp_open_data": _Attrs(position_id=1),
            "protocol_fees": _Attrs(total_usd=None),
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.protocol_fees_usd == ""


class TestExtractedDataGuards:
    """Phase θ — final guard + missing extracted_data short-circuit (line 210)."""

    def test_missing_extracted_data_with_position_id_returns_event(self):
        """No extracted_data but seeded position_id → event returned."""
        intent = MockIntent("LP_OPEN", position_id="intent-pid")
        result = MockResult(position_id="result-pid")
        result.extracted_data = {}  # empty → short-circuit path
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.position_id == "result-pid"
        # None of the enrichment phases ran
        assert event.amount0 == ""
        assert event.protocol_fees_usd == ""

    def test_missing_extracted_data_with_empty_position_id_returns_none(self):
        """No extracted_data AND no position_id → None (early short-circuit)."""
        intent = MockIntent("LP_OPEN")  # no intent.position_id
        result = MockResult(position_id="")  # no result.position_id
        result.extracted_data = {}
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is None

    def test_final_guard_returns_none_when_position_id_empty_after_enrichment(self):
        """If lp_open_data is present but position_id ends empty, final guard
        returns None (line 300)."""
        # No seed, no lp_open.position_id → final position_id stays ''
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="")
        # lp_open missing position_id attr entirely; branch that sets
        # event.position_id requires hasattr(lp_open, "position_id")
        result.extracted_data = {"lp_open_data": _Attrs(liquidity=5)}
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is None


class TestPhaseOrderingInvariant:
    """Critical invariant: γ (lp_open) before ε (swap fallback) before ζ (perp)."""

    def test_lp_open_seeds_first_then_swap_respects_filled_slots(self):
        """If reversed (swap before lp_open), token0/token1 would be swap's
        token_in/token_out, then lp_open's token0/token1 would overwrite —
        end state same but amount0/amount1 would be wrong (lp_open.amount0 is
        actual deposit amount, swap.amount_in is pre-deposit swap leg).

        This test pins the CORRECT phase ordering end-state:
            token0/token1 = LP pair (from intent)
            amount0/amount1 = LP deposit amounts (from lp_open_data)
        """
        intent = MockLPIntent("LP_OPEN", token0="WETH", token1="USDC")
        result = MockResult(position_id="any")
        result.extracted_data = {
            # Note: dict insertion order is intentionally swap-first here to
            # prove that order-in-dict does NOT matter; the code reads keys.
            "swap_amounts": _Attrs(
                token_in="USDC",
                token_out="WETH",
                amount_in_decimal="1000",
                amount_out_decimal="0.4",
            ),
            "lp_open_data": _Attrs(
                position_id=1, amount0=5, amount1=10_000
            ),
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        # LP pair identities preserved from intent, amounts from lp_open_data
        assert event.token0 == "WETH"
        assert event.token1 == "USDC"
        assert event.amount0 == "5"
        assert event.amount1 == "10000"


class TestLPOpenLPCloseCoexistence:
    """#1710 defensive: lp_close CAN clobber lp_open amount0/amount1 today."""

    def test_lp_close_clobbers_lp_open_amounts_LATENT_BUG_1710(self):
        """DOCUMENTS #1710: If extracted_data contains BOTH lp_open_data and
        lp_close_data, the close phase (δ) unconditionally overwrites the
        amount0/amount1 that the open phase (γ) populated.

        Lifecycle-wise this shouldn't happen (an intent is either an OPEN or
        a CLOSE, never both), but it's not asserted. This test pins the
        current buggy-but-stable behavior so the 5i refactor is provably
        behavior-preserving. When #1710 lands, this test flips.
        """
        intent = MockLPIntent("LP_OPEN", token0="WETH", token1="USDC")
        result = MockResult(position_id="any")
        result.extracted_data = {
            "lp_open_data": _Attrs(
                position_id=1, amount0=100, amount1=200
            ),
            "lp_close_data": _Attrs(
                amount0_received=999, amount1_received=888
            ),
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        # Bug: close clobbers open amounts
        assert event.amount0 == "999"
        assert event.amount1 == "888"
        # Token identity still from lp_open / intent
        assert event.token0 == "WETH"
        assert event.token1 == "USDC"


class TestIntentDispatch:
    """Phase α — INTENT_TO_EVENT_TYPE dispatch (all non-LP/non-PERP → None)."""

    @pytest.mark.parametrize(
        "intent_type",
        [
            "SWAP",
            "SUPPLY",
            "WITHDRAW",
            "BORROW",
            "REPAY",
            "STAKE",
            "UNSTAKE",
            "HOLD",
            "BRIDGE",
            "UNWRAP",
            "",          # empty-string intent_type
            "UNKNOWN",   # unregistered
        ],
    )
    def test_non_lp_non_perp_intents_return_none(self, intent_type):
        intent = MockIntent(intent_type)
        result = MockResult(position_id="x")
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is None

    def test_intent_without_intent_type_attr_returns_none(self):
        """Plain object w/o intent_type attr → dispatch sees '' → None."""
        class NoType:
            pass
        event = build_position_event_from_intent(
            deployment_id="d", intent=NoType(), result=MockResult(position_id="x")
        )
        assert event is None

    def test_intent_type_as_raw_string_works(self):
        """intent_type can be a plain string (no .value attribute)."""
        class StringTypeIntent:
            intent_type = "LP_OPEN"  # not an enum, just a str
            protocol = "uniswap_v3"
            position_id = "str-intent-pid"
        event = build_position_event_from_intent(
            deployment_id="d",
            intent=StringTypeIntent(),
            result=MockResult(position_id="res-pid"),
        )
        assert event is not None
        assert event.event_type == "OPEN"
        assert event.position_type == "LP"


class TestSeedingAndBaseFields:
    """Phase β — seed: position_id / tx_hash / gas_usd / protocol / chain."""

    def test_result_position_id_preferred_over_intent_position_id(self):
        intent = MockIntent("LP_OPEN", position_id="intent-pid")
        result = MockResult(position_id="result-pid")
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.position_id == "result-pid"

    def test_falls_back_to_intent_position_id_when_result_empty(self):
        intent = MockIntent("LP_OPEN", position_id="intent-pid")
        result = MockResult(position_id="")  # falsy → fallback
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.position_id == "intent-pid"

    def test_result_none_returns_none_when_no_intent_position_id(self):
        """result=None path: no tx_hash/gas, no extracted, no position_id → None."""
        intent = MockIntent("LP_OPEN")  # no position_id
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=None
        )
        assert event is None

    def test_result_none_with_intent_position_id_returns_event(self):
        """With result=None but intent.position_id set, event is emitted."""
        intent = MockIntent("LP_OPEN", position_id="only-from-intent")
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=None
        )
        assert event is not None
        assert event.position_id == "only-from-intent"
        assert event.tx_hash == ""
        assert event.gas_usd == ""

    def test_deployment_id_and_ledger_entry_id_wired_through(self):
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="p")
        event = build_position_event_from_intent(
            deployment_id="strat:deploy-xyz",
            intent=intent,
            result=result,
            ledger_entry_id="ledger-42",
            chain="base",
        )
        assert event is not None
        assert event.deployment_id == "strat:deploy-xyz"
        assert event.ledger_entry_id == "ledger-42"
        assert event.chain == "base"

    def test_protocol_default_empty_when_intent_has_none(self):
        """`getattr(intent, 'protocol', '') or ''` coalesces None → ''."""
        class NoProto:
            intent_type = type("IT", (), {"value": "LP_OPEN"})()
            protocol = None
            position_id = "pid"
        event = build_position_event_from_intent(
            deployment_id="d", intent=NoProto(), result=MockResult(position_id="pid")
        )
        assert event is not None
        assert event.protocol == ""

    def test_gas_cost_usd_none_yields_empty_string(self):
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="p")
        result.gas_cost_usd = None
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.gas_usd == ""

    def test_empty_transaction_results_leaves_tx_hash_empty(self):
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="p")
        result.transaction_results = []  # no tx results
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.tx_hash == ""

    def test_transaction_results_tx_hash_none_coalesces_to_empty(self):
        """tx_hash=None on first tx result → '' via `or ''`."""
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="p")
        result.transaction_results[0].tx_hash = None
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.tx_hash == ""
