"""Tests for position lifecycle events (Phase 2, VIB-2774/2775).

Validates:
- PositionEvent creation from LP/perp intents
- SWAP/SUPPLY intents produce no position events
- SQLite persistence and querying
- Position history (chronological lifecycle)
"""

import asyncio
from dataclasses import asdict, fields
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

    def test_supply_produces_lending_collateral_event(self):
        """VIB-4085 — SUPPLY now emits a LENDING_COLLATERAL position event.
        Pre-fix this returned None (lending was explicitly excluded)."""
        intent = MockIntent("SUPPLY")
        # MockResult doesn't carry lending extracted_data; the event is
        # still emitted with position_id derived from chain+protocol+
        # wallet+asset. Without ``extracted`` the helper short-circuits
        # only for non-lending intents.
        result = MockResult()
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
            chain="arbitrum",
            wallet_address="0xtestwallet",
            post_state={"collateral_value_usd": "1.0", "debt_value_usd": "0"},
        )
        assert event is not None
        assert event.position_type == "LENDING_COLLATERAL"
        assert event.event_type == "OPEN"

    def test_borrow_produces_lending_debt_event(self):
        """VIB-4085 — BORROW emits a LENDING_DEBT position event."""
        intent = MockIntent("BORROW")
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=MockResult(),
            chain="arbitrum",
            wallet_address="0xtestwallet",
            post_state={"collateral_value_usd": "1.0", "debt_value_usd": "0.5"},
        )
        assert event is not None
        assert event.position_type == "LENDING_DEBT"
        assert event.event_type == "OPEN"

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

    def test_gas_usd_computed_from_total_gas_cost_wei_when_legacy_field_absent(self):
        """Regression: position_events.gas_usd was empty whenever the
        orchestrator populated total_gas_cost_wei (the modern path) and
        the legacy ``gas_cost_usd`` attribute was None. Same VIB-3658
        class as transaction_ledger; the writer here must mirror the
        ledger's ``compute_gas_usd`` precedence.
        """
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="12345", tx_hash="0xfeed")
        # Strip the legacy field, set the modern one (200,000 gas × 0.5 gwei).
        result.gas_cost_usd = None
        result.total_gas_cost_wei = 200_000 * 500_000_000  # 0.5 gwei in wei
        oracle = {"ETH": "3000.00"}
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
            chain="arbitrum",
            price_oracle=oracle,
        )
        assert event is not None
        # 200000 × 5e8 wei / 1e18 × $3000 = 0.0001 ETH × $3000 = $0.30
        assert event.gas_usd == "0.300000"

    def test_gas_usd_legacy_field_takes_precedence(self):
        """When upstream already computed gas_cost_usd (e.g. prediction-handler
        path), don't recompute — preserve backward compatibility."""
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="12345", tx_hash="0xfeed")
        result.total_gas_cost_wei = 200_000 * 500_000_000
        # gas_cost_usd is "2.50" (default on MockResult) — that wins.
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
            chain="arbitrum",
            price_oracle={"ETH": "3000.00"},
        )
        assert event is not None
        assert event.gas_usd == "2.50"

    def test_gas_usd_empty_when_no_oracle_and_no_legacy_field(self):
        """Honest absence: no oracle, no legacy gas_cost_usd, no synthesis.
        Ledger writer behaves identically; lane-symmetry preserved."""
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="12345", tx_hash="0xfeed")
        result.gas_cost_usd = None
        result.total_gas_cost_wei = 200_000 * 500_000_000
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
            chain="arbitrum",
            price_oracle=None,
        )
        assert event is not None
        assert event.gas_usd == ""


class TestIntentToEventMapping:
    """Verify the intent->event mapping covers LP and perps only."""

    def test_lp_intents_mapped(self):
        assert "LP_OPEN" in INTENT_TO_EVENT_TYPE
        assert "LP_CLOSE" in INTENT_TO_EVENT_TYPE
        assert "LP_COLLECT_FEES" in INTENT_TO_EVENT_TYPE

    def test_perp_intents_mapped(self):
        assert "PERP_OPEN" in INTENT_TO_EVENT_TYPE
        assert "PERP_CLOSE" in INTENT_TO_EVENT_TYPE

    def test_lending_intents_mapped(self):
        """VIB-4085 — SUPPLY/BORROW/REPAY/WITHDRAW/DELEVERAGE are now
        mapped (previously the intent map was LP+PERP only)."""
        for intent_type in ("SUPPLY", "BORROW", "REPAY", "WITHDRAW", "DELEVERAGE"):
            assert intent_type in INTENT_TO_EVENT_TYPE

    def test_non_position_intents_not_mapped(self):
        """SWAP / STAKE / UNSTAKE / HOLD remain excluded — generic swaps
        and staking are not lifecycle-tracked here. Spot/SWAP lifecycle
        is parked under VIB-4088 pending design alignment."""
        for intent_type in ("SWAP", "STAKE", "UNSTAKE", "HOLD"):
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
# Issues #1709 (perp position_id silent override) and #1710 (lp_close
# clobbering lp_open amounts) have been FIXED.  The pinned tests have
# been flipped and now assert the corrected behaviour:
#   - #1709: perp still wins on mismatch but a WARNING is emitted.
#   - #1710: lp_close no longer overwrites lp_open amounts; WARNING emitted.


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
        """Missing attrs → `getattr(lp_close, 'amount0_received', None) is None`
        so the empty-string event defaults survive.
        """
        intent = MockIntent("LP_CLOSE", position_id="pos-5")
        result = MockResult()
        result.extracted_data = {"lp_close_data": _Attrs()}  # no attrs at all
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.amount0 == ""
        assert event.amount1 == ""

    def test_lp_close_zero_received_amounts_are_preserved(self):
        """CR #1751 regression: a measured zero close amount must reach the
        event (and therefore persistence) as "0" rather than being coerced to
        "" by truthiness. Pre-fix the code used `str(... or "")`, which
        silently dropped explicit zeros. `amount0`/`amount1` are written
        straight through to `almanak/framework/state/backends/sqlite.py`, so
        losing a zero here is a real data-integrity bug.
        """
        intent = MockIntent("LP_CLOSE", position_id="pos-zero")
        result = MockResult()
        result.extracted_data = {
            "lp_close_data": _Attrs(amount0_received=0, amount1_received=0)
        }
        event = build_position_event_from_intent(
            deployment_id="d", intent=intent, result=result
        )
        assert event is not None
        assert event.amount0 == "0"
        assert event.amount1 == "0"


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

    def test_perp_position_id_mismatch_warns_issue_1709(self, caplog):
        """Fix #1709: perp.position_id still wins on mismatch, but a WARNING
        is emitted so the silent-override behaviour is no longer silent.

        Precedence (post-fix): perp > result > intent. The perp extractor is
        typically the most authoritative source for perp NFT ids, so it
        remains the tie-breaker when the seeded value differs. The fix makes
        the disagreement observable instead of invisible.
        """
        import logging

        intent = MockIntent("PERP_OPEN", protocol="gmx_v2", position_id="from-intent")
        result = MockResult(position_id="from-result")  # seeded
        result.extracted_data = {
            "perp_data": _Attrs(position_id="from-perp-extractor")
        }
        with caplog.at_level(logging.WARNING, logger="almanak.framework.observability.position_events"):
            event = build_position_event_from_intent(
                deployment_id="d", intent=intent, result=result
            )
        assert event is not None
        # Perp still wins on mismatch — but the warning makes it auditable.
        assert event.position_id == "from-perp-extractor"
        # A WARNING must be emitted on mismatch.
        assert any("#1709" in r.getMessage() for r in caplog.records)

    def test_perp_position_id_agreement_does_not_warn(self, caplog):
        """Fix #1709: when perp.position_id agrees with the seeded value,
        no warning is emitted (the mismatch path is the only loud path).
        """
        import logging

        intent = MockIntent("PERP_OPEN", protocol="gmx_v2", position_id="same")
        result = MockResult(position_id="same")
        result.extracted_data = {"perp_data": _Attrs(position_id="same")}
        with caplog.at_level(logging.WARNING, logger="almanak.framework.observability.position_events"):
            event = build_position_event_from_intent(
                deployment_id="d", intent=intent, result=result
            )
        assert event is not None
        assert event.position_id == "same"
        assert not any("#1709" in r.getMessage() for r in caplog.records)

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
    """#1710 fix: lp_close MUST NOT clobber lp_open amount0/amount1."""

    def test_lp_close_preserves_lp_open_amounts_issue_1710(self, caplog):
        """Fix #1710: If extracted_data somehow contains BOTH lp_open_data and
        lp_close_data on the same intent, the close phase (δ) must not
        overwrite the amount0/amount1 that the open phase (γ) populated.

        Lifecycle-wise this shouldn't happen (an intent is either an OPEN or
        a CLOSE, never both), but the bug pre-fix silently clobbered the
        deposit amounts with the received amounts. After the fix, lp_open
        amounts are preserved and a WARNING is logged so the anomaly is
        visible instead of silent.
        """
        import logging

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
        with caplog.at_level(logging.WARNING, logger="almanak.framework.observability.position_events"):
            event = build_position_event_from_intent(
                deployment_id="d", intent=intent, result=result
            )
        assert event is not None
        # Fix: lp_open amounts preserved; close amounts dropped on the floor.
        assert event.amount0 == "100"
        assert event.amount1 == "200"
        # Token identity still from lp_open / intent
        assert event.token0 == "WETH"
        assert event.token1 == "USDC"
        # A WARNING must be emitted so the mutual-exclusivity violation is visible.
        assert any("#1710" in r.getMessage() for r in caplog.records)

    def test_coexistence_warning_fires_even_when_lp_open_amounts_empty(self, caplog):
        """CR #1751 round 2 regression: the collision warning must fire
        whenever BOTH lp_open_data and lp_close_data are present on the
        same intent, even if lp_open_data did not populate event.amount0/
        amount1 (e.g., payload carried position_id but no amounts).

        Pre-fix the warning was keyed off event.amount0/amount1 truthiness,
        so an lp_open with missing amount attrs silently suppressed the
        collision log — hiding a real mutual-exclusivity violation from
        operators. The collision itself is the anomaly; value preservation
        is handled independently below.
        """
        import logging

        # lp_open_data carries position_id but NO amounts. lp_close fills
        # them. Collision is still a real anomaly that must be logged.
        intent = MockLPIntent("LP_OPEN", token0="WETH", token1="USDC")
        result = MockResult(position_id="any")
        result.extracted_data = {
            "lp_open_data": _Attrs(position_id=1),  # no amount0/amount1
            "lp_close_data": _Attrs(
                amount0_received=999, amount1_received=888
            ),
        }
        with caplog.at_level(logging.WARNING, logger="almanak.framework.observability.position_events"):
            event = build_position_event_from_intent(
                deployment_id="d", intent=intent, result=result
            )
        assert event is not None
        # Empty lp_open slots were filled by lp_close (preservation-only-
        # for-non-empty-slots still applies).
        assert event.amount0 == "999"
        assert event.amount1 == "888"
        # Warning MUST fire on the payload collision itself.
        assert any("#1710" in r.getMessage() for r in caplog.records)


class TestIntentDispatch:
    """Phase α — INTENT_TO_EVENT_TYPE dispatch.

    Non-position-tracked intents (SWAP, STAKE, UNSTAKE, HOLD, BRIDGE,
    UNWRAP, unregistered) → None. Lending intents (SUPPLY/BORROW/REPAY/
    WITHDRAW/DELEVERAGE) DO produce events as of VIB-4085 — see
    ``test_position_events_lending_vib4085.py`` for that contract.
    """

    @pytest.mark.parametrize(
        "intent_type",
        [
            "SWAP",
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


# --- Phase 5j: serialization-stability + SQLite roundtrip guardrails ---
#
# These tests pin the serialization contract of PositionEvent so that silent
# schema drift -- e.g. an innocuous-looking field add/remove/rename -- cannot
# land without an explicit reviewer decision.
#
# Three surfaces are guarded:
#
#   1. to_dict() keyset  -- feeds pnl_attributor.py (:632) and any external
#      JSON consumer.  Any change here ripples into the attribution payload
#      and into downstream PnL tooling.
#   2. asdict() keyset   -- pins the PositionEvent dataclass-field keyset.
#      The SQLite INSERT at backends/sqlite.py:~2086 is positional, so each
#      field's presence and position is a production contract.
#   3. SQLite roundtrip  -- full save_position_event -> get_position_history
#      cycle.  Guards the positional INSERT from column-order bugs and also
#      pins the "None vs empty string" coalescing rules on read-back.
#
# GOLDEN KEYSETS: these match the PositionEvent dataclass EXACTLY as of
# Phase 5i (32 fields).  When this test breaks intentionally (a field was
# added/removed on purpose), update BOTH the golden frozenset AND the SQLite
# INSERT statement + column list in almanak/framework/state/backends/sqlite.py
# in the SAME PR.  Otherwise the column-order contract silently drifts.

# Order-insensitive golden keyset (sets ignore insertion order but assert
# exact membership).  Phase 5i snapshot.
_POSITION_EVENT_GOLDEN_KEYS: frozenset[str] = frozenset(
    {
        # Identity + wiring
        "id",
        "deployment_id",
        "cycle_id",
        "execution_mode",
        "position_id",
        "position_type",
        "event_type",
        "timestamp",
        "protocol",
        "chain",
        # Token amounts (raw observables)
        "token0",
        "token1",
        "amount0",
        "amount1",
        "value_usd",
        # LP-specific
        "tick_lower",
        "tick_upper",
        "liquidity",
        "in_range",
        "fees_token0",
        "fees_token1",
        # Perp-specific
        "leverage",
        "entry_price",
        "mark_price",
        "unrealized_pnl",
        "is_long",
        # Execution details
        "tx_hash",
        "gas_usd",
        "ledger_entry_id",
        # VIB-3205 protocol fees
        "protocol_fees_usd",
        # Versioned attribution
        "attribution_json",
        "attribution_version",
    }
)


class TestPositionEventGoldenKeyset:
    """Phase 5j — golden-keyset contract for PositionEvent serialization.

    Locks the keyset of to_dict() and asdict() output to a hardcoded golden
    set.  Any future attribute add/remove/rename trips this and forces an
    explicit reviewer decision, because the keyset feeds both the SQLite
    positional INSERT (column contract) and the pnl_attributor to_dict()
    consumer.
    """

    def test_to_dict_keyset_matches_golden(self):
        """PositionEvent().to_dict() key-set == golden frozenset."""
        actual = frozenset(PositionEvent().to_dict().keys())
        missing = _POSITION_EVENT_GOLDEN_KEYS - actual
        extra = actual - _POSITION_EVENT_GOLDEN_KEYS
        assert not missing, f"to_dict() missing golden keys: {sorted(missing)}"
        assert not extra, (
            f"to_dict() has unexpected keys: {sorted(extra)}. "
            "If intentional, update _POSITION_EVENT_GOLDEN_KEYS AND the "
            "SQLite INSERT column list in backends/sqlite.py in the same PR."
        )
        assert actual == _POSITION_EVENT_GOLDEN_KEYS

    def test_asdict_keyset_matches_golden(self):
        """asdict(PositionEvent()) key-set == golden frozenset.

        Pins the dataclass-field contract that the positional SQLite INSERT
        at backends/sqlite.py:~2086 depends on.
        """
        actual = frozenset(asdict(PositionEvent()).keys())
        missing = _POSITION_EVENT_GOLDEN_KEYS - actual
        extra = actual - _POSITION_EVENT_GOLDEN_KEYS
        assert not missing, f"asdict() missing golden keys: {sorted(missing)}"
        assert not extra, (
            f"asdict() has unexpected keys: {sorted(extra)}. "
            "If intentional, update _POSITION_EVENT_GOLDEN_KEYS AND the "
            "SQLite INSERT column list in backends/sqlite.py in the same PR."
        )
        assert actual == _POSITION_EVENT_GOLDEN_KEYS

    def test_to_dict_and_asdict_have_same_keys(self):
        """to_dict() only differs from asdict() in VALUE shape (timestamp),
        not in key shape.  A drift between the two would silently break
        JSON consumers that assume they're interchangeable."""
        e = PositionEvent()
        assert set(e.to_dict().keys()) == set(asdict(e).keys())

    def test_dataclass_fields_match_golden(self):
        """dataclasses.fields() matches the golden set -- catches field
        renames that happen to leave the keyset count unchanged."""
        actual = frozenset(f.name for f in fields(PositionEvent))
        assert actual == _POSITION_EVENT_GOLDEN_KEYS

    def test_dataclass_field_count_is_32(self):
        """Hard-coded field count.  Cheap redundant check that forces any
        additive/subtractive change to touch this test explicitly."""
        assert len(fields(PositionEvent)) == 32


class TestPositionEventToDictValueShape:
    """Phase 5j — value-shape contract for to_dict() output.

    Verifies types and representations of each value class emitted by
    to_dict() so that pnl_attributor and any JSON consumer can rely on
    stable shapes (ISO-8601 timestamps, str-typed numerics, bool/None
    preserved on tri-state fields).
    """

    def test_timestamp_is_iso_string(self):
        """to_dict() stringifies timestamp via datetime.isoformat()."""
        ts = datetime(2026, 4, 1, 12, 30, 45, tzinfo=UTC)
        e = PositionEvent(timestamp=ts)
        d = e.to_dict()
        assert isinstance(d["timestamp"], str)
        assert d["timestamp"] == ts.isoformat()
        assert d["timestamp"] == "2026-04-01T12:30:45+00:00"

    def test_asdict_leaves_timestamp_as_datetime(self):
        """Contrast: asdict() preserves datetime, to_dict() does not.

        This is the only value-shape difference between the two; pinning it
        guards against accidental reversal (e.g. someone moving the isoformat
        call into __post_init__).
        """
        ts = datetime(2026, 4, 1, 12, 30, 45, tzinfo=UTC)
        e = PositionEvent(timestamp=ts)
        raw = asdict(e)
        assert isinstance(raw["timestamp"], datetime)
        assert raw["timestamp"] == ts

    def test_string_fields_default_to_empty_string(self):
        """All str-typed defaults surface as '' (never None) in to_dict()."""
        d = PositionEvent().to_dict()
        empty_string_fields = [
            "deployment_id",
            "cycle_id",
            "execution_mode",
            "position_id",
            "position_type",
            "event_type",
            "protocol",
            "chain",
            "token0",
            "token1",
            "amount0",
            "amount1",
            "value_usd",
            "liquidity",
            "fees_token0",
            "fees_token1",
            "leverage",
            "entry_price",
            "mark_price",
            "unrealized_pnl",
            "tx_hash",
            "gas_usd",
            "ledger_entry_id",
            "protocol_fees_usd",
        ]
        for fname in empty_string_fields:
            assert d[fname] == "", f"expected '' for {fname}, got {d[fname]!r}"
            assert isinstance(d[fname], str), (
                f"{fname} type drift: {type(d[fname]).__name__}"
            )

    def test_tri_state_fields_default_to_none(self):
        """tick_lower / tick_upper / in_range / is_long are tri-state: None
        is a first-class value meaning 'unobserved', distinct from 0/False."""
        d = PositionEvent().to_dict()
        for fname in ("tick_lower", "tick_upper", "in_range", "is_long"):
            assert d[fname] is None, f"{fname} should default to None, got {d[fname]!r}"

    def test_numeric_string_fields_stringify_provided_values(self):
        """Numeric fields are str-typed -- value is never re-serialized."""
        e = PositionEvent(
            amount0="1.234567890123456789",
            amount1="0",
            value_usd="42.00",
            liquidity="1000000000000000000",
            leverage="10.5",
            entry_price="2500.12345",
            mark_price="2510.00",
            unrealized_pnl="-12.34",
            gas_usd="2.50",
            protocol_fees_usd="0",
        )
        d = e.to_dict()
        # Stored verbatim -- no rounding / scientific-notation drift.
        assert d["amount0"] == "1.234567890123456789"
        assert d["amount1"] == "0"
        assert d["value_usd"] == "42.00"
        assert d["liquidity"] == "1000000000000000000"
        assert d["leverage"] == "10.5"
        assert d["entry_price"] == "2500.12345"
        assert d["mark_price"] == "2510.00"
        assert d["unrealized_pnl"] == "-12.34"
        assert d["gas_usd"] == "2.50"
        # Measured zero ("0") distinct from unknown ("").
        assert d["protocol_fees_usd"] == "0"
        assert d["protocol_fees_usd"] != ""

    def test_booleans_preserved(self):
        """in_range / is_long preserve True/False literals (not coerced)."""
        e = PositionEvent(in_range=True, is_long=False)
        d = e.to_dict()
        assert d["in_range"] is True
        assert d["is_long"] is False
        assert isinstance(d["in_range"], bool)
        assert isinstance(d["is_long"], bool)

    def test_integer_tick_fields_preserved_as_int(self):
        """tick_lower / tick_upper are int | None -- pin the int type
        (not stringified), because ticks are signed integers that signed
        queries depend on."""
        e = PositionEvent(tick_lower=-1000, tick_upper=1000)
        d = e.to_dict()
        assert d["tick_lower"] == -1000
        assert d["tick_upper"] == 1000
        assert isinstance(d["tick_lower"], int)
        assert isinstance(d["tick_upper"], int)

    def test_attribution_fields_default_shape(self):
        """attribution_json defaults to "{}" (empty JSON object literal),
        attribution_version defaults to 0 (int, not string)."""
        d = PositionEvent().to_dict()
        assert d["attribution_json"] == "{}"
        assert d["attribution_version"] == 0
        assert isinstance(d["attribution_json"], str)
        assert isinstance(d["attribution_version"], int)

    def test_id_is_uuid_string(self):
        """id default is a UUID4 stringified -- pin type AND contract.

        Per-CodeRabbit feedback: assert UUID4 parseability AND round-trip
        equality so a regression to a random str-generator (or UUID1/3/5)
        trips the test rather than sliding through on string-typing alone.
        """
        import uuid

        d = PositionEvent().to_dict()
        assert isinstance(d["id"], str)
        # Parseable as UUID, version 4 specifically.
        parsed = uuid.UUID(d["id"])
        assert parsed.version == 4
        # Canonical round-trip: the default-factory output is exactly the
        # str() of the generated UUID (no surrounding whitespace / braces).
        assert str(parsed) == d["id"]
        # Two separate events get distinct ids (factory called per-instance).
        other = PositionEvent().to_dict()
        assert uuid.UUID(other["id"]).version == 4
        assert d["id"] != other["id"]


class TestPositionEventSQLiteRoundtrip:
    """Phase 5j — end-to-end save -> read-back contract.

    The INSERT at backends/sqlite.py:~2086 is POSITIONAL (32 '?' placeholders
    paired with 32 attribute reads by NAME in the value tuple).  If someone
    adds a column to the SQL but forgets to insert the matching attribute in
    the tuple (or vice versa), every write silently misaligns by one column
    and the error shows up only hours later during PnL reconstruction.
    These tests trip that class of bug at PR time.
    """

    def test_full_field_roundtrip(self, store):
        """Build a PositionEvent with EVERY persisted field set to a unique,
        recognizable value; save; read back via get_position_history; assert
        field-by-field equality.  A column-order swap would surface as a
        value appearing under the wrong key in the read-back dict.
        """
        ts = datetime(2026, 3, 15, 10, 20, 30, tzinfo=UTC)
        event = PositionEvent(
            id="evt-phase5j-001",
            deployment_id="strat:phase5j",
            cycle_id="cycle-77",
            execution_mode="live",
            position_id="pos-9999",
            position_type="LP",
            event_type="OPEN",
            timestamp=ts,
            protocol="uniswap_v3",
            chain="arbitrum",
            token0="USDC",
            token1="WETH",
            amount0="1000.00",
            amount1="0.5",
            value_usd="9999.77",  # distinct sentinel to detect column drift
            tick_lower=-887272,
            tick_upper=887272,
            liquidity="1000000000000000000",
            in_range=True,
            fees_token0="0.10",
            fees_token1="0.0001",
            leverage="3.0",
            entry_price="2500.00",
            mark_price="2505.00",
            unrealized_pnl="5.00",
            is_long=True,
            tx_hash="0xdeadbeef",
            gas_usd="4.25",
            ledger_entry_id="ledger-001",
            protocol_fees_usd="0.25",
            attribution_json='{"v": 1}',
            attribution_version=1,
        )

        ok = asyncio.get_event_loop().run_until_complete(
            store.save_position_event(event)
        )
        assert ok

        history = asyncio.get_event_loop().run_until_complete(
            store.get_position_history("strat:phase5j", "pos-9999")
        )
        assert len(history) == 1
        row = history[0]

        # String fields -- stored verbatim.
        assert row["id"] == "evt-phase5j-001"
        assert row["deployment_id"] == "strat:phase5j"
        assert row["cycle_id"] == "cycle-77"
        assert row["execution_mode"] == "live"
        assert row["position_id"] == "pos-9999"
        assert row["position_type"] == "LP"
        assert row["event_type"] == "OPEN"
        assert row["protocol"] == "uniswap_v3"
        assert row["chain"] == "arbitrum"
        assert row["token0"] == "USDC"
        assert row["token1"] == "WETH"
        assert row["amount0"] == "1000.00"
        assert row["amount1"] == "0.5"
        # Distinct sentinel asserted explicitly: if value_usd got mis-routed
        # into another column (e.g. entry_price), this fails because no
        # other field in the payload holds "9999.77".
        assert row["value_usd"] == "9999.77"
        assert row["liquidity"] == "1000000000000000000"
        assert row["fees_token0"] == "0.10"
        assert row["fees_token1"] == "0.0001"
        assert row["leverage"] == "3.0"
        assert row["entry_price"] == "2500.00"
        assert row["mark_price"] == "2505.00"
        assert row["unrealized_pnl"] == "5.00"
        assert row["tx_hash"] == "0xdeadbeef"
        assert row["gas_usd"] == "4.25"
        assert row["ledger_entry_id"] == "ledger-001"
        assert row["protocol_fees_usd"] == "0.25"
        assert row["attribution_json"] == '{"v": 1}'

        # Integer fields -- stored as integers (signed ticks intact).
        assert row["tick_lower"] == -887272
        assert row["tick_upper"] == 887272
        assert row["attribution_version"] == 1

        # Booleans -- SQLite stores as 0/1 integers; roundtrip compares with
        # `== 1` / `== 0` via truthy-int to preserve the current contract.
        # If this ever changes (e.g. pydantic-style bool coercion), we want
        # to know at PR time.
        assert row["in_range"] in (1, True)
        assert row["is_long"] in (1, True)

        # Timestamp -- ISO-8601 string (written as event.timestamp.isoformat()).
        assert row["timestamp"] == ts.isoformat()

    def test_minimal_fields_roundtrip(self, store):
        """Persist a PositionEvent with ONLY the mandatory fields set (all
        other fields default to "" / None / 0 / "{}"), read back, and assert
        the empty-string / None coalescing rules on read-back match the
        write-side contract.  Specifically exercises the VIB-3205 guard at
        backends/sqlite.py:~2134 where `protocol_fees_usd` preserves "" for
        "parser did not emit" distinct from "0" for "measured zero"."""
        event = PositionEvent(
            deployment_id="strat:min",
            position_id="min-pos",
            position_type="PERP",
            event_type="OPEN",
        )
        ok = asyncio.get_event_loop().run_until_complete(
            store.save_position_event(event)
        )
        assert ok

        history = asyncio.get_event_loop().run_until_complete(
            store.get_position_history("strat:min", "min-pos")
        )
        assert len(history) == 1
        row = history[0]

        # Mandatory wiring.
        assert row["deployment_id"] == "strat:min"
        assert row["position_id"] == "min-pos"
        assert row["position_type"] == "PERP"
        assert row["event_type"] == "OPEN"

        # Empty-string default fields -- preserved as "" through the roundtrip
        # (NOT coerced to NULL).  If an unintended NULL appears, downstream
        # string concatenations / Decimal(...) construction breaks.
        empty_string_fields = [
            "cycle_id",
            "execution_mode",
            "protocol",
            "chain",
            "token0",
            "token1",
            "amount0",
            "amount1",
            "value_usd",
            "liquidity",
            "fees_token0",
            "fees_token1",
            "leverage",
            "entry_price",
            "mark_price",
            "unrealized_pnl",
            "tx_hash",
            "gas_usd",
            "ledger_entry_id",
        ]
        for fname in empty_string_fields:
            assert row[fname] == "", (
                f"{fname} should roundtrip '' (unknown), got {row[fname]!r}"
            )

        # VIB-3205 empty-vs-zero invariant: unset protocol_fees_usd must
        # surface as "" (unknown), distinct from "0" (measured zero).
        assert row["protocol_fees_usd"] == ""
        assert row["protocol_fees_usd"] != "0"

        # Tri-state fields remain NULL (SQLite NULL, Python None).
        for fname in ("tick_lower", "tick_upper", "in_range", "is_long"):
            assert row[fname] is None, (
                f"{fname} should roundtrip None, got {row[fname]!r}"
            )

        # Attribution defaults.
        assert row["attribution_json"] == "{}"
        assert row["attribution_version"] == 0

        # Timestamp is auto-generated (datetime.now(UTC)) -- assert it round-
        # trips as an ISO string without guessing its exact value.
        assert isinstance(row["timestamp"], str)
        assert "T" in row["timestamp"]  # ISO-8601 'YYYY-MM-DDTHH:MM:SS...' shape.
