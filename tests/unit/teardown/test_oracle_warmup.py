"""Tests for the teardown price-oracle warm + validate seam (VIB-4842).

Covers:
- An un-warmed MarketSnapshot is warmed for the plan's token set (+ native gas
  token) and produces a complete oracle dict before compile.
- A genuinely unpriceable token raises a *named* teardown pre-flight error,
  not a bare compiler ValueError surfaced three layers down.
- Both the ``execute`` path (Intent objects) and the ``resume`` path
  (serialized intent dicts) warm the oracle.
- Wrapped<->native alias, case-insensitivity, and known-stablecoin $1 fallback
  are honoured so validation does not fail loud on tokens the compiler would
  resolve anyway.
"""

import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.intents.vocabulary import Intent
from almanak.framework.teardown.config import TeardownConfig
from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownPositionSummary,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.oracle_warmup import (
    TeardownPriceOracleError,
    extract_required_token_chains,
    extract_required_tokens,
    warm_and_validate_oracle,
)
from almanak.framework.teardown.teardown_manager import TeardownManager


class _FakeMarket:
    """A MarketSnapshot-like fake with a price oracle warmed by .price().

    ``price()`` populates ``_cache`` only for tokens in ``priceable`` (mirrors a
    real oracle that knows some tokens and fails on others). ``_cache`` is what
    ``get_price_oracle_dict()`` exposes — exactly the snapshot contract under
    test (un-warmed cache -> empty dict until .price() runs).
    """

    def __init__(self, priceable: dict[str, Decimal]):
        self._priceable = priceable
        self._cache: dict[str, Decimal] = {}
        self.price_calls: list[str] = []
        # (token, chain) tuples, for asserting per-intent chain threading.
        self.price_call_chains: list[tuple[str, str | None]] = []

    def price(self, token: str, quote: str = "USD", *, chain=None) -> Decimal:
        self.price_calls.append(token)
        self.price_call_chains.append((token, chain))
        key = token.upper()
        if key in self._priceable:
            self._cache[key] = self._priceable[key]
            return self._priceable[key]
        raise ValueError(f"Cannot determine price for {token}/{quote}")

    def get_price_oracle_dict(self, with_sources: bool = False) -> dict:
        return dict(self._cache)


# ---------------------------------------------------------------------------
# extract_required_tokens
# ---------------------------------------------------------------------------


def test_extract_tokens_from_swap_intent_includes_native_gas():
    intent = Intent.swap(from_token="WETH", to_token="USDC", amount="all", chain="arbitrum")
    tokens = extract_required_tokens([intent], "arbitrum")
    assert {"WETH", "USDC", "ETH"} <= tokens


def test_extract_tokens_from_pool_string():
    intent = Intent.lp_open(
        pool="WETH/USDC/3000",
        amount0=Decimal("1"),
        amount1=Decimal("1"),
        range_lower=Decimal("1000"),
        range_upper=Decimal("2000"),
        chain="ethereum",
    )
    tokens = extract_required_tokens([intent], "ethereum")
    assert {"WETH", "USDC", "ETH"} <= tokens


def test_extract_tokens_from_serialized_dict():
    # resume path stores serialized intent dicts
    d = {"from_token": "wstETH", "to_token": "USDC", "chain": "ethereum", "type": "SWAP"}
    tokens = extract_required_tokens([d], "ethereum")
    assert {"WSTETH", "USDC", "ETH"} <= tokens


def test_extract_skips_addresses_and_includes_polygon_native():
    # raw 0x token addresses are resolved by the connector, not priced by symbol
    d = {"from_token": "0xabc0000000000000000000000000000000000000", "to_token": "USDC", "type": "SWAP"}
    tokens = extract_required_tokens([d], "polygon")
    assert "USDC" in tokens
    assert not any(t.startswith("0X") for t in tokens)
    # polygon native gas symbols
    assert {"MATIC", "POL"} <= tokens


def test_extract_multichain_plan_includes_each_chains_native_gas():
    """VIB-4842 P2: a multi-chain plan warms every chain's native gas token."""
    arb_swap = {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"}
    poly_swap = {"from_token": "WMATIC", "to_token": "USDC", "chain": "polygon", "type": "SWAP"}
    tokens = extract_required_tokens([arb_swap, poly_swap], None)
    # Both chains' native gas tokens present (not just one chain's).
    assert "ETH" in tokens  # arbitrum native
    assert {"MATIC", "POL"} <= tokens  # polygon native
    # Plan tokens themselves.
    assert {"WETH", "WMATIC", "USDC"} <= tokens


def test_extract_token_chains_maps_each_token_to_its_intent_chain():
    """VIB-4842 P1: each token carries the chain of the intent it came from."""
    arb_swap = {"from_token": "ARB", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"}
    base_swap = {"from_token": "DEGEN", "to_token": "USDC", "chain": "base", "type": "SWAP"}
    mapping = extract_required_token_chains([arb_swap, base_swap], None)
    assert mapping["ARB"] == "arbitrum"
    assert mapping["DEGEN"] == "base"
    # Native gas tokens map to their own chain.
    assert mapping["ETH"] in {"arbitrum", "base"}


def test_extract_token_chains_uses_fallback_when_intent_has_no_chain():
    """A token on a chain-less intent inherits the plan-wide fallback chain."""
    d = {"from_token": "WETH", "to_token": "USDC", "type": "SWAP"}  # no chain
    mapping = extract_required_token_chains([d], "arbitrum")
    assert mapping["WETH"] == "arbitrum"
    assert mapping["USDC"] == "arbitrum"


# ---------------------------------------------------------------------------
# warm_and_validate_oracle
# ---------------------------------------------------------------------------


def test_warm_populates_oracle_for_swap_plan():
    """Un-warmed market is warmed for the plan token set + gas, dict complete."""
    market = _FakeMarket({"WETH": Decimal("3400"), "USDC": Decimal("1"), "ETH": Decimal("3400")})
    intent = Intent.swap(from_token="WETH", to_token="USDC", amount="all", chain="arbitrum")

    # Pre-condition: un-warmed oracle is empty.
    assert market.get_price_oracle_dict() == {}

    oracle = warm_and_validate_oracle(market, [intent], "arbitrum")

    assert oracle is not None
    assert oracle["WETH"] == Decimal("3400")
    assert oracle["USDC"] == Decimal("1")
    # All required tokens were warmed via the sync price() entry point.
    assert {"WETH", "USDC", "ETH"} <= set(market.price_calls)


def test_warm_resolves_wrapped_native_alias_without_failing():
    """WETH required but oracle only holds ETH — alias resolves, no loud fail."""
    # ETH priceable, WETH NOT directly priceable -> relies on alias in validation.
    market = _FakeMarket({"ETH": Decimal("3400"), "USDC": Decimal("1")})
    intent = Intent.swap(from_token="WETH", to_token="USDC", amount="all", chain="arbitrum")

    oracle = warm_and_validate_oracle(market, [intent], "arbitrum")

    assert oracle is not None
    # WETH validated via ETH alias even though price(WETH) raised.
    assert oracle.get("ETH") == Decimal("3400")


def test_warm_resolves_native_from_wrapped_only_oracle():
    """VIB-4842: native ETH required but oracle holds only WETH — the reverse
    (native<-wrapped) alias must resolve so the pre-flight does not falsely
    block a teardown the compiler would have priced via its bidirectional
    alias expansion."""
    # WETH priceable; the native gas token ETH is NOT directly priceable, so
    # validation must fall back to the WETH price via the reverse alias.
    market = _FakeMarket({"WETH": Decimal("3400"), "USDC": Decimal("1")})
    intent = Intent.swap(from_token="WETH", to_token="USDC", amount="all", chain="arbitrum")

    # ETH is added to the required set as arbitrum's native gas token; with only
    # WETH priced this raised TeardownPriceOracleError before the reverse-alias fix.
    oracle = warm_and_validate_oracle(market, [intent], "arbitrum")

    assert oracle is not None
    assert oracle.get("WETH") == Decimal("3400")


def test_warm_stablecoin_fallback_when_unpriced():
    """An unpriced known stablecoin does not fail the pre-flight ($1 fallback)."""
    # DAI not priceable by the oracle, but it is a known stablecoin.
    market = _FakeMarket({"WETH": Decimal("3400"), "ETH": Decimal("3400")})
    intent = Intent.swap(from_token="WETH", to_token="DAI", amount="all", chain="arbitrum")

    # Should NOT raise — DAI resolves via stablecoin fallback in the compiler.
    oracle = warm_and_validate_oracle(market, [intent], "arbitrum")
    assert oracle is not None


def test_warm_raises_named_error_for_unpriceable_token():
    """A genuinely unpriceable non-stable token fails loud with its name."""
    # ARB is neither priceable, a wrapped-native, nor a stablecoin.
    market = _FakeMarket({"USDC": Decimal("1"), "ETH": Decimal("3400")})
    intent = Intent.swap(from_token="ARB", to_token="USDC", amount="all", chain="arbitrum")

    with pytest.raises(TeardownPriceOracleError) as exc:
        warm_and_validate_oracle(market, [intent], "arbitrum")

    msg = str(exc.value)
    assert "ARB" in msg  # names the offending token
    assert "missing" in msg.lower()
    assert "sources tried" in msg.lower()  # names the warming attempt


def test_warm_no_market_returns_none():
    assert warm_and_validate_oracle(None, [], "arbitrum") is None


def test_warm_no_required_tokens_returns_fetched_verbatim():
    """LP_CLOSE-only plan (tokens resolved on-chain) returns the fetched dict."""
    intent = Intent.lp_close(position_id="123", chain="ethereum")
    # lp_close carries no token symbols and no pool symbols -> only native gas.
    # Use a market that prices ETH so we isolate the "no intent tokens" path.
    market = _FakeMarket({"ETH": Decimal("3400")})
    oracle = warm_and_validate_oracle(market, [intent], "ethereum")
    assert oracle is not None
    assert oracle["ETH"] == Decimal("3400")


def test_warm_threads_each_intents_chain_into_price_call():
    """VIB-4842 P1: price() is called with each token's own intent chain."""
    market = _FakeMarket(
        {
            "WETH": Decimal("3400"),
            "ETH": Decimal("3400"),
            "DEGEN": Decimal("0.01"),
            "USDC": Decimal("1"),
        }
    )
    arb_swap = Intent.swap(from_token="WETH", to_token="USDC", amount="all", chain="arbitrum")
    base_swap = Intent.swap(from_token="DEGEN", to_token="USDC", amount="all", chain="base")

    oracle = warm_and_validate_oracle(market, [arb_swap, base_swap], None)
    assert oracle is not None

    chain_by_token = dict(market.price_call_chains)
    # Each token priced on the chain of the intent that referenced it.
    assert chain_by_token["WETH"] == "arbitrum"
    assert chain_by_token["DEGEN"] == "base"
    # Native gas tokens carry a concrete chain (not None).
    assert chain_by_token["ETH"] in {"arbitrum", "base"}


def test_warm_multichain_warms_both_native_gas_tokens():
    """VIB-4842 P2: a multi-chain plan warms each chain's native gas symbol."""
    market = _FakeMarket(
        {
            "WETH": Decimal("3400"),
            "ETH": Decimal("3400"),
            "WMATIC": Decimal("0.5"),
            "MATIC": Decimal("0.5"),
            "POL": Decimal("0.5"),
            "USDC": Decimal("1"),
        }
    )
    arb_swap = {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"}
    poly_swap = {"from_token": "WMATIC", "to_token": "USDC", "chain": "polygon", "type": "SWAP"}

    warm_and_validate_oracle(market, [arb_swap, poly_swap], None)

    warmed = {t.upper() for t in market.price_calls}
    # Both chains' native gas tokens were warmed.
    assert "ETH" in warmed  # arbitrum native
    assert {"MATIC", "POL"} <= warmed  # polygon native


def test_warm_best_effort_does_not_raise_on_missing():
    """raise_on_missing=False: a still-missing token logs but does not raise."""
    # ARB is genuinely unpriceable, but in best-effort mode we must not raise.
    market = _FakeMarket({"USDC": Decimal("1"), "ETH": Decimal("3400")})
    intent = Intent.swap(from_token="ARB", to_token="USDC", amount="all", chain="arbitrum")

    # Would raise in the default (pre-flight) mode.
    with pytest.raises(TeardownPriceOracleError):
        warm_and_validate_oracle(market, [intent], "arbitrum")

    # Best-effort: returns the warmed dict instead of raising.
    oracle = warm_and_validate_oracle(market, [intent], "arbitrum", raise_on_missing=False)
    assert oracle is not None
    # USDC still got warmed (cache populated for the rest of the plan).
    assert oracle["USDC"] == Decimal("1")


# ---------------------------------------------------------------------------
# Manager integration — execute path
# ---------------------------------------------------------------------------


def _make_strategy(intents):
    strategy = MagicMock()
    strategy.deployment_id = "test_strat"
    strategy.name = "Test Strategy"
    strategy.chain = "arbitrum"
    strategy.uses_safe_wallet = False
    strategy.pause = AsyncMock()

    positions = MagicMock(spec=TeardownPositionSummary)
    positions.positions = []
    positions.total_value_usd = Decimal("10000")
    positions.has_liquidation_risk = False
    positions.chains_involved = {"arbitrum"}
    strategy.get_open_positions.return_value = positions
    strategy.generate_teardown_intents.return_value = intents
    return strategy


@pytest.mark.asyncio
async def test_execute_warms_oracle_before_compile():
    """execute() warms the plan token set; _execute_intents sees a full oracle."""
    market = _FakeMarket({"WETH": Decimal("3400"), "USDC": Decimal("1"), "ETH": Decimal("3400")})
    intent = Intent.swap(from_token="WETH", to_token="USDC", amount="all", chain="arbitrum")
    strategy = _make_strategy([intent])

    captured = []

    async def spy_execute(*args, **kwargs):
        captured.append(kwargs.get("price_oracle"))
        return MagicMock(success=True, results=[], error=None)

    manager = TeardownManager()
    manager._execute_intents = spy_execute
    manager.cancel_window.run_cancel_window = AsyncMock(return_value=MagicMock(was_cancelled=False))
    manager.safety_guard.validate_teardown_request = MagicMock(return_value=MagicMock(all_passed=True))

    await manager.execute(strategy=strategy, mode="graceful", market=market)

    assert len(captured) == 1
    assert captured[0]["WETH"] == Decimal("3400")
    assert captured[0]["USDC"] == Decimal("1")
    # Warmed via sync price() before compile.
    assert "WETH" in market.price_calls


@pytest.mark.asyncio
async def test_execute_raises_named_preflight_error_for_unpriceable():
    """execute() surfaces the named teardown pre-flight error, not a bare ValueError.

    ALM-2766 (CR#3): the fail-loud pre-flight warm now applies only to
    risk-reducing intents — clampable ``amount='all'`` swap-backs are warmed
    best-effort so an unpriceable commingled swap-back cannot block the closers.
    This test therefore uses a FIXED-amount swap (NOT a clampable swap-back) to
    exercise the named pre-flight error for the intents that still fail loud.
    """
    market = _FakeMarket({"USDC": Decimal("1"), "ETH": Decimal("3400")})
    intent = Intent.swap(from_token="ARB", to_token="USDC", amount=Decimal("100"), chain="arbitrum")
    strategy = _make_strategy([intent])

    execute_called = []

    async def spy_execute(*args, **kwargs):
        execute_called.append(True)
        return MagicMock(success=True, results=[], error=None)

    manager = TeardownManager()
    manager._execute_intents = spy_execute
    manager.cancel_window.run_cancel_window = AsyncMock(return_value=MagicMock(was_cancelled=False))
    manager.safety_guard.validate_teardown_request = MagicMock(return_value=MagicMock(all_passed=True))

    result = await manager.execute(strategy=strategy, mode="graceful", market=market)

    # Pre-flight failure is surfaced as a failed result naming the token, and
    # no intent ever reached execution (pre-flight = before on-chain risk).
    assert result.success is False
    assert "ARB" in result.error
    assert not execute_called


# ---------------------------------------------------------------------------
# Manager integration — resume path
# ---------------------------------------------------------------------------


def _resumable_state(
    intents_json: str,
    *,
    current_intent_index: int = 0,
    completed_intents: int | None = None,
    total_intents: int = 1,
) -> TeardownState:
    # ``completed_intents`` is the authoritative progress signal — it is bumped
    # AFTER an intent executes, whereas ``current_intent_index`` is persisted
    # BEFORE execution. They are decoupled so the resume-progress detection can
    # be exercised independently (VIB-4842 Codex re-audit). Default keeps the
    # historical "index == completed" coupling for the existing cases.
    if completed_intents is None:
        completed_intents = current_intent_index
    return TeardownState(
        teardown_id="td_123",
        deployment_id="test_strat",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=total_intents,
        completed_intents=completed_intents,
        current_intent_index=current_intent_index,
        started_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),  # fresh = not stale
        pending_intents_json=intents_json,
        cancel_window_until=datetime.now(UTC),
        config_json="{}",
    )


@pytest.mark.asyncio
async def test_resume_warms_oracle_from_serialized_intents():
    """resume() warms the oracle from serialized intent dicts before re-executing."""
    market = _FakeMarket({"WETH": Decimal("3400"), "USDC": Decimal("1"), "ETH": Decimal("3400")})
    strategy = _make_strategy([])

    intents_json = json.dumps([{"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"}])
    state = _resumable_state(intents_json)
    state_manager = MagicMock()
    state_manager.get_teardown_state = AsyncMock(return_value=state)

    captured = []

    async def spy_execute(*args, **kwargs):
        captured.append(kwargs.get("price_oracle"))
        return MagicMock(success=True, results=[], error=None)

    config = TeardownConfig.default()
    config.staleness_threshold_seconds = 100000  # never stale -> use stored intents
    manager = TeardownManager(state_manager=state_manager, config=config)
    manager._execute_intents = spy_execute

    await manager.resume(deployment_id="test_strat", strategy=strategy, market=market)

    assert len(captured) == 1
    assert captured[0]["WETH"] == Decimal("3400")
    assert "WETH" in market.price_calls


@pytest.mark.asyncio
async def test_resume_raises_named_preflight_error_for_unpriceable():
    """resume() also fails loud on an unpriceable token before re-executing."""
    market = _FakeMarket({"USDC": Decimal("1"), "ETH": Decimal("3400")})
    strategy = _make_strategy([])

    intents_json = json.dumps([{"from_token": "ARB", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"}])
    state = _resumable_state(intents_json)
    state_manager = MagicMock()
    state_manager.get_teardown_state = AsyncMock(return_value=state)

    execute_called = []

    async def spy_execute(*args, **kwargs):
        execute_called.append(True)
        return MagicMock(success=True, results=[], error=None)

    config = TeardownConfig.default()
    config.staleness_threshold_seconds = 100000
    manager = TeardownManager(state_manager=state_manager, config=config)
    manager._execute_intents = spy_execute

    with pytest.raises(TeardownPriceOracleError) as exc:
        await manager.resume(deployment_id="test_strat", strategy=strategy, market=market)

    assert "ARB" in str(exc.value)
    assert not execute_called


@pytest.mark.asyncio
async def test_resume_past_progress_skips_loud_gate_for_unpriceable():
    """VIB-4842 P1: resume past progress (index>0) must NOT fail loud.

    Some closing intents already landed on-chain; failing loud on a still-
    unpriceable token would block the next risk-reducing intent and strand a
    partially-unwound position. The gate is skipped — execution continues.
    """
    market = _FakeMarket({"USDC": Decimal("1"), "ETH": Decimal("3400")})
    strategy = _make_strategy([])

    # Two-intent plan, first already done (index 1). The remaining intent
    # references ARB, which is genuinely unpriceable.
    intents_json = json.dumps(
        [
            {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
            {"from_token": "ARB", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
        ]
    )
    state = _resumable_state(intents_json, current_intent_index=1, total_intents=2)
    state_manager = MagicMock()
    state_manager.get_teardown_state = AsyncMock(return_value=state)
    state_manager.save_teardown_state = AsyncMock()
    state_manager.delete_teardown_state = AsyncMock()

    captured = []

    async def spy_execute(*args, **kwargs):
        captured.append(kwargs.get("price_oracle"))
        return MagicMock(success=True, results=[], error=None)

    config = TeardownConfig.default()
    config.staleness_threshold_seconds = 100000
    manager = TeardownManager(state_manager=state_manager, config=config)
    manager._execute_intents = spy_execute

    # Must NOT raise — execution continues despite the unpriceable token.
    result = await manager.resume(deployment_id="test_strat", strategy=strategy, market=market)

    assert result is not None
    # _execute_intents was reached (the gate did not block the unwind).
    assert len(captured) == 1
    # The warmed dict was still passed through (best-effort), not None-by-raise.
    assert captured[0] is not None


@pytest.mark.asyncio
async def test_resume_completed_but_index_zero_skips_loud_gate():
    """VIB-4842 Codex re-audit P1: progress is detected via completed-count, not index.

    ``current_intent_index`` is persisted BEFORE intent ``i`` runs and is never
    advanced afterward — only ``completed_intents`` is bumped post-execution. So
    after a restart where the first intent (``i == 0``) already landed on-chain,
    the persisted state can have ``completed_intents > 0`` while
    ``current_intent_index`` is still 0. Gating on the index alone would misread
    this as a fresh start and fail loud, blocking the next risk-reducing intent.
    The gate must use the completed-count: any progress → best-effort warm, no raise.
    """
    market = _FakeMarket({"USDC": Decimal("1"), "ETH": Decimal("3400")})
    strategy = _make_strategy([])

    # First intent already landed (completed_intents == 1) but the index was
    # persisted as 0 (the pre-execution checkpoint for intent 0). The remaining
    # work references ARB, which is genuinely unpriceable.
    intents_json = json.dumps(
        [
            {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
            {"from_token": "ARB", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
        ]
    )
    state = _resumable_state(intents_json, current_intent_index=0, completed_intents=1, total_intents=2)
    state_manager = MagicMock()
    state_manager.get_teardown_state = AsyncMock(return_value=state)
    state_manager.save_teardown_state = AsyncMock()
    state_manager.delete_teardown_state = AsyncMock()

    captured = []

    async def spy_execute(*args, **kwargs):
        captured.append(kwargs.get("price_oracle"))
        return MagicMock(success=True, results=[], error=None)

    config = TeardownConfig.default()
    config.staleness_threshold_seconds = 100000
    manager = TeardownManager(state_manager=state_manager, config=config)
    manager._execute_intents = spy_execute

    # Must NOT raise — completed_intents > 0 means real on-chain progress, so
    # the loud pre-flight gate is skipped and the unwind continues.
    result = await manager.resume(deployment_id="test_strat", strategy=strategy, market=market)

    assert result is not None
    assert len(captured) == 1
    # Best-effort warm passed a (partial) dict through, not None-by-raise.
    assert captured[0] is not None


@pytest.mark.asyncio
async def test_resume_starts_from_completed_floor_not_lagging_index():
    """VIB-4842 Codex re-audit P1: resume must not re-execute a completed intent.

    ``current_intent_index`` is persisted BEFORE intent ``i`` runs and is never
    advanced after a success — only ``completed_intents`` is bumped afterward. So
    a restart after intent 0 landed on-chain leaves ``completed_intents == 1``
    while ``current_intent_index`` still reads 0. Passing the raw index as
    ``start_from_index`` would re-run intent 0 (a duplicate LP_CLOSE / closing
    action). resume() must floor the start at ``completed_intents`` so it
    continues at the next UNFINISHED intent (index 1).
    """
    # Both tokens priceable so the loud gate is irrelevant — this isolates the
    # resume-index arithmetic, not the oracle warmup branch.
    market = _FakeMarket({"WETH": Decimal("3400"), "USDC": Decimal("1"), "ETH": Decimal("3400")})
    strategy = _make_strategy([])

    intents_json = json.dumps(
        [
            {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
            {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
        ]
    )
    # Intent 0 finished (completed_intents == 1); index lags at 0.
    state = _resumable_state(intents_json, current_intent_index=0, completed_intents=1, total_intents=2)
    state_manager = MagicMock()
    state_manager.get_teardown_state = AsyncMock(return_value=state)
    state_manager.save_teardown_state = AsyncMock()
    state_manager.delete_teardown_state = AsyncMock()

    captured_start = []

    async def spy_execute(*args, **kwargs):
        captured_start.append(kwargs.get("start_from_index"))
        return MagicMock(success=True, results=[], error=None)

    config = TeardownConfig.default()
    config.staleness_threshold_seconds = 100000
    manager = TeardownManager(state_manager=state_manager, config=config)
    manager._execute_intents = spy_execute

    await manager.resume(deployment_id="test_strat", strategy=strategy, market=market)

    assert len(captured_start) == 1
    # Floor at completed_intents → start at the next unfinished intent (1),
    # NOT the lagging current_intent_index (0). Intent 0 is not re-executed.
    assert captured_start[0] == 1


@pytest.mark.asyncio
async def test_resume_honors_larger_persisted_index_over_completed():
    """The floor is ``max(index, completed_intents)`` — a legitimately larger
    persisted index is still honored (never regress below the index).
    """
    market = _FakeMarket({"WETH": Decimal("3400"), "USDC": Decimal("1"), "ETH": Decimal("3400")})
    strategy = _make_strategy([])

    intents_json = json.dumps(
        [
            {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
            {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
            {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
        ]
    )
    # Index already advanced to 2 (about to run intent 2); completed_intents
    # lags at 1. max(2, 1) == 2.
    state = _resumable_state(intents_json, current_intent_index=2, completed_intents=1, total_intents=3)
    state_manager = MagicMock()
    state_manager.get_teardown_state = AsyncMock(return_value=state)
    state_manager.save_teardown_state = AsyncMock()
    state_manager.delete_teardown_state = AsyncMock()

    captured_start = []

    async def spy_execute(*args, **kwargs):
        captured_start.append(kwargs.get("start_from_index"))
        return MagicMock(success=True, results=[], error=None)

    config = TeardownConfig.default()
    config.staleness_threshold_seconds = 100000
    manager = TeardownManager(state_manager=state_manager, config=config)
    manager._execute_intents = spy_execute

    await manager.resume(deployment_id="test_strat", strategy=strategy, market=market)

    assert len(captured_start) == 1
    assert captured_start[0] == 2


@pytest.mark.asyncio
async def test_resume_fresh_index_still_fails_loud_for_unpriceable():
    """Resume at index 0 (no progress) is a genuine pre-flight → still fails loud."""
    market = _FakeMarket({"USDC": Decimal("1"), "ETH": Decimal("3400")})
    strategy = _make_strategy([])

    intents_json = json.dumps([{"from_token": "ARB", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"}])
    state = _resumable_state(intents_json, current_intent_index=0)
    state_manager = MagicMock()
    state_manager.get_teardown_state = AsyncMock(return_value=state)

    execute_called = []

    async def spy_execute(*args, **kwargs):
        execute_called.append(True)
        return MagicMock(success=True, results=[], error=None)

    config = TeardownConfig.default()
    config.staleness_threshold_seconds = 100000
    manager = TeardownManager(state_manager=state_manager, config=config)
    manager._execute_intents = spy_execute

    with pytest.raises(TeardownPriceOracleError):
        await manager.resume(deployment_id="test_strat", strategy=strategy, market=market)
    assert not execute_called


@pytest.mark.asyncio
async def test_resume_regeneration_resets_completed_intents_and_starts_from_zero():
    """VIB-4842 Codex re-audit P1: regenerating intents resets the progress counter.

    A stale resumable teardown regenerates its intent plan (lines ~679-690):
    ``pending_intents_json`` is replaced with a freshly generated list and
    ``current_intent_index`` is reset to 0. Before the fix, ``completed_intents``
    was left at its OLD-plan value (N), so the resume floor
    ``max(current_intent_index=0, completed_intents=N) == N`` started the NEW
    plan at index N — skipping the first N regenerated closes, or (when the new
    plan is shorter than N) executing nothing and marking the teardown COMPLETE
    while regenerated on-chain risk-reducing closes were never run.

    The regeneration branch must also reset ``completed_intents = 0`` so the
    floor becomes ``max(0, 0) == 0`` and the resumed run executes every intent
    of the freshly generated plan from index 0.
    """
    # All tokens priceable so the loud oracle gate is irrelevant — this isolates
    # the regeneration / resume-index arithmetic.
    market = _FakeMarket({"WETH": Decimal("3400"), "USDC": Decimal("1"), "ETH": Decimal("3400")})

    # The regenerated plan is BRAND NEW (2 intents) and is what resume should
    # run from index 0. It is intentionally SHORTER than the old plan's
    # completed_intents (=2): under the bug, max(0, 2)==2 would skip the whole
    # new plan and mark teardown COMPLETE with zero closes executed.
    #
    # ``resume()`` serializes the regenerated plan via ``i.to_dict()`` (the
    # strategy-supplied teardown intents carry that method), so the fakes here
    # expose ``to_dict`` to match that contract.
    class _RegenIntent:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return dict(self._payload)

    regenerated = [
        _RegenIntent({"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"}),
        _RegenIntent({"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"}),
    ]
    strategy = _make_strategy(regenerated)

    # Old (stale) plan: 3 intents, 2 already completed. The stored JSON is the
    # OLD plan; regeneration replaces it before execution.
    old_intents_json = json.dumps(
        [
            {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
            {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
            {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
        ]
    )
    state = _resumable_state(old_intents_json, current_intent_index=2, completed_intents=2, total_intents=3)
    # Force the staleness branch: backdate updated_at well past the threshold.
    state.updated_at = datetime(2020, 1, 1, tzinfo=UTC)
    state_manager = MagicMock()
    state_manager.get_teardown_state = AsyncMock(return_value=state)
    state_manager.save_teardown_state = AsyncMock()
    state_manager.delete_teardown_state = AsyncMock()

    captured_start = []
    captured_intents = []

    async def spy_execute(*args, **kwargs):
        captured_start.append(kwargs.get("start_from_index"))
        captured_intents.append(kwargs.get("intents"))
        return MagicMock(success=True, results=[], error=None)

    config = TeardownConfig.default()
    config.staleness_threshold_seconds = 1  # any age > 1s is stale -> regenerate
    manager = TeardownManager(state_manager=state_manager, config=config)
    manager._execute_intents = spy_execute

    await manager.resume(deployment_id="test_strat", strategy=strategy, market=market)

    # Regeneration ran (the stored plan was replaced with the fresh 2-intent one).
    assert state.pending_intents_json == json.dumps([i.to_dict() for i in regenerated])
    # The progress counter was reset on the regeneration path.
    assert state.completed_intents == 0
    assert state.current_intent_index == 0
    # Resume starts from index 0 of the NEW plan — no regenerated close skipped.
    assert len(captured_start) == 1
    assert captured_start[0] == 0
    # And it runs against the regenerated 2-intent plan, not the old 3-intent one.
    assert len(captured_intents[0]) == 2


@pytest.mark.asyncio
async def test_resume_stale_regeneration_with_prior_progress_skips_loud_gate():
    """VIB-4842 Codex re-audit P1: the oracle GATE and the resume INDEX must read
    DECOUPLED progress signals.

    This is the entanglement the round-4 reset created. A stale resumable teardown
    that ALREADY made on-chain progress regenerates its plan; the regeneration
    branch resets ``completed_intents = 0`` / ``current_intent_index = 0`` so the
    resume INDEX runs the full regenerated plan from index 0 (round-4 fix). But
    that same reset zeroes the counters the oracle GATE reads. If the GATE re-read
    the live (just-reset) counters it would see 0/0, misclassify this
    partially-unwound run as a genuine fresh start, and FAIL LOUD on an
    unpriceable regenerated close — blocking the remaining risk-reducing intents
    and stranding a partially-unwound position (violates teardown's
    inverted-failure semantics; AGENTS.md §Teardown).

    The fix captures ``had_prior_progress`` at the TOP of resume() — before any
    reset — and routes the GATE through it. Net effect for THIS case: GATE is
    best-effort (no raise) AND resume INDEX == 0 (full new plan runs).
    """
    # The regenerated plan contains an UNPRICEABLE token (ARB). Under the bug the
    # gate would fail loud here because the reset zeroed the progress signal it
    # reads; with the fix the captured prior-progress flag keeps it best-effort.
    market = _FakeMarket({"WETH": Decimal("3400"), "USDC": Decimal("1"), "ETH": Decimal("3400")})

    class _RegenIntent:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return dict(self._payload)

    regenerated = [
        _RegenIntent({"from_token": "ARB", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"}),
        _RegenIntent({"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"}),
    ]
    strategy = _make_strategy(regenerated)

    # Old (stale) plan with REAL prior on-chain progress: 3 intents, 2 completed.
    old_intents_json = json.dumps(
        [
            {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
            {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
            {"from_token": "WETH", "to_token": "USDC", "chain": "arbitrum", "type": "SWAP"},
        ]
    )
    state = _resumable_state(old_intents_json, current_intent_index=2, completed_intents=2, total_intents=3)
    # Force the staleness branch so regeneration (and its counter reset) runs.
    state.updated_at = datetime(2020, 1, 1, tzinfo=UTC)
    state_manager = MagicMock()
    state_manager.get_teardown_state = AsyncMock(return_value=state)
    state_manager.save_teardown_state = AsyncMock()
    state_manager.delete_teardown_state = AsyncMock()

    captured_start = []
    captured_oracle = []

    async def spy_execute(*args, **kwargs):
        captured_start.append(kwargs.get("start_from_index"))
        captured_oracle.append(kwargs.get("price_oracle"))
        return MagicMock(success=True, results=[], error=None)

    config = TeardownConfig.default()
    config.staleness_threshold_seconds = 1  # any age > 1s is stale -> regenerate
    manager = TeardownManager(state_manager=state_manager, config=config)
    manager._execute_intents = spy_execute

    # Must NOT raise — prior progress was captured BEFORE the regeneration reset,
    # so the gate stays best-effort despite the unpriceable regenerated close.
    result = await manager.resume(deployment_id="test_strat", strategy=strategy, market=market)

    assert result is not None
    # The gate did not block the unwind: _execute_intents was reached once.
    assert len(captured_start) == 1
    # Best-effort warm passed a (partial) dict through, not None-by-raise.
    assert captured_oracle[0] is not None
    # Resume INDEX is decoupled from the gate: the reset counters still yield a
    # start of 0 so the FULL regenerated plan runs (round-4 behaviour preserved).
    assert state.completed_intents == 0
    assert state.current_intent_index == 0
    assert captured_start[0] == 0
