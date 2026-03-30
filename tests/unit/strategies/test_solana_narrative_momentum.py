"""Tests for Solana Narrative Momentum strategy."""

import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.dexscreener.models import (
    DexLiquidity,
    DexPair,
    DexPriceChange,
    DexToken,
    DexTxnCounts,
    DexTxns,
    DexVolume,
)


def _make_pair(
    dex_id: str = "meteora",
    base_symbol: str = "BONK",
    base_address: str = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    quote_symbol: str = "SOL",
    quote_address: str = "So11111111111111111111111111111111111111112",
    price_usd: str = "0.00001234",
    price_native: str = "0.001",
    liquidity_usd: float = 500_000,
    volume_h24: float = 5_000_000,
    volume_h1: float = 300_000,
    h1_change: float = 5.0,
    buy_ratio: float = 0.65,
    pair_address: str = "pool_abc123",
    age_hours: float = 24.0,
) -> DexPair:
    """Create a DexPair test fixture."""
    created_ms = int((time.time() - age_hours * 3600) * 1000) if age_hours else None
    return DexPair(
        chain_id="solana",
        dex_id=dex_id,
        pair_address=pair_address,
        url=f"https://dexscreener.com/solana/{pair_address}",
        base_token=DexToken(address=base_address, name=base_symbol, symbol=base_symbol),
        quote_token=DexToken(address=quote_address, name=quote_symbol, symbol=quote_symbol),
        price_native=price_native,
        price_usd=price_usd,
        txns=DexTxns(
            m5=DexTxnCounts(buys=10, sells=5),
            h1=DexTxnCounts(
                buys=int(100 * buy_ratio),
                sells=int(100 * (1 - buy_ratio)),
            ),
            h6=DexTxnCounts(buys=300, sells=200),
            h24=DexTxnCounts(buys=1000, sells=800),
        ),
        volume=DexVolume(m5=50_000, h1=volume_h1, h6=1_500_000, h24=volume_h24),
        price_change=DexPriceChange(m5=0.5, h1=h1_change, h6=8.0, h24=15.0),
        liquidity=DexLiquidity(usd=liquidity_usd, base=0, quote=0),
        fdv=10_000_000,
        market_cap=8_000_000,
        pair_created_at=created_ms,
    )


# ---------------------------------------------------------------------------
# Scoring function
# ---------------------------------------------------------------------------


class TestScorePoolCandidate:
    def test_high_vol_tvl_scores_high(self):
        from almanak.demo_strategies.solana_narrative_momentum.strategy import _score_pool_candidate

        pair = _make_pair(volume_h24=10_000_000, liquidity_usd=500_000)
        # vol/tvl = 20, buy_ratio = 0.65, h1_change = 5.0, liq = 500k
        score = _score_pool_candidate(pair, min_vol_tvl=10.0)
        assert score > 40  # Should score well across all factors

    def test_low_vol_tvl_scores_zero_for_vol(self):
        from almanak.demo_strategies.solana_narrative_momentum.strategy import _score_pool_candidate

        pair = _make_pair(volume_h24=100_000, liquidity_usd=500_000)
        # vol/tvl = 0.2, below min_vol_tvl=10
        score = _score_pool_candidate(pair, min_vol_tvl=10.0)
        # Should still get points from other factors, but not vol/tvl
        assert score > 0  # buy pressure + momentum + liquidity
        assert score < 40  # but not high overall

    def test_zero_liquidity_safe(self):
        from almanak.demo_strategies.solana_narrative_momentum.strategy import _score_pool_candidate

        pair = _make_pair(liquidity_usd=0)
        score = _score_pool_candidate(pair, min_vol_tvl=10.0)
        assert score >= 0  # Should not crash

    def test_neutral_buy_ratio_low_score(self):
        from almanak.demo_strategies.solana_narrative_momentum.strategy import _score_pool_candidate

        pair = _make_pair(buy_ratio=0.50, h1_change=0, liquidity_usd=50_000, volume_h24=100)
        score = _score_pool_candidate(pair, min_vol_tvl=10.0)
        assert score == 0  # All factors at minimum

    def test_high_liquidity_bonus(self):
        from almanak.demo_strategies.solana_narrative_momentum.strategy import _score_pool_candidate

        pair_low = _make_pair(liquidity_usd=50_000, buy_ratio=0.5, h1_change=0, volume_h24=100)
        pair_high = _make_pair(liquidity_usd=1_500_000, buy_ratio=0.5, h1_change=0, volume_h24=100)
        score_low = _score_pool_candidate(pair_low, min_vol_tvl=10.0)
        score_high = _score_pool_candidate(pair_high, min_vol_tvl=10.0)
        assert score_high > score_low


# ---------------------------------------------------------------------------
# Strategy class
# ---------------------------------------------------------------------------


def _make_strategy(**config_overrides):
    """Create a strategy instance with mock dependencies."""
    from almanak.demo_strategies.solana_narrative_momentum.strategy import SolanaNarrativeMomentumStrategy

    config = {
        "chain": "solana",
        "base_token": "USDC",
        "lp_amount_base": "0.5",
        "lp_amount_quote": "0.5",
        "max_slippage_pct": 3.0,
        "min_liquidity_usd": 100_000,
        "min_volume_h24": 1_000_000,
        "min_vol_tvl_ratio": 10.0,
        "min_buy_ratio": 0.52,
        "min_h1_change_pct": 1.0,
        "min_age_hours": 2,
        "max_age_hours": 168,
        "volume_drop_exit_pct": 50.0,
        "preferred_dex": "meteora",
        "range_width_pct": 30.0,
    }
    config.update(config_overrides)

    strategy = SolanaNarrativeMomentumStrategy.__new__(SolanaNarrativeMomentumStrategy)
    strategy.config = config  # StrategyBase sets self.config = config
    strategy.state = {}  # Runner sets self.state as a dict
    strategy._position = None
    strategy._entry_volume_h24 = 0.0
    strategy._strategy_id = "test-strategy-123"
    strategy._wallet_address = "test_wallet"
    return strategy


class TestScreenAndLP:
    """Test the screening + LP entry flow."""

    @pytest.mark.asyncio
    async def test_no_candidates_returns_hold(self):
        strategy = _make_strategy()

        mock_client = AsyncMock()
        mock_client.get_solana_meme_candidates = AsyncMock(return_value=[])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.demo_strategies.solana_narrative_momentum.strategy.DexScreenerClient",
            return_value=mock_client,
        ):
            result = await strategy._screen_and_lp()

        assert result.intent_type.value == "HOLD"
        assert "No Solana pool candidates" in result.reason

    @pytest.mark.asyncio
    async def test_no_matching_dex_returns_hold(self):
        """Candidates exist but none on Raydium/Meteora."""
        strategy = _make_strategy()

        # Pair on unsupported DEX
        pair = _make_pair(dex_id="orca", volume_h24=5_000_000, liquidity_usd=200_000)

        mock_client = AsyncMock()
        mock_client.get_solana_meme_candidates = AsyncMock(return_value=[pair])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.demo_strategies.solana_narrative_momentum.strategy.DexScreenerClient",
            return_value=mock_client,
        ):
            result = await strategy._screen_and_lp()

        assert result.intent_type.value == "HOLD"
        assert "No pools with sufficient momentum" in result.reason

    @pytest.mark.asyncio
    async def test_good_candidate_returns_lp_open(self):
        """A Meteora pool passing all filters should produce LPOpenIntent."""
        strategy = _make_strategy()

        pair = _make_pair(
            dex_id="meteora",
            volume_h24=5_000_000,
            liquidity_usd=300_000,
            h1_change=3.0,
            buy_ratio=0.60,
            price_usd="150.50",
            pair_address="pool_meteora_abc",
        )

        mock_client = AsyncMock()
        mock_client.get_solana_meme_candidates = AsyncMock(return_value=[pair])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.demo_strategies.solana_narrative_momentum.strategy.DexScreenerClient",
            return_value=mock_client,
        ):
            result = await strategy._screen_and_lp()

        assert result.intent_type.value == "LP_OPEN"
        assert result.protocol == "meteora_dlmm"
        assert result.pool == "pool_meteora_abc"
        assert result.amount0 == Decimal("0.5")
        assert result.amount1 == Decimal("0.5")
        assert result.range_lower > 0
        assert result.range_upper > result.range_lower

    @pytest.mark.asyncio
    async def test_raydium_pool_uses_raydium_protocol(self):
        """A Raydium pool should use raydium_clmm protocol."""
        strategy = _make_strategy(preferred_dex="raydium")

        pair = _make_pair(
            dex_id="raydium",
            volume_h24=8_000_000,
            liquidity_usd=400_000,
            h1_change=2.0,
            buy_ratio=0.58,
            price_usd="0.05",
            pair_address="pool_raydium_xyz",
        )

        mock_client = AsyncMock()
        mock_client.get_solana_meme_candidates = AsyncMock(return_value=[pair])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.demo_strategies.solana_narrative_momentum.strategy.DexScreenerClient",
            return_value=mock_client,
        ):
            result = await strategy._screen_and_lp()

        assert result.intent_type.value == "LP_OPEN"
        assert result.protocol == "raydium_clmm"

    @pytest.mark.asyncio
    async def test_prefers_configured_dex(self):
        """When multiple DEXs have candidates, prefer the configured one."""
        strategy = _make_strategy(preferred_dex="raydium")

        meteora_pair = _make_pair(
            dex_id="meteora",
            volume_h24=10_000_000,
            liquidity_usd=500_000,
            h1_change=5.0,
            buy_ratio=0.70,
            pair_address="pool_meteora_1",
        )
        raydium_pair = _make_pair(
            dex_id="raydium",
            volume_h24=5_000_000,
            liquidity_usd=300_000,
            h1_change=2.0,
            buy_ratio=0.55,
            pair_address="pool_raydium_1",
        )

        mock_client = AsyncMock()
        mock_client.get_solana_meme_candidates = AsyncMock(return_value=[meteora_pair, raydium_pair])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.demo_strategies.solana_narrative_momentum.strategy.DexScreenerClient",
            return_value=mock_client,
        ):
            result = await strategy._screen_and_lp()

        assert result.intent_type.value == "LP_OPEN"
        assert result.protocol == "raydium_clmm"
        assert result.pool == "pool_raydium_1"

    @pytest.mark.asyncio
    async def test_range_width_calculation(self):
        """Price range should be +/- range_width_pct/2 around current price."""
        strategy = _make_strategy(range_width_pct=20.0)

        pair = _make_pair(
            dex_id="meteora",
            volume_h24=5_000_000,
            liquidity_usd=200_000,
            h1_change=2.0,
            buy_ratio=0.55,
            price_usd="100.0",
            price_native="100.0",
            pair_address="pool_range_test",
        )

        mock_client = AsyncMock()
        mock_client.get_solana_meme_candidates = AsyncMock(return_value=[pair])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.demo_strategies.solana_narrative_momentum.strategy.DexScreenerClient",
            return_value=mock_client,
        ):
            result = await strategy._screen_and_lp()

        # 20% width => +/- 10% => range [90, 110]
        assert result.range_lower == Decimal("90.0")
        assert result.range_upper == Decimal("110.0")


class TestManagePosition:
    """Test the position management / exit flow."""

    @pytest.mark.asyncio
    async def test_volume_drop_triggers_exit(self):
        """When volume drops >50%, should exit position."""
        strategy = _make_strategy()
        strategy._position = {
            "pair_address": "pool_abc",
            "dex_id": "meteora",
            "protocol": "meteora_dlmm",
            "base_token": "BONK",
            "base_token_address": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
            "quote_token": "SOL",
            "quote_token_address": "So11111111111111111111111111111111111111112",
            "entry_price_usd": 0.00001234,
            "entry_volume_h24": 5_000_000,
            "entry_time": "2026-03-17T00:00:00+00:00",
            "pool": "pool_abc",
        }
        strategy.state["position_id"] = "pos_123"

        # Current volume is down 60%
        current_pair = _make_pair(volume_h24=2_000_000, pair_address="pool_abc")

        mock_client = AsyncMock()
        mock_client.get_pair = AsyncMock(return_value=current_pair)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.demo_strategies.solana_narrative_momentum.strategy.DexScreenerClient",
            return_value=mock_client,
        ):
            result = await strategy._manage_position()

        assert result.intent_type.value == "LP_CLOSE"
        assert result.position_id == "pos_123"
        assert result.pool == "pool_abc"
        assert result.collect_fees is True

    @pytest.mark.asyncio
    async def test_volume_stable_holds(self):
        """When volume is stable, should hold."""
        strategy = _make_strategy()
        strategy._position = {
            "pair_address": "pool_abc",
            "dex_id": "meteora",
            "protocol": "meteora_dlmm",
            "base_token": "BONK",
            "base_token_address": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
            "quote_token": "SOL",
            "quote_token_address": "So11111111111111111111111111111111111111112",
            "entry_price_usd": 0.00001234,
            "entry_volume_h24": 5_000_000,
            "entry_time": "2026-03-17T00:00:00+00:00",
            "pool": "pool_abc",
        }

        # Volume slightly up
        current_pair = _make_pair(volume_h24=5_500_000, pair_address="pool_abc")

        mock_client = AsyncMock()
        mock_client.get_pair = AsyncMock(return_value=current_pair)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.demo_strategies.solana_narrative_momentum.strategy.DexScreenerClient",
            return_value=mock_client,
        ):
            result = await strategy._manage_position()

        assert result.intent_type.value == "HOLD"

    @pytest.mark.asyncio
    async def test_spot_position_exits_via_swap(self):
        """Spot positions should exit with a swap intent."""
        strategy = _make_strategy()
        strategy._position = {
            "pair_address": "pool_abc",
            "dex_id": "orca",
            "protocol": "jupiter",
            "base_token": "BONK",
            "base_token_address": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
            "quote_token": "SOL",
            "quote_token_address": "So11111111111111111111111111111111111111112",
            "entry_price_usd": 0.00001234,
            "entry_volume_h24": 5_000_000,
            "entry_time": "2026-03-17T00:00:00+00:00",
            "position_type": "spot",
        }

        # Volume dropped 70%
        current_pair = _make_pair(volume_h24=1_500_000, pair_address="pool_abc")

        mock_client = AsyncMock()
        mock_client.get_pair = AsyncMock(return_value=current_pair)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.demo_strategies.solana_narrative_momentum.strategy.DexScreenerClient",
            return_value=mock_client,
        ):
            result = await strategy._manage_position()

        assert result.intent_type.value == "SWAP"
        assert result.from_token == "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
        assert result.to_token == "USDC"

    @pytest.mark.asyncio
    async def test_cannot_fetch_volume_holds(self):
        """When DexScreener returns None, hold."""
        strategy = _make_strategy()
        strategy._position = {
            "pair_address": "pool_abc",
            "dex_id": "meteora",
            "protocol": "meteora_dlmm",
            "base_token": "BONK",
            "base_token_address": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
            "quote_token": "SOL",
            "quote_token_address": "So11111111111111111111111111111111111111112",
            "entry_price_usd": 0.00001234,
            "entry_volume_h24": 5_000_000,
            "entry_time": "2026-03-17T00:00:00+00:00",
            "pool": "pool_abc",
        }

        mock_client = AsyncMock()
        mock_client.get_pair = AsyncMock(return_value=None)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.demo_strategies.solana_narrative_momentum.strategy.DexScreenerClient",
            return_value=mock_client,
        ):
            result = await strategy._manage_position()

        assert result.intent_type.value == "HOLD"


class TestTeardown:
    def test_supports_teardown(self):
        strategy = _make_strategy()
        assert strategy.supports_teardown() is True

    def test_no_position_empty_teardown(self):
        strategy = _make_strategy()
        strategy._position = None
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0
        assert strategy.generate_teardown_intents(mode=MagicMock()) == []

    def test_lp_position_teardown(self):
        strategy = _make_strategy()
        strategy._position = {
            "pool": "pool_abc",
            "protocol": "meteora_dlmm",
            "base_token": "BONK",
            "quote_token": "SOL",
        }
        strategy.state["position_id"] = "pos_123"

        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].position_id == "pos_123"
        assert summary.positions[0].protocol == "meteora_dlmm"

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"
        assert intents[0].position_id == "pos_123"

    def test_spot_position_teardown(self):
        strategy = _make_strategy()
        strategy._position = {
            "position_type": "spot",
            "protocol": "jupiter",
            "base_token": "BONK",
            "base_token_address": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
        }

        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].position_id == "spot_BONK"
        assert summary.positions[0].protocol == "jupiter"

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(mode=TeardownMode.HARD)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "SWAP"
        assert intents[0].max_slippage == Decimal("0.05")


class TestStatePersistence:
    def test_get_persistent_state(self):
        strategy = _make_strategy()
        strategy._position = {"pool": "abc"}
        strategy._entry_volume_h24 = 1_000_000
        strategy.state["position_id"] = "pos_42"

        state = strategy.get_persistent_state()
        assert state["position"] == {"pool": "abc"}
        assert state["entry_volume_h24"] == 1_000_000
        assert state["position_id"] == "pos_42"

    def test_load_persistent_state(self):
        strategy = _make_strategy()
        strategy.load_persistent_state({
            "position": {"pool": "xyz"},
            "entry_volume_h24": 2_000_000,
            "position_id": "pos_99",
            "pool": "xyz",
            "protocol": "meteora_dlmm",
        })
        assert strategy._position == {"pool": "xyz"}
        assert strategy._entry_volume_h24 == 2_000_000
        assert strategy.state["position_id"] == "pos_99"
        assert strategy.state["pool"] == "xyz"
        assert strategy.state["protocol"] == "meteora_dlmm"

    def test_load_empty_state(self):
        strategy = _make_strategy()
        strategy.load_persistent_state({})
        assert strategy._position is None
        assert strategy._entry_volume_h24 == 0.0


class TestOnIntentExecuted:
    def test_tracks_lp_position(self):
        strategy = _make_strategy()

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_OPEN"
        mock_intent.pool = "pool_abc"
        mock_intent.protocol = "meteora_dlmm"

        mock_result = MagicMock()
        mock_result.position_id = "pos_new_456"

        strategy.on_intent_executed(mock_intent, success=True, result=mock_result)

        assert strategy.state["position_id"] == "pos_new_456"
        assert strategy.state["pool"] == "pool_abc"
        assert strategy.state["protocol"] == "meteora_dlmm"

    def test_ignores_failed_execution(self):
        strategy = _make_strategy()

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_OPEN"

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert "position_id" not in strategy.state

    def test_swap_clears_position_on_success(self):
        """Successful exit SWAP should clear position state."""
        strategy = _make_strategy()
        strategy._position = {"position_type": "spot", "base_token": "BONK"}

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"

        strategy.on_intent_executed(mock_intent, success=True, result=MagicMock())
        assert strategy._position is None
        assert strategy._entry_volume_h24 == 0.0

    def test_swap_without_position_is_noop(self):
        """SWAP when no position is set should not crash."""
        strategy = _make_strategy()

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"

        strategy.on_intent_executed(mock_intent, success=True, result=MagicMock())

        assert "position_id" not in strategy.state
