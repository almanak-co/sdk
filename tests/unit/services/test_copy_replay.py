"""Tests for deterministic copy replay harness."""

from pathlib import Path

from almanak.framework.services.copy_trading_models import CopyTradingConfigV2
from almanak.framework.testing.copy_replay import CopyReplayRunner


def _make_config() -> CopyTradingConfigV2:
    return CopyTradingConfigV2.from_config(
        {
            "leaders": [{"address": "0x489ee077994B6658eFaCA1507F1FBB620B9308aa", "chain": "arbitrum"}],
            "global_policy": {"action_types": ["SWAP"], "protocols": ["uniswap_v3"], "tokens": ["USDC", "WETH"]},
            "sizing": {"mode": "fixed_usd", "fixed_usd": "100"},
            "risk": {
                "max_trade_usd": "1000",
                "min_trade_usd": "10",
                "max_daily_notional_usd": "10000",
                "max_open_positions": 10,
                "max_slippage": "0.01",
            },
        }
    )


def test_replay_runner_loads_and_replays_fixture() -> None:
    fixture = Path("tests/fixtures/copy_trading/sample_replay.jsonl")
    runner = CopyReplayRunner(config=_make_config())

    result = runner.run(fixture, shadow=True)

    assert result["signals_loaded"] == 1
    assert result["decisions_made"] == 1
    assert result["approved"] == 1
    assert result["mapped_intents"] == 1
