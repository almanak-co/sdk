from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import normalize_token_key
from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel, PnLBacktester
from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL
from tests.unit.backtesting.pnl._mocks import MockDataProvider

BASE_CBBTC = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
BASE_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
PLATFORM_NATIVE_ALIAS = "0x0000000000000000000000000000000000000000"


def _token_funding() -> list[dict[str, str]]:
    return [
        {
            "symbol": "cbBTC",
            "address": BASE_CBBTC,
            "chain": "base",
            "amount": "200",
            "amount_type": "usd",
        },
        {
            "symbol": "USDC",
            "address": BASE_USDC,
            "chain": "base",
            "amount": "200",
            "amount_type": "usd",
        },
    ]


def _config(start: datetime) -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(hours=1),
        interval_seconds=3600,
        token_funding=_token_funding(),
        chain="base",
        tokens=["cbBTC", "USDC"],
        include_gas_costs=False,
        inclusion_delay_blocks=0,
        preflight_validation=False,
    )


def _backtester() -> PnLBacktester:
    provider = MockDataProvider(
        base_prices={
            normalize_token_key("base", BASE_CBBTC): Decimal("100000"),
            normalize_token_key("base", BASE_USDC): Decimal("1"),
        }
    )
    return PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
    )


class RecordingHoldStrategy:
    deployment_id = "recording_hold"

    def __init__(self) -> None:
        self.first_cbbtc_balance: Decimal | None = None
        self.first_usdc_balance: Decimal | None = None

    def decide(self, market: Any) -> None:
        if self.first_cbbtc_balance is None:
            self.first_cbbtc_balance = market.balance(BASE_CBBTC).balance
            self.first_usdc_balance = market.balance(BASE_USDC).balance
        return None


class RecordingNativeHoldStrategy:
    deployment_id = "recording_native_hold"

    def __init__(self) -> None:
        self.first_native_balance: Decimal | None = None

    def decide(self, market: Any) -> None:
        if self.first_native_balance is None:
            self.first_native_balance = market.balance("ETH").balance
        return None


@dataclass
class _SwapIntent:
    intent_type: str = "SWAP"
    from_token: str = BASE_CBBTC
    to_token: str = BASE_USDC
    amount_usd: Decimal = Decimal("100")
    protocol: str = "uniswap_v3"


class SellOnceStrategy:
    deployment_id = "sell_once"

    def __init__(self) -> None:
        self._decided = False

    def decide(self, _market: Any) -> _SwapIntent | None:
        if self._decided:
            return None
        self._decided = True
        return _SwapIntent()


@pytest.mark.asyncio
async def test_first_snapshot_exposes_exact_funded_token_addresses() -> None:
    start = datetime(2026, 6, 1)
    strategy = RecordingHoldStrategy()

    result = await _backtester().backtest(strategy, _config(start))

    assert result.initial_portfolio_value_usd == Decimal("400")
    assert strategy.first_cbbtc_balance == Decimal("0.002")
    assert strategy.first_usdc_balance == Decimal("200")
    assert result.final_capital_usd == Decimal("400.000")


@pytest.mark.asyncio
async def test_funded_address_native_token_can_be_sold_without_insufficient_balance() -> None:
    start = datetime(2026, 6, 1)

    result = await _backtester().backtest(SellOnceStrategy(), _config(start))

    assert result.success
    assert result.trades
    assert result.trades[0].success
    assert "insufficient" not in str(result.trades[0].metadata).lower()


@pytest.mark.parametrize("chain", ["base", "arbitrum"])
@pytest.mark.asyncio
async def test_platform_zero_address_native_funding_reaches_simulation_loop(chain: str) -> None:
    """Base/Aave and Arbitrum/GMX input shape no longer dies before decide()."""
    start = datetime(2026, 6, 1)
    native = normalize_token_key(chain, NATIVE_SENTINEL)
    other_chain = "arbitrum" if chain == "base" else "base"
    declared_funding = [
        {
            "symbol": "ETH",
            "address": PLATFORM_NATIVE_ALIAS,
            "amount": "300",
            "amount_type": "usd",
        },
        {
            "symbol": "USDC",
            "address": BASE_USDC,
            "chain": other_chain,
            "amount": "1",
            "amount_type": "usd",
        },
    ]
    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(hours=1),
        interval_seconds=3600,
        token_funding=deepcopy(declared_funding),
        chain=chain,
        tokens=[native],  # type: ignore[list-item]  # engine-native TokenRef input
        include_gas_costs=False,
        inclusion_delay_blocks=0,
        preflight_validation=False,
    )
    provider = MockDataProvider(base_prices={native: Decimal("3000")})
    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        token_addresses={"ETH": native},
    )
    strategy = RecordingNativeHoldStrategy()
    declared_hash = config.calculate_config_hash()

    result = await backtester.backtest(strategy, config)

    assert result.success
    assert result.initial_portfolio_value_usd == Decimal("300")
    assert strategy.first_native_balance == Decimal("0.1")
    assert config.token_funding == declared_funding
    assert config.calculate_config_hash() == declared_hash
    assert result.config_hash == declared_hash
    assert result.config["token_funding"] == declared_funding


POLYGON_NATIVE_PRECOMPILE = "0x0000000000000000000000000000000000001010"
POLYGON_USDC = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"


class RecordingPolSellOnceStrategy:
    """Read the funded POL balance by symbol, then sell some native POL once."""

    deployment_id = "recording_pol_sell_once"

    def __init__(self) -> None:
        self.first_pol_balance: Decimal | None = None
        self._decided = False

    def decide(self, market: Any) -> _SwapIntent | None:
        if self.first_pol_balance is None:
            self.first_pol_balance = market.balance("POL").balance
        if self._decided:
            return None
        self._decided = True
        return _SwapIntent(from_token="POL", to_token=POLYGON_USDC, amount_usd=Decimal("10"))


@pytest.mark.asyncio
async def test_polygon_native_precompile_funding_is_visible_to_symbol_reads_and_swaps() -> None:
    """ALM-3058: funding POL by ``0x...1010`` must not leave ``balance("POL")`` at 0.

    Before the fold the wallet was keyed by the precompile address while every
    symbol-form read resolved POL to the native sentinel, so the funded
    balance was invisible and each native-POL SWAP was rejected as
    short-from-nothing.
    """
    start = datetime(2026, 6, 1)
    native = normalize_token_key("polygon", NATIVE_SENTINEL)
    usdc = normalize_token_key("polygon", POLYGON_USDC)
    declared_funding = [
        {
            "symbol": "POL",
            "address": POLYGON_NATIVE_PRECOMPILE,
            "chain": "polygon",
            "amount": "100",
            "amount_type": "token",
        }
    ]
    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(hours=2),
        interval_seconds=3600,
        token_funding=deepcopy(declared_funding),
        chain="polygon",
        tokens=[native, usdc],  # type: ignore[list-item]  # engine-native TokenRef input
        include_gas_costs=False,
        inclusion_delay_blocks=0,
        preflight_validation=False,
    )
    provider = MockDataProvider(base_prices={native: Decimal("0.5"), usdc: Decimal("1")})
    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        token_addresses={"POL": native, "USDC": usdc},
    )
    strategy = RecordingPolSellOnceStrategy()

    result = await backtester.backtest(strategy, config)

    assert result.success
    assert result.initial_portfolio_value_usd == Decimal("50")
    assert strategy.first_pol_balance == Decimal("100")
    assert result.trades
    assert result.trades[0].success, result.trades[0].error
    assert "insufficient" not in str(result.trades[0].metadata).lower()
    assert config.token_funding == declared_funding


class RecordingPolProbeStrategy:
    """Read the funded POL under every spelling a strategy may use, then sell once."""

    deployment_id = "recording_pol_probe"

    def __init__(self) -> None:
        self.reads: dict[str, Decimal] = {}
        self._decided = False

    def decide(self, market: Any) -> _SwapIntent | None:
        if not self.reads:
            self.reads = {
                "POL": market.balance("POL").balance,
                "POL@polygon": market.balance("POL", chain="polygon").balance,
                "MATIC": market.balance("MATIC").balance,
                "alias": market.balance(POLYGON_NATIVE_PRECOMPILE).balance,
                "sentinel": market.balance(NATIVE_SENTINEL).balance,
            }
        if self._decided:
            return None
        self._decided = True
        return _SwapIntent(from_token="POL", to_token=POLYGON_USDC, amount_usd=Decimal("10"))


@pytest.mark.asyncio
async def test_polygon_native_precompile_funding_without_caller_symbol_map() -> None:
    """The platform runner registers no symbol map of its own, so the engine must
    serve the funded POL under the alias address, the accepted symbol, and the
    canonical symbol alike, and a native-POL SWAP must fill."""
    start = datetime(2026, 6, 1)
    native = normalize_token_key("polygon", NATIVE_SENTINEL)
    usdc = normalize_token_key("polygon", POLYGON_USDC)
    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(hours=2),
        interval_seconds=3600,
        token_funding=[
            {
                "symbol": "POL",
                "address": POLYGON_NATIVE_PRECOMPILE,
                "chain": "polygon",
                "amount": "100",
                "amount_type": "token",
            },
            {"symbol": "USDC", "address": POLYGON_USDC, "chain": "polygon", "amount": "50", "amount_type": "token"},
        ],
        chain="polygon",
        tokens=[native, usdc],  # type: ignore[list-item]  # engine-native TokenRef input
        include_gas_costs=False,
        inclusion_delay_blocks=0,
        preflight_validation=False,
    )
    provider = MockDataProvider(base_prices={native: Decimal("0.5"), usdc: Decimal("1")})
    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
    )
    strategy = RecordingPolProbeStrategy()

    result = await backtester.backtest(strategy, config)

    assert result.success, result.error
    assert strategy.reads == {key: Decimal("100") for key in ("POL", "POL@polygon", "MATIC", "alias", "sentinel")}
    assert [trade.success for trade in result.trades] == [True]
    assert not result.decision_input_failures
    assert normalize_token_key("polygon", POLYGON_NATIVE_PRECOMPILE) == native
    assert normalize_token_key("arbitrum", POLYGON_NATIVE_PRECOMPILE) == ("arbitrum", POLYGON_NATIVE_PRECOMPILE)
