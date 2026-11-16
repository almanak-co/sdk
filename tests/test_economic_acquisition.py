"""Pinned accounting inputs preserve Hub identity, fees and dynamic versions."""

from types import SimpleNamespace

import pytest

from almanak.backtest.economic_acquisition import read_rates, read_risk
from almanak.backtest.economic_data import RAY
from almanak.gateway.ethereum import DataError
from tests.test_economic_math import reserve as reserve


def reader_for(reserve):
    responses = {
        "getAddedAssets(uint256)": (1000 * 10**18,),
        "getAddedShares(uint256)": (1000 * 10**18,),
        "getAssetDrawnIndex(uint256)": (RAY,),
        "getAssetDrawnShares(uint256)": (500 * 10**18,),
        "getAssetDrawnRate(uint256)": (RAY // 10,),
        "getAssetPremiumData(uint256)": (100 * 10**18, 100 * 10**18 * RAY),
        "getAssetConfig(uint256)": ("0x" + "11" * 20, 1000, "0x" + "22" * 20, "0x" + "00" * 20),
        "getAssetUnderlyingAndDecimals(uint256)": (
            reserve.source.asset.reference,
            reserve.asset.decimals,
        ),
        "getLiquidationConfig()": (105 * 10**16, 95 * 10**16, 5000),
    }
    calls = []

    def call(target, signature, block, **kwargs):
        assert block == reserve.block
        calls.append((target, signature, kwargs))
        if signature == "getDynamicReserveConfig(uint256,uint32)":
            return (8000 - 100 * kwargs["args"][1], 10500, 1000)
        return responses[signature]

    return SimpleNamespace(chain=reserve.source.chain, call=call, calls=calls, responses=responses)


def test_instantaneous_rates_include_premium_and_fee_dilution(reserve):
    reader = reader_for(reserve)
    rates = read_rates(reader, reserve, reserve.block)
    assert rates.supply_index_ray == rates.borrow_index_ray == RAY
    assert rates.supply_apr_ray == (600 * 10**18 * RAY * 9 // 100) // (1000 * 10**18 + 10**6)
    assert rates.borrow_apr_ray == RAY // 10
    assert rates.source.hub == reserve.source.hub
    assert all(call[0] == reserve.source.hub for call in reader.calls)


@pytest.mark.parametrize("fault", ["chain", "asset", "decimals"])
def test_accounting_rejects_mismatched_identity(reserve, fault):
    reader = reader_for(reserve)
    if fault == "chain":
        reader.chain = reader.chain.model_copy(update={"environment": "another"})
    else:
        original = reader.responses["getAssetUnderlyingAndDecimals(uint256)"]
        reader.responses["getAssetUnderlyingAndDecimals(uint256)"] = (
            ("0x" + "ff" * 20, original[1]) if fault == "asset" else (original[0], 6)
        )
    with pytest.raises(DataError):
        read_rates(reader, reserve, reserve.block)


def test_risk_retains_all_dynamic_versions(reserve, monkeypatch):
    reserve = reserve.model_copy(update={"dynamic_config_key": 2})
    monkeypatch.setattr("almanak.backtest.economic_acquisition.read_reserve", lambda *args: reserve)
    reader = reader_for(reserve)
    risk = read_risk(reader, reserve.source, reserve.block)
    assert [c.collateral_factor_bps for c in risk.configs] == [8000, 7900, 7800]
    assert risk.config(0).collateral_factor_bps != risk.config(2).collateral_factor_bps
    assert risk.liquidation.target_health_factor == 105 * 10**16
