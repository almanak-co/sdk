"""Behavioral tests for ``Web3GasOracle.get_gas_price``.

Covers every branch of the fetch-and-assemble path with the RPC helpers
(`_fetch_gas_fees`, `_fetch_l1_data_cost`, `_calculate_usd_cost`) mocked at
the instance seam — no network, no real AsyncWeb3 calls:

- L1 chain: gwei conversion, no L1 components, USD cost wired through
- L2 chain (in ``L2_CHAINS``): L1 components populated and the data-cost
  converted to wei for USD estimation; (None, None) L1 result -> 0 wei
- DataSourceError from a helper re-raised unchanged
- TimeoutError -> DataSourceUnavailable with retry_after
- generic exception -> DataSourceError
- unconfigured chain -> DataSourceUnavailable from _get_web3
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.defi.gas import L2_CHAINS, GasPrice, Web3GasOracle
from almanak.framework.data.interfaces import DataSourceError, DataSourceUnavailable

GWEI = 10**9


def _oracle() -> Web3GasOracle:
    return Web3GasOracle(
        rpc_urls={"Ethereum": "http://rpc.invalid/eth", "arbitrum": "http://rpc.invalid/arb"}
    )


def _run(coro):
    return asyncio.run(coro)


class TestGetGasPriceL1:
    def test_l1_chain_returns_converted_fees(self) -> None:
        oracle = _oracle()
        assert "ethereum" not in L2_CHAINS  # guard: this is the non-L2 path

        with (
            patch.object(oracle, "_get_web3", return_value=MagicMock()),
            patch.object(oracle, "_fetch_gas_fees", new=AsyncMock(return_value=(30 * GWEI, 2 * GWEI))),
            patch.object(oracle, "_fetch_l1_data_cost", new=AsyncMock()) as l1_mock,
            patch.object(oracle, "_calculate_usd_cost", new=AsyncMock(return_value=Decimal("5.25"))) as usd_mock,
        ):
            price = _run(oracle.get_gas_price("Ethereum"))

        assert isinstance(price, GasPrice)
        assert price.chain == "ethereum"  # normalized to lowercase
        assert price.base_fee_gwei == Decimal("30")
        assert price.priority_fee_gwei == Decimal("2")
        assert price.max_fee_gwei == Decimal("32")
        assert price.l1_base_fee_gwei is None
        assert price.l1_data_cost_gwei is None
        assert price.estimated_cost_usd == Decimal("5.25")
        l1_mock.assert_not_awaited()
        usd_mock.assert_awaited_once_with(max_fee_wei=32 * GWEI, l1_data_cost_wei=0)


class TestGetGasPriceL2:
    def test_l2_chain_includes_l1_components(self) -> None:
        oracle = _oracle()
        assert "arbitrum" in L2_CHAINS

        with (
            patch.object(oracle, "_get_web3", return_value=MagicMock()),
            patch.object(oracle, "_fetch_gas_fees", new=AsyncMock(return_value=(1 * GWEI, 0))),
            patch.object(
                oracle,
                "_fetch_l1_data_cost",
                new=AsyncMock(return_value=(Decimal("25"), Decimal("12"))),
            ),
            patch.object(oracle, "_calculate_usd_cost", new=AsyncMock(return_value=Decimal("0.42"))) as usd_mock,
        ):
            price = _run(oracle.get_gas_price("arbitrum"))

        assert price.chain == "arbitrum"
        assert price.l1_base_fee_gwei == Decimal("25")
        assert price.l1_data_cost_gwei == Decimal("12")
        assert price.estimated_cost_usd == Decimal("0.42")
        # 12 gwei of L1 data cost forwarded to USD estimation in wei.
        usd_mock.assert_awaited_once_with(max_fee_wei=1 * GWEI, l1_data_cost_wei=12 * GWEI)

    def test_l2_chain_without_l1_data_uses_zero_wei(self) -> None:
        oracle = _oracle()

        with (
            patch.object(oracle, "_get_web3", return_value=MagicMock()),
            patch.object(oracle, "_fetch_gas_fees", new=AsyncMock(return_value=(1 * GWEI, 1 * GWEI))),
            patch.object(oracle, "_fetch_l1_data_cost", new=AsyncMock(return_value=(None, None))),
            patch.object(oracle, "_calculate_usd_cost", new=AsyncMock(return_value=Decimal("0"))) as usd_mock,
        ):
            price = _run(oracle.get_gas_price("arbitrum"))

        assert price.l1_base_fee_gwei is None
        assert price.l1_data_cost_gwei is None
        usd_mock.assert_awaited_once_with(max_fee_wei=2 * GWEI, l1_data_cost_wei=0)


class TestGetGasPriceErrors:
    def test_data_source_error_reraised_unchanged(self) -> None:
        oracle = _oracle()
        original = DataSourceError("upstream broke")

        with (
            patch.object(oracle, "_get_web3", return_value=MagicMock()),
            patch.object(oracle, "_fetch_gas_fees", new=AsyncMock(side_effect=original)),
        ):
            with pytest.raises(DataSourceError) as excinfo:
                _run(oracle.get_gas_price("ethereum"))

        assert excinfo.value is original

    def test_timeout_maps_to_unavailable_with_retry_after(self) -> None:
        oracle = _oracle()

        with (
            patch.object(oracle, "_get_web3", return_value=MagicMock()),
            patch.object(oracle, "_fetch_gas_fees", new=AsyncMock(side_effect=TimeoutError())),
        ):
            with pytest.raises(DataSourceUnavailable) as excinfo:
                _run(oracle.get_gas_price("ethereum"))

        assert excinfo.value.retry_after == 5.0
        assert "RPC timeout for chain 'ethereum'" in str(excinfo.value)

    def test_generic_error_wrapped_as_data_source_error(self) -> None:
        oracle = _oracle()

        with (
            patch.object(oracle, "_get_web3", return_value=MagicMock()),
            patch.object(oracle, "_fetch_gas_fees", new=AsyncMock(side_effect=ValueError("bad json"))),
        ):
            with pytest.raises(DataSourceError, match="Failed to fetch gas price for chain 'ethereum'"):
                _run(oracle.get_gas_price("ethereum"))

    def test_unconfigured_chain_raises_unavailable(self) -> None:
        oracle = _oracle()

        with pytest.raises(DataSourceUnavailable, match="No RPC URL configured"):
            _run(oracle.get_gas_price("polygon"))
