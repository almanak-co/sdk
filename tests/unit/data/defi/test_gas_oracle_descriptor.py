"""Tests for L2 fee-oracle descriptor integration in gas.py (Plan 026).

Covers:
- GasProfile descriptor validation (kind/address constraints).
- Derivation parity: registry-derived L2_CHAINS and L2_GAS_ORACLE_ADDRESSES
  equal the historical hard-coded literals.
- _fetch_l1_data_cost dispatch routing: kind → correct fetcher for all three
  chains; non-L2 chain returns (None, None).
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.core.chains._descriptor import KNOWN_L1_FEE_ORACLE_KINDS, GasProfile
from almanak.framework.data.defi.gas import (
    L2_CHAINS,
    L2_GAS_ORACLE_ADDRESSES,
    Web3GasOracle,
)


# ---------------------------------------------------------------------------
# GasProfile descriptor validation tests
# ---------------------------------------------------------------------------


class TestGasProfileL1OracleValidation:
    """Descriptor-level validation for the two new GasProfile fields."""

    def test_valid_arbitrum_kind_and_address_accepted(self) -> None:
        profile = GasProfile(
            l1_fee_oracle_kind="arbitrum_nodeinterface",
            l1_fee_oracle_address="0x000000000000000000000000000000000000006C",
        )
        assert profile.l1_fee_oracle_kind == "arbitrum_nodeinterface"
        assert profile.l1_fee_oracle_address == "0x000000000000000000000000000000000000006C"

    def test_valid_op_gaspriceoracle_kind_and_address_accepted(self) -> None:
        profile = GasProfile(
            l1_fee_oracle_kind="op_gaspriceoracle",
            l1_fee_oracle_address="0x420000000000000000000000000000000000000F",
        )
        assert profile.l1_fee_oracle_kind == "op_gaspriceoracle"
        assert profile.l1_fee_oracle_address == "0x420000000000000000000000000000000000000F"

    def test_both_none_is_valid(self) -> None:
        profile = GasProfile()
        assert profile.l1_fee_oracle_kind is None
        assert profile.l1_fee_oracle_address is None

    def test_unknown_kind_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown l1_fee_oracle_kind"):
            GasProfile(
                l1_fee_oracle_kind="bogus_oracle",
                l1_fee_oracle_address="0x420000000000000000000000000000000000000F",
            )

    def test_kind_without_address_raises(self) -> None:
        with pytest.raises(ValueError, match="l1_fee_oracle_address is None"):
            GasProfile(l1_fee_oracle_kind="op_gaspriceoracle")

    def test_address_without_kind_raises(self) -> None:
        with pytest.raises(ValueError, match="l1_fee_oracle_kind is None"):
            GasProfile(l1_fee_oracle_address="0x420000000000000000000000000000000000000F")

    def test_invalid_address_format_raises(self) -> None:
        with pytest.raises(ValueError, match="0x-prefixed 40-hex-character string"):
            GasProfile(
                l1_fee_oracle_kind="op_gaspriceoracle",
                l1_fee_oracle_address="not-an-address",
            )

    def test_known_kinds_frozenset_contents(self) -> None:
        assert KNOWN_L1_FEE_ORACLE_KINDS == frozenset(
            {"arbitrum_nodeinterface", "op_gaspriceoracle"}
        )


# ---------------------------------------------------------------------------
# Derivation parity tests
# ---------------------------------------------------------------------------


class TestL2ConstantsDerivationParity:
    """Derived L2_CHAINS and L2_GAS_ORACLE_ADDRESSES must equal the historical literals."""

    # Historical literals (verbatim from gas.py before Plan 026).
    _HISTORICAL_L2_CHAINS = {"arbitrum", "optimism", "base"}
    _HISTORICAL_L2_GAS_ORACLE_ADDRESSES = {
        "optimism": "0x420000000000000000000000000000000000000F",
        "base": "0x420000000000000000000000000000000000000F",
        "arbitrum": "0x000000000000000000000000000000000000006C",
    }

    def test_l2_chains_set_equals_historical(self) -> None:
        assert L2_CHAINS == self._HISTORICAL_L2_CHAINS

    def test_l2_gas_oracle_addresses_equals_historical(self) -> None:
        assert L2_GAS_ORACLE_ADDRESSES == self._HISTORICAL_L2_GAS_ORACLE_ADDRESSES

    def test_l2_chains_subset_of_gas_oracle_addresses(self) -> None:
        assert L2_CHAINS == set(L2_GAS_ORACLE_ADDRESSES.keys())


# ---------------------------------------------------------------------------
# Dispatch routing tests
# ---------------------------------------------------------------------------


class TestFetchL1DataCostDispatch:
    """_fetch_l1_data_cost dispatches to the correct fetcher based on descriptor kind."""

    _ARB_RESULT = (Decimal("25.0"), Decimal("0.5"))
    _OP_RESULT = (Decimal("30.0"), Decimal("0.8"))

    def _make_oracle(self) -> Web3GasOracle:
        """Build a Web3GasOracle without requiring real RPC URLs."""
        oracle = Web3GasOracle.__new__(Web3GasOracle)
        oracle._rpc_urls = {}
        oracle._web3_instances = {}
        oracle._price_oracle = None
        oracle._request_timeout = 10
        return oracle

    @pytest.mark.asyncio
    async def test_arbitrum_dispatches_to_arbitrum_fetcher(self) -> None:
        oracle = self._make_oracle()
        mock_web3 = MagicMock()

        with (
            patch.object(
                oracle, "_fetch_arbitrum_l1_cost", new=AsyncMock(return_value=self._ARB_RESULT)
            ) as arb_mock,
            patch.object(
                oracle, "_fetch_optimism_l1_cost", new=AsyncMock(return_value=self._OP_RESULT)
            ) as op_mock,
        ):
            result = await oracle._fetch_l1_data_cost(mock_web3, "arbitrum")

        assert result == self._ARB_RESULT
        arb_mock.assert_awaited_once()
        op_mock.assert_not_awaited()
        # Confirm the address passed matches the registered descriptor
        call_address = arb_mock.call_args[0][1]
        assert call_address == "0x000000000000000000000000000000000000006C"

    @pytest.mark.asyncio
    async def test_optimism_dispatches_to_optimism_fetcher(self) -> None:
        oracle = self._make_oracle()
        mock_web3 = MagicMock()

        with (
            patch.object(
                oracle, "_fetch_arbitrum_l1_cost", new=AsyncMock(return_value=self._ARB_RESULT)
            ) as arb_mock,
            patch.object(
                oracle, "_fetch_optimism_l1_cost", new=AsyncMock(return_value=self._OP_RESULT)
            ) as op_mock,
        ):
            result = await oracle._fetch_l1_data_cost(mock_web3, "optimism")

        assert result == self._OP_RESULT
        op_mock.assert_awaited_once()
        arb_mock.assert_not_awaited()
        # Confirm the address passed matches the registered descriptor
        call_address = op_mock.call_args[0][1]
        assert call_address == "0x420000000000000000000000000000000000000F"

    @pytest.mark.asyncio
    async def test_base_dispatches_to_optimism_fetcher(self) -> None:
        oracle = self._make_oracle()
        mock_web3 = MagicMock()

        with (
            patch.object(
                oracle, "_fetch_arbitrum_l1_cost", new=AsyncMock(return_value=self._ARB_RESULT)
            ) as arb_mock,
            patch.object(
                oracle, "_fetch_optimism_l1_cost", new=AsyncMock(return_value=self._OP_RESULT)
            ) as op_mock,
        ):
            result = await oracle._fetch_l1_data_cost(mock_web3, "base")

        assert result == self._OP_RESULT
        op_mock.assert_awaited_once()
        arb_mock.assert_not_awaited()
        # base uses the same OP-stack predeploy address as optimism
        call_address = op_mock.call_args[0][1]
        assert call_address == "0x420000000000000000000000000000000000000F"

    @pytest.mark.asyncio
    async def test_non_l2_chain_returns_none_none(self) -> None:
        oracle = self._make_oracle()
        mock_web3 = MagicMock()

        with (
            patch.object(
                oracle, "_fetch_arbitrum_l1_cost", new=AsyncMock(return_value=self._ARB_RESULT)
            ) as arb_mock,
            patch.object(
                oracle, "_fetch_optimism_l1_cost", new=AsyncMock(return_value=self._OP_RESULT)
            ) as op_mock,
        ):
            result = await oracle._fetch_l1_data_cost(mock_web3, "ethereum")

        assert result == (None, None)
        arb_mock.assert_not_awaited()
        op_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_chain_returns_none_none(self) -> None:
        oracle = self._make_oracle()
        mock_web3 = MagicMock()

        with (
            patch.object(
                oracle, "_fetch_arbitrum_l1_cost", new=AsyncMock(return_value=self._ARB_RESULT)
            ) as arb_mock,
            patch.object(
                oracle, "_fetch_optimism_l1_cost", new=AsyncMock(return_value=self._OP_RESULT)
            ) as op_mock,
        ):
            result = await oracle._fetch_l1_data_cost(mock_web3, "totally-unknown-chain")

        assert result == (None, None)
        arb_mock.assert_not_awaited()
        op_mock.assert_not_awaited()
