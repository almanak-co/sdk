"""Tests for FluidSDK — addresses, chain validation, debt guard, state overrides."""

import pytest
from web3 import Web3

from almanak.framework.connectors.fluid.sdk import (
    DEFAULT_GAS_ESTIMATES,
    FLUID_ADDRESSES,
    DexPoolData,
    FluidSDKError,
    _build_erc20_state_override,
    _compute_mapping_slot,
    _compute_nested_mapping_slot,
    _BALANCE_MAPPING_SLOTS,
    _ALLOWANCE_MAPPING_SLOTS,
)


class TestFluidAddresses:
    def test_arbitrum_exists(self):
        assert "arbitrum" in FLUID_ADDRESSES
        arb = FLUID_ADDRESSES["arbitrum"]
        for key in ("dex_factory", "dex_resolver", "dex_reserves_resolver", "liquidity_resolver", "vault_resolver"):
            assert key in arb

    def test_no_ethereum_yet(self):
        assert "ethereum" not in FLUID_ADDRESSES


class TestDexPoolData:
    def test_construction(self):
        data = DexPoolData(dex_address="0x" + "1" * 40, token0="0x" + "a" * 40, token1="0x" + "b" * 40,
                           fee_bps=100, is_smart_collateral=False, is_smart_debt=False)
        assert data.fee_bps == 100
        assert not data.is_smart_debt


class TestGasEstimates:
    def test_all_present(self):
        for key in ("approve", "operate_open", "operate_close"):
            assert key in DEFAULT_GAS_ESTIMATES
            assert DEFAULT_GAS_ESTIMATES[key] > 20_000


class TestChainValidation:
    def test_unsupported_chain(self):
        from unittest.mock import patch
        with patch("almanak.framework.connectors.fluid.sdk.Web3"):
            with pytest.raises(FluidSDKError, match="not supported"):
                from almanak.framework.connectors.fluid.sdk import FluidSDK
                FluidSDK(chain="polygon", rpc_url="https://fake")


class TestDebtGuard:
    def test_nonzero_debt_raises(self):
        from unittest.mock import MagicMock, patch
        with patch("almanak.framework.connectors.fluid.sdk.Web3") as mock_web3_cls:
            mock_w3 = MagicMock()
            mock_web3_cls.return_value = mock_w3
            mock_web3_cls.HTTPProvider = MagicMock()
            mock_web3_cls.to_checksum_address = lambda x: x
            from almanak.framework.connectors.fluid.sdk import FluidSDK
            with patch.dict(FLUID_ADDRESSES, {"testchain": FLUID_ADDRESSES["arbitrum"]}):
                sdk = FluidSDK(chain="testchain", rpc_url="https://fake")
                with pytest.raises(FluidSDKError, match="smart-debt"):
                    sdk.build_operate_tx(dex_address="0x" + "1" * 40, nft_id=0, new_col=1000, new_debt=500, to="0x" + "a" * 40)


# =========================================================================
# State Override Helpers (VIB-1696)
# =========================================================================

HOLDER = "0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa"
SPENDER = "0xbBbBBBBbbBBBbbbBbbBbbbbBBbBbbbbBbBbbBBbB"
TOKEN = "0xcCcCcCCcCCCcCccCcCccCCcCCCcCcCccCcCCCcCc"


class TestComputeMappingSlot:
    """Verify keccak256(abi.encode(address, uint256)) for simple mappings."""

    def test_returns_hex_string(self):
        slot = _compute_mapping_slot(HOLDER, 0)
        assert slot.startswith("0x")
        assert len(slot) == 66  # 0x + 64 hex chars

    def test_different_slots_differ(self):
        s0 = _compute_mapping_slot(HOLDER, 0)
        s1 = _compute_mapping_slot(HOLDER, 1)
        assert s0 != s1

    def test_different_addresses_differ(self):
        s_a = _compute_mapping_slot(HOLDER, 0)
        s_b = _compute_mapping_slot(SPENDER, 0)
        assert s_a != s_b

    def test_matches_web3_keccak(self):
        """Cross-check against manual abi.encode computation."""
        addr_int = int(HOLDER, 16)
        encoded = addr_int.to_bytes(32, "big") + (0).to_bytes(32, "big")
        expected = "0x" + Web3.keccak(encoded).hex()
        assert _compute_mapping_slot(HOLDER, 0) == expected


class TestComputeNestedMappingSlot:
    """Verify keccak256 for nested mapping(address => mapping(address => T))."""

    def test_returns_hex_string(self):
        slot = _compute_nested_mapping_slot(HOLDER, SPENDER, 1)
        assert slot.startswith("0x")
        assert len(slot) == 66

    def test_order_matters(self):
        """allowances[owner][spender] != allowances[spender][owner]."""
        s1 = _compute_nested_mapping_slot(HOLDER, SPENDER, 1)
        s2 = _compute_nested_mapping_slot(SPENDER, HOLDER, 1)
        assert s1 != s2

    def test_different_mapping_slots(self):
        s1 = _compute_nested_mapping_slot(HOLDER, SPENDER, 1)
        s10 = _compute_nested_mapping_slot(HOLDER, SPENDER, 10)
        assert s1 != s10


class TestBuildErc20StateOverride:
    """Verify the combined state override dict structure."""

    def test_structure(self):
        override = _build_erc20_state_override(TOKEN, HOLDER, SPENDER)
        assert TOKEN in override
        assert "stateDiff" in override[TOKEN]

    def test_covers_all_slot_patterns(self):
        override = _build_erc20_state_override(TOKEN, HOLDER, SPENDER)
        state_diff = override[TOKEN]["stateDiff"]
        expected_count = len(_BALANCE_MAPPING_SLOTS) + len(_ALLOWANCE_MAPPING_SLOTS)
        assert len(state_diff) == expected_count

    def test_all_values_are_max_uint256(self):
        override = _build_erc20_state_override(TOKEN, HOLDER, SPENDER)
        max_val = "0x" + "ff" * 32
        for val in override[TOKEN]["stateDiff"].values():
            assert val == max_val

    def test_all_keys_are_valid_hex(self):
        override = _build_erc20_state_override(TOKEN, HOLDER, SPENDER)
        for key in override[TOKEN]["stateDiff"]:
            assert key.startswith("0x")
            assert len(key) == 66


class TestGetSwapQuoteWithOverrides:
    """Verify get_swap_quote passes state_override to eth_call."""

    def test_calls_with_state_override(self):
        from unittest.mock import MagicMock, patch

        with patch("almanak.framework.connectors.fluid.sdk.Web3") as mock_web3_cls:
            mock_w3 = MagicMock()
            mock_web3_cls.return_value = mock_w3
            mock_web3_cls.HTTPProvider = MagicMock()
            mock_web3_cls.to_checksum_address = lambda x: x
            mock_web3_cls.keccak = Web3.keccak

            # Mock swapIn().call() to return a valid amount
            mock_swap_fn = MagicMock()
            mock_swap_fn.call.return_value = 999_000
            mock_contract = MagicMock()
            mock_contract.functions.swapIn.return_value = mock_swap_fn
            mock_w3.eth.contract.return_value = mock_contract

            # Mock get_dex_data
            from almanak.framework.connectors.fluid.sdk import FluidSDK
            with patch.dict(FLUID_ADDRESSES, {"testchain": FLUID_ADDRESSES["arbitrum"]}):
                sdk = FluidSDK(chain="testchain", rpc_url="https://fake")
                sdk.get_dex_data = MagicMock(return_value=DexPoolData(
                    dex_address="0x" + "1" * 40,
                    token0="0x" + "a" * 40,
                    token1="0x" + "b" * 40,
                ))

                result = sdk.get_swap_quote(
                    dex_address="0x" + "1" * 40,
                    swap0to1=True,
                    amount_in=1_000_000,
                    to="0x" + "c" * 40,
                )

            assert result == 999_000
            # Verify state_override was passed
            call_kwargs = mock_swap_fn.call.call_args
            assert "state_override" in call_kwargs.kwargs
