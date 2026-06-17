"""Unit tests for FluidVaultSDK (VIB-5031) — pinned ABI + nft resolution.

Every constant is byte-verified against
``docs/internal/qa/fluid-vault-verification-2026-06-12.md`` — these tests
pin the in-source ABI/selector constants to the verified bytes so any
drift (a re-typed signature, a re-ordered struct) fails loudly, and pin
the typed-ABI decode against synthetic ``eth_abi``-encoded blobs of the
exact verified shapes (97-word VaultEntireData / 12-word UserPosition).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from eth_abi import encode as abi_encode
from web3 import Web3

from almanak.connectors.fluid.sdk import FluidSDKError
from almanak.connectors.fluid.vault_sdk import (
    INT256_MIN,
    OPERATE_SELECTOR,
    USER_POSITION_TYPE,
    VAULT_ENTIRE_DATA_TYPE,
    FluidVaultSDK,
    position_from_tuple,
    vault_data_from_tuple,
)

ARB_VAULT_1 = "0xeAbBfca72F8a8bf14C4ac59e69ECB2eB69F0811C"
ARB_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
NATIVE = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
ZERO = "0x" + "0" * 40


def _sdk() -> FluidVaultSDK:
    return FluidVaultSDK(chain="arbitrum", rpc_url="http://localhost:8545")


class TestConstructorGuards:
    def test_disconnected_gateway_client_raises_fast(self):
        # A provided-but-disconnected gateway client must fail at
        # construction, not opaquely on the first eth_call.
        class _Disconnected:
            is_connected = False

        with pytest.raises(FluidSDKError, match="not connected"):
            FluidVaultSDK(chain="arbitrum", gateway_client=_Disconnected())

    def test_no_transport_raises(self):
        with pytest.raises(FluidSDKError, match="rpc_url"):
            FluidVaultSDK(chain="arbitrum")


class TestEnsureGatewayConnected:
    """Re-check gateway connectivity before EACH resolver read (CodeRabbit #2856).

    The constructor's ``is_connected`` guard is point-in-time: a GatewayClient
    can drop its channel afterward. Every resolver read must fast-fail with a
    typed error rather than an opaque provider error on the wire.
    """

    def _gateway_sdk(self, *, connected: bool) -> FluidVaultSDK:
        # Build via rpc_url (no live provider needed), then attach a gateway
        # client whose connectivity we control; the guard reads is_connected.
        sdk = _sdk()
        sdk._gateway_client = SimpleNamespace(is_connected=connected)
        return sdk

    def test_no_op_for_rpc_only_sdk(self):
        # The rpc_url path holds no gateway client — the guard must never raise.
        _sdk()._ensure_gateway_connected()

    def test_connected_gateway_passes(self):
        self._gateway_sdk(connected=True)._ensure_gateway_connected()

    @pytest.mark.parametrize(
        ("method", "args"),
        [
            ("get_vault_entire_data", (ARB_VAULT_1,)),
            ("position_by_nft_id", (12542,)),
            ("positions_by_user", (WALLET,)),
        ],
    )
    def test_stale_gateway_fast_fails_before_read(self, method: str, args: tuple):
        sdk = self._gateway_sdk(connected=False)
        # "not connected" is unique to the guard; a resolver .call() that slipped
        # past it would raise a DIFFERENT error (no node at localhost), so this
        # pins that the guard fires BEFORE the on-chain read.
        with pytest.raises(FluidSDKError, match="not connected"):
            getattr(sdk, method)(*args)


def _user_position_tuple(nft_id: int = 12542, supply: int = 10**18, borrow: int = 500_000_001):
    # (nftId, owner, isLiquidated, isSupplyPosition, tick, tickId,
    #  beforeSupply, beforeBorrow, beforeDustBorrow, supply, borrow, dustBorrow)
    return (nft_id, WALLET, False, False, -14338, 1, supply, borrow, 329994, supply, borrow, 329994)


def _vault_data_tuple(
    vault: str = ARB_VAULT_1,
    supply_token: str = NATIVE,
    borrow_token: str = ARB_USDC,
    vault_id: int = 1,
    vault_type: int = 10000,
    oracle_price: int = 2_500 * 10**18,
):
    constants = (
        ZERO,  # liquidity
        ZERO,  # factory
        ZERO,  # operateImplementation
        ZERO,  # adminImplementation
        ZERO,  # secondaryImplementation
        ZERO,  # deployer
        ZERO,  # supply
        ZERO,  # borrow
        (supply_token, ZERO),  # supplyToken
        (borrow_token, ZERO),  # borrowToken
        vault_id,
        vault_type,
        b"\x00" * 32,
        b"\x00" * 32,
        b"\x00" * 32,
        b"\x00" * 32,
    )
    configs = (0, 0, 8700, 9200, 9500, 0, 100, 0, ZERO, oracle_price, oracle_price, ZERO, 0)
    exchange = (0,) * 10 + (0, 0, 0, 0)
    totals = (10**21, 5 * 10**11, 0, 0, 0, 0)
    limits = (0, 0, 10**24, 0, 0, 10**12, 0, 0)
    vault_state = (1, -14338, 0, 0, 0, 0, (0, 0, 0, 0, 0, 0, 0))
    supply_data = (True,) + (0,) * 10
    borrow_data = (True,) + (0,) * 10
    return (
        vault,
        False,
        False,
        constants,
        configs,
        exchange,
        totals,
        limits,
        vault_state,
        supply_data,
        borrow_data,
    )


class TestPinnedSelectors:
    """The in-source ABI derives EXACTLY the verified selectors (full bytes)."""

    @pytest.mark.parametrize(
        ("signature", "pinned"),
        [
            ("operate(uint256,int256,int256,address)", "032d2276"),
            ("positionsByUser(address)", "347ca8bb"),
            ("positionByNftId(uint256)", "144128e8"),
            ("getVaultEntireData(address)", "09c062e2"),
            ("getVaultAddress(uint256)", "e6bd26a2"),
        ],
    )
    def test_selector_matches_verification_report(self, signature: str, pinned: str):
        assert Web3.keccak(text=signature)[:4].hex() == pinned

    def test_operate_selector_constant(self):
        assert OPERATE_SELECTOR == "0x032d2276"


class TestPinnedStructShapes:
    """The flattened type strings encode the verified static word counts."""

    def test_user_position_is_12_static_words(self):
        blob = abi_encode([USER_POSITION_TYPE], [_user_position_tuple()])
        assert len(blob) == 12 * 32

    def test_vault_entire_data_is_97_static_words(self):
        blob = abi_encode([VAULT_ENTIRE_DATA_TYPE], [_vault_data_tuple()])
        assert len(blob) == 97 * 32


class TestOperateCalldata:
    def test_operate_calldata_selector_and_args(self):
        data = _sdk().encode_operate_calldata(0, 10**18, 500_000_000, WALLET)
        assert data.startswith(OPERATE_SELECTOR)
        body = data[10:]
        assert body[0:64] == f"{0:064x}"  # nftId = 0 (mint)
        assert body[64:128] == f"{10**18:064x}"  # +1 ETH collateral
        assert body[128:192] == f"{500_000_000:064x}"  # +500e6 USDC debt
        assert body[192:256].endswith(WALLET[2:].lower())

    def test_negative_delta_encodes_twos_complement(self):
        # The verification report's live repay: operate(12542, 0, -200000000, wallet)
        # encoded debtAmt = 0xff...f4143e00 — the exact two's-complement form.
        data = _sdk().encode_operate_calldata(12542, 0, -200_000_000, WALLET)
        debt_word = data[10:][128:192]
        assert debt_word == f"{(1 << 256) - 200_000_000:064x}"
        assert debt_word.endswith("f4143e00")

    def test_int256_min_sentinel_encodes(self):
        data = _sdk().encode_operate_calldata(12542, INT256_MIN, INT256_MIN, WALLET)
        body = data[10:]
        sentinel = f"{(1 << 256) + INT256_MIN:064x}"
        assert body[64:128] == sentinel
        assert body[128:192] == sentinel
        assert sentinel.startswith("8000")

    def test_build_operate_tx_carries_value_for_native_leg(self):
        tx = _sdk().build_operate_tx(ARB_VAULT_1, 0, 10**18, 0, WALLET, value=10**18)
        assert tx["to"] == Web3.to_checksum_address(ARB_VAULT_1)
        assert tx["value"] == 10**18
        assert tx["data"].startswith(OPERATE_SELECTOR)
        assert tx["gas"] > 0


class TestTupleMappers:
    def test_position_from_tuple_uses_scaled_fields_9_and_10(self):
        # Resolver supply/borrow (fields 9/10) are already exchange-price-
        # scaled token amounts — the mapper must use THOSE, not the
        # pre-adjustment before* fields (verification report note 4).
        raw = list(_user_position_tuple())
        raw[6] = 111  # beforeSupply — must NOT be picked up
        raw[7] = 222  # beforeBorrow — must NOT be picked up
        position = position_from_tuple(tuple(raw), vault=ARB_VAULT_1)
        assert position.nft_id == 12542
        assert position.supply == 10**18
        assert position.borrow == 500_000_001
        assert position.vault == ARB_VAULT_1.lower()
        assert position.is_liquidated is False

    def test_vault_data_from_tuple_decodes_verified_fields(self):
        data = vault_data_from_tuple(_vault_data_tuple())
        assert data.vault == ARB_VAULT_1.lower()
        assert data.supply_token == NATIVE
        assert data.borrow_token == ARB_USDC
        assert data.vault_id == 1
        assert data.vault_type == 10000  # VAULT_T1_TYPE
        assert data.collateral_factor == 8700
        assert data.liquidation_threshold == 9200
        assert data.liquidation_penalty == 100
        assert data.oracle_price_operate == 2_500 * 10**18
        assert data.is_smart_col is False
        assert data.is_smart_debt is False


class TestResolveUserNftForVault:
    """ADR §1.4 / §3.4 — None / lowest-id / warn-on-multiple / fail-closed."""

    def _positions(self, *pairs):
        return [position_from_tuple(_user_position_tuple(nft_id=nft_id), vault=vault) for nft_id, vault in pairs]

    def test_no_position_returns_none(self):
        sdk = _sdk()
        with patch.object(FluidVaultSDK, "positions_by_user", return_value=[]):
            assert sdk.resolve_user_nft_for_vault(WALLET, ARB_VAULT_1) is None

    def test_single_position_on_vault_resolved(self):
        sdk = _sdk()
        positions = self._positions((12542, ARB_VAULT_1))
        with patch.object(FluidVaultSDK, "positions_by_user", return_value=positions):
            assert sdk.resolve_user_nft_for_vault(WALLET, ARB_VAULT_1.upper()) == 12542

    def test_other_vault_positions_ignored(self):
        sdk = _sdk()
        positions = self._positions((7, "0x" + "1" * 40), (12542, ARB_VAULT_1))
        with patch.object(FluidVaultSDK, "positions_by_user", return_value=positions):
            assert sdk.resolve_user_nft_for_vault(WALLET, ARB_VAULT_1) == 12542

    def test_multiple_positions_lowest_id_wins_with_warning(self, caplog):
        sdk = _sdk()
        positions = self._positions((900, ARB_VAULT_1), (12542, ARB_VAULT_1))
        with (
            patch.object(FluidVaultSDK, "positions_by_user", return_value=positions),
            caplog.at_level("WARNING"),
        ):
            assert sdk.resolve_user_nft_for_vault(WALLET, ARB_VAULT_1) == 900
        assert any("lowest nftId" in record.message for record in caplog.records)

    def test_read_failure_raises_never_returns_none(self):
        # A transport/rpc failure must be DISTINGUISHABLE from "no position":
        # returning None here would let a compiler mint a duplicate NFT.
        sdk = _sdk()
        with (
            patch.object(FluidVaultSDK, "positions_by_user", side_effect=FluidSDKError("rpc unreachable")),
            pytest.raises(FluidSDKError),
        ):
            sdk.resolve_user_nft_for_vault(WALLET, ARB_VAULT_1)

    def test_misaligned_resolver_arrays_fail_closed(self):
        sdk = _sdk()
        raw_position = _user_position_tuple()
        with patch.object(
            sdk._resolver.functions,
            "positionsByUser",
        ) as positions_fn:
            positions_fn.return_value.call.return_value = ([raw_position], [])
            with pytest.raises(FluidSDKError, match="misaligned"):
                sdk.positions_by_user(WALLET)
