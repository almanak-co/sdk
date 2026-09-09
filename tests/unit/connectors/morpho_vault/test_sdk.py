"""Tests for MetaMorpho Vault SDK."""

import json
from unittest.mock import MagicMock

import pytest

from almanak.connectors.morpho_vault.sdk import (
    DEFAULT_GAS_ESTIMATES,
    DEPOSIT_SELECTOR,
    ERC20_APPROVE_SELECTOR,
    MAX_QUEUE_LENGTH,
    MAX_UINT256,
    REDEEM_SELECTOR,
    SUPPORTED_CHAINS,
    DepositExceedsCapError,
    InsufficientSharesError,
    MetaMorphoSDK,
    MetaMorphoSDKError,
    RPCError,
    UnsupportedChainError,
    VaultInfo,
    VaultMarketConfig,
    VaultNotFoundError,
    VaultPosition,
    _decode_address,
    _decode_uint256,
    _encode_address,
    _encode_uint256,
)

VAULT_ADDR = "0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB"
USER_ADDR = "0x1234567890123456789012345678901234567890"
ASSET_ADDR = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"


# =============================================================================
# Helpers
# =============================================================================


def _make_gateway(result_value="0x" + "00" * 31 + "01"):
    """Create a mock gateway_client whose rpc.Call returns a successful response."""
    gw = MagicMock()
    resp = MagicMock()
    resp.success = True
    resp.result = json.dumps(result_value)
    gw.rpc.Call.return_value = resp
    return gw


def _make_gateway_multi(results: list[str]):
    """Create a mock gateway_client that returns different results per call."""
    gw = MagicMock()
    responses = []
    for r in results:
        resp = MagicMock()
        resp.success = True
        resp.result = json.dumps(r)
        responses.append(resp)
    gw.rpc.Call.side_effect = responses
    return gw


# =============================================================================
# Encoding / Decoding
# =============================================================================


class TestEncoding:
    def test_encode_address(self):
        encoded = _encode_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
        assert len(encoded) == 64
        assert encoded.endswith("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")

    def test_encode_uint256_zero(self):
        encoded = _encode_uint256(0)
        assert encoded == "0" * 64

    def test_encode_uint256_one(self):
        encoded = _encode_uint256(1)
        assert encoded == "0" * 63 + "1"

    def test_encode_uint256_large(self):
        val = 10**18
        encoded = _encode_uint256(val)
        assert int(encoded, 16) == val

    def test_encode_uint256_negative_raises(self):
        with pytest.raises(ValueError, match="negative"):
            _encode_uint256(-1)

    def test_decode_uint256_basic(self):
        assert _decode_uint256("0x" + "0" * 63 + "a") == 10

    def test_decode_uint256_empty(self):
        assert _decode_uint256("0x") == 0
        assert _decode_uint256("") == 0

    def test_decode_address(self):
        hex_str = "0x" + "0" * 24 + "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        result = _decode_address(hex_str)
        assert result == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"


# =============================================================================
# Constructor
# =============================================================================


class TestSDKConstructor:
    def test_valid_chains(self):
        gw = _make_gateway()
        for chain in SUPPORTED_CHAINS:
            sdk = MetaMorphoSDK(gw, chain)
            assert sdk._chain == chain

    def test_unsupported_chain_raises(self):
        gw = _make_gateway()
        with pytest.raises(UnsupportedChainError, match="not supported"):
            MetaMorphoSDK(gw, "polygon")

    def test_chain_case_insensitive(self):
        gw = _make_gateway()
        sdk = MetaMorphoSDK(gw, "Ethereum")
        assert sdk._chain == "ethereum"


# =============================================================================
# Read Methods
# =============================================================================


class TestReadMethods:
    def test_get_vault_asset(self):
        addr_hex = "0x" + "0" * 24 + ASSET_ADDR[2:].lower()
        gw = _make_gateway(addr_hex)
        sdk = MetaMorphoSDK(gw, "ethereum")
        result = sdk.get_vault_asset(VAULT_ADDR)
        assert result == "0x" + ASSET_ADDR[2:].lower()

    def test_get_total_assets(self):
        val = 1_000_000_000_000  # 1M USDC (6 decimals)
        gw = _make_gateway("0x" + _encode_uint256(val))
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.get_total_assets(VAULT_ADDR) == val

    def test_get_total_supply(self):
        val = 999_000_000_000_000_000_000_000  # ~999 shares (18 decimals)
        gw = _make_gateway("0x" + _encode_uint256(val))
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.get_total_supply(VAULT_ADDR) == val

    def test_get_share_price(self):
        price = 1_001_000  # ~1.001 USDC per share (6 dec underlying)
        # get_share_price calls get_decimals() first, then convertToAssets()
        gw = _make_gateway_multi(
            [
                "0x" + _encode_uint256(18),  # decimals() -> 18
                "0x" + _encode_uint256(price),  # convertToAssets(1e18) -> price
            ]
        )
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.get_share_price(VAULT_ADDR) == price

    def test_get_decimals(self):
        gw = _make_gateway("0x" + _encode_uint256(18))
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.get_decimals(VAULT_ADDR) == 18

    def test_get_balance_of(self):
        balance = 500 * 10**18
        gw = _make_gateway("0x" + _encode_uint256(balance))
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.get_balance_of(VAULT_ADDR, USER_ADDR) == balance

    def test_get_max_deposit(self):
        max_dep = MAX_UINT256
        gw = _make_gateway("0x" + _encode_uint256(max_dep))
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.get_max_deposit(VAULT_ADDR, USER_ADDR) == max_dep

    def test_get_max_redeem(self):
        max_red = 100 * 10**18
        gw = _make_gateway("0x" + _encode_uint256(max_red))
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.get_max_redeem(VAULT_ADDR, USER_ADDR) == max_red

    def test_preview_deposit(self):
        shares = 999 * 10**18
        gw = _make_gateway("0x" + _encode_uint256(shares))
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.preview_deposit(VAULT_ADDR, 1000 * 10**6) == shares

    def test_preview_redeem(self):
        assets = 1001 * 10**6
        gw = _make_gateway("0x" + _encode_uint256(assets))
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.preview_redeem(VAULT_ADDR, 1000 * 10**18) == assets

    def test_convert_to_assets(self):
        assets = 500 * 10**6
        gw = _make_gateway("0x" + _encode_uint256(assets))
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.convert_to_assets(VAULT_ADDR, 500 * 10**18) == assets

    def test_convert_to_shares(self):
        shares = 500 * 10**18
        gw = _make_gateway("0x" + _encode_uint256(shares))
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.convert_to_shares(VAULT_ADDR, 500 * 10**6) == shares


class TestMetaMorphoSpecificReads:
    def test_get_curator(self):
        curator = "0x" + "0" * 24 + "abcd" * 10
        gw = _make_gateway(curator)
        sdk = MetaMorphoSDK(gw, "ethereum")
        result = sdk.get_curator(VAULT_ADDR)
        assert result.startswith("0x")
        assert len(result) == 42

    def test_get_fee(self):
        fee = 50_000_000_000_000_000  # 5% = 0.05 * 1e18
        gw = _make_gateway("0x" + _encode_uint256(fee))
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.get_fee(VAULT_ADDR) == fee

    def test_get_timelock(self):
        gw = _make_gateway("0x" + _encode_uint256(86400))  # 1 day
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.get_timelock(VAULT_ADDR) == 86400

    def test_is_allocator_true(self):
        gw = _make_gateway("0x" + _encode_uint256(1))
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.is_allocator(VAULT_ADDR, USER_ADDR) is True

    def test_is_allocator_false(self):
        gw = _make_gateway("0x" + _encode_uint256(0))
        sdk = MetaMorphoSDK(gw, "ethereum")
        assert sdk.is_allocator(VAULT_ADDR, USER_ADDR) is False

    def test_get_supply_queue(self):
        market_id = "0x" + "ab" * 32
        gw = _make_gateway_multi(
            [
                "0x" + _encode_uint256(1),  # length = 1
                market_id,
            ]
        )
        sdk = MetaMorphoSDK(gw, "ethereum")
        queue = sdk.get_supply_queue(VAULT_ADDR)
        assert len(queue) == 1

    def test_get_supply_queue_empty(self):
        gw = _make_gateway("0x" + _encode_uint256(0))
        sdk = MetaMorphoSDK(gw, "ethereum")
        queue = sdk.get_supply_queue(VAULT_ADDR)
        assert queue == []

    def test_get_supply_queue_exceeds_max_length(self):
        gw = _make_gateway("0x" + _encode_uint256(MAX_QUEUE_LENGTH + 1))
        sdk = MetaMorphoSDK(gw, "ethereum")
        with pytest.raises(MetaMorphoSDKError, match="exceeds maximum"):
            sdk.get_supply_queue(VAULT_ADDR)

    def test_get_withdraw_queue_exceeds_max_length(self):
        gw = _make_gateway("0x" + _encode_uint256(MAX_QUEUE_LENGTH + 1))
        sdk = MetaMorphoSDK(gw, "ethereum")
        with pytest.raises(MetaMorphoSDKError, match="exceeds maximum"):
            sdk.get_withdraw_queue(VAULT_ADDR)


# =============================================================================
# Composite Read Methods
# =============================================================================


class TestCompositeReads:
    def test_get_vault_info(self):
        """get_vault_info should aggregate multiple RPC calls."""
        asset_hex = "0x" + "0" * 24 + ASSET_ADDR[2:].lower()
        total_assets = "0x" + _encode_uint256(1_000_000_000)
        total_supply = "0x" + _encode_uint256(999 * 10**18)
        share_price = "0x" + _encode_uint256(1_001_000)
        decimals = "0x" + _encode_uint256(18)
        curator_hex = "0x" + "0" * 24 + "cc" * 20
        fee = "0x" + _encode_uint256(50_000_000_000_000_000)
        timelock = "0x" + _encode_uint256(86400)

        # get_vault_info calls: version probe (withdrawQueueLength answers -> v1),
        # asset, totalAssets, totalSupply, share_price (decimals + convertToAssets),
        # decimals, curator, fee, timelock
        gw = _make_gateway_multi(
            [
                "0x" + _encode_uint256(3),  # withdrawQueueLength() -> MetaMorpho v1
                asset_hex,
                total_assets,
                total_supply,
                decimals,
                share_price,  # get_share_price: decimals() then convertToAssets()
                decimals,
                curator_hex,
                fee,
                timelock,
            ]
        )
        sdk = MetaMorphoSDK(gw, "ethereum")
        info = sdk.get_vault_info(VAULT_ADDR)

        assert isinstance(info, VaultInfo)
        assert info.address == VAULT_ADDR
        assert info.total_assets == 1_000_000_000
        assert info.decimals == 18
        assert info.fee == 50_000_000_000_000_000
        assert info.timelock == 86400
        assert info.vault_version == "v1"

    def test_get_position(self):
        shares = 100 * 10**18
        assets = 100_500_000  # ~100.5 USDC

        gw = _make_gateway_multi(
            [
                "0x" + _encode_uint256(shares),
                "0x" + _encode_uint256(assets),
            ]
        )
        sdk = MetaMorphoSDK(gw, "ethereum")
        pos = sdk.get_position(VAULT_ADDR, USER_ADDR)

        assert isinstance(pos, VaultPosition)
        assert pos.vault_address == VAULT_ADDR
        assert pos.user == USER_ADDR
        assert pos.shares == shares
        assert pos.assets == assets

    def test_get_position_zero_shares(self):
        gw = _make_gateway("0x" + _encode_uint256(0))
        sdk = MetaMorphoSDK(gw, "ethereum")
        pos = sdk.get_position(VAULT_ADDR, USER_ADDR)
        assert pos.shares == 0
        assert pos.assets == 0


# =============================================================================
# Write Methods
# =============================================================================


class TestWriteMethods:
    def test_build_deposit_tx(self):
        gw = _make_gateway()
        sdk = MetaMorphoSDK(gw, "ethereum")
        tx = sdk.build_deposit_tx(VAULT_ADDR, 1000 * 10**6, USER_ADDR)

        assert tx["to"] == VAULT_ADDR
        assert tx["from"] == USER_ADDR
        assert tx["data"].startswith(DEPOSIT_SELECTOR)
        assert tx["value"] == "0"
        assert tx["gas_estimate"] == DEFAULT_GAS_ESTIMATES["deposit"]

    def test_build_deposit_tx_zero_amount_raises(self):
        gw = _make_gateway()
        sdk = MetaMorphoSDK(gw, "ethereum")
        with pytest.raises(ValueError, match="positive"):
            sdk.build_deposit_tx(VAULT_ADDR, 0, USER_ADDR)

    def test_build_deposit_tx_overflow_raises(self):
        gw = _make_gateway()
        sdk = MetaMorphoSDK(gw, "ethereum")
        with pytest.raises(ValueError, match="MAX_UINT256"):
            sdk.build_deposit_tx(VAULT_ADDR, MAX_UINT256 + 1, USER_ADDR)

    def test_build_redeem_tx(self):
        gw = _make_gateway()
        sdk = MetaMorphoSDK(gw, "ethereum")
        tx = sdk.build_redeem_tx(VAULT_ADDR, 500 * 10**18, USER_ADDR, USER_ADDR)

        assert tx["to"] == VAULT_ADDR
        assert tx["data"].startswith(REDEEM_SELECTOR)
        assert tx["gas_estimate"] == DEFAULT_GAS_ESTIMATES["redeem"]

    def test_build_redeem_tx_zero_shares_raises(self):
        gw = _make_gateway()
        sdk = MetaMorphoSDK(gw, "ethereum")
        with pytest.raises(ValueError, match="positive"):
            sdk.build_redeem_tx(VAULT_ADDR, 0, USER_ADDR, USER_ADDR)

    def test_build_approve_tx(self):
        gw = _make_gateway()
        sdk = MetaMorphoSDK(gw, "ethereum")
        tx = sdk.build_approve_tx(ASSET_ADDR, VAULT_ADDR, 1000 * 10**6, USER_ADDR)

        assert tx["to"] == ASSET_ADDR
        assert tx["data"].startswith(ERC20_APPROVE_SELECTOR)
        assert tx["gas_estimate"] == DEFAULT_GAS_ESTIMATES["approve"]


# =============================================================================
# Error Handling
# =============================================================================


class TestErrors:
    def test_rpc_error_on_failed_response(self):
        gw = MagicMock()
        resp = MagicMock()
        resp.success = False
        resp.error = "timeout"
        gw.rpc.Call.return_value = resp

        sdk = MetaMorphoSDK(gw, "ethereum")
        with pytest.raises(RPCError, match="timeout"):
            sdk.get_vault_asset(VAULT_ADDR)

    def test_vault_not_found_on_empty_response(self):
        gw = MagicMock()
        resp = MagicMock()
        resp.success = True
        resp.result = json.dumps("0x")
        gw.rpc.Call.return_value = resp

        sdk = MetaMorphoSDK(gw, "ethereum")
        with pytest.raises(VaultNotFoundError, match="Empty response"):
            sdk.get_vault_asset(VAULT_ADDR)

    def test_exception_hierarchy(self):
        assert issubclass(VaultNotFoundError, MetaMorphoSDKError)
        assert issubclass(UnsupportedChainError, MetaMorphoSDKError)
        assert issubclass(RPCError, MetaMorphoSDKError)
        assert issubclass(DepositExceedsCapError, MetaMorphoSDKError)
        assert issubclass(InsufficientSharesError, MetaMorphoSDKError)


# =============================================================================
# Data Classes
# =============================================================================


class TestDataClasses:
    def test_vault_info_fields(self):
        info = VaultInfo(
            address=VAULT_ADDR,
            asset=ASSET_ADDR,
            total_assets=1_000_000,
            total_supply=999 * 10**18,
            share_price=1_001_000,
            decimals=18,
            curator=USER_ADDR,
            fee=50_000_000_000_000_000,
            timelock=86400,
        )
        assert info.address == VAULT_ADDR
        assert info.decimals == 18

    def test_vault_position_fields(self):
        pos = VaultPosition(
            vault_address=VAULT_ADDR,
            user=USER_ADDR,
            shares=100 * 10**18,
            assets=100_500_000,
        )
        assert pos.vault_address == VAULT_ADDR
        assert pos.shares == 100 * 10**18

    def test_vault_market_config_fields(self):
        config = VaultMarketConfig(
            market_id="0x" + "ab" * 32,
            cap=10**18,
            enabled=True,
            removable_at=0,
        )
        assert config.enabled is True


# Morpho Vault V2 — generation fingerprinting, redeem sizing, simulation, gates.

from almanak.connectors.morpho_vault.sdk import (  # noqa: E402
    ASSET_SELECTOR,
    BALANCE_OF_SELECTOR,
    CONVERT_TO_ASSETS_SELECTOR,
    CURATOR_SELECTOR,
    DECIMALS_SELECTOR,
    GATE_CAN_RECEIVE_SHARES_SELECTOR,
    GATE_CAN_SEND_ASSETS_SELECTOR,
    MAX_REDEEM_SELECTOR,
    TOTAL_ASSETS_SELECTOR,
    TOTAL_SUPPLY_SELECTOR,
    V2_ADAPTERS_LENGTH_SELECTOR,
    V2_SEND_ASSETS_GATE_SELECTOR,
    VAULT_VERSION_V1,
    VAULT_VERSION_V2,
    WITHDRAW_QUEUE_LENGTH_SELECTOR,
    UnsupportedVaultError,
    VaultGatedError,
    VaultIlliquidError,
)

_WORD_ZERO = "0x" + "0" * 64
_WORD_ONE = "0x" + _encode_uint256(1)
_GATE_ADDR = "0x" + "9a" * 20


def _scripted_gateway(script):
    """Gateway whose rpc.Call answers by calldata selector; unscripted selectors REVERT.

    ``script`` maps ``(to.lower(), data)`` or ``(to.lower(), selector)`` -> hex
    result. This mirrors the real chain, where a v1-only selector reverts on V2
    and vice versa, so the fingerprint logic is exercised for real.
    """
    gw = MagicMock()

    def call(request, timeout=None):
        params = json.loads(request.params)
        call_obj = params[0]
        to, data = call_obj["to"].lower(), call_obj["data"]
        resp = MagicMock()
        value = script.get((to, data), script.get((to, data[:10])))
        if value is None:
            resp.success = False
            resp.error = "execution reverted"
            resp.result = ""
        else:
            resp.success = True
            resp.error = ""
            resp.result = json.dumps(value)
        resp._call_obj = call_obj
        return resp

    gw.rpc.Call.side_effect = call
    return gw


def _v1_script(extra=None):
    return {(VAULT_ADDR.lower(), WITHDRAW_QUEUE_LENGTH_SELECTOR): "0x" + _encode_uint256(5), **(extra or {})}


def _v2_script(extra=None):
    return {(VAULT_ADDR.lower(), V2_ADAPTERS_LENGTH_SELECTOR): _WORD_ONE, **(extra or {})}


class TestGenerationDetection:
    def test_v1_fingerprint(self):
        sdk = MetaMorphoSDK(_scripted_gateway(_v1_script()), "base")
        assert sdk.detect_vault_version(VAULT_ADDR) == VAULT_VERSION_V1
        assert sdk.is_vault_v2(VAULT_ADDR) is False

    def test_v2_fingerprint_when_v1_selector_reverts(self):
        sdk = MetaMorphoSDK(_scripted_gateway(_v2_script()), "base")
        assert sdk.detect_vault_version(VAULT_ADDR) == VAULT_VERSION_V2
        assert sdk.is_vault_v2(VAULT_ADDR) is True

    def test_transport_failure_is_not_read_as_a_reverting_selector(self):
        """A timeout / unavailable node must surface as RPCError, never flip the generation verdict."""
        gw = MagicMock()
        resp = MagicMock()
        resp.success = False
        resp.error = "UNAVAILABLE: connection timed out"
        resp.result = ""
        gw.rpc.Call.return_value = resp
        sdk = MetaMorphoSDK(gw, "base")
        with pytest.raises(RPCError):
            sdk.detect_vault_version(VAULT_ADDR)

    def test_neither_fingerprint_is_refused_not_guessed(self):
        sdk = MetaMorphoSDK(_scripted_gateway({}), "base")
        with pytest.raises(UnsupportedVaultError):
            sdk.detect_vault_version(VAULT_ADDR)

    def test_fingerprint_is_cached_per_address(self):
        gw = _scripted_gateway(_v2_script())
        sdk = MetaMorphoSDK(gw, "base")
        sdk.detect_vault_version(VAULT_ADDR)
        sdk.detect_vault_version(VAULT_ADDR.lower())
        sdk.detect_vault_version(VAULT_ADDR)
        # one v1 probe (reverts) + one v2 probe, then cache hits
        assert gw.rpc.Call.call_count == 2


class TestRedeemableShares:
    def test_v1_uses_max_redeem(self):
        script = _v1_script({(VAULT_ADDR.lower(), MAX_REDEEM_SELECTOR): "0x" + _encode_uint256(41)})
        script[(VAULT_ADDR.lower(), BALANCE_OF_SELECTOR)] = "0x" + _encode_uint256(42)
        sdk = MetaMorphoSDK(_scripted_gateway(script), "base")
        assert sdk.get_redeemable_shares(VAULT_ADDR, USER_ADDR) == 41

    def test_v2_uses_balance_of_because_max_redeem_is_zero_by_design(self):
        script = _v2_script({(VAULT_ADDR.lower(), MAX_REDEEM_SELECTOR): _WORD_ZERO})
        script[(VAULT_ADDR.lower(), BALANCE_OF_SELECTOR)] = "0x" + _encode_uint256(8_605_857_896_403_843_614_675)
        sdk = MetaMorphoSDK(_scripted_gateway(script), "base")
        assert sdk.get_max_redeem(VAULT_ADDR, USER_ADDR) == 0
        assert sdk.get_redeemable_shares(VAULT_ADDR, USER_ADDR) == 8_605_857_896_403_843_614_675


class TestRedeemSimulation:
    def test_simulation_is_sent_from_owner_and_returns_assets(self):
        script = _v2_script({(VAULT_ADDR.lower(), REDEEM_SELECTOR): "0x" + _encode_uint256(99_000_000)})
        gw = _scripted_gateway(script)
        sdk = MetaMorphoSDK(gw, "base")
        assets = sdk.simulate_redeem(VAULT_ADDR, 10**18, receiver=USER_ADDR, owner=USER_ADDR)
        assert assets == 99_000_000
        sent = json.loads(gw.rpc.Call.call_args.args[0].params)[0]
        assert sent["from"].lower() == USER_ADDR.lower()
        assert sent["data"].startswith(REDEEM_SELECTOR)
        assert sent["data"].endswith(_encode_address(USER_ADDR))

    def test_transport_failure_is_not_reported_as_illiquidity(self):
        """A node outage during the dry-run must surface as RPCError, not as a liquidity verdict."""
        gw = _scripted_gateway(_v2_script())
        inner = gw.rpc.Call.side_effect

        def call(request, timeout=None):
            if json.loads(request.params)[0]["data"].startswith(REDEEM_SELECTOR):
                resp = MagicMock()
                resp.success = False
                resp.error = "UNAVAILABLE: connection timed out"
                resp.result = ""
                return resp
            return inner(request, timeout)

        gw.rpc.Call.side_effect = call
        sdk = MetaMorphoSDK(gw, "base")
        with pytest.raises(RPCError):
            sdk.simulate_redeem(VAULT_ADDR, 10**18, receiver=USER_ADDR, owner=USER_ADDR)

    def test_revert_surfaces_as_illiquid_and_names_the_escape_hatch(self):
        sdk = MetaMorphoSDK(_scripted_gateway(_v2_script()), "base")  # redeem selector unscripted -> revert
        with pytest.raises(VaultIlliquidError) as excinfo:
            sdk.simulate_redeem(VAULT_ADDR, 10**18, receiver=USER_ADDR, owner=USER_ADDR)
        assert "forceDeallocate" in str(excinfo.value)
        assert "NOT sent" in str(excinfo.value)


class TestGates:
    def _gate_word(self, address):
        return "0x" + "0" * 24 + address[2:].lower()

    def test_v1_vault_never_consults_gates(self):
        gw = _scripted_gateway(_v1_script())
        sdk = MetaMorphoSDK(gw, "base")
        sdk.check_deposit_gate(VAULT_ADDR, USER_ADDR)
        sdk.check_redeem_gates(VAULT_ADDR, USER_ADDR, USER_ADDR)
        assert gw.rpc.Call.call_count == 1  # only the fingerprint

    def test_v2_without_gates_is_open(self):
        script = _v2_script(
            {
                (VAULT_ADDR.lower(), "0x7e729ac4"): _WORD_ZERO,  # receive-shares gate unset
                (VAULT_ADDR.lower(), "0x93ab2ab7"): _WORD_ZERO,  # send-shares gate unset
                (VAULT_ADDR.lower(), "0x54cde13e"): _WORD_ZERO,  # receive-assets gate unset
                (VAULT_ADDR.lower(), V2_SEND_ASSETS_GATE_SELECTOR): _WORD_ZERO,  # send-assets gate unset
            }
        )
        sdk = MetaMorphoSDK(_scripted_gateway(script), "base")
        sdk.check_deposit_gate(VAULT_ADDR, USER_ADDR)
        sdk.check_redeem_gates(VAULT_ADDR, USER_ADDR, USER_ADDR)

    def test_v2_receive_gate_refusal_raises(self):
        script = _v2_script(
            {
                (VAULT_ADDR.lower(), "0x7e729ac4"): self._gate_word(_GATE_ADDR),
                (_GATE_ADDR.lower(), GATE_CAN_RECEIVE_SHARES_SELECTOR + _encode_address(USER_ADDR)): _WORD_ZERO,
            }
        )
        sdk = MetaMorphoSDK(_scripted_gateway(script), "base")
        with pytest.raises(VaultGatedError):
            sdk.check_deposit_gate(VAULT_ADDR, USER_ADDR)

    def test_v2_receive_gate_allow_passes(self):
        script = _v2_script(
            {
                (VAULT_ADDR.lower(), "0x7e729ac4"): self._gate_word(_GATE_ADDR),
                (_GATE_ADDR.lower(), GATE_CAN_RECEIVE_SHARES_SELECTOR + _encode_address(USER_ADDR)): _WORD_ONE,
            }
        )
        sdk = MetaMorphoSDK(_scripted_gateway(script), "base")
        sdk.check_deposit_gate(VAULT_ADDR, USER_ADDR)

    def test_v2_gate_that_does_not_answer_fails_closed(self):
        script = _v2_script({(VAULT_ADDR.lower(), "0x7e729ac4"): self._gate_word(_GATE_ADDR)})
        sdk = MetaMorphoSDK(_scripted_gateway(script), "base")
        with pytest.raises(VaultGatedError):
            sdk.check_deposit_gate(VAULT_ADDR, USER_ADDR)

    def test_v2_send_assets_gate_refusal_raises(self):
        script = _v2_script(
            {
                (VAULT_ADDR.lower(), V2_SEND_ASSETS_GATE_SELECTOR): self._gate_word(_GATE_ADDR),
                (VAULT_ADDR.lower(), "0x7e729ac4"): _WORD_ZERO,
                (_GATE_ADDR.lower(), GATE_CAN_SEND_ASSETS_SELECTOR + _encode_address(USER_ADDR)): _WORD_ZERO,
            }
        )
        sdk = MetaMorphoSDK(_scripted_gateway(script), "base")
        with pytest.raises(VaultGatedError, match="send-assets gate"):
            sdk.check_deposit_gate(VAULT_ADDR, USER_ADDR, USER_ADDR)

    def test_v2_send_assets_gate_allow_still_checks_receive_shares(self):
        script = _v2_script(
            {
                (VAULT_ADDR.lower(), V2_SEND_ASSETS_GATE_SELECTOR): self._gate_word(_GATE_ADDR),
                (_GATE_ADDR.lower(), GATE_CAN_SEND_ASSETS_SELECTOR + _encode_address(USER_ADDR)): _WORD_ONE,
                (VAULT_ADDR.lower(), "0x7e729ac4"): self._gate_word(_GATE_ADDR),
                (_GATE_ADDR.lower(), GATE_CAN_RECEIVE_SHARES_SELECTOR + _encode_address(USER_ADDR)): _WORD_ZERO,
            }
        )
        sdk = MetaMorphoSDK(_scripted_gateway(script), "base")
        with pytest.raises(VaultGatedError, match="receive-shares gate"):
            sdk.check_deposit_gate(VAULT_ADDR, USER_ADDR, USER_ADDR)

    def test_gate_address_transport_failure_is_not_unset(self):
        """A timeout reading the gate address must not proceed as 'no gate'."""
        gw = _scripted_gateway(_v2_script())
        inner = gw.rpc.Call.side_effect

        def call(request, timeout=None):
            data = json.loads(request.params)[0]["data"]
            if data == V2_SEND_ASSETS_GATE_SELECTOR or data == "0x7e729ac4":
                resp = MagicMock()
                resp.success = False
                resp.error = "UNAVAILABLE: connection timed out"
                resp.result = ""
                return resp
            return inner(request, timeout)

        gw.rpc.Call.side_effect = call
        sdk = MetaMorphoSDK(gw, "base")
        with pytest.raises(RPCError, match="timed out"):
            sdk.check_deposit_gate(VAULT_ADDR, USER_ADDR)

    def test_malformed_gate_address_fails_closed(self):
        script = _v2_script({(VAULT_ADDR.lower(), V2_SEND_ASSETS_GATE_SELECTOR): "0x00"})
        sdk = MetaMorphoSDK(_scripted_gateway(script), "base")
        with pytest.raises(RPCError, match="malformed gate address"):
            sdk.check_deposit_gate(VAULT_ADDR, USER_ADDR)


class TestVaultInfoV2:
    def test_get_vault_info_reads_v2_selectors_not_v1(self):
        adapter_addr = "0x" + "ad" * 20
        word = lambda addr: "0x" + "0" * 24 + addr[2:].lower()  # noqa: E731
        script = _v2_script(
            {
                (VAULT_ADDR.lower(), ASSET_SELECTOR): word(ASSET_ADDR),
                (VAULT_ADDR.lower(), TOTAL_ASSETS_SELECTOR): "0x" + _encode_uint256(429_000_000_000_000),
                (VAULT_ADDR.lower(), TOTAL_SUPPLY_SELECTOR): "0x" + _encode_uint256(413 * 10**24),
                (VAULT_ADDR.lower(), DECIMALS_SELECTOR): "0x" + _encode_uint256(18),
                (VAULT_ADDR.lower(), CONVERT_TO_ASSETS_SELECTOR): "0x" + _encode_uint256(1_039_759),
                (VAULT_ADDR.lower(), CURATOR_SELECTOR): word("0x" + "cc" * 20),
                (VAULT_ADDR.lower(), "0xad468d11"): word(adapter_addr),  # the liquidity adapter read
                (VAULT_ADDR.lower(), "0x87788782"): "0x" + _encode_uint256(0),  # performance fee read
                (VAULT_ADDR.lower(), "0xa6f7f5d6"): "0x" + _encode_uint256(0),  # management fee read
                (VAULT_ADDR.lower(), "0x4ef501ac" + _encode_uint256(0)): word(adapter_addr),  # first adapter entry
                (VAULT_ADDR.lower(), "0x99e99183" + _encode_address(adapter_addr)): "0x" + _encode_uint256(10**13),
            }
        )
        sdk = MetaMorphoSDK(_scripted_gateway(script), "base")
        info = sdk.get_vault_info(VAULT_ADDR)
        assert info.vault_version == VAULT_VERSION_V2
        assert info.timelock == 0
        assert info.management_fee == 0
        assert info.liquidity_adapter == adapter_addr
        assert info.adapters == [adapter_addr]
        assert info.force_deallocate_penalty == 10**13
        assert info.share_price == 1_039_759
