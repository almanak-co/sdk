"""Tests for LagoonVaultDeployer -- factory addresses, ABI encoding, calldata, receipt parsing."""

import json
from unittest.mock import MagicMock

import pytest
from eth_abi import decode as abi_decode

from almanak.framework.connectors.lagoon.deployer import (
    APPROVE_GAS_ESTIMATE,
    CREATE_VAULT_PROXY_SELECTOR,
    DEFAULT_LOGIC_SELECTOR,
    DEPLOY_GAS_ESTIMATE,
    ERC20_APPROVE_SELECTOR,
    FACTORY_ADDRESSES,
    INIT_STRUCT_ABI_TYPE,
    MAX_UINT256,
    MIN_DEPLOY_DELAY,
    PROXY_DEPLOYED_TOPIC,
    REGISTRY_SELECTOR,
    LagoonVaultDeployer,
    VaultDeployParams,
    VaultDeployResult,
)
from almanak.framework.connectors.lagoon.sdk import _encode_address, _encode_uint256

# --- Test Fixtures ---

SAFE_ADDRESS = "0x1111111111111111111111111111111111111111"
ADMIN_ADDRESS = "0x2222222222222222222222222222222222222222"
DEPLOYER_ADDRESS = "0x3333333333333333333333333333333333333333"
FEE_RECEIVER = "0x4444444444444444444444444444444444444444"
UNDERLYING_TOKEN = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"  # USDC on Base
VAULT_ADDRESS = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
LOGIC_ADDRESS = "0xE50554ec802375C9c3F9c087a8a7bb8C26d3DEDf"

DETERMINISTIC_SALT = bytes.fromhex("ab" * 32)


@pytest.fixture
def deployer():
    return LagoonVaultDeployer()


@pytest.fixture
def base_params():
    """Deploy params for Base with explicit logic address (no RPC needed)."""
    return VaultDeployParams(
        chain="base",
        underlying_token_address=UNDERLYING_TOKEN,
        name="Test Vault",
        symbol="tVLT",
        safe_address=SAFE_ADDRESS,
        admin_address=ADMIN_ADDRESS,
        fee_receiver_address=FEE_RECEIVER,
        deployer_address=DEPLOYER_ADDRESS,
        logic_address=LOGIC_ADDRESS,
        salt=DETERMINISTIC_SALT,
    )


@pytest.fixture
def arb_params():
    """Deploy params for Arbitrum with explicit logic address."""
    return VaultDeployParams(
        chain="arbitrum",
        underlying_token_address=UNDERLYING_TOKEN,
        name="Arb Vault",
        symbol="aVLT",
        safe_address=SAFE_ADDRESS,
        admin_address=ADMIN_ADDRESS,
        fee_receiver_address=FEE_RECEIVER,
        deployer_address=DEPLOYER_ADDRESS,
        logic_address=LOGIC_ADDRESS,
        salt=DETERMINISTIC_SALT,
    )


# --- Factory Address Tests ---


class TestFactoryAddresses:
    def test_known_chains_have_addresses(self):
        expected_chains = {"ethereum", "arbitrum", "base", "avalanche", "sonic", "plasma", "polygon", "optimism"}
        assert set(FACTORY_ADDRESSES.keys()) == expected_chains

    def test_get_factory_address_base(self):
        address = LagoonVaultDeployer.get_factory_address("base")
        assert address == "0x6FC0F2320483fa03FBFdF626DDbAE2CC4B112b51"

    def test_get_factory_address_arbitrum(self):
        address = LagoonVaultDeployer.get_factory_address("arbitrum")
        assert address == "0x9De724B0efEe0FbA07FE21a16B9Bf9bBb5204Fb4"

    def test_get_factory_address_case_insensitive(self):
        address = LagoonVaultDeployer.get_factory_address("Ethereum")
        assert address == FACTORY_ADDRESSES["ethereum"]

    def test_get_factory_address_unsupported_chain(self):
        with pytest.raises(ValueError, match="No Lagoon factory configured for chain 'fantom'"):
            LagoonVaultDeployer.get_factory_address("fantom")

    def test_all_addresses_are_valid(self):
        for chain, address in FACTORY_ADDRESSES.items():
            assert address.startswith("0x"), f"{chain} factory address missing 0x prefix"
            assert len(address) == 42, f"{chain} factory address wrong length"


# --- VaultDeployParams Tests ---


class TestVaultDeployParams:
    def test_defaults(self, base_params):
        assert base_params.management_rate_bps == 200
        assert base_params.performance_rate_bps == 2000
        assert base_params.enable_whitelist is False
        assert base_params.rate_update_cooldown == 86400
        assert base_params.deploy_delay == 86400
        assert base_params.valuation_manager_address is None
        assert base_params.whitelist_manager_address is None

    def test_custom_rates(self):
        params = VaultDeployParams(
            chain="base",
            underlying_token_address=UNDERLYING_TOKEN,
            name="V",
            symbol="V",
            safe_address=SAFE_ADDRESS,
            admin_address=ADMIN_ADDRESS,
            fee_receiver_address=FEE_RECEIVER,
            deployer_address=DEPLOYER_ADDRESS,
            logic_address=LOGIC_ADDRESS,
            management_rate_bps=100,
            performance_rate_bps=1000,
        )
        assert params.management_rate_bps == 100
        assert params.performance_rate_bps == 1000


# --- Calldata Encoding Tests ---


class TestCalldata:
    def test_selector(self, deployer, base_params):
        tx = deployer.build_deploy_vault_tx(base_params)
        assert tx["data"].startswith(CREATE_VAULT_PROXY_SELECTOR)

    def test_tx_structure(self, deployer, base_params):
        tx = deployer.build_deploy_vault_tx(base_params)
        assert tx["to"] == FACTORY_ADDRESSES["base"]
        assert tx["from"] == DEPLOYER_ADDRESS
        assert tx["value"] == "0"
        assert tx["gas_estimate"] == DEPLOY_GAS_ESTIMATE

    def test_tx_targets_arbitrum_factory(self, deployer, arb_params):
        tx = deployer.build_deploy_vault_tx(arb_params)
        assert tx["to"] == FACTORY_ADDRESSES["arbitrum"]

    def test_all_chains_use_same_selector(self, deployer):
        """All chains use the unified createVaultProxy selector."""
        for chain in FACTORY_ADDRESSES:
            params = VaultDeployParams(
                chain=chain,
                underlying_token_address=UNDERLYING_TOKEN,
                name="V",
                symbol="V",
                safe_address=SAFE_ADDRESS,
                admin_address=ADMIN_ADDRESS,
                fee_receiver_address=FEE_RECEIVER,
                deployer_address=DEPLOYER_ADDRESS,
                logic_address=LOGIC_ADDRESS,
                salt=DETERMINISTIC_SALT,
            )
            tx = deployer.build_deploy_vault_tx(params)
            assert tx["data"].startswith(CREATE_VAULT_PROXY_SELECTOR), f"Chain {chain} has wrong selector"

    def test_decode_calldata_roundtrip(self, deployer, base_params):
        """ABI-decode the calldata back and verify all fields."""
        tx = deployer.build_deploy_vault_tx(base_params)
        # Strip selector (4 bytes = 8 hex chars + "0x" prefix)
        raw = bytes.fromhex(tx["data"][len(CREATE_VAULT_PROXY_SELECTOR):])

        decoded = abi_decode(
            ["address", "address", "uint256", INIT_STRUCT_ABI_TYPE, "bytes32"],
            raw,
        )

        logic, owner, delay, init_struct, salt = decoded
        assert logic.lower() == LOGIC_ADDRESS.lower()
        assert owner.lower() == SAFE_ADDRESS.lower()
        assert delay == MIN_DEPLOY_DELAY

        # InitStruct fields -- Lagoon v0.5.0 field order:
        # safe, whitelistManager, valuationManager, admin, feeReceiver
        assert init_struct[0].lower() == UNDERLYING_TOKEN.lower()  # underlying
        assert init_struct[1] == "Test Vault"  # name
        assert init_struct[2] == "tVLT"  # symbol
        assert init_struct[3].lower() == SAFE_ADDRESS.lower()  # safe
        assert init_struct[4].lower() == ADMIN_ADDRESS.lower()  # whitelistManager (defaults to admin)
        assert init_struct[5].lower() == ADMIN_ADDRESS.lower()  # valuationManager (defaults to admin)
        assert init_struct[6].lower() == ADMIN_ADDRESS.lower()  # admin
        assert init_struct[7].lower() == FEE_RECEIVER.lower()  # feeReceiver
        assert init_struct[8] == 200  # managementRate
        assert init_struct[9] == 2000  # performanceRate
        assert init_struct[10] is False  # enableWhitelist
        assert init_struct[11] == 86400  # rateUpdateCooldown

        assert salt == DETERMINISTIC_SALT

    def test_custom_valuation_manager(self, deployer, base_params):
        custom = "0x5555555555555555555555555555555555555555"
        base_params.valuation_manager_address = custom
        tx = deployer.build_deploy_vault_tx(base_params)
        raw = bytes.fromhex(tx["data"][len(CREATE_VAULT_PROXY_SELECTOR):])
        decoded = abi_decode(
            ["address", "address", "uint256", INIT_STRUCT_ABI_TYPE, "bytes32"],
            raw,
        )
        init_struct = decoded[3]
        assert init_struct[5].lower() == custom.lower()  # valuationManager is at index 5 in v0.5.0

    def test_deploy_delay_validation(self, deployer):
        params = VaultDeployParams(
            chain="base",
            underlying_token_address=UNDERLYING_TOKEN,
            name="V",
            symbol="V",
            safe_address=SAFE_ADDRESS,
            admin_address=ADMIN_ADDRESS,
            fee_receiver_address=FEE_RECEIVER,
            deployer_address=DEPLOYER_ADDRESS,
            logic_address=LOGIC_ADDRESS,
            deploy_delay=100,
        )
        with pytest.raises(ValueError, match="deploy_delay must be >= 86400"):
            deployer.build_deploy_vault_tx(params)

    def test_invalid_salt_length(self, deployer):
        params = VaultDeployParams(
            chain="base",
            underlying_token_address=UNDERLYING_TOKEN,
            name="V",
            symbol="V",
            safe_address=SAFE_ADDRESS,
            admin_address=ADMIN_ADDRESS,
            fee_receiver_address=FEE_RECEIVER,
            deployer_address=DEPLOYER_ADDRESS,
            logic_address=LOGIC_ADDRESS,
            salt=b"short",
        )
        with pytest.raises(ValueError, match="Salt must be exactly 32 bytes"):
            deployer.build_deploy_vault_tx(params)

    def test_random_salt_when_none(self, deployer):
        params = VaultDeployParams(
            chain="base",
            underlying_token_address=UNDERLYING_TOKEN,
            name="V",
            symbol="V",
            safe_address=SAFE_ADDRESS,
            admin_address=ADMIN_ADDRESS,
            fee_receiver_address=FEE_RECEIVER,
            deployer_address=DEPLOYER_ADDRESS,
            logic_address=LOGIC_ADDRESS,
            salt=None,
        )
        tx1 = deployer.build_deploy_vault_tx(params)
        tx2 = deployer.build_deploy_vault_tx(params)
        # Random salt should differ (with overwhelming probability)
        assert tx1["data"] != tx2["data"]

    def test_deterministic_encoding(self, deployer, base_params):
        """Same params should produce same encoding."""
        tx1 = deployer.build_deploy_vault_tx(base_params)
        tx2 = deployer.build_deploy_vault_tx(base_params)
        assert tx1["data"] == tx2["data"]


# --- Deploy Receipt Parsing Tests ---


class TestDeployReceiptParsing:
    def test_proxy_deployed_event_in_data(self):
        """ProxyDeployed event has vault address in data (not topics)."""
        # Data: address proxy (32 bytes) + address deployer (32 bytes)
        vault_padded = "000000000000000000000000" + VAULT_ADDRESS[2:]
        deployer_padded = "000000000000000000000000" + DEPLOYER_ADDRESS[2:]
        receipt = {
            "status": "0x1",
            "transactionHash": "0xabc123",
            "logs": [
                {
                    "topics": [PROXY_DEPLOYED_TOPIC],
                    "data": "0x" + vault_padded + deployer_padded,
                }
            ],
        }
        result = LagoonVaultDeployer.parse_deploy_receipt(receipt)
        assert result.success is True
        assert result.vault_address == VAULT_ADDRESS
        assert result.transaction_hash == "0xabc123"

    def test_int_status(self):
        vault_padded = "000000000000000000000000" + VAULT_ADDRESS[2:]
        deployer_padded = "000000000000000000000000" + DEPLOYER_ADDRESS[2:]
        receipt = {
            "status": 1,
            "transactionHash": "0xdef456",
            "logs": [
                {
                    "topics": [PROXY_DEPLOYED_TOPIC],
                    "data": "0x" + vault_padded + deployer_padded,
                }
            ],
        }
        result = LagoonVaultDeployer.parse_deploy_receipt(receipt)
        assert result.success is True
        assert result.vault_address == VAULT_ADDRESS

    def test_reverted_transaction(self):
        receipt = {"status": "0x0", "transactionHash": "0xfail", "logs": []}
        result = LagoonVaultDeployer.parse_deploy_receipt(receipt)
        assert result.success is False
        assert result.error == "Transaction reverted"

    def test_missing_event(self):
        receipt = {
            "status": "0x1",
            "transactionHash": "0xno_event",
            "logs": [{"topics": ["0x1234"], "data": "0x"}],
        }
        result = LagoonVaultDeployer.parse_deploy_receipt(receipt)
        assert result.success is False
        assert "No vault deployment event found" in result.error

    def test_empty_logs(self):
        receipt = {"status": "0x1", "transactionHash": "0xempty", "logs": []}
        result = LagoonVaultDeployer.parse_deploy_receipt(receipt)
        assert result.success is False

    def test_transaction_hash_field_variants(self):
        vault_padded = "000000000000000000000000" + VAULT_ADDRESS[2:]
        deployer_padded = "0" * 64
        receipt = {
            "status": 1,
            "transaction_hash": "0xsnake_case",
            "logs": [
                {
                    "topics": [PROXY_DEPLOYED_TOPIC],
                    "data": "0x" + vault_padded + deployer_padded,
                }
            ],
        }
        result = LagoonVaultDeployer.parse_deploy_receipt(receipt)
        assert result.transaction_hash == "0xsnake_case"

    def test_short_data_skipped(self):
        """Data too short to contain an address should not match."""
        receipt = {
            "status": "0x1",
            "transactionHash": "0xshort",
            "logs": [
                {
                    "topics": [PROXY_DEPLOYED_TOPIC],
                    "data": "0x1234",
                }
            ],
        }
        result = LagoonVaultDeployer.parse_deploy_receipt(receipt)
        assert result.success is False


# --- Approve Underlying Tests ---


class TestApproveUnderlying:
    def test_tx_structure(self):
        tx = LagoonVaultDeployer.build_approve_underlying_tx(UNDERLYING_TOKEN, VAULT_ADDRESS, SAFE_ADDRESS)
        assert tx["to"] == UNDERLYING_TOKEN
        assert tx["from"] == SAFE_ADDRESS
        assert tx["value"] == "0"
        assert tx["gas_estimate"] == APPROVE_GAS_ESTIMATE

    def test_approve_selector(self):
        tx = LagoonVaultDeployer.build_approve_underlying_tx(UNDERLYING_TOKEN, VAULT_ADDRESS, SAFE_ADDRESS)
        assert tx["data"].startswith(ERC20_APPROVE_SELECTOR)

    def test_spender_is_vault(self):
        tx = LagoonVaultDeployer.build_approve_underlying_tx(UNDERLYING_TOKEN, VAULT_ADDRESS, SAFE_ADDRESS)
        calldata = tx["data"][len(ERC20_APPROVE_SELECTOR):]
        spender = calldata[0:64]
        assert VAULT_ADDRESS.lower()[2:] in spender

    def test_amount_is_max_uint256(self):
        tx = LagoonVaultDeployer.build_approve_underlying_tx(UNDERLYING_TOKEN, VAULT_ADDRESS, SAFE_ADDRESS)
        calldata = tx["data"][len(ERC20_APPROVE_SELECTOR):]
        amount = int(calldata[64:128], 16)
        assert amount == MAX_UINT256


# --- ActionBundle Tests ---


class TestDeployBundle:
    def test_bundle_intent_type(self, deployer, base_params):
        bundle = deployer.build_deploy_vault_bundle(base_params)
        assert bundle.intent_type == "DEPLOY_LAGOON_VAULT"

    def test_bundle_has_one_transaction(self, deployer, base_params):
        bundle = deployer.build_deploy_vault_bundle(base_params)
        assert len(bundle.transactions) == 1

    def test_bundle_metadata(self, deployer, base_params):
        bundle = deployer.build_deploy_vault_bundle(base_params)
        assert bundle.metadata["chain"] == "base"
        assert bundle.metadata["vault_name"] == "Test Vault"
        assert bundle.metadata["vault_symbol"] == "tVLT"


class TestPostDeployBundle:
    def test_bundle_intent_type(self):
        bundle = LagoonVaultDeployer.build_post_deploy_bundle(UNDERLYING_TOKEN, VAULT_ADDRESS, SAFE_ADDRESS)
        assert bundle.intent_type == "APPROVE_VAULT_UNDERLYING"

    def test_bundle_metadata(self):
        bundle = LagoonVaultDeployer.build_post_deploy_bundle(UNDERLYING_TOKEN, VAULT_ADDRESS, SAFE_ADDRESS)
        assert bundle.metadata["vault_address"] == VAULT_ADDRESS
        assert bundle.metadata["safe_address"] == SAFE_ADDRESS


# --- Get Default Logic Tests ---


class TestGetDefaultLogic:
    def test_requires_gateway_client(self):
        deployer = LagoonVaultDeployer(gateway_client=None)
        with pytest.raises(RuntimeError, match="Gateway client required"):
            deployer.get_default_logic("base")

    def test_reads_registry_then_logic(self):
        """get_default_logic makes two RPC calls: registry() then defaultLogic()."""
        mock_client = MagicMock()
        registry_addr = "6dA4D1859bA1d02D095D2246142CdAd52233e27C"
        logic_addr = "E50554ec802375C9c3F9c087a8a7bb8C26d3DEDf"

        # First call returns registry, second returns logic
        registry_response = MagicMock()
        registry_response.success = True
        registry_response.result = json.dumps("0x" + "0" * 24 + registry_addr)

        logic_response = MagicMock()
        logic_response.success = True
        logic_response.result = json.dumps("0x" + "0" * 24 + logic_addr)

        mock_client.rpc.Call.side_effect = [registry_response, logic_response]

        deployer = LagoonVaultDeployer(gateway_client=mock_client)
        result = deployer.get_default_logic("base")

        assert result == "0x" + logic_addr
        assert mock_client.rpc.Call.call_count == 2

    def test_rpc_failure(self):
        mock_client = MagicMock()
        response = MagicMock()
        response.success = False
        response.error = "timeout"
        mock_client.rpc.Call.return_value = response

        deployer = LagoonVaultDeployer(gateway_client=mock_client)
        with pytest.raises(RuntimeError, match="RPC call failed"):
            deployer.get_default_logic("ethereum")

    def test_build_tx_without_logic_uses_zero_address(self, deployer):
        """build_deploy_vault_tx without logic_address passes address(0) to factory."""
        params = VaultDeployParams(
            chain="base",
            underlying_token_address=UNDERLYING_TOKEN,
            name="V",
            symbol="V",
            safe_address=SAFE_ADDRESS,
            admin_address=ADMIN_ADDRESS,
            fee_receiver_address=FEE_RECEIVER,
            deployer_address=DEPLOYER_ADDRESS,
            salt=DETERMINISTIC_SALT,
            # logic_address=None (default)
        )
        tx = deployer.build_deploy_vault_tx(params)
        # Decode and verify logic is zero address
        raw = bytes.fromhex(tx["data"][len(CREATE_VAULT_PROXY_SELECTOR):])
        decoded = abi_decode(
            ["address", "address", "uint256", INIT_STRUCT_ABI_TYPE, "bytes32"],
            raw,
        )
        assert decoded[0] == "0x0000000000000000000000000000000000000000"
