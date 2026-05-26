"""Unit tests for Polymarket CTF SDK.

Tests cover:
- Transaction building (approvals, split, merge, redeem)
- Token ID calculation
- Allowance checking with mocked web3
- Resolution status checking
"""

from unittest.mock import MagicMock

import pytest
from eth_abi import decode as abi_decode
from web3 import Web3

from almanak.connectors.polymarket.ctf_sdk import (
    BINARY_PARTITION,
    GAS_ESTIMATES,
    INDEX_SET_NO,
    INDEX_SET_YES,
    MAX_UINT256,
    ZERO_BYTES32,
    AllowanceStatus,
    CtfSDK,
    ResolutionStatus,
    TransactionData,
)
from almanak.connectors.polymarket.models import (
    CONDITIONAL_TOKENS,
    CTF_EXCHANGE_V2,
    NEG_RISK_ADAPTER,
    NEG_RISK_EXCHANGE_V2,
    PUSD,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sdk() -> CtfSDK:
    """Create a CtfSDK instance for testing."""
    return CtfSDK()


@pytest.fixture
def wallet_address() -> str:
    """Test wallet address."""
    return "0x742d35Cc6634C0532925a3b844Bc9e7595f2bD68"


@pytest.fixture
def condition_id() -> str:
    """Test condition ID."""
    return "0x9915bea232fa12b20058f9cea1187ea51366352bf833393676cd0db557a58249"


@pytest.fixture
def mock_web3() -> MagicMock:
    """Create a mock web3 instance."""
    web3 = MagicMock()
    web3.eth.contract.return_value = MagicMock()
    return web3


# =============================================================================
# SDK Initialization Tests
# =============================================================================


class TestCtfSDKInit:
    """Tests for CtfSDK initialization."""

    def test_default_initialization(self, sdk: CtfSDK) -> None:
        """Test SDK initializes with default contract addresses."""
        assert sdk.ctf_exchange == CTF_EXCHANGE_V2
        assert sdk.neg_risk_exchange == NEG_RISK_EXCHANGE_V2
        assert sdk.conditional_tokens == CONDITIONAL_TOKENS
        assert sdk.neg_risk_adapter == NEG_RISK_ADAPTER
        assert sdk.pusd == PUSD
        assert sdk.chain_id == 137

    def test_custom_addresses(self) -> None:
        """Test SDK with custom contract addresses."""
        custom_exchange = "0x1234567890123456789012345678901234567890"
        sdk = CtfSDK(ctf_exchange=custom_exchange)
        assert sdk.ctf_exchange.lower() == custom_exchange.lower()

    def test_abis_loaded(self, sdk: CtfSDK) -> None:
        """Test that ABIs are loaded."""
        assert len(sdk._erc20_abi) > 0
        assert len(sdk._erc1155_abi) > 0
        assert len(sdk._conditional_tokens_abi) > 0
        # V2: dropped V1 _ctf_exchange_abi (never used; off-chain CLOB).
        # Added Onramp/Offramp ABIs for collateral wrap/unwrap.
        assert len(sdk._collateral_onramp_abi) > 0
        assert len(sdk._collateral_offramp_abi) > 0


# =============================================================================
# Transaction Building Tests
# =============================================================================


class TestBuildApproveCollateralTx:
    """Tests for V2 collateral approval transaction building."""

    def test_approve_pusd_to_ctf_exchange(self, sdk: CtfSDK, wallet_address: str) -> None:
        """Approve pUSD spending to CTF Exchange V2."""
        tx = sdk.build_approve_collateral_tx(
            asset=PUSD,
            spender=CTF_EXCHANGE_V2,
            sender=wallet_address,
            amount=MAX_UINT256,
        )

        assert isinstance(tx, TransactionData)
        assert tx.to.lower() == PUSD.lower()
        assert tx.data.startswith("0x095ea7b3")  # approve(address,uint256)
        assert tx.gas_estimate == GAS_ESTIMATES["approve_erc20"]
        assert tx.value == 0

    def test_approve_source_asset_to_onramp(self, sdk: CtfSDK, wallet_address: str) -> None:
        """Approve source asset (USDC.e) to CollateralOnramp."""
        tx = sdk.build_approve_collateral_tx(
            asset=sdk.source_asset,
            spender=sdk.collateral_onramp,
            sender=wallet_address,
        )
        assert tx.to.lower() == sdk.source_asset.lower()
        assert tx.data.startswith("0x095ea7b3")

    def test_to_tx_params(self, sdk: CtfSDK, wallet_address: str) -> None:
        """TransactionData → web3 params."""
        tx = sdk.build_approve_collateral_tx(asset=PUSD, spender=CTF_EXCHANGE_V2, sender=wallet_address)

        params = tx.to_tx_params(wallet_address)
        assert params["from"] == wallet_address
        assert params["to"].lower() == PUSD.lower()
        assert params["data"].hex() == tx.data[2:]
        assert params["value"] == 0
        assert params["gas"] == tx.gas_estimate


class TestBuildWrapUnwrapTx:
    """Tests for V2 collateral ramp tx builders."""

    def test_wrap_targets_onramp_with_correct_selector(self, sdk: CtfSDK, wallet_address: str) -> None:
        tx = sdk.build_wrap_to_pusd_tx(wallet_address, amount=1_000_000)
        assert tx.to.lower() == sdk.collateral_onramp.lower()
        # wrap(address,address,uint256) selector
        assert tx.data.startswith("0x62355638")
        assert tx.gas_estimate == GAS_ESTIMATES["wrap"]

    def test_unwrap_targets_offramp_with_correct_selector(self, sdk: CtfSDK, wallet_address: str) -> None:
        tx = sdk.build_unwrap_from_pusd_tx(wallet_address, amount=1_000_000)
        assert tx.to.lower() == sdk.collateral_offramp.lower()
        # unwrap(address,address,uint256) selector
        assert tx.data.startswith("0x8cc7104f")
        assert tx.gas_estimate == GAS_ESTIMATES["unwrap"]

    def test_wrap_payload_encodes_args_in_correct_order(self, sdk: CtfSDK, wallet_address: str) -> None:
        """abi-decode the calldata and assert (asset, recipient, amount) round-trip.

        Selector-only checks would let an arg-swap regression slip — e.g. the
        Onramp ABI is `wrap(address asset, address to, uint256 amount)` and a
        mistaken `(to, asset, amount)` ordering would still match the selector
        but mint pUSD into the contract address. This pins the actual encoding.
        """
        amount = 1_234_567
        tx = sdk.build_wrap_to_pusd_tx(wallet_address, amount=amount)

        payload = bytes.fromhex(tx.data[10:])  # strip "0x" + 4-byte selector
        asset, recipient, decoded_amount = abi_decode(["address", "address", "uint256"], payload)

        assert Web3.to_checksum_address(asset) == sdk.source_asset
        assert Web3.to_checksum_address(recipient) == Web3.to_checksum_address(wallet_address)
        assert decoded_amount == amount

    def test_unwrap_payload_encodes_args_in_correct_order(self, sdk: CtfSDK, wallet_address: str) -> None:
        """Mirror of the wrap encoding test — guards Offramp.unwrap arg order."""
        amount = 9_876_543
        tx = sdk.build_unwrap_from_pusd_tx(wallet_address, amount=amount)

        payload = bytes.fromhex(tx.data[10:])
        asset, recipient, decoded_amount = abi_decode(["address", "address", "uint256"], payload)

        assert Web3.to_checksum_address(asset) == sdk.source_asset
        assert Web3.to_checksum_address(recipient) == Web3.to_checksum_address(wallet_address)
        assert decoded_amount == amount

    def test_wrap_honours_explicit_source_asset_override(self, sdk: CtfSDK, wallet_address: str) -> None:
        """Callers may pass a non-default source (e.g. native USDC after the
        Onramp pause flips). The override must end up in the encoded payload."""
        native_usdc = Web3.to_checksum_address("0x3c499c542cef5e3811e1192ce70d8cc03d5c3359")
        tx = sdk.build_wrap_to_pusd_tx(wallet_address, amount=1_000_000, source_asset=native_usdc)

        payload = bytes.fromhex(tx.data[10:])
        asset, _, _ = abi_decode(["address", "address", "uint256"], payload)
        assert Web3.to_checksum_address(asset) == native_usdc


class TestBuildApproveConditionalTokensTx:
    """Tests for ERC-1155 approval transaction building."""

    def test_builds_valid_approval(self, sdk: CtfSDK, wallet_address: str) -> None:
        """Test building ERC-1155 setApprovalForAll transaction."""
        tx = sdk.build_approve_conditional_tokens_tx(
            operator=CTF_EXCHANGE_V2,
            approved=True,
            sender=wallet_address,
        )

        assert isinstance(tx, TransactionData)
        assert tx.to.lower() == CONDITIONAL_TOKENS.lower()
        assert tx.data.startswith("0x")
        assert tx.gas_estimate == GAS_ESTIMATES["approve_erc1155"]
        assert "Approve" in tx.description

    def test_builds_revoke(self, sdk: CtfSDK, wallet_address: str) -> None:
        """Test building revocation transaction."""
        tx = sdk.build_approve_conditional_tokens_tx(
            operator=CTF_EXCHANGE_V2,
            approved=False,
            sender=wallet_address,
        )

        assert "Revoke" in tx.description

    def test_encodes_setApprovalForAll(self, sdk: CtfSDK, wallet_address: str) -> None:
        """Test that setApprovalForAll selector is correct."""
        tx = sdk.build_approve_conditional_tokens_tx(
            operator=CTF_EXCHANGE_V2,
            approved=True,
            sender=wallet_address,
        )

        # setApprovalForAll(address,bool) selector = 0xa22cb465
        assert tx.data.startswith("0xa22cb465")


class TestBuildSplitTx:
    """Tests for split position transaction building."""

    def test_builds_valid_split(self, sdk: CtfSDK, wallet_address: str, condition_id: str) -> None:
        """Test building split transaction."""
        amount = 1000000  # 1 USDC

        tx = sdk.build_split_tx(
            condition_id=condition_id,
            amount=amount,
            sender=wallet_address,
        )

        assert isinstance(tx, TransactionData)
        assert tx.to.lower() == CONDITIONAL_TOKENS.lower()
        assert tx.data.startswith("0x")
        assert tx.gas_estimate == GAS_ESTIMATES["split_position"]
        assert "Split" in tx.description

    def test_accepts_bytes_condition_id(self, sdk: CtfSDK, wallet_address: str, condition_id: str) -> None:
        """Test split with bytes condition ID."""
        condition_bytes = bytes.fromhex(condition_id.replace("0x", ""))

        tx = sdk.build_split_tx(
            condition_id=condition_bytes,
            amount=1000000,
            sender=wallet_address,
        )

        assert tx.data.startswith("0x")


class TestBuildMergeTx:
    """Tests for merge positions transaction building."""

    def test_builds_valid_merge(self, sdk: CtfSDK, wallet_address: str, condition_id: str) -> None:
        """Test building merge transaction."""
        amount = 1000000

        tx = sdk.build_merge_tx(
            condition_id=condition_id,
            amount=amount,
            sender=wallet_address,
        )

        assert isinstance(tx, TransactionData)
        assert tx.to.lower() == CONDITIONAL_TOKENS.lower()
        assert tx.data.startswith("0x")
        assert tx.gas_estimate == GAS_ESTIMATES["merge_positions"]
        assert "Merge" in tx.description


class TestBuildRedeemTx:
    """Tests for redeem positions transaction building."""

    def test_builds_valid_redeem(self, sdk: CtfSDK, wallet_address: str, condition_id: str) -> None:
        """Test building redeem transaction."""
        tx = sdk.build_redeem_tx(
            condition_id=condition_id,
            index_sets=BINARY_PARTITION,
            sender=wallet_address,
        )

        assert isinstance(tx, TransactionData)
        assert tx.to.lower() == CONDITIONAL_TOKENS.lower()
        assert tx.data.startswith("0x")
        assert tx.gas_estimate == GAS_ESTIMATES["redeem_positions"]
        assert "Redeem" in tx.description

    def test_redeem_single_outcome(self, sdk: CtfSDK, wallet_address: str, condition_id: str) -> None:
        """Test redeeming only YES tokens."""
        tx = sdk.build_redeem_tx(
            condition_id=condition_id,
            index_sets=[INDEX_SET_YES],
            sender=wallet_address,
        )

        assert tx.data.startswith("0x")


# =============================================================================
# Position ID Calculation Tests
# =============================================================================


class TestPositionIdCalculation:
    """Tests for position ID calculation."""

    def test_get_collection_id(self, sdk: CtfSDK, condition_id: str) -> None:
        """Test collection ID calculation."""
        condition_bytes = bytes.fromhex(condition_id.replace("0x", ""))

        yes_collection = sdk.get_collection_id(condition_bytes, INDEX_SET_YES)
        no_collection = sdk.get_collection_id(condition_bytes, INDEX_SET_NO)

        # Collection IDs should be 32 bytes
        assert len(yes_collection) == 32
        assert len(no_collection) == 32

        # YES and NO should have different collection IDs
        assert yes_collection != no_collection

    def test_get_position_id(self, sdk: CtfSDK) -> None:
        """Test position ID calculation."""
        collection_id = bytes(32)  # Zero collection

        position_id = sdk.get_position_id(PUSD, collection_id)

        # Position ID should be a large integer
        assert isinstance(position_id, int)
        assert position_id >= 0

    def test_get_token_ids_for_condition(self, sdk: CtfSDK, condition_id: str) -> None:
        """Test getting YES/NO token IDs for a condition."""
        yes_token_id, no_token_id = sdk.get_token_ids_for_condition(condition_id)

        # Token IDs should be large integers
        assert isinstance(yes_token_id, int)
        assert isinstance(no_token_id, int)

        # YES and NO should have different token IDs
        assert yes_token_id != no_token_id

    def test_token_ids_deterministic(self, sdk: CtfSDK, condition_id: str) -> None:
        """Test that token ID calculation is deterministic."""
        yes1, no1 = sdk.get_token_ids_for_condition(condition_id)
        yes2, no2 = sdk.get_token_ids_for_condition(condition_id)

        assert yes1 == yes2
        assert no1 == no2


# =============================================================================
# Allowance Checking Tests
# =============================================================================


class TestCheckAllowances:
    """Tests for allowance checking with mocked web3."""

    def test_check_allowances_all_approved(self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock) -> None:
        """Test checking allowances when all are approved (V2 6-tx coverage)."""
        # V2 queries 3 contracts: source asset (USDC.e), pUSD, CTF.
        source_contract = MagicMock()
        source_contract.functions.balanceOf.return_value.call.return_value = 1000000
        source_contract.functions.allowance.return_value.call.return_value = MAX_UINT256

        pusd_contract = MagicMock()
        pusd_contract.functions.balanceOf.return_value.call.return_value = 500000
        pusd_contract.functions.allowance.return_value.call.return_value = MAX_UINT256

        ctf_contract = MagicMock()
        ctf_contract.functions.isApprovedForAll.return_value.call.return_value = True

        mock_web3.eth.contract.side_effect = [source_contract, pusd_contract, ctf_contract]

        status = sdk.check_allowances(wallet_address, mock_web3)

        assert isinstance(status, AllowanceStatus)
        assert status.source_asset_balance == 1000000
        assert status.pusd_balance == 500000
        assert status.source_asset_approved_onramp is True
        assert status.pusd_approved_ctf_exchange is True
        assert status.pusd_approved_neg_risk_exchange is True
        assert status.pusd_approved_neg_risk_adapter is True
        assert status.ctf_approved_for_ctf_exchange is True
        assert status.ctf_approved_for_neg_risk_adapter is True
        assert status.fully_approved is True

    def test_check_allowances_none_approved(self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock) -> None:
        """Test checking allowances when none are approved."""
        source_contract = MagicMock()
        source_contract.functions.balanceOf.return_value.call.return_value = 1000000
        source_contract.functions.allowance.return_value.call.return_value = 0

        pusd_contract = MagicMock()
        pusd_contract.functions.balanceOf.return_value.call.return_value = 0
        pusd_contract.functions.allowance.return_value.call.return_value = 0

        ctf_contract = MagicMock()
        ctf_contract.functions.isApprovedForAll.return_value.call.return_value = False

        mock_web3.eth.contract.side_effect = [source_contract, pusd_contract, ctf_contract]

        status = sdk.check_allowances(wallet_address, mock_web3)

        assert status.source_asset_approved_onramp is False
        assert status.pusd_approved_ctf_exchange is False
        assert status.pusd_approved_neg_risk_exchange is False
        assert status.pusd_approved_neg_risk_adapter is False
        assert status.ctf_approved_for_ctf_exchange is False
        assert status.ctf_approved_for_neg_risk_adapter is False
        assert status.fully_approved is False


class TestEnsureAllowances:
    """Tests for ensure_allowances method."""

    def test_returns_empty_when_approved(self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock) -> None:
        """Test that no transactions needed when already approved."""
        source_contract = MagicMock()
        source_contract.functions.balanceOf.return_value.call.return_value = 1000000
        source_contract.functions.allowance.return_value.call.return_value = MAX_UINT256

        pusd_contract = MagicMock()
        pusd_contract.functions.balanceOf.return_value.call.return_value = 500000
        pusd_contract.functions.allowance.return_value.call.return_value = MAX_UINT256

        ctf_contract = MagicMock()
        ctf_contract.functions.isApprovedForAll.return_value.call.return_value = True

        mock_web3.eth.contract.side_effect = [source_contract, pusd_contract, ctf_contract]

        transactions = sdk.ensure_allowances(wallet_address, mock_web3)

        assert transactions == []

    def test_returns_all_approvals_when_none_approved(
        self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock
    ) -> None:
        """Test that all V2 approval transactions returned when none approved."""
        source_contract = MagicMock()
        source_contract.functions.balanceOf.return_value.call.return_value = 1000000
        source_contract.functions.allowance.return_value.call.return_value = 0

        pusd_contract = MagicMock()
        pusd_contract.functions.balanceOf.return_value.call.return_value = 0
        pusd_contract.functions.allowance.return_value.call.return_value = 0

        ctf_contract = MagicMock()
        ctf_contract.functions.isApprovedForAll.return_value.call.return_value = False

        mock_web3.eth.contract.side_effect = [source_contract, pusd_contract, ctf_contract]

        transactions = sdk.ensure_allowances(wallet_address, mock_web3)

        # V2 6-tx set: source→Onramp, pUSD→CTFv2, pUSD→NegRiskv2,
        # pUSD→NegRiskAdapter, CTF→CTFv2, CTF→NegRiskAdapter.
        assert len(transactions) == 6
        assert all(isinstance(tx, TransactionData) for tx in transactions)

    def test_emits_approvals_in_canonical_order(self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock) -> None:
        """The 6-tx set must come out in dependency order.

        The first wrap call needs source→Onramp to land first; subsequent
        BUY fills need pUSD→exchange approvals before the matcher pulls
        collateral. A swap (e.g. CTF approvals before the source approval)
        would break the very first auto-setup pass against a fresh wallet.
        """
        source_contract = MagicMock()
        source_contract.functions.balanceOf.return_value.call.return_value = 0
        source_contract.functions.allowance.return_value.call.return_value = 0

        pusd_contract = MagicMock()
        pusd_contract.functions.balanceOf.return_value.call.return_value = 0
        pusd_contract.functions.allowance.return_value.call.return_value = 0

        ctf_contract = MagicMock()
        ctf_contract.functions.isApprovedForAll.return_value.call.return_value = False

        mock_web3.eth.contract.side_effect = [source_contract, pusd_contract, ctf_contract]

        txs = sdk.ensure_allowances(wallet_address, mock_web3)

        # Targets — leg 1: source asset; legs 2-4: pUSD; legs 5-6: CTF (ERC-1155).
        assert [tx.to.lower() for tx in txs] == [
            sdk.source_asset.lower(),
            sdk.pusd.lower(),
            sdk.pusd.lower(),
            sdk.pusd.lower(),
            sdk.conditional_tokens.lower(),
            sdk.conditional_tokens.lower(),
        ]

        # Spender of each ERC-20 approval (decoded from calldata).
        for i, expected_spender in enumerate(
            [sdk.collateral_onramp, sdk.ctf_exchange, sdk.neg_risk_exchange, sdk.neg_risk_adapter]
        ):
            payload = bytes.fromhex(txs[i].data[10:])
            spender, _amount = abi_decode(["address", "uint256"], payload)
            assert Web3.to_checksum_address(spender) == expected_spender, (
                f"Approval #{i} spender mismatch: expected {expected_spender}, got {spender}"
            )

        # Operator on each ERC-1155 setApprovalForAll (CTF→CTFv2, CTF→Adapter).
        for i, expected_operator in enumerate([sdk.ctf_exchange, sdk.neg_risk_adapter], start=4):
            payload = bytes.fromhex(txs[i].data[10:])
            operator, approved = abi_decode(["address", "bool"], payload)
            assert Web3.to_checksum_address(operator) == expected_operator
            assert approved is True


# =============================================================================
# Token Balance Tests
# =============================================================================


class TestGetTokenBalance:
    """Tests for token balance queries."""

    def test_get_token_balance(self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock) -> None:
        """Test getting single token balance."""
        ctf_contract = MagicMock()
        ctf_contract.functions.balanceOf.return_value.call.return_value = 5000000

        mock_web3.eth.contract.return_value = ctf_contract

        balance = sdk.get_token_balance(wallet_address, 12345, mock_web3)

        assert balance == 5000000
        ctf_contract.functions.balanceOf.assert_called_once()

    def test_get_token_balances_batch(self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock) -> None:
        """Test getting multiple token balances."""
        ctf_contract = MagicMock()
        ctf_contract.functions.balanceOfBatch.return_value.call.return_value = [
            1000000,
            2000000,
            3000000,
        ]

        mock_web3.eth.contract.return_value = ctf_contract

        balances = sdk.get_token_balances(wallet_address, [111, 222, 333], mock_web3)

        assert balances == [1000000, 2000000, 3000000]

    def test_get_pusd_balance(self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock) -> None:
        """Test getting V2 trading collateral (pUSD) balance."""
        pusd_contract = MagicMock()
        pusd_contract.functions.balanceOf.return_value.call.return_value = 10000000

        mock_web3.eth.contract.return_value = pusd_contract

        balance = sdk.get_pusd_balance(wallet_address, mock_web3)

        assert balance == 10000000

    def test_get_source_asset_balance(self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock) -> None:
        """Test getting source-asset (USDC.e) balance."""
        source_contract = MagicMock()
        source_contract.functions.balanceOf.return_value.call.return_value = 7_500_000

        mock_web3.eth.contract.return_value = source_contract

        balance = sdk.get_source_asset_balance(wallet_address, mock_web3)

        assert balance == 7_500_000


# =============================================================================
# Resolution Status Tests
# =============================================================================


class TestGetConditionResolution:
    """Tests for condition resolution checking."""

    def test_unresolved_condition(self, sdk: CtfSDK, condition_id: str, mock_web3: MagicMock) -> None:
        """Test checking unresolved condition."""
        ctf_contract = MagicMock()
        ctf_contract.functions.payoutDenominator.return_value.call.return_value = 0

        mock_web3.eth.contract.return_value = ctf_contract

        status = sdk.get_condition_resolution(condition_id, mock_web3)

        assert isinstance(status, ResolutionStatus)
        assert status.is_resolved is False
        assert status.payout_denominator == 0
        assert status.payout_numerators == []
        assert status.winning_outcome is None

    def test_resolved_yes_wins(self, sdk: CtfSDK, condition_id: str, mock_web3: MagicMock) -> None:
        """Test checking condition resolved to YES."""
        ctf_contract = MagicMock()
        ctf_contract.functions.payoutDenominator.return_value.call.return_value = 1
        ctf_contract.functions.getOutcomeSlotCount.return_value.call.return_value = 2
        ctf_contract.functions.payoutNumerators.return_value.call.side_effect = [
            1,  # YES wins
            0,  # NO loses
        ]

        mock_web3.eth.contract.return_value = ctf_contract

        status = sdk.get_condition_resolution(condition_id, mock_web3)

        assert status.is_resolved is True
        assert status.payout_denominator == 1
        assert status.payout_numerators == [1, 0]
        assert status.winning_outcome == 0  # YES

    def test_resolved_no_wins(self, sdk: CtfSDK, condition_id: str, mock_web3: MagicMock) -> None:
        """Test checking condition resolved to NO."""
        ctf_contract = MagicMock()
        ctf_contract.functions.payoutDenominator.return_value.call.return_value = 1
        ctf_contract.functions.getOutcomeSlotCount.return_value.call.return_value = 2
        ctf_contract.functions.payoutNumerators.return_value.call.side_effect = [
            0,  # YES loses
            1,  # NO wins
        ]

        mock_web3.eth.contract.return_value = ctf_contract

        status = sdk.get_condition_resolution(condition_id, mock_web3)

        assert status.is_resolved is True
        assert status.payout_numerators == [0, 1]
        assert status.winning_outcome == 1  # NO

    def test_accepts_bytes_condition_id(self, sdk: CtfSDK, condition_id: str, mock_web3: MagicMock) -> None:
        """Test with bytes condition ID."""
        condition_bytes = bytes.fromhex(condition_id.replace("0x", ""))

        ctf_contract = MagicMock()
        ctf_contract.functions.payoutDenominator.return_value.call.return_value = 0

        mock_web3.eth.contract.return_value = ctf_contract

        status = sdk.get_condition_resolution(condition_bytes, mock_web3)

        assert status.condition_id == condition_id


# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for module constants."""

    def test_max_uint256(self) -> None:
        """Test MAX_UINT256 value."""
        assert MAX_UINT256 == 2**256 - 1

    def test_zero_bytes32(self) -> None:
        """Test ZERO_BYTES32 value."""
        assert len(ZERO_BYTES32) == 32
        assert ZERO_BYTES32 == b"\x00" * 32

    def test_index_sets(self) -> None:
        """Test index set constants."""
        assert INDEX_SET_YES == 1
        assert INDEX_SET_NO == 2
        assert BINARY_PARTITION == [1, 2]

    def test_gas_estimates(self) -> None:
        """Test gas estimates are reasonable."""
        assert GAS_ESTIMATES["approve_erc20"] >= 30_000
        assert GAS_ESTIMATES["approve_erc1155"] >= 30_000
        assert GAS_ESTIMATES["split_position"] >= 100_000
        assert GAS_ESTIMATES["merge_positions"] >= 100_000
        assert GAS_ESTIMATES["redeem_positions"] >= 100_000


# =============================================================================
# AllowanceStatus property + dataclass tests
# =============================================================================


class TestAllowanceStatusProperties:
    """The 6 V2 approval properties on AllowanceStatus must reflect the
    underlying allowance values exactly. ``fully_approved`` is ANDed across
    all six — any partial state must read as not fully approved."""

    @staticmethod
    def _status(**overrides) -> AllowanceStatus:
        defaults: dict = {
            "source_asset_balance": 0,
            "pusd_balance": 0,
            "source_asset_allowance_onramp": MAX_UINT256,
            "pusd_allowance_ctf_exchange": MAX_UINT256,
            "pusd_allowance_neg_risk_exchange": MAX_UINT256,
            "pusd_allowance_neg_risk_adapter": MAX_UINT256,
            "ctf_approved_for_ctf_exchange": True,
            "ctf_approved_for_neg_risk_adapter": True,
        }
        defaults.update(overrides)
        return AllowanceStatus(**defaults)

    def test_fully_approved_when_all_set(self) -> None:
        assert self._status().fully_approved is True

    @pytest.mark.parametrize(
        "missing_field",
        [
            "source_asset_allowance_onramp",
            "pusd_allowance_ctf_exchange",
            "pusd_allowance_neg_risk_exchange",
            "pusd_allowance_neg_risk_adapter",
        ],
    )
    def test_not_fully_approved_when_one_erc20_leg_missing(self, missing_field: str) -> None:
        """ERC-20 allowance == 0 → that property is False, fully_approved False."""
        status = self._status(**{missing_field: 0})
        assert status.fully_approved is False

    @pytest.mark.parametrize(
        "missing_field",
        ["ctf_approved_for_ctf_exchange", "ctf_approved_for_neg_risk_adapter"],
    )
    def test_not_fully_approved_when_one_erc1155_leg_missing(self, missing_field: str) -> None:
        """Either CTF operator approval missing → not fully approved."""
        status = self._status(**{missing_field: False})
        assert status.fully_approved is False

    def test_individual_property_flags(self) -> None:
        """Each property reflects its underlying allowance value vs. ``SUFFICIENT_ALLOWANCE_THRESHOLD``."""
        partial = self._status(
            source_asset_allowance_onramp=0,
            pusd_allowance_ctf_exchange=MAX_UINT256,
            pusd_allowance_neg_risk_exchange=0,
            pusd_allowance_neg_risk_adapter=MAX_UINT256,
        )
        assert partial.source_asset_approved_onramp is False
        assert partial.pusd_approved_ctf_exchange is True
        assert partial.pusd_approved_neg_risk_exchange is False
        assert partial.pusd_approved_neg_risk_adapter is True

    def test_dust_allowance_does_not_count_as_approved(self) -> None:
        """Sufficiency threshold, not ``> 0`` — a 1-wei leftover allowance
        must NOT mark the wallet as ready or ``ensure_allowances`` would skip
        the MAX_UINT256 approval and the next wrap/order would revert as soon
        as it spent past that dust amount."""
        dust = self._status(source_asset_allowance_onramp=1)
        assert dust.source_asset_approved_onramp is False

    def test_below_threshold_allowance_does_not_count_as_approved(self) -> None:
        """A finite spend-budget allowance (e.g. user pre-approved $1) is
        still below the sufficiency threshold and triggers re-approval."""
        small = self._status(pusd_allowance_ctf_exchange=10**12)  # $1M in 6dp
        assert small.pusd_approved_ctf_exchange is False

    def test_max_uint256_allowance_counts_as_approved(self) -> None:
        """The standard ``ensure_allowances`` path issues MAX_UINT256 — that
        must satisfy the predicate even if a small spend has consumed some."""
        full = self._status(source_asset_allowance_onramp=MAX_UINT256)
        assert full.source_asset_approved_onramp is True
        # And one wei below MAX should still pass — realistic spend can never
        # take a MAX approval below MAX_UINT256 // 2 (the threshold).
        nearly_full = self._status(source_asset_allowance_onramp=MAX_UINT256 - 1)
        assert nearly_full.source_asset_approved_onramp is True


class TestEnsureAllowancesPartialState:
    """Each leg of the 6-tx approval set must be independently emitted only
    when missing. Passing all-zero or all-MAX is the easy case — the hard
    cases are the 4 partial states a real wallet typically lands in."""

    def _setup_servicer(
        self,
        sdk: CtfSDK,
        mock_web3: MagicMock,
        *,
        source_allowance: int,
        pusd_allowance_ctf: int,
        pusd_allowance_neg_risk: int,
        pusd_allowance_adapter: int,
        ctf_approved_for_ctf: bool,
        ctf_approved_for_adapter: bool,
    ) -> None:
        """Wire the per-call allowance returns onto a fresh mock web3."""
        source_contract = MagicMock()
        source_contract.functions.balanceOf.return_value.call.return_value = 1_000_000_000
        source_contract.functions.allowance.return_value.call.return_value = source_allowance

        pusd_contract = MagicMock()
        pusd_contract.functions.balanceOf.return_value.call.return_value = 0
        pusd_contract.functions.allowance.return_value.call.side_effect = [
            pusd_allowance_ctf,
            pusd_allowance_neg_risk,
            pusd_allowance_adapter,
        ]

        ctf_contract = MagicMock()
        ctf_contract.functions.isApprovedForAll.return_value.call.side_effect = [
            ctf_approved_for_ctf,
            ctf_approved_for_adapter,
        ]

        mock_web3.eth.contract.side_effect = [source_contract, pusd_contract, ctf_contract]

    def test_only_source_approval_missing(self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock) -> None:
        """Only the source→Onramp leg is missing — emit exactly that one."""
        self._setup_servicer(
            sdk,
            mock_web3,
            source_allowance=0,
            pusd_allowance_ctf=MAX_UINT256,
            pusd_allowance_neg_risk=MAX_UINT256,
            pusd_allowance_adapter=MAX_UINT256,
            ctf_approved_for_ctf=True,
            ctf_approved_for_adapter=True,
        )
        txs = sdk.ensure_allowances(wallet_address, mock_web3)
        assert len(txs) == 1
        assert txs[0].to.lower() == sdk.source_asset.lower()

    def test_only_pusd_neg_risk_adapter_missing(self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock) -> None:
        """Mirror of the production "neg-risk fill rejected" case: pUSD →
        Adapter is the leg most often forgotten in V1→V2 migrations."""
        self._setup_servicer(
            sdk,
            mock_web3,
            source_allowance=MAX_UINT256,
            pusd_allowance_ctf=MAX_UINT256,
            pusd_allowance_neg_risk=MAX_UINT256,
            pusd_allowance_adapter=0,
            ctf_approved_for_ctf=True,
            ctf_approved_for_adapter=True,
        )
        txs = sdk.ensure_allowances(wallet_address, mock_web3)
        assert len(txs) == 1
        # Approving pUSD → NegRisk Adapter
        assert txs[0].to.lower() == sdk.pusd.lower()
        spender, _ = abi_decode(["address", "uint256"], bytes.fromhex(txs[0].data[10:]))
        assert Web3.to_checksum_address(spender) == sdk.neg_risk_adapter

    def test_only_ctf_operator_for_adapter_missing(
        self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock
    ) -> None:
        """ERC-1155 setApprovalForAll for the NegRisk Adapter is missing."""
        self._setup_servicer(
            sdk,
            mock_web3,
            source_allowance=MAX_UINT256,
            pusd_allowance_ctf=MAX_UINT256,
            pusd_allowance_neg_risk=MAX_UINT256,
            pusd_allowance_adapter=MAX_UINT256,
            ctf_approved_for_ctf=True,
            ctf_approved_for_adapter=False,
        )
        txs = sdk.ensure_allowances(wallet_address, mock_web3)
        assert len(txs) == 1
        assert txs[0].to.lower() == sdk.conditional_tokens.lower()
        operator, approved = abi_decode(["address", "bool"], bytes.fromhex(txs[0].data[10:]))
        assert Web3.to_checksum_address(operator) == sdk.neg_risk_adapter
        assert approved is True

    def test_three_missing_emits_only_three(self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock) -> None:
        """Half-applied state (a previous run partially failed): exactly
        three approvals must come out."""
        self._setup_servicer(
            sdk,
            mock_web3,
            source_allowance=MAX_UINT256,  # have
            pusd_allowance_ctf=MAX_UINT256,  # have
            pusd_allowance_neg_risk=0,  # missing
            pusd_allowance_adapter=0,  # missing
            ctf_approved_for_ctf=True,  # have
            ctf_approved_for_adapter=False,  # missing
        )
        txs = sdk.ensure_allowances(wallet_address, mock_web3)
        assert len(txs) == 3


# =============================================================================
# Approval amount overrides
# =============================================================================


class TestBuildApproveCollateralTxAmount:
    """build_approve_collateral_tx defaults to MAX_UINT256 but accepts overrides."""

    def test_default_amount_is_max_uint256(self, sdk: CtfSDK, wallet_address: str) -> None:
        tx = sdk.build_approve_collateral_tx(asset=PUSD, spender=CTF_EXCHANGE_V2, sender=wallet_address)
        _spender, amount = abi_decode(["address", "uint256"], bytes.fromhex(tx.data[10:]))
        assert amount == MAX_UINT256

    def test_custom_amount_encoded(self, sdk: CtfSDK, wallet_address: str) -> None:
        """A finite amount round-trips through abi_encode/decode unchanged."""
        custom = 7_777_777
        tx = sdk.build_approve_collateral_tx(asset=PUSD, spender=CTF_EXCHANGE_V2, sender=wallet_address, amount=custom)
        _spender, amount = abi_decode(["address", "uint256"], bytes.fromhex(tx.data[10:]))
        assert amount == custom

    def test_zero_amount_is_a_revoke(self, sdk: CtfSDK, wallet_address: str) -> None:
        """An ``amount=0`` approval is the canonical revocation pattern."""
        tx = sdk.build_approve_collateral_tx(asset=PUSD, spender=CTF_EXCHANGE_V2, sender=wallet_address, amount=0)
        _spender, amount = abi_decode(["address", "uint256"], bytes.fromhex(tx.data[10:]))
        assert amount == 0


# =============================================================================
# Position ID / collection ID — bytes vs hex condition input
# =============================================================================


class TestPositionIdEdgeCases:
    """Position-ID derivation must be agnostic to ``str`` vs ``bytes`` input
    and produce the same value regardless of leading 0x or case."""

    def test_get_token_ids_for_condition_accepts_bytes(self, sdk: CtfSDK, condition_id: str) -> None:
        """bytes input matches str input."""
        from_str = sdk.get_token_ids_for_condition(condition_id)
        from_bytes = sdk.get_token_ids_for_condition(bytes.fromhex(condition_id.replace("0x", "")))
        assert from_str == from_bytes

    def test_get_token_ids_for_condition_no_0x_prefix(self, sdk: CtfSDK, condition_id: str) -> None:
        """Hex without 0x prefix produces the same token IDs."""
        from_with = sdk.get_token_ids_for_condition(condition_id)
        from_without = sdk.get_token_ids_for_condition(condition_id.replace("0x", ""))
        assert from_with == from_without

    def test_collection_id_changes_with_parent(self, sdk: CtfSDK, condition_id: str) -> None:
        """A non-default parent collection produces a different collection ID."""
        condition_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        root_collection = sdk.get_collection_id(condition_bytes, INDEX_SET_YES)
        nonzero_parent = b"\x11" * 32
        nested_collection = sdk.get_collection_id(condition_bytes, INDEX_SET_YES, parent_collection_id=nonzero_parent)
        assert root_collection != nested_collection

    def test_yes_no_token_ids_distinct(self, sdk: CtfSDK, condition_id: str) -> None:
        """The two binary outcomes must hash to distinct position IDs."""
        yes, no = sdk.get_token_ids_for_condition(condition_id)
        assert yes != no
        assert yes > 0 and no > 0


# =============================================================================
# build_split_tx / build_merge_tx — bytes-input parity
# =============================================================================


class TestBuildMergeRedeemBytesInput:
    """Both string and bytes condition_id inputs must produce identical calldata
    so callers needn't normalize before calling."""

    def test_merge_tx_bytes_matches_str(self, sdk: CtfSDK, wallet_address: str, condition_id: str) -> None:
        condition_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        from_str = sdk.build_merge_tx(condition_id, amount=1_000_000, sender=wallet_address)
        from_bytes = sdk.build_merge_tx(condition_bytes, amount=1_000_000, sender=wallet_address)
        assert from_str.data == from_bytes.data

    def test_redeem_tx_bytes_matches_str(self, sdk: CtfSDK, wallet_address: str, condition_id: str) -> None:
        condition_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        from_str = sdk.build_redeem_tx(condition_id, index_sets=[1, 2], sender=wallet_address)
        from_bytes = sdk.build_redeem_tx(condition_bytes, index_sets=[1, 2], sender=wallet_address)
        assert from_str.data == from_bytes.data

    def test_split_targets_conditional_tokens(self, sdk: CtfSDK, wallet_address: str, condition_id: str) -> None:
        """split tx must hit the Conditional Tokens contract (not the exchange)."""
        tx = sdk.build_split_tx(condition_id, amount=1_000_000, sender=wallet_address)
        assert tx.to.lower() == sdk.conditional_tokens.lower()

    def test_merge_targets_conditional_tokens(self, sdk: CtfSDK, wallet_address: str, condition_id: str) -> None:
        tx = sdk.build_merge_tx(condition_id, amount=1_000_000, sender=wallet_address)
        assert tx.to.lower() == sdk.conditional_tokens.lower()

    def test_redeem_uses_provided_index_sets(self, sdk: CtfSDK, wallet_address: str, condition_id: str) -> None:
        """Different index_sets must yield distinct calldata."""
        tx_yes = sdk.build_redeem_tx(condition_id, index_sets=[INDEX_SET_YES], sender=wallet_address)
        tx_both = sdk.build_redeem_tx(condition_id, index_sets=[INDEX_SET_YES, INDEX_SET_NO], sender=wallet_address)
        assert tx_yes.data != tx_both.data


# =============================================================================
# ResolutionStatus dataclass
# =============================================================================


class TestResolutionStatus:
    """Dataclass shape — ensure the optional ``winning_outcome`` defaults to
    None (so callers can rely on it for "unresolved" detection)."""

    def test_unresolved_default_winning_outcome_none(self) -> None:
        status = ResolutionStatus(
            condition_id="0x" + "00" * 32,
            is_resolved=False,
            payout_denominator=0,
            payout_numerators=[],
        )
        assert status.winning_outcome is None
        assert status.is_resolved is False

    def test_resolved_winning_outcome_int(self) -> None:
        status = ResolutionStatus(
            condition_id="0x" + "01" * 32,
            is_resolved=True,
            payout_denominator=1,
            payout_numerators=[1, 0],
            winning_outcome=0,
        )
        assert status.winning_outcome == 0


# =============================================================================
# get_condition_resolution edge cases
# =============================================================================


class TestGetConditionResolutionEdgeCases:
    """Defensive paths in get_condition_resolution."""

    def test_outcome_count_query_failure_falls_back_to_two(
        self, sdk: CtfSDK, condition_id: str, mock_web3: MagicMock
    ) -> None:
        """If ``getOutcomeSlotCount`` reverts, default to binary (2)."""
        ctf_contract = MagicMock()
        ctf_contract.functions.payoutDenominator.return_value.call.return_value = 1
        ctf_contract.functions.getOutcomeSlotCount.return_value.call.side_effect = Exception("revert")
        ctf_contract.functions.payoutNumerators.return_value.call.side_effect = [1, 0]

        mock_web3.eth.contract.return_value = ctf_contract

        status = sdk.get_condition_resolution(condition_id, mock_web3)

        assert status.is_resolved is True
        assert len(status.payout_numerators) == 2  # fell back to binary

    def test_winning_outcome_picks_first_nonzero(self, sdk: CtfSDK, condition_id: str, mock_web3: MagicMock) -> None:
        """For multi-outcome (e.g. neg-risk) markets, ``winning_outcome`` is
        the index of the *first* non-zero numerator."""
        ctf_contract = MagicMock()
        ctf_contract.functions.payoutDenominator.return_value.call.return_value = 1
        ctf_contract.functions.getOutcomeSlotCount.return_value.call.return_value = 4
        ctf_contract.functions.payoutNumerators.return_value.call.side_effect = [0, 0, 1, 0]

        mock_web3.eth.contract.return_value = ctf_contract

        status = sdk.get_condition_resolution(condition_id, mock_web3)

        assert status.is_resolved is True
        assert status.winning_outcome == 2
        assert status.payout_numerators == [0, 0, 1, 0]

    def test_resolution_returns_canonical_hex_condition_id(
        self, sdk: CtfSDK, condition_id: str, mock_web3: MagicMock
    ) -> None:
        """Hex input → output retains the original hex string (with 0x)."""
        ctf_contract = MagicMock()
        ctf_contract.functions.payoutDenominator.return_value.call.return_value = 0
        mock_web3.eth.contract.return_value = ctf_contract

        status = sdk.get_condition_resolution(condition_id, mock_web3)
        assert status.condition_id == condition_id


# =============================================================================
# TransactionData defaults
# =============================================================================


class TestTransactionDataDefaults:
    """Plain-data tests for the TransactionData dataclass."""

    def test_defaults(self) -> None:
        tx = TransactionData(to="0xabc", data="0x")
        assert tx.value == 0
        assert tx.gas_estimate == 100_000
        assert tx.description == ""

    def test_to_tx_params_checksums_to(self) -> None:
        tx = TransactionData(to="0x" + "ab" * 20, data="0x1234")  # all-lowercase
        params = tx.to_tx_params(sender="0x" + "cd" * 20)
        # web3 checksums to mixed case
        assert params["to"] == Web3.to_checksum_address("0x" + "ab" * 20)
        assert params["from"].lower() == "0x" + "cd" * 20
