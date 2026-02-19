"""Unit tests for Polymarket CTF SDK.

Tests cover:
- Transaction building (approvals, split, merge, redeem)
- Token ID calculation
- Allowance checking with mocked web3
- Resolution status checking
"""

from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.polymarket.ctf_sdk import (
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
from almanak.framework.connectors.polymarket.models import (
    CONDITIONAL_TOKENS,
    CTF_EXCHANGE,
    NEG_RISK_ADAPTER,
    NEG_RISK_EXCHANGE,
    USDC_POLYGON,
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
        assert sdk.ctf_exchange == CTF_EXCHANGE
        assert sdk.neg_risk_exchange == NEG_RISK_EXCHANGE
        assert sdk.conditional_tokens == CONDITIONAL_TOKENS
        assert sdk.neg_risk_adapter == NEG_RISK_ADAPTER
        assert sdk.usdc == USDC_POLYGON
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
        assert len(sdk._ctf_exchange_abi) > 0


# =============================================================================
# Transaction Building Tests
# =============================================================================


class TestBuildApproveUsdcTx:
    """Tests for USDC approval transaction building."""

    def test_builds_valid_tx(self, sdk: CtfSDK, wallet_address: str) -> None:
        """Test building USDC approval transaction."""
        tx = sdk.build_approve_usdc_tx(
            spender=CTF_EXCHANGE,
            amount=MAX_UINT256,
            sender=wallet_address,
        )

        assert isinstance(tx, TransactionData)
        assert tx.to.lower() == USDC_POLYGON.lower()
        assert tx.data.startswith("0x")
        assert tx.gas_estimate == GAS_ESTIMATES["approve_erc20"]
        assert tx.value == 0
        assert "Approve USDC" in tx.description

    def test_encodes_approve_function(self, sdk: CtfSDK, wallet_address: str) -> None:
        """Test that the approve function selector is correct."""
        tx = sdk.build_approve_usdc_tx(
            spender=CTF_EXCHANGE,
            amount=1000000,
            sender=wallet_address,
        )

        # approve(address,uint256) selector = 0x095ea7b3
        assert tx.data.startswith("0x095ea7b3")

    def test_to_tx_params(self, sdk: CtfSDK, wallet_address: str) -> None:
        """Test converting TransactionData to web3 params."""
        tx = sdk.build_approve_usdc_tx(
            spender=CTF_EXCHANGE,
            amount=MAX_UINT256,
            sender=wallet_address,
        )

        params = tx.to_tx_params(wallet_address)
        assert params["from"] == wallet_address
        assert params["to"] == sdk.usdc
        # data is converted to HexBytes in to_tx_params
        assert params["data"].hex() == tx.data[2:]  # Remove 0x prefix for comparison
        assert params["value"] == 0
        assert params["gas"] == tx.gas_estimate


class TestBuildApproveConditionalTokensTx:
    """Tests for ERC-1155 approval transaction building."""

    def test_builds_valid_approval(self, sdk: CtfSDK, wallet_address: str) -> None:
        """Test building ERC-1155 setApprovalForAll transaction."""
        tx = sdk.build_approve_conditional_tokens_tx(
            operator=CTF_EXCHANGE,
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
            operator=CTF_EXCHANGE,
            approved=False,
            sender=wallet_address,
        )

        assert "Revoke" in tx.description

    def test_encodes_setApprovalForAll(self, sdk: CtfSDK, wallet_address: str) -> None:
        """Test that setApprovalForAll selector is correct."""
        tx = sdk.build_approve_conditional_tokens_tx(
            operator=CTF_EXCHANGE,
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

        position_id = sdk.get_position_id(USDC_POLYGON, collection_id)

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
        """Test checking allowances when all are approved."""
        # Configure mock contract calls
        usdc_contract = MagicMock()
        usdc_contract.functions.balanceOf.return_value.call.return_value = 1000000
        usdc_contract.functions.allowance.return_value.call.return_value = MAX_UINT256

        ctf_contract = MagicMock()
        ctf_contract.functions.isApprovedForAll.return_value.call.return_value = True

        mock_web3.eth.contract.side_effect = [usdc_contract, ctf_contract]

        status = sdk.check_allowances(wallet_address, mock_web3)

        assert isinstance(status, AllowanceStatus)
        assert status.usdc_balance == 1000000
        assert status.usdc_approved_ctf_exchange is True
        assert status.usdc_approved_neg_risk_exchange is True
        assert status.ctf_approved_for_ctf_exchange is True
        assert status.ctf_approved_for_neg_risk_adapter is True
        assert status.fully_approved is True

    def test_check_allowances_none_approved(self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock) -> None:
        """Test checking allowances when none are approved."""
        usdc_contract = MagicMock()
        usdc_contract.functions.balanceOf.return_value.call.return_value = 1000000
        usdc_contract.functions.allowance.return_value.call.return_value = 0

        ctf_contract = MagicMock()
        ctf_contract.functions.isApprovedForAll.return_value.call.return_value = False

        mock_web3.eth.contract.side_effect = [usdc_contract, ctf_contract]

        status = sdk.check_allowances(wallet_address, mock_web3)

        assert status.usdc_approved_ctf_exchange is False
        assert status.usdc_approved_neg_risk_exchange is False
        assert status.ctf_approved_for_ctf_exchange is False
        assert status.ctf_approved_for_neg_risk_adapter is False
        assert status.fully_approved is False


class TestEnsureAllowances:
    """Tests for ensure_allowances method."""

    def test_returns_empty_when_approved(self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock) -> None:
        """Test that no transactions needed when already approved."""
        usdc_contract = MagicMock()
        usdc_contract.functions.balanceOf.return_value.call.return_value = 1000000
        usdc_contract.functions.allowance.return_value.call.return_value = MAX_UINT256

        ctf_contract = MagicMock()
        ctf_contract.functions.isApprovedForAll.return_value.call.return_value = True

        mock_web3.eth.contract.side_effect = [usdc_contract, ctf_contract]

        transactions = sdk.ensure_allowances(wallet_address, mock_web3)

        assert transactions == []

    def test_returns_all_approvals_when_none_approved(
        self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock
    ) -> None:
        """Test that all approval transactions returned when none approved."""
        usdc_contract = MagicMock()
        usdc_contract.functions.balanceOf.return_value.call.return_value = 1000000
        usdc_contract.functions.allowance.return_value.call.return_value = 0

        ctf_contract = MagicMock()
        ctf_contract.functions.isApprovedForAll.return_value.call.return_value = False

        mock_web3.eth.contract.side_effect = [usdc_contract, ctf_contract]

        transactions = sdk.ensure_allowances(wallet_address, mock_web3)

        # Should return 4 transactions: 2 USDC approvals, 2 ERC-1155 approvals
        assert len(transactions) == 4
        assert all(isinstance(tx, TransactionData) for tx in transactions)


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

    def test_get_usdc_balance(self, sdk: CtfSDK, wallet_address: str, mock_web3: MagicMock) -> None:
        """Test getting USDC balance."""
        usdc_contract = MagicMock()
        usdc_contract.functions.balanceOf.return_value.call.return_value = 10000000

        mock_web3.eth.contract.return_value = usdc_contract

        balance = sdk.get_usdc_balance(wallet_address, mock_web3)

        assert balance == 10000000


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
