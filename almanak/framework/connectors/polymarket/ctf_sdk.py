"""Polymarket CTF (Conditional Token Framework) On-Chain SDK.

Provides on-chain interaction with the Gnosis Conditional Token Framework
for position management, token approvals, and redemption operations.

Polymarket uses a hybrid architecture:
- Off-chain CLOB for order matching (see clob_client.py)
- On-chain CTF for token ownership and settlement (this module)

Key Contract Addresses (Polygon Mainnet):
- CTF Exchange: 0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E
- Neg Risk Exchange: 0xC5d563A36AE78145C45a50134d48A1215220f80a
- Conditional Tokens: 0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
- Neg Risk Adapter: 0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296
- USDC (Polygon): 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174

Example:
    from almanak.framework.connectors.polymarket import CtfSDK
    from web3 import Web3

    web3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
    sdk = CtfSDK()

    # Check allowances
    status = sdk.check_allowances(wallet_address, web3)

    # Build approval transaction if needed
    if not status.usdc_approved_ctf_exchange:
        tx = sdk.build_approve_usdc_tx(CTF_EXCHANGE, MAX_UINT256, wallet_address)

    # Check if market is resolved
    resolution = sdk.get_condition_resolution(condition_id, web3)
    if resolution.is_resolved:
        tx = sdk.build_redeem_tx(condition_id, [1, 2], wallet_address)
"""

import json
import logging
import os
from dataclasses import dataclass
from typing import Any

from eth_abi import encode as abi_encode
from hexbytes import HexBytes
from web3 import Web3

from .models import (
    CONDITIONAL_TOKENS,
    CTF_EXCHANGE,
    NEG_RISK_ADAPTER,
    NEG_RISK_EXCHANGE,
    POLYGON_CHAIN_ID,
    USDC_POLYGON,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Maximum uint256 for unlimited approvals
MAX_UINT256 = 2**256 - 1

# Zero bytes32 (root parent collection)
ZERO_BYTES32 = b"\x00" * 32

# Index sets for binary markets
# YES = 1 (0b01), NO = 2 (0b10)
INDEX_SET_YES = 1
INDEX_SET_NO = 2
BINARY_PARTITION = [INDEX_SET_YES, INDEX_SET_NO]

# Gas estimates for CTF operations
# Note: Polygon USDC.e is a proxy contract that requires ~58k gas for approve
# We use 80k as a safe margin for ERC20/ERC1155 approvals on proxy contracts
GAS_ESTIMATES = {
    "approve_erc20": 80_000,
    "approve_erc1155": 80_000,
    "split_position": 150_000,
    "merge_positions": 150_000,
    "redeem_positions": 200_000,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class TransactionData:
    """Transaction data for on-chain operations.

    Attributes:
        to: Contract address to call
        data: Encoded function call data
        value: ETH value to send (usually 0)
        gas_estimate: Estimated gas for the transaction
        description: Human-readable description
    """

    to: str
    data: str
    value: int = 0
    gas_estimate: int = 100_000
    description: str = ""

    def to_tx_params(self, sender: str) -> dict[str, Any]:
        """Convert to web3 transaction parameters.

        Args:
            sender: Transaction sender address

        Returns:
            Dict with transaction parameters for web3
        """
        return {
            "from": sender,
            "to": Web3.to_checksum_address(self.to),
            "data": HexBytes(self.data),
            "value": self.value,
            "gas": self.gas_estimate,
        }


@dataclass
class AllowanceStatus:
    """Status of token allowances for Polymarket trading.

    Attributes:
        usdc_balance: USDC balance in token units
        usdc_allowance_ctf_exchange: USDC allowance for CTF Exchange
        usdc_allowance_neg_risk_exchange: USDC allowance for Neg Risk Exchange
        ctf_approved_for_ctf_exchange: ERC-1155 approved for CTF Exchange
        ctf_approved_for_neg_risk_adapter: ERC-1155 approved for Neg Risk Adapter
    """

    usdc_balance: int
    usdc_allowance_ctf_exchange: int
    usdc_allowance_neg_risk_exchange: int
    ctf_approved_for_ctf_exchange: bool
    ctf_approved_for_neg_risk_adapter: bool

    @property
    def usdc_approved_ctf_exchange(self) -> bool:
        """Check if USDC is approved for CTF Exchange."""
        return self.usdc_allowance_ctf_exchange > 0

    @property
    def usdc_approved_neg_risk_exchange(self) -> bool:
        """Check if USDC is approved for Neg Risk Exchange."""
        return self.usdc_allowance_neg_risk_exchange > 0

    @property
    def fully_approved(self) -> bool:
        """Check if all necessary approvals are in place."""
        return (
            self.usdc_approved_ctf_exchange
            and self.usdc_approved_neg_risk_exchange
            and self.ctf_approved_for_ctf_exchange
            and self.ctf_approved_for_neg_risk_adapter
        )


@dataclass
class ResolutionStatus:
    """Resolution status of a condition.

    Attributes:
        condition_id: The condition ID (bytes32 hex string)
        is_resolved: Whether the condition has been resolved
        payout_denominator: Denominator for payout calculation
        payout_numerators: List of payout numerators for each outcome
        winning_outcome: Index of winning outcome (0=YES, 1=NO) or None if not resolved
    """

    condition_id: str
    is_resolved: bool
    payout_denominator: int
    payout_numerators: list[int]
    winning_outcome: int | None = None


# =============================================================================
# CTF SDK
# =============================================================================


class CtfSDK:
    """Low-level SDK for Polymarket CTF on-chain operations.

    This SDK provides methods to:
    - Check and set token approvals
    - Query token balances
    - Build split, merge, and redeem transactions
    - Check condition resolution status

    All transaction building methods return TransactionData objects that
    can be signed and submitted using a signer.

    Example:
        sdk = CtfSDK()
        web3 = Web3(Web3.HTTPProvider(rpc_url))

        # Check if wallet needs approvals
        status = sdk.check_allowances("0x...", web3)

        if not status.usdc_approved_ctf_exchange:
            tx = sdk.build_approve_usdc_tx(CTF_EXCHANGE, MAX_UINT256, "0x...")
            # Sign and submit tx...

        # Build redeem transaction for resolved market
        tx = sdk.build_redeem_tx(
            condition_id="0x...",
            index_sets=[1, 2],
            sender="0x...",
        )
    """

    def __init__(
        self,
        chain_id: int = POLYGON_CHAIN_ID,
        ctf_exchange: str = CTF_EXCHANGE,
        neg_risk_exchange: str = NEG_RISK_EXCHANGE,
        conditional_tokens: str = CONDITIONAL_TOKENS,
        neg_risk_adapter: str = NEG_RISK_ADAPTER,
        usdc: str = USDC_POLYGON,
    ) -> None:
        """Initialize the CTF SDK.

        Args:
            chain_id: Chain ID (default: Polygon 137)
            ctf_exchange: CTF Exchange contract address
            neg_risk_exchange: Neg Risk Exchange contract address
            conditional_tokens: Conditional Tokens contract address
            neg_risk_adapter: Neg Risk Adapter contract address
            usdc: USDC token address
        """
        self.chain_id = chain_id
        self.ctf_exchange = Web3.to_checksum_address(ctf_exchange)
        self.neg_risk_exchange = Web3.to_checksum_address(neg_risk_exchange)
        self.conditional_tokens = Web3.to_checksum_address(conditional_tokens)
        self.neg_risk_adapter = Web3.to_checksum_address(neg_risk_adapter)
        self.usdc = Web3.to_checksum_address(usdc)

        # Load ABIs
        self._abi_dir = os.path.join(os.path.dirname(__file__), "abis")
        self._erc20_abi = self._load_abi("erc20")
        self._erc1155_abi = self._load_abi("erc1155")
        self._conditional_tokens_abi = self._load_abi("conditional_tokens")
        self._ctf_exchange_abi = self._load_abi("ctf_exchange")

        logger.info(
            "CtfSDK initialized for chain_id=%d, ctf_exchange=%s, conditional_tokens=%s",
            chain_id,
            ctf_exchange,
            conditional_tokens,
        )

    def _load_abi(self, name: str) -> list[dict]:
        """Load ABI from file."""
        abi_path = os.path.join(self._abi_dir, f"{name}.json")
        try:
            with open(abi_path) as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"ABI file not found: {abi_path}")
            return []

    # =========================================================================
    # Token Approvals
    # =========================================================================

    def build_approve_usdc_tx(
        self,
        spender: str,
        amount: int,
        sender: str,
    ) -> TransactionData:
        """Build USDC approval transaction.

        Approves the spender (typically CTF Exchange or Neg Risk Exchange)
        to spend USDC on behalf of the sender.

        Args:
            spender: Address to approve (e.g., CTF_EXCHANGE)
            amount: Amount to approve (use MAX_UINT256 for unlimited)
            sender: Transaction sender address

        Returns:
            TransactionData for the approval
        """
        spender = Web3.to_checksum_address(spender)

        # Encode approve(address,uint256)
        selector = bytes(Web3.keccak(text="approve(address,uint256)")[:4])
        data = selector + abi_encode(["address", "uint256"], [spender, amount])

        return TransactionData(
            to=self.usdc,
            data="0x" + data.hex(),
            gas_estimate=GAS_ESTIMATES["approve_erc20"],
            description=f"Approve USDC spending for {spender[:10]}...",
        )

    def build_approve_conditional_tokens_tx(
        self,
        operator: str,
        approved: bool,
        sender: str,
    ) -> TransactionData:
        """Build ERC-1155 setApprovalForAll transaction.

        Approves the operator (typically CTF Exchange or Neg Risk Adapter)
        to transfer conditional tokens on behalf of the sender.

        Args:
            operator: Address to approve (e.g., CTF_EXCHANGE)
            approved: True to approve, False to revoke
            sender: Transaction sender address

        Returns:
            TransactionData for the approval
        """
        operator = Web3.to_checksum_address(operator)

        # Encode setApprovalForAll(address,bool)
        selector = bytes(Web3.keccak(text="setApprovalForAll(address,bool)")[:4])
        data = selector + abi_encode(["address", "bool"], [operator, approved])

        return TransactionData(
            to=self.conditional_tokens,
            data="0x" + data.hex(),
            gas_estimate=GAS_ESTIMATES["approve_erc1155"],
            description=f"{'Approve' if approved else 'Revoke'} CTF operator {operator[:10]}...",
        )

    def check_allowances(self, wallet: str, web3: Any) -> AllowanceStatus:
        """Check all relevant token allowances.

        Queries USDC allowances and ERC-1155 operator approvals needed
        for trading on Polymarket.

        Args:
            wallet: Wallet address to check
            web3: Web3 instance

        Returns:
            AllowanceStatus with all allowance information
        """
        wallet = Web3.to_checksum_address(wallet)

        # Create contract instances
        usdc_contract = web3.eth.contract(address=self.usdc, abi=self._erc20_abi)
        ctf_contract = web3.eth.contract(address=self.conditional_tokens, abi=self._conditional_tokens_abi)

        # Query USDC balance and allowances
        usdc_balance = usdc_contract.functions.balanceOf(wallet).call()
        usdc_allowance_ctf = usdc_contract.functions.allowance(wallet, self.ctf_exchange).call()
        usdc_allowance_neg_risk = usdc_contract.functions.allowance(wallet, self.neg_risk_exchange).call()

        # Query ERC-1155 operator approvals
        ctf_approved_exchange = ctf_contract.functions.isApprovedForAll(wallet, self.ctf_exchange).call()
        ctf_approved_adapter = ctf_contract.functions.isApprovedForAll(wallet, self.neg_risk_adapter).call()

        return AllowanceStatus(
            usdc_balance=usdc_balance,
            usdc_allowance_ctf_exchange=usdc_allowance_ctf,
            usdc_allowance_neg_risk_exchange=usdc_allowance_neg_risk,
            ctf_approved_for_ctf_exchange=ctf_approved_exchange,
            ctf_approved_for_neg_risk_adapter=ctf_approved_adapter,
        )

    def ensure_allowances(self, wallet: str, web3: Any) -> list[TransactionData]:
        """Build transactions to ensure all necessary approvals.

        Checks current allowance status and returns a list of transactions
        needed to set up all required approvals for trading.

        Args:
            wallet: Wallet address
            web3: Web3 instance

        Returns:
            List of TransactionData for any needed approvals
        """
        status = self.check_allowances(wallet, web3)
        transactions = []

        if not status.usdc_approved_ctf_exchange:
            transactions.append(self.build_approve_usdc_tx(self.ctf_exchange, MAX_UINT256, wallet))

        if not status.usdc_approved_neg_risk_exchange:
            transactions.append(self.build_approve_usdc_tx(self.neg_risk_exchange, MAX_UINT256, wallet))

        if not status.ctf_approved_for_ctf_exchange:
            transactions.append(self.build_approve_conditional_tokens_tx(self.ctf_exchange, True, wallet))

        if not status.ctf_approved_for_neg_risk_adapter:
            transactions.append(self.build_approve_conditional_tokens_tx(self.neg_risk_adapter, True, wallet))

        return transactions

    # =========================================================================
    # Token Balances
    # =========================================================================

    def get_token_balance(self, wallet: str, token_id: int, web3: Any) -> int:
        """Get ERC-1155 token balance.

        Args:
            wallet: Wallet address
            token_id: Conditional token ID (position ID)
            web3: Web3 instance

        Returns:
            Token balance in base units
        """
        wallet = Web3.to_checksum_address(wallet)
        ctf_contract = web3.eth.contract(address=self.conditional_tokens, abi=self._conditional_tokens_abi)
        return ctf_contract.functions.balanceOf(wallet, token_id).call()

    def get_token_balances(self, wallet: str, token_ids: list[int], web3: Any) -> list[int]:
        """Get multiple ERC-1155 token balances in a single call.

        Args:
            wallet: Wallet address
            token_ids: List of conditional token IDs
            web3: Web3 instance

        Returns:
            List of token balances in base units
        """
        wallet = Web3.to_checksum_address(wallet)
        ctf_contract = web3.eth.contract(address=self.conditional_tokens, abi=self._conditional_tokens_abi)

        # Create list of wallet addresses (same wallet for all)
        wallets = [wallet] * len(token_ids)
        return ctf_contract.functions.balanceOfBatch(wallets, token_ids).call()

    def get_usdc_balance(self, wallet: str, web3: Any) -> int:
        """Get USDC balance.

        Args:
            wallet: Wallet address
            web3: Web3 instance

        Returns:
            USDC balance in base units (6 decimals)
        """
        wallet = Web3.to_checksum_address(wallet)
        usdc_contract = web3.eth.contract(address=self.usdc, abi=self._erc20_abi)
        return usdc_contract.functions.balanceOf(wallet).call()

    # =========================================================================
    # Position ID Calculation
    # =========================================================================

    def get_collection_id(
        self,
        condition_id: bytes,
        index_set: int,
        parent_collection_id: bytes = ZERO_BYTES32,
    ) -> bytes:
        """Calculate collection ID for an outcome.

        Args:
            condition_id: Condition ID (32 bytes)
            index_set: Outcome index set (1=YES, 2=NO for binary)
            parent_collection_id: Parent collection (default: root)

        Returns:
            Collection ID (32 bytes)
        """
        # Collection ID = keccak256(parentCollectionId, conditionId, indexSet)
        encoded = abi_encode(
            ["bytes32", "bytes32", "uint256"],
            [parent_collection_id, condition_id, index_set],
        )
        return Web3.keccak(encoded)

    def get_position_id(self, collateral: str, collection_id: bytes) -> int:
        """Calculate ERC-1155 position ID from collection ID.

        Args:
            collateral: Collateral token address (USDC)
            collection_id: Collection ID (32 bytes)

        Returns:
            Position ID (uint256)
        """
        collateral = Web3.to_checksum_address(collateral)
        encoded = abi_encode(["address", "bytes32"], [collateral, collection_id])
        return int(Web3.keccak(encoded).hex(), 16)

    def get_token_ids_for_condition(self, condition_id: str | bytes) -> tuple[int, int]:
        """Get YES and NO token IDs for a binary condition.

        Args:
            condition_id: Condition ID (hex string or bytes)

        Returns:
            Tuple of (yes_token_id, no_token_id)
        """
        if isinstance(condition_id, str):
            condition_id = bytes.fromhex(condition_id.replace("0x", ""))

        yes_collection = self.get_collection_id(condition_id, INDEX_SET_YES)
        no_collection = self.get_collection_id(condition_id, INDEX_SET_NO)

        yes_token_id = self.get_position_id(self.usdc, yes_collection)
        no_token_id = self.get_position_id(self.usdc, no_collection)

        return yes_token_id, no_token_id

    # =========================================================================
    # Split / Merge / Redeem Operations
    # =========================================================================

    def build_split_tx(
        self,
        condition_id: str | bytes,
        amount: int,
        sender: str,
    ) -> TransactionData:
        """Build split position transaction.

        Splits USDC into YES and NO conditional tokens.
        Requires USDC approval for Conditional Tokens contract.

        Args:
            condition_id: Condition ID (hex string or bytes)
            amount: Amount of USDC to split (in base units)
            sender: Transaction sender address

        Returns:
            TransactionData for the split operation
        """
        condition_bytes = (
            bytes.fromhex(condition_id.replace("0x", "")) if isinstance(condition_id, str) else condition_id
        )

        # Encode splitPosition(IERC20, bytes32, bytes32, uint256[], uint256)
        selector = bytes(Web3.keccak(text="splitPosition(address,bytes32,bytes32,uint256[],uint256)")[:4])
        data = selector + abi_encode(
            ["address", "bytes32", "bytes32", "uint256[]", "uint256"],
            [self.usdc, ZERO_BYTES32, condition_bytes, BINARY_PARTITION, amount],
        )

        return TransactionData(
            to=self.conditional_tokens,
            data="0x" + data.hex(),
            gas_estimate=GAS_ESTIMATES["split_position"],
            description=f"Split {amount} USDC into YES/NO tokens",
        )

    def build_merge_tx(
        self,
        condition_id: str | bytes,
        amount: int,
        sender: str,
    ) -> TransactionData:
        """Build merge positions transaction.

        Merges equal amounts of YES and NO tokens back into USDC.
        Requires ERC-1155 approval for Conditional Tokens contract.

        Args:
            condition_id: Condition ID (hex string or bytes)
            amount: Amount of each outcome token to merge
            sender: Transaction sender address

        Returns:
            TransactionData for the merge operation
        """
        condition_bytes = (
            bytes.fromhex(condition_id.replace("0x", "")) if isinstance(condition_id, str) else condition_id
        )

        # Encode mergePositions(IERC20, bytes32, bytes32, uint256[], uint256)
        selector = bytes(Web3.keccak(text="mergePositions(address,bytes32,bytes32,uint256[],uint256)")[:4])
        data = selector + abi_encode(
            ["address", "bytes32", "bytes32", "uint256[]", "uint256"],
            [self.usdc, ZERO_BYTES32, condition_bytes, BINARY_PARTITION, amount],
        )

        return TransactionData(
            to=self.conditional_tokens,
            data="0x" + data.hex(),
            gas_estimate=GAS_ESTIMATES["merge_positions"],
            description=f"Merge {amount} YES+NO tokens into USDC",
        )

    def build_redeem_tx(
        self,
        condition_id: str | bytes,
        index_sets: list[int],
        sender: str,
    ) -> TransactionData:
        """Build redeem positions transaction.

        Redeems winning positions after market resolution.
        Only works if the condition has been resolved.

        Args:
            condition_id: Condition ID (hex string or bytes)
            index_sets: List of index sets to redeem (e.g., [1, 2] for both)
            sender: Transaction sender address

        Returns:
            TransactionData for the redemption
        """
        condition_bytes = (
            bytes.fromhex(condition_id.replace("0x", "")) if isinstance(condition_id, str) else condition_id
        )

        # Encode redeemPositions(IERC20, bytes32, bytes32, uint256[])
        selector = bytes(Web3.keccak(text="redeemPositions(address,bytes32,bytes32,uint256[])")[:4])
        data = selector + abi_encode(
            ["address", "bytes32", "bytes32", "uint256[]"],
            [self.usdc, ZERO_BYTES32, condition_bytes, index_sets],
        )

        return TransactionData(
            to=self.conditional_tokens,
            data="0x" + data.hex(),
            gas_estimate=GAS_ESTIMATES["redeem_positions"],
            description="Redeem winning positions",
        )

    # =========================================================================
    # Condition Resolution
    # =========================================================================

    def get_condition_resolution(self, condition_id: str | bytes, web3: Any) -> ResolutionStatus:
        """Get resolution status of a condition.

        Checks if a condition has been resolved and returns payout information.

        Args:
            condition_id: Condition ID (hex string or bytes)
            web3: Web3 instance

        Returns:
            ResolutionStatus with resolution information
        """
        if isinstance(condition_id, str):
            condition_id_str = condition_id
            condition_id = bytes.fromhex(condition_id.replace("0x", ""))
        else:
            condition_id_str = "0x" + condition_id.hex()

        ctf_contract = web3.eth.contract(address=self.conditional_tokens, abi=self._conditional_tokens_abi)

        # Get payout denominator (0 if not resolved)
        payout_denom = ctf_contract.functions.payoutDenominator(condition_id).call()

        is_resolved = payout_denom > 0

        # Get payout numerators for each outcome
        payout_numerators = []
        winning_outcome = None

        if is_resolved:
            # Binary markets have 2 outcomes (YES=0, NO=1)
            try:
                outcome_count = ctf_contract.functions.getOutcomeSlotCount(condition_id).call()
            except Exception:
                outcome_count = 2  # Default for binary markets

            for i in range(outcome_count):
                numerator = ctf_contract.functions.payoutNumerators(condition_id, i).call()
                payout_numerators.append(numerator)

                # Winning outcome has non-zero numerator
                if numerator > 0 and winning_outcome is None:
                    winning_outcome = i

        return ResolutionStatus(
            condition_id=condition_id_str,
            is_resolved=is_resolved,
            payout_denominator=payout_denom,
            payout_numerators=payout_numerators,
            winning_outcome=winning_outcome,
        )


__all__ = [
    "CtfSDK",
    "TransactionData",
    "AllowanceStatus",
    "ResolutionStatus",
    "MAX_UINT256",
    "ZERO_BYTES32",
    "INDEX_SET_YES",
    "INDEX_SET_NO",
    "BINARY_PARTITION",
    "GAS_ESTIMATES",
]
