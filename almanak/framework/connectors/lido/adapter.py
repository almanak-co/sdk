"""Lido Adapter.

This module provides an adapter for interacting with Lido liquid staking protocol.

Lido is a decentralized liquid staking protocol supporting:
- Stake ETH to receive stETH
- Wrap stETH to wstETH (non-rebasing)
- Unwrap wstETH to stETH

Supported chains:
- Ethereum (full staking + wrap/unwrap)
- Arbitrum, Optimism, Polygon (wstETH only)

Example:
    from almanak.framework.connectors.lido import LidoAdapter, LidoConfig

    config = LidoConfig(
        chain="ethereum",
        wallet_address="0x...",
    )
    adapter = LidoAdapter(config)

    # Stake ETH to receive stETH
    result = adapter.stake(amount=Decimal("1.0"))

    # Wrap stETH to wstETH
    result = adapter.wrap(amount=Decimal("1.0"))
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType
    from almanak.framework.intents.vocabulary import StakeIntent, UnstakeIntent
    from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Lido contract addresses per chain
LIDO_ADDRESSES: dict[str, dict[str, str]] = {
    "ethereum": {
        "steth": "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
        "wsteth": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        "withdrawal_queue": "0x889edC2eDab5f40e902b864aD4d7AdE8E412F9B1",
    },
    "arbitrum": {
        "wsteth": "0x5979D7b546E38E414F7E9822514be443A4800529",
    },
    "optimism": {
        "wsteth": "0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb",
    },
    "polygon": {
        "wsteth": "0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD",
    },
}

# Function selectors
# stETH.submit(address _referral) payable - stake ETH
LIDO_STAKE_SELECTOR = "0xa1903eab"
# wstETH.wrap(uint256 _stETHAmount) - wrap stETH to wstETH
LIDO_WRAP_SELECTOR = "0xea598cb0"
# wstETH.unwrap(uint256 _wstETHAmount) - unwrap wstETH to stETH
LIDO_UNWRAP_SELECTOR = "0xde0e9a3e"
# WithdrawalQueue.requestWithdrawals(uint256[],address) - request stETH withdrawal
LIDO_REQUEST_WITHDRAWALS_SELECTOR = "0xd6681042"
# WithdrawalQueue.claimWithdrawals(uint256[],uint256[]) - claim finalized withdrawals
LIDO_CLAIM_WITHDRAWALS_SELECTOR = "0x85e8362f"

# Gas estimates for Lido operations
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "stake": 100000,
    "wrap": 80000,
    "unwrap": 80000,
    "request_withdrawal": 150000,  # Base gas, add ~30k per additional request
    "claim_withdrawal": 100000,  # Base gas, add ~20k per additional request
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class LidoConfig:
    """Configuration for Lido adapter.

    Attributes:
        chain: Blockchain network (ethereum, arbitrum, optimism, polygon)
        wallet_address: User wallet address
    """

    chain: str
    wallet_address: str

    def __post_init__(self) -> None:
        """Validate configuration."""
        valid_chains = set(LIDO_ADDRESSES.keys())
        if self.chain not in valid_chains:
            raise ValueError(f"Invalid chain: {self.chain}. Valid chains: {valid_chains}")
        if not self.wallet_address.startswith("0x") or len(self.wallet_address) != 42:
            raise ValueError(f"Invalid wallet address: {self.wallet_address}. Must be 0x-prefixed 40 hex chars.")


@dataclass
class TransactionResult:
    """Result of a transaction build operation.

    Attributes:
        success: Whether operation succeeded
        tx_data: Transaction data (to, value, data)
        gas_estimate: Estimated gas
        description: Human-readable description
        error: Error message if failed
    """

    success: bool
    tx_data: dict[str, Any] | None = None
    gas_estimate: int = 0
    description: str = ""
    error: str | None = None


# =============================================================================
# Adapter
# =============================================================================


class LidoAdapter:
    """Adapter for Lido liquid staking protocol.

    This adapter provides methods for interacting with Lido:
    - Stake ETH to receive stETH
    - Wrap stETH to wstETH (non-rebasing)
    - Unwrap wstETH back to stETH

    Note: stETH is a rebasing token - balances change daily as rewards accrue.
    wstETH is non-rebasing and preferred for DeFi integrations.

    Example:
        config = LidoConfig(
            chain="ethereum",
            wallet_address="0x...",
        )
        adapter = LidoAdapter(config)

        # Stake ETH to get stETH
        result = adapter.stake(Decimal("1.0"))

        # Wrap stETH to wstETH
        result = adapter.wrap(Decimal("1.0"))

        # Unwrap wstETH back to stETH
        result = adapter.unwrap(Decimal("1.0"))
    """

    def __init__(self, config: LidoConfig, token_resolver: TokenResolverType | None = None) -> None:
        """Initialize the adapter.

        Args:
            config: Adapter configuration
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address

        # Contract addresses
        chain_addresses = LIDO_ADDRESSES[config.chain]
        self.steth_address = chain_addresses.get("steth")
        self.wsteth_address = chain_addresses.get("wsteth")
        self.withdrawal_queue_address = chain_addresses.get("withdrawal_queue")

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        logger.info(f"LidoAdapter initialized for chain={config.chain}, wallet={config.wallet_address[:10]}...")

    def stake(
        self,
        amount: Decimal,
        referral: str = "0x0000000000000000000000000000000000000000",
    ) -> TransactionResult:
        """Build a stake transaction to receive stETH.

        Stakes ETH to the Lido stETH contract and receives stETH in return.
        Only available on Ethereum mainnet.

        Args:
            amount: Amount of ETH to stake
            referral: Referral address (default: zero address)

        Returns:
            TransactionResult with transaction data
        """
        try:
            if self.steth_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Staking not available on {self.chain}. Only Ethereum supported.",
                )

            # Amount in wei
            amount_wei = int(amount * Decimal(10**18))

            # Build calldata: submit(address _referral)
            calldata = LIDO_STAKE_SELECTOR + self._pad_address(referral)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.steth_address,
                    "value": amount_wei,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["stake"],
                description=f"Stake {amount} ETH to Lido for stETH",
            )

        except Exception as e:
            logger.exception(f"Failed to build stake transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def wrap(self, amount: Decimal) -> TransactionResult:
        """Build a wrap transaction to convert stETH to wstETH.

        Args:
            amount: Amount of stETH to wrap

        Returns:
            TransactionResult with transaction data
        """
        try:
            if self.wsteth_address is None:
                return TransactionResult(
                    success=False,
                    error=f"wstETH not available on {self.chain}.",
                )

            # Amount in wei
            amount_wei = int(amount * Decimal(10**18))

            # Build calldata: wrap(uint256 _stETHAmount)
            calldata = LIDO_WRAP_SELECTOR + self._pad_uint256(amount_wei)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.wsteth_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["wrap"],
                description=f"Wrap {amount} stETH to wstETH",
            )

        except Exception as e:
            logger.exception(f"Failed to build wrap transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def unwrap(self, amount: Decimal) -> TransactionResult:
        """Build an unwrap transaction to convert wstETH to stETH.

        Args:
            amount: Amount of wstETH to unwrap

        Returns:
            TransactionResult with transaction data
        """
        try:
            if self.wsteth_address is None:
                return TransactionResult(
                    success=False,
                    error=f"wstETH not available on {self.chain}.",
                )

            # Amount in wei
            amount_wei = int(amount * Decimal(10**18))

            # Build calldata: unwrap(uint256 _wstETHAmount)
            calldata = LIDO_UNWRAP_SELECTOR + self._pad_uint256(amount_wei)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.wsteth_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["unwrap"],
                description=f"Unwrap {amount} wstETH to stETH",
            )

        except Exception as e:
            logger.exception(f"Failed to build unwrap transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def request_withdrawal(
        self,
        amounts: list[Decimal],
        owner: str | None = None,
    ) -> TransactionResult:
        """Build a withdrawal request transaction.

        Requests stETH withdrawal from the Lido withdrawal queue.
        Each amount in the list creates a separate withdrawal request.
        Only available on Ethereum mainnet.

        Note: Requires prior approval of stETH to the withdrawal queue contract.

        Args:
            amounts: List of stETH amounts to withdraw
            owner: Address to own the withdrawal requests (default: wallet_address)

        Returns:
            TransactionResult with transaction data
        """
        try:
            if self.withdrawal_queue_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Withdrawal queue not available on {self.chain}. Only Ethereum supported.",
                )

            if not amounts:
                return TransactionResult(
                    success=False,
                    error="At least one withdrawal amount is required.",
                )

            # Use wallet address if owner not specified
            owner_address = owner if owner else self.wallet_address

            # Convert amounts to wei
            amounts_wei = [int(amount * Decimal(10**18)) for amount in amounts]

            # Build calldata: requestWithdrawals(uint256[] calldata _amounts, address _owner)
            # Dynamic array encoding:
            # - offset to array (32 bytes)
            # - owner address (32 bytes)
            # - array length (32 bytes)
            # - array elements (32 bytes each)
            calldata = LIDO_REQUEST_WITHDRAWALS_SELECTOR
            # Offset to first dynamic arg (after static args: offset=64, owner at 32)
            calldata += self._pad_uint256(64)  # offset to amounts array
            calldata += self._pad_address(owner_address)  # owner
            calldata += self._pad_uint256(len(amounts_wei))  # array length
            for amount_wei in amounts_wei:
                calldata += self._pad_uint256(amount_wei)

            total_amount = sum(amounts)
            gas_estimate = DEFAULT_GAS_ESTIMATES["request_withdrawal"] + (len(amounts) - 1) * 30000

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.withdrawal_queue_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=gas_estimate,
                description=f"Request withdrawal of {total_amount} stETH ({len(amounts)} request(s))",
            )

        except Exception as e:
            logger.exception(f"Failed to build request_withdrawal transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def claim_withdrawals(
        self,
        request_ids: list[int],
        hints: list[int] | None = None,
    ) -> TransactionResult:
        """Build a claim withdrawals transaction.

        Claims finalized withdrawal requests, sending ETH to msg.sender.
        Only available on Ethereum mainnet.

        Note: Request IDs are returned when calling requestWithdrawals().
        Hints can be obtained from findCheckpointHints() on the withdrawal queue,
        or pass None/empty list for the contract to compute them (higher gas).

        Args:
            request_ids: List of withdrawal request IDs to claim
            hints: Checkpoint hints for each request ID (optional, improves gas)

        Returns:
            TransactionResult with transaction data
        """
        try:
            if self.withdrawal_queue_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Withdrawal queue not available on {self.chain}. Only Ethereum supported.",
                )

            if not request_ids:
                return TransactionResult(
                    success=False,
                    error="At least one request ID is required.",
                )

            # Use empty hints if not provided (contract will compute, higher gas)
            hint_values = hints if hints else [0] * len(request_ids)

            if len(hint_values) != len(request_ids):
                return TransactionResult(
                    success=False,
                    error=f"Number of hints ({len(hint_values)}) must match number of request IDs ({len(request_ids)}).",
                )

            # Build calldata: claimWithdrawals(uint256[] calldata _requestIds, uint256[] calldata _hints)
            # Both are dynamic arrays, so we need offsets for each
            # Layout:
            # - offset to requestIds array (32 bytes)
            # - offset to hints array (32 bytes)
            # - requestIds array: length + elements
            # - hints array: length + elements
            calldata = LIDO_CLAIM_WITHDRAWALS_SELECTOR

            # Offset to requestIds array (after both offset params = 64 bytes)
            calldata += self._pad_uint256(64)
            # Offset to hints array (after requestIds: 64 + 32 + 32*len)
            hints_offset = 64 + 32 + 32 * len(request_ids)
            calldata += self._pad_uint256(hints_offset)

            # requestIds array: length + elements
            calldata += self._pad_uint256(len(request_ids))
            for req_id in request_ids:
                calldata += self._pad_uint256(req_id)

            # hints array: length + elements
            calldata += self._pad_uint256(len(hint_values))
            for hint in hint_values:
                calldata += self._pad_uint256(hint)

            gas_estimate = DEFAULT_GAS_ESTIMATES["claim_withdrawal"] + (len(request_ids) - 1) * 20000

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.withdrawal_queue_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=gas_estimate,
                description=f"Claim {len(request_ids)} withdrawal request(s)",
            )

        except Exception as e:
            logger.exception(f"Failed to build claim_withdrawals transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Utility Methods
    # =========================================================================

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        addr_clean = addr.lower().replace("0x", "")
        return addr_clean.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    # =========================================================================
    # Intent Compilation Methods
    # =========================================================================

    def compile_stake_intent(
        self,
        intent: StakeIntent,
        market_snapshot: Any | None = None,
    ) -> ActionBundle:
        """Compile a StakeIntent to an ActionBundle.

        This method converts a high-level StakeIntent into executable transaction
        data. It handles the receive_wrapped flag:
        - If receive_wrapped=True: stake ETH -> stETH, then wrap stETH -> wstETH
        - If receive_wrapped=False: stake ETH -> stETH only

        Args:
            intent: The StakeIntent to compile
            market_snapshot: Optional market data (not used for Lido)

        Returns:
            ActionBundle containing transaction(s) for execution

        Raises:
            ValueError: If amount="all" is not resolved before compilation
        """
        from almanak.framework.models.reproduction_bundle import ActionBundle

        # Validate amount is resolved
        if intent.amount == "all":
            raise ValueError(
                "amount='all' must be resolved before compilation. "
                "Use Intent.set_resolved_amount() to resolve chained amounts."
            )

        amount: Decimal = intent.amount  # type: ignore[assignment]

        transactions: list[dict[str, Any]] = []
        total_gas = 0

        # Step 1: Stake ETH to get stETH
        stake_result = self.stake(amount)
        if not stake_result.success or stake_result.tx_data is None:
            return ActionBundle(
                intent_type="STAKE",
                transactions=[],
                metadata={
                    "error": stake_result.error or "Unknown error",
                    "intent_id": intent.intent_id,
                    "protocol": "lido",
                },
            )

        stake_tx_data = stake_result.tx_data
        transactions.append(
            {
                "to": stake_tx_data["to"],
                "value": stake_tx_data["value"],
                "data": stake_tx_data["data"],
                "gas_estimate": stake_result.gas_estimate,
                "description": stake_result.description,
                "action_type": "stake",
            }
        )
        total_gas += stake_result.gas_estimate

        # Step 2: If receive_wrapped, approve stETH spending then wrap to wstETH
        if intent.receive_wrapped:
            if self.wsteth_address is None:
                return ActionBundle(
                    intent_type="STAKE",
                    transactions=[],
                    metadata={
                        "error": f"wstETH not available on {self.chain}",
                        "intent_id": intent.intent_id,
                        "protocol": "lido",
                    },
                )

            # Approve wstETH contract to spend stETH
            amount_wei = int(amount * Decimal(10**18))
            approve_data = "0x095ea7b3" + self._pad_address(self.wsteth_address) + self._pad_uint256(amount_wei)
            transactions.append(
                {
                    "to": self.steth_address,
                    "value": 0,
                    "data": approve_data,
                    "gas_estimate": 50000,
                    "description": f"Approve wstETH to spend {amount} stETH",
                    "action_type": "approve",
                }
            )
            total_gas += 50000

            wrap_result = self.wrap(amount)
            if not wrap_result.success or wrap_result.tx_data is None:
                return ActionBundle(
                    intent_type="STAKE",
                    transactions=[],
                    metadata={
                        "error": wrap_result.error or "Unknown error",
                        "intent_id": intent.intent_id,
                        "protocol": "lido",
                    },
                )

            wrap_tx_data = wrap_result.tx_data
            transactions.append(
                {
                    "to": wrap_tx_data["to"],
                    "value": wrap_tx_data["value"],
                    "data": wrap_tx_data["data"],
                    "gas_estimate": wrap_result.gas_estimate,
                    "description": wrap_result.description,
                    "action_type": "wrap",
                }
            )
            total_gas += wrap_result.gas_estimate

        # Build metadata
        output_token = "wstETH" if intent.receive_wrapped else "stETH"
        metadata = {
            "intent_id": intent.intent_id,
            "protocol": "lido",
            "token_in": intent.token_in,
            "amount": str(amount),
            "output_token": output_token,
            "receive_wrapped": intent.receive_wrapped,
            "chain": self.chain,
            "total_gas_estimate": total_gas,
            "num_transactions": len(transactions),
        }

        return ActionBundle(
            intent_type="STAKE",
            transactions=transactions,
            metadata=metadata,
        )

    def compile_unstake_intent(
        self,
        intent: UnstakeIntent,
        market_snapshot: Any | None = None,
    ) -> ActionBundle:
        """Compile an UnstakeIntent to an ActionBundle.

        This method converts a high-level UnstakeIntent into executable transaction
        data. It handles the token_in type:
        - If token_in is wstETH: unwrap wstETH -> stETH first, then request withdrawal
        - If token_in is stETH: request withdrawal directly

        Note: This only initiates the withdrawal request. Actual ETH claiming happens
        separately after the withdrawal is finalized (claim_withdrawals).

        Args:
            intent: The UnstakeIntent to compile
            market_snapshot: Optional market data (not used for Lido)

        Returns:
            ActionBundle containing transaction(s) for execution

        Raises:
            ValueError: If amount="all" is not resolved before compilation
        """
        from almanak.framework.models.reproduction_bundle import ActionBundle

        # Validate amount is resolved
        if intent.amount == "all":
            raise ValueError(
                "amount='all' must be resolved before compilation. "
                "Use Intent.set_resolved_amount() to resolve chained amounts."
            )

        amount: Decimal = intent.amount  # type: ignore[assignment]

        transactions: list[dict[str, Any]] = []
        total_gas = 0

        # Normalize token_in to uppercase for comparison
        token_in_upper = intent.token_in.upper()
        is_wrapped = token_in_upper in ("WSTETH", "WST_ETH", "WRAPPED_STETH")

        # Step 1: If input is wstETH, unwrap to stETH first
        if is_wrapped:
            unwrap_result = self.unwrap(amount)
            if not unwrap_result.success or unwrap_result.tx_data is None:
                return ActionBundle(
                    intent_type="UNSTAKE",
                    transactions=[],
                    metadata={
                        "error": unwrap_result.error or "Unknown error",
                        "intent_id": intent.intent_id,
                        "protocol": "lido",
                    },
                )

            unwrap_tx_data = unwrap_result.tx_data
            transactions.append(
                {
                    "to": unwrap_tx_data["to"],
                    "value": unwrap_tx_data["value"],
                    "data": unwrap_tx_data["data"],
                    "gas_estimate": unwrap_result.gas_estimate,
                    "description": unwrap_result.description,
                    "action_type": "unwrap",
                }
            )
            total_gas += unwrap_result.gas_estimate

        # Step 2: Request withdrawal for stETH amount
        # For wstETH, the amount is the wstETH amount which unwraps to approximately
        # the same amount of stETH (slight difference due to exchange rate)
        # In production, we'd query the exchange rate, but for now we use 1:1
        steth_amount = amount

        withdrawal_result = self.request_withdrawal([steth_amount], owner=self.wallet_address)
        if not withdrawal_result.success or withdrawal_result.tx_data is None:
            return ActionBundle(
                intent_type="UNSTAKE",
                transactions=[],
                metadata={
                    "error": withdrawal_result.error or "Unknown error",
                    "intent_id": intent.intent_id,
                    "protocol": "lido",
                },
            )

        withdrawal_tx_data = withdrawal_result.tx_data
        transactions.append(
            {
                "to": withdrawal_tx_data["to"],
                "value": withdrawal_tx_data["value"],
                "data": withdrawal_tx_data["data"],
                "gas_estimate": withdrawal_result.gas_estimate,
                "description": withdrawal_result.description,
                "action_type": "request_withdrawal",
            }
        )
        total_gas += withdrawal_result.gas_estimate

        # Build metadata
        metadata = {
            "intent_id": intent.intent_id,
            "protocol": "lido",
            "token_in": intent.token_in,
            "amount": str(amount),
            "output_token": "ETH",  # Final output after withdrawal is finalized
            "requires_unwrap": is_wrapped,
            "chain": self.chain,
            "total_gas_estimate": total_gas,
            "num_transactions": len(transactions),
            "note": "Withdrawal request initiated. ETH claiming available after finalization.",
        }

        return ActionBundle(
            intent_type="UNSTAKE",
            transactions=transactions,
            metadata=metadata,
        )
