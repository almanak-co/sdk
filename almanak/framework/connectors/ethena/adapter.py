"""Ethena Adapter.

This module provides an adapter for interacting with Ethena protocol.

Ethena is a synthetic dollar protocol supporting:
- Stake USDe to receive sUSDe (yield-bearing)
- Unstake sUSDe to receive USDe (with cooldown period)

Supported chains:
- Ethereum (full staking + unstaking)

sUSDe is an ERC4626 vault token that accrues yield from delta-neutral strategies.

Example:
    from almanak.framework.connectors.ethena import EthenaAdapter, EthenaConfig

    config = EthenaConfig(
        chain="ethereum",
        wallet_address="0x...",
    )
    adapter = EthenaAdapter(config)

    # Stake USDe to receive sUSDe
    result = adapter.stake_usde(amount=Decimal("1000.0"))

    # Start cooldown to unstake sUSDe
    result = adapter.unstake_susde(amount=Decimal("1000.0"))
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

# Ethena contract addresses per chain
ETHENA_ADDRESSES: dict[str, dict[str, str]] = {
    "ethereum": {
        "usde": "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",
        "susde": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
    },
}

# Function selectors
# ERC20.approve(address spender, uint256 amount)
ERC20_APPROVE_SELECTOR = "0x095ea7b3"
# sUSDe.deposit(uint256 assets, address receiver) - ERC4626 deposit
ETHENA_DEPOSIT_SELECTOR = "0x6e553f65"
# sUSDe.cooldownAssets(uint256 assets) - Start cooldown for unstaking
# Selector: cast sig "cooldownAssets(uint256)" = 0xcdac52ed
ETHENA_COOLDOWN_ASSETS_SELECTOR = "0xcdac52ed"
# sUSDe.cooldownShares(uint256 shares) - Start cooldown for unstaking (shares variant)
# Selector: cast sig "cooldownShares(uint256)" = 0x9343d9e1
ETHENA_COOLDOWN_SHARES_SELECTOR = "0x9343d9e1"
# sUSDe.unstake(address receiver) - Complete unstake after cooldown
# Selector: cast sig "unstake(address)" = 0xf2888dbb
# Bug history: was 0x2e17de78 (unstake(uint256)) -- caught by iter 99, fixed VIB-1529
ETHENA_UNSTAKE_SELECTOR = "0xf2888dbb"

# Gas estimates for Ethena operations
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "approve": 50000,
    "stake": 150000,
    "unstake_cooldown": 120000,
    "unstake_complete": 100000,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class EthenaConfig:
    """Configuration for Ethena adapter.

    Attributes:
        chain: Blockchain network (ethereum)
        wallet_address: User wallet address
    """

    chain: str
    wallet_address: str

    def __post_init__(self) -> None:
        """Validate configuration."""
        valid_chains = set(ETHENA_ADDRESSES.keys())
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


class EthenaAdapter:
    """Adapter for Ethena synthetic dollar protocol.

    This adapter provides methods for interacting with Ethena:
    - Stake USDe to receive sUSDe (yield-bearing vault token)
    - Unstake sUSDe to receive USDe (requires cooldown period)

    Note: sUSDe is an ERC4626 vault. Unstaking has a cooldown period
    (typically 7 days) before assets can be withdrawn.

    Example:
        config = EthenaConfig(
            chain="ethereum",
            wallet_address="0x...",
        )
        adapter = EthenaAdapter(config)

        # Stake USDe to get sUSDe
        result = adapter.stake_usde(Decimal("1000.0"))

        # Start cooldown for unstaking
        result = adapter.unstake_susde(Decimal("1000.0"))
    """

    def __init__(self, config: EthenaConfig, token_resolver: TokenResolverType | None = None) -> None:
        """Initialize the adapter.

        Args:
            config: Adapter configuration
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address

        # Contract addresses
        chain_addresses = ETHENA_ADDRESSES[config.chain]
        self.usde_address = chain_addresses.get("usde")
        self.susde_address = chain_addresses.get("susde")

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        logger.info(f"EthenaAdapter initialized for chain={config.chain}, wallet={config.wallet_address[:10]}...")

    def approve_usde(self, amount: Decimal) -> TransactionResult:
        """Build an approval transaction for USDe to sUSDe contract.

        This must be called before staking to allow the sUSDe contract
        to transfer USDe from the user's wallet.

        Args:
            amount: Amount of USDe to approve

        Returns:
            TransactionResult with approval transaction data
        """
        try:
            if self.usde_address is None or self.susde_address is None:
                return TransactionResult(
                    success=False,
                    error=f"USDe/sUSDe not available on {self.chain}.",
                )

            # Amount in wei (USDe has 18 decimals)
            amount_wei = int(amount * Decimal(10**18))

            # Build calldata: approve(address spender, uint256 amount)
            calldata = ERC20_APPROVE_SELECTOR + self._pad_address(self.susde_address) + self._pad_uint256(amount_wei)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.usde_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["approve"],
                description=f"Approve {amount} USDe for sUSDe contract",
            )

        except Exception as e:
            logger.exception(f"Failed to build approval transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def stake_usde(
        self,
        amount: Decimal,
        receiver: str | None = None,
    ) -> TransactionResult:
        """Build a stake transaction to deposit USDe and receive sUSDe.

        Deposits USDe into the sUSDe ERC4626 vault to receive yield-bearing sUSDe.

        Args:
            amount: Amount of USDe to stake
            receiver: Address to receive sUSDe (default: wallet_address)

        Returns:
            TransactionResult with transaction data
        """
        try:
            if self.susde_address is None:
                return TransactionResult(
                    success=False,
                    error=f"sUSDe not available on {self.chain}.",
                )

            receiver_addr = receiver or self.wallet_address

            # Amount in wei (USDe has 18 decimals)
            amount_wei = int(amount * Decimal(10**18))

            # Build calldata: deposit(uint256 assets, address receiver)
            calldata = ETHENA_DEPOSIT_SELECTOR + self._pad_uint256(amount_wei) + self._pad_address(receiver_addr)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.susde_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["stake"],
                description=f"Stake {amount} USDe to Ethena for sUSDe",
            )

        except Exception as e:
            logger.exception(f"Failed to build stake transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def unstake_susde(self, amount: Decimal) -> TransactionResult:
        """Build a transaction to start cooldown for unstaking sUSDe.

        Initiates the cooldown period for unstaking sUSDe. After the cooldown
        period (typically 7 days), the USDe can be withdrawn.

        Args:
            amount: Amount of sUSDe assets to unstake (in USDe terms)

        Returns:
            TransactionResult with transaction data
        """
        try:
            if self.susde_address is None:
                return TransactionResult(
                    success=False,
                    error=f"sUSDe not available on {self.chain}.",
                )

            # Amount in wei
            amount_wei = int(amount * Decimal(10**18))

            # Build calldata: cooldownAssets(uint256 assets)
            calldata = ETHENA_COOLDOWN_ASSETS_SELECTOR + self._pad_uint256(amount_wei)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.susde_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["unstake_cooldown"],
                description=f"Start cooldown for unstaking {amount} USDe worth of sUSDe",
            )

        except Exception as e:
            logger.exception(f"Failed to build unstake transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def complete_unstake(self, receiver: str | None = None) -> TransactionResult:
        """Build a transaction to complete unstaking after cooldown period.

        Completes the unstaking process after the cooldown period (typically 7 days)
        has elapsed. This withdraws the previously locked USDe to the receiver.

        Args:
            receiver: Address to receive the USDe (default: wallet_address)

        Returns:
            TransactionResult with transaction data
        """
        try:
            if self.susde_address is None:
                return TransactionResult(
                    success=False,
                    error=f"sUSDe not available on {self.chain}.",
                )

            receiver_addr = receiver or self.wallet_address

            # Build calldata: unstake(address receiver)
            calldata = ETHENA_UNSTAKE_SELECTOR + self._pad_address(receiver_addr)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.susde_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["unstake_complete"],
                description=f"Complete unstaking to {receiver_addr[:10]}...",
            )

        except Exception as e:
            logger.exception(f"Failed to build complete_unstake transaction: {e}")
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
        data. For Ethena, this stakes USDe to receive sUSDe.

        Args:
            intent: The StakeIntent to compile
            market_snapshot: Optional market data (not used for Ethena)

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

        # Step 1: Approve USDe for sUSDe contract
        approve_result = self.approve_usde(amount)
        if not approve_result.success or approve_result.tx_data is None:
            return ActionBundle(
                intent_type="STAKE",
                transactions=[],
                metadata={
                    "error": approve_result.error or "Approval failed",
                    "intent_id": intent.intent_id,
                    "protocol": "ethena",
                },
            )

        approve_tx_data = approve_result.tx_data
        transactions.append(
            {
                "to": approve_tx_data["to"],
                "value": approve_tx_data["value"],
                "data": approve_tx_data["data"],
                "gas_estimate": approve_result.gas_estimate,
                "description": approve_result.description,
                "action_type": "approve",
            }
        )
        total_gas += approve_result.gas_estimate

        # Step 2: Stake USDe to get sUSDe
        stake_result = self.stake_usde(amount)
        if not stake_result.success or stake_result.tx_data is None:
            return ActionBundle(
                intent_type="STAKE",
                transactions=[],
                metadata={
                    "error": stake_result.error or "Unknown error",
                    "intent_id": intent.intent_id,
                    "protocol": "ethena",
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

        # Build metadata
        metadata = {
            "intent_id": intent.intent_id,
            "protocol": "ethena",
            "token_in": intent.token_in,
            "amount": str(amount),
            "output_token": "sUSDe",
            "chain": self.chain,
            "total_gas_estimate": total_gas,
            "num_transactions": len(transactions),
        }

        return ActionBundle(
            intent_type="STAKE",
            transactions=transactions,
            metadata=metadata,
        )

    def _compile_complete_unstake(self, intent: UnstakeIntent) -> ActionBundle:
        """Compile phase 2 of Ethena unstake: call ``unstake(address receiver)``."""
        from almanak.framework.models.reproduction_bundle import ActionBundle

        result = self.complete_unstake(receiver=self.wallet_address)
        if not result.success or result.tx_data is None:
            return ActionBundle(
                intent_type="UNSTAKE",
                transactions=[],
                metadata={
                    "error": result.error or "complete_unstake failed",
                    "protocol": "ethena",
                    "phase": "complete",
                },
            )

        return ActionBundle(
            intent_type="UNSTAKE",
            transactions=[
                {
                    "to": result.tx_data["to"],
                    "value": result.tx_data["value"],
                    "data": result.tx_data["data"],
                    "gas_estimate": result.gas_estimate,
                    "description": result.description,
                    "action_type": "unstake_complete",
                }
            ],
            metadata={
                "protocol": "ethena",
                "phase": "complete",
                "receiver": self.wallet_address,
                "chain": self.chain,
                "note": "Phase 2: unstake(address receiver) called after cooldown expired.",
            },
        )

    def compile_unstake_intent(
        self,
        intent: UnstakeIntent,
        market_snapshot: Any | None = None,
    ) -> ActionBundle:
        """Compile an UnstakeIntent to an ActionBundle.

        This method converts a high-level UnstakeIntent into executable transaction
        data. For Ethena, this supports two phases via ``protocol_params``:

        Phase 1 (default, ``protocol_params={"phase": "cooldown"}``):
            Initiates the 7-day cooldown by calling ``cooldownAssets(uint256)``.
        Phase 2 (``protocol_params={"phase": "complete"}``):
            Completes the withdrawal after cooldown by calling ``unstake(address)``.
            Only valid after cooldown has elapsed (use Anvil ``evm_increaseTime``
            for testing).

        Args:
            intent: The UnstakeIntent to compile
            market_snapshot: Optional market data (not used for Ethena)

        Returns:
            ActionBundle containing transaction(s) for execution

        Raises:
            ValueError: If amount="all" is not resolved before compilation
        """
        from almanak.framework.models.reproduction_bundle import ActionBundle

        # Route to phase 2 (complete unstake) when requested via protocol_params
        phase = (intent.protocol_params or {}).get("phase", "cooldown")
        if phase not in ("cooldown", "complete"):
            raise ValueError(f"Invalid Ethena unstake phase: {phase!r}. Must be 'cooldown' or 'complete'.")
        if phase == "complete":
            if intent.amount != "all":
                raise ValueError(
                    "Ethena complete unstake withdraws the full matured cooldown balance; use amount='all'."
                )
            return self._compile_complete_unstake(intent)

        # Validate amount is resolved
        if intent.amount == "all":
            raise ValueError(
                "amount='all' must be resolved before compilation. "
                "Use Intent.set_resolved_amount() to resolve chained amounts."
            )

        amount: Decimal = intent.amount  # type: ignore[assignment]

        transactions: list[dict[str, Any]] = []
        total_gas = 0

        # Initiate cooldown for sUSDe -> USDe (phase 1)
        cooldown_result = self.unstake_susde(amount)
        if not cooldown_result.success or cooldown_result.tx_data is None:
            return ActionBundle(
                intent_type="UNSTAKE",
                transactions=[],
                metadata={
                    "error": cooldown_result.error or "Unknown error",
                    "intent_id": intent.intent_id,
                    "protocol": "ethena",
                },
            )

        cooldown_tx_data = cooldown_result.tx_data
        transactions.append(
            {
                "to": cooldown_tx_data["to"],
                "value": cooldown_tx_data["value"],
                "data": cooldown_tx_data["data"],
                "gas_estimate": cooldown_result.gas_estimate,
                "description": cooldown_result.description,
                "action_type": "cooldown",
            }
        )
        total_gas += cooldown_result.gas_estimate

        # Build metadata
        metadata = {
            "intent_id": intent.intent_id,
            "protocol": "ethena",
            "token_in": intent.token_in,
            "amount": str(amount),
            "output_token": "USDe",  # Final output after cooldown completes
            "chain": self.chain,
            "total_gas_estimate": total_gas,
            "num_transactions": len(transactions),
            "cooldown_required": True,
            "note": "Cooldown initiated. USDe withdrawal available after ~7 days via complete_unstake().",
        }

        return ActionBundle(
            intent_type="UNSTAKE",
            transactions=transactions,
            metadata=metadata,
        )
