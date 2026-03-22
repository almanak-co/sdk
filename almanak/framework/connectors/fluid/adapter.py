"""Fluid DEX Adapter — swaps + LP scaffolding for Arbitrum.

Provides swap and LP operations for Fluid DEX T1 pools on Arbitrum.
Phase 1: swaps via swapIn() are fully functional. LP deposit reverts
on-chain (Liquidity-layer routing — follow-up for phase 2).

Example:
    from almanak.framework.connectors.fluid import FluidAdapter, FluidConfig

    config = FluidConfig(
        chain="arbitrum",
        wallet_address="0x...",
        rpc_url="https://...",
    )
    adapter = FluidAdapter(config)
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from web3 import Web3

from almanak.framework.connectors.fluid.sdk import (
    DEFAULT_GAS_ESTIMATES,
    DexPoolData,
    FluidSDK,
    FluidSDKError,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)

# Max int256 for full withdrawal
MAX_INT256 = 2**255 - 1

# Max uint256 for unlimited approvals
MAX_UINT256 = 2**256 - 1


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class FluidConfig:
    """Configuration for Fluid DEX adapter.

    Args:
        chain: Chain name (must be "arbitrum" for phase 1)
        wallet_address: Address of the wallet executing transactions
        rpc_url: RPC endpoint URL
        default_slippage_bps: Default slippage tolerance in basis points (default: 50 = 0.5%)
    """

    chain: str
    wallet_address: str
    rpc_url: str
    default_slippage_bps: int = 50


@dataclass
class FluidPositionDetails:
    """Typed position details for Fluid DEX LP positions.

    Stored in PositionInfo.details as a dict (via asdict()).
    String NFT ID is consistent with Uniswap V3 / TraderJoe patterns.

    Attributes:
        fluid_nft_id: NFT token ID (string for consistency with other connectors)
        dex_address: Pool contract address
        token0: Token0 address
        token1: Token1 address
        swap_fee_apr: Swap fee APR (from exchange price data)
        lending_yield_apr: Lending yield APR (from liquidity resolver)
        combined_apr: Combined APR (swap_fee_apr + lending_yield_apr)
        is_smart_collateral: Whether pool has smart collateral enabled
        is_smart_debt: Whether pool has smart debt enabled
    """

    fluid_nft_id: str
    dex_address: str
    token0: str
    token1: str
    swap_fee_apr: float = 0.0
    lending_yield_apr: float = 0.0
    combined_apr: float = 0.0
    is_smart_collateral: bool = False
    is_smart_debt: bool = False


@dataclass
class TransactionData:
    """Transaction data for Fluid operations.

    Attributes:
        to: Target contract address
        data: Encoded calldata (hex string)
        value: Native token value (wei)
        gas: Gas estimate
        description: Human-readable description
        tx_type: Transaction type identifier
    """

    to: str
    data: str
    value: int = 0
    gas: int = 0
    description: str = ""
    tx_type: str = "fluid_operate"

    @property
    def gas_estimate(self) -> int:
        return self.gas

    def to_dict(self) -> dict[str, Any]:
        return {
            "to": self.to,
            "data": self.data,
            "value": self.value,
            "gas_estimate": self.gas,
            "description": self.description,
            "tx_type": self.tx_type,
        }


# =============================================================================
# FluidAdapter
# =============================================================================


class FluidAdapter:
    """High-level adapter for Fluid DEX LP operations.

    Provides LP open/close with compile-time encumbrance guard.
    Phase 1 operates only on unencumbered pools (no smart-debt/collateral).

    Args:
        config: FluidConfig with chain, wallet, and RPC settings
        token_resolver: Optional TokenResolver for symbol -> address resolution
    """

    def __init__(
        self,
        config: FluidConfig,
        token_resolver: "TokenResolverType | None" = None,
    ) -> None:
        self.config = config
        self.chain = config.chain.lower()

        if self.chain != "arbitrum":
            raise FluidSDKError(f"Fluid DEX phase 1 supports Arbitrum only. Got: {config.chain}")

        self._sdk = FluidSDK(chain=self.chain, rpc_url=config.rpc_url)

        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens import get_token_resolver

            self._token_resolver = get_token_resolver()

    # =========================================================================
    # Token Resolution
    # =========================================================================

    def resolve_token_address(self, token: str) -> str:
        """Resolve a token symbol or address to checksummed address.

        Args:
            token: Token symbol (e.g., "USDC") or address

        Returns:
            Checksummed address
        """
        if token.startswith("0x") and len(token) == 42:
            return Web3.to_checksum_address(token)
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return Web3.to_checksum_address(resolved.address)
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[FluidAdapter] Cannot resolve token: {e.reason}",
            ) from e

    def get_token_decimals(self, token: str) -> int:
        """Get decimals for a token.

        Args:
            token: Token symbol or address

        Returns:
            Token decimals (never defaults to 18 — raises if unknown)
        """
        return self._token_resolver.get_decimals(self.chain, token)

    # =========================================================================
    # Pool Discovery
    # =========================================================================

    def find_pool(self, token0: str, token1: str) -> str | None:
        """Find a Fluid DEX pool for a token pair.

        Args:
            token0: First token symbol or address
            token1: Second token symbol or address

        Returns:
            Pool address if found, None otherwise
        """
        addr0 = self.resolve_token_address(token0)
        addr1 = self.resolve_token_address(token1)
        return self._sdk.find_dex_by_tokens(addr0, addr1)

    def get_pool_data(self, dex_address: str) -> DexPoolData:
        """Get full pool data from the resolver.

        Args:
            dex_address: Pool contract address

        Returns:
            DexPoolData with configs, reserves, and encumbrance flags
        """
        return self._sdk.get_dex_data(dex_address)

    # =========================================================================
    # Encumbrance Guard (Phase 2)
    # =========================================================================
    # On-chain position-level debt verification is a phase-2 requirement.
    # Phase 1 enforces newDebt=0 at the SDK layer (build_operate_tx),
    # so positions created by this connector are always unencumbered.
    # Phase 2 will add readFromStorage-based debt checks for arbitrary NFT IDs.

    # =========================================================================
    # LP Open
    # =========================================================================

    def build_add_liquidity_transaction(
        self,
        dex_address: str,
        amount0: Decimal,
        amount1: Decimal,
        token0_decimals: int,
        token1_decimals: int,
    ) -> TransactionData:
        """Build a transaction to open a new LP position.

        Phase 1 limitation: Fluid DEX deposit() reverts on all pools due to complex
        Liquidity-layer routing. LP open is wired but not functional on-chain.
        Proper share calculation requires Liquidity-layer integration (follow-up).

        Raises:
            FluidSDKError: Always — LP deposit is not yet supported.
        """
        raise FluidSDKError(
            "Fluid DEX LP deposit is not yet supported on-chain. "
            "The Liquidity-layer routing causes reverts on all pools. "
            "This is a known phase-1 limitation — LP support is a follow-up."
        )

    # =========================================================================
    # LP Close
    # =========================================================================

    def build_remove_liquidity_transaction(
        self,
        dex_address: str,
        nft_id: int,
    ) -> TransactionData:
        """Build a transaction to close an LP position (full withdrawal).

        Calls operate(nftId, -MAX_INT256, 0, wallet) on the pool contract.
        The negative max collateral delta means "withdraw everything".

        ENCUMBRANCE GUARD: This method refuses to build the transaction if the
        pool has smart-collateral or smart-debt enabled.

        Args:
            dex_address: Pool contract address
            nft_id: NFT position ID to close

        Returns:
            TransactionData with the operate() call

        Raises:
            FluidSDKError: If the operation fails
        """
        # Phase 1: positions are always opened with newDebt=0 (enforced by SDK),
        # so they're safe to close. Phase 2 will add on-chain debt verification.
        if self._sdk.is_position_encumbered(dex_address, nft_id=nft_id):
            raise FluidSDKError(
                f"Position #{nft_id} in {dex_address} has outstanding debt. Cannot close encumbered positions safely."
            )

        tx_data = self._sdk.build_operate_tx(
            dex_address=dex_address,
            nft_id=nft_id,
            new_col=-MAX_INT256,  # Full withdrawal
            new_debt=0,  # No debt changes
            to=self.config.wallet_address,
        )

        return TransactionData(
            to=tx_data["to"],
            data=tx_data["data"],
            value=tx_data["value"],
            gas=tx_data.get("gas", DEFAULT_GAS_ESTIMATES["operate_close"]),
            description=f"Close Fluid LP position #{nft_id} in {dex_address}",
            tx_type="fluid_operate_close",
        )

    # =========================================================================
    # Position Details
    # =========================================================================

    def get_position_details(
        self,
        nft_id: int,
        dex_address: str,
    ) -> FluidPositionDetails:
        """Build FluidPositionDetails for a position.

        Reads pool data from the resolver to populate APR fields.

        Args:
            nft_id: NFT position ID
            dex_address: Pool contract address

        Returns:
            FluidPositionDetails with pool data and APR info
        """
        pool_data = self._sdk.get_dex_data(dex_address)

        # Estimate swap fee APR from fee_bps and reserves
        # This is a rough estimate — actual APR depends on volume
        swap_fee_apr = pool_data.fee_bps / 10000.0 * 365  # Very rough annualized

        # Lending yield APR from exchange price growth
        # exchange prices use 1e12 precision
        lending_yield_apr = 0.0  # Requires historical data to compute

        return FluidPositionDetails(
            fluid_nft_id=str(nft_id),
            dex_address=dex_address,
            token0=pool_data.token0,
            token1=pool_data.token1,
            swap_fee_apr=swap_fee_apr,
            lending_yield_apr=lending_yield_apr,
            combined_apr=swap_fee_apr + lending_yield_apr,
            is_smart_collateral=pool_data.is_smart_collateral,
            is_smart_debt=pool_data.is_smart_debt,
        )

    # =========================================================================
    # Approval helpers
    # =========================================================================

    def build_approve_tx(
        self,
        token_address: str,
        spender: str,
        amount: int | None = None,
    ) -> TransactionData:
        """Build an ERC20 approval transaction.

        Args:
            token_address: Token contract address
            spender: Address to approve spending for
            amount: Amount to approve (None = max uint256)

        Returns:
            TransactionData for the approval
        """
        approve_amount = amount if amount is not None else MAX_UINT256

        # Use ABI encoding for safety (avoid manual hex encoding on money-critical path)
        erc20_approve_abi = [
            {
                "inputs": [
                    {"name": "spender", "type": "address"},
                    {"name": "amount", "type": "uint256"},
                ],
                "name": "approve",
                "outputs": [{"type": "bool"}],
                "stateMutability": "nonpayable",
                "type": "function",
            }
        ]
        token_contract = Web3().eth.contract(
            address=Web3.to_checksum_address(token_address),
            abi=erc20_approve_abi,
        )
        data = token_contract.encode_abi(
            "approve",
            [Web3.to_checksum_address(spender), approve_amount],
        )

        return TransactionData(
            to=Web3.to_checksum_address(token_address),
            data=data,
            value=0,
            gas=DEFAULT_GAS_ESTIMATES["approve"],
            description=f"Approve {spender} to spend token {token_address}",
            tx_type="approve",
        )

    # =========================================================================
    # Result Enrichment - called by ResultEnricher
    # =========================================================================

    def extract_position_id(self, receipt: dict) -> int | None:
        """Extract LP position NFT tokenId from operate() receipt.

        Called by ResultEnricher for LP_OPEN intents.

        Args:
            receipt: Transaction receipt dict

        Returns:
            NFT position ID or None if not found
        """
        from almanak.framework.connectors.fluid.receipt_parser import FluidReceiptParser

        parser = FluidReceiptParser()
        return parser.extract_position_id(receipt)
