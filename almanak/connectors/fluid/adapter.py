"""Fluid DEX Adapter — swap-surface helpers.

Thin high-level wrapper over :class:`FluidSDK` for operator tooling and
scripts: token resolution, pool discovery, quoting, and approve/swap
transaction building. The intent path does NOT go through this adapter —
``FluidCompiler`` drives the SDK directly.

LP scaffolding that previously lived here was removed in Phase 1
(VIB-5029): it modelled the wrong contract family (Vault-style
``operate(nftId, …)``, which does not exist on DEX pools) and direct pool
LP deposits are whitelist-gated on-chain anyway (Phase-0 finding,
VIB-5028 §V4). LP support returns via SmartLending / smart vaults in
Phase 4 (VIB-5032).

Example:
    from almanak.connectors.fluid import FluidAdapter, FluidConfig

    config = FluidConfig(
        chain="arbitrum",
        wallet_address="0x...",
        rpc_url="https://...",
    )
    adapter = FluidAdapter(config)
"""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from web3 import Web3

from almanak.connectors.fluid.sdk import (
    DEFAULT_GAS_ESTIMATES,
    FLUID_ADDRESSES,
    FLUID_NATIVE_TOKEN,
    DexPoolData,
    FluidSDK,
    FluidSDKError,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)

# Max uint256 for unlimited approvals
MAX_UINT256 = 2**256 - 1


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class FluidConfig:
    """Configuration for Fluid DEX adapter.

    Args:
        chain: Chain name (any chain in ``FLUID_ADDRESSES``)
        wallet_address: Address of the wallet executing transactions
        rpc_url: DEPRECATED — direct RPC URL. Kept for ad-hoc script usage.
            Strategies running in isolated containers must use gateway_client.
        gateway_client: Gateway client for routing eth_call through the
            gateway's RpcService. Preferred over rpc_url.
        default_slippage_bps: Default slippage tolerance in basis points (default: 50 = 0.5%)
    """

    chain: str
    wallet_address: str
    rpc_url: str | None = None
    default_slippage_bps: int = 50
    gateway_client: "GatewayClient | None" = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.rpc_url is None and self.gateway_client is None:
            raise FluidSDKError("FluidConfig requires either rpc_url (deprecated) or gateway_client")


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
    tx_type: str = "fluid_swap"

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
    """High-level adapter for Fluid DEX swap operations.

    Args:
        config: FluidConfig with chain, wallet, and transport settings
        token_resolver: Optional TokenResolver for symbol -> address resolution
    """

    def __init__(
        self,
        config: FluidConfig,
        token_resolver: "TokenResolverType | None" = None,
    ) -> None:
        self.config = config
        self.chain = config.chain.lower()

        if self.chain not in FLUID_ADDRESSES:
            raise FluidSDKError(
                f"Fluid DEX not supported on chain: {config.chain}. Supported: {list(FLUID_ADDRESSES.keys())}"
            )

        self._sdk = FluidSDK(
            chain=self.chain,
            rpc_url=config.rpc_url,
            gateway_client=config.gateway_client,
        )

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
    # Pool Discovery + Quoting
    # =========================================================================

    def find_pool(self, token0: str, token1: str) -> str | None:
        """Find a Fluid DEX pool for a token pair (order-insensitive).

        Args:
            token0: First token symbol or address
            token1: Second token symbol or address

        Returns:
            Pool address if found, None otherwise
        """
        addr0 = self.resolve_token_address(token0)
        addr1 = self.resolve_token_address(token1)
        return self._sdk.find_dex_by_tokens(addr0, addr1)

    def find_pool_for_pair(self, token_in: str, token_out: str) -> tuple[str, bool] | None:
        """Find the pool and swap direction for an exact-input pair.

        Returns ``(pool_address, swap0to1)`` or None.
        """
        addr_in = self.resolve_token_address(token_in)
        addr_out = self.resolve_token_address(token_out)
        return self._sdk.find_pool_for_pair(addr_in, addr_out)

    def get_pool_data(self, dex_address: str) -> DexPoolData:
        """Get full pool data (tokens + smart-collateral/debt flags)."""
        return self._sdk.get_dex_data(dex_address)

    def get_swap_quote(self, token_in: str, token_out: str, amount_in: int) -> int:
        """Quote an exact-input swap via the DexReservesResolver.

        Args:
            token_in: Input token symbol or address
            token_out: Output token symbol or address
            amount_in: Input amount in the token's smallest unit

        Returns:
            Expected output amount in the token's smallest unit

        Raises:
            FluidSDKError: If no pool exists or the quote fails
            FluidMinAmountError: If the size is limit-gated (retryable)
        """
        found = self.find_pool_for_pair(token_in, token_out)
        if found is None:
            raise FluidSDKError(f"No Fluid pool for {token_in}->{token_out} on {self.chain}")
        pool_address, swap0to1 = found
        return self._sdk.get_swap_quote(pool_address, swap0to1, amount_in)

    # =========================================================================
    # Transaction building
    # =========================================================================

    def build_swap_transaction(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        amount_out_min: int,
        value: int | None = None,
    ) -> TransactionData:
        """Build a ``swapIn`` transaction for an exact-input swap.

        Args:
            token_in: Input token symbol or address
            token_out: Output token symbol or address
            amount_in: Input amount in the token's smallest unit
            amount_out_min: Minimum acceptable output (slippage protection)
            value: Native value. Defaults to ``amount_in`` for native inputs
                and ``0`` for ERC-20 inputs; an explicit value that
                contradicts the input leg raises (the pool enforces
                ``msg.value == amountIn`` for native, ``0`` for ERC-20 —
                a mismatched transaction would revert on-chain).

        Returns:
            TransactionData targeting the per-pair pool contract
        """
        found = self.find_pool_for_pair(token_in, token_out)
        if found is None:
            raise FluidSDKError(f"No Fluid pool for {token_in}->{token_out} on {self.chain}")
        pool_address, swap0to1 = found

        native_input = self.resolve_token_address(token_in).lower() == FLUID_NATIVE_TOKEN.lower()
        required_value = amount_in if native_input else 0
        if value is None:
            value = required_value
        elif value != required_value:
            leg = "native" if native_input else "ERC-20"
            raise FluidSDKError(
                f"value={value} contradicts the {leg} input leg of {token_in}->{token_out}: "
                f"Fluid pools require msg.value == {required_value} (got {value}); "
                f"the transaction would revert on-chain"
            )
        tx = self._sdk.build_swap_tx(
            dex_address=pool_address,
            swap0to1=swap0to1,
            amount_in=amount_in,
            amount_out_min=amount_out_min,
            to=self.config.wallet_address,
            value=value,
        )
        return TransactionData(
            to=tx["to"],
            data=tx["data"],
            value=tx["value"],
            gas=tx["gas"],
            description=f"Swap {token_in} -> {token_out} via Fluid pool {pool_address}",
            tx_type="swap",
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
