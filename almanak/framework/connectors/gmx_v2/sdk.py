"""
GMX V2 Perpetuals SDK for Arbitrum

GMX V2 is a decentralized perpetual exchange that uses an order-based system.
Unlike AMMs, positions are opened/closed through orders that are executed by keepers.

Key concepts:
- Orders are created via ExchangeRouter.createOrder()
- Orders require execution fees paid in ETH
- Keepers execute orders asynchronously
- Positions are identified by (account, market, collateralToken, isLong)

Order Types:
- MarketIncrease (2): Open or increase a position at market price
- MarketDecrease (4): Close or decrease a position at market price
- LimitIncrease (3): Open/increase at a limit price
- LimitDecrease (5): Close/decrease at a limit price

This SDK is ported from src-v0 and simplified for the new intent-based architecture.
"""

import json
import os
from dataclasses import dataclass
from enum import IntEnum
from typing import Any

from web3 import Web3
from web3.contract import Contract

from almanak.core.contracts import GMX_V2, GMX_V2_TOKENS


class OrderType(IntEnum):
    """GMX V2 Order Types"""

    MARKET_SWAP = 0
    LIMIT_SWAP = 1
    MARKET_INCREASE = 2
    LIMIT_INCREASE = 3
    MARKET_DECREASE = 4
    LIMIT_DECREASE = 5
    STOP_LOSS_DECREASE = 6
    LIQUIDATION = 7


class DecreasePositionSwapType(IntEnum):
    """GMX V2 Decrease Position Swap Types"""

    NO_SWAP = 0
    SWAP_PNL_TOKEN_TO_COLLATERAL_TOKEN = 1
    SWAP_COLLATERAL_TOKEN_TO_PNL_TOKEN = 2


# GMX V2 Arbitrum Contract Addresses (verified from Arbiscan - Jan 2026)
# Note: GMX upgraded the Exchange Router in Oct 2025
GMX_V2_SDK_ADDRESSES = {
    "arbitrum": {
        "EXCHANGE_ROUTER": GMX_V2["arbitrum"]["exchange_router"],
        "DATA_STORE": GMX_V2["arbitrum"]["data_store"],
        "ORDER_VAULT": GMX_V2["arbitrum"]["order_vault"],
        "READER": GMX_V2["arbitrum"]["reader"],
        # Markets
        "ETH_USD_MARKET": GMX_V2["arbitrum"]["eth_usd_market"],
        "BTC_USD_MARKET": GMX_V2["arbitrum"]["btc_usd_market"],
        # Tokens
        "WETH": GMX_V2_TOKENS["arbitrum"]["WETH"],
        "WBTC": GMX_V2_TOKENS["arbitrum"]["WBTC"],
        "USDC": GMX_V2_TOKENS["arbitrum"]["USDC"],
        "USDT": GMX_V2_TOKENS["arbitrum"]["USDT"],
    }
}

# Minimum execution fee (in wei) - keepers need this to execute orders
# GMX V2 validates: executionFee >= gasLimit * tx.gasprice
# Note: 0.001 ETH is sufficient for testing; increase to 0.005 ETH for production
MIN_EXECUTION_FEE_FALLBACK = 1_000_000_000_000_000  # 0.001 ETH (testing)

# Gas limits for different order types
INCREASE_ORDER_GAS_LIMIT = 3_000_000  # ~3M gas for increase orders
DECREASE_ORDER_GAS_LIMIT = 3_000_000  # ~3M gas for decrease orders


@dataclass
class GMXV2OrderParams:
    """Parameters for creating a GMX V2 order."""

    from_address: str
    market: str
    initial_collateral_token: str
    initial_collateral_delta_amount: int
    size_delta_usd: int
    is_long: bool
    acceptable_price: int
    execution_fee: int
    trigger_price: int = 0
    referral_code: bytes = b"\x00" * 32


@dataclass
class GMXV2TransactionData:
    """Transaction data returned by the SDK."""

    to: str
    value: int
    data: str
    gas_estimate: int
    description: str


class GMXV2SDK:
    """
    SDK for interacting with GMX V2 perpetuals on Arbitrum.

    This SDK builds transactions for creating orders using the ExchangeRouter's
    multicall function which atomically:
    1. Sends collateral to OrderVault (via sendWnt or sendTokens)
    2. Creates the order
    """

    GAS_BUFFER = 0.3

    def __init__(self, rpc_url: str, chain: str = "arbitrum"):
        """
        Initialize GMX V2 SDK.

        Args:
            rpc_url: RPC endpoint URL
            chain: Target chain (only 'arbitrum' supported currently)
        """
        if chain != "arbitrum":
            raise ValueError(f"GMX V2 SDK only supports Arbitrum, got {chain}")

        self.web3 = Web3(Web3.HTTPProvider(rpc_url))
        self.chain = chain

        # Get contract addresses
        self.addresses = GMX_V2_SDK_ADDRESSES[chain]
        self.EXCHANGE_ROUTER_ADDRESS = self.addresses["EXCHANGE_ROUTER"]
        self.ORDER_VAULT_ADDRESS = self.addresses["ORDER_VAULT"]
        self.WETH_ADDRESS = self.addresses["WETH"]

        # Set ABI directory
        self.abi_dir = os.path.join(os.path.dirname(__file__), "abis")

        # Load contracts
        self.exchange_router = self._load_contract("exchange_router", self.EXCHANGE_ROUTER_ADDRESS)
        self.erc20_abi = self._load_abi("erc20")

    def _load_abi(self, name: str) -> list[dict]:
        """Load ABI from file."""
        abi_path = os.path.join(self.abi_dir, f"{name}.json")
        with open(abi_path) as f:
            return json.load(f)

    def _load_contract(self, abi_name: str, address: str) -> Contract:
        """Load contract with ABI."""
        abi = self._load_abi(abi_name)
        return self.web3.eth.contract(address=self.web3.to_checksum_address(address), abi=abi)

    def get_market_address(self, index_token_symbol: str) -> str:
        """
        Get GMX V2 market address for an index token.

        Args:
            index_token_symbol: "ETH" or "BTC"

        Returns:
            Market address
        """
        markets = {
            "ETH": self.addresses["ETH_USD_MARKET"],
            "BTC": self.addresses["BTC_USD_MARKET"],
            "WETH": self.addresses["ETH_USD_MARKET"],
            "WBTC": self.addresses["BTC_USD_MARKET"],
            "ETH/USD": self.addresses["ETH_USD_MARKET"],
            "BTC/USD": self.addresses["BTC_USD_MARKET"],
        }
        market = markets.get(index_token_symbol.upper())
        if not market:
            raise ValueError(f"Unsupported market: {index_token_symbol}. Supported: ETH, BTC")
        return market

    def get_execution_fee(
        self,
        order_type: str = "increase",
        multiplier: float = 1.5,
    ) -> int:
        """
        Calculate execution fee for GMX order dynamically.

        GMX V2 validates: executionFee >= adjustedGasLimit * tx.gasprice

        Args:
            order_type: "increase" or "decrease" to select appropriate gas limit
            multiplier: Safety multiplier (default 1.5x for testing, use 2.0x for production)

        Returns:
            Execution fee in wei
        """
        gas_limit = DECREASE_ORDER_GAS_LIMIT if order_type == "decrease" else INCREASE_ORDER_GAS_LIMIT

        try:
            gas_price = self.web3.eth.gas_price
            execution_fee = int(gas_limit * gas_price * multiplier)
            return max(execution_fee, MIN_EXECUTION_FEE_FALLBACK)
        except Exception:
            return MIN_EXECUTION_FEE_FALLBACK * 2

    def build_increase_order_multicall(
        self,
        params: GMXV2OrderParams,
    ) -> GMXV2TransactionData:
        """
        Build a multicall transaction to create an increase order.

        This combines:
        1. sendWnt or sendTokens (collateral to OrderVault)
        2. createOrder

        Args:
            params: Order parameters

        Returns:
            Transaction data ready for execution
        """
        calls = []

        # Check if collateral is WETH (send ETH) or other token
        is_weth_collateral = self.web3.to_checksum_address(
            params.initial_collateral_token
        ) == self.web3.to_checksum_address(self.WETH_ADDRESS)

        # GMX V2 requires sendWnt for execution fee to OrderVault
        # Call 1: sendWnt for execution fee (always required)
        send_exec_fee_call = self.exchange_router.encode_abi(
            "sendWnt", [self.ORDER_VAULT_ADDRESS, params.execution_fee]
        )
        calls.append(bytes.fromhex(send_exec_fee_call[2:]))  # Remove 0x prefix

        if is_weth_collateral:
            # Call 2: sendWnt for collateral (sends ETH as WETH to OrderVault)
            send_wnt_call = self.exchange_router.encode_abi(
                "sendWnt", [self.ORDER_VAULT_ADDRESS, params.initial_collateral_delta_amount]
            )
            calls.append(bytes.fromhex(send_wnt_call[2:]))  # Remove 0x prefix
        else:
            # Call 2: sendTokens (sends ERC20 to OrderVault)
            send_tokens_call = self.exchange_router.encode_abi(
                "sendTokens",
                [
                    self.web3.to_checksum_address(params.initial_collateral_token),
                    self.ORDER_VAULT_ADDRESS,
                    params.initial_collateral_delta_amount,
                ],
            )
            calls.append(bytes.fromhex(send_tokens_call[2:]))

        # Build order params struct
        addresses: tuple[Any, ...] = (
            self.web3.to_checksum_address(params.from_address),  # receiver
            self.web3.to_checksum_address(params.from_address),  # cancellationReceiver
            "0x0000000000000000000000000000000000000000",  # callbackContract
            "0x0000000000000000000000000000000000000000",  # uiFeeReceiver
            self.web3.to_checksum_address(params.market),  # market
            self.web3.to_checksum_address(params.initial_collateral_token),  # initialCollateralToken
            [],  # swapPath (empty for direct collateral)
        )

        numbers = (
            params.size_delta_usd,  # sizeDeltaUsd
            params.initial_collateral_delta_amount,  # initialCollateralDeltaAmount
            params.trigger_price,  # triggerPrice (0 for market orders)
            params.acceptable_price,  # acceptablePrice
            params.execution_fee,  # executionFee
            0,  # callbackGasLimit
            0,  # minOutputAmount
            0,  # validFromTime
        )

        referral_code = (
            params.referral_code
            if isinstance(params.referral_code, bytes)
            else bytes.fromhex(params.referral_code.replace("0x", ""))
        )

        order_params: tuple[Any, ...] = (
            addresses,
            numbers,
            OrderType.MARKET_INCREASE,  # orderType
            DecreasePositionSwapType.NO_SWAP,  # decreasePositionSwapType
            params.is_long,  # isLong
            False,  # shouldUnwrapNativeToken
            False,  # autoCancel
            referral_code,  # referralCode
            [],  # dataList
        )

        # Call 3: createOrder
        create_order_call = self.exchange_router.encode_abi("createOrder", [order_params])
        calls.append(bytes.fromhex(create_order_call[2:]))

        # Calculate total value to send
        total_value = params.execution_fee
        if is_weth_collateral:
            total_value += params.initial_collateral_delta_amount

        # Build multicall transaction
        multicall_calldata = self.exchange_router.encode_abi("multicall", [calls])

        gas_estimate = int(INCREASE_ORDER_GAS_LIMIT * (1 + self.GAS_BUFFER))

        return GMXV2TransactionData(
            to=self.EXCHANGE_ROUTER_ADDRESS,
            value=total_value,
            data=multicall_calldata,
            gas_estimate=gas_estimate,
            description=f"Open {'LONG' if params.is_long else 'SHORT'} position via GMX V2 multicall",
        )

    def build_decrease_order_multicall(
        self,
        params: GMXV2OrderParams,
    ) -> GMXV2TransactionData:
        """
        Build a multicall transaction to create a decrease order.

        For decrease orders, no collateral needs to be sent to OrderVault.
        Only the execution fee is needed, sent via sendWnt.

        Args:
            params: Order parameters

        Returns:
            Transaction data ready for execution
        """
        calls = []

        # Call 1: sendWnt for execution fee (always required)
        send_exec_fee_call = self.exchange_router.encode_abi(
            "sendWnt", [self.ORDER_VAULT_ADDRESS, params.execution_fee]
        )
        calls.append(bytes.fromhex(send_exec_fee_call[2:]))  # Remove 0x prefix

        # Build order params struct
        addresses: tuple[Any, ...] = (
            self.web3.to_checksum_address(params.from_address),  # receiver
            self.web3.to_checksum_address(params.from_address),  # cancellationReceiver
            "0x0000000000000000000000000000000000000000",  # callbackContract
            "0x0000000000000000000000000000000000000000",  # uiFeeReceiver
            self.web3.to_checksum_address(params.market),  # market
            self.web3.to_checksum_address(params.initial_collateral_token),  # initialCollateralToken
            [],  # swapPath
        )

        numbers = (
            params.size_delta_usd,  # sizeDeltaUsd
            params.initial_collateral_delta_amount,  # initialCollateralDeltaAmount
            params.trigger_price,  # triggerPrice (0 for market orders)
            params.acceptable_price,  # acceptablePrice
            params.execution_fee,  # executionFee
            0,  # callbackGasLimit
            0,  # minOutputAmount
            0,  # validFromTime
        )

        referral_code = (
            params.referral_code
            if isinstance(params.referral_code, bytes)
            else bytes.fromhex(params.referral_code.replace("0x", ""))
        )

        order_params: tuple[Any, ...] = (
            addresses,
            numbers,
            OrderType.MARKET_DECREASE,  # orderType
            DecreasePositionSwapType.NO_SWAP,  # decreasePositionSwapType
            params.is_long,  # isLong
            True,  # shouldUnwrapNativeToken (receive ETH if collateral is WETH)
            False,  # autoCancel
            referral_code,  # referralCode
            [],  # dataList
        )

        # Call 2: createOrder
        create_order_call = self.exchange_router.encode_abi("createOrder", [order_params])
        calls.append(bytes.fromhex(create_order_call[2:]))

        # Build multicall transaction
        multicall_calldata = self.exchange_router.encode_abi("multicall", [calls])

        gas_estimate = int(DECREASE_ORDER_GAS_LIMIT * (1 + self.GAS_BUFFER))

        return GMXV2TransactionData(
            to=self.EXCHANGE_ROUTER_ADDRESS,
            value=params.execution_fee,
            data=multicall_calldata,
            gas_estimate=gas_estimate,
            description=f"Close {'LONG' if params.is_long else 'SHORT'} position via GMX V2",
        )


def get_gmx_v2_sdk(rpc_url: str, chain: str = "arbitrum") -> GMXV2SDK:
    """Factory function to create a GMX V2 SDK instance."""
    return GMXV2SDK(rpc_url, chain)
