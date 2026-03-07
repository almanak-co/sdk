"""BENQI Lending Adapter (Compound V2 fork on Avalanche).

This module provides an adapter for interacting with the BENQI lending protocol
on Avalanche, supporting supply, withdraw, borrow, and repay operations.

BENQI uses a qiToken model (Compound V2 architecture):
- Each asset has a corresponding qiToken (e.g., qiUSDC, qiAVAX)
- Supply mints qiTokens, withdraw redeems them
- Borrowing is per-asset, collateral enabled via Comptroller.enterMarkets()
- Interest accrues via exchange rate between qiToken and underlying

Supported chain: Avalanche

Example:
    from almanak.framework.connectors.benqi import BenqiAdapter, BenqiConfig

    config = BenqiConfig(
        chain="avalanche",
        wallet_address="0x...",
    )
    adapter = BenqiAdapter(config)

    # Supply USDC
    result = adapter.supply(asset="USDC", amount=Decimal("1000"))

    # Borrow AVAX against USDC collateral
    result = adapter.borrow(asset="AVAX", amount=Decimal("10"))
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# BENQI Comptroller address on Avalanche
BENQI_COMPTROLLER_ADDRESS = "0x486Af39519B4Dc9a7fCcd318217352830E8AD9b4"

# qiToken addresses on Avalanche (underlying -> qiToken)
BENQI_QI_TOKENS: dict[str, dict[str, Any]] = {
    "AVAX": {
        "qi_token": "0x5C0401e81Bc07Ca70fAD469b451682c0d747Ef1c",
        "underlying": None,  # Native AVAX
        "decimals": 18,
        "is_native": True,
    },
    "USDC": {
        "qi_token": "0xB715808a78F6041E46d61Cb123C9B4A27056AE9C",
        "underlying": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "decimals": 6,
        "is_native": False,
    },
    "USDT": {
        "qi_token": "0xd8fcDa6ec4Bdc547C0827B8804e89aCd817d56EF",
        "underlying": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
        "decimals": 6,
        "is_native": False,
    },
    "WETH.e": {
        "qi_token": "0x334AD834Cd4481BB02d09615E7c11a00579A7909",
        "underlying": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        "decimals": 18,
        "is_native": False,
    },
    "BTC.b": {
        "qi_token": "0x89a415b3D20098E6A6C8f7a59001C67BD3129821",
        "underlying": "0x152b9d0FdC40C096DE20232Db1E35AE6A57FA6c0",
        "decimals": 8,
        "is_native": False,
    },
    "sAVAX": {
        "qi_token": "0xF362feA9659cf036792c9cb02f8ff8198E21B4cB",
        "underlying": "0x2b2C81e08f1Af8835a78Bb2A90AE924ACE0eA4bE",
        "decimals": 18,
        "is_native": False,
    },
}

# Function selectors for qiToken operations (Compound V2 ABI)
BENQI_MINT_SELECTOR = "0xa0712d68"  # mint(uint256) - ERC20 qiTokens
BENQI_MINT_NATIVE_SELECTOR = "0x1249c58b"  # mint() payable - qiAVAX
BENQI_REDEEM_SELECTOR = "0xdb006a75"  # redeem(uint256) - by qiToken amount
BENQI_REDEEM_UNDERLYING_SELECTOR = "0x852a12e3"  # redeemUnderlying(uint256) - by underlying amount
BENQI_BORROW_SELECTOR = "0xc5ebeaec"  # borrow(uint256)
BENQI_REPAY_BORROW_SELECTOR = "0x0e752702"  # repayBorrow(uint256)
BENQI_REPAY_BORROW_NATIVE_SELECTOR = "0x4e4d9fea"  # repayBorrow() payable - qiAVAX

# Comptroller function selectors
BENQI_ENTER_MARKETS_SELECTOR = "0xc2998238"  # enterMarkets(address[])

# ERC20 approve
ERC20_APPROVE_SELECTOR = "0x095ea7b3"  # approve(address,uint256)

# Max uint256 for approvals
MAX_UINT256 = 2**256 - 1

# Gas estimates for BENQI operations (measured on Avalanche mainnet fork).
# These are used as gas limits for dependent transactions in the simulator
# with no additional buffer, so they include ~40% headroom over measured values.
# Measured: approve=55K, supply=247K, enterMarkets=99K, borrow=489K, repay=~300K, withdraw=~350K
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "supply": 350000,
    "supply_native": 350000,
    "withdraw": 500000,
    "borrow": 700000,
    "repay": 500000,
    "repay_native": 500000,
    "enter_markets": 200000,
    "approve": 80000,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class BenqiConfig:
    """Configuration for BENQI adapter."""

    chain: str = "avalanche"
    wallet_address: str = ""
    default_slippage_bps: int = 50

    def __post_init__(self) -> None:
        if self.chain != "avalanche":
            raise ValueError(f"BENQI is only available on Avalanche, got: {self.chain}")


@dataclass
class BenqiMarketInfo:
    """Information about a BENQI market (qiToken)."""

    asset: str
    qi_token_address: str
    underlying_address: str | None
    decimals: int
    is_native: bool


@dataclass
class BenqiPosition:
    """User position in BENQI."""

    supplied: dict[str, Decimal] = field(default_factory=dict)
    borrowed: dict[str, Decimal] = field(default_factory=dict)


@dataclass
class TransactionResult:
    """Result of building a transaction."""

    success: bool
    tx_data: dict[str, Any] | None = None
    gas_estimate: int = 0
    description: str = ""
    error: str | None = None


# =============================================================================
# Adapter
# =============================================================================


class BenqiAdapter:
    """Adapter for BENQI lending protocol on Avalanche.

    Provides methods to build transactions for supply, withdraw, borrow, and repay
    operations using the qiToken (Compound V2) architecture.
    """

    def __init__(self, config: BenqiConfig, token_resolver: TokenResolverType | None = None) -> None:
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address
        self.comptroller_address = BENQI_COMPTROLLER_ADDRESS
        self.token_resolver = token_resolver

    def _resolve_asset_key(self, asset: str) -> str | None:
        """Resolve asset symbol to canonical BENQI_QI_TOKENS key.

        Handles case-insensitive matching for mixed-case keys like WETH.e, BTC.b, sAVAX.
        """
        if asset in BENQI_QI_TOKENS:
            return asset
        asset_upper = asset.upper()
        for key in BENQI_QI_TOKENS:
            if key.upper() == asset_upper:
                return key
        return None

    def get_market_info(self, asset: str) -> BenqiMarketInfo | None:
        """Get market info for an asset."""
        canonical_key = self._resolve_asset_key(asset)
        if canonical_key is None:
            return None
        market = BENQI_QI_TOKENS[canonical_key]
        return BenqiMarketInfo(
            asset=canonical_key,
            qi_token_address=market["qi_token"],
            underlying_address=market["underlying"],
            decimals=market["decimals"],
            is_native=market["is_native"],
        )

    def get_qi_token_address(self, asset: str) -> str | None:
        """Get the qiToken address for an asset."""
        canonical_key = self._resolve_asset_key(asset)
        if canonical_key is None:
            return None
        market = BENQI_QI_TOKENS.get(canonical_key)
        return market["qi_token"] if market else None

    def get_supported_assets(self) -> list[str]:
        """Get list of supported asset symbols."""
        return list(BENQI_QI_TOKENS.keys())

    # -------------------------------------------------------------------------
    # Supply (Lend)
    # -------------------------------------------------------------------------

    def supply(self, asset: str, amount: Decimal) -> TransactionResult:
        """Build a supply (lend) transaction.

        For ERC20 tokens: calls mint(uint256) on the qiToken.
        For AVAX: calls mint() payable on qiAVAX.

        Args:
            asset: Asset symbol (e.g., "USDC", "AVAX")
            amount: Amount in underlying token units (e.g., 1000 USDC)

        Returns:
            TransactionResult with TX data for execution
        """
        market = self.get_market_info(asset)
        if not market:
            return TransactionResult(
                success=False,
                error=f"Unsupported asset: {asset}. Supported: {self.get_supported_assets()}",
            )

        amount_wei = int(amount * Decimal(10**market.decimals))

        if market.is_native:
            # qiAVAX: mint() payable with msg.value
            calldata = BENQI_MINT_NATIVE_SELECTOR
            return TransactionResult(
                success=True,
                tx_data={
                    "to": market.qi_token_address,
                    "data": calldata,
                    "value": amount_wei,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply_native"],
                description=f"Supply {amount} AVAX to BENQI (mint qiAVAX)",
            )
        else:
            # ERC20: mint(uint256 mintAmount) on qiToken
            calldata = BENQI_MINT_SELECTOR + self._encode_uint256(amount_wei)
            return TransactionResult(
                success=True,
                tx_data={
                    "to": market.qi_token_address,
                    "data": calldata,
                    "value": 0,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply"],
                description=f"Supply {amount} {asset} to BENQI (mint qi{asset})",
            )

    # -------------------------------------------------------------------------
    # Withdraw (Redeem)
    # -------------------------------------------------------------------------

    def withdraw(self, asset: str, amount: Decimal, *, withdraw_all: bool = False) -> TransactionResult:
        """Build a withdraw transaction.

        Calls redeemUnderlying(uint256) on the qiToken to withdraw exact underlying amount.
        For withdraw_all, uses redeem(uint256.max) to redeem all qiTokens.

        Args:
            asset: Asset symbol
            amount: Amount of underlying to withdraw
            withdraw_all: If True, withdraw entire balance

        Returns:
            TransactionResult with TX data
        """
        market = self.get_market_info(asset)
        if not market:
            return TransactionResult(
                success=False,
                error=f"Unsupported asset: {asset}. Supported: {self.get_supported_assets()}",
            )

        if withdraw_all:
            # Compound V2 redeem() requires exact qiToken amount, not MAX_UINT256.
            # Without RPC access to query qiToken balance, withdraw_all is unsupported.
            return TransactionResult(
                success=False,
                error=(
                    "withdraw_all is not supported for BENQI without on-chain qiToken balance. "
                    "Use a specific amount with redeemUnderlying instead."
                ),
            )
        else:
            amount_wei = int(amount * Decimal(10**market.decimals))
            # redeemUnderlying(uint256) for exact underlying amount
            calldata = BENQI_REDEEM_UNDERLYING_SELECTOR + self._encode_uint256(amount_wei)
            description = f"Withdraw {amount} {asset} from BENQI (redeem qi{asset})"

        return TransactionResult(
            success=True,
            tx_data={
                "to": market.qi_token_address,
                "data": calldata,
                "value": 0,
            },
            gas_estimate=DEFAULT_GAS_ESTIMATES["withdraw"],
            description=description,
        )

    # -------------------------------------------------------------------------
    # Borrow
    # -------------------------------------------------------------------------

    def borrow(self, asset: str, amount: Decimal) -> TransactionResult:
        """Build a borrow transaction.

        Calls borrow(uint256) on the qiToken. Requires collateral to be supplied
        and enterMarkets() called first.

        Args:
            asset: Asset symbol to borrow
            amount: Amount to borrow in underlying units

        Returns:
            TransactionResult with TX data
        """
        market = self.get_market_info(asset)
        if not market:
            return TransactionResult(
                success=False,
                error=f"Unsupported asset: {asset}. Supported: {self.get_supported_assets()}",
            )

        amount_wei = int(amount * Decimal(10**market.decimals))
        calldata = BENQI_BORROW_SELECTOR + self._encode_uint256(amount_wei)

        return TransactionResult(
            success=True,
            tx_data={
                "to": market.qi_token_address,
                "data": calldata,
                "value": 0,
            },
            gas_estimate=DEFAULT_GAS_ESTIMATES["borrow"],
            description=f"Borrow {amount} {asset} from BENQI",
        )

    # -------------------------------------------------------------------------
    # Repay
    # -------------------------------------------------------------------------

    def repay(self, asset: str, amount: Decimal, *, repay_all: bool = False) -> TransactionResult:
        """Build a repay transaction.

        For ERC20: calls repayBorrow(uint256) on the qiToken.
        For AVAX: calls repayBorrow() payable on qiAVAX.
        For repay_all: uses MAX_UINT256 to repay full debt.

        Args:
            asset: Asset symbol
            amount: Amount to repay
            repay_all: If True, repay entire outstanding debt

        Returns:
            TransactionResult with TX data
        """
        market = self.get_market_info(asset)
        if not market:
            return TransactionResult(
                success=False,
                error=f"Unsupported asset: {asset}. Supported: {self.get_supported_assets()}",
            )

        if market.is_native:
            # qiAVAX: repayBorrow() payable
            # For repay_all on native AVAX, the caller must pass the outstanding
            # debt balance as `amount` since we cannot query on-chain state here.
            amount_wei = int(amount * Decimal(10**market.decimals))
            calldata = BENQI_REPAY_BORROW_NATIVE_SELECTOR
            return TransactionResult(
                success=True,
                tx_data={
                    "to": market.qi_token_address,
                    "data": calldata,
                    "value": amount_wei,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["repay_native"],
                description=f"Repay {'all' if repay_all else amount} AVAX to BENQI",
            )
        else:
            repay_amount = MAX_UINT256 if repay_all else int(amount * Decimal(10**market.decimals))
            calldata = BENQI_REPAY_BORROW_SELECTOR + self._encode_uint256(repay_amount)
            desc = f"Repay all {asset} debt on BENQI" if repay_all else f"Repay {amount} {asset} to BENQI"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": market.qi_token_address,
                    "data": calldata,
                    "value": 0,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["repay"],
                description=desc,
            )

    # -------------------------------------------------------------------------
    # Comptroller Operations
    # -------------------------------------------------------------------------

    def enter_markets(self, assets: list[str]) -> TransactionResult:
        """Build an enterMarkets transaction to enable assets as collateral.

        Args:
            assets: List of asset symbols to enable as collateral

        Returns:
            TransactionResult with TX data
        """
        qi_token_addresses = []
        for asset in assets:
            market = self.get_market_info(asset)
            if not market:
                return TransactionResult(
                    success=False,
                    error=f"Unsupported asset: {asset}",
                )
            qi_token_addresses.append(market.qi_token_address)

        # enterMarkets(address[]) - ABI encode the address array
        encoded = self._encode_address_array(qi_token_addresses)
        calldata = BENQI_ENTER_MARKETS_SELECTOR + encoded

        return TransactionResult(
            success=True,
            tx_data={
                "to": self.comptroller_address,
                "data": calldata,
                "value": 0,
            },
            gas_estimate=DEFAULT_GAS_ESTIMATES["enter_markets"] * len(assets),
            description=f"Enable {', '.join(assets)} as collateral on BENQI",
        )

    # -------------------------------------------------------------------------
    # ABI Encoding Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _encode_uint256(value: int) -> str:
        """Encode a uint256 value as hex (no 0x prefix)."""
        return f"{value:064x}"

    @staticmethod
    def _encode_address(address: str) -> str:
        """Encode an address as 32-byte hex (no 0x prefix)."""
        addr = address.lower().replace("0x", "")
        return addr.zfill(64)

    @staticmethod
    def _encode_address_array(addresses: list[str]) -> str:
        """ABI encode a dynamic address array (no 0x prefix).

        Layout: offset(32) + length(32) + address[0](32) + address[1](32) + ...
        """
        offset = f"{32:064x}"  # offset to start of array data
        length = f"{len(addresses):064x}"
        encoded_addrs = "".join(BenqiAdapter._encode_address(a) for a in addresses)
        return offset + length + encoded_addrs
