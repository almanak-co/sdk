"""Joe Lend (Banker Joe) Adapter (Compound V2 fork on Avalanche).

This module provides an adapter for interacting with the Joe Lend (Banker Joe)
lending protocol on Avalanche, supporting supply, withdraw, borrow, and repay operations.

Joe Lend uses a jToken model (Compound V2 architecture):
- Each asset has a corresponding jToken (e.g., jUSDC, jAVAX)
- Supply mints jTokens, withdraw redeems them
- Borrowing is per-asset, collateral enabled via Joetroller.enterMarkets()
- Interest accrues via exchange rate between jToken and underlying

Supported chain: Avalanche

Example:
    from almanak.framework.connectors.joelend import JoeLendAdapter, JoeLendConfig

    config = JoeLendConfig(
        chain="avalanche",
        wallet_address="0x...",
    )
    adapter = JoeLendAdapter(config)

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

# Joetroller (Comptroller) address on Avalanche
JOELEND_JOETROLLER_ADDRESS = "0xdc13687554205E5b89Ac783db14bb5bba4A1eDaC"

# jToken addresses on Avalanche (underlying -> jToken)
JOELEND_J_TOKENS: dict[str, dict[str, Any]] = {
    "AVAX": {
        "j_token": "0xC22F01ddc8010Ee05574028528614634684EC29e",
        "underlying": None,  # Native AVAX
        "decimals": 18,
        "is_native": True,
    },
    "USDC.e": {
        "j_token": "0xEd6AaF91a2B084bd594DBd1245be3691F9f637aC",
        "underlying": "0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664",
        "decimals": 6,
        "is_native": False,
    },
    "USDT.e": {
        "j_token": "0x8b650e26404AC6837539ca96812f0123601E4448",
        "underlying": "0xc7198437980c041c805A1EDcbA50c1Ce5db95118",
        "decimals": 6,
        "is_native": False,
    },
    "WETH.e": {
        "j_token": "0x929f5caB61DFEc79a5431a7734a68D714C4633fa",
        "underlying": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        "decimals": 18,
        "is_native": False,
    },
    "WBTC.e": {
        "j_token": "0x3fE38b7b610C0ACD10296fEf69d9b18eB7a9eB1F",
        "underlying": "0x50b7545627a5162F82A992c33b87aDc75187B218",
        "decimals": 8,
        "is_native": False,
    },
    "DAI.e": {
        "j_token": "0xc988c170d0E38197DC634A45bF00169C7Aa7CA19",
        "underlying": "0xd586E7F844cEa2F87f50152665BCbc2C279D8d70",
        "decimals": 18,
        "is_native": False,
    },
}

# Function selectors for jToken operations (Compound V2 ABI)
JOELEND_MINT_SELECTOR = "0xa0712d68"  # mint(uint256) - ERC20 jTokens
JOELEND_MINT_NATIVE_SELECTOR = "0x1249c58b"  # mint() payable - jAVAX
JOELEND_REDEEM_SELECTOR = "0xdb006a75"  # redeem(uint256) - by jToken amount
JOELEND_REDEEM_UNDERLYING_SELECTOR = "0x852a12e3"  # redeemUnderlying(uint256) - by underlying amount
JOELEND_BORROW_SELECTOR = "0xc5ebeaec"  # borrow(uint256)
JOELEND_REPAY_BORROW_SELECTOR = "0x0e752702"  # repayBorrow(uint256)
JOELEND_REPAY_BORROW_NATIVE_SELECTOR = "0x4e4d9fea"  # repayBorrow() payable - jAVAX

# Joetroller function selectors
JOELEND_ENTER_MARKETS_SELECTOR = "0xc2998238"  # enterMarkets(address[])
JOELEND_EXIT_MARKET_SELECTOR = "0xede4edd0"  # exitMarket(address)

# ERC20 approve
ERC20_APPROVE_SELECTOR = "0x095ea7b3"  # approve(address,uint256)

# Max uint256 for approvals
MAX_UINT256 = 2**256 - 1

# Gas estimates for Joe Lend operations (measured on Avalanche mainnet fork).
# These are used as gas limits for dependent transactions in the simulator
# with no additional buffer, so they include ~40% headroom over measured values.
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
class JoeLendConfig:
    """Configuration for Joe Lend adapter."""

    chain: str = "avalanche"
    wallet_address: str = ""
    default_slippage_bps: int = 50

    def __post_init__(self) -> None:
        if self.chain != "avalanche":
            raise ValueError(f"Joe Lend is only available on Avalanche, got: {self.chain}")


@dataclass
class JoeLendMarketInfo:
    """Information about a Joe Lend market (jToken)."""

    asset: str
    j_token_address: str
    underlying_address: str | None
    decimals: int
    is_native: bool


@dataclass
class JoeLendPosition:
    """User position in Joe Lend."""

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


class JoeLendAdapter:
    """Adapter for Joe Lend (Banker Joe) lending protocol on Avalanche.

    Provides methods to build transactions for supply, withdraw, borrow, and repay
    operations using the jToken (Compound V2) architecture.
    """

    def __init__(self, config: JoeLendConfig, token_resolver: TokenResolverType | None = None) -> None:
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address
        self.joetroller_address = JOELEND_JOETROLLER_ADDRESS
        self.token_resolver = token_resolver

    def _resolve_asset_key(self, asset: str) -> str | None:
        """Resolve asset symbol to canonical JOELEND_J_TOKENS key.

        Handles case-insensitive matching for mixed-case keys like WETH.e, WBTC.e, DAI.e.
        """
        if asset in JOELEND_J_TOKENS:
            return asset
        asset_upper = asset.upper()
        for key in JOELEND_J_TOKENS:
            if key.upper() == asset_upper:
                return key
        return None

    def get_market_info(self, asset: str) -> JoeLendMarketInfo | None:
        """Get market info for an asset."""
        canonical_key = self._resolve_asset_key(asset)
        if canonical_key is None:
            return None
        market = JOELEND_J_TOKENS[canonical_key]
        return JoeLendMarketInfo(
            asset=canonical_key,
            j_token_address=market["j_token"],
            underlying_address=market["underlying"],
            decimals=market["decimals"],
            is_native=market["is_native"],
        )

    def get_j_token_address(self, asset: str) -> str | None:
        """Get the jToken address for an asset."""
        canonical_key = self._resolve_asset_key(asset)
        if canonical_key is None:
            return None
        market = JOELEND_J_TOKENS.get(canonical_key)
        return market["j_token"] if market else None

    def get_supported_assets(self) -> list[str]:
        """Get list of supported asset symbols."""
        return list(JOELEND_J_TOKENS.keys())

    # -------------------------------------------------------------------------
    # Supply (Lend)
    # -------------------------------------------------------------------------

    def supply(self, asset: str, amount: Decimal) -> TransactionResult:
        """Build a supply (lend) transaction.

        For ERC20 tokens: calls mint(uint256) on the jToken.
        For AVAX: calls mint() payable on jAVAX.

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
            # jAVAX: mint() payable with msg.value
            calldata = JOELEND_MINT_NATIVE_SELECTOR
            return TransactionResult(
                success=True,
                tx_data={
                    "to": market.j_token_address,
                    "data": calldata,
                    "value": amount_wei,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply_native"],
                description=f"Supply {amount} AVAX to Joe Lend (mint jAVAX)",
            )
        else:
            # ERC20: mint(uint256 mintAmount) on jToken
            calldata = JOELEND_MINT_SELECTOR + self._encode_uint256(amount_wei)
            return TransactionResult(
                success=True,
                tx_data={
                    "to": market.j_token_address,
                    "data": calldata,
                    "value": 0,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply"],
                description=f"Supply {amount} {asset} to Joe Lend (mint j{asset})",
            )

    # -------------------------------------------------------------------------
    # Withdraw (Redeem)
    # -------------------------------------------------------------------------

    def withdraw(
        self, asset: str, amount: Decimal, *, withdraw_all: bool = False, redeem_amount: int | None = None
    ) -> TransactionResult:
        """Build a withdraw transaction.

        Calls redeemUnderlying(uint256) on the jToken to withdraw exact underlying amount.
        For withdraw_all with redeem_amount, uses redeem(uint256) with exact jToken count.
        For withdraw_all with amount > 0, uses redeemUnderlying(amount_wei) with the tracked amount.

        Args:
            asset: Asset symbol
            amount: Amount of underlying to withdraw
            withdraw_all: If True, withdraw entire balance
            redeem_amount: Exact jToken amount for redeem() when withdraw_all=True.
                Not yet wired from the compiler — reserved for future use when
                the compiler can query on-chain jToken balances.

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
            if redeem_amount is not None:
                # Use redeem(uint256) with exact jToken amount
                calldata = JOELEND_REDEEM_SELECTOR + self._encode_uint256(redeem_amount)
                description = f"Withdraw all {asset} from Joe Lend (redeem {redeem_amount} j{asset})"
            elif amount > 0:
                # Use redeemUnderlying(uint256) with the strategy's tracked supply amount
                amount_wei = int(amount * Decimal(10**market.decimals))
                calldata = JOELEND_REDEEM_UNDERLYING_SELECTOR + self._encode_uint256(amount_wei)
                description = f"Withdraw all ~{amount} {asset} from Joe Lend (redeem j{asset})"
            else:
                return TransactionResult(
                    success=False,
                    error=(
                        "withdraw_all requires redeem_amount parameter (jToken balance) "
                        "or a positive amount for redeemUnderlying."
                    ),
                )
        else:
            amount_wei = int(amount * Decimal(10**market.decimals))
            # redeemUnderlying(uint256) for exact underlying amount
            calldata = JOELEND_REDEEM_UNDERLYING_SELECTOR + self._encode_uint256(amount_wei)
            description = f"Withdraw {amount} {asset} from Joe Lend (redeem j{asset})"

        return TransactionResult(
            success=True,
            tx_data={
                "to": market.j_token_address,
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

        Calls borrow(uint256) on the jToken. Requires collateral to be supplied
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
        calldata = JOELEND_BORROW_SELECTOR + self._encode_uint256(amount_wei)

        return TransactionResult(
            success=True,
            tx_data={
                "to": market.j_token_address,
                "data": calldata,
                "value": 0,
            },
            gas_estimate=DEFAULT_GAS_ESTIMATES["borrow"],
            description=f"Borrow {amount} {asset} from Joe Lend",
        )

    # -------------------------------------------------------------------------
    # Repay
    # -------------------------------------------------------------------------

    def repay(self, asset: str, amount: Decimal, *, repay_all: bool = False) -> TransactionResult:
        """Build a repay transaction.

        For ERC20: calls repayBorrow(uint256) on the jToken.
        For AVAX: calls repayBorrow() payable on jAVAX.
        For repay_all on ERC20: uses MAX_UINT256 to repay full debt.
        For repay_all on native AVAX: caller must pass exact debt as amount
        (MAX_UINT256 trick doesn't apply to msg.value).

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
            # jAVAX: repayBorrow() payable
            amount_wei = int(amount * Decimal(10**market.decimals))
            if repay_all:
                if amount_wei == 0:
                    return TransactionResult(
                        success=False,
                        error="repay_all on native AVAX requires a positive amount (query debt balance first).",
                    )
                # Add 0.1% buffer to account for interest accrual between query and execution.
                # Assumes sub-minute execution latency (sufficient for ~876% APY).
                # Excess native AVAX is returned by the protocol automatically.
                amount_wei = int(amount_wei * Decimal("1.001"))
            calldata = JOELEND_REPAY_BORROW_NATIVE_SELECTOR
            return TransactionResult(
                success=True,
                tx_data={
                    "to": market.j_token_address,
                    "data": calldata,
                    "value": amount_wei,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["repay_native"],
                description=f"Repay {'all' if repay_all else amount} AVAX to Joe Lend",
            )
        else:
            repay_amount = MAX_UINT256 if repay_all else int(amount * Decimal(10**market.decimals))
            calldata = JOELEND_REPAY_BORROW_SELECTOR + self._encode_uint256(repay_amount)
            desc = f"Repay all {asset} debt on Joe Lend" if repay_all else f"Repay {amount} {asset} to Joe Lend"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": market.j_token_address,
                    "data": calldata,
                    "value": 0,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["repay"],
                description=desc,
            )

    # -------------------------------------------------------------------------
    # Joetroller Operations
    # -------------------------------------------------------------------------

    def enter_markets(self, assets: list[str]) -> TransactionResult:
        """Build an enterMarkets transaction to enable assets as collateral.

        Args:
            assets: List of asset symbols to enable as collateral

        Returns:
            TransactionResult with TX data
        """
        j_token_addresses = []
        for asset in assets:
            market = self.get_market_info(asset)
            if not market:
                return TransactionResult(
                    success=False,
                    error=f"Unsupported asset: {asset}",
                )
            j_token_addresses.append(market.j_token_address)

        # enterMarkets(address[]) - ABI encode the address array
        encoded = self._encode_address_array(j_token_addresses)
        calldata = JOELEND_ENTER_MARKETS_SELECTOR + encoded

        return TransactionResult(
            success=True,
            tx_data={
                "to": self.joetroller_address,
                "data": calldata,
                "value": 0,
            },
            gas_estimate=DEFAULT_GAS_ESTIMATES["enter_markets"] * len(assets),
            description=f"Enable {', '.join(assets)} as collateral on Joe Lend",
        )

    def exit_market(self, asset: str) -> TransactionResult:
        """Build an exitMarket transaction to remove an asset from collateral.

        Note: exitMarket is per-asset (not array), unlike enterMarkets.
        Will revert if the user has outstanding borrows against this collateral.

        Args:
            asset: Asset symbol to remove from collateral

        Returns:
            TransactionResult with TX data
        """
        market = self.get_market_info(asset)
        if not market:
            return TransactionResult(
                success=False,
                error=f"Unsupported asset: {asset}",
            )

        calldata = JOELEND_EXIT_MARKET_SELECTOR + self._encode_address(market.j_token_address)

        return TransactionResult(
            success=True,
            tx_data={
                "to": self.joetroller_address,
                "data": calldata,
                "value": 0,
            },
            gas_estimate=DEFAULT_GAS_ESTIMATES["enter_markets"],
            description=f"Remove {asset} from Joe Lend collateral",
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
        encoded_addrs = "".join(JoeLendAdapter._encode_address(a) for a in addresses)
        return offset + length + encoded_addrs
