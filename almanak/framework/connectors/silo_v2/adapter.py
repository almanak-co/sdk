"""Silo V2 adapter for Almanak SDK.

Silo V2 is an isolated lending protocol on Avalanche. Each market consists of
two ERC-4626 vaults (silo0 + silo1) sharing a SiloConfig contract.

Architecture:
- SiloConfig: Immutable config contract per market pair
- Silo0 / Silo1: ERC-4626 vaults, one per asset in the pair
- Collateral is implicit: depositing into one silo enables borrowing from the paired silo
- No enterMarkets / comptroller — each market is fully isolated

Supported markets on Avalanche:
- WAVAX / USDC (most liquid)
- sAVAX / WAVAX
- BTC.b / WAVAX

Contract addresses verified on-chain via getSilos() and asset() calls.
"""

import logging
from dataclasses import dataclass
from decimal import Decimal

from web3 import Web3

logger = logging.getLogger(__name__)

# =============================================================================
# Constants
# =============================================================================

MAX_UINT256 = 2**256 - 1

# Silo V2 function selectors (4-byte keccak prefixes)
SILO_V2_FUNCTION_SELECTORS = {
    # deposit(uint256 _assets, address _receiver, uint8 _collateralType)
    "deposit": "0xb7ec8d4b",
    # withdraw(uint256 _assets, address _receiver, address _owner, uint8 _collateralType)
    "withdraw": "0xb8337c2a",
    # borrow(uint256 _assets, address _receiver, address _borrower)
    "borrow": "0xd5164184",
    # repay(uint256 _assets, address _borrower)
    "repay": "0xacb70815",
    # redeem(uint256 _shares, address _receiver, address _owner, uint8 _collateralType)
    "redeem": "0xda537660",
    # repayShares(uint256 _shares, address _borrower)
    "repay_shares": "0xe36754eb",
    # maxRepay(address _borrower)
    "max_repay": "0x5f301149",
    # asset()
    "asset": "0x38d52e0f",
    # getSilos() on SiloConfig
    "get_silos": "0xaecc90cb",
}

# CollateralType enum values
COLLATERAL_TYPE_COLLATERAL = 0  # Borrowable deposits, earns interest
COLLATERAL_TYPE_PROTECTED = 1  # Non-borrowable, guaranteed withdrawal


@dataclass
class SiloV2MarketInfo:
    """Information about a Silo V2 market pair."""

    market_name: str
    silo_config: str
    silo0_address: str  # Vault for asset0
    silo1_address: str  # Vault for asset1
    asset0_symbol: str
    asset0_address: str
    asset1_symbol: str
    asset1_address: str


# Silo V2 market registry on Avalanche
# Addresses verified on-chain via getSilos() and asset() calls
SILO_V2_MARKETS: dict[str, SiloV2MarketInfo] = {
    "WAVAX/USDC": SiloV2MarketInfo(
        market_name="WAVAX/USDC",
        silo_config="0xF806aF0CC54197E7850a33f8101916752fE72f55",
        silo0_address="0xDa4b05e351696296060e6a1245C55e32DF8bFC84",
        silo1_address="0xfA5f7d5BcD70dC2F031eE906fc692a9e19584CB0",
        asset0_symbol="WAVAX",
        asset0_address="0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        asset1_symbol="USDC",
        asset1_address="0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
    ),
    "sAVAX/WAVAX": SiloV2MarketInfo(
        market_name="sAVAX/WAVAX",
        silo_config="0x1EbEca6f9A57d0B0E777710790de3F70dbeCCE26",
        silo0_address="0x4F33946808aa9cE48aeE24335e623f8C299bc630",
        silo1_address="0x506D6D820E5835B9b1d28C58019d0095bA0918fD",
        asset0_symbol="sAVAX",
        asset0_address="0x2b2C81e08f1Af8835a78Bb2A90AE924ACE0eA4bE",
        asset1_symbol="WAVAX",
        asset1_address="0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
    ),
    # BTC.b/WAVAX: silo addresses not yet verified on-chain, excluded until confirmed
    # silo_config="0x8b84c6C770Dd9856B5830C5A2DB1893d08f9BF0f"
}

# Reverse lookup: token symbol -> list of (market_name, silo_address, asset_index)
_TOKEN_TO_SILO_MAP: dict[str, list[tuple[str, str, int]]] = {}


def _build_token_map() -> None:
    """Build reverse lookup from token symbols to silo addresses."""
    for market_name, market in SILO_V2_MARKETS.items():
        for symbol, silo_addr, idx in [
            (market.asset0_symbol, market.silo0_address, 0),
            (market.asset1_symbol, market.silo1_address, 1),
        ]:
            key = symbol.upper()
            if key not in _TOKEN_TO_SILO_MAP:
                _TOKEN_TO_SILO_MAP[key] = []
            _TOKEN_TO_SILO_MAP[key].append((market_name, silo_addr, idx))


_build_token_map()


@dataclass
class SiloV2Position:
    """A position in a Silo V2 market."""

    market_name: str
    silo_address: str
    asset_symbol: str
    asset_address: str
    amount: Decimal
    is_collateral: bool = True


@dataclass
class TransactionResult:
    """Result of building a Silo V2 transaction."""

    success: bool
    tx_data: dict | None = None
    gas_estimate: int = 0
    description: str = ""
    error: str | None = None


@dataclass
class SiloV2Config:
    """Configuration for Silo V2 adapter."""

    chain: str = "avalanche"
    wallet_address: str = ""


class SiloV2Adapter:
    """Adapter for Silo V2 lending protocol on Avalanche.

    Provides supply, withdraw, borrow, and repay operations for Silo V2
    isolated lending markets.
    """

    def __init__(self, config: SiloV2Config) -> None:
        self.chain = config.chain
        self.wallet_address = Web3.to_checksum_address(config.wallet_address)

    def find_market(
        self,
        asset0_symbol: str | None = None,
        asset1_symbol: str | None = None,
    ) -> SiloV2MarketInfo | None:
        """Find a market by asset pair symbols.

        Args:
            asset0_symbol: First asset symbol (collateral side)
            asset1_symbol: Second asset symbol (borrow side)

        Returns:
            Market info or None if not found
        """
        if asset0_symbol and asset1_symbol:
            a0 = asset0_symbol.upper()
            a1 = asset1_symbol.upper()
            # Try both orderings
            for market in SILO_V2_MARKETS.values():
                m0 = market.asset0_symbol.upper()
                m1 = market.asset1_symbol.upper()
                if (m0 == a0 and m1 == a1) or (m0 == a1 and m1 == a0):
                    return market
        return None

    def find_silo_for_asset(
        self,
        asset_symbol: str,
        market_name: str | None = None,
    ) -> tuple[SiloV2MarketInfo, str, int] | None:
        """Find the silo address for a given asset.

        Args:
            asset_symbol: Token symbol (e.g., "USDC", "WAVAX")
            market_name: Optional market name to narrow lookup

        Returns:
            Tuple of (market_info, silo_address, asset_index) or None
        """
        key = asset_symbol.upper()
        entries = _TOKEN_TO_SILO_MAP.get(key, [])

        if not entries:
            return None

        if market_name:
            for mname, silo_addr, idx in entries:
                if mname == market_name:
                    return SILO_V2_MARKETS[mname], silo_addr, idx
            return None

        # Default to first entry (most liquid market listed first)
        mname, silo_addr, idx = entries[0]
        return SILO_V2_MARKETS[mname], silo_addr, idx

    def _encode_deposit(self, amount_wei: int, receiver: str, collateral_type: int = 0) -> str:
        """Encode deposit(uint256,address,uint8) calldata."""
        selector = SILO_V2_FUNCTION_SELECTORS["deposit"]
        amount_hex = f"{amount_wei:064x}"
        receiver_hex = f"{int(receiver, 16):064x}"
        type_hex = f"{collateral_type:064x}"
        return f"{selector}{amount_hex}{receiver_hex}{type_hex}"

    def _encode_withdraw(self, amount_wei: int, receiver: str, owner: str, collateral_type: int = 0) -> str:
        """Encode withdraw(uint256,address,address,uint8) calldata."""
        selector = SILO_V2_FUNCTION_SELECTORS["withdraw"]
        amount_hex = f"{amount_wei:064x}"
        receiver_hex = f"{int(receiver, 16):064x}"
        owner_hex = f"{int(owner, 16):064x}"
        type_hex = f"{collateral_type:064x}"
        return f"{selector}{amount_hex}{receiver_hex}{owner_hex}{type_hex}"

    def _encode_redeem(self, shares: int, receiver: str, owner: str, collateral_type: int = 0) -> str:
        """Encode redeem(uint256,address,address,uint8) calldata for full withdrawal."""
        selector = SILO_V2_FUNCTION_SELECTORS["redeem"]
        shares_hex = f"{shares:064x}"
        receiver_hex = f"{int(receiver, 16):064x}"
        owner_hex = f"{int(owner, 16):064x}"
        type_hex = f"{collateral_type:064x}"
        return f"{selector}{shares_hex}{receiver_hex}{owner_hex}{type_hex}"

    def _encode_borrow(self, amount_wei: int, receiver: str, borrower: str) -> str:
        """Encode borrow(uint256,address,address) calldata."""
        selector = SILO_V2_FUNCTION_SELECTORS["borrow"]
        amount_hex = f"{amount_wei:064x}"
        receiver_hex = f"{int(receiver, 16):064x}"
        borrower_hex = f"{int(borrower, 16):064x}"
        return f"{selector}{amount_hex}{receiver_hex}{borrower_hex}"

    def _encode_repay(self, amount_wei: int, borrower: str) -> str:
        """Encode repay(uint256,address) calldata."""
        selector = SILO_V2_FUNCTION_SELECTORS["repay"]
        amount_hex = f"{amount_wei:064x}"
        borrower_hex = f"{int(borrower, 16):064x}"
        return f"{selector}{amount_hex}{borrower_hex}"

    def _encode_repay_shares(self, shares: int, borrower: str) -> str:
        """Encode repayShares(uint256,address) calldata for full repay."""
        selector = SILO_V2_FUNCTION_SELECTORS["repay_shares"]
        shares_hex = f"{shares:064x}"
        borrower_hex = f"{int(borrower, 16):064x}"
        return f"{selector}{shares_hex}{borrower_hex}"

    def supply(
        self,
        asset: str,
        amount: Decimal,
        market_name: str | None = None,
        collateral_type: int = COLLATERAL_TYPE_COLLATERAL,
    ) -> TransactionResult:
        """Build a deposit transaction for Silo V2.

        Args:
            asset: Token symbol to deposit (e.g., "USDC")
            amount: Amount in human-readable units
            market_name: Optional market name (e.g., "WAVAX/USDC")
            collateral_type: 0=Collateral (borrowable), 1=Protected

        Returns:
            TransactionResult with encoded calldata
        """
        result = self.find_silo_for_asset(asset, market_name)
        if not result:
            return TransactionResult(
                success=False,
                error=f"No Silo V2 market found for asset: {asset}",
            )

        market, silo_address, asset_idx = result

        # Get decimals from token resolver
        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()
        decimals = resolver.get_decimals(self.chain, asset)
        amount_wei = int(amount * Decimal(10**decimals))

        calldata = self._encode_deposit(amount_wei, self.wallet_address, collateral_type)

        silo_checksum = Web3.to_checksum_address(silo_address)
        logger.info(
            f"Silo V2 deposit: {amount} {asset} -> silo {silo_checksum} "
            f"(market {market.market_name}, collateral_type={collateral_type})"
        )

        return TransactionResult(
            success=True,
            tx_data={
                "to": silo_checksum,
                "data": calldata,
                "value": 0,
            },
            gas_estimate=250_000,
            description=f"Deposit {amount} {asset} to Silo V2 ({market.market_name})",
        )

    def withdraw(
        self,
        asset: str,
        amount: Decimal,
        market_name: str | None = None,
        withdraw_all: bool = False,
        collateral_type: int = COLLATERAL_TYPE_COLLATERAL,
    ) -> TransactionResult:
        """Build a withdraw transaction for Silo V2.

        Args:
            asset: Token symbol to withdraw
            amount: Amount in human-readable units (ignored if withdraw_all)
            market_name: Optional market name
            withdraw_all: If True, withdraw all available balance
            collateral_type: 0=Collateral, 1=Protected

        Returns:
            TransactionResult with encoded calldata
        """
        result = self.find_silo_for_asset(asset, market_name)
        if not result:
            return TransactionResult(
                success=False,
                error=f"No Silo V2 market found for asset: {asset}",
            )

        market, silo_address, _asset_idx = result

        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()
        decimals = resolver.get_decimals(self.chain, asset)

        if withdraw_all:
            # MAX_UINT256 causes NotEnoughLiquidity on Silo V2.
            # Caller must provide the exact deposited amount for withdraw_all.
            if amount <= 0:
                return TransactionResult(
                    success=False,
                    error="Silo V2 withdraw_all requires an explicit amount (query deposit balance first)",
                )
            amount_wei = int(amount * Decimal(10**decimals))
            calldata = self._encode_withdraw(
                amount_wei,
                self.wallet_address,
                self.wallet_address,
                collateral_type,
            )
            amount_display = "all"
        else:
            amount_wei = int(amount * Decimal(10**decimals))
            calldata = self._encode_withdraw(
                amount_wei,
                self.wallet_address,
                self.wallet_address,
                collateral_type,
            )
            amount_display = str(amount)

        silo_checksum = Web3.to_checksum_address(silo_address)
        logger.info(f"Silo V2 withdraw: {amount_display} {asset} from silo {silo_checksum}")

        return TransactionResult(
            success=True,
            tx_data={
                "to": silo_checksum,
                "data": calldata,
                "value": 0,
            },
            gas_estimate=250_000,
            description=f"Withdraw {amount_display} {asset} from Silo V2 ({market.market_name})",
        )

    def borrow(
        self,
        collateral_asset: str,
        borrow_asset: str,
        borrow_amount: Decimal,
    ) -> TransactionResult:
        """Build a borrow transaction for Silo V2.

        The caller must deposit collateral separately before borrowing.
        This method only builds the borrow() call on the borrow silo.

        Args:
            collateral_asset: Collateral token symbol (identifies the market pair)
            borrow_asset: Token to borrow
            borrow_amount: Amount to borrow in human-readable units

        Returns:
            TransactionResult with encoded calldata
        """
        market = self.find_market(collateral_asset, borrow_asset)
        if not market:
            return TransactionResult(
                success=False,
                error=f"No Silo V2 market found for pair: {collateral_asset}/{borrow_asset}",
            )

        # Determine which silo to borrow from
        borrow_upper = borrow_asset.upper()
        if market.asset0_symbol.upper() == borrow_upper:
            borrow_silo = market.silo0_address
        elif market.asset1_symbol.upper() == borrow_upper:
            borrow_silo = market.silo1_address
        else:
            return TransactionResult(
                success=False,
                error=f"Borrow asset {borrow_asset} not found in market {market.market_name}",
            )

        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()
        decimals = resolver.get_decimals(self.chain, borrow_asset)
        borrow_amount_wei = int(borrow_amount * Decimal(10**decimals))

        calldata = self._encode_borrow(borrow_amount_wei, self.wallet_address, self.wallet_address)
        borrow_silo_checksum = Web3.to_checksum_address(borrow_silo)

        logger.info(
            f"Silo V2 borrow: {borrow_amount} {borrow_asset} from silo {borrow_silo_checksum} "
            f"(market {market.market_name})"
        )

        return TransactionResult(
            success=True,
            tx_data={
                "to": borrow_silo_checksum,
                "data": calldata,
                "value": 0,
            },
            gas_estimate=350_000,
            description=f"Borrow {borrow_amount} {borrow_asset} from Silo V2 ({market.market_name})",
        )

    def repay(
        self,
        asset: str,
        amount: Decimal,
        market_name: str | None = None,
        repay_all: bool = False,
    ) -> TransactionResult:
        """Build a repay transaction for Silo V2.

        Args:
            asset: Token to repay
            amount: Amount to repay (ignored if repay_all)
            market_name: Optional market name
            repay_all: If True, repay using MAX_UINT256

        Returns:
            TransactionResult with encoded calldata
        """
        result = self.find_silo_for_asset(asset, market_name)
        if not result:
            return TransactionResult(
                success=False,
                error=f"No Silo V2 market found for asset: {asset}",
            )

        market, silo_address, _asset_idx = result

        if repay_all:
            # Silo V2 does NOT use MAX_UINT256 for full repay (unlike Compound forks).
            # Full repay requires maxRepayShares() which needs RPC access.
            # Caller must provide the exact debt amount for repay_all.
            if amount <= 0:
                return TransactionResult(
                    success=False,
                    error="Silo V2 repay_all requires an explicit amount (query debt balance first)",
                )
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            decimals = resolver.get_decimals(self.chain, asset)
            amount_wei = int(amount * Decimal(10**decimals))
            calldata = self._encode_repay(amount_wei, self.wallet_address)
            amount_display = "all"
        else:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            decimals = resolver.get_decimals(self.chain, asset)
            amount_wei = int(amount * Decimal(10**decimals))
            calldata = self._encode_repay(amount_wei, self.wallet_address)
            amount_display = str(amount)

        silo_checksum = Web3.to_checksum_address(silo_address)
        logger.info(f"Silo V2 repay: {amount_display} {asset} to silo {silo_checksum}")

        return TransactionResult(
            success=True,
            tx_data={
                "to": silo_checksum,
                "data": calldata,
                "value": 0,
            },
            gas_estimate=250_000,
            description=f"Repay {amount_display} {asset} to Silo V2 ({market.market_name})",
        )

    def redeem_shares(
        self,
        shares: int,
        market_name: str | None = None,
        silo_address: str | None = None,
        collateral_type: int = COLLATERAL_TYPE_COLLATERAL,
    ) -> TransactionResult:
        """Build a redeem transaction using exact share amount.

        Used for withdraw_all to avoid ERC-4626 rounding issues.

        Args:
            shares: Exact share amount to redeem
            market_name: Market name for metadata
            silo_address: The silo vault address
            collateral_type: 0=Collateral, 1=Protected

        Returns:
            TransactionResult with encoded calldata
        """
        if not silo_address:
            return TransactionResult(
                success=False,
                error="silo_address required for redeem_shares",
            )

        calldata = self._encode_redeem(shares, self.wallet_address, self.wallet_address, collateral_type)
        silo_checksum = Web3.to_checksum_address(silo_address)

        logger.info(f"Silo V2 redeem: {shares} shares from silo {silo_checksum}")

        return TransactionResult(
            success=True,
            tx_data={
                "to": silo_checksum,
                "data": calldata,
                "value": 0,
            },
            gas_estimate=250_000,
            description=f"Redeem {shares} shares from Silo V2 ({market_name or 'unknown'})",
        )

    def get_market_info(self, asset_symbol: str, market_name: str | None = None) -> SiloV2MarketInfo | None:
        """Get market info for a given asset.

        Args:
            asset_symbol: Token symbol
            market_name: Optional market name to narrow lookup

        Returns:
            SiloV2MarketInfo or None
        """
        result = self.find_silo_for_asset(asset_symbol, market_name)
        if result:
            return result[0]
        return None

    def get_silo_address(self, asset_symbol: str, market_name: str | None = None) -> str | None:
        """Get the silo vault address for a given asset.

        Args:
            asset_symbol: Token symbol
            market_name: Optional market name

        Returns:
            Checksummed silo address or None
        """
        result = self.find_silo_for_asset(asset_symbol, market_name)
        if result:
            return Web3.to_checksum_address(result[1])
        return None
