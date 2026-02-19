"""Kraken CEX SDK.

Core SDK for interacting with the Kraken exchange API.
Provides methods for:
- Account balance queries
- Spot trading (market orders)
- Deposits and withdrawals
- Status polling

This is ported from the Enterprise codebase and adapted for stack-v2
patterns (Pydantic v2, structlog, async-friendly design).

Example:
    from almanak.framework.connectors.kraken import KrakenSDK, KrakenCredentials

    credentials = KrakenCredentials.from_env()
    sdk = KrakenSDK(credentials)

    # Get balances
    balances = sdk.get_balances(["ETH", "USDC"])

    # Execute swap
    userref = sdk.generate_userref()
    txid = sdk.swap(
        asset_in="USDC",
        asset_out="ETH",
        amount_in=1000_000000,  # 1000 USDC (6 decimals)
        decimals_in=6,
        userref=userref,
    )

    # Check status
    status = sdk.get_swap_status(txid, userref)
"""

import uuid
from decimal import Decimal
from functools import lru_cache
from typing import Any

import structlog

from .exceptions import (
    KrakenAPIError,
    KrakenAuthenticationError,
    KrakenInsufficientFundsError,
    KrakenMinimumOrderError,
    KrakenOrderNotFoundError,
    KrakenUnknownAssetError,
    KrakenUnknownPairError,
    KrakenWithdrawalAddressNotWhitelistedError,
)
from .models import (
    KrakenBalance,
    KrakenConfig,
    KrakenCredentials,
    KrakenMarketInfo,
)
from .token_resolver import KrakenChainMapper, KrakenTokenResolver

logger = structlog.get_logger(__name__)


class KrakenSDK:
    """SDK for Kraken exchange operations.

    Wraps the python-kraken-sdk library with additional functionality:
    - Token resolution (stack-v2 symbols -> Kraken symbols)
    - Chain mapping for deposits/withdrawals
    - Amount validation and precision handling
    - Status polling for async operations

    Thread Safety:
        This class is NOT thread-safe. Use separate instances per thread
        or implement proper synchronization.

    Example:
        sdk = KrakenSDK(KrakenCredentials.from_env())

        # Get available balance
        balances = sdk.get_balances(["USDC", "ETH"])
        print(f"USDC available: {balances['USDC'].available}")

        # Execute a swap
        userref = sdk.generate_userref()
        txid = sdk.swap(
            asset_in="USDC",
            asset_out="ETH",
            amount_in=1000_000000,  # 1000 USDC
            decimals_in=6,
            userref=userref,
        )
    """

    def __init__(
        self,
        credentials: KrakenCredentials | None = None,
        config: KrakenConfig | None = None,
        token_resolver: KrakenTokenResolver | None = None,
        chain_mapper: KrakenChainMapper | None = None,
    ) -> None:
        """Initialize Kraken SDK.

        Args:
            credentials: API credentials. If not provided, loads from env.
            config: SDK configuration. Uses defaults if not provided.
            token_resolver: Custom token resolver. Uses default if not provided.
            chain_mapper: Custom chain mapper. Uses default if not provided.
        """
        # Load credentials
        if credentials is None:
            credentials = KrakenCredentials.from_env()

        self.config = config or KrakenConfig()
        self.token_resolver = token_resolver or KrakenTokenResolver()
        self.chain_mapper = chain_mapper or KrakenChainMapper()

        # Initialize Kraken API clients
        # Import here to avoid requiring kraken-sdk when not needed
        try:
            from kraken.spot import Funding, Market, Trade, User
        except ImportError as e:
            raise ImportError(
                "python-kraken-sdk is required for Kraken integration. Install with: pip install python-kraken-sdk"
            ) from e

        api_key = credentials.api_key.get_secret_value()
        api_secret = credentials.api_secret.get_secret_value()

        self.user = User(key=api_key, secret=api_secret)
        self.market = Market(key=api_key, secret=api_secret)
        self.trade = Trade(key=api_key, secret=api_secret)
        self.funding = Funding(key=api_key, secret=api_secret)

        logger.info("KrakenSDK initialized")

    # =========================================================================
    # Balance Operations
    # =========================================================================

    def get_all_balances(self) -> dict[str, KrakenBalance]:
        """Get all account balances.

        Returns:
            Dict mapping Kraken asset symbol to KrakenBalance
        """
        try:
            raw_balances = self.user.get_balances()
        except Exception as e:
            self._handle_api_error(e, "get_balances")
            raise  # _handle_api_error may not always raise

        result = {}
        for asset, data in raw_balances.items():
            balance = KrakenBalance.from_kraken_response(asset, data)
            # Only include non-zero balances
            if balance.total > 0:
                result[asset] = balance

        return result

    def get_balances(
        self,
        assets: list[str],
        chain: str = "ethereum",
    ) -> dict[str, KrakenBalance]:
        """Get balance information for specified assets.

        Args:
            assets: List of token symbols (e.g., ["USDC", "ETH"])
            chain: Chain for token resolution

        Returns:
            Dict mapping asset symbol to KrakenBalance
        """
        # Convert to Kraken symbols
        kraken_symbols = {}
        for asset in assets:
            kraken_sym = self.token_resolver.to_kraken_symbol(chain, asset)
            kraken_symbols[asset] = kraken_sym

        try:
            raw_balances = self.user.get_balances()
        except Exception as e:
            self._handle_api_error(e, "get_balances")
            raise  # _handle_api_error may not always raise

        result = {}
        for asset, kraken_sym in kraken_symbols.items():
            # Find matching balance (Kraken may return with prefix like 'XXBT')
            balance_data = None
            for key, data in raw_balances.items():
                if kraken_sym in key or key in kraken_sym:
                    balance_data = data
                    break

            if balance_data:
                result[asset] = KrakenBalance.from_kraken_response(asset, balance_data)
            else:
                # Asset not found, return zero balance
                result[asset] = KrakenBalance(
                    asset=asset,
                    total=Decimal("0"),
                    available=Decimal("0"),
                    held=Decimal("0"),
                )

        return result

    def get_balance(
        self,
        asset: str,
        chain: str = "ethereum",
    ) -> KrakenBalance:
        """Get balance for a single asset.

        Args:
            asset: Token symbol
            chain: Chain for token resolution

        Returns:
            KrakenBalance for the asset
        """
        return self.get_balances([asset], chain)[asset]

    # =========================================================================
    # Market Data
    # =========================================================================

    @lru_cache(maxsize=128)  # noqa: B019 - intentional caching on long-lived SDK instance
    def get_market_info(
        self,
        base_asset: str,
        quote_asset: str,
        chain: str = "ethereum",
    ) -> KrakenMarketInfo:
        """Get market information for a trading pair.

        Args:
            base_asset: Base token symbol
            quote_asset: Quote token symbol
            chain: Chain for token resolution

        Returns:
            KrakenMarketInfo with pair details

        Raises:
            KrakenUnknownPairError: If pair doesn't exist
        """
        pair = self.token_resolver.get_trading_pair(base_asset, quote_asset, chain)

        try:
            data = self.market.get_asset_pairs(pair)
        except Exception as e:
            self._handle_api_error(e, "get_market_info")

        if not data:
            raise KrakenUnknownPairError(pair)

        # Extract first (and should be only) result
        pair_name, pair_info = next(iter(data.items()))
        return KrakenMarketInfo.from_kraken_response(pair_name, pair_info)

    def market_exists(
        self,
        base_asset: str,
        quote_asset: str,
        chain: str = "ethereum",
    ) -> bool:
        """Check if a trading pair exists.

        Also checks the inverse pair (quote/base).

        Args:
            base_asset: Base token symbol
            quote_asset: Quote token symbol
            chain: Chain for token resolution

        Returns:
            True if pair exists (in either direction)
        """
        try:
            self.get_market_info(base_asset, quote_asset, chain)
            return True
        except KrakenUnknownPairError:
            pass

        # Try inverse
        try:
            self.get_market_info(quote_asset, base_asset, chain)
            return True
        except KrakenUnknownPairError:
            return False

    def is_market_inverted(
        self,
        asset_in: str,
        asset_out: str,
        chain: str = "ethereum",
    ) -> bool:
        """Check if market pair is inverted from asset_in/asset_out order.

        Kraken markets have a specific base/quote ordering. This checks
        if the natural order (asset_in first) matches Kraken's order.

        Args:
            asset_in: Input asset symbol
            asset_out: Output asset symbol
            chain: Chain for token resolution

        Returns:
            True if Kraken's base/quote is opposite to asset_in/asset_out
        """
        try:
            self.get_market_info(asset_in, asset_out, chain)
            return False  # Direct order works
        except KrakenUnknownPairError:
            pass

        try:
            self.get_market_info(asset_out, asset_in, chain)
            return True  # Inverted order works
        except KrakenUnknownPairError as e:
            raise KrakenUnknownPairError(f"{asset_in}/{asset_out}") from e

    # =========================================================================
    # Swap Operations
    # =========================================================================

    @staticmethod
    def generate_userref() -> int:
        """Generate a unique userref for order idempotency.

        The userref is a 32-bit signed integer that identifies
        an order for idempotency. It must be persisted before
        submitting the order.

        Returns:
            Unique userref (int32)
        """
        unique_id = uuid.uuid4()
        # Convert UUID to 32-bit signed integer
        userref = int(unique_id.int % (2**31 - 1)) - (2**31 - 1)
        return userref

    def validate_swap_amount(
        self,
        asset_in: str,
        asset_out: str,
        amount_in: int,
        decimals_in: int,
        chain: str = "ethereum",
    ) -> int:
        """Validate and floor swap amount to Kraken precision.

        Args:
            asset_in: Input asset symbol
            asset_out: Output asset symbol
            amount_in: Amount in wei units
            decimals_in: Decimals of input asset
            chain: Chain for token resolution

        Returns:
            Floored amount that meets Kraken requirements

        Raises:
            KrakenMinimumOrderError: If amount is below minimum
            KrakenInsufficientFundsError: If balance is insufficient
        """
        # Determine market order and get info
        inverted = self.is_market_inverted(asset_in, asset_out, chain)
        if inverted:
            base, quote = asset_out, asset_in
        else:
            base, quote = asset_in, asset_out

        market_info = self.get_market_info(base, quote, chain)

        # Get Kraken precision for rounding
        lot_decimals = market_info.lot_decimals
        rounding_factor = Decimal(10) ** (decimals_in - lot_decimals)
        rounded_amount = int((Decimal(amount_in) // rounding_factor) * rounding_factor)

        # Check minimum order size
        if asset_in == base:
            min_amount = market_info.get_min_order_base(decimals_in)
            if rounded_amount < min_amount:
                raise KrakenMinimumOrderError(
                    f"Amount {rounded_amount} below minimum {min_amount}",
                    pair=f"{base}/{quote}",
                    amount=str(rounded_amount),
                    minimum=str(min_amount),
                )
        else:
            min_cost = market_info.get_min_cost_quote(decimals_in)
            if rounded_amount < min_cost:
                raise KrakenMinimumOrderError(
                    f"Amount {rounded_amount} below minimum cost {min_cost}",
                    pair=f"{base}/{quote}",
                    amount=str(rounded_amount),
                    minimum=str(min_cost),
                )

        # Check balance
        balance = self.get_balance(asset_in, chain)
        available = int(balance.available * Decimal(10) ** decimals_in)
        if rounded_amount > available:
            raise KrakenInsufficientFundsError(
                f"Insufficient {asset_in} balance",
                asset=asset_in,
                requested=str(rounded_amount),
                available=str(available),
            )

        return rounded_amount

    def swap(
        self,
        asset_in: str,
        asset_out: str,
        amount_in: int,
        decimals_in: int,
        userref: int,
        chain: str = "ethereum",
        deadline: int | None = None,
    ) -> str:
        """Execute a market swap on Kraken.

        Args:
            asset_in: Input asset symbol (e.g., "USDC")
            asset_out: Output asset symbol (e.g., "ETH")
            amount_in: Amount in wei units
            decimals_in: Decimals of input asset
            userref: Unique order reference for idempotency
            chain: Chain for token resolution
            deadline: Optional deadline timestamp

        Returns:
            Order transaction ID (txid)

        Raises:
            KrakenMinimumOrderError: If amount is below minimum
            KrakenInsufficientFundsError: If balance is insufficient
            KrakenAPIError: If API call fails
        """
        # Validate and floor amount
        floored_amount = self.validate_swap_amount(asset_in, asset_out, amount_in, decimals_in, chain)

        # Determine market direction
        inverted = self.is_market_inverted(asset_in, asset_out, chain)
        if inverted:
            base, quote = asset_out, asset_in
            side = "buy"  # Buying base with quote
        else:
            base, quote = asset_in, asset_out
            side = "sell"  # Selling base for quote

        pair = self.token_resolver.get_trading_pair(base, quote, chain)

        # Convert to Kraken units (human readable)
        kraken_amount = Decimal(floored_amount) / Decimal(10) ** decimals_in

        # Build order flags
        oflags = ["fciq"]  # Fee in quote currency

        # If buying with quote currency (viqc = volume in quote currency)
        if side == "buy" and asset_in == quote:
            oflags.append("viqc")
            # Adjust for fee when specifying volume in quote
            fee_pct = self.get_market_info(base, quote, chain).taker_fee / 100
            kraken_amount = kraken_amount / (Decimal("1") + fee_pct)

        logger.info(
            "Executing swap",
            pair=pair,
            side=side,
            amount=str(kraken_amount),
            userref=userref,
        )

        try:
            response = self.trade.create_order(
                ordertype="market",
                side=side,
                volume=float(kraken_amount),
                pair=pair,
                oflags=oflags,
                userref=userref,
                deadline=deadline,
            )
        except Exception as e:
            self._handle_api_error(e, "swap")

        txid = response["txid"][0]
        logger.info("Swap order placed", txid=txid, userref=userref)
        return txid

    def get_swap_status(self, txid: str, userref: int) -> str:
        """Get status of a swap order.

        Args:
            txid: Order transaction ID
            userref: Order reference

        Returns:
            Status string: "pending", "success", "failed",
            "partial", "cancelled", "unknown"
        """
        try:
            orders = self.user.get_orders_info(txid=[txid], userref=userref)
        except Exception as e:
            self._handle_api_error(e, "get_swap_status")

        if not orders or txid not in orders:
            raise KrakenOrderNotFoundError(txid, userref)

        order = orders[txid]
        status = order["status"]
        oflags = order.get("oflags", "")
        vol_exec = Decimal(str(order.get("vol_exec", "0")))
        vol = Decimal(str(order.get("vol", "0")))
        cost = Decimal(str(order.get("cost", "0")))

        # Volume-in-quote mode
        if "viqc" in oflags:
            if status in ("pending", "open"):
                return "pending"
            elif status == "closed":
                return "success" if cost > 0 else "failed"
            elif status in ("canceled", "expired"):
                return "cancelled" if cost == 0 else "success"
            return "unknown"

        # Volume-in-base mode
        if status in ("pending", "open"):
            return "pending"
        elif status == "closed":
            if vol_exec == 0:
                return "failed"
            elif vol_exec == vol:
                return "success"
            elif vol_exec < vol:
                return "partial"
            return "unknown"
        elif status in ("canceled", "expired"):
            if vol_exec == 0:
                return "cancelled"
            elif vol_exec > 0:
                return "partial"
            return "unknown"

        return "unknown"

    def get_swap_result(
        self,
        txid: str,
        userref: int,
        asset_in: str,
        asset_out: str,
        decimals_in: int,
        decimals_out: int,
        chain: str = "ethereum",
    ) -> dict[str, Any]:
        """Get detailed result of a completed swap.

        Args:
            txid: Order transaction ID
            userref: Order reference
            asset_in: Input asset symbol
            asset_out: Output asset symbol
            decimals_in: Input asset decimals
            decimals_out: Output asset decimals
            chain: Chain for token resolution

        Returns:
            Dict with: amount_in, amount_out, fee, average_price, timestamp
        """
        try:
            orders = self.user.get_orders_info(txid=[txid], userref=userref)
        except Exception as e:
            self._handle_api_error(e, "get_swap_result")

        if not orders or txid not in orders:
            raise KrakenOrderNotFoundError(txid, userref)

        order = orders[txid]

        # Determine base/quote ordering
        inverted = self.is_market_inverted(asset_in, asset_out, chain)
        if inverted:
            base, quote = asset_out, asset_in
            dec_base, dec_quote = decimals_out, decimals_in
        else:
            base, quote = asset_in, asset_out
            dec_base, dec_quote = decimals_in, decimals_out

        vol_exec = Decimal(str(order.get("vol_exec", "0")))
        cost = Decimal(str(order.get("cost", "0")))
        fee = Decimal(str(order.get("fee", "0")))
        avg_price = Decimal(str(order.get("price", "0")))
        close_time = int(order.get("closetm", 0))

        # Fee in quote by default (fciq flag)
        fee_in_quote = "fcib" not in order.get("oflags", "")

        # Calculate amounts in wei
        amount_base = int(vol_exec * Decimal(10) ** dec_base)
        amount_quote = int(cost * Decimal(10) ** dec_quote)
        fee_wei = int(fee * Decimal(10) ** (dec_quote if fee_in_quote else dec_base))

        # Adjust for fee direction
        side = order["descr"]["type"].lower()
        if side == "buy":
            # Buying base: amount_quote includes fee
            if fee_in_quote:
                amount_quote += fee_wei
        else:
            # Selling base: amount_quote minus fee
            if fee_in_quote:
                amount_quote -= fee_wei

        # Map to asset_in/asset_out
        if asset_in == base:
            amount_in_wei = amount_base
            amount_out_wei = amount_quote
        else:
            amount_in_wei = amount_quote
            amount_out_wei = amount_base
            avg_price = Decimal("1") / avg_price if avg_price else Decimal("0")

        return {
            "txid": txid,
            "userref": userref,
            "amount_in": amount_in_wei,
            "amount_out": amount_out_wei,
            "fee": fee_wei,
            "fee_asset": quote if fee_in_quote else base,
            "average_price": avg_price,
            "timestamp": close_time,
        }

    # =========================================================================
    # Withdrawal Operations
    # =========================================================================

    def get_withdrawal_addresses(
        self,
        asset: str,
        chain: str,
    ) -> set[str]:
        """Get whitelisted withdrawal addresses.

        Args:
            asset: Asset symbol
            chain: Target chain

        Returns:
            Set of whitelisted addresses (checksummed)
        """
        kraken_symbol = self.token_resolver.to_kraken_symbol(chain, asset)
        method_name = self.chain_mapper.get_withdraw_method(chain)

        # Handle USDC.e -> USDC mapping
        if kraken_symbol == "USDC.e":
            kraken_symbol = "USDC"

        try:
            addresses = self.funding.withdraw_addresses(asset=kraken_symbol, method=method_name)
        except Exception as e:
            self._handle_api_error(e, "get_withdrawal_addresses")

        # Checksum addresses
        from web3 import Web3

        return {Web3.to_checksum_address(a["address"]) for a in addresses}

    def get_withdrawal_key(
        self,
        asset: str,
        chain: str,
        address: str,
    ) -> str:
        """Get Kraken withdrawal key for an address.

        Kraken requires a "key" (label) for withdrawals, not the address.

        Args:
            asset: Asset symbol
            chain: Target chain
            address: Destination address

        Returns:
            Kraken withdrawal key

        Raises:
            KrakenWithdrawalAddressNotWhitelistedError: If address not found
        """
        from web3 import Web3

        kraken_symbol = self.token_resolver.to_kraken_symbol(chain, asset)
        method_name = self.chain_mapper.get_withdraw_method(chain)
        address = Web3.to_checksum_address(address)

        if kraken_symbol == "USDC.e":
            kraken_symbol = "USDC"

        try:
            addresses = self.funding.withdraw_addresses(asset=kraken_symbol, method=method_name)
        except Exception as e:
            self._handle_api_error(e, "get_withdrawal_key")

        for entry in addresses:
            if Web3.to_checksum_address(entry["address"]) == address:
                return entry["key"]

        raise KrakenWithdrawalAddressNotWhitelistedError(address, asset, chain)

    def withdraw(
        self,
        asset: str,
        chain: str,
        amount: int,
        decimals: int,
        to_address: str,
    ) -> str:
        """Initiate a withdrawal from Kraken.

        Args:
            asset: Asset symbol
            chain: Target chain
            amount: Amount in wei units
            decimals: Asset decimals
            to_address: Destination address (must be whitelisted)

        Returns:
            Withdrawal reference ID (refid)

        Raises:
            KrakenWithdrawalAddressNotWhitelistedError: If address not whitelisted
            KrakenInsufficientFundsError: If balance insufficient
        """
        from web3 import Web3

        to_address = Web3.to_checksum_address(to_address)

        # Verify address is whitelisted
        if self.config.require_withdrawal_whitelist:
            whitelisted = self.get_withdrawal_addresses(asset, chain)
            if to_address not in whitelisted:
                raise KrakenWithdrawalAddressNotWhitelistedError(to_address, asset, chain)

        # Get withdrawal key
        withdrawal_key = self.get_withdrawal_key(asset, chain, to_address)

        # Convert to Kraken units
        kraken_symbol = self.token_resolver.to_kraken_symbol(chain, asset)
        if kraken_symbol == "USDC.e":
            kraken_symbol = "USDC"

        kraken_amount = Decimal(amount) / Decimal(10) ** decimals

        logger.info(
            "Initiating withdrawal",
            asset=asset,
            chain=chain,
            amount=str(kraken_amount),
            to_address=to_address,
        )

        try:
            response = self.funding.withdraw_funds(
                asset=kraken_symbol,
                key=withdrawal_key,
                amount=float(kraken_amount),
            )
        except Exception as e:
            self._handle_api_error(e, "withdraw")

        refid = response["refid"]
        logger.info("Withdrawal initiated", refid=refid)
        return refid

    def get_withdrawal_status(
        self,
        asset: str,
        chain: str,
        refid: str | None = None,
        tx_hash: str | None = None,
    ) -> str | None:
        """Get status of a withdrawal.

        Args:
            asset: Asset symbol
            chain: Target chain
            refid: Kraken reference ID (from withdraw())
            tx_hash: On-chain transaction hash (if available)

        Returns:
            Status: "pending", "success", "failed", or None if not found
        """
        if not refid and not tx_hash:
            raise ValueError("Must provide either refid or tx_hash")

        kraken_symbol = self.token_resolver.to_kraken_symbol(chain, asset)
        method_name = self.chain_mapper.get_withdraw_method(chain)

        if kraken_symbol == "USDC.e":
            kraken_symbol = "USDC"

        try:
            withdrawals = self.funding.get_recent_withdraw_status(asset=kraken_symbol, method=method_name)
        except Exception as e:
            self._handle_api_error(e, "get_withdrawal_status")

        for w in withdrawals:
            if (refid and w.get("refid") == refid) or (tx_hash and w.get("txid") == tx_hash):
                status = w.get("status", "")
                if status == "Success":
                    return "success"
                elif status == "Failure":
                    return "failed"
                elif status in ("Initial", "Pending", "Settled"):
                    return "pending"
                return "unknown"

        return None

    def get_withdrawal_tx_hash(
        self,
        asset: str,
        chain: str,
        refid: str,
    ) -> str | None:
        """Get on-chain transaction hash for a withdrawal.

        Args:
            asset: Asset symbol
            chain: Target chain
            refid: Kraken reference ID

        Returns:
            On-chain tx hash if available, None otherwise
        """
        kraken_symbol = self.token_resolver.to_kraken_symbol(chain, asset)
        method_name = self.chain_mapper.get_withdraw_method(chain)

        if kraken_symbol == "USDC.e":
            kraken_symbol = "USDC"

        try:
            withdrawals = self.funding.get_recent_withdraw_status(asset=kraken_symbol, method=method_name)
        except Exception as e:
            self._handle_api_error(e, "get_withdrawal_tx_hash")

        for w in withdrawals:
            if w.get("refid") == refid:
                return w.get("txid")

        return None

    # =========================================================================
    # Deposit Operations
    # =========================================================================

    def get_deposit_addresses(
        self,
        asset: str,
        chain: str,
    ) -> set[str]:
        """Get Kraken deposit addresses for an asset.

        Args:
            asset: Asset symbol
            chain: Source chain

        Returns:
            Set of deposit addresses (checksummed)
        """
        from web3 import Web3

        kraken_symbol = self.token_resolver.to_kraken_symbol(chain, asset)
        method_name = self.chain_mapper.get_deposit_method(chain, asset)

        if kraken_symbol == "USDC.e":
            kraken_symbol = "USDC"

        try:
            addresses = self.funding.get_deposit_address(asset=kraken_symbol, method=method_name)
        except Exception as e:
            self._handle_api_error(e, "get_deposit_addresses")

        return {Web3.to_checksum_address(a["address"]) for a in addresses}

    def get_deposit_status(
        self,
        tx_hash: str,
        asset: str | None = None,
        chain: str | None = None,
    ) -> str | None:
        """Get status of a deposit by transaction hash.

        Args:
            tx_hash: On-chain transaction hash
            asset: Optional asset for filtering
            chain: Optional chain for filtering

        Returns:
            Status: "pending", "success", "failed", or None if not found
        """
        kraken_symbol = None
        method_name = None

        if asset and chain:
            kraken_symbol = self.token_resolver.to_kraken_symbol(chain, asset)
            method_name = self.chain_mapper.get_deposit_method(chain, asset)
            if kraken_symbol == "USDC.e":
                kraken_symbol = "USDC"

        try:
            deposits = self.funding.get_recent_deposits_status(asset=kraken_symbol, method=method_name)
        except Exception as e:
            self._handle_api_error(e, "get_deposit_status")

        for d in deposits:
            if d.get("txid") == tx_hash:
                status = d.get("status", "")
                if status == "Success":
                    return "success"
                elif status == "Failure":
                    return "failed"
                elif status in ("Pending", "Settled"):
                    return "pending"
                return "unknown"

        return None

    # =========================================================================
    # Error Handling
    # =========================================================================

    def _handle_api_error(self, error: Exception, operation: str) -> None:
        """Convert Kraken errors to our exception types."""
        error_msg = str(error)

        # Check for common error patterns
        if "EAPI:Invalid key" in error_msg:
            raise KrakenAuthenticationError("Invalid API key or insufficient permissions") from error

        if "EGeneral:Invalid arguments" in error_msg:
            raise KrakenAPIError([error_msg]) from error

        if "EFunding:Unknown asset" in error_msg:
            raise KrakenUnknownAssetError(error_msg) from error

        if "EQuery:Unknown asset pair" in error_msg:
            raise KrakenUnknownPairError("Unknown trading pair") from error

        if "Unknown asset" in error_msg and "pair" in error_msg.lower():
            raise KrakenUnknownPairError("Unknown trading pair") from error

        if "EOrder:Insufficient funds" in error_msg:
            raise KrakenInsufficientFundsError(
                error_msg, asset="unknown", requested="unknown", available="unknown"
            ) from error

        # Generic API error
        logger.error(
            "Kraken API error",
            operation=operation,
            error=error_msg,
        )
        raise KrakenAPIError([error_msg]) from error


__all__ = [
    "KrakenSDK",
]
