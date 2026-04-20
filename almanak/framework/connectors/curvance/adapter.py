"""Curvance Adapter — high-level transaction builder.

The adapter produces ``TransactionResult`` objects (``to`` / ``value`` / ``data``
/ gas estimate) that the compiler wraps into ActionBundle entries. It does NOT
submit transactions — that's the executor's job.

Design mirrors ``MorphoBlueAdapter``:
    - Dataclass config with chain, wallet, optional gateway client
    - ``supply_collateral``, ``borrow``, ``repay``, ``withdraw_collateral`` methods
    - Each returns ``TransactionResult(success, tx_data={to, value, data}, gas_estimate, description)``

Curvance's per-market architecture means the target contract differs per action:
    - supply / withdraw  -> collateral cToken
    - borrow / repay     -> borrowable cToken

All target addresses are resolved via ``market_id`` (MarketManager address).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.data.tokens.exceptions import TokenResolutionError

from .constants import (
    CURVANCE_MARKETS,
    CURVANCE_PROTOCOL_CONTRACTS,
    SUPPORTED_CHAINS,
    CurvanceMarket,
    get_market,
)
from .sdk import (
    DEFAULT_GAS_ESTIMATES,
    encode_borrow,
    encode_deposit_as_collateral,
    encode_redeem_collateral,
    encode_repay,
    encode_withdraw_collateral,
)

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType
    from almanak.framework.gateway_client import GatewayClient


logger = logging.getLogger(__name__)


# =============================================================================
# Domain dataclasses
# =============================================================================


@dataclass
class CurvanceConfig:
    """Configuration for CurvanceAdapter.

    Attributes:
        chain: Target chain (currently only "monad").
        wallet_address: User wallet address (0x-prefixed EVM address).
        gateway_client: Optional gateway client for on-chain reads. Reserved for
            future use (pause checks, debt balance queries). Adapter builds
            calldata offline so the gateway is not required for transaction
            construction.
    """

    chain: str
    wallet_address: str
    gateway_client: GatewayClient | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        chain_lower = self.chain.lower()
        if chain_lower not in SUPPORTED_CHAINS:
            raise ValueError(
                f"Curvance is not supported on '{self.chain}'. Supported chains: {', '.join(SUPPORTED_CHAINS)}"
            )
        self.chain = chain_lower
        if not self.wallet_address.startswith("0x") or len(self.wallet_address) != 42:
            raise ValueError(f"Invalid wallet address: {self.wallet_address}. Must be 0x-prefixed 40 hex chars.")


@dataclass
class CurvanceMarketInfo:
    """Serializable snapshot of a Curvance market for logging / metadata."""

    name: str
    market_manager: str
    collateral_ctoken: str
    borrowable_ctoken: str
    collateral_symbol: str
    debt_symbol: str

    @classmethod
    def from_market(cls, market: CurvanceMarket) -> CurvanceMarketInfo:
        return cls(
            name=market.name,
            market_manager=market.market_manager,
            collateral_ctoken=market.collateral_ctoken,
            borrowable_ctoken=market.borrowable_ctoken,
            collateral_symbol=market.collateral_symbol,
            debt_symbol=market.debt_symbol,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "market_manager": self.market_manager,
            "collateral_ctoken": self.collateral_ctoken,
            "borrowable_ctoken": self.borrowable_ctoken,
            "collateral_symbol": self.collateral_symbol,
            "debt_symbol": self.debt_symbol,
        }


@dataclass
class TransactionResult:
    """Result of a transaction build operation.

    Mirrors MorphoBlueAdapter.TransactionResult so compiler code can consume
    both adapters with a single dispatch path.
    """

    success: bool
    tx_data: dict[str, Any] | None = None
    gas_estimate: int = 0
    description: str = ""
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "tx_data": self.tx_data,
            "gas_estimate": self.gas_estimate,
            "description": self.description,
            "error": self.error,
        }


# =============================================================================
# Adapter
# =============================================================================


class CurvanceAdapter:
    """Adapter for Curvance lending protocol (Monad).

    Produces EVM calldata for supply/borrow/repay/withdraw against Curvance
    isolated markets. The adapter is stateless between calls — each operation
    returns a standalone ``TransactionResult``.

    Example:
        >>> config = CurvanceConfig(chain="monad", wallet_address="0x...")
        >>> adapter = CurvanceAdapter(config)
        >>> market_id = "0xb3E9E0134354cc91b7FB9F9d6C3ab0dE7854BB49"  # WETH-USDC
        >>> result = adapter.supply_collateral(market_id=market_id, amount=Decimal("0.001"))
        >>> result.tx_data["to"]   # -> WETH cToken address
        >>> result.tx_data["data"] # -> depositAsCollateral calldata
    """

    def __init__(
        self,
        config: CurvanceConfig,
        token_resolver: TokenResolverType | None = None,
    ) -> None:
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address

        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        self._protocol_contracts = CURVANCE_PROTOCOL_CONTRACTS[self.chain]
        self._markets = CURVANCE_MARKETS[self.chain]

        logger.info(
            "CurvanceAdapter initialized on chain=%s, wallet=%s, markets=%d",
            self.chain,
            config.wallet_address[:10] + "...",
            len(self._markets),
        )

    # -------------------------------------------------------------------------
    # Market lookup helpers
    # -------------------------------------------------------------------------

    @property
    def markets(self) -> dict[str, CurvanceMarket]:
        """All registered markets on this chain (keyed by MarketManager address, lowercase)."""
        return dict(self._markets)

    def get_market(self, market_id: str | None = None) -> CurvanceMarket:
        """Resolve a market by MarketManager address; uses chain default if omitted."""
        return get_market(self.chain, market_id)

    def _token_decimals(self, symbol: str) -> int:
        """Look up decimals for a token on the adapter's chain."""
        try:
            token = self._token_resolver.resolve(symbol, self.chain)
        except TokenResolutionError as e:
            raise ValueError(
                f"Cannot resolve token '{symbol}' on {self.chain} — ensure it's registered in the token registry: {e}"
            ) from e
        return token.decimals

    # -------------------------------------------------------------------------
    # Transaction builders
    # -------------------------------------------------------------------------

    def supply_collateral(
        self,
        market_id: str,
        amount: Decimal,
        on_behalf_of: str | None = None,
    ) -> TransactionResult:
        """Build a ``depositAsCollateral`` transaction.

        Deposits the underlying asset into the market's collateral cToken and
        atomically posts the resulting shares as collateral. No separate
        ``enterMarkets`` call is required.

        Args:
            market_id: MarketManager address (case-insensitive).
            amount: Amount of underlying asset (human units).
            on_behalf_of: Receiver of the cToken shares. Defaults to wallet_address.

        Returns:
            TransactionResult with tx_data targeting the collateral cToken.
        """
        try:
            market = self.get_market(market_id)
            decimals = self._token_decimals(market.collateral_symbol)
            amount_wei = int(amount * Decimal(10**decimals))
            if amount_wei <= 0:
                return TransactionResult(
                    success=False,
                    error=f"Invalid supply amount {amount} {market.collateral_symbol}",
                )

            receiver = on_behalf_of or self.wallet_address
            calldata = encode_deposit_as_collateral(amount_wei, receiver)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": market.collateral_ctoken,
                    "value": 0,
                    "data": "0x" + calldata.hex(),
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply_collateral"],
                description=(f"Supply {amount} {market.collateral_symbol} to Curvance {market.name} (as collateral)"),
            )
        except (KeyError, ValueError) as e:
            return TransactionResult(success=False, error=str(e))

    def borrow(
        self,
        market_id: str,
        amount: Decimal,
        receiver: str | None = None,
    ) -> TransactionResult:
        """Build a ``borrow`` transaction on the market's BorrowableCToken.

        Args:
            market_id: MarketManager address (case-insensitive).
            amount: Amount of debt asset to borrow (human units).
            receiver: Address that receives the borrowed assets. Defaults to
                ``wallet_address``. The debt is always owned by ``msg.sender``
                (the wallet); only the destination of the proceeds can be
                redirected.

        Returns:
            TransactionResult with tx_data targeting the borrowable cToken.
        """
        try:
            market = self.get_market(market_id)
            decimals = self._token_decimals(market.debt_symbol)
            amount_wei = int(amount * Decimal(10**decimals))
            if amount_wei <= 0:
                return TransactionResult(
                    success=False,
                    error=f"Invalid borrow amount {amount} {market.debt_symbol}",
                )

            target_receiver = receiver or self.wallet_address
            calldata = encode_borrow(amount_wei, target_receiver)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": market.borrowable_ctoken,
                    "value": 0,
                    "data": "0x" + calldata.hex(),
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["borrow"],
                description=(f"Borrow {amount} {market.debt_symbol} from Curvance {market.name}"),
            )
        except (KeyError, ValueError) as e:
            return TransactionResult(success=False, error=str(e))

    def repay(
        self,
        market_id: str,
        amount: Decimal,
        repay_full: bool = False,
    ) -> TransactionResult:
        """Build a ``repay`` transaction on the market's BorrowableCToken.

        When ``repay_full`` is True, the adapter encodes ``repay(0)`` — Curvance
        treats 0 as "repay the caller's entire outstanding debt" (verified in
        ``BorrowableCToken.repay`` NatSpec and ``_repay`` logic). MAX_UINT256 is
        NOT a repay sentinel here; that would be treated as an over-repayment
        and revert.

        Caller balance requirement: ``repay(0)`` calls ``debtBalance(msg.sender)``
        at execution time and does ``transferFrom(msg.sender, market, that_amount)``.
        The caller must hold the full debt (principal + accrued interest) in the
        debt-underlying token at submission time, or ``transferFrom`` reverts.
        Mainnet-verified 2026-04-18 on Monad cUSDC ``0x8EE9FC...774``: TX
        ``0x176a941e…687cdb`` pulled 11,000,025 USDC to clear an 11,000,013
        principal borrow after ~20 min of accrued interest. Strategies should
        keep ≳0.01% slack over the last-read ``debtBalance`` when using this
        path.

        Args:
            market_id: MarketManager address.
            amount: Amount of debt asset to repay (human units). Ignored when
                ``repay_full=True``.
            repay_full: If True, repay the entire outstanding debt using the
                0-sentinel.

        Returns:
            TransactionResult with tx_data targeting the borrowable cToken.
        """
        try:
            market = self.get_market(market_id)

            if repay_full:
                amount_wei = 0
                description = f"Repay full debt ({market.debt_symbol}) on Curvance {market.name}"
            else:
                decimals = self._token_decimals(market.debt_symbol)
                amount_wei = int(amount * Decimal(10**decimals))
                if amount_wei <= 0:
                    return TransactionResult(
                        success=False,
                        error=f"Invalid repay amount {amount} {market.debt_symbol}",
                    )
                description = f"Repay {amount} {market.debt_symbol} on Curvance {market.name}"

            calldata = encode_repay(amount_wei)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": market.borrowable_ctoken,
                    "value": 0,
                    "data": "0x" + calldata.hex(),
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["repay"],
                description=description,
            )
        except (KeyError, ValueError) as e:
            return TransactionResult(success=False, error=str(e))

    def withdraw_collateral(
        self,
        market_id: str,
        amount: Decimal,
        withdraw_all: bool = False,
        receiver: str | None = None,
        share_balance: int | None = None,
    ) -> TransactionResult:
        """Build a collateral-withdraw transaction.

        Two paths are supported:

            - **Asset-amount** (default) → ``withdrawCollateral(assets, receiver, owner)``
              on the collateral cToken. Forces collateral to unwind and sends
              ``assets`` of the underlying to ``receiver``.
            - **Share-amount** via ``withdraw_all=True`` + ``share_balance``
              → ``redeemCollateral(shares, receiver, owner)`` on the cToken.
              Used for exact full-exit when the strategy has read ``balanceOf``
              from the collateral cToken and wants to burn every share cleanly.

        Args:
            market_id: MarketManager address.
            amount: Amount of underlying asset to withdraw (human units).
                Ignored when ``withdraw_all=True``.
            withdraw_all: Redeem full share balance. Requires ``share_balance``.
            receiver: Recipient of the underlying. Defaults to wallet_address.
            share_balance: Exact cToken share balance to burn. Required when
                ``withdraw_all=True``. Curvance's ``redeemCollateral`` does NOT
                treat MAX_UINT256 as a clamping sentinel — callers MUST read
                the share balance themselves.

        Returns:
            TransactionResult with tx_data targeting the collateral cToken.

        Note:
            Curvance enforces a 20-minute ``MIN_HOLD_PERIOD`` on collateral
            before it can be withdrawn. Earlier calls revert with
            ``MarketManager__MinimumHoldPeriod()``.
        """
        try:
            market = self.get_market(market_id)
            target_receiver = receiver or self.wallet_address

            if withdraw_all:
                if share_balance is None or share_balance <= 0:
                    return TransactionResult(
                        success=False,
                        error=(
                            "Curvance withdraw_all=True requires share_balance; the "
                            "underlying redeemCollateral() does not accept a "
                            "MAX_UINT256 sentinel. Query cToken.balanceOf(user) first."
                        ),
                    )
                calldata = encode_redeem_collateral(share_balance, target_receiver, self.wallet_address)
                description = (
                    f"Withdraw all {market.collateral_symbol} collateral "
                    f"({share_balance} shares) from Curvance {market.name}"
                )
            else:
                decimals = self._token_decimals(market.collateral_symbol)
                amount_wei = int(amount * Decimal(10**decimals))
                if amount_wei <= 0:
                    return TransactionResult(
                        success=False,
                        error=f"Invalid withdraw amount {amount} {market.collateral_symbol}",
                    )
                calldata = encode_withdraw_collateral(amount_wei, target_receiver, self.wallet_address)
                description = f"Withdraw {amount} {market.collateral_symbol} collateral from Curvance {market.name}"

            return TransactionResult(
                success=True,
                tx_data={
                    "to": market.collateral_ctoken,
                    "value": 0,
                    "data": "0x" + calldata.hex(),
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["withdraw_collateral"],
                description=description,
            )
        except (KeyError, ValueError) as e:
            return TransactionResult(success=False, error=str(e))

    # -------------------------------------------------------------------------
    # Spender lookups (for ERC20 approve wiring in the compiler)
    # -------------------------------------------------------------------------

    def get_supply_spender(self, market_id: str) -> str:
        """Return the contract that must be approved before ``supply_collateral``.

        For Curvance this is the collateral cToken — it calls
        ``transferFrom(user, self, assets)`` inside ``depositAsCollateral``.
        """
        return self.get_market(market_id).collateral_ctoken

    def get_repay_spender(self, market_id: str) -> str:
        """Return the contract that must be approved before ``repay``.

        For Curvance this is the BorrowableCToken — it calls
        ``transferFrom(payer, self, assets)`` inside ``repay``.
        """
        return self.get_market(market_id).borrowable_ctoken
