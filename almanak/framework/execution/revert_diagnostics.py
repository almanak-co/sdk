"""Revert diagnostics for understanding why transactions failed.

This module provides utilities to diagnose transaction reverts by comparing
wallet balances against intent requirements. When a transaction reverts with
an error like "STF" (SafeTransferFrom), this helps identify the root cause.

Common revert reasons:
- Insufficient token balance
- Insufficient token approval
- Insufficient native ETH for gas + protocol fees
- Slippage exceeded
- Price moved out of range
- Insufficient gas
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from ..intents.vocabulary import (
    BorrowIntent,
    IntentType,
    LPCloseIntent,
    LPOpenIntent,
    PerpOpenIntent,
    RepayIntent,
    SupplyIntent,
    SwapIntent,
    WithdrawIntent,
)

logger = logging.getLogger(__name__)

# Protocol-specific execution fees (native token, in ETH)
# These are estimates - actual fees vary with gas prices
PROTOCOL_EXECUTION_FEES: dict[str, Decimal] = {
    "gmx_v2": Decimal("0.001"),  # GMX V2 keeper execution fee (conservative)
    "gmx_v2_production": Decimal("0.005"),  # Higher for production reliability
}

# Estimated gas costs for different intent types (in ETH, at ~0.1 gwei on Arbitrum)
# Used for pre-flight native ETH balance checks
ESTIMATED_GAS_COSTS: dict[str, Decimal] = {
    "perp_open": Decimal("0.0005"),  # ~5M gas at 0.1 gwei
    "perp_close": Decimal("0.0005"),
    "swap": Decimal("0.0001"),
    "supply": Decimal("0.0002"),
    "borrow": Decimal("0.0002"),
    "default": Decimal("0.0002"),
}

# Well-known token addresses by chain
TOKEN_ADDRESSES: dict[str, dict[str, str]] = {
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDC.e": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "ARB": "0x912CE59144191C1204E64559FE8253a0e49E6548",
        "GMX": "0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a",
        "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
    },
    "base": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
    },
    "ethereum": {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
    },
}

# Token decimals
TOKEN_DECIMALS: dict[str, int] = {
    "WETH": 18,
    "ETH": 18,
    "USDC": 6,
    "USDC.e": 6,
    "USDbC": 6,
    "USDT": 6,
    "ARB": 18,
    "GMX": 18,
    "WBTC": 8,
}


@dataclass
class TokenRequirement:
    """A token and amount required by an intent."""

    symbol: str
    amount: Decimal
    address: str | None = None
    decimals: int = 18


@dataclass
class NativeETHRequirement:
    """Native ETH requirement for gas and protocol execution fees.

    This is separate from token requirements because native ETH is used
    for transaction gas AND protocol-specific execution fees (e.g., GMX keepers).
    """

    gas_estimate: Decimal  # Estimated gas cost in ETH
    execution_fee: Decimal  # Protocol execution fee in ETH (e.g., GMX keeper fee)
    protocol: str | None = None  # Protocol name (e.g., "gmx_v2")
    description: str = ""  # Human-readable explanation

    @property
    def total(self) -> Decimal:
        """Total native ETH required."""
        return self.gas_estimate + self.execution_fee


@dataclass
class BalanceCheck:
    """Result of checking a token balance against a requirement."""

    symbol: str
    required: Decimal | str  # Can be "all" for dynamic amounts
    actual: Decimal
    sufficient: bool
    shortfall: Decimal | str  # How much more is needed (0 if sufficient)

    def format(self) -> str:
        """Format the balance check for display."""
        status = "✓" if self.sufficient else "✗"
        # Handle required/shortfall that might be strings (e.g., "all")
        req_str = f"{self.required:.6f}" if isinstance(self.required, Decimal) else str(self.required)
        short_str = f"{self.shortfall:.6f}" if isinstance(self.shortfall, Decimal) else str(self.shortfall)
        actual_str = f"{self.actual:.6f}" if isinstance(self.actual, Decimal) else str(self.actual)

        if self.sufficient:
            return f"  {status} {self.symbol}: {actual_str} (need {req_str})"
        else:
            return f"  {status} {self.symbol}: {actual_str} (need {req_str}, short {short_str})"


@dataclass
class NativeETHCheck:
    """Result of checking native ETH balance for gas + execution fees."""

    required: Decimal
    actual: Decimal
    sufficient: bool
    shortfall: Decimal
    breakdown: str  # e.g., "gas: 0.0005 ETH + GMX execution fee: 0.001 ETH"

    def format(self) -> str:
        """Format the native ETH check for display."""
        status = "✓" if self.sufficient else "✗"
        if self.sufficient:
            return f"  {status} Native ETH: {self.actual:.6f} (need {self.required:.6f} for {self.breakdown})"
        else:
            return f"  {status} Native ETH: {self.actual:.6f} (need {self.required:.6f}, short {self.shortfall:.6f})\n      Breakdown: {self.breakdown}"


@dataclass
class RevertDiagnostic:
    """Diagnostic information about why a transaction may have reverted."""

    intent_type: IntentType
    chain: str
    wallet: str
    balance_checks: list[BalanceCheck]
    likely_cause: str
    suggestions: list[str]
    raw_error: str | None = None
    native_eth_check: NativeETHCheck | None = None
    gas_warnings: list[str] | None = None

    @property
    def has_insufficient_balance(self) -> bool:
        """Check if any balance is insufficient."""
        return any(not check.sufficient for check in self.balance_checks)

    @property
    def has_insufficient_native_eth(self) -> bool:
        """Check if native ETH is insufficient for gas + fees."""
        return self.native_eth_check is not None and not self.native_eth_check.sufficient

    def format(self) -> str:
        """Format the diagnostic for logging/display."""
        lines = [
            "",
            "=" * 60,
            "REVERT DIAGNOSTIC",
            "=" * 60,
            f"Intent: {self.intent_type.value}",
            f"Chain: {self.chain}",
            f"Wallet: {self.wallet}",
            "",
        ]

        # Native ETH check (gas + execution fees) - show first as it's often the issue
        if self.native_eth_check:
            lines.append("Native ETH (gas + execution fees):")
            lines.append(self.native_eth_check.format())
            lines.append("")

        lines.append("Token Balances vs Requirements:")

        for check in self.balance_checks:
            lines.append(check.format())

        if self.gas_warnings:
            lines.append("")
            lines.append("Gas Estimation Warnings:")
            for warning in self.gas_warnings:
                lines.append(f"  - {warning}")

        lines.extend(
            [
                "",
                f"Likely Cause: {self.likely_cause}",
                "",
                "Suggestions:",
            ]
        )

        for i, suggestion in enumerate(self.suggestions, 1):
            lines.append(f"  {i}. {suggestion}")

        if self.raw_error:
            lines.extend(
                [
                    "",
                    f"Raw Error: {self.raw_error}",
                ]
            )

        lines.append("=" * 60)

        return "\n".join(lines)


def get_token_address(symbol: str, chain: str) -> str | None:
    """Get the token address for a symbol on a chain."""
    chain_tokens = TOKEN_ADDRESSES.get(chain, {})
    return chain_tokens.get(symbol) or chain_tokens.get(symbol.upper())


def extract_token_requirements(
    intent: SwapIntent
    | LPOpenIntent
    | LPCloseIntent
    | BorrowIntent
    | RepayIntent
    | SupplyIntent
    | WithdrawIntent
    | PerpOpenIntent,
    chain: str,
) -> list[TokenRequirement]:
    """Extract the token requirements from an intent.

    Args:
        intent: The intent to extract requirements from
        chain: The chain the intent will execute on

    Returns:
        List of TokenRequirement objects
    """
    requirements = []

    if isinstance(intent, SwapIntent):
        # Swap requires the from_token
        symbol = intent.from_token
        if intent.amount is not None and intent.amount != "all" and isinstance(intent.amount, Decimal):
            requirements.append(
                TokenRequirement(
                    symbol=symbol,
                    amount=intent.amount,
                    address=get_token_address(symbol, chain),
                    decimals=TOKEN_DECIMALS.get(symbol, 18),
                )
            )
        # Note: amount_usd requires price lookup which we skip here

    elif isinstance(intent, LPOpenIntent):
        # LP Open requires both tokens
        # Parse pool to get token symbols (e.g., "WETH/USDC/500" or "WETH/USDC.e/500")
        pool_parts = intent.pool.split("/")
        if len(pool_parts) >= 2:
            token0_symbol = pool_parts[0]
            token1_symbol = pool_parts[1]

            if intent.amount0 > 0:
                requirements.append(
                    TokenRequirement(
                        symbol=token0_symbol,
                        amount=intent.amount0,
                        address=get_token_address(token0_symbol, chain),
                        decimals=TOKEN_DECIMALS.get(token0_symbol, 18),
                    )
                )

            if intent.amount1 > 0:
                requirements.append(
                    TokenRequirement(
                        symbol=token1_symbol,
                        amount=intent.amount1,
                        address=get_token_address(token1_symbol, chain),
                        decimals=TOKEN_DECIMALS.get(token1_symbol, 18),
                    )
                )

    elif isinstance(intent, SupplyIntent):
        # Supply requires the token being supplied
        if intent.amount != "all" and isinstance(intent.amount, Decimal):
            requirements.append(
                TokenRequirement(
                    symbol=intent.token,
                    amount=intent.amount,
                    address=get_token_address(intent.token, chain),
                    decimals=TOKEN_DECIMALS.get(intent.token, 18),
                )
            )

    elif isinstance(intent, RepayIntent):
        # Repay requires the token being repaid
        if intent.amount != "all" and isinstance(intent.amount, Decimal):
            requirements.append(
                TokenRequirement(
                    symbol=intent.token,
                    amount=intent.amount,
                    address=get_token_address(intent.token, chain),
                    decimals=TOKEN_DECIMALS.get(intent.token, 18),
                )
            )

    elif isinstance(intent, PerpOpenIntent):
        # Perp open requires collateral token
        # Skip if amount is "all" - can't check balance requirements for dynamic amounts
        if intent.collateral_amount != "all" and isinstance(intent.collateral_amount, Decimal):
            requirements.append(
                TokenRequirement(
                    symbol=intent.collateral_token,
                    amount=intent.collateral_amount,
                    address=get_token_address(intent.collateral_token, chain),
                    decimals=TOKEN_DECIMALS.get(intent.collateral_token, 18),
                )
            )

    return requirements


def extract_native_eth_requirement(
    intent: SwapIntent
    | LPOpenIntent
    | LPCloseIntent
    | BorrowIntent
    | RepayIntent
    | SupplyIntent
    | WithdrawIntent
    | PerpOpenIntent,
    chain: str,
) -> NativeETHRequirement | None:
    """Extract native ETH requirement for gas and protocol execution fees.

    This is critical for protocols like GMX that require execution fees
    paid in native ETH to keepers who execute the orders.

    Args:
        intent: The intent to extract requirements from
        chain: The chain the intent will execute on

    Returns:
        NativeETHRequirement if the intent needs special ETH handling, else None
    """
    intent_type = intent.intent_type.value.lower()

    # Get base gas estimate for this intent type
    gas_estimate = ESTIMATED_GAS_COSTS.get(intent_type, ESTIMATED_GAS_COSTS["default"])

    # Check for protocol-specific execution fees
    if isinstance(intent, PerpOpenIntent):
        # GMX V2 requires execution fee for keepers
        # This is the most common cause of "insufficient funds" errors
        protocol = getattr(intent, "protocol", None) or "gmx_v2"
        execution_fee = PROTOCOL_EXECUTION_FEES.get(protocol, Decimal("0.001"))

        return NativeETHRequirement(
            gas_estimate=gas_estimate,
            execution_fee=execution_fee,
            protocol=protocol,
            description=f"gas (~{gas_estimate} ETH) + {protocol} keeper execution fee (~{execution_fee} ETH)",
        )

    # For other intents, just return gas estimate if on a chain where it matters
    # Most L2s have low gas costs, so we return None for simplicity
    return None


async def check_native_eth_balance(
    requirement: NativeETHRequirement,
    wallet: str,
    chain: str,
    web3_provider: Any,
) -> NativeETHCheck:
    """Check if wallet has sufficient native ETH for gas + execution fees.

    Args:
        requirement: Native ETH requirement
        wallet: Wallet address to check
        chain: Chain to check on
        web3_provider: Web3 balance provider instance

    Returns:
        NativeETHCheck with balance status
    """
    try:
        # Get native ETH balance (symbol "ETH" or chain-specific)
        balance_result = await web3_provider.get_balance("ETH")
        actual = balance_result.balance
    except Exception as e:
        logger.warning(f"Failed to check native ETH balance: {e}")
        actual = Decimal("0")

    required = requirement.total
    sufficient = actual >= required
    shortfall = max(Decimal("0"), required - actual)

    return NativeETHCheck(
        required=required,
        actual=actual,
        sufficient=sufficient,
        shortfall=shortfall,
        breakdown=requirement.description,
    )


async def check_balances(
    requirements: list[TokenRequirement],
    wallet: str,
    chain: str,
    web3_provider: Any,
) -> list[BalanceCheck]:
    """Check wallet balances against requirements.

    Args:
        requirements: List of token requirements
        wallet: Wallet address to check
        chain: Chain to check on
        web3_provider: Web3 balance provider instance (Web3BalanceProvider)

    Returns:
        List of BalanceCheck results
    """
    checks = []

    for req in requirements:
        try:
            # Use the balance provider's get_balance method (takes token symbol)
            balance_result = await web3_provider.get_balance(req.symbol)
            actual = balance_result.balance

            sufficient = actual >= req.amount
            shortfall = max(Decimal("0"), req.amount - actual)

            checks.append(
                BalanceCheck(
                    symbol=req.symbol,
                    required=req.amount,
                    actual=actual,
                    sufficient=sufficient,
                    shortfall=shortfall,
                )
            )

        except Exception as e:
            logger.warning(f"Failed to check balance for {req.symbol}: {e}")
            # Assume insufficient if we can't check
            checks.append(
                BalanceCheck(
                    symbol=req.symbol,
                    required=req.amount,
                    actual=Decimal("0"),
                    sufficient=False,
                    shortfall=req.amount,
                )
            )

    return checks


def determine_likely_cause(
    balance_checks: list[BalanceCheck],
    raw_error: str | None,
    native_eth_check: NativeETHCheck | None = None,
    gas_warnings: list[str] | None = None,
) -> tuple[str, list[str]]:
    """Determine the likely cause of a revert and suggest fixes.

    Args:
        balance_checks: Results of balance checks
        raw_error: The raw error message if available
        native_eth_check: Result of native ETH balance check
        gas_warnings: Warnings from gas estimation (e.g., STF reverts during eth_estimateGas)

    Returns:
        Tuple of (likely_cause, suggestions)
    """
    suggestions = []
    error_lower = raw_error.lower() if raw_error else ""

    # PRIORITY 0: Signing / address mismatch errors
    # These are configuration errors, not on-chain reverts
    mismatch_patterns = ["does not match signer", "from_address", "signing failed", "address mismatch"]
    if raw_error and any(p in error_lower for p in mismatch_patterns):
        cause = "Configuration error: wallet address mismatch"
        suggestions.append("The transaction's from_address does not match the configured signer")
        suggestions.append("Check ALMANAK_PRIVATE_KEY matches the wallet address in your strategy config")
        # Try to extract addresses from the error for context
        if "does not match" in error_lower:
            suggestions.append(f"Details: {raw_error[:200]}")
        # Also note gas estimation warnings if present
        if gas_warnings:
            for w in gas_warnings:
                suggestions.append(f"Note: gas estimation also detected: {w}")
        return cause, suggestions

    # PRIORITY 0.5: Compilation / build errors
    compilation_patterns = ["compilation failed", "build failed", "compile error", "intent compilation"]
    if raw_error and any(p in error_lower for p in compilation_patterns):
        cause = "Intent compilation error"
        suggestions.append("The intent could not be compiled into transactions")
        suggestions.append(f"Details: {raw_error[:200]}")
        return cause, suggestions

    # PRIORITY 1: Check for insufficient native ETH (gas + execution fees)
    # This is often the root cause for "insufficient funds" errors
    if native_eth_check and not native_eth_check.sufficient:
        cause = "Insufficient native ETH for gas + execution fees"
        suggestions.append(f"Send at least {native_eth_check.shortfall:.6f} ETH to your wallet on this chain")
        suggestions.append(f"Required: {native_eth_check.required:.6f} ETH ({native_eth_check.breakdown})")
        suggestions.append(f"Current balance: {native_eth_check.actual:.6f} ETH")

        # Check if this is a GMX-related issue
        if "gmx" in native_eth_check.breakdown.lower():
            suggestions.append("Note: GMX V2 requires an execution fee paid to keepers who execute your order")
            suggestions.append("For testing, you can reduce MIN_EXECUTION_FEE_FALLBACK in src/connectors/gmx_v2/sdk.py")

        return cause, suggestions

    # PRIORITY 2: Check for insufficient token balances
    insufficient = [c for c in balance_checks if not c.sufficient]
    if insufficient:
        tokens = ", ".join(c.symbol for c in insufficient)
        cause = f"Insufficient balance for: {tokens}"

        for check in insufficient:
            # Handle shortfall that might be a string (e.g., "all") or Decimal
            shortfall_str = f"{check.shortfall:.6f}" if isinstance(check.shortfall, Decimal) else str(check.shortfall)
            if check.symbol == "WETH":
                suggestions.append(
                    f"Wrap ETH to WETH: cast send 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1 'deposit()' --value {shortfall_str}ether --rpc-url <RPC> --private-key <KEY>"
                )
            elif check.symbol in ("USDC", "USDC.e", "USDT"):
                suggestions.append(f"Acquire {shortfall_str} {check.symbol} via swap or bridge")
            else:
                suggestions.append(f"Acquire {shortfall_str} more {check.symbol}")

        return cause, suggestions

    # If balances are sufficient, it might be an approval issue
    if raw_error and "stf" in error_lower:
        cause = "Token transfer failed (STF) - balances OK, likely an approval issue"
        stf_suggestions = [
            "Check token approvals for the Position Manager contract",
            "Approvals may have been consumed or not confirmed before mint TX",
            "The system now clears approval cache on retry - try again",
        ]
        if gas_warnings:
            for w in gas_warnings:
                stf_suggestions.append(f"Note: gas estimation also detected: {w}")
        return cause, stf_suggestions

    # Check for common error patterns
    if raw_error:
        if "stf" in error_lower or "safetransferfrom" in error_lower:
            return (
                "Token transfer failed (STF) - likely insufficient balance or approval",
                ["Check token balance", "Check token approval for the contract"],
            )

        if "slippage" in error_lower or "price" in error_lower:
            return (
                "Slippage or price check failed",
                ["Increase slippage tolerance", "Try smaller trade size", "Wait for less volatile conditions"],
            )

        if "deadline" in error_lower or "expired" in error_lower:
            return ("Transaction deadline expired", ["Increase deadline", "Use faster gas settings"])

        if "liquidity" in error_lower:
            return ("Insufficient liquidity in pool", ["Try smaller amount", "Use a different pool/route"])

    # Check gas warnings for clues when error is otherwise unknown
    if gas_warnings:
        gas_warning_text = "; ".join(gas_warnings)
        gas_lower = gas_warning_text.lower()

        if "stf" in gas_lower or "safetransferfrom" in gas_lower:
            return (
                "Gas estimation detected STF revert - likely an approval issue",
                [
                    "Check token approvals for the target contract",
                    f"Gas estimation details: {gas_warning_text}",
                ],
            )

        return (
            "Unknown - balances appear sufficient but gas estimation detected issues",
            [
                f"Gas estimation warnings: {gas_warning_text}",
                "Check token approvals",
                "Verify contract parameters",
            ],
        )

    # Default
    return (
        "Unknown - balances appear sufficient",
        ["Check token approvals", "Verify contract parameters", "Review transaction simulation"],
    )


async def diagnose_revert(
    intent: Any,
    chain: str,
    wallet: str,
    web3_provider: Any,
    raw_error: str | None = None,
    gas_warnings: list[str] | None = None,
) -> RevertDiagnostic:
    """Diagnose why a transaction may have reverted.

    This is the main entry point for revert diagnostics. Call this when
    a transaction reverts to get helpful information about the cause.

    Args:
        intent: The intent that failed
        chain: The chain the transaction was on
        wallet: The wallet address
        web3_provider: Web3 balance provider for checking balances
        raw_error: The raw error message if available
        gas_warnings: Warnings collected during gas estimation (e.g., STF reverts)

    Returns:
        RevertDiagnostic with analysis and suggestions

    Example:
        diagnostic = await diagnose_revert(
            intent=lp_open_intent,
            chain="arbitrum",
            wallet="0x...",
            web3_provider=balance_provider,
            raw_error="STF",
            gas_warnings=["tx 3/3: execution reverted: STF"],
        )
        logger.error(diagnostic.format())
    """
    # Extract what the intent needs (ERC-20 tokens)
    requirements = extract_token_requirements(intent, chain)

    # Check actual ERC-20 balances
    balance_checks = await check_balances(requirements, wallet, chain, web3_provider)

    # Check native ETH requirements (gas + protocol execution fees)
    native_eth_check: NativeETHCheck | None = None
    native_eth_req = extract_native_eth_requirement(intent, chain)
    if native_eth_req:
        native_eth_check = await check_native_eth_balance(native_eth_req, wallet, chain, web3_provider)

    # Determine cause and suggestions (native ETH check has priority)
    likely_cause, suggestions = determine_likely_cause(balance_checks, raw_error, native_eth_check, gas_warnings)

    return RevertDiagnostic(
        intent_type=intent.intent_type,
        chain=chain,
        wallet=wallet,
        balance_checks=balance_checks,
        likely_cause=likely_cause,
        suggestions=suggestions,
        raw_error=raw_error,
        native_eth_check=native_eth_check,
        gas_warnings=gas_warnings,
    )


# =============================================================================
# Verbose Revert Report
# =============================================================================

# Known function selectors for common DeFi operations
KNOWN_FUNCTION_SELECTORS: dict[str, str] = {
    # ERC-20
    "0x095ea7b3": "approve(address,uint256)",
    "0x23b872dd": "transferFrom(address,address,uint256)",
    "0xa9059cbb": "transfer(address,uint256)",
    # Uniswap V3 Router
    "0x414bf389": "exactInputSingle(ExactInputSingleParams)",
    "0xc04b8d59": "exactInput(ExactInputParams)",
    "0x5ae401dc": "multicall(uint256,bytes[])",
    "0xac9650d8": "multicall(bytes[])",
    # Uniswap V3 Position Manager
    "0x88316456": "mint(MintParams)",
    "0x0c49ccbe": "decreaseLiquidity(DecreaseLiquidityParams)",
    "0xfc6f7865": "collect(CollectParams)",
    "0x42966c68": "burn(uint256)",
    # Uniswap V2 Router
    "0xe8e33700": "addLiquidity(...)",
    "0xbaa2abde": "removeLiquidity(...)",
    "0x38ed1739": "swapExactTokensForTokens(...)",
    "0x8803dbee": "swapTokensForExactTokens(...)",
    # Aave V3
    "0x617ba037": "supply(address,uint256,address,uint16)",
    "0x69328dec": "withdraw(address,uint256,address)",
    "0xa415bcad": "borrow(address,uint256,uint256,uint16,address)",
    "0x573ade81": "repay(address,uint256,uint256,address)",
    # PancakeSwap V3
    "0x04e45aaf": "exactInputSingle(ExactInputSingleParams)",
    "0xb858183f": "exactInput(ExactInputParams)",
    # Enso
    "0x2075cd40": "route(bytes)",
    # GMX V2
    "0x5b88e8c6": "createOrder(CreateOrderParams)",
    "0x35ce5ed8": "multicall(bytes[])",
}


def decode_calldata_selector(calldata: str) -> str:
    """Decode calldata into human-readable function name if known.

    Args:
        calldata: The full hex calldata string (e.g., "0x095ea7b3...")

    Returns:
        Human-readable function signature or "unknown(0x...)" if not recognized
    """
    if not calldata or len(calldata) < 10:
        return "unknown(empty)"
    selector = calldata[:10].lower()
    return KNOWN_FUNCTION_SELECTORS.get(selector, f"unknown({selector})")


@dataclass
class TransactionDetails:
    """Details of a single transaction for debugging.

    Captures the key information needed to understand what a transaction
    was trying to do and why it may have failed.
    """

    tx_hash: str
    to_address: str
    value_wei: int
    gas_limit: int
    gas_used: int | None
    nonce: int
    calldata_selector: str  # e.g., "0x095ea7b3"
    calldata_decoded: str  # e.g., "approve(address,uint256)"
    calldata_full: str  # Full hex calldata
    success: bool
    revert_reason: str | None = None

    def format(self) -> str:
        """Format transaction details for display."""
        status = "SUCCESS" if self.success else "REVERTED"
        lines = [
            f"    TX Hash: {self.tx_hash}",
            f"    Status: {status}",
            f"    To: {self.to_address}",
            f"    Value: {self.value_wei} wei",
            f"    Gas Limit: {self.gas_limit:,}",
            f"    Gas Used: {self.gas_used:,}" if self.gas_used is not None else "    Gas Used: N/A",
            f"    Nonce: {self.nonce}",
            f"    Function: {self.calldata_decoded}",
            f"    Calldata: {self.calldata_selector}...",
        ]
        if self.revert_reason:
            lines.append(f"    Revert Reason: {self.revert_reason}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "tx_hash": self.tx_hash,
            "to_address": self.to_address,
            "value_wei": self.value_wei,
            "gas_limit": self.gas_limit,
            "gas_used": self.gas_used,
            "nonce": self.nonce,
            "calldata_selector": self.calldata_selector,
            "calldata_decoded": self.calldata_decoded,
            "calldata_full": self.calldata_full,
            "success": self.success,
            "revert_reason": self.revert_reason,
        }


@dataclass
class IntentDetails:
    """Details of the intent that was being executed.

    Captures the high-level intent parameters as specified by the strategy,
    before compilation into actions.
    """

    intent_type: str
    intent_id: str
    params: dict[str, Any]

    def format(self) -> str:
        """Format intent details for display."""
        lines = [
            f"  Intent Type: {self.intent_type}",
            f"  Intent ID: {self.intent_id}",
            "  Parameters:",
        ]
        for key, value in self.params.items():
            # Skip internal fields
            if key.startswith("_"):
                continue
            # Truncate long values
            value_str = str(value)
            if len(value_str) > 80:
                value_str = value_str[:80] + "..."
            lines.append(f"    {key}: {value_str}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "intent_type": self.intent_type,
            "intent_id": self.intent_id,
            "params": self.params,
        }


@dataclass
class ActionDetails:
    """Details of a single action in the bundle.

    Captures the compiled action parameters that were generated from the intent.
    """

    action_type: str
    protocol: str
    params: dict[str, Any]

    def format(self) -> str:
        """Format action details for display."""
        lines = [
            f"    Type: {self.action_type}",
            f"    Protocol: {self.protocol}",
            "    Params:",
        ]
        for key, value in self.params.items():
            # Truncate long values
            value_str = str(value)
            if len(value_str) > 80:
                value_str = value_str[:80] + "..."
            lines.append(f"      {key}: {value_str}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "action_type": self.action_type,
            "protocol": self.protocol,
            "params": self.params,
        }


@dataclass
class VerboseRevertReport:
    """Comprehensive report for debugging transaction reverts.

    Aggregates all available information about a failed execution:
    - Execution context (strategy, chain, wallet, correlation_id)
    - Intent parameters as specified by the strategy
    - Action parameters as compiled from the intent
    - Transaction details (calldata, gas, nonce)
    - Timing information

    Use format() for human-readable output or to_dict() for JSON serialization.

    Example:
        report = build_verbose_revert_report(context, bundle, tx_results)
        logger.error(report.format())
    """

    # Execution context
    strategy_id: str
    chain: str
    wallet_address: str
    correlation_id: str
    intent_description: str

    # Timing
    started_at: datetime
    failed_at: datetime
    execution_phase: str

    # Intent details (what the strategy requested)
    intent: IntentDetails | None = None

    # Action details (all actions in bundle)
    actions: list[ActionDetails] = field(default_factory=list)

    # Transaction details
    transactions: list[TransactionDetails] = field(default_factory=list)

    # Raw error
    raw_error: str | None = None

    def format(self) -> str:
        """Format the complete report for logging/display."""
        separator = "=" * 70
        lines = [
            "",
            separator,
            "VERBOSE REVERT REPORT",
            separator,
            "",
            "--- EXECUTION CONTEXT ---",
            f"Strategy ID: {self.strategy_id}",
            f"Chain: {self.chain}",
            f"Wallet: {self.wallet_address}",
            f"Correlation ID: {self.correlation_id}",
            f"Intent Description: {self.intent_description}",
            "",
            f"Started At: {self.started_at.isoformat()}",
            f"Failed At: {self.failed_at.isoformat()}",
            f"Execution Phase: {self.execution_phase}",
            "",
        ]

        if self.intent:
            lines.extend(
                [
                    "--- INTENT DETAILS ---",
                    self.intent.format(),
                    "",
                ]
            )

        if self.actions:
            lines.append("--- ACTIONS ---")
            for i, action in enumerate(self.actions, 1):
                lines.append(f"  Action {i}/{len(self.actions)}:")
                lines.append(action.format())
            lines.append("")

        if self.transactions:
            lines.append("--- TRANSACTIONS ---")
            for i, tx in enumerate(self.transactions, 1):
                lines.append(f"  Transaction {i}/{len(self.transactions)}:")
                lines.append(tx.format())
                lines.append("")

        if self.raw_error:
            lines.extend(
                [
                    "--- RAW ERROR ---",
                    self.raw_error[:500] if len(self.raw_error) > 500 else self.raw_error,
                    "",
                ]
            )

        lines.append(separator)
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "strategy_id": self.strategy_id,
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "correlation_id": self.correlation_id,
            "intent_description": self.intent_description,
            "started_at": self.started_at.isoformat(),
            "failed_at": self.failed_at.isoformat(),
            "execution_phase": self.execution_phase,
            "intent": self.intent.to_dict() if self.intent else None,
            "actions": [a.to_dict() for a in self.actions],
            "transactions": [t.to_dict() for t in self.transactions],
            "raw_error": self.raw_error,
        }


def build_verbose_revert_report(
    context: Any,  # ExecutionContext
    action_bundle: Any,  # ActionBundle
    transaction_results: list[Any],  # list[TransactionResult]
    intent: Any | None = None,
    raw_error: str | None = None,
    started_at: datetime | None = None,
) -> VerboseRevertReport:
    """Build a comprehensive revert report from available execution data.

    Args:
        context: ExecutionContext with strategy_id, chain, wallet, etc.
        action_bundle: The ActionBundle that was being executed
        transaction_results: List of TransactionResult from execution
        intent: Optional original intent object (SwapIntent, LPOpenIntent, etc.)
        raw_error: Optional raw error message
        started_at: Optional start time of execution

    Returns:
        VerboseRevertReport ready for format() or to_dict()

    Example:
        report = build_verbose_revert_report(
            context=context,
            action_bundle=action_bundle,
            transaction_results=result.transaction_results,
            raw_error=first_reverted.error,
        )
        logger.error(report.format())
    """
    # Build intent details if intent is provided
    intent_details = None
    if intent is not None:
        intent_type_value = getattr(intent, "intent_type", "UNKNOWN")
        if hasattr(intent_type_value, "value"):
            intent_type_value = intent_type_value.value

        # Get intent params - try model_dump first (Pydantic), then __dict__
        if hasattr(intent, "model_dump"):
            intent_params = intent.model_dump()
        elif hasattr(intent, "__dict__"):
            intent_params = {k: v for k, v in intent.__dict__.items() if not k.startswith("_")}
        else:
            intent_params = {}

        intent_details = IntentDetails(
            intent_type=str(intent_type_value),
            intent_id=str(getattr(intent, "id", getattr(intent, "intent_id", ""))),
            params=intent_params,
        )

    # Build action details from action_bundle
    action_details_list: list[ActionDetails] = []
    if hasattr(action_bundle, "actions"):
        for action in action_bundle.actions:
            action_type = getattr(action, "type", "UNKNOWN")
            if hasattr(action_type, "value"):
                action_type = action_type.value

            protocol = getattr(action, "protocol", "UNKNOWN")
            if hasattr(protocol, "value"):
                protocol = protocol.value

            # Get action params
            params = getattr(action, "params", None)
            if params is not None:
                if hasattr(params, "model_dump"):
                    params_dict = params.model_dump()
                elif hasattr(params, "__dict__"):
                    params_dict = {k: v for k, v in params.__dict__.items() if not k.startswith("_")}
                else:
                    params_dict = {"raw": str(params)}
            else:
                params_dict = {}

            action_details_list.append(
                ActionDetails(
                    action_type=str(action_type),
                    protocol=str(protocol),
                    params=params_dict,
                )
            )

    # Build transaction details
    tx_details_list: list[TransactionDetails] = []

    # Get transaction dicts from action_bundle if available
    bundle_tx_dicts: list[dict[str, Any]] = []
    if hasattr(action_bundle, "transactions"):
        for tx in action_bundle.transactions:
            if hasattr(tx, "tx_dict") and tx.tx_dict:
                bundle_tx_dicts.append(tx.tx_dict)

    for i, tr in enumerate(transaction_results):
        # Get base info from TransactionResult
        tx_hash = getattr(tr, "tx_hash", "")
        success = getattr(tr, "success", False)
        gas_used = getattr(tr, "gas_used", None)
        error = getattr(tr, "error", None)

        # Try to get receipt for additional info
        receipt = getattr(tr, "receipt", None)
        to_address = ""
        if receipt:
            to_address = getattr(receipt, "to_address", getattr(receipt, "to", ""))

        # Try to get tx_dict for calldata, value, nonce, gas
        value_wei = 0
        gas_limit = 0
        nonce = 0
        calldata = ""

        if i < len(bundle_tx_dicts):
            tx_dict = bundle_tx_dicts[i]
            to_address = to_address or tx_dict.get("to", "")
            value_wei = int(tx_dict.get("value", 0))
            gas_limit = tx_dict.get("gas", tx_dict.get("gasLimit", 0))
            nonce = tx_dict.get("nonce", 0)
            calldata = tx_dict.get("data", tx_dict.get("input", ""))

        # Decode calldata
        calldata_selector = calldata[:10] if calldata and len(calldata) >= 10 else ""
        calldata_decoded = decode_calldata_selector(calldata)

        tx_details_list.append(
            TransactionDetails(
                tx_hash=tx_hash,
                to_address=to_address,
                value_wei=value_wei,
                gas_limit=gas_limit,
                gas_used=gas_used,
                nonce=nonce,
                calldata_selector=calldata_selector,
                calldata_decoded=calldata_decoded,
                calldata_full=calldata,
                success=success,
                revert_reason=error,
            )
        )

    return VerboseRevertReport(
        strategy_id=getattr(context, "strategy_id", "unknown"),
        chain=getattr(context, "chain", "unknown"),
        wallet_address=getattr(context, "wallet_address", "unknown"),
        correlation_id=getattr(context, "correlation_id", ""),
        intent_description=getattr(context, "intent_description", ""),
        started_at=started_at or datetime.now(UTC),
        failed_at=datetime.now(UTC),
        execution_phase="CONFIRMATION",  # Reverts happen at confirmation
        intent=intent_details,
        actions=action_details_list,
        transactions=tx_details_list,
        raw_error=raw_error,
    )
