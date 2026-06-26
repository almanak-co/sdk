"""Pydantic request/response schemas for all agent tools.

Every tool has a paired Request and Response model. Response models include
optional ``decision_hints`` (machine-readable) and ``explanation`` (human-readable)
fields so agents can reason about results.

All action tools accept a ``dry_run`` flag that triggers simulation without
on-chain execution.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, Field, field_validator, model_validator

from almanak.core.chains import DEFAULT_CHAIN, DEFAULT_VAULT_CHAIN


def _validate_positive_decimal(v: str, field_name: str) -> str:
    """Validate that a string represents a positive decimal number."""
    try:
        d = Decimal(v)
    except (InvalidOperation, TypeError, ValueError) as e:
        raise ValueError(f"{field_name} must be a valid decimal string, got '{v}'") from e
    if d <= 0:
        raise ValueError(f"{field_name} must be positive, got '{v}'")
    return v


def _validate_positive_or_all(v: str, field_name: str) -> str:
    """Validate that a string is either 'all' or a positive decimal."""
    if v.lower() == "all":
        return v
    return _validate_positive_decimal(v, field_name)


def _validate_non_negative_int_string(v: str, field_name: str) -> str:
    """Validate that a string represents a non-negative integer."""
    try:
        n = int(v)
    except (TypeError, ValueError) as e:
        raise ValueError(f"{field_name} must be a valid integer string, got '{v}'") from e
    if n < 0:
        raise ValueError(f"{field_name} must be non-negative, got '{v}'")
    return v


_ETH_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


def _normalize_protocol_key(protocol: str) -> str:
    """Canonicalize protocol identifiers for capability lookups.

    The PROTOCOL_CAPABILITIES table uses snake_case keys (e.g. ``morpho_blue``).
    Operators typing ``--protocol morpho-blue`` or ``"morpho blue"`` on the CLI
    (or agents passing aliases) would otherwise miss the capability entry and
    skip schema-level guards. Collapse hyphens and whitespace into underscores
    before lookup. (CodeRabbit PR #1535 review.)
    """
    return protocol.strip().lower().replace("-", "_").replace(" ", "_")


def _protocol_requires_market_id(protocol: str) -> bool:
    """Return True if the protocol uses isolated markets and needs ``market_id``.

    Read from the authoritative PROTOCOL_CAPABILITIES table so schema checks
    stay in sync with the intent-layer validator. Imported lazily to avoid a
    circular import between schemas.py and framework/intents/*.
    """
    from almanak.framework.intents.vocabulary import PROTOCOL_CAPABILITIES

    return bool(PROTOCOL_CAPABILITIES.get(_normalize_protocol_key(protocol), {}).get("requires_market_id", False))


def _check_market_id(protocol: str, market_id: str | None, op: str) -> None:
    """Fail fast if ``market_id`` is missing for an isolated-market protocol.

    Surfacing this at the schema layer means tool invocations (via agent or
    CLI) fail before the gateway RPC round-trip, matching the UX of other
    required fields. The compiler still validates independently as defense in
    depth. (Review: CodeRabbit + Claude pr-auditor on PR #1535.)
    """
    if _protocol_requires_market_id(protocol) and (not market_id or not market_id.strip()):
        raise ValueError(f"market_id is required when protocol='{protocol}' for {op} (isolated-market protocol)")


# =============================================================================
# Shared envelope
# =============================================================================


class ToolResponse(BaseModel):
    """Standard wrapper returned by every tool."""

    status: str = Field(description="'success', 'simulated', 'blocked', or 'error'")
    data: dict | None = Field(default=None, description="Tool-specific result payload")
    error: dict | None = Field(default=None, description="Structured error if status == 'error'")
    decision_hints: dict | None = Field(default=None, description="Machine-readable hints for agent reasoning")
    explanation: str | None = Field(default=None, description="Human-readable context about the result")


# =============================================================================
# READ TOOLS
# =============================================================================


class GetPriceRequest(BaseModel):
    """Get the current USD price of a token."""

    token: str = Field(description="Token symbol (e.g. 'ETH', 'USDC') or contract address")
    chain: str = Field(default=DEFAULT_CHAIN, description="Blockchain name (e.g. 'arbitrum', 'base', 'ethereum')")


class GetPriceResponse(BaseModel):
    token: str
    price_usd: float
    source: str = ""
    timestamp: str = ""
    change_24h_pct: float | None = None
    high_24h: float | None = None
    low_24h: float | None = None


class GetBalanceRequest(BaseModel):
    """Get the balance of a single token in a wallet."""

    token: str = Field(description="Token symbol or address")
    chain: str = Field(default=DEFAULT_CHAIN)
    wallet_address: str = Field(default="", description="Wallet to query. Defaults to strategy wallet.")


class GetBalanceResponse(BaseModel):
    token: str
    balance: str = Field(description="Balance in human-readable units")
    balance_usd: str = Field(description="Balance converted to USD")


class BatchGetBalancesRequest(BaseModel):
    """Get token balances for a wallet."""

    chain: str = Field(default=DEFAULT_CHAIN)
    tokens: list[str] = Field(description="Token symbols to query (e.g. ['ETH', 'USDC'])")
    wallet_address: str = Field(default="", description="Wallet to query. Defaults to strategy wallet.")


class BatchGetBalancesResponse(BaseModel):
    balances: list[dict] = Field(description="List of {token, balance, balance_usd} dicts")
    total_usd: str = Field(default="0", description="Sum of all balances in USD")


class GetIndicatorRequest(BaseModel):
    """Calculate a technical indicator for a token."""

    token: str = Field(description="Token symbol")
    indicator: str = Field(description="One of: rsi, sma, ema, macd, bollinger, atr")
    period: int = Field(default=14, description="Look-back period")
    chain: str = Field(default=DEFAULT_CHAIN)


class GetIndicatorResponse(BaseModel):
    indicator: str
    value: float
    signal: str | None = Field(default=None, description="Interpretation: 'overbought', 'oversold', 'neutral', etc.")
    extra: dict | None = Field(default=None, description="Indicator-specific extra fields (e.g. MACD histogram)")


class GetPoolStateRequest(BaseModel):
    """Get details about a liquidity pool."""

    token_a: str = Field(description="First token symbol")
    token_b: str = Field(description="Second token symbol")
    fee_tier: int = Field(default=3000, description="Pool fee tier in hundredths of a bip (e.g. 500, 3000, 10000)")
    chain: str = Field(default=DEFAULT_CHAIN)
    protocol: str = Field(default="uniswap_v3")
    pool_address: str = Field(default="", description="Explicit pool contract address (bypasses computed address)")


class GetPoolStateResponse(BaseModel):
    pool_address: str = ""
    current_price: str = ""
    tick: int = 0
    liquidity: str = ""
    volume_24h_usd: str = ""
    fee_apr: str = ""
    tvl_usd: str = ""


class GetLPPositionRequest(BaseModel):
    """Get details about an existing LP position."""

    position_id: str = Field(description="NFT token ID of the LP position")
    chain: str = Field(default=DEFAULT_CHAIN)
    protocol: str = Field(default="uniswap_v3")


class GetLPPositionResponse(BaseModel):
    position_id: str
    token_a: str = ""
    token_b: str = ""
    fee_tier: int = 0
    tick_lower: int = 0
    tick_upper: int = 0
    liquidity: str = ""
    tokens_owed_a: str = ""
    tokens_owed_b: str = ""
    in_range: bool = True
    current_tick: int | None = None
    fees_a_usd: float | None = None
    fees_b_usd: float | None = None
    total_fees_usd: float | None = None


class ListLPPositionsRequest(BaseModel):
    """List all LP positions owned by a wallet.

    Iterates NonfungiblePositionManager.tokenOfOwnerByIndex(owner, i) for
    i in [0, balanceOf(owner)). Returns a compact summary per position so
    operators can pick a tokenId without drilling into `get_lp_position`.
    """

    chain: str = Field(default=DEFAULT_CHAIN)
    protocol: str = Field(default="uniswap_v3", description="LP protocol (uniswap_v3 only for v1).")
    wallet_address: str = Field(default="", description="Wallet to query. Defaults to strategy wallet.")
    network: str = Field(default="", description="'mainnet' or 'anvil'; empty means gateway default.")
    include_empty: bool = Field(
        default=False,
        description="Include positions with zero liquidity (default: skip burned/fully-withdrawn positions).",
    )


class LPPositionSummary(BaseModel):
    position_id: str
    token0: str = Field(description="token0 contract address (lowercased)")
    token1: str = Field(description="token1 contract address (lowercased)")
    token0_symbol: str = ""
    token1_symbol: str = ""
    fee_tier: int = 0
    liquidity: str = ""


class ListLPPositionsResponse(BaseModel):
    chain: str
    protocol: str
    wallet_address: str
    count: int = 0
    positions: list[LPPositionSummary] = []


class ListLendingPositionsRequest(BaseModel):
    """List lending positions for a wallet on a supported protocol.

    v1: Aave V3 only. Returns account-level totals + health factor via
    Pool.getUserAccountData(user). Per-reserve drill-down is a future
    extension.
    """

    chain: str = Field(default=DEFAULT_CHAIN)
    protocol: str = Field(default="aave_v3", description="Lending protocol (aave_v3 only for v1).")
    wallet_address: str = Field(default="", description="Wallet to query. Defaults to strategy wallet.")
    network: str = Field(default="", description="'mainnet' or 'anvil'; empty means gateway default.")


class ListLendingPositionsResponse(BaseModel):
    chain: str
    protocol: str
    wallet_address: str
    total_collateral_usd: str = ""
    total_debt_usd: str = ""
    available_borrows_usd: str = ""
    current_liquidation_threshold_bps: int = 0
    ltv_bps: int = 0
    health_factor: str = Field(default="", description="1e18-scaled health factor as decimal string; '∞' when no debt.")


class ListLendingReservesRequest(BaseModel):
    """List a lending protocol's reserves with per-reserve config flags.

    Read-only discovery. Answers "which assets can I actually borrow / use as
    collateral on this market?" before configuring a strategy — so an operator
    doesn't pick a supply-only or paused reserve and only discover it at the
    borrow step of a lifecycle run (VIB-4925). Enumerates the live on-chain
    reserve set via the PoolDataProvider's ``getAllReservesTokens()`` (not a
    curated table) and reads each reserve's live ``getReserveConfigurationData``.
    """

    chain: str = Field(default=DEFAULT_CHAIN)
    protocol: str = Field(default="aave_v3", description="Lending protocol (aave_v3 / Aave-V2-fork shaped for v1).")
    asset: str = Field(default="", description="Optional single reserve symbol to filter to (e.g. 'WMATIC').")
    network: str = Field(default="", description="'mainnet' or 'anvil'; empty means gateway default.")


class LendingReserveSummary(BaseModel):
    symbol: str
    address: str = Field(description="Reserve underlying token address (lowercased)")
    borrowing_enabled: bool | None = Field(
        default=None,
        description="True/False from live reserve config; None when the per-reserve read failed (unknown).",
    )
    usage_as_collateral_enabled: bool | None = None
    is_active: bool | None = None
    is_frozen: bool | None = None
    ltv_bps: int | None = Field(default=None, description="Loan-to-value in basis points; None when unknown.")
    error: str = Field(default="", description="Per-reserve read error, when the config could not be fetched.")


class ListLendingReservesResponse(BaseModel):
    schema_version: int = 1
    chain: str
    protocol: str
    pool_data_provider: str = ""
    count: int = 0
    total_matched: int = Field(
        default=0, description="Reserves matching the query before the safety cap; > count when truncated."
    )
    truncated: bool = Field(
        default=False,
        description="True when the reserve list was capped (count < total_matched) — list is not complete.",
    )
    truncation_reason: str = Field(
        default="",
        description="Why the list was truncated: '' | 'max_reserves' | 'latency_budget_exceeded'.",
    )
    reserves: list[LendingReserveSummary] = Field(default_factory=list)


class GetPortfolioRequest(BaseModel):
    """Summarize a wallet's on-chain positions on a single chain.

    Combines native balance, ERC20 balances (via batch_get_balances for
    common symbols or a caller-supplied list), LP positions, and lending
    positions.
    """

    chain: str = Field(default=DEFAULT_CHAIN)
    wallet_address: str = Field(default="", description="Wallet to query. Defaults to strategy wallet.")
    tokens: list[str] = Field(
        default_factory=list,
        description="ERC20 symbols to include in the balance snapshot (e.g. ['USDC', 'WETH']).",
    )
    network: str = Field(default="", description="'mainnet' or 'anvil'; empty means gateway default.")


class GetPortfolioResponse(BaseModel):
    chain: str
    wallet_address: str
    native_balance: str = ""
    native_symbol: str = ""
    token_balances: list[dict] = Field(default_factory=list)
    lp_positions: list[LPPositionSummary] = Field(default_factory=list)
    lending: dict | None = Field(
        default=None,
        description="Aave V3 summary (total_collateral_usd / total_debt_usd / health_factor) or None if no position.",
    )


class ResolveTokenRequest(BaseModel):
    """Resolve a token symbol or address to full metadata."""

    token: str = Field(description="Token symbol (e.g. 'USDC') or contract address")
    chain: str = Field(default=DEFAULT_CHAIN)


class ResolveTokenResponse(BaseModel):
    symbol: str
    address: str
    decimals: int
    chain: str
    source: str = Field(default="", description="Resolution source: 'memory', 'disk', 'static', 'gateway'")


class GetRiskMetricsRequest(BaseModel):
    """Get portfolio risk metrics."""

    chain: str = Field(default=DEFAULT_CHAIN)
    window_days: int = Field(default=30, description="Look-back window for risk calculations")


class GetRiskMetricsResponse(BaseModel):
    portfolio_value_usd: str = ""
    var_95: str = Field(default="", description="Value at Risk (95% confidence) as decimal fraction")
    sharpe_ratio: str = ""
    volatility_annualized: str = ""
    max_drawdown_pct: str = Field(default="", description="Max peak-to-trough decline as decimal fraction")
    data_points: int = Field(default=0, description="Number of portfolio snapshots used for calculations")
    data_sufficient: bool = Field(default=False, description="True when enough snapshots exist for all metrics")
    warnings: list[str] = Field(default_factory=list, description="Warnings about data quality or coverage")


# =============================================================================
# PLANNING / SAFETY TOOLS
# =============================================================================


class CompileIntentRequest(BaseModel):
    """Compile a high-level intent into an executable ActionBundle."""

    intent_type: str = Field(description="Intent type: swap, lp_open, lp_close, supply, borrow, repay, etc.")
    params: dict = Field(description="Intent parameters (token_in, token_out, amount, etc.)")
    chain: str = Field(default=DEFAULT_CHAIN)


class CompileIntentResponse(BaseModel):
    bundle_id: str = Field(description="Opaque ID for the compiled bundle")
    actions: list[dict] = Field(description="List of actions in the bundle")
    gas_estimate_usd: str = ""
    warnings: list[str] = Field(default_factory=list)


class SimulateIntentRequest(BaseModel):
    """Dry-run an intent or compiled bundle without on-chain execution."""

    bundle_id: str | None = Field(default=None, description="ID of a previously compiled bundle")
    intent_type: str | None = Field(default=None, description="Or specify intent directly for ad-hoc simulation")
    params: dict | None = Field(default=None, description="Intent params if intent_type is provided")
    chain: str = Field(default=DEFAULT_CHAIN)


class SimulateIntentResponse(BaseModel):
    success: bool
    estimated_output: dict = Field(default_factory=dict, description="Expected token amounts post-execution")
    price_impact: str = ""
    gas_estimate_usd: str = ""
    revert_reason: str | None = None


class ValidateRiskRequest(BaseModel):
    """Check an intent against RiskGuard constraints."""

    intent_type: str = Field(description="Intent type to validate")
    params: dict = Field(description="Intent parameters")
    chain: str = Field(default=DEFAULT_CHAIN)


class ValidateRiskResponse(BaseModel):
    allowed: bool
    violations: list[str] = Field(default_factory=list, description="List of failed risk checks")
    suggestions: list[str] = Field(default_factory=list, description="How to fix violations")


class EstimateGasRequest(BaseModel):
    """Estimate gas cost for an intent."""

    intent_type: str = Field(description="Intent type")
    params: dict = Field(description="Intent parameters")
    chain: str = Field(default=DEFAULT_CHAIN)


class EstimateGasResponse(BaseModel):
    gas_units: int = 0
    gas_price_gwei: str = ""
    cost_usd: str = ""
    cost_native: str = ""


class ComputeRebalanceCandidateRequest(BaseModel):
    """Deterministic check: is an LP rebalance worth the gas cost?"""

    position_id: str = Field(description="Current LP position NFT token ID")
    fee_tier: int = Field(default=3000, description="Pool fee tier")
    chain: str = Field(default=DEFAULT_VAULT_CHAIN)
    estimated_daily_volume: str = Field(default="5000", description="Estimated daily pool volume in USD")
    our_liquidity_share: str = Field(default="0.1", description="Our share of pool liquidity (0-1)")


class ComputeRebalanceCandidateResponse(BaseModel):
    viable: bool = False
    reason: str = ""
    breakdown: dict = Field(default_factory=dict)


# =============================================================================
# ACTION TOOLS
# =============================================================================


class SwapTokensRequest(BaseModel):
    """Execute a token swap on a DEX. Supports cross-chain swaps via destination_chain."""

    token_in: str = Field(description="Token to sell (symbol or address)")
    token_out: str = Field(description="Token to buy (symbol or address)")
    amount: str = Field(description="Amount to swap as a decimal string (in token_in units)")
    slippage_bps: int = Field(
        default=50, ge=1, le=1000, description="Max slippage in basis points (1-1000, i.e. 0.01%-10%)"
    )
    chain: str = Field(default=DEFAULT_CHAIN)
    destination_chain: str | None = Field(
        default=None,
        description="Destination chain for cross-chain swaps (None for same-chain). Falls back to default aggregator if protocol is not specified.",
    )
    protocol: str | None = Field(default=None, description="Specific DEX; None = best available")
    dry_run: bool = Field(default=False, description="If true, simulate only -- do not execute on-chain")
    execution_wallet: str | None = Field(
        default=None, description="Override wallet for execution (e.g. Safe address for vault funds)"
    )

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive(cls, v: str) -> str:
        return _validate_positive_decimal(v, "amount")


class SwapTokensResponse(BaseModel):
    tx_hash: str | None = None
    amount_in: str = ""
    amount_out: str = ""
    effective_price: str = ""
    price_impact: str = ""
    gas_usd: str = ""


class OpenLPPositionRequest(BaseModel):
    """Open a new concentrated liquidity position."""

    token_a: str = Field(description="First token symbol or address")
    token_b: str = Field(description="Second token symbol or address")
    amount_a: str = Field(description="Amount of token_a as decimal string")
    amount_b: str = Field(description="Amount of token_b as decimal string")
    fee_tier: int = Field(default=3000, ge=100, le=100000, description="Pool fee tier (100-100000)")
    price_lower: str = Field(description="Lower price bound (required; use tick-min for full range)")
    price_upper: str = Field(description="Upper price bound (required; use tick-max for full range)")
    chain: str = Field(default=DEFAULT_CHAIN)
    protocol: str = Field(default="uniswap_v3")
    dry_run: bool = Field(default=False)
    execution_wallet: str | None = Field(
        default=None, description="Override wallet for execution (e.g. Safe address for vault funds)"
    )

    @field_validator("amount_a", "amount_b")
    @classmethod
    def amounts_must_be_positive(cls, v: str) -> str:
        return _validate_positive_decimal(v, "amount")

    @model_validator(mode="after")
    def price_lower_lt_upper(self) -> OpenLPPositionRequest:
        try:
            lower = Decimal(self.price_lower)
            upper = Decimal(self.price_upper)
            if lower >= upper:
                raise ValueError(f"price_lower ({self.price_lower}) must be less than price_upper ({self.price_upper})")
        except InvalidOperation:
            pass  # Non-numeric price strings handled elsewhere
        return self


class OpenLPPositionResponse(BaseModel):
    tx_hash: str | None = None
    position_id: str | None = None
    liquidity: str = ""
    tick_lower: int = 0
    tick_upper: int = 0
    gas_usd: str = ""


class CloseLPPositionRequest(BaseModel):
    """Close or reduce a liquidity position."""

    position_id: str = Field(description="NFT token ID of the LP position")
    amount: str = Field(default="all", description="Must be 'all' (partial close is not supported)")
    collect_fees: bool = Field(default=True, description="Collect accrued fees during close")
    chain: str = Field(default=DEFAULT_CHAIN)
    protocol: str = Field(default="uniswap_v3")
    pool: str = Field(
        default="",
        description=(
            "Optional pool hint (e.g. 'WETH/USDC/3000'). Only consulted by venues whose "
            "close path needs the pool currencies — notably Uniswap V4, where the position "
            "is keyed by a pool-id rather than a self-describing NFT. Leave empty to let the "
            "connector resolve the pool from the position id on-chain."
        ),
    )
    dry_run: bool = Field(default=False)


class CloseLPPositionResponse(BaseModel):
    tx_hash: str | None = None
    token_a_received: str = ""
    token_b_received: str = ""
    fees_collected_a: str = ""
    fees_collected_b: str = ""
    gas_usd: str = ""


class SupplyLendingRequest(BaseModel):
    """Supply tokens to a lending protocol."""

    token: str = Field(description="Token to supply")
    amount: str = Field(description="Amount as decimal string")
    protocol: str = Field(default="aave_v3", description="Lending protocol")
    use_as_collateral: bool = Field(default=True)
    market_id: str | None = Field(
        default=None,
        description="Required for isolated-market protocols (Morpho Blue); ignored for unified-pool protocols",
    )
    chain: str = Field(default=DEFAULT_CHAIN)
    dry_run: bool = Field(default=False)

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive(cls, v: str) -> str:
        return _validate_positive_decimal(v, "amount")

    @model_validator(mode="after")
    def validate_market_id(self) -> SupplyLendingRequest:
        _check_market_id(self.protocol, self.market_id, "supply")
        return self


class SupplyLendingResponse(BaseModel):
    tx_hash: str | None = None
    amount_supplied: str = ""
    gas_usd: str = ""


class BorrowLendingRequest(BaseModel):
    """Borrow tokens from a lending protocol."""

    token: str = Field(description="Token to borrow")
    amount: str = Field(description="Amount to borrow as decimal string")
    collateral_token: str = Field(description="Token to use as collateral")
    collateral_amount: str = Field(description="Amount of collateral as decimal string, or 'all'")
    protocol: str = Field(default="aave_v3")
    market_id: str | None = Field(
        default=None,
        description="Required for isolated-market protocols (Morpho Blue); ignored for unified-pool protocols",
    )
    chain: str = Field(default=DEFAULT_CHAIN)
    dry_run: bool = Field(default=False)

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive(cls, v: str) -> str:
        return _validate_positive_decimal(v, "amount")

    @field_validator("collateral_amount")
    @classmethod
    def collateral_amount_must_be_positive_or_all(cls, v: str) -> str:
        return _validate_positive_or_all(v, "collateral_amount")

    @model_validator(mode="after")
    def validate_market_id(self) -> BorrowLendingRequest:
        _check_market_id(self.protocol, self.market_id, "borrow")
        return self


class BorrowLendingResponse(BaseModel):
    tx_hash: str | None = None
    amount_borrowed: str = ""
    gas_usd: str = ""


class RepayLendingRequest(BaseModel):
    """Repay a lending position."""

    token: str = Field(description="Token to repay")
    amount: str = Field(description="Amount as decimal string, or 'all' for full repayment")
    protocol: str = Field(default="aave_v3")
    market_id: str | None = Field(
        default=None,
        description="Required for isolated-market protocols (Morpho Blue); ignored for unified-pool protocols",
    )
    chain: str = Field(default=DEFAULT_CHAIN)
    dry_run: bool = Field(default=False)

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive_or_all(cls, v: str) -> str:
        return _validate_positive_or_all(v, "amount")

    @model_validator(mode="after")
    def validate_market_id(self) -> RepayLendingRequest:
        _check_market_id(self.protocol, self.market_id, "repay")
        return self


class RepayLendingResponse(BaseModel):
    tx_hash: str | None = None
    amount_repaid: str = ""
    gas_usd: str = ""


class WithdrawLendingRequest(BaseModel):
    """Withdraw supplied tokens from a lending protocol."""

    token: str = Field(description="Token to withdraw")
    amount: str = Field(description="Amount as decimal string, or 'all' for full withdrawal")
    protocol: str = Field(default="aave_v3")
    market_id: str | None = Field(
        default=None,
        description="Required for protocols with isolated markets (e.g. Morpho Blue); ignored for Aave V3",
    )
    is_collateral: bool = Field(
        default=True,
        description="Morpho Blue only: True withdraws collateral, False withdraws loan token",
    )
    chain: str = Field(default=DEFAULT_CHAIN)
    dry_run: bool = Field(default=False)

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive_or_all(cls, v: str) -> str:
        return _validate_positive_or_all(v, "amount")

    @model_validator(mode="after")
    def validate_market_id(self) -> WithdrawLendingRequest:
        _check_market_id(self.protocol, self.market_id, "withdraw")
        return self


class WithdrawLendingResponse(BaseModel):
    tx_hash: str | None = None
    amount_withdrawn: str = ""
    gas_usd: str = ""


class BridgeTokensRequest(BaseModel):
    """Bridge tokens from one chain to another."""

    token: str = Field(description="Token to bridge (symbol or address)")
    amount: str = Field(description="Amount to bridge as a decimal string (in token units)")
    from_chain: str = Field(description="Source chain (e.g. 'base', 'arbitrum')")
    to_chain: str = Field(description="Destination chain (e.g. 'arbitrum', 'ethereum')")
    slippage_bps: int = Field(
        default=50, ge=1, le=1000, description="Max slippage in basis points (1-1000, i.e. 0.01%-10%)"
    )
    preferred_bridge: str | None = Field(
        default=None, description="Preferred bridge adapter (e.g. 'across', 'stargate')"
    )
    dry_run: bool = Field(default=False, description="If true, simulate only -- do not execute on-chain")
    execution_wallet: str | None = Field(
        default=None, description="Override wallet for execution (e.g. Safe address for vault funds)"
    )

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive(cls, v: str) -> str:
        return _validate_positive_decimal(v, "amount")

    @model_validator(mode="after")
    def chains_must_differ(self) -> BridgeTokensRequest:
        if self.from_chain.lower() == self.to_chain.lower():
            raise ValueError(f"from_chain and to_chain must be different, got '{self.from_chain}'")
        return self


class BridgeTokensResponse(BaseModel):
    tx_hash: str | None = None
    amount_bridged: str = ""
    from_chain: str = ""
    to_chain: str = ""
    bridge_used: str = ""
    estimated_arrival_seconds: int | None = None
    gas_usd: str = ""


class UnwrapNativeRequest(BaseModel):
    """Unwrap wrapped native tokens (e.g. WETH -> ETH, WMATIC -> MATIC)."""

    token: str = Field(description="Wrapped token symbol (e.g. 'WETH', 'WMATIC', 'WAVAX')")
    amount: str = Field(description="Amount to unwrap as a decimal string, or 'all'")
    chain: str = Field(default=DEFAULT_CHAIN, description="Blockchain name")
    dry_run: bool = Field(default=False, description="If true, simulate only")
    execution_wallet: str | None = Field(default=None, description="Override wallet for execution")

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive_or_all(cls, v: str) -> str:
        return _validate_positive_or_all(v, "amount")


class UnwrapNativeResponse(BaseModel):
    tx_hash: str | None = None
    amount_unwrapped: str = ""
    token: str = ""
    chain: str = ""
    gas_usd: str = ""


class ExecuteCompiledBundleRequest(BaseModel):
    """Execute a previously compiled and simulated ActionBundle."""

    bundle_id: str = Field(description="ID returned by compile_intent")
    require_simulation: bool = Field(default=True, description="Require successful simulation before execution")
    chain: str = Field(default=DEFAULT_CHAIN)
    dry_run: bool = Field(default=False)


class ExecuteCompiledBundleResponse(BaseModel):
    tx_hashes: list[str] = Field(default_factory=list)
    success: bool = False
    gas_used_usd: str = ""
    receipts: list[dict] = Field(default_factory=list)


# =============================================================================
# STATE TOOLS
# =============================================================================


class SaveAgentStateRequest(BaseModel):
    """Persist agent/strategy state."""

    state: dict = Field(description="Arbitrary JSON-serializable state to persist")
    deployment_id: str = Field(default="", description="Deployment identifier; uses default if empty")


class SaveAgentStateResponse(BaseModel):
    version: int = 0
    checksum: str = ""


class LoadAgentStateRequest(BaseModel):
    """Load previously saved state."""

    deployment_id: str = Field(default="", description="Deployment identifier; uses default if empty")


class LoadAgentStateResponse(BaseModel):
    state: dict = Field(default_factory=dict)
    version: int = 0


class RecordAgentDecisionRequest(BaseModel):
    """Record an agent decision for audit trail."""

    decision_summary: str = Field(description="What the agent decided and why")
    tool_calls: list[dict] = Field(default_factory=list, description="Tool calls made during this decision")
    intent_type: str | None = Field(default=None, description="Resulting intent type, if any")
    deployment_id: str = Field(default="")


class RecordAgentDecisionResponse(BaseModel):
    recorded: bool = True
    decision_id: str = ""


# =============================================================================
# VAULT TOOLS
# =============================================================================


class DeployVaultRequest(BaseModel):
    """Deploy a new Lagoon vault via factory contract."""

    chain: str = Field(description="Chain to deploy on (e.g. 'base', 'ethereum', 'arbitrum')")
    name: str = Field(description="Vault display name (e.g. 'Almanak DeFAI Vault')")
    symbol: str = Field(description="Vault share token symbol (e.g. 'aALM')")
    underlying_token_address: str = Field(description="Address of the vault's underlying token (e.g. USDC)")
    safe_address: str = Field(description="Pre-deployed Safe wallet address (vault owner)")
    admin_address: str = Field(description="Admin address for vault governance (usually same as safe)")
    fee_receiver_address: str = Field(description="Address to receive management/performance fees")
    deployer_address: str = Field(description="EOA address that signs the factory deploy tx")
    valuation_manager_address: str | None = Field(
        default=None,
        description="Address that can propose vault valuations (defaults to admin_address)",
    )
    dry_run: bool = Field(default=False, description="If true, simulate only")

    @field_validator(
        "underlying_token_address",
        "safe_address",
        "admin_address",
        "fee_receiver_address",
        "deployer_address",
    )
    @classmethod
    def address_must_be_valid_eth(cls, v: str) -> str:
        if not _ETH_ADDRESS_RE.match(v):
            raise ValueError(f"Invalid Ethereum address: '{v}'. Must match 0x followed by 40 hex characters.")
        return v

    @field_validator("valuation_manager_address")
    @classmethod
    def optional_address_must_be_valid_eth(cls, v: str | None) -> str | None:
        if v is not None and not _ETH_ADDRESS_RE.match(v):
            raise ValueError(f"Invalid Ethereum address: '{v}'. Must match 0x followed by 40 hex characters.")
        return v


class DeployVaultResponse(BaseModel):
    status: str = Field(description="'success', 'simulated', or 'error'")
    vault_address: str | None = Field(default=None, description="Deployed vault contract address")
    tx_hash: str | None = None
    message: str = ""


class GetVaultStateRequest(BaseModel):
    """Read current state of a Lagoon vault."""

    vault_address: str = Field(description="Vault contract address")
    chain: str = Field(default=DEFAULT_VAULT_CHAIN, description="Chain where vault is deployed")


class GetVaultStateResponse(BaseModel):
    status: str = ""
    total_assets: str = Field(default="0", description="Total assets under management (raw units)")
    pending_deposits: str = Field(default="0", description="Pending deposit amount (raw units)")
    pending_redeems: str = Field(default="0", description="Pending redemption amount (raw units)")
    share_price: str = Field(default="0", description="Current share price as decimal string")


class SettleVaultRequest(BaseModel):
    """Run a vault settlement cycle (propose + settle deposits/redeems)."""

    vault_address: str = Field(description="Vault contract address")
    chain: str = Field(default=DEFAULT_VAULT_CHAIN, description="Chain where vault is deployed")
    new_total_assets: str | None = Field(
        default=None, description="Override NAV in raw underlying units; auto-computed if omitted"
    )
    safe_address: str = Field(description="Safe wallet address (vault owner)")
    valuator_address: str = Field(description="Address authorized to propose valuations")
    dry_run: bool = Field(default=False, description="If true, simulate only")

    @field_validator("new_total_assets")
    @classmethod
    def new_total_assets_must_be_non_negative(cls, v: str | None) -> str | None:
        if v is not None:
            return _validate_non_negative_int_string(v, "new_total_assets")
        return v


class SettleVaultResponse(BaseModel):
    status: str = ""
    new_total_assets: str = Field(default="0", description="Total assets after settlement")
    epoch_id: int = Field(default=0, description="Settlement epoch number")
    tx_hash: str | None = None
    message: str = ""


class ApproveVaultUnderlyingRequest(BaseModel):
    """Approve the vault to pull underlying tokens from the Safe (for redemption settlement)."""

    vault_address: str = Field(description="Vault contract address")
    underlying_token: str = Field(description="Address of the underlying ERC20 token")
    safe_address: str = Field(description="Safe wallet address that holds the underlying tokens")
    chain: str = Field(default=DEFAULT_VAULT_CHAIN, description="Chain where vault is deployed")
    dry_run: bool = Field(default=False, description="If true, simulate only")


class ApproveVaultUnderlyingResponse(BaseModel):
    status: str = ""
    tx_hash: str | None = None
    message: str = ""


class DepositVaultRequest(BaseModel):
    """Deposit underlying tokens into a Lagoon vault (approve + requestDeposit)."""

    vault_address: str = Field(description="Vault contract address")
    underlying_token: str = Field(description="Address of the vault's underlying token (e.g. USDC address)")
    amount: str = Field(description="Amount to deposit in raw underlying units (e.g. '10000000' for 10 USDC)")
    chain: str = Field(default=DEFAULT_VAULT_CHAIN, description="Chain where vault is deployed")
    depositor_address: str = Field(default="", description="Depositor address (defaults to strategy wallet if empty)")
    dry_run: bool = Field(default=False, description="If true, simulate only")

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive_int(cls, v: str) -> str:
        v = _validate_non_negative_int_string(v, "amount")
        if int(v) == 0:
            raise ValueError("amount must be positive, got '0'")
        return v


class DepositVaultResponse(BaseModel):
    status: str = ""
    tx_hash: str | None = None
    amount_deposited: str = Field(default="0", description="Amount deposited in raw underlying units")
    message: str = ""


class TeardownVaultRequest(BaseModel):
    """Initiate a deterministic vault teardown: close positions, swap to underlying, final settle."""

    vault_address: str = Field(description="Vault contract address")
    safe_address: str = Field(description="Safe wallet address (vault owner)")
    valuator_address: str = Field(description="Address authorized to propose valuations")
    chain: str = Field(default=DEFAULT_VAULT_CHAIN, description="Chain where vault is deployed")
    dry_run: bool = Field(default=False, description="If true, simulate only")


class TeardownVaultResponse(BaseModel):
    status: str = ""
    positions_closed: int = Field(default=0, description="Number of LP positions closed")
    swaps_executed: int = Field(default=0, description="Number of token swaps to underlying")
    final_nav: str = Field(default="0", description="Final NAV after teardown")
    tx_hashes: list[str] = Field(default_factory=list, description="All transaction hashes")
    message: str = ""


# ── WRAP NATIVE ────────────────────────────────────────────────────────


class WrapNativeRequest(BaseModel):
    """Wrap native tokens to their ERC-20 equivalent (e.g. ETH -> WETH, MATIC -> WMATIC)."""

    token: str = Field(description="Wrapped token symbol to receive (e.g. 'WETH', 'WMATIC', 'WAVAX')")
    amount: str = Field(description="Amount of native token to wrap as a decimal string, or 'all'")
    chain: str = Field(default=DEFAULT_CHAIN, description="Blockchain name")
    dry_run: bool = Field(default=False, description="If true, simulate only")
    execution_wallet: str | None = Field(default=None, description="Override wallet for execution")

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive_or_all(cls, v: str) -> str:
        return _validate_positive_or_all(v, "amount")


class WrapNativeResponse(BaseModel):
    tx_hash: str | None = None
    amount_wrapped: str = ""
    token: str = ""
    chain: str = ""
    gas_usd: str = ""


# ── WALLET OVERVIEW ────────────────────────────────────────────────────


class GetWalletOverviewRequest(BaseModel):
    """Get a complete wallet balance overview in a single call.

    Automatically queries all common tokens for the chain (ETH, WETH, USDC, USDT, etc.)
    plus any additional tokens specified. Filters out dust balances below min_balance_usd.
    """

    chain: str = Field(default=DEFAULT_CHAIN, description="Blockchain name")
    wallet_address: str = Field(default="", description="Wallet to query. Defaults to strategy wallet.")
    min_balance_usd: float = Field(default=0.01, ge=0, description="Minimum USD balance to include (filters dust)")
    extra_tokens: list[str] = Field(
        default_factory=list,
        description="Additional token symbols to query beyond the default set",
    )


class GetWalletOverviewResponse(BaseModel):
    wallet_address: str = ""
    chain: str = ""
    tokens: list[dict] = Field(default_factory=list, description="List of {symbol, balance, balance_usd}")
    total_usd: str = "0"


# ── CHECK PROTOCOL SUPPORT ────────────────────────────────────────────


class CheckProtocolSupportRequest(BaseModel):
    """Check whether the SDK supports a given protocol on a given chain.

    Returns supported actions, recommended strategy template, and known issues.
    Uses static SDK registry data — no network calls needed.
    """

    protocol: str = Field(description="Protocol name (e.g. 'uniswap_v3', 'morpho_blue', 'fluid-dex')")
    chain: str = Field(default="", description="Chain to check (e.g. 'arbitrum'). If empty, returns all chains.")


class CheckProtocolSupportResponse(BaseModel):
    supported: bool = False
    protocol: str = ""
    chain: str = ""
    supported_chains: list[str] = Field(default_factory=list, description="Chains where protocol is supported")
    supported_actions: list[str] = Field(default_factory=list, description="Actions: swap, lp, lending, perps, yield")
    sdk_template: str | None = Field(default=None, description="Recommended strategy template")
    notes: str = ""
