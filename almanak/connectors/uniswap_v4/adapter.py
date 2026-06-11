"""Uniswap V4 Adapter — compile Swap, LP, and CollectFees intents to ActionBundles.

Follows the same pattern as UniswapV3Adapter but targets V4's
singleton PoolManager architecture via the canonical UniversalRouter (swaps)
and PositionManager with flash accounting (LP operations).

ERC-20 swap flow (3 transactions):
  1. ERC-20 approve input token to Permit2
  2. Permit2.approve(universalRouter, token, amount, expiration)
  3. UniversalRouter.execute([V4_SWAP_EXACT_IN_SINGLE], [params], deadline)

LP mint flow (5 transactions):
  1-2. ERC-20 approve token0 + token1 to Permit2
  3-4. Permit2.approve(positionManager, token0/token1, amount, expiration)
  5. PositionManager.modifyLiquidities([MINT_POSITION, SETTLE_PAIR], deadline)

LP close flow (1 transaction):
  1. PositionManager.modifyLiquidities([DECREASE_LIQUIDITY, TAKE_PAIR, BURN_POSITION], deadline)

Example:
    from almanak.connectors.uniswap_v4.adapter import UniswapV4Adapter

    adapter = UniswapV4Adapter(chain="arbitrum")
    bundle = adapter.compile_swap_intent(intent, price_oracle)
    bundle = adapter.compile_lp_open_intent(intent, price_oracle)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3_TOKENS
from almanak.connectors.uniswap_v4.hooks import HookFlags
from almanak.connectors.uniswap_v4.sdk import (
    NATIVE_CURRENCY,
    PERMIT2_ADDRESS,
    LPDecreaseParams,
    LPMintParams,
    SwapTransaction,
    UniswapV4SDK,
)
from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import native_symbols_for
from almanak.framework.data.tokens import TokenNotFoundError

from .addresses import UNISWAP_V4

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver
    from almanak.framework.gateway_client import GatewayClient
    from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent, SwapIntent
    from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


# =============================================================================
# Exceptions
# =============================================================================


class UniswapV4FailLoudError(ValueError):
    """Base for V4 compile errors that must surface to the strategy author as a
    raised exception rather than a soft-error empty ActionBundle.

    Subclasses are re-raised (not swallowed) by ``compile_lp_open_intent``'s
    trailing ``except`` block so a money-safety guard never silently degrades
    into an empty bundle. Subclasses ``ValueError`` so existing
    ``isinstance(..., ValueError)`` callers keep working (VIB-2180).
    """


class UniswapV4UnsupportedPoolError(UniswapV4FailLoudError):
    """Pool shape is outside the V0 supported surface (hookless ERC20-ERC20).

    Raised at compile time by the adapter before any transaction is built, so
    strategies fail loud on unsupported pool shapes instead of submitting
    transactions that the receipt parser / accounting layer cannot interpret.

    V0 (VIB-4426) supports only:
    - hooks == 0x0000…0000 (no hook contract attached)
    - currency0 != 0x0000…0000 (no native-ETH currency leg)

    Salt is intentionally NOT validated here: per VIB-4426 design §Q7,
    salt = bytes32(tokenId) is the canonical PositionManager._mint path, so
    a non-zero salt is the normal case and must not be rejected.
    """

    pass


class UniswapV4EstimatedPriceWithoutOptInError(UniswapV4FailLoudError):
    """LP_OPEN compiled with an estimated (non-on-chain) sqrtPrice while the user's
    max_slippage is too tight to absorb estimate divergence, and the strategy did
    not opt in via protocol_params['allow_estimated_price'].

    The adapter refuses to silently widen the user's slippage tolerance by more
    than 2x to make an estimated-price LP open succeed (VIB-2180). The strategy
    author must either widen max_slippage or opt in explicitly.
    """


# =============================================================================
# Module constants
# =============================================================================

# Slippage floor when sqrtPrice came from an on-chain StateView query — accurate,
# so 5% covers normal price movement (pre-existing behaviour, VIB-2180).
ON_CHAIN_MIN_SLIPPAGE = Decimal("0.05")
# Slippage floor when sqrtPrice is estimated (oracle / range midpoint). An estimate
# can diverge from real pool state, so a wider buffer avoids PoolManager
# MaximumAmountExceeded reverts. Was a silent 30% override; now a 10% floor gated
# by an explicit opt-in for tight-slippage users (VIB-2180).
ESTIMATED_PRICE_MIN_SLIPPAGE = Decimal("0.10")


# =============================================================================
# Config
# =============================================================================


@dataclass
class UniswapV4Config:
    """Configuration for UniswapV4Adapter.

    Attributes:
        chain: Chain name (e.g. "arbitrum").
        wallet_address: Wallet address for building transactions.
        rpc_url: Optional RPC URL for on-chain quotes (direct-HTTP fallback).
        default_fee_tier: Default fee tier for swaps. Default 3000 (0.3%).
        default_slippage_bps: Default slippage in basis points. Default 50 (0.5%).
        gateway_client: Optional GatewayClient. When provided, on-chain
            eth_call queries route through ``gateway_client.eth_call`` and
            the ``rpc_url`` fallback is never exercised.
    """

    chain: str
    wallet_address: str = ""
    rpc_url: str | None = None
    default_fee_tier: int = 3000
    default_slippage_bps: int = 50
    gateway_client: GatewayClient | None = field(default=None, repr=False, compare=False)


# =============================================================================
# Adapter
# =============================================================================


class UniswapV4Adapter:
    """Uniswap V4 swap adapter for intent compilation.

    Compiles SwapIntents into ActionBundles containing approve + swap
    transactions targeting the V4 swap router.

    Args:
        chain: Chain name.
        config: Optional UniswapV4Config. If not provided, chain is used.
        token_resolver: Optional TokenResolver for symbol -> address resolution.
    """

    def __init__(
        self,
        chain: str | None = None,
        config: UniswapV4Config | None = None,
        token_resolver: TokenResolver | None = None,
        gateway_client: GatewayClient | None = None,
    ) -> None:
        if config is not None:
            self.chain = config.chain.lower()
            self.wallet_address = config.wallet_address
            self.rpc_url = config.rpc_url
            self.default_fee_tier = config.default_fee_tier
            self.default_slippage_bps = config.default_slippage_bps
            self._gateway_client = gateway_client or config.gateway_client
        elif chain is not None:
            self.chain = chain.lower()
            self.wallet_address = ""
            self.rpc_url = None
            self.default_fee_tier = 3000
            self.default_slippage_bps = 50
            self._gateway_client = gateway_client
        else:
            raise ValueError("Either chain or config must be provided")

        if self.chain not in UNISWAP_V4:
            raise ValueError(f"Uniswap V4 not supported on '{self.chain}'. Supported: {', '.join(UNISWAP_V4.keys())}")

        self.addresses = UNISWAP_V4[self.chain]
        self._sdk = UniswapV4SDK(chain=self.chain, rpc_url=self.rpc_url, gateway_client=self._gateway_client)
        self._token_resolver = token_resolver

    def get_position_liquidity(self, token_id: int, rpc_url: str | None = None) -> int:
        """Query on-chain liquidity for a V4 LP position.

        Args:
            token_id: NFT token ID of the LP position.
            rpc_url: Optional RPC URL override.

        Returns:
            Liquidity amount (uint128). Raises ValueError if position is empty or query fails.
        """
        liquidity = self._sdk.get_position_liquidity(token_id, rpc_url=rpc_url)
        if liquidity == 0:
            raise ValueError(f"Position {token_id} has zero liquidity — already closed or invalid tokenId")
        return liquidity

    def swap_exact_input(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        slippage_bps: int | None = None,
        fee_tier: int | None = None,
        price_ratio: Decimal | None = None,
    ) -> SwapResult:
        """Build swap transactions for exact input amount.

        Args:
            token_in: Input token symbol or address.
            token_out: Output token symbol or address.
            amount_in: Input amount in human-readable units.
            slippage_bps: Slippage tolerance in bps. Default from config.
            fee_tier: Fee tier. Default from config.
            price_ratio: Price ratio (token_out per token_in) for cross-decimal quotes.

        Returns:
            SwapResult with transactions list.
        """
        slippage_bps = slippage_bps or self.default_slippage_bps
        fee_tier = fee_tier or self.default_fee_tier

        # Resolve tokens
        token_in_addr, token_in_dec = self._resolve_token(token_in)
        token_out_addr, token_out_dec = self._resolve_token(token_out)

        # Convert to smallest units
        amount_in_raw = int(amount_in * Decimal(10**token_in_dec))

        # Prefer executable pool-state quotes when gateway/RPC is configured.
        # Offline compilation still uses the local estimate for permission
        # discovery and unit tests that intentionally avoid chain I/O.
        gateway_connected = self._gateway_client is not None and getattr(self._gateway_client, "is_connected", False)
        if gateway_connected or self.rpc_url:
            try:
                quote = self._sdk.get_quote(
                    token_in=token_in_addr,
                    token_out=token_out_addr,
                    amount_in=amount_in_raw,
                    fee_tier=fee_tier,
                    token_in_decimals=token_in_dec,
                    token_out_decimals=token_out_dec,
                )
            except Exception as exc:
                logger.warning("V4 executable quote failed, falling back to local estimate: %s", exc)
                quote = self._sdk.get_quote_local(
                    token_in=token_in_addr,
                    token_out=token_out_addr,
                    amount_in=amount_in_raw,
                    fee_tier=fee_tier,
                    token_in_decimals=token_in_dec,
                    token_out_decimals=token_out_dec,
                    price_ratio=price_ratio,
                )
        else:
            quote = self._sdk.get_quote_local(
                token_in=token_in_addr,
                token_out=token_out_addr,
                amount_in=amount_in_raw,
                fee_tier=fee_tier,
                token_in_decimals=token_in_dec,
                token_out_decimals=token_out_dec,
                price_ratio=price_ratio,
            )

        # Build transactions
        transactions: list[SwapTransaction] = []

        # For ERC-20 tokens, use Permit2 flow:
        #   1. ERC-20 approve input token to Permit2
        #   2. Permit2.approve(universalRouter, token, amount, expiration)
        # Native ETH skips both (sent as msg.value)
        is_native = token_in_addr.lower() == NATIVE_CURRENCY
        if not is_native:
            # TX 1: Approve Permit2 to spend input token
            approve_tx = self._sdk.build_approve_tx(
                token_address=token_in_addr,
                spender=PERMIT2_ADDRESS,
                amount=amount_in_raw,
            )
            transactions.append(approve_tx)

            # TX 2: Grant UniversalRouter allowance via Permit2
            permit2_tx = self._sdk.build_permit2_approve_tx(
                token_address=token_in_addr,
                spender=self.addresses["universal_router"],
                amount=amount_in_raw,
            )
            transactions.append(permit2_tx)

        # Build swap tx
        if not self.wallet_address:
            raise ValueError(
                "wallet_address must be set before building swap transactions. "
                "Provide wallet_address via UniswapV4Config or set adapter.wallet_address."
            )

        # Build swap tx
        swap_tx = self._sdk.build_swap_tx(
            quote=quote,
            recipient=self.wallet_address,
            slippage_bps=slippage_bps,
        )
        transactions.append(swap_tx)

        amount_out_minimum = quote.amount_out * (10000 - slippage_bps) // 10000

        return SwapResult(
            success=True,
            transactions=transactions,
            amount_in=amount_in_raw,
            amount_out_minimum=amount_out_minimum,
            amount_out_quoted=quote.amount_out,
            gas_estimate=sum(tx.gas_estimate for tx in transactions),
        )

    def compile_swap_intent(
        self,
        intent: SwapIntent,
        price_oracle: dict[str, Decimal] | None = None,
    ) -> ActionBundle:
        """Compile a SwapIntent to an ActionBundle.

        This method integrates with the intent system to convert high-level
        swap intents into executable transaction bundles.

        Args:
            intent: The SwapIntent to compile.
            price_oracle: Optional price map for USD conversions.

        Returns:
            ActionBundle containing transactions for execution.
        """
        from almanak.framework.intents.vocabulary import IntentType
        from almanak.framework.models.reproduction_bundle import ActionBundle

        if price_oracle is None:
            price_oracle = {}

        # Determine swap amount
        if intent.amount is not None:
            if intent.amount == "all":
                raise ValueError(
                    "amount='all' must be resolved before compilation. "
                    "Use Intent.set_resolved_amount() to resolve chained amounts."
                )
            amount_in: Decimal = intent.amount  # type: ignore[assignment]
        elif intent.amount_usd is not None:
            from_price = price_oracle.get(intent.from_token.upper())
            if not from_price:
                raise ValueError(
                    f"Price unavailable for '{intent.from_token}' -- cannot convert amount_usd "
                    "to token amount. Ensure the price oracle includes this token."
                )
            amount_in = intent.amount_usd / from_price
        else:
            raise ValueError("Either amount or amount_usd must be specified")

        slippage_bps = int(intent.max_slippage * 10000)

        # Compute price ratio for cross-decimal quote accuracy
        computed_price_ratio = None
        from_price = price_oracle.get(intent.from_token.upper())
        to_price = price_oracle.get(intent.to_token.upper())
        if from_price and to_price and to_price > 0:
            computed_price_ratio = Decimal(str(from_price)) / Decimal(str(to_price))

        result = self.swap_exact_input(
            token_in=intent.from_token,
            token_out=intent.to_token,
            amount_in=amount_in,
            slippage_bps=slippage_bps,
            price_ratio=computed_price_ratio,
        )

        if not result.success:
            return ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[],
                metadata={
                    "error": result.error,
                    "intent_id": intent.intent_id,
                },
            )

        # Resolve token metadata for orchestrator compatibility
        # (orchestrator expects from_token/to_token as dicts with address, symbol, decimals, is_native)
        from_addr, from_dec = self._resolve_token(intent.from_token)
        to_addr, to_dec = self._resolve_token(intent.to_token)

        def _check_native(symbol: str) -> bool:
            """Check if token is native using token resolver.

            Uses resolve_for_swap() to match _resolve_token() behavior — ensures
            native tokens like ETH are wrapped (ETH->WETH) so is_native=False,
            preventing the orchestrator from incorrectly skipping balance checks.
            """
            if self._token_resolver:
                try:
                    resolved = self._token_resolver.resolve_for_swap(symbol, self.chain)
                    return resolved.is_native
                except Exception as e:
                    logger.debug("Could not resolve is_native for %s: %s", symbol, e)
            return False

        from_token_dict = {
            "symbol": intent.from_token,
            "address": from_addr,
            "decimals": from_dec,
            "is_native": _check_native(intent.from_token),
        }
        to_token_dict = {
            "symbol": intent.to_token,
            "address": to_addr,
            "decimals": to_dec,
            "is_native": _check_native(intent.to_token),
        }

        # VIB-3203: Human-readable pre-slippage quote so ResultEnricher can compute
        # realized slippage_bps from on-chain amounts.
        expected_output_human: str | None = None
        if result.amount_out_quoted and to_dec is not None:
            expected_output_human = str(Decimal(str(result.amount_out_quoted)) / Decimal(10**to_dec))

        metadata: dict[str, Any] = {
            "intent_id": intent.intent_id,
            "from_token": from_token_dict,
            "to_token": to_token_dict,
            "amount_in": str(result.amount_in),
            "amount_out_minimum": str(result.amount_out_minimum),
            "slippage_bps": slippage_bps,
            "chain": self.chain,
            "router": self.addresses["universal_router"],
            "pool_manager": self.addresses["pool_manager"],
            "gas_estimate": result.gas_estimate,
            "protocol_version": "v4",
        }
        if expected_output_human is not None:
            metadata["expected_output_human"] = expected_output_human

        return ActionBundle(
            intent_type=IntentType.SWAP.value,
            transactions=[tx_to_dict(tx) for tx in result.transactions],
            metadata=metadata,
        )

    # crap-allowlist: VIB-4426 — compile_lp_open_intent is the canonical V4 LP-open
    # compilation pipeline (resolve tokens, normalize pool key, validate slippage,
    # encode multicall, build calldata, simulate). T06 added 2 V0 scope guards
    # (hooks != 0, native-ETH currency0) inline at the natural validation point;
    # extracting them into a helper would not change cc materially because the
    # function's cc is dominated by the sequential pipeline. Coverage at 84% with
    # 9 inline test files; a connector-pipeline refactor is the right epic for
    # this and lives outside VIB-4426 PR-1 scope.
    def compile_lp_open_intent(  # noqa: C901
        self,
        intent: LPOpenIntent,
        price_oracle: dict[str, Decimal] | None = None,
    ) -> ActionBundle:
        """Compile an LPOpenIntent to an ActionBundle for V4 PositionManager.

        Builds transactions for:
        1-2. ERC-20 approve token0 + token1 to Permit2
        3-4. Permit2.approve(PositionManager, token0/token1)
        5. PositionManager.modifyLiquidities([MINT_POSITION, SETTLE_PAIR])

        Args:
            intent: LPOpenIntent with pool, amounts, and price range.
            price_oracle: Optional price map for liquidity estimation.

        Returns:
            ActionBundle containing LP mint transactions.
        """
        from almanak.framework.intents.vocabulary import IntentType
        from almanak.framework.models.reproduction_bundle import ActionBundle

        if not self.wallet_address:
            raise ValueError(
                "wallet_address must be set before building LP transactions. "
                "Provide wallet_address via UniswapV4Config or set adapter.wallet_address."
            )

        if price_oracle is None:
            price_oracle = {}

        warnings: list[str] = []

        try:
            # Parse pool to get token pair and fee
            token0_symbol, token1_symbol, fee = self._parse_pool(intent.pool)

            # Resolve tokens (for_v4_pool=True to use address(0) for native currency)
            token0_addr, token0_dec = self._resolve_token(token0_symbol, for_v4_pool=True)
            token1_addr, token1_dec = self._resolve_token(token1_symbol, for_v4_pool=True)

            # Ensure sorted order (V4 requirement: currency0 < currency1)
            pair_swapped = int(token0_addr, 16) > int(token1_addr, 16)
            if pair_swapped:
                token0_addr, token1_addr = token1_addr, token0_addr
                token0_dec, token1_dec = token1_dec, token0_dec
                token0_symbol, token1_symbol = token1_symbol, token0_symbol
                # Swap amounts to match sorted order
                amount0 = intent.amount1
                amount1 = intent.amount0
            else:
                amount0 = intent.amount0
                amount1 = intent.amount1

            # Convert amounts to wei
            amount0_wei = int(Decimal(str(amount0)) * Decimal(10**token0_dec))
            amount1_wei = int(Decimal(str(amount1)) * Decimal(10**token1_dec))

            # Convert price range to ticks — invert range when pair was reordered
            # If the pair was swapped, the caller's price is token1/token0 but V4 expects token0/token1
            if pair_swapped:
                range_lower = Decimal(1) / Decimal(str(intent.range_upper))
                range_upper = Decimal(1) / Decimal(str(intent.range_lower))
            else:
                range_lower = Decimal(str(intent.range_lower))
                range_upper = Decimal(str(intent.range_upper))
            tick_lower = self._sdk.price_to_tick(range_lower, token0_dec, token1_dec)
            tick_upper = self._sdk.price_to_tick(range_upper, token0_dec, token1_dec)

            # Snap ticks to tick spacing
            tick_spacing = intent.protocol_params.get("tick_spacing") if intent.protocol_params else None
            if tick_spacing is None:
                from almanak.connectors.uniswap_v4.sdk import TICK_SPACING

                tick_spacing = TICK_SPACING.get(fee, 60)
            tick_lower = (tick_lower // tick_spacing) * tick_spacing
            tick_upper = (tick_upper // tick_spacing) * tick_spacing
            if tick_lower == tick_upper:
                tick_upper += tick_spacing

            # Get sqrtPriceX96: prefer on-chain query, fall back to estimate
            sqrt_price_x96 = None
            used_onchain_price = False
            # VIB-2180: surface which source produced the sqrtPrice so strategy
            # authors can tell an LP opened on an estimate vs real pool state.
            # Exactly three labels: on_chain, oracle_estimate, range_midpoint_estimate.
            price_source = "on_chain"

            # Parse hook address early (needed for pool key in StateView query)
            hooks = NATIVE_CURRENCY  # default: no hooks
            hook_data = b""
            if intent.protocol_params:
                hooks = intent.protocol_params.get("hooks", NATIVE_CURRENCY)
                hook_data_hex = intent.protocol_params.get("hook_data", "")
                if hook_data_hex:
                    hook_data = bytes.fromhex(hook_data_hex.replace("0x", ""))

            # Hook warning: pool has hooks but hookData is empty
            if hooks != NATIVE_CURRENCY:
                hook_flags = HookFlags.from_address(hooks)
                if hook_flags.has_any_liquidity_hooks and not hook_data:
                    warnings.append(
                        f"Pool uses hooks ({hooks[:10]}...) with liquidity callbacks "
                        f"({', '.join(hook_flags.active_flags)}), but hookData is empty. "
                        "This may cause the transaction to revert if the hook requires data."
                    )

            pool_key = self._sdk.compute_pool_key(token0_addr, token1_addr, fee, tick_spacing, hooks)

            # V0 scope guard (VIB-4475): reject pool shapes outside hookless ERC20-ERC20.
            # Salt is NOT validated — per VIB-4426 §Q7, salt = bytes32(tokenId) is the
            # canonical PositionManager._mint path (see v4-periphery _mint() source) and
            # is the normal case for any minted position.
            self._reject_unsupported_v0_pool(pool_key)

            # Try on-chain query via StateView.getSlot0()
            if self.rpc_url:
                sqrt_price_x96 = self._sdk.get_pool_sqrt_price(pool_key, rpc_url=self.rpc_url)
                if sqrt_price_x96:
                    used_onchain_price = True
                    logger.info("V4 LP_OPEN: using on-chain sqrtPriceX96=%d for liquidity computation", sqrt_price_x96)

            # Fallback: estimate from oracle prices
            if sqrt_price_x96 is None:
                mid_price = None
                price0 = price_oracle.get(token0_symbol.upper())
                price1 = price_oracle.get(token1_symbol.upper())
                if price0 and price1 and price1 > 0:
                    mid_price = Decimal(str(price0)) / Decimal(str(price1))
                    price_source = "oracle_estimate"
                elif range_lower is not None and range_upper is not None:
                    mid_price = (range_lower + range_upper) / 2
                    price_source = "range_midpoint_estimate"

                if mid_price and mid_price > 0:
                    sqrt_price_x96 = self._sdk.estimate_sqrt_price_x96(mid_price, token0_dec, token1_dec)
                    logger.info("V4 LP_OPEN: using estimated sqrtPriceX96=%d from oracle prices", sqrt_price_x96)
                else:
                    # Last resort: arithmetic mean of range sqrt ratios — a range
                    # midpoint in tick space, so labelled range_midpoint_estimate.
                    from almanak.connectors.uniswap_v4.sdk import _tick_to_sqrt_ratio_x96

                    sqrt_price_x96 = (_tick_to_sqrt_ratio_x96(tick_lower) + _tick_to_sqrt_ratio_x96(tick_upper)) // 2
                    price_source = "range_midpoint_estimate"
                    logger.info("V4 LP_OPEN: using tick-range midpoint sqrtPriceX96=%d", sqrt_price_x96)

            # Preserve the intent's requested amount as the hard spend cap.
            # When price estimates are uncertain, reduce liquidity instead of
            # raising amount*_max above what the user asked to spend.
            #
            # VIB-2180: an on-chain sqrtPrice keeps the pre-existing 5% floor. An
            # estimated sqrtPrice needs a wider buffer, but rather than silently
            # bumping the user to a fixed 30% we refuse to widen their tolerance by
            # more than 2x unless they explicitly opt in.
            user_slippage = getattr(intent, "max_slippage", None)
            if user_slippage is None:
                user_slippage = Decimal("0.005")

            if used_onchain_price:
                # On-chain sqrtPrice is accurate; keep the pre-existing 5% floor.
                effective_slippage = max(user_slippage, ON_CHAIN_MIN_SLIPPAGE)
            else:
                # Estimated sqrtPrice needs a wider buffer to avoid PoolManager
                # MaximumAmountExceeded reverts. Refuse to silently widen the user's
                # tolerance by more than 2x unless they explicitly opt in.
                allow_estimated = (intent.protocol_params or {}).get("allow_estimated_price") is True
                if not allow_estimated and user_slippage * 2 < ESTIMATED_PRICE_MIN_SLIPPAGE:
                    raise UniswapV4EstimatedPriceWithoutOptInError(
                        f"V4 LP_OPEN: on-chain sqrtPrice unavailable (pool may be uninitialised "
                        f"or StateView reverted), so the price is estimated ({price_source}). "
                        f"Estimated-price fallback requires max_slippage >= "
                        f"{ESTIMATED_PRICE_MIN_SLIPPAGE * 100:.0f}% to avoid PoolManager reverts; "
                        f"your max_slippage={user_slippage * 100:.2f}% is too tight. To proceed, set "
                        f"intent.protocol_params['allow_estimated_price'] = True. (VIB-2180)"
                    )
                effective_slippage = max(user_slippage, ESTIMATED_PRICE_MIN_SLIPPAGE)

            if effective_slippage > user_slippage:
                logger.warning(
                    "V4 LP_OPEN: widening user slippage %s%% to %s%% (price_source=%s)",
                    user_slippage * 100,
                    effective_slippage * 100,
                    price_source,
                )
            slippage_bps = int(effective_slippage * 10000)
            slippage_mult = Decimal(10000 + slippage_bps) / Decimal(10000)
            liquidity_amount0 = int(Decimal(amount0_wei) / slippage_mult)
            liquidity_amount1 = int(Decimal(amount1_wei) / slippage_mult)
            amount0_max = amount0_wei
            amount1_max = amount1_wei

            liquidity = self._sdk.compute_liquidity_from_amounts(
                sqrt_price_x96, tick_lower, tick_upper, liquidity_amount0, liquidity_amount1
            )

            if liquidity <= 0:
                return ActionBundle(
                    intent_type=IntentType.LP_OPEN.value,
                    transactions=[],
                    metadata={"error": "Computed liquidity is zero — check amounts and price range"},
                )

            mint_params = LPMintParams(
                pool_key=pool_key,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                liquidity=liquidity,
                amount0_max=amount0_max,
                amount1_max=amount1_max,
                owner=self.wallet_address,
                hook_data=hook_data,
            )

            # Build transactions
            transactions: list[SwapTransaction] = []
            position_manager = self.addresses["position_manager"]

            # Approvals for both tokens via Permit2
            for token_addr, amount_max in [(token0_addr, amount0_max), (token1_addr, amount1_max)]:
                if token_addr.lower() == NATIVE_CURRENCY:
                    continue
                transactions.append(self._sdk.build_approve_tx(token_addr, PERMIT2_ADDRESS, amount_max))
                transactions.append(self._sdk.build_permit2_approve_tx(token_addr, position_manager, amount_max))

            # Mint position TX
            mint_tx = self._sdk.build_mint_position_tx(mint_params)
            transactions.append(mint_tx)

            # Build token metadata dicts
            token0_dict = {"symbol": token0_symbol, "address": token0_addr, "decimals": token0_dec}
            token1_dict = {"symbol": token1_symbol, "address": token1_addr, "decimals": token1_dec}

            # VIB-4636 — derive a compile-time current tick from the
            # sqrtPriceX96 the adapter just sized liquidity against. The V4
            # mint itself never moves price, so this tick is correct for
            # post-mint accounting as long as no other tx interleaves. The
            # receipt parser leaves ``lp_open_data.current_tick`` None for
            # pure-mint receipts (no in-receipt Swap event to read tick
            # from); the enricher uses this metadata key as the fallback so
            # the persisted ``accounting_events`` payload carries a real
            # ``current_tick`` / ``in_range`` instead of NULL. Source-of-
            # truth note: when ``used_onchain_price`` is True this is the
            # actual on-chain tick at compile time; when False it is the
            # oracle-derived estimate and inherits the same accuracy
            # caveat the slippage cap is widened for.
            from almanak.connectors.uniswap_v4.sdk import sqrt_ratio_x96_to_tick

            compile_time_current_tick = sqrt_ratio_x96_to_tick(sqrt_price_x96)

            metadata: dict[str, Any] = {
                "intent_id": intent.intent_id,
                "token0": token0_dict,
                "token1": token1_dict,
                "amount0_desired": str(amount0_wei),
                "amount1_desired": str(amount1_wei),
                "amount0_liquidity_budget": str(liquidity_amount0),
                "amount1_liquidity_budget": str(liquidity_amount1),
                "tick_lower": tick_lower,
                "tick_upper": tick_upper,
                "liquidity": str(liquidity),
                "fee": fee,
                "chain": self.chain,
                "position_manager": position_manager,
                "pool_manager": self.addresses["pool_manager"],
                "hooks": hooks,
                "gas_estimate": sum(tx.gas_estimate for tx in transactions),
                "protocol_version": "v4",
                "effective_slippage_bps": slippage_bps,
                # VIB-2180: surface the sqrtPrice provenance to the strategy author.
                "price_source": price_source,
                "estimated_sqrt_price_x96": (str(sqrt_price_x96) if price_source != "on_chain" else None),
                "compile_time_current_tick": compile_time_current_tick,
                "compile_time_current_tick_source": "onchain" if used_onchain_price else "estimated",
            }
            if warnings:
                metadata["warnings"] = warnings

            return ActionBundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=[tx_to_dict(tx) for tx in transactions],
                metadata=metadata,
            )

        except UniswapV4FailLoudError:
            # VIB-4475: V0 scope violations and VIB-2180: estimated-price-without-opt-in
            # are fail-loud money-safety guards, not soft-error bundles. The strategy
            # author needs to see an exception, not a silent empty bundle.
            raise
        except Exception as e:
            logger.error("V4 LP_OPEN compilation failed: %s", e)
            return ActionBundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=[],
                metadata={"error": str(e), "intent_id": intent.intent_id},
            )

    def compile_lp_close_intent(
        self,
        intent: LPCloseIntent,
        liquidity: int = 0,
        currency0: str = "",
        currency1: str = "",
    ) -> ActionBundle:
        """Compile an LPCloseIntent to an ActionBundle for V4 PositionManager.

        Builds a single transaction:
        PositionManager.modifyLiquidities([DECREASE_LIQUIDITY, TAKE_PAIR, BURN_POSITION])

        Args:
            intent: LPCloseIntent with position_id.
            liquidity: Total liquidity to withdraw (must be provided by caller,
                typically from on-chain position query).
            currency0: Token0 address (sorted). Required for TAKE_PAIR.
            currency1: Token1 address (sorted). Required for TAKE_PAIR.

        Returns:
            ActionBundle containing LP close transactions.
        """
        from almanak.framework.intents.vocabulary import IntentType
        from almanak.framework.models.reproduction_bundle import ActionBundle

        if not self.wallet_address:
            raise ValueError("wallet_address must be set before building LP close transactions.")

        # V0 scope guard (VIB-4475): reject native-ETH currency leg before building tx.
        # Hooks are not visible at close (the position is identified by token_id alone),
        # so only the currency check is enforceable here; the hook guard lives in
        # compile_lp_open_intent. Salt is intentionally not validated — per VIB-4426
        # §Q7, salt = bytes32(tokenId) is the canonical PositionManager._mint path.
        if currency0 and currency0.lower() == NATIVE_CURRENCY:
            raise UniswapV4UnsupportedPoolError(
                f"Uniswap V4 LP close has currency0={currency0} (native ETH) but native-ETH legs "
                "are not in V0 scope. V0 (VIB-4426) supports only ERC20-ERC20 pools. "
                "Native-ETH currency support is tracked by VIB-4483 (P-V1-B)."
            )

        try:
            token_id = int(intent.position_id)
        except (ValueError, TypeError):
            from almanak.framework.intents.vocabulary import IntentType
            from almanak.framework.models.reproduction_bundle import ActionBundle

            return ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[],
                metadata={"error": f"Invalid position ID: {intent.position_id}"},
            )

        # Parse hook data and slippage minimums from protocol_params
        hook_data = b""
        amount0_min = 0
        amount1_min = 0
        protocol_params = getattr(intent, "protocol_params", None) or {}
        if protocol_params:
            hook_data_hex = protocol_params.get("hook_data", "")
            if hook_data_hex:
                hook_data = bytes.fromhex(hook_data_hex.replace("0x", ""))
            amount0_min = int(protocol_params.get("amount0_min", 0))
            amount1_min = int(protocol_params.get("amount1_min", 0))

        decrease_params = LPDecreaseParams(
            token_id=token_id,
            liquidity=liquidity,
            amount0_min=amount0_min,
            amount1_min=amount1_min,
            hook_data=hook_data,
        )

        # burn=False: withdraw liquidity + collect fees without burning the NFT.
        # The BURN_POSITION action encoding has a calldata boundary issue
        # (SliceOutOfBounds) when combined with DECREASE_LIQUIDITY + TAKE_PAIR.
        # The position NFT remains with 0 liquidity, which is harmless.
        close_tx = self._sdk.build_decrease_liquidity_tx(
            params=decrease_params,
            currency0=currency0,
            currency1=currency1,
            recipient=self.wallet_address,
            burn=False,
        )

        position_manager = self.addresses["position_manager"]

        return ActionBundle(
            intent_type=IntentType.LP_CLOSE.value,
            transactions=[tx_to_dict(close_tx)],
            metadata={
                "intent_id": intent.intent_id,
                "position_id": str(token_id),
                "liquidity_removed": str(liquidity),
                "chain": self.chain,
                "position_manager": position_manager,
                "pool_manager": self.addresses["pool_manager"],
                "gas_estimate": close_tx.gas_estimate,
                "protocol_version": "v4",
                "warnings": (
                    [
                        "amount0_min and amount1_min are set to 0 (no slippage protection on withdrawal). "
                        "Provide 'amount0_min' and 'amount1_min' via protocol_params for MEV protection."
                    ]
                    if amount0_min == 0 and amount1_min == 0
                    else []
                ),
            },
        )

    def compile_collect_fees_intent(
        self,
        position_id: int,
        currency0: str,
        currency1: str,
        hook_data: bytes = b"",
    ) -> ActionBundle:
        """Compile a collect-fees operation for a V4 LP position.

        Args:
            position_id: NFT token ID.
            currency0: Token0 address (sorted).
            currency1: Token1 address (sorted).
            hook_data: Optional hook data for hooked pools.

        Returns:
            ActionBundle containing fee collection transaction.
        """
        from almanak.framework.intents.vocabulary import IntentType
        from almanak.framework.models.reproduction_bundle import ActionBundle

        if not self.wallet_address:
            raise ValueError("wallet_address must be set before building collect fees transactions.")

        collect_tx = self._sdk.build_collect_fees_tx(
            token_id=position_id,
            currency0=currency0,
            currency1=currency1,
            recipient=self.wallet_address,
            hook_data=hook_data,
        )

        return ActionBundle(
            intent_type=IntentType.LP_COLLECT_FEES.value,
            transactions=[tx_to_dict(collect_tx)],
            metadata={
                "position_id": str(position_id),
                "chain": self.chain,
                "position_manager": self.addresses["position_manager"],
                "gas_estimate": collect_tx.gas_estimate,
                "protocol_version": "v4",
            },
        )

    @staticmethod
    def _reject_unsupported_v0_pool(pool_key: Any) -> None:
        """Fail-loud guard for VIB-4426 V0 pool shapes (hookless ERC20-ERC20).

        Rejects:
        - hooks != 0x0000…0000 — VIB-4485 (P-V1-D) will lift this.
        - currency0 == 0x0000…0000 (native-ETH leg) — VIB-4483 (P-V1-B) will lift this.

        Does NOT validate salt: per VIB-4426 §Q7, salt = bytes32(tokenId) is the
        canonical PositionManager._mint path and is always non-zero for a minted
        position. Rejecting non-zero salt would break every real LP open.
        """
        hooks_norm = pool_key.hooks.lower() if isinstance(pool_key.hooks, str) else pool_key.hooks
        currency0_norm = pool_key.currency0.lower() if isinstance(pool_key.currency0, str) else pool_key.currency0
        if hooks_norm != NATIVE_CURRENCY:
            raise UniswapV4UnsupportedPoolError(
                f"Uniswap V4 pool has hooks={pool_key.hooks} but hook support is not in V0 scope. "
                "V0 (VIB-4426) supports only hookless ERC20-ERC20 pools. "
                "Hook support is tracked by VIB-4485 (P-V1-D)."
            )
        if currency0_norm == NATIVE_CURRENCY:
            raise UniswapV4UnsupportedPoolError(
                f"Uniswap V4 pool has currency0={pool_key.currency0} (native ETH) but native-ETH legs "
                "are not in V0 scope. V0 (VIB-4426) supports only ERC20-ERC20 pools. "
                "Native-ETH currency support is tracked by VIB-4483 (P-V1-B)."
            )

    @staticmethod
    def _parse_pool(pool: str) -> tuple[str, str, int]:
        """Parse pool string into (token0_symbol, token1_symbol, fee).

        Expected format: "TOKEN0/TOKEN1/FEE" (e.g. "WETH/USDC/3000")
        """
        parts = pool.split("/")
        if len(parts) != 3:
            raise ValueError(f"Invalid pool format: '{pool}'. Expected 'TOKEN0/TOKEN1/FEE' (e.g. 'WETH/USDC/3000')")
        return parts[0], parts[1], int(parts[2])

    def _resolve_token(self, token: str, for_v4_pool: bool = False) -> tuple[str, int]:
        """Resolve token symbol to (address, decimals).

        Args:
            token: Token symbol (e.g. "USDC") or address.
            for_v4_pool: If True, remap the CURRENT chain's native symbols to
                V4's zero address instead of their wrapped equivalents.
                V4 pools support the native currency directly via address(0).

        Returns:
            Tuple of (address, decimals).
        """
        # Check for native token symbols — V4 supports the native currency as
        # address(0). Per-chain set derived from ``ChainDescriptor.native``
        # (VIB-4851 A1); the legacy chain-blind {ETH, AVAX, MATIC, BNB} remapped
        # e.g. "MATIC" on ethereum — a real ERC-20 there — to address(0).
        if for_v4_pool and token.upper() in native_symbols_for(self.chain):
            # Non-empty symbol set implies the chain is registered.
            return NATIVE_CURRENCY, ChainRegistry.resolve(self.chain).native.decimals

        # If already an address, resolve decimals (never assume 18)
        if token.startswith("0x") and len(token) == 42:
            if self._token_resolver:
                resolved = self._token_resolver.resolve(token, self.chain)
                return resolved.address, resolved.decimals
            raise TokenNotFoundError(
                token=token,
                chain=self.chain,
                reason="Cannot resolve decimals without a token_resolver",
                suggestions=["Provide a token_resolver or use token symbols instead"],
            )

        # Resolve by symbol
        if self._token_resolver:
            resolved = self._token_resolver.resolve_for_swap(token, self.chain)
            return resolved.address, resolved.decimals

        # Fallback: use UNISWAP_V3_TOKENS registry for address
        chain_tokens = UNISWAP_V3_TOKENS.get(self.chain, {})
        address = chain_tokens.get(token.upper())
        if address:
            # Known decimals only — never assume 18
            decimals_map = {
                "USDC": 6,
                "USDT": 6,
                "USDC.e": 6,
                "USDT.e": 6,
                "WBTC": 8,
                "WETH": 18,
                "ETH": 18,
                "DAI": 18,
                "LINK": 18,
                "UNI": 18,
                "WAVAX": 18,
                "AVAX": 18,
                "WMATIC": 18,
                "WBNB": 18,
            }
            decimals = decimals_map.get(token.upper())
            if decimals is None:
                raise TokenNotFoundError(
                    token=token,
                    chain=self.chain,
                    reason="Token address found but decimals unknown",
                    suggestions=["Provide a token_resolver for reliable decimal resolution"],
                )
            return address, decimals

        raise TokenNotFoundError(
            token=token,
            chain=self.chain,
            reason="Token not in static registry",
        )


# =============================================================================
# Result types
# =============================================================================


@dataclass
class SwapResult:
    """Result of building swap transactions."""

    success: bool
    transactions: list[SwapTransaction]
    amount_in: int = 0
    amount_out_minimum: int = 0
    # VIB-3203: pre-slippage-discount expected output amount (raw units).
    # Used by IntentCompiler to persist `expected_output_human` in ActionBundle.metadata
    # so ResultEnricher can compute realized slippage_bps.
    amount_out_quoted: int = 0
    gas_estimate: int = 0
    error: str | None = None


def tx_to_dict(tx: SwapTransaction) -> dict[str, Any]:
    """Convert SwapTransaction to dict for ActionBundle."""
    return {
        "to": tx.to,
        "value": str(tx.value),
        "data": tx.data,
        "gas_estimate": tx.gas_estimate,
        "description": tx.description,
    }


__all__ = [
    "SwapResult",
    "UniswapV4Adapter",
    "UniswapV4Config",
    "UniswapV4EstimatedPriceWithoutOptInError",
    "UniswapV4FailLoudError",
    "UniswapV4UnsupportedPoolError",
]
