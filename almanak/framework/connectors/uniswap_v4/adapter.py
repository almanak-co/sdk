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
    from almanak.framework.connectors.uniswap_v4.adapter import UniswapV4Adapter

    adapter = UniswapV4Adapter(chain="arbitrum")
    bundle = adapter.compile_swap_intent(intent, price_oracle)
    bundle = adapter.compile_lp_open_intent(intent, price_oracle)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.core.contracts import UNISWAP_V4
from almanak.framework.connectors.uniswap_v4.hooks import HookFlags
from almanak.framework.connectors.uniswap_v4.sdk import (
    NATIVE_CURRENCY,
    PERMIT2_ADDRESS,
    LPDecreaseParams,
    LPMintParams,
    SwapTransaction,
    UniswapV4SDK,
)
from almanak.framework.data.tokens import TokenNotFoundError

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver
    from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent, SwapIntent
    from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


# =============================================================================
# Config
# =============================================================================


@dataclass
class UniswapV4Config:
    """Configuration for UniswapV4Adapter.

    Attributes:
        chain: Chain name (e.g. "arbitrum").
        wallet_address: Wallet address for building transactions.
        rpc_url: Optional RPC URL for on-chain quotes.
        default_fee_tier: Default fee tier for swaps. Default 3000 (0.3%).
        default_slippage_bps: Default slippage in basis points. Default 50 (0.5%).
    """

    chain: str
    wallet_address: str = ""
    rpc_url: str | None = None
    default_fee_tier: int = 3000
    default_slippage_bps: int = 50


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
    ) -> None:
        if config is not None:
            self.chain = config.chain.lower()
            self.wallet_address = config.wallet_address
            self.rpc_url = config.rpc_url
            self.default_fee_tier = config.default_fee_tier
            self.default_slippage_bps = config.default_slippage_bps
        elif chain is not None:
            self.chain = chain.lower()
            self.wallet_address = ""
            self.rpc_url = None
            self.default_fee_tier = 3000
            self.default_slippage_bps = 50
        else:
            raise ValueError("Either chain or config must be provided")

        if self.chain not in UNISWAP_V4:
            raise ValueError(f"Uniswap V4 not supported on '{self.chain}'. Supported: {', '.join(UNISWAP_V4.keys())}")

        self.addresses = UNISWAP_V4[self.chain]
        self._sdk = UniswapV4SDK(chain=self.chain, rpc_url=self.rpc_url)
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

        # Get quote
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

        return ActionBundle(
            intent_type=IntentType.SWAP.value,
            transactions=[tx_to_dict(tx) for tx in result.transactions],
            metadata={
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
            },
        )

    def compile_lp_open_intent(
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
                from almanak.framework.connectors.uniswap_v4.sdk import TICK_SPACING

                tick_spacing = TICK_SPACING.get(fee, 60)
            tick_lower = (tick_lower // tick_spacing) * tick_spacing
            tick_upper = (tick_upper // tick_spacing) * tick_spacing
            if tick_lower == tick_upper:
                tick_upper += tick_spacing

            # Get sqrtPriceX96: prefer on-chain query, fall back to estimate
            sqrt_price_x96 = None
            used_onchain_price = False

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
                elif range_lower is not None and range_upper is not None:
                    mid_price = (range_lower + range_upper) / 2

                if mid_price and mid_price > 0:
                    sqrt_price_x96 = self._sdk.estimate_sqrt_price_x96(mid_price, token0_dec, token1_dec)
                    logger.info("V4 LP_OPEN: using estimated sqrtPriceX96=%d from oracle prices", sqrt_price_x96)
                else:
                    # Last resort: arithmetic mean of range sqrt ratios
                    from almanak.framework.connectors.uniswap_v4.sdk import _tick_to_sqrt_ratio_x96

                    sqrt_price_x96 = (_tick_to_sqrt_ratio_x96(tick_lower) + _tick_to_sqrt_ratio_x96(tick_upper)) // 2
                    logger.info("V4 LP_OPEN: using tick-range midpoint sqrtPriceX96=%d", sqrt_price_x96)

            liquidity = self._sdk.compute_liquidity_from_amounts(
                sqrt_price_x96, tick_lower, tick_upper, amount0_wei, amount1_wei
            )

            if liquidity <= 0:
                return ActionBundle(
                    intent_type=IntentType.LP_OPEN.value,
                    transactions=[],
                    metadata={"error": "Computed liquidity is zero — check amounts and price range"},
                )

            # Compute max amounts with slippage buffer.
            # On-chain sqrtPrice is accurate so 5% covers normal price movement.
            # Estimated sqrtPrice (oracle-based) can diverge significantly from
            # actual V4 pool state, so use 30% to avoid MaximumAmountExceeded
            # reverts from the PoolManager.
            if used_onchain_price:
                lp_default_slippage = Decimal("0.05")  # 5% for on-chain price
            else:
                lp_default_slippage = Decimal("0.30")  # 30% for estimated price
            intent_slippage = getattr(intent, "max_slippage", None)
            if intent_slippage is None:
                intent_slippage = Decimal("0.005")
            effective_slippage = max(lp_default_slippage, intent_slippage)
            if effective_slippage > intent_slippage:
                logger.warning(
                    "V4 LP_OPEN: overriding user slippage %s%% with LP minimum %s%%",
                    intent_slippage * 100,
                    lp_default_slippage * 100,
                )
            slippage_bps = int(effective_slippage * 10000)
            slippage_mult = Decimal(10000 + slippage_bps) / Decimal(10000)
            amount0_max = int(Decimal(amount0_wei) * slippage_mult)
            amount1_max = int(Decimal(amount1_wei) * slippage_mult)

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

            metadata: dict[str, Any] = {
                "intent_id": intent.intent_id,
                "token0": token0_dict,
                "token1": token1_dict,
                "amount0_desired": str(amount0_wei),
                "amount1_desired": str(amount1_wei),
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
            }
            if warnings:
                metadata["warnings"] = warnings

            return ActionBundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=[tx_to_dict(tx) for tx in transactions],
                metadata=metadata,
            )

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
            for_v4_pool: If True, remap native tokens (ETH/AVAX/MATIC) to
                V4's zero address instead of their wrapped equivalents.
                V4 pools support native ETH directly via address(0).

        Returns:
            Tuple of (address, decimals).
        """
        # Check for native token symbols — V4 supports native ETH as address(0)
        native_symbols = {"ETH", "AVAX", "MATIC", "BNB"}
        if for_v4_pool and token.upper() in native_symbols:
            return NATIVE_CURRENCY, 18

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
        from almanak.core.contracts import UNISWAP_V3_TOKENS

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
]
