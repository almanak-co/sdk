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

from almanak.connectors._strategy_base.slippage import (
    SlippagePrecisionError,
    compute_min_amount_out_from_bps,
    slippage_to_bps,
)
from almanak.connectors.uniswap_v4.hooks import HookFlags, compute_pool_id
from almanak.connectors.uniswap_v4.sdk import (
    NATIVE_CURRENCY,
    PERMIT2_ADDRESS,
    LPDecreaseParams,
    LPMintParams,
    PoolKey,
    SwapQuote,
    SwapTransaction,
    UniswapV4SDK,
)
from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import native_symbols_for
from almanak.framework.data.tokens import TokenNotFoundError, build_swap_token_meta, get_token_resolver
from almanak.framework.intents._compiler_helpers import deadline_from_now

from .addresses import UNISWAP_V4

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver
    from almanak.framework.gateway_client import GatewayClient
    from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent, SwapIntent
    from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


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


# On-chain sqrtPrice is accurate; a 5% floor covers normal price movement.
ON_CHAIN_MIN_SLIPPAGE = Decimal("0.05")
# Estimated sqrtPrice can diverge from pool state, so a wider buffer avoids
# PoolManager MaximumAmountExceeded reverts; tight-slippage users opt in explicitly.
ESTIMATED_PRICE_MIN_SLIPPAGE = Decimal("0.10")


@dataclass(frozen=True)
class _LPOpenPool:
    token0_symbol: str
    token1_symbol: str
    token0_addr: str
    token1_addr: str
    token0_dec: int
    token1_dec: int
    amount0_wei: int
    amount1_wei: int
    range_lower: Decimal
    range_upper: Decimal
    tick_lower: int
    tick_upper: int
    fee: int
    hooks: str
    hook_data: bytes
    pool_key: PoolKey
    pool_id: str
    warnings: list[str]


@dataclass(frozen=True)
class _LPOpenPrice:
    sqrt_price_x96: int
    used_onchain_price: bool
    source: str


@dataclass(frozen=True)
class _LPOpenLiquidity:
    amount0_budget: int
    amount1_budget: int
    amount0_max: int
    amount1_max: int
    liquidity: int
    slippage_bps: int


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
        managed_fork: Tri-state managed-fork declaration threaded from the
            compiler context (ALM-3184). ``None`` means undeclared, in which
            case the price-impact guard probes the node rather than trusting
            the shape of ``rpc_url``.
        default_deadline_seconds: Transaction deadline in seconds.
    """

    chain: str
    wallet_address: str = ""
    rpc_url: str | None = None
    default_fee_tier: int = 3000
    default_slippage_bps: int = 50
    gateway_client: GatewayClient | None = field(default=None, repr=False, compare=False)
    # New fields go last: field order is the positional constructor order.
    managed_fork: bool | None = None
    default_deadline_seconds: int = 300

    def __post_init__(self) -> None:
        deadline_from_now(self.default_deadline_seconds, now_ts=0)


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
            self.managed_fork = config.managed_fork
            self.default_deadline_seconds = config.default_deadline_seconds
            self._gateway_client = gateway_client or config.gateway_client
        elif chain is not None:
            self.chain = chain.lower()
            self.wallet_address = ""
            self.rpc_url = None
            self.default_fee_tier = 3000
            self.default_slippage_bps = 50
            self.managed_fork = None
            self.default_deadline_seconds = 300
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

    def get_position_currencies(self, token_id: int, rpc_url: str | None = None) -> tuple[str, str]:
        """Resolve a V4 position's ``(currency0, currency1)`` from its NFT id, on-chain.

        Reads the position's ``PoolKey`` via ``PositionManager.getPoolAndPositionInfo``
        and returns the two currency addresses in canonical sorted order. Lets a V4
        position be closed from its id alone when the open-time currencies are not
        otherwise available (VIB-5361 operator recovery / ``ax lp-close``).

        Args:
            token_id: NFT token ID of the LP position.
            rpc_url: Optional RPC URL override.

        Returns:
            ``(currency0, currency1)`` lowercased EVM addresses, currency0 < currency1.
        """
        pool_key = self._sdk.get_position_pool_key(token_id, rpc_url=rpc_url)
        return pool_key.currency0, pool_key.currency1

    def swap_exact_input(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        slippage_bps: int | None = None,
        fee_tier: int | None = None,
        price_ratio: Decimal | None = None,
        *,
        max_price_impact: Decimal | None = None,
        config_max_price_impact: Decimal | None = None,
        offline_mode: bool = False,
        using_placeholders: bool = False,
    ) -> SwapResult:
        """Build swap transactions for exact input amount.

        Args:
            token_in: Input token symbol or address.
            token_out: Output token symbol or address.
            amount_in: Input amount in human-readable units.
            slippage_bps: Slippage tolerance in bps. Default from config.
            fee_tier: Fee tier. Default from config.
            price_ratio: Price ratio (token_out per token_in) for cross-decimal quotes.
            max_price_impact: Per-intent price-impact ceiling (``intent.max_price_impact``)
                or None to fall back to ``config_max_price_impact`` (VIB-2058).
            config_max_price_impact: Compiler-config price-impact default
                (``ctx.max_price_impact_pct``). Defaults to 5% when unset.
            offline_mode: Permission-discovery / placeholder compile. When True an
                executable-quote failure degrades to the local estimate instead of
                failing closed (the swap is never broadcast in this mode).
            using_placeholders: Whether the price oracle holds placeholder prices —
                relaxes the price-impact guard (oracle estimate is not real).

        Returns:
            SwapResult with transactions list. ``success=False`` (no transactions)
            when the executable quote is unavailable online (fail-closed, C1), the
            executable quote returns zero output (C1), or the price impact exceeds
            tolerance (C2) — see VIB-2058 (https://linear.app/almanak/issue/VIB-2058).

        Raises:
            ValueError: for *permanent* compilation failures that should halt the
                strategy rather than retry — an unresolvable token, a missing
                ``wallet_address``, or the VIB-3875 cross-decimal guard
                (``get_quote_local`` with no ``price_ratio`` and mismatched token
                decimals). These deliberately propagate (not wrapped in a retryable
                ``SwapResult``) and are caught + classified COMPILATION_PERMANENT at
                the compiler boundary (``UniswapV4Compiler.compile_swap``). The
                ``success=False`` returns above are the *retryable* failures.
        """
        slippage_bps = self.default_slippage_bps if slippage_bps is None else slippage_bps
        fee_tier = fee_tier or self.default_fee_tier

        token_in_addr, token_in_dec = self._resolve_token(token_in)
        token_out_addr, token_out_dec = self._resolve_token(token_out)

        # Human-readable amount to smallest units.
        amount_in_raw = int(amount_in * Decimal(10**token_in_dec))

        # Never back a real minOut with a theoretical estimate; offline
        # discovery may use the local estimate because nothing is broadcast.
        quote, quote_source = self._quote_for_swap(
            token_in_addr=token_in_addr,
            token_out_addr=token_out_addr,
            amount_in_raw=amount_in_raw,
            fee_tier=fee_tier,
            token_in_dec=token_in_dec,
            token_out_dec=token_out_dec,
            price_ratio=price_ratio,
            offline_mode=offline_mode,
        )
        if quote is None:
            return SwapResult(
                success=False,
                transactions=[],
                error=(
                    f"On-chain V4 quote unavailable for {token_in} -> {token_out} at fee "
                    f"tier {fee_tier}. Refusing to compile a swap backed only by a "
                    f"theoretical estimate (would risk a silent no-op). Check gateway/RPC "
                    f"availability and that the pool is initialized with liquidity."
                ),
            )

        # A zero-output on-chain quote is degenerate (e.g. uninitialized pool);
        # fail closed rather than building a swap with zero minimum output.
        if quote_source == "onchain_quoter" and quote.amount_out <= 0:
            return SwapResult(
                success=False,
                transactions=[],
                error=(
                    f"On-chain V4 quote for {token_in} -> {token_out} at fee tier "
                    f"{fee_tier} returned zero output (degenerate / uninitialized pool). "
                    f"Refusing to compile a swap with a zero minimum-output (silent no-op risk)."
                ),
            )

        # Price-impact guard is only meaningful against an executable quote
        # plus a real oracle estimate.
        guard_failure = self._check_swap_price_impact(
            quote_source=quote_source,
            quoter_amount=quote.amount_out,
            amount_in=amount_in,
            token_out_dec=token_out_dec,
            price_ratio=price_ratio,
            max_price_impact=max_price_impact,
            config_max_price_impact=config_max_price_impact,
            using_placeholders=using_placeholders,
            token_in=token_in,
            token_out=token_out,
        )
        if guard_failure is not None:
            return guard_failure

        transactions: list[SwapTransaction] = []

        # ERC-20 path needs approve then Permit2 grant; native ETH skips both as msg.value.
        is_native = token_in_addr.lower() == NATIVE_CURRENCY
        if not is_native:
            approve_tx = self._sdk.build_approve_tx(
                token_address=token_in_addr,
                spender=PERMIT2_ADDRESS,
                amount=amount_in_raw,
            )
            transactions.append(approve_tx)

            permit2_tx = self._sdk.build_permit2_approve_tx(
                token_address=token_in_addr,
                spender=self.addresses["universal_router"],
                amount=amount_in_raw,
            )
            transactions.append(permit2_tx)

        if not self.wallet_address:
            raise ValueError(
                "wallet_address must be set before building swap transactions. "
                "Provide wallet_address via UniswapV4Config or set adapter.wallet_address."
            )

        swap_tx = self._sdk.build_swap_tx(
            quote=quote,
            recipient=self.wallet_address,
            slippage_bps=slippage_bps,
            deadline=deadline_from_now(self.default_deadline_seconds),
        )
        transactions.append(swap_tx)

        amount_out_minimum = compute_min_amount_out_from_bps(quote.amount_out, slippage_bps)

        return SwapResult(
            success=True,
            transactions=transactions,
            amount_in=amount_in_raw,
            amount_out_minimum=amount_out_minimum,
            amount_out_quoted=quote.amount_out,
            gas_estimate=sum(tx.gas_estimate for tx in transactions),
            quote_source=quote_source,
        )

    def _quote_for_swap(
        self,
        *,
        token_in_addr: str,
        token_out_addr: str,
        amount_in_raw: int,
        fee_tier: int,
        token_in_dec: int,
        token_out_dec: int,
        price_ratio: Decimal | None,
        offline_mode: bool,
    ) -> tuple[SwapQuote | None, str]:
        """Select the quote backing ``amount_out_minimum`` (VIB-2058 C1/C3).

        Mirrors the V3 swap path's executable-or-fail-closed contract
        (``uniswap_v3/compiler.py`` quoter selection): an executable on-chain quote
        when connected, the oracle-derived local estimate only when genuinely
        offline or in a non-broadcasting compile.

        Returns ``(quote, quote_source)``. A ``None`` quote signals fail-closed —
        the caller refuses to compile rather than fabricate a money-path minOut.
        """
        gateway_connected = self._gateway_client is not None and getattr(self._gateway_client, "is_connected", False)
        connected = gateway_connected or bool(self.rpc_url)

        local_kwargs: dict[str, Any] = {
            "token_in": token_in_addr,
            "token_out": token_out_addr,
            "amount_in": amount_in_raw,
            "fee_tier": fee_tier,
            "token_in_decimals": token_in_dec,
            "token_out_decimals": token_out_dec,
            "price_ratio": price_ratio,
        }

        if not connected:
            # Offline with no gateway/RPC: the oracle-derived local estimate is the fallback.
            return self._sdk.get_quote_local(**local_kwargs), "local_estimate"

        try:
            quote = self._sdk.get_quote(
                token_in=token_in_addr,
                token_out=token_out_addr,
                amount_in=amount_in_raw,
                fee_tier=fee_tier,
                token_in_decimals=token_in_dec,
                token_out_decimals=token_out_dec,
            )
            return quote, "onchain_quoter"
        except Exception as exc:
            if offline_mode:
                # Non-broadcasting compiles degrade to the local estimate.
                logger.warning(
                    "V4 executable quote failed in offline-mode compile; using local estimate: %s",
                    exc,
                )
                return self._sdk.get_quote_local(**local_kwargs), "local_estimate"
            # Connected quote failure fails closed; never substitute a theoretical estimate.
            logger.warning(
                "V4 executable quote failed (gateway/RPC connected, online compile); failing closed: %s",
                exc,
            )
            return None, "unavailable"

    def _check_swap_price_impact(
        self,
        *,
        quote_source: str,
        quoter_amount: int,
        amount_in: Decimal,
        token_out_dec: int,
        price_ratio: Decimal | None,
        max_price_impact: Decimal | None,
        config_max_price_impact: Decimal | None,
        using_placeholders: bool,
        token_in: str,
        token_out: str,
    ) -> SwapResult | None:
        """Liquidity / price-impact guard (VIB-2058 C2), parity with V3.

        Reuses the framework's protocol-agnostic ``check_price_impact`` helper.
        Returns a failed ``SwapResult`` when impact exceeds tolerance (pool likely
        illiquid → would silently no-op), else ``None`` (proceed).

        Only runs against an executable on-chain quote: a ``local_estimate`` is
        itself oracle-derived, so comparing it to the oracle estimate is circular.
        Skipped on a confirmed managed Anvil fork (C4 — fork pool state and the
        live oracle are not time-aligned); ALM-3184 requires that confirmation to
        be a positive signal, never the shape of ``rpc_url``.
        """
        if quote_source != "onchain_quoter":
            return None
        if price_ratio is None:
            # Without an oracle estimate there is nothing to compare; depth stays unguarded.
            return None

        from almanak.framework.execution.fork_signal import resolve_managed_fork

        if resolve_managed_fork(self.managed_fork):
            logger.info(
                "Skipping V4 price-impact guard: managed Anvil fork confirmed (%s). Fork pool state "
                "and live oracle prices are not time-aligned.",
                self.rpc_url,
            )
            return None

        from almanak.framework.intents._compiler_helpers import PriceImpactDecision, check_price_impact

        oracle_estimate = int(amount_in * price_ratio * Decimal(10**token_out_dec))
        result = check_price_impact(
            oracle_estimate=oracle_estimate,
            quoter_amount=quoter_amount,
            intent_max_impact=max_price_impact,
            config_max_impact=config_max_price_impact if config_max_price_impact is not None else Decimal("0.05"),
            offline_mode=using_placeholders,
            using_placeholders=using_placeholders,
        )
        if result.decision is PriceImpactDecision.IMPACT_TOO_HIGH and result.price_impact is not None:
            return SwapResult(
                success=False,
                transactions=[],
                error=(
                    f"Price impact too high for {token_in} -> {token_out}: on-chain quote "
                    f"implies {result.price_impact:.1%} impact vs oracle (max allowed "
                    f"{result.effective_max_impact:.2%}). Likely cause: insufficient pool "
                    f"liquidity at the selected fee tier. Refusing to compile a swap that "
                    f"would likely no-op or be sandwiched."
                ),
            )
        return None

    def compile_swap_intent(
        self,
        intent: SwapIntent,
        price_oracle: dict[str, Decimal] | None = None,
        *,
        config_max_price_impact: Decimal | None = None,
        permission_discovery: bool = False,
        using_placeholders: bool = False,
    ) -> ActionBundle:
        """Compile a SwapIntent to an ActionBundle.

        This method integrates with the intent system to convert high-level
        swap intents into executable transaction bundles.

        Args:
            intent: The SwapIntent to compile.
            price_oracle: Optional price map for USD conversions.
            config_max_price_impact: Compiler-config price-impact default
                (``ctx.max_price_impact_pct``); the per-intent override is read from
                ``intent.max_price_impact`` (VIB-2058).
            permission_discovery: Permission-discovery compile — relaxes the
                executable-quote fail-closed rule (nothing is broadcast).
            using_placeholders: Price oracle holds placeholder prices — relaxes the
                price-impact guard.

        Returns:
            ActionBundle containing transactions for execution.
        """
        from almanak.framework.intents.vocabulary import IntentType
        from almanak.framework.models.reproduction_bundle import ActionBundle

        if price_oracle is None:
            price_oracle = {}

        if intent.amount is not None:
            if intent.amount == "all":
                raise ValueError(
                    "amount='all' must be resolved before compilation. "
                    "Use Intent.set_resolved_amount() to resolve chained amounts."
                )
            amount_in: Decimal = intent.amount  # type: ignore[assignment]
        elif intent.amount_usd is not None:
            # Lenient oracle lookup handles address-form legs; direct symbol keys win.
            from almanak.framework.intents.compiler_queries import lenient_oracle_price

            from_price = lenient_oracle_price(
                price_oracle, intent.from_token, getattr(intent, "chain", None) or self.chain
            )
            if not from_price:
                raise ValueError(
                    f"Price unavailable for '{intent.from_token}' -- cannot convert amount_usd "
                    "to token amount. Ensure the price oracle includes this token."
                )
            amount_in = intent.amount_usd / from_price
        else:
            raise ValueError("Either amount or amount_usd must be specified")

        slippage_bps = slippage_to_bps(intent.max_slippage)

        # Price ratio keeps cross-decimal quotes accurate.
        from almanak.framework.intents.compiler_queries import lenient_oracle_price

        computed_price_ratio = None
        from_price = lenient_oracle_price(price_oracle, intent.from_token, getattr(intent, "chain", None) or self.chain)
        to_price = lenient_oracle_price(price_oracle, intent.to_token, getattr(intent, "chain", None) or self.chain)
        if from_price and to_price and to_price > 0:
            computed_price_ratio = Decimal(str(from_price)) / Decimal(str(to_price))

        result = self.swap_exact_input(
            token_in=intent.from_token,
            token_out=intent.to_token,
            amount_in=amount_in,
            slippage_bps=slippage_bps,
            price_ratio=computed_price_ratio,
            max_price_impact=intent.max_price_impact,
            config_max_price_impact=config_max_price_impact,
            offline_mode=permission_discovery or using_placeholders,
            using_placeholders=using_placeholders,
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

        # Pre-slippage human-readable quote for realized-slippage computation.
        expected_output_human: str | None = None
        if result.amount_out_quoted and to_dec is not None:
            expected_output_human = str(Decimal(str(result.amount_out_quoted)) / Decimal(10**to_dec))

        metadata: dict[str, Any] = {
            "intent_id": intent.intent_id,
            "from_token": from_token_dict,
            "to_token": to_token_dict,
            "swap_token_meta": build_swap_token_meta(from_token_dict, to_token_dict, chain=self.chain),
            "amount_in": str(result.amount_in),
            "amount_out_minimum": str(result.amount_out_minimum),
            "slippage_bps": slippage_bps,
            "chain": self.chain,
            "router": self.addresses["universal_router"],
            "pool_manager": self.addresses["pool_manager"],
            "gas_estimate": result.gas_estimate,
            "protocol_version": "v4",
            # Whether minOut came from an executable quote or an offline estimate.
            "quote_source": result.quote_source,
        }
        if expected_output_human is not None:
            metadata["expected_output_human"] = expected_output_human

        return ActionBundle(
            intent_type=IntentType.SWAP.value,
            transactions=[tx_to_dict(tx) for tx in result.transactions],
            metadata=metadata,
        )

    def _prepare_lp_open_pool(self, intent: LPOpenIntent) -> _LPOpenPool:
        """Resolve and normalize the intent into the canonical V4 pool order."""
        token0_symbol, token1_symbol, fee = self._parse_pool(intent.pool)
        token0_addr, token0_dec = self._resolve_token(token0_symbol, for_v4_pool=True)
        token1_addr, token1_dec = self._resolve_token(token1_symbol, for_v4_pool=True)

        pair_swapped = int(token0_addr, 16) > int(token1_addr, 16)
        if pair_swapped:
            token0_addr, token1_addr = token1_addr, token0_addr
            token0_dec, token1_dec = token1_dec, token0_dec
            token0_symbol, token1_symbol = token1_symbol, token0_symbol
            amount0, amount1 = intent.amount1, intent.amount0
        else:
            amount0, amount1 = intent.amount0, intent.amount1

        amount0_wei = int(Decimal(str(amount0)) * Decimal(10**token0_dec))
        amount1_wei = int(Decimal(str(amount1)) * Decimal(10**token1_dec))
        if pair_swapped:
            range_lower = Decimal(1) / Decimal(str(intent.range_upper))
            range_upper = Decimal(1) / Decimal(str(intent.range_lower))
        else:
            range_lower = Decimal(str(intent.range_lower))
            range_upper = Decimal(str(intent.range_upper))
        tick_lower = self._sdk.price_to_tick(range_lower, token0_dec, token1_dec)
        tick_upper = self._sdk.price_to_tick(range_upper, token0_dec, token1_dec)

        tick_spacing = intent.protocol_params.get("tick_spacing") if intent.protocol_params else None
        if tick_spacing is None:
            from almanak.connectors.uniswap_v4.sdk import TICK_SPACING

            tick_spacing = TICK_SPACING.get(fee, 60)
        tick_lower = (tick_lower // tick_spacing) * tick_spacing
        tick_upper = (tick_upper // tick_spacing) * tick_spacing
        if tick_lower == tick_upper:
            tick_upper += tick_spacing

        hooks = NATIVE_CURRENCY
        hook_data = b""
        if intent.protocol_params:
            hooks = intent.protocol_params.get("hooks", NATIVE_CURRENCY)
            hook_data_hex = intent.protocol_params.get("hook_data", "")
            if hook_data_hex:
                hook_data = bytes.fromhex(hook_data_hex.replace("0x", ""))

        warnings: list[str] = []
        if hooks != NATIVE_CURRENCY:
            hook_flags = HookFlags.from_address(hooks)
            if hook_flags.has_any_liquidity_hooks and not hook_data:
                warnings.append(
                    f"Pool uses hooks ({hooks[:10]}...) with liquidity callbacks "
                    f"({', '.join(hook_flags.active_flags)}), but hookData is empty. "
                    "This may cause the transaction to revert if the hook requires data."
                )

        pool_key = self._sdk.compute_pool_key(token0_addr, token1_addr, fee, tick_spacing, hooks)
        self._reject_unsupported_v0_pool(pool_key)
        return _LPOpenPool(
            token0_symbol=token0_symbol,
            token1_symbol=token1_symbol,
            token0_addr=token0_addr,
            token1_addr=token1_addr,
            token0_dec=token0_dec,
            token1_dec=token1_dec,
            amount0_wei=amount0_wei,
            amount1_wei=amount1_wei,
            range_lower=range_lower,
            range_upper=range_upper,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            fee=fee,
            hooks=hooks,
            hook_data=hook_data,
            pool_key=pool_key,
            pool_id=compute_pool_id(pool_key),
            warnings=warnings,
        )

    def _resolve_lp_open_price(
        self,
        intent: LPOpenIntent,
        pool: _LPOpenPool,
        price_oracle: dict[str, Decimal],
    ) -> _LPOpenPrice:
        """Choose the on-chain or estimated price used to size liquidity."""
        sqrt_price_x96 = None
        used_onchain_price = False
        price_source = "on_chain"

        if self.rpc_url:
            sqrt_price_x96 = self._sdk.get_pool_sqrt_price(pool.pool_key, rpc_url=self.rpc_url)
            if sqrt_price_x96:
                used_onchain_price = True
                logger.info("V4 LP_OPEN: using on-chain sqrtPriceX96=%d for liquidity computation", sqrt_price_x96)

        if sqrt_price_x96 is None:
            from almanak.framework.intents.compiler_queries import lenient_oracle_price

            mid_price = None
            price0 = lenient_oracle_price(
                price_oracle,
                pool.token0_symbol,
                getattr(intent, "chain", None) or self.chain,
            )
            price1 = lenient_oracle_price(
                price_oracle,
                pool.token1_symbol,
                getattr(intent, "chain", None) or self.chain,
            )
            if price0 and price1 and price1 > 0:
                mid_price = Decimal(str(price0)) / Decimal(str(price1))
                price_source = "oracle_estimate"
            elif pool.range_lower is not None and pool.range_upper is not None:
                mid_price = (pool.range_lower + pool.range_upper) / 2
                price_source = "range_midpoint_estimate"

            if mid_price and mid_price > 0:
                sqrt_price_x96 = self._sdk.estimate_sqrt_price_x96(mid_price, pool.token0_dec, pool.token1_dec)
                logger.info("V4 LP_OPEN: using estimated sqrtPriceX96=%d from oracle prices", sqrt_price_x96)
            else:
                from almanak.connectors.uniswap_v4.sdk import _tick_to_sqrt_ratio_x96

                sqrt_price_x96 = (
                    _tick_to_sqrt_ratio_x96(pool.tick_lower) + _tick_to_sqrt_ratio_x96(pool.tick_upper)
                ) // 2
                price_source = "range_midpoint_estimate"
                logger.info("V4 LP_OPEN: using tick-range midpoint sqrtPriceX96=%d", sqrt_price_x96)

        return _LPOpenPrice(
            sqrt_price_x96=sqrt_price_x96,
            used_onchain_price=used_onchain_price,
            source=price_source,
        )

    def _compute_lp_open_liquidity(
        self,
        intent: LPOpenIntent,
        pool: _LPOpenPool,
        price: _LPOpenPrice,
    ) -> _LPOpenLiquidity:
        """Apply the slippage policy while keeping requested amounts as hard caps."""
        user_slippage = getattr(intent, "max_slippage", None)
        if user_slippage is None:
            user_slippage = Decimal("0.005")

        if price.used_onchain_price:
            effective_slippage = max(user_slippage, ON_CHAIN_MIN_SLIPPAGE)
        else:
            allow_estimated = (intent.protocol_params or {}).get("allow_estimated_price") is True
            if not allow_estimated and user_slippage * 2 < ESTIMATED_PRICE_MIN_SLIPPAGE:
                raise UniswapV4EstimatedPriceWithoutOptInError(
                    f"V4 LP_OPEN: on-chain sqrtPrice unavailable (pool may be uninitialised "
                    f"or StateView reverted), so the price is estimated ({price.source}). "
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
                price.source,
            )
        slippage_bps = slippage_to_bps(effective_slippage)
        slippage_mult = Decimal(10000 + slippage_bps) / Decimal(10000)
        amount0_budget = int(Decimal(pool.amount0_wei) / slippage_mult)
        amount1_budget = int(Decimal(pool.amount1_wei) / slippage_mult)
        liquidity = self._sdk.compute_liquidity_from_amounts(
            price.sqrt_price_x96,
            pool.tick_lower,
            pool.tick_upper,
            amount0_budget,
            amount1_budget,
        )
        return _LPOpenLiquidity(
            amount0_budget=amount0_budget,
            amount1_budget=amount1_budget,
            amount0_max=pool.amount0_wei,
            amount1_max=pool.amount1_wei,
            liquidity=liquidity,
            slippage_bps=slippage_bps,
        )

    def _build_lp_open_transactions(
        self,
        pool: _LPOpenPool,
        liquidity: _LPOpenLiquidity,
    ) -> list[SwapTransaction]:
        """Build Permit2 approvals followed by the PositionManager mint."""
        mint_params = LPMintParams(
            pool_key=pool.pool_key,
            tick_lower=pool.tick_lower,
            tick_upper=pool.tick_upper,
            liquidity=liquidity.liquidity,
            amount0_max=liquidity.amount0_max,
            amount1_max=liquidity.amount1_max,
            owner=self.wallet_address,
            hook_data=pool.hook_data,
        )
        transactions: list[SwapTransaction] = []
        position_manager = self.addresses["position_manager"]
        for token_addr, amount_max in [
            (pool.token0_addr, liquidity.amount0_max),
            (pool.token1_addr, liquidity.amount1_max),
        ]:
            if token_addr.lower() == NATIVE_CURRENCY:
                continue
            transactions.append(self._sdk.build_approve_tx(token_addr, PERMIT2_ADDRESS, amount_max))
            transactions.append(self._sdk.build_permit2_approve_tx(token_addr, position_manager, amount_max))

        transactions.append(
            self._sdk.build_mint_position_tx(
                mint_params,
                deadline=deadline_from_now(self.default_deadline_seconds),
            )
        )
        return transactions

    def _build_lp_open_bundle(
        self,
        intent: LPOpenIntent,
        pool: _LPOpenPool,
        price: _LPOpenPrice,
        liquidity: _LPOpenLiquidity,
        transactions: list[SwapTransaction],
    ) -> ActionBundle:
        """Serialize LP-open transactions and compile-time position metadata."""
        from almanak.connectors.uniswap_v4.sdk import sqrt_ratio_x96_to_tick
        from almanak.framework.intents.vocabulary import IntentType
        from almanak.framework.models.reproduction_bundle import ActionBundle

        token0_dict = {"symbol": pool.token0_symbol, "address": pool.token0_addr, "decimals": pool.token0_dec}
        token1_dict = {"symbol": pool.token1_symbol, "address": pool.token1_addr, "decimals": pool.token1_dec}
        compile_time_current_tick = sqrt_ratio_x96_to_tick(price.sqrt_price_x96)
        metadata: dict[str, Any] = {
            "intent_id": intent.intent_id,
            "token0": token0_dict,
            "token1": token1_dict,
            "amount0_desired": str(pool.amount0_wei),
            "amount1_desired": str(pool.amount1_wei),
            "amount0_liquidity_budget": str(liquidity.amount0_budget),
            "amount1_liquidity_budget": str(liquidity.amount1_budget),
            "tick_lower": pool.tick_lower,
            "tick_upper": pool.tick_upper,
            "liquidity": str(liquidity.liquidity),
            "fee": pool.fee,
            "chain": self.chain,
            "position_manager": self.addresses["position_manager"],
            "pool_manager": self.addresses["pool_manager"],
            "hooks": pool.hooks,
            "gas_estimate": sum(tx.gas_estimate for tx in transactions),
            "protocol_version": "v4",
            "effective_slippage_bps": liquidity.slippage_bps,
            "price_source": price.source,
            "estimated_sqrt_price_x96": (str(price.sqrt_price_x96) if price.source != "on_chain" else None),
            "compile_time_current_tick": compile_time_current_tick,
            "compile_time_current_tick_source": "onchain" if price.used_onchain_price else "estimated",
            "pool_id": pool.pool_id,
            "protocol": (getattr(intent, "protocol", None) or "uniswap_v4"),
            "registry_handle": getattr(intent, "registry_handle", None),
        }
        if pool.warnings:
            metadata["warnings"] = pool.warnings

        return ActionBundle(
            intent_type=IntentType.LP_OPEN.value,
            transactions=[tx_to_dict(tx) for tx in transactions],
            metadata=metadata,
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

        try:
            pool = self._prepare_lp_open_pool(intent)
            price = self._resolve_lp_open_price(intent, pool, price_oracle)
            liquidity = self._compute_lp_open_liquidity(intent, pool, price)
            if liquidity.liquidity <= 0:
                return ActionBundle(
                    intent_type=IntentType.LP_OPEN.value,
                    transactions=[],
                    metadata={"error": "Computed liquidity is zero — check amounts and price range"},
                )
            transactions = self._build_lp_open_transactions(pool, liquidity)
            return self._build_lp_open_bundle(intent, pool, price, liquidity, transactions)

        except SlippagePrecisionError:
            raise
        except UniswapV4FailLoudError:
            # Scope and estimated-price guards raise instead of returning an empty bundle.
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

        # Native currency0 is supported on close; TAKE_PAIR returns raw ETH with
        # no special-casing. Salt is not validated; non-zero salt is the normal mint path.

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

        # Skip burning: BURN_POSITION encoding faults with DECREASE+TAKE;
        # leaving a zero-liquidity NFT is harmless.
        close_tx = self._sdk.build_decrease_liquidity_tx(
            params=decrease_params,
            currency0=currency0,
            currency1=currency1,
            recipient=self.wallet_address,
            deadline=deadline_from_now(self.default_deadline_seconds),
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
            deadline=deadline_from_now(self.default_deadline_seconds),
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
        """Fail-loud guard for unsupported V4 pool shapes.

        Rejects:
        - hooks != 0x0000…0000 — VIB-4485 (P-V1-D) will lift this.

        Native-ETH currency0 (currency0 == 0x0000…0000) is NO LONGER rejected:
        VIB-4483 (P-V1-B) lifted that guard. Native-ETH V4 pools are supported —
        the SDK threads the native leg as ``msg.value`` and the runner stamps the
        post-mint native deposit amount onto ``LPOpenData`` via the gateway
        ``QueryV4PositionState`` read.

        Does NOT validate salt: per VIB-4426 §Q7, salt = bytes32(tokenId) is the
        canonical PositionManager._mint path and is always non-zero for a minted
        position. Rejecting non-zero salt would break every real LP open.
        """
        hooks_norm = pool_key.hooks.lower() if isinstance(pool_key.hooks, str) else pool_key.hooks
        if hooks_norm != NATIVE_CURRENCY:
            raise UniswapV4UnsupportedPoolError(
                f"Uniswap V4 pool has hooks={pool_key.hooks} but hook support is not in V0 scope. "
                "V0 (VIB-4426) supports only hookless ERC20-ERC20 pools. "
                "Hook support is tracked by VIB-4485 (P-V1-D)."
            )
        # Native currency0 is supported; the native leg travels as msg.value and
        # is measured post-mint, not from the receipt.

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
        # V4 uses address(0) for the native currency; the symbol set is per-chain.
        if for_v4_pool and token.upper() in native_symbols_for(self.chain):
            return NATIVE_CURRENCY, ChainRegistry.resolve(self.chain).native.decimals

        # Never assume 18 decimals for address-form tokens.
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

        if self._token_resolver:
            resolved = self._token_resolver.resolve_for_swap(token, self.chain)
            return resolved.address, resolved.decimals

        # Fall back to the framework static resolver, not a sibling catalogue;
        # offline lookup with no auto-wrap, failing closed with the resolver error.
        resolved = get_token_resolver().resolve(token, self.chain, skip_gateway=True, log_errors=False)
        return resolved.address, resolved.decimals


@dataclass
class SwapResult:
    """Result of building swap transactions."""

    success: bool
    transactions: list[SwapTransaction]
    amount_in: int = 0
    amount_out_minimum: int = 0
    # Pre-slippage expected output in raw units for realized-slippage computation.
    amount_out_quoted: int = 0
    gas_estimate: int = 0
    error: str | None = None
    # Whether minOut came from an executable quoter, an offline estimate, or is unstamped.
    quote_source: str = ""


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
