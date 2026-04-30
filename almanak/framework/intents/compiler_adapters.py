"""Compiler protocol adapters — DEX, LP, and lending adapters.

These are extracted from compiler.py for file-size management.
All symbols remain importable from ``almanak.framework.intents.compiler``.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Literal, Protocol

from .compiler_constants import (
    AAVE_BORROW_SELECTOR,
    AAVE_FLASH_LOAN_SELECTOR,
    AAVE_FLASH_LOAN_SIMPLE_SELECTOR,
    AAVE_REPAY_SELECTOR,
    AAVE_SET_COLLATERAL_SELECTOR,
    AAVE_SUPPLY_SELECTOR,
    AAVE_V2_DEPOSIT_SELECTOR,
    AAVE_V2_FORKS,
    AAVE_WITHDRAW_SELECTOR,
    BALANCER_FLASH_LOAN_SELECTOR,
    BALANCER_VAULT_ADDRESSES,
    DEFAULT_GAS_ESTIMATES,
    DEFAULT_SWAP_FEE_TIER,
    LENDING_POOL_ADDRESSES,
    LP_POSITION_MANAGERS,
    NFT_POSITION_BURN_SELECTOR,
    NFT_POSITION_COLLECT_SELECTOR,
    NFT_POSITION_DECREASE_SELECTOR,
    NFT_POSITION_MINT_SELECTOR,
    PROTOCOL_ROUTERS,
    SWAP_FEE_TIERS,
    SWAP_FEE_TIERS_CHAIN,
    SWAP_QUOTER_ADDRESSES,
    SWAP_ROUTER_ALGEBRA_PROTOCOLS,
    SWAP_ROUTER_V1_CHAIN_OVERRIDES,
    SWAP_ROUTER_V1_PROTOCOLS,
    get_gas_estimate,
)

logger = logging.getLogger("almanak.framework.intents.compiler")


# =============================================================================
# Protocol Adapter Protocol
# =============================================================================


class SwapProtocolAdapter(Protocol):
    """Protocol interface for DEX adapters."""

    def get_swap_calldata(
        self,
        from_token: str,
        to_token: str,
        amount_in: int,
        min_amount_out: int,
        recipient: str,
        deadline: int,
    ) -> bytes:
        """Generate calldata for a swap transaction."""
        ...

    def get_router_address(self) -> str:
        """Get the router address for this protocol."""
        ...

    def estimate_gas(self, from_token: str, to_token: str) -> int:
        """Estimate gas for a swap."""
        ...


class LPProtocolAdapter(Protocol):
    """Protocol interface for LP (liquidity provider) adapters."""

    def get_mint_calldata(
        self,
        token0: str,
        token1: str,
        fee: int,
        tick_lower: int,
        tick_upper: int,
        amount0_desired: int,
        amount1_desired: int,
        amount0_min: int,
        amount1_min: int,
        recipient: str,
        deadline: int,
    ) -> bytes:
        """Generate calldata for minting a new LP position."""
        ...

    def get_decrease_liquidity_calldata(
        self,
        token_id: int,
        liquidity: int,
        amount0_min: int,
        amount1_min: int,
        deadline: int,
    ) -> bytes:
        """Generate calldata for decreasing liquidity in an existing position."""
        ...

    def get_collect_calldata(
        self,
        token_id: int,
        recipient: str,
        amount0_max: int,
        amount1_max: int,
    ) -> bytes:
        """Generate calldata for collecting tokens from a position."""
        ...

    def get_position_manager_address(self) -> str:
        """Get the NFT position manager address for this protocol."""
        ...

    def estimate_mint_gas(self) -> int:
        """Estimate gas for minting a new position."""
        ...

    def estimate_close_gas(self, collect_fees: bool) -> int:
        """Estimate gas for closing a position."""
        ...


class LendingProtocolAdapter(Protocol):
    """Protocol interface for lending adapters."""

    def get_supply_calldata(
        self,
        asset: str,
        amount: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for supplying collateral."""
        ...

    def get_borrow_calldata(
        self,
        asset: str,
        amount: int,
        interest_rate_mode: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for borrowing tokens."""
        ...

    def get_repay_calldata(
        self,
        asset: str,
        amount: int,
        interest_rate_mode: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for repaying borrowed tokens."""
        ...

    def get_pool_address(self) -> str:
        """Get the lending pool address for this protocol."""
        ...

    def estimate_supply_gas(self) -> int:
        """Estimate gas for supply operation."""
        ...

    def estimate_borrow_gas(self) -> int:
        """Estimate gas for borrow operation."""
        ...

    def estimate_repay_gas(self) -> int:
        """Estimate gas for repay operation."""
        ...


# =============================================================================
# Default Protocol Adapter
# =============================================================================


class DefaultSwapAdapter:
    """Default swap adapter using Uniswap V3-style interface.

    This adapter generates calldata compatible with Uniswap V3's
    SwapRouter interface (exactInputSingle).

    Note: Instances are single-use per swap compilation. The compiler creates
    a fresh adapter in ``_compile_swap`` for each SwapIntent. Mutable state
    (``_cached_fee``, ``last_quoted_amount_out``) is therefore never carried
    across different token pairs or amounts.
    """

    def __init__(
        self,
        chain: str,
        protocol: str = "uniswap_v3",
        pool_selection_mode: Literal["auto", "fixed"] = "auto",
        fixed_fee_tier: int | None = None,
        rpc_url: str | None = None,
        rpc_timeout: float = 10.0,
        gateway_client: Any | None = None,
    ) -> None:
        """Initialize the adapter.

        Args:
            chain: Target blockchain
            protocol: Protocol name for router lookup
            pool_selection_mode: "auto" to quote all tiers (when possible), "fixed" for deterministic tier
            fixed_fee_tier: Optional fixed fee tier (required when pool_selection_mode="fixed")
            rpc_url: Optional RPC URL for on-chain quote queries in auto mode
            rpc_timeout: HTTP timeout for on-chain quote calls in seconds
            gateway_client: Optional gateway client for gateway-routed quoter calls
        """
        self.chain = chain
        self.protocol = protocol
        self.pool_selection_mode = pool_selection_mode
        self.fixed_fee_tier = fixed_fee_tier
        self.rpc_url = rpc_url
        self.rpc_timeout = rpc_timeout
        self.gateway_client = gateway_client
        self.last_fee_selection: dict[str, Any] = {}
        self.last_quoted_amount_out: int | None = None
        self._cached_fee: int | None = None

        # Get router address
        chain_routers = PROTOCOL_ROUTERS.get(chain, {})
        self.router_address = chain_routers.get(protocol, "0x0000000000000000000000000000000000000000")

    def get_router_address(self) -> str:
        """Get the router address."""
        return self.router_address

    def select_fee_tier(self, from_token: str, to_token: str, amount_in: int) -> int:
        """Pre-select fee tier and cache the result.

        Call this before get_swap_calldata() to make quoter data available
        for slippage adjustments. The selected fee tier is cached and reused
        by get_swap_calldata().

        Returns:
            Selected fee tier (bps).
        """
        # Clear previous state so stale data is never carried across calls.
        self._cached_fee = None
        self.last_quoted_amount_out = None
        fee = self._select_fee_tier(from_token, to_token, amount_in)
        self._cached_fee = fee
        return fee

    def get_quoted_amount_out(self) -> int | None:
        """Return the best quoted amount_out from the last fee tier selection.

        Only available after select_fee_tier() or get_swap_calldata() when
        the quoter was used (auto mode with RPC). Returns None if quoter
        was not used or no valid quotes were returned.
        """
        return self.last_quoted_amount_out

    def get_swap_calldata(
        self,
        from_token: str,
        to_token: str,
        amount_in: int,
        min_amount_out: int,
        recipient: str,
        deadline: int,  # used by SwapRouter V1; ignored by SwapRouter02
    ) -> bytes:
        """Generate calldata for exactInputSingle swap.

        Args:
            from_token: Input token address
            to_token: Output token address
            amount_in: Amount of input tokens (in wei)
            min_amount_out: Minimum output amount (in wei)
            recipient: Address to receive output tokens
            deadline: Transaction deadline (used by SwapRouter V1; ignored by SwapRouter02)

        Returns:
            Encoded calldata for the swap
        """
        sqrt_price_limit = 0

        # Algebra V1.9 forks (Camelot) use a no-fee, with-deadline encoding.
        # Fees are determined dynamically by the pool, not passed by the caller (VIB-1636).
        if self.protocol in SWAP_ROUTER_ALGEBRA_PROTOCOLS:
            # Algebra V1.9 exactInputSingle: 7-param WITHOUT fee, WITH deadline
            # selector: 0xbc651188
            # Struct: tokenIn, tokenOut, recipient, deadline, amountIn, amountOutMinimum, limitSqrtPrice
            selector = "0xbc651188"
            params = (
                self._pad_address(from_token)
                + self._pad_address(to_token)
                + self._pad_address(recipient)
                + self._pad_uint256(deadline)
                + self._pad_uint256(amount_in)
                + self._pad_uint256(min_amount_out)
                + self._pad_uint160(sqrt_price_limit)
            )
            return bytes.fromhex(selector[2:] + params)

        # Use cached fee tier if pre-selected via select_fee_tier()
        if self._cached_fee is not None:
            fee = self._cached_fee
        else:
            fee = self._select_fee_tier(from_token, to_token, amount_in)

        chain_v1_overrides = SWAP_ROUTER_V1_CHAIN_OVERRIDES.get(self.chain, frozenset())
        if self.protocol in SWAP_ROUTER_V1_PROTOCOLS or self.protocol in chain_v1_overrides:
            # Original SwapRouter (V1) exactInputSingle: 8-param WITH deadline
            # selector: 0x414bf389
            # Struct: tokenIn, tokenOut, fee, recipient, deadline, amountIn, amountOutMinimum, sqrtPriceLimitX96
            selector = "0x414bf389"
            swap_deadline = deadline
            params = (
                self._pad_address(from_token)
                + self._pad_address(to_token)
                + self._pad_uint24(fee)
                + self._pad_address(recipient)
                + self._pad_uint256(swap_deadline)
                + self._pad_uint256(amount_in)
                + self._pad_uint256(min_amount_out)
                + self._pad_uint160(sqrt_price_limit)
            )
        else:
            # SwapRouter02 / IV3SwapRouter exactInputSingle: 7-param WITHOUT deadline
            # selector: 0x04e45aaf
            # Struct: tokenIn, tokenOut, fee, recipient, amountIn, amountOutMinimum, sqrtPriceLimitX96
            selector = "0x04e45aaf"
            params = (
                self._pad_address(from_token)
                + self._pad_address(to_token)
                + self._pad_uint24(fee)
                + self._pad_address(recipient)
                + self._pad_uint256(amount_in)
                + self._pad_uint256(min_amount_out)
                + self._pad_uint160(sqrt_price_limit)
            )

        return bytes.fromhex(selector[2:] + params)

    def _supported_fee_tiers(self) -> tuple[int, ...]:
        """Return supported fee tiers for current protocol, with chain-specific overrides."""
        chain_key = (str(self.chain).lower(), self.protocol)
        return SWAP_FEE_TIERS_CHAIN.get(chain_key, SWAP_FEE_TIERS.get(self.protocol, ()))

    def _select_fee_tier(self, from_token: str, to_token: str, amount_in: int) -> int:
        """Select fee tier using fixed mode, on-chain quotes, or safe heuristic fallback."""
        # Algebra V1.9 forks (Camelot) have NO fee tiers — pools use a single
        # dynamic fee determined by the pool's volatility model. The Algebra
        # quoter ABI also differs from Uniswap V3's QuoterV2 (no `fee` arg, no
        # struct), so dispatch to a dedicated quoter path that populates
        # ``last_quoted_amount_out`` for the price-impact guard. The Algebra
        # swap calldata encoder ignores the return value (``get_swap_calldata``
        # short-circuits to the Algebra encoding), but we still surface the
        # actual dynamic fee resolved by the quoter so callers / telemetry that
        # cache or log ``_cached_fee`` see the real value rather than 0.
        # Returns 0 only when the quoter genuinely could not resolve a fee
        # (offline, unconfigured, RPC failure, or zero-liquidity).
        if self.protocol in SWAP_ROUTER_ALGEBRA_PROTOCOLS:
            self._quote_algebra_swap(from_token, to_token, amount_in)
            return int(self.last_fee_selection.get("selected_fee_tier") or 0)

        candidates = self._supported_fee_tiers()
        if self.pool_selection_mode == "fixed":
            if not candidates or self.fixed_fee_tier is None or self.fixed_fee_tier not in candidates:
                raise ValueError(
                    f"Invalid fixed fee tier {self.fixed_fee_tier} for protocol {self.protocol}. "
                    f"Available tiers: {list(candidates)}"
                )
            self.last_fee_selection = {
                "mode": "fixed",
                "source": "fixed_config",
                "selected_fee_tier": self.fixed_fee_tier,
                "candidate_fee_tiers": list(candidates),
            }
            return self.fixed_fee_tier

        if not candidates:
            self.last_fee_selection = {
                "mode": "unsupported",
                "source": "fallback_default",
                "selected_fee_tier": 3000,
                "candidate_fee_tiers": [],
            }
            return 3000

        if self.pool_selection_mode == "auto":
            quoted = self._select_fee_tier_by_quoter(from_token, to_token, amount_in, candidates)
            if quoted is not None:
                self.last_fee_selection = {
                    "mode": "auto",
                    "source": "quoter_best_quote",
                    "selected_fee_tier": quoted["fee_tier"],
                    "candidate_fee_tiers": list(candidates),
                    "quoted_candidates": quoted["quoted_candidates"],
                }
                return quoted["fee_tier"]

        heuristic_fee = self._select_fee_tier_heuristic(from_token, to_token)
        if heuristic_fee not in candidates:
            heuristic_fee = DEFAULT_SWAP_FEE_TIER.get(self.protocol, candidates[0])
        self.last_fee_selection = {
            "mode": self.pool_selection_mode,
            "source": "heuristic_fallback",
            "selected_fee_tier": heuristic_fee,
            "candidate_fee_tiers": list(candidates),
        }
        return heuristic_fee

    def _select_fee_tier_heuristic(self, from_token: str, to_token: str) -> int:
        """Conservative heuristic when no on-chain quoting is available."""
        from_lower = from_token.lower()
        to_lower = to_token.lower()
        from ..data.tokens import get_token_resolver

        resolver = get_token_resolver()

        def resolve_address(symbol: str, probe: bool = False) -> str | None:
            """Resolve a token symbol to its address.

            Args:
                symbol: Token symbol to resolve.
                probe: If True, suppress WARNING-level resolver logs for
                    expected probe failures (e.g. USDC.e on chains with
                    only native USDC).
            """
            try:
                # Use log_errors=False for probe lookups (expected failures should not warn).
                # This is thread-safe -- unlike mutating a shared logger level, the
                # log_errors flag is passed per-call and does not affect other threads.
                token = resolver.resolve(symbol, self.chain, log_errors=not probe)
            except Exception:
                return None
            if token is None:
                return None
            address = getattr(token, "address", None)
            return address.lower() if isinstance(address, str) else None

        usdc_addr = resolve_address("USDC")
        usdc_bridged = resolve_address("USDC.e", probe=True) or resolve_address("USDC_BRIDGED", probe=True)

        # Only resolve the wrapped native token for the current chain (not all chains)
        _wrapped_symbols = {
            "ethereum": "WETH",
            "arbitrum": "WETH",
            "optimism": "WETH",
            "base": "WETH",
            "polygon": "WMATIC",
            "avalanche": "WAVAX",
            "plasma": "WXPL",
            "bsc": "WBNB",
            "mantle": "WMNT",
            "sonic": "WS",
            "xlayer": "WOKB",
            "monad": "WMON",
            "zerog": "W0G",
        }
        _wn_symbol = _wrapped_symbols.get(self.chain)
        wrapped_native_addr = resolve_address(_wn_symbol) if _wn_symbol else None

        is_usdc = bool(usdc_addr and usdc_addr in (from_lower, to_lower))
        is_usdc_bridged = bool(usdc_bridged and usdc_bridged in (from_lower, to_lower))
        is_native_wrapped = bool(wrapped_native_addr and wrapped_native_addr in (from_lower, to_lower))
        if (is_usdc or is_usdc_bridged) and is_native_wrapped:
            return 100 if self.protocol == "pancakeswap_v3" else 500
        return DEFAULT_SWAP_FEE_TIER.get(self.protocol, 3000)

    def _select_fee_tier_by_quoter(
        self,
        from_token: str,
        to_token: str,
        amount_in: int,
        candidates: tuple[int, ...],
    ) -> dict[str, Any] | None:
        """Try quoting all candidate tiers via QuoterV2 and return best output tier."""
        if self.gateway_client is None and not self.rpc_url:
            return None
        quoter_address = SWAP_QUOTER_ADDRESSES.get(self.chain, {}).get(self.protocol)
        if not quoter_address:
            return None

        try:
            from web3 import Web3
        except ImportError:
            return None

        if self.gateway_client is not None and getattr(self.gateway_client, "is_connected", False):
            from almanak.framework.web3.gateway_provider import get_gateway_web3

            web3 = get_gateway_web3(
                self.gateway_client,
                chain=self.chain,
                request_timeout=self.rpc_timeout,
            )
        else:
            if self.rpc_url is None:
                return None
            web3 = Web3(
                Web3.HTTPProvider(
                    self.rpc_url,
                    request_kwargs={"timeout": self.rpc_timeout},
                )
            )
        if not web3.is_connected():
            return None

        # Two Quoter ABIs exist in the wild. Most Uniswap V3 forks deploy the
        # V2 struct-form Quoter, but some (e.g. Jaine on 0G Chain) ship the
        # older V1 five-arg signature. We try V2 first and transparently fall
        # back to V1 per-call if the contract reverts, so either variant works.
        quoter_abi_v2 = [
            {
                "inputs": [
                    {
                        "components": [
                            {"internalType": "address", "name": "tokenIn", "type": "address"},
                            {"internalType": "address", "name": "tokenOut", "type": "address"},
                            {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
                            {"internalType": "uint24", "name": "fee", "type": "uint24"},
                            {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"},
                        ],
                        "internalType": "struct IQuoterV2.QuoteExactInputSingleParams",
                        "name": "params",
                        "type": "tuple",
                    }
                ],
                "name": "quoteExactInputSingle",
                "outputs": [
                    {"internalType": "uint256", "name": "amountOut", "type": "uint256"},
                    {"internalType": "uint160", "name": "sqrtPriceX96After", "type": "uint160"},
                    {"internalType": "uint32", "name": "initializedTicksCrossed", "type": "uint32"},
                    {"internalType": "uint256", "name": "gasEstimate", "type": "uint256"},
                ],
                "stateMutability": "nonpayable",
                "type": "function",
            }
        ]
        quoter_abi_v1 = [
            {
                "inputs": [
                    {"internalType": "address", "name": "tokenIn", "type": "address"},
                    {"internalType": "address", "name": "tokenOut", "type": "address"},
                    {"internalType": "uint24", "name": "fee", "type": "uint24"},
                    {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
                    {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"},
                ],
                "name": "quoteExactInputSingle",
                "outputs": [{"internalType": "uint256", "name": "amountOut", "type": "uint256"}],
                "stateMutability": "nonpayable",
                "type": "function",
            }
        ]

        quoter_checksum = web3.to_checksum_address(quoter_address)
        contract_v2 = web3.eth.contract(address=quoter_checksum, abi=quoter_abi_v2)
        contract_v1 = web3.eth.contract(address=quoter_checksum, abi=quoter_abi_v1)
        from_addr = web3.to_checksum_address(from_token)
        to_addr = web3.to_checksum_address(to_token)

        def _quote_fee_tier(fee_tier: int) -> dict[str, int] | None:
            """Quote a single fee tier. Returns result dict or None on failure."""
            try:
                amount_out, _, _, gas_estimate = contract_v2.functions.quoteExactInputSingle(
                    (from_addr, to_addr, amount_in, fee_tier, 0)
                ).call()
                if amount_out > 0:
                    return {
                        "fee_tier": fee_tier,
                        "amount_out": int(amount_out),
                        "gas_estimate": int(gas_estimate),
                    }
            except Exception as exc_v2:
                # Fall through to V1: some UniV3 forks (Jaine/0G) only ship the
                # legacy 5-arg Quoter. V1 doesn't return gas_estimate; use a
                # conservative placeholder so downstream tier selection falls
                # back to `amount_out` as the sole ranking signal rather than
                # treating V1 quotes as "free" relative to V2 ones.
                try:
                    amount_out = contract_v1.functions.quoteExactInputSingle(
                        from_addr, to_addr, fee_tier, amount_in, 0
                    ).call()
                    if amount_out > 0:
                        return {
                            "fee_tier": fee_tier,
                            "amount_out": int(amount_out),
                            # Typical single-pool V3 swap ≈ 120–180k gas; pick a
                            # mid-range sentinel that's distinguishable from a
                            # real V2 zero-gas reading without skewing ranking.
                            "gas_estimate": 150_000,
                        }
                except Exception as exc_v1:
                    logger.debug(
                        "Fee-tier quote failed for fee_tier=%s (v2: %s | v1: %s)",
                        fee_tier,
                        exc_v2,
                        exc_v1,
                    )
            return None

        # Query all fee tiers in parallel to avoid sequential RPC latency
        quoted_candidates: list[dict[str, int]] = []
        with ThreadPoolExecutor(max_workers=len(candidates)) as executor:
            futures = {executor.submit(_quote_fee_tier, ft): ft for ft in candidates}
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    quoted_candidates.append(result)

        if not quoted_candidates:
            return None

        best = max(quoted_candidates, key=lambda quote: (quote["amount_out"], -quote["fee_tier"]))
        # Store the best quoted amount for downstream slippage adjustments
        self.last_quoted_amount_out = int(best["amount_out"])
        return {
            "fee_tier": int(best["fee_tier"]),
            "quoted_candidates": quoted_candidates,
        }

    def _quote_algebra_swap(self, from_token: str, to_token: str, amount_in: int) -> None:
        """Quote a swap on Algebra V1.9 forks (Camelot V3) — VIB-3750.

        Algebra differs from Uniswap V3 in two important ways:

          1. **No fee tiers.** Pools have a single dynamic fee determined by the
             pool's volatility oracle. The quoter does not accept a `fee` argument
             and instead returns the fee that *would* be charged.
          2. **Different ABI.** Both args are flat (no struct):

                 quoteExactInputSingle(
                     address tokenIn,
                     address tokenOut,
                     uint256 amountIn,
                     uint160 limitSqrtPrice,
                 ) -> (uint256 amountOut, uint16 fee)

        We populate ``last_quoted_amount_out`` so the compiler's price-impact
        guard runs identically to the Uniswap-V3 path. ``last_fee_selection`` is
        populated with diagnostic context (the dynamic fee returned by the
        quoter) but the value is not used to encode calldata — Algebra's swap
        encoder ignores fees entirely (see ``get_swap_calldata`` Algebra branch).

        On a "quoter returned 0" outcome we leave ``last_quoted_amount_out``
        as ``None`` (which the price-impact guard surfaces as the typed
        ``QUOTER_MISSING_FAIL_CLOSED`` error) AND record
        ``"source": "quoter_returned_zero"`` in ``last_fee_selection`` so
        downstream logs / metrics distinguish "no liquidity" (zero amount with
        a successful call) from "quoter unreachable" (RPC failure).

        On any RPC / decode error we fall back to ``last_quoted_amount_out=None``
        — same fail-closed behaviour as the Uniswap path. We never silently
        default to 0.
        """
        # Always seed last_fee_selection so callers always see a record of
        # what was attempted (matches the V3 path's invariants).
        self.last_fee_selection = {
            "mode": self.pool_selection_mode,
            "source": "algebra_quoter_unavailable",
            "selected_fee_tier": None,
            "candidate_fee_tiers": [],
            "protocol_family": "algebra_v1_9",
        }

        if self.gateway_client is None and not self.rpc_url:
            # Offline path — leave quoter result as None. The price-impact guard
            # in ``check_price_impact`` distinguishes offline_mode from
            # online-but-quoter-missing and handles each correctly.
            return

        quoter_address = SWAP_QUOTER_ADDRESSES.get(self.chain, {}).get(self.protocol)
        if not quoter_address:
            self.last_fee_selection["source"] = "algebra_quoter_unconfigured"
            logger.warning(
                "Algebra quoter not configured for protocol=%s on chain=%s — "
                "swap compilation will fail-closed via price-impact guard. "
                "Add an entry to SWAP_QUOTER_ADDRESSES to enable.",
                self.protocol,
                self.chain,
            )
            return

        try:
            from web3 import Web3
        except ImportError:
            return

        if self.gateway_client is not None and getattr(self.gateway_client, "is_connected", False):
            from almanak.framework.web3.gateway_provider import get_gateway_web3

            web3 = get_gateway_web3(
                self.gateway_client,
                chain=self.chain,
                request_timeout=self.rpc_timeout,
            )
        else:
            if self.rpc_url is None:
                return
            web3 = Web3(
                Web3.HTTPProvider(
                    self.rpc_url,
                    request_kwargs={"timeout": self.rpc_timeout},
                )
            )
        if not web3.is_connected():
            return

        # Algebra V3 quoter ABI — flat args, returns (amountOut, fee).
        # Source: docs.algebra.finance + Camelot deployed-contracts page.
        algebra_quoter_abi = [
            {
                "inputs": [
                    {"internalType": "address", "name": "tokenIn", "type": "address"},
                    {"internalType": "address", "name": "tokenOut", "type": "address"},
                    {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
                    {"internalType": "uint160", "name": "limitSqrtPrice", "type": "uint160"},
                ],
                "name": "quoteExactInputSingle",
                "outputs": [
                    {"internalType": "uint256", "name": "amountOut", "type": "uint256"},
                    {"internalType": "uint16", "name": "fee", "type": "uint16"},
                ],
                "stateMutability": "nonpayable",
                "type": "function",
            }
        ]

        try:
            quoter = web3.eth.contract(
                address=web3.to_checksum_address(quoter_address),
                abi=algebra_quoter_abi,
            )
            from_addr = web3.to_checksum_address(from_token)
            to_addr = web3.to_checksum_address(to_token)
            amount_out, dynamic_fee = quoter.functions.quoteExactInputSingle(from_addr, to_addr, amount_in, 0).call()
        except Exception as exc:
            # RPC / decode failure — leave last_quoted_amount_out as None so
            # the price-impact guard short-circuits to fail-closed. Distinct
            # from "quoter returned 0" below.
            self.last_fee_selection["source"] = "algebra_quoter_call_failed"
            self.last_fee_selection["error"] = str(exc)
            logger.debug(
                "Algebra quoter call failed for protocol=%s chain=%s: %s",
                self.protocol,
                self.chain,
                exc,
            )
            return

        amount_out = int(amount_out)
        if amount_out <= 0:
            # Successful call but zero output — pool has no liquidity at this
            # size. Surface the distinct signal in last_fee_selection and let
            # the price-impact guard fail-closed via QUOTER_MISSING_FAIL_CLOSED.
            self.last_fee_selection["source"] = "algebra_quoter_returned_zero"
            self.last_fee_selection["dynamic_fee"] = int(dynamic_fee)
            logger.warning(
                "Algebra quoter returned amountOut=0 for %s on %s "
                "(quoter=%s, fee=%s). Likely cause: pool has no liquidity at the "
                "requested size for this token pair.",
                self.protocol,
                self.chain,
                quoter_address,
                int(dynamic_fee),
            )
            return

        self.last_quoted_amount_out = amount_out
        self.last_fee_selection = {
            "mode": self.pool_selection_mode,
            "source": "algebra_quoter",
            # Algebra has no caller-selected fee tier; record the dynamic fee
            # returned by the pool so logs surface what was actually quoted.
            "selected_fee_tier": int(dynamic_fee),
            "candidate_fee_tiers": [],
            "protocol_family": "algebra_v1_9",
            "quoted_amount_out": amount_out,
        }

    def estimate_gas(self, from_token: str, to_token: str) -> int:
        """Estimate gas for a swap.

        Args:
            from_token: Input token address
            to_token: Output token address

        Returns:
            Estimated gas units (chain-aware for proxy tokens)
        """
        # Check if this is a native token swap (requires wrap/unwrap)
        native_placeholder = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE".lower()
        if from_token.lower() == native_placeholder or to_token.lower() == native_placeholder:
            return get_gas_estimate(self.chain, "swap_simple") + get_gas_estimate(self.chain, "wrap_eth")
        return get_gas_estimate(self.chain, "swap_simple")

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        addr_clean = addr.lower().replace("0x", "")
        return addr_clean.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_uint160(value: int) -> str:
        """Pad uint160 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_uint24(value: int) -> str:
        """Pad uint24 to 32 bytes."""
        return hex(value)[2:].zfill(64)


class UniswapV3LPAdapter:
    """LP adapter for Uniswap V3 NonfungiblePositionManager.

    This adapter generates calldata for managing concentrated liquidity
    positions on Uniswap V3 and compatible protocols.
    """

    def __init__(self, chain: str, protocol: str = "uniswap_v3") -> None:
        """Initialize the adapter.

        Args:
            chain: Target blockchain
            protocol: Protocol name for position manager lookup
        """
        self.chain = chain
        self.protocol = protocol

        # Get position manager address
        chain_managers = LP_POSITION_MANAGERS.get(chain, {})
        self.position_manager_address = chain_managers.get(protocol, "0x0000000000000000000000000000000000000000")

    def get_position_manager_address(self) -> str:
        """Get the NFT position manager address."""
        return self.position_manager_address

    def get_mint_calldata(
        self,
        token0: str,
        token1: str,
        fee: int,
        tick_lower: int,
        tick_upper: int,
        amount0_desired: int,
        amount1_desired: int,
        amount0_min: int,
        amount1_min: int,
        recipient: str,
        deadline: int,
    ) -> bytes:
        """Generate calldata for minting a new LP position.

        Args:
            token0: Address of token0 (must be sorted, lower address first)
            token1: Address of token1 (must be sorted, higher address second)
            fee: Fee tier (500, 3000, 10000 for 0.05%, 0.3%, 1%)
            tick_lower: Lower tick bound for the position
            tick_upper: Upper tick bound for the position
            amount0_desired: Desired amount of token0 to deposit
            amount1_desired: Desired amount of token1 to deposit
            amount0_min: Minimum amount of token0 to deposit (slippage protection)
            amount1_min: Minimum amount of token1 to deposit (slippage protection)
            recipient: Address to receive the position NFT
            deadline: Transaction deadline (Unix timestamp)

        Returns:
            Encoded calldata for the mint transaction
        """
        # mint(MintParams) selector
        selector = NFT_POSITION_MINT_SELECTOR

        # Encode MintParams struct:
        # struct MintParams {
        #     address token0;
        #     address token1;
        #     uint24 fee;
        #     int24 tickLower;
        #     int24 tickUpper;
        #     uint256 amount0Desired;
        #     uint256 amount1Desired;
        #     uint256 amount0Min;
        #     uint256 amount1Min;
        #     address recipient;
        #     uint256 deadline;
        # }

        params = (
            self._pad_address(token0)
            + self._pad_address(token1)
            + self._pad_uint24(fee)
            + self._pad_int24(tick_lower)
            + self._pad_int24(tick_upper)
            + self._pad_uint256(amount0_desired)
            + self._pad_uint256(amount1_desired)
            + self._pad_uint256(amount0_min)
            + self._pad_uint256(amount1_min)
            + self._pad_address(recipient)
            + self._pad_uint256(deadline)
        )

        return bytes.fromhex(selector[2:] + params)

    def get_decrease_liquidity_calldata(
        self,
        token_id: int,
        liquidity: int,
        amount0_min: int,
        amount1_min: int,
        deadline: int,
    ) -> bytes:
        """Generate calldata for decreasing liquidity in a position.

        Args:
            token_id: NFT token ID of the position
            liquidity: Amount of liquidity to remove
            amount0_min: Minimum amount of token0 to receive
            amount1_min: Minimum amount of token1 to receive
            deadline: Transaction deadline (Unix timestamp)

        Returns:
            Encoded calldata for the decreaseLiquidity transaction
        """
        # decreaseLiquidity(DecreaseLiquidityParams) selector
        selector = NFT_POSITION_DECREASE_SELECTOR

        # Encode DecreaseLiquidityParams struct:
        # struct DecreaseLiquidityParams {
        #     uint256 tokenId;
        #     uint128 liquidity;
        #     uint256 amount0Min;
        #     uint256 amount1Min;
        #     uint256 deadline;
        # }

        params = (
            self._pad_uint256(token_id)
            + self._pad_uint128(liquidity)
            + self._pad_uint256(amount0_min)
            + self._pad_uint256(amount1_min)
            + self._pad_uint256(deadline)
        )

        return bytes.fromhex(selector[2:] + params)

    def get_collect_calldata(
        self,
        token_id: int,
        recipient: str,
        amount0_max: int,
        amount1_max: int,
    ) -> bytes:
        """Generate calldata for collecting tokens from a position.

        This collects both:
        - Tokens from decreased liquidity
        - Accumulated trading fees

        Args:
            token_id: NFT token ID of the position
            recipient: Address to receive the collected tokens
            amount0_max: Maximum amount of token0 to collect
            amount1_max: Maximum amount of token1 to collect

        Returns:
            Encoded calldata for the collect transaction
        """
        # collect(CollectParams) selector
        selector = NFT_POSITION_COLLECT_SELECTOR

        # Encode CollectParams struct:
        # struct CollectParams {
        #     uint256 tokenId;
        #     address recipient;
        #     uint128 amount0Max;
        #     uint128 amount1Max;
        # }

        params = (
            self._pad_uint256(token_id)
            + self._pad_address(recipient)
            + self._pad_uint128(amount0_max)
            + self._pad_uint128(amount1_max)
        )

        return bytes.fromhex(selector[2:] + params)

    def get_burn_calldata(self, token_id: int) -> bytes:
        """Generate calldata for burning a position NFT.

        Note: The position must be empty (all liquidity removed and collected)
        before burning.

        Args:
            token_id: NFT token ID of the position to burn

        Returns:
            Encoded calldata for the burn transaction
        """
        # burn(uint256 tokenId) selector
        selector = NFT_POSITION_BURN_SELECTOR

        params = self._pad_uint256(token_id)

        return bytes.fromhex(selector[2:] + params)

    def estimate_mint_gas(self) -> int:
        """Estimate gas for minting a new position (chain-aware)."""
        return get_gas_estimate(self.chain, "lp_mint")

    def estimate_close_gas(self, collect_fees: bool) -> int:
        """Estimate gas for closing a position (decrease + collect + optional burn).

        Args:
            collect_fees: Whether fees will be collected (always True for close)

        Returns:
            Total estimated gas for the close operation (chain-aware)
        """
        # decreaseLiquidity + collect + burn
        gas = get_gas_estimate(self.chain, "lp_decrease_liquidity")
        gas += get_gas_estimate(self.chain, "lp_collect")
        gas += get_gas_estimate(self.chain, "lp_burn")
        return gas

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        addr_clean = addr.lower().replace("0x", "")
        return addr_clean.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_uint128(value: int) -> str:
        """Pad uint128 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_uint24(value: int) -> str:
        """Pad uint24 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_int24(value: int) -> str:
        """Pad int24 to 32 bytes (signed, two's complement)."""
        if value < 0:
            # Two's complement for negative int24
            # int24 range: -8388608 to 8388607
            value = (1 << 256) + value
        return hex(value)[2:].zfill(64)


class AaveV3Adapter:
    """Lending adapter for Aave V3 protocol.

    This adapter generates calldata for interacting with Aave V3 lending pools,
    supporting supply, borrow, and repay operations.

    Aave V3 features:
    - Efficiency Mode (E-Mode) for higher LTVs between correlated assets
    - Isolation Mode for new assets with limited debt ceiling
    - Variable and stable interest rates (stable being deprecated)
    """

    _AAVE_V2_FORKS = AAVE_V2_FORKS

    def __init__(self, chain: str, protocol: str = "aave_v3") -> None:
        """Initialize the adapter.

        Args:
            chain: Target blockchain
            protocol: Protocol name for pool lookup
        """
        self.chain = chain
        self.protocol = protocol
        self._is_v2_fork = protocol in self._AAVE_V2_FORKS

        # Get pool address
        chain_pools = LENDING_POOL_ADDRESSES.get(chain, {})
        self.pool_address = chain_pools.get(protocol, "0x0000000000000000000000000000000000000000")

    def get_pool_address(self) -> str:
        """Get the Aave V3 Pool address."""
        return self.pool_address

    def get_supply_calldata(
        self,
        asset: str,
        amount: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for supplying assets.

        Aave V3: supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)
        Aave V2 forks (Radiant V2): deposit(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)

        Both have identical parameter layouts, only the function selector differs.

        Args:
            asset: Token address to supply
            amount: Amount to supply (in token's smallest units)
            on_behalf_of: Address to credit with the supply

        Returns:
            Encoded calldata for the supply/deposit transaction
        """
        # No referral code (0)
        referral_code = 0

        params = (
            self._pad_address(asset)
            + self._pad_uint256(amount)
            + self._pad_address(on_behalf_of)
            + self._pad_uint16(referral_code)
        )

        selector = AAVE_V2_DEPOSIT_SELECTOR if self._is_v2_fork else AAVE_SUPPLY_SELECTOR
        return bytes.fromhex(selector[2:] + params)

    def get_borrow_calldata(
        self,
        asset: str,
        amount: int,
        interest_rate_mode: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for borrowing from Aave V3.

        Aave V3 borrow function:
        borrow(address asset, uint256 amount, uint256 interestRateMode,
               uint16 referralCode, address onBehalfOf)

        Args:
            asset: Token address to borrow
            amount: Amount to borrow (in token's smallest units)
            interest_rate_mode: 1 for stable (deprecated), 2 for variable
            on_behalf_of: Address to debit with the borrow

        Returns:
            Encoded calldata for the borrow transaction
        """
        # No referral code (0)
        referral_code = 0

        params = (
            self._pad_address(asset)
            + self._pad_uint256(amount)
            + self._pad_uint256(interest_rate_mode)
            + self._pad_uint16(referral_code)
            + self._pad_address(on_behalf_of)
        )

        return bytes.fromhex(AAVE_BORROW_SELECTOR[2:] + params)

    def get_repay_calldata(
        self,
        asset: str,
        amount: int,
        interest_rate_mode: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for repaying borrowed tokens to Aave V3.

        Aave V3 repay function:
        repay(address asset, uint256 amount, uint256 interestRateMode, address onBehalfOf)

        To repay the full debt, pass MAX_UINT256 as amount.

        Args:
            asset: Token address to repay
            amount: Amount to repay (in token's smallest units), MAX_UINT256 for full
            interest_rate_mode: 1 for stable (deprecated), 2 for variable
            on_behalf_of: Address that has the debt being repaid

        Returns:
            Encoded calldata for the repay transaction
        """
        params = (
            self._pad_address(asset)
            + self._pad_uint256(amount)
            + self._pad_uint256(interest_rate_mode)
            + self._pad_address(on_behalf_of)
        )

        return bytes.fromhex(AAVE_REPAY_SELECTOR[2:] + params)

    def get_withdraw_calldata(
        self,
        asset: str,
        amount: int,
        to: str,
    ) -> bytes:
        """Generate calldata for withdrawing supplied assets from Aave V3.

        Aave V3 withdraw function:
        withdraw(address asset, uint256 amount, address to)

        To withdraw all supplied assets, pass MAX_UINT256 as amount.

        Args:
            asset: Token address to withdraw
            amount: Amount to withdraw (in token's smallest units), MAX_UINT256 for full
            to: Address to receive the withdrawn tokens

        Returns:
            Encoded calldata for the withdraw transaction
        """
        params = self._pad_address(asset) + self._pad_uint256(amount) + self._pad_address(to)

        return bytes.fromhex(AAVE_WITHDRAW_SELECTOR[2:] + params)

    def get_set_collateral_calldata(
        self,
        asset: str,
        use_as_collateral: bool,
    ) -> bytes:
        """Generate calldata for enabling/disabling an asset as collateral.

        Aave V3 setUserUseReserveAsCollateral function:
        setUserUseReserveAsCollateral(address asset, bool useAsCollateral)

        This must be called after supplying to enable borrowing against the asset.

        Args:
            asset: Token address to enable/disable as collateral
            use_as_collateral: True to enable, False to disable

        Returns:
            Encoded calldata for the setUserUseReserveAsCollateral transaction
        """
        params = self._pad_address(asset) + self._pad_uint256(1 if use_as_collateral else 0)

        return bytes.fromhex(AAVE_SET_COLLATERAL_SELECTOR[2:] + params)

    def estimate_set_collateral_gas(self) -> int:
        """Estimate gas for setUserUseReserveAsCollateral operation."""
        return 150000  # Aave V3 can use more gas with incentives

    def estimate_supply_gas(self) -> int:
        """Estimate gas for supply operation."""
        return DEFAULT_GAS_ESTIMATES["lending_supply"]

    def estimate_borrow_gas(self) -> int:
        """Estimate gas for borrow operation."""
        return DEFAULT_GAS_ESTIMATES["lending_borrow"]

    def estimate_repay_gas(self) -> int:
        """Estimate gas for repay operation."""
        return DEFAULT_GAS_ESTIMATES["lending_repay"]

    def estimate_withdraw_gas(self) -> int:
        """Estimate gas for withdraw operation."""
        return DEFAULT_GAS_ESTIMATES["lending_withdraw"]

    def estimate_flash_loan_gas(self) -> int:
        """Estimate gas for flash loan operation (base only, not including callbacks)."""
        return DEFAULT_GAS_ESTIMATES["flash_loan"]

    def estimate_flash_loan_simple_gas(self) -> int:
        """Estimate gas for simple flash loan operation (base only, not including callbacks)."""
        return DEFAULT_GAS_ESTIMATES["flash_loan_simple"]

    def get_flash_loan_simple_calldata(
        self,
        receiver_address: str,
        asset: str,
        amount: int,
        params: bytes = b"",
    ) -> bytes:
        """Generate calldata for a simple (single-asset) flash loan.

        Aave V3 flashLoanSimple function:
        flashLoanSimple(
            address receiverAddress,
            address asset,
            uint256 amount,
            bytes calldata params,
            uint16 referralCode
        )

        The receiver contract must implement executeOperation() and return the
        borrowed amount plus premium (0.09% on Aave) within the same transaction.

        Args:
            receiver_address: Contract that will receive and handle the flash loan
            asset: Token address to borrow
            amount: Amount to borrow (in token's smallest units)
            params: Extra data to pass to receiver's executeOperation

        Returns:
            Encoded calldata for the flashLoanSimple transaction
        """
        # Calculate params offset (after fixed params: 5 * 32 bytes)
        params_offset = 5 * 32  # receiver(32) + asset(32) + amount(32) + paramsOffset(32) + referralCode(32)

        # Encode params data
        params_hex = params.hex() if params else ""
        params_len = len(params)

        encoded = (
            self._pad_address(receiver_address)
            + self._pad_address(asset)
            + self._pad_uint256(amount)
            + self._pad_uint256(params_offset)
            + self._pad_uint16(0)  # referral code
            + self._pad_uint256(params_len)
        )

        if params_len > 0:
            # Pad params to 32-byte boundary
            padded_params = params_hex + "0" * ((64 - len(params_hex) % 64) % 64)
            encoded += padded_params

        return bytes.fromhex(AAVE_FLASH_LOAN_SIMPLE_SELECTOR[2:] + encoded)

    def get_flash_loan_calldata(
        self,
        receiver_address: str,
        assets: list[str],
        amounts: list[int],
        modes: list[int],
        on_behalf_of: str,
        params: bytes = b"",
    ) -> bytes:
        """Generate calldata for a multi-asset flash loan.

        Aave V3 flashLoan function:
        flashLoan(
            address receiverAddress,
            address[] calldata assets,
            uint256[] calldata amounts,
            uint256[] calldata modes,
            address onBehalfOf,
            bytes calldata params,
            uint16 referralCode
        )

        Modes:
        - 0: No debt opened (must repay within same transaction) - for atomic arb
        - 1: Open stable rate debt
        - 2: Open variable rate debt

        Args:
            receiver_address: Contract that will receive and handle the flash loan
            assets: List of token addresses to borrow
            amounts: List of amounts to borrow (in token's smallest units)
            modes: List of debt modes (0, 1, or 2) for each asset
            on_behalf_of: Address to receive debt if mode != 0
            params: Extra data to pass to receiver's executeOperation

        Returns:
            Encoded calldata for the flashLoan transaction
        """
        n_assets = len(assets)

        # Calculate offsets for dynamic arrays
        # Fixed params before arrays: receiverAddress(32) + 3 array offsets(32*3) + onBehalfOf(32) + params offset(32) + referralCode(32) = 7*32
        assets_offset = 7 * 32
        amounts_offset = assets_offset + 32 + n_assets * 32  # length(32) + data(32*n)
        modes_offset = amounts_offset + 32 + n_assets * 32
        params_offset = modes_offset + 32 + n_assets * 32

        # Build header
        encoded = self._pad_address(receiver_address)
        encoded += self._pad_uint256(assets_offset)
        encoded += self._pad_uint256(amounts_offset)
        encoded += self._pad_uint256(modes_offset)
        encoded += self._pad_address(on_behalf_of)
        encoded += self._pad_uint256(params_offset)
        encoded += self._pad_uint16(0)  # referral code

        # Encode assets array
        encoded += self._pad_uint256(n_assets)
        for addr in assets:
            encoded += self._pad_address(addr)

        # Encode amounts array
        encoded += self._pad_uint256(n_assets)
        for amount_val in amounts:
            encoded += self._pad_uint256(amount_val)

        # Encode modes array
        encoded += self._pad_uint256(n_assets)
        for mode in modes:
            encoded += self._pad_uint256(mode)

        # Encode params
        params_hex = params.hex() if params else ""
        params_len = len(params)
        encoded += self._pad_uint256(params_len)
        if params_len > 0:
            padded_params = params_hex + "0" * ((64 - len(params_hex) % 64) % 64)
            encoded += padded_params

        return bytes.fromhex(AAVE_FLASH_LOAN_SELECTOR[2:] + encoded)

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        addr_clean = addr.lower().replace("0x", "")
        return addr_clean.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_uint16(value: int) -> str:
        """Pad uint16 to 32 bytes."""
        return hex(value)[2:].zfill(64)


class BalancerAdapter:
    """Flash loan adapter for Balancer Vault.

    Balancer flash loans have zero fees (no premium), making them ideal for
    arbitrage strategies. The Vault contract holds all pool liquidity.

    Balancer Vault flash loan function:
    flashLoan(
        IFlashLoanRecipient recipient,
        IERC20[] memory tokens,
        uint256[] memory amounts,
        bytes memory userData
    )

    Key differences from Aave:
    - Zero fees (no premium to repay)
    - All tokens and amounts in arrays (batch flash loans native)
    - userData is arbitrary bytes passed to receiver
    - Receiver must implement receiveFlashLoan() not executeOperation()
    """

    def __init__(self, chain: str, protocol: str = "balancer") -> None:
        """Initialize the adapter.

        Args:
            chain: Target blockchain
            protocol: Protocol name (always "balancer")
        """
        self.chain = chain
        self.protocol = protocol

        # Get vault address
        self.vault_address = BALANCER_VAULT_ADDRESSES.get(chain, "0x0000000000000000000000000000000000000000")

    def get_vault_address(self) -> str:
        """Get the Balancer Vault address."""
        return self.vault_address

    def get_flash_loan_calldata(
        self,
        recipient: str,
        tokens: list[str],
        amounts: list[int],
        user_data: bytes = b"",
    ) -> bytes:
        """Generate calldata for a Balancer flash loan.

        Balancer flashLoan function:
        flashLoan(
            IFlashLoanRecipient recipient,
            IERC20[] memory tokens,
            uint256[] memory amounts,
            bytes memory userData
        )

        Args:
            recipient: Contract address that will receive and handle the flash loan
            tokens: List of token addresses to borrow
            amounts: List of amounts to borrow (in token's smallest units)
            user_data: Extra data to pass to receiver's receiveFlashLoan

        Returns:
            Encoded calldata for the flashLoan transaction
        """
        n_tokens = len(tokens)
        if n_tokens != len(amounts):
            raise ValueError("tokens and amounts must have same length")

        # ABI encoding for flashLoan(address,address[],uint256[],bytes)
        # Layout:
        # - recipient (32 bytes, padded address)
        # - offset to tokens array (32 bytes)
        # - offset to amounts array (32 bytes)
        # - offset to userData (32 bytes)
        # - tokens array: length (32) + addresses (32 * n)
        # - amounts array: length (32) + amounts (32 * n)
        # - userData: length (32) + data (padded to 32)

        # Calculate offsets
        # Fixed header: recipient(32) + 3 offsets(32*3) = 128 bytes
        tokens_offset = 128
        amounts_offset = tokens_offset + 32 + n_tokens * 32
        user_data_offset = amounts_offset + 32 + n_tokens * 32

        # Build header
        encoded = self._pad_address(recipient)
        encoded += self._pad_uint256(tokens_offset)
        encoded += self._pad_uint256(amounts_offset)
        encoded += self._pad_uint256(user_data_offset)

        # Encode tokens array
        encoded += self._pad_uint256(n_tokens)
        for token in tokens:
            encoded += self._pad_address(token)

        # Encode amounts array
        encoded += self._pad_uint256(n_tokens)
        for amount in amounts:
            encoded += self._pad_uint256(amount)

        # Encode userData
        user_data_hex = user_data.hex() if user_data else ""
        user_data_len = len(user_data)
        encoded += self._pad_uint256(user_data_len)
        if user_data_len > 0:
            # Pad to 32-byte boundary
            padded_data = user_data_hex + "0" * ((64 - len(user_data_hex) % 64) % 64)
            encoded += padded_data

        return bytes.fromhex(BALANCER_FLASH_LOAN_SELECTOR[2:] + encoded)

    def get_flash_loan_simple_calldata(
        self,
        recipient: str,
        token: str,
        amount: int,
        user_data: bytes = b"",
    ) -> bytes:
        """Generate calldata for a single-token flash loan.

        This is a convenience method that wraps get_flash_loan_calldata
        for single-token flash loans.

        Args:
            recipient: Contract address that will receive the flash loan
            token: Token address to borrow
            amount: Amount to borrow (in token's smallest units)
            user_data: Extra data to pass to receiver's receiveFlashLoan

        Returns:
            Encoded calldata for the flashLoan transaction
        """
        return self.get_flash_loan_calldata(
            recipient=recipient,
            tokens=[token],
            amounts=[amount],
            user_data=user_data,
        )

    def estimate_flash_loan_gas(self) -> int:
        """Estimate gas for a multi-token flash loan (base only, not including callbacks)."""
        return DEFAULT_GAS_ESTIMATES["balancer_flash_loan"]

    def estimate_flash_loan_simple_gas(self) -> int:
        """Estimate gas for a single-token flash loan (base only, not including callbacks)."""
        return DEFAULT_GAS_ESTIMATES["balancer_flash_loan_simple"]

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad an address to 32 bytes (64 hex chars)."""
        clean_addr = addr.lower().replace("0x", "")
        return clean_addr.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad a uint256 to 32 bytes (64 hex chars)."""
        return hex(value)[2:].zfill(64)
