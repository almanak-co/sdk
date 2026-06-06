"""Swap adapter for Uniswap V3-style exactInputSingle routers."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Literal

logger = logging.getLogger(__name__)


# Chains where probing for a bridged USDC variant is meaningful for the
# fee-tier heuristic. Outside this set the resolver has no entry for
# ``USDC.e`` / ``USDC_BRIDGED`` and the probe burns ~15s per call against
# the gateway TokenService timeout.
_BRIDGED_USDC_PROBE_CHAINS: frozenset[str] = frozenset(
    {
        "arbitrum",
        "optimism",
        "polygon",
        "avalanche",
        "berachain",
    }
)

_CHAIN_WRAPPED_NATIVE: dict[str, str] = {
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
    "berachain": "WBERA",
}


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

        from almanak.framework.intents.compiler_constants import PROTOCOL_ROUTERS

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

    def apply_external_quote_selection(
        self,
        *,
        fee_tier: int,
        amount_out: int,
        source: str,
        fee_selection: Mapping[str, Any] | None = None,
    ) -> None:
        """Cache a quote selected by a connector-owned provider."""
        self._cached_fee = fee_tier
        self.last_quoted_amount_out = amount_out
        if fee_selection is not None:
            self.last_fee_selection = dict(fee_selection)
            self.last_fee_selection.setdefault("selected_fee_tier", fee_tier)
            self.last_fee_selection.setdefault("source", source)
            return
        self.last_fee_selection = {
            "mode": self.pool_selection_mode,
            "source": source,
            "selected_fee_tier": fee_tier,
            "candidate_fee_tiers": [fee_tier],
        }

    def get_swap_calldata(
        self,
        from_token: str,
        to_token: str,
        amount_in: int,
        min_amount_out: int,
        recipient: str,
        deadline: int,
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
        from almanak.framework.intents.compiler_constants import (
            SWAP_ROUTER_ALGEBRA_PROTOCOLS,
            SWAP_ROUTER_V1_CHAIN_OVERRIDES,
            SWAP_ROUTER_V1_PROTOCOLS,
        )

        if self.protocol in SWAP_ROUTER_ALGEBRA_PROTOCOLS:
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

        fee = (
            self._cached_fee if self._cached_fee is not None else self._select_fee_tier(from_token, to_token, amount_in)
        )

        chain_v1_overrides = SWAP_ROUTER_V1_CHAIN_OVERRIDES.get(self.chain, frozenset())
        if self.protocol in SWAP_ROUTER_V1_PROTOCOLS or self.protocol in chain_v1_overrides:
            selector = "0x414bf389"
            params = (
                self._pad_address(from_token)
                + self._pad_address(to_token)
                + self._pad_uint24(fee)
                + self._pad_address(recipient)
                + self._pad_uint256(deadline)
                + self._pad_uint256(amount_in)
                + self._pad_uint256(min_amount_out)
                + self._pad_uint160(sqrt_price_limit)
            )
        else:
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
        from almanak.framework.intents.compiler_constants import SWAP_FEE_TIERS, SWAP_FEE_TIERS_CHAIN

        chain_key = (str(self.chain).lower(), self.protocol)
        return SWAP_FEE_TIERS_CHAIN.get(chain_key, SWAP_FEE_TIERS.get(self.protocol, ()))

    def _select_fee_tier(self, from_token: str, to_token: str, amount_in: int) -> int:
        """Select fee tier using fixed mode, on-chain quotes, or safe heuristic fallback."""
        from almanak.framework.intents.compiler_constants import DEFAULT_SWAP_FEE_TIER, SWAP_ROUTER_ALGEBRA_PROTOCOLS

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
        from almanak.framework.intents.compiler_constants import DEFAULT_SWAP_FEE_TIER

        from_lower = from_token.lower()
        to_lower = to_token.lower()
        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()

        def resolve_address(symbol: str, probe: bool = False) -> str | None:
            try:
                token = resolver.resolve(symbol, self.chain, log_errors=not probe)
            except Exception:
                return None
            if token is None:
                return None
            address = getattr(token, "address", None)
            return address.lower() if isinstance(address, str) else None

        usdc_addr = resolve_address("USDC")
        if self.chain in _BRIDGED_USDC_PROBE_CHAINS:
            usdc_bridged = resolve_address("USDC.e", probe=True) or resolve_address("USDC_BRIDGED", probe=True)
        else:
            usdc_bridged = None

        wrapped_symbol = _CHAIN_WRAPPED_NATIVE.get(self.chain)
        wrapped_native_addr = resolve_address(wrapped_symbol) if wrapped_symbol else None

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
        from almanak.framework.intents.compiler_constants import SWAP_QUOTER_ADDRESSES

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
                Web3.HTTPProvider(  # vib-2986-exempt: gateway-internal fallback
                    self.rpc_url,
                    request_kwargs={"timeout": self.rpc_timeout},
                )
            )
        if not web3.is_connected():
            return None

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

        def quote_fee_tier(fee_tier: int) -> dict[str, int] | None:
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
                try:
                    amount_out = contract_v1.functions.quoteExactInputSingle(
                        from_addr, to_addr, fee_tier, amount_in, 0
                    ).call()
                    if amount_out > 0:
                        return {
                            "fee_tier": fee_tier,
                            "amount_out": int(amount_out),
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

        quoted_candidates: list[dict[str, int]] = []
        with ThreadPoolExecutor(max_workers=len(candidates)) as executor:
            futures = {executor.submit(quote_fee_tier, ft): ft for ft in candidates}
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    quoted_candidates.append(result)

        if not quoted_candidates:
            return None

        best = max(quoted_candidates, key=lambda quote: (quote["amount_out"], -quote["fee_tier"]))
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
        from almanak.framework.intents.compiler_constants import SWAP_QUOTER_ADDRESSES

        self.last_fee_selection = {
            "mode": self.pool_selection_mode,
            "source": "algebra_quoter_unavailable",
            "selected_fee_tier": None,
            "candidate_fee_tiers": [],
            "protocol_family": "algebra_v1_9",
        }

        if self.gateway_client is None and not self.rpc_url:
            return

        quoter_address = SWAP_QUOTER_ADDRESSES.get(self.chain, {}).get(self.protocol)
        if not quoter_address:
            self.last_fee_selection["source"] = "algebra_quoter_unconfigured"
            logger.warning(
                "Algebra quoter not configured for protocol=%s on chain=%s. "
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
                Web3.HTTPProvider(  # vib-2986-exempt: gateway-internal fallback
                    self.rpc_url,
                    request_kwargs={"timeout": self.rpc_timeout},
                )
            )
        if not web3.is_connected():
            return

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
        from almanak.framework.intents.compiler_constants import get_gas_estimate

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


__all__ = ["DefaultSwapAdapter"]
