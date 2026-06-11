"""Fluid DEX SDK — low-level contract interactions.

Handles direct Web3 calls to Fluid DEX contracts on arbitrum, base,
ethereum, and polygon:

- DexReservesResolver: pool enumeration (``getAllPools``) and swap quotes
  (``estimateSwapIn``). This is Fluid's official quoting surface — quotes
  match on-chain execution to the wei (verified Phase 0, VIB-5028).
- DexFactory / DexResolver: pool counts and address enumeration.
- FluidDexT1 pool contracts: ``constantsView()`` raw decoding for token
  pairs and smart-collateral/debt flags; ``swapIn()`` calldata building.

All reads are standard eth_call routed through the gateway when a
``gateway_client`` is provided (production path); a direct ``rpc_url``
is supported for ad-hoc scripts and tests only.

History: the original quote path simulated ``swapIn`` via eth_call with
brute-forced ERC-20 balance/allowance storage overrides. That shim never
produced a valid balance on proxy tokens (e.g. Arbitrum USDC), so
``transferFrom`` failed inside the simulation with ``FluidSafeTransferError``
(0xdee51a8a) — which was misread as "pool rejects swaps at any amount" and
led to the connector being disabled (VIB-2822). Root cause documented in
``docs/internal/qa/fluid-protocol-validation-2026-06-10.md`` §V1.7.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from web3 import Web3
from web3.providers import HTTPProvider

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)

# =============================================================================
# Contract Addresses
# =============================================================================

# Fluid deploys deterministically — these addresses are identical on every
# supported chain (verified on-chain per chain at Phase 0, VIB-5028, against
# Instadapp/fluid-contracts-public deployments).
_FLUID_CORE_CONTRACTS: dict[str, str] = {
    "dex_factory": "0x91716C4EDA1Fb55e84Bf8b4c7085f84285c19085",
    "dex_resolver": "0x11D80CfF056Cef4F9E6d23da8672fE9873e5cC07",
    "dex_reserves_resolver": "0x05Bd8269A20C472b148246De20E6852091BF16Ff",
    "liquidity": "0x52Aa899454998Be5b000Ad077a46Bbe360F4e497",
    "liquidity_resolver": "0xca13A15de31235A37134B4717021C35A3CF25C60",
    "vault_resolver": "0xA5C3E16523eeeDDcC34706b0E6bE88b4c6EA95cC",
    "lending_resolver": "0x48D32f49aFeAEC7AE66ad7B9264f446fc11a1569",
}

FLUID_ADDRESSES: dict[str, dict[str, str]] = {
    "arbitrum": dict(_FLUID_CORE_CONTRACTS),
    "base": dict(_FLUID_CORE_CONTRACTS),
    "ethereum": dict(_FLUID_CORE_CONTRACTS),
    "polygon": dict(_FLUID_CORE_CONTRACTS),
}

# Fluid pools pair the chain's native gas token directly (no WETH wrapping).
# This sentinel is the token0/token1 value pools report for the native leg.
FLUID_NATIVE_TOKEN = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

# Gas estimates for Fluid operations (Phase-0 measured: swapIn=190,067;
# fToken deposit/withdraw route through the Liquidity layer — conservative
# ceilings, the execution pipeline re-estimates before submission)
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "approve": 46_000,
    "swap": 250_000,
    "supply": 500_000,
    "withdraw": 500_000,
}

# =============================================================================
# Minimal ABIs
# =============================================================================

DEX_FACTORY_ABI = [
    {
        "inputs": [],
        "name": "totalDexes",
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

DEX_RESOLVER_ABI = [
    {
        "inputs": [],
        "name": "getAllDexAddresses",
        "outputs": [{"type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# DexReservesResolver — Fluid's official quoting/enumeration surface.
# estimateSwapIn is declared nonpayable on-chain but is designed to be
# consumed via eth_call (it simulates the swap and reverts internally with
# the FluidDexSwapResult carrier, which the resolver decodes and returns).
DEX_RESERVES_RESOLVER_ABI = [
    {
        "inputs": [],
        "name": "getAllPools",
        "outputs": [
            {
                "components": [
                    {"name": "pool", "type": "address"},
                    {"name": "token0", "type": "address"},
                    {"name": "token1", "type": "address"},
                    {"name": "fee", "type": "uint256"},
                ],
                "type": "tuple[]",
            }
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "dex_", "type": "address"},
            {"name": "swap0to1_", "type": "bool"},
            {"name": "amountIn_", "type": "uint256"},
            {"name": "amountOutMin_", "type": "uint256"},
        ],
        "name": "estimateSwapIn",
        "outputs": [{"name": "amountOut_", "type": "uint256"}],
        "stateMutability": "payable",
        "type": "function",
    },
]

# FluidDexT1 pool — swapIn/swapOut for swaps
DEX_SWAP_ABI = [
    {
        "inputs": [
            {"name": "swap0to1_", "type": "bool"},
            {"name": "amountIn_", "type": "uint256"},
            {"name": "amountOutMin_", "type": "uint256"},
            {"name": "to_", "type": "address"},
        ],
        "name": "swapIn",
        "outputs": [{"name": "amountOut_", "type": "uint256"}],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "swap0to1_", "type": "bool"},
            {"name": "amountOut_", "type": "uint256"},
            {"name": "amountInMax_", "type": "uint256"},
            {"name": "to_", "type": "address"},
        ],
        "name": "swapOut",
        "outputs": [{"name": "amountIn_", "type": "uint256"}],
        "stateMutability": "payable",
        "type": "function",
    },
]

# LendingResolver — fToken enumeration (verified live on arbitrum: 9 fTokens
# incl. fUSDC 0x1A99…6096). Fluid lists exactly one fToken per underlying
# per chain.
LENDING_RESOLVER_ABI = [
    {
        "inputs": [],
        "name": "getAllFTokens",
        "outputs": [{"type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# fTokens are standard ERC-4626 vaults (Phase-0 suite V2). Minimal surface:
# reads for resolution/pre-flight, writes for deposit/withdraw/redeem.
ERC4626_ABI = [
    {
        "inputs": [],
        "name": "asset",
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "maxWithdraw",
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "maxRedeem",
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "shares", "type": "uint256"}],
        "name": "convertToAssets",
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "assets", "type": "uint256"},
            {"name": "receiver", "type": "address"},
        ],
        "name": "deposit",
        "outputs": [{"name": "shares", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "assets", "type": "uint256"},
            {"name": "receiver", "type": "address"},
            {"name": "owner", "type": "address"},
        ],
        "name": "withdraw",
        "outputs": [{"name": "shares", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "shares", "type": "uint256"},
            {"name": "receiver", "type": "address"},
            {"name": "owner", "type": "address"},
        ],
        "name": "redeem",
        "outputs": [{"name": "assets", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class DexPoolData:
    """Data about a Fluid DEX pool.

    Attributes:
        dex_address: Pool contract address
        token0: Token0 address (``FLUID_NATIVE_TOKEN`` for the native leg)
        token1: Token1 address
        fee_raw: Raw fee value as reported by the reserves resolver
            (protocol-internal units — do not assume bps)
        is_smart_collateral: Whether smart collateral is enabled
            (only populated by ``get_dex_data``; LP deposits require it)
        is_smart_debt: Whether smart debt is enabled
    """

    dex_address: str
    token0: str
    token1: str
    fee_raw: int = 0
    is_smart_collateral: bool = False
    is_smart_debt: bool = False


# =============================================================================
# Errors
# =============================================================================


class FluidSDKError(Exception):
    """Raised when a Fluid SDK operation fails."""


class FluidMinAmountError(FluidSDKError):
    """Raised when a swap is rejected by the pool's time-expanding limits.

    Fluid's Liquidity layer enforces withdrawable/borrowable limits that
    expand over time after large utilisation. A swap rejected for limits is
    retryable later — distinct from a hard failure (no pool, bad params).
    """


# Fluid uses one generic custom error per module, each wrapping a uint256
# errorId (see ``contracts/protocols/*/errorTypes.sol`` in
# Instadapp/fluid-contracts-public). Selectors verified Phase 0 (VIB-5028):
FLUID_MODULE_ERROR_SELECTORS: dict[str, str] = {
    "2fee3e0e": "FluidDexError",
    "dee51a8a": "FluidSafeTransferError",
    "60121cca": "FluidVaultError",
    "dcab82e2": "FluidLiquidityError",
    "d50d7512": "FluidLiquidityCalcsError",
    "aeae7c0d": "FluidDexFactoryError",
}

# Revert-carrier "errors" that are actually return values from estimate
# helpers — never surface these as failures.
FLUID_RESULT_CARRIER_SELECTORS: dict[str, str] = {
    "b3bfda99": "FluidDexSwapResult",
    "1458577f": "FluidDexPerfectLiquidityOutput",
}

# DexT1 errorIds (contracts/protocols/dex/errorTypes.sol). Names matter for
# diagnostics; ids absent here render numerically.
DEX_T1_ERROR_IDS: dict[int, str] = {
    51001: "DexT1__AlreadyEntered",
    51002: "DexT1__NotAnAuth",
    51003: "DexT1__SmartColNotEnabled",
    51004: "DexT1__SmartDebtNotEnabled",
    51005: "DexT1__PoolNotInitialized",
    51006: "DexT1__TokenReservesTooLow",
    51007: "DexT1__EthAndAmountInMisMatch",
    51008: "DexT1__EthSentForNonNativeSwap",
    51009: "DexT1__NoSwapRoute",
    51010: "DexT1__NotEnoughAmountOut",
    51011: "DexT1__LiquidityLayerTokenUtilizationCapReached",
    51012: "DexT1__HookReturnedFalse",
    51013: "DexT1__UserSupplyInNotOn",
    51014: "DexT1__UserDebtInNotOn",
    51015: "DexT1__AboveDepositMax",
    51016: "DexT1__MsgValueLowOnDepositOrPayback",
    51017: "DexT1__WithdrawLimitReached",
    51018: "DexT1__BelowWithdrawMin",
    51019: "DexT1__DebtLimitReached",
    51020: "DexT1__BelowBorrowMin",
    51021: "DexT1__AbovePaybackMax",
    51022: "DexT1__InvalidDepositAmts",
    51023: "DexT1__DepositAmtsZero",
    51024: "DexT1__SharesMintedLess",
    51025: "DexT1__WithdrawalNotEnough",
    51026: "DexT1__InvalidWithdrawAmts",
    51027: "DexT1__WithdrawAmtsZero",
    51028: "DexT1__WithdrawExcessSharesBurn",
    51029: "DexT1__InvalidBorrowAmts",
    51030: "DexT1__BorrowAmtsZero",
    51031: "DexT1__BorrowExcessSharesMinted",
    51032: "DexT1__PaybackAmtTooHigh",
    51033: "DexT1__InvalidPaybackAmts",
    51034: "DexT1__PaybackAmtsZero",
    51035: "DexT1__PaybackSharedBurnedLess",
    51036: "DexT1__NothingToArbitrage",
    51037: "DexT1__MsgSenderNotLiquidity",
    51038: "DexT1__ReentrancyBitShouldBeOn",
    51039: "DexT1__OraclePriceFetchAlreadyEntered",
    51040: "DexT1__OracleUpdateHugeSwapDiff",
    51041: "DexT1__Token0ShouldBeSmallerThanToken1",
    51042: "DexT1__OracleMappingOverflow",
    51043: "DexT1__SwapAndArbitragePaused",
    51044: "DexT1__ExceedsAmountInMax",
    51045: "DexT1__SwapInLimitingAmounts",
    51046: "DexT1__SwapOutLimitingAmounts",
    51047: "DexT1__MintAmtOverflow",
    51048: "DexT1__BurnAmtOverflow",
    51049: "DexT1__LimitingAmountsSwapAndNonPerfectActions",
    51050: "DexT1__InsufficientOracleData",
    51051: "DexT1__SharesAmountInsufficient",
    51052: "DexT1__CenterPriceOutOfRange",
    51053: "DexT1__DebtReservesTooLow",
    51054: "DexT1__SwapAndDepositTooLowOrTooHigh",
    51055: "DexT1__WithdrawAndSwapTooLowOrTooHigh",
    51056: "DexT1__BorrowAndSwapTooLowOrTooHigh",
    51057: "DexT1__SwapAndPaybackTooLowOrTooHigh",
    51058: "DexT1__InvalidImplementation",
    51059: "DexT1__OnlyDelegateCallAllowed",
    51060: "DexT1__IncorrectDataLength",
}

# errorIds whose semantic is "the Liquidity layer's time-expanding limits
# (or per-swap caps) currently reject this size" — retryable later or at a
# smaller size, NOT a hard integration failure.
_LIMIT_GATED_ERROR_IDS: frozenset[int] = frozenset(
    {
        51006,  # TokenReservesTooLow
        51009,  # NoSwapRoute
        51010,  # NotEnoughAmountOut
        51011,  # LiquidityLayerTokenUtilizationCapReached
        51017,  # WithdrawLimitReached
        51019,  # DebtLimitReached
        51045,  # SwapInLimitingAmounts
        51046,  # SwapOutLimitingAmounts
        51049,  # LimitingAmountsSwapAndNonPerfectActions
    }
)


def decode_fluid_revert(raw_hex: str) -> str:
    """Decode a Fluid revert into a human-readable error message.

    Handles standard ``Error(string)`` / ``Panic(uint256)``, Fluid's
    per-module ``<Module>Error(uint256 errorId)`` wrappers (with DexT1
    errorId names), and the estimate result-carriers.

    Args:
        raw_hex: Raw hex revert data (with or without 0x prefix)

    Returns:
        Human-readable error message
    """
    data = raw_hex.removeprefix("0x")
    if len(data) < 8:
        return f"Unknown revert: 0x{data}"

    selector = data[:8].lower()

    # Standard Solidity Error(string)
    if selector == "08c379a0" and len(data) >= 136:
        try:
            offset = int(data[8:72], 16)
            length = int(data[72:136], 16)
            start = 8 + offset * 2 + 64  # selector + offset word + length word
            message_hex = data[start : start + length * 2]
            return bytes.fromhex(message_hex).decode("utf-8", errors="replace")
        except (ValueError, IndexError):
            pass  # Fall through to unknown

    # Standard Solidity Panic(uint256)
    if selector == "4e487b71":
        if len(data) >= 72:
            panic_code = int(data[8:72], 16)
            return f"Solidity Panic(0x{panic_code:02x})"
        return "Solidity Panic"

    carrier = FLUID_RESULT_CARRIER_SELECTORS.get(selector)
    if carrier is not None:
        return f"{carrier} (estimate result carrier, not a failure)"

    module_error = FLUID_MODULE_ERROR_SELECTORS.get(selector)
    if module_error is not None and len(data) >= 72:
        error_id = int(data[8:72], 16)
        name = DEX_T1_ERROR_IDS.get(error_id)
        if name is not None:
            return f"{module_error}({error_id} {name})"
        return f"{module_error}(errorId={error_id})"

    return f"Unknown revert (selector=0x{selector}): 0x{data[:40]}..."


def fluid_error_id(raw_hex: str) -> int | None:
    """Extract the uint256 errorId from a Fluid module-error revert.

    Returns None for non-Fluid reverts (Error(string), Panic, carriers,
    unknown selectors).
    """
    data = raw_hex.removeprefix("0x")
    if len(data) < 72:
        return None
    if data[:8].lower() not in FLUID_MODULE_ERROR_SELECTORS:
        return None
    try:
        return int(data[8:72], 16)
    except ValueError:
        return None


def fluid_error_module(raw_hex: str) -> str | None:
    """Name of the Fluid module error wrapping this revert (or None).

    Each Fluid module numbers its errorIds independently, so an errorId is
    only meaningful TOGETHER with its module selector — e.g. DexT1 id 51049
    (limit-gated swap) must not be conflated with a numerically equal id
    from FluidVaultError/FluidLiquidityError.
    """
    data = raw_hex.removeprefix("0x")
    if len(data) < 8:
        return None
    return FLUID_MODULE_ERROR_SELECTORS.get(data[:8].lower())


def _extract_revert_hex(error: Exception) -> str | None:
    """Extract raw hex revert data from a Web3 error.

    Checks structured fields first (``error.data``, args dicts from
    Alchemy/Infura-style providers), then falls back to regex extraction
    from the error message text.
    """
    import re

    data_attr = getattr(error, "data", None)
    if isinstance(data_attr, str) and data_attr.startswith("0x"):
        return data_attr
    if error.args and isinstance(error.args[0], dict):
        data = error.args[0].get("data")
        if isinstance(data, str) and data.startswith("0x"):
            return data
    match = re.search(r"(0x[0-9a-fA-F]{8,})", str(error))
    if match:
        return match.group(1)
    return None


# =============================================================================
# FluidSDK
# =============================================================================


class FluidSDK:
    """Low-level Fluid DEX protocol SDK.

    Pool enumeration and swap quoting go through the DexReservesResolver
    (single eth_call each); token-pair / smart-flag verification reads the
    pool's ``constantsView()`` raw words.

    Args:
        chain: Chain name (one of ``FLUID_ADDRESSES``)
        rpc_url: DEPRECATED — direct RPC URL. Bypasses the gateway and is
            only used for ad-hoc scripts. Prefer gateway_client for any
            code path that runs in a strategy container.
        gateway_client: Gateway client used to route all eth_call traffic
            through the gateway's RpcService. Preferred over rpc_url for
            production code paths.
    """

    def __init__(
        self,
        chain: str,
        rpc_url: str | None = None,
        gateway_client: "GatewayClient | None" = None,
    ) -> None:
        chain_lower = chain.lower()
        if chain_lower not in FLUID_ADDRESSES:
            raise FluidSDKError(f"Fluid DEX not supported on chain: {chain}. Supported: {list(FLUID_ADDRESSES.keys())}")
        if rpc_url is None and gateway_client is None:
            raise FluidSDKError("FluidSDK requires either rpc_url (deprecated) or gateway_client")

        self.chain = chain_lower
        self.rpc_url = rpc_url
        self._gateway_client = gateway_client
        if gateway_client is not None:
            from almanak.framework.web3.gateway_provider import GatewayWeb3Provider

            self.w3 = Web3(GatewayWeb3Provider(gateway_client, chain=chain_lower))
        else:
            self.w3 = Web3(HTTPProvider(rpc_url))  # vib-2986-exempt: gateway-internal fallback
        self._addresses = FLUID_ADDRESSES[chain_lower]

        self._factory = self.w3.eth.contract(
            address=Web3.to_checksum_address(self._addresses["dex_factory"]),
            abi=DEX_FACTORY_ABI,
        )
        self._resolver = self.w3.eth.contract(
            address=Web3.to_checksum_address(self._addresses["dex_resolver"]),
            abi=DEX_RESOLVER_ABI,
        )
        self._reserves_resolver = self.w3.eth.contract(
            address=Web3.to_checksum_address(self._addresses["dex_reserves_resolver"]),
            abi=DEX_RESERVES_RESOLVER_ABI,
        )

        # Cache function selectors for raw pool reads
        self._constants_view_sel = self.w3.keccak(text="constantsView()")[:4].hex()
        self._read_storage_sel = self.w3.keccak(text="readFromStorage(bytes32)")[:4].hex()

        # Pool list is instance-cached: SDK instances are short-lived (one per
        # compile / quote), and pool discovery + quoting may enumerate more
        # than once within that window. New pool deployments are rare enough
        # that an instance-lifetime cache cannot serve a stale answer that
        # matters; a fresh SDK always re-reads.
        self._pools_cache: list[DexPoolData] | None = None
        # fToken enumeration + underlying->fToken map (same lifetime rationale).
        self._ftokens_cache: list[str] | None = None
        self._ftoken_by_underlying_cache: dict[str, str] | None = None

    # =========================================================================
    # Pool enumeration / discovery
    # =========================================================================

    def get_all_dex_addresses(self) -> list[str]:
        """Get all Fluid DEX pool addresses from the resolver."""
        try:
            addresses = self._resolver.functions.getAllDexAddresses().call()
            return [Web3.to_checksum_address(a) for a in addresses]
        except Exception as e:
            raise FluidSDKError(f"Failed to get DEX addresses: {e}") from e

    def get_total_dexes(self) -> int:
        """Get the total number of Fluid DEX pools."""
        try:
            return self._factory.functions.totalDexes().call()
        except Exception as e:
            raise FluidSDKError(f"Failed to get total DEXes: {e}") from e

    def get_all_pools(self) -> list[DexPoolData]:
        """Enumerate all pools with token pairs in a single eth_call.

        Uses ``DexReservesResolver.getAllPools()``. Smart-collateral/debt
        flags are NOT populated here (use ``get_dex_data`` per pool when
        needed); ``fee_raw`` is the resolver-reported raw fee value.
        The result is cached for the lifetime of this SDK instance.
        """
        if self._pools_cache is not None:
            return self._pools_cache
        try:
            rows = self._reserves_resolver.functions.getAllPools().call()
        except Exception as e:
            raise FluidSDKError(f"Failed to enumerate Fluid pools: {e}") from e
        pools: list[DexPoolData] = []
        for row in rows:
            pool, token0, token1, fee = row
            pools.append(
                DexPoolData(
                    dex_address=Web3.to_checksum_address(pool),
                    token0=Web3.to_checksum_address(token0),
                    token1=Web3.to_checksum_address(token1),
                    fee_raw=int(fee),
                )
            )
        self._pools_cache = pools
        return pools

    def find_pool_for_pair(self, token_in: str, token_out: str) -> tuple[str, bool] | None:
        """Find the Fluid pool for a swap pair and the swap direction.

        Args:
            token_in: Input token address (``FLUID_NATIVE_TOKEN`` for native)
            token_out: Output token address

        Returns:
            ``(pool_address, swap0to1)`` where ``swap0to1`` is True when
            ``token_in`` is the pool's token0; None when no pool exists.
        """
        tin = token_in.lower()
        tout = token_out.lower()
        for pool in self.get_all_pools():
            t0 = pool.token0.lower()
            t1 = pool.token1.lower()
            if t0 == tin and t1 == tout:
                return pool.dex_address, True
            if t0 == tout and t1 == tin:
                return pool.dex_address, False
        return None

    def find_dex_by_tokens(self, token0: str, token1: str) -> str | None:
        """Find a Fluid DEX pool for a token pair (order-insensitive)."""
        found = self.find_pool_for_pair(token0, token1)
        return found[0] if found is not None else None

    def get_dex_data(self, dex_address: str) -> DexPoolData:
        """Get pool data by calling constantsView() and readFromStorage().

        Uses raw eth_call to avoid ABI decoding issues with the complex
        DexEntireData struct. constantsView() returns 18 words:
        - word[9]: token0 address
        - word[10]: token1 address

        readFromStorage(bytes32(0)) returns dexVariables:
        - bit 1: isSmartCollateralEnabled
        - bit 2: isSmartDebtEnabled
        """
        addr = Web3.to_checksum_address(dex_address)

        try:
            cv_data = self.w3.eth.call(
                {
                    "to": addr,
                    "data": bytes.fromhex(self._constants_view_sel),
                }
            )

            if len(cv_data) < 11 * 32:
                raise FluidSDKError(f"constantsView() returned only {len(cv_data)} bytes, expected >= {11 * 32}")

            token0 = Web3.to_checksum_address("0x" + cv_data[9 * 32 + 12 : 10 * 32].hex())
            token1 = Web3.to_checksum_address("0x" + cv_data[10 * 32 + 12 : 11 * 32].hex())

            storage_data = self.w3.eth.call(
                {
                    "to": addr,
                    "data": bytes.fromhex(self._read_storage_sel + "00" * 32),
                }
            )
            if len(storage_data) < 32:
                raise FluidSDKError(f"readFromStorage() returned only {len(storage_data)} bytes, expected >= 32")
            dex_vars = int.from_bytes(storage_data[:32], "big")

            is_smart_col = bool((dex_vars >> 1) & 1)
            is_smart_debt = bool((dex_vars >> 2) & 1)

            return DexPoolData(
                dex_address=addr,
                token0=token0,
                token1=token1,
                fee_raw=0,  # not exposed by constantsView; use get_all_pools
                is_smart_collateral=is_smart_col,
                is_smart_debt=is_smart_debt,
            )
        except FluidSDKError:
            raise
        except Exception as e:
            raise FluidSDKError(f"Failed to get DEX data for {dex_address}: {e}") from e

    # =========================================================================
    # Swap quote + calldata
    # =========================================================================

    def get_swap_quote(
        self,
        dex_address: str,
        swap0to1: bool,
        amount_in: int,
    ) -> int:
        """Quote an exact-input swap via ``DexReservesResolver.estimateSwapIn``.

        This is Fluid's official quote path; Phase-0 validation showed the
        quote matches real ``swapIn`` output to the wei.

        Args:
            dex_address: Pool contract address
            swap0to1: True to swap token0->token1, False for token1->token0
            amount_in: Input amount in the token's smallest unit

        Returns:
            Expected output amount in the token's smallest unit

        Raises:
            FluidMinAmountError: The size is rejected by the pool's
                time-expanding limits (retryable later / at smaller size).
            FluidSDKError: Any other quote failure.
        """
        pool_addr = Web3.to_checksum_address(dex_address)
        try:
            amount_out = self._reserves_resolver.functions.estimateSwapIn(pool_addr, swap0to1, amount_in, 0).call()
        except Exception as e:
            raw_hex = _extract_revert_hex(e)
            if raw_hex:
                decoded = decode_fluid_revert(raw_hex)
                error_id = fluid_error_id(raw_hex)
                # _LIMIT_GATED_ERROR_IDS are DexT1 ids — only meaningful on a
                # FluidDexError revert (modules number errorIds independently).
                if error_id in _LIMIT_GATED_ERROR_IDS and fluid_error_module(raw_hex) == "FluidDexError":
                    raise FluidMinAmountError(
                        f"Fluid pool {pool_addr} rejected this swap size: {decoded}. "
                        f"Fluid limits expand over time — retry later or reduce size."
                    ) from e
                raise FluidSDKError(f"Fluid swap quote failed: {decoded}") from e
            raise FluidSDKError(f"Failed to get swap quote: {e}") from e
        if amount_out == 0:
            # The resolver returns 0 (rather than reverting) for sizes beyond
            # the current limits — observed at Phase 0 for a $50M quote.
            raise FluidMinAmountError(
                f"Fluid pool {pool_addr} returned a zero quote for amount_in={amount_in} — "
                f"size exceeds current pool limits. Fluid limits expand over time — "
                f"retry later or reduce size."
            )
        return int(amount_out)

    def encode_swap_in_calldata(
        self,
        swap0to1: bool,
        amount_in: int,
        amount_out_min: int,
        to: str,
    ) -> str:
        """ABI-encode ``swapIn`` calldata (offline — no RPC interaction)."""
        contract = Web3().eth.contract(abi=DEX_SWAP_ABI)
        return contract.encode_abi("swapIn", args=[swap0to1, amount_in, amount_out_min, Web3.to_checksum_address(to)])

    def build_swap_tx(
        self,
        dex_address: str,
        swap0to1: bool,
        amount_in: int,
        amount_out_min: int,
        to: str,
        value: int = 0,
    ) -> dict[str, Any]:
        """Build a swapIn transaction for a Fluid DEX pool.

        Args:
            dex_address: Pool contract address
            swap0to1: True to swap token0->token1, False for token1->token0
            amount_in: Input amount in token's smallest unit
            amount_out_min: Minimum acceptable output (slippage protection)
            to: Recipient address
            value: Native token value (must equal amount_in for native-input
                swaps; 0 for ERC-20 inputs)

        Returns:
            Transaction dict with 'to', 'data', 'value', 'gas'
        """
        return {
            "to": Web3.to_checksum_address(dex_address),
            "data": self.encode_swap_in_calldata(swap0to1, amount_in, amount_out_min, to),
            "value": value,
            "gas": DEFAULT_GAS_ESTIMATES["swap"],
        }

    # =========================================================================
    # fToken lending (ERC-4626) — VIB-5030
    # =========================================================================

    def _ftoken_contract(self, ftoken_address: str):
        return self.w3.eth.contract(address=Web3.to_checksum_address(ftoken_address), abi=ERC4626_ABI)

    def get_all_ftokens(self) -> list[str]:
        """Enumerate the chain's Fluid fTokens via the LendingResolver.

        The result is cached for the lifetime of this SDK instance (fToken
        listings change rarely; SDK instances are per-compile).
        """
        if self._ftokens_cache is not None:
            return self._ftokens_cache
        resolver = self.w3.eth.contract(
            address=Web3.to_checksum_address(self._addresses["lending_resolver"]),
            abi=LENDING_RESOLVER_ABI,
        )
        try:
            addresses = resolver.functions.getAllFTokens().call()
        except Exception as e:
            raise FluidSDKError(f"Failed to enumerate Fluid fTokens: {e}") from e
        self._ftokens_cache = [Web3.to_checksum_address(a) for a in addresses]
        return self._ftokens_cache

    def find_ftoken_for_underlying(self, underlying: str) -> str | None:
        """Resolve the fToken whose ERC-4626 ``asset()`` is ``underlying``.

        Fluid lists exactly one fToken per underlying per chain, so the
        first match is THE market. Returns None when Fluid has no market
        for the asset on this chain.
        """
        target = underlying.lower()
        if self._ftoken_by_underlying_cache is None:
            mapping: dict[str, str] = {}
            for ftoken in self.get_all_ftokens():
                try:
                    asset = self._ftoken_contract(ftoken).functions.asset().call()
                except Exception as e:
                    raise FluidSDKError(f"Failed to read asset() of Fluid fToken {ftoken}: {e}") from e
                mapping[str(asset).lower()] = ftoken
            self._ftoken_by_underlying_cache = mapping
        return self._ftoken_by_underlying_cache.get(target)

    def get_max_withdraw(self, ftoken_address: str, owner: str) -> int:
        """ERC-4626 ``maxWithdraw(owner)`` — reflects Fluid's time-expanding
        withdrawal limits, NOT just the owner's balance. The compile-time
        pre-flight reads this to distinguish "limit-gated, retry later" from
        a hard failure."""
        try:
            return int(
                self._ftoken_contract(ftoken_address).functions.maxWithdraw(Web3.to_checksum_address(owner)).call()
            )
        except Exception as e:
            raise FluidSDKError(f"Failed to read maxWithdraw on Fluid fToken {ftoken_address}: {e}") from e

    def get_max_redeem(self, ftoken_address: str, owner: str) -> int:
        """ERC-4626 ``maxRedeem(owner)`` in shares (limit-aware)."""
        try:
            return int(
                self._ftoken_contract(ftoken_address).functions.maxRedeem(Web3.to_checksum_address(owner)).call()
            )
        except Exception as e:
            raise FluidSDKError(f"Failed to read maxRedeem on Fluid fToken {ftoken_address}: {e}") from e

    def get_ftoken_share_balance(self, ftoken_address: str, owner: str) -> int:
        """fToken share balance of ``owner`` (ERC-20 balanceOf on the vault)."""
        try:
            return int(
                self._ftoken_contract(ftoken_address).functions.balanceOf(Web3.to_checksum_address(owner)).call()
            )
        except Exception as e:
            raise FluidSDKError(f"Failed to read fToken balance on {ftoken_address}: {e}") from e

    def convert_to_assets(self, ftoken_address: str, shares: int) -> int:
        """ERC-4626 ``convertToAssets(shares)`` — underlying value of shares."""
        try:
            return int(self._ftoken_contract(ftoken_address).functions.convertToAssets(shares).call())
        except Exception as e:
            raise FluidSDKError(f"Failed to read convertToAssets on {ftoken_address}: {e}") from e

    def build_deposit_tx(self, ftoken_address: str, assets: int, receiver: str) -> dict[str, Any]:
        """Build an ERC-4626 ``deposit(assets, receiver)`` transaction."""
        contract = Web3().eth.contract(abi=ERC4626_ABI)
        return {
            "to": Web3.to_checksum_address(ftoken_address),
            "data": contract.encode_abi("deposit", args=[assets, Web3.to_checksum_address(receiver)]),
            "value": 0,
            "gas": DEFAULT_GAS_ESTIMATES["supply"],
        }

    def build_withdraw_tx(self, ftoken_address: str, assets: int, receiver: str, owner: str) -> dict[str, Any]:
        """Build an ERC-4626 ``withdraw(assets, receiver, owner)`` transaction."""
        contract = Web3().eth.contract(abi=ERC4626_ABI)
        return {
            "to": Web3.to_checksum_address(ftoken_address),
            "data": contract.encode_abi(
                "withdraw",
                args=[assets, Web3.to_checksum_address(receiver), Web3.to_checksum_address(owner)],
            ),
            "value": 0,
            "gas": DEFAULT_GAS_ESTIMATES["withdraw"],
        }

    def build_redeem_tx(self, ftoken_address: str, shares: int, receiver: str, owner: str) -> dict[str, Any]:
        """Build an ERC-4626 ``redeem(shares, receiver, owner)`` transaction.

        Full exits go through redeem-by-shares (never withdraw-by-assets):
        burning the exact share balance cannot strand rounding dust.
        """
        contract = Web3().eth.contract(abi=ERC4626_ABI)
        return {
            "to": Web3.to_checksum_address(ftoken_address),
            "data": contract.encode_abi(
                "redeem",
                args=[shares, Web3.to_checksum_address(receiver), Web3.to_checksum_address(owner)],
            ),
            "value": 0,
            "gas": DEFAULT_GAS_ESTIMATES["withdraw"],
        }
