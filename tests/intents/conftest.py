"""Shared fixtures and helpers for Intent tests.

This module provides common infrastructure for all per-chain Intent tests:
- Chain configuration (tokens, balance slots, RPC URLs)
- Anvil auto-management (start/stop per test session)
- Wallet funding utilities
- Token balance helpers
- Web3 connection management
- Price oracle with CoinGecko
"""

import os
import time
import weakref
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
import requests
from web3 import Web3
from web3.exceptions import TimeExhausted
from web3.providers.rpc.async_rpc import AsyncHTTPProvider

# =============================================================================
# Test Timeouts (Fail Fast)
# =============================================================================

# Local Anvil RPC calls should return quickly; when they don't, the fork is usually stalled.
# Keep these defaults aggressive to avoid 10+ minute cascades when an Anvil instance hangs.
TEST_RPC_CONNECT_TIMEOUT_SECONDS = float(os.environ.get("ALMANAK_TEST_RPC_CONNECT_TIMEOUT_SECONDS", "3"))
TEST_RPC_READ_TIMEOUT_SECONDS = float(os.environ.get("ALMANAK_TEST_RPC_READ_TIMEOUT_SECONDS", "10"))
TEST_WEB3_DEFAULT_HTTP_TIMEOUT_SECONDS = float(os.environ.get("ALMANAK_TEST_WEB3_HTTP_TIMEOUT_SECONDS", "10"))
TEST_CAST_TIMEOUT_SECONDS = float(os.environ.get("ALMANAK_TEST_CAST_TIMEOUT_SECONDS", "15"))

# ExecutionOrchestrator / Submitter confirmation timeout (upper bound for receipt polling).
TEST_TX_TIMEOUT_SECONDS = float(os.environ.get("ALMANAK_TEST_TX_TIMEOUT_SECONDS", "30"))

# When local Anvil stalls, retries just waste time. Keep to 0 by default for intent tests.
TEST_SUBMITTER_MAX_RETRIES = int(os.environ.get("ALMANAK_TEST_SUBMITTER_MAX_RETRIES", "0"))

# requests-style (connect, read) timeouts for sync Web3 HTTPProvider
TEST_WEB3_REQUEST_TIMEOUT = (TEST_RPC_CONNECT_TIMEOUT_SECONDS, TEST_RPC_READ_TIMEOUT_SECONDS)

# Retry config for Anvil RPC calls during wallet funding.
# Only applies to setup-time RPC calls (anvil_setBalance, anvil_setStorageAt, evm_mine),
# NOT to test-time execution. Zero overhead on happy path.
TEST_FUNDING_RPC_MAX_RETRIES = int(os.environ.get("ALMANAK_TEST_FUNDING_RPC_MAX_RETRIES", "3"))
TEST_FUNDING_RPC_BACKOFF_SECONDS = float(os.environ.get("ALMANAK_TEST_FUNDING_RPC_BACKOFF_SECONDS", "2.0"))

# Health check timeout for recovery path (generous, since the fork is already degraded).
TEST_RECOVERY_HEALTH_TIMEOUT_SECONDS = float(
    os.environ.get("ALMANAK_TEST_RECOVERY_HEALTH_TIMEOUT_SECONDS", "15.0")
)

# Fixed Anvil recovery policy for intent tests
TEST_ANVIL_RECOVERY_MAX_RESTARTS = 2
TEST_ANVIL_RECOVERY_SETTLE_SECONDS = 0.5
TEST_ANVIL_RECOVERY_PROBE_TIMEOUT_SECONDS = 3.0
TEST_ANVIL_PROBE_SENTINEL_WALLET = "0x000000000000000000000000000000000000dEaD"

# =============================================================================
# Constants
# =============================================================================

# Default max slippage for swap intent tests (20%).
# High tolerance because CoinGecko oracle prices can diverge from on-chain pool prices.
SWAP_MAX_SLIPPAGE = Decimal("0.20")

# Default Anvil port
ANVIL_PORT = 8545
ANVIL_URL = f"http://localhost:{ANVIL_PORT}"

# Chain configurations
CHAIN_CONFIGS = {
    "base": {
        "rpc_url": "https://mainnet.base.org",
        "chain_id": 8453,
        "alchemy_key": "base",
        "tokens": {
            "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "WETH": "0x4200000000000000000000000000000000000006",
            "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
            "wstETH": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
        },
        "balance_slots": {
            "USDC": 9,
            "WETH": 3,
            "USDbC": 9,
            "wstETH": 1,
        },
    },
    "avalanche": {
        "rpc_url": "https://api.avax.network/ext/bc/C/rpc",
        "chain_id": 43114,
        "alchemy_key": "avax",
        "tokens": {
            "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            "USDT": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
        },
        "balance_slots": {
            "USDC": 9,
            "WAVAX": 3,
            "USDT": 2,
        },
    },
    "ethereum": {
        "rpc_url": "https://eth.llamarpc.com",
        "chain_id": 1,
        "alchemy_key": "eth",
        "tokens": {
            "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            "wstETH": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        },
        "balance_slots": {
            "USDC": 9,
            "WETH": 3,
            "USDT": 2,
            "wstETH": 0,
        },
    },
    "arbitrum": {
        "rpc_url": "https://arb1.arbitrum.io/rpc",
        "chain_id": 42161,
        "alchemy_key": "arb",
        "tokens": {
            "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        },
        "balance_slots": {
            "USDC": 9,
            "WETH": 51,
            "USDT": 51,
        },
    },
    "optimism": {
        "rpc_url": "https://mainnet.optimism.io",
        "chain_id": 10,
        "alchemy_key": "opt",
        "tokens": {
            "USDC": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
            "WETH": "0x4200000000000000000000000000000000000006",
            "USDT": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
        },
        "balance_slots": {
            "USDC": 9,
            "WETH": 3,
            "USDT": 2,
        },
    },
    "polygon": {
        "rpc_url": "https://polygon-rpc.com",
        "chain_id": 137,
        "alchemy_key": "polygon",
        "tokens": {
            "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
            "WETH": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
            "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        },
        "balance_slots": {
            "USDC": 9,
            "WETH": 0,  # UChildERC20Proxy (PoS bridge): _balances is slot 0 in ERC20 base
            "USDT": 0,  # UChildERC20Proxy (PoS bridge): _balances is slot 0 in ERC20 base
        },
    },
    "bsc": {
        "rpc_url": "https://bsc-dataseed.binance.org",
        "chain_id": 56,
        "alchemy_key": "bnb",
        "tokens": {
            "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
            "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
            "USDT": "0x55d398326f99059fF775485246999027B3197955",
        },
        "balance_slots": {
            "USDC": 1,  # Binance-Peg USDC uses slot 1
            "WBNB": 3,
            "USDT": 1,  # Binance-Peg USDT uses slot 1
        },
    },
    "bnb": {  # Alias for bsc (canonical name used by framework)
        "rpc_url": "https://bsc-dataseed.binance.org",
        "chain_id": 56,
        "alchemy_key": "bnb",
        "tokens": {
            "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
            "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
            "USDT": "0x55d398326f99059fF775485246999027B3197955",
        },
        "balance_slots": {
            "USDC": 1,  # Binance-Peg USDC uses slot 1
            "WBNB": 3,
            "USDT": 1,  # Binance-Peg USDT uses slot 1
        },
    },
    "linea": {
        "rpc_url": "https://rpc.linea.build",
        "chain_id": 59144,
        "alchemy_key": "linea",
        "tokens": {
            "USDC": "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
            "WETH": "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
            "USDT": "0xA219439258ca9da29E9Cc4cE5596924745e12B93",
        },
        "balance_slots": {
            "USDC": 0,
            "WETH": 0,
            "USDT": 0,
        },
    },
    "blast": {
        "rpc_url": "https://rpc.blast.io",
        "chain_id": 81457,
        "alchemy_key": None,  # No Alchemy support, uses public RPC
        "tokens": {
            "USDB": "0x4300000000000000000000000000000000000003",
            "WETH": "0x4300000000000000000000000000000000000004",
        },
        "balance_slots": {
            "USDB": 0,
            "WETH": 0,
        },
    },
    "mantle": {
        "rpc_url": "https://rpc.mantle.xyz",
        "chain_id": 5000,
        "alchemy_key": None,  # No Alchemy support, uses public RPC
        "tokens": {
            "WMNT": "0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
            "USDC": "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9",
            "WETH": "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111",
            "USDT": "0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE",
        },
        "balance_slots": {
            "WMNT": 0,  # Unused — wraps from native MNT
            "USDC": 9,  # Bridged USDC uses slot 9 (verified via cast index + cast storage)
            "WETH": 0,  # L2 predeploy WETH uses slot 0
            "USDT": 0,  # Bridged USDT uses slot 0
        },
    },
}
# Import Anvil fixtures and constants from shared gateway conftest.
# Note: We do NOT import CHAIN_CONFIGS from conftest_gateway to avoid conflict with local definition
from tests.conftest_gateway import (
    CHAIN_ANVIL_PORTS,
    TEST_PRIVATE_KEY,
    TEST_WALLET,
    # Anvil fixtures (session-scoped, auto-started)
    anvil_arbitrum,
    anvil_avalanche,
    anvil_base,
    anvil_bsc,
    anvil_ethereum,
    anvil_optimism,
    anvil_polygon,
    get_anvil_rpc_url,
)

# Re-export for test files that import from this module
__all__ = [
    # Anvil fixtures (auto-started, session-scoped)
    "anvil_arbitrum",
    "anvil_avalanche",
    "anvil_base",
    "anvil_bsc",
    "anvil_ethereum",
    "anvil_optimism",
    "anvil_polygon",
    # Price oracle fixtures (session-scoped per chain)
    "price_oracle_arbitrum",
    "price_oracle_avalanche",
    "price_oracle_base",
    "price_oracle_bsc",
    "price_oracle_bnb",
    "price_oracle_ethereum",
    "price_oracle_optimism",
    "price_oracle_polygon",
    # Utilities
    "fund_native_token",
    "fund_erc20_token",
    "get_anvil_rpc_url",
    "is_anvil_running",
    # Constants
    "TEST_WALLET",
    "TEST_PRIVATE_KEY",
    "TEST_RPC_CONNECT_TIMEOUT_SECONDS",
    "TEST_RPC_READ_TIMEOUT_SECONDS",
    "TEST_WEB3_DEFAULT_HTTP_TIMEOUT_SECONDS",
    "TEST_WEB3_REQUEST_TIMEOUT",
    "TEST_CAST_TIMEOUT_SECONDS",
    "TEST_TX_TIMEOUT_SECONDS",
    "TEST_SUBMITTER_MAX_RETRIES",
    "SWAP_MAX_SLIPPAGE",
    "CHAIN_CONFIGS",
    "CHAIN_ANVIL_PORTS",
    # Helper functions
    "get_token_balance",
    "get_token_decimals",
    "format_token_amount",
]

# ERC20 ABI for balance/allowance checks
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "remaining", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
]




# =============================================================================
# Helper Functions
# =============================================================================

_ASYNC_HTTP_PROVIDERS: weakref.WeakSet[AsyncHTTPProvider] = weakref.WeakSet()


def _enable_async_http_provider_tracking() -> None:
    """Track AsyncHTTPProvider instances so we can close leaked aiohttp sessions.

    Web3's AsyncHTTPProvider caches aiohttp ClientSessions per event loop.
    In intent tests we create lots of short-lived submitters/providers; if those
    sessions aren't closed, we can end up with many open connections and noisy
    "Unclosed client session" warnings (and, in worst cases, resource pressure).
    """

    if getattr(AsyncHTTPProvider, "_almanak_tracking_enabled", False):
        return

    original_init = AsyncHTTPProvider.__init__

    def tracked_init(self, *args: Any, **kwargs: Any) -> None:  # type: ignore[no-untyped-def]
        original_init(self, *args, **kwargs)
        _ASYNC_HTTP_PROVIDERS.add(self)

    AsyncHTTPProvider.__init__ = tracked_init  # type: ignore[method-assign]
    AsyncHTTPProvider._almanak_tracking_enabled = True  # type: ignore[attr-defined]


_enable_async_http_provider_tracking()


@pytest.fixture(scope="session", autouse=True)
def configure_web3_default_http_timeout():
    """Reduce Web3's default HTTP timeout for intent tests.

    Web3 defaults to 30s per HTTP request. When a forked Anvil instance stalls,
    those 30s timeouts compound across many RPC calls and make failures take
    minutes. In intent tests we prefer failing fast and restarting Anvil.
    """
    try:
        from web3._utils import http as web3_http
    except Exception:
        yield
        return

    original = web3_http.DEFAULT_HTTP_TIMEOUT
    web3_http.DEFAULT_HTTP_TIMEOUT = TEST_WEB3_DEFAULT_HTTP_TIMEOUT_SECONDS
    try:
        yield
    finally:
        web3_http.DEFAULT_HTTP_TIMEOUT = original


def make_intent_test_web3(rpc_url: str) -> Web3:
    """Create a Web3 HTTP provider using intent-test timeout defaults."""
    return Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": TEST_WEB3_REQUEST_TIMEOUT}))


def _is_timeout_chain_error(error: BaseException) -> bool:
    """Return True if error/cause chain indicates an RPC timeout."""
    current: BaseException | None = error
    visited: set[int] = set()

    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, TimeoutError | requests.exceptions.Timeout):
            return True
        message = str(current).lower()
        if "read timed out" in message or "timed out" in message:
            return True
        current = current.__cause__ or current.__context__

    return False


def _rpc_response_success(response: Any) -> bool:
    """Return True when a JSON-RPC response does not contain an error."""
    if isinstance(response, dict):
        return "error" not in response
    return True


def _probe_anvil_admin_rpc(rpc_url: str) -> bool:
    """Probe admin RPC methods required by intent fixture seeding.

    A healthy fork for our setup path must answer both anvil_setBalance and
    evm_mine, not just eth_chainId/eth_blockNumber.
    """
    probe_timeout = (
        min(TEST_RPC_CONNECT_TIMEOUT_SECONDS, TEST_ANVIL_RECOVERY_PROBE_TIMEOUT_SECONDS),
        TEST_ANVIL_RECOVERY_PROBE_TIMEOUT_SECONDS,
    )
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": probe_timeout}))
    if not w3.is_connected():
        return False

    set_balance_resp = w3.provider.make_request("anvil_setBalance", [TEST_ANVIL_PROBE_SENTINEL_WALLET, "0x0"])
    if not _rpc_response_success(set_balance_resp):
        return False

    mine_resp = w3.provider.make_request("evm_mine", [])
    return _rpc_response_success(mine_resp)


def _force_restart_anvil(anvil_instance: Any, chain_name: str, attempt: int) -> tuple[bool, str]:
    """Force-restart an Anvil fixture and verify admin RPC readiness."""
    restart = getattr(anvil_instance, "restart", None)
    get_rpc_url = getattr(anvil_instance, "get_rpc_url", None)

    if not callable(restart) or not callable(get_rpc_url):
        print(f"WARNING: {chain_name} recovery attempt {attempt}: missing restart/get_rpc_url on fixture")
        return (False, "")

    print(f"WARNING: {chain_name} recovery attempt {attempt}/{TEST_ANVIL_RECOVERY_MAX_RESTARTS}: forcing Anvil restart")
    restarted = restart(health_timeout_seconds=TEST_RECOVERY_HEALTH_TIMEOUT_SECONDS)
    if not restarted:
        print(f"WARNING: {chain_name} recovery attempt {attempt}: Anvil restart failed")
        return (False, "")

    time.sleep(TEST_ANVIL_RECOVERY_SETTLE_SECONDS)
    recovered_rpc_url = get_rpc_url()
    try:
        admin_ready = _probe_anvil_admin_rpc(recovered_rpc_url)
    except Exception as e:
        print(f"WARNING: {chain_name} recovery attempt {attempt}: admin RPC probe raised {type(e).__name__}: {e}")
        return (False, recovered_rpc_url)

    if not admin_ready:
        print(f"WARNING: {chain_name} recovery attempt {attempt}: admin RPC probe failed at {recovered_rpc_url}")
        return (False, recovered_rpc_url)

    print(f"WARNING: {chain_name} recovery attempt {attempt}: restart+probe succeeded at {recovered_rpc_url}")
    return (True, recovered_rpc_url)


def seed_wallet_state_with_recovery(
    *,
    seed_wallet_state: Callable[[Web3, str], str],
    web3: Web3,
    rpc_url: str,
    anvil_instance: Any,
    chain_name: str,
) -> str:
    """Seed wallet state with forced restart recovery on local Anvil timeout."""
    active_web3 = web3
    active_rpc_url = rpc_url
    last_timeout_error: Exception | None = None

    for attempt in range(TEST_ANVIL_RECOVERY_MAX_RESTARTS + 1):
        try:
            return seed_wallet_state(active_web3, active_rpc_url)
        except Exception as e:
            if not _is_timeout_chain_error(e):
                raise
            last_timeout_error = e

            if attempt >= TEST_ANVIL_RECOVERY_MAX_RESTARTS:
                break

            restart_attempt = attempt + 1
            restarted, recovered_rpc_url = _force_restart_anvil(anvil_instance, chain_name, restart_attempt)
            if not restarted:
                continue

            active_rpc_url = recovered_rpc_url
            active_web3 = make_intent_test_web3(active_rpc_url)

    if last_timeout_error is None:
        raise RuntimeError(f"{chain_name} Anvil recovery failed without timeout error context")

    raise RuntimeError(
        f"{chain_name} Anvil wallet seed failed after {TEST_ANVIL_RECOVERY_MAX_RESTARTS} forced restart attempts "
        f"(rpc_url={active_rpc_url}, last_error={type(last_timeout_error).__name__}: {last_timeout_error})"
    ) from last_timeout_error


def get_latest_block(rpc_url: str) -> int:
    """Get the latest block number from an RPC endpoint.

    Uses in-process Web3 provider instead of subprocess (cast).
    """
    w3 = make_intent_test_web3(rpc_url)
    return w3.eth.block_number


def is_anvil_running(rpc_url: str = ANVIL_URL) -> bool:
    """Check if Anvil is running and responding."""
    try:
        web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 2}))
        return web3.is_connected()
    except Exception:
        return False


def _retry_on_network_error(
    func: Callable[..., Any],
    description: str,
    max_retries: int = TEST_FUNDING_RPC_MAX_RETRIES,
    backoff_seconds: float = TEST_FUNDING_RPC_BACKOFF_SECONDS,
) -> Any:
    """Retry a callable on transient network errors with linear backoff.

    Catches ReadTimeout, ConnectionError, and web3 TimeExhausted.
    Zero overhead on happy path - returns immediately on first success.

    Args:
        func: Zero-argument callable to retry
        description: Human-readable label for log messages
        max_retries: Maximum number of attempts (0 treated as 1)
        backoff_seconds: Base backoff between retries (multiplied by attempt number)

    Returns:
        Return value of func()

    Raises:
        Last caught exception if all retries exhausted
    """
    attempts = max(1, max_retries)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, TimeExhausted) as e:
            last_error = e
            if attempt < attempts:
                delay = backoff_seconds * attempt
                print(
                    f"  [retry] {description} attempt {attempt}/{attempts} failed "
                    f"({type(e).__name__}), retrying in {delay:.0f}s..."
                )
                time.sleep(delay)
            else:
                print(
                    f"  [retry] {description} failed after {attempts} attempts "
                    f"({type(e).__name__}: {e})"
                )
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"{description} failed without a captured retryable exception")


def _retry_rpc_call(
    w3: Web3,
    method: str,
    params: list,
    max_retries: int = TEST_FUNDING_RPC_MAX_RETRIES,
    backoff_seconds: float = TEST_FUNDING_RPC_BACKOFF_SECONDS,
) -> Any:
    """Retry an Anvil RPC call with linear backoff and error checking.

    Delegates to _retry_on_network_error for transient failures.
    Additionally checks for JSON-RPC error payloads and raises on failure.

    Args:
        w3: Web3 instance connected to Anvil
        method: RPC method name
        params: RPC parameters
        max_retries: Maximum number of attempts
        backoff_seconds: Base backoff between retries (multiplied by attempt number)

    Returns:
        RPC response

    Raises:
        RuntimeError: If the RPC response contains an error field
        Last caught network exception if all retries exhausted
    """
    response = _retry_on_network_error(
        lambda: w3.provider.make_request(method, params),
        description=method,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
    )
    if not _rpc_response_success(response):
        error_payload = response.get("error") if isinstance(response, dict) else response
        raise RuntimeError(f"{method} returned RPC error: {error_payload}")
    return response


def fund_native_token(wallet: str, amount_wei: int, rpc_url: str) -> None:
    """Fund a wallet with native token (ETH/AVAX/etc).

    Uses in-process Web3 provider RPC instead of subprocess (cast).
    Retries on transient network errors (ReadTimeout, ConnectionError).
    """
    w3 = make_intent_test_web3(rpc_url)
    checksum_wallet = Web3.to_checksum_address(wallet)
    current_balance = w3.eth.get_balance(checksum_wallet)
    if current_balance >= amount_wei:
        return

    amount_hex = hex(amount_wei)
    _retry_rpc_call(w3, "anvil_setBalance", [wallet, amount_hex])


def _calculate_mapping_slot(wallet: str, balance_slot: int) -> str:
    """Calculate the storage slot for a mapping entry (balanceOf).

    Equivalent to `cast index address <wallet> <slot>` but in-process.

    Uses keccak256(abi.encode(key, slot)) per Solidity storage layout.
    """
    from eth_hash.auto import keccak as keccak256

    # Pad wallet address to 32 bytes
    key_padded = wallet.lower().replace("0x", "").zfill(64)
    # Pad slot number to 32 bytes
    slot_padded = hex(balance_slot)[2:].zfill(64)
    # Concatenate and hash
    concat = bytes.fromhex(key_padded + slot_padded)
    return "0x" + keccak256(concat).hex()


def fund_erc20_token(
    wallet: str,
    token_address: str,
    amount: int,
    balance_slot: int,
    rpc_url: str,
) -> None:
    """Fund a wallet with ERC20 tokens using storage manipulation.

    Uses in-process Web3 provider and keccak256 instead of subprocess (cast).
    """
    w3 = make_intent_test_web3(rpc_url)

    # Calculate storage slot in-process (replaces `cast index`)
    storage_slot = _calculate_mapping_slot(wallet, balance_slot)

    # Format amount as 32-byte hex
    amount_hex = f"0x{amount:064x}"

    # Set storage via Anvil RPC (with retry for transient failures)
    _retry_rpc_call(w3, "anvil_setStorageAt", [token_address, storage_slot, amount_hex])

    # Mine a block to apply changes
    _retry_rpc_call(w3, "evm_mine", [])


def get_token_balance(web3: Web3, token_address: str, wallet: str) -> int:
    """Get ERC20 token balance for a wallet.

    Works with any Web3 instance (gateway-backed or direct).
    """
    contract = web3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
    return contract.functions.balanceOf(Web3.to_checksum_address(wallet)).call()


def get_token_decimals(web3: Web3, token_address: str) -> int:
    """Get ERC20 token decimals.

    Works with any Web3 instance (gateway-backed or direct).
    """
    contract = web3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
    return contract.functions.decimals().call()


def format_token_amount(amount: int, decimals: int) -> Decimal:
    """Convert raw token amount to decimal representation."""
    return Decimal(amount) / Decimal(10**decimals)


def get_chain_name_from_id(chain_id: int) -> str:
    """Get chain name from chain ID."""
    chain_id_to_name = {
        1: "ethereum",
        10: "optimism",
        56: "bsc",
        137: "polygon",
        8453: "base",
        42161: "arbitrum",
        43114: "avalanche",
    }
    return chain_id_to_name.get(chain_id, f"unknown_{chain_id}")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def test_wallet() -> str:
    """Return the default test wallet address."""
    return TEST_WALLET


def _wrap_native_token(wallet: str, weth_address: str, amount: int, rpc_url: str) -> None:
    """Wrap native tokens to get WETH/WAVAX/etc.

    Uses in-process Web3 transaction from an unlocked (auto-impersonate) wallet
    instead of subprocess (cast send).

    This is more reliable than storage slot manipulation because WETH
    storage layouts can vary across chains and implementations.
    """
    w3 = make_intent_test_web3(rpc_url)
    checksum_wallet = Web3.to_checksum_address(wallet)
    checksum_weth = Web3.to_checksum_address(weth_address)

    def _wrap_call() -> None:
        tx_hash = w3.eth.send_transaction({
            "from": checksum_wallet,
            "to": checksum_weth,
            "value": amount,
        })
        w3.eth.wait_for_transaction_receipt(tx_hash, timeout=TEST_RPC_READ_TIMEOUT_SECONDS)

    _retry_on_network_error(_wrap_call, description="wrap_native_token")


# =============================================================================
# Test Markers
# =============================================================================


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line("markers", "base: Tests that run on Base chain")
    config.addinivalue_line("markers", "avalanche: Tests that run on Avalanche chain")
    config.addinivalue_line("markers", "ethereum: Tests that run on Ethereum chain")
    config.addinivalue_line("markers", "arbitrum: Tests that run on Arbitrum chain")
    config.addinivalue_line("markers", "optimism: Tests that run on Optimism chain")
    config.addinivalue_line("markers", "polygon: Tests that run on Polygon chain")
    config.addinivalue_line("markers", "bsc: Tests that run on BSC chain")
    config.addinivalue_line("markers", "linea: Tests that run on Linea chain")
    config.addinivalue_line("markers", "blast: Tests that run on Blast chain")
    config.addinivalue_line("markers", "mantle: Tests that run on Mantle chain")
    config.addinivalue_line("markers", "swap: Tests for SwapIntent")
    config.addinivalue_line("markers", "lp: Tests for LP intents (Open/Close)")
    config.addinivalue_line("markers", "lending: Tests for lending intents")
    config.addinivalue_line("markers", "perps: Tests for perps intents")
    config.addinivalue_line("markers", "supply: Tests for supply intents")
    config.addinivalue_line("markers", "borrow: Tests for borrow intents")


# =============================================================================
# Module-Scoped Baseline Snapshot / Revert for Test Isolation
# =============================================================================

# Baseline map: (chain_id, module_path) -> baseline_snapshot_id
# Captured once per module after funding is complete, re-armed after each revert.
_module_baselines: dict[tuple[int, str], str] = {}


def _get_baseline_key(request: pytest.FixtureRequest) -> tuple[int, str]:
    """Build a baseline map key from the current test request.

    Returns:
        Tuple of (chain_id_or_-1, module_path)
    """
    chain_id = -1
    try:
        chain_id = int(request.getfixturevalue("chain_id"))
    except Exception:
        try:
            web3 = request.getfixturevalue("web3")
            if web3 is not None:
                chain_id = int(web3.eth.chain_id)
        except Exception:
            pass
    module_path = request.fspath.strpath if hasattr(request, "fspath") else str(request.node.module)
    return (chain_id, module_path)


def _capture_baseline(web3_instance: Any) -> str | None:
    """Capture a baseline snapshot on the Anvil fork.

    Returns:
        Snapshot ID or None on failure
    """
    try:
        resp = web3_instance.provider.make_request("evm_snapshot", [])
        snapshot_id = resp.get("result")
        if snapshot_id is None:
            print(f"WARNING: evm_snapshot returned no result: {resp}")
        return snapshot_id
    except Exception as e:
        print(f"WARNING: baseline capture failed ({type(e).__name__}: {e})")
        return None


def _revert_to_baseline(web3_instance: Any, snapshot_id: str) -> bool:
    """Revert to a baseline snapshot.

    Returns:
        True if revert succeeded
    """
    try:
        resp = web3_instance.provider.make_request("evm_revert", [snapshot_id])
        return bool(resp.get("result"))
    except Exception as e:
        print(f"WARNING: baseline revert failed ({type(e).__name__}: {e})")
        return False


@pytest.fixture(autouse=True)
def anvil_snapshot(request):
    """Snapshot/revert Anvil state around each test using module baselines.

    On first test in a module: captures baseline after funding is complete
    (late-binding web3 and funded_wallet fixtures).

    On each test: reverts to baseline, then re-arms a new snapshot.

    On revert failure: attempts fork restart -> reseed -> new baseline.

    Requires ``web3`` fixture to be available in the test's scope.
    Tests without a ``web3`` fixture run without snapshot isolation.
    """
    # Skip if no web3 fixture available
    if "web3" not in request.fixturenames:
        yield
        return

    try:
        web3 = request.getfixturevalue("web3")
    except Exception:
        yield
        return

    if web3 is None:
        yield
        return

    key = _get_baseline_key(request)

    # Ensure baseline exists for this module (late-bind funded_wallet)
    if key not in _module_baselines:
        # Trigger funded_wallet if available (ensures funding is done before baseline)
        if "funded_wallet" in request.fixturenames:
            try:
                request.getfixturevalue("funded_wallet")
            except Exception:
                pass

        baseline_id = _capture_baseline(web3)
        if baseline_id is None:
            print("WARNING: Could not capture module baseline; running without isolation")
            yield
            return
        _module_baselines[key] = baseline_id
        print(f"  [baseline] Captured module baseline {baseline_id} for {key[1]}")

    # Revert to baseline before this test
    baseline_id = _module_baselines[key]
    reverted = _revert_to_baseline(web3, baseline_id)

    if not reverted:
        # Attempt recovery: restart fork, reseed, rebuild baseline
        print(f"WARNING: Baseline revert failed for {key[1]}, attempting recovery...")
        recovered = _attempt_recovery(request, web3, key)
        if not recovered:
            pytest.fail(
                f"Anvil recovery failed for module {key[1]} (chain_id={key[0]}). "
                "Fork is unhealthy and state isolation cannot be guaranteed."
            )

    # Re-arm: capture new snapshot for next test's revert
    new_baseline = _capture_baseline(web3)
    if new_baseline is not None:
        _module_baselines[key] = new_baseline
    else:
        print(f"WARNING: Failed to re-arm baseline for {key[1]}; next test may trigger recovery")

    yield

    # No teardown revert needed; the NEXT test's setup reverts to baseline


def _attempt_recovery(request: pytest.FixtureRequest, web3_instance: Any, key: tuple[int, str]) -> bool:
    """Attempt to recover from a failed baseline revert.

    Tries to restart the Anvil fork via the anvil_instance fixture,
    reseed the wallet, and capture a new baseline.

    Returns:
        True if recovery succeeded
    """
    try:
        anvil = request.getfixturevalue("anvil_instance")
    except Exception as e:
        print(f"WARNING: Recovery unavailable (anvil_instance fixture not found): {e}")
        return False

    try:
        chain_name = str(getattr(anvil, "chain", key[0]))
        recovered = False
        for attempt in range(1, TEST_ANVIL_RECOVERY_MAX_RESTARTS + 1):
            recovered, _ = _force_restart_anvil(anvil, chain_name, attempt)
            if recovered:
                break
        if not recovered:
            print("WARNING: Fork restart failed during recovery")
            return False

        try:
            reseed_wallet_state = request.getfixturevalue("reseed_wallet_state")
        except Exception as e:
            print(f"WARNING: Recovery unavailable (reseed_wallet_state fixture not found): {e}")
            return False

        if not callable(reseed_wallet_state):
            print("WARNING: Recovery unavailable (reseed_wallet_state is not callable)")
            return False

        try:
            reseed_wallet_state()
        except Exception as e:
            print(f"WARNING: Re-funding failed during recovery: {e}")
            return False

        # Capture new baseline after restart + reseed
        recovered_rpc_url = anvil.get_rpc_url()
        recovered_web3 = make_intent_test_web3(recovered_rpc_url)
        new_baseline = _capture_baseline(recovered_web3)
        if new_baseline is not None:
            _module_baselines[key] = new_baseline
            print(f"  [baseline] Recovery successful, new baseline {new_baseline}")
            return True
        return False
    except Exception as e:
        print(f"WARNING: Recovery attempt failed: {e}")
        return False


@pytest_asyncio.fixture(autouse=True)
async def close_web3_async_http_sessions():
    """Close leaked aiohttp ClientSessions created by web3 AsyncHTTPProvider between tests."""
    yield

    for provider in list(_ASYNC_HTTP_PROVIDERS):
        manager = getattr(provider, "_request_session_manager", None)
        if manager is None:
            continue

        # Close cached async sessions (aiohttp ClientSession)
        for _, session in manager.session_cache.items():
            closed = getattr(session, "closed", True)
            if not closed:
                try:
                    await session.close()
                except Exception:
                    # Best-effort cleanup; don't fail tests on teardown.
                    pass

        manager.session_cache.clear()


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def web3() -> Web3:
    """Web3 connection to Anvil."""
    if not is_anvil_running(ANVIL_URL):
        pytest.skip("Anvil is not running. Start Anvil first.")

    w3 = Web3(Web3.HTTPProvider(ANVIL_URL, request_kwargs={"timeout": TEST_WEB3_REQUEST_TIMEOUT}))
    assert w3.is_connected(), "Failed to connect to Anvil"
    return w3


@pytest.fixture(scope="module")
def chain_id(web3: Web3) -> int:
    """Get the chain ID from Anvil."""
    return web3.eth.chain_id


@pytest.fixture(scope="module")
def chain_name(chain_id: int) -> str:
    """Get chain name from chain ID."""
    for name, config in CHAIN_CONFIGS.items():
        if config["chain_id"] == chain_id:
            return name
    pytest.skip(f"Unsupported chain ID: {chain_id}")
    return ""  # Unreachable, but needed for type checker


@pytest.fixture(scope="module")
def chain_config(chain_name: str) -> dict:
    """Get chain configuration."""
    return CHAIN_CONFIGS[chain_name]


@pytest.fixture(scope="module")
def test_private_key() -> str:
    """Private key for test wallet."""
    return TEST_PRIVATE_KEY


@pytest.fixture(scope="module")
def funded_wallet(
    web3: Web3,
    chain_config: dict,
) -> str:
    """Fund test wallet with native token and ERC20 tokens."""
    wallet = TEST_WALLET

    # Fund with native token (10 ETH/AVAX/etc)
    native_amount = 10 * 10**18
    fund_native_token(wallet, native_amount, ANVIL_URL)

    # Fund with all configured tokens
    tokens = chain_config.get("tokens", {})
    balance_slots = chain_config.get("balance_slots", {})

    for token_symbol, token_address in tokens.items():
        if token_symbol not in balance_slots:
            print(f"Warning: No balance slot for {token_symbol}, skipping funding")
            continue
        balance_slot = balance_slots[token_symbol]

        # Get token decimals
        decimals = get_token_decimals(web3, token_address)

        # Fund with 1 million tokens
        amount = 1_000_000 * (10**decimals)
        fund_erc20_token(wallet, token_address, amount, balance_slot, ANVIL_URL)

        # Verify funding
        balance = get_token_balance(web3, token_address, wallet)
        print(f"  Funded {token_symbol}: {format_token_amount(balance, decimals)}")

    return wallet


# =============================================================================
# Session-Scoped Price Oracles (One Per Chain)
# =============================================================================
#
# Similar to Anvil fixtures, we create session-scoped price oracle fixtures
# per chain. This ensures prices are fetched ONCE at session start, aligned
# with when the Anvil fork is created. This eliminates flakiness caused by
# price divergence between CoinGecko (live) and Anvil fork (frozen state).
#
# NOTE: These fixtures use a direct CoinGecko HTTP call (no gateway dependency)
# so that only the tested chain's Anvil fork needs to start.
# =============================================================================

from almanak.gateway.data.price.coingecko import GLOBAL_TOKEN_IDS


def _fetch_prices_sync(chain_name: str) -> dict[str, Decimal]:
    """Fetch prices synchronously via direct CoinGecko HTTP call.

    Uses GLOBAL_TOKEN_IDS to resolve token symbols to CoinGecko IDs,
    then makes a single batch /simple/price request. Supports both
    free and pro CoinGecko API via COINGECKO_API_KEY env var.

    Args:
        chain_name: Chain name to fetch prices for

    Returns:
        Dict mapping token symbols to USD prices
    """
    config = CHAIN_CONFIGS.get(chain_name, {})
    token_symbols = list(config.get("tokens", {}).keys())

    # Resolve symbols to CoinGecko IDs
    symbol_to_cg_id: dict[str, str] = {}
    for symbol in token_symbols:
        cg_id = GLOBAL_TOKEN_IDS.get(symbol.upper())
        if cg_id is None:
            raise ValueError(
                f"No CoinGecko ID found for token '{symbol}'. "
                f"Add it to GLOBAL_TOKEN_IDS in almanak/gateway/data/price/coingecko.py"
            )
        symbol_to_cg_id[symbol] = cg_id

    # Deduplicate CoinGecko IDs (e.g. USDC and USDC.E both map to "usd-coin")
    unique_cg_ids = sorted(set(symbol_to_cg_id.values()))

    # Determine API host and headers
    api_key = os.environ.get("COINGECKO_API_KEY", "")
    if api_key:
        base_url = "https://pro-api.coingecko.com/api/v3/simple/price"
        headers = {"x-cg-pro-api-key": api_key}
    else:
        base_url = "https://api.coingecko.com/api/v3/simple/price"
        headers = {}

    params = {"ids": ",".join(unique_cg_ids), "vs_currencies": "usd"}

    # Fetch with retry on 429
    print(f"\n  Fetching prices for {chain_name} via direct CoinGecko HTTP:")
    resp = None
    for attempt in range(3):
        resp = requests.get(base_url, params=params, headers=headers, timeout=15)
        if resp.status_code == 429:
            backoff = (attempt + 1)  # 1s, 2s
            print(f"    Rate limited (429), retrying in {backoff}s (attempt {attempt + 1}/3)...")
            time.sleep(backoff)
            continue
        resp.raise_for_status()
        break
    else:
        raise RuntimeError(
            f"CoinGecko rate limited after 3 attempts for {chain_name}. "
            f"Set COINGECKO_API_KEY env var for higher limits."
        )

    data = resp.json()

    # Build symbol -> price map
    prices: dict[str, Decimal] = {}
    missing = []
    for symbol, cg_id in symbol_to_cg_id.items():
        entry = data.get(cg_id, {})
        usd_price = entry.get("usd")
        if usd_price is None:
            missing.append(f"{symbol} (cg_id={cg_id})")
            continue
        prices[symbol] = Decimal(str(usd_price))
        print(f"    {symbol}: ${usd_price}")

    if missing:
        raise RuntimeError(
            f"CoinGecko returned no USD price for: {', '.join(missing)}. "
            f"Response keys: {list(data.keys())}"
        )

    return prices


def _create_price_oracle_fixture(chain_name: str):
    """Factory function to create session-scoped price oracle per chain.

    Similar to Anvil fixture pattern in conftest_gateway.py, creates a
    separate fixture per chain that fetches prices once per session.

    Args:
        chain_name: Chain name (e.g., "arbitrum", "base", "bsc")

    Returns:
        A pytest fixture function
    """

    @pytest.fixture(scope="session")
    def price_oracle_fixture() -> dict[str, Decimal]:
        """Fetch prices once per session for all tokens in this chain.

        Uses direct CoinGecko HTTP call to avoid gateway dependency.

        Returns:
            Dict mapping token symbols to USD prices
        """
        return _fetch_prices_sync(chain_name)

    return price_oracle_fixture


# Create session-scoped price oracle fixtures for each supported chain
# (matches the chains that have Anvil fixtures in conftest_gateway.py)
price_oracle_arbitrum = _create_price_oracle_fixture("arbitrum")
price_oracle_base = _create_price_oracle_fixture("base")
price_oracle_ethereum = _create_price_oracle_fixture("ethereum")
price_oracle_avalanche = _create_price_oracle_fixture("avalanche")
price_oracle_bsc = _create_price_oracle_fixture("bsc")
price_oracle_bnb = _create_price_oracle_fixture("bnb")  # Alias for bsc
price_oracle_optimism = _create_price_oracle_fixture("optimism")
price_oracle_polygon = _create_price_oracle_fixture("polygon")


# =============================================================================
# Backward-Compatible Price Oracle Selector
# =============================================================================


@pytest.fixture(scope="module")
def price_oracle(chain_name: str, request) -> dict[str, Decimal]:
    """Select the appropriate session-scoped price oracle for this chain.

    This fixture maintains backward compatibility with existing tests
    while routing to the session-scoped oracle for the specific chain.

    The session-scoped oracles fetch prices once at session start,
    ensuring alignment with the Anvil fork block state.

    Args:
        chain_name: Chain name from the chain_name fixture
        request: Pytest request object for fixture access

    Returns:
        Dict mapping token symbols to USD prices
    """
    # Map chain names to their session-scoped fixtures
    fixture_map = {
        "arbitrum": "price_oracle_arbitrum",
        "base": "price_oracle_base",
        "ethereum": "price_oracle_ethereum",
        "avalanche": "price_oracle_avalanche",
        "bsc": "price_oracle_bsc",
        "bnb": "price_oracle_bnb",
        "optimism": "price_oracle_optimism",
        "polygon": "price_oracle_polygon",
    }

    fixture_name = fixture_map.get(chain_name)
    if not fixture_name:
        pytest.skip(f"No price oracle fixture for chain: {chain_name}")

    return request.getfixturevalue(fixture_name)
