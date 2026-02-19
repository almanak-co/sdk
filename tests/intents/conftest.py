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
import subprocess
import time
import weakref
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
import requests
from web3 import Web3
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

# =============================================================================
# Constants
# =============================================================================

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
            "WETH": 3,
            "USDT": 2,
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
            "USDC": "0x09Bc4E0D10E52d8DA1060e8ef425f2dB24b36C7C",
            "WETH": "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111",
            "USDT": "0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE",
        },
        "balance_slots": {
            "USDC": 0,
            "WETH": 0,
            "USDT": 0,
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


def get_latest_block(rpc_url: str) -> int:
    """Get the latest block number from an RPC endpoint."""
    result = subprocess.run(
        ["cast", "block-number", "--rpc-url", rpc_url],
        capture_output=True,
        text=True,
        check=True,
        timeout=TEST_CAST_TIMEOUT_SECONDS,
    )
    return int(result.stdout.strip())


def is_anvil_running(rpc_url: str = ANVIL_URL) -> bool:
    """Check if Anvil is running and responding."""
    try:
        web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 2}))
        return web3.is_connected()
    except Exception:
        return False


def fund_native_token(wallet: str, amount_wei: int, rpc_url: str) -> None:
    """Fund a wallet with native token (ETH/AVAX/etc)."""
    amount_hex = hex(amount_wei)
    subprocess.run(
        ["cast", "rpc", "anvil_setBalance", wallet, amount_hex, "--rpc-url", rpc_url],
        capture_output=True,
        check=True,
        timeout=TEST_CAST_TIMEOUT_SECONDS,
    )


def fund_erc20_token(
    wallet: str,
    token_address: str,
    amount: int,
    balance_slot: int,
    rpc_url: str,
) -> None:
    """Fund a wallet with ERC20 tokens using storage manipulation."""
    # Calculate storage slot using cast index
    result = subprocess.run(
        ["cast", "index", "address", wallet, str(balance_slot)],
        capture_output=True,
        text=True,
        check=True,
        timeout=TEST_CAST_TIMEOUT_SECONDS,
    )
    storage_slot = result.stdout.strip()

    # Format amount as 32-byte hex
    amount_hex = f"0x{amount:064x}"

    # Set storage
    subprocess.run(
        [
            "cast", "rpc", "anvil_setStorageAt",
            token_address, storage_slot, amount_hex,
            "--rpc-url", rpc_url,
        ],
        capture_output=True,
        check=True,
        timeout=TEST_CAST_TIMEOUT_SECONDS,
    )

    # Mine a block to apply changes
    subprocess.run(
        ["cast", "rpc", "evm_mine", "--rpc-url", rpc_url],
        capture_output=True,
        check=True,
        timeout=TEST_CAST_TIMEOUT_SECONDS,
    )


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

    This is more reliable than storage slot manipulation because WETH
    storage layouts can vary across chains and implementations.
    """
    # Use cast to send ETH to WETH contract (calls deposit())
    subprocess.run(
        [
            "cast",
            "send",
            weth_address,
            "--value",
            str(amount),
            "--from",
            wallet,
            "--unlocked",
            "--rpc-url",
            rpc_url,
        ],
        capture_output=True,
        check=True,
        timeout=TEST_CAST_TIMEOUT_SECONDS,
    )


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
# Snapshot / Revert for Test Isolation
# =============================================================================


@pytest.fixture(autouse=True)
def anvil_snapshot(request):
    """Snapshot Anvil state before each test and revert after.

    This ensures complete test isolation -- no on-chain state (balances,
    approvals, positions, debt) leaks between tests.

    Requires a ``web3`` fixture to be available in the test's scope.
    Tests without a ``web3`` fixture run without snapshot isolation.
    """
    web3 = request.getfixturevalue("web3") if "web3" in request.fixturenames else None
    if web3 is None:
        yield
        return

    try:
        snapshot_resp = web3.provider.make_request("evm_snapshot", [])
        snapshot_id = snapshot_resp.get("result")
        if snapshot_id is None:
            raise RuntimeError(f"evm_snapshot failed: {snapshot_resp}")
    except Exception as e:
        print(f"WARNING: evm_snapshot failed ({type(e).__name__}: {e}); running without snapshot isolation")
        yield
        return

    yield
    try:
        revert_resp = web3.provider.make_request("evm_revert", [snapshot_id])
        reverted = revert_resp.get("result")
        if not reverted:
            raise RuntimeError(f"evm_revert failed: {revert_resp}")
    except Exception as e:
        print(f"WARNING: evm_revert failed ({type(e).__name__}: {e})")


@pytest.fixture(autouse=True)
def restart_anvil_on_rpc_timeouts():
    """No-op placeholder kept for backward compatibility with test collection."""
    yield


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
