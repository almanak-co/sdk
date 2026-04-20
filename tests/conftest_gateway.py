"""Shared fixtures for gateway-based integration tests.

These fixtures start a gateway server connected to Anvil forks,
allowing tests to use the gateway architecture for on-chain interactions.

Usage:
    In your test file, import the fixtures you need:

        from tests.conftest_gateway import (
            gateway_server,
            gateway_client,
            gateway_web3_arbitrum,
            # etc.
        )

    Or add to your conftest.py:

        pytest_plugins = ["tests.conftest_gateway"]

Note on async/sync compatibility:
    The gateway server is async (grpc.aio.server), but test functions and
    the Web3 provider are sync. To avoid blocking the event loop, the server
    runs in a dedicated thread with its own event loop.

Anvil Management:
    Anvil instances are started automatically by session-scoped fixtures.
    Each chain gets a dynamically allocated port to avoid clashes in parallel runs.
    The gateway is configured via environment variables (ANVIL_{CHAIN}_PORT) to
    route RPC requests to the correct Anvil instance.
"""

import asyncio
import logging
import os
import socket
import threading
import time
from collections.abc import Generator
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.anvil.fork_manager import RollingForkManager
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.framework.web3 import get_gateway_web3
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.server import GatewayServer
from almanak.gateway.utils.rpc_provider import get_rpc_url

logger = logging.getLogger(__name__)


# Gateway port for tests (avoid conflict with dev gateway on 50051)
TEST_GATEWAY_PORT = 50099

# Supported chains for Anvil fixtures
SUPPORTED_CHAINS = ["arbitrum", "base", "ethereum", "avalanche", "bsc", "optimism", "polygon"]


# =============================================================================
# Port Allocation
# =============================================================================


def find_free_port() -> int:
    """Find an available port by binding to port 0.

    The OS assigns an available ephemeral port. We immediately close the socket
    and return the port number. There's a small race window before Anvil binds,
    but it's reliable for test scenarios.

    Returns:
        Available port number
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port


def is_gateway_running(port: int = TEST_GATEWAY_PORT) -> bool:
    """Check if gateway is running on the given port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


# =============================================================================
# Anvil Fixture Class
# =============================================================================


class AnvilFixture:
    """Manages an Anvil fork instance for testing.

    Wraps the async RollingForkManager in a sync-compatible interface.
    The Anvil process runs in a dedicated thread with its own event loop.

    Attributes:
        chain: Chain name (e.g., "arbitrum", "base")
        port: Dynamically allocated port for this Anvil instance
    """

    def __init__(self, chain: str, fork_rpc_url: str):
        """Initialize Anvil fixture.

        Args:
            chain: Chain name for the fork
            fork_rpc_url: Mainnet RPC URL to fork from
        """
        self.chain = chain
        self.fork_rpc_url = fork_rpc_url
        self.port = find_free_port()

        self._manager: RollingForkManager | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._stop_event = threading.Event()
        self._error: Exception | None = None

    def start(self, timeout: float = 60.0) -> None:
        """Start the Anvil fork in a background thread.

        Args:
            timeout: Maximum seconds to wait for Anvil to be ready

        Raises:
            RuntimeError: If Anvil fails to start within timeout
        """
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

        if not self._ready.wait(timeout=timeout):
            if self._error:
                raise RuntimeError(f"Anvil for {self.chain} failed to start: {self._error}")
            raise RuntimeError(f"Anvil for {self.chain} failed to start within {timeout}s")

        # Check for errors that occurred during startup (ready is set in finally block)
        if self._error:
            raise RuntimeError(f"Anvil for {self.chain} failed to start: {self._error}")

        logger.info(f"Anvil fixture started: chain={self.chain}, port={self.port}")

    def stop(self) -> None:
        """Stop the Anvil fork and clean up resources."""
        if self._loop and self._manager:
            try:
                # Check if loop is still running before trying to schedule coroutine
                if self._loop.is_running():
                    future = asyncio.run_coroutine_threadsafe(self._manager.stop(), self._loop)
                    future.result(timeout=10)
            except Exception as e:
                logger.warning(f"Error stopping Anvil manager for {self.chain}: {e}")

        if self._loop:
            try:
                if not self._loop.is_closed():
                    self._loop.call_soon_threadsafe(self._stop_event.set)
            except RuntimeError:
                # Loop already closed, just set the event directly
                self._stop_event.set()

        if self._thread:
            self._thread.join(timeout=5.0)

        logger.info(f"Anvil fixture stopped: chain={self.chain}")

    def get_rpc_url(self) -> str:
        """Get the local RPC URL for this Anvil instance.

        Returns:
            RPC URL (e.g., "http://127.0.0.1:54321")
        """
        return f"http://127.0.0.1:{self.port}"

    def fund_wallet(self, address: str, eth_amount: Decimal) -> bool:
        """Fund a wallet with ETH (sync wrapper).

        Args:
            address: Wallet address to fund
            eth_amount: Amount of ETH to set as balance

        Returns:
            True if funding successful
        """
        if not self._loop or not self._manager:
            logger.error(f"Cannot fund wallet: Anvil for {self.chain} not running")
            return False

        try:
            future = asyncio.run_coroutine_threadsafe(
                self._manager.fund_wallet(address, eth_amount),
                self._loop,
            )
            return future.result(timeout=30)
        except Exception as e:
            logger.error(f"Failed to fund wallet on {self.chain}: {e}")
            return False

    def fund_tokens(self, address: str, tokens: dict[str, Decimal]) -> bool:
        """Fund a wallet with ERC-20 tokens (sync wrapper).

        Args:
            address: Wallet address to fund
            tokens: Dict mapping token symbol to amount

        Returns:
            True if all tokens funded successfully
        """
        if not self._loop or not self._manager:
            logger.error(f"Cannot fund tokens: Anvil for {self.chain} not running")
            return False

        try:
            future = asyncio.run_coroutine_threadsafe(
                self._manager.fund_tokens(address, tokens),
                self._loop,
            )
            return future.result(timeout=60)
        except Exception as e:
            logger.error(f"Failed to fund tokens on {self.chain}: {e}")
            return False

    def health_check(self, timeout_seconds: float = 2.0) -> bool:
        """Check if the Anvil fork is healthy (sync wrapper).

        Args:
            timeout_seconds: Timeout for the health probe

        Returns:
            True if fork is healthy
        """
        if not self._loop or not self._manager:
            return False

        try:
            if not self._loop.is_running():
                return False
            future = asyncio.run_coroutine_threadsafe(
                self._manager.health_check(timeout_seconds=timeout_seconds),
                self._loop,
            )
            return future.result(timeout=timeout_seconds + 2)
        except Exception as e:
            logger.debug(f"Health check failed for {self.chain}: {e}")
            return False

    def restart(self, timeout: float = 60.0, health_timeout_seconds: float = 2.0) -> bool:
        """Restart the Anvil fork (sync wrapper).

        Stops the current fork thread and starts a new one on the same port.

        Args:
            timeout: Maximum seconds to wait for restart
            health_timeout_seconds: Timeout for the post-restart health probe

        Returns:
            True if restart successful
        """
        logger.info(f"Restarting Anvil fixture for {self.chain}...")

        # Stop existing thread/process
        self.stop()

        # Verify old thread actually terminated
        if self._thread and self._thread.is_alive():
            logger.error(f"Anvil thread did not stop cleanly for {self.chain}")
            return False

        # Reset internal state
        self._error = None
        self._ready = threading.Event()
        self._stop_event = threading.Event()

        # Start fresh
        try:
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

            if not self._ready.wait(timeout=timeout):
                if self._error:
                    logger.error(f"Anvil restart failed for {self.chain}: {self._error}")
                    return False
                logger.error(f"Anvil restart timed out for {self.chain}")
                return False

            if self._error:
                logger.error(f"Anvil restart failed for {self.chain}: {self._error}")
                return False

            # Verify post-restart health via RPC probe
            if not self.health_check(timeout_seconds=health_timeout_seconds):
                logger.error(f"Anvil restart completed but health check failed for {self.chain}")
                self.stop()
                return False

            logger.info(f"Anvil fixture restarted: chain={self.chain}, port={self.port}")
            return True
        except Exception as e:
            logger.error(f"Anvil restart exception for {self.chain}: {e}")
            return False

    def ensure_healthy(self, timeout_seconds: float = 2.0) -> bool:
        """Ensure the Anvil fork is healthy, restarting if necessary.

        Args:
            timeout_seconds: Timeout for the health probe

        Returns:
            True if fork is healthy (possibly after restart)
        """
        if self.health_check(timeout_seconds=timeout_seconds):
            return True

        logger.warning(f"Anvil fork unhealthy for {self.chain}, attempting restart...")
        if not self.restart(health_timeout_seconds=timeout_seconds):
            return False
        return self.health_check(timeout_seconds=timeout_seconds)

    def _run(self) -> None:
        """Thread target: create event loop and run Anvil manager."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        try:
            # Map bsc -> bnb for RollingForkManager (which uses bnb internally)
            chain_for_manager = "bnb" if self.chain == "bsc" else self.chain

            self._manager = RollingForkManager(
                rpc_url=self.fork_rpc_url,
                chain=chain_for_manager,
                anvil_port=self.port,
                auto_impersonate=True,
                startup_timeout_seconds=60.0,  # Match AnvilFixture.start() timeout
            )

            # Start Anvil
            started = self._loop.run_until_complete(self._manager.start())
            if not started:
                self._error = RuntimeError("RollingForkManager.start() returned False")
                return

            self._ready.set()

            # Run until stop is requested
            while not self._stop_event.is_set():
                self._loop.run_until_complete(asyncio.sleep(0.1))

        except Exception as e:
            self._error = e
            logger.exception(f"Anvil thread for {self.chain} failed: {e}")
        finally:
            self._ready.set()  # Unblock start() even on failure
            if self._loop:
                self._loop.close()


class GatewayServerThread:
    """Runs the async gateway server in a dedicated thread.

    This solves the issue where sync gRPC clients block when used with
    an async gRPC server in the same process/event loop.
    """

    def __init__(self, settings: GatewaySettings, anvil_ports: dict[str, int] | None = None):
        """Initialize gateway server thread.

        Args:
            settings: Gateway configuration
            anvil_ports: Optional mapping of chain -> Anvil port for dynamic routing.
                         If provided, sets ANVIL_{CHAIN}_PORT env vars before starting.
        """
        self.settings = settings
        self.anvil_ports = anvil_ports or {}
        self._server: GatewayServer | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()
        self._stop_requested = threading.Event()
        self._original_env: dict[str, str | None] = {}

    def _setup_anvil_env(self) -> None:
        """Set environment variables for Anvil port routing.

        The rpc_provider module reads ANVIL_{CHAIN}_PORT env vars to determine
        which port to use for each chain when network=anvil.
        """
        for chain, port in self.anvil_ports.items():
            env_var = f"ANVIL_{chain.upper()}_PORT"
            self._original_env[env_var] = os.environ.get(env_var)
            os.environ[env_var] = str(port)
            logger.debug(f"Set {env_var}={port}")

    def _restore_anvil_env(self) -> None:
        """Restore original environment variables."""
        for env_var, original_value in self._original_env.items():
            if original_value is None:
                os.environ.pop(env_var, None)
            else:
                os.environ[env_var] = original_value

    def _run_server(self) -> None:
        """Thread target: create event loop and run server."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        self._server = GatewayServer(self.settings)

        async def run():
            await self._server.start()
            self._started.set()
            # Wait until stop is requested
            while not self._stop_requested.is_set():
                await asyncio.sleep(0.1)
            await self._server.stop()

        self._loop.run_until_complete(run())
        self._loop.close()

    def start(self) -> None:
        """Start the server in a background thread."""
        # Set up Anvil port environment variables
        self._setup_anvil_env()

        self._thread = threading.Thread(target=self._run_server, daemon=True)
        self._thread.start()
        # Wait for server to be ready
        if not self._started.wait(timeout=10.0):
            self._restore_anvil_env()
            raise RuntimeError("Gateway server failed to start within 10 seconds")
        # Additional delay to ensure gRPC services are registered
        time.sleep(0.2)

    def stop(self) -> None:
        """Stop the server and wait for thread to exit."""
        self._stop_requested.set()
        if self._thread:
            self._thread.join(timeout=5.0)
        self._restore_anvil_env()


# =============================================================================
# Session-Scoped Anvil Fixtures
# =============================================================================


def _create_anvil_fixture(chain: str):
    """Factory function to create Anvil fixtures for each chain.

    Args:
        chain: Chain name (e.g., "arbitrum", "base")

    Returns:
        A pytest fixture function
    """
    # Map chain names to Alchemy-compatible names for RPC URL lookup
    # rpc_provider uses "bnb" while we use "bsc" as the fixture name
    rpc_chain_name = "bnb" if chain == "bsc" else chain

    @pytest.fixture(scope="session")
    def anvil_fixture() -> Generator[AnvilFixture, None, None]:
        """Start Anvil fork (auto-started, dynamic port)."""
        try:
            # Get mainnet RPC URL to fork from
            fork_rpc_url = get_rpc_url(rpc_chain_name, network="mainnet")
        except ValueError as e:
            pytest.skip(f"Cannot start Anvil for {chain}: {e}")
            return

        anvil = AnvilFixture(chain=chain, fork_rpc_url=fork_rpc_url)

        try:
            anvil.start()
        except RuntimeError as e:
            pytest.skip(f"Failed to start Anvil for {chain}: {e}")
            return

        yield anvil
        anvil.stop()

    return anvil_fixture


# Create Anvil fixtures for each supported chain
anvil_arbitrum = _create_anvil_fixture("arbitrum")
anvil_base = _create_anvil_fixture("base")
anvil_ethereum = _create_anvil_fixture("ethereum")
anvil_avalanche = _create_anvil_fixture("avalanche")
anvil_bsc = _create_anvil_fixture("bsc")
anvil_optimism = _create_anvil_fixture("optimism")
anvil_polygon = _create_anvil_fixture("polygon")
anvil_mantle = _create_anvil_fixture("mantle")


# =============================================================================
# Gateway Server Fixture
# =============================================================================


@pytest.fixture(scope="session")
def gateway_server(
    anvil_arbitrum: AnvilFixture,
    anvil_base: AnvilFixture,
    anvil_ethereum: AnvilFixture,
    anvil_avalanche: AnvilFixture,
    anvil_bsc: AnvilFixture,
    anvil_optimism: AnvilFixture,
    anvil_polygon: AnvilFixture,
) -> Generator[GatewayServerThread, None, None]:
    """Start gateway server connected to Anvil forks.

    This fixture depends on all Anvil fixtures, ensuring they are started
    before the gateway. The gateway is configured to route RPC requests
    to the correct Anvil instance via environment variables.

    Uses allow_insecure=True since tests don't use auth tokens.
    Uses network=anvil to route RPC calls to local Anvil instances.
    """
    # Collect Anvil ports from all fixtures
    anvil_ports = {
        "arbitrum": anvil_arbitrum.port,
        "base": anvil_base.port,
        "ethereum": anvil_ethereum.port,
        "avalanche": anvil_avalanche.port,
        "bsc": anvil_bsc.port,
        "bnb": anvil_bsc.port,  # Alias for rpc_provider compatibility
        "optimism": anvil_optimism.port,
        "polygon": anvil_polygon.port,
    }

    settings = GatewaySettings(
        grpc_port=TEST_GATEWAY_PORT,
        grpc_host="127.0.0.1",
        network="anvil",  # Use Anvil RPC URLs
        metrics_enabled=False,
        audit_enabled=False,
        allow_insecure=True,
    )

    server_thread = GatewayServerThread(settings, anvil_ports=anvil_ports)
    server_thread.start()

    logger.info(f"Gateway test server started on port {TEST_GATEWAY_PORT}")
    yield server_thread
    server_thread.stop()
    logger.info("Gateway test server stopped")


@pytest.fixture(scope="session")
def gateway_client(gateway_server: GatewayServerThread) -> Generator[GatewayClient, None, None]:
    """Provide connected gateway client for tests.

    The client is sync and can safely call the async server running
    in a separate thread. Session-scoped to match the gateway_server fixture
    and avoid reconnection issues between modules.
    """
    # Ensure gateway_server fixture ran (explicit dependency usage for ARG001)
    assert gateway_server is not None, "gateway_server fixture required"
    logger.info(f"Creating gateway client for port {TEST_GATEWAY_PORT}")

    # Verify server is running before connecting
    if not is_gateway_running(TEST_GATEWAY_PORT):
        logger.error(f"Gateway server not running on port {TEST_GATEWAY_PORT}")
        pytest.fail(f"Gateway server fixture did not start properly on port {TEST_GATEWAY_PORT}")

    config = GatewayClientConfig(
        host="127.0.0.1",
        port=TEST_GATEWAY_PORT,
    )
    client = GatewayClient(config)
    client.connect()

    # Verify health check works
    if not client.health_check():
        logger.warning("Gateway health check returned False - server may not be fully ready")

    logger.info("Gateway client connected successfully")
    yield client
    client.disconnect()
    logger.info("Gateway client disconnected")


# =============================================================================
# Chain-specific Web3 fixtures
# =============================================================================


@pytest.fixture(scope="module")
def gateway_web3_arbitrum(gateway_client: GatewayClient, anvil_arbitrum: AnvilFixture) -> Web3:
    """Web3 instance for Arbitrum via gateway."""
    return get_gateway_web3(gateway_client, chain="arbitrum")


@pytest.fixture(scope="module")
def gateway_web3_base(gateway_client: GatewayClient, anvil_base: AnvilFixture) -> Web3:
    """Web3 instance for Base via gateway."""
    return get_gateway_web3(gateway_client, chain="base")


@pytest.fixture(scope="module")
def gateway_web3_avalanche(gateway_client: GatewayClient, anvil_avalanche: AnvilFixture) -> Web3:
    """Web3 instance for Avalanche via gateway."""
    return get_gateway_web3(gateway_client, chain="avalanche")


@pytest.fixture(scope="module")
def gateway_web3_bsc(gateway_client: GatewayClient, anvil_bsc: AnvilFixture) -> Web3:
    """Web3 instance for BSC via gateway."""
    return get_gateway_web3(gateway_client, chain="bsc")


@pytest.fixture(scope="module")
def gateway_web3_ethereum(gateway_client: GatewayClient, anvil_ethereum: AnvilFixture) -> Web3:
    """Web3 instance for Ethereum via gateway."""
    return get_gateway_web3(gateway_client, chain="ethereum")


@pytest.fixture(scope="module")
def gateway_web3_optimism(gateway_client: GatewayClient, anvil_optimism: AnvilFixture) -> Web3:
    """Web3 instance for Optimism via gateway."""
    return get_gateway_web3(gateway_client, chain="optimism")


@pytest.fixture(scope="module")
def gateway_web3_polygon(gateway_client: GatewayClient, anvil_polygon: AnvilFixture) -> Web3:
    """Web3 instance for Polygon via gateway."""
    return get_gateway_web3(gateway_client, chain="polygon")


# =============================================================================
# Token funding utilities (direct to Anvil - test infrastructure)
# =============================================================================

# Default port mapping for backward compatibility
# These are overridden by ANVIL_{CHAIN}_PORT env vars when using fixtures
CHAIN_ANVIL_PORTS = {
    "arbitrum": 8545,
    "bsc": 8546,
    "bnb": 8546,
    "avalanche": 8547,
    "base": 8548,
    "ethereum": 8549,
    "optimism": 8550,
    "polygon": 8551,
    "mantle": 8556,
}
# Alias for internal use
_DEFAULT_ANVIL_PORTS = CHAIN_ANVIL_PORTS


def get_anvil_rpc_url(chain: str) -> str:
    """Get Anvil RPC URL for a chain.

    Checks for ANVIL_{CHAIN}_PORT environment variable first (set by fixtures),
    falls back to default port mapping.
    """
    chain_upper = chain.upper()
    env_var = f"ANVIL_{chain_upper}_PORT"
    port_str = os.environ.get(env_var)

    if port_str:
        port = int(port_str)
    else:
        port = _DEFAULT_ANVIL_PORTS.get(chain.lower(), 8545)

    return f"http://127.0.0.1:{port}"


def is_anvil_running(chain: str) -> bool:
    """Check if Anvil is running for a chain.

    Note: When using the new AnvilFixture approach, this check is unnecessary
    since fixtures guarantee Anvil is running.
    """
    rpc_url = get_anvil_rpc_url(chain)
    # Extract port from URL
    port = int(rpc_url.split(":")[-1])
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def fund_native_token(wallet: str, amount_wei: int, chain: str) -> None:
    """Fund wallet with native token via Anvil RPC.

    This goes directly to Anvil, not through gateway.
    Token funding is test infrastructure, not strategy code.

    Args:
        wallet: Wallet address to fund
        amount_wei: Amount in wei
        chain: Chain name for port lookup
    """
    import subprocess

    rpc_url = get_anvil_rpc_url(chain)
    subprocess.run(
        ["cast", "rpc", "anvil_setBalance", wallet, hex(amount_wei), "--rpc-url", rpc_url],
        capture_output=True,
        check=True,
    )


def fund_erc20_token(
    wallet: str,
    token_address: str,
    amount: int,
    balance_slot: int,
    chain: str,
) -> None:
    """Fund wallet with ERC20 token via Anvil storage manipulation.

    This goes directly to Anvil, not through gateway.

    Args:
        wallet: Wallet address to fund
        token_address: ERC20 token contract address
        amount: Amount in token's smallest unit (wei for 18 decimals)
        balance_slot: Storage slot for the balanceOf mapping
        chain: Chain name for port lookup
    """
    import subprocess

    rpc_url = get_anvil_rpc_url(chain)

    # Calculate storage slot for wallet's balance
    result = subprocess.run(
        ["cast", "index", "address", wallet, str(balance_slot)],
        capture_output=True,
        text=True,
        check=True,
    )
    storage_slot = result.stdout.strip()

    # Set storage
    amount_hex = f"0x{amount:064x}"
    subprocess.run(
        ["cast", "rpc", "anvil_setStorageAt", token_address, storage_slot, amount_hex, "--rpc-url", rpc_url],
        capture_output=True,
        check=True,
    )


# =============================================================================
# Common test data
# =============================================================================

# Test wallet (Anvil's first account)
TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

# Chain configurations with token addresses and balance slots
CHAIN_CONFIGS = {
    "arbitrum": {
        "chain_id": 42161,
        "tokens": {
            "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        },
        "balance_slots": {"USDC": 9, "WETH": 3, "USDT": 51},
    },
    "base": {
        "chain_id": 8453,
        "tokens": {
            "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "WETH": "0x4200000000000000000000000000000000000006",
            "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
        },
        "balance_slots": {"USDC": 9, "WETH": 3, "USDbC": 9},
    },
    "avalanche": {
        "chain_id": 43114,
        "tokens": {
            "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            "USDT": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
        },
        "balance_slots": {"USDC": 9, "WAVAX": 3, "USDT": 2},
    },
    "bsc": {
        "chain_id": 56,
        "tokens": {
            "USDT": "0x55d398326f99059fF775485246999027B3197955",
            "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
            "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        },
        "balance_slots": {"USDT": 1, "WBNB": 3, "USDC": 1},  # Binance-Peg tokens use slot 1
    },
    "ethereum": {
        "chain_id": 1,
        "tokens": {
            "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        },
        "balance_slots": {"USDC": 9, "WETH": 3, "USDT": 2, "DAI": 2},
    },
    "optimism": {
        "chain_id": 10,
        "tokens": {
            "USDC": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
            "WETH": "0x4200000000000000000000000000000000000006",
            "USDT": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
        },
        "balance_slots": {"USDC": 9, "WETH": 3, "USDT": 2},
    },
    "polygon": {
        "chain_id": 137,
        "tokens": {
            "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
            "WMATIC": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
            "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        },
        "balance_slots": {"USDC": 9, "WMATIC": 3, "USDT": 2},
    },
}
