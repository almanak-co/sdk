"""Managed gateway - runs a gateway server in a background daemon thread.

Used by `almanak strat run` to auto-start a gateway when none is running.
Based on the proven GatewayServerThread pattern from tests/conftest_gateway.py.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import socket
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.framework.backtesting.paper.fork_manager import RollingForkManager
    from almanak.gateway.server import GatewayServer

from almanak.gateway.core.settings import GatewaySettings

logger = logging.getLogger(__name__)


def find_free_port() -> int:
    """Find an available port by binding to port 0.

    The OS assigns an available ephemeral port. We immediately close the socket
    and return the port number. There is a small race window before Anvil binds,
    but it is reliable in practice.

    Returns:
        Available port number
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        return s.getsockname()[1]


def is_port_in_use(host: str, port: int) -> bool:
    """Check if a TCP port is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1.0)
        result = s.connect_ex((host, port))
        return result == 0


class GatewayPortUnavailableError(RuntimeError):
    """Raised when no available gateway port can be found in the search range."""

    def __init__(self, start_port: int, end_port: int) -> None:
        super().__init__(
            f"No available port found in range {start_port}-{end_port}. "
            "Set a specific port with: almanak strat run --gateway-port <port>"
        )


def find_available_gateway_port(host: str, preferred_port: int, max_attempts: int = 10) -> int:
    """Find an available gateway port starting from preferred_port.

    Tries preferred_port, then preferred_port+1, etc. up to max_attempts.

    Args:
        host: Host to check.
        preferred_port: Starting port to try.
        max_attempts: Maximum number of ports to try before giving up.

    Returns:
        The first available port found.

    Raises:
        GatewayPortUnavailableError: If no available port is found within max_attempts.
    """
    for offset in range(max_attempts):
        candidate = preferred_port + offset
        if not is_port_in_use(host, candidate):
            return candidate
    raise GatewayPortUnavailableError(preferred_port, preferred_port + max_attempts - 1)


def derive_isolated_wallet(master_private_key: str, strategy_name: str) -> tuple[str, str]:
    """Derive a deterministic child private key from a master key and strategy name.

    Uses HMAC-SHA256 to produce a unique 32-byte key per strategy, ensuring
    wallet isolation when running multiple strategies on a shared Anvil fork.

    Args:
        master_private_key: Hex-encoded master private key (with or without 0x prefix)
        strategy_name: Strategy identifier used as derivation salt

    Returns:
        Tuple of (derived_private_key_hex, derived_wallet_address)

    Raises:
        ValueError: If master_private_key is not a valid 32-byte hex-encoded private key
    """
    from eth_account import Account

    key_hex = master_private_key.removeprefix("0x")
    try:
        key_bytes = bytes.fromhex(key_hex)
    except ValueError as e:
        raise ValueError(f"ALMANAK_PRIVATE_KEY is not valid hex: {e}") from e
    if len(key_bytes) != 32:
        raise ValueError(
            f"ALMANAK_PRIVATE_KEY must be 32 bytes (64 hex chars), got {len(key_bytes)} bytes. "
            "Check that the key is a valid EVM private key."
        )
    derived = hmac.new(key_bytes, strategy_name.encode(), hashlib.sha256).digest()
    derived_key = "0x" + derived.hex()
    account = Account.from_key(derived_key)
    return derived_key, account.address


class ManagedGateway:
    """Runs a gateway gRPC server in a background daemon thread.

    Designed for use by `almanak strat run` to auto-start a gateway.
    The thread is a daemon thread so it dies when the main process exits.

    Usage:
        gateway = ManagedGateway(settings)
        gateway.start()  # blocks until healthy
        try:
            # ... run strategy ...
        finally:
            gateway.stop()

    Or as a context manager:
        with ManagedGateway(settings) as gw:
            # ... run strategy ...
    """

    def __init__(
        self,
        settings: GatewaySettings,
        anvil_chains: list[str] | None = None,
        wallet_address: str | None = None,
        anvil_funding: dict[str, float | int | str] | None = None,
    ):
        self.settings = settings
        self._anvil_chains = anvil_chains or []
        self._wallet_address = wallet_address
        self._anvil_funding = anvil_funding or {}
        self._anvil_managers: dict[str, RollingForkManager] = {}
        self._original_env: dict[str, str | None] = {}
        self._server: GatewayServer | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()
        self._stop_requested = threading.Event()
        self._startup_error: BaseException | None = None

    @property
    def host(self) -> str:
        return self.settings.grpc_host

    @property
    def port(self) -> int:
        return self.settings.grpc_port

    async def _start_anvil_forks(self) -> None:
        """Start Anvil fork instances for each configured chain.

        For each chain, allocates a free port, starts a RollingForkManager,
        and sets ANVIL_{CHAIN}_PORT env var so the gateway's RPC provider
        routes to the correct Anvil instance.
        """
        import shutil

        from almanak.framework.backtesting.paper.fork_manager import RollingForkManager
        from almanak.gateway.utils.rpc_provider import get_rpc_url

        if not shutil.which("anvil"):
            raise RuntimeError(
                "Anvil is not installed. Install Foundry to get Anvil:\n\n"
                "  curl -L https://foundry.paradigm.xyz | bash && foundryup\n\n"
                "See https://book.getfoundry.sh/getting-started/installation for details."
            )

        try:
            for chain in self._anvil_chains:
                port = find_free_port()
                fork_url = get_rpc_url(chain, network="mainnet")
                manager = RollingForkManager(
                    rpc_url=fork_url,
                    chain=chain,
                    anvil_port=port,
                )
                ok = await manager.start()
                if not ok:
                    raise RuntimeError(f"Failed to start Anvil fork for {chain} on port {port}")
                self._anvil_managers[chain] = manager
                # Set env var so gateway RPC provider routes to this Anvil
                env_var = f"ANVIL_{chain.upper()}_PORT"
                self._original_env[env_var] = os.environ.get(env_var)
                os.environ[env_var] = str(port)
                # Redact API key from fork URL to avoid leaking secrets in logs
                # Handles Alchemy (/v2/KEY), Tenderly (/KEY), and other providers
                from urllib.parse import urlparse

                parsed = urlparse(fork_url)
                path = parsed.path.rstrip("/")
                path_parts = path.rsplit("/", 1)
                redacted_path = f"{path_parts[0]}/***" if len(path_parts) > 1 else path
                # Strip userinfo (user:pass@) from netloc to avoid leaking credentials
                host = parsed.netloc.split("@")[-1]
                redacted_url = f"{parsed.scheme}://{host}{redacted_path}"
                logger.info("Anvil fork started for %s on port %d (fork: %s)", chain, port, redacted_url)
        except Exception:
            # Clean up any forks that were already started before re-raising
            await self._stop_anvil_forks()
            raise

    # Native gas tokens that are funded via anvil_setBalance (not ERC-20 transfer)
    NATIVE_TOKEN_SYMBOLS = frozenset({"ETH", "AVAX", "MATIC", "BNB", "S", "POL"})
    CHAIN_NATIVE_SYMBOL: dict[str, str] = {
        "ethereum": "ETH",
        "arbitrum": "ETH",
        "optimism": "ETH",
        "base": "ETH",
        "polygon": "MATIC",
        "avalanche": "AVAX",
        "bsc": "BNB",
        "sonic": "S",
        "plasma": "ETH",
    }

    async def _fund_anvil_wallets(self) -> None:
        """Fund the wallet on each Anvil fork using the anvil_funding config.

        Reads token amounts from self._anvil_funding (set from config.json).
        Native tokens (ETH, AVAX, etc.) are funded via anvil_setBalance.
        ERC-20 tokens are funded via storage slot manipulation.
        If anvil_funding is empty, no funding is performed.
        Errors are logged but do not prevent gateway startup.
        """
        if not self._anvil_funding:
            logger.info("No anvil_funding in config -- skipping wallet funding")
            return

        from decimal import Decimal

        # Determine wallet address
        wallet = self._wallet_address
        if not wallet:
            pk = os.environ.get("ALMANAK_PRIVATE_KEY", "")
            if not pk:
                logger.warning("No wallet address or ALMANAK_PRIVATE_KEY set -- skipping Anvil funding")
                return
            try:
                from eth_account import Account

                pk_hex = pk if pk.startswith("0x") else f"0x{pk}"
                wallet = Account.from_key(pk_hex).address
            except Exception as e:
                logger.warning(f"Could not derive wallet from ALMANAK_PRIVATE_KEY: {e}")
                return

        logger.info(f"Anvil funding for {wallet[:10]}...: {self._anvil_funding}")

        # Separate native tokens (per-symbol) from ERC-20s
        native_amounts: dict[str, Decimal] = {}
        erc20_tokens: dict[str, Decimal] = {}
        for symbol, amount in self._anvil_funding.items():
            try:
                parsed = Decimal(str(amount))
            except Exception as e:
                logger.warning(f"Skipping invalid anvil_funding value for {symbol}: {amount!r} ({e})")
                continue
            if symbol.upper() in self.NATIVE_TOKEN_SYMBOLS:
                sym = symbol.upper()
                native_amounts[sym] = native_amounts.get(sym, Decimal("0")) + parsed
            else:
                erc20_tokens[symbol] = parsed

        for chain, manager in self._anvil_managers.items():
            try:
                # Only fund the native token that matches this chain
                chain_native = self.CHAIN_NATIVE_SYMBOL.get(chain)
                native_amount = native_amounts.get(chain_native, Decimal("0")) if chain_native else Decimal("0")
                if native_amount > 0:
                    await manager.fund_wallet(wallet, native_amount)
                if erc20_tokens:
                    await manager.fund_tokens(wallet, erc20_tokens)
                logger.info(f"Anvil funding complete for {chain}")
            except Exception as e:
                logger.warning(f"Anvil funding failed for {chain}: {e}")

    async def _stop_anvil_forks(self) -> None:
        """Stop all managed Anvil fork instances and restore env vars."""
        for chain, manager in self._anvil_managers.items():
            await manager.stop()
            logger.info("Anvil fork stopped for %s", chain)
        # Restore env vars
        for env_var, original in self._original_env.items():
            if original is None:
                os.environ.pop(env_var, None)
            else:
                os.environ[env_var] = original

    def _run_server(self) -> None:
        """Thread target: create event loop and run server."""
        try:
            from almanak.gateway.server import GatewayServer

            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

            server = GatewayServer(self.settings)
            self._server = server

            async def run():
                if self._anvil_chains and self.settings.network == "anvil":
                    await self._start_anvil_forks()
                    await self._fund_anvil_wallets()
                await server.start()
                self._started.set()
                while not self._stop_requested.is_set():
                    await asyncio.sleep(0.1)
                await server.stop()
                if self._anvil_managers:
                    await self._stop_anvil_forks()

            self._loop.run_until_complete(run())
            self._loop.close()
        except Exception as e:
            logger.exception("Managed gateway failed to start")
            if self._anvil_managers and self._loop is not None and not self._loop.is_closed():
                try:
                    self._loop.run_until_complete(self._stop_anvil_forks())
                except Exception:
                    logger.debug("Suppressed error stopping Anvil forks during cleanup", exc_info=True)
            if self._loop is not None and not self._loop.is_closed():
                self._loop.close()
            self._startup_error = e
            self._started.set()  # unblock the caller so it can see the error

    def start(self, timeout: float = 10.0) -> None:
        """Start the gateway in a background daemon thread.

        Blocks until the server is healthy or the timeout expires.
        Cleans up the background thread on any failure before re-raising.

        Args:
            timeout: Max seconds to wait for the server to become healthy.

        Raises:
            RuntimeError: If the gateway is already started, fails to start, or times out.
        """
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError("Gateway already started")

        # Reset state for a clean start
        self._stop_requested = threading.Event()
        self._started = threading.Event()
        self._startup_error = None

        self._thread = threading.Thread(
            target=self._run_server,
            daemon=True,
            name="managed-gateway",
        )
        self._thread.start()

        if not self._started.wait(timeout=timeout):
            self._cleanup_on_failure()
            raise RuntimeError(f"Managed gateway failed to start within {timeout}s")

        if self._startup_error is not None:
            self._cleanup_on_failure()
            raise RuntimeError(f"Managed gateway failed to start: {self._startup_error}") from self._startup_error

        # Poll gRPC health check to confirm services are registered
        try:
            self._wait_for_healthy(timeout=timeout)
        except RuntimeError:
            self._cleanup_on_failure()
            raise
        logger.info("Managed gateway started on %s:%d", self.host, self.port)

    def _cleanup_on_failure(self) -> None:
        """Stop the background thread after a failed start. Suppresses stop errors."""
        try:
            self.stop(timeout=3.0)
        except Exception:
            logger.debug("Suppressed error during startup cleanup", exc_info=True)

    def _wait_for_healthy(self, timeout: float) -> None:
        """Poll the gRPC health endpoint until it reports SERVING.

        Uses a monotonic deadline so the total wait never exceeds timeout,
        regardless of per-call latency.
        """
        import grpc
        from grpc_health.v1 import health_pb2, health_pb2_grpc

        deadline = time.monotonic() + timeout
        channel = grpc.insecure_channel(f"{self.host}:{self.port}")
        stub = health_pb2_grpc.HealthStub(channel)
        poll_event = threading.Event()
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(f"Managed gateway health check did not pass within {timeout}s")
                try:
                    call_timeout = min(1.0, remaining)
                    resp = stub.Check(health_pb2.HealthCheckRequest(service=""), timeout=call_timeout)
                    if resp.status == health_pb2.HealthCheckResponse.SERVING:
                        return
                except grpc.RpcError:
                    pass
                # Sleep briefly before retrying, but respect the deadline
                poll_event.wait(min(0.1, max(0, deadline - time.monotonic())))
        finally:
            channel.close()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the server to stop and wait for the thread to exit.

        Args:
            timeout: Max seconds to wait for the thread to join.
        """
        self._stop_requested.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        logger.info("Managed gateway stopped")

    def reset_anvil_forks(self) -> bool:
        """Reset all Anvil forks to the latest mainnet block and re-fund wallets.

        Thread-safe: schedules async work on the gateway's event loop and blocks
        until complete. Intended for use as a pre-iteration callback in the
        strategy runner loop.

        Returns:
            True if all resets succeeded, False otherwise.
        """
        if not self._anvil_managers:
            logger.warning("reset_anvil_forks called but no Anvil managers are running")
            return False
        if self._loop is None or self._loop.is_closed():
            logger.error("reset_anvil_forks called but gateway event loop is not available")
            return False

        async def _reset_all() -> bool:
            for chain, manager in self._anvil_managers.items():
                ok = await manager.reset_to_latest()
                if not ok:
                    logger.error(f"Failed to reset Anvil fork for {chain}")
                    return False
                logger.info(f"Anvil fork reset for {chain} to latest block")
            await self._fund_anvil_wallets()
            return True

        import concurrent.futures

        future = asyncio.run_coroutine_threadsafe(_reset_all(), self._loop)
        try:
            return future.result(timeout=60.0)
        except concurrent.futures.TimeoutError:
            logger.error("reset_anvil_forks timed out after 60s")
            return False
        except Exception as e:
            logger.exception(f"reset_anvil_forks failed: {e}")
            return False

    def __enter__(self) -> ManagedGateway:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
