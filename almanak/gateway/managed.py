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
    from almanak.framework.anvil.fork_manager import RollingForkManager
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
        external_anvil_ports: dict[str, int] | None = None,
        keep_anvil: bool = False,
    ):
        self.settings = settings
        # Normalize chain names (e.g., "bnb" -> "bsc") via central resolver
        raw_chains = anvil_chains or []
        raw_ports = external_anvil_ports or {}
        normalized: list[str] = []
        normalized_ports: dict[str, int] = {}
        try:
            from almanak.core.constants import resolve_chain_name

            for c in raw_chains:
                try:
                    normalized.append(resolve_chain_name(c))
                except ValueError:
                    normalized.append(c.strip().lower())
            for chain_key, port in raw_ports.items():
                try:
                    normalized_ports[resolve_chain_name(chain_key)] = port
                except ValueError:
                    normalized_ports[chain_key.strip().lower()] = port
        except ImportError:
            normalized = raw_chains
            normalized_ports = raw_ports
        self._anvil_chains = normalized
        self._wallet_address = wallet_address
        self._anvil_funding = anvil_funding or {}
        self._external_anvil_ports = normalized_ports
        self._keep_anvil = keep_anvil
        self._anvil_managers: dict[str, RollingForkManager] = {}
        self._original_env: dict[str, str | None] = {}
        self._server: GatewayServer | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()
        self._stop_requested = threading.Event()
        self._startup_error: BaseException | None = None
        # Chains currently undergoing an intentional reset (watchdog must skip these)
        self._resetting_chains: set[str] = set()

    @property
    def host(self) -> str:
        return self.settings.grpc_host

    @property
    def port(self) -> int:
        return self.settings.grpc_port

    # Chains where free-tier public RPCs lack archive state, causing Anvil fork
    # operations (eth_getStorageAt for ERC-20 approvals, etc.) to fail silently.
    ARCHIVE_RPC_REQUIRED_CHAINS = frozenset({"polygon", "ethereum", "avalanche"})

    def _check_archive_rpc_availability(self) -> None:
        """Warn if any target chain needs archive RPC but only has public RPCs.

        Checks whether ALCHEMY_API_KEY or a chain-specific RPC URL is set.
        If not, emits a warning for each affected chain so users know the
        fork will likely fail on contract storage access.
        """
        has_alchemy = bool(os.environ.get("ALCHEMY_API_KEY"))
        has_generic_rpc = bool(os.environ.get("RPC_URL") or os.environ.get("ALMANAK_RPC_URL"))

        for chain in self._anvil_chains:
            if chain.lower() not in self.ARCHIVE_RPC_REQUIRED_CHAINS:
                continue
            # Skip if external Anvil is provided (user manages RPC)
            if chain in self._external_anvil_ports:
                continue
            # Check chain-specific env vars
            chain_upper = chain.upper()
            has_chain_rpc = bool(
                os.environ.get(f"{chain_upper}_RPC_URL") or os.environ.get(f"ALMANAK_{chain_upper}_RPC_URL")
            )
            if not has_alchemy and not has_generic_rpc and not has_chain_rpc:
                logger.warning(
                    "Chain '%s' requires an archive-capable RPC for Anvil fork testing. "
                    "Set ALCHEMY_API_KEY in your .env file or provide a chain-specific RPC URL "
                    "(%s_RPC_URL). Free-tier public RPCs will likely fail on contract storage access "
                    "(eth_getStorageAt).",
                    chain,
                    chain_upper,
                )

    async def _start_anvil_forks(self) -> None:
        """Start Anvil fork instances for each configured chain.

        For each chain, allocates a free port, starts a RollingForkManager,
        and sets ANVIL_{CHAIN}_PORT env var so the gateway's RPC provider
        routes to the correct Anvil instance.
        """
        import shutil

        from almanak.framework.anvil.fork_manager import RollingForkManager
        from almanak.gateway.utils.rpc_provider import get_rpc_url

        if not shutil.which("anvil"):
            raise RuntimeError(
                "Anvil is not installed. Install Foundry to get Anvil:\n\n"
                "  curl -L https://foundry.paradigm.xyz | bash && foundryup\n\n"
                "See https://book.getfoundry.sh/getting-started/installation for details."
            )

        # Pre-flight: warn if chains need archive RPC but no premium key is set
        self._check_archive_rpc_availability()

        try:
            for chain in self._anvil_chains:
                env_var = f"ANVIL_{chain.upper()}_PORT"

                if chain in self._external_anvil_ports:
                    # External Anvil: set env var, don't start a process
                    port = self._external_anvil_ports[chain]
                    self._original_env[env_var] = os.environ.get(env_var)
                    os.environ[env_var] = str(port)

                    if not is_port_in_use("127.0.0.1", port):
                        raise RuntimeError(
                            f"External Anvil for {chain} not reachable on port {port}. "
                            f"Start it first or remove --anvil-port {chain}={port}"
                        )
                    logger.info("Using external Anvil for %s on port %d", chain, port)
                    continue

                # Managed Anvil: start a new fork
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
            await self._stop_anvil_forks(force=True)
            raise

    # Native gas tokens that are funded via anvil_setBalance (not ERC-20 transfer)
    NATIVE_TOKEN_SYMBOLS = frozenset({"ETH", "AVAX", "MATIC", "BNB", "S", "POL", "MNT", "MON"})
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
        "mantle": "MNT",
        "monad": "MON",
    }

    async def _fund_anvil_wallets(self, chains: list[str] | None = None) -> None:
        """Fund the wallet on each Anvil fork using the anvil_funding config.

        Reads token amounts from self._anvil_funding (set from config.json).
        Native tokens (ETH, AVAX, etc.) are funded via anvil_setBalance.
        ERC-20 tokens are funded via storage slot manipulation.
        If anvil_funding is empty, no funding is performed.
        Errors are logged but do not prevent gateway startup.

        Args:
            chains: If provided, only fund wallets on these chains. If None,
                    fund all managed chains. Pass a single-chain list when
                    re-funding after a watchdog restart to avoid resetting
                    paper-trading state on healthy forks.
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

        managers_to_fund = {c: m for c, m in self._anvil_managers.items() if chains is None or c in chains}
        for chain, manager in managers_to_fund.items():
            try:
                # Only fund the native token that matches this chain
                chain_native = self.CHAIN_NATIVE_SYMBOL.get(chain)
                # Warn if the user specified "ETH" but this chain uses a different native token.
                # This is a common footgun: BSC/Avalanche/Polygon use BNB/AVAX/MATIC, not ETH.
                if chain_native and chain_native != "ETH" and "ETH" in native_amounts:
                    eth_amount = native_amounts["ETH"]
                    logger.warning(
                        "anvil_funding contains 'ETH' (%.4f) but chain='%s' uses '%s' as native token. "
                        "Did you mean '%s'? The 'ETH' entry will NOT fund native gas on this chain. "
                        "Update your config.json anvil_funding key.",
                        eth_amount,
                        chain,
                        chain_native,
                        chain_native,
                    )
                native_amount = native_amounts.get(chain_native, Decimal("0")) if chain_native else Decimal("0")
                if native_amount > 0:
                    await manager.fund_wallet(wallet, native_amount)
                if erc20_tokens:
                    await manager.fund_tokens(wallet, erc20_tokens)
                logger.info(f"Anvil funding complete for {chain}")
            except Exception as e:
                logger.warning(f"Anvil funding failed for {chain}: {e}")

    async def _stop_anvil_forks(self, *, force: bool = False) -> None:
        """Stop all managed Anvil fork instances and restore env vars.

        When keep_anvil is True (and force is False), managed Anvil processes
        are left running and their env vars are preserved. External Anvil env
        vars are always restored since the user manages those processes.

        Args:
            force: If True, always stop managed forks regardless of keep_anvil.
                   Used during error cleanup to avoid orphaned processes.
        """
        if not force and self._keep_anvil and self._anvil_managers:
            for chain, manager in self._anvil_managers.items():
                logger.info(
                    "Keeping Anvil alive for %s on port %d (PID %s)",
                    chain,
                    manager.anvil_port,
                    manager._process.pid if manager._process else "unknown",
                )
            # Restore env vars only for external forks (user manages those)
            for env_var, original in self._original_env.items():
                chain_from_var = env_var.replace("ANVIL_", "").replace("_PORT", "").lower()
                if chain_from_var in self._external_anvil_ports:
                    if original is None:
                        os.environ.pop(env_var, None)
                    else:
                        os.environ[env_var] = original
            return

        # Normal shutdown: stop all managed forks
        for chain, manager in self._anvil_managers.items():
            await manager.stop()
            logger.info("Anvil fork stopped for %s", chain)
        # Restore all env vars
        for env_var, original in self._original_env.items():
            if original is None:
                os.environ.pop(env_var, None)
            else:
                os.environ[env_var] = original

    # Anvil watchdog check interval (seconds). Env-overridable for tests.
    _WATCHDOG_INTERVAL: float = float(os.environ.get("ALMANAK_ANVIL_WATCHDOG_INTERVAL", "5.0"))

    async def _anvil_watchdog(self) -> None:
        """Background task: detect crashed Anvil processes and restart them.

        Runs on the gateway event loop. Checks each managed Anvil process
        every _WATCHDOG_INTERVAL seconds. If a process has exited (poll() is
        not None), resets it to the latest block and re-funds the wallet.

        Does not restart if a gateway shutdown has been requested.
        """
        while not self._stop_requested.is_set():
            await asyncio.sleep(self._WATCHDOG_INTERVAL)
            if self._stop_requested.is_set():
                break
            for chain, manager in list(self._anvil_managers.items()):
                if chain in self._resetting_chains:
                    continue  # Skip chains being intentionally reset via reset_anvil_forks()
                if not manager.is_running:
                    logger.warning(
                        "Anvil fork for %s is no longer running (process exited). Restarting...",
                        chain,
                    )
                    try:
                        ok = await manager.reset_to_latest()
                        if ok:
                            await self._fund_anvil_wallets(chains=[chain])
                            logger.info("Anvil fork for %s restarted successfully", chain)
                        else:
                            logger.error("Failed to restart Anvil fork for %s", chain)
                    except Exception:
                        logger.exception("Error restarting Anvil fork for %s", chain)

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
                # Start Anvil watchdog alongside the main server loop
                if self._anvil_managers:
                    watchdog_task = asyncio.ensure_future(self._anvil_watchdog())
                else:
                    watchdog_task = None
                while not self._stop_requested.is_set():
                    await asyncio.sleep(0.1)
                if watchdog_task is not None:
                    watchdog_task.cancel()
                    try:
                        await watchdog_task
                    except asyncio.CancelledError:
                        pass
                await server.stop()
                if self._anvil_managers:
                    await self._stop_anvil_forks()

            self._loop.run_until_complete(run())
            self._loop.close()
        except Exception as e:
            logger.exception("Managed gateway failed to start")
            if self._anvil_managers and self._loop is not None and not self._loop.is_closed():
                try:
                    self._loop.run_until_complete(self._stop_anvil_forks(force=True))
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
            chains = list(self._anvil_managers.keys())
            self._resetting_chains.update(chains)
            try:
                for chain, manager in self._anvil_managers.items():
                    ok = await manager.reset_to_latest()
                    if not ok:
                        logger.error(f"Failed to reset Anvil fork for {chain}")
                        return False
                    logger.info(f"Anvil fork reset for {chain} to latest block")
                await self._fund_anvil_wallets()
            finally:
                self._resetting_chains.difference_update(chains)
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
