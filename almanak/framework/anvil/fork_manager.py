"""Rolling Fork Manager for Paper Trading.

This module provides a manager that maintains an Anvil fork at the latest block
for real-time paper trading. It supports:
- Starting and stopping Anvil fork processes
- Resetting the fork to the latest mainnet block
- Funding wallets with ETH and ERC-20 tokens
- Automatic reconnection on fork failures

Usage:
    fork_manager = RollingForkManager(
        rpc_url="https://arb1.arbitrum.io/rpc",
        chain="arbitrum",
        anvil_port=8546,
    )

    await fork_manager.start()
    await fork_manager.fund_wallet("0x...", eth_amount=Decimal("10"))
    await fork_manager.fund_tokens("0x...", {"USDC": Decimal("10000")})

    # Reset to latest block periodically
    await fork_manager.reset_to_latest()

    await fork_manager.stop()
"""

import asyncio
import logging
import re
import socket
import subprocess
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.config.framework import framework_config_from_env
from almanak.core.chains._helpers import (
    anvil_balance_slots_map,
    anvil_block_gas_limit_map,
    anvil_funding_tokens_map,
    anvil_whale_tokens_map,
    wrapped_native_deposit_symbol_map,
)

logger = logging.getLogger(__name__)


def _default_fork_cache_path() -> str | None:
    """Resolve the Anvil fork cache path from the typed framework config."""
    cache = framework_config_from_env().anvil_fork_cache_path
    return str(cache) if cache is not None else None


def _default_fork_rpc_timeout() -> float:
    """Resolve the Anvil fork RPC timeout from the typed framework config."""
    return framework_config_from_env().anvil_fork_rpc_timeout_seconds


def _default_fork_health_timeout() -> float:
    """Resolve the Anvil fork health timeout from the typed framework config."""
    return framework_config_from_env().anvil_fork_health_timeout_seconds


_cached_anvil_flags: set[str] | None = None
_anvil_flags_detected: bool = False


def _get_anvil_supported_flags() -> set[str]:
    """Probe anvil --help to discover which flags are actually supported.

    Returns:
        Set of long flag names (e.g. {"--cache-path", "--silent", ...}).
        On failure, returns an empty set so callers can fail-safe.
        Only caches successful detections — transient failures are retried.
    """
    global _cached_anvil_flags, _anvil_flags_detected
    if _anvil_flags_detected:
        return _cached_anvil_flags  # type: ignore[return-value]
    try:
        completed = subprocess.run(
            ["anvil", "--help"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if completed.returncode != 0:
            logger.debug("anvil --help failed (rc=%s): %s", completed.returncode, completed.stderr.strip())
            return set()
        output = completed.stdout
        # Extract all --long-flag patterns from help output
        flags = set(re.findall(r"(--[a-z][a-z0-9-]*)", output))
        if not flags:
            logger.debug("anvil --help returned no parseable long flags")
            return set()
        _cached_anvil_flags = flags
        _anvil_flags_detected = True
        return flags
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
        logger.debug("Failed to probe anvil flags: %s", exc)
    return set()


# =============================================================================
# Constants
# =============================================================================


# Chain IDs for supported chains.
#
# Derived view over :class:`ChainRegistry` (VIB-4801). Anvil can only fork
# EVM chains, so Solana is excluded by family filter.
def _build_chain_ids() -> Mapping[str, int]:
    from types import MappingProxyType

    from almanak.core.chains import ChainRegistry
    from almanak.core.enums import ChainFamily

    return MappingProxyType({d.name: d.chain_id for d in ChainRegistry.all() if d.family is ChainFamily.EVM})


CHAIN_IDS: Mapping[str, int] = _build_chain_ids()

# Per-chain Anvil block gas limit overrides. When set, ``--gas-limit <X>`` is
# passed to the Anvil fork so transactions with chain-specific outsized gas
# limits are not rejected as "intrinsic gas too high".
#
# History:
#  * VIB-3666 (April 2026): first override at 1B for Mantle, wired through the
#    wrong flag (``--block-gas-limit``) which Anvil silently dropped (VIB-3746).
#  * VIB-3746 fix: re-wired through ``--gas-limit``. 1B was sufficient for
#    APPROVE/SUPPLY, the only Mantle ops covered at the time.
#  * #2103 / VIB-4820 (May 2026): unskipping the Agni LP intent tests
#    exposed that the lp_mint compiler estimate is 1B, times the 1.5x framework
#    gas buffer, yields 1.5B per-tx gas_limit. 1B was no longer enough.
#  * 4ce722bec attempted to keep 1B and dropped the alternative
#    ``--disable-block-gas-limit`` branch; this re-broke the LP-open tests at
#    submission with "intrinsic gas too high".
#  * This entry raises the override to 3B — well above the 1.5B per-tx ceiling
#    with comfortable headroom for future protocols on Mantle, while keeping
#    an explicit numeric block ceiling (Anvil 1.7.x rejects combining
#    ``--disable-block-gas-limit`` with ``--gas-limit``, and the
#    disable-flag-alone path showed receipt-not-mined hangs in CI).
#
# Mantle uses a non-standard gas accounting system (L1 calldata overhead
# included in tx.gasLimit), so the per-chain override is required; mainstream
# EVM chains do not need an entry here.
_CHAIN_BLOCK_GAS_LIMITS: Mapping[str, int] = anvil_block_gas_limit_map()


# Common ERC-20 token addresses per chain
TOKEN_ADDRESSES: Mapping[str, Mapping[str, str]] = anvil_funding_tokens_map()
# (Per-chain rows live on ``ChainDescriptor.anvil.funding_tokens`` —
# one file per chain under almanak/core/chains/. VIB-4851 CS-6.)


# Token decimals
TOKEN_DECIMALS: dict[str, int] = {
    "WETH": 18,
    "WETH.e": 18,
    "ETH": 18,
    "USDC": 6,
    "USDC.e": 6,
    "USDbC": 6,
    "USDT": 6,
    "DAI": 18,
    "WBTC": 8,
    "BTC.b": 8,
    "sAVAX": 18,
    "WMATIC": 18,
    "WAVAX": 18,
    "WBNB": 18,
    "BUSD": 18,
    "ARB": 18,
    "GMX": 18,
    "OP": 18,
    "wstETH": 18,
    "stETH": 18,
    "rETH": 18,
    "swETH": 18,
    "ankrETH": 18,
    "pufETH": 18,
    "cbETH": 18,
    "WEETH": 18,
    "WXPL": 18,
    "USDT0": 6,
    "FUSDT0": 6,
    "PENDLE": 18,
    "WBERA": 18,
    "HONEY": 18,
    "wS": 18,
    "WMON": 18,
    "WOKB": 18,
    "xETH": 18,
}


# Known ERC-20 balanceOf storage slots per chain.
# Sourced from verified intent tests (tests/intents/conftest.py CHAIN_CONFIGS).
# Using known slots avoids slow brute-force probing and is 100% reliable.
KNOWN_BALANCE_SLOTS: Mapping[str, Mapping[str, int]] = anvil_balance_slots_map()

# Tokens where storage slot manipulation produces a valid balanceOf() but
# transferFrom() reverts (proxy implementation mismatch).  For these tokens,
# whale impersonation is used instead of slot patching.
# Format: { chain: { token_symbol_upper: whale_address } }
WHALE_FUNDED_TOKENS: Mapping[str, Mapping[str, str]] = anvil_whale_tokens_map()

# Wrapped native tokens that can be funded via deposit() instead of storage
# slot manipulation.  deposit() is more reliable for these contracts because
# it uses the contract's own logic (no proxy/slot mismatch risk).
WRAPPED_NATIVE_TOKENS: Mapping[str, str] = wrapped_native_deposit_symbol_map()
# (Membership == ``anvil.wrapped_native_deposit`` chains; symbol comes from
# ``NativeToken.wrapped_symbol`` verbatim — sonic is "wS"; the deposit-funding
# comparison below uppercases both sides. VIB-4851 CS-6.)


# =============================================================================
# Fork Manager Configuration
# =============================================================================


@dataclass
class ForkManagerConfig:
    """Configuration for RollingForkManager.

    Attributes:
        rpc_url: Archive RPC URL to fork from
        chain: Chain name (e.g., "arbitrum", "ethereum")
        anvil_port: Port to run Anvil on (default 8546)
        startup_timeout_seconds: Timeout for Anvil startup (default 30)
        auto_impersonate: Enable auto-impersonation for any address (default True)
        block_time: Optional block time in seconds (default None = instant)
        fork_block_number: Optional specific block to fork from (default None = latest)
        cache_path: Optional path for Anvil's RPC response cache (reduces upstream RPC calls).
            Defaults to ANVIL_FORK_CACHE_PATH env var if set.
    """

    rpc_url: str
    chain: str
    anvil_port: int = 8546
    startup_timeout_seconds: float = 30.0
    auto_impersonate: bool = True
    block_time: int | None = None
    fork_block_number: int | None = None
    cache_path: str | None = field(default_factory=_default_fork_cache_path)

    def __post_init__(self) -> None:
        """Validate configuration."""
        if not self.rpc_url:
            raise ValueError("rpc_url cannot be empty")

        chain_lower = self.chain.lower()
        if chain_lower not in CHAIN_IDS:
            valid_chains = ", ".join(sorted(CHAIN_IDS.keys()))
            raise ValueError(f"Unsupported chain '{self.chain}'. Valid chains: {valid_chains}")
        self.chain = chain_lower

        if self.anvil_port <= 0 or self.anvil_port > 65535:
            raise ValueError(f"Invalid port: {self.anvil_port}")

        if self.startup_timeout_seconds <= 0:
            raise ValueError(f"startup_timeout_seconds must be positive: {self.startup_timeout_seconds}")

    @property
    def chain_id(self) -> int:
        """Get the chain ID for the configured chain."""
        return CHAIN_IDS[self.chain]

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "rpc_url": self._mask_url(self.rpc_url),
            "chain": self.chain,
            "chain_id": self.chain_id,
            "anvil_port": self.anvil_port,
            "startup_timeout_seconds": self.startup_timeout_seconds,
            "auto_impersonate": self.auto_impersonate,
            "block_time": self.block_time,
            "fork_block_number": self.fork_block_number,
        }

    @staticmethod
    def _mask_url(url: str) -> str:
        """Mask sensitive parts of URL for logging."""
        import re

        if not url:
            return url
        # Mask API keys in path or query
        masked = re.sub(
            r"(api[_-]?key|apikey|key|token)=([^&]+)",
            r"\1=***",
            url,
            flags=re.IGNORECASE,
        )
        # Mask API keys in URL path (common for Alchemy/Infura)
        masked = re.sub(r"/([a-zA-Z0-9_-]{20,})(/|$)", r"/***\2", masked)
        return masked


# =============================================================================
# Rolling Fork Manager
# =============================================================================


@dataclass
class RollingForkManager:
    """Manages an Anvil fork that can be reset to the latest mainnet block.

    This manager provides:
    - Async start/stop lifecycle for Anvil fork
    - Reset to latest block functionality for real-time paper trading
    - Wallet funding with ETH and ERC-20 tokens
    - Health checking and automatic recovery

    The fork runs in a subprocess and communicates via JSON-RPC.

    Attributes:
        rpc_url: Archive RPC URL to fork from
        chain: Chain name (e.g., "arbitrum")
        anvil_port: Port for Anvil (default 8546)
        startup_timeout_seconds: Total readiness budget — caps the
            ``_wait_for_ready`` poll loop (default 30).
        auto_impersonate: Enable auto-impersonation (default True)
        rpc_timeout_seconds: Per-call timeout for post-ready operational
            JSON-RPC calls (env ``ALMANAK_FORK_RPC_TIMEOUT``).
        health_timeout_seconds: Per-probe timeout for the readiness health
            check (the ``eth_blockNumber`` probe inside ``_wait_for_ready``),
            distinct from ``rpc_timeout_seconds`` and from the
            ``startup_timeout_seconds`` total budget (env
            ``ALMANAK_FORK_HEALTH_TIMEOUT``).

    Example:
        manager = RollingForkManager(
            rpc_url="https://arb1.arbitrum.io/rpc",
            chain="arbitrum",
        )
        await manager.start()
        await manager.fund_wallet("0x...", eth_amount=Decimal("10"))
        # ... paper trading ...
        await manager.stop()
    """

    rpc_url: str
    chain: str
    anvil_port: int = 8546
    startup_timeout_seconds: float = 30.0
    auto_impersonate: bool = True
    block_time: int | None = None
    fork_block_number: int | None = None
    cache_path: str | None = field(default_factory=_default_fork_cache_path)
    # When True, spawn Anvil in its OWN session (start_new_session) so the fork
    # survives the runner's process-group signals and its exit — required for a
    # `--keep-anvil` post-teardown audit window (VIB-5063). Off by default: a
    # normal run leaves Anvil in the runner's group and stops it explicitly via
    # `.stop()`, so nothing changes unless the caller opted into keep-alive.
    keep_alive_detached: bool = False

    # Timeout defaults (env-overridable)
    rpc_timeout_seconds: float = field(default_factory=_default_fork_rpc_timeout)
    health_timeout_seconds: float = field(default_factory=_default_fork_health_timeout)

    # Internal state (not initialized in __init__)
    _process: subprocess.Popen[bytes] | None = field(default=None, repr=False, init=False)
    _is_running: bool = field(default=False, repr=False, init=False)
    _current_block: int | None = field(default=None, repr=False, init=False)
    _start_time: float | None = field(default=None, repr=False, init=False)

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not self.rpc_url:
            raise ValueError("rpc_url cannot be empty")

        chain_lower = self.chain.lower()
        if chain_lower not in CHAIN_IDS:
            valid_chains = ", ".join(sorted(CHAIN_IDS.keys()))
            raise ValueError(f"Unsupported chain '{self.chain}'. Valid chains: {valid_chains}")
        self.chain = chain_lower

        if self.anvil_port <= 0 or self.anvil_port > 65535:
            raise ValueError(f"Invalid port: {self.anvil_port}")

        if self.rpc_timeout_seconds <= 0:
            raise ValueError(f"rpc_timeout_seconds must be positive: {self.rpc_timeout_seconds}")
        if self.health_timeout_seconds <= 0:
            raise ValueError(f"health_timeout_seconds must be positive: {self.health_timeout_seconds}")

    @property
    def chain_id(self) -> int:
        """Get the chain ID for the configured chain."""
        return CHAIN_IDS[self.chain]

    @property
    def is_running(self) -> bool:
        """Check if the fork is currently running."""
        return self._is_running and self._process is not None and self._process.poll() is None

    @property
    def current_block(self) -> int | None:
        """Get the current fork block number."""
        return self._current_block

    async def _validate_source_chain_id(self) -> None:
        """Validate the source RPC URL returns the expected chain ID.

        Calls eth_chainId on the fork source RPC to catch misconfigured
        RPC URLs early (e.g., Arbitrum URL used for a Base strategy).

        Raises:
            RuntimeError: If source chain ID doesn't match expected chain
        """
        import aiohttp

        payload = {
            "jsonrpc": "2.0",
            "method": "eth_chainId",
            "params": [],
            "id": 1,
        }

        try:
            from almanak.gateway.utils.ssl_context import build_ssl_context

            ssl_ctx = build_ssl_context()
            connector = aiohttp.TCPConnector(ssl=ssl_ctx)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(
                    self.rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=self.rpc_timeout_seconds)
                ) as response:
                    result_data: dict[str, Any] = await response.json()
                    if "error" in result_data:
                        logger.warning("Could not verify source RPC chain ID: %s", result_data["error"])
                        return

                    source_chain_id = int(result_data["result"], 16)
                    expected_chain_id = self.chain_id

                    if source_chain_id != expected_chain_id:
                        actual_chain = next(
                            (name for name, cid in CHAIN_IDS.items() if cid == source_chain_id),
                            f"unknown (chain_id={source_chain_id})",
                        )
                        raise RuntimeError(
                            f"Fork source RPC chain_id mismatch: expected {expected_chain_id} "
                            f"({self.chain}) but got {source_chain_id} ({actual_chain}). "
                            f"Check RPC_URL configuration for the {self.chain} chain."
                        )

                    logger.debug("Source RPC chain_id validated: %d (%s)", source_chain_id, self.chain)
        except (TimeoutError, aiohttp.ClientError, OSError) as e:
            logger.warning("Could not verify source RPC chain ID due to a network error: %s", e)
        except (KeyError, ValueError) as e:
            logger.warning("Failed to parse chain ID from RPC response: %s", e)

    async def start(self) -> bool:
        """Start the Anvil fork.

        Starts Anvil as a subprocess forking from the configured RPC URL.
        Waits for Anvil to be ready to accept connections.

        Returns:
            True if Anvil started successfully, False otherwise

        Raises:
            RuntimeError: If Anvil is already running
        """
        if self._is_running:
            logger.warning("Anvil fork is already running")
            return True

        try:
            # Validate source RPC chain ID before forking
            await self._validate_source_chain_id()

            # Build Anvil command
            cmd = self._build_anvil_command()

            logger.info(
                f"Starting Anvil fork: chain={self.chain}, port={self.anvil_port}, "
                f"fork_block={self.fork_block_number or 'latest'}"
            )
            masked_cmd = [ForkManagerConfig._mask_url(arg) for arg in cmd]
            logger.debug(f"Anvil command: {' '.join(masked_cmd)}")

            # Start Anvil process. When keep_alive_detached is set, put Anvil in its
            # own session so a `--keep-anvil` fork outlives the runner's exit and is
            # immune to process-group signals (Ctrl-C / terminal SIGHUP); the runner
            # still stops it explicitly by PID via .stop() on the normal path.
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=self.keep_alive_detached,
            )

            # Wait for Anvil to be ready
            ready = await self._wait_for_ready()
            if not ready:
                logger.error("Anvil fork startup timed out")
                await self.stop()
                return False

            self._is_running = True
            self._start_time = time.time()

            # For chains with a block-gas-limit override, mine a single empty
            # block so the override is baked in BEFORE any pytest fixture
            # captures an evm_snapshot. Anvil applies ``--gas-limit`` only on
            # the first mined block after fork creation: if a pristine
            # snapshot is taken at the raw forked state (gas_limit inherited
            # from mainnet, e.g. ~60M for Mantle), every subsequent
            # ``evm_revert`` restores that low ceiling and the override is
            # silently lost — txs above 60M then fail with "intrinsic gas
            # too high". Pre-mining here means any later snapshot/revert
            # cycle preserves the override (VIB-3666 / VIB-3746 / #2103).
            if self.chain in _CHAIN_BLOCK_GAS_LIMITS:
                success, _ = await self._rpc_call_raw("evm_mine", [])
                if not success:
                    logger.warning(
                        "evm_mine to bake in --gas-limit override for chain=%s failed; "
                        "subsequent evm_revert may lose the block-gas-limit ceiling",
                        self.chain,
                    )

            # Get current block number
            self._current_block = await self._get_block_number()

            logger.info(
                f"Anvil fork started: port={self.anvil_port}, block={self._current_block}, chain_id={self.chain_id}"
            )
            return True

        except FileNotFoundError:
            logger.error("Anvil not found. Please install Foundry: curl -L https://foundry.paradigm.xyz | bash")
            return False
        except Exception as e:
            logger.exception(f"Failed to start Anvil fork: {e}")
            await self.stop()
            return False

    async def stop(self) -> None:
        """Stop the Anvil fork.

        Terminates the Anvil subprocess and cleans up resources.
        """
        if self._process is not None:
            try:
                self._process.terminate()
                # Wait for process to terminate with timeout
                try:
                    loop = asyncio.get_running_loop()
                    await asyncio.wait_for(
                        loop.run_in_executor(None, self._process.wait),
                        timeout=5.0,
                    )
                except RuntimeError:
                    # Event loop closing/closed — fall back to synchronous wait
                    logger.debug("Event loop closing, using synchronous process wait")
                    self._process.wait(timeout=5)
                except TimeoutError:
                    logger.warning("Anvil process did not terminate, killing")
                    self._process.kill()
                    self._process.wait()
            except Exception as e:
                logger.warning(f"Error stopping Anvil: {e}")
            finally:
                self._process = None

        self._is_running = False
        self._current_block = None
        self._start_time = None

        # Wait for port to be freed (skip if event loop is shutting down)
        try:
            await self._wait_for_port_free(timeout=5.0)
        except RuntimeError:
            # Event loop closing — port will be freed when process exits
            logger.debug("Skipping port-free wait: event loop closing")
        logger.info("Anvil fork stopped")

    async def _wait_for_port_free(self, timeout: float = 5.0) -> None:
        """Wait for the Anvil port to be freed after process termination.

        Uses SO_REUSEADDR on the check socket to match Anvil's own binding
        behavior (tokio sets SO_REUSEADDR by default on Unix). This avoids
        false negatives from TCP TIME_WAIT connections after process exit.

        Args:
            timeout: Maximum seconds to wait for port to be freed
        """
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    s.bind(("127.0.0.1", self.anvil_port))
                    return  # Port is free (or only TIME_WAIT remnants)
            except OSError:
                await asyncio.sleep(0.1)
        logger.debug(f"Port {self.anvil_port} still in use after {timeout}s")

    async def reset_to_latest(self) -> bool:
        """Reset the fork to the latest mainnet block.

        Uses Anvil's anvil_reset RPC to re-fork in-place without restarting the
        process. This is much faster and avoids upstream RPC rate limiting from
        repeated process startups. Falls back to stop/start if anvil_reset fails.

        Returns:
            True if reset successful, False otherwise
        """
        # Try in-place reset via anvil_reset RPC (preferred — no process restart)
        if self.is_running:
            try:
                success, _ = await self._rpc_call_raw(
                    "anvil_reset",
                    [{"forking": {"jsonRpcUrl": self.rpc_url}}],
                )
                if success:
                    # Clear pinned block so next auto-restart forks latest too
                    self.fork_block_number = None
                    # Update current block number
                    block_hex = await self._rpc_call("eth_blockNumber", [])
                    if block_hex:
                        self._current_block = int(block_hex, 16)

                    # VIB-2552: Assert chain ID integrity after reset
                    await self._assert_chain_id_after_reset()

                    logger.info(f"Fork reset in-place to block {self._current_block}")
                    return True
                else:
                    logger.warning("anvil_reset RPC failed, falling back to stop/start")
            except Exception as e:
                logger.warning(f"anvil_reset failed ({e}), falling back to stop/start")

        # Fallback: stop and restart the process
        logger.info("Resetting fork via process restart...")
        original_fork_block = self.fork_block_number

        try:
            await self.stop()
            self.fork_block_number = None
            success = await self.start()

            if success:
                # VIB-2552: Also assert chain ID after stop/start fallback
                await self._assert_chain_id_after_reset()
                logger.info(f"Fork reset to latest block: {self._current_block}")
            else:
                logger.error("Failed to reset fork to latest block")
                self.fork_block_number = original_fork_block

            return success

        except Exception as e:
            logger.exception(f"Error resetting fork: {e}")
            self.fork_block_number = original_fork_block
            return False

    async def advance_time(self, seconds: int) -> bool:
        """Advance the fork's block timestamp and mine a new block.

        Used by persistent fork mode to simulate passage of time so that
        on-chain yield accrual (lending interest, etc.) progresses.

        Args:
            seconds: Number of seconds to advance the block timestamp.

        Returns:
            True if successful, False otherwise.
        """
        if not self.is_running:
            logger.warning("Cannot advance time: fork is not running")
            return False

        try:
            success, _ = await self._rpc_call_raw("evm_increaseTime", [seconds])
            if not success:
                logger.warning(f"evm_increaseTime({seconds}) failed")
                return False

            success, _ = await self._rpc_call_raw("evm_mine", [])
            if not success:
                logger.warning("evm_mine failed after evm_increaseTime")
                return False

            # Update current block number
            block_hex = await self._rpc_call("eth_blockNumber", [])
            if block_hex:
                self._current_block = int(block_hex, 16)

            logger.debug(f"Advanced fork time by {seconds}s, block={self._current_block}")
            return True
        except Exception as e:
            logger.warning(f"Failed to advance fork time: {e}")
            return False

    async def _assert_chain_id_after_reset(self) -> None:
        """Assert that the fork reports the expected chain ID after a reset.

        VIB-2552: After anvil_reset, verify that eth_chainId matches the
        expected chain ID. If mismatched, attempt to fix with anvil_setChainId.
        This prevents "invalid chain id for signer" errors on non-default chains.
        """
        expected_chain_id = CHAIN_IDS.get(self.chain)
        if expected_chain_id is None:
            return  # Unknown chain, skip assertion

        chain_id_hex = await self._rpc_call("eth_chainId", [])
        if not chain_id_hex:
            logger.warning(f"Could not query eth_chainId after reset for chain={self.chain}")
            return

        actual_chain_id = int(chain_id_hex, 16)
        if actual_chain_id == expected_chain_id:
            return  # All good

        logger.warning(
            f"Chain ID mismatch after anvil_reset: expected {expected_chain_id} "
            f"({self.chain}), got {actual_chain_id}. Attempting anvil_setChainId fix."
        )

        # Attempt to fix with anvil_setChainId
        try:
            success, _ = await self._rpc_call_raw("anvil_setChainId", [expected_chain_id])
            if success:
                logger.info(f"Fixed chain ID via anvil_setChainId: {expected_chain_id} ({self.chain})")
            else:
                logger.error(
                    f"anvil_setChainId failed for chain={self.chain}. "
                    f"Transactions may fail with 'invalid chain id for signer'."
                )
        except Exception as e:
            logger.error(f"anvil_setChainId error: {e}")

    async def _clear_7702_delegation(self, address: str) -> None:
        """Strip EIP-7702 delegation code from a funded wallet on the fork.

        Mainnet EOAs increasingly carry 7702 delegation designators
        (code ``0xef01...``). On a fork, that delegated code intercepts
        plain ETH transfers and protocol native-token legs (e.g. Fluid
        ``operate()`` collateral refunds revert with FluidLiquidityError
        11011), so the intent-test harness has cleared it per-test for a
        while — but the managed-gateway funding path did not, breaking any
        native-ETH-receiving demo/E2E on managed Anvil.

        Only the CANONICAL 7702 designator is cleared: exactly 23 bytes —
        ``0xef0100`` followed by the 20-byte delegate address. Real contract
        wallets (Zodiac Safes) also have code at the wallet address and
        clearing them would destroy the wallet — they never match this
        shape. Failures are logged and swallowed: funding must not be
        blocked by a hygiene step.
        """
        try:
            success, code = await self._rpc_call_raw("eth_getCode", [address, "latest"])
            if not success or not isinstance(code, str):
                return
            normalized = code.lower()
            # 23 bytes = "0x" + 46 hex chars; prefix 0xef0100 (EIP-7702 §delegation designation).
            if normalized.startswith("0xef0100") and len(normalized) == 48:
                cleared, _ = await self._rpc_call_raw("anvil_setCode", [address, "0x"])
                if cleared:
                    logger.info(f"Cleared EIP-7702 delegation code from {address[:10]}... on {self.chain} fork")
                else:
                    logger.warning(f"Failed to clear EIP-7702 delegation code from {address[:10]}... on {self.chain}")
        except Exception as e:
            logger.warning(f"7702 delegation check failed for {address[:10]}...: {e}")

    async def fund_wallet(self, address: str, eth_amount: Decimal) -> bool:
        """Fund a wallet with ETH.

        Uses Anvil's anvil_setBalance RPC method to set the ETH balance.

        Args:
            address: Wallet address to fund
            eth_amount: Amount of ETH to set as balance

        Returns:
            True if funding successful, False otherwise
        """
        if not self.is_running:
            logger.error("Cannot fund wallet: Anvil fork not running")
            return False

        await self._clear_7702_delegation(address)

        try:
            # Convert ETH to wei (hex string)
            wei_amount = int(eth_amount * Decimal("1e18"))
            wei_hex = hex(wei_amount)

            # Call anvil_setBalance (returns null on success)
            success, _ = await self._rpc_call_raw("anvil_setBalance", [address, wei_hex])

            if success:
                logger.info(f"Funded {address[:10]}... with {eth_amount} ETH")
                return True
            else:
                logger.error(f"Failed to fund wallet {address[:10]}...")
                return False

        except Exception as e:
            logger.exception(f"Error funding wallet: {e}")
            return False

    # crap-allowlist: 5-tier funding fallback — wrapped-native deposit() / whale
    # impersonation / known balance slot / anvil_deal / brute-force slot probe.
    # Each tier exists to fund a class of token the prior tier can't handle safely,
    # and the "first-that-succeeds-wins" sequencing relies on shared skip_storage_fallback
    # state that doesn't cleanly split across helper boundaries.
    async def fund_tokens(  # noqa: C901
        self,
        address: str,
        tokens: dict[str, Decimal],
    ) -> bool:
        """Fund a wallet with ERC-20 tokens.

        Token keys can be symbols ("USDC", "wstETH") or ERC-20 addresses
        ("0xf951E335..."). Symbols are matched case-insensitively.

        Funding priority:
        1. Known storage slots (fast, reliable -- verified in intent tests)
        2. anvil_deal RPC (works on newer Anvil versions)
        3. Brute-force storage slot probing (slow fallback)

        Args:
            address: Wallet address to fund
            tokens: Dict mapping token symbol/address to amount

        Returns:
            True if all tokens funded successfully, False otherwise
        """
        if not self.is_running:
            logger.error("Cannot fund tokens: Anvil fork not running")
            return False

        from almanak.framework.data.tokens import get_token_resolver
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError, TokenResolutionError

        resolver = get_token_resolver()
        chain_tokens = TOKEN_ADDRESSES.get(self.chain, {})
        known_slots = KNOWN_BALANCE_SLOTS.get(self.chain, {})

        # Build case-insensitive lookup indexes for local tables
        chain_tokens_ci = {k.lower(): v for k, v in chain_tokens.items()}
        known_slots_ci = {k.lower(): v for k, v in known_slots.items()}
        token_decimals_ci = {k.lower(): v for k, v in TOKEN_DECIMALS.items()}

        # Load paper-local token overrides (VIB-2378). Imported from the anvil
        # package (not backtesting) so funding never pulls in report_generator
        # /jinja2 or the paper engine — see token_overrides module docstring.
        from almanak.framework.anvil.token_overrides import load_token_overrides

        paper_overrides_raw = load_token_overrides(self.chain)
        paper_overrides_ci = {k.lower(): v for k, v in paper_overrides_raw.items()}

        success = True

        for token_key, amount in tokens.items():
            is_raw_address = isinstance(token_key, str) and token_key.startswith(("0x", "0X")) and len(token_key) == 42

            token_address: str | None = None
            decimals: int | None = None
            display_name = token_key  # for log messages

            if is_raw_address:
                # Token key is an ERC-20 address — use it directly
                token_address = token_key.lower()
                try:
                    # Resolver normalizes case internally, so checksummed/lower/upper all work
                    resolved = resolver.resolve(token_address, self.chain)
                    decimals = resolved.decimals
                    display_name = resolved.symbol or token_key[:10] + "..."
                except (TokenNotFoundError, TokenResolutionError):
                    # Not in resolver — try on-chain decimals() call
                    decimals = await self._fetch_decimals_onchain(token_address)
                    if decimals is not None:
                        logger.info(f"Resolved decimals={decimals} for {token_key[:10]}... via on-chain call")
            else:
                # Token key is a symbol — resolve via TokenResolver then local fallbacks
                try:
                    resolved = resolver.resolve(token_key, self.chain)
                    token_address = resolved.address
                    decimals = resolved.decimals
                    display_name = resolved.symbol or token_key
                except (TokenNotFoundError, TokenResolutionError):
                    # Check paper-local overrides before local TOKEN_ADDRESSES (VIB-2378)
                    override = paper_overrides_ci.get(token_key.lower())
                    if override is not None:
                        token_address = override.address
                        decimals = override.decimals
                        display_name = token_key
                        logger.info(f"Resolved {token_key} via paper-local token override: {token_address}")
                        # Address-only overrides (decimals=None): try on-chain lookup
                        if decimals is None:
                            decimals = await self._fetch_decimals_onchain(token_address)
                            if decimals is not None:
                                logger.info(f"Resolved decimals={decimals} for {display_name} via on-chain call")
                    else:
                        # Fallback to local TOKEN_ADDRESSES (case-insensitive)
                        token_address = chain_tokens_ci.get(token_key.lower())
                        decimals = token_decimals_ci.get(token_key.lower())

            if not token_address:
                if is_raw_address:
                    logger.error(
                        f"Cannot resolve token address {token_key} on {self.chain}. "
                        f"Verify the contract is deployed on this chain."
                    )
                else:
                    logger.warning(f"Unknown token {token_key} for chain {self.chain}, skipping")
                success = False
                continue
            if decimals is None:
                logger.error(
                    f"Unknown decimals for {display_name} on {self.chain}, skipping (refusing to default to 18)"
                )
                success = False
                continue

            # Convert to token units (hex string)
            token_units = int(amount * Decimal(10**decimals))
            amount_hex = hex(token_units)

            try:
                funded = False
                skip_storage_fallback = False

                # Use display_name (resolved symbol) for priority-0 lookups so
                # raw-address inputs also match (e.g., "0xC02...Cc2" resolves to "WETH").
                lookup_symbol = display_name.upper()

                # Priority 0a: Wrapped native deposit() (VIB-2571)
                # For WETH/WAVAX/WBNB etc., calling deposit() with ETH value is
                # more reliable than storage slot manipulation (proxy/slot issues).
                wrapped_native = WRAPPED_NATIVE_TOKENS.get(self.chain)
                if wrapped_native and lookup_symbol == wrapped_native.upper():
                    funded = await self._fund_wrapped_native_via_deposit(
                        address, token_address, amount_hex, amount, display_name
                    )
                    if funded:
                        # deposit() succeeded: skip storage-slot paths to avoid
                        # producing mixed on-chain state (balanceOf vs internal).
                        skip_storage_fallback = True
                    # else: deposit() failed (e.g. transient RPC error, insufficient
                    # native balance); fall through to known-slot / anvil_deal / brute-force.
                    # WAVAX slot 3 is a reliable fallback on Avalanche (VIB-2690).

                # Priority 0b: Whale impersonation (VIB-2571)
                # For tokens where storage slot patches pass balanceOf but break
                # transferFrom (e.g., Ethereum USDC FiatTokenProxy, Base cbBTC).
                if not funded:
                    whale_tokens = WHALE_FUNDED_TOKENS.get(self.chain, {})
                    whale_address = whale_tokens.get(lookup_symbol)
                    if whale_address:
                        funded = await self._fund_token_via_whale(
                            address, token_address, amount_hex, whale_address, display_name
                        )
                        # Whale-funded tokens are listed precisely because slot
                        # patching produces broken internal state (blacklist
                        # mapping, allowances). Skip storage fallback regardless
                        # of whale outcome — falling through would corrupt proxy
                        # state on tokens we know are unsafe for slot patching.
                        skip_storage_fallback = True
                        if not funded:
                            # `_fund_token_via_whale` logs only at debug; surface
                            # the failure so the cause (e.g. whale has no gas,
                            # transfer reverted) is visible at the top level
                            # instead of just "Failed to fund X" with no reason.
                            logger.error(
                                f"Whale impersonation failed for {display_name} on {self.chain}; "
                                f"refusing storage-slot fallback (would corrupt proxy state)"
                            )

                # Priority 1: Known storage slot (fast and reliable)
                # Look up by original symbol key (case-insensitive)
                if not funded and not skip_storage_fallback:
                    known_slot = known_slots_ci.get(token_key.lower())
                    if known_slot is not None:
                        funded = await self._set_balance_at_slot(
                            address, token_address, amount_hex, known_slot, display_name
                        )

                # Priority 2: anvil_deal RPC (returns null on success)
                if not funded:
                    deal_success, _ = await self._rpc_call_raw(
                        "anvil_deal",
                        [token_address, address, amount_hex],
                    )
                    if deal_success:
                        logger.info(f"Funded {address[:10]}... with {amount} {display_name} via anvil_deal")
                        funded = True

                # Priority 3: Brute-force storage slot probing
                if not funded and not skip_storage_fallback:
                    funded = await self._fund_token_via_storage(address, token_address, amount_hex, display_name)

                if not funded:
                    logger.error(f"Failed to fund {display_name} for {address[:10]}...")
                    success = False

            except Exception as e:
                logger.exception(f"Error funding {display_name}: {e}")
                success = False

        return success

    async def _fetch_decimals_onchain(self, token_address: str) -> int | None:
        """Fetch ERC-20 decimals via on-chain eth_call.

        Uses the decimals() selector (0x313ce567) as a last-resort fallback
        when the token is not in the static registry or resolver.

        Args:
            token_address: Lowercased ERC-20 contract address

        Returns:
            Number of decimals, or None if the call fails
        """
        try:
            ok, result = await self._rpc_call_raw(
                "eth_call",
                [{"to": token_address, "data": "0x313ce567"}, "latest"],
            )
            if ok and result and isinstance(result, str) and len(result) >= 2:
                decimals_val = int(result, 16)
                if 0 <= decimals_val <= 77:
                    return decimals_val
                logger.warning("On-chain decimals() returned implausible value %d for %s", decimals_val, token_address)
                return None
        except Exception as e:
            logger.debug("On-chain decimals() call failed for %s: %s", token_address, e)
        return None

    async def _set_balance_at_slot(
        self,
        wallet_address: str,
        token_address: str,
        amount_hex: str,
        slot: int,
        token_symbol: str,
    ) -> bool:
        """Set token balance at a specific known storage slot.

        Args:
            wallet_address: Address to fund
            token_address: Token contract address
            amount_hex: Amount in hex (already scaled to token decimals)
            slot: The known balanceOf mapping slot number
            token_symbol: Token symbol for logging

        Returns:
            True if successful (balance verified)
        """
        storage_slot = self._calculate_mapping_slot(wallet_address, slot)

        success, _ = await self._rpc_call_raw(
            "anvil_setStorageAt",
            [token_address, storage_slot, self._pad_hex_to_32_bytes(amount_hex)],
        )

        if not success:
            return False

        # Mine a block to apply storage changes
        await self._rpc_call_raw("evm_mine", [])

        # Verify the balance was set
        balance = await self._get_token_balance(token_address, wallet_address)
        expected = int(amount_hex, 16)

        if balance == expected:
            logger.info(f"Funded {wallet_address[:10]}... with {token_symbol} via known slot {slot}")
            return True

        logger.debug(f"Known slot {slot} for {token_symbol}: balance {balance} != expected {expected}")
        return False

    async def _fund_wrapped_native_via_deposit(
        self,
        wallet_address: str,
        token_address: str,
        amount_hex: str,
        amount: Decimal,
        token_symbol: str,
    ) -> bool:
        """Fund wrapped native token (WETH, WAVAX, etc.) via deposit().

        Calls the token contract's deposit() function with ETH value.
        More reliable than storage slot manipulation for wrapped native
        tokens, especially on Ethereum where WETH9 slot 3 can produce
        incorrect balances on Anvil forks.

        The wallet must already have sufficient native balance (set via
        fund_wallet before fund_tokens).
        """
        # deposit() function selector
        deposit_selector = "0xd0e30db0"

        try:
            # Send native currency to the wrapped token contract via deposit()
            # On Anvil, all accounts are unlocked — no signing needed
            success, tx_hash = await self._rpc_call_raw(
                "eth_sendTransaction",
                [
                    {
                        "from": wallet_address,
                        "to": token_address,
                        "value": amount_hex,
                        "data": deposit_selector,
                    }
                ],
            )

            if not success:
                logger.debug(f"deposit() call failed for {token_symbol}")
                return False

            # Mine a block to confirm the transaction
            await self._rpc_call_raw("evm_mine", [])

            # Verify the balance
            balance = await self._get_token_balance(token_address, wallet_address)
            expected = int(amount_hex, 16)

            if balance >= expected:
                logger.info(f"Funded {wallet_address[:10]}... with {amount} {token_symbol} via deposit()")
                return True

            logger.debug(f"deposit() for {token_symbol}: balance {balance} < expected {expected}")
            return False

        except Exception as e:
            logger.debug(f"deposit() funding failed for {token_symbol}: {e}")
            return False

    async def _fund_token_via_whale(
        self,
        wallet_address: str,
        token_address: str,
        amount_hex: str,
        whale_address: str,
        token_symbol: str,
    ) -> bool:
        """Fund token by impersonating a whale and transferring.

        For proxy tokens (e.g., Ethereum USDC) where storage slot patching
        makes balanceOf() return the right value but transferFrom() reverts.
        Impersonation produces a real transfer with consistent internal state.
        """
        # Gas top-up threshold: any whale with < 0.1 ETH gets bumped to 1 ETH
        # for the duration of this call. Most realistic whales are contract
        # addresses (Aave aTokens, lending pools) that hold huge token reserves
        # but carry 0 ETH, so eth_sendTransaction would fail with "Insufficient
        # funds for gas * price + value" without this.
        MIN_GAS_WEI = 10**17
        TOPUP_WEI = 10**18  # 1 ETH — plenty for one transfer

        original_balance_hex: str | None = None
        try:
            # Impersonate the whale account
            success, _ = await self._rpc_call_raw("anvil_impersonateAccount", [whale_address])
            if not success:
                logger.debug(f"Failed to impersonate whale {whale_address[:10]}... for {token_symbol}")
                return False

            try:
                # Read the whale's current ETH balance. If it already has enough
                # for gas, leave it alone — don't perturb fork state needlessly.
                # If we do top up, capture the original so we can restore it in
                # the finally block (keeps the whale's balance invariant from
                # the strategy's perspective once impersonation ends).
                #
                # Bail out hard if the balance read fails: treating a failed
                # eth_getBalance as 0 would mean later restoring the whale to
                # "0x0", silently zeroing a non-zero balance that we just
                # couldn't read due to transient RPC error. Better to fail the
                # whale-funding attempt cleanly than to mutate fork state we
                # can't see.
                ok, balance_result = await self._rpc_call_raw("eth_getBalance", [whale_address, "latest"])
                if not ok or not isinstance(balance_result, str):
                    logger.debug(
                        f"Failed to read ETH balance for whale {whale_address[:10]}... — "
                        f"aborting whale-funded transfer to avoid clobbering unknown balance"
                    )
                    return False
                current_balance = int(balance_result, 16)
                if current_balance < MIN_GAS_WEI:
                    original_balance_hex = balance_result
                    topup_hex = "0x" + format(TOPUP_WEI, "x")
                    await self._rpc_call_raw("anvil_setBalance", [whale_address, topup_hex])

                # ERC-20 transfer(address,uint256) selector = 0xa9059cbb
                # Encode: selector + address padded to 32 bytes + amount padded to 32 bytes
                addr_padded = wallet_address.lower().replace("0x", "").zfill(64)
                amt_padded = amount_hex.replace("0x", "").zfill(64)
                calldata = "0xa9059cbb" + addr_padded + amt_padded

                success, _tx_hash = await self._rpc_call_raw(
                    "eth_sendTransaction",
                    [
                        {
                            "from": whale_address,
                            "to": token_address,
                            "data": calldata,
                        }
                    ],
                )

                if not success:
                    logger.debug(f"Whale transfer failed for {token_symbol}")
                    return False

                # Mine a block to confirm
                await self._rpc_call_raw("evm_mine", [])

                # Verify
                balance = await self._get_token_balance(token_address, wallet_address)
                expected = int(amount_hex, 16)

                if balance >= expected:
                    logger.info(f"Funded {wallet_address[:10]}... with {token_symbol} via whale impersonation")
                    return True

                logger.debug(f"Whale transfer for {token_symbol}: balance {balance} < expected {expected}")
                return False
            finally:
                # Restore the whale's original ETH balance if we topped it up,
                # so the fork's state is observably unchanged from outside this
                # function. Best-effort — never let cleanup mask the result.
                if original_balance_hex is not None:
                    try:
                        await self._rpc_call_raw("anvil_setBalance", [whale_address, original_balance_hex])
                    except Exception as e:
                        logger.debug(f"Failed to restore whale balance for {whale_address[:10]}...: {e}")
                # Stop impersonation regardless of transfer result
                await self._rpc_call_raw("anvil_stopImpersonatingAccount", [whale_address])

        except Exception as e:
            logger.debug(f"Whale funding failed for {token_symbol}: {e}")
            return False

    async def _fund_token_via_storage(
        self,
        wallet_address: str,
        token_address: str,
        amount_hex: str,
        token_symbol: str,
    ) -> bool:
        """Fund tokens by brute-force probing common storage slots.

        Last-resort fallback when known slots and anvil_deal both fail.

        Args:
            wallet_address: Address to fund
            token_address: Token contract address
            amount_hex: Amount in hex (already scaled to token decimals)
            token_symbol: Token symbol for logging

        Returns:
            True if successful
        """
        common_slots = [0, 1, 2, 3, 4, 5, 9, 51, 52]
        expected = int(amount_hex, 16)

        for slot in common_slots:
            snap_id: str | None = None
            try:
                # Snapshot before the probe so wrong-slot writes can be rolled
                # back. Without this, a write to a slot that turns out NOT to
                # be `_balances` can corrupt unrelated state on proxy tokens —
                # e.g. cbBTC's FiatTokenV2_2 has the `blacklisted` mapping at
                # slot 3, so writing `keccak256(wallet||3) = nonzero` flags the
                # wallet as blacklisted and every subsequent approve/transfer
                # reverts with "Blacklistable: account is blacklisted" even
                # though balanceOf(wallet) returns the expected value once we
                # eventually hit slot 9.
                snap_ok, snap_id = await self._rpc_call_raw("evm_snapshot", [])
                if not snap_ok or snap_id is None:
                    logger.warning(
                        f"evm_snapshot unsupported on this fork; aborting storage-slot probe for "
                        f"{token_symbol} to avoid corrupting proxy state"
                    )
                    return False

                storage_slot = self._calculate_mapping_slot(wallet_address, slot)
                success, _ = await self._rpc_call_raw(
                    "anvil_setStorageAt",
                    [token_address, storage_slot, self._pad_hex_to_32_bytes(amount_hex)],
                )

                if success:
                    # Mine a block to apply storage changes
                    await self._rpc_call_raw("evm_mine", [])

                    balance = await self._get_token_balance(token_address, wallet_address)

                    if balance == expected:
                        # Right slot — keep the write. The snapshot is left
                        # uncommitted; evm_revert is never called for this
                        # iteration so the write persists.
                        logger.info(f"Funded {wallet_address[:10]}... with {token_symbol} via brute-force slot {slot}")
                        return True

                # Wrong slot — revert the write before trying the next one.
                # If the revert RPC itself fails, the wrong-slot write persists
                # and any subsequent probe could compound the corruption that
                # snapshot/revert exists to prevent. Bail out hard.
                reverted, _ = await self._rpc_call_raw("evm_revert", [snap_id])
                snap_id = None
                if not reverted:
                    logger.warning(
                        f"evm_revert failed while probing slot {slot} for {token_symbol}; "
                        f"aborting probe to avoid leaving corrupt state from earlier writes"
                    )
                    return False

            except Exception as e:
                logger.debug(f"Storage slot {slot} failed for {token_symbol}: {e}")
                # Best-effort revert if we took a snapshot before the failure.
                # Same hard-abort rule as the wrong-slot path: a revert that
                # silently fails leaves the fork dirty.
                if snap_id is not None:
                    try:
                        reverted, _ = await self._rpc_call_raw("evm_revert", [snap_id])
                        if not reverted:
                            logger.warning(
                                f"evm_revert failed while cleaning up slot {slot} for {token_symbol}; aborting probe"
                            )
                            return False
                    except Exception:
                        return False
                continue

        logger.warning(
            f"Could not determine storage slot for {token_symbol} on {self.chain}. "
            f"Token may have non-standard storage layout."
        )
        return False

    def get_rpc_url(self) -> str:
        """Get the local RPC URL for the fork.

        Returns:
            RPC URL string (e.g., "http://127.0.0.1:8546")
        """
        return f"http://127.0.0.1:{self.anvil_port}"

    def _build_anvil_command(self) -> list[str]:
        """Build the Anvil command with all options.

        Returns:
            List of command arguments
        """
        cmd = [
            "anvil",
            "--fork-url",
            self.rpc_url,
            "--port",
            str(self.anvil_port),
            "--chain-id",
            str(self.chain_id),
        ]

        if self.fork_block_number is not None:
            cmd.extend(["--fork-block-number", str(self.fork_block_number)])

        if self.auto_impersonate:
            cmd.append("--auto-impersonate")

        if self.block_time is not None:
            cmd.extend(["--block-time", str(self.block_time)])

        # Upstream RPC settings - prevent Anvil from hanging on
        # expensive fork calls (e.g. simulating large failing swaps)
        cmd.extend(["--timeout", "120000"])  # 120s upstream RPC timeout
        cmd.extend(["--retries", "3"])  # Retry failed upstream calls

        # Set base fee to 0 so transactions are never rejected for gas price being
        # below the forked chain's real base fee (e.g. Polygon at 150-200+ gwei).
        # This replaces the old --no-gas-cap flag which only existed in Anvil 0.4.x
        # and was removed in Foundry 1.x. Gas is always fake/free on Anvil forks.
        cmd.extend(["--block-base-fee-per-gas", "0"])

        # Override the block gas limit for chains with non-standard gas accounting
        # (VIB-3666 / VIB-3746 / #2103). See ``_CHAIN_BLOCK_GAS_LIMITS`` for the
        # full history of why an explicit numeric override is preferred over
        # ``--disable-block-gas-limit`` (Anvil 1.7.x rejects combining the two,
        # and the disable-flag-alone path observed receipt-not-mined hangs in
        # the CI matrix).
        if self.chain in _CHAIN_BLOCK_GAS_LIMITS:
            cmd.extend(["--gas-limit", str(_CHAIN_BLOCK_GAS_LIMITS[self.chain])])

        # Cache upstream RPC responses to reduce Alchemy/RPC calls
        if self.cache_path:
            if "--cache-path" in _get_anvil_supported_flags():
                cmd.extend(["--cache-path", self.cache_path])
            else:
                logger.warning("Anvil does not support --cache-path; skipping RPC disk cache")

        # Silent mode for cleaner logs
        cmd.append("--silent")

        return cmd

    async def _wait_for_ready(self) -> bool:
        """Wait for Anvil to be ready to accept connections.

        Returns:
            True if Anvil is ready, False if timed out
        """
        start_time = time.time()

        while time.time() - start_time < self.startup_timeout_seconds:
            if self._is_port_open():
                # Verify RPC is responding. The readiness health probe is bounded
                # by health_timeout_seconds (per-probe), distinct from
                # startup_timeout_seconds (the total readiness budget capping this
                # loop) and rpc_timeout_seconds (post-ready operational calls).
                try:
                    block = await self._get_block_number(timeout_override=self.health_timeout_seconds)
                    if block is not None:
                        return True
                except Exception:
                    pass

            # Check if process died
            if self._process is not None and self._process.poll() is not None:
                stdout, stderr = self._process.communicate()
                logger.error(f"Anvil process exited unexpectedly. stdout: {stdout.decode()}, stderr: {stderr.decode()}")
                return False

            await asyncio.sleep(0.5)

        return False

    def _is_port_open(self) -> bool:
        """Check if the Anvil port is accepting connections.

        Returns:
            True if port is open
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                s.connect(("127.0.0.1", self.anvil_port))
                return True
        except (TimeoutError, ConnectionRefusedError, OSError):
            return False

    async def _rpc_call(
        self,
        method: str,
        params: list[Any],
        timeout_override: float | None = None,
    ) -> Any:
        """Make a JSON-RPC call to the fork.

        Args:
            method: RPC method name
            params: Method parameters
            timeout_override: Per-call timeout in seconds. When ``None`` (every
                post-ready operational call) the steady-state ``rpc_timeout_seconds``
                applies; the readiness health probe passes ``health_timeout_seconds``.

        Returns:
            Result from the RPC call, or None on error
        """
        success, result = await self._rpc_call_raw(method, params, timeout_override=timeout_override)
        if not success:
            return None
        return result

    async def _rpc_call_raw(
        self,
        method: str,
        params: list[Any],
        timeout_override: float | None = None,
    ) -> tuple[bool, Any]:
        """Make a JSON-RPC call and return success status separately from result.

        Unlike _rpc_call(), this method distinguishes between "success with null
        result" and "error". Anvil methods like anvil_setBalance, anvil_setStorageAt,
        and evm_mine return null on success, so callers must not treat None as failure.

        Args:
            method: RPC method name
            params: Method parameters
            timeout_override: Per-call timeout in seconds. ``None`` (the default,
                used by all post-ready operational calls) selects
                ``rpc_timeout_seconds``; the readiness health probe in
                ``_wait_for_ready`` passes ``health_timeout_seconds`` so the
                readiness-probe and steady-state RPC budgets are tuned separately.

        Returns:
            Tuple of (success, result). success is True when the JSON-RPC response
            has no "error" field, even if result is None.
        """
        import aiohttp

        timeout_seconds = self.rpc_timeout_seconds if timeout_override is None else timeout_override
        url = self.get_rpc_url()
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=payload, timeout=aiohttp.ClientTimeout(total=timeout_seconds)
                ) as response:
                    result_data: dict[str, Any] = await response.json()
                    if "error" in result_data:
                        logger.debug(f"RPC error for {method}: {result_data['error']}")
                        return (False, None)
                    return (True, result_data.get("result"))
        except Exception as e:
            logger.debug(f"RPC call failed for {method}: {e}")
            return (False, None)

    async def _get_block_number(self, timeout_override: float | None = None) -> int | None:
        """Get the current block number from the fork.

        Args:
            timeout_override: Per-call timeout in seconds. ``None`` uses
                ``rpc_timeout_seconds``; the readiness health probe passes
                ``health_timeout_seconds``.

        Returns:
            Block number or None on error
        """
        result = await self._rpc_call("eth_blockNumber", [], timeout_override=timeout_override)
        if result is not None:
            return int(result, 16)
        return None

    async def _get_token_balance(
        self,
        token_address: str,
        wallet_address: str,
    ) -> int:
        """Get the ERC-20 token balance for an address.

        Args:
            token_address: Token contract address
            wallet_address: Address to check balance for

        Returns:
            Token balance in smallest units
        """
        # ERC-20 balanceOf(address) selector: 0x70a08231
        # padded address parameter
        padded_address = wallet_address.lower().replace("0x", "").zfill(64)
        data = f"0x70a08231{padded_address}"

        result = await self._rpc_call(
            "eth_call",
            [{"to": token_address, "data": data}, "latest"],
        )

        if result and result != "0x":
            return int(result, 16)
        return 0

    def _calculate_mapping_slot(self, key: str, slot: int) -> str:
        """Calculate the storage slot for a mapping entry.

        For mappings, the storage slot is keccak256(key . slot) where key and slot
        are both 32-byte values and . is concatenation.

        Uses Ethereum's Keccak-256 (NOT NIST SHA3-256 -- they differ).

        Args:
            key: The mapping key (e.g., wallet address)
            slot: The base slot number for the mapping

        Returns:
            Hex string of the calculated storage slot
        """
        from eth_hash.auto import keccak as keccak256

        # Pad key to 32 bytes
        key_padded = key.lower().replace("0x", "").zfill(64)

        # Pad slot to 32 bytes
        slot_padded = hex(slot)[2:].zfill(64)

        # Concatenate and hash with Keccak-256
        concat = bytes.fromhex(key_padded + slot_padded)
        hash_result = keccak256(concat).hex()

        return "0x" + hash_result

    def _pad_hex_to_32_bytes(self, hex_value: str) -> str:
        """Pad a hex value to 32 bytes (64 hex characters).

        Args:
            hex_value: Hex string (with or without 0x prefix)

        Returns:
            Hex string padded to 32 bytes with 0x prefix
        """
        value = hex_value.replace("0x", "")
        padded = value.zfill(64)
        return "0x" + padded

    def to_dict(self) -> dict[str, Any]:
        """Serialize manager state to dictionary.

        Returns:
            Dictionary with manager configuration and state
        """
        return {
            "rpc_url": ForkManagerConfig._mask_url(self.rpc_url),
            "chain": self.chain,
            "chain_id": self.chain_id,
            "anvil_port": self.anvil_port,
            "is_running": self.is_running,
            "current_block": self._current_block,
            "fork_rpc_url": self.get_rpc_url() if self.is_running else None,
        }


__all__ = [
    "RollingForkManager",
    "ForkManagerConfig",
    "CHAIN_IDS",
    "TOKEN_ADDRESSES",
    "TOKEN_DECIMALS",
    "KNOWN_BALANCE_SLOTS",
]
