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
import os
import socket
import subprocess
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================


# Chain IDs for supported chains
CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "arbitrum": 42161,
    "optimism": 10,
    "polygon": 137,
    "base": 8453,
    "avalanche": 43114,
    "bsc": 56,
    "linea": 59144,
    "plasma": 9745,
    "blast": 81457,
    "mantle": 5000,
    "berachain": 80094,
    "sonic": 146,
    "monad": 143,
}


# Common ERC-20 token addresses per chain
TOKEN_ADDRESSES: dict[str, dict[str, str]] = {
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDC.e": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        "ARB": "0x912CE59144191C1204E64559FE8253a0e49E6548",
        "GMX": "0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a",
        "WSTETH": "0x5979D7b546E38E414F7E9822514be443A4800529",
    },
    "ethereum": {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "wstETH": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
    },
    "optimism": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
        "USDC.e": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
        "USDT": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
        "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "OP": "0x4200000000000000000000000000000000000042",
    },
    "base": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
        "DAI": "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
        "wstETH": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
    },
    "polygon": {
        "WMATIC": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        "WETH": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        "USDC.e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        "DAI": "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",
    },
    "avalanche": {
        "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "WETH.e": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "USDC.e": "0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664",
        "USDT": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
    },
    "bsc": {
        "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "BUSD": "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56",
        "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "USDT": "0x55d398326f99059fF775485246999027B3197955",
    },
    "linea": {
        "USDC": "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
        "WETH": "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
        "USDT": "0xA219439258ca9da29E9Cc4cE5596924745e12B93",
    },
    "plasma": {
        "WXPL": "0x6100E367285b01F48D07953803A2d8dCA5D19873",
        "USDT0": "0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb",
        "FUSDT0": "0x1DD4b13fcAE900C60a350589BE8052959D2Ed27B",
        "PENDLE": "0x17Bac5F906c9A0282aC06a59958D85796c831f24",
    },
    "berachain": {
        "WBERA": "0x6969696969696969696969696969696969696969",
        "HONEY": "0xFCBD14DC51f0A4d49d5E53C2E0950e0bC26d0Dce",
        "USDC.e": "0x549943e04f40284185054145c6E4e9568C1D3241",
        "WETH": "0x2F6F07CDcf3588944Bf4C42aC74ff24bF56e7590",
        "WBTC": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",
        "USDT0": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",
    },
}


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
    "WMATIC": 18,
    "WAVAX": 18,
    "WBNB": 18,
    "BUSD": 18,
    "ARB": 18,
    "GMX": 18,
    "OP": 18,
    "wstETH": 18,
    "WXPL": 18,
    "USDT0": 6,
    "FUSDT0": 6,
    "PENDLE": 18,
    "WBERA": 18,
    "HONEY": 18,
}


# Known ERC-20 balanceOf storage slots per chain.
# Sourced from verified intent tests (tests/intents/conftest.py CHAIN_CONFIGS).
# Using known slots avoids slow brute-force probing and is 100% reliable.
KNOWN_BALANCE_SLOTS: dict[str, dict[str, int]] = {
    "arbitrum": {"USDC": 9, "WETH": 51, "USDC.e": 51, "USDT": 51, "DAI": 2, "WBTC": 51, "ARB": 51, "GMX": 0},
    "ethereum": {"USDC": 9, "WETH": 3, "USDT": 2, "DAI": 2, "WBTC": 0, "wstETH": 0},
    "base": {"USDC": 9, "WETH": 3, "USDbC": 9, "DAI": 0, "wstETH": 1},
    "avalanche": {"USDC": 9, "WAVAX": 3, "USDT": 2, "USDC.e": 0},
    "optimism": {"USDC": 9, "WETH": 3, "USDT": 2, "USDC.e": 0, "OP": 0},
    "polygon": {"USDC": 9, "WETH": 3, "USDT": 2, "WMATIC": 3, "USDC.e": 0},
    "bsc": {"USDC": 1, "WBNB": 3, "USDT": 1, "BUSD": 0},
    "linea": {"USDC": 0, "WETH": 0, "USDT": 0},
}


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
    """

    rpc_url: str
    chain: str
    anvil_port: int = 8546
    startup_timeout_seconds: float = 30.0
    auto_impersonate: bool = True
    block_time: int | None = None
    fork_block_number: int | None = None

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
        startup_timeout_seconds: Timeout for startup (default 30)
        auto_impersonate: Enable auto-impersonation (default True)

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

    # Timeout defaults (env-overridable)
    rpc_timeout_seconds: float = field(default_factory=lambda: float(os.environ.get("ALMANAK_FORK_RPC_TIMEOUT", "8.0")))
    health_timeout_seconds: float = field(
        default_factory=lambda: float(os.environ.get("ALMANAK_FORK_HEALTH_TIMEOUT", "5.0"))
    )

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
            async with aiohttp.ClientSession() as session:
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

            # Start Anvil process
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            # Wait for Anvil to be ready
            ready = await self._wait_for_ready()
            if not ready:
                logger.error("Anvil fork startup timed out")
                await self.stop()
                return False

            self._is_running = True
            self._start_time = time.time()

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
                    await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(None, self._process.wait),
                        timeout=5.0,
                    )
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

        # Wait for port to be freed
        await self._wait_for_port_free(timeout=5.0)
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

        This stops the current fork and starts a new one forking from
        the latest block on mainnet. Useful for keeping paper trading
        in sync with real-time market conditions.

        Returns:
            True if reset successful, False otherwise
        """
        logger.info("Resetting fork to latest block...")

        # Store current fork_block_number setting
        original_fork_block = self.fork_block_number

        try:
            # Stop current fork
            await self.stop()

            # Start new fork at latest block (clear any specific block setting)
            self.fork_block_number = None

            # Start fresh fork
            success = await self.start()

            if success:
                logger.info(f"Fork reset to latest block: {self._current_block}")
            else:
                logger.error("Failed to reset fork to latest block")
                # Restore original setting
                self.fork_block_number = original_fork_block

            return success

        except Exception as e:
            logger.exception(f"Error resetting fork: {e}")
            self.fork_block_number = original_fork_block
            return False

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

    async def fund_tokens(
        self,
        address: str,
        tokens: dict[str, Decimal],
    ) -> bool:
        """Fund a wallet with ERC-20 tokens.

        Funding priority:
        1. Known storage slots (fast, reliable -- verified in intent tests)
        2. anvil_deal RPC (works on newer Anvil versions)
        3. Brute-force storage slot probing (slow fallback)

        Args:
            address: Wallet address to fund
            tokens: Dict mapping token symbol to amount (e.g., {"USDC": Decimal("10000")})

        Returns:
            True if all tokens funded successfully, False otherwise
        """
        if not self.is_running:
            logger.error("Cannot fund tokens: Anvil fork not running")
            return False

        from almanak.framework.data.tokens import get_token_resolver
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        resolver = get_token_resolver()
        chain_tokens = TOKEN_ADDRESSES.get(self.chain, {})
        known_slots = KNOWN_BALANCE_SLOTS.get(self.chain, {})
        success = True

        for token_symbol, amount in tokens.items():
            # Resolve token address and decimals via TokenResolver (fail-fast, no default 18)
            token_address: str | None = None
            decimals: int | None = None
            try:
                resolved = resolver.resolve(token_symbol, self.chain)
                token_address = resolved.address
                decimals = resolved.decimals
            except TokenNotFoundError:
                # Fallback to local TOKEN_ADDRESSES for Anvil-specific tokens not in resolver
                token_address = chain_tokens.get(token_symbol) or chain_tokens.get(token_symbol.upper())
                # Use explicit None checks to avoid falsy-zero bug (0 decimals is valid)
                decimals = TOKEN_DECIMALS.get(token_symbol)
                if decimals is None:
                    decimals = TOKEN_DECIMALS.get(token_symbol.upper())

            if not token_address:
                logger.warning(f"Unknown token {token_symbol} for chain {self.chain}, skipping")
                success = False
                continue
            if decimals is None:
                logger.error(
                    f"Unknown decimals for {token_symbol} on {self.chain}, skipping (refusing to default to 18)"
                )
                success = False
                continue

            # Convert to token units (hex string)
            token_units = int(amount * Decimal(10**decimals))
            amount_hex = hex(token_units)

            try:
                funded = False

                # Priority 1: Known storage slot (fast and reliable)
                known_slot = (
                    known_slots.get(token_symbol)
                    if known_slots.get(token_symbol) is not None
                    else known_slots.get(token_symbol.upper())
                )
                if known_slot is not None:
                    funded = await self._set_balance_at_slot(
                        address, token_address, amount_hex, known_slot, token_symbol
                    )

                # Priority 2: anvil_deal RPC (returns null on success)
                if not funded:
                    deal_success, _ = await self._rpc_call_raw(
                        "anvil_deal",
                        [token_address, address, amount_hex],
                    )
                    if deal_success:
                        logger.info(f"Funded {address[:10]}... with {amount} {token_symbol} via anvil_deal")
                        funded = True

                # Priority 3: Brute-force storage slot probing
                if not funded:
                    funded = await self._fund_token_via_storage(address, token_address, amount_hex, token_symbol)

                if not funded:
                    logger.error(f"Failed to fund {token_symbol} for {address[:10]}...")
                    success = False

            except Exception as e:
                logger.exception(f"Error funding {token_symbol}: {e}")
                success = False

        return success

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

        for slot in common_slots:
            try:
                storage_slot = self._calculate_mapping_slot(wallet_address, slot)

                success, _ = await self._rpc_call_raw(
                    "anvil_setStorageAt",
                    [token_address, storage_slot, self._pad_hex_to_32_bytes(amount_hex)],
                )

                if success:
                    # Mine a block to apply storage changes
                    await self._rpc_call_raw("evm_mine", [])

                    balance = await self._get_token_balance(token_address, wallet_address)
                    expected = int(amount_hex, 16)

                    if balance == expected:
                        logger.info(f"Funded {wallet_address[:10]}... with {token_symbol} via brute-force slot {slot}")
                        return True

            except Exception as e:
                logger.debug(f"Storage slot {slot} failed for {token_symbol}: {e}")
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
                # Verify RPC is responding
                try:
                    block = await self._get_block_number()
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
    ) -> Any:
        """Make a JSON-RPC call to the fork.

        Args:
            method: RPC method name
            params: Method parameters

        Returns:
            Result from the RPC call, or None on error
        """
        success, result = await self._rpc_call_raw(method, params)
        if not success:
            return None
        return result

    async def _rpc_call_raw(
        self,
        method: str,
        params: list[Any],
    ) -> tuple[bool, Any]:
        """Make a JSON-RPC call and return success status separately from result.

        Unlike _rpc_call(), this method distinguishes between "success with null
        result" and "error". Anvil methods like anvil_setBalance, anvil_setStorageAt,
        and evm_mine return null on success, so callers must not treat None as failure.

        Args:
            method: RPC method name
            params: Method parameters

        Returns:
            Tuple of (success, result). success is True when the JSON-RPC response
            has no "error" field, even if result is None.
        """
        import aiohttp

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
                    url, json=payload, timeout=aiohttp.ClientTimeout(total=self.rpc_timeout_seconds)
                ) as response:
                    result_data: dict[str, Any] = await response.json()
                    if "error" in result_data:
                        logger.debug(f"RPC error for {method}: {result_data['error']}")
                        return (False, None)
                    return (True, result_data.get("result"))
        except Exception as e:
            logger.debug(f"RPC call failed for {method}: {e}")
            return (False, None)

    async def _get_block_number(self) -> int | None:
        """Get the current block number from the fork.

        Returns:
            Block number or None on error
        """
        result = await self._rpc_call("eth_blockNumber", [])
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

        if result:
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
