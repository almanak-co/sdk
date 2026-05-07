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
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.config.framework import framework_config_from_env

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
    "xlayer": 196,
    "zerog": 16661,
}

# Per-chain Anvil block gas limit overrides. When set, --gas-limit is passed to
# the Anvil fork so transactions are never rejected for exceeding the block gas limit.
# Mantle uses a non-standard gas accounting system (L1 calldata overhead included in
# tx.gasLimit), so a generous block gas limit prevents false "intrinsic gas too high" errors.
#
# NOTE on flag name: Foundry/Anvil exposes the block-gas-limit override as
# ``--gas-limit <GAS_LIMIT>`` (per ``anvil --help``). VIB-3666 originally wired
# ``--block-gas-limit`` here, which Anvil does not advertise — the
# ``_get_anvil_supported_flags()`` guard then silently dropped the override and
# Mantle continued to reject APPROVE/SUPPLY transactions with ``intrinsic gas
# too high -- tx.gas_limit > env.block.gas_limit`` (VIB-3746). The correct flag
# is ``--gas-limit``.
_CHAIN_BLOCK_GAS_LIMITS: dict[str, int] = {
    "mantle": 1_000_000_000,  # 1B — Mantle L2 non-standard gas accounting (VIB-3666/VIB-3746)
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
        "wstETH": "0x5979D7b546E38E414F7E9822514be443A4800529",
    },
    "ethereum": {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "wstETH": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        "stETH": "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
        "rETH": "0xae78736Cd615f374D3085123A210448E74Fc6393",
        "cbETH": "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704",
        "swETH": "0xf951E335afb289353dc249e82926178EaC7DEd78",
        "ankrETH": "0xE95A203B1a91a908F9B9CE46459d101078c2c3cb",
        "pufETH": "0xD9A442856C234a39a81a089C06451EBAa4306a72",
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
        "BTC.b": "0x152b9d0FdC40C096757F570A51E494bd4b943E50",
        "sAVAX": "0x2b2C81e08f1Af8835a78Bb2A90AE924ACE0eA4bE",
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
    "sonic": {
        "wS": "0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38",
        "WETH": "0x50c42dEAcD8Fc9773493ED674b675bE577f2634b",
        "USDC": "0x29219dd400f2Bf60E5a23d13Be72B486D4038894",
        "USDT": "0x6047828dc181963ba44974801FF68e538dA5eaF9",
    },
    "monad": {
        "WMON": "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A",
        "WETH": "0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242",
        "USDC": "0x754704Bc059F8C67012fEd69BC8A327a5aafb603",
        "USDT0": "0xe7cd86e13AC4309349F30B3435a9d337750fC82D",
        "WBTC": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",
    },
    "xlayer": {
        "WOKB": "0xe538905cf8410324e03A5A23C1c177a474D59b2b",
        "WETH": "0x5A77f1443D16ee5761d310e38b62f77f726bC71c",
        "xETH": "0xE7B000003A45145decf8a28FC755aD5eC5EA025A",
        "USDC": "0x74b7F16337b8972027F6196A17a631aC6dE26d22",
        "USDT": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",  # USD₮0 (Aave V3.6 reserve)
        "USDT0": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",  # USD₮0 (Aave V3.6 reserve)
        "WBTC": "0xEA034fb02eB1808C2cc3adbC15f447B93CbE08e1",
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
KNOWN_BALANCE_SLOTS: dict[str, dict[str, int]] = {
    "arbitrum": {
        "USDC": 9,
        "WETH": 51,
        "USDC.e": 51,
        "USDT": 51,
        "DAI": 2,
        "WBTC": 51,
        "ARB": 51,
        "GMX": 0,
        "wstETH": 1,
    },
    "ethereum": {"USDC": 9, "WETH": 3, "USDT": 2, "DAI": 2, "WBTC": 0, "wstETH": 0},
    "base": {"USDC": 9, "WETH": 3, "USDbC": 9, "DAI": 0, "wstETH": 1},
    "avalanche": {"USDC": 9, "WAVAX": 3, "USDT": 2, "USDC.e": 0, "WETH.e": 0, "BTC.b": 0, "sAVAX": 0},
    "optimism": {"USDC": 9, "WETH": 3, "USDT": 0, "USDC.e": 0, "OP": 0},
    "polygon": {"USDC": 9, "WETH": 3, "USDT": 2, "WMATIC": 3, "USDC.e": 0},
    "bsc": {"USDC": 1, "WBNB": 3, "USDT": 1, "BUSD": 0},
    "linea": {"USDC": 9, "WETH": 3, "USDT": 51},  # verified on-chain 2026-04-12 (VIB-2724)
    "sonic": {"USDC": 9, "WETH": 0},  # USDC: confirmed iter-100. WETH: bridged, try slot 0
    "xlayer": {"USDT0": 51},  # confirmed: USD₮0 (0x779Ded...) uses OZ upgradeable slot 51
}

# Tokens where storage slot manipulation produces a valid balanceOf() but
# transferFrom() reverts (proxy implementation mismatch).  For these tokens,
# whale impersonation is used instead of slot patching.
# Format: { chain: { token_symbol_upper: whale_address } }
WHALE_FUNDED_TOKENS: dict[str, dict[str, str]] = {
    "ethereum": {
        # USDC FiatTokenProxy: slot 9 sets balanceOf but transferFrom reverts
        # because implementation contract's internal state is inconsistent.
        # Circle: Treasury is a reliable large holder.
        "USDC": "0x37305B1cD40574E4C5Ce33f8e8306Be057fD7341",
    },
}

# Wrapped native tokens that can be funded via deposit() instead of storage
# slot manipulation.  deposit() is more reliable for these contracts because
# it uses the contract's own logic (no proxy/slot mismatch risk).
WRAPPED_NATIVE_TOKENS: dict[str, str] = {
    "ethereum": "WETH",
    "arbitrum": "WETH",
    "base": "WETH",
    "optimism": "WETH",
    "polygon": "WMATIC",
    "linea": "WETH",
    "avalanche": "WAVAX",
    "bsc": "WBNB",
    "sonic": "WS",
    "mantle": "WMNT",
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
    cache_path: str | None = field(default_factory=_default_fork_cache_path)

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

        # Load paper-local token overrides (VIB-2378)
        from almanak.framework.backtesting.paper.token_overrides import load_token_overrides

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
                # transferFrom (e.g., Ethereum USDC FiatTokenProxy).
                if not funded:
                    whale_tokens = WHALE_FUNDED_TOKENS.get(self.chain, {})
                    whale_address = whale_tokens.get(lookup_symbol)
                    if whale_address:
                        funded = await self._fund_token_via_whale(
                            address, token_address, amount_hex, whale_address, display_name
                        )
                        # Whale-funded tokens are listed precisely because slot
                        # patching produces broken internal state; skip storage.
                        skip_storage_fallback = True

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
        try:
            # Impersonate the whale account
            success, _ = await self._rpc_call_raw("anvil_impersonateAccount", [whale_address])
            if not success:
                logger.debug(f"Failed to impersonate whale {whale_address[:10]}... for {token_symbol}")
                return False

            try:
                # ERC-20 transfer(address,uint256) selector = 0xa9059cbb
                # Encode: selector + address padded to 32 bytes + amount padded to 32 bytes
                addr_padded = wallet_address.lower().replace("0x", "").zfill(64)
                amt_padded = amount_hex.replace("0x", "").zfill(64)
                calldata = "0xa9059cbb" + addr_padded + amt_padded

                success, tx_hash = await self._rpc_call_raw(
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

        # Set base fee to 0 so transactions are never rejected for gas price being
        # below the forked chain's real base fee (e.g. Polygon at 150-200+ gwei).
        # This replaces the old --no-gas-cap flag which only existed in Anvil 0.4.x
        # and was removed in Foundry 1.x. Gas is always fake/free on Anvil forks.
        cmd.extend(["--block-base-fee-per-gas", "0"])

        # Override block gas limit for chains with non-standard gas accounting
        # (VIB-3666 / VIB-3746). Without this, some chains (e.g. Mantle) cause
        # "intrinsic gas too high" rejections because the forked block.gas_limit
        # is too low for the SDK's computed tx.gas_limit.
        #
        # Foundry exposes this as ``--gas-limit`` (see ``anvil --help``):
        # there is no ``--block-gas-limit`` flag — the previous wiring under
        # VIB-3666 was silently dropped by the supported-flags guard and Mantle
        # APPROVEs continued to revert (VIB-3746).
        #
        # Pass unconditionally for chains in _CHAIN_BLOCK_GAS_LIMITS. The flag
        # has been stable since Foundry 1.0 and earlier; the previous
        # _get_anvil_supported_flags() gate masked CI environments where the
        # probe transiently returned an empty set (issue #2103 — Foundry 1.7.0
        # in CI silently dropped the override and Mantle Agni LP txs reverted
        # with "intrinsic gas too high"). If a future Anvil version drops this
        # flag, surface that as a startup failure rather than silent drop.
        #
        # Belt-and-suspenders: also pass ``--disable-block-gas-limit`` which
        # disables Anvil's ``tx.gas_limit <= block.gas_limit`` constraint
        # outright. Mantle's L1-calldata-included gas accounting can produce
        # tx-level gas estimates that exceed even a 1B block-gas-limit
        # override (the in-CI symptom of #2103 even after dropping the
        # supported-flags gate). With both flags present, the check is
        # disabled regardless of how Anvil interprets ``--gas-limit``.
        if self.chain in _CHAIN_BLOCK_GAS_LIMITS:
            cmd.extend(["--gas-limit", str(_CHAIN_BLOCK_GAS_LIMITS[self.chain])])
            cmd.append("--disable-block-gas-limit")

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
