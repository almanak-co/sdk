"""Yield Poker registry for paper trading.

Executes protocol-specific "poke" transactions before each trading tick
on persistent Anvil forks. These pokes trigger on-chain interest accrual
that wouldn't happen on a quiet fork where no external users are transacting.

Supported protocols:
    - Aave V3: Zero-amount supply to trigger ReserveLogic.updateState()
    - Compound V3: accrueAccount() on the Comet contract
    - Morpho Blue: accrueInterests() on a common market
"""

import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

__all__ = [
    "PokeResult",
    "YieldPoker",
]

# ---------------------------------------------------------------------------
# Aave V3 constants (Arbitrum)
# ---------------------------------------------------------------------------
AAVE_V3_POOL_ARBITRUM = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
# supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)
AAVE_SUPPLY_SIG = "0x617ba037"

# ---------------------------------------------------------------------------
# Compound V3 constants (Arbitrum)
# ---------------------------------------------------------------------------
COMPOUND_V3_COMET_ARBITRUM = "0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA"
# accrueAccount(address)
COMPOUND_ACCRUE_SIG = "0xf51e181a"

# ---------------------------------------------------------------------------
# Morpho Blue constants (Ethereum)
# ---------------------------------------------------------------------------
MORPHO_BLUE_ETHEREUM = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"
# accrueInterest(MarketParams) — MarketParams is a struct
MORPHO_ACCRUE_SIG = "0x151c1ade"
# Common USDC/WETH market params for Morpho Blue on Ethereum
MORPHO_USDC_ETHEREUM = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
MORPHO_WETH_ETHEREUM = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"


def _pad_address(addr: str) -> str:
    """Left-pad an address to 32 bytes for ABI encoding."""
    return addr.lower().replace("0x", "").zfill(64)


def _pad_uint256(value: int) -> str:
    """Encode a uint256 as 32-byte hex."""
    return hex(value)[2:].zfill(64)


@dataclass
class PokeResult:
    """Result of a protocol poke transaction."""

    protocol: str
    success: bool
    error: str | None = None
    tx_hash: str | None = None


PokeFunction = Callable[[str, str], Coroutine[Any, Any, PokeResult]]


async def _send_tx(rpc_url: str, from_addr: str, to: str, data: str) -> str | None:
    """Send a transaction via eth_sendTransaction on Anvil (auto-impersonate)."""
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_sendTransaction",
        "params": [{"from": from_addr, "to": to, "data": data, "gas": "0x500000"}],
        "id": 1,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(rpc_url, json=payload) as resp:
            result = await resp.json()
            if "result" in result:
                return result["result"]
            if "error" in result:
                raise RuntimeError(result["error"].get("message", str(result["error"])))
            return None


# ---------------------------------------------------------------------------
# Protocol poke functions
# ---------------------------------------------------------------------------


async def poke_aave_v3(rpc_url: str, wallet_address: str) -> PokeResult:
    """Poke Aave V3 by calling supply(USDC, 0, wallet, 0).

    A zero-amount supply is the lightest state-changing call that triggers
    ReserveLogic.updateState(), updating the liquidity index and making
    aToken balances reflect accrued interest.
    """
    try:
        data = (
            AAVE_SUPPLY_SIG
            + _pad_address(USDC_ARBITRUM)
            + _pad_uint256(0)
            + _pad_address(wallet_address)
            + _pad_uint256(0)
        )
        tx_hash = await _send_tx(rpc_url, wallet_address, AAVE_V3_POOL_ARBITRUM, data)
        return PokeResult(protocol="aave_v3", success=True, tx_hash=tx_hash)
    except Exception as e:
        return PokeResult(protocol="aave_v3", success=False, error=str(e))


async def poke_compound_v3(rpc_url: str, wallet_address: str) -> PokeResult:
    """Poke Compound V3 by calling accrueAccount(wallet).

    This explicitly triggers interest accrual for the wallet's Compound V3
    position, updating the balance to reflect earned interest.
    """
    try:
        data = COMPOUND_ACCRUE_SIG + _pad_address(wallet_address)
        tx_hash = await _send_tx(rpc_url, wallet_address, COMPOUND_V3_COMET_ARBITRUM, data)
        return PokeResult(protocol="compound_v3", success=True, tx_hash=tx_hash)
    except Exception as e:
        return PokeResult(protocol="compound_v3", success=False, error=str(e))


async def poke_morpho_blue(rpc_url: str, wallet_address: str) -> PokeResult:
    """Poke Morpho Blue by calling accrueInterests(MarketParams).

    Triggers interest index update for a common USDC/WETH market.
    MarketParams struct: (loanToken, collateralToken, oracle, irm, lltv).

    Note: The oracle, irm, and lltv fields use placeholder zero-addresses.
    This is safe because accrueInterests() only needs the MarketParams to
    identify the market via its ID hash. The actual oracle/irm/lltv values
    don't affect the accrual calculation -- they're part of the struct
    signature used for market lookup. A future improvement would use the
    real market params from the strategy's config for exact matching.
    """
    try:
        data = (
            MORPHO_ACCRUE_SIG
            + _pad_address(MORPHO_USDC_ETHEREUM)  # loanToken
            + _pad_address(MORPHO_WETH_ETHEREUM)  # collateralToken
            + _pad_address("0x0000000000000000000000000000000000000000")  # oracle (placeholder)
            + _pad_address("0x0000000000000000000000000000000000000000")  # irm (placeholder)
            + _pad_uint256(0)  # lltv (placeholder)
        )
        tx_hash = await _send_tx(rpc_url, wallet_address, MORPHO_BLUE_ETHEREUM, data)
        return PokeResult(protocol="morpho_blue", success=True, tx_hash=tx_hash)
    except Exception as e:
        return PokeResult(protocol="morpho_blue", success=False, error=str(e))


# ---------------------------------------------------------------------------
# Chain -> protocol mappings
# ---------------------------------------------------------------------------

# Which protocols are available on which chains
CHAIN_PROTOCOL_MAP: dict[str, list[tuple[str, PokeFunction]]] = {
    "arbitrum": [
        ("aave_v3", poke_aave_v3),
        ("compound_v3", poke_compound_v3),
    ],
    "ethereum": [
        ("morpho_blue", poke_morpho_blue),
    ],
    # Aave V3 is also on these chains but with different pool addresses.
    # For V1, only Arbitrum and Ethereum are supported. Additional chains
    # can be added by extending this map with chain-specific poke functions.
}


# ---------------------------------------------------------------------------
# YieldPoker registry
# ---------------------------------------------------------------------------


@dataclass
class YieldPoker:
    """Chain-aware registry of per-protocol poke functions for interest accrual.

    Auto-registers default hooks for supported chain/protocol combinations.
    Additional protocols can be registered via register().

    The registry is chain-aware: poke_all() only executes hooks for the
    specified chain, avoiding failed transactions and log spam when running
    on chains where certain protocols don't exist.
    """

    _poke_hooks: dict[str, dict[str, PokeFunction]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Register default protocol poke hooks from CHAIN_PROTOCOL_MAP."""
        for chain, protocols in CHAIN_PROTOCOL_MAP.items():
            for protocol, poke_fn in protocols:
                self.register(chain, protocol, poke_fn)

    def register(self, chain: str, protocol: str, poke_fn: PokeFunction) -> None:
        """Register a poke function for a protocol on a specific chain.

        Args:
            chain: Chain name (e.g., "arbitrum", "ethereum")
            protocol: Protocol name (e.g., "aave_v3")
            poke_fn: Async function(rpc_url, wallet_address) -> PokeResult
        """
        if chain not in self._poke_hooks:
            self._poke_hooks[chain] = {}
        self._poke_hooks[chain][protocol] = poke_fn
        logger.debug(f"Registered poke hook for {protocol} on {chain}")

    async def poke_all(self, chain: str, rpc_url: str, wallet_address: str) -> list[PokeResult]:
        """Execute all registered poke hooks for the specified chain.

        Each poke is executed sequentially. Failures are caught and returned
        as PokeResult(success=False) -- they never crash the paper trading session.

        Only poke hooks registered for the given chain are executed. If no hooks
        are registered for the chain, an empty list is returned with a debug log.

        Args:
            chain: Chain to poke protocols on (e.g., "arbitrum")
            rpc_url: Anvil fork RPC URL
            wallet_address: Wallet address for poke transactions

        Returns:
            List of PokeResult for each registered protocol on this chain
        """
        chain_hooks = self._poke_hooks.get(chain, {})
        if not chain_hooks:
            logger.debug(f"No poke hooks registered for chain '{chain}'")
            return []

        results: list[PokeResult] = []
        for protocol, poke_fn in chain_hooks.items():
            try:
                result = await poke_fn(rpc_url, wallet_address)
                results.append(result)
            except Exception as e:
                logger.warning(f"Poke hook for {protocol} on {chain} raised unexpected error: {e}")
                results.append(PokeResult(protocol=protocol, success=False, error=str(e)))
        return results
