"""Fluid SmartLending SDK (DEX LP, Phase 4 / VIB-5032).

SmartLending wrappers are fungible ERC-20-share wrappers over Fluid DEX pools.
Direct pool LP is whitelist-gated (``DexT1__UserSupplyInNotOn`` 51013); the
wrapper IS the whitelisted supplier, so an EOA/Safe LPs through it. Verified
end-to-end on Arbitrum forks — see
``docs/internal/qa/fluid-smartlending-validation-2026-06-12.md``.

All reads route through the gateway (``GatewayWeb3Provider``). NO per-token
storage-slot overrides (the VIB-2822 anti-pattern Phase 1 removed) and NO
direct egress.

Surface (selectors verified on-chain):

* ``deposit(uint256 token0Amt, uint256 token1Amt, uint256 minShares, address to)``
  ``0xfad3cc4b`` — flexible any-ratio deposit; native leg via ``msg.value``.
* ``withdraw(uint256 token0Amt, uint256 token1Amt, uint256 maxShares, address to)``
  ``0xd331bef7`` — exact-tokens-out; ``maxShares`` cap.
* ``withdrawPerfect(uint256 shares, uint256 minToken0, uint256 minToken1, address to)``
  ``0x35f0df98`` — proportional burn (balanced positions only).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from web3 import HTTPProvider, Web3
from web3.types import HexStr

from almanak.connectors.fluid.sdk import (
    FluidSDKError,
    decode_fluid_revert,
    fluid_error_id,
)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)

# ``UserSupplyInNotOn`` — the per-pool "deposits disabled" gate (Phase-0 §V4).
_DEX_USER_SUPPLY_IN_NOT_ON = 51013

# DEX deposit estimate revert-carrier selector (carries shares in word 0).
_DEX_PERFECT_OUTPUT_SELECTOR = "0xe8d35d06"

# Default gas budgets (mirrors fluid/sdk.py DEFAULT_GAS_ESTIMATES shape).
_GAS_DEPOSIT = 1_200_000
_GAS_WITHDRAW = 1_200_000

# Minimal ABIs for the wrapper + resolver reads we issue.
_WRAPPER_ABI: list[dict[str, Any]] = [
    {"name": "TOKEN0", "inputs": [], "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
    {"name": "TOKEN1", "inputs": [], "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
    {"name": "DEX", "inputs": [], "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
    {
        "name": "totalSupply",
        "inputs": [],
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "balanceOf",
        "inputs": [{"type": "address"}],
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "deposit",
        "inputs": [{"type": "uint256"}, {"type": "uint256"}, {"type": "uint256"}, {"type": "address"}],
        "outputs": [{"type": "uint256"}],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "name": "withdraw",
        "inputs": [{"type": "uint256"}, {"type": "uint256"}, {"type": "uint256"}, {"type": "address"}],
        "outputs": [{"type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "name": "withdrawPerfect",
        "inputs": [{"type": "uint256"}, {"type": "uint256"}, {"type": "uint256"}, {"type": "address"}],
        "outputs": [{"type": "uint256"}, {"type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

_RESOLVER_ABI: list[dict[str, Any]] = [
    {
        "name": "getAllSmartLendingAddresses",
        "inputs": [],
        "outputs": [{"type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# DEX deposit-estimate ABI (estimate=true reverts with the share carrier).
_DEX_ABI: list[dict[str, Any]] = [
    {
        "name": "deposit",
        "inputs": [{"type": "uint256"}, {"type": "uint256"}, {"type": "uint256"}, {"type": "bool"}],
        "outputs": [{"type": "uint256"}],
        "stateMutability": "payable",
        "type": "function",
    },
]


class FluidDexLpError(FluidSDKError):
    """SmartLending LP error."""


class FluidDexLpDepositDisabledError(FluidDexLpError):
    """The target pool has deposits disabled (51013) — retryable later."""


@dataclass(frozen=True)
class SmartLendingData:
    """Decoded + self-verified SmartLending wrapper state."""

    wrapper: str
    dex: str
    token0: str
    token1: str
    total_supply: int
    reserve0: int
    reserve1: int
    exchange_price: int


_HEX_CHARS = frozenset("0123456789abcdefABCDEF")


def _scrub_hex_payload(s: str) -> str | None:
    """Extract the CONTIGUOUS hex payload starting at the first ``0x``.

    Stops at the first non-hex character so trailing prose does NOT leak in:
    ``"revert 0x08c379a0, data: ..."`` → ``"0x08c379a0"`` (not
    ``"0x08c379a0da..."`` with the ``d``/``a`` from "data" appended, which would
    corrupt ``fluid_error_id()`` / carrier matching and misclassify a
    deposit-disabled pool or a quote failure).
    """
    idx = s.find("0x")
    if idx < 0:
        return None
    body_chars: list[str] = []
    for c in s[idx + 2 :]:
        if c not in _HEX_CHARS:
            break
        body_chars.append(c)
    if not body_chars:
        return None
    return "0x" + "".join(body_chars)


def _extract_revert_data(error: Exception) -> str | None:
    """Pull the hex revert payload out of a web3 ContractLogicError chain."""
    for attr in ("data", "message"):
        val = getattr(error, attr, None)
        if isinstance(val, str):
            hexpart = _scrub_hex_payload(val.strip().strip("'\""))
            if hexpart is not None:
                return hexpart
    args = getattr(error, "args", None)
    if args:
        for a in args:
            if isinstance(a, str):
                hexpart = _scrub_hex_payload(a.strip().strip("'\""))
                if hexpart is not None:
                    return hexpart
    return None


class FluidSmartLendingSDK:
    """Gateway-routed reads + tx builders for Fluid SmartLending LP."""

    def __init__(
        self,
        chain: str,
        resolver_address: str,
        rpc_url: str | None = None,
        gateway_client: GatewayClient | None = None,
    ) -> None:
        if rpc_url is None and gateway_client is None:
            raise FluidDexLpError("FluidSmartLendingSDK requires either rpc_url (deprecated) or gateway_client")
        self.chain = chain.lower()
        self._gateway_client = gateway_client
        if gateway_client is not None:
            from almanak.framework.web3.gateway_provider import GatewayWeb3Provider

            self.w3 = Web3(GatewayWeb3Provider(gateway_client, chain=self.chain))
        else:
            # Direct-RPC fallback for the local intent-test harness / local-dev
            # tooling ONLY — the SAME sanctioned connector-SDK pattern as the two
            # sibling Fluid SDKs (sdk.py, vault_sdk.py) and ~15 other connectors.
            # In the strategy container this branch is unreachable: the compiler's
            # ``_build_sdk`` passes ``rpc_url=None`` whenever a gateway is connected
            # and strategies hold no RPC URL, so production reads always route
            # through ``GatewayWeb3Provider``. Belt-and-suspenders: in a HOSTED
            # deployment (the security perimeter) an rpc_url fallback must NEVER
            # be reachable — fail closed rather than open a direct egress path.
            # Codebase-wide migration off this pattern is tracked in VIB-5122.
            from almanak.framework.deployment.mode import is_hosted

            if is_hosted():
                raise FluidDexLpError(
                    "FluidSmartLendingSDK direct-RPC fallback is forbidden in hosted deployments — "
                    "a connected gateway is required (gateway boundary). This indicates a "
                    "misconfiguration: the strategy container must never hold an RPC URL."
                )
            self.w3 = Web3(HTTPProvider(rpc_url))  # vib-2986-exempt: gateway-internal fallback (local/test only)
        self._resolver = self.w3.eth.contract(
            address=Web3.to_checksum_address(resolver_address),
            abi=_RESOLVER_ABI,
        )

    # -- enumeration --------------------------------------------------------

    def get_all_smart_lendings(self) -> list[str]:
        try:
            addrs = self._resolver.functions.getAllSmartLendingAddresses().call()
            return [Web3.to_checksum_address(a) for a in addrs]
        except Exception as e:  # noqa: BLE001
            raise FluidDexLpError(f"Failed to enumerate SmartLending wrappers: {e}") from e

    # -- wrapper reads ------------------------------------------------------

    def _wrapper(self, wrapper: str):
        return self.w3.eth.contract(address=Web3.to_checksum_address(wrapper), abi=_WRAPPER_ABI)

    def get_share_balance(self, wrapper: str, owner: str) -> int:
        try:
            return int(self._wrapper(wrapper).functions.balanceOf(Web3.to_checksum_address(owner)).call())
        except Exception as e:  # noqa: BLE001
            raise FluidDexLpError(f"Failed to read SmartLending share balance on {wrapper}: {e}") from e

    def get_smart_lending_data(self, wrapper: str) -> SmartLendingData:
        """Read + SELF-VERIFY the resolver struct (VIB-5024 decode-fragility guard).

        ``getSmartLendingEntireData`` is decoded by word position
        (``[6]=totalSupply, [7]=reserve0, [8]=reserve1, [9]=token0,
        [10]=token1, [11]=dex, [14]=exchange_price``). Because positional
        decoding of an undocumented resolver struct is the VIB-5024/5038
        getSlot0 fragility class, every decoded address/total is cross-checked
        against the wrapper's OWN getters (``TOKEN0/TOKEN1/DEX/totalSupply``)
        before being trusted. Any mismatch fails closed.
        """
        checksum = Web3.to_checksum_address(wrapper)
        # getSmartLendingEntireData(address) selector.
        selector = self.w3.keccak(text="getSmartLendingEntireData(address)")[:4].hex()
        if not selector.startswith("0x"):
            selector = "0x" + selector
        data = selector + checksum[2:].lower().rjust(64, "0")
        try:
            raw = self.w3.eth.call({"to": self._resolver.address, "data": HexStr(data)})
        except Exception as e:  # noqa: BLE001
            raise FluidDexLpError(f"getSmartLendingEntireData failed for {wrapper}: {e}") from e

        hexstr = raw.hex()
        if hexstr.startswith("0x"):
            hexstr = hexstr[2:]
        # ABI-encoded dynamic struct: word[0] = offset (0x20). Struct fields
        # begin at that offset; with offset 0x20 the struct word index i maps
        # to absolute word (1 + i). We index the absolute words directly.
        words = [hexstr[i : i + 64] for i in range(0, len(hexstr), 64)]
        if len(words) < 15:
            raise FluidDexLpError(f"getSmartLendingEntireData returned {len(words)} words (<15) for {wrapper}")

        def w_int(i: int) -> int:
            return int(words[i], 16)

        def w_addr(i: int) -> str:
            return Web3.to_checksum_address("0x" + words[i][24:])

        total_supply = w_int(6)
        reserve0 = w_int(7)
        reserve1 = w_int(8)
        token0 = w_addr(9)
        token1 = w_addr(10)
        dex = w_addr(11)
        exchange_price = w_int(14)

        # Self-verification against the wrapper's own getters.
        wc = self._wrapper(wrapper)
        try:
            on_t0 = Web3.to_checksum_address(wc.functions.TOKEN0().call())
            on_t1 = Web3.to_checksum_address(wc.functions.TOKEN1().call())
            on_dex = Web3.to_checksum_address(wc.functions.DEX().call())
            on_ts = int(wc.functions.totalSupply().call())
        except Exception as e:  # noqa: BLE001
            raise FluidDexLpError(f"SmartLending self-verify getters failed for {wrapper}: {e}") from e
        if (token0, token1, dex) != (on_t0, on_t1, on_dex) or total_supply != on_ts:
            raise FluidDexLpError(
                "SmartLending struct decode failed self-verification "
                f"(wrapper={wrapper}): decoded token0/token1/dex/totalSupply="
                f"{token0}/{token1}/{dex}/{total_supply} vs on-chain "
                f"{on_t0}/{on_t1}/{on_dex}/{on_ts}"
            )

        return SmartLendingData(
            wrapper=checksum,
            dex=dex,
            token0=token0,
            token1=token1,
            total_supply=total_supply,
            reserve0=reserve0,
            reserve1=reserve1,
            exchange_price=exchange_price,
        )

    def position_token_amounts(self, wrapper: str, shares: int) -> tuple[int, int]:
        """Per-share proportional claim on the pool reserves (base units)."""
        if shares <= 0:
            return (0, 0)
        d = self.get_smart_lending_data(wrapper)
        if d.total_supply <= 0:
            return (0, 0)
        t0 = shares * d.reserve0 // d.total_supply
        t1 = shares * d.reserve1 // d.total_supply
        return (t0, t1)

    # -- deposit quote + enabled pre-flight ---------------------------------

    def quote_deposit_shares(self, dex: str, token0_amt: int, token1_amt: int) -> int:
        """Estimate shares for a deposit via the DEX estimate revert-carrier.

        ``DEX.deposit(t0, t1, 0, estimate=true)`` reverts with selector
        ``0xe8d35d06`` carrying the share amount in word 0 (verified on-chain;
        equals the wrapper mint when the exchange price is 1e18).
        """
        dex_c = self.w3.eth.contract(address=Web3.to_checksum_address(dex), abi=_DEX_ABI)
        try:
            # estimate=true always reverts; a non-revert is unexpected.
            shares = dex_c.functions.deposit(token0_amt, token1_amt, 0, True).call()
            return int(shares)
        except Exception as e:  # noqa: BLE001
            revert = _extract_revert_data(e)
            if revert and revert.lower().startswith(_DEX_PERFECT_OUTPUT_SELECTOR):
                body = revert[len(_DEX_PERFECT_OUTPUT_SELECTOR) :]
                if len(body) >= 64:
                    return int(body[:64], 16)
            raise FluidDexLpError(
                f"Deposit-share estimate failed on DEX {dex}: {decode_fluid_revert(revert or '')}"
            ) from e

    def check_deposit_enabled(self, wrapper: str, token0_amt: int, token1_amt: int, wallet: str) -> None:
        """Refuse a deposit-disabled pool at COMPILE (the 51013 pre-flight).

        eth_call of the wrapper ``deposit`` with NO allowance: the
        ``UserSupplyInNotOn`` gate is checked BEFORE the token pull, so a
        disabled pool reverts 51013; an enabled pool reverts later on the
        token transferFrom (``FluidSafeTransferError``) — which we treat as
        ENABLED. No state overrides (gateway-safe, token-agnostic).
        """
        wc = self._wrapper(wrapper)
        try:
            wc.functions.deposit(token0_amt, token1_amt, 0, Web3.to_checksum_address(wallet)).call(
                {"from": Web3.to_checksum_address(wallet)}
            )
            return  # would-succeed → enabled
        except Exception as e:  # noqa: BLE001
            revert = _extract_revert_data(e)
            if revert and fluid_error_id(revert) == _DEX_USER_SUPPLY_IN_NOT_ON:
                raise FluidDexLpDepositDisabledError(
                    f"Fluid SmartLending pool {wrapper} has deposits disabled "
                    f"(DexT1__UserSupplyInNotOn / {_DEX_USER_SUPPLY_IN_NOT_ON}) — limit-gated (retryable)"
                ) from e
            # Any other revert (e.g. FluidSafeTransferError from the unapproved
            # token pull) means the supply gate PASSED → pool is enabled.
            return

    # -- tx builders --------------------------------------------------------

    def build_deposit_tx(
        self, wrapper: str, token0_amt: int, token1_amt: int, min_shares: int, to: str, value: int = 0
    ) -> dict[str, Any]:
        contract = Web3().eth.contract(abi=_WRAPPER_ABI)
        return {
            "to": Web3.to_checksum_address(wrapper),
            "data": contract.encode_abi(
                "deposit", args=[token0_amt, token1_amt, min_shares, Web3.to_checksum_address(to)]
            ),
            "value": value,
            # ``gas_estimate`` is the key the orchestrator's
            # ``_build_unsigned_transactions`` reads (default 100k otherwise);
            # a DEX-LP deposit is ~190k gas, so the wrong key OOGs the tx.
            "gas_estimate": _GAS_DEPOSIT,
        }

    def build_withdraw_tx(
        self, wrapper: str, token0_amt: int, token1_amt: int, max_shares: int, to: str
    ) -> dict[str, Any]:
        contract = Web3().eth.contract(abi=_WRAPPER_ABI)
        return {
            "to": Web3.to_checksum_address(wrapper),
            "data": contract.encode_abi(
                "withdraw", args=[token0_amt, token1_amt, max_shares, Web3.to_checksum_address(to)]
            ),
            "value": 0,
            "gas_estimate": _GAS_WITHDRAW,
        }

    def build_withdraw_perfect_tx(
        self, wrapper: str, shares: int, min_token0: int, min_token1: int, to: str
    ) -> dict[str, Any]:
        contract = Web3().eth.contract(abi=_WRAPPER_ABI)
        return {
            "to": Web3.to_checksum_address(wrapper),
            "data": contract.encode_abi(
                "withdrawPerfect", args=[shares, min_token0, min_token1, Web3.to_checksum_address(to)]
            ),
            "value": 0,
            "gas_estimate": _GAS_WITHDRAW,
        }


__all__ = [
    "FluidDexLpDepositDisabledError",
    "FluidDexLpError",
    "FluidSmartLendingSDK",
    "SmartLendingData",
]
